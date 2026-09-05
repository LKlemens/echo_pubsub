defmodule EchoPubSub.Producer do
  @moduledoc false
  use GenServer
  use TypedStruct
  require Logger

  alias EchoPubSub.FaultInjection

  @type cursor :: non_neg_integer()
  @type group :: atom()

  @typedoc """
  Producer GenServer state.

  Fields:

    * `group` - the `:pg` group this producer delivers to (its delivery scope)
    * `write_cursor` - count of messages ever written; the next write lands here
    * `read_cursors` - map of node to its next-needed cursor; everything below it that node has acked
    * `buffer` - ring buffer of the last `buffer_size` messages; oldest overwritten on wrap
    * `batch_interval` - milliseconds to batch writes before a flush; `0` flushes immediately
    * `call_timeout` - timeout in milliseconds for synchronous calls to remote nodes
    * `capacity_warning_threshold` - buffer fill ratio (0.0-1.0) that triggers a capacity warning
    * `capacity_warning_interval` - minimum seconds between capacity warnings
    * `flush_timer` - reference of the pending flush timer, or `nil` when none is scheduled
    * `last_capacity_warning_at` - monotonic seconds of the last capacity warning, or `nil` if never
  """
  typedstruct enforce: true do
    field(:group, group())
    field(:write_cursor, cursor(), default: 0)
    field(:read_cursors, %{node() => cursor()})
    field(:buffer, :array.array())
    field(:batch_interval, timeout())
    field(:call_timeout, timeout())
    field(:capacity_warning_threshold, float())
    field(:capacity_warning_interval, non_neg_integer())
    field(:flush_timer, reference() | nil, enforce: false)
    field(:last_capacity_warning_at, integer() | nil, enforce: false)
  end

  # The retry flag threaded through the fan-out reduces: did any remote send fail,
  # so we must schedule a retry flush?
  @no_failures false
  @had_failure true

  def start_link(
        {buffer_size, batch_interval, call_timeout, capacity_warning_threshold,
         capacity_warning_interval, group}
      ) do
    GenServer.start_link(
      __MODULE__,
      {buffer_size, batch_interval, call_timeout, capacity_warning_threshold,
       capacity_warning_interval, group},
      name: name(group)
    )
  end

  def buffer_and_send(group, message) do
    GenServer.call(name(group), {:write, message})
  end

  def name(group) do
    Module.concat(group, Producer)
  end

  defp pg_members(group) do
    :pg.get_members(Phoenix.PubSub, group)
  end

  @impl GenServer
  def init(
        {buffer_size, batch_interval, call_timeout, capacity_warning_threshold,
         capacity_warning_interval, group}
      ) do
    {_ref, pids} = :pg.monitor(Phoenix.PubSub, group)

    Enum.each(pids, &GenServer.call(&1, {:register, node()}))

    state = %__MODULE__{
      group: group,
      write_cursor: 0,
      read_cursors: Map.new(pids, &{node(&1), 0}),
      buffer: :array.new(buffer_size),
      batch_interval: batch_interval,
      call_timeout: call_timeout,
      capacity_warning_threshold: capacity_warning_threshold,
      capacity_warning_interval: capacity_warning_interval,
      flush_timer: nil,
      last_capacity_warning_at: nil
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:write, message}, _from, state) do
    i = rem(state.write_cursor, :array.size(state.buffer))
    buffer = :array.set(i, message, state.buffer)

    flush_timer = maybe_start_flush_timer(state)

    state = %{
      state
      | buffer: buffer,
        write_cursor: state.write_cursor + 1,
        flush_timer: flush_timer
    }

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({_ref, :join, _group, new_pids}, state) do
    {state, has_failure} =
      Enum.reduce(new_pids, {state, @no_failures}, fn pid, {acc_state, any_failure?} ->
        {new_state, status} = process_joined(pid, acc_state)
        {new_state, any_failure? or status == :error}
      end)

    state = if has_failure, do: schedule_retry_flush(state), else: state
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({_ref, :leave, _group, _leaving}, state), do: {:noreply, state}

  @impl GenServer
  def handle_info(:flush_all, state) do
    min_read_cursor =
      state.read_cursors |> Map.values() |> Enum.min(fn -> state.write_cursor end)

    buffer_size = state.write_cursor - min_read_cursor
    buffer_capacity = :array.size(state.buffer)

    :telemetry.execute(
      [:echo_pubsub, :buffer, :flush],
      %{buffer_size: buffer_size, buffer_capacity: buffer_capacity},
      %{group: state.group}
    )

    state = maybe_warn_capacity(state, buffer_size, buffer_capacity)

    # Phoenix PubSub handles local dispatch automatically via dispatch/5,
    # so we only send messages to remote nodes to avoid duplicate local delivery
    remote_pids =
      pg_members(state.group)
      |> Enum.filter(&(node(&1) != node()))

    {state, has_failure} = fan_out(remote_pids, state)

    # Advance local node cursor since local delivery is handled by Phoenix.PubSub dispatch
    state = %{state | read_cursors: Map.put(state.read_cursors, node(), state.write_cursor)}

    state = %{state | flush_timer: nil}
    state = if has_failure, do: schedule_retry_flush(state), else: state
    {:noreply, state}
  end

  # Per-node sends are independent (each only advances its own read cursor over an
  # immutable buffer snapshot), so >= 2 remotes fan out concurrently: sum of
  # round-trips becomes the slowest one. Fewer, or disabled, stays sequential.
  defp fan_out(remote_pids, state) do
    if concurrent?() and length(remote_pids) >= 2 do
      fan_out_concurrent(remote_pids, state)
    else
      fan_out_sequential(remote_pids, state)
    end
  end

  defp fan_out_sequential(remote_pids, state) do
    Enum.reduce(remote_pids, {state, @no_failures}, fn pid, {acc_state, _any_failure?} = acc ->
      node = node(pid)
      apply_verdict(node, deliver(pid, prepare_batch(node, acc_state), acc_state), acc)
    end)
  end

  defp fan_out_concurrent(remote_pids, state) do
    supervisor = Module.concat(state.group, TaskSupervisor)
    call_timeout = state.call_timeout

    # Prepare batches here (reads own heap; big payloads stay refc-shared) so tasks
    # carry only message wrappers, never the whole ring buffer.
    jobs =
      Enum.map(remote_pids, fn pid ->
        node = node(pid)
        {pid, node, prepare_batch(node, state)}
      end)

    supervisor
    |> Task.Supervisor.async_stream_nolink(
      jobs,
      fn {pid, node, batch} -> {node, deliver_batch(pid, batch, call_timeout)} end,
      ordered: false,
      max_concurrency: length(jobs),
      timeout: :infinity
    )
    |> Enum.reduce({state, @no_failures}, fn
      {:ok, {node, verdict}}, acc -> apply_verdict(node, verdict, acc)
      # Task dies only on an unexpected bug (safe_call catches remote failures):
      # keep the cursor, force a retry.
      {:exit, _reason}, {acc_state, _any_failure?} -> {acc_state, @had_failure}
    end)
  end

  defp concurrent?, do: Application.get_env(:echo_pubsub, :concurrent_flush, true)

  # Pure: decide what this node needs. Runs in the producer (reads the buffer).
  defp prepare_batch(node, state) do
    next_needed = Map.get(state.read_cursors, node, 0)
    oldest_buffered = max(state.write_cursor - :array.size(state.buffer), 0)

    cond do
      # Node has acked everything written - nothing to send.
      next_needed >= state.write_cursor -> :caught_up
      # Node's next message was overwritten in the ring - can't replay without a gap.
      next_needed < oldest_buffered -> {:expired, oldest_buffered - next_needed}
      # Node is behind but its messages are still buffered - replay the gap in order.
      true -> {:messages, messages_since(next_needed, state)}
    end
  end

  # Remote call carrying no buffer, so it is safe in a task. Returns a verdict.
  defp deliver(pid, batch, state), do: deliver_batch(pid, batch, state.call_timeout)

  defp deliver_batch(_pid, :caught_up, _call_timeout), do: :ok

  defp deliver_batch(pid, {:expired, missed}, call_timeout) do
    case safe_call(pid, {:expired, node()}, call_timeout, nil) do
      :ok -> {:expired, missed}
      :error -> :expired_failed
    end
  end

  defp deliver_batch(pid, {:messages, messages}, call_timeout) do
    case safe_call(pid, messages, call_timeout, nil) do
      :ok -> :ok
      :error -> {:failed, length(messages)}
    end
  end

  # Fold a verdict into state + telemetry (producer only). Cursor advances only on ack.
  defp apply_verdict(node, :ok, {state, failure}), do: {advance(state, node), failure}

  defp apply_verdict(node, {:expired, missed}, {state, failure}) do
    emit_expired(state.group, node, missed)
    {advance(state, node), failure}
  end

  defp apply_verdict(node, {:failed, batch_size}, {state, _failure}) do
    emit_sync_failure(state.group, node, batch_size)
    {state, @had_failure}
  end

  defp apply_verdict(_node, :expired_failed, {state, _failure}), do: {state, @had_failure}

  defp advance(state, node) do
    %{state | read_cursors: Map.put(state.read_cursors, node, state.write_cursor)}
  end

  defp process_joined(pid, state) do
    node = node(pid)

    if Map.has_key?(state.read_cursors, node) do
      resume(pid, node, state)
    else
      GenServer.call(pid, {:register, node()})
      {%{state | read_cursors: Map.put(state.read_cursors, node, state.write_cursor)}, :ok}
    end
  end

  # Replay a rejoined node via the shared prepare/deliver path, so join and flush
  # stay in lock-step. Expiry is acked and retried like a batch: a lost notice
  # would be a silent gap, the exact failure this library prevents.
  defp resume(pid, node, state) do
    {new_state, failure} =
      apply_verdict(node, deliver(pid, prepare_batch(node, state), state), {state, @no_failures})

    {new_state, if(failure, do: :error, else: :ok)}
  end

  defp emit_expired(group, node, missed_messages) do
    :telemetry.execute(
      [:echo_pubsub, :buffer, :expired],
      %{count: 1, missed_messages: missed_messages},
      %{group: group, node: node}
    )
  end

  # Messages from `cursor` up to (but not including) the write cursor, in order.
  defp messages_since(cursor, state) do
    Enum.map(cursor..(state.write_cursor - 1)//1, &get_message(&1, state))
  end

  defp get_message(cursor, state) do
    i = rem(cursor, :array.size(state.buffer))
    :array.get(i, state.buffer)
  end

  defp maybe_start_flush_timer(%__MODULE__{batch_interval: 0} = state) do
    send(self(), :flush_all)
    state
  end

  defp maybe_start_flush_timer(state) do
    if is_nil(state.flush_timer) do
      %{state | flush_timer: Process.send_after(self(), :flush_all, state.batch_interval)}
    else
      state
    end
  end

  @retry_interval 200
  defp schedule_retry_flush(state) do
    if is_nil(state.flush_timer) do
      :telemetry.execute(
        [:echo_pubsub, :retry, :scheduled],
        %{count: 1},
        %{group: state.group}
      )

      %{state | flush_timer: Process.send_after(self(), :flush_all, @retry_interval)}
    else
      state
    end
  end

  # Under an injected fault, outgoing sends short-circuit to :error so the batch
  # stays buffered and replays on recovery - the send-side half of the partition.
  defp safe_call(pid, messages, call_timeout, _state) do
    if FaultInjection.ok?() do
      do_safe_call(pid, messages, call_timeout)
    else
      :error
    end
  end

  defp do_safe_call(pid, messages, call_timeout) do
    case GenServer.call(pid, messages, call_timeout) do
      :ok -> :ok
      _ -> :error
    end
  catch
    _, _ -> :error
  end

  defp emit_sync_failure(group, node, batch_size) do
    :telemetry.execute(
      [:echo_pubsub, :sync, :failure],
      %{count: 1, batch_size: batch_size},
      %{group: group, node: node}
    )
  end

  defp maybe_warn_capacity(state, _buffer_size, 0), do: state

  defp maybe_warn_capacity(state, buffer_size, buffer_capacity) do
    now = System.monotonic_time(:second)
    ratio = buffer_size / buffer_capacity

    should_warn =
      ratio >= state.capacity_warning_threshold and
        (is_nil(state.last_capacity_warning_at) or
           now - state.last_capacity_warning_at >= state.capacity_warning_interval)

    if should_warn do
      percentage = Float.round(ratio * 100, 1)

      :telemetry.execute(
        [:echo_pubsub, :buffer, :capacity_warning],
        %{buffer_size: buffer_size, buffer_capacity: buffer_capacity, ratio: ratio},
        %{group: state.group}
      )

      Logger.warning(
        "Buffer at #{percentage}% capacity (#{buffer_size}/#{buffer_capacity}) for group #{inspect(state.group)}"
      )

      %{state | last_capacity_warning_at: now}
    else
      state
    end
  end
end
