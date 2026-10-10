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
    * `in_flight` - map of node to the `{task reference, target cursor}` of the batch being delivered to it
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
    field(:in_flight, %{node() => {reference(), cursor()}}, default: %{})
    field(:buffer, :array.array())
    field(:batch_interval, timeout())
    field(:call_timeout, timeout())
    field(:capacity_warning_threshold, float())
    field(:capacity_warning_interval, non_neg_integer())
    field(:flush_timer, reference() | nil, enforce: false)
    field(:last_capacity_warning_at, integer() | nil, enforce: false)
  end

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
      in_flight: %{},
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

    state =
      %{state | buffer: buffer, write_cursor: state.write_cursor + 1}
      |> maybe_start_flush_timer()

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({_ref, :join, _group, new_pids}, state) do
    {:noreply, Enum.reduce(new_pids, state, &process_joined/2)}
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

    state = Enum.reduce(remote_pids, state, &dispatch_node(&1, node(&1), &2))

    # Advance local node cursor since local delivery is handled by Phoenix.PubSub dispatch
    state = %{state | read_cursors: Map.put(state.read_cursors, node(), state.write_cursor)}

    {:noreply, %{state | flush_timer: nil}}
  end

  # A delivery task reporting its verdict. The cursor advances to the cursor the
  # batch was prepared at, not to the current one: writes that landed while the
  # batch was in flight are not acked by it.
  @impl GenServer
  def handle_info({ref, verdict}, state) when is_reference(ref) do
    case pop_in_flight(state, ref) do
      :error ->
        {:noreply, state}

      {:ok, node, target, state} ->
        Process.demonitor(ref, [:flush])

        {:noreply, state |> apply_verdict(node, target, verdict) |> maybe_follow_up(node)}
    end
  end

  # A delivery task that died without reporting - only an unexpected bug, since
  # safe_call catches every remote failure. Keep the cursor, force a retry.
  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case pop_in_flight(state, ref) do
      :error -> {:noreply, state}
      {:ok, _node, _target, state} -> {:noreply, schedule_retry_flush(state)}
    end
  end

  # Anything else must not take the buffer and every read cursor down with it.
  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  # Deliveries run in tasks and report back, so the producer keeps serving writes
  # while a slow or unreachable peer burns its call timeout. At most one batch per
  # node is in flight, which is what keeps batches arriving in cursor order.
  defp dispatch_node(pid, node, state) do
    if Map.has_key?(state.in_flight, node) do
      state
    else
      start_delivery(pid, node, prepare_batch(node, state), state)
    end
  end

  defp start_delivery(_pid, _node, :caught_up, state), do: state

  defp start_delivery(pid, node, batch, state) do
    target = state.write_cursor
    call_timeout = state.call_timeout
    supervisor = Module.concat(state.group, TaskSupervisor)

    # The batch is prepared here (reads own heap; big payloads stay refc-shared)
    # so the task carries only message wrappers, never the whole ring buffer.
    task =
      Task.Supervisor.async_nolink(supervisor, fn -> deliver_batch(pid, batch, call_timeout) end)

    %{state | in_flight: Map.put(state.in_flight, node, {task.ref, target})}
  end

  defp pop_in_flight(state, ref) do
    case Enum.find(state.in_flight, fn {_node, {task_ref, _target}} -> task_ref == ref end) do
      nil ->
        :error

      {node, {_task_ref, target}} ->
        {:ok, node, target, %{state | in_flight: Map.delete(state.in_flight, node)}}
    end
  end

  # A node skipped during a flush because it was mid-delivery still needs the
  # writes that piled up behind that batch.
  defp maybe_follow_up(state, node) do
    if Map.get(state.read_cursors, node, 0) < state.write_cursor do
      maybe_start_flush_timer(state)
    else
      state
    end
  end

  # Pure: decide what this node needs. Runs in the producer (reads the buffer).
  defp prepare_batch(node, state) do
    next_needed = Map.get(state.read_cursors, node, 0)
    buffer_size = :array.size(state.buffer)
    # How many messages this node is behind. Read cursor only advances *to*
    # write_cursor, so lag is always >= 0.
    lag = state.write_cursor - next_needed

    cond do
      # Acked everything written - nothing to send.
      lag == 0 -> :caught_up
      # Behind by more than the buffer holds - the oldest (lag - buffer_size)
      # messages were overwritten and can't be replayed without a gap.
      lag > buffer_size -> {:expired, lag - buffer_size}
      # Behind but the whole gap is still buffered - replay it in order.
      true -> {:messages, messages_since(next_needed, state)}
    end
  end

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
  defp apply_verdict(state, node, target, :ok), do: advance(state, node, target)

  defp apply_verdict(state, node, target, {:expired, missed}) do
    emit_expired(state.group, node, missed)
    advance(state, node, target)
  end

  defp apply_verdict(state, node, _target, {:failed, batch_size}) do
    emit_sync_failure(state.group, node, batch_size)
    schedule_retry_flush(state)
  end

  defp apply_verdict(state, _node, _target, :expired_failed), do: schedule_retry_flush(state)

  # Records that `node` has acked every message below `target`.
  defp advance(state, node, target) do
    %{state | read_cursors: Map.put(state.read_cursors, node, target)}
  end

  # Join and flush share dispatch_node/3 so the caught-up / replay / expired
  # decision lives only in prepare_batch/2.
  defp process_joined(pid, state) do
    node = node(pid)

    if Map.has_key?(state.read_cursors, node) do
      dispatch_node(pid, node, state)
    else
      GenServer.call(pid, {:register, node()})
      %{state | read_cursors: Map.put(state.read_cursors, node, state.write_cursor)}
    end
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
