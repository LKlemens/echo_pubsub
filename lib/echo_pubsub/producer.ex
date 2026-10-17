defmodule EchoPubSub.Producer do
  @moduledoc """
  Sends this node's broadcasts to the workers on other nodes, one producer per
  pool group.

  Every broadcast is written to a ring buffer of the last `buffer_size`
  messages. For each remote node the producer keeps a read cursor: the position
  up to which that node has acknowledged. A flush sends each node the messages
  from its cursor to the write cursor, and the cursor moves only when the node's
  worker replies `:ok`, so a failed batch is sent again on the next flush.

  Remote calls run in tasks, so a slow or hung node never blocks broadcasts.
  A node is registered before its first batch:

    * `{:register, node}` when the producer has no cursor for it (new node,
      forgotten node, or this producer restarted), so a worker that knew this
      producer reports `{:cursor_expired, node}`
    * `{:resume, node}` when the cursor survived and only the worker restarted

  A node more than `buffer_size` messages behind gets `{:expired, node}`
  instead of messages it can no longer be sent. Local subscribers never go
  through the producer: Phoenix delivers local broadcasts directly.
  """
  use GenServer
  use TypedStruct
  require Logger

  require EchoPubSub.FaultInjection, as: FaultInjection

  @type cursor :: non_neg_integer()
  @type group :: atom()

  @typedoc """
  Producer GenServer state.

  Fields:

    * `group` - the `:pg` group this producer delivers to (its delivery scope)
    * `write_cursor` - count of messages ever written; the next write lands here
    * `read_cursors` - map of node to its next-needed cursor; everything below it that node has acked.
      A node is absent until its registration is acked
    * `registering` - map of node to the `{task reference, start cursor, worker pid}` of its pending
      registration; the reference is `nil` while a failed attempt waits for its retry
    * `workers` - map of node to the worker pid this producer last registered with
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
    field(:registering, %{node() => {reference() | nil, cursor(), pid()}}, default: %{})
    field(:workers, %{node() => pid()}, default: %{})
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

    state = %__MODULE__{
      group: group,
      write_cursor: 0,
      read_cursors: %{},
      registering: %{},
      workers: %{},
      in_flight: %{},
      buffer: :array.new(buffer_size),
      batch_interval: batch_interval,
      call_timeout: call_timeout,
      capacity_warning_threshold: capacity_warning_threshold,
      capacity_warning_interval: capacity_warning_interval,
      flush_timer: nil,
      last_capacity_warning_at: nil
    }

    # Registration runs in tasks, so a hung peer cannot block startup.
    {:ok, Enum.reduce(pids, state, &process_joined/2)}
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
    members = pg_members(state.group)
    state = forget_departed(state, Enum.map(members, &node/1))

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
    remote_pids = Enum.filter(members, &(node(&1) != node()))

    state = Enum.reduce(remote_pids, state, &dispatch_node(&1, node(&1), &2))

    # Advance local node cursor since local delivery is handled by Phoenix.PubSub dispatch
    state = %{state | read_cursors: Map.put(state.read_cursors, node(), state.write_cursor)}

    {:noreply, %{state | flush_timer: nil}}
  end

  # A registration or delivery task reporting back. A delivery advances the cursor
  # to the cursor the batch was prepared at, not to the current one: writes that
  # landed while the batch was in flight are not acked by it.
  @impl GenServer
  def handle_info({ref, result}, state) when is_reference(ref) do
    case find_task(state, ref) do
      {:registration, node, start, pid} ->
        Process.demonitor(ref, [:flush])
        {:noreply, apply_registration(state, node, start, pid, result)}

      {:delivery, node, target} ->
        Process.demonitor(ref, [:flush])
        state = %{state | in_flight: Map.delete(state.in_flight, node)}
        {:noreply, state |> apply_verdict(node, target, result) |> maybe_follow_up(node)}

      :unknown ->
        {:noreply, state}
    end
  end

  # A task that died without reporting - only an unexpected bug, since safe_call
  # catches every remote failure. Keep the cursor, force a retry.
  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case find_task(state, ref) do
      {:registration, node, start, pid} ->
        {:noreply, apply_registration(state, node, start, pid, :error)}

      {:delivery, node, _target} ->
        state = %{state | in_flight: Map.delete(state.in_flight, node)}
        {:noreply, schedule_retry_flush(state)}

      :unknown ->
        {:noreply, state}
    end
  end

  # Anything else must not take the buffer and every read cursor down with it.
  @impl GenServer
  def handle_info(_message, state), do: {:noreply, state}

  # Registrations and deliveries run in tasks and report back, so the producer
  # keeps serving writes while a slow or unreachable peer burns its call timeout.
  # At most one task per node runs at a time, which keeps batches arriving in
  # cursor order and keeps every message behind the node's registration.
  defp dispatch_node(pid, node, state) do
    cond do
      registration_running?(state, node) or delivery_running?(state, node) -> state
      needs_registration?(state, pid, node) -> start_registration(pid, node, state)
      true -> start_delivery(pid, node, prepare_batch(node, state), state)
    end
  end

  defp registration_running?(state, node) do
    match?({:ok, {ref, _start, _pid}} when is_reference(ref), Map.fetch(state.registering, node))
  end

  # A node needs registering when this producer has never registered it, or when
  # its worker has restarted since (a new pid that does not know this producer).
  defp needs_registration?(state, pid, node), do: Map.get(state.workers, node) != pid

  defp delivery_running?(state, node), do: Map.has_key?(state.in_flight, node)

  defp start_registration(pid, node, state) do
    start = registration_start(state, node)
    call_timeout = state.call_timeout
    supervisor = Module.concat(state.group, TaskSupervisor)
    message = registration_message(state, node)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        safe_call(pid, message, call_timeout, nil)
      end)

    %{state | registering: Map.put(state.registering, node, {task.ref, start, pid})}
  end

  # A producer that still holds the node's cursor (its worker restarted) has lost
  # nothing, so it resumes: the worker records it without a cursor_expired.
  defp registration_message(state, node) do
    if Map.has_key?(state.read_cursors, node),
      do: {:resume, node()},
      else: {:register, node()}
  end

  # On the first attempt the node joins the current batch window: it starts at
  # the cursor the last flush ended at (the local node's cursor), so it gets every
  # write not yet flushed, as it would have had it been a member when that flush
  # ran. A retry keeps that start, so writes flushed while earlier attempts were
  # failing are still delivered.
  defp registration_start(state, node) do
    case Map.fetch(state.registering, node) do
      {:ok, {nil, start, _pid}} -> start
      :error -> Map.get(state.read_cursors, node(), 0)
    end
  end

  # A node registered again after a worker restart keeps its read cursor, so the
  # messages its old worker missed are still replayed.
  defp apply_registration(state, node, start, pid, :ok) do
    %{
      state
      | registering: Map.delete(state.registering, node),
        workers: Map.put(state.workers, node, pid),
        read_cursors: Map.put_new(state.read_cursors, node, start)
    }
    |> maybe_follow_up(node)
  end

  # The node stays unregistered, so the retry flush registers it again from the
  # same start.
  defp apply_registration(state, node, start, pid, :error) do
    %{state | registering: Map.put(state.registering, node, {nil, start, pid})}
    |> schedule_retry_flush()
  end

  # A node that left and fell more than a buffer behind could only be told it
  # expired, so it is forgotten. If it comes back it registers again and its
  # worker decides: one that still remembers this producer reports cursor_expired,
  # a rebooted one starts fresh. The worker's memory survives a worker restart.
  defp forget_departed(state, member_nodes) do
    buffer_size = :array.size(state.buffer)

    departed =
      for {node, cursor} <- state.read_cursors,
          node != node(),
          node not in member_nodes,
          not delivery_running?(state, node),
          state.write_cursor - cursor > buffer_size do
        node
      end

    %{
      state
      | read_cursors: Map.drop(state.read_cursors, departed),
        workers: Map.drop(state.workers, departed),
        registering: Map.drop(state.registering, departed)
    }
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

  # Every task ref belongs to exactly one map: registrations and deliveries
  # never share a ref.
  defp find_task(state, ref) do
    case {find_entry(state.registering, ref), find_entry(state.in_flight, ref)} do
      {{node, {_ref, start, pid}}, nil} -> {:registration, node, start, pid}
      {nil, {node, {_ref, target}}} -> {:delivery, node, target}
      {nil, nil} -> :unknown
    end
  end

  defp find_entry(tasks, ref) do
    Enum.find(tasks, fn {_node, task} -> elem(task, 0) == ref end)
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
  # Only registered nodes get here, so the read cursor always exists.
  defp prepare_batch(node, state) do
    next_needed = Map.fetch!(state.read_cursors, node)
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

  # Join and flush share dispatch_node/3 so the register / deliver decision lives
  # in one place. The local worker is skipped, as in flush_all: Phoenix already
  # delivers local broadcasts, so registering it would only send its subscribers a
  # false cursor_expired.
  defp process_joined(pid, state) when node(pid) == node(), do: state
  defp process_joined(pid, state), do: dispatch_node(pid, node(pid), state)

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
    FaultInjection.if_ok do
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
