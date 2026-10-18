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

    * `{:register, node, missed_writes?}` when the producer has no cursor for it (new node,
      forgotten node, or this producer restarted), so a worker that knew this
      producer reports `{:cursor_expired, node}`
    * `{:resume, node}` when the cursor survived and only the worker restarted

  After a failed registration or delivery the node is probed with `:ping` and
  is retried only once it answers, so a hung worker does not pile up one copy
  of the call per retry.

  A node more than `buffer_size` messages behind gets `{:expired, node}`
  instead of messages it can no longer be sent. Local subscribers never go
  through the producer: Phoenix delivers local broadcasts directly.
  """
  use GenServer

  alias EchoPubSub.Producer.Delivery
  alias EchoPubSub.Producer.Registration
  alias EchoPubSub.Producer.RingBuffer
  alias EchoPubSub.Producer.State
  alias EchoPubSub.Producer.Telemetry
  alias EchoPubSub.Producer.Timers

  @type group :: atom()

  @typedoc "Producer options; all are required. See `EchoPubSub` for what each one does."
  @type option ::
          {:group, group()}
          | {:buffer_size, pos_integer()}
          | {:batch_interval, non_neg_integer()}
          | {:call_timeout, timeout()}
          | {:retry_interval, pos_integer()}
          | {:capacity_warning_threshold, number()}
          | {:capacity_warning_interval, non_neg_integer()}

  @doc "Starts the producer for `opts[:group]`, registered under `name/1`."
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: name(Keyword.fetch!(opts, :group)))
  end

  @doc """
  Writes `message` to the group's buffer; it is sent to remote nodes on the next
  flush. Waits only for the local producer, never for remote nodes.
  """
  @spec buffer_and_send(group(), term()) :: :ok
  def buffer_and_send(group, message) do
    GenServer.call(name(group), {:write, message})
  end

  @doc "The registered name of the group's producer."
  @spec name(group()) :: atom()
  def name(group) do
    Module.concat(group, Producer)
  end

  defp pg_members(group) do
    :pg.get_members(Phoenix.PubSub, group)
  end

  @impl GenServer
  @spec init([option()]) :: {:ok, State.t()}
  def init(opts) do
    group = Keyword.fetch!(opts, :group)
    {_ref, pids} = :pg.monitor(Phoenix.PubSub, group)
    state = State.new(group, opts)

    # Registration runs in tasks, so a hung peer cannot block startup.
    {:ok, dispatch_members(state, pids)}
  end

  @impl GenServer
  def handle_call({:write, message}, _from, state) do
    state = state |> State.write(message) |> Timers.flush_soon()

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({_ref, :join, _group, new_pids}, state) do
    {:noreply, dispatch_members(state, new_pids)}
  end

  @impl GenServer
  def handle_info({_ref, :leave, _group, _leaving}, state), do: {:noreply, state}

  @impl GenServer
  def handle_info(:flush_all, state) do
    members = pg_members(state.group)

    state =
      state
      |> forget_departed(members)
      |> report_buffer_usage()
      |> dispatch_members(members)
      |> State.ack_local()
      |> State.clear_flush_timer()

    {:noreply, state}
  end

  # A registration or delivery task reporting back. A delivery advances the cursor
  # to the cursor the batch was prepared at, not to the current one: writes that
  # landed while the batch was in flight are not acked by it.
  @impl GenServer
  def handle_info({ref, result}, state) when is_reference(ref) do
    case State.find_task(state, ref) do
      {:registration, node, start, pid} ->
        Process.demonitor(ref, [:flush])
        {:noreply, Registration.finish(state, node, start, pid, result)}

      {:delivery, node, target} ->
        Process.demonitor(ref, [:flush])
        {:noreply, Delivery.finish(state, node, target, result)}

      :unknown ->
        {:noreply, state}
    end
  end

  # A task that died without reporting - only an unexpected bug, since Remote
  # catches every remote failure. Keep the cursor, force a retry.
  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case State.find_task(state, ref) do
      {:registration, node, start, pid} ->
        {:noreply, Registration.finish(state, node, start, pid, :error)}

      {:delivery, node, _target} ->
        {:noreply, Delivery.abort(state, node)}

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
      Registration.running?(state, node) or Delivery.running?(state, node) -> state
      State.suspect?(state, node) -> Delivery.probe(state, pid, node)
      Registration.needed?(state, pid, node) -> Registration.start(state, pid, node)
      true -> Delivery.start(state, pid, node)
    end
  end

  # A node that left and fell more than a buffer behind could only be told it
  # expired, so it is forgotten. If it comes back it registers again and its
  # worker decides: one that still remembers this producer reports cursor_expired,
  # a rebooted one starts fresh. The worker's memory survives a worker restart.
  defp forget_departed(state, members) do
    member_nodes = Enum.map(members, &node/1)

    departed =
      state.read_cursors |> Map.keys() |> Enum.filter(&departed?(state, &1, member_nodes))

    State.forget(state, departed)
  end

  # Gone from :pg, nothing in flight, and further behind than the buffer can replay.
  defp departed?(state, node, member_nodes) do
    node != node() and
      node not in member_nodes and
      not Delivery.running?(state, node) and
      State.beyond_buffer?(state, node)
  end

  defp report_buffer_usage(state) do
    unacked = State.unacked(state)
    capacity = RingBuffer.capacity(state.buffer)
    Telemetry.flush(state.group, unacked, capacity)

    last_warning_at =
      Telemetry.maybe_warn_capacity(
        state.group,
        unacked,
        capacity,
        state.capacity_warning_threshold,
        state.capacity_warning_interval,
        state.last_capacity_warning_at
      )

    State.set_last_warning(state, last_warning_at)
  end

  defp dispatch_members(state, pids), do: Enum.reduce(pids, state, &dispatch_member/2)

  # Join and flush share dispatch_node/3 so the register / deliver decision lives
  # in one place. The local worker is skipped: Phoenix already delivers local
  # broadcasts, so registering or sending to it would only duplicate them or send
  # its subscribers a false cursor_expired.
  defp dispatch_member(pid, state) when node(pid) == node(), do: state
  defp dispatch_member(pid, state), do: dispatch_node(pid, node(pid), state)
end
