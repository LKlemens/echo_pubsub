defmodule EchoPubSub.Worker do
  @moduledoc """
  Receives batches from the producers on other nodes and broadcasts them to
  this node's subscribers, one worker per pool group.

  The worker joins the group's `:pg` group, which is how producers find it.
  It replies `:ok` to a batch only after broadcasting it locally; the producer
  moves that node's read cursor only on that reply.

  It also remembers which producer nodes have registered with it, in an ETS
  table owned by the adapter supervisor so the record survives a worker
  restart. A `{:register, node}` from a producer it already knows means that
  producer lost this node's cursor, so the worker sends `{:cursor_expired, node}`
  to every local topic. `{:resume, node}` only records the producer, and
  `{:expired, node}` always sends the notice.
  """
  use GenServer

  require EchoPubSub.FaultInjection, as: FaultInjection

  def start_link({name, group}) do
    GenServer.start_link(__MODULE__, {name, group}, name: Module.concat(group, Worker))
  end

  @doc """
  Names the table of producer nodes registered with this group's worker.

  The adapter supervisor owns the table, so a restarted worker still knows which
  producers registered before and can tell them apart from new ones.
  """
  @spec registrations_table(atom()) :: atom()
  def registrations_table(group), do: Module.concat(group, Registrations)

  @impl true
  def init({name, group}) do
    :ok = pg_join(group)
    {:ok, %{pubsub: name, registrations: registrations_table(group)}}
  end

  @impl true
  def handle_call({:forward_to_local, topic, message, dispatcher}, _from, state) do
    Phoenix.PubSub.local_broadcast(state.pubsub, topic, message, dispatcher)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call([{:forward_to_local, _, _, _} | _] = messages, from, state) do
    FaultInjection.if_ok do
      deliver_batch(messages, from, state)
      {:reply, :ok, state}
    else
      {:reply, :error, state}
    end
  end

  @impl true
  def handle_call({:expired, node}, _from, state) do
    FaultInjection.if_ok do
      broadcast_expired_message(state.pubsub, node)
      # A producer that forgot this node reaches it here, not through :register,
      # so record it to still report a later restart of that producer.
      :ets.insert(state.registrations, {node})
      {:reply, :ok, state}
    else
      {:reply, :error, state}
    end
  end

  @impl true
  def handle_call({:register, node}, _from, state) do
    if registered?(state, node), do: broadcast_expired_message(state.pubsub, node)

    :ets.insert(state.registrations, {node})
    {:reply, :ok, state}
  end

  # The producer kept this node's cursor across a worker restart, so nothing was
  # missed: record it for a later producer restart, without a notice.
  @impl true
  def handle_call({:resume, node}, _from, state) do
    :ets.insert(state.registrations, {node})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(_, _from, state) do
    {:reply, {:error, :bad_message}, state}
  end

  defp registered?(state, node), do: :ets.member(state.registrations, node)

  defp deliver_batch(messages, from, state) do
    Enum.each(messages, fn message -> handle_call(message, from, state) end)
  end

  defp broadcast_expired_message(pubsub, node) do
    # The registry holds one entry per subscriber, so a topic repeats once per
    # subscriber; without uniq each subscriber would get the notice that many times.
    topics = pubsub |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}]) |> Enum.uniq()

    Enum.each(topics, fn topic ->
      Phoenix.PubSub.local_broadcast(pubsub, topic, {:cursor_expired, node})
    end)
  end

  defp pg_join(group) do
    :ok = :pg.join(Phoenix.PubSub, group, self())
  end
end
