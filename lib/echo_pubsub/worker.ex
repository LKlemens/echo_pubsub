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

  @doc "Starts the worker for `group`, broadcasting batches to the `name` PubSub server."
  @spec start_link({atom(), atom()}) :: GenServer.on_start()
  def start_link({name, group}) do
    GenServer.start_link(__MODULE__, {name, group}, name: name(group))
  end

  @doc "The registered name of the group's worker."
  @spec name(atom()) :: atom()
  def name(group), do: Module.concat(group, Worker)

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
    {:ok, %{pubsub: name, group: group, registrations: registrations_table(group)}}
  end

  @impl true
  def handle_call({:forward_to_local, _, _, _} = message, _from, state) do
    forward_to_local(state, message)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call([{:forward_to_local, _, _, _} | _] = messages, _from, state) do
    FaultInjection.if_ok state.group do
      Enum.each(messages, &forward_to_local(state, &1))
      {:reply, :ok, state}
    else
      {:reply, :error, state}
    end
  end

  @impl true
  def handle_call({:expired, node}, _from, state) do
    FaultInjection.if_ok state.group do
      broadcast_expired_message(state.pubsub, node)
      {:reply, :ok, state}
    else
      {:reply, :error, state}
    end
  end

  @impl true
  def handle_call({:register, node}, _from, state) do
    if registered?(state, node), do: broadcast_expired_message(state.pubsub, node)

    record_producer(state, node)
    {:reply, :ok, state}
  end

  # The producer kept this node's cursor across a worker restart, so nothing was
  # missed: record it for a later producer restart, without a notice.
  @impl true
  def handle_call({:resume, node}, _from, state) do
    record_producer(state, node)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(_, _from, state) do
    {:reply, {:error, :bad_message}, state}
  end

  defp registered?(state, node), do: :ets.member(state.registrations, node)

  defp record_producer(state, node), do: :ets.insert(state.registrations, {node})

  defp forward_to_local(state, {:forward_to_local, topic, message, dispatcher}) do
    Phoenix.PubSub.local_broadcast(state.pubsub, topic, message, dispatcher)
  end

  defp broadcast_expired_message(pubsub, node) do
    Enum.each(subscribed_topics(pubsub), fn topic ->
      Phoenix.PubSub.local_broadcast(pubsub, topic, {:cursor_expired, node})
    end)
  end

  # The registry holds one entry per subscriber, so a topic repeats once per
  # subscriber; without uniq each subscriber would get the notice that many times.
  defp subscribed_topics(pubsub) do
    pubsub |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}]) |> Enum.uniq()
  end

  defp pg_join(group) do
    :ok = :pg.join(Phoenix.PubSub, group, self())
  end
end
