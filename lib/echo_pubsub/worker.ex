defmodule EchoPubSub.Worker do
  @moduledoc false
  use GenServer

  alias EchoPubSub.FaultInjection

  def start_link({name, group}) do
    GenServer.start_link(__MODULE__, {name, group}, name: Module.concat(group, Worker))
  end

  @impl true
  def init({name, group}) do
    :ok = pg_join(group)
    {:ok, %{pubsub: name, registrations: MapSet.new(), last_batch: []}}
  end

  @impl true
  def handle_call({:forward_to_local, topic, message, dispatcher}, _from, state) do
    Phoenix.PubSub.local_broadcast(state.pubsub, topic, message, dispatcher)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call([{:forward_to_local, _, _, _} | _] = messages, from, state) do
    state = %{state | last_batch: messages}

    if FaultInjection.ok?() do
      deliver_batch(messages, from, state)
      {:reply, :ok, state}
    else
      {:reply, :error, state}
    end
  end

  @impl true
  def handle_call({:expired, node}, _from, state) do
    if FaultInjection.ok?() do
      broadcast_expired_message(state.pubsub, node)
      {:reply, :ok, state}
    else
      {:reply, :error, state}
    end
  end

  @impl true
  def handle_call({:register, node}, _from, state) do
    if MapSet.member?(state.registrations, node),
      do: broadcast_expired_message(state.pubsub, node)

    {:reply, :ok, %{state | registrations: MapSet.put(state.registrations, node)}}
  end

  @impl true
  def handle_call({:registered?, node}, _from, state) do
    {:reply, MapSet.member?(state.registrations, node), state}
  end

  @impl true
  def handle_call(:get_last_batch, _from, state) do
    {:reply, state.last_batch, state}
  end

  @impl true
  def handle_call(_, _from, state) do
    {:reply, {:error, :bad_message}, state}
  end

  defp deliver_batch(messages, from, state) do
    Enum.each(messages, fn message -> handle_call(message, from, state) end)
  end

  defp broadcast_expired_message(pubsub, node) do
    topics = Registry.select(pubsub, [{{:"$1", :_, :_}, [], [:"$1"]}])

    Enum.each(topics, fn topic ->
      Phoenix.PubSub.local_broadcast(pubsub, topic, {:cursor_expired, node})
    end)
  end

  defp pg_join(group) do
    :ok = :pg.join(Phoenix.PubSub, group, self())
  end
end
