defmodule EchoPubSub.TestSubscriber do
  @moduledoc "Test GenServer that subscribes to a topic and buffers received messages."
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def subscribe(pubsub, topic) do
    GenServer.call(__MODULE__, {:subscribe, pubsub, topic})
  end

  def get_message do
    GenServer.call(__MODULE__, :get_message)
  end

  @doc "Every message received so far, oldest first, without removing any."
  def messages do
    GenServer.call(__MODULE__, :messages)
  end

  @impl GenServer
  def init(_) do
    {:ok, []}
  end

  @impl GenServer
  def handle_call({:subscribe, pubsub, topic}, _from, messages) do
    Phoenix.PubSub.subscribe(pubsub, topic)
    {:reply, :ok, messages}
  end

  @impl GenServer
  def handle_call(:get_message, _from, messages) do
    {message, messages} = List.pop_at(messages, -1)
    {:reply, message, messages}
  end

  @impl GenServer
  def handle_call(:messages, _from, messages) do
    {:reply, Enum.reverse(messages), messages}
  end

  @impl GenServer
  def handle_info(message, buffer) do
    {:noreply, [message | buffer]}
  end
end
