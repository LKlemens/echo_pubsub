defmodule EchoPubSub.BenchCollector do
  @moduledoc """
  Subscriber for the end-to-end throughput benchmark. One named collector per node
  counts `{:m, _, _}` messages; the publisher blocks on `await_count/2` per node
  until each has received all N. Count-based (not a sentinel) so it is correct across
  a producer pool, where messages from different senders take independent, unordered
  pipelines. Supervised in the Fly app; started ad hoc via `ensure_started/1` on the
  local `:peer` harness.
  """
  use GenServer

  @name __MODULE__
  @pubsub PubSubTest

  @doc "Supervised start (Fly app)."
  @spec start_link(String.t()) :: GenServer.on_start()
  def start_link(topic), do: GenServer.start_link(__MODULE__, topic, name: @name)

  @doc "Start the collector subscribed to `topic` if absent, else reset it. Idempotent."
  @spec ensure_started(String.t()) :: :ok
  def ensure_started(topic) do
    case Process.whereis(@name) do
      nil ->
        {:ok, _pid} = GenServer.start(__MODULE__, topic, name: @name)
        :ok

      _pid ->
        reset()
    end
  end

  @doc "Clear the received count before a new sample."
  @spec reset() :: :ok
  def reset, do: GenServer.call(@name, :reset)

  @doc "Number of `{:m, _, _}` messages received since the last reset."
  @spec count() :: non_neg_integer()
  def count, do: GenServer.call(@name, :count)

  @doc "Block until at least `target` messages have arrived; returns the count then."
  @spec await_count(non_neg_integer(), timeout()) :: non_neg_integer()
  def await_count(target, timeout \\ :infinity),
    do: GenServer.call(@name, {:await_count, target}, timeout)

  @impl GenServer
  def init(topic) do
    Phoenix.PubSub.subscribe(@pubsub, topic)
    {:ok, %{count: 0, waiting: nil}}
  end

  @impl GenServer
  def handle_call(:reset, _from, _state), do: {:reply, :ok, %{count: 0, waiting: nil}}

  def handle_call(:count, _from, state), do: {:reply, state.count, state}

  def handle_call({:await_count, target}, _from, %{count: count} = state) when count >= target do
    {:reply, count, state}
  end

  def handle_call({:await_count, target}, from, state) do
    {:noreply, %{state | waiting: {from, target}}}
  end

  @impl GenServer
  def handle_info({:m, _seq, _payload}, state) do
    count = state.count + 1

    case state.waiting do
      {from, target} when count >= target ->
        GenServer.reply(from, count)
        {:noreply, %{state | count: count, waiting: nil}}

      _ ->
        {:noreply, %{state | count: count}}
    end
  end

  # Ignore anything else on the topic (e.g. {:cursor_expired, node}).
  def handle_info(_other, state), do: {:noreply, state}
end
