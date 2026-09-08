defmodule EchoPubSub.ThroughputBench do
  @moduledoc """
  Shared publisher-side helper for the throughput benchmark, used by both the local
  `:peer` harness (`bench/throughput.exs`, via `:peer.call/5`) and the Fly cluster
  runner (`EchoPubSub.Bench.Runner`). Compiled in `:test` and `:bench` so it runs
  compiled (interpreting the publish loop would wreck the timing).
  """

  # counters index 1 = buffer overflow (:expired), index 2 = capacity warnings
  @counters_key {__MODULE__, :counters}
  @pubsub PubSubTest

  @doc """
  Attach telemetry handlers counting buffer-overflow and capacity-warning events
  on this node (resetting any previous counters), so the publisher can tell whether
  the sweep pushed the system into overflow.
  """
  @spec install_counters() :: :ok
  def install_counters do
    _ = :telemetry.detach(handler_id())

    ref = :counters.new(2, [:write_concurrency])
    :persistent_term.put(@counters_key, ref)

    :telemetry.attach_many(
      handler_id(),
      [
        [:echo_pubsub, :buffer, :expired],
        [:echo_pubsub, :buffer, :capacity_warning]
      ],
      &__MODULE__.handle_event/4,
      nil
    )

    :ok
  end

  @doc false
  def handle_event([:echo_pubsub, :buffer, :expired], measurements, _meta, _cfg) do
    :counters.add(counters(), 1, Map.get(measurements, :count, 1))
  end

  def handle_event([:echo_pubsub, :buffer, :capacity_warning], _measurements, _meta, _cfg) do
    :counters.add(counters(), 2, 1)
  end

  @doc "Return `{overflow_count, capacity_warning_count}` observed since the last install."
  @spec read_counters() :: {non_neg_integer(), non_neg_integer()}
  def read_counters do
    ref = counters()
    {:counters.get(ref, 1), :counters.get(ref, 2)}
  end

  defp publish_loop(0, _topic, _payload), do: :ok

  defp publish_loop(n, topic, payload) do
    Phoenix.PubSub.broadcast!(@pubsub, topic, {:m, n, payload})
    publish_loop(n - 1, topic, payload)
  end

  @doc """
  Measure **end-to-end delivery**: publish `count` messages from `publishers`
  concurrent processes (distinct pids spread across the producer pool), then block
  until every `subscriber_nodes` collector has received all `count`. Runs on the
  publisher so the span is one clock. Returns `{elapsed_us, received_counts}` (one
  count per node, same order) for loss verification.
  """
  @spec run_e2e(pos_integer(), String.t(), binary(), [node()], pos_integer()) ::
          {non_neg_integer(), [non_neg_integer()]}
  def run_e2e(count, topic, payload, subscriber_nodes, publishers) do
    start = System.monotonic_time(:microsecond)

    count
    |> shares(publishers)
    |> Enum.map(fn share -> Task.async(fn -> publish_loop(share, topic, payload) end) end)
    |> Task.await_many(:infinity)

    received_counts =
      Enum.map(subscriber_nodes, fn node ->
        GenServer.call({EchoPubSub.BenchCollector, node}, {:await_count, count}, :infinity)
      end)

    elapsed = System.monotonic_time(:microsecond) - start
    {elapsed, received_counts}
  end

  # Split `count` into `publishers` nearly-equal chunks that sum to `count`.
  # `count` rarely divides evenly (e.g. 10 over 3 is 3 each with 1 left over), so
  # each chunk gets the whole part `div(count, publishers)` and the `leftover`
  # `rem(count, publishers)` messages are handed to the first chunks, one apiece.
  # e.g. shares(10, 3) => [4, 3, 3] (3 each, and the 1 leftover goes to the first).
  defp shares(count, publishers) do
    base = div(count, publishers)
    leftover = rem(count, publishers)
    for i <- 1..publishers, do: share_for(i, base, leftover)
  end

  # The remainder is spread over the first `leftover` publishers, one extra each;
  # every other publisher gets the base amount.
  defp share_for(index, base, leftover) when index <= leftover, do: base + 1
  defp share_for(_index, base, _leftover), do: base

  defp counters, do: :persistent_term.get(@counters_key)
  defp handler_id, do: "throughput-bench-#{node()}"
end
