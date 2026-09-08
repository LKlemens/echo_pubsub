defmodule EchoPubSub.Bench.Runner do
  @moduledoc """
  Trigger an end-to-end throughput run across the whole Fly cluster from one node.
  `batch_interval`/`pool_size`/`buffer_size` are deploy-time env (set in `fly.toml`);
  `run/1` measures the current cluster. Invoke via
  `bin/echo_pubsub rpc 'EchoPubSub.Bench.Runner.run(messages: 50_000, payload: 1024)'`.
  """
  alias EchoPubSub.{BenchCollector, ThroughputBench}

  @topic "bench"

  @doc "Opts: :messages (50_000), :payload bytes (1024), :publishers (schedulers), :samples (3)."
  @spec run(keyword()) :: map()
  def run(opts \\ []) do
    messages = Keyword.get(opts, :messages, 50_000)
    payload = :binary.copy("x", Keyword.get(opts, :payload, 1024))
    publishers = Keyword.get(opts, :publishers, System.schedulers_online())
    samples = Keyword.get(opts, :samples, 3)

    nodes = [node() | Node.list()]
    warmup = max(div(messages, 10), 1)

    Enum.each(nodes, &:erpc.call(&1, BenchCollector, :ensure_started, [@topic]))
    Process.sleep(500)

    ThroughputBench.run_e2e(warmup, @topic, payload, nodes, publishers)
    ThroughputBench.install_counters()

    results =
      for _ <- 1..samples do
        Enum.each(nodes, &:erpc.call(&1, BenchCollector, :reset, []))
        ThroughputBench.run_e2e(messages, @topic, payload, nodes, publishers)
      end

    {overflow, cap_warn} = ThroughputBench.read_counters()

    elapsed_times_us = Enum.map(results, fn {elapsed_us, _counts} -> elapsed_us end)
    median_us = elapsed_times_us |> Enum.sort() |> Enum.at(div(samples, 2))

    delivered? = Enum.all?(results, fn {_us, counts} -> Enum.all?(counts, &(&1 == messages)) end)

    median_seconds = median_us / 1_000_000
    msgs_per_sec = round(messages / median_seconds)

    IO.puts(
      "nodes=#{length(nodes)} payload=#{byte_size(payload)} msg/s=#{msgs_per_sec} " <>
        "delivered=#{delivered?} overflow=#{overflow} cap_warn=#{cap_warn}"
    )

    %{
      nodes: length(nodes),
      msgs_per_sec: msgs_per_sec,
      delivered: delivered?,
      overflow: overflow
    }
  end
end
