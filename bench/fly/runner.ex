defmodule EchoPubSub.Bench.Runner do
  @moduledoc """
  Trigger an end-to-end throughput run across the whole Fly cluster from one node.
  Per-run knobs are opts to `run/1`; `:pool_size`/`:buffer_size`/`:batch_interval`
  restart the PubSub on every node in-process (via `EchoPubSub.Bench.App.reconfigure/1`),
  so you can sweep them without a redeploy. Omitted ones fall back to the boot env
  (`fly.toml`). Invoke via
  `bin/echo_pubsub rpc 'EchoPubSub.Bench.Runner.run(messages: 50_000, payload: 10, batch_interval: 0)'`.
  """
  require Logger

  alias EchoPubSub.{BenchCollector, ThroughputBench}

  @topic "bench"

  @doc """
  Opts: `:messages` (50_000), `:payload` bytes (10), `:publishers` (schedulers),
  `:samples` (3), and the reconfigurable `:pool_size`/`:buffer_size`/`:batch_interval`.
  """
  @spec run(keyword()) :: map()
  def run(opts \\ []) do
    messages = Keyword.get(opts, :messages, 50_000)
    payload = :binary.copy("x", Keyword.get(opts, :payload, 10))
    publishers = Keyword.get(opts, :publishers, System.schedulers_online())
    samples = Keyword.get(opts, :samples, 3)

    nodes = [node() | Node.list()]
    warmup = max(div(messages, 10), 1)

    Logger.info(
      "[bench] bench started: nodes=#{length(nodes)} messages=#{messages} " <>
        "payload=#{byte_size(payload)}B publishers=#{publishers} samples=#{samples} " <>
        "reconfig=#{inspect(Keyword.take(opts, [:pool_size, :buffer_size, :batch_interval]))}"
    )

    reconfigure_all(nodes, opts)

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

    # Effective config: the PubSub opts actually in effect now (persisted by
    # reconfigure/1, so accurate across runs) plus this run's params.
    live = EchoPubSub.Bench.App.current_opts()

    config =
      [
        batch_interval: live[:batch_interval],
        pool_size: live[:pool_size],
        buffer_size: live[:buffer_size],
        publishers: publishers,
        samples: samples,
        messages: messages
      ]

    IO.puts(
      "nodes=#{length(nodes)} payload=#{byte_size(payload)} msg/s=#{msgs_per_sec} " <>
        "delivered=#{delivered?} overflow=#{overflow} cap_warn=#{cap_warn} #{inspect(config)}"
    )

    %{
      nodes: length(nodes),
      msgs_per_sec: msgs_per_sec,
      delivered: delivered?,
      overflow: overflow,
      config: config
    }
  end

  # Apply pool_size/buffer_size/batch_interval, one node at a time. Restarting
  # every node's PubSub at once races: a fresh producer's init registers with each
  # :pg member, and a peer worker torn down at the same instant is :noproc, which
  # crashes init. Serialized + a settle per node keeps peers up and lets :pg
  # leave/join propagate before the next restart. No-op when no such knob is given.
  defp reconfigure_all(nodes, opts) do
    reconfig = Keyword.take(opts, [:pool_size, :buffer_size, :batch_interval])

    if reconfig != [] do
      Logger.info("[bench] reconfiguring #{length(nodes)} nodes -> #{inspect(reconfig)}")

      Enum.each(nodes, fn node ->
        :ok = :erpc.call(node, EchoPubSub.Bench.App, :reconfigure, [reconfig])
        Process.sleep(1_500)
      end)

      Logger.info("[bench] reconfigure done across cluster")
      Process.sleep(1_000)
    end
  end
end
