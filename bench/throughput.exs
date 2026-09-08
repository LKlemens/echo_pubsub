# End-to-end delivery throughput benchmark for EchoPubSub. Subscribes a collector
# on every node, publishes N messages + a sentinel, times until all collectors have
# them all, and verifies no loss (delivered_ok). Swept over nodes, batch_interval,
# and payload size.
#
#   MIX_ENV=test mix run bench/throughput.exs   # needs :test for the test helpers
#
# Env vars (defaults in parens):
#   NODES           max node count to sweep to    (4)
#   MESSAGES        messages per timed sample      (50_000)
#   SAMPLES         timed samples, median is kept  (3)
#   BUFFER_SIZE     producer ring buffer size      (MESSAGES * 2; below MESSAGES warns)
#   BATCH_INTERVALS comma list of batch_intervals  (0,100)
#   PAYLOAD_SIZES   comma list of payload bytes    (10,200)
#   POOL_SIZE       producers/workers per node     (1)
#   PUBLISHERS      concurrent sender processes    (POOL_SIZE; routing hashes on pid)
#
# Runs = len(BATCH_INTERVALS) * len(PAYLOAD_SIZES) * NODES.
# CAVEAT: peers share this machine's cores, so read the curve shape, not absolutes.

alias EchoPubSub.BenchCollector
alias EchoPubSub.Cluster
alias EchoPubSub.ThroughputBench

env_int = fn name, default -> System.get_env(name, default) |> String.to_integer() end

nodes_max = env_int.("NODES", "4")
messages = env_int.("MESSAGES", "50000")
samples = env_int.("SAMPLES", "3")
buffer_size = env_int.("BUFFER_SIZE", Integer.to_string(messages * 2))
pool_size = env_int.("POOL_SIZE", "1")
# Concurrent publisher processes; routing hashes on pid, so a pool needs >1 sender.
publishers = env_int.("PUBLISHERS", Integer.to_string(pool_size))

batch_intervals =
  System.get_env("BATCH_INTERVALS", "0,100")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer(String.trim(&1)))

payload_sizes =
  System.get_env("PAYLOAD_SIZES", "10,200")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer(String.trim(&1)))

if buffer_size < messages do
  IO.puts("WARNING: buffer_size (#{buffer_size}) < messages (#{messages}); overflow expected.")
end

topic = "bench"
warmup = max(div(messages, 10), 1)
call_timeout = :infinity

median = fn values ->
  sorted = Enum.sort(values)
  Enum.at(sorted, div(length(sorted), 2))
end

IO.puts("""
EchoPubSub end-to-end throughput benchmark
  schedulers_online = #{System.schedulers_online()}
  nodes 1..#{nodes_max}, messages/sample = #{messages}, samples = #{samples}
  buffer_size = #{buffer_size}, batch_intervals = #{inspect(batch_intervals, charlists: :as_lists)}
  payload_sizes = #{inspect(payload_sizes, charlists: :as_lists)}
  pool_size = #{pool_size}, publishers = #{publishers}
""")

IO.puts(
  String.pad_trailing("batch_ms", 10) <>
    String.pad_trailing("payload_b", 11) <>
    String.pad_trailing("nodes", 7) <>
    String.pad_trailing("msg/s", 13) <>
    String.pad_trailing("delivered", 11) <>
    String.pad_trailing("overflow", 10) <> "cap_warn"
)

# One (batch_interval, payload_size, node_count) measurement. Returns a result map.
run_case = fn batch_interval, payload_size, k ->
  # Unique per iteration: :peer.stop returns before epmd releases the name, so
  # reused names across configs fail with "name ... seems to be in use".
  run_id = System.unique_integer([:positive])
  names = for i <- 1..k, do: "n#{run_id}_#{i}"
  nodes =
    Cluster.spawn_nodes(names,
      batch_interval: batch_interval,
      buffer_size: buffer_size,
      pool_size: pool_size
    )

  publisher = hd(nodes)

  # Built once, outside the timed loop, so we measure send cost not construction.
  payload = :binary.copy("x", payload_size)

  subscriber_nodes = Enum.map(nodes, & &1.node)
  Enum.each(nodes, &Cluster.apply(&1, BenchCollector, :ensure_started, [topic]))
  Process.sleep(300) # let :pg membership propagate before relying on fan-out

  # Warmup (discarded) settles the VM and peer discovery.
  :peer.call(publisher.pid, ThroughputBench, :run_e2e, [warmup, topic, payload, subscriber_nodes, publishers], call_timeout)
  :peer.call(publisher.pid, ThroughputBench, :install_counters, [], call_timeout)

  results =
    for _ <- 1..samples do
      Enum.each(nodes, &Cluster.apply(&1, BenchCollector, :reset, []))

      :peer.call(
        publisher.pid,
        ThroughputBench,
        :run_e2e,
        [messages, topic, payload, subscriber_nodes, publishers],
        call_timeout
      )
    end

  ok = Enum.all?(results, fn {_us, counts} -> Enum.all?(counts, &(&1 == messages)) end)
  median_us = median.(Enum.map(results, fn {us, _counts} -> us end))
  delivered_ok = if(ok, do: "1", else: "0")

  {overflow, cap_warn} =
    :peer.call(publisher.pid, ThroughputBench, :read_counters, [], call_timeout)

  Enum.each(nodes, fn n -> :peer.stop(n.pid) end)
  Process.sleep(200)

  msgs_per_sec = messages * 1_000_000 / median_us

  IO.puts(
    String.pad_trailing(Integer.to_string(batch_interval), 10) <>
      String.pad_trailing(Integer.to_string(payload_size), 11) <>
      String.pad_trailing(Integer.to_string(k), 7) <>
      String.pad_trailing(:erlang.float_to_binary(msgs_per_sec, decimals: 0), 13) <>
      String.pad_trailing(delivered_ok, 11) <>
      String.pad_trailing(Integer.to_string(overflow), 10) <>
      Integer.to_string(cap_warn)
  )

  %{
    batch_interval: batch_interval,
    payload_bytes: payload_size,
    nodes: k,
    msgs_per_sec: msgs_per_sec,
    delivered_ok: delivered_ok,
    overflow_count: overflow,
    capacity_warnings: cap_warn
  }
end

results =
  for batch_interval <- batch_intervals, payload_size <- payload_sizes, k <- 1..nodes_max do
    run_case.(batch_interval, payload_size, k)
  end

File.mkdir_p!("bench")

header =
  "pool_size,publishers,batch_interval,payload_bytes,nodes,msgs_per_sec,delivered_ok," <>
    "overflow_count,capacity_warnings,messages,buffer_size,samples"

rows =
  Enum.map(results, fn r ->
    Enum.join(
      [
        pool_size,
        publishers,
        r.batch_interval,
        r.payload_bytes,
        r.nodes,
        :erlang.float_to_binary(r.msgs_per_sec, decimals: 2),
        r.delivered_ok,
        r.overflow_count,
        r.capacity_warnings,
        messages,
        buffer_size,
        samples
      ],
      ","
    )
  end)

comment =
  "# schedulers_online=#{System.schedulers_online()} " <>
    "single-machine peers share cores; curve shape is meaningful, absolute numbers are not"

File.write!("bench/results.csv", Enum.join([comment, header | rows], "\n") <> "\n")

IO.puts("\nWrote bench/results.csv (#{length(results)} rows)")
