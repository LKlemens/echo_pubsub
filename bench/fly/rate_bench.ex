defmodule EchoPubSub.RateBench do
  @moduledoc """
  Steady-rate benchmark: publishes at a fixed rate for a fixed time and reports
  what each `batch_interval` costs. Unlike the throughput bench, which floods the
  producer, this shows moderate traffic, where `batch_interval: 0` may send one
  call per message and a timer adds its delay to every message.

  Per row: flushes per second on the publisher, CPU on the publisher and on one
  receiver, and per-message latency (send to subscriber) as p50, p99 and max.
  Latency on remote nodes compares two clocks, so the publisher's own local
  latency is reported as a baseline.

  On Fly, from one machine of the cluster:

      EchoPubSub.RateBench.run()
      EchoPubSub.RateBench.run(rates: [1_000], batch_intervals: [0, 20, 100])
  """

  alias EchoPubSub.RateBench.Collector

  @pubsub PubSubTest
  @topic "rate"
  @counter_key {__MODULE__, :flushes}

  @type row :: %{atom() => term()}

  @doc """
  Runs every rate under every batch interval on the whole cluster, switching the
  interval on every node with `EchoPubSub.Bench.Runner.reconfigure_all/2`.

  Opts: `:rates` (`[100, 1_000, 10_000]` msg/s), `:duration` (10_000 ms per row),
  `:payload` (30 bytes), `:batch_intervals` (`[0, 100]`).
  """
  @spec run(keyword()) :: [row()]
  def run(opts \\ []) do
    nodes = [node() | Enum.sort(Node.list())]
    intervals = Keyword.get(opts, :batch_intervals, [0, 100])

    IO.puts(header(length(nodes)))

    for interval <- intervals,
        rate <- Keyword.get(opts, :rates, [100, 1_000, 10_000]) do
      EchoPubSub.Bench.Runner.reconfigure_all(nodes, batch_interval: interval)
      row = measure(nodes, rate, opts)
      row = Map.put(row, :batch_ms, interval)
      IO.puts(format(row))
      row
    end
  end

  @doc """
  Measures one row on `nodes` (the publisher first) with whatever batch interval
  is in effect. Opts as in `run/1`.
  """
  @spec measure([node()], pos_integer(), keyword()) :: row()
  def measure([publisher | receivers] = nodes, rate, opts \\ []) do
    publisher == node() || raise ArgumentError, "run on the publishing node"

    duration = Keyword.get(opts, :duration, 10_000)
    payload = :binary.copy("x", Keyword.get(opts, :payload, 30))
    total = div(rate * duration, 1_000)
    cpu_node = List.first(receivers, publisher)

    Enum.each(nodes, &:erpc.call(&1, Collector, :restart, [@pubsub, @topic]))
    install_flush_counter()

    cpu_before = sample_cpu([publisher, cpu_node])
    started = System.monotonic_time(:millisecond)
    publish_at(rate, total, payload, started)
    window_ms = System.monotonic_time(:millisecond) - started
    cpu_after = sample_cpu([publisher, cpu_node])
    flushes = :counters.get(:persistent_term.get(@counter_key), 1)

    # Latencies are only complete once every node has every message.
    stats = Enum.map(nodes, &:erpc.call(&1, Collector, :await, [total, 10_000], 15_000))
    [local | remote] = stats
    remote_latencies = remote |> Enum.flat_map(& &1.latencies) |> Enum.sort()
    [cpu_publisher, cpu_receiver] = Enum.zip_with(cpu_before, cpu_after, &utilization/2)

    %{
      rate: rate,
      flushes_per_s: round(flushes * 1_000 / window_ms),
      publisher_cpu: cpu_publisher,
      receiver_cpu: cpu_receiver,
      local_p50: percentile(Enum.sort(local.latencies), 50),
      remote_p50: percentile(remote_latencies, 50),
      remote_p99: percentile(remote_latencies, 99),
      remote_max: List.last(remote_latencies),
      delivered: Enum.all?(stats, &(&1.count == total))
    }
  end

  # Sends the messages due by now every millisecond, so the rate holds even when
  # one sleep overshoots.
  defp publish_at(rate, total, payload, started, sent \\ 0)
  defp publish_at(_rate, total, _payload, _started, total), do: :ok

  defp publish_at(rate, total, payload, started, sent) do
    elapsed = System.monotonic_time(:millisecond) - started
    due = min(total, div(rate * elapsed, 1_000))

    for seq <- (sent + 1)..due//1 do
      Phoenix.PubSub.broadcast!(@pubsub, @topic, {:rate, seq, System.os_time(:microsecond), payload})
    end

    if due < total, do: Process.sleep(1)
    publish_at(rate, total, payload, started, max(sent, due))
  end

  defp install_flush_counter do
    ref = :counters.new(1, [:write_concurrency])
    :persistent_term.put(@counter_key, ref)
    handler = "rate-bench-flushes"
    _ = :telemetry.detach(handler)
    :telemetry.attach(handler, [:echo_pubsub, :buffer, :flush], &__MODULE__.count_flush/4, ref)
  end

  @doc false
  def count_flush(_event, _measurements, _meta, ref), do: :counters.add(ref, 1, 1)

  defp sample_cpu(nodes), do: Enum.map(nodes, &:erpc.call(&1, :scheduler, :sample_all, []))

  defp utilization(before, later) do
    {:total, share, _text} = List.keyfind(:scheduler.utilization(before, later), :total, 0)
    Float.round(share * 100, 1)
  end

  defp percentile([], _p), do: nil

  defp percentile(sorted, p) do
    Enum.at(sorted, min(length(sorted) - 1, div(length(sorted) * p, 100)))
  end

  defp header(nodes) do
    """

    ## steady rate, nodes=#{nodes} (latency in ms; remote = other nodes' clocks)

    batch_ms | rate | flushes/s | publisher_cpu% | receiver_cpu% | local_p50 | remote_p50 | remote_p99 | remote_max | delivered
    ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--\
    """
  end

  defp format(row) do
    Enum.map_join(
      [
        row.batch_ms,
        row.rate,
        row.flushes_per_s,
        row.publisher_cpu,
        row.receiver_cpu,
        ms(row.local_p50),
        ms(row.remote_p50),
        ms(row.remote_p99),
        ms(row.remote_max),
        row.delivered
      ],
      " | ",
      &to_string/1
    )
  end

  defp ms(nil), do: "-"
  defp ms(us), do: :erlang.float_to_binary(us / 1_000, decimals: 1)
end

defmodule EchoPubSub.RateBench.Collector do
  @moduledoc """
  Records, on one node, how long each `{:rate, seq, sent_us, payload}` message took
  from `broadcast!` on the publisher to this subscriber.
  """
  use GenServer

  @name __MODULE__

  @doc """
  Starts a fresh collector subscribed to `topic`, replacing any old one: a
  reconfigure restarts the PubSub, which drops the old subscription.
  """
  @spec restart(atom(), String.t()) :: :ok
  def restart(pubsub, topic) do
    if pid = Process.whereis(@name), do: GenServer.stop(pid)
    {:ok, _pid} = GenServer.start(__MODULE__, {pubsub, topic}, name: @name)
    :ok
  end

  @doc "Waits up to `timeout` ms for `total` messages, then returns the count and latencies (µs)."
  @spec await(non_neg_integer(), timeout()) :: %{count: non_neg_integer(), latencies: [integer()]}
  def await(total, timeout), do: GenServer.call(@name, {:await, total, timeout}, timeout + 1_000)

  @impl GenServer
  def init({pubsub, topic}) do
    # Scheduler wall time stays on only while the process that enabled it lives;
    # the bench samples it from short-lived erpc processes, so the collector holds it.
    _ = :erlang.system_flag(:scheduler_wall_time, true)
    :ok = Phoenix.PubSub.subscribe(pubsub, topic)
    {:ok, %{count: 0, latencies: [], waiting: nil}}
  end

  @impl GenServer
  def handle_call({:await, total, _timeout}, _from, %{count: count} = state) when count >= total do
    {:reply, result(state), state}
  end

  def handle_call({:await, total, timeout}, from, state) do
    Process.send_after(self(), :give_up, timeout)
    {:noreply, %{state | waiting: {from, total}}}
  end

  @impl GenServer
  def handle_info({:rate, _seq, sent_us, _payload}, state) do
    latency = System.os_time(:microsecond) - sent_us
    state = %{state | count: state.count + 1, latencies: [latency | state.latencies]}

    case state.waiting do
      {from, total} when state.count >= total ->
        GenServer.reply(from, result(state))
        {:noreply, %{state | waiting: nil}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(:give_up, %{waiting: {from, _total}} = state) do
    GenServer.reply(from, result(state))
    {:noreply, %{state | waiting: nil}}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp result(state), do: %{count: state.count, latencies: state.latencies}
end
