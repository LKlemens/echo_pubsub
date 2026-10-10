defmodule EchoPubSub.RetryBench do
  @moduledoc """
  One failing node, growing retry batch. Three nodes: this one (A) publishes, B is
  healthy, C fails. While C fails the producer keeps C's unacked messages and
  re-sends all of them on every retry, so C's batch grows step by step. Each step
  reports the batch size, the cost of one attempt, peak memory on A and C, C's
  mailbox and the latency of a probe on the healthy B; a final row reports C's
  recovery.

  Failure modes:

    * `:reject` - C's worker answers `:error` at once (fault injection).
    * `:hung` - C's worker is suspended, so every attempt waits out the
      `call_timeout` and stays queued in C's mailbox.

  Runs on A, used by `bench/retry.exs` locally and via `rpc` on Fly:

      EchoPubSub.RetryBench.run(mode: :reject, payloads: [200])
  """

  alias EchoPubSub.BenchCollector

  @pubsub PubSubTest
  @adapter PubSubTest.Adapter
  @topic "bench"
  @retry_interval 200

  @type mode :: :reject | :hung
  @type row :: %{atom() => term()}

  @doc """
  Opts: `:failing` (C, defaults to the last of `Node.list/0`), `:healthy` (B, the
  first other node), `:mode` (`:reject`), `:steps` (6k..10k), `:payloads` (`[200, 700]`
  bytes per message), `:call_timeout` (5000, only used to pace `:hung` steps).
  Returns one list of rows per payload.
  """
  @spec run(keyword()) :: [row()]
  def run(opts \\ []) do
    others = Node.list() |> Enum.sort()
    failing = Keyword.get(opts, :failing, List.last(others))
    healthy = Keyword.get(opts, :healthy, hd(others -- [failing]))
    mode = Keyword.get(opts, :mode, :reject)
    steps = Keyword.get(opts, :steps, [6_000, 7_000, 8_000, 9_000, 10_000])
    payloads = Keyword.get(opts, :payloads, [200, 700])
    call_timeout = Keyword.get(opts, :call_timeout, 5_000)

    ctx = %{
      failing: failing,
      healthy: healthy,
      mode: mode,
      group: group(),
      # Reject attempts are fast; a hung attempt holds the in-flight slot for the
      # whole call_timeout, so wait for it to time out and retry once.
      settle_ms:
        if(mode == :hung, do: call_timeout + 2 * @retry_interval, else: 2 * @retry_interval)
    }

    IO.puts("""

    ## mode=#{mode} A=#{node()} B=#{healthy} C=#{failing}
    """)

    Enum.flat_map(payloads, &run_payload(&1, steps, ctx))
  end

  defp run_payload(payload_bytes, steps, ctx) do
    template = message_template(payload_bytes)
    nodes = [node(), ctx.healthy, ctx.failing]
    Enum.each(nodes, &:erpc.call(&1, BenchCollector, :ensure_started, [@topic]))
    Enum.each(nodes, &:erpc.call(&1, __MODULE__, :start_sampler, []))

    baseline = Enum.map([node(), ctx.failing], &:erpc.call(&1, :erlang, :memory, [:total]))

    IO.puts(
      "baseline before failure: A #{mb(hd(baseline))} MB, C #{mb(List.last(baseline))} MB\n"
    )

    IO.puts("""
    ### payload=#{payload_bytes}B (one message = #{:erlang.external_size({:m, 0, template})} B on the wire)

    backlog | batch_mb | attempt_ms | A_peak_mb | A_binary_mb | C_peak_mb | C_mailbox | B_lag_ms
    ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:\
    """)

    fail(ctx)

    {rows, published} =
      Enum.map_reduce(steps, 0, fn step, published ->
        Enum.each(nodes, &:erpc.call(&1, __MODULE__, :reset_peak, []))
        publish(published + 1, step, template)
        Process.sleep(ctx.settle_ms)
        {row, published} = measure_step(step, payload_bytes, ctx)
        print_step(row)
        {row, published}
      end)

    recovery = recover(published, ctx)
    print_recovery(recovery)

    Enum.each(nodes, &:erpc.call(&1, __MODULE__, :stop_sampler, []))
    rows ++ [recovery]
  end

  defp measure_step(step, payload_bytes, ctx) do
    batch = c_backlog(ctx)
    batch_bytes = :erlang.external_size(batch)

    # A reject attempt is one real round trip: serialize, send, C rejects. A hung
    # attempt always lasts the full call_timeout, so it is not re-measured (that
    # would queue one more copy on C).
    attempt_ms =
      case ctx.mode do
        :reject -> time_ms(fn -> GenServer.call(c_worker(ctx), batch, :infinity) end)
        :hung -> nil
      end

    # Probe on B: everything published so far has arrived, then one more message.
    published = step + 1
    :erpc.call(ctx.healthy, BenchCollector, :await_count, [step], :infinity)
    b_lag_ms = time_ms(fn -> probe(published, ctx) end)

    {a_peak, a_binary} = peak()
    {c_peak, _} = :erpc.call(ctx.failing, __MODULE__, :peak, [])

    row = %{
      payload_b: payload_bytes,
      backlog: length(batch),
      batch_mb: mb(batch_bytes),
      attempt_ms: attempt_ms,
      a_peak_mb: mb(a_peak),
      a_binary_mb: mb(a_binary),
      c_peak_mb: mb(c_peak),
      c_mailbox: :erpc.call(ctx.failing, __MODULE__, :worker_queue_len, [ctx.group]),
      b_lag_ms: b_lag_ms
    }

    {row, published}
  end

  defp probe(seq, ctx) do
    Phoenix.PubSub.broadcast!(@pubsub, @topic, {:m, seq, :probe})
    :erpc.call(ctx.healthy, BenchCollector, :await_count, [seq], :infinity)
  end

  # Recover C, time until it holds every published message, then wait for the
  # count to settle: anything above `published` is a duplicate delivery.
  defp recover(published, ctx) do
    :erpc.call(ctx.failing, __MODULE__, :reset_peak, [])

    recover_ms =
      time_ms(fn ->
        heal(ctx)
        :erpc.call(ctx.failing, BenchCollector, :await_count, [published], :infinity)
      end)

    received = settled_count(ctx.failing)
    {c_peak, _} = :erpc.call(ctx.failing, __MODULE__, :peak, [])

    %{
      recover_ms: recover_ms,
      expected: published,
      c_received: received,
      duplicates: received - published,
      c_peak_mb: mb(c_peak)
    }
  end

  defp settled_count(node, last \\ -1) do
    count = :erpc.call(node, BenchCollector, :count, [])

    if count == last do
      count
    else
      Process.sleep(1_000)
      settled_count(node, count)
    end
  end

  defp fail(%{mode: :reject} = ctx),
    do: :erpc.call(ctx.failing, Application, :put_env, [:echo_pubsub, :fault_injection, :error])

  defp fail(%{mode: :hung} = ctx), do: :erpc.call(ctx.failing, :sys, :suspend, [c_worker(ctx)])

  defp heal(%{mode: :reject} = ctx),
    do: :erpc.call(ctx.failing, Application, :put_env, [:echo_pubsub, :fault_injection, :ok])

  defp heal(%{mode: :hung} = ctx), do: :erpc.call(ctx.failing, :sys, :resume, [c_worker(ctx)])

  defp publish(from, to, template) do
    Enum.each(from..to//1, fn seq ->
      # A fresh note per message, as real events would carry their own data;
      # sharing the template's binary would hide it from A's memory.
      message = %{template | id: seq, note: :binary.copy(template.note)}
      Phoenix.PubSub.broadcast!(@pubsub, @topic, {:m, seq, message})
    end)
  end

  # The exact messages the producer would send C next: everything from C's read
  # cursor up to the write cursor.
  defp c_backlog(ctx) do
    state = :sys.get_state(Module.concat(ctx.group, Producer))
    cursor = Map.get(state.read_cursors, ctx.failing, 0)
    size = :array.size(state.buffer)

    Enum.map(cursor..(state.write_cursor - 1)//1, &:array.get(rem(&1, size), state.buffer))
  end

  defp c_worker(ctx) do
    {Module.concat(ctx.group, Worker), ctx.failing}
  end

  # broadcast!/3 from this process routes to the group picked by its pid hash.
  defp group do
    groups = :persistent_term.get(@adapter)
    elem(groups, :erlang.phash2(self(), tuple_size(groups)))
  end

  # A scoreboard-like event padded with a note so one message is `bytes` on the wire.
  defp message_template(bytes) do
    base = %{
      id: 0,
      match: "fra-mun",
      player: "Kowalski",
      min: 67,
      event: :goal,
      score: {2, 1},
      note: ""
    }

    pad = bytes - :erlang.external_size({:m, 0, base})
    %{base | note: :binary.copy("x", max(pad, 0))}
  end

  @doc false
  @spec worker_queue_len(atom()) :: non_neg_integer()
  def worker_queue_len(group) do
    {:message_queue_len, len} =
      Process.info(Process.whereis(Module.concat(group, Worker)), :message_queue_len)

    len
  end

  # Memory sampler: polls every 10 ms and keeps the peaks, so a spike while a big
  # batch is serialized or held is not missed between two reads.
  @sampler __MODULE__.Sampler

  @doc false
  @spec start_sampler() :: :ok
  def start_sampler do
    stop_sampler()
    pid = spawn(fn -> sample({0, 0}) end)
    Process.register(pid, @sampler)
    :ok
  end

  @doc false
  @spec stop_sampler() :: :ok
  def stop_sampler do
    if pid = Process.whereis(@sampler), do: Process.exit(pid, :kill)
    :ok
  end

  @doc false
  @spec reset_peak() :: :ok
  def reset_peak, do: send(@sampler, :reset) && :ok

  @doc "Peak `{total, binary}` bytes since the last reset."
  @spec peak() :: {non_neg_integer(), non_neg_integer()}
  def peak do
    send(@sampler, {:peak, self()})

    receive do
      {:peak, peak} -> peak
    end
  end

  defp sample({total, binary} = peak) do
    receive do
      :reset -> sample({0, 0})
      {:peak, from} -> send(from, {:peak, peak}) && sample(peak)
    after
      10 ->
        sample({max(total, :erlang.memory(:total)), max(binary, :erlang.memory(:binary))})
    end
  end

  defp print_step(row) do
    IO.puts(
      Enum.map_join(
        [
          row.backlog,
          row.batch_mb,
          row.attempt_ms || "timeout",
          row.a_peak_mb,
          row.a_binary_mb,
          row.c_peak_mb,
          row.c_mailbox,
          row.b_lag_ms
        ],
        " | ",
        &to_string/1
      )
    )
  end

  defp print_recovery(r) do
    IO.puts("""

    recovery: #{r.recover_ms} ms until C held all #{r.expected}; C received #{r.c_received} \
    (#{r.duplicates} duplicates), C peak #{r.c_peak_mb} MB
    """)
  end

  defp time_ms(fun) do
    {us, _} = :timer.tc(fun)
    Float.round(us / 1000, 1)
  end

  defp mb(bytes), do: Float.round(bytes / 1_048_576, 1)
end
