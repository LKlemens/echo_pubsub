# EchoPubSub benchmarks

Two harnesses for the same end-to-end delivery benchmark (publish N messages,
wait until every node received all N, verify no loss):

- **Local** (`throughput.exs`) — a single-machine cluster of `:peer` nodes. Fast to
  run, CPU/copy bound.
- **Fly.io** (`fly/`) — real machines across regions, where delivery is
  network-latency bound.

## Run locally

Needs `MIX_ENV=test` (the harness reuses the test-only cluster helpers):

```sh
# defaults: nodes 1..4, batch 0 & 100ms, payloads 10/200 bytes
MIX_ENV=test mix run bench/throughput.exs
```

It prints a summary table and writes `bench/results.csv`.

Tune the sweep with env vars (total runs = `BATCH_INTERVALS × PAYLOAD_SIZES × NODES`,
so narrow the other dimensions when sweeping one widely):

Var               | Meaning                                   | Default
:-----------------| :---------------------------------------- | :-------------
`NODES`           | max node count to sweep to (1..N)         | `4`
`MESSAGES`        | messages per timed sample                 | `50_000`
`SAMPLES`         | timed samples, median is kept             | `3`
`BUFFER_SIZE`     | producer ring buffer size                 | `MESSAGES × 2`
`BATCH_INTERVALS` | comma list of batch intervals (ms)        | `0,100`
`PAYLOAD_SIZES`   | comma list of payload sizes (bytes)       | `10,200`
`POOL_SIZE`       | producers/workers per node                | `1`
`PUBLISHERS`      | concurrent sender processes (hash on pid) | `POOL_SIZE`

The buffer defaults to `2 × MESSAGES` so a run never overflows; setting it below
`MESSAGES` makes the harness warn that overflow is expected. `PUBLISHERS` should be
`> 1` when `POOL_SIZE > 1`, since broadcast routing hashes on the sender pid — one
sender only ever hits one producer.

```sh
# e.g. sweep 1..8 nodes, compare batch intervals, fixed 1KB payload, pool of 4
NODES=8 BATCH_INTERVALS=0,100,200 PAYLOAD_SIZES=10 POOL_SIZE=4 PUBLISHERS=4 MIX_ENV=test mix run bench/throughput.exs
```

> **Caveat:** all peer nodes share this machine's cores, so more nodes also adds
> core contention. Read the *shape* of the curve, not the absolute numbers.

To visualise `bench/results.csv`, open `bench/throughput.livemd` in
[Livebook](https://livebook.dev/) and run all cells — it renders accept-rate and
buffer-overflow charts with VegaLite.

## Run on Fly.io

Same benchmark across real machines. On one box everything is CPU/copy bound; on a
real cluster delivery is network-latency bound, which is where `concurrent_flush`
and `pool_size` earn their keep.

> Run every command below from the **repo root**. Each `fly` command takes
> `-c bench/fly/fly.toml`, which also identifies the app (via its `app =` line).
> `fly deploy` additionally needs `--dockerfile` and the root build context
> (the `Dockerfile` copies `mix.exs`, `lib/`, `rel/`).

### What's here

- `fly/bench_app.ex` — `:bench` release app: libcluster (DNSPoll over
  `<app>.internal`), the EchoPubSub PubSub (`PubSubTest`), and a subscribed
  `BenchCollector`.
- `fly/runner.ex` — `EchoPubSub.Bench.Runner.run/1`, triggered on one node.
- `fly/throughput_bench.ex`, `fly/bench_collector.ex` — shared with the local harness.
- `fly/Dockerfile`, `fly/fly.toml`, and repo-root `rel/env.sh.eex` — packaging.

Deploy-time knobs are env in `fly/fly.toml`: `POOL_SIZE`, `BUFFER_SIZE`, `BATCH_INTERVAL`.

### Deploy

```sh
fly launch --no-deploy --copy-config --name echo-pubsub-bench   # once, bootstraps fly.toml
fly secrets set -c bench/fly/fly.toml RELEASE_COOKIE="$(openssl rand -base64 24)"

# --ha=false stops Fly auto-creating a passive standby machine (see Notes)
fly deploy --ha=false -c bench/fly/fly.toml --dockerfile bench/fly/Dockerfile

fly scale count 3 -c bench/fly/fly.toml        # N *active* machines
```

### Run

```sh
fly ssh console -c bench/fly/fly.toml
/app/bin/echo_pubsub rpc 'IO.inspect(Node.list())'             # wait for a 2-element list (N-1 peers) => 3-node cluster
/app/bin/echo_pubsub rpc 'EchoPubSub.Bench.Runner.run(messages: 100_000, payload: 10)'
```

`Node.list()` excludes the node you're on; a shorter list means DNSPoll hasn't
discovered every peer yet, so wait a few seconds and retry.

Prints e.g. `nodes=3 payload=1024 msg/s=... delivered=true overflow=0`.
Change node count with `fly scale count N -c bench/fly/fly.toml`; batch/pool via
`fly secrets`/`fly.toml` env then redeploy.

### Stop (avoid cost)

```sh
fly scale count 0 -c bench/fly/fly.toml
```

### Notes

- Dist runs over IPv6 6PN (`ERL_AFLAGS=-proto_dist inet6_tcp` in `rel/env.sh.eex`).
- All machines must share `RELEASE_COOKIE`.
- If `Node.list()` is empty, give DNSPoll a few seconds or bump `polling_interval`.
- Deploy with `--ha=false`. By default Fly adds a passive **standby** machine
  (shown as `app†`, state `stopped`) that only boots on host failure — it counts
  toward `scale count` but never clusters, so you silently run one node short.
  There's no `fly.toml` key for this; it's a deploy-time flag. To fix an existing
  one: `fly machine destroy <standby-id> --force` then redeploy with `--ha=false`.
