# EchoPubSub

[![CI](https://github.com/LKlemens/echo_pubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/LKlemens/echo_pubsub/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/LKlemens/echo_pubsub/badge.svg?branch=main)](https://coveralls.io/github/LKlemens/echo_pubsub?branch=main)
[![hex.pm version](https://img.shields.io/hexpm/v/echo_pubsub.svg)](https://hex.pm/packages/echo_pubsub)
[![hex.pm license](https://img.shields.io/hexpm/l/echo_pubsub.svg)](https://github.com/LKlemens/echo_pubsub/blob/main/LICENSE)

A Phoenix.PubSub adapter that distributes messages between nodes using the erlang `:pg` module, like the default adapter, however with the additional guarentees of "at least once" delivery.

This means that a transient network failure - even one as short as ~1ms, with the nodes still connected to the cluster - does not silently lose messages. Anything a node did not acknowledge stays buffered and is replayed to it, in order, as soon as delivery succeeds again, thanks to a buffer of messages and read cursors.

New to the idea? [At-least-once PubSub](https://klemens.blog/blog/at-least-once-pubsub/)
is a gentle introduction to why the default adapter can drop messages and how
EchoPubSub prevents it.

See the [Docs](https://echo-pubsub.hexdocs.pm/EchoPubSub.html) for more information.

## Demo

[![Scoreboard demo](https://github.com/LKlemens/score_board/releases/download/media-v1/demo.gif)](https://scoreboard-pool.fly.dev/)

[Live demo](https://scoreboard-pool.fly.dev/): a three-node BEAM cluster where
you can take a node offline mid-game and watch the events it missed replay in
order when it comes back.

## How it works

`Phoenix.PubSub.PG2` is fire-and-forget: a broadcast is sent once, with no
acknowledgement and no buffer. A network failure as short as ~1ms silently drops
every message sent while delivery to a node is failing, even though that node
never left the cluster.

EchoPubSub makes delivery **at-least-once**:

- **Buffer + cursors** - each broadcaster keeps a ring buffer of recent messages,
  plus a per-node read cursor that advances only on an acked delivery.

- **Replay once delivery succeeds again** - a node that missed messages is
  replayed exactly those messages, in order.

- **Told if it fell behind** - if it stayed unreachable long enough that those
  messages were overwritten in the bounded buffer, it gets `{:cursor_expired, node_name}`
  (see [Usage](#usage)) telling it to reload from a source of truth.

**Core guarantee: either you receive every message in order, or you are told you
fell behind** - never a silent gap.

See [how it works](https://echo-pubsub.hexdocs.pm/how-it-works.html) for diagrams,
the cursor internals, and failure scenarios.

## When to use it

EchoPubSub gives you at-least-once cross-node delivery **without standing up a
dedicated message broker**. If you already run a BEAM cluster, you reuse it - no
extra service to deploy, secure, monitor, or scale.

A good fit for small-to-mid projects that need reliable cross-node messaging but
don't want the operational burden of Kafka / RabbitMQ / NATS:

- **Replicated in-memory caches** - a missed invalidation means a node serves
  stale data forever. EchoPubSub replays it once the network recovers, or sends
  `{:cursor_expired, node}` to trigger a reload - never a silent stale node.
- **Event logs / projections / derived state** kept in sync across nodes.
- **Presence / state fan-out** where a dropped update corrupts a peer's view.
- Anything currently on plain `Phoenix.PubSub` that quietly breaks during network
  blips.

**When to reach for a real broker instead:** durable persistence across a full
cluster restart, replay from disk / long retention, cross-language consumers,
huge backlogs, or delivery to non-BEAM systems. EchoPubSub's buffer is in-memory
and bounded - it closes the network-blip gap, it is not a durable log.

## Caveats

**At-least-once means possibly-more-than-once.** If a message is delivered and
handled but its `ack` is lost, the cursor doesn't advance and the message is re-sent
- so a handler can see the same message twice. Make handlers tolerate duplicates:
send absolute state rather than deltas, or dedupe by a per-message id. See
[handling duplicate deliveries](https://echo-pubsub.hexdocs.pm/how-it-works.html#handling-duplicate-deliveries)
for worked examples.

## Usage

*Note: I used LLM for typing - but ideas and decisions were mine*


```elixir
def deps do
  [
    {:echo_pubsub, "~> 0.2.4"}
  ]
end
```

> **Not a drop-in replacement for Phoenix.PubSub.** At-least-once delivery costs
> more than fire-and-forget (buffering, acked cross-node calls). Keep the default
> PubSub for ordinary broadcasts and run EchoPubSub *alongside* it, using it only
> for cross-node data that must not be lost (replicated caches, event logs,
> derived state).


Add it to your supervision tree. Running EchoPubSub *alongside* your existing
default PubSub, both children default to the same child id (that of the
`Phoenix.PubSub` supervisor), so give each a distinct `id:`:

```elixir
# application.ex
children = [
  Supervisor.child_spec({Phoenix.PubSub, name: MyApp.PubSub}, id: MyApp.PubSub),
  Supervisor.child_spec(
    {Phoenix.PubSub, name: MyApp.EchoPubSub, adapter: EchoPubSub},
    id: MyApp.EchoPubSub
  )
]
```

Config Options

Option                  | Description                                                               | Default        |
:-----------------------| :------------------------------------------------------------------------ | :------------- |
`:name`                 | The required name to register the PubSub processes, ie: `MyApp.PubSub`    |                |
`:pool_size`            | The number of workers and producers on each node                          | 1              |
`:buffer_size`          | The numbers of messages to hold in memory for each producer in the pool   | 10_000         |
`:batch_interval`       | Milliseconds to batch writes before a flush; `0` flushes immediately      | 200            |

With `:pool_size > 1` there are independent producers and order/at-least-once is
*per producer* (a broadcast routes by sender pid) - see
[Pools and ordering](https://echo-pubsub.hexdocs.pm/how-it-works.html#pools-and-ordering).

Subscribing processes should handle the message `{:cursor_expired, node_name}` which indicates that your client
has been unreachable long enough that your position in the broadcaster's buffer has been overwritten. At this point it is the subscribing process's job to return to a valid state i.e. reloading state from source like database or another node.
The same message also arrives when a peer registers your node after flushing writes your node will not receive, for example right after startup.

## Benchmarks

Fly.io cluster (fra, `performance-4x`), `pool_size=1`, no message lost.

**Throughput under full load** (4 publishers, 10 B payload):

| nodes | `batch_interval=100` | `batch_interval=0` |
|------:|---------------------:|-------------------:|
| 3     | ~127k msg/s          | ~96k msg/s         |
| 4     | ~124k msg/s          | ~92k msg/s         |

**Steady rate** (3 nodes, 1 publisher, 30 B payload):

| `batch_interval` | msg/s        | remote p50 | remote p99 | calls/s per node | publisher CPU |
|-----------------:|-------------:|-----------:|-----------:|-----------------:|--------------:|
|                0 |        1,000 |     1.7 ms |     4.0 ms |              687 |          2.4% |
|                0 |       10,000 |     2.1 ms |     4.7 ms |              677 |          4.3% |
|                0 | 65,911 (max) |     1.3 ms |     7.7 ms |              953 |         17.3% |
|              100 |        1,000 |    53.1 ms |   103.1 ms |               10 |          0.3% |
|              100 |       10,000 |    56.7 ms |   105.9 ms |               10 |          1.3% |
|              100 | 99,820 (max) |    94.7 ms |   156.1 ms |                9 |         12.0% |

- **Latency vs cost:** `0` delivers ~25x faster; `100` sends far fewer, bigger
  calls and uses less CPU (share of 4 vCPUs above).
- **`0` still batches:** one batch per node is in flight and writes that land
  meanwhile go out in the next one, so calls level off at ~700-1,000 per node per
  second and batches grow with the rate instead.
- **Throughput:** `0` is lower at small payloads and with one publisher, which
  tops out at ~66k msg/s; with 4 publishers at 30-200 B it is level or ahead.
- **Payload size:** 200 B costs ~20-55% throughput against 10-30 B, so send small
  deltas, not whole objects.
- [How to run](https://github.com/LKlemens/echo_pubsub/blob/benchmark/bench/README.md),
  detailed results: [throughput](https://github.com/LKlemens/echo_pubsub/blob/benchmark/bench/fly/results-fra-batching.md),
  [steady rate](https://github.com/LKlemens/echo_pubsub/blob/benchmark/bench/fly/results-fra-rate.md).

## Credits

EchoPubSub is a fork of [`phoenix_pubsub_buffered`](https://github.com/enewbury/phoenix_pubsub_buffered)
by [Eric Newbury](https://github.com/enewbury), who designed and built the
original at-least-once buffered PubSub adapter. All credit for the core design
goes to him - this fork builds on that foundation with batched inter-node
delivery, automatic replay on failure, flush-path expiry detection, telemetry,
capacity warnings, and additional configuration options.
