# EchoPubSub

[![CI](https://github.com/LKlemens/echo_pubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/LKlemens/echo_pubsub/actions/workflows/ci.yml)
[![hex.pm version](https://img.shields.io/hexpm/v/echo_pubsub.svg)](https://hex.pm/packages/echo_pubsub)
[![hex.pm license](https://img.shields.io/hexpm/l/echo_pubsub.svg)](https://github.com/LKlemens/echo_pubsub/blob/main/LICENSE)

A Phoenix.PubSub adapter that distributes messages between nodes using the erlang `:pg` module, like the default adapter, however with the additional guarentees of "at least once" delivery. 

This means that nodes can disconnect temporarily from the cluster - even for a blip as short as ~1ms - and then "catch up" when they rejoin, thanks to a buffer of messages and read cursors.

See the [Docs](https://hexdocs.pm/echo_pubsub/EchoPubSub.html) for more information.

## The Problem: messages lost during temporary network problems

The default `Phoenix.PubSub.PG2` adapter is **fire-and-forget**. When a node
broadcasts, the message is delivered to the nodes that are connected *at that
moment*. There is no buffer and no acknowledgement - if a node is unreachable
when the broadcast happens, the message is simply gone for that node.

Even a momentary network problem - a blip lasting as little as 1ms - means
silent data loss for any message broadcast during it:

```
        Node A (broadcaster)            Node B (disconnected)
        ────────────────────            ────────────────────
  t0    broadcast msg 1   ───────────▶  received msg 1
  t1    ┌─ network blip (e.g. ~1ms): B drops out ─┐
  t2    broadcast msg 2   ──────✗               (never arrives)
  t3    broadcast msg 3   ──────✗               (never arrives)
  t4    └─ B reconnects ────────────────────────┘
  t5    broadcast msg 4   ───────────▶  received msg 4
```

When B comes back at `t4` it carries on as if nothing happened: it has
**no idea** that msg 2 and msg 3 ever existed. There is no error, no gap
detection - just a hole in the stream. For anything that relies on the message
stream being complete (replicated caches, event logs, derived state), this
quietly corrupts B's view of the world.

## How EchoPubSub solves it

EchoPubSub upgrades delivery from fire-and-forget to **at-least-once** by having
each broadcasting node remember what it has sent and to whom:

- **Ring buffer of recent messages** - every producer keeps the last
  `:buffer_size` messages in memory, each stamped with a monotonically
  increasing *write cursor*.
- **Per-node read cursors** - the producer tracks how far each remote node has
  acknowledged. Remote delivery is a synchronous, acked call; a node's cursor
  only advances once it confirms receipt.
- **Replay on reconnect** - when a briefly disconnected node rejoins, the producer sees
  its read cursor is behind the write cursor and replays exactly the messages it
  missed, in order, before resuming normal flow.

Applied to the scenario above, B's cursor stays at msg 1 while it is
disconnected. On reconnect the producer replays msg 2 and msg 3, so B catches up
with **no gaps** before msg 4 arrives:

```
        Node A (broadcaster)            Node B (disconnected)
        ────────────────────            ────────────────────
  t0    broadcast msg 1   ───────────▶  received msg 1   (B cursor → 1)
  t1    ┌─ network blip (e.g. ~1ms): B drops out ─┐
  t2    broadcast msg 2     buffered             (B cursor stuck at 1)
  t3    broadcast msg 3     buffered             (B cursor stuck at 1)
  t4    └─ B reconnects ────────────────────────┘
  t4'   replay msg 2, 3   ───────────▶  received msg 2, 3 (B cursor → 3)
  t5    broadcast msg 4   ───────────▶  received msg 4    (B cursor → 4)
```

### When the buffer can't cover the gap

The buffer is bounded, so a node that stays gone long enough that its missed
messages get overwritten by newer writes cannot be replayed without gaps.
Rather than silently delivering a corrupted stream, EchoPubSub gives up on
replay *explicitly*: it sends the subscribing process a
`{:cursor_expired, node_name}` message (see [Usage](#usage)) so the application
can recover to a valid state - typically by reloading from a source of truth
such as the database or another node.

This is the core guarantee: **either you receive every message in order, or you
are told that you fell behind.** There are never silent gaps - receipt of
message 3 guarantees you have already received messages 1 and 2.

## Usage

*Note: I used LLM for typing - but ideas and decisions were mine*


```elixir
def deps do
  [
    {:echo_pubsub, "~> 0.1.0"}
  ]
end

# application.ex
children = [
  # ...,
  {Phoenix.PubSub, name: MyApp.PubSub, adapter: EchoPubSub}
]
```

Config Options

Option                  | Description                                                               | Default        |
:-----------------------| :------------------------------------------------------------------------ | :------------- |
`:name`                 | The required name to register the PubSub processes, ie: `MyApp.PubSub`    |                |
`:pool_size`            | Determines the number of workers and producers on each node               | 1              |
`:buffer_size`          | The numbers of messages to hold in memory for each producer in the pool   | 10_000         |

Subscribing processes should handle the message `{:cursor_expired, node_name}` which indicates that your client
has been disconnected long enough that your position in the broadcaster's buffer has been overwritten. At this point it is the subscribing process's job to return to a valid state i.e. reloading state from source like database or another node.

## Credits

EchoPubSub is a fork of [`phoenix_pubsub_buffered`](https://github.com/enewbury/phoenix_pubsub_buffered)
by [Eric Newbury](https://github.com/enewbury), who designed and built the
original at-least-once buffered PubSub adapter. All credit for the core design
goes to him - this fork builds on that foundation with batched inter-node
delivery, automatic replay on failure, flush-path expiry detection, telemetry,
capacity warnings, and additional configuration options.
