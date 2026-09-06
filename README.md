# EchoPubSub

[![CI](https://github.com/LKlemens/echo_pubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/LKlemens/echo_pubsub/actions/workflows/ci.yml)
[![hex.pm version](https://img.shields.io/hexpm/v/echo_pubsub.svg)](https://hex.pm/packages/echo_pubsub)
[![hex.pm license](https://img.shields.io/hexpm/l/echo_pubsub.svg)](https://github.com/LKlemens/echo_pubsub/blob/main/LICENSE)

A Phoenix.PubSub adapter that distributes messages between nodes using the erlang `:pg` module, like the default adapter, however with the additional guarentees of "at least once" delivery. 

This means that nodes can disconnect temporarily from the cluster - even for a blip as short as ~1ms - and then "catch up" when they rejoin, thanks to a buffer of messages and read cursors.

See the [Docs](https://hexdocs.pm/echo_pubsub/EchoPubSub.html) for more information.

## How it works

`Phoenix.PubSub.PG2` is fire-and-forget: a broadcast reaches only the nodes
connected at that instant. A blip as short as ~1ms silently drops messages for any
node briefly unreachable.

EchoPubSub makes delivery **at-least-once**:

- **Buffer + cursors** - each broadcaster keeps a ring buffer of recent messages,
  plus a per-node read cursor that advances only on an acked delivery.

- **Replay on reconnect** - a reconnecting node is replayed exactly the messages
  it missed, in order.

- **Told if it fell behind** - if it stayed gone long enough that those messages
  were overwritten in the bounded buffer, it gets `{:cursor_expired, node_name}`
  (see [Usage](#usage)) telling it to reload from a source of truth.

**Core guarantee: either you receive every message in order, or you are told you
fell behind** - never a silent gap.

See [how it works](docs/how-it-works.md) for diagrams, the cursor internals, and
failure scenarios.

## Usage

*Note: I used LLM for typing - but ideas and decisions were mine*

> **Not a drop-in replacement for Phoenix.PubSub.** At-least-once delivery costs
> more than fire-and-forget (buffering, acked cross-node calls). Keep the default
> PubSub for ordinary broadcasts and run EchoPubSub *alongside* it, using it only
> for cross-node data that must not be lost (replicated caches, event logs,
> derived state).


```elixir
def deps do
  [
    {:echo_pubsub, "~> 0.1.0"}
  ]
end

# application.ex
Both children default to the same child id (`Phoenix.PubSub.Supervisor`), so give each a distinct `id:`:

```elixir
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
`:pool_size`            | Determines the number of workers and producers on each node               | 1              |
`:buffer_size`          | The numbers of messages to hold in memory for each producer in the pool   | 10_000         |
`:batch_interval`       | Milliseconds to batch writes before a flush; `0` flushes immediately      | 200            |

Subscribing processes should handle the message `{:cursor_expired, node_name}` which indicates that your client
has been disconnected long enough that your position in the broadcaster's buffer has been overwritten. At this point it is the subscribing process's job to return to a valid state i.e. reloading state from source like database or another node.

## Credits

EchoPubSub is a fork of [`phoenix_pubsub_buffered`](https://github.com/enewbury/phoenix_pubsub_buffered)
by [Eric Newbury](https://github.com/enewbury), who designed and built the
original at-least-once buffered PubSub adapter. All credit for the core design
goes to him - this fork builds on that foundation with batched inter-node
delivery, automatic replay on failure, flush-path expiry detection, telemetry,
capacity warnings, and additional configuration options.
