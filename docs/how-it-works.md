# How EchoPubSub works

This is the full walkthrough of what EchoPubSub does and why. For the quick pitch
and setup, see the [README](../README.md).

## The problem: messages lost during temporary network problems

The default `Phoenix.PubSub.PG2` adapter is **fire-and-forget**. When a node
broadcasts, the message is delivered to the nodes that are connected *at that
moment*. There is no buffer and no acknowledgement - if a node is unreachable when
the broadcast happens, the message is simply gone for that node.

Even a momentary network problem - a blip lasting as little as 1ms - means silent
data loss for any message broadcast during it:

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

When B comes back at `t4` it carries on as if nothing happened: it has **no idea**
that msg 2 and msg 3 ever existed. There is no error, no gap detection - just a
hole in the stream. For anything that relies on the message stream being complete
(replicated caches, event logs, derived state), this quietly corrupts B's view of
the world.

## How EchoPubSub solves it

EchoPubSub upgrades delivery from fire-and-forget to **at-least-once** by having
each broadcasting node remember what it has sent and to whom:

- **Ring buffer of recent messages** - every producer keeps the last
  `:buffer_size` messages in memory, each stamped with a monotonically increasing
  *write cursor*.
- **Per-node read cursors** - the producer tracks how far each remote node has
  acknowledged. Remote delivery is a synchronous, acked call; a node's cursor only
  advances once it confirms receipt.
- **Replay on reconnect** - when a briefly disconnected node rejoins, the producer
  sees its read cursor is behind the write cursor and replays exactly the messages
  it missed, in order, before resuming normal flow.

Applied to the scenario above, B's cursor stays at msg 1 while it is disconnected.
On reconnect the producer replays msg 2 and msg 3, so B catches up with **no
gaps** before msg 4 arrives:

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
messages get overwritten by newer writes cannot be replayed without gaps. Rather
than silently delivering a corrupted stream, EchoPubSub gives up on replay
*explicitly*: it sends the subscribing process a `{:cursor_expired, node_name}`
message so the application can recover to a valid state - typically by reloading
from a source of truth such as the database or another node.

This is the core guarantee: **either you receive every message in order, or you
are told that you fell behind.** There are never silent gaps - receipt of message
3 guarantees you have already received messages 1 and 2.

## State

Each producer holds three things in its state:

- **`write_cursor`** - a count of every message ever written. It is *absolute* and
  *monotonic*: it only ever increases, and the next write always lands at its
  current value.
- **`read_cursors[node]`** - the next cursor each remote node still needs.
  Everything below it, that node has already acknowledged. It advances only on a
  confirmed delivery (`advance/2` sets it to the current `write_cursor`); a failed
  send leaves it untouched, so the same messages are resent on the next flush.
  That is what makes delivery at-least-once.
- **`buffer`** - an Erlang `:array` holding the last `buffer_size` messages. The
  message with cursor `c` lives in slot `rem(c, buffer_size)`, so after
  `buffer_size` further writes its slot is reused and the old message is gone.

The important split is that **the cursors are absolute and unbounded while the
buffer is bounded and wraps.** Subtracting one from the other tells you both where
a node is and whether its messages still exist - no wrap-around bookkeeping
needed.

## The flush decision

On each flush the producer decides, per remote node, what that node needs. The
whole decision (`prepare_batch/2`) turns on one quantity: how far behind the node
is.

```
next_needed = read_cursors[node]        # or 0 if the node is unknown
lag         = write_cursor - next_needed # how many messages this node is behind

lag == 0            -> :caught_up                    # acked everything, send nothing
lag >  buffer_size  -> {:expired, lag - buffer_size} # oldest overwritten, unrecoverable
else                -> {:messages, gap}              # replay next_needed..write_cursor-1
```

Because the read cursor only ever advances *to* the write cursor, `lag` is always
`>= 0`, and `lag == 0` means fully caught up. When `lag` exceeds `buffer_size`,
the oldest `lag - buffer_size` messages have already been overwritten in the ring,
so they cannot be replayed and the node is told it expired. Otherwise the whole
gap is still buffered and is replayed in order. The absolute cursors can be huge,
but `lag` stays small and bounded, so this is the only comparison that matters.

## Worked example (buffer size 4)

Write m0, m1, m2 (`write_cursor` = 3) and let node B acknowledge them, so
`read_cursors[B]` = 3:

```
slots: 0:m0 1:m1 2:m2 3:_       write=3  B=3   lag=0        -> :caught_up
```

B drops off the network. A writes m3 and m4:

```
slots: 0:m4 1:m1 2:m2 3:m3      write=5  B=3   lag=2 (<=4)  -> {:messages, [m3, m4]}
```

B is two behind, both still buffered, so on reconnect it gets m3 and m4 - no gap.
Now B stays gone while A writes m5, m6, m7:

```
slots: 0:m4 1:m5 2:m6 3:m7      write=8  B=3   lag=5 (>4)   -> {:expired, 5-4=1}
```

B is now five behind but the buffer only holds four, so cursor 3 (m3) was
overwritten. B is told `:cursor_expired` and reloads from a source of truth. Its
cursor is then advanced to 8 so it is not re-expired on the next flush.

## Scenarios

**A new node joins mid-stream.** It is seeded at the current `write_cursor`, not
at 0. If `write_cursor` is 100000, the joiner's cursor becomes 100000, so it is
immediately `:caught_up` - it gets no historical backlog, only future messages.
(A cursor of 0 only happens at init, when `write_cursor` is also 0.)

**A brief disconnect (blip).** The node's cursor stays put while it is gone. On
rejoin the producer replays the buffered gap in order, then resumes - no gap, as
in the diagram above.

**A node falls too far behind.** Its missed messages are overwritten in the ring,
so replay is impossible; it receives `{:cursor_expired, node}` and the application
reloads from the database or another node.

**A remote worker crashes.** The read cursor lives in the *producer's* state on the
broadcasting node, so the worker crashing does not touch it. Its supervisor
restarts the worker, it re-joins the `:pg` group, and the producer - seeing the
node is still known - resumes delivery from the stored cursor. Any batch that was
in flight when the worker died was never acknowledged, so its cursor never
advanced and those messages are replayed. Nothing in the node-level stream is
lost. (This protects delivery *to that node*: if only the worker crashed, its
subscribers survive and get the gap filled. If the node's whole VM restarted, its
subscribers are new and only receive the buffered tail, so cold subscribers should
reload from a source of truth on startup - and if the gap outran the buffer,
`:cursor_expired` fires anyway.)

**The producer crashes.** On restart `init` resets `write_cursor` to 0 with an
empty buffer and re-registers with every worker. Each worker sees the node as
*already registered* and broadcasts `:cursor_expired` to its local subscribers, so
everyone reloads from a source of truth. Safe, with no silent gap.

## Cursor overflow is safe

The cursors increment forever - only the buffer wraps. This is safe because Erlang
integers are arbitrary-precision, so they never overflow. Past `2^59` a small
integer is promoted to a bignum (a few extra words of memory, not a failure):

```
2^59            = 576,460,752,303,423,488   ≈ 5.76e17   (signed 60-bit small-int max)
/ 1e6 msg/s     = 5.76e11 s
/ 31,557,600    ≈ 18,250 years              just to leave the small-int range
```

So correctness is effectively unbounded.

## Delivery mechanics

- **Batching.** `batch_interval` milliseconds buffers writes and then triggers one
  `:flush_all`; `0` flushes immediately. Full batches amortize the round-trip
  across many messages, which is where throughput comes from.
- **Concurrent fan-out.** With two or more remote peers, the flush delivers to each
  node in its own task (via a `Task.Supervisor`), so it costs the slowest single
  round-trip instead of the sum of all of them. Disable with
  `config :echo_pubsub, concurrent_flush: false`.
- **Retry.** Any failed send leaves that node's cursor untouched and schedules a
  retry flush, so the unacknowledged messages are redelivered.

## Handling duplicate deliveries

At-least-once delivery means a message can arrive **more than once**. The producer
advances a node's read cursor only once it receives the delivery `ack`. If the
message was received and handled successfully but the `ack` was lost on the way
back - the cursor does not advance, so the same message is re-sent on the next
flush. There is no way to distinguish "message lost" from "ack lost", so the safe
choice is always to resend; consumers must therefore tolerate duplicates.

There are two common ways to make that safe.

### 1. Make handling idempotent - send state, not deltas

If applying a message twice yields the same result, a duplicate is harmless.
Prefer broadcasting absolute values over incremental ones:

```elixir
# idempotent: applying it twice leaves the balance at 100
Phoenix.PubSub.broadcast(MyApp.EchoPubSub, "acct:1", {:balance, 100})

# NOT idempotent: a redelivery adds 10 twice
Phoenix.PubSub.broadcast(MyApp.EchoPubSub, "acct:1", {:add_balance, 10})
```

The same idea covers cache invalidation ("key X is now V" is safe to reapply) and
presence/state fan-out (broadcast the full state, not a diff).

### 2. Dedupe by message id

When the payload is genuinely an event that can't be made idempotent, stamp each
message with a unique id and skip ids you have already handled:

```elixir
# publisher
Phoenix.PubSub.broadcast(MyApp.EchoPubSub, "events", {:event, System.unique_integer([:positive]), payload})

# subscriber
def handle_info({:event, id, payload}, state) do
  if MapSet.member?(state.seen, id) do
    {:noreply, state}                                   # duplicate - ignore
  else
    apply_event(payload)
    {:noreply, %{state | seen: MapSet.put(state.seen, id)}}
  end
end
```

Bound the `seen` set so it does not grow forever - an LRU, a ring of recent ids,
or a time window (duplicates only ever arrive close together, on the retry after a
lost ack, so a short window is enough). For ids that must be unique across nodes,
use something like `{node(), System.unique_integer([:positive])}` or a UUID rather
than a bare integer.
