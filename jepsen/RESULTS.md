# Jepsen results for echo_pubsub

Runs from 2026-10-18 on Jepsen's 5-node Docker cluster, against echo_pubsub from
v0.2.1 up to v0.2.2. Jepsen found three bugs, all fixed in v0.2.2, and v0.2.2
passes every run.

## What was tested

- **Property:** at-least-once delivery. Every broadcast acked by a node reaches
  every subscriber on every node. Duplicates are allowed. A loss is allowed only
  when the subscriber is told (`{:cursor_expired, node}`) or when a restart
  explains it: the producer's buffer is in memory, and so is the subscriber's
  mailbox. See [README.md](README.md#what-is-checked) for the checker's verdicts.
- **Excuses are time-bounded:** a restart or `cursor_expired` excuses a loss or
  gap only if it happened after the broadcast was invoked. The first version of
  the checker accepted a restart at any time, and that hid the first bug below.
- **Cluster:** 5 Debian nodes (n1–n5), one `node_app` release on each, which takes
  broadcasts over HTTP and logs every message it receives.
- **Faults (nemesis):** iptables partitions, SIGKILL of one node or a minority,
  and SIGSTOP pauses.
- **Workloads:**
  - `delivery`: a buffer of 1,000,000 messages, so no node should expire because
    it fell behind; 120 s runs.
  - `expiry`: a buffer of 50 messages; partitioned nodes expire, and every gap
    they see must be explained by a `cursor_expired`.
  - Canary: `delivery` with a 50-message buffer must fail. It shows the checker
    catches real violations.

## Bugs found in echo_pubsub (fixed in v0.2.2)

1. **Silent loss after a restart.** A restarted node's producer broadcast right
   away, but only to nodes it had already registered. A node registered a moment
   later started at the latest flush. That node had never registered with this
   producer before, because both rebooted or a partition blocked it, so its
   worker did not know the producer and no `cursor_expired` was sent. The first
   writes of the new producer were skipped without notice: 69 losses in the
   first 0.8–1.6 s after boot, across 4 subscriber/producer pairs. **Fix:** a
   producer that registers a node after it has already flushed writes tells the
   node they are missing, so its subscribers get `cursor_expired`.
2. **Duplicate backlog against a hung worker.** While a node was paused, every
   timed-out delivery was retried, and each retry queued another copy of the
   backlog in the worker's mailbox. When the node resumed it delivered all of
   them: up to 10,871 duplicates per node from about 4,000 messages. **Fix:**
   after a failed delivery the producer pings the node and resends only once it
   answers, so a timed-out batch arrives at most twice.
3. **One `cursor_expired` per registration retry.** The same thing happened to
   registrations: retries against a frozen worker queued several register
   messages, and subscribers got one `cursor_expired` for each. **Fix:**
   registration pings first too, so subscribers get it at most twice.

## Harness bugs fixed along the way

- **Slow restarts:** a restarted node took about 16 s to accept HTTP, because
  joining the cluster blocked on partitioned peers. Each connect now runs in its
  own task.
- **Double VM start:** after a kill, Jepsen's restart booted a second VM next to
  the running one, because the pidfile pointed at the launcher script. Its fake
  `boot` log line excused losses that should have failed the run. The start is
  now skipped when `beam.smp` is already running.
- **Whole-cluster kills:** the kill fault could take down all 5 nodes, which
  excused every loss as a crash. It now kills one node or a minority only.

## Results

`delivery` workload, 120 s each:

| Version | Faults | Acked | Lost | Gaps | Duplicates per node | Valid |
| --- | --- | ---: | ---: | ---: | --- | --- |
| before fix 1, old checker | partition, kill, pause | 4,060 | 0 | 0 | up to 10,871 | yes (losses wrongly excused) |
| before fix 1 | partition, kill, pause | - | 69 | 1,811 | - | **no** |
| before fix 1 | partition, pause | - | 0 | 0 | - | yes |
| fix 1 | partition, kill, pause | 3,799 | 0 | 0 | - | yes |
| fix 1 + 2 | partition, pause | 4,644 | 0 | 0 | 19 / 16 / 25 / 10 (n1–n4) | yes |
| fix 1 + 2 | partition, kill, pause | 2,758 | 0 | 0 | 3 / 3 / 6 / 6 (n1–n4) | yes |
| **v0.2.2** (fix 1 + 2 + 3) | partition, kill, pause | 4,244 | 0 | 0 | 6 / 5 / 3 / 2 / 5 | **yes** |

The 1,811 gaps before fix 1 are the same 69 losses, counted again for each later
message that arrived after a missing one. Without kills the bug did not show,
so it needs a restart.

**No silent loss in v0.2.2.** Some deliveries are missing, but each one came
with a crash or a `cursor_expired`, which is what at-least-once allows. `Lost`
counts the missing deliveries nothing explains, the bug this suite looks for.

Loss and gap counts are per subscriber node and message, not per unique message:
4,244 acked broadcasts × 5 nodes = 21,220 expected deliveries. Of those, the
v0.2.2 run missed 2,884 (~14%), all excused, and the checker confirmed each
excuse happened after the broadcast:

- **2,068 lost to a producer crash:** the node acked the broadcast, then was
  killed before it flushed. The buffer is in memory, so an ack means "accepted",
  not "durable". The killed node's own subscriber is counted too.
- **497 lost after `cursor_expired`:** the subscriber was told it missed messages
  and must reload its state. This is the notice fix 1 added; before it, the same
  loss was silent.
- **319 lost to a subscriber restart:** the message was in the mailbox, or not
  yet delivered, when the subscriber's node was killed; it reloads on restart.
- **2,582 gaps excused:** not extra losses, but messages that arrived after a
  hole that one of the cases above explains.

The share is high because the nemesis injects a fault every 10 s on average,
kills included, and a killed node stays down for a while; outside
such tests, crash losses happen only when a node dies with unflushed writes.
