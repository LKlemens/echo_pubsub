# Fly.io benchmark — small payloads, 3–4 nodes (fra)

End-to-end delivery benchmark on a real Fly.io cluster, where delivery is
network-bound: shows what batching, payload size and node count cost, and why you
should send small updates, not whole objects.

## Setup

- **Cluster:** Fly.io, all in **fra** (single region), 6PN* WireGuard mesh.
- **Machines:** `performance-4x` (4 vCPU, 8 GB), 3 or 4, no standby (`--ha=false`).
- **Config:** `pool_size = 1`, `publishers = 4` (= `schedulers_online`),
  `buffer_size = 200_000`.
- **Samples:** median of 3, 50k msgs/sample for both batch intervals.
- All runs `delivered=true`, `overflow=0`. Runtime: OTP 27 release, `MIX_ENV=bench`.
- **Version:** v0.2.2 (`benchmark` branch with main merged), run on 2026-10-18.

\* **6PN** = Fly's *IPv6 Private Network*: a per-org WireGuard-encrypted mesh over
which machines reach each other by private `fdaa:…` addresses (`<app>.internal`).
The cluster's inter-node traffic (and every acked flush) rides it.

## Results (msg/sec, all delivered, overflow 0)

| batch_ms | payload_B | 3 nodes | 4 nodes | before: 3 nodes | before: 4 nodes |
|:---------|:----------|--------:|--------:|----------------:|----------------:|
| **100**  | 10        | 127,360 | 124,073 |          79,445 |          75,468 |
| 100      | 20        | 159,121 | 117,309 |          74,521 |          77,692 |
| 100      | 30        | 124,106 | 113,003 |          67,543 |          69,162 |
| 100      | **200**   |  69,052 |  54,406 |          41,784 |          43,479 |
| **0**    | 10        |  96,294 |  91,568 |           1,319 |             787 |
| 0        | 20        | 114,176 |  99,654 |           1,199 |             607 |
| 0        | 30        | 127,934 | 133,124 |           2,124 |             788 |
| 0        | 200       |  79,944 |  65,699 |               - |               - |

"Before" is the earlier run of this benchmark, from before the producer handed
each node's batch to its own task (one synchronous, acked call per flush); its
`batch=0` rows used 10k msgs/sample.

## Takeaways

- **`batch=0` is no longer a trap: ~70-170x faster than before.** A flush now
  hands each remote node's batch to a task and returns, and only one batch per
  node is in flight. Writes that land while a batch is in flight pile up and go
  out together in the next one, so the batching happens on its own. Before, every
  message was a synchronous 6PN round trip.
- **`batch=100` is ~1.5-2x faster than before** for the same reason: the producer
  keeps accepting writes while a flush is on the wire.
- **`batch=0` and `batch=100` are now close.** `batch=100` is ~20-40% ahead at
  10-20 B; `batch=0` is ~3-20% ahead at 30-200 B. With one batch in flight per
  node, the in-flight round trip already sets the batch size, so the timer adds
  little either way. They differ in latency and cost; see the next point.
- **Below full load, `batch=0` trades CPU for ~25x lower latency.** This bench
  floods the producer, so it shows neither. At a steady rate
  ([results-fra-rate.md](results-fra-rate.md)), runs from 100 to 100,000 msg/s
  on 3 nodes:
  - *Latency:* `batch=0` delivers to remote nodes in ~1-2 ms p50 / ~3-8 ms p99,
    against ~53-95 ms / ~104-156 ms at `batch=100`, where every message waits for
    the timer.
  - *Calls:* `batch=0` sends at most ~700-1,000 calls per node per second, capped
    by the round trip since one batch per node is in flight; past that, batches
    grow instead (~15 messages at 10k msg/s, ~70 at 66k). `batch=100` sends a
    fixed 10. Each extra node adds its own stream of calls.
  - *CPU:* `batch=0` uses ~2-10x more on the publisher: 0.5-4.3% of 4 vCPUs up to
    10k msg/s against 0.2-1.3%, and 11.0% against 3.9% at 30k.
  - *One publisher's ceiling:* with `batch=0` a single publishing process tops
    out at ~66k msg/s (asked for 100k), against ~100k with `batch=100`: the
    producer runs a flush pass between its synchronous writes.
- **Payload size still bites**: 200 B costs ~20-55% against 10-30 B.
- **A 4th node costs ~5-25%**: one more acked batch per flush.
- **Single producer:** `pool_size = 1` funnels all sends through one producer;
  raising `pool_size` is what parallelizes the producer side.

## Send small updates, not whole objects

A "whole object" broadcast is a full record; a "delta" is just what changed:

```
whole (192 B):
{"user_id":1042,"name":"Ada Lovelace","email":"ada@example.com","plan":"pro",
 "seats":5,"status":"active","country":"GB","updated_at":"2026-09-08T11:22:33Z",
 "last_login":"2026-09-08T10:15:00Z"}

delta  (29 B):  {"user_id":1042,"plan":"pro"}
```

At ~200 B the whole object already costs ~40-55% throughput vs a ~30 B delta — and it
grows with the record. Broadcasting the changed field(s) instead keeps messages
tiny (round-trip-bound, not byte-bound) and pairs naturally with EchoPubSub's
at-least-once semantics: a `{:set, field, value}`-style delta is idempotent, so a
redelivered duplicate is harmless. Reserve whole-object sends for a cold resync
after `{:cursor_expired, node}`.
