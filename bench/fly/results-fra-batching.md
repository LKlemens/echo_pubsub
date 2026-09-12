# Fly.io benchmark — small payloads, 3–4 nodes (fra)

End-to-end delivery benchmark on a real Fly.io cluster, where delivery is
network-bound: shows the batching win and why you should send small updates, not
whole objects.

## Setup

- **Cluster:** Fly.io, all in **fra** (single region), 6PN* WireGuard mesh.
- **Machines:** `performance-4x` (4 vCPU, 8 GB), 3 or 4, no standby (`--ha=false`).
- **Config:** `pool_size = 1`, `publishers = 4` (= `schedulers_online`),
  `buffer_size = 200_000`.
- **Samples:** median of 3. `batch=100` uses 50k msgs/sample; `batch=0` uses 10k
  (~50× slower, so fewer — msg/s is a rate).
- All runs `delivered=true`, `overflow=0`. Runtime: OTP 28 release, `MIX_ENV=bench`.

\* **6PN** = Fly's *IPv6 Private Network*: a per-org WireGuard-encrypted mesh over
which machines reach each other by private `fdaa:…` addresses (`<app>.internal`).
The cluster's inter-node traffic (and every acked flush) rides it.

## Results (msg/sec, all delivered, overflow 0)

| batch_ms | payload_B | 3 nodes | 4 nodes |
|:---------|:----------|--------:|--------:|
| **100**  | 10        |  79,445 |  75,468 |
| 100      | 20        |  74,521 |  77,692 |
| 100      | 30        |  67,543 |  69,162 |
| 100      | **200**   |  41,784 |  43,479 |
| **0**    | 10        |   1,319 |     787 |
| 0        | 20        |   1,199 |     607 |
| 0        | 30        |   2,124 |     788 |

## Takeaways

- **Batching is decisive over a network:** `batch=100` beats `batch=0` by
  **~60–95×**. At `batch=0` every message is a synchronous acked 6PN round-trip —
  latency, not CPU, is the wall.
- **Small payloads (10–30 B): negligible** (a few %). But **size starts to bite
  once messages grow**: a 200 B payload drops 4-node throughput from ~75k to ~43k
  (**~42%**) — the bytes now cost more than the fan-out.
- **Send small updates, not whole objects.** See below.
- **Single producer:** `pool_size = 1` funnels all sends through one producer;
  `publishers = 4` pipelines its synchronous write/flush round-trips (that's why
  these numbers are ~1.6× the earlier `publishers = 1` run). Raising `pool_size`
  is what actually parallelizes the producer side.

## Send small updates, not whole objects

A "whole object" broadcast is a full record; a "delta" is just what changed:

```
whole (192 B):
{"user_id":1042,"name":"Ada Lovelace","email":"ada@example.com","plan":"pro",
 "seats":5,"status":"active","country":"GB","updated_at":"2026-09-08T11:22:33Z",
 "last_login":"2026-09-08T10:15:00Z"}

delta  (29 B):  {"user_id":1042,"plan":"pro"}
```

At ~200 B the whole object already costs ~42% throughput vs a ~30 B delta — and it
grows with the record. Broadcasting the changed field(s) instead keeps messages
tiny (round-trip-bound, not byte-bound) and pairs naturally with EchoPubSub's
at-least-once semantics: a `{:set, field, value}`-style delta is idempotent, so a
redelivered duplicate is harmless. Reserve whole-object sends for a cold resync
after `{:cursor_expired, node}`.

## Cost note

Cluster scaled to 0 after the run (no machines billing). Bring it back with
`fly scale count 3 -c bench/fly/fly.toml`.
