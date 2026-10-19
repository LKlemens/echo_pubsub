# Fly.io benchmark — steady rate, batch_interval 0 vs 100 (fra)

The throughput bench floods the producer, and there `batch_interval: 0` came out
close to `100`. This one publishes at a fixed rate, the way most apps do, to show
what each setting costs in calls, CPU and latency. Produced by
`EchoPubSub.RateBench.run()`.

## Setup

- **Cluster:** 3 × `performance-4x` (4 vCPU, 8 GB) in **fra**, no standby.
- **Load:** one publisher on one node, 30 B payload; three separate 10 s runs per
  setting, at 100, 1,000 and 10,000 msg/s. `pool_size = 1`.
- **Measured, on the publisher:**
  - *flushes/s:* `[:echo_pubsub, :buffer, :flush]` events, one per pass over the
    remote nodes. A pass sends nothing to a node that already has a batch in
    flight, so a flush is not a call.
  - *calls/s:* `[:echo_pubsub, :sync, :batch]` events, one per batch actually
    sent, summed over both remote nodes; *msgs/call* is the average batch size.
  - *CPU:* scheduler utilization of the whole VM (100% = all 4 schedulers busy),
    on the publisher and on one receiver.
- **Latency:** from `broadcast!` on the publisher to a subscriber, on the
  publisher itself (local) and on the two other nodes (remote).
- All rows delivered every message to every node.
- **Version:** v0.2.2 (same behaviour as v0.2.3) plus the unreleased `[:echo_pubsub, :sync, :batch]` event,
  `benchmark` branch, run on 2026-10-19. These runs also had an experimental
  change that let writes queued together share one flush; it changed nothing
  measurable here and was dropped.

Remote latency subtracts timestamps from two machines, so it includes their clock
skew. The 100 ms rows show the expected ~50 ms average wait for the timer, so the
skew is small next to the effects measured here.

## Results

batch_ms | rate | flushes/s | calls/s | msgs/call | publisher_cpu% | receiver_cpu% | local_p50 | remote_p50 | remote_p99 | remote_max
---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:
0 | 100 | 100 | 200 | 1.0 | 0.5 | 0.2 | 0.1 | 1.6 | 3.0 | 4.6
0 | 1000 | 1949 | 1374 | 1.5 | 2.4 | 0.6 | 0.1 | 1.7 | 4.0 | 7.7
0 | 10000 | 10902 | 1354 | 14.8 | 4.3 | 0.9 | 0.0 | 2.1 | 4.7 | 23.2
100 | 100 | 9 | 18 | 11.0 | 0.2 | 0.0 | 0.1 | 52.8 | 103.0 | 105.5
100 | 1000 | 10 | 20 | 102.0 | 0.3 | 0.0 | 0.0 | 53.1 | 103.1 | 105.2
100 | 10000 | 10 | 20 | 1019.3 | 1.3 | 0.2 | 0.0 | 56.7 | 105.9 | 110.1

Latency in ms. Calls are over 2 remote nodes, so halve them for one node.

### Higher rates

Same setup, at 30,000 and 100,000 msg/s; `sent/s` is the rate the single
publisher actually reached.

batch_ms | rate | sent/s | flushes/s | calls/s | msgs/call | publisher_cpu% | receiver_cpu% | local_p50 | remote_p50 | remote_p99 | remote_max
---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:
0 | 30000 | 29991 | 30703 | 2006 | 29.9 | 11.0 | 1.7 | 0.0 | 1.0 | 3.2 | 64.7
0 | 100000 | **65911** | 66354 | 1905 | 69.2 | 17.3 | 2.8 | 0.0 | 1.3 | 7.7 | 66.3
100 | 30000 | 29997 | 10 | 19 | 3085.7 | 3.9 | 0.7 | 0.0 | 64.7 | 111.8 | 120.4
100 | 100000 | 99820 | 9 | 18 | 10697.4 | 12.0 | 2.4 | 0.0 | 94.7 | 156.1 | 268.4

Run on 2026-10-19, same version.

## Takeaways

- **`batch_interval: 0` cuts latency ~25x**: remote p50 ~2 ms and p99 ~4-5 ms,
  against ~53 ms and ~104 ms with a 100 ms timer, which delays each message by
  half the interval on average and the whole interval at worst.
- **Calls level off with `0`.** Only one batch per node is in flight, so the
  round trip caps the call rate: ~700 calls per node per second at both 1,000 and
  10,000 msg/s, with batches growing from 1.5 to ~15 messages. At low rates every
  message is its own call (100 msg/s = 200 calls/s to 2 nodes). A 100 ms timer
  holds it at 10 calls per node per second, with batches of up to ~1,000.
  Higher up, `0` stays at ~1,000 calls per node per second while batches grow
  to 30 messages at 30k msg/s and ~70 at 66k msg/s.
- **With `0`, one publisher tops out at ~66k msg/s.** Asked for 100k, it reached
  65,911; with `100` it reached 99,820. Each `broadcast!` is a synchronous call
  to the producer, and with `0` the producer runs a flush pass after every write
  before it takes the next one. Several publishers (or `pool_size > 1`) should
  go further; this run did not test that. Latency stays low meanwhile: remote p50 1.3 ms, p99 7.7 ms.
- **`100` gets slower under load too:** at 100k msg/s its remote p50 rises to
  ~95 ms and p99 to ~156 ms, as each ~10,000-message batch takes longer to send
  and apply.
- **Flushes are not calls.** With `0` the producer runs about one flush per
  message, but most find a batch still on the wire and send nothing, so a flush
  costs little.
- **The CPU bill is small up to 10k msg/s**: the publisher uses 0.5-4.3% of its
  4 vCPUs at 0 against 0.2-1.3% at 100, and the receiver stays under 1% either
  way. At 30k msg/s it is 11.0% against 3.9%, about 3x.
- **Keep the option.** It trades latency for calls; with `0` calls grow with
  nodes (one stream per node, each capped by its round trip), which this 3-node
  run does not stress. The data does support a lower default: at 3 nodes and up
  to 10k msg/s, `0` (or a few ms) buys a large latency win for little CPU.
