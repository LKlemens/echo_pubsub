# Fly.io benchmark — steady rate, batch_interval 0 vs 100 (fra)

The throughput bench floods the producer, and there `batch_interval: 0` came out
close to `100`. This one publishes at a fixed rate, the way most apps do, to show
what each setting costs in calls, CPU and latency. Produced by
`EchoPubSub.RateBench.run()`.

## Setup

- **Cluster:** 3 × `performance-4x` (4 vCPU, 8 GB) in **fra**, no standby.
- **Load:** one publisher on one node, 30 B payload, 10 s per row at 100, 1,000
  and 10,000 msg/s. `pool_size = 1`.
- **Measured:** flushes per second on the publisher (`[:echo_pubsub, :buffer,
  :flush]` events); CPU as scheduler utilization of the whole VM (100% = all 4
  schedulers busy) on the publisher and on one receiver; latency from `broadcast!`
  on the publisher to a subscriber, on the publisher itself (local) and on the
  two other nodes (remote).
- All rows delivered every message to every node.
- **Version:** v0.2.2 (`benchmark` branch), run on 2026-10-18.

Remote latency subtracts timestamps from two machines, so it includes their clock
skew. The 100 ms rows show the expected ~50 ms average wait for the timer, so the
skew is small next to the effects measured here.

## Results

batch_ms | rate | flushes/s | publisher_cpu% | receiver_cpu% | local_p50 | remote_p50 | remote_p99 | remote_max
---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---:
0 | 100 | 100 | 0.5 | 0.2 | 0.1 | 2.1 | 3.4 | 5.1
0 | 1000 | 1998 | 2.9 | 0.7 | 0.1 | 2.1 | 4.1 | 16.3
0 | 10000 | 11125 | 4.3 | 0.8 | 0.0 | 2.4 | 4.2 | 24.8
100 | 100 | 9 | 0.1 | 0.0 | 0.1 | 53.4 | 103.6 | 104.9
100 | 1000 | 10 | 0.3 | 0.0 | 0.0 | 53.6 | 103.6 | 105.7
100 | 10000 | 10 | 1.2 | 0.2 | 0.0 | 57.4 | 106.2 | 110.5

Latency in ms.

## Takeaways

- **`batch_interval: 0` cuts latency ~25x**: remote p50 ~2 ms and p99 ~4 ms,
  against ~53 ms and ~104 ms with a 100 ms timer, which delays each message by
  half the interval on average and the whole interval at worst.
- **It costs ~100-1,000x more flushes.** At 0 a flush runs for every write that
  finds no batch in flight: one per message at 100 msg/s, about two per message
  at 1,000 msg/s, and close to one per message at 10,000 msg/s. A 100 ms timer
  holds it at 10 per second whatever the rate. Each flush sends one call to every
  remote node with something pending, so calls grow with rate × nodes.
- **The CPU bill is small at this scale**: the publisher uses 0.5-4.3% of its 4
  vCPUs at 0 against 0.1-1.2% at 100 (about 3-10x more), and the receiver stays
  under 1% either way.
- **Keep the option.** It is the knob that trades latency for calls, and the cost
  side grows with cluster size and message rate, which this 3-node run does not
  stress. What the data does support is a lower default: at 3 nodes and up to
  10k msg/s, `0` (or a few ms) buys a large latency win for little CPU.
- **About two flushes per message at 1,000 msg/s** suggests a flush that finds
  nothing new to send, likely the follow-up a delivery schedules when writes
  landed while it was in flight. Coalescing those could trim the cost of `0`.
