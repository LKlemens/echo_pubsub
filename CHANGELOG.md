# Changelog

## Unreleased
### Added
- `:max_batch_size` option: the most messages sent to a node in one call, or
  `:infinity` for no cap (default: `:infinity`, as before).

### Fixed
- A node too slow to broadcast its whole backlog within `:call_timeout` no longer
  gets the same backlog resent forever. After an outage the recovery batch was
  everything the node missed in one call. If the worker needed longer than
  `:call_timeout` for it (many local subscribers), the call timed out, the cursor
  stayed, and the same backlog was sent again, so the cursor never moved and every
  message arrived several times. A failed batch now halves that node's batch size,
  and each acked batch that left messages behind doubles it and flushes again at
  once.

## v0.2.4 - 2026-10-19
### Added
- Telemetry event `[:echo_pubsub, :sync, :batch]` for every batch sent to a
  remote node, with `count` and `batch_size` measurements and `group` and `node`
  metadata: the success counterpart of `[:echo_pubsub, :sync, :failure]`, for
  counting calls and batch sizes.

## v0.2.3 - 2026-10-19
### Added
- A `pre-push` git hook in `.githooks/` that runs the CI checks (format, compile
  with warnings as errors, credo, tests and dialyzer). Enable it with
  `git config core.hooksPath .githooks`.

### Changed
- The README benchmarks show v0.2.2 numbers: throughput under full load, and
  latency and cost at a steady rate for `batch_interval` 0 and 100.

### Fixed
- Dialyzer on OTP 28 no longer reports `contract_with_opaque` for
  `EchoPubSub.Producer.State.new/2`: the producer keeps its suspect nodes in a
  plain map instead of a `MapSet`.

## v0.2.2 - 2026-10-18
### Added
- `:retry_interval` option: milliseconds to wait before flushing again after a
  failed call (default: 200, the value that was hard-coded before).

### Fixed
- A node registered by a producer that had already flushed writes is now sent
  `{:cursor_expired, node}`. Before, if its worker did not know that producer
  (both restarted, or the producer's old boot never reached it), those writes were
  skipped silently. Found with Jepsen tests that kill nodes.
- A hung worker (paused VM, long GC) no longer gets one copy of the backlog per
  retry. After a failed delivery the producer pings the node and resends the batch
  only once it answers, so a timed-out batch arrives at most twice. Older workers
  answer the ping with `{:error, :bad_message}`, which counts as alive. In Jepsen
  runs with pauses, duplicates per node fell from up to 10,871 to 10-25. Once the
  node answers, the producer flushes to it at once instead of waiting a
  `batch_interval`.
- A registration retried against a hung worker no longer sends its subscribers
  one `{:cursor_expired, node}` per retry: the producer pings first here too, so
  they get it at most twice.
- Mixed-version clusters: registration now sends `{:register, node, missed_writes?}`,
  which a v0.2.1 or older worker rejects, so delivery from an upgraded node to it
  waits (messages stay buffered) until it is upgraded. Upgraded workers still
  accept `{:register, node}` from older producers.

## v0.2.1 - 2026-10-18
### Fixed
- A worker that keeps crashing no longer restarts its producer: each group's
  producer and worker now have their own supervisors, so the producer keeps its
  buffer and peers get no `{:cursor_expired, node}`.

## v0.2.0 - 2026-10-17
### Fixed
- A peer that hangs right after joining can no longer stall or crash the producer:
  registration (`{:register, node}`) now runs in a task like deliveries, also at
  startup, so a restart can't loop on one unresponsive node.
- A node seen by a flush before its join notice no longer gets a false
  `{:cursor_expired, node}` or a replay of the whole history; it joins the current
  batch window once registered.
- Each subscriber gets `{:cursor_expired, node}` once, not once per subscriber
  sharing its topic.
- Nodes that leave for good are forgotten once they fall more than `:buffer_size`
  behind, so a scale-down no longer pins the buffer metric or fires the capacity
  warning forever. A node whose worker restarts is registered again and keeps its
  cursor.
- A worker restart no longer sends its subscribers a false `{:cursor_expired, _}`,
  neither from its own producer nor from a peer that kept its cursor. A producer
  that still holds a node's cursor sends the new `{:resume, node}` instead of
  `{:register, node}`.
- A worker remembers which producers registered with it across its own restarts
  (an ETS table owned by the adapter supervisor), so a node that was down for more
  than a buffer is still told it expired.

### Added
- Options are validated at startup: an invalid `:pool_size`, `:buffer_size`,
  `:call_timeout`, `:batch_interval`, `:capacity_warning_interval` or
  `:capacity_warning_threshold` raises an `ArgumentError` naming the option,
  instead of crashing later (e.g. `buffer_size: 0` on the first write).
- Moduledoc section on loading state at startup: connect, subscribe, then load.

### Changed
- Mixed-version clusters: a v0.1.x worker rejects `{:resume, node}`, so delivery
  from an upgraded node to it waits (messages stay buffered) until it is upgraded.
- A node forgotten after a long absence gets `{:cursor_expired, node}` through its
  registration, so `[:echo_pubsub, :buffer, :expired]` does not fire for it.
- The worker no longer keeps the last batch in memory, and test-only calls
  (`:get_last_batch`, `:registered?`) are gone.
- Internals: the producer is split into focused modules (state, ring buffer,
  registration, delivery, timers, remote calls, telemetry) and options are
  resolved by `EchoPubSub.Options`. No behaviour change.
  builds instead of being dead code.

## v0.1.7 - 2026-10-05
### Changed
- The flush no longer waits for remote deliveries. Each node's batch is handed to
  its own task and the verdict is applied when it comes back, so an unresponsive
  peer that burns the full `:call_timeout` (5000 ms by default) can no longer stall
  the producer - previously every broadcast on that node blocked behind it and
  could itself time out. At most one batch per node is in flight, so batches still
  arrive in cursor order, and a cursor advances to the cursor its batch was
  prepared at rather than to the current write cursor.
- `:concurrent_flush` is removed: deliveries are always concurrent now.

### Fixed
- The producer survives an unexpected message instead of crashing on it and losing
  the buffer together with every node's read cursor.
- The flush timer field held the whole producer state instead of the timer
  reference; it now holds the reference, as its type says.

## v0.1.6 - 2026-10-05
### Changed
- Documentation now describes the gap EchoPubSub closes as a transient network
  failure rather than a node leaving and rejoining the cluster: messages are lost
  while delivery to a node fails even though both nodes stay members of the
  cluster, and they are replayed once delivery succeeds again.

## v0.1.5 - 2026-10-04
### Added
- README: a demo section (three-node cluster, node taken offline mid-game, missed
  events replayed on rejoin) and a benchmark section with verified Fly.io
  throughput numbers plus links to the benchmark branch.
- `docs/how-it-works.md`: a "Pools and ordering" section explaining that
  `pool_size > 1` gives independent producers, so order and at-least-once hold per
  producer rather than across the pool, and a scenario covering a worker that acks
  and then crashes (duplicates possible, never drops).

### Changed
- Documentation links now point at hexdocs instead of relative repository paths.
- Corrected the `:buffer_size` example in `EchoPubSub` to match the documented
  default of `10_000`.
- Dropped `docs/benchmarks.md` from the published docs extras; the benchmark
  results live on the benchmark branch.

## v0.1.4 - 2026-09-06
### Added
- Concurrent fan-out: each flush delivers to remote nodes in parallel (one task
  per node via a `Task.Supervisor`), so a flush costs the slowest single
  round-trip instead of the sum of all of them. On by default and engaging at 2+
  remote peers; disable with `config :echo_pubsub, concurrent_flush: false`.
- Guides: `docs/how-it-works.md` (cursor internals, flush decision, failure
  scenarios, duplicate handling); README rewritten with
  "When to use it", "Caveats", and the run-alongside-Phoenix.PubSub setup.

### Changed
- The producer GenServer state is now a typed struct with a documented `t()`
  (compile-time-only `typed_struct` dependency), replacing the raw map.

## v0.1.3 - 2026-08-16
### Added
- Bidirectional fault injection for simulating a network partition. Previously
  only the worker rejected *incoming* batches; the producer now also
  short-circuits *outgoing* sends under an injected fault, so a partitioned node
  neither receives nor emits. Both directions keep the at-least-once/replay
  semantics - messages stay buffered and replay in order on recovery, and holding
  the fault long enough overflows the sender's own ring buffer, delivering
  `{:cursor_expired, node}` to the peer.

### Changed
- Centralized the compile-time fault switch in a single `EchoPubSub.FaultInjection`
  module (`ok?/0`), shared by the producer and worker instead of each carrying its
  own gate and duplicated code paths. A consuming app opts in with
  `config :echo_pubsub, :enable_fault_injection, true` (e.g. for demos).
- Dropped the unused `:sleep` fault mode and removed leftover `dbg/1` calls from
  the worker; the `:fault_injection` flag is now effectively boolean (`:ok`
  delivers, anything else rejects).

## v0.1.2 - 2026-06-15
### Fixed
- Detect ring-buffer expiry on the flush path, not just on node (re)join. When a
  remote node lagged far enough that its unacked messages were overwritten (e.g.
  during a sustained failure with replay), the producer would replay overwritten
  slots - silently delivering wrong/duplicated messages and breaking the "no gaps"
  guarantee. It now emits `[:echo_pubsub, :buffer, :expired]` and sends
  `{:cursor_expired, node}` instead. An expired node is resumed at the current
  write cursor so it is not re-expired on every subsequent flush.
- Ack and retry the `{:cursor_expired, node}` notice like a normal batch. It was
  previously a raw `GenServer.call`: an unreachable peer at expiry time crashed the
  producer (losing the buffer and every node's read cursor), and the lagging node's
  cursor was advanced even if the notice never arrived - a silent gap. The cursor
  now only advances once the peer acks, and a failed notice is retried on the next
  flush.
- Skip flushing a node that is already caught up, avoiding a spurious empty batch
  that the worker rejected as a bad message (triggering a needless retry).
- Guard the capacity-warning calculation against a zero-sized buffer.

### Changed
- Moved the test-only failure-injection hook out of the production worker code
  path. It is now compiled in only under `Mix.env() == :test` and reads
  `config :echo_pubsub, :fault_injection` (previously the generic `:msg`/`:val`
  read on every batch in production).

## v0.1.1 - 2026-06-15
### Fixed
- Stop duplicating messages on the broadcasting node. Local delivery is handled by
  `Phoenix.PubSub` dispatch, so the producer now only forwards to remote nodes and
  advances the local read cursor itself.

### Added
- Batched inter-node delivery: messages are accumulated and flushed on a timer
  instead of being sent one-by-one. Controlled by `:batch_interval` (default `200`
  ms; `0` flushes immediately).
- Automatic replay of undelivered messages: when a remote node rejects, times out,
  or fails a batch, the producer keeps the messages buffered and schedules a retry
  flush.
- New configuration options, settable per PubSub name via the supervisor or
  Application config (opts take precedence): `:batch_interval`, `:call_timeout`
  (default `5000` ms), `:capacity_warning_threshold` (default `0.4`), and
  `:capacity_warning_interval` (default `60` s).
- Buffer capacity warnings: a throttled `Logger.warning` is emitted once the buffer
  fill ratio crosses `:capacity_warning_threshold`.
- Telemetry events: `[:phoenix_pubsub_buffered, :buffer, :flush]`,
  `[..., :buffer, :expired]`, `[..., :buffer, :capacity_warning]`,
  `[..., :sync, :failure]`, and `[..., :retry, :scheduled]`.

### Changed
- Renamed the OTP application from `:phoenix_pubsub_buffered` to `:distributed_pubsub`.
- Added `:telemetry ~> 1.0` dependency.

## v0.1.0 - 2024-5-2
- Initialize project
