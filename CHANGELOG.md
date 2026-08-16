# Changelog

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
