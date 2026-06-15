# Changelog

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
