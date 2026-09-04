# Changelog

## [Unreleased]

### Added

- Dedicated RESP2/RESP3 subscriber with binary channel/pattern streams, independent local handles,
  bounded drop-incoming queues and drop counters, bounded acknowledgement lifecycle, and terminal
  cancellation/disposal semantics. Reconnect, tracking, and sharded subscriptions remain unsupported.
- Bounded RESP2/RESP3 concurrent connection-loss regressions and recovery from a non-listening
  loopback endpoint without replaying an unsent write.
- Opt-in, ownership-validated Docker stop/start runner with configurable cycles, resource samples,
  independent cleanup, and a manual Resilience workflow. Live restart execution remains opt-in.
- Opt-in connection-owner metrics and activity tracing using only BCL APIs, with fixed-cardinality
  operation/error tags, reconnect/attempt counts, durations, and an active-operation gauge.
- Listener exception isolation, privacy and ambient-trace regression tests, and live recovery
  telemetry assertions; no command text, keys, values, credentials, or endpoint labels are recorded.
- Standalone connection ownership with shared bounded connection attempts, capped exponential
  backoff and jitter, fail-fast admission, lifecycle health, and explicit per-operation retry opt-in.
- Owner command, pipeline, and script deadlines cover connection acquisition and execution while
  preserving delivery ambiguity across authorized retries; ordinary writes are never replayed.
- Deterministic recovery/cancellation/admission coverage and live repeated connection-loss recovery
  with restored RESP2/RESP3, database, client-name, and script-cache state.
- Binary-safe structured Lua scripts with separate keys and arguments, cached `EVALSHA` execution,
  coordinated `NOSCRIPT` recovery, isolated deadlines, and source-bearing pipeline commands.
- Cluster script routing with cross-slot validation and fresh `ASKING` for every recovery attempt.
- Live script-cache recovery and owner-checked lease release/extension coverage.

- Explicit command delivery status for server, protocol, transport, cancellation, and operation-timeout
  failures.
- Isolated per-operation deadline methods for standalone and cluster generic commands and pipelines;
  late replies remain in the bounded FIFO and are drained without invalidating healthy connections.
- Configurable response-drain timeout terminates a connection when a reply retained after an
  isolated deadline never arrives, settling all pending work without reassigning FIFO reply slots.

## [1.0.0] - 2026-09-04

First stable release of ValkeyDotNet.

### Added

- Dependency-free RESP2 and RESP3 client implemented entirely with the .NET base class library.
- Binary-safe generic command API and typed helpers for common string, counter, and hash operations.
- TCP and TLS connections, ACL authentication, client naming, and logical database selection.
- Command pipelining and concurrent single-socket multiplexing with FIFO reply matching.
- Continuous RESP3 push-frame delivery outside subscription mode.
- Primary-routed Valkey Cluster support using `CLUSTER SHARDS` with `CLUSTER SLOTS` fallback.
- CRC16 hash-slot and hash-tag routing, endpoint mapping, bounded `MOVED` and `ASK` handling, and
  slot-grouped cluster pipelines.
- Configurable bounded connections per cluster node for response head-of-line isolation.

### Reliability and security

- Response byte, decoded-element, nesting-depth, and pending-request limits.
- Connection invalidation after ambiguous cancellation, transport failure, or protocol failure.
- Rejection of commands that would silently change connection state or reply framing.
- Platform TLS certificate validation by default and credential-safe exception behavior.

### Compatibility

- Verified against Valkey 9.1, 8.1, and 7.2.
- Includes deterministic protocol, transport, cancellation, concurrency, TLS, and cluster tests plus
  disposable live standalone and three-primary cluster suites.
