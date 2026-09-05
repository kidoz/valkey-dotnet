# Changelog

## [Unreleased]

### Fixed

- Cluster discovery skips failed/loading former primaries when a promoted primary is available.
  Opt-in subscription recovery waits within its existing budget while no primary is available;
  ordinary command transport failures are still not replayed.

- Separate subscriber acknowledgement deadlines/caller cancellation from reader shutdown. Pending
  completions report terminal loss or disposal; socket disposal still interrupts outstanding I/O.
  Add cross-mode disposal, cancellation, and deadline regression coverage.
- Preserve completed subscriber acknowledgements and sanitized server rejections when an immediate
  remote close races with the lifecycle caller. Confirmed handles retain buffered messages; closing
  before acknowledgement still fails the operation.

### Added

- Opt-in atomic migration rollback runner and manual workflow: close one verified export link after
  snapshot import, verify failed jobs and provisional-key cleanup, and preserve source data/sharded
  streams. Local-only migration debug is confined to the new owned rollback fixture.

- Opt-in pre-transfer atomic migration cancellation runner and manual workflow, checking active-to-cancelled
  job identity, unchanged source keys/expiration/slot maps, and uninterrupted RESP2/RESP3 sharded streams.

- Opt-in atomic slot-migration runner and manual workflow checking correlated EXPORT/IMPORT job
  completion, binary keys and absolute expiration, and same-stream sharded relocation in RESP2/RESP3.

- Opt-in nonempty-key migration runner and manual workflow verifying binary MIGRATE transfers,
  expiration metadata, intermediate ASK/TRYAGAIN behavior, and same-stream sharded cutover.

- Opt-in native ASK-migration runner and manual workflow covering repeated command ASK/ASKING,
  binary values, source-local sharded subscriptions during migration, and same-stream cutover.

- Opt-in four-node primary-failover runner covering RESP2/RESP3 stream recovery with healthy and
  unavailable discovery seeds, replica-readiness/membership checks, and a manual Primary failover workflow.
- Opt-in owned three-primary slot-migration runner, bounded RESP2/RESP3 same-stream recovery checks,
  random loopback ports, ownership-verified cleanup, and a manual Slot migration workflow.
  Live execution remains explicitly opt-in.
- Opt-in established sharded subscription topology recovery with the same handle and bounded queue,
  serialized temporary discovery, bounded known-primary fallback, restoration MOVED/ASK handling,
  and loss/attempt/relocation counters. Add deterministic lifecycle, TLS, and resource-bound coverage;
  live empty-slot migration and single-primary failover passed on Valkey 9.1.2.
- Bounded initial sharded subscription ASK handling with slot/endpoint validation, same-socket
  ASKING before SSUBSCRIBE, seed TLS/ACL preservation, and unchanged slot ownership. Same-endpoint
  recovery repeats ASKING; live ASK migration evidence remains pending.
- RESP2/RESP3 sharded subscriber mode and a bounded cluster subscriber with dedicated slot-routed
  streams, endpoint mapping, initial MOVED-triggered topology refresh, and explicit topology-loss
  failures by default, with optional topology recovery.
- Sharded lifecycle regressions and gated three-primary live SPUBLISH/SSUBSCRIBE integration cases.
- Standalone RESP3 tracking client with binary invalidations, NOLOOP, BCAST/PREFIX, tracking
  re-enablement on replacement, and bounded async delivery with invalidate-all on overflow or loss.
- Tracking lifecycle regressions and gated live invalidation/recovery cases; no local cache or
  cluster-tracking consistency claim is included.
- Opt-in subscriber recovery with bounded equal-jitter backoff, a total recovery deadline, restored
  channel/pattern streams, local unsubscribe during restoration, and loss/attempt/success counters.
- Subscriber recovery regressions covering ambiguous changes, restoration races, repeated losses,
  TLS/session settings, parsing bounds, and disposal; an explicitly gated live connection-kill test.
- Dedicated RESP2/RESP3 subscriber with binary channel/pattern streams, independent local handles,
  bounded drop-incoming queues and drop counters, bounded acknowledgement lifecycle, and terminal
  cancellation/disposal semantics. Sharded subscriptions use a separate mode; RESP3 tracking uses
  its own command-client API.
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
