# Changelog

## [Unreleased]

### Added

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
