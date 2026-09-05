# RESP3 client tracking

`ValkeyTrackingClient` owns a standalone RESP3 command connection and a bounded invalidation stream.
It composes the [connection owner](connection-owner.md); it does not store application cache entries.
An ordinary `ValkeyClient`, cluster client, or subscriber is unchanged by its creation.

## Configuration

The constructor accepts `ValkeyConnectionOwnerOptions? connectionOptions` and
`ValkeyTrackingOptions? trackingOptions`. Construction validates settings and copies prefix bytes;
the first `ConnectAsync` or command opens the socket.

| Tracking option | Default | Contract |
|---|---|---|
| `NoLoop` | `false` | `NOLOOP` suppresses notifications caused by this connection's own writes. |
| `Broadcast` | `false` | `BCAST` receives matching-key invalidations without first reading those keys. |
| `Prefixes` | Empty | Binary `PREFIX` values; require broadcast mode. Empty list broadcasts all keys. No duplicates or overlapping prefixes. Maximum 256 prefixes, 1 MiB total bytes. |
| `QueueCapacity` | 256 | Buffered batches, from 1 to 1,048,576. Overflow produces an invalidate-all notification. |

Both configured and negotiated protocol must be RESP3. Initialization performs HELLO, optional
SELECT, and `CLIENT TRACKING ON` with the configured flags before any application command is admitted.
Every replacement repeats this initialization with the same TLS, ACL, database, client name, and
parser/pending-request bounds. Tracking setup errors are sanitized; authentication error codes
`WRONGPASS`, `NOAUTH`, and permission code `NOPERM` are retained, with other setup errors classified
as `ERR`. A rejected setup or negotiated downgrade faults connection acquisition without retries.

## Commands and recovery

`ConnectAsync`, `ExecuteAsync`, `ExecuteWithDeadlineAsync`, `ExecuteRetryableAsync`,
`ExecuteRetryableWithDeadlineAsync`, `ExecutePipelineAsync`, `ExecutePipelineWithDeadlineAsync`,
`ExecuteScriptAsync`, and `ExecuteScriptWithDeadlineAsync` use the owned
tracked connection. Pipeline errors remain positional. Deadlines detach the caller while late
replies drain. Only the `ExecuteRetryable*` methods authorize bounded replay; other methods never replay a
transport failure.

Recovery is **on demand**, not a background heartbeat. Detected connection loss emits invalidate-all
immediately; the next command or `ConnectAsync` acquires a replacement. Default tracking forgets the
old server-side read set, so applications repopulate entries through reads on the new connection.
Broadcast prefixes are registered again automatically. Messages missed while disconnected are not
replayed. `State` is a lifecycle snapshot, not proof that a silent network partition has been detected.

Raw `CLIENT TRACKING`, `CLIENT CACHING`, `AUTH`, and `SELECT` are rejected before application command
bytes are written, including in pipelines. Existing connection-state command restrictions still
apply. There is no runtime tracking reconfiguration, RESP2 redirect mode, OPTIN/OPTOUT mode, or
cluster-wide tracking lifecycle in this API.

## Invalidation delivery

`ReadInvalidationsAsync` permits one active enumeration. Cancelling enumeration leaves tracking
active; another enumeration can subsequently drain retained notifications. Consumer code never runs
on the socket reader. Push frames do not consume ordinary FIFO replies.

`ValkeyInvalidation` exposes:

- `Keys`: binary key memory, including empty keys and invalid UTF-8;
- `InvalidateAll`: reset after server flush, connection loss, queue overflow, or disposal;
- `Version`: a client-local increasing notification version.

The decoded wire form is `push ["invalidate", array-of-blob-keys]`; a null key list means invalidate
all, while an empty array means no keys. Malformed invalidation frames terminate the physical
connection and settle pending commands. Other well-formed push kinds are ignored. All frames pass
through the existing byte, element, and nesting limits before delivery.

When full, the queue is cleared and replaced with one invalidate-all batch. Subsequent overflows
retain a reset, including when capacity is one. `QueueOverflows` counts these full-queue events;
it is a polling counter, not a Meter instrument. Memory is bounded by queue capacity multiplied by
the configured decoded-response bounds, plus active parsing and consumer-retained notifications.

`InvalidationVersion` advances before handing off each invalidation/reset. It can help consumers
detect changes during a cache fill, but it is not a cache consistency algorithm. Async command
continuations and invalidation consumers run independently: receive order alone does not serialize
application cache mutations. Applications own cache synchronization, stale-fill rejection, local
handling of writes under NOLOOP, maximum local TTLs, and liveness checks for silent connections.
Serving cached entries while disconnected or ignoring/resetting the stream can serve stale data.

`DisposeAsync` closes the owner, replaces remaining notifications with a final invalidate-all batch,
and completes the stream. Repeated disposal is safe. Enumeration drains that reset before ending.

## Verification scope

Deterministic tests cover setup and prefix snapshots, binary/fragmented pushes, FIFO pipeline errors,
null/empty invalidations, malformed frames, bounded overflow, replacement with TLS/ACL/session
settings, replacement parser bounds, isolated deadlines, cancellation, disposal, and reader ownership.
Live tests are gated by `VALKEYDOTNET_ENDPOINT`; the exact-ID connection-kill case additionally
requires `VALKEYDOTNET_RUN_TRACKING_RECOVERY_TESTS=1` and an isolated disposable endpoint.
The new live cases have not been executed as part of this change. Server-restart, live TLS, cluster
tracking, and prolonged resource-soak evidence are not established.

Wire semantics follow the official [CLIENT TRACKING command](https://valkey.io/commands/client-tracking/)
and [client-side caching specification](https://valkey.io/topics/client-side-caching/).
