# Sharded Pub/Sub

`ValkeyClusterSubscriber` discovers cluster primaries and routes each binary shard channel using
the same CRC16/hash-tag slot calculation as command keys. Every returned `ValkeyShardedSubscription`
owns a separate dedicated subscriber socket and bounded async stream. Discovery and publishing
connections never enter subscription mode.

## Routing and delivery modes

| API | Wire commands | Routing |
|---|---|---|
| `ValkeySubscriber` default mode | SUBSCRIBE/PSUBSCRIBE, UNSUBSCRIBE/PUNSUBSCRIBE | Caller-selected endpoint; global channels and patterns. |
| `ValkeySubscriber` with `UseShardedPubSub=true` | SSUBSCRIBE/SUNSUBSCRIBE | Caller-selected endpoint; no automatic cluster discovery. Only `SubscribeShardedAsync` is accepted. |
| `ValkeyClusterSubscriber.SubscribeAsync` | SSUBSCRIBE/SUNSUBSCRIBE | Discovered slot primary; independent connection per handle, including duplicate channel names. |
| `ValkeyClusterClient.ExecuteAsync(channel, new ValkeyCommand("SPUBLISH", channel, payload), token)` | SPUBLISH | Existing bounded command routing and redirect handling. |

Sharded and global Pub/Sub use separate channel namespaces. There are no sharded patterns. Global
and sharded subscriptions cannot be mixed on one `ValkeySubscriber`. `ValkeyPubSubMessage.IsSharded`
identifies `smessage`; its `Pattern` is null. Channels and payloads remain binary-safe.

The server forwards sharded publications within the owning shard, unlike global publications.
See the official [SSUBSCRIBE command](https://valkey.io/commands/ssubscribe/) and
[sharded Pub/Sub specification](https://valkey.io/topics/pubsub/#sharded-pubsub).

## Cluster subscriber options

| Option | Default | Bound or behavior |
|---|---|---|
| `Cluster` | New `ValkeyClusterOptions` | SHARDS/SLOTS discovery, endpoint mapping, and seed transport/security/parser settings. Database must be zero. |
| `MaxSubscriptions` | 256 | 1–16,384 retained handles/dedicated sockets. Terminal handles retain capacity until disposed. |
| `MaxConcurrentOperations` | 64 | 1–1,048,576 admitted lifecycle operations; excess calls fail with `ValkeyCapacityException`. |
| `QueueCapacity` | 128 | 1–1,048,576 buffered messages per handle. Incoming overflow is dropped and counted. |
| `MaxChannelBytes` | 16 KiB | 1 byte–1 MiB configured ceiling; empty channel names remain valid. |
| `OperationTimeout` | 10 seconds | Total admission, connect, topology-refresh, and acknowledgement budget. |
| `EnableReconnect` | false | Opt-in restoration on the same endpoint after transport loss. |
| `EnableTopologyRecovery` | false | Refreshes routing after transport loss or a valid server shard removal; implies reconnect. Preserves the handle and queue. |
| `MaxTopologyRefreshEndpoints` | 4 | 1–32 distinct discovery endpoints per refresh: original seed, then known primaries. |
| `MaxReconnectAttempts` | 3 | 1–100 attempts per recovery cycle. Each attempt allows at most `Cluster.MaxRedirects` restoration redirects. |
| `InitialReconnectDelay` / `MaxReconnectDelay` | 100 ms / 2 seconds | Bounded equal-jitter exponential backoff. |
| `RecoveryTimeout` | 30 seconds | Total recovery budget, including backoff, discovery admission, connections, redirects, and restoration acknowledgements. |

Retained discovery uses at most `Cluster.MaxNodeConnections` ordinary connections. With topology
recovery enabled, refresh uses at most **one additional temporary discovery socket** per cluster
subscriber, serialized separately from lifecycle operations. These are separate from the dedicated
subscription limit. Lifecycle writes are serialized, while socket readers and
consumer streams operate independently. No extra message-forwarding worker or application callback
runs on a socket reader. A single channel can also share local handles within a node-level
`ValkeySubscriber`; only its last handle sends SUNSUBSCRIBE.

## Initial routing and topology changes

An initial `MOVED` rejection closes the attempted subscriber, refreshes topology through the existing
validated discovery parser, and retries the channel's current primary. The initial attempt plus at
most `Cluster.MaxRedirects` redirects (MOVED and ASK combined) share one operation deadline.
Endpoint text from a MOVED rejection is discarded, not followed.

Initial `ASK` closes the rejected socket, validates the reported slot against the binary channel,
and opens a dedicated connection to the mapped redirect endpoint. After HELLO/session setup, it
sends `ASKING`, requires a simple-string `OK`, and sends SSUBSCRIBE next on that same socket.
There is no intervening application command or shared command connection. ASK does not update slot
ownership; later subscriptions still begin at the known primary. This follows the
[ASKING contract](https://valkey.io/commands/asking/) and
[cluster redirect semantics](https://valkey.io/topics/cluster-spec/#ask-redirection).

ASK redirect text is limited to 1,024 ASCII bytes in addition to the normal RESP limits. Invalid
slots, ports, host syntax, control bytes, and extra fields fail with sanitized cluster errors.
DNS names, IPv4, bracketed/unbracketed IPv6, and empty-host redirects are supported; an empty host
uses the source connection's host. `EndpointMapper` applies before connecting. The mapper and
server-announced addresses must remain within the cluster's trusted network boundary: redirected
connections reuse seed TLS, certificate validation, ACL, protocol, client-name, and parser settings.
Raw redirect text is not included in public exception messages or inner exceptions.

When opted-in same-endpoint recovery replaces an ASK-established socket, it repeats ASKING before
restoring the one shard registration. Without topology recovery, a new redirect during restoration
remains terminal. The node-level `ValkeySubscriber` alone does not follow ASK.
`RefreshTopologyAsync` changes routing for future subscriptions only; it does not move, duplicate,
or recreate existing streams.

## Established subscription recovery

Set `EnableTopologyRecovery = true` to keep the same handle, enumerator, bounded queue, and
`Completion` task across recoverable topology changes. A transport loss or a well-formed unsolicited
SUNSUBSCRIBE for the confirmed channel retires the old socket and starts the existing subscriber's
single recovery loop. It refreshes the slot map and restores SSUBSCRIBE at the current primary.
Restoration MOVED responses trigger another validated refresh; ASK uses the dedicated ASKING
sequence above without changing ownership. No message-forwarding queue or second reader is added.

Each refresh tries the original seed followed by distinct known primaries, capped by
`MaxTopologyRefreshEndpoints`. Each candidate has the seed's `ConnectTimeout` budget covering both
connection and discovery, within the total recovery deadline. Transport failure or candidate timeout
can try the next node; authentication, permission, TLS validation, invalid topology, and malformed
RESP failures are terminal. Temporary discovery connections are disposed before trying another
candidate. All discovered connections retain seed security and parser settings. A cluster with no
reachable known primary cannot recover through this policy; it does not discover unknown replicas
or provide Sentinel discovery.

Without this option, unsolicited shard removal remains a terminal `ValkeyClusterException`.
Malformed acknowledgements and deliveries remain terminal `ValkeyProtocolException` in either mode.
Exhausted recovery/redirect budgets complete the stream with a terminal failure; the handle must
still be disposed to release capacity. During recovery, unsubscribe removes local intent and closes
the handle without waiting for the server; disposal cancels and joins discovery/restoration.

This is reactive recovery, not proactive topology polling or silent-partition detection. Missed
messages are not replayed or counted; this API provides no durable delivery, cache-state
reconstruction, or exactly-once guarantee. Consumers must reconcile application state after loss.

## Handle lifecycle

`ReadAllAsync` exposes the bounded binary stream; multiple enumerators compete, and cancelling
enumeration does not unsubscribe. `DroppedMessages`, `IsConnected`, `IsReconnecting`,
`ConnectionLosses`, `ReconnectAttempts`, `SuccessfulReconnects`, `SuccessfulRelocations`, `Failure`,
and `Completion` describe the underlying subscriber lifetime. Losses include valid server shard
removal; successful relocations count recoveries to a different mapped host or port. Attempts count
outer recovery attempts, not individual redirect sockets. Local overflow alone increments drops.
State is a snapshot, not a silent-partition detector. Polling counters are not Meter instruments.

`UnsubscribeAsync` sends SUNSUBSCRIBE and then closes the handle's dedicated socket. Once admitted,
failure or cancellation also closes that handle. Cancellation before admission leaves it active.
Operation deadline expiry is typed as `ValkeyCommandTimeoutException`; a written lifecycle command
is delivery-ambiguous. Caller cancellation after writing retains `ValkeyCommandCanceledException`.

`DisposeAsync` on a handle closes its socket directly without waiting for an acknowledgement or
lifecycle admission, and releases capacity only after the physical reader/recovery stops. Disposing
the cluster subscriber cancels acquisition, closes every retained stream, and disposes discovery
connections. Duplicate channel handles on separate sockets remain independent.

## Verification scope

An opt-in [isolated slot-migration runner](../how-to/run-slot-migration-tests.md) now exercises
real legacy empty-slot moves while retaining established handles. On 2026-09-05, both RESP2/RESP3
cases passed on Valkey 9.1.2: three moves per protocol, six successful relocations, zero local queue
drops, and verified cleanup of the two fresh clusters. Binary delivery, server registration
ownership, unchanged streams, and unrelated-channel isolation passed. This is legacy empty-slot
migration evidence, not primary-failover, forced-ASK, or soak evidence. See the
[execution record and limits](resilience-evidence.md#slot-migration-runner).

The topology-recovery increment adds 32 scripted loopback cases covering RESP2/RESP3 same-stream
relocation after transport loss or server shard removal, restoration MOVED/ASK, redirect limits,
invalid/denied replies, seed fallback and endpoint caps, discovery timeout, disposal/unsubscribe,
TLS/ACL/client-name/parser-bound preservation, and three relocation cycles with one retained queue.
These tests do not establish live migration/failover compatibility, long-duration resource stability,
or throughput/allocation performance. Maintainer security review of recovery endpoint handling is
required before release.

On 2026-09-05, `just ci` and the full Release suite passed all 435 cases with no failures or skips;
Debug/Release builds and formatting were clean. Five additional Release runs of all 77 cluster
subscriber cases also passed. Release/repeat results are under
`artifacts/resilience/sharded-relocation/`. No live topology experiment was run for this increment.

The initial ASK increment passed all 403 unit cases locally on 2026-09-05, including 33 new cases
for RESP2/RESP3 binary routing, unchanged slot ownership, endpoint mapping, bounded redirect loops,
ASK-to-MOVED transitions, malformed/oversized redirects, ASKING rejection, timeout/cancellation/
disposal, TLS/ACL preservation, and repeated ASKING on same-endpoint recovery. These are scripted
loopback and parser tests; no live ASK/slot-migration experiment was run for this increment.
The full Release suite also passed all 403 cases, and five repeated runs of the 45 cluster-subscriber
cases passed without failures or skips. Formatting and Debug/Release builds were clean. Results
are recorded locally under `artifacts/resilience/sharded-ask/`. Redirect parsing and endpoint trust
handling require maintainer security review before release.

Deterministic cases cover RESP2/RESP3 binary and fragmented delivery, separate modes, duplicate
handles, endpoint mapping, initial MOVED refresh, bounded/sanitized redirect failures, unsolicited
unsubscription, same-endpoint restoration, queue overflow, parser bounds, admission cancellation,
deadlines, and disposal. These tests do not establish live cluster compatibility or topology migration.

The live `ShardedPubSubRoutesAcrossThreePrimariesWithIndependentDuplicateHandles` cases are gated by
`VALKEYDOTNET_CLUSTER_ENDPOINTS` and optional `VALKEYDOTNET_CLUSTER_MAPPED_HOST`. They exercise
RESP2/RESP3 channels covering three primary slot ranges, SPUBLISH, binary payloads, independent
duplicate handles, and final unsubscription. Both cases passed locally on 2026-09-05 against a fresh
three-primary Valkey 9.1.2 cluster, with no failures or skips. Announced node hostnames were mapped
to localhost-published ports. The host was macOS arm64 with SDK 10.0.400 and runtime .NET 10.0.11;
servers ran Linux aarch64 without TLS. Each node had a 128 MiB memory limit and one CPU budget.
All 16,384 slots were healthy before testing. Afterward every node had zero keys, no shard channels,
and no remaining application client connections. All three owned containers and their network were
removed after ownership-label checks; existing containers were untouched. The local result is
`artifacts/resilience/sharded-mto9sn1m/sharded.trx`.

This live success is separate from overall unit-suite readiness: immediate-close acknowledgement
and rejection regressions failed intermittently during that verification session. A subsequent
shutdown-isolation change passed the expanded unit suite and repeated runs. See
[subscriber verification evidence](subscriber.md#verification-evidence).
Live failover, nonempty/atomic slot migration, forced ASK, TLS recovery, prolonged soak, and performance
evidence remain separate work.
