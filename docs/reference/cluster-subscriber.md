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
| `MaxReconnectAttempts` | 3 | 1–100 attempts per same-endpoint recovery cycle. |
| `InitialReconnectDelay` / `MaxReconnectDelay` | 100 ms / 2 seconds | Bounded equal-jitter exponential backoff. |
| `RecoveryTimeout` | 30 seconds | Total same-endpoint recovery budget, including restoration acknowledgements. |

Discovery uses at most `Cluster.MaxNodeConnections` ordinary connections; those are separate from
the dedicated subscription limit. Lifecycle writes are serialized, while socket readers and
consumer streams operate independently. No extra message-forwarding worker or application callback
runs on a socket reader. A single channel can also share local handles within a node-level
`ValkeySubscriber`; only its last handle sends SUNSUBSCRIBE.

## Initial routing and topology changes

An initial `MOVED` rejection closes the attempted subscriber, refreshes topology through the existing
validated discovery parser, and retries the channel's current primary. The initial attempt plus at
most `Cluster.MaxRedirects` refresh/retry cycles share one operation deadline. Endpoint text from a
subscriber rejection is discarded, not followed. `EndpointMapper` remains the explicit trust boundary
for discovered addresses; mapped connections preserve TLS, certificate validation, ACL, protocol,
client-name, and parser settings from the successful seed.

Initial `ASK` fails with `ValkeyClusterException`; the client does not issue ASKING or transparently
subscribe during an importing-slot transition. `RefreshTopologyAsync` changes routing for future
subscriptions only; it does not move, duplicate, or recreate existing streams.

An unsolicited, well-formed SUNSUBSCRIBE for a confirmed channel terminates that node subscriber
with `ValkeyClusterException`. Malformed acknowledgements or deliveries terminate it with
`ValkeyProtocolException`. A MOVED/ASK rejection during opt-in restoration is terminal and preserves
only its error category, never arbitrary server text. Automatic relocation on slot migration or
primary failover is **not implemented**. Consumers dispose the failed handle, refresh topology, and
explicitly subscribe again. Missed messages are not replayed; this API provides no durable delivery,
cache-state reconstruction, or exactly-once guarantee.

## Handle lifecycle

`ReadAllAsync` exposes the bounded binary stream; multiple enumerators compete, and cancelling
enumeration does not unsubscribe. `DroppedMessages`, `IsConnected`, `IsReconnecting`,
`SuccessfulReconnects`, `Failure`, and `Completion` describe the underlying subscriber lifetime.
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

Deterministic cases cover RESP2/RESP3 binary and fragmented delivery, separate modes, duplicate
handles, endpoint mapping, initial MOVED refresh, bounded/sanitized redirect failures, unsolicited
unsubscription, same-endpoint restoration, queue overflow, parser bounds, admission cancellation,
deadlines, and disposal. These tests do not establish live cluster compatibility or topology migration.

The live `ShardedPubSubRoutesAcrossThreePrimariesWithIndependentDuplicateHandles` cases are gated by
`VALKEYDOTNET_CLUSTER_ENDPOINTS` and optional `VALKEYDOTNET_CLUSTER_MAPPED_HOST`. They exercise
RESP2/RESP3 channels covering three primary slot ranges, SPUBLISH, binary payloads, independent
duplicate handles, and final unsubscription. They have not been executed as part of this change.
Live failover, slot migration, TLS recovery, prolonged soak, and performance evidence remain separate work.
