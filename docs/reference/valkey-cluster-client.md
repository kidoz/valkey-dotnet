# `ValkeyClusterClient`

For key-routed Lua scripts, see the [script API reference](scripts.md). Script methods validate that
all keys share a slot and recover the script cache on the selected physical connection.

An asynchronous key-routed client for Valkey Cluster. Implements `IAsyncDisposable`.

Namespace: `ValkeyDotNet`.

## Lifecycle

```csharp
public static Task<ValkeyClusterClient> ConnectAsync(
    ValkeyClusterOptions? options = null,
    CancellationToken cancellationToken = default)
```

Tries seed nodes in order. The first seed that completes the `HELLO` handshake and returns a valid,
complete `CLUSTER SHARDS` response supplies the initial 16,384-slot primary map. If the server
rejects `CLUSTER SHARDS`, discovery falls back to `CLUSTER SLOTS`. The connected seed is retained;
connections to other primaries are opened lazily and reused.

SHARDS discovery skips primaries marked `fail` or `loading`, so a failed former master is not
selected ahead of its promoted online replacement. Missing health is accepted for compatibility;
unknown health is rejected. Initial discovery and explicit refresh reject a map with no available
primary for a shard rather than installing it. Only opt-in
[sharded subscription recovery](cluster-subscriber.md#established-subscription-recovery) retries
unavailable-primary discovery within its existing recovery budget; command writes are not replayed.

```csharp
public ValueTask DisposeAsync()
```

Idempotent. Closes every node connection opened by the cluster client.

## Options

`ValkeyClusterOptions` has five members:

| Member | Default | Meaning |
|---|---|---|
| `SeedNodes` | one default `ValkeyClientOptions` | Nodes tried during discovery. The successful seed's TLS, authentication, protocol, database, connect and response-drain timeouts, pending-request limit, and parser settings are copied to discovered node connections. |
| `MaxRedirects` | `5` | Maximum `MOVED` or `ASK` redirects followed for one command. Valid range: 0–16. |
| `MaxNodeConnections` | `256` | Maximum retained node connections, bounding file-descriptor and memory growth as endpoints change. Valid range: 1–16,384. |
| `ConnectionsPerNode` | `1` | Multiplexed connections opened per active node. Values above one reduce response head-of-line blocking between independent workloads. Valid range: 1–16 and no greater than `MaxNodeConnections`. |
| `EndpointMapper` | `null` | Optional translation from an announced host and port to a reachable or TLS-valid endpoint. The returned endpoint is validated before use. |

At least one seed is required. Every seed is validated before networking begins.

## Routing

```csharp
public Task<RespValue> ExecuteAsync(
    ValkeyArgument routingKey,
    ValkeyCommand command,
    CancellationToken cancellationToken = default)

public Task<RespValue> ExecuteWithDeadlineAsync(
    ValkeyArgument routingKey,
    ValkeyCommand command,
    TimeSpan timeout,
    CancellationToken cancellationToken = default)
```

The routing key is hashed with CRC16/XMODEM and reduced to one of 16,384 slots. A non-empty substring
inside the first `{...}` pair is used as the hash tag. The caller must supply a routing key belonging
to the command and must keep every key in a multi-key command in that same slot. Server `CROSSSLOT`
errors are propagated as `ValkeyServerException`.

`MOVED` redirects update the reported slot assignment before retrying. `ASK` redirects do not update
the slot map; the client pipelines `ASKING` and the redirected command on the same node connection.
Redirect endpoints with an empty host reuse the current host, and IPv6 endpoints are split at the
last colon. A redirect reporting a slot different from the routing key is rejected. Malformed
redirects throw `ValkeyClusterException`.

`ExecuteWithDeadlineAsync` applies one deadline across lazy node connection, the initial command,
and all bounded redirects. A timeout after any command attempt reports `MayHaveBeenSent`; the
affected node connection drains a late reply. If the reply does not arrive within the seed's
`ResponseDrainTimeout`, the physical connection is retired and replaced for the next command; the
ambiguous timed-out command is not replayed.

```csharp
public Task RefreshTopologyAsync(CancellationToken cancellationToken = default)
```

Reloads and atomically replaces the complete slot map from the original seed endpoint.

## Cluster pipelines

```csharp
public Task<IReadOnlyList<RespValue>> ExecutePipelineAsync(
    IEnumerable<ValkeyClusterCommand> commands,
    CancellationToken cancellationToken = default)

public Task<IReadOnlyList<RespValue>> ExecutePipelineWithDeadlineAsync(
    IEnumerable<ValkeyClusterCommand> commands,
    TimeSpan timeout,
    CancellationToken cancellationToken = default)
```

Each `ValkeyClusterCommand` pairs a command with its routing key. The client groups commands by the
current primary, pipelines each node group, and executes independent groups concurrently. Replies
retain the caller's input order. Ordinary server errors remain in place; `MOVED` and `ASK` replies
are followed individually only after their initial node pipeline has been drained.
The deadline method shares one deadline across all node groups and redirect work.

## Convenience methods

The cluster client provides key-routed forms of `GET`, `SET`, `DEL` for one key, `INCRBY`, `HSET`, and
`HGET`, plus `PingAsync` against the seed connection. Their return types and expiry semantics match
the corresponding `ValkeyClient` methods.

## Scope

The current cluster layer routes commands to primaries. It does not yet provide replica reads,
cluster-wide scans, automatic command-key discovery, cross-slot command splitting, Sentinel
discovery, general-purpose connection pooling, or transport retries. A transport failure is never retried
automatically because the library cannot know whether a write reached the server.
