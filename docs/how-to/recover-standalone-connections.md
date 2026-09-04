# Recover a standalone connection

Use `ValkeyConnectionOwner` from the development version when a long-lived standalone client must
replace failed sockets. It is not included in the published 1.0.0 package yet.

Keep one owner for a workload and dispose it when that workload stops:

```csharp
await using var owner = new ValkeyConnectionOwner(new ValkeyConnectionOwnerOptions
{
    Connection = new ValkeyClientOptions
    {
        Host = "localhost",
        ClientName = "cache-worker",
        MaxPendingRequests = 256,
    },
    MaxConcurrentOperations = 512,
    MaxConnectAttempts = 3,
    InitialReconnectDelay = TimeSpan.FromMilliseconds(100),
    MaxReconnectDelay = TimeSpan.FromSeconds(2),
    MaxCommandRetries = 1,
});

// Optional: connect before accepting application traffic.
await owner.ConnectAsync(cancellationToken);

// This write is attempted once. A later operation can use a replacement connection.
await owner.ExecuteWithDeadlineAsync(
    new ValkeyCommand("SET", "cache:key", "value", "PX", 30000),
    TimeSpan.FromSeconds(2), cancellationToken);

// Explicitly authorize a retry for this read. The deadline covers recovery too.
var value = await owner.ExecuteRetryableWithDeadlineAsync(
    new ValkeyCommand("GET", "cache:key"),
    TimeSpan.FromSeconds(2), cancellationToken);
```

Use TLS on untrusted networks; see [Connect over TLS](connect-over-tls.md). Supply database and client
name through `Connection`, so replacement sockets repeat those settings. Do not depend on session
state established by separate generic commands surviving a reconnect.

Handle `ValkeyCapacityException` as local overload: no command was sent. Apply application admission
control or shed work instead of spinning on immediate retries. Inspect `owner.State` for lifecycle
health; `Faulted` requires correcting configuration and constructing a new owner.

Keep mutating operations on the ordinary methods unless your application proves that replaying the
exact operation is safe. In particular, do not mark `SET … NX PX`, release, or lease extension
retryable merely because the operation is atomic. A transport error or timeout with
`MayHaveBeenSent` leaves the result unknown. See [Handle errors](handle-errors.md).

Use separate owners for blocking and latency-sensitive workloads. See the
[owner reference](../reference/connection-owner.md) for bounds, deadlines, and supported operations.
