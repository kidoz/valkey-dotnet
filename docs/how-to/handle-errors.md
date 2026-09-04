# Handle errors

The decision that matters is whether the connection survives. Server errors leave it usable;
transport, protocol, and cancellation failures do not.

## Handle a server error and keep going

```csharp
try
{
    await valkey.IncrementAsync("session:42");
}
catch (ValkeyServerException exception) when (exception.ErrorCode == "WRONGTYPE")
{
    // The key holds a hash, not a counter. The connection is still fine.
    await valkey.DeleteAsync(["session:42"]);
    await valkey.IncrementAsync("session:42");
}
```

An error reply is a complete, well-formed frame. The client consumed all of it, so the next command
works normally. Filter on `ErrorCode` rather than matching message text.

## Reconnect after a connection failure

```csharp
async Task<ValkeyClient> EnsureConnectedAsync(ValkeyClient? current, ValkeyClientOptions options)
{
    if (current is not null)
        return current;
    return await ValkeyClient.ConnectAsync(options);
}

try
{
    value = await client.GetStringAsync(key);
}
catch (ValkeyConnectionException)
{
    await client.DisposeAsync();
    client = await ValkeyClient.ConnectAsync(options);
    value = await client.GetStringAsync(key);
}
```

The physical `ValkeyClient` does not reconnect itself. After `ValkeyConnectionException`, `ValkeyProtocolException`, or a
cancelled operation, the client is permanently invalidated — every subsequent call throws
`ObjectDisposedException`. Build a new one.

For managed replacement in the development version, use the
[standalone connection owner](recover-standalone-connections.md). Its ordinary methods do not replay
failed commands; explicitly retryable methods require operation-specific authorization.

**Only retry commands that are safe to repeat.** A `GET` is idempotent. An `INCR` is not: the first
attempt may have applied before the connection broke, so a blind retry can double-count.

## Use an isolated operation deadline

```csharp
try
{
    await valkey.ExecuteWithDeadlineAsync(new ValkeyCommand("GET", key), TimeSpan.FromSeconds(2));
}
catch (ValkeyCommandTimeoutException exception)
{
    // NotSent is safe to retry. MayHaveBeenSent requires operation-specific reasoning.
    Console.WriteLine(exception.DeliveryStatus);
}
```

The explicit deadline stops waiting for this operation without interrupting socket I/O. When it
expires after enqueue, the background reader still drains the late reply, so unrelated callers and
the connection remain usable. Do not blindly retry a mutation when `DeliveryStatus` is
`MayHaveBeenSent`.

Caller cancellation is intentionally stronger. Cancelling before enqueue sends nothing. Cancelling
after enqueue throws `ValkeyCommandCanceledException`, reports `MayHaveBeenSent`, and invalidates the
connection because the stream may sit between protocol frames. Prefer the explicit deadline method
for an isolated deadline and a server-side bound where one exists. Set `ResponseDrainTimeout` on
`ValkeyClientOptions` to limit how long an absent late reply may retain FIFO capacity; expiry of that
drain timeout terminates the stalled connection and faults its remaining callers.

## Check pipeline replies

`ExecutePipelineAsync` returns errors in place instead of throwing, so an unchecked result hides
failures:

```csharp
var replies = await valkey.ExecutePipelineAsync(commands);
foreach (var reply in replies)
    reply.ThrowIfError();
```

See [Pipeline commands](pipeline-commands.md) for per-reply handling.

## Catch library failures

```csharp
catch (ValkeyCommandTimeoutException exception)
{
    logger.LogWarning(exception, "Valkey operation deadline elapsed");
}
catch (ValkeyCommandCanceledException exception)
{
    logger.LogWarning(exception, "Valkey operation was cancelled after enqueue");
}
catch (ValkeyException exception)
{
    logger.LogError(exception, "Valkey operation failed");
}
```

Never log the exception alongside `ValkeyClientOptions`. `Password` and `Username` must not reach
logs; log `Host`, `Port`, and `ErrorCode` instead.

## Related

- [Exceptions](../reference/exceptions.md) — the full hierarchy and which failures invalidate.
- [Connection model](../explanation/connection-model.md) — why invalidation is the safe default.
