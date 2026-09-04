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

There is no automatic reconnect. After `ValkeyConnectionException`, `ValkeyProtocolException`, or a
cancelled operation, the client is permanently invalidated — every subsequent call throws
`ObjectDisposedException`. Build a new one.

**Only retry commands that are safe to repeat.** A `GET` is idempotent. An `INCR` is not: the first
attempt may have applied before the connection broke, so a blind retry can double-count.

## Distinguish your cancellation from a timeout

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

try
{
    await valkey.ExecuteAsync(new ValkeyCommand("GET", key), cts.Token);
}
catch (OperationCanceledException) when (cts.IsCancellationRequested)
{
    // Your deadline. The connection is now unusable — dispose and reconnect.
}
```

`TimeoutException` means `ConnectTimeout` elapsed during `ConnectAsync`. `OperationCanceledException`
means your token fired. Both leave nothing to reuse.

Cancelling mid-command is expensive by design: the stream may sit between protocol frames, so the
client cannot know where the next reply starts. Prefer a server-side bound where one exists rather
than cancelling the client.

## Check pipeline replies

`ExecutePipelineAsync` returns errors in place instead of throwing, so an unchecked result hides
failures:

```csharp
var replies = await valkey.ExecutePipelineAsync(commands);
foreach (var reply in replies)
    reply.ThrowIfError();
```

See [Pipeline commands](pipeline-commands.md) for per-reply handling.

## Catch everything from this library

```csharp
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
