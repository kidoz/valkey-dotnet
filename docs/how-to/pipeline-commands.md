# Pipeline commands

Pipelining writes one contiguous batch without waiting for individual replies, collapsing *n* round
trips into one. Use it when you have several independent commands and round-trip latency dominates.

## Send a batch

```csharp
var replies = await valkey.ExecutePipelineAsync(
    [
        new ValkeyCommand("SET", "a", "1"),
        new ValkeyCommand("INCR", "a"),
        new ValkeyCommand("GET", "a"),
    ]
);

foreach (var reply in replies)
    reply.ThrowIfError();

Console.WriteLine(replies[2].AsString()); // 2
```

Replies come back in request order, so `replies[i]` always answers `commands[i]`.

## Check every reply

`ExecutePipelineAsync` returns error replies **in place** instead of throwing. This is deliberate: the
client must drain every reply to keep the connection synchronized, so it cannot abandon the batch at
the first error. The cost is that ignoring the result silently ignores failures.

Fail on the first error:

```csharp
foreach (var reply in replies)
    reply.ThrowIfError();
```

Or handle them individually:

```csharp
for (var i = 0; i < replies.Count; i++)
{
    if (replies[i].Type is RespType.SimpleError or RespType.BlobError)
    {
        var failure = replies[i].ToServerException();
        logger.LogWarning("Command {Index} failed: {Code}", i, failure.ErrorCode);
        continue;
    }

    Process(replies[i]);
}
```

## Build a batch dynamically

```csharp
var commands = keys.Select(key => new ValkeyCommand("GET", key)).ToArray();
var replies = await valkey.ExecutePipelineAsync(commands);

var values = replies
    .Select(reply => reply.IsNull ? null : reply.AsString())
    .ToArray();
```

An empty sequence returns an empty list without touching the socket. A `null` element throws
`ArgumentException` before anything is written.

## Size the batch

Every reply in a batch is materialized before the call returns, so a batch of *n* commands holds *n*
replies in memory at once. Each individual reply is bounded by `MaxResponseBytes`, and the command
count is bounded by `MaxPendingRequests`. Chunk large workloads below both limits:

```csharp
foreach (var chunk in keys.Chunk(1_000))
{
    var replies = await valkey.ExecutePipelineAsync(
        chunk.Select(key => new ValkeyCommand("GET", key))
    );
    // consume replies before the next chunk
}
```

## What pipelining is not

- **Not a transaction.** Commands are not atomic and other clients interleave. For atomicity use
  `MULTI`/`EXEC` through [`ExecuteAsync`](send-any-command.md), or a Lua script.
- **Not for dependent commands.** The whole batch is submitted before any reply is returned to the
  caller, so a command cannot use an earlier result. Await them separately, or move the logic
  server-side into a script.
- **Not required for concurrency.** Ordinary calls are already multiplexed. Pipelining keeps a batch
  contiguous and reduces writes and flushes; it does not make the commands atomic.

## Failure behaviour

A transport failure or protocol violation mid-batch invalidates the connection and throws — you get
no partial result. Commands already accepted by the server may still have applied, so a pipeline of
writes is not all-or-nothing. Design for it, or use a transaction.

## Related

- [Performance baseline](../reference/performance-baseline.md) — measured protocol costs.
- [Connection model](../explanation/connection-model.md) — why replies must be drained in order.
