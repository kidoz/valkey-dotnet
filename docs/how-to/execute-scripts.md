# Execute reusable scripts

Create one reusable script and pass each operation's keys and data separately:

```csharp
var release = new ValkeyScript("""
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """);

var reply = await client.ExecuteScriptWithDeadlineAsync(
    release,
    keys: [lockKey],
    arguments: [ownerToken],
    timeout: TimeSpan.FromSeconds(2),
    cancellationToken: cancellationToken);

bool released = reply.AsInt64() == 1;
```

Keep owner tokens and other application values in `ARGV`. Never interpolate untrusted values into
Lua source. Use a cryptographically random owner token for each acquisition. The same script API
works on a cluster client; place all keys for a multi-key script in one hash slot with a shared hash tag.

For a pipeline, build source-bearing commands so each entry is independent of cache state:

```csharp
var replies = await client.ExecutePipelineAsync(
    [release.CreateCommand([firstLockKey], [firstOwner]),
     release.CreateCommand([secondLockKey], [secondOwner])],
    cancellationToken);
foreach (var result in replies)
    result.ThrowIfError();
```

A timeout or transport failure may occur after the script has changed data. Inspect the delivery
status and apply an operation-specific policy before retrying. A Valkey lease provides coordination;
lease expiry, failover, or a paused owner can permit overlapping work. Correctness for external
resources requires fencing or another authoritative concurrency check.

See [script reference](../reference/scripts.md) for cache recovery, concurrency, and deadline semantics.
