# Send any command

The convenience methods cover a handful of common operations. Everything else — every current Valkey
command and every future one — goes through `ExecuteAsync` without waiting for library support.

## Send a command

```csharp
var reply = await valkey.ExecuteAsync(new ValkeyCommand("EXPIRE", "session:42", 300));
Console.WriteLine(reply.AsInt64()); // 1 when the timeout was set
```

`ValkeyCommand` upper-cases the name and rejects one that is empty or is not printable ASCII.
Arguments convert implicitly from `string`, `byte[]`, `ReadOnlyMemory<byte>`, `int`, `long`, and
`double`.

## Read the reply by type

Match the accessor to what the command actually returns:

```csharp
// Integer reply
var count = (await valkey.ExecuteAsync(new ValkeyCommand("LLEN", "queue"))).AsInt64();

// Array reply
var members = await valkey.ExecuteAsync(new ValkeyCommand("SMEMBERS", "tags"));
foreach (var member in members.AsArray())
    Console.WriteLine(member.AsString());

// Map reply (RESP3)
var config = await valkey.ExecuteAsync(new ValkeyCommand("CONFIG", "GET", "maxmemory"));
foreach (var pair in config.AsMap())
    Console.WriteLine($"{pair.Key.AsString()} = {pair.Value.AsString()}");
```

A mismatched accessor throws `InvalidOperationException` naming the actual type. Check
[RESP values](../reference/resp-values.md) when unsure.

## Handle binary payloads

Keys and values are bytes, not text. Use `AsBytes()` and pass `byte[]` or `ReadOnlyMemory<byte>` to
keep arbitrary payloads intact:

```csharp
byte[] blob = Compress(payload);
await valkey.ExecuteAsync(new ValkeyCommand("SET", "doc:7", blob));

var stored = await valkey.ExecuteAsync(new ValkeyCommand("GET", "doc:7"));
byte[] roundTripped = stored.AsBytes().ToArray();
```

`AsString()` decodes as UTF-8 and will corrupt data that is not valid UTF-8.

## Run a transaction

```csharp
var replies = await valkey.ExecutePipelineAsync(
    [
        new ValkeyCommand("MULTI"),
        new ValkeyCommand("INCR", "counter"),
        new ValkeyCommand("EXPIRE", "counter", 60),
        new ValkeyCommand("EXEC"),
    ]
);

foreach (var reply in replies)
    reply.ThrowIfError();

var results = replies[^1].AsArray(); // EXEC returns the queued command results
```

The queued commands each reply `QUEUED`; `EXEC` returns an array with their real results.

## Run a script

```csharp
const string Script = """
    local current = redis.call('GET', KEYS[1])
    if current == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """;

var released = await valkey.ExecuteAsync(
    new ValkeyCommand("EVAL", Script, 1, "lock:job", lockToken)
);
```

Scripting is the way to make several operations atomic without relying on exclusive access to a
multiplexed connection.

## Commands the client refuses

Some commands redefine the connection itself. Sending them would leave the client waiting for a reply
that never comes, or reporting connection state that is no longer true. The client rejects them with
`ValkeyUnsupportedCommandException` **before writing anything**, so the connection stays usable:

```csharp
try
{
    await valkey.ExecuteAsync(new ValkeyCommand("SUBSCRIBE", "news"));
}
catch (ValkeyUnsupportedCommandException exception)
{
    // exception.Command is "SUBSCRIBE". Nothing was sent; valkey still works.
}
```

The list is `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `SSUBSCRIBE`, `SUNSUBSCRIBE`,
`MONITOR`, `RESET`, `HELLO`, and `CLIENT REPLY`. Other `CLIENT` subcommands — `CLIENT SETNAME`,
`CLIENT INFO`, `CLIENT LIST` — work normally. See [Exceptions](../reference/exceptions.md) for why
each one is on the list, and [Connection model](../explanation/connection-model.md) for the
underlying reason.

## Commands that work but cost you

Blocking commands (`BLPOP`, `BRPOP`, `XREAD BLOCK`) create response head-of-line blocking: later
commands can be written, but their replies cannot be delivered first. Use a dedicated client instance
if you need them.

## Related

- [`ValkeyClient`](../reference/valkey-client.md) — the full method surface.
- [Pipeline commands](pipeline-commands.md) — batching several commands.
