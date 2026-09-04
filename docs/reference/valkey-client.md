# `ValkeyClient`

An asynchronous client for a single Valkey node. Implements `IAsyncDisposable`. Safe for concurrent
callers: commands are serialized and replies are returned in wire order.

Namespace: `ValkeyDotNet`.

## Lifecycle

```csharp
public static Task<ValkeyClient> ConnectAsync(
    ValkeyClientOptions? options = null,
    CancellationToken cancellationToken = default)
```

Opens the TCP connection, performs the TLS handshake when
[`UseTls`](client-options.md) is set, then sends one `HELLO` carrying protocol selection, ACL
authentication, and the client name. Issues `SELECT` when `Database` is non-zero. The whole sequence
is bounded by `ConnectTimeout`; exceeding it throws `TimeoutException`. Any failure disposes the
socket before propagating.

```csharp
public ValueTask DisposeAsync()
```

Idempotent. Closes the stream and the socket.

## Properties and events

| Member | Type | Meaning |
|---|---|---|
| `ServerInfo` | `RespValue` | The reply to the initial `HELLO`. `RespValue.Null()` until connected. |
| `NegotiatedProtocol` | `ValkeyProtocol` | The protocol the server reported in its `HELLO` reply, which may be lower than `Protocol` requested. A reply that reports no supported version fails the connect with `ValkeyProtocolException`. |
| `PushReceived` | `event Action<RespValue>?` | Raised when a RESP3 push frame is read while awaiting a command reply. |

`PushReceived` fires on the thread draining the reply. There is no idle background reader, so pushes
surface only while a command is in flight. Exceptions thrown by a handler are swallowed so a caller
bug cannot desynchronize the wire.

## Generic commands

```csharp
public Task<RespValue> ExecuteAsync(
    ValkeyCommand command,
    CancellationToken cancellationToken = default)
```

Sends one command and returns its reply. Throws `ValkeyServerException` when the server replies with
an error. Throws `ArgumentNullException` when `command` is null, and
`ValkeyUnsupportedCommandException` — before writing anything — for a command that would change the
connection state the client owns.

```csharp
public Task<IReadOnlyList<RespValue>> ExecutePipelineAsync(
    IEnumerable<ValkeyCommand> commands,
    CancellationToken cancellationToken = default)
```

Writes every command, flushes once, then reads one reply per command. Returns replies in request
order. **Error replies are returned in place rather than thrown** — call `ThrowIfError()` on each.
Returns an empty list for an empty sequence. Throws `ArgumentNullException` for a null sequence and
`ArgumentException` when any element is null. Every command is checked against the unsupported list
before the batch is written, so one `ValkeyUnsupportedCommandException` rejects the batch whole
rather than half-sending it.

## Commands the client refuses

`SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, `MONITOR`,
`RESET`, `HELLO`, and `CLIENT REPLY` are rejected with `ValkeyUnsupportedCommandException` before
they reach the wire. Each of them would redefine the connection — its protocol, its database, its
authenticated user, or whether replies arrive at all — behind a client that assumes none of that
changes. Rejecting them leaves the connection usable; sending them would not. See
[Exceptions](exceptions.md) for the per-command reasoning.

## Convenience methods

Thin wrappers over `ExecuteAsync`. Each takes a trailing `CancellationToken cancellationToken = default`.

| Method | Command | Returns |
|---|---|---|
| `PingAsync()` | `PING` | `Task<string>` |
| `GetAsync(string key)` | `GET` | `Task<byte[]?>` — `null` when the key is absent |
| `GetStringAsync(string key)` | `GET` | `Task<string?>` — UTF-8 decoded |
| `SetAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? expiry, bool onlyIfNotExists, bool onlyIfExists)` | `SET [PX] [NX] [XX]` | `Task<bool>` — `false` when a conditional set did not apply |
| `SetStringAsync(string key, string value, ...)` | `SET` | `Task<bool>` — UTF-8 encodes the value |
| `DeleteAsync(IEnumerable<string> keys)` | `DEL` | `Task<long>` — keys removed; `0` for an empty sequence |
| `IncrementAsync(string key, long amount = 1)` | `INCRBY` | `Task<long>` — value after the increment |
| `HashSetAsync(string key, string field, ReadOnlyMemory<byte> value)` | `HSET` | `Task<bool>` — `true` when the field was created |
| `HashGetAsync(string key, string field)` | `HGET` | `Task<byte[]?>` — `null` when absent |

`SetAsync` throws `ArgumentException` when both `onlyIfNotExists` and `onlyIfExists` are set, and
`ArgumentOutOfRangeException` when `expiry` is not positive. Expiry is sent as `PX` with the duration
rounded up to whole milliseconds.

Anything not listed goes through `ExecuteAsync`; see
[Send any command](../how-to/send-any-command.md).

## Concurrency and failure

One `SemaphoreSlim` gate serializes command execution, so concurrent callers queue rather than
interleave on the wire. On `OperationCanceledException`, `IOException`, `SocketException`, or
`ValkeyProtocolException` the client invalidates the connection before propagating — the stream may
sit between frames, so it cannot safely be reused. `IOException` and `SocketException` are wrapped in
`ValkeyConnectionException`. Calling any method after invalidation throws `ObjectDisposedException`.
