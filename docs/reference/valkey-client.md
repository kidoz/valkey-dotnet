# `ValkeyClient`

An asynchronous client for a single Valkey node. Implements `IAsyncDisposable`. Safe for concurrent
callers: writes are serialized, commands overlap, and one reader returns replies in wire order.

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

`PushReceived` fires on the client's response-reader continuation. That reader runs continuously after
the handshake, so RESP3 pushes can surface while no command is in flight. Exceptions thrown by a
handler are swallowed so a caller bug cannot desynchronize the wire.

## Generic commands

```csharp
public Task<RespValue> ExecuteAsync(
    ValkeyCommand command,
    CancellationToken cancellationToken = default)

public Task<RespValue> ExecuteWithDeadlineAsync(
    ValkeyCommand command,
    TimeSpan timeout,
    CancellationToken cancellationToken = default)
```

Sends one command and returns its reply. Throws `ValkeyServerException` when the server replies with
an error. Throws `ArgumentNullException` when `command` is null, and
`ValkeyUnsupportedCommandException` — before writing anything — for a command that would change the
connection state the client owns.

`ExecuteWithDeadlineAsync` sets a deadline for this operation without cancelling socket I/O. If the
deadline expires before the command enters the pending queue, `ValkeyCommandTimeoutException` reports
`NotSent`. After enqueue it reports `MayHaveBeenSent`; the caller stops waiting, but the background
reader drains the late reply so unrelated commands and the connection remain usable. If it cannot
drain within `ResponseDrainTimeout`, the entire stalled connection is terminated safely. A positive
timeout no longer than approximately 49.7 days is required.

```csharp
public Task<IReadOnlyList<RespValue>> ExecutePipelineAsync(
    IEnumerable<ValkeyCommand> commands,
    CancellationToken cancellationToken = default)

public Task<IReadOnlyList<RespValue>> ExecutePipelineWithDeadlineAsync(
    IEnumerable<ValkeyCommand> commands,
    TimeSpan timeout,
    CancellationToken cancellationToken = default)
```

Writes every command contiguously, flushes once, then awaits one FIFO reply per command. Returns
replies in request order. **Error replies are returned in place rather than thrown** — call
`ThrowIfError()` on each.
Returns an empty list for an empty sequence. Throws `ArgumentNullException` for a null sequence and
`ArgumentException` when any element is null. Every command is checked against the unsupported list
before the batch is written, so one `ValkeyUnsupportedCommandException` rejects the batch whole
rather than half-sending it.
The deadline method applies one deadline to the whole batch. After enqueue, every positional reply
is still drained even when the caller's deadline has expired.

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

Structured script execution is covered in the [script API reference](scripts.md), including
`ExecuteScriptAsync`, `ExecuteScriptWithDeadlineAsync`, and source-bearing pipeline commands.

One `SemaphoreSlim` serializes writes only. Each written command enters a FIFO pending-response queue,
and one background reader assigns the next non-push frame to the next pending command. Concurrent
callers therefore share one socket without interleaving command bytes or running multiple readers.
A pipeline holds the write gate for its whole batch, so its commands remain contiguous.
`MaxPendingRequests` bounds the queue; excess ordinary callers wait asynchronously, and a larger
pipeline is rejected before writing.

Cancellation before a caller enters the pending queue leaves the connection untouched. Cancellation
after enqueue invalidates the connection: a write may be partial and abandoning a positional reply
could assign it to another caller. Every other pending caller is faulted with
`ValkeyConnectionException`; the canceled caller observes `ValkeyCommandCanceledException`, an
`OperationCanceledException` subtype whose `DeliveryStatus` is `MayHaveBeenSent`. An I/O or protocol
failure likewise invalidates the connection and faults all pending work. Calling any method after
invalidation throws `ObjectDisposedException`.

An explicit operation deadline has different semantics from caller cancellation. It never interrupts
socket I/O or removes the pending FIFO entry, so it does not invalidate the connection. The deadline
is measured from admission, but an in-progress socket write is allowed to reach its next safe
completion point before the timeout is observed; interrupting a partially written RESP frame would
make the shared connection unsafe. Timed-out entries remain bounded by `MaxPendingRequests` until
their replies arrive. If they do not drain within `ResponseDrainTimeout`, the client terminates the
connection and faults every remaining FIFO entry with `ValkeyConnectionException`; no reply slot is
removed or reassigned. Connect a new standalone client after this terminal failure. Cluster clients
discard and replace the unusable node connection for the next command; the timed-out command itself
is never replayed.
