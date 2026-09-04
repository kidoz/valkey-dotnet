# Exceptions

All library-specific exceptions derive from `ValkeyException`.

```text
Exception
└── ValkeyException
    ├── ValkeyProtocolException
    ├── ValkeyServerException
    ├── ValkeyConnectionException
    ├── ValkeyClusterException
    └── ValkeyUnsupportedCommandException
```

## `ValkeyException`

Base type. Constructors take `(string message)` and `(string message, Exception innerException)`.
Catch it to handle every failure originating in this library.

## `ValkeyProtocolException`

The bytes on the wire were not valid RESP, or a configured bound was exceeded:

- an unknown type prefix
- a malformed length, integer, or double
- a negative length on a type that has no null form — only RESP2 null bulk strings (`$-1`) and null
  arrays (`*-1`) may be negative; a negative blob error, verbatim string, map, set, or push is invalid
- a frame exceeding `MaxResponseBytes`
- a reply declaring or decoding more values than `MaxResponseElements`
- nesting deeper than `MaxNestingDepth`
- a `HELLO` reply that does not report a supported protocol version

Always invalidates the connection. The stream position is no longer trustworthy, so recovery means
opening a new client.

## `ValkeyServerException`

The server returned a RESP error reply. Carries `ErrorCode`, the leading token of the message up to
the first space — `WRONGTYPE`, `NOAUTH`, `ERR`, `MOVED`, and so on. When the message has no space,
`ErrorCode` is the whole message.

```csharp
try
{
    await valkey.IncrementAsync("a-string-key");
}
catch (ValkeyServerException exception) when (exception.ErrorCode == "WRONGTYPE")
{
    // The key holds a value of the wrong kind.
}
```

Does **not** invalidate the connection: an error reply is a well-formed, fully consumed frame, so the
client stays usable.

Thrown by `ExecuteAsync` and the convenience methods. **Not** thrown by `ExecutePipelineAsync`, which
returns errors in place — call `ThrowIfError()` on each reply.

## `ValkeyConnectionException`

The transport failed. Wraps the underlying `IOException` or `SocketException` as `InnerException`.
Always invalidates the connection.

## `ValkeyClusterException`

Cluster seed discovery, topology validation, endpoint parsing, or bounded redirection failed. The
underlying seed, server, or connection failure is retained as `InnerException` when available.

## `ValkeyUnsupportedCommandException`

The command would change connection state the client owns, so it is rejected **before** anything is
written. Carries `Command`, the rejected name including the subcommand where that is what made it
unsupported.

| Command | Why |
|---|---|
| `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `SSUBSCRIBE`, `SUNSUBSCRIBE` | On RESP3 these reply with push frames, which the reply loop delivers to `PushReceived` and keeps waiting past. On RESP2 they put the connection into subscriber mode, where normal commands no longer work. |
| `MONITOR` | Turns the connection into an unsolicited stream of server events. |
| `RESET` | Discards the protocol, database, and authentication state `ConnectAsync` established. |
| `HELLO` | The handshake belongs to `ConnectAsync`. Re-running it would silently change the protocol while `NegotiatedProtocol` kept reporting the old one. |
| `CLIENT REPLY` | `OFF` and `SKIP` suppress replies, leaving the reader waiting for a frame that never arrives. |

Nothing reached the wire, so the connection is untouched and still usable — including for other
`CLIENT` subcommands such as `CLIENT SETNAME`.

Both `ExecuteAsync` and `ExecutePipelineAsync` check every command before writing, so one unsupported
command in a batch rejects the whole batch without sending any of it.

## Exceptions from the BCL

The library also surfaces standard exceptions directly:

| Exception | Raised when |
|---|---|
| `ArgumentException` | Invalid host, `Username` without `Password`, both `NX` and `XX`, a null command in a pipeline, a command name that is not printable ASCII. |
| `ArgumentNullException` | Null command, null command name, null argument array, null key sequence, null value passed to a `ValkeyArgument` conversion. |
| `ArgumentOutOfRangeException` | Port, database, protocol, connect timeout, response/element/nesting bounds, or a non-positive expiry. |
| `TimeoutException` | `ConnectTimeout` elapsed during connect, TLS handshake, or the initial `HELLO`. |
| `OperationCanceledException` | The supplied `CancellationToken` was cancelled. Invalidates the connection when I/O was in flight. |
| `ObjectDisposedException` | The client was disposed or invalidated by an earlier failure. |
| `InvalidOperationException` | A `RespValue` accessor was called for the wrong `RespType`. |

## Which failures invalidate the connection

| Exception | Connection reusable |
|---|---|
| `ValkeyServerException` | yes |
| `ValkeyUnsupportedCommandException` | yes |
| `InvalidOperationException` from an accessor | yes |
| `ArgumentException` family (thrown before any write) | yes |
| `ValkeyProtocolException` | no |
| `ValkeyConnectionException` | no |
| `OperationCanceledException` during I/O | no |

See [Handle errors](../how-to/handle-errors.md) for the practical pattern and
[Connection model](../explanation/connection-model.md) for why cancellation is fatal.
