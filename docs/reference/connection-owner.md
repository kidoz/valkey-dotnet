# Standalone connection owner

`ValkeyConnectionOwner` owns one current `ValkeyClient` and replaces terminal connections on demand.
It is a separate lifecycle layer: `ValkeyClient` itself remains one terminal physical connection.
This API is available in the unreleased development version, not the published 1.0.0 package.

## Operations

Construction validates `ValkeyConnectionOwnerOptions` but opens no socket. `ConnectAsync` warms the
owner; command operations also establish the connection when needed. All I/O methods accept an
optional `CancellationToken` last. `DisposeAsync` is idempotent, cancels shared connection attempts,
closes the current client, and prevents further admission.

| Methods | Behavior |
|---|---|
| `ExecuteAsync`, `ExecuteWithDeadlineAsync` | One command attempt; no transport replay. |
| `ExecuteRetryableAsync`, `ExecuteRetryableWithDeadlineAsync` | Caller-authorized transport replay, bounded by `MaxCommandRetries`. |
| `ExecutePipelineAsync`, `ExecutePipelineWithDeadlineAsync` | One batch attempt; positional server errors remain in the result list; no replay. |
| `ExecuteScriptAsync`, `ExecuteScriptWithDeadlineAsync` | Binary-safe script execution with connection-scoped `NOSCRIPT` recovery; no transport replay. |

Deadline methods use one budget across connection acquisition, backoff, command admission, and
execution, including explicitly authorized retries. Deadline expiry while acquiring a connection or
awaiting physical command admission reports `NotSent`; expiry after enqueue reports
`MayHaveBeenSent`. A later connection failure, cancellation, or timeout does not erase an earlier
ambiguous transport attempt. The exception's `Timeout` is the full configured budget.

Cancellation or deadline expiry while waiting for the shared connection ends only that caller's
wait. The bounded connection cycle can finish for other callers, even if all current waiters leave.
Disposing the owner cancels the cycle itself. Cancellation after command enqueue and isolated
deadline draining retain the physical client's existing semantics.

## Retry contract

Ordinary commands, pipelines, and scripts are never replayed after transport failure. A subsequent
operation can obtain a replacement connection without restarting the process.

The two `ExecuteRetryable…` methods are explicit caller authorization to repeat the exact command
after an ambiguous transport failure. They do not classify command names, provide an idempotency
key, or prove a write safe. Server errors, malformed replies, cancellation, and command deadlines
are not retried. Connection establishment can independently retry transient connection failures.
All command arguments and their underlying binary buffers remain caller-owned and must stay
unchanged until the operation completes, including retries.

Success after replay does not prove an earlier attempt did not execute. In particular, marking a
lock acquisition or lease extension retryable without an operation-specific correctness argument
can produce incorrect ownership or timing assumptions.

## Options

| Property | Default | Bound / meaning |
|---|---|---|
| `Connection` | `new ValkeyClientOptions()` | Same validated settings object used for every initial and replacement connection. |
| `MaxConcurrentOperations` | 1024 | 1–1,048,576; counts all admitted operations, including reconnect waiters and active commands. |
| `MaxConnectAttempts` | 3 | 1–100 attempts per shared acquisition cycle. |
| `InitialReconnectDelay` | 100 ms | At least 1 ms and no greater than `MaxReconnectDelay`. |
| `MaxReconnectDelay` | 5 s | At most 4,294,967,294 ms. |
| `MaxCommandRetries` | 1 | 0–16 additional attempts; used only by the explicitly retryable methods. |

Admission is fail-fast: excess operations receive `ValkeyCapacityException` with `NotSent`. There is
no unbounded offline queue. This operation-count bound complements `Connection.MaxPendingRequests`,
which bounds written commands per physical connection; one pipeline is one owner operation but can
consume multiple physical reply slots. These are not byte budgets for caller-owned payloads.

Concurrent acquisitions share one attempt sequence, including its backoff. Following failure number
`n`, the delay is equal jitter in `[cap / 2, cap)`, where
`cap = min(MaxReconnectDelay, InitialReconnectDelay × 2^(n−1))`; the exponent saturates after 31
failures. The first connection has no delay. A successfully established connection resets the
failure count. The next acquisition after a failed cycle waits any unelapsed backoff; idle time
counts toward that delay. There is no idle background reconnect loop.

Connect/socket/I/O timeouts and transport failures consume the configured attempt budget. Exhaustion
throws `ValkeyConnectionException` with `NotSent` when no command attempt has occurred. TLS
authentication, server handshake rejection, malformed handshake, and argument failures put the
owner in `Faulted`; later calls rethrow the retained failure without opening another socket. New
configuration requires a new owner. Each attempt retains the configured `ConnectTimeout`.

## Lifecycle and limits

`State` is a `ValkeyConnectionState` snapshot, not a liveness probe or a promise that the next command
will succeed. A socket can fail immediately after selection. `Host` and `Port` expose the configured
endpoint, never credentials or server response text.

| State | Meaning |
|---|---|
| `NeverConnected` | Constructed, no acquisition started. |
| `Connecting` | Shared acquisition before the first successful connection, including backoff. |
| `Connected` | Current client has not been observed terminal. |
| `Reconnecting` | Shared acquisition after an earlier successful connection, including backoff. |
| `Disconnected` | Terminal client or exhausted transient acquisition; later calls can acquire again. |
| `Faulted` | Retained terminal configuration/authentication/handshake failure. |
| `Disposed` | Disposal has started; no further operation admission. |

Every replacement reapplies TLS validation, ACL credentials, requested protocol, database, client
name, connect/drain timeouts, parser bounds, and pending-request limits through `Connection`.
Runtime session mutations are not restored: for example, `SELECT`, `CLIENT SETNAME`, transactions,
and tracking established by generic commands do not become owner configuration. Multi-call
connection-affine protocols require a dedicated physical client. No physical-client lease, push
callback forwarding, subscriber lifecycle, general-purpose pool, or cluster routing is exposed here.

See [Recover a standalone connection](../how-to/recover-standalone-connections.md) for usage and
[Exceptions](exceptions.md) for delivery semantics.
