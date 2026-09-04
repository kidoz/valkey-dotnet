# The connection model

This page explains what one `ValkeyClient` is, why it behaves the way it does under concurrency and
failure, and what that rules out. It is background, not instruction — for the rules themselves see
[`ValkeyClient`](../reference/valkey-client.md).

## One client is one socket

A `ValkeyClient` owns exactly one TCP (or TLS) connection to one node. There is no pool, no
multiplexer, and no discovery. That is a deliberate floor, not an oversight: pooling, cluster
routing, and reconnect are each substantial subsystems with their own failure modes, and building
them on an unproven transport produces bugs that are very hard to attribute.

## Why replies must stay in order

RESP has no request identifiers. The protocol is a stream of framed values, and the only thing tying
a reply to a request is *position*: the *n*-th reply answers the *n*-th command. Nothing in a reply
says which command produced it.

Everything else follows from that.

If two callers could write to the socket concurrently, their bytes would interleave and the server
would see a corrupt command. If a reader abandoned a reply halfway, the next read would start
mid-frame and every subsequent reply would be attributed to the wrong caller — the failure mode is
not an exception, it is one caller silently receiving another caller's data.

So the client serializes commands through a single gate. Concurrent callers are safe, but they queue;
they do not overlap on the wire. A slow command delays everyone behind it, which is why blocking
commands need their own client instance.

## Why cancellation destroys the connection

Cancelling a command that has already written bytes leaves the connection in an unknown state. The
server will still execute the command and still send a reply. If the client kept the connection, that
orphaned reply would be read as the answer to whatever command came next.

The client cannot avoid this by "draining" the pending reply, because it does not always know how
many replies are outstanding or how large they are — that is exactly what it was cancelled out of
determining.

So cancellation during I/O invalidates the connection. This is expensive and it is the right trade:
the alternative is a data-correctness bug that surfaces as one user seeing another user's value.

The same reasoning applies to `ValkeyProtocolException` and `ValkeyConnectionException`. Once the
stream position is untrustworthy, the only safe move is to stop using it.

A `ValkeyServerException` is different. An error reply is a complete, correctly framed value that the
client fully consumed. The stream position is still known, so the connection survives.

## Why some commands are refused outright

A few commands do not answer a question; they change what the connection *is*. `SUBSCRIBE` moves it
into subscriber mode and — on RESP3 — answers with a push frame rather than an ordinary reply, so a
client that skips pushes while waiting for a reply waits forever. `RESET` throws away the protocol,
database, and authentication the handshake established. `HELLO` renegotiates the protocol under a
client that already recorded it. `CLIENT REPLY OFF` stops replies arriving at all. `MONITOR` replaces
replies with a stream of server events.

Each of those breaks the same assumption everything above rests on: that the *n*-th reply answers the
*n*-th command, on a connection whose protocol and identity are fixed at connect time.

The client could accept them and desynchronize, which fails silently and late. It refuses them
instead, before writing a byte, so the failure is immediate, named, and costs nothing — the
connection is untouched and still works. That is what "unsupported behavior fails explicitly" means
here: not a comment in the docs, a rejection at the call site.

The alternative — a real subscriber mode with a background reader and a connection state machine — is
a feature this library has deliberately not built yet. Refusing is honest about that; sending and
hoping would not be.

## Why pipeline errors are returned, not thrown

`ExecutePipelineAsync` writes *n* commands and must then read *n* replies. Throwing on the first
error reply would abandon the remaining ones, leaving them buffered in the socket — the same
desynchronization as an abandoned read.

Returning errors in place is what lets the client finish draining. The cost is that the caller must
check each reply; the benefit is that a failed command in a batch does not cost you the connection.

## Why replies are lossless

Most clients map replies to command-specific .NET types. That requires the client to know every
command, and it discards information — RESP3 distinguishes a null from an empty string, a set from an
array, a double from a string, and carries attribute metadata that a typed mapping throws away.

`RespValue` keeps the wire shape and lets the caller project. The consequence is that any Valkey
command, including ones released after this library, works immediately through `ExecuteAsync`. The
typed convenience methods are a thin layer of ergonomics on top, not the supported surface.

## Bounds as a security property

`MaxResponseBytes`, `MaxResponseElements`, and `MaxNestingDepth` exist because the reader parses
whatever arrives. A compromised or malfunctioning server can claim a 40 GiB bulk string or nest
arrays thousands deep. An unbounded parser turns that into an out-of-memory kill or a stack overflow.

A byte limit alone is not enough, because bytes on the wire and bytes in memory are not the same
quantity. `*2147483647\r\n` is thirteen bytes; a parser that believes the header and sizes a
collection from it has already lost, whatever it does next. So the reader trusts no declared count:
it rejects a cardinality that cannot fit in the bytes still allowed, grows collections to what
actually arrives, and charges every decoded value against `MaxResponseElements`. The bound is on what
gets *built*, not only on what gets read.

The bounds are on the *client* because the client is the thing being protected. They are not tuning
knobs; raising them by default weakens the process against a hostile peer.

## What this design defers

Cluster routing, Sentinel discovery, pooling, automatic reconnect, and subscription-mode Pub/Sub are
absent. Each needs a decision this library has not yet made — which commands are safe to retry after
which failures, how subscription state is restored across a reconnect, how backpressure is signalled.
Guessing at those would bake wrong answers into the transport.

The generic command API means none of that blocks day-to-day use. Scripting via `EVAL` covers the
atomicity cases that would otherwise need a held connection.

## Related

- [Handle errors](../how-to/handle-errors.md) — acting on this model.
- [Exceptions](../reference/exceptions.md) — which failures invalidate.
