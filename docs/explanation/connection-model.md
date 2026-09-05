# The connection model

This page explains what one `ValkeyClient` is, why it behaves the way it does under concurrency and
failure, and what that rules out. It is background, not instruction — for the rules themselves see
[`ValkeyClient`](../reference/valkey-client.md).

## One client is one socket

A `ValkeyClient` owns exactly one TCP (or TLS) connection to one node and multiplexes ordinary
commands on that stream. `ValkeyClusterClient` composes these single-node clients: it retains a
bounded, configurable number of connections per primary and routes keys through an atomically
replaceable slot map. Every connection has exactly one response reader and the same ordering and
cancellation rules described below.

## Why replies must stay in order

RESP has no request identifiers. The protocol is a stream of framed values, and the only thing tying
a reply to a request is *position*: the *n*-th reply answers the *n*-th command. Nothing in a reply
says which command produced it.

Everything else follows from that.

If two callers could write to the socket concurrently, their bytes would interleave and the server
would see a corrupt command. If a reader abandoned a reply halfway, the next read would start
mid-frame and every subsequent reply would be attributed to the wrong caller — the failure mode is
not an exception, it is one caller silently receiving another caller's data.

So the client serializes only writes through a single gate, then records each expected response in a
FIFO queue. One background reader drains replies and completes those pending callers in position.
Commands can all be in flight on one socket, but neither their bytes nor their replies can be
reordered.

RESP ordering still creates head-of-line blocking: a slow command delays delivery of every reply
behind it, even when the server has finished later work. Blocking commands therefore need their own
client instance or a separate connection from a cluster node's bounded connection set.

## Why cancellation destroys the connection

Cancelling a command that has already written bytes leaves the connection in an unknown state. The
server will still execute the command and still send a reply. If the client kept the connection, that
orphaned reply would be read as the answer to whatever command came next.

The multiplexer uses enqueue as the safety boundary. Cancellation before enqueue is just a canceled
wait. After enqueue, the write may be partial or its positional reply may already be arriving, so the
client invalidates the whole connection and faults the other pending callers. This is expensive and
deliberately conservative: the alternative is a data-correctness bug that surfaces as one user
seeing another caller's value.

The same reasoning applies to `ValkeyProtocolException` and `ValkeyConnectionException`. Once the
stream position is untrustworthy, the only safe move is to stop using it.

A `ValkeyServerException` is different. An error reply is a complete, correctly framed value that the
client fully consumed. The stream position is still known, so the connection survives.

An isolated operation deadline does not cancel the read or remove its FIFO slot. The reader may
still receive the late positional reply and keep the connection synchronized. That cannot continue
forever when the server never replies: after `ResponseDrainTimeout`, the client retires the whole
socket and settles every retained slot as a connection failure. It never skips the missing slot or
reassigns a later reply. A cluster client replaces that terminal node connection for a subsequent
command without replaying the ambiguous command.

## Why recovery has a separate owner

`ValkeyConnectionOwner` provides a long-lived standalone lifecycle without making a broken physical
stream reusable. Concurrent callers share one bounded connection cycle. A failed write is not
automatically repeated just because a replacement socket is available: the original server may
already have applied it. Replay therefore requires an explicitly retryable operation at the call
site, where the application can judge its safety.

Admission limits cap owner work even while offline, and jittered backoff bounds reconnection
pressure. A waiter's cancellation ends that wait without destroying a shared attempt needed by
other callers. See the [owner reference](../reference/connection-owner.md) for exact semantics.

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

The unreleased `ValkeySubscriber` supplies a separate background reader and acknowledgement state
machine for Pub/Sub. It never changes an ordinary command connection into a subscriber. Its bounded
async streams keep application handlers off the socket reader; see the
[subscriber reference](../reference/subscriber.md).

## Why pipeline errors are returned, not thrown

`ExecutePipelineAsync` writes *n* contiguous commands and queues *n* reply slots. Throwing on the
first error reply would prevent the caller from observing the remaining results and make redirect
handling incomplete, even though the shared response reader must continue draining them.

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

`MaxPendingRequests` bounds the number of FIFO reply slots on each connection. Without it, callers
could enqueue work faster than a slow server answers and grow multiplexer state without limit.

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

Replica reads, cluster-wide scans, Sentinel discovery, and general-purpose pooling remain absent.
The dedicated subscriber treats disconnect as terminal by default, with opt-in bounded restoration
of confirmed channel/pattern or shard subscriptions on the same endpoint. Tracking invalidations
use a separate client. The cluster subscriber adds slot-routed sharded primitives with dedicated
sockets, but automatic relocation during slot migration and its live recovery evidence remain work
for a later state machine; current topology loss is an explicit failure. See the
[sharded Pub/Sub contract](../reference/cluster-subscriber.md).

The generic command API means none of that blocks day-to-day use. Scripting via `EVAL` covers the
atomicity cases that would otherwise need a held connection.

## Related

- [Handle errors](../how-to/handle-errors.md) — acting on this model.
- [Exceptions](../reference/exceptions.md) — which failures invalidate.
