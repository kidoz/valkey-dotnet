# Dedicated subscriber

Available in the unreleased development version. `ValkeySubscriber` owns a separate connection for
RESP2/RESP3 `SUBSCRIBE`, `PSUBSCRIBE`, and their unsubscribe operations. Ordinary `ValkeyClient`
connections still reject subscription-mode commands. Publishing uses the ordinary binary-safe
generic command API.

The wire implementation follows the [Valkey Pub/Sub documentation](https://valkey.io/topics/pubsub/)
and [RESP specification](https://valkey.io/topics/protocol/): successful subscription commands receive
acknowledgement frames, not ordinary FIFO command replies. Channel and pattern registrations are
distinct. A publish matching both produces separate deliveries. Pub/Sub is independent of the
selected database and is best-effort, with no durable replay.

## API and lifecycle

| API | Behavior |
|---|---|
| `ValkeySubscriber.ConnectAsync(options, cancellationToken)` | Opens TCP or TLS, authenticates with HELLO, honors negotiated protocol and optional SELECT, then starts one reader. |
| `SubscribeAsync(channel, cancellationToken)` | Returns a local handle after the matching server acknowledgement. |
| `SubscribePatternAsync(pattern, cancellationToken)` | Same behavior for a binary glob pattern. |
| `ValkeySubscription.ReadAllAsync(cancellationToken)` | Async stream of binary `ValkeyPubSubMessage` values. |
| `ValkeySubscription.UnsubscribeAsync(cancellationToken)` | Removes this local handle. While connected, the final handle waits for the server unsubscribe acknowledgement. During recovery, removal is local and restoration reconciles any in-flight server registration. |
| `ValkeySubscription.DisposeAsync()` | Unsubscribes with the configured operation timeout. Idempotent after removal or owner shutdown. |
| `ValkeySubscriber.DisposeAsync()` | Closes its socket, completes queues, settles pending operations, and joins the reader. Does not wait for application message-processing tasks. |
| `IsConnected`, `IsReconnecting`, `Failure`, `Completion` | State snapshots, terminal cause, and normally completing reader/recovery lifetime task. No liveness guarantee. |
| `ConnectionLosses`, `ReconnectAttempts`, `SuccessfulReconnects` | Polling counters for observed loss intervals, attempted replacement connections, and fully reconciled restoration cycles. Not Meter instruments or lost-message counts. |

Names are copied after operation admission; caller storage is not retained. `Channel` and `Payload`
remain opaque bytes. `Pattern` is null for direct delivery, distinct from an empty pattern. Returned
memory is read-only and can be shared across local handles; application code must not mutate it.

Each handle has its own queue. Duplicate handles share one server subscription but receive
independent local deliveries. Multiple enumerators on the *same* handle compete for messages; they
are not broadcast consumers. Cancelling enumeration does not unsubscribe the handle. Buffered
messages drain before a removed handle completes, or before a terminal connection error is raised.

The library never invokes application handlers. Processing, decoding, malformed application-payload
policy, and handler task lifetime belong to the consumer. A handler exception cannot propagate into
the socket reader. Arbitrary payload bytes are valid; malformed RESP or invalid acknowledgement
shape, kind, name, or count terminates only the subscriber connection.

## Bounds and overflow

| `ValkeySubscriberOptions` property | Default | Bound |
|---|---|---|
| `Connection` | New `ValkeyClientOptions` | Existing TLS, handshake timeout, byte, element and nesting bounds apply. |
| `QueueCapacity` | 128 | Buffered messages per local handle; 1–1,048,576. |
| `MaxSubscriptions` | 1,024 | Local handles, including duplicates; 1–1,048,576. |
| `MaxChannelBytes` | 16,384 | Subscription name/pattern bytes; 1–1,048,576. Empty names are allowed. |
| `MaxConcurrentOperations` | 64 | Executing and waiting lifecycle operations; 1–1,048,576. |
| `OperationTimeout` | 5 seconds | One admission-inclusive budget for each subscribe/unsubscribe call. |
| `EnableReconnect` | `false` | Opts into transport-loss recovery and restoration of confirmed registrations. |
| `MaxReconnectAttempts` | 3 | Replacement attempts per loss interval; 1–100. |
| `InitialReconnectDelay` | 100 ms | Initial exponential-backoff ceiling; positive and no greater than the maximum. |
| `MaxReconnectDelay` | 2 seconds | Maximum backoff ceiling. Each delay uses equal jitter between half the ceiling and the ceiling. |
| `RecoveryTimeout` | 30 seconds | Total per-cycle budget, including old-writer retirement, backoff, connect, and every restore/unsubscribe acknowledgement. |

Overflow drops the **incoming** local delivery without blocking the reader. `DroppedMessages` on a
handle counts its dropped deliveries; the subscriber's counter aggregates drops across all handles,
including removed ones. These counters are polling properties, not Meter instruments. One full
queue does not stop other handles or subscription acknowledgements.

Message queues are count-bounded, not independently byte-budgeted. Their worst-case retained payload
is bounded by queue capacity × handles × configured frame-byte limit, plus decoding overhead.
The inherited 64 MiB frame limit can therefore permit substantial memory retention with large
queues. `Connection.MaxPendingRequests` and `ResponseDrainTimeout` are command-client options, not
subscriber queue or acknowledgement settings.

## Failure semantics

- Capacity exhaustion rejects before writing with `ValkeyCapacityException` / `NotSent`.
- Cancellation while waiting for the lifecycle gate cancels only that operation.
- Timeout before writing reports `ValkeyCommandTimeoutException` / `NotSent`.
- Cancellation or timeout after a lifecycle write may have started terminates the subscriber,
  reports `MayHaveBeenSent`, and completes all its streams. It never affects an ordinary command
  connection. Subscription operations do not use the ordinary client's isolated deadline/drain API.
- Server rejection fails only the pending change; confirmed registrations remain intact. Exception
  text omits server-controlled details and preserves only `NOAUTH`, `WRONGPASS`, `NOPERM`, or `ERR`.
- EOF and transport loss are terminal by default; opted-in recovery can replace the physical
  connection. Protocol errors and parser-bound violations remain terminal even with recovery enabled.
  Buffered deliveries can drain before streams report a terminal error. Subsequent new operations
  reject as disposed.
- Cancellation can race with acknowledgement; cancellation may win even when the server applied
  the change. Closing the connection removes its server subscriptions.

## Optional recovery

`EnableReconnect = true` preserves the existing handles and their bounded queues after transport
loss. One supervisor retires the old socket and waits for its writer to exit before publishing a
replacement. It reapplies the same connection settings, including TLS validation, credentials,
protocol negotiation, database, client name, and parsing bounds. It restores each distinct confirmed
channel or pattern once, retaining duplicate local handles without duplicate server registrations.

The initial `ConnectAsync` still makes one bounded connection attempt. Automatic retry starts only
after an established connection is lost. Connect/I/O and restoration-acknowledgement timeouts can be
retried within both configured bounds. Authentication rejection, permission rejection, TLS validation
failure, and malformed frames terminate recovery immediately. Exhaustion closes all streams; there
is no perpetual background retry loop. Each later loss begins a fresh bounded backoff cycle.

New subscribe calls during recovery fail with `ValkeyConnectionException` / `NotSent`; no new offline
subscription queue is created. Previously admitted operations remain bounded by their original
operation timeout. A change interrupted after writing fails without automatic replay. An unconfirmed
new subscription is not restored. A previously confirmed registration whose unsubscribe lacked an
acknowledgement remains desired until its handle is explicitly removed.

Unsubscribing while reconnecting removes the local handle immediately. If its restore command is
already in flight, restoration consumes that acknowledgement and unsubscribes the now-unused server
registration before declaring recovery complete. No new messages are enqueued to a removed handle;
already buffered messages can still drain. Disposal cancels backoff, connect, and restore work and
joins the supervisor. Caller cancellation of an in-flight public lifecycle change remains terminal;
it does not silently authorize restoration.

`IsConnected` becomes true only after all remaining registrations are restored and removals are
reconciled. Messages from already-restored registrations can arrive while other registrations are
still being restored. Queued messages from before a disconnect remain available; messages missed
during the gap cannot be reconstructed or counted. Successful restoration is not proof that a local
cache is current. Consumers can observe `ConnectionLosses` and apply their own missed-invalidation
policy. Duplicate channel/pattern matches remain separate deliveries; no application deduplication is
performed.

There is no key-tracking lifecycle, sharded Pub/Sub, topology migration, or
subscriber activity/Meter instrumentation yet. Connecting this standalone subscriber to a cluster
node does not add topology-aware routing. This first version is not full invalidation-readiness
certification.

## Verification evidence

On 2026-09-05, the local macOS arm64 build completed without warnings and all 250 server-free tests
passed, including 28 subscriber cases. Subscriber coverage includes RESP2/RESP3, byte-fragmented
binary delivery, empty versus null values, duplicate handles, queue overflow, rejected changes,
malformed acknowledgements, parser bounds, cancellation, disposal, EOF, and TLS/authentication.

The new live Pub/Sub test passed separately against fresh disposable Valkey 9.1, 8.1, and 7.2
containers: two protocol cases per version, six passes with no skips. It checks binary channels,
patterns and payloads, duplicate-handle ownership, unsubscribe acknowledgements, and an unaffected
publisher connection. All three test containers were removed; pre-existing containers were untouched.
This is local evidence, not a subscriber soak, recovery, cluster-routing, or cross-platform CI claim.

The recovery follow-up passed all 271 local unit tests, including 49 subscriber cases, on
2026-09-05. New deterministic cases cover binary stream preservation, shared registration ownership,
unsubscribe during handshake and in-flight restoration, failed unconfirmed changes, bounded retries,
terminal rejection, recovery timeout, disposal, TLS/session restoration, parser bounds, and twelve
successive reconnects per protocol. The new live `CLIENT KILL ID` test is opt-in and has not yet been
executed; the earlier six live Pub/Sub passes do not establish live restoration evidence.
