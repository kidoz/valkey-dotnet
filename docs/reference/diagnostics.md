# Connection-owner diagnostics

Available in the unreleased development version. `ValkeyConnectionOwnerOptions.EnableTelemetry`
defaults to `false`. Enabling it opts that owner into process-wide BCL metrics and listener-selected
activities. The shipping package has no telemetry SDK, exporter, dependency, background export
task, or telemetry queue.

`ValkeyDiagnostics.MeterName` and `ValkeyDiagnostics.ActivitySourceName` both equal
`ValkeyDotNet.ConnectionOwner`. Instruments live for the process lifetime; disposing one owner does
not dispose the shared source. Disabled owners do not invoke this instrumentation or initialize it.

## Metrics

| Instrument | Type / unit | Measurement |
|---|---|---|
| `valkey.owner.operations` | Counter / `{operation}` | Completed logical owner calls, including calls that failed admission. |
| `valkey.owner.operation.failures` | Counter / `{operation}` | Failed calls; a returned pipeline containing any server error counts once. |
| `valkey.owner.operation.duration` | Histogram / `s` | Logical-call duration, including acquisition, backoff, retries, and script recovery. |
| `valkey.owner.operations.active` | ObservableGauge / `{operation}` | Current executing wrappers across enabled owners, including connection waiters. |
| `valkey.owner.connection.attempts` | Counter / `{attempt}` | Physical connect/handshake attempts initiated by enabled owners. |
| `valkey.owner.connection.failures` | Counter / `{attempt}` | Those attempts that throw, including cancellation; not later established-socket failures. |
| `valkey.owner.connection.duration` | Histogram / `s` | Duration per physical connect/handshake attempt, excluding reconnect backoff. |
| `valkey.owner.reconnects` | Counter / `{connection}` | Replacement clients accepted after an earlier successful connection; excludes initial connection. |

Operation counters and duration use `valkey.operation.kind`: exactly `connect`, `command`,
`pipeline`, or `script`. Failures additionally carry `error.type`. Connection duration/failure
instruments carry only `error.type` on failure. Other instruments have no tags.

The fixed error categories are `capacity`, `protocol`, `server`, `authentication`, `connection`,
`timeout`, `canceled`, `disposed`, `invalid_argument`, and `_OTHER`. These classify the exception
observed at the instrumented boundary, without reading its message, command, server error code, or
inner exception. For example, a `WRONGPASS` server reply is `server`; TLS `AuthenticationException`
is `authentication`. A returned pipeline with one or more error values is `server`.

These are logical-call metrics, not wire-command counts. A pipeline is one operation; an explicitly
retried command is one operation; a script's `EVALSHA`/`NOSCRIPT` recovery remains one operation.
Successful recovery does not increment logical-operation failures. `ConnectAsync` warmups count as
operations even when they reuse an existing connection. Public argument checks performed before
entering the owner execution wrapper do not produce measurements.

The active gauge is not the physical FIFO length or the number of live sockets. It returns to zero
after a caller deadline or cancellation even if the shared connection attempt or late response
draining continues. It retains only a process-wide count, never references to owners. Events from
different concurrent calls, connection publication, and reconnect notification have no global order.

## Activities

An attached `ActivityListener` must select the source and sample an activity; `EnableTelemetry`
alone does not create exported traces. Ordinary operations produce a `Client` activity named
`valkey`; connection warmup produces an `Internal` activity named `valkey.connect`. One activity
covers the logical call, including retries. Connection attempts do not create additional spans.

Library-added attributes are limited to:

- `db.system.name = valkey`;
- `valkey.operation.kind`, with the four values above;
- `error.type` on failure, with the fixed categories above.

Failures set `ActivityStatusCode.Error` without a description. Successful activity status is unset.
No exception events, command names/text, keys, arguments, script text/digests, payloads, usernames,
passwords, client names, database labels, or host/port labels are attached. Parent trace context is
preserved; baggage or tags added by application instrumentation remain application-controlled.

The `Client` kind, `db.system.name`, and `error.type` follow the applicable
[OpenTelemetry database span conventions](https://opentelemetry.io/docs/specs/semconv/db/database-spans/).
Endpoint/database attributes are deliberately omitted for privacy; these owner-level instruments
do not claim full database semantic-convention compliance. There is no command-text capture switch.

## Listener safety and limits

Listener calls occur outside the owner's synchronization lock and outside physical transport locks.
Publication, sampling, measurement, and activity callback exceptions are isolated from command
results. One throwing listener can prevent other listeners from observing a measurement; delivery
is best effort. If initial instrument/source publication throws, that instrument/source can remain
unavailable for the process lifetime. Observable-collection callbacks invoked by a collector belong
to that collector and do not run on the transport path.

Callbacks execute synchronously and can add latency. They must be short and non-blocking; exception
isolation does not protect against a callback that never returns or synchronously waits for the
operation it is observing. Completion-metric callbacks are excluded from recorded metric duration,
but activity/listener overhead can affect end-to-end latency and span duration.

Direct `ValkeyClient` calls and cluster operations are not instrumented by this owner layer.
Established-connection failures hidden by a successful retry, individual retry counts, redirects,
physical FIFO occupancy, subscription restoration, dropped push messages, and subscriber handoff
latency remain outside this first telemetry slice.

See [Collect owner telemetry](../how-to/collect-telemetry.md) for setup.
