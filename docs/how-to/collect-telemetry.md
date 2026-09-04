# Collect owner telemetry

Use the development version; the published 1.0.0 package does not contain this API yet. Enable
telemetry for each standalone owner you want to observe:

```csharp
await using var owner = new ValkeyConnectionOwner(new ValkeyConnectionOwnerOptions
{
    Connection = new ValkeyClientOptions { Host = "localhost" },
    EnableTelemetry = true,
});
```

Subscribe your application's metrics collector to `ValkeyDiagnostics.MeterName` and its tracing
collector to `ValkeyDiagnostics.ActivitySourceName`. Both names are `ValkeyDotNet.ConnectionOwner`.
Choose sampling and exporters in the application, not in the driver.

For a minimal BCL-only metrics listener, attach it before sending operations:

```csharp
using var listener = new System.Diagnostics.Metrics.MeterListener();
listener.InstrumentPublished = (instrument, subscribed) =>
{
    if (instrument.Meter.Name == ValkeyDiagnostics.MeterName)
        subscribed.EnableMeasurementEvents(instrument);
};
listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
{
    // Hand off to your bounded, non-blocking application collector.
});
listener.SetMeasurementEventCallback<double>((instrument, value, tags, state) =>
{
    // Duration values are seconds.
});
listener.Start();

await owner.ExecuteAsync(new ValkeyCommand("PING"), cancellationToken);
listener.RecordObservableInstruments(); // Collect the current active-operation gauge.
```

For tracing, register an `ActivityListener` that selects the source and supplies a sampling policy,
or configure your application's tracing SDK to select that source. Keep callbacks non-blocking and
do not wait synchronously for the owner operation from inside a callback. Dispose application
listeners when collection stops.

Use operation failures and duration for caller-visible outcomes, connection-attempt failures for
startup/recovery problems, and reconnect counts for replacement frequency. Do not treat a zero
active-operation gauge as proof that all late replies drained or all sockets closed. Check
`owner.State` for lifecycle health.

See the [instrument reference](../reference/diagnostics.md) for counting rules, privacy guarantees,
and current scope. General BCL collector behavior is described in Microsoft's
[metrics instrumentation guide](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation).
