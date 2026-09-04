using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

public sealed class ValkeyDiagnosticsTests
{
    private static readonly string[] ActivityTags = ["db.system.name", "valkey.operation.kind", "error.type"];
    private static readonly string[] MetricTags = ["valkey.operation.kind", "error.type"];
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SamplingCanDisableActivitiesWithoutDisablingMetrics()
    {
        using var capture = new Capture(ActivitySamplingResult.None);
        await using var server = EchoServer();
        await using var owner = EnabledOwner(server);
        await owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken);
        Assert.Empty(capture.Activities);
        Assert.Equal(1, capture.Total("valkey.owner.operations"));
    }

    [Fact]
    public async Task HierarchicalParentIdentifiersArePreserved()
    {
        using var capture = new Capture();
        using var parent = new Activity("legacy-parent");
        parent.SetIdFormat(ActivityIdFormat.Hierarchical);
        parent.Start();
        await using var server = EchoServer();
        await using var owner = EnabledOwner(server);
        await owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken);
        Assert.Equal(parent.Id, Assert.Single(capture.Activities).ParentId);
        Assert.Same(parent, Activity.Current);
    }

    [Fact]
    public async Task TelemetryIsSilentByDefaultEvenWhenListenersAreAttached()
    {
        using var capture = new Capture();
        await using var server = EchoServer();
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions { Connection = server.ClientOptions() }
        );
        Assert.Equal("PONG", (await owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken)).AsString());
        Assert.Empty(capture.Measurements);
        Assert.Empty(capture.Activities);
    }

    [Fact]
    public async Task EnabledMetricsCountLogicalCallsAndPipelineErrorsWithoutSensitiveTags()
    {
        using var capture = new Capture();
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("+PONG\r\n");
            await session.ReadCommandAsync();
            await session.SendAsync("-ERR private-key private-value private-command\r\n");
            await session.ReadCommandAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("+OK\r\n-WRONGTYPE private-key\r\n");
            await session.ReadCommandAsync();
            await session.SendAsync("+script-result\r\n");
            await session.ReadCommandAsync();
        });
        await using var owner = EnabledOwner(server);
        using var parent = new Activity("parent");
        parent.Start();
        await owner.ConnectAsync(TestToken);
        await owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken);
        await Assert.ThrowsAsync<ValkeyServerException>(() =>
            owner.ExecuteAsync(new ValkeyCommand("private-command", "private-key", "private-value"), TestToken)
        );
        var replies = await owner.ExecutePipelineAsync(
            [new("SET", "private-key", "private-value"), new("GET", "private-key")],
            TestToken
        );
        Assert.Equal(RespType.SimpleError, replies[1].Type);
        var script = new ValkeyScript("return 'private-source'");
        Assert.Equal(
            "script-result",
            (await owner.ExecuteScriptAsync(script, ["private-key"], ["private-value"], TestToken)).AsString()
        );
        Assert.Same(parent, Activity.Current);
        Assert.Equal(5, capture.Total("valkey.owner.operations"));
        Assert.Equal(2, capture.Total("valkey.owner.operation.failures"));
        Assert.Equal(1, capture.Total("valkey.owner.connection.attempts"));
        Assert.Equal(0, capture.Total("valkey.owner.connection.failures"));
        Assert.Equal(5, capture.Activities.Count);
        Assert.All(capture.Activities, activity => Assert.Equal(parent.Id, activity.ParentId));
        Assert.Equal(2, capture.Activities.Count(activity => activity.Status == ActivityStatusCode.Error));
        Assert.All(
            capture.Activities,
            activity =>
            {
                Assert.Equal("valkey", activity.GetTagItem("db.system.name"));
                Assert.Null(activity.StatusDescription);
                Assert.Empty(activity.Events);
                Assert.All(activity.TagObjects, tag => Assert.Contains(tag.Key, ActivityTags));
            }
        );
        Assert.All(
            capture.Measurements,
            measurement =>
            {
                Assert.All(
                    measurement.Tags,
                    tag =>
                    {
                        Assert.Contains(tag.Key, MetricTags);
                        Assert.Contains(
                            tag.Value,
                            new object[] { "connect", "command", "pipeline", "script", "server" }
                        );
                    }
                );
                if (measurement.Name.EndsWith(".duration", StringComparison.Ordinal))
                {
                    Assert.Equal("s", measurement.Unit);
                    Assert.True(measurement.Value >= 0);
                }
            }
        );
        capture.Collect();
        Assert.Equal(0, capture.LatestActive());
    }

    [Fact]
    public async Task RecoveryCountsAttemptsAndReconnectsButNotDuplicateLogicalOperations()
    {
        using var capture = new Capture();
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                if (index == 0)
                    session.Close();
                else
                {
                    await session.SendAsync("+value\r\n");
                    await session.ReadCommandAsync();
                }
            }
        );
        await using var owner = EnabledOwner(server);
        Assert.Equal(
            "value",
            (await owner.ExecuteRetryableAsync(new ValkeyCommand("GET", "private-key"), TestToken)).AsString()
        );
        // Publication and its telemetry can complete in either order across waiter continuations.
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        while (capture.Total("valkey.owner.reconnects") == 0)
            await Task.Delay(1, timeout.Token);
        await owner.DisposeAsync();
        Assert.Equal(1, capture.Total("valkey.owner.operations"));
        Assert.Equal(2, capture.Total("valkey.owner.connection.attempts"));
        Assert.Equal(0, capture.Total("valkey.owner.operation.failures"));
        Assert.Single(capture.Activities);
        Assert.Equal(1, capture.Total("valkey.owner.reconnects"));
    }

    [Fact]
    public async Task FailedConnectionAttemptsAndTerminalRejectionAreClassifiedWithoutErrorText()
    {
        using var capture = new Capture();
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync("-WRONGPASS private-auth-details\r\n");
        });
        await using var owner = EnabledOwner(server);
        await Assert.ThrowsAsync<ValkeyServerException>(() => owner.ConnectAsync(TestToken));
        await Assert.ThrowsAsync<ValkeyServerException>(() => owner.ConnectAsync(TestToken));
        Assert.Equal(1, capture.Total("valkey.owner.connection.attempts"));
        Assert.Equal(1, capture.Total("valkey.owner.connection.failures"));
        Assert.Equal(2, capture.Total("valkey.owner.operation.failures"));
        Assert.All(capture.Activities, activity => Assert.Equal("server", activity.GetTagItem("error.type")));
        capture.Collect();
        Assert.Equal(0, capture.LatestActive());
    }

    [Fact]
    public async Task ActiveGaugeIncludesWaitersAndReturnsToZeroAfterCancellationAndOverload()
    {
        using var capture = new Capture();
        var hello = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ReadCommandAsync();
            hello.SetResult();
            await session.ReadCommandAsync();
        });
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = server.ClientOptions(),
                EnableTelemetry = true,
                MaxConcurrentOperations = 1,
            }
        );
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var pending = owner.ConnectAsync(canceled.Token);
        await hello.Task.WaitAsync(TimeSpan.FromSeconds(10), TestToken);
        capture.Collect();
        Assert.Equal(1, capture.LatestActive());
        await Assert.ThrowsAsync<ValkeyCapacityException>(() => owner.ConnectAsync(TestToken));
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        capture.Collect();
        Assert.Equal(0, capture.LatestActive());
        Assert.Equal(2, capture.Total("valkey.owner.operation.failures"));
        Assert.Contains(capture.Activities, activity => Equals("capacity", activity.GetTagItem("error.type")));
        Assert.Contains(capture.Activities, activity => Equals("canceled", activity.GetTagItem("error.type")));
        await owner.DisposeAsync();
    }

    [Theory]
    [InlineData("sample")]
    [InlineData("start")]
    [InlineData("stop")]
    [InlineData("metric")]
    public async Task ThrowingListenersCannotChangeResultsOrCorruptTheAmbientActivity(string stage)
    {
        var invoked = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ValkeyDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
            {
                if (stage == "sample")
                {
                    Interlocked.Increment(ref invoked);
                    throw new InvalidOperationException("listener failed");
                }
                return ActivitySamplingResult.AllData;
            },
            ActivityStarted = _ =>
            {
                if (stage == "start")
                {
                    Interlocked.Increment(ref invoked);
                    throw new InvalidOperationException("listener failed");
                }
            },
            ActivityStopped = _ =>
            {
                if (stage == "stop")
                {
                    Interlocked.Increment(ref invoked);
                    throw new InvalidOperationException("listener failed");
                }
            },
        };
        ActivitySource.AddActivityListener(listener);
        using var meter = new MeterListener();
        meter.InstrumentPublished = (instrument, subscribed) =>
        {
            if (instrument.Meter.Name == ValkeyDiagnostics.MeterName)
                subscribed.EnableMeasurementEvents(instrument);
        };
        meter.SetMeasurementEventCallback<long>(
            (_, _, _, _) =>
            {
                if (stage == "metric")
                {
                    Interlocked.Increment(ref invoked);
                    throw new InvalidOperationException("listener failed");
                }
            }
        );
        meter.SetMeasurementEventCallback<double>(
            (_, _, _, _) =>
            {
                if (stage == "metric")
                {
                    Interlocked.Increment(ref invoked);
                    throw new InvalidOperationException("listener failed");
                }
            }
        );
        meter.Start();
        using var parent = new Activity("parent");
        parent.Start();
        await using var server = EchoServer();
        await using var owner = EnabledOwner(server);
        Assert.Equal("PONG", (await owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken)).AsString());
        Assert.Same(parent, Activity.Current);
        Assert.Equal("PONG", (await owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken)).AsString());
        Assert.Same(parent, Activity.Current);
        Assert.True(Volatile.Read(ref invoked) > 0);
        var failure = await Assert.ThrowsAsync<ValkeyServerException>(() =>
            owner.ExecuteAsync(new ValkeyCommand("FAIL"), TestToken)
        );
        Assert.Equal("ERR", failure.ErrorCode);
        Assert.Equal(ValkeyCommandDeliveryStatus.ReplyReceived, failure.DeliveryStatus);
        Assert.Same(parent, Activity.Current);
    }

    [Fact]
    public async Task MeasurementCallbacksCanInspectOwnerStateFromAnotherThread()
    {
        await using var server = EchoServer();
        await using var owner = EnabledOwner(server);
        var blocked = 0;
        var observed = 0;
        using var meter = new MeterListener();
        meter.InstrumentPublished = (instrument, subscribed) =>
        {
            if (instrument.Meter.Name == ValkeyDiagnostics.MeterName)
                subscribed.EnableMeasurementEvents(instrument);
        };
        meter.SetMeasurementEventCallback<long>(
            (_, _, _, _) =>
            {
                var read = Task.Run(() => owner.State, TestToken);
                if (!read.Wait(TimeSpan.FromSeconds(5), TestToken))
                    Interlocked.Increment(ref blocked);
                Interlocked.Increment(ref observed);
            }
        );
        meter.Start();
        Assert.Equal("PONG", (await owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken)).AsString());
        Assert.Equal(0, Volatile.Read(ref blocked));
        Assert.True(Volatile.Read(ref observed) > 0);
    }

    private static ValkeyConnectionOwner EnabledOwner(FakeValkeyServer server) =>
        new(
            new ValkeyConnectionOwnerOptions
            {
                Connection = server.ClientOptions(),
                EnableTelemetry = true,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(4),
            }
        );

    private static FakeValkeyServer EchoServer() =>
        FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            while (true)
            {
                var command = await session.ReadCommandAsync();
                await session.SendAsync(command[0] == "FAIL" ? "-ERR original failure\r\n" : "+PONG\r\n");
            }
        });

    private sealed record CapturedMeasurement(
        string Name,
        string? Unit,
        double Value,
        KeyValuePair<string, object?>[] Tags
    );

    private sealed class Capture : IDisposable
    {
        private readonly MeterListener _meter = new();
        private readonly ActivityListener _activity = new();
        internal ConcurrentQueue<CapturedMeasurement> Measurements { get; } = new();
        internal ConcurrentQueue<Activity> Activities { get; } = new();

        internal Capture(ActivitySamplingResult sampling = ActivitySamplingResult.AllData)
        {
            _meter.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == ValkeyDiagnostics.MeterName)
                    listener.EnableMeasurementEvents(instrument);
            };
            _meter.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) =>
                    Measurements.Enqueue(new(instrument.Name, instrument.Unit, value, tags.ToArray()))
            );
            _meter.SetMeasurementEventCallback<double>(
                (instrument, value, tags, _) =>
                    Measurements.Enqueue(new(instrument.Name, instrument.Unit, value, tags.ToArray()))
            );
            _meter.Start();
            _activity.ShouldListenTo = source => source.Name == ValkeyDiagnostics.ActivitySourceName;
            _activity.Sample = (ref ActivityCreationOptions<ActivityContext> _) => sampling;
            _activity.SampleUsingParentId = (ref ActivityCreationOptions<string> _) => sampling;
            _activity.ActivityStopped = Activities.Enqueue;
            ActivitySource.AddActivityListener(_activity);
        }

        internal double Total(string name) => Measurements.Where(m => m.Name == name).Sum(m => m.Value);

        internal void Collect() => _meter.RecordObservableInstruments();

        internal double LatestActive() => Measurements.Last(m => m.Name == "valkey.owner.operations.active").Value;

        public void Dispose()
        {
            _activity.Dispose();
            _meter.Dispose();
        }
    }
}
