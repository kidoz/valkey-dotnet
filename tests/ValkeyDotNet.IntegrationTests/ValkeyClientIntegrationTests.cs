using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed class ValkeyClientIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedServerRestartsRecoverWithoutOfflineWriteReplayOrResourceGrowth(ValkeyProtocol protocol)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_RESTART_TESTS") != "1")
            Assert.Skip(
                "Set VALKEYDOTNET_RUN_RESTART_TESTS=1 to create and restart an isolated disposable Docker server."
            );
        var version = Environment.GetEnvironmentVariable("VALKEYDOTNET_RESILIENCE_VERSION") ?? "9.1";
        var cycles = int.Parse(
            Environment.GetEnvironmentVariable("VALKEYDOTNET_RESILIENCE_CYCLES") ?? "3",
            CultureInfo.InvariantCulture
        );
        if (cycles is < 1 or > 100)
            throw new InvalidOperationException("VALKEYDOTNET_RESILIENCE_CYCLES must be between 1 and 100.");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(10));
        var token = deadline.Token;
        await using var server = new RestartValkeyServer(version);
        await server.StartNewAsync(token);
        var activeOperations = -1L;
        using var metrics = new MeterListener();
        metrics.InstrumentPublished = (instrument, listener) =>
        {
            if (
                instrument.Meter.Name == ValkeyDiagnostics.MeterName
                && instrument.Name == "valkey.owner.operations.active"
            )
                listener.EnableMeasurementEvents(instrument);
        };
        metrics.SetMeasurementEventCallback<long>(
            (_, value, _, _) => Interlocked.Exchange(ref activeOperations, value)
        );
        metrics.Start();
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    Protocol = protocol,
                    ClientName = "resilience-owner",
                    ConnectTimeout = TimeSpan.FromMilliseconds(300),
                    MaxPendingRequests = 16,
                },
                MaxConcurrentOperations = 16,
                MaxConnectAttempts = 2,
                EnableTelemetry = true,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(10),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(40),
            }
        );
        var script = new ValkeyScript("return ARGV[1]");
        var previousRun = ReadInfoValue(
            (await owner.ExecuteAsync(new ValkeyCommand("INFO", "server"), token)).AsString()!,
            "run_id"
        );
        Assert.Equal("warm", (await owner.ExecuteScriptAsync(script, [], ["warm"], token)).AsString());
        using var process = Process.GetCurrentProcess();
        long? baselineHeap = null;
        int? baselineHandles = null;
        for (var cycle = 0; cycle < cycles; cycle++)
        {
            await server.StopAsync(token);
            while (owner.State == ValkeyConnectionState.Connected)
                await Task.Delay(10, token);
            Assert.Equal(ValkeyConnectionState.Disconnected, owner.State);
            var offline = await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
                owner.ExecuteAsync(new ValkeyCommand("INCR", "offline-write"), token)
            );
            Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, offline.DeliveryStatus);
            Assert.Equal(ValkeyConnectionState.Disconnected, owner.State);
            await server.RestartAsync(token);
            var replies = await Task.WhenAll(
                Enumerable.Range(0, 16).Select(i => owner.ExecuteAsync(new ValkeyCommand("ECHO", i), token))
            );
            Assert.Equal(
                Enumerable.Range(0, 16).Select(i => i.ToString(CultureInfo.InvariantCulture)),
                replies.Select(reply => reply.AsString())
            );
            Assert.Equal("reloaded", (await owner.ExecuteScriptAsync(script, [], ["reloaded"], token)).AsString());
            Assert.True((await owner.ExecuteAsync(new ValkeyCommand("GET", "offline-write"), token)).IsNull);
            var info = (await owner.ExecuteAsync(new ValkeyCommand("INFO", "server"), token)).AsString()!;
            var currentRun = ReadInfoValue(info, "run_id");
            Assert.NotEqual(previousRun, currentRun);
            previousRun = currentRun;
            var clients = (await owner.ExecuteAsync(new ValkeyCommand("CLIENT", "LIST"), token)).AsString()!;
            Assert.Single(
                clients.Split('\n', StringSplitOptions.RemoveEmptyEntries),
                line => line.Split(' ').Contains("name=resilience-owner", StringComparer.Ordinal)
            );
            metrics.RecordObservableInstruments();
            Assert.Equal(0, Interlocked.Read(ref activeOperations));
            var heap = GC.GetTotalMemory(forceFullCollection: true);
            process.Refresh();
            var handles = process.HandleCount;
            baselineHeap ??= heap;
            baselineHandles ??= handles;
            Assert.True(
                heap <= baselineHeap.Value + 16 * 1024 * 1024,
                "Post-GC heap exceeded the restart smoke-test growth budget."
            );
            Assert.True(
                handles <= baselineHandles.Value + 32,
                "Open handles exceeded the restart smoke-test growth budget."
            );
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"version={ReadInfoValue(info, "valkey_version")} protocol={protocol} cycle={cycle + 1}/{cycles} "
                    + $"heap_bytes={heap} handles={handles} pool_threads={ThreadPool.ThreadCount} queued_work={ThreadPool.PendingWorkItemCount} owner_clients=1 active_operations=0"
            );
        }
        await owner.DisposeAsync();
        Assert.Equal(ValkeyConnectionState.Disposed, owner.State);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Completed restart experiment on isolated project {server.Project}; fixture cleanup follows."
        );
    }

    private static string ReadInfoValue(string info, string key)
    {
        var prefix = key + ":";
        return info.Split('\n')
            .Single(line => line.StartsWith(prefix, StringComparison.Ordinal))[prefix.Length..]
            .Trim();
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ConnectionOwnerRecoversFromLiveConnectionLossAndRestoresConfiguredState(ValkeyProtocol protocol)
    {
        var endpoint = GetEndpoint();
        var token = TestContext.Current.CancellationToken;
        var attempts = 0L;
        var reconnects = 0L;
        var activities = 0;
        var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var metrics = new MeterListener();
        metrics.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == ValkeyDiagnostics.MeterName)
                listener.EnableMeasurementEvents(instrument);
        };
        metrics.SetMeasurementEventCallback<long>(
            (instrument, value, _, _) =>
            {
                if (instrument.Name == "valkey.owner.connection.attempts")
                    Interlocked.Add(ref attempts, value);
                if (instrument.Name == "valkey.owner.reconnects" && Interlocked.Add(ref reconnects, value) == 3)
                    recovered.TrySetResult();
            }
        );
        metrics.Start();
        using var tracing = new ActivityListener
        {
            ShouldListenTo = source => source.Name == ValkeyDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = _ => Interlocked.Increment(ref activities),
        };
        ActivitySource.AddActivityListener(tracing);
        var connection = new ValkeyClientOptions
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            Protocol = protocol,
            ClientName = "valkey-owner-recovery",
            Database = 2,
        };
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions { Connection = connection, EnableTelemetry = true }
        );
        await using var control = await ValkeyClient.ConnectAsync(connection, token);
        var script = new ValkeyScript("return ARGV[1]");
        var previousId = (await owner.ExecuteAsync(new ValkeyCommand("CLIENT", "ID"), token)).AsInt64();
        for (var cycle = 0; cycle < 3; cycle++)
        {
            Assert.Equal("value", (await owner.ExecuteScriptAsync(script, [], ["value"], token)).AsString());
            Assert.Equal(
                1,
                (await control.ExecuteAsync(new ValkeyCommand("CLIENT", "KILL", "ID", previousId), token)).AsInt64()
            );
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
            deadline.CancelAfter(TimeSpan.FromSeconds(10));
            while (owner.State == ValkeyConnectionState.Connected)
                await Task.Delay(10, deadline.Token);
            Assert.Equal(ValkeyConnectionState.Disconnected, owner.State);
            await control.ExecuteAsync(new ValkeyCommand("SCRIPT", "FLUSH"), token);
            var results = await owner.ExecutePipelineWithDeadlineAsync(
                [
                    new ValkeyCommand("CLIENT", "ID"),
                    new ValkeyCommand("CLIENT", "GETNAME"),
                    new ValkeyCommand("CLIENT", "INFO"),
                ],
                TimeSpan.FromSeconds(10),
                token
            );
            var currentId = results[0].AsInt64();
            Assert.NotEqual(previousId, currentId);
            Assert.Equal("valkey-owner-recovery", results[1].AsString());
            Assert.Contains("db=2", results[2].AsString(), StringComparison.Ordinal);
            Assert.Contains(
                protocol == ValkeyProtocol.Resp2 ? "resp=2" : "resp=3",
                results[2].AsString(),
                StringComparison.Ordinal
            );
            Assert.Equal(
                "reloaded",
                (
                    await owner.ExecuteScriptWithDeadlineAsync(
                        script,
                        [],
                        ["reloaded"],
                        TimeSpan.FromSeconds(10),
                        token
                    )
                ).AsString()
            );
            previousId = currentId;
        }
        await recovered.Task.WaitAsync(TimeSpan.FromSeconds(10), token);
        Assert.Equal(4, Interlocked.Read(ref attempts));
        Assert.Equal(3, Interlocked.Read(ref reconnects));
        Assert.Equal(10, Volatile.Read(ref activities));
    }

    private static (string Host, int Port) GetEndpoint()
    {
        var endpoint = Environment.GetEnvironmentVariable("VALKEYDOTNET_ENDPOINT");
        if (string.IsNullOrWhiteSpace(endpoint))
            Assert.Skip("Set VALKEYDOTNET_ENDPOINT to run the live Valkey integration test.");

        var parts = endpoint.Split(':', 2);
        return (parts[0], parts.Length == 2 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 6379);
    }

    [Fact]
    public async Task ClientRoundTripsAgainstLiveServer()
    {
        var endpoint = GetEndpoint();
        var options = new ValkeyClientOptions
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            ClientName = "valkey-dotnet-tests",
        };
        await using var client = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);
        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));

        var key = "valkey-dotnet:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        Assert.True(
            await client.SetStringAsync(
                key,
                "works",
                TimeSpan.FromSeconds(30),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        Assert.Equal("works", await client.GetStringAsync(key, TestContext.Current.CancellationToken));
        Assert.Equal(1L, await client.DeleteAsync([key], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ResponseDrainTimeoutRetiresABlockedLiveConnectionAndAllowsReplacement()
    {
        var endpoint = GetEndpoint();
        var options = new ValkeyClientOptions
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            ClientName = "valkey-dotnet-stall-test",
            ResponseDrainTimeout = TimeSpan.FromMilliseconds(500),
        };
        await using var client = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);
        var key = "valkey-dotnet:blocked:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

        var blocked = client.ExecuteWithDeadlineAsync(
            new ValkeyCommand("BLPOP", key, 0),
            TimeSpan.FromMilliseconds(500),
            TestContext.Current.CancellationToken
        );
        var deadlineFailure = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(async () => await blocked);
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, deadlineFailure.DeliveryStatus);

        var connectionFailure = await Assert.ThrowsAsync<ValkeyConnectionException>(async () =>
            await client.PingAsync(TestContext.Current.CancellationToken)
        );
        Assert.IsType<TimeoutException>(connectionFailure.InnerException);

        await using var replacement = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);
        Assert.Equal("PONG", await replacement.PingAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ScriptsPreserveBinaryDataAndRecoverAfterFlushAndConnectionReplacement(ValkeyProtocol protocol)
    {
        var endpoint = GetEndpoint();
        var options = new ValkeyClientOptions
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            Protocol = protocol,
        };
        await using var client = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);
        var key = "valkey-dotnet:script:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        var increment = new ValkeyScript(
            "local n = redis.call('INCR', KEYS[1]); redis.call('PEXPIRE', KEYS[1], 60000); return n"
        );
        var echo = new ValkeyScript("return {KEYS[1], ARGV[1]}");
        byte[] binaryKey = [0, 255, 13, 10];
        byte[] binaryValue = [254, 0, 10, 13];
        try
        {
            var echoed = (
                await client.ExecuteScriptAsync(echo, [binaryKey], [binaryValue], TestContext.Current.CancellationToken)
            ).AsArray();
            Assert.Equal(binaryKey, echoed[0].AsBytes().ToArray());
            Assert.Equal(binaryValue, echoed[1].AsBytes().ToArray());
            for (var round = 0; round < 2; round++)
            {
                await client.ExecuteAsync(new ValkeyCommand("SCRIPT", "FLUSH"), TestContext.Current.CancellationToken);
                var replies = await Task.WhenAll(
                    Enumerable
                        .Range(0, 16)
                        .Select(_ =>
                            client.ExecuteScriptAsync(increment, [key], [], TestContext.Current.CancellationToken)
                        )
                );
                Assert.Equal(
                    Enumerable.Range(round * 16 + 1, 16).Select(i => (long)i),
                    replies.Select(r => r.AsInt64()).Order()
                );
            }
            await using var replacement = await ValkeyClient.ConnectAsync(
                options,
                TestContext.Current.CancellationToken
            );
            await replacement.ExecuteAsync(new ValkeyCommand("SCRIPT", "FLUSH"), TestContext.Current.CancellationToken);
            Assert.Equal(
                33,
                (
                    await replacement.ExecuteScriptWithDeadlineAsync(
                        increment,
                        [key],
                        [],
                        TimeSpan.FromSeconds(5),
                        TestContext.Current.CancellationToken
                    )
                ).AsInt64()
            );
            var pipelined = await replacement.ExecutePipelineAsync(
                [echo.CreateCommand([binaryKey], [binaryValue])],
                TestContext.Current.CancellationToken
            );
            Assert.Equal(binaryValue, pipelined[0].AsArray()[1].AsBytes().ToArray());
        }
        finally
        {
            await client.DeleteAsync([key], TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task LeaseScriptsRejectStaleOwnersAndAtomicallyExtendAndRelease()
    {
        var endpoint = GetEndpoint();
        await using var client = await ValkeyClient.ConnectAsync(
            new ValkeyClientOptions { Host = endpoint.Host, Port = endpoint.Port },
            TestContext.Current.CancellationToken
        );
        var key = "valkey-dotnet:lease:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        var release = new ValkeyScript(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
        );
        var extend = new ValkeyScript(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end"
        );
        byte[] oldOwner = [0, 255, 1];
        byte[] newOwner = [0, 255, 2];
        try
        {
            Assert.True(
                await client.SetAsync(
                    key,
                    oldOwner,
                    TimeSpan.FromMinutes(1),
                    onlyIfNotExists: true,
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            Assert.False(
                await client.SetAsync(
                    key,
                    newOwner,
                    TimeSpan.FromMinutes(1),
                    onlyIfNotExists: true,
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            // Expire the old lease before acquiring a replacement owner on the same key.
            await client.ExecuteAsync(new ValkeyCommand("PEXPIRE", key, 0), TestContext.Current.CancellationToken);
            Assert.True(
                await client.SetAsync(
                    key,
                    newOwner,
                    TimeSpan.FromMinutes(1),
                    onlyIfNotExists: true,
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            Assert.Equal(
                0,
                (
                    await client.ExecuteScriptAsync(release, [key], [oldOwner], TestContext.Current.CancellationToken)
                ).AsInt64()
            );
            Assert.Equal(
                0,
                (
                    await client.ExecuteScriptAsync(
                        extend,
                        [key],
                        [oldOwner, 120000],
                        TestContext.Current.CancellationToken
                    )
                ).AsInt64()
            );
            Assert.Equal(
                1,
                (
                    await client.ExecuteScriptAsync(
                        extend,
                        [key],
                        [newOwner, 120000],
                        TestContext.Current.CancellationToken
                    )
                ).AsInt64()
            );
            Assert.InRange(
                (
                    await client.ExecuteAsync(new ValkeyCommand("PTTL", key), TestContext.Current.CancellationToken)
                ).AsInt64(),
                60001,
                120000
            );
            Assert.Equal(newOwner, await client.GetAsync(key, TestContext.Current.CancellationToken));
            Assert.Equal(
                1,
                (
                    await client.ExecuteScriptAsync(release, [key], [newOwner], TestContext.Current.CancellationToken)
                ).AsInt64()
            );
            Assert.Null(await client.GetAsync(key, TestContext.Current.CancellationToken));
        }
        finally
        {
            await client.DeleteAsync([key], TestContext.Current.CancellationToken);
        }
    }
}
