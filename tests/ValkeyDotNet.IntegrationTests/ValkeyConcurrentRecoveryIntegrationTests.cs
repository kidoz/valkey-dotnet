using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using ValkeyDotNet.Benchmarks;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed class ValkeyConcurrentRecoveryIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedConcurrentRecoveryPreservesRepliesStreamsAndResourceBounds(ValkeyProtocol protocol)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_CONCURRENT_RECOVERY") != "1")
        {
            Assert.Skip("Opt in only to concurrent connection kills on a new owned disposable Docker server.");
        }
        var cycles = ConcurrentRecoverySettings.ParseCycles(
            Environment.GetEnvironmentVariable("VALKEYDOTNET_CONCURRENT_RECOVERY_CYCLES")
        );
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var server = new OwnedBenchmarkServer();
        await server.StartAsync(token);
        var output = TestContext.Current.TestOutputHelper;
        output?.WriteLine(
            $"Owned project={server.Project}; protocol={protocol}; warmup=2; cycles={cycles}; owners=4; subscribers=4; callers_per_owner=16; runtime={RuntimeInformation.FrameworkDescription}; os={RuntimeInformation.OSDescription}; arch={RuntimeInformation.ProcessArchitecture}; image={server.ImageId}"
        );
        await using var control = await ValkeyClient.ConnectAsync(Options("control"), token);
        await using var sampler = await ValkeyClient.ConnectAsync(Options("sampler"), token);
        output?.WriteLine((await control.ExecuteAsync(new ValkeyCommand("INFO", "SERVER"), token)).AsString()!);
        var owners = new List<ValkeyConnectionOwner>();
        var subscribers = new List<ValkeySubscriber>();
        var handles = new List<ValkeySubscription>();
        var streams = new List<IAsyncEnumerator<ValkeyPubSubMessage>>();
        var channels = Enumerable
            .Range(0, ConcurrentRecoverySettings.Participants)
            .Select(index =>
                Encoding.UTF8.GetBytes(ConcurrentRecoverySettings.Name(server.Project, "subscriber", index) + ":\0\r\n")
            )
            .ToArray();
        var resources = new List<IAsyncDisposable>();
        using var probe = new RecoveryResourceProbe();
        try
        {
            for (var index = 0; index < ConcurrentRecoverySettings.Participants; index++)
            {
                var owner = new ValkeyConnectionOwner(
                    new ValkeyConnectionOwnerOptions
                    {
                        Connection = Options("owner", index),
                        EnableTelemetry = true,
                        MaxConcurrentOperations = ConcurrentRecoverySettings.CallersPerOwner,
                        MaxConnectAttempts = 1,
                    }
                );
                resources.Add(owner);
                owners.Add(owner);
                await owner.ConnectAsync(token);
                var subscriber = await ValkeySubscriber.ConnectAsync(
                    new ValkeySubscriberOptions
                    {
                        Connection = Options("subscriber", index),
                        EnableReconnect = true,
                        QueueCapacity = 8,
                        MaxSubscriptions = 1,
                        MaxConcurrentOperations = 1,
                        MaxReconnectAttempts = 1,
                        InitialReconnectDelay = TimeSpan.FromSeconds(1),
                        MaxReconnectDelay = TimeSpan.FromSeconds(1),
                        RecoveryTimeout = TimeSpan.FromSeconds(10),
                    },
                    token
                );
                resources.Add(subscriber);
                subscribers.Add(subscriber);
                var handle = await subscriber.SubscribeAsync(channels[index], token);
                handles.Add(handle);
                streams.Add(handle.ReadAllAsync(token).GetAsyncEnumerator(token));
            }
            await VerifyDeliveryAsync(0, token);
            var initial = ConcurrentRecoverySettings.SelectTargets(
                await ClientsAsync(control, token),
                server.Project,
                protocol
            );
            Assert.Equal(8, initial.Length);
            probe.Capture(ConcurrentRecoverySettings.ExpectedClients);
            long baselineHeap = 0;
            int? baselineHandles = null;
            long maximumSettledHeap = 0;
            for (var cycle = 1; cycle <= cycles + ConcurrentRecoverySettings.WarmupCycles; cycle++)
            {
                using var cycleDeadline = CancellationTokenSource.CreateLinkedTokenSource(token);
                cycleDeadline.CancelAfter(TimeSpan.FromSeconds(15));
                var cycleToken = cycleDeadline.Token;
                // Configuration and exact names/IDs/settings are rechecked immediately before each burst.
                await server.VerifyRunningOwnershipAsync(cycleToken);
                var oldIds = ConcurrentRecoverySettings.SelectTargets(
                    await ClientsAsync(control, cycleToken),
                    server.Project,
                    protocol
                );
                var acceptedBefore = await AcceptedAsync(cycleToken);
                using var sampling = CancellationTokenSource.CreateLinkedTokenSource(cycleToken);
                var sampleTask = SampleAsync(sampling.Token);
                Task? burst = null;
                try
                {
                    var rechecked = ConcurrentRecoverySettings.SelectTargets(
                        await ClientsAsync(control, cycleToken),
                        server.Project,
                        protocol
                    );
                    Assert.Equal(oldIds, rechecked);
                    foreach (
                        var reply in await control.ExecutePipelineAsync(
                            oldIds.Select(id => new ValkeyCommand("CLIENT", "KILL", "ID", id)),
                            cycleToken
                        )
                    )
                    {
                        Assert.Equal(1, reply.AsInt64());
                    }
                    await UntilAsync(
                        () =>
                            owners.All(owner => owner.State == ValkeyConnectionState.Disconnected)
                            && subscribers.All(subscriber =>
                                subscriber.ConnectionLosses == cycle && subscriber.SuccessfulReconnects == cycle - 1
                            ),
                        cycleToken
                    );
                    // All four subscriber recovery windows overlap before any one completes.
                    var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    var commands = Enumerable
                        .Range(0, owners.Count)
                        .SelectMany(ownerIndex =>
                            Enumerable
                                .Range(0, ConcurrentRecoverySettings.CallersPerOwner)
                                .Select(caller => EchoAsync(ownerIndex, caller, cycle, gate.Task, cycleToken))
                        )
                        .ToArray();
                    Assert.Equal(64, commands.Count(task => !task.IsCompleted));
                    burst = Task.WhenAll(commands);
                    gate.SetResult();
                    await burst;
                    await UntilAsync(
                        () => subscribers.All(subscriber => subscriber.SuccessfulReconnects == cycle),
                        cycleToken
                    );
                    await VerifyDeliveryAsync(cycle, cycleToken);
                    var newIds = ConcurrentRecoverySettings.SelectTargets(
                        await ClientsAsync(control, cycleToken),
                        server.Project,
                        protocol
                    );
                    Assert.Empty(oldIds.Intersect(newIds));
                    Assert.Equal(8, await AcceptedAsync(cycleToken) - acceptedBefore);
                    Assert.All(owners, owner => Assert.Equal(ValkeyConnectionState.Connected, owner.State));
                    foreach (var subscriber in subscribers)
                    {
                        Assert.Null(subscriber.Failure);
                        Assert.Equal(cycle, subscriber.ConnectionLosses);
                        Assert.Equal(cycle, subscriber.ReconnectAttempts);
                        Assert.Equal(0, subscriber.DroppedMessages);
                    }
                }
                finally
                {
                    await cycleDeadline.CancelAsync();
                    await sampling.CancelAsync();
                    try
                    {
                        if (burst is not null)
                        {
                            await burst;
                        }
                    }
                    finally
                    {
                        await sampleTask;
                    }
                }
                Assert.Equal(0, probe.ActiveOperations());
                var heap = GC.GetTotalMemory(forceFullCollection: true);
                using var process = System.Diagnostics.Process.GetCurrentProcess();
                var count = process.HandleCount;
                int? handlesNow = count > 0 ? count : null;
                if (cycle == ConcurrentRecoverySettings.WarmupCycles)
                {
                    baselineHeap = heap;
                    baselineHandles = handlesNow;
                }
                if (cycle >= ConcurrentRecoverySettings.WarmupCycles)
                {
                    maximumSettledHeap = Math.Max(maximumSettledHeap, heap);
                    Assert.True(
                        heap <= baselineHeap + ConcurrentRecoverySettings.HeapGrowthBudget,
                        "Post-GC heap grew beyond the 16 MiB smoke budget."
                    );
                    if (baselineHandles is not null && handlesNow is not null)
                    {
                        Assert.True(
                            handlesNow <= baselineHandles + 32,
                            "Settled process handles exceeded the growth budget."
                        );
                    }
                }
                output?.WriteLine(
                    $"cycle={cycle}; killed=8; accepted_replacements=8; overlapping_subscriber_recoveries=4; replies=64; deliveries=4; settled_clients=10; active_owner_operations=0; outstanding_burst_tasks=0; heap_bytes={heap}; handles={(handlesNow?.ToString(CultureInfo.InvariantCulture) ?? "unsupported")}"
                );
            }
            output?.WriteLine(
                $"Resource summary: samples={probe.Samples}; max_sampled_clients={probe.MaximumClients}; max_sampled_active_owner_operations={probe.MaximumActiveOwnerOperations}; max_scheduled_burst_tasks=64; baseline_heap={baselineHeap}; max_settled_heap={maximumSettledHeap}; max_sampled_live_heap={probe.MaximumLiveHeap}; max_sampled_working_set={probe.MaximumWorkingSet}; max_sampled_handles={(probe.MaximumHandles?.ToString(CultureInfo.InvariantCulture) ?? "unsupported")}; max_pool_threads={probe.MaximumPoolThreads}; max_queued_pool_work={probe.MaximumQueuedPoolWork}"
            );
            foreach (var handle in handles)
            {
                Assert.Equal(0, handle.DroppedMessages);
                await handle.UnsubscribeAsync(token);
            }
            foreach (var stream in streams)
            {
                Assert.False(await stream.MoveNextAsync());
            }
        }
        finally
        {
            try
            {
                await Task.WhenAll(streams.Select(async stream => await stream.DisposeAsync()));
            }
            finally
            {
                await Task.WhenAll(resources.Select(async resource => await resource.DisposeAsync()));
            }
        }
        await UntilAsync(async () => (await ClientsAsync(control, token)).Length == 2, token);
        Assert.Empty((await control.ExecuteAsync(new ValkeyCommand("PUBSUB", "CHANNELS"), token)).AsArray());
        Assert.Equal(0, (await control.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64());
        Assert.Equal("PONG", await control.PingAsync(token));
        await sampler.DisposeAsync();
        await control.DisposeAsync();
        await server.DisposeAsync();
        output?.WriteLine("Verified owned-container cleanup; no existing endpoint or network was modified.");

        ValkeyClientOptions Options(string role, int index = 0) =>
            new()
            {
                Host = "127.0.0.1",
                Port = server.Port,
                Protocol = protocol,
                ClientName = ConcurrentRecoverySettings.Name(server.Project, role, index),
                Database = role is "owner" or "subscriber" ? 1 : 0,
                ConnectTimeout = TimeSpan.FromSeconds(2),
                MaxPendingRequests = 64,
                MaxResponseBytes = 65536,
                MaxResponseElements = 1024,
                MaxNestingDepth = 16,
            };

        async Task<long> AcceptedAsync(CancellationToken ct) =>
            ConcurrentRecoverySettings.ConnectionsReceived(
                (await control.ExecuteAsync(new ValkeyCommand("INFO", "STATS"), ct)).AsString()!
            );

        async Task EchoAsync(int ownerIndex, int caller, int cycle, Task gate, CancellationToken ct)
        {
            byte[] payload = [(byte)ownerIndex, (byte)caller, (byte)cycle, 0, 255, 13, 10];
            await gate.WaitAsync(ct);
            var reply = await owners[ownerIndex]
                .ExecuteWithDeadlineAsync(new ValkeyCommand("ECHO", payload), TimeSpan.FromSeconds(5), ct);
            Assert.Equal(payload, reply.AsBytes().ToArray());
        }

        async Task VerifyDeliveryAsync(int cycle, CancellationToken ct)
        {
            for (var index = 0; index < subscribers.Count; index++)
            {
                byte[] payload = [(byte)cycle, (byte)index, 0, 255, 13, 10];
                Assert.Equal(
                    1,
                    (await control.ExecuteAsync(new ValkeyCommand("PUBLISH", channels[index], payload), ct)).AsInt64()
                );
                var next = streams[index].MoveNextAsync().AsTask();
                try
                {
                    Assert.True(await next.WaitAsync(ct));
                }
                catch
                {
                    await deadline.CancelAsync();
                    await ((Task)next).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                    throw;
                }
                Assert.Equal(channels[index], streams[index].Current.Channel.ToArray());
                Assert.Equal(payload, streams[index].Current.Payload.ToArray());
            }
        }

        async Task SampleAsync(CancellationToken ct)
        {
            try
            {
                while (true)
                {
                    // Cycle shutdown joins this bounded request instead of canceling its socket I/O.
                    probe.Capture((await ClientsAsync(sampler, token)).Length);
                    await Task.Delay(20, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            catch
            {
                await deadline.CancelAsync();
                throw;
            }
        }
    }

    private static async Task<RecoveryClient[]> ClientsAsync(ValkeyClient client, CancellationToken token) =>
        ConcurrentRecoverySettings.ParseClients(
            (
                await client.ExecuteWithDeadlineAsync(
                    new ValkeyCommand("CLIENT", "LIST"),
                    TimeSpan.FromSeconds(2),
                    token
                )
            ).AsString()!
        );

    private static Task UntilAsync(Func<bool> predicate, CancellationToken token) =>
        UntilAsync(() => Task.FromResult(predicate()), token);

    private static async Task UntilAsync(Func<Task<bool>> predicate, CancellationToken token)
    {
        while (!await predicate())
        {
            await Task.Delay(10, token);
        }
    }
}
