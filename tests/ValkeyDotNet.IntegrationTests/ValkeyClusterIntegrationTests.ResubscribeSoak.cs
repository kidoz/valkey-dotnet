using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedResubscribeSoakPreservesStreamsAndBoundsSettledResources(ValkeyProtocol protocol)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_RESUBSCRIBE_SOAK") != "1")
        {
            Assert.Skip("Set VALKEYDOTNET_RUN_RESUBSCRIBE_SOAK=1 to soak a fresh owned disposable Docker cluster.");
        }
        var cycles = ResubscribeSoakSettings.ParseCycles(
            Environment.GetEnvironmentVariable("VALKEYDOTNET_RESUBSCRIBE_CYCLES")
        );
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(15));
        var token = deadline.Token;
        await using var cluster = new MigrationValkeyCluster();
        var output = TestContext.Current.TestOutputHelper;
        output?.WriteLine(
            $"Owned project: {cluster.Project}; protocol={protocol}; warmup={ResubscribeSoakSettings.WarmupCycles}; measured_cycles={cycles}; runtime={RuntimeInformation.FrameworkDescription}; os={RuntimeInformation.OSDescription}; arch={RuntimeInformation.ProcessArchitecture}"
        );
        await cluster.StartNewAsync(token);
        await using var node0 = await ValkeyClient.ConnectAsync(cluster.NodeOptions(0, protocol), token);
        await using var node1 = await ValkeyClient.ConnectAsync(cluster.NodeOptions(1, protocol), token);
        await using var node2 = await ValkeyClient.ConnectAsync(cluster.NodeOptions(2, protocol), token);
        ValkeyClient[] inspectors = [node0, node1, node2];
        for (var node = 0; node < inspectors.Length; node++)
        {
            output?.WriteLine(
                (await inspectors[node].ExecuteAsync(new ValkeyCommand("INFO", "SERVER"), token)).AsString()!
            );
        }
        var tag = FindKey(cluster.Project, 0, 5460);
        byte[] channel = [.. Encoding.UTF8.GetBytes("{" + tag + "}"), 0, 255, 13, 10];
        var stationary = Encoding.UTF8.GetBytes(FindKey(cluster.Project + "-stationary", 10923, 16383));
        var slot = ValkeyClusterClient.GetHashSlot(channel);
        await using var publisher = await ValkeyClusterClient.ConnectAsync(cluster.Options(protocol), token);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = cluster.Options(protocol),
                EnableTopologyRecovery = true,
                MaxSubscriptions = 2,
                QueueCapacity = 8,
                MaxReconnectAttempts = 10,
                RecoveryTimeout = TimeSpan.FromSeconds(30),
            },
            token
        );
        await using var moving = await subscriber.SubscribeAsync(channel, token);
        await using var untouched = await subscriber.SubscribeAsync(stationary, token);
        await using var messages = moving.ReadAllAsync(token).GetAsyncEnumerator(token);
        await using var stationaryMessages = untouched.ReadAllAsync(token).GetAsyncEnumerator(token);
        var completion = moving.Completion;
        var stationaryCompletion = untouched.Completion;
        await VerifyDeliveryAsync(publisher, channel, messages, 0, token);
        await VerifyDeliveryAsync(publisher, stationary, stationaryMessages, 0, token);
        using var process = Process.GetCurrentProcess();
        var elapsed = Stopwatch.StartNew();
        long baselineHeap = 0;
        long maximumHeap = 0;
        long finalHeap = 0;
        var baselineHandles = 0;
        var maximumHandles = 0;
        for (var cycle = 1; cycle <= cycles + ResubscribeSoakSettings.WarmupCycles; cycle++)
        {
            var target = cycle % 2;
            var cycleElapsed = Stopwatch.StartNew();
            await cluster.MoveEmptySlotAsync(slot, 1 - target, target, token);
            using var recovery = CancellationTokenSource.CreateLinkedTokenSource(token);
            recovery.CancelAfter(TimeSpan.FromSeconds(35));
            while (moving.SuccessfulRelocations < cycle || !moving.IsConnected)
            {
                Assert.Null(moving.Failure);
                Assert.False(completion.IsCompleted);
                await Task.Delay(20, recovery.Token);
            }
            Assert.Same(completion, moving.Completion);
            Assert.Same(stationaryCompletion, untouched.Completion);
            Assert.False(completion.IsCompleted);
            Assert.False(stationaryCompletion.IsCompleted);
            Assert.Null(moving.Failure);
            Assert.Null(untouched.Failure);
            Assert.Equal(cycle, moving.ConnectionLosses);
            Assert.Equal(cycle, moving.SuccessfulReconnects);
            Assert.Equal(cycle, moving.SuccessfulRelocations);
            Assert.InRange(moving.ReconnectAttempts, cycle, cycle * 10L);
            Assert.Equal(0, untouched.ConnectionLosses);
            Assert.Equal(0, untouched.ReconnectAttempts);
            Assert.Equal(2, subscriber.SubscriptionCount);
            await VerifyDeliveryAsync(publisher, channel, messages, cycle, token);
            await VerifyDeliveryAsync(publisher, stationary, stationaryMessages, cycle, token);
            Assert.Equal(0, moving.DroppedMessages);
            Assert.Equal(0, untouched.DroppedMessages);
            await VerifySettledSoakConnectionsAsync(inspectors, cluster.Project, channel, stationary, target, token);
            if (cycle >= ResubscribeSoakSettings.WarmupCycles)
            {
                // This includes the test runner and Docker subprocess orchestration, not just the library.
                finalHeap = GC.GetTotalMemory(forceFullCollection: true);
                process.Refresh();
                var handles = process.HandleCount;
                if (cycle == ResubscribeSoakSettings.WarmupCycles)
                {
                    baselineHeap = finalHeap;
                    baselineHandles = handles;
                }
                maximumHeap = Math.Max(maximumHeap, finalHeap);
                maximumHandles = Math.Max(maximumHandles, handles);
                Assert.True(
                    finalHeap <= baselineHeap + ResubscribeSoakSettings.HeapGrowthBudget,
                    "Post-GC heap exceeded the 16 MiB bounded-soak growth budget."
                );
                if (baselineHandles > 0 && handles > 0)
                {
                    Assert.True(
                        handles <= baselineHandles + ResubscribeSoakSettings.HandleGrowthBudget,
                        "Process handles exceeded the bounded-soak growth budget."
                    );
                }
                output?.WriteLine(
                    $"sample={cycle - ResubscribeSoakSettings.WarmupCycles}/{cycles}; heap_bytes={finalHeap}; handles={(handles > 0 ? handles.ToString(System.Globalization.CultureInfo.InvariantCulture) : "unsupported")}; working_set_bytes={process.WorkingSet64}; pool_threads={ThreadPool.ThreadCount}; queued_work={ThreadPool.PendingWorkItemCount}"
                );
            }
            output?.WriteLine(
                $"cycle={cycle}; target={target}; cycle_ms={cycleElapsed.ElapsedMilliseconds}; elapsed_ms={elapsed.ElapsedMilliseconds}; named_clients=9; shard_registrations=2; losses={moving.ConnectionLosses}; attempts={moving.ReconnectAttempts}; relocations={moving.SuccessfulRelocations}; dropped=0"
            );
        }
        output?.WriteLine(
            $"Soak summary: measured_cycles={cycles}; warmup={ResubscribeSoakSettings.WarmupCycles}; elapsed_ms={elapsed.ElapsedMilliseconds}; baseline_heap_bytes={baselineHeap}; max_heap_bytes={maximumHeap}; final_heap_bytes={finalHeap}; baseline_handles={baselineHandles}; max_handles={maximumHandles} (zero is unsupported)"
        );
        await moving.UnsubscribeAsync(token);
        await untouched.UnsubscribeAsync(token);
        Assert.False(await messages.MoveNextAsync());
        Assert.False(await stationaryMessages.MoveNextAsync());
        Assert.Equal(0, subscriber.SubscriptionCount);
        await subscriber.DisposeAsync();
        await publisher.DisposeAsync();
        foreach (var inspector in inspectors)
        {
            await inspector.DisposeAsync();
        }
        for (var node = 0; node < 3; node++)
        {
            Assert.Equal("0", await cluster.CommandAsync(node, ["DBSIZE"], token));
            Assert.Equal("", await cluster.CommandAsync(node, ["PUBSUB", "SHARDCHANNELS"], token));
            Assert.Equal(
                0,
                ResubscribeSoakSettings.CountNamedClients(
                    await cluster.CommandAsync(node, ["CLIENT", "LIST"], token),
                    cluster.Project
                )
            );
        }
        await cluster.DisposeAsync();
        output?.WriteLine($"Verified cleanup of owned containers/network: {cluster.Project}");
    }

    private static async Task VerifySettledSoakConnectionsAsync(
        ValkeyClient[] inspectors,
        string project,
        byte[] channel,
        byte[] stationary,
        int target,
        CancellationToken token
    )
    {
        // Close acknowledgement and peer-side socket removal can race; allow bounded settlement.
        using var settle = CancellationTokenSource.CreateLinkedTokenSource(token);
        settle.CancelAfter(TimeSpan.FromSeconds(5));
        for (var node = 0; node < inspectors.Length; node++)
        {
            // One inspector and publisher per primary, a retained discovery seed on node 0,
            // one moving shard socket on its owner, and one stationary shard socket on node 2.
            var expected = 2 + (node == 0 ? 1 : 0) + (node == target || node == 2 ? 1 : 0);
            int actual;
            do
            {
                var clients = (
                    await inspectors[node].ExecuteAsync(new ValkeyCommand("CLIENT", "LIST"), settle.Token)
                ).AsString()!;
                actual = ResubscribeSoakSettings.CountNamedClients(clients, project);
                if (actual != expected)
                {
                    await Task.Delay(20, settle.Token);
                }
            } while (actual != expected);
            var registrations = (
                await inspectors[node]
                    .ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel, stationary), settle.Token)
            ).AsArray();
            Assert.Equal(4, registrations.Count);
            Assert.Equal(channel, registrations[0].AsBytes().ToArray());
            Assert.Equal(node == target ? 1 : 0, registrations[1].AsInt64());
            Assert.Equal(stationary, registrations[2].AsBytes().ToArray());
            Assert.Equal(node == 2 ? 1 : 0, registrations[3].AsInt64());
        }
    }
}
