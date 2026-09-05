using System.Diagnostics;
using System.Text;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedSlotMigrationPreservesShardedHandleAndStream(ValkeyProtocol protocol)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_MIGRATION_TESTS") != "1")
        {
            Assert.Skip(
                "Set VALKEYDOTNET_RUN_MIGRATION_TESTS=1 to create and migrate an owned disposable Docker cluster."
            );
        }
        var cycles = MigrationValkeyCluster.ParseCycles(
            Environment.GetEnvironmentVariable("VALKEYDOTNET_MIGRATION_CYCLES")
        );
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var cluster = new MigrationValkeyCluster();
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Owned project: {cluster.Project}; protocol={protocol}; cycles={cycles}"
        );
        await cluster.StartNewAsync(token);
        for (var node = 0; node < 3; node++)
        {
            TestContext.Current.TestOutputHelper?.WriteLine(
                await cluster.CommandAsync(node, ["INFO", "SERVER"], token)
            );
        }
        var tag = FindKey(cluster.Project, 0, 5460);
        byte[] channel = [.. Encoding.UTF8.GetBytes("{" + tag + "}"), 0, 255, 13, 10];
        var stationary = FindKey(cluster.Project + "-stationary", 10923, 16383);
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
        await using var handle = await subscriber.SubscribeAsync(channel, token);
        await using var untouched = await subscriber.SubscribeAsync(stationary, token);
        await using var messages = handle.ReadAllAsync(token).GetAsyncEnumerator(token);
        await using var stationaryMessages = untouched.ReadAllAsync(token).GetAsyncEnumerator(token);
        var completion = handle.Completion;
        await VerifyDeliveryAsync(publisher, channel, messages, 0, token);
        await VerifyDeliveryAsync(publisher, Encoding.UTF8.GetBytes(stationary), stationaryMessages, 0, token);
        for (var cycle = 1; cycle <= cycles; cycle++)
        {
            var source = (cycle - 1) % 2;
            var target = cycle % 2;
            var elapsed = Stopwatch.StartNew();
            await cluster.MoveEmptySlotAsync(slot, source, target, token);
            using var recovery = CancellationTokenSource.CreateLinkedTokenSource(token);
            recovery.CancelAfter(TimeSpan.FromSeconds(35));
            while (handle.SuccessfulRelocations < cycle || !handle.IsConnected)
            {
                Assert.Null(handle.Failure);
                Assert.False(completion.IsCompleted);
                await Task.Delay(20, recovery.Token);
            }
            Assert.Same(completion, handle.Completion);
            Assert.Equal(cycle, handle.SuccessfulRelocations);
            Assert.Equal(cycle, handle.SuccessfulReconnects);
            Assert.Equal(cycle, handle.ConnectionLosses);
            Assert.Equal(2, subscriber.SubscriptionCount);
            await VerifyDeliveryAsync(publisher, channel, messages, cycle, token);
            await VerifyDeliveryAsync(publisher, Encoding.UTF8.GetBytes(stationary), stationaryMessages, cycle, token);
            Assert.Equal(0, untouched.ConnectionLosses);
            Assert.Equal(0, handle.DroppedMessages);
            // Independent server-side evidence: one registration at the target, none at the source.
            await using var oldNode = await ValkeyClient.ConnectAsync(cluster.NodeOptions(source, protocol), token);
            await using var newNode = await ValkeyClient.ConnectAsync(cluster.NodeOptions(target, protocol), token);
            Assert.Equal(
                0,
                (await oldNode.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                    .AsArray()[1]
                    .AsInt64()
            );
            Assert.Equal(
                1,
                (await newNode.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                    .AsArray()[1]
                    .AsInt64()
            );
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"cycle={cycle}; slot={slot}; source={source}; target={target}; elapsed_ms={elapsed.ElapsedMilliseconds}; losses={handle.ConnectionLosses}; attempts={handle.ReconnectAttempts}; relocations={handle.SuccessfulRelocations}; dropped={handle.DroppedMessages}"
            );
        }
        await handle.UnsubscribeAsync(token);
        await untouched.UnsubscribeAsync(token);
        Assert.False(await messages.MoveNextAsync());
        Assert.False(await stationaryMessages.MoveNextAsync());
        Assert.Equal(0, subscriber.SubscriptionCount);
        await subscriber.DisposeAsync();
        await publisher.DisposeAsync();
        for (var node = 0; node < 3; node++)
        {
            Assert.Equal("0", await cluster.CommandAsync(node, ["DBSIZE"], token));
            Assert.Equal("", await cluster.CommandAsync(node, ["PUBSUB", "SHARDCHANNELS"], token));
            Assert.DoesNotContain(
                "name=" + cluster.Project,
                await cluster.CommandAsync(node, ["CLIENT", "LIST"], token),
                StringComparison.Ordinal
            );
        }
        await cluster.DisposeAsync();
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Verified cleanup of owned containers/network: {cluster.Project}"
        );
    }

    private static async Task VerifyDeliveryAsync(
        ValkeyClusterClient publisher,
        byte[] channel,
        IAsyncEnumerator<ValkeyPubSubMessage> messages,
        int sequence,
        CancellationToken token
    )
    {
        byte[] payload = [255, 0, 13, 10, .. BitConverter.GetBytes(sequence)];
        Assert.Equal(
            1,
            (await publisher.ExecuteAsync(channel, new ValkeyCommand("SPUBLISH", channel, payload), token)).AsInt64()
        );
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), token));
        Assert.Equal(channel, messages.Current.Channel.ToArray());
        Assert.Equal(payload, messages.Current.Payload.ToArray());
        Assert.True(messages.Current.IsSharded);
        Assert.Null(messages.Current.Pattern);
    }
}
