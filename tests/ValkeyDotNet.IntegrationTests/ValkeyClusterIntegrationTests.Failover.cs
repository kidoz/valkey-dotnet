using System.Diagnostics;
using System.Text;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2, false)]
    [InlineData(ValkeyProtocol.Resp3, false)]
    [InlineData(ValkeyProtocol.Resp2, true)]
    [InlineData(ValkeyProtocol.Resp3, true)]
    public async Task OwnedPrimaryFailoverPreservesShardedStream(ValkeyProtocol protocol, bool loseSeed)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_FAILOVER_TESTS") != "1")
        {
            Assert.Skip(
                "Set VALKEYDOTNET_RUN_FAILOVER_TESTS=1 to stop a primary in a fresh owned four-node Docker cluster."
            );
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var cluster = new MigrationValkeyCluster(includeReplica: true);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Owned project: {cluster.Project}; protocol={protocol}; lose_seed={loseSeed}"
        );
        await cluster.StartNewAsync(token);
        for (var index = 0; index < 4; index++)
        {
            var version = (await cluster.CommandAsync(index, ["INFO", "SERVER"], token))
                .Split('\n')
                .Single(line => line.StartsWith("valkey_version:", StringComparison.Ordinal))
                .Trim();
            TestContext.Current.TestOutputHelper?.WriteLine($"node={index}; {version}");
            Assert.Equal("0", await cluster.CommandAsync(index, ["DBSIZE"], token));
        }
        var tag = FindKey(cluster.Project, 0, 5460);
        byte[] channel = [.. Encoding.UTF8.GetBytes("{" + tag + "}"), 0, 255, 13, 10];
        var stationary = Encoding.UTF8.GetBytes(FindKey(cluster.Project + "-stationary", 10923, 16383));
        var slot = ValkeyClusterClient.GetHashSlot(channel);
        // Publisher routing is refreshed explicitly after promotion; no failed write is replayed.
        await using var publisher = await ValkeyClusterClient.ConnectAsync(cluster.Options(protocol, seed: 1), token);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = cluster.Options(protocol, seed: loseSeed ? 0 : 1),
                EnableTopologyRecovery = true,
                MaxSubscriptions = 2,
                QueueCapacity = 8,
                MaxReconnectAttempts = 20,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(500),
                MaxReconnectDelay = TimeSpan.FromSeconds(2),
                RecoveryTimeout = TimeSpan.FromSeconds(60),
            },
            token
        );
        await using var handle = await subscriber.SubscribeAsync(channel, token);
        await using var untouched = await subscriber.SubscribeAsync(stationary, token);
        await using var messages = handle.ReadAllAsync(token).GetAsyncEnumerator(token);
        await using var stationaryMessages = untouched.ReadAllAsync(token).GetAsyncEnumerator(token);
        var completion = handle.Completion;
        await VerifyDeliveryAsync(publisher, channel, messages, 0, token);
        await VerifyDeliveryAsync(publisher, stationary, stationaryMessages, 0, token);
        var elapsed = Stopwatch.StartNew();
        await cluster.StopOwnedPrimaryAsync(token);
        await cluster.WaitForPromotionAsync(slot, token);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"promotion_ms={elapsed.ElapsedMilliseconds}; attempts={handle.ReconnectAttempts}"
        );
        using var recovery = CancellationTokenSource.CreateLinkedTokenSource(token);
        recovery.CancelAfter(TimeSpan.FromSeconds(65));
        while (handle.SuccessfulRelocations < 1 || !handle.IsConnected)
        {
            Assert.Null(handle.Failure);
            Assert.False(completion.IsCompleted);
            await Task.Delay(20, recovery.Token);
        }
        await cluster.AssertPrimaryStoppedAsync(token); // The missing seed was not restarted to make recovery pass.
        Assert.Same(completion, handle.Completion);
        Assert.False(completion.IsCompleted);
        Assert.Null(handle.Failure);
        Assert.Equal(1, handle.ConnectionLosses);
        Assert.Equal(1, handle.SuccessfulReconnects);
        Assert.Equal(1, handle.SuccessfulRelocations);
        Assert.InRange(handle.ReconnectAttempts, 1, 20);
        Assert.Equal(0, untouched.ConnectionLosses);
        Assert.Equal(2, subscriber.SubscriptionCount);
        await publisher.RefreshTopologyAsync(token);
        await VerifyDeliveryAsync(publisher, channel, messages, 1, token);
        await VerifyDeliveryAsync(publisher, stationary, stationaryMessages, 1, token);
        await using (var promoted = await ValkeyClient.ConnectAsync(cluster.NodeOptions(3, protocol), token))
        {
            Assert.Equal(
                1,
                (await promoted.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                    .AsArray()[1]
                    .AsInt64()
            );
        }
        Assert.Equal(0, handle.DroppedMessages);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"slot={slot}; elapsed_ms={elapsed.ElapsedMilliseconds}; losses={handle.ConnectionLosses}; attempts={handle.ReconnectAttempts}; relocations={handle.SuccessfulRelocations}; dropped={handle.DroppedMessages}; primary_stopped=true"
        );
        await handle.UnsubscribeAsync(token);
        await untouched.UnsubscribeAsync(token);
        Assert.False(await messages.MoveNextAsync());
        Assert.False(await stationaryMessages.MoveNextAsync());
        Assert.Equal(0, subscriber.SubscriptionCount);
        await subscriber.DisposeAsync();
        await publisher.DisposeAsync();
        for (var index = 1; index < 4; index++)
        {
            Assert.Equal("0", await cluster.CommandAsync(index, ["DBSIZE"], token));
            Assert.Equal("", await cluster.CommandAsync(index, ["PUBSUB", "SHARDCHANNELS"], token));
            Assert.DoesNotContain(
                "name=" + cluster.Project,
                await cluster.CommandAsync(index, ["CLIENT", "LIST"], token),
                StringComparison.Ordinal
            );
        }
        await cluster.DisposeAsync();
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Verified cleanup of owned containers/network: {cluster.Project}"
        );
    }
}
