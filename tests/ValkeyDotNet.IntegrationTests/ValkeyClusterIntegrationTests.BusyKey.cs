using System.Globalization;
using System.Text;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedBusyKeyRejectionPreservesConflictingCopiesAndSourceStream(ValkeyProtocol protocol)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_BUSYKEY_TESTS") != "1")
        {
            Assert.Skip("Set VALKEYDOTNET_RUN_BUSYKEY_TESTS=1 to test a conflict in a fresh owned Docker cluster.");
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var cluster = new MigrationValkeyCluster();
        var output = TestContext.Current.TestOutputHelper;
        output?.WriteLine($"Owned BUSYKEY project: {cluster.Project}; protocol={protocol}");
        await cluster.StartNewAsync(token);
        for (var node = 0; node < 3; node++)
        {
            Assert.Equal("0", await cluster.CommandAsync(node, ["DBSIZE"], token));
            Assert.Equal(
                "enable-debug-command\nno",
                await cluster.CommandAsync(node, ["CONFIG", "GET", "enable-debug-command"], token)
            );
            output?.WriteLine(await cluster.CommandAsync(node, ["INFO", "SERVER"], token));
        }
        var tag = FindKey(cluster.Project, 0, 5460);
        byte[] channel = [.. Encoding.UTF8.GetBytes("{" + tag + "}"), 0, 255, 13, 10];
        byte[] key = [.. channel, 1];
        cluster.ValidateTransferKey(key);
        var slot = ValkeyClusterClient.GetHashSlot(key);
        var number = slot.ToString(CultureInfo.InvariantCulture);
        var sourceValue = Enumerable.Range(0, 4096).Select(index => (byte)(index % 256)).ToArray();
        byte[] targetValue = [255, 0, 13, 10, 99];
        var stationary = Encoding.UTF8.GetBytes(FindKey(cluster.Project + "-stationary", 10923, 16383));
        await using var commands = await ValkeyClusterClient.ConnectAsync(cluster.Options(protocol), token);
        await using var source = await ValkeyClient.ConnectAsync(cluster.NodeOptions(0, protocol), token);
        await using var target = await ValkeyClient.ConnectAsync(cluster.NodeOptions(1, protocol), token);
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
        var stationaryCompletion = untouched.Completion;
        Assert.Equal(
            "OK",
            (
                await source.ExecuteAsync(new ValkeyCommand("SET", key, sourceValue, "NX", "PX", "120000"), token)
            ).AsString()
        );
        var sourceExpiry = (await source.ExecuteAsync(new ValkeyCommand("PEXPIRETIME", key), token)).AsInt64();
        Assert.True(sourceExpiry > 0);
        await VerifyDeliveryAsync(commands, channel, messages, 0, token);
        await VerifyDeliveryAsync(commands, stationary, stationaryMessages, 0, token);
        await cluster.BeginSlotMigrationAsync(slot, 0, 1, 1, token);
        // Create exactly one known conflicting copy in the IMPORTING node. NX forbids an overwrite.
        Assert.Equal(
            "OK",
            (
                await ExecuteBusyKeyTargetAsync(
                    target,
                    new ValkeyCommand("SET", key, targetValue, "NX", "PX", "90000"),
                    token
                )
            ).AsString()
        );
        var targetExpiry = (
            await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PEXPIRETIME", key), token)
        ).AsInt64();
        Assert.True(targetExpiry > 0);
        Assert.NotEqual(sourceExpiry, targetExpiry);
        for (var phase = 0; phase < 2; phase++)
        {
            if (phase == 1)
            {
                await cluster.RejectOwnedConflictingMigrationAsync(key, protocol, token);
            }
            // Read each node independently: a routed GET alone would hide the destination conflict.
            Assert.Equal(
                sourceValue,
                (await source.ExecuteAsync(new ValkeyCommand("GET", key), token)).AsBytes().ToArray()
            );
            Assert.Equal(
                targetValue,
                (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("GET", key), token)).AsBytes().ToArray()
            );
            Assert.Equal(
                sourceExpiry,
                (await source.ExecuteAsync(new ValkeyCommand("PEXPIRETIME", key), token)).AsInt64()
            );
            Assert.Equal(
                targetExpiry,
                (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PEXPIRETIME", key), token)).AsInt64()
            );
            Assert.InRange((await source.ExecuteAsync(new ValkeyCommand("PTTL", key), token)).AsInt64(), 1, 120000);
            Assert.InRange(
                (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PTTL", key), token)).AsInt64(),
                1,
                90000
            );
            foreach (var node in new[] { source, target })
            {
                var keys = (
                    await node.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "2"), token)
                ).AsArray();
                Assert.Equal(key, Assert.Single(keys).AsBytes().ToArray());
            }
            await VerifySlotOwnerAsync(cluster, slot, 0, protocol, token);
            Assert.Equal(
                sourceValue,
                (await commands.ExecuteAsync(key, new ValkeyCommand("GET", key), token)).AsBytes().ToArray()
            );
            await VerifyDeliveryAsync(commands, channel, messages, phase + 1, token);
            await VerifyDeliveryAsync(commands, stationary, stationaryMessages, phase + 1, token);
            Assert.Same(completion, handle.Completion);
            Assert.Same(stationaryCompletion, untouched.Completion);
            foreach (var subscription in new[] { handle, untouched })
            {
                Assert.False(subscription.Completion.IsCompleted);
                Assert.Null(subscription.Failure);
                Assert.True(subscription.IsConnected);
                Assert.Equal(0, subscription.ConnectionLosses);
                Assert.Equal(0, subscription.ReconnectAttempts);
                Assert.Equal(0, subscription.SuccessfulRelocations);
                Assert.Equal(0, subscription.DroppedMessages);
            }
            Assert.Equal(2, subscriber.SubscriptionCount);
            Assert.Equal(
                1,
                (await source.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                    .AsArray()[1]
                    .AsInt64()
            );
            Assert.Equal(
                0,
                (await target.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                    .AsArray()[1]
                    .AsInt64()
            );
        }
        output?.WriteLine(
            $"slot={slot}; conflict=retained; source_bytes=4096; destination_bytes=5; source_expiry_shift_ms=0; destination_expiry_shift_ms=0; losses=0; attempts=0; relocations=0; dropped=0"
        );
        // Teardown only, not a production conflict-resolution policy: delete the two known fixture copies.
        Assert.Equal(1, (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("DEL", key), token)).AsInt64());
        Assert.Equal(1, (await source.ExecuteAsync(new ValkeyCommand("DEL", key), token)).AsInt64());
        await handle.UnsubscribeAsync(token);
        await untouched.UnsubscribeAsync(token);
        Assert.False(await messages.MoveNextAsync());
        Assert.False(await stationaryMessages.MoveNextAsync());
        Assert.Equal(0, subscriber.SubscriptionCount);
        await subscriber.DisposeAsync();
        await commands.DisposeAsync();
        await source.DisposeAsync();
        await target.DisposeAsync();
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
        output?.WriteLine($"Verified cleanup of owned containers/network: {cluster.Project}");
    }

    private static async Task<RespValue> ExecuteBusyKeyTargetAsync(
        ValkeyClient target,
        ValkeyCommand command,
        CancellationToken token
    )
    {
        var replies = await target.ExecutePipelineAsync([new ValkeyCommand("ASKING"), command], token);
        Assert.Equal(2, replies.Count);
        Assert.Equal("OK", replies[0].AsString());
        replies[1].ThrowIfError();
        return replies[1];
    }
}
