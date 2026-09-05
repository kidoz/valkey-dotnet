using System.Globalization;
using System.Text;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2, false)]
    [InlineData(ValkeyProtocol.Resp2, true)]
    [InlineData(ValkeyProtocol.Resp3, false)]
    [InlineData(ValkeyProtocol.Resp3, true)]
    public async Task OwnedBulkConflictReconcilesPartialSuccessWithoutReplay(
        ValkeyProtocol protocol,
        bool conflictFirst
    )
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_BULK_CONFLICT_TESTS") != "1")
        {
            Assert.Skip("Set VALKEYDOTNET_RUN_BULK_CONFLICT_TESTS=1 to test a two-key batch in a fresh owned cluster.");
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var cluster = new MigrationValkeyCluster();
        var output = TestContext.Current.TestOutputHelper;
        output?.WriteLine(
            $"Owned bulk-conflict project: {cluster.Project}; protocol={protocol}; conflict_first={conflictFirst}"
        );
        await cluster.StartNewAsync(token);
        for (var index = 0; index < 3; index++)
        {
            Assert.Equal("0", await cluster.CommandAsync(index, ["DBSIZE"], token));
            Assert.Equal(
                "enable-debug-command\nno",
                await cluster.CommandAsync(index, ["CONFIG", "GET", "enable-debug-command"], token)
            );
        }
        output?.WriteLine(await cluster.CommandAsync(0, ["INFO", "SERVER"], token));
        var tag = FindKey(cluster.Project, 0, 5460);
        byte[] channel = [.. Encoding.UTF8.GetBytes("{" + tag + "}"), 0, 255, 13, 10];
        byte[] movingKey = [.. channel, 1];
        byte[] conflictingKey = [.. channel, 2];
        cluster.ValidateBulkTransferKeys(movingKey, conflictingKey);
        var slot = ValkeyClusterClient.GetHashSlot(movingKey);
        var number = slot.ToString(CultureInfo.InvariantCulture);
        var movingValue = Enumerable.Range(0, 4096).Select(index => (byte)(index % 256)).ToArray();
        byte[] sourceConflict = [0, 255, 13, 10, 11];
        byte[] targetConflict = [255, 0, 13, 10, 22];
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
                await source.ExecuteAsync(new ValkeyCommand("SET", movingKey, movingValue, "NX", "PX", "120000"), token)
            ).AsString()
        );
        Assert.Equal(
            "OK",
            (
                await source.ExecuteAsync(new ValkeyCommand("SET", conflictingKey, sourceConflict, "NX"), token)
            ).AsString()
        );
        var sourceExpiry = (await source.ExecuteAsync(new ValkeyCommand("PEXPIRETIME", movingKey), token)).AsInt64();
        Assert.True(sourceExpiry > 0);
        await VerifyDeliveryAsync(commands, channel, messages, 0, token);
        await VerifyDeliveryAsync(commands, stationary, stationaryMessages, 0, token);
        await cluster.BeginSlotMigrationAsync(slot, 0, 1, 2, token);
        Assert.Equal(
            "OK",
            (
                await ExecuteBusyKeyTargetAsync(
                    target,
                    new ValkeyCommand("SET", conflictingKey, targetConflict, "NX", "PX", "90000"),
                    token
                )
            ).AsString()
        );
        var conflictExpiry = (
            await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PEXPIRETIME", conflictingKey), token)
        ).AsInt64();
        Assert.True(conflictExpiry > 0);
        Assert.Equal(-1, (await source.ExecuteAsync(new ValkeyCommand("PTTL", conflictingKey), token)).AsInt64());
        Assert.True((await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("GET", movingKey), token)).IsNull);
        await cluster.MigrateOwnedBulkWithConflictAsync(movingKey, conflictingKey, conflictFirst, protocol, token);
        var targetExpiry = (
            await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PEXPIRETIME", movingKey), token)
        ).AsInt64();
        Assert.True(targetExpiry > 0);
        Assert.InRange(targetExpiry - sourceExpiry, -1000, 1000);
        for (var phase = 1; phase <= 2; phase++)
        {
            // A received batch error is not an all-keys rollback. Inspect each physical copy separately.
            await MigrationValkeyCluster.VerifyBulkKeysAsync(source, number, [conflictingKey], token);
            await MigrationValkeyCluster.VerifyBulkKeysAsync(target, number, [movingKey, conflictingKey], token);
            Assert.Equal(
                movingValue,
                (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("GET", movingKey), token))
                    .AsBytes()
                    .ToArray()
            );
            Assert.Equal(
                sourceConflict,
                (await source.ExecuteAsync(new ValkeyCommand("GET", conflictingKey), token)).AsBytes().ToArray()
            );
            Assert.Equal(
                targetConflict,
                (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("GET", conflictingKey), token))
                    .AsBytes()
                    .ToArray()
            );
            Assert.Equal(
                targetExpiry,
                (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PEXPIRETIME", movingKey), token)).AsInt64()
            );
            Assert.Equal(
                conflictExpiry,
                (
                    await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PEXPIRETIME", conflictingKey), token)
                ).AsInt64()
            );
            Assert.Equal(
                -1,
                (await source.ExecuteAsync(new ValkeyCommand("PEXPIRETIME", conflictingKey), token)).AsInt64()
            );
            Assert.Equal(-1, (await source.ExecuteAsync(new ValkeyCommand("PTTL", conflictingKey), token)).AsInt64());
            Assert.InRange(
                (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PTTL", movingKey), token)).AsInt64(),
                1,
                121000
            );
            Assert.InRange(
                (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("PTTL", conflictingKey), token)).AsInt64(),
                1,
                90000
            );
            Assert.Equal(
                "ASK",
                (
                    await Assert.ThrowsAsync<ValkeyServerException>(() =>
                        source.ExecuteAsync(new ValkeyCommand("GET", movingKey), token)
                    )
                ).ErrorCode
            );
            Assert.Equal(
                movingValue,
                (await commands.ExecuteAsync(movingKey, new ValkeyCommand("GET", movingKey), token)).AsBytes().ToArray()
            );
            Assert.Equal(
                sourceConflict,
                (await commands.ExecuteAsync(conflictingKey, new ValkeyCommand("GET", conflictingKey), token))
                    .AsBytes()
                    .ToArray()
            );
            Assert.Equal(
                "TRYAGAIN",
                (
                    await Assert.ThrowsAsync<ValkeyServerException>(() =>
                        commands.ExecuteAsync(movingKey, new ValkeyCommand("MGET", movingKey, conflictingKey), token)
                    )
                ).ErrorCode
            );
            await VerifySlotOwnerAsync(cluster, slot, 0, protocol, token);
            await VerifyDeliveryAsync(commands, channel, messages, phase, token);
            await VerifyDeliveryAsync(commands, stationary, stationaryMessages, phase, token);
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
            $"slot={slot}; conflict_first={conflictFirst}; moving_expiry_shift_ms={targetExpiry - sourceExpiry}; conflict_expiry_shift_ms=0; source_conflict_persistent=true; losses=0; attempts=0; relocations=0; dropped=0"
        );
        // Teardown only: no production conflict-resolution policy. Delete each exact known copy.
        Assert.Equal(
            1,
            (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("DEL", movingKey), token)).AsInt64()
        );
        Assert.Equal(
            1,
            (await ExecuteBusyKeyTargetAsync(target, new ValkeyCommand("DEL", conflictingKey), token)).AsInt64()
        );
        Assert.Equal(1, (await source.ExecuteAsync(new ValkeyCommand("DEL", conflictingKey), token)).AsInt64());
        await handle.UnsubscribeAsync(token);
        await untouched.UnsubscribeAsync(token);
        Assert.False(await messages.MoveNextAsync());
        Assert.False(await stationaryMessages.MoveNextAsync());
        Assert.Equal(0, subscriber.SubscriptionCount);
        await subscriber.DisposeAsync();
        await commands.DisposeAsync();
        await source.DisposeAsync();
        await target.DisposeAsync();
        for (var index = 0; index < 3; index++)
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
        output?.WriteLine($"Verified cleanup of owned containers/network: {cluster.Project}");
    }
}
