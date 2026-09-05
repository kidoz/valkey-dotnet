using System.Globalization;
using System.Text;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedMigrationForcesCommandAskWhileShardStreamWaitsForCutover(ValkeyProtocol protocol)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_ASK_TESTS") != "1")
        {
            Assert.Skip("Set VALKEYDOTNET_RUN_ASK_TESTS=1 to migrate a slot in a fresh owned Docker cluster.");
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var cluster = new MigrationValkeyCluster();
        TestContext.Current.TestOutputHelper?.WriteLine($"Owned ASK project: {cluster.Project}; protocol={protocol}");
        await cluster.StartNewAsync(token);
        var version = (await cluster.CommandAsync(0, ["INFO", "SERVER"], token))
            .Split('\n')
            .Single(line => line.StartsWith("valkey_version:", StringComparison.Ordinal))
            .Trim();
        TestContext.Current.TestOutputHelper?.WriteLine(version);
        for (var index = 0; index < 3; index++)
        {
            Assert.Equal("0", await cluster.CommandAsync(index, ["DBSIZE"], token));
        }
        var tag = FindKey(cluster.Project, 0, 5460);
        byte[] channel = [.. Encoding.UTF8.GetBytes("{" + tag + "}"), 0, 255, 13, 10];
        byte[] retainedKey = [.. channel, 1];
        byte[] redirectedKey = [.. channel, 2];
        byte[] value = [255, 0, 13, 10, 42];
        var stationary = Encoding.UTF8.GetBytes(FindKey(cluster.Project + "-stationary", 10923, 16383));
        var slot = ValkeyClusterClient.GetHashSlot(channel);
        await using var commands = await ValkeyClusterClient.ConnectAsync(cluster.Options(protocol), token);
        await using var source = await ValkeyClient.ConnectAsync(cluster.NodeOptions(0, protocol), token);
        await using var target = await ValkeyClient.ConnectAsync(cluster.NodeOptions(1, protocol), token);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = cluster.Options(protocol),
                EnableTopologyRecovery = true,
                MaxSubscriptions = 3,
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
        Assert.Equal(
            "OK",
            (await commands.ExecuteAsync(retainedKey, new ValkeyCommand("SET", retainedKey, value), token)).AsString()
        );
        await VerifyDeliveryAsync(commands, channel, messages, 0, token);
        await VerifyDeliveryAsync(commands, stationary, stationaryMessages, 0, token);

        // Hold the intermediate state open: one known key stays at source, target starts empty.
        await cluster.BeginSlotMigrationAsync(slot, 0, 1, 1, token);
        var ask = await Assert.ThrowsAsync<ValkeyServerException>(() =>
            source.ExecuteAsync(new ValkeyCommand("GET", redirectedKey), token)
        );
        Assert.Equal($"ASK {slot.ToString(CultureInfo.InvariantCulture)} node-2:6379", ask.Message);
        var moved = await Assert.ThrowsAsync<ValkeyServerException>(() =>
            target.ExecuteAsync(new ValkeyCommand("GET", redirectedKey), token)
        );
        Assert.Equal("MOVED", moved.ErrorCode);
        var asksBefore = await ErrorCountAsync(source, "ASK", token);
        var movedBefore = await ErrorCountAsync(target, "MOVED", token);
        Assert.Equal(
            "OK",
            (
                await commands.ExecuteAsync(redirectedKey, new ValkeyCommand("SET", redirectedKey, value), token)
            ).AsString()
        );
        for (var attempt = 0; attempt < 2; attempt++)
        {
            Assert.Equal(
                value,
                (await commands.ExecuteAsync(redirectedKey, new ValkeyCommand("GET", redirectedKey), token))
                    .AsBytes()
                    .ToArray()
            );
        }
        // Every operation still visits the source and sends a fresh ASKING at the destination.
        Assert.Equal(3, await ErrorCountAsync(source, "ASK", token) - asksBefore);
        Assert.Equal(movedBefore, await ErrorCountAsync(target, "MOVED", token));
        Assert.Equal(
            value,
            (await commands.ExecuteAsync(retainedKey, new ValkeyCommand("GET", retainedKey), token)).AsBytes().ToArray()
        );
        var accepted = await target.ExecutePipelineAsync(
            [new ValkeyCommand("ASKING"), new ValkeyCommand("GET", redirectedKey)],
            token
        );
        Assert.Equal("OK", accepted[0].AsString());
        Assert.Equal(value, accepted[1].AsBytes().ToArray());
        Assert.Equal(
            "MOVED",
            (
                await Assert.ThrowsAsync<ValkeyServerException>(() =>
                    target.ExecuteAsync(new ValkeyCommand("GET", redirectedKey), token)
                )
            ).ErrorCode
        );

        // Native sharded Pub/Sub deliberately remains on the source until SETSLOT NODE.
        var shardAsksBefore = await ErrorCountAsync(source, "ASK", token);
        await using (var duringMigration = await subscriber.SubscribeAsync(channel, token))
        {
            Assert.Equal(
                2,
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
            await duringMigration.UnsubscribeAsync(token);
        }
        await VerifyDeliveryAsync(commands, channel, messages, 1, token);
        Assert.Equal(shardAsksBefore, await ErrorCountAsync(source, "ASK", token));
        Assert.Equal(0, handle.ConnectionLosses);
        Assert.Equal(0, handle.SuccessfulRelocations);
        await VerifySlotOwnerAsync(cluster, slot, 0, protocol, token);

        // Delete only the two known fixture keys before empty-slot cutover; no MIGRATE claim.
        Assert.Equal(
            1,
            (await commands.ExecuteAsync(retainedKey, new ValkeyCommand("DEL", retainedKey), token)).AsInt64()
        );
        Assert.Equal(
            1,
            (await commands.ExecuteAsync(redirectedKey, new ValkeyCommand("DEL", redirectedKey), token)).AsInt64()
        );
        await cluster.CompleteEmptySlotMigrationAsync(slot, 0, 1, token);
        using var recovery = CancellationTokenSource.CreateLinkedTokenSource(token);
        recovery.CancelAfter(TimeSpan.FromSeconds(35));
        while (handle.SuccessfulRelocations < 1 || !handle.IsConnected)
        {
            Assert.Null(handle.Failure);
            Assert.False(completion.IsCompleted);
            await Task.Delay(20, recovery.Token);
        }
        await VerifySlotOwnerAsync(cluster, slot, 1, protocol, token);
        await VerifyDeliveryAsync(commands, channel, messages, 2, token);
        await VerifyDeliveryAsync(commands, stationary, stationaryMessages, 2, token);
        Assert.Same(completion, handle.Completion);
        Assert.False(completion.IsCompleted);
        Assert.Null(handle.Failure);
        Assert.Equal(1, handle.ConnectionLosses);
        Assert.Equal(1, handle.SuccessfulReconnects);
        Assert.Equal(1, handle.SuccessfulRelocations);
        Assert.Equal(0, handle.DroppedMessages);
        Assert.Equal(0, untouched.ConnectionLosses);
        Assert.Equal(
            0,
            (await source.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                .AsArray()[1]
                .AsInt64()
        );
        Assert.Equal(
            1,
            (await target.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                .AsArray()[1]
                .AsInt64()
        );
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"slot={slot}; verified_command_asks=3; shard_asks=0; relocations={handle.SuccessfulRelocations}; attempts={handle.ReconnectAttempts}; dropped={handle.DroppedMessages}"
        );
        await handle.UnsubscribeAsync(token);
        await untouched.UnsubscribeAsync(token);
        Assert.False(await messages.MoveNextAsync());
        Assert.False(await stationaryMessages.MoveNextAsync());
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
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Verified cleanup of owned containers/network: {cluster.Project}"
        );
    }

    private static async Task<long> ErrorCountAsync(ValkeyClient client, string code, CancellationToken token)
    {
        var info = (await client.ExecuteAsync(new ValkeyCommand("INFO", "ERRORSTATS"), token)).AsString()!;
        var prefix = "errorstat_" + code + ":count=";
        var line = info.Split('\n').SingleOrDefault(line => line.StartsWith(prefix, StringComparison.Ordinal));
        return line is null ? 0 : long.Parse(line.AsSpan(prefix.Length).Trim(), CultureInfo.InvariantCulture);
    }

    private static async Task VerifySlotOwnerAsync(
        MigrationValkeyCluster cluster,
        int slot,
        int owner,
        ValkeyProtocol protocol,
        CancellationToken token
    )
    {
        var expected = await cluster.CommandAsync(owner, ["CLUSTER", "MYID"], token);
        for (var index = 0; index < 3; index++)
        {
            await using var node = await ValkeyClient.ConnectAsync(cluster.NodeOptions(index, protocol), token);
            var ranges = (await node.ExecuteAsync(new ValkeyCommand("CLUSTER", "SLOTS"), token)).AsArray();
            var range = ranges
                .Single(value => value.AsArray()[0].AsInt64() <= slot && value.AsArray()[1].AsInt64() >= slot)
                .AsArray();
            Assert.Equal(expected, range[2].AsArray()[2].AsString());
        }
    }
}
