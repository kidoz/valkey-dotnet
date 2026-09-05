using System.Globalization;
using System.Text;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedNonemptyMigrationPreservesBinaryKeysExpiryAndShardStream(ValkeyProtocol protocol)
    {
        await VerifyOwnedKeyMigrationAsync(protocol, OwnedMigrationMode.Legacy);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedAtomicMigrationPreservesBinaryKeysExpiryAndShardStream(ValkeyProtocol protocol)
    {
        await VerifyOwnedKeyMigrationAsync(protocol, OwnedMigrationMode.Atomic);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedAtomicCancellationPreservesSourceKeysExpiryAndShardStream(ValkeyProtocol protocol)
    {
        await VerifyOwnedKeyMigrationAsync(protocol, OwnedMigrationMode.CancelBeforeTransfer);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedAtomicLinkFailureCleansImportedKeysAndPreservesSourceStream(ValkeyProtocol protocol)
    {
        await VerifyOwnedKeyMigrationAsync(protocol, OwnedMigrationMode.DisconnectAfterSnapshot);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedMigrateIoErrorBeforeRestorePreservesSourceDataAndStream(ValkeyProtocol protocol)
    {
        await VerifyOwnedKeyMigrationAsync(protocol, OwnedMigrationMode.SourceOnlyIoError);
    }

    private enum OwnedMigrationMode
    {
        Legacy,
        Atomic,
        CancelBeforeTransfer,
        DisconnectAfterSnapshot,
        LostMigrateReply,
        SourceOnlyIoError,
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedLostMigrateReplyIsReconciledWithoutReplay(ValkeyProtocol protocol)
    {
        await VerifyOwnedKeyMigrationAsync(protocol, OwnedMigrationMode.LostMigrateReply);
    }

    private static async Task VerifyOwnedKeyMigrationAsync(ValkeyProtocol protocol, OwnedMigrationMode mode)
    {
        var atomic =
            mode
            is not (
                OwnedMigrationMode.Legacy
                or OwnedMigrationMode.LostMigrateReply
                or OwnedMigrationMode.SourceOnlyIoError
            );
        var sourceOnlyIoError = mode == OwnedMigrationMode.SourceOnlyIoError;
        var cancelBeforeTransfer = mode == OwnedMigrationMode.CancelBeforeTransfer;
        var disconnectAfterSnapshot = mode == OwnedMigrationMode.DisconnectAfterSnapshot;
        var retainsSource = cancelBeforeTransfer || disconnectAfterSnapshot || sourceOnlyIoError;
        var flag = mode switch
        {
            OwnedMigrationMode.Legacy => "VALKEYDOTNET_RUN_KEY_TRANSFER_TESTS",
            OwnedMigrationMode.Atomic => "VALKEYDOTNET_RUN_ATOMIC_MIGRATION_TESTS",
            OwnedMigrationMode.CancelBeforeTransfer => "VALKEYDOTNET_RUN_ATOMIC_CANCELLATION_TESTS",
            OwnedMigrationMode.LostMigrateReply => "VALKEYDOTNET_RUN_MIGRATE_REPLY_LOSS_TESTS",
            OwnedMigrationMode.SourceOnlyIoError => "VALKEYDOTNET_RUN_MIGRATE_IOERR_TESTS",
            _ => "VALKEYDOTNET_RUN_ATOMIC_ROLLBACK_TESTS",
        };
        if (Environment.GetEnvironmentVariable(flag) != "1")
        {
            Assert.Skip($"Set {flag}=1 to migrate keys in a fresh owned Docker cluster.");
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var cluster = new MigrationValkeyCluster(enableMigrationDebug: disconnectAfterSnapshot);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"Owned key-migration project: {cluster.Project}; protocol={protocol}; mode={mode}"
        );
        await cluster.StartNewAsync(token);
        var version = (await cluster.CommandAsync(0, ["INFO", "SERVER"], token))
            .Split('\n')
            .Single(line => line.StartsWith("valkey_version:", StringComparison.Ordinal))
            .Trim();
        TestContext.Current.TestOutputHelper?.WriteLine(version);
        for (var index = 0; index < 3; index++)
        {
            Assert.Equal("0", await cluster.CommandAsync(index, ["DBSIZE"], token));
            Assert.Equal(
                "enable-debug-command\n" + cluster.MigrationDebugMode,
                await cluster.CommandAsync(index, ["CONFIG", "GET", "enable-debug-command"], token)
            );
        }
        var tag = FindKey(cluster.Project, 0, 5460);
        byte[] channel = [.. Encoding.UTF8.GetBytes("{" + tag + "}"), 0, 255, 13, 10];
        byte[] expiringKey = [.. channel, 1];
        byte[] persistentKey = [.. channel, 2];
        var expiringValue = Enumerable.Range(0, 4096).Select(index => (byte)(index % 256)).ToArray();
        byte[] persistentValue = [255, 0, 13, 10, 42];
        var slot = ValkeyClusterClient.GetHashSlot(channel);
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
        Assert.Equal(
            "OK",
            (
                await commands.ExecuteAsync(
                    expiringKey,
                    new ValkeyCommand("SET", expiringKey, expiringValue, "PX", "120000"),
                    token
                )
            ).AsString()
        );
        Assert.Equal(
            "OK",
            (
                await commands.ExecuteAsync(
                    persistentKey,
                    new ValkeyCommand("SET", persistentKey, persistentValue),
                    token
                )
            ).AsString()
        );
        var originalExpiry = (
            await commands.ExecuteAsync(expiringKey, new ValkeyCommand("PEXPIRETIME", expiringKey), token)
        ).AsInt64();
        Assert.True(originalExpiry > 0);
        await VerifyTransferredValuesAsync(
            commands,
            expiringKey,
            expiringValue,
            persistentKey,
            persistentValue,
            originalExpiry,
            token
        );
        await VerifyDeliveryAsync(commands, channel, messages, 0, token);
        await VerifyDeliveryAsync(commands, stationary, stationaryMessages, 0, token);

        var number = slot.ToString(CultureInfo.InvariantCulture);
        if (sourceOnlyIoError)
        {
            await cluster.BeginSlotMigrationAsync(slot, 0, 1, 2, token);
            await cluster.TimeoutOwnedKeyBeforeRestoreAsync(expiringKey, protocol, token);
        }
        else if (disconnectAfterSnapshot)
        {
            await cluster.FailAtomicSlotMigrationAfterSnapshotAsync(
                expiringKey,
                persistentKey,
                protocol,
                async bounded =>
                {
                    await VerifySlotOwnerAsync(cluster, slot, 0, protocol, bounded);
                    await VerifyTransferredValuesAsync(
                        commands,
                        expiringKey,
                        expiringValue,
                        persistentKey,
                        persistentValue,
                        originalExpiry,
                        bounded
                    );
                    await VerifyDeliveryAsync(commands, channel, messages, 1, bounded);
                    await VerifyDeliveryAsync(commands, stationary, stationaryMessages, 1, bounded);
                    Assert.Equal(0, handle.ConnectionLosses);
                },
                token
            );
        }
        else if (cancelBeforeTransfer)
        {
            await VerifySlotOwnerAsync(cluster, slot, 0, protocol, token);
            await cluster.CancelAtomicSlotMigrationBeforeTransferAsync(slot, 0, 1, protocol, token);
        }
        else if (atomic)
        {
            await VerifySlotOwnerAsync(cluster, slot, 0, protocol, token);
            await cluster.RunAtomicSlotMigrationAsync(slot, 0, 1, protocol, token);
        }
        else
        {
            await cluster.BeginSlotMigrationAsync(slot, 0, 1, 2, token);
            if (mode == OwnedMigrationMode.LostMigrateReply)
            {
                await cluster.MigrateOwnedKeyWithLostReplyAsync(expiringKey, protocol, token);
            }
            else
            {
                await cluster.MigrateOwnedKeyAsync(expiringKey, 0, 1, 2, protocol, token);
            }
            var remaining = (
                await source.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "2"), token)
            ).AsArray();
            Assert.Equal(persistentKey, Assert.Single(remaining).AsBytes().ToArray());
            var transferred = (
                await target.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "2"), token)
            ).AsArray();
            Assert.Equal(expiringKey, Assert.Single(transferred).AsBytes().ToArray());
            Assert.Equal(
                "ASK",
                (
                    await Assert.ThrowsAsync<ValkeyServerException>(() =>
                        source.ExecuteAsync(new ValkeyCommand("GET", expiringKey), token)
                    )
                ).ErrorCode
            );
            var mixed = await Assert.ThrowsAsync<ValkeyServerException>(() =>
                commands.ExecuteAsync(expiringKey, new ValkeyCommand("MGET", expiringKey, persistentKey), token)
            );
            Assert.Equal("TRYAGAIN", mixed.ErrorCode);
            Assert.Equal("PONG", await commands.PingAsync(token));
            await VerifyTransferredValuesAsync(
                commands,
                expiringKey,
                expiringValue,
                persistentKey,
                persistentValue,
                originalExpiry,
                token
            );
            await VerifyDeliveryAsync(commands, channel, messages, 1, token);
            Assert.Equal(0, handle.ConnectionLosses);
            await VerifySlotOwnerAsync(cluster, slot, 0, protocol, token);

            await cluster.MigrateOwnedKeyAsync(persistentKey, 0, 1, 1, protocol, token);
            Assert.Empty(
                (await source.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "2"), token)).AsArray()
            );
            Assert.Equal(
                2,
                (await target.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "2"), token))
                    .AsArray()
                    .Count
            );
            await VerifyTransferredValuesAsync(
                commands,
                expiringKey,
                expiringValue,
                persistentKey,
                persistentValue,
                originalExpiry,
                token
            );
            await VerifyDeliveryAsync(commands, channel, messages, 2, token);
            Assert.Equal(0, handle.ConnectionLosses);
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
            await cluster.CompleteSlotMigrationAsync(slot, 0, 1, 2, token);
        }
        using var recovery = CancellationTokenSource.CreateLinkedTokenSource(token);
        recovery.CancelAfter(TimeSpan.FromSeconds(35));
        var expectedRelocations = retainsSource ? 0 : 1;
        while (handle.SuccessfulRelocations < expectedRelocations || !handle.IsConnected)
        {
            Assert.Null(handle.Failure);
            Assert.False(completion.IsCompleted);
            await Task.Delay(20, recovery.Token);
        }
        await VerifySlotOwnerAsync(cluster, slot, retainsSource ? 0 : 1, protocol, token);
        var emptyNode = retainsSource ? target : source;
        var owner = retainsSource ? source : target;
        Assert.Empty(
            (await emptyNode.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "2"), token)).AsArray()
        );
        var finalKeys = (
            await owner.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "3"), token)
        ).AsArray();
        Assert.Equal(2, finalKeys.Count);
        Assert.Contains(finalKeys, key => key.AsBytes().Span.SequenceEqual(expiringKey));
        Assert.Contains(finalKeys, key => key.AsBytes().Span.SequenceEqual(persistentKey));
        await VerifyTransferredValuesAsync(
            commands,
            expiringKey,
            expiringValue,
            persistentKey,
            persistentValue,
            originalExpiry,
            token
        );
        await VerifyDeliveryAsync(commands, channel, messages, 3, token);
        await VerifyDeliveryAsync(commands, stationary, stationaryMessages, 3, token);
        Assert.Same(completion, handle.Completion);
        Assert.False(completion.IsCompleted);
        Assert.Null(handle.Failure);
        Assert.Equal(expectedRelocations, handle.ConnectionLosses);
        Assert.Equal(expectedRelocations, handle.SuccessfulReconnects);
        Assert.Equal(expectedRelocations, handle.SuccessfulRelocations);
        if (retainsSource)
        {
            Assert.Equal(0, handle.ReconnectAttempts);
        }
        Assert.Equal(0, handle.DroppedMessages);
        Assert.Equal(0, untouched.ConnectionLosses);
        Assert.Equal(
            retainsSource ? 1 : 0,
            (await source.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                .AsArray()[1]
                .AsInt64()
        );
        Assert.Equal(
            retainsSource ? 0 : 1,
            (await target.ExecuteAsync(new ValkeyCommand("PUBSUB", "SHARDNUMSUB", channel), token))
                .AsArray()[1]
                .AsInt64()
        );
        var finalExpiry = (await owner.ExecuteAsync(new ValkeyCommand("PEXPIRETIME", expiringKey), token)).AsInt64();
        if (atomic || sourceOnlyIoError)
        {
            Assert.Equal(originalExpiry, finalExpiry);
        }
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"slot={slot}; destination_keys={(retainsSource ? 0 : 2)}; expiry_shift_ms={finalExpiry - originalExpiry}; mode={mode}; relocations={handle.SuccessfulRelocations}; attempts={handle.ReconnectAttempts}; dropped={handle.DroppedMessages}"
        );
        Assert.Equal(
            2,
            (
                await commands.ExecuteAsync(expiringKey, new ValkeyCommand("DEL", expiringKey, persistentKey), token)
            ).AsInt64()
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

    private static async Task VerifyTransferredValuesAsync(
        ValkeyClusterClient client,
        byte[] expiringKey,
        byte[] expiringValue,
        byte[] persistentKey,
        byte[] persistentValue,
        long originalExpiry,
        CancellationToken token
    )
    {
        Assert.Equal(
            expiringValue,
            (await client.ExecuteAsync(expiringKey, new ValkeyCommand("GET", expiringKey), token)).AsBytes().ToArray()
        );
        Assert.Equal(
            persistentValue,
            (await client.ExecuteAsync(persistentKey, new ValkeyCommand("GET", persistentKey), token))
                .AsBytes()
                .ToArray()
        );
        var expiry = (
            await client.ExecuteAsync(expiringKey, new ValkeyCommand("PEXPIRETIME", expiringKey), token)
        ).AsInt64();
        // MIGRATE carries relative TTL; allow up to one second for local transfer/clock skew.
        Assert.InRange(expiry - originalExpiry, -1000, 1000);
        Assert.InRange(
            (await client.ExecuteAsync(expiringKey, new ValkeyCommand("PTTL", expiringKey), token)).AsInt64(),
            1,
            121000
        );
        Assert.Equal(
            -1,
            (await client.ExecuteAsync(persistentKey, new ValkeyCommand("PTTL", persistentKey), token)).AsInt64()
        );
    }
}
