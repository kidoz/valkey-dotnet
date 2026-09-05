using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    internal void ValidateBulkTransferKeys(byte[] movingKey, byte[] conflictingKey)
    {
        ValidateTransferKey(movingKey);
        ValidateTransferKey(conflictingKey);
        if (
            movingKey.AsSpan().SequenceEqual(conflictingKey)
            || ValkeyClusterClient.GetHashSlot(movingKey) != ValkeyClusterClient.GetHashSlot(conflictingKey)
        )
        {
            throw new ArgumentException("Bulk conflict requires two distinct owned keys in one slot.");
        }
    }

    internal async Task MigrateOwnedBulkWithConflictAsync(
        byte[] movingKey,
        byte[] conflictingKey,
        bool conflictFirst,
        ValkeyProtocol protocol,
        CancellationToken token
    )
    {
        ValidateBulkTransferKeys(movingKey, conflictingKey);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        var bounded = deadline.Token;
        var (sourceId, targetId) = await VerifyMigrationMembershipAsync(0, 1, bounded);
        var slot = ValkeyClusterClient.GetHashSlot(movingKey);
        var number = slot.ToString(CultureInfo.InvariantCulture);
        Assert.Contains(
            "[" + number + "->-" + targetId + "]",
            await CommandAsync(0, ["CLUSTER", "NODES"], bounded),
            StringComparison.Ordinal
        );
        Assert.Contains(
            "[" + number + "-<-" + sourceId + "]",
            await CommandAsync(1, ["CLUSTER", "NODES"], bounded),
            StringComparison.Ordinal
        );
        await using var source = await ValkeyClient.ConnectAsync(NodeOptions(0, protocol), bounded);
        await using var target = await ValkeyClient.ConnectAsync(NodeOptions(1, protocol), bounded);
        // Only this path permits three physical copies: two exact source keys and one target conflict.
        // The ordinary migration fixture's total-key bound remains two.
        await VerifyBulkKeysAsync(source, number, [movingKey, conflictingKey], bounded);
        await VerifyBulkKeysAsync(target, number, [conflictingKey], bounded);
        Assert.Equal("0", await CommandAsync(2, ["DBSIZE"], bounded));
        Assert.DoesNotContain(
            "cmdstat_migrate:",
            (await source.ExecuteAsync(new ValkeyCommand("INFO", "COMMANDSTATS"), bounded)).AsString()!,
            StringComparison.Ordinal
        );
        await VerifyNodeAsync(0, bounded);
        await VerifyNodeAsync(1, bounded);
        var first = conflictFirst ? conflictingKey : movingKey;
        var second = conflictFirst ? movingKey : conflictingKey;
        // Exactly one two-key batch, no COPY/REPLACE/auth, retry, or automatic resolution.
        var error = await Assert.ThrowsAsync<ValkeyServerException>(() =>
            source.ExecuteAsync(
                new ValkeyCommand("MIGRATE", Service(1), "6379", "", "0", "2000", "KEYS", first, second),
                bounded
            )
        );
        Assert.Equal("ERR", error.ErrorCode);
        Assert.Equal(ValkeyCommandDeliveryStatus.ReplyReceived, error.DeliveryStatus);
        Assert.Equal("ERR Target instance replied with error: BUSYKEY Target key name already exists.", error.Message);
        Assert.Equal("PONG", await source.PingAsync(bounded));
        byte[] sentinel = [255, 0, 13, 10, 42];
        Assert.Equal(
            sentinel,
            (await source.ExecuteAsync(new ValkeyCommand("ECHO", sentinel), bounded)).AsBytes().ToArray()
        );
        var stats = (await source.ExecuteAsync(new ValkeyCommand("INFO", "COMMANDSTATS"), bounded)).AsString()!;
        Assert.StartsWith(
            "cmdstat_migrate:calls=1,",
            Assert.Single(stats.Split('\n'), line => line.StartsWith("cmdstat_migrate:", StringComparison.Ordinal)),
            StringComparison.Ordinal
        );
        await VerifyBulkKeysAsync(source, number, [conflictingKey], bounded);
        await VerifyBulkKeysAsync(target, number, [movingKey, conflictingKey], bounded);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"slot={slot}; conflict_first={conflictFirst}; migrate_calls=1; batch_keys=2; outer_error=ERR; target_error=BUSYKEY; delivery=ReplyReceived; source_keys=1; destination_keys=2; same_connection_ping=PONG; replay=false; overwrite=false; cutover=false"
        );
    }

    internal static async Task VerifyBulkKeysAsync(
        ValkeyClient client,
        string slot,
        byte[][] expected,
        CancellationToken token
    )
    {
        var keys = (
            await client.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", slot, "3"), token)
        ).AsArray();
        Assert.Equal(expected.Length, keys.Count);
        foreach (var key in expected)
        {
            Assert.Single(keys, value => value.AsBytes().Span.SequenceEqual(key));
        }
        Assert.Equal(
            expected.Length,
            (await client.ExecuteAsync(new ValkeyCommand("CLUSTER", "COUNTKEYSINSLOT", slot), token)).AsInt64()
        );
        Assert.Equal(expected.Length, (await client.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64());
    }
}
