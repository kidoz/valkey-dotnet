using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    internal async Task RejectOwnedConflictingMigrationAsync(
        byte[] key,
        ValkeyProtocol protocol,
        CancellationToken token
    )
    {
        ValidateTransferKey(key);
        var slot = ValkeyClusterClient.GetHashSlot(key);
        var (sourceId, targetId) = await VerifyMigrationAsync(slot, 0, 1, 1, token, expectedTargetKeys: 1);
        var number = slot.ToString(CultureInfo.InvariantCulture);
        Assert.Contains(
            "[" + number + "->-" + targetId + "]",
            await CommandAsync(0, ["CLUSTER", "NODES"], token),
            StringComparison.Ordinal
        );
        Assert.Contains(
            "[" + number + "-<-" + sourceId + "]",
            await CommandAsync(1, ["CLUSTER", "NODES"], token),
            StringComparison.Ordinal
        );
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        var bounded = deadline.Token;
        await using var source = await ValkeyClient.ConnectAsync(NodeOptions(0, protocol), bounded);
        await using var target = await ValkeyClient.ConnectAsync(NodeOptions(1, protocol), bounded);
        foreach (var client in new[] { source, target })
        {
            var keys = (
                await client.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "2"), bounded)
            ).AsArray();
            Assert.Equal(key, Assert.Single(keys).AsBytes().ToArray());
        }
        var before = (await source.ExecuteAsync(new ValkeyCommand("INFO", "COMMANDSTATS"), bounded)).AsString()!;
        Assert.DoesNotContain("cmdstat_migrate:", before, StringComparison.Ordinal);
        await VerifyNodeAsync(0, bounded);
        await VerifyNodeAsync(1, bounded);
        // Deliberately no REPLACE, COPY, KEYS, or retry. One conflicting single-key transfer only.
        var error = await Assert.ThrowsAsync<ValkeyServerException>(() =>
            source.ExecuteAsync(new ValkeyCommand("MIGRATE", Service(1), "6379", key, "0", "2000"), bounded)
        );
        Assert.Equal("ERR", error.ErrorCode);
        Assert.Equal(ValkeyCommandDeliveryStatus.ReplyReceived, error.DeliveryStatus);
        Assert.Equal("ERR Target instance replied with error: BUSYKEY Target key name already exists.", error.Message);
        // A fully received server rejection leaves the same physical connection synchronized and usable.
        Assert.Equal("PONG", await source.PingAsync(bounded));
        byte[] sentinel = [255, 0, 13, 10, 42];
        Assert.Equal(
            sentinel,
            (await source.ExecuteAsync(new ValkeyCommand("ECHO", sentinel), bounded)).AsBytes().ToArray()
        );
        var after = (await source.ExecuteAsync(new ValkeyCommand("INFO", "COMMANDSTATS"), bounded)).AsString()!;
        Assert.StartsWith(
            "cmdstat_migrate:calls=1,",
            Assert.Single(after.Split('\n'), line => line.StartsWith("cmdstat_migrate:", StringComparison.Ordinal)),
            StringComparison.Ordinal
        );
        await VerifyMigrationAsync(slot, 0, 1, 1, bounded, expectedTargetKeys: 1);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"slot={slot}; migrate_calls=1; outer_error=ERR; target_error=BUSYKEY; delivery=ReplyReceived; same_connection_ping=PONG; source_keys=1; destination_keys=1; overwrite=false; replay=false; cutover=false"
        );
    }
}
