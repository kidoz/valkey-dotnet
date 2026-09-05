using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    internal async Task MigrateOwnedKeyWithLostReplyAsync(byte[] key, ValkeyProtocol protocol, CancellationToken token)
    {
        ValidateTransferKey(key);
        var slot = ValkeyClusterClient.GetHashSlot(key);
        var (sourceId, targetId) = await VerifyMigrationAsync(slot, 0, 1, 2, token);
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
        await VerifyNodeAsync(0, bounded);
        await VerifyNodeAsync(1, bounded);
        await using var proxy = new MigrationReplyLossProxy(NodeOptions(0, protocol).Port);
        await using var client = await ValkeyClient.ConnectAsync(
            new ValkeyClientOptions
            {
                Host = "127.0.0.1",
                Port = proxy.Port,
                Protocol = protocol,
                ClientName = Project,
                ConnectTimeout = TimeSpan.FromSeconds(2),
            },
            bounded
        );
        // The proxy only withholds this connection's next exact OK; other application sockets are untouched.
        proxy.Arm();
        var error = await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            client.ExecuteAsync(new ValkeyCommand("MIGRATE", Service(1), "6379", key, "0", "2000"), bounded)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        await proxy.Completion.WaitAsync(bounded);
        Assert.Equal(1, proxy.DroppedAcknowledgements);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.PingAsync(bounded));
        // Reconcile from independent node-local observations, never by replaying MIGRATE or deleting a copy.
        await VerifyMigrationAsync(slot, 0, 1, 1, bounded, expectedTargetKeys: 1);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"slot={slot}; dropped_migrate_ok=1; caller_delivery=MayHaveBeenSent; subsequent_ping=ObjectDisposedException; source_keys=1; destination_keys=1; migrate_replayed=false"
        );
    }
}
