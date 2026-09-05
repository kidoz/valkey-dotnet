using System.Globalization;
using System.Text;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    internal void ValidateTransferKey(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (key.Length > 512 || !key.AsSpan().StartsWith(Encoding.UTF8.GetBytes("{" + Project + ":")))
        {
            throw new ArgumentException(
                "Only a bounded binary key in this fixture namespace may be transferred.",
                nameof(key)
            );
        }
    }

    internal async Task MigrateOwnedKeyAsync(
        byte[] key,
        int source,
        int target,
        int remainingSourceKeys,
        ValkeyProtocol protocol,
        CancellationToken token
    )
    {
        ValidateTransferKey(key);
        if (remainingSourceKeys is < 1 or > 2)
        {
            throw new ArgumentOutOfRangeException(nameof(remainingSourceKeys));
        }
        var slot = ValkeyClusterClient.GetHashSlot(key);
        var (sourceId, targetId) = await VerifyMigrationAsync(
            slot,
            source,
            target,
            remainingSourceKeys,
            token,
            2 - remainingSourceKeys
        );
        var number = slot.ToString(CultureInfo.InvariantCulture);
        Assert.Contains(
            "[" + number + "->-" + targetId + "]",
            await CommandAsync(source, ["CLUSTER", "NODES"], token),
            StringComparison.Ordinal
        );
        Assert.Contains(
            "[" + number + "-<-" + sourceId + "]",
            await CommandAsync(target, ["CLUSTER", "NODES"], token),
            StringComparison.Ordinal
        );
        using var transfer = CancellationTokenSource.CreateLinkedTokenSource(token);
        transfer.CancelAfter(TimeSpan.FromSeconds(10));
        await using var client = await ValkeyClient.ConnectAsync(NodeOptions(source, protocol), transfer.Token);
        // Recheck the exact socket endpoints immediately before authorizing server-to-server I/O.
        await VerifyNodeAsync(source, transfer.Token);
        await VerifyNodeAsync(target, transfer.Token);
        var reply = await client.ExecuteAsync(
            new ValkeyCommand("MIGRATE", Service(target), "6379", key, "0", "2000"),
            transfer.Token
        );
        // Never retry an IOERR/timeout: the key might then exist at both nodes.
        Assert.Equal("OK", reply.AsString());
        Assert.Equal(
            (remainingSourceKeys - 1).ToString(CultureInfo.InvariantCulture),
            await CommandAsync(source, ["CLUSTER", "COUNTKEYSINSLOT", number], token)
        );
        Assert.Equal(
            (3 - remainingSourceKeys).ToString(CultureInfo.InvariantCulture),
            await CommandAsync(target, ["CLUSTER", "COUNTKEYSINSLOT", number], token)
        );
    }
}
