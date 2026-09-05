using System.Text;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class BulkConflictTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task BulkTransferRequiresInitializedOwnedPrimaries(bool includeReplica, bool conflictFirst)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        var prefix = Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.MigrateOwnedBulkWithConflictAsync(
                [.. prefix, 0],
                [.. prefix, 255],
                conflictFirst,
                ValkeyProtocol.Resp3,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Theory]
    [InlineData("duplicate")]
    [InlineData("cross-slot")]
    [InlineData("external-first")]
    [InlineData("external-second")]
    [InlineData("oversized-first")]
    [InlineData("oversized-second")]
    public async Task InvalidBulkKeysAreRejectedBeforeDockerAccess(string variant)
    {
        await using var cluster = new MigrationValkeyCluster();
        byte[] first = [.. Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}"), 0];
        byte[] second = [.. Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}"), 255];
        switch (variant)
        {
            case "duplicate":
                second = first.ToArray();
                break;
            case "cross-slot":
                second = Encoding.UTF8.GetBytes("{" + cluster.Project + ":1}");
                Assert.NotEqual(ValkeyClusterClient.GetHashSlot(first), ValkeyClusterClient.GetHashSlot(second));
                break;
            case "external-first":
                first = "external"u8.ToArray();
                break;
            case "external-second":
                second = "external"u8.ToArray();
                break;
            case "oversized-first":
                first = [.. first, .. new byte[512]];
                break;
            case "oversized-second":
                second = [.. second, .. new byte[512]];
                break;
        }
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.MigrateOwnedBulkWithConflictAsync(
                first,
                second,
                false,
                ValkeyProtocol.Resp2,
                TestContext.Current.CancellationToken
            )
        );
    }
}
