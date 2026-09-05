using System.Text;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class MigrationBusyKeyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConflictInjectionRequiresInitializedOwnedThreePrimaryCluster(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        var key = Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.RejectOwnedConflictingMigrationAsync(
                key,
                ValkeyProtocol.Resp3,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task ConflictInjectionRejectsExternalAndOversizedKeysBeforeDockerAccess()
    {
        await using var cluster = new MigrationValkeyCluster();
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.RejectOwnedConflictingMigrationAsync(
                "external"u8.ToArray(),
                ValkeyProtocol.Resp2,
                TestContext.Current.CancellationToken
            )
        );
        byte[] oversized = [.. Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}"), .. new byte[512]];
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.RejectOwnedConflictingMigrationAsync(
                oversized,
                ValkeyProtocol.Resp2,
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            cluster.RejectOwnedConflictingMigrationAsync(
                null!,
                ValkeyProtocol.Resp2,
                TestContext.Current.CancellationToken
            )
        );
    }
}
