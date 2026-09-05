using System.Text;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class MigrationIoErrorTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransferTimeoutRequiresInitializedOwnedThreePrimaryCluster(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        var key = Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.TimeoutOwnedKeyBeforeRestoreAsync(key, ValkeyProtocol.Resp3, TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task TimeoutRejectsUnscopedAndOversizedKeysBeforeDockerAccess()
    {
        await using var cluster = new MigrationValkeyCluster();
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.TimeoutOwnedKeyBeforeRestoreAsync(
                "external"u8.ToArray(),
                ValkeyProtocol.Resp2,
                TestContext.Current.CancellationToken
            )
        );
        byte[] key = [.. Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}"), .. new byte[512]];
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.TimeoutOwnedKeyBeforeRestoreAsync(key, ValkeyProtocol.Resp2, TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public void PausedRestoreObservationKeepsExactIdentityAndAllowsNoMatch()
    {
        Assert.Null(MigrationValkeyCluster.PausedRestoreClientId(""));
        Assert.Null(MigrationValkeyCluster.PausedRestoreClientId("id=1 cmd=ping flags=N\n"));
        Assert.Equal(
            "42",
            MigrationValkeyCluster.PausedRestoreClientId(
                "id=1 cmd=ping flags=N\nid=42 cmd=restore-asking flags=b age=0\r\n"
            )
        );
        Assert.Throws<InvalidOperationException>(() =>
            MigrationValkeyCluster.PausedRestoreClientId(new string('x', 16385))
        );
    }

    [Theory]
    [InlineData("cmd=restore-asking flags=b")]
    [InlineData("id=0 cmd=restore-asking flags=b")]
    [InlineData("id=-1 cmd=restore-asking flags=b")]
    [InlineData("id=+1 cmd=restore-asking flags=b")]
    [InlineData("id=18446744073709551616 cmd=restore-asking flags=b")]
    [InlineData("id=1 cmd=restore-asking flags=N")]
    [InlineData("id=1 cmd=restore-asking")]
    [InlineData("id=1 cmd=restore-asking flags=b\nid=2 cmd=restore-asking flags=b")]
    [InlineData("id=1 id=2 cmd=restore-asking flags=b")]
    [InlineData("id=1 cmd=restore-asking cmd=ping flags=b")]
    [InlineData("id=1 cmd=restore-asking flags=b flags=N")]
    public void PausedRestoreObservationRejectsInvalidOrAmbiguousIdentity(string text)
    {
        Assert.Throws<InvalidOperationException>(() => MigrationValkeyCluster.PausedRestoreClientId(text));
    }
}
