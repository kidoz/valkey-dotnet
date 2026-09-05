namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

// Harness-only checks: these never start Docker or mutate a server.
public sealed class MigrationValkeyClusterTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(2)]
    public async Task RefusesMigrationWithUnexpectedKeyBudget(int expectedSourceKeys)
    {
        await using var cluster = new MigrationValkeyCluster();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cluster.BeginSlotMigrationAsync(0, 0, 1, expectedSourceKeys, TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesMigrationBeforeOwnedClusterInitialization(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.BeginSlotMigrationAsync(0, 0, 1, 0, TestContext.Current.CancellationToken)
        );
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.CompleteEmptySlotMigrationAsync(0, 0, 1, TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesPrimaryStopBeforeOwnedReplicaInitialization(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.StopOwnedPrimaryAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReplicaProfileMapsFourthNodeAndKeepsSeedSelectionExplicit()
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica: true);
        Assert.Equal(
            cluster.NodeOptions(1, ValkeyProtocol.Resp3).Port,
            Assert.Single(cluster.Options(ValkeyProtocol.Resp3, seed: 1).SeedNodes).Port
        );
        var mapped = cluster
            .Options(ValkeyProtocol.Resp3, seed: 1)
            .EndpointMapper!(new ValkeyClusterEndpoint("node-4", 6379));
        Assert.Equal(cluster.NodeOptions(3, ValkeyProtocol.Resp3).Port, mapped.Port);
        Assert.StartsWith("valkey-dotnet-failover-tests-", cluster.Project, StringComparison.Ordinal);
        Assert.Equal(
            4,
            Enumerable
                .Range(0, 4)
                .Select(index => cluster.NodeOptions(index, ValkeyProtocol.Resp3).Port)
                .Distinct()
                .Count()
        );
        Assert.Throws<InvalidOperationException>(() =>
            cluster.Options(ValkeyProtocol.Resp3).EndpointMapper!(new ValkeyClusterEndpoint("node-5", 6379))
        );
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("21")]
    [InlineData("invalid")]
    [InlineData("2147483648")]
    public void RejectsUnboundedOrInvalidCycleCount(string text)
    {
        Assert.Throws<InvalidOperationException>(() => MigrationValkeyCluster.ParseCycles(text));
    }

    [Fact]
    public void AcceptsDefaultAndBoundaryCycleCounts()
    {
        Assert.Equal(3, MigrationValkeyCluster.ParseCycles(null));
        Assert.Equal(1, MigrationValkeyCluster.ParseCycles("1"));
        Assert.Equal(20, MigrationValkeyCluster.ParseCycles("20"));
    }

    [Theory]
    [InlineData("external.invalid", 6379)]
    [InlineData("node-1", 6380)]
    [InlineData("node-4", 6379)]
    public async Task RejectsAnnouncementsOutsideOwnedNodes(string host, int port)
    {
        await using var cluster = new MigrationValkeyCluster();
        Assert.Throws<InvalidOperationException>(() =>
            cluster.Options(ValkeyProtocol.Resp3).EndpointMapper!(new ValkeyClusterEndpoint(host, port))
        );
    }

    [Fact]
    public async Task MapsOnlyOwnedNodesToDistinctLoopbackPorts()
    {
        await using var cluster = new MigrationValkeyCluster();
        var mapped = new HashSet<int>();
        for (var index = 0; index < 3; index++)
        {
            var node = cluster.NodeOptions(index, ValkeyProtocol.Resp2);
            var endpoint = cluster
                .Options(ValkeyProtocol.Resp2)
                .EndpointMapper!(new ValkeyClusterEndpoint("node-" + (index + 1), 6379));
            Assert.Equal("127.0.0.1", endpoint.Host);
            Assert.Equal(node.Port, endpoint.Port);
            Assert.Equal(ValkeyProtocol.Resp2, node.Protocol);
            Assert.True(mapped.Add(endpoint.Port));
        }
    }
}
