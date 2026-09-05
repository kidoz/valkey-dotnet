namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

// Harness-only checks: these never start Docker or mutate a server.
public sealed class MigrationValkeyClusterTests
{
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
