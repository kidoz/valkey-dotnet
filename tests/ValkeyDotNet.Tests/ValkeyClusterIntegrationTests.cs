using System.Globalization;

namespace ValkeyDotNet.Tests;

public sealed class ValkeyClusterIntegrationTests
{
    [Fact]
    public async Task ClusterRoutesAndPipelinesAcrossPrimaries()
    {
        var endpointText = Environment.GetEnvironmentVariable("VALKEYDOTNET_CLUSTER_ENDPOINTS");
        if (string.IsNullOrWhiteSpace(endpointText))
            Assert.Skip("Set VALKEYDOTNET_CLUSTER_ENDPOINTS to run the live Valkey Cluster test.");

        var seeds = endpointText
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(ParseEndpoint)
            .Select(static endpoint => new ValkeyClientOptions { Host = endpoint.Host, Port = endpoint.Port })
            .ToArray();
        var mappedHost = Environment.GetEnvironmentVariable("VALKEYDOTNET_CLUSTER_MAPPED_HOST");
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            new ValkeyClusterOptions
            {
                SeedNodes = seeds,
                EndpointMapper = string.IsNullOrWhiteSpace(mappedHost)
                    ? null
                    : endpoint => new ValkeyClusterEndpoint(mappedHost, endpoint.Port),
            },
            TestContext.Current.CancellationToken
        );

        Assert.Equal("PONG", await cluster.PingAsync(TestContext.Current.CancellationToken));
        var prefix = "valkey-dotnet:cluster:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        var keys = new[] { FindKey(prefix, 0, 5_460), FindKey(prefix, 5_461, 10_922), FindKey(prefix, 10_923, 16_383) };

        var operationsCompleted = false;
        try
        {
            var writes = await cluster.ExecutePipelineAsync(
                keys.Select(
                    (key, index) => new ValkeyClusterCommand(key, new ValkeyCommand("SET", key, $"value-{index}"))
                ),
                TestContext.Current.CancellationToken
            );
            Assert.All(writes, static reply => Assert.Equal("OK", reply.AsString()));

            var reads = await cluster.ExecutePipelineAsync(
                keys.Select(key => new ValkeyClusterCommand(key, new ValkeyCommand("GET", key))),
                TestContext.Current.CancellationToken
            );
            Assert.Equal(["value-0", "value-1", "value-2"], reads.Select(static reply => reply.AsString()));

            await cluster.RefreshTopologyAsync(TestContext.Current.CancellationToken);
            operationsCompleted = true;
        }
        finally
        {
            foreach (var key in keys)
            {
                try
                {
                    await cluster.DeleteAsync(key, TestContext.Current.CancellationToken);
                }
                catch (ValkeyException) when (!operationsCompleted)
                {
                    // Preserve the operation failure; best-effort cleanup must not replace it.
                }
            }
        }
    }

    private static ValkeyClusterEndpoint ParseEndpoint(string value)
    {
        var separator = value.LastIndexOf(':');
        if (separator <= 0)
            throw new FormatException($"Cluster endpoint '{value}' must contain a host and port.");
        var host = value[..separator].Trim('[', ']');
        var port = int.Parse(value[(separator + 1)..], CultureInfo.InvariantCulture);
        return new ValkeyClusterEndpoint(host, port);
    }

    private static string FindKey(string prefix, int minimumSlot, int maximumSlot)
    {
        for (var index = 0; ; index++)
        {
            var key = prefix + ':' + index.ToString(CultureInfo.InvariantCulture);
            var slot = ValkeyClusterClient.GetHashSlot(key);
            if (slot >= minimumSlot && slot <= maximumSlot)
                return key;
        }
    }
}
