using System.Globalization;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ShardedPubSubRoutesAcrossThreePrimariesWithIndependentDuplicateHandles(ValkeyProtocol protocol)
    {
        var endpointText = Environment.GetEnvironmentVariable("VALKEYDOTNET_CLUSTER_ENDPOINTS");
        if (string.IsNullOrWhiteSpace(endpointText))
        {
            Assert.Skip("Set VALKEYDOTNET_CLUSTER_ENDPOINTS to an isolated disposable three-primary cluster.");
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        var token = deadline.Token;
        var seeds = endpointText
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(ParseEndpoint)
            .Select(endpoint => new ValkeyClientOptions
            {
                Host = endpoint.Host,
                Port = endpoint.Port,
                Protocol = protocol,
            })
            .ToArray();
        var mappedHost = Environment.GetEnvironmentVariable("VALKEYDOTNET_CLUSTER_MAPPED_HOST");
        var options = new ValkeyClusterOptions
        {
            SeedNodes = seeds,
            EndpointMapper = string.IsNullOrWhiteSpace(mappedHost)
                ? null
                : endpoint => new ValkeyClusterEndpoint(mappedHost, endpoint.Port),
        };
        await using var publisher = await ValkeyClusterClient.ConnectAsync(options, token);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions { Cluster = options },
            token
        );
        var prefix = "shards-" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        var tags = new[] { FindKey(prefix, 0, 5460), FindKey(prefix, 5461, 10922), FindKey(prefix, 10923, 16383) };
        foreach (var tag in tags)
        {
            byte[] channel = [.. System.Text.Encoding.UTF8.GetBytes("{" + tag + "}"), 0, 255, 13, 10];
            byte[] payload = [255, 0, 13, 10];
            await using var first = await subscriber.SubscribeAsync(channel, token);
            await using var second = await subscriber.SubscribeAsync(channel, token);
            await first.UnsubscribeAsync(token);
            await using var messages = second.ReadAllAsync(token).GetAsyncEnumerator(token);
            Assert.Equal(
                1,
                (
                    await publisher.ExecuteAsync(channel, new ValkeyCommand("SPUBLISH", channel, payload), token)
                ).AsInt64()
            );
            Assert.True(await messages.MoveNextAsync());
            Assert.True(messages.Current.IsSharded);
            Assert.Null(messages.Current.Pattern);
            Assert.Equal(channel, messages.Current.Channel.ToArray());
            Assert.Equal(payload, messages.Current.Payload.ToArray());
            Assert.Equal(0, second.DroppedMessages);
            await second.UnsubscribeAsync(token);
            Assert.Equal(
                0,
                (
                    await publisher.ExecuteAsync(channel, new ValkeyCommand("SPUBLISH", channel, payload), token)
                ).AsInt64()
            );
        }
        Assert.Equal(0, subscriber.SubscriptionCount);
        Assert.Equal("PONG", await publisher.PingAsync(token));
    }

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

            var script = new ValkeyScript("return {redis.call('GET', KEYS[1]), ARGV[1]}");
            byte[] owner = [0, 255, 13, 10];
            foreach (var key in keys)
            {
                await cluster.ExecuteAsync(
                    key,
                    new ValkeyCommand("SCRIPT", "FLUSH"),
                    TestContext.Current.CancellationToken
                );
                var results = await Task.WhenAll(
                    Enumerable
                        .Range(0, 8)
                        .Select(_ =>
                            cluster.ExecuteScriptWithDeadlineAsync(
                                script,
                                [key],
                                [owner],
                                TimeSpan.FromSeconds(10),
                                TestContext.Current.CancellationToken
                            )
                        )
                );
                Assert.All(results, result => Assert.Equal(owner, result.AsArray()[1].AsBytes().ToArray()));
                Assert.All(
                    results,
                    result =>
                        Assert.Equal(
                            "value-" + Array.IndexOf(keys, key).ToString(CultureInfo.InvariantCulture),
                            result.AsArray()[0].AsString()
                        )
                );
            }

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
