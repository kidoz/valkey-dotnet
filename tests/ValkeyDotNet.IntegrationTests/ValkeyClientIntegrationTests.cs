using System.Globalization;

namespace ValkeyDotNet.IntegrationTests;

public sealed class ValkeyClientIntegrationTests
{
    private static (string Host, int Port) GetEndpoint()
    {
        var endpoint = Environment.GetEnvironmentVariable("VALKEYDOTNET_ENDPOINT");
        if (string.IsNullOrWhiteSpace(endpoint))
            Assert.Skip("Set VALKEYDOTNET_ENDPOINT to run the live Valkey integration test.");

        var parts = endpoint.Split(':', 2);
        return (parts[0], parts.Length == 2 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 6379);
    }

    [Fact]
    public async Task ClientRoundTripsAgainstLiveServer()
    {
        var endpoint = GetEndpoint();
        var options = new ValkeyClientOptions
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            ClientName = "valkey-dotnet-tests",
        };
        await using var client = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);
        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));

        var key = "valkey-dotnet:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        Assert.True(
            await client.SetStringAsync(
                key,
                "works",
                TimeSpan.FromSeconds(30),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        Assert.Equal("works", await client.GetStringAsync(key, TestContext.Current.CancellationToken));
        Assert.Equal(1L, await client.DeleteAsync([key], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ResponseDrainTimeoutRetiresABlockedLiveConnectionAndAllowsReplacement()
    {
        var endpoint = GetEndpoint();
        var options = new ValkeyClientOptions
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            ClientName = "valkey-dotnet-stall-test",
            ResponseDrainTimeout = TimeSpan.FromMilliseconds(500),
        };
        await using var client = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);
        var key = "valkey-dotnet:blocked:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

        var blocked = client.ExecuteWithDeadlineAsync(
            new ValkeyCommand("BLPOP", key, 0),
            TimeSpan.FromMilliseconds(500),
            TestContext.Current.CancellationToken
        );
        var deadlineFailure = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(async () => await blocked);
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, deadlineFailure.DeliveryStatus);

        var connectionFailure = await Assert.ThrowsAsync<ValkeyConnectionException>(async () =>
            await client.PingAsync(TestContext.Current.CancellationToken)
        );
        Assert.IsType<TimeoutException>(connectionFailure.InnerException);

        await using var replacement = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);
        Assert.Equal("PONG", await replacement.PingAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ScriptsPreserveBinaryDataAndRecoverAfterFlushAndConnectionReplacement(ValkeyProtocol protocol)
    {
        var endpoint = GetEndpoint();
        var options = new ValkeyClientOptions
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            Protocol = protocol,
        };
        await using var client = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);
        var key = "valkey-dotnet:script:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        var increment = new ValkeyScript(
            "local n = redis.call('INCR', KEYS[1]); redis.call('PEXPIRE', KEYS[1], 60000); return n"
        );
        var echo = new ValkeyScript("return {KEYS[1], ARGV[1]}");
        byte[] binaryKey = [0, 255, 13, 10];
        byte[] binaryValue = [254, 0, 10, 13];
        try
        {
            var echoed = (
                await client.ExecuteScriptAsync(echo, [binaryKey], [binaryValue], TestContext.Current.CancellationToken)
            ).AsArray();
            Assert.Equal(binaryKey, echoed[0].AsBytes().ToArray());
            Assert.Equal(binaryValue, echoed[1].AsBytes().ToArray());
            for (var round = 0; round < 2; round++)
            {
                await client.ExecuteAsync(new ValkeyCommand("SCRIPT", "FLUSH"), TestContext.Current.CancellationToken);
                var replies = await Task.WhenAll(
                    Enumerable
                        .Range(0, 16)
                        .Select(_ =>
                            client.ExecuteScriptAsync(increment, [key], [], TestContext.Current.CancellationToken)
                        )
                );
                Assert.Equal(
                    Enumerable.Range(round * 16 + 1, 16).Select(i => (long)i),
                    replies.Select(r => r.AsInt64()).Order()
                );
            }
            await using var replacement = await ValkeyClient.ConnectAsync(
                options,
                TestContext.Current.CancellationToken
            );
            await replacement.ExecuteAsync(new ValkeyCommand("SCRIPT", "FLUSH"), TestContext.Current.CancellationToken);
            Assert.Equal(
                33,
                (
                    await replacement.ExecuteScriptWithDeadlineAsync(
                        increment,
                        [key],
                        [],
                        TimeSpan.FromSeconds(5),
                        TestContext.Current.CancellationToken
                    )
                ).AsInt64()
            );
            var pipelined = await replacement.ExecutePipelineAsync(
                [echo.CreateCommand([binaryKey], [binaryValue])],
                TestContext.Current.CancellationToken
            );
            Assert.Equal(binaryValue, pipelined[0].AsArray()[1].AsBytes().ToArray());
        }
        finally
        {
            await client.DeleteAsync([key], TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task LeaseScriptsRejectStaleOwnersAndAtomicallyExtendAndRelease()
    {
        var endpoint = GetEndpoint();
        await using var client = await ValkeyClient.ConnectAsync(
            new ValkeyClientOptions { Host = endpoint.Host, Port = endpoint.Port },
            TestContext.Current.CancellationToken
        );
        var key = "valkey-dotnet:lease:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        var release = new ValkeyScript(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
        );
        var extend = new ValkeyScript(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end"
        );
        byte[] oldOwner = [0, 255, 1];
        byte[] newOwner = [0, 255, 2];
        try
        {
            Assert.True(
                await client.SetAsync(
                    key,
                    oldOwner,
                    TimeSpan.FromMinutes(1),
                    onlyIfNotExists: true,
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            Assert.False(
                await client.SetAsync(
                    key,
                    newOwner,
                    TimeSpan.FromMinutes(1),
                    onlyIfNotExists: true,
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            // Expire the old lease before acquiring a replacement owner on the same key.
            await client.ExecuteAsync(new ValkeyCommand("PEXPIRE", key, 0), TestContext.Current.CancellationToken);
            Assert.True(
                await client.SetAsync(
                    key,
                    newOwner,
                    TimeSpan.FromMinutes(1),
                    onlyIfNotExists: true,
                    cancellationToken: TestContext.Current.CancellationToken
                )
            );
            Assert.Equal(
                0,
                (
                    await client.ExecuteScriptAsync(release, [key], [oldOwner], TestContext.Current.CancellationToken)
                ).AsInt64()
            );
            Assert.Equal(
                0,
                (
                    await client.ExecuteScriptAsync(
                        extend,
                        [key],
                        [oldOwner, 120000],
                        TestContext.Current.CancellationToken
                    )
                ).AsInt64()
            );
            Assert.Equal(
                1,
                (
                    await client.ExecuteScriptAsync(
                        extend,
                        [key],
                        [newOwner, 120000],
                        TestContext.Current.CancellationToken
                    )
                ).AsInt64()
            );
            Assert.InRange(
                (
                    await client.ExecuteAsync(new ValkeyCommand("PTTL", key), TestContext.Current.CancellationToken)
                ).AsInt64(),
                60001,
                120000
            );
            Assert.Equal(newOwner, await client.GetAsync(key, TestContext.Current.CancellationToken));
            Assert.Equal(
                1,
                (
                    await client.ExecuteScriptAsync(release, [key], [newOwner], TestContext.Current.CancellationToken)
                ).AsInt64()
            );
            Assert.Null(await client.GetAsync(key, TestContext.Current.CancellationToken));
        }
        finally
        {
            await client.DeleteAsync([key], TestContext.Current.CancellationToken);
        }
    }
}
