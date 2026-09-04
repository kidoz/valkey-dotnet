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
}
