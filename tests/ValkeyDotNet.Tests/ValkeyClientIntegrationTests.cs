using System.Globalization;

namespace ValkeyDotNet.Tests;

public sealed class ValkeyClientIntegrationTests
{
    [Fact]
    public async Task ClientRoundTripsAgainstLiveServer()
    {
        var endpoint = Environment.GetEnvironmentVariable("VALKEYDOTNET_ENDPOINT");
        if (string.IsNullOrWhiteSpace(endpoint))
            Assert.Skip("Set VALKEYDOTNET_ENDPOINT to run the live Valkey integration test.");

        var parts = endpoint.Split(':', 2);
        var options = new ValkeyClientOptions
        {
            Host = parts[0],
            Port = parts.Length == 2 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 6379,
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
}
