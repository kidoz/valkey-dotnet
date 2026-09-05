using System.Net;
using System.Net.Sockets;
using System.Text;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

// Process-local transport checks; never start Docker or contact an external endpoint.
public sealed class MigrationReplyLossProxyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StopsAtTheByteBudgetInEitherDirection(bool fromServer)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        var token = deadline.Token;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start(1);
        await using var proxy = new MigrationReplyLossProxy(((IPEndPoint)listener.LocalEndpoint).Port);
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, proxy.Port, token);
        using var server = await listener.AcceptTcpClientAsync(token);
        var writer = fromServer ? server.GetStream() : client.GetStream();
        var reader = fromServer ? client.GetStream() : server.GetStream();
        var drain = DrainAsync();
        await writer.WriteAsync(new byte[65537], token);
        await Assert.ThrowsAsync<InvalidOperationException>(() => proxy.Completion.WaitAsync(token));
        Assert.InRange(await drain, 0, 65536);
        Assert.Equal(0, proxy.DroppedAcknowledgements);

        async Task<int> DrainAsync()
        {
            var buffer = new byte[4096];
            var total = 0;
            while (true)
            {
                var count = await reader.ReadAsync(buffer, token);
                if (count == 0)
                {
                    return total;
                }
                total += count;
            }
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(65536)]
    public void RejectsInvalidSourcePort(int port)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new MigrationReplyLossProxy(port));
    }

    [Fact]
    public async Task CanOnlyArmOnceAndDisposesWithoutAcceptedClient()
    {
        await using var proxy = new MigrationReplyLossProxy(1);
        proxy.Arm();
        Assert.Throws<InvalidOperationException>(proxy.Arm);
        await proxy.DisposeAsync();
        await proxy.DisposeAsync();
        Assert.Throws<InvalidOperationException>(proxy.Arm);
        Assert.Equal(0, proxy.DroppedAcknowledgements);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    public async Task RelaysBinaryTrafficThenDropsOnlyOneSuccessReply(int fragmentSize)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        var token = deadline.Token;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start(1);
        await using var proxy = new MigrationReplyLossProxy(((IPEndPoint)listener.LocalEndpoint).Port);
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, proxy.Port, token);
        using var server = await listener.AcceptTcpClientAsync(token);
        server.NoDelay = true;
        byte[] payload = [0, 255, 13, 10];
        await client.GetStream().WriteAsync(payload, token);
        var received = new byte[payload.Length];
        await server.GetStream().ReadExactlyAsync(received, token);
        Assert.Equal(payload, received);
        await server.GetStream().WriteAsync(payload, token);
        await client.GetStream().ReadExactlyAsync(received, token);
        Assert.Equal(payload, received);
        proxy.Arm();
        var ok = "+OK\r\n"u8.ToArray();
        for (var index = 0; index < ok.Length; index += fragmentSize)
        {
            await server.GetStream().WriteAsync(ok.AsMemory(index, fragmentSize), token);
        }
        Assert.Equal(0, await client.GetStream().ReadAsync(received, token));
        await proxy.Completion.WaitAsync(token);
        Assert.Equal(1, proxy.DroppedAcknowledgements);
        using var second = new TcpClient();
        await Assert.ThrowsAsync<SocketException>(async () =>
            await second.ConnectAsync(IPAddress.Loopback, proxy.Port, token)
        );
    }

    [Theory]
    [InlineData("+NOKEY\r\n")]
    [InlineData("-ERR refused\r\n")]
    [InlineData("+NO\r\n")]
    public async Task RefusesUnexpectedReplyInsteadOfClaimingSuccessfulTransfer(string reply)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        var token = deadline.Token;
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start(1);
        await using var proxy = new MigrationReplyLossProxy(((IPEndPoint)listener.LocalEndpoint).Port);
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, proxy.Port, token);
        using var server = await listener.AcceptTcpClientAsync(token);
        proxy.Arm();
        await server.GetStream().WriteAsync(Encoding.UTF8.GetBytes(reply), token);
        await Assert.ThrowsAsync<InvalidOperationException>(() => proxy.Completion.WaitAsync(token));
        Assert.Equal(0, proxy.DroppedAcknowledgements);
    }
}
