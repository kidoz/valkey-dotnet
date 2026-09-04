using System.Globalization;
using System.Text;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

public sealed class ValkeyScriptTests
{
    [Fact]
    public void ScriptSeparatesBinaryKeysAndArgumentsAndUsesTheProtocolHash()
    {
        var script = new ValkeyScript("return 'Immabe a cached script'");
        Assert.Equal("c664a3bf70bd1d45c4284ffebb65a6f2299bfc9f", script.Sha1);
        byte[] key = [0, 255, 13, 10];
        byte[] argument = [254, 0, 10, 13];
        var command = script.CreateCommand([key], [argument]);
        Assert.Equal("EVAL", command.Name);
        Assert.Equal("1", Encoding.ASCII.GetString(command.Arguments[1].Bytes.Span));
        Assert.Equal(key, command.Arguments[2].Bytes.ToArray());
        Assert.Equal(argument, command.Arguments[3].Bytes.ToArray());
        Assert.Throws<ArgumentException>(() => new ValkeyScript(" "));
        Assert.Throws<ArgumentNullException>(() => script.CreateCommand(null!, []));
        Assert.Throws<ArgumentNullException>(() => script.CreateCommand([], null!));
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ConcurrentMissesReloadOncePerConnectionAndAfterFlush(ValkeyProtocol protocol)
    {
        const int callers = 8;
        const string source = "return ARGV[1]";
        var script = new ValkeyScript(source);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(
                protocol == ValkeyProtocol.Resp2 ? FakeValkeyServer.HelloResp2 : FakeValkeyServer.HelloResp3
            );
            for (var round = 0; round < 2; round++)
            {
                for (var i = 0; i < callers; i++)
                    Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
                for (var i = 0; i < callers; i++)
                    await session.SendAsync("-NOSCRIPT No matching script\r\n");

                Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
                await session.SendAsync("-NOSCRIPT No matching script\r\n");
                var load = await session.ReadCommandAsync();
                Assert.Equal("EVAL", load[0]);
                Assert.Equal(source, load[1]);
                await session.SendAsync($"+{load[3]}\r\n");
                for (var i = 1; i < callers; i++)
                {
                    var cached = await session.ReadCommandAsync();
                    Assert.Equal("EVALSHA", cached[0]);
                    await session.SendAsync($"+{cached[3]}\r\n");
                }
                Assert.Equal(["SCRIPT", "FLUSH"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
            }
        });
        await using var client = await ValkeyClient.ConnectAsync(
            new ValkeyClientOptions
            {
                Host = "127.0.0.1",
                Port = server.Port,
                Protocol = protocol,
            },
            TestContext.Current.CancellationToken
        );
        for (var round = 0; round < 2; round++)
        {
            // Separate script instances with equal source must share recovery coordination.
            var calls = Enumerable
                .Range(0, callers)
                .Select(i =>
                    client.ExecuteScriptAsync(new ValkeyScript(source), [], [i], TestContext.Current.CancellationToken)
                );
            var replies = await Task.WhenAll(calls)
                .WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            Assert.Equal(
                Enumerable.Range(0, callers).Select(i => i.ToString(CultureInfo.InvariantCulture)),
                replies.Select(r => r.AsString())
            );
            await client.ExecuteAsync(new ValkeyCommand("SCRIPT", "FLUSH"), TestContext.Current.CancellationToken);
        }
        await server.Session;
        Assert.Equal(2, server.ReceivedCommands.Count(c => c[0] == "EVAL"));
    }

    [Theory]
    [InlineData("ERR script failed")]
    [InlineData("NOPERM permission denied")]
    public async Task OrdinaryScriptErrorsAreNotRetried(string error)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
            await session.SendAsync($"-{error}\r\n");
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        var failure = await Assert.ThrowsAsync<ValkeyServerException>(() =>
            client.ExecuteScriptAsync(new ValkeyScript("return 1"), [], [], TestContext.Current.CancellationToken)
        );
        Assert.Equal(error, failure.Message);
        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FallbackFailureIsNeverReplayed(bool disconnect)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            for (var i = 0; i < 2; i++)
            {
                Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
                await session.SendAsync("-NOSCRIPT Missing\r\n");
            }
            Assert.Equal("EVAL", (await session.ReadCommandAsync())[0]);
            if (disconnect)
                session.Close();
            else
            {
                await session.SendAsync("-NOSCRIPT script returned this error\r\n");
                Assert.Equal(["PING"], await session.ReadCommandAsync());
                await session.SendAsync("+PONG\r\n");
            }
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        var call = client.ExecuteScriptAsync(
            new ValkeyScript("return 1"),
            [],
            [],
            TestContext.Current.CancellationToken
        );
        if (disconnect)
        {
            var failure = await Assert.ThrowsAsync<ValkeyConnectionException>(() => call);
            Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
        }
        else
        {
            await Assert.ThrowsAsync<ValkeyServerException>(() => call);
            Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
        }
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c[0] == "EVAL");
    }

    [Fact]
    public async Task DeadlineDuringFallbackDrainsTheReplyAndDoesNotReplay()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            for (var i = 0; i < 2; i++)
            {
                Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
                await session.SendAsync("-NOSCRIPT Missing\r\n");
            }
            Assert.Equal("EVAL", (await session.ReadCommandAsync())[0]);
            written.SetResult();
            await release.Task;
            await session.SendAsync(":1\r\n");
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        try
        {
            var call = client.ExecuteScriptWithDeadlineAsync(
                new ValkeyScript("return 1"),
                [],
                [],
                TimeSpan.FromMilliseconds(500),
                TestContext.Current.CancellationToken
            );
            await written.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            var failure = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() => call);
            Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
        }
        finally
        {
            release.TrySetResult();
        }
        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DeadlineWhileWaitingForRecoveryDoesNotSendAnotherAttempt()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new ValkeyScript("return ARGV[1]");
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal("leader", (await session.ReadCommandAsync())[3]);
            Assert.Equal("follower", (await session.ReadCommandAsync())[3]);
            // Only the leader's initial error is sent until it demonstrably owns the recovery gate.
            await session.SendAsync("-NOSCRIPT Missing\r\n");
            Assert.Equal("leader", (await session.ReadCommandAsync())[3]);
            await session.SendAsync("-NOSCRIPT Missing\r\n");
            held.SetResult();
            await release.Task;
            await session.SendAsync("-NOSCRIPT Missing\r\n");
            var fallback = await session.ReadCommandAsync();
            Assert.Equal(["EVAL", "return ARGV[1]", "0", "leader"], fallback);
            await session.SendAsync("+leader\r\n");
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        var leader = client.ExecuteScriptAsync(script, [], ["leader"], TestContext.Current.CancellationToken);
        try
        {
            var follower = client.ExecuteScriptWithDeadlineAsync(
                script,
                [],
                ["follower"],
                TimeSpan.FromMilliseconds(500),
                TestContext.Current.CancellationToken
            );
            await held.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            var failure = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() => follower);
            Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
        }
        finally
        {
            release.TrySetResult();
        }
        Assert.Equal("leader", (await leader).AsString());
        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CancellationDuringFallbackIsAmbiguousAndInvalidatesTheConnection()
    {
        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            for (var i = 0; i < 2; i++)
            {
                Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
                await session.SendAsync("-NOSCRIPT Missing\r\n");
            }
            Assert.Equal("EVAL", (await session.ReadCommandAsync())[0]);
            written.SetResult();
            await session.ReadCommandAsync();
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var call = client.ExecuteScriptAsync(new ValkeyScript("return 1"), [], [], cancellation.Token);
        await written.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();
        var failure = await Assert.ThrowsAsync<ValkeyCommandCanceledException>(() => call);
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.PingAsync(TestContext.Current.CancellationToken)
        );
    }
}
