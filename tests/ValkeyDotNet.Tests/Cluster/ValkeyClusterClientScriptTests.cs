using System.Globalization;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests.Cluster;

public sealed class ValkeyClusterClientScriptTests
{
    [Theory]
    [InlineData("MOVED")]
    [InlineData("ASK")]
    public async Task RedirectedScriptsRecoverOnTheTargetAndRepeatAskingWhenRequired(string redirect)
    {
        const string key = "{script}:key";
        var script = new ValkeyScript("return ARGV[1]");
        var slot = ValkeyClusterClient.GetHashSlot(key);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            for (var attempt = 0; attempt < 3; attempt++)
            {
                if (redirect == "ASK")
                {
                    Assert.Equal(["ASKING"], await session.ReadCommandAsync());
                    await session.SendAsync("+OK\r\n");
                }
                var command = await session.ReadCommandAsync();
                Assert.Equal(attempt < 2 ? "EVALSHA" : "EVAL", command[0]);
                Assert.Equal(["1", key, "result"], command[2..]);
                await session.SendAsync(attempt < 2 ? "-NOSCRIPT Missing\r\n" : "+result\r\n");
            }
        });
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(Topology(seedServer!.Port));
            Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
            await session.SendAsync($"-{redirect} {slot} 127.0.0.1:{target.Port}\r\n");
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()] },
            TestContext.Current.CancellationToken
        );
        var reply = await cluster.ExecuteScriptWithDeadlineAsync(
            script,
            [key],
            ["result"],
            TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken
        );
        Assert.Equal("result", reply.AsString());
        await target.Session;
        Assert.Single(target.ReceivedCommands, c => c[0] == "EVAL");
    }

    [Fact]
    public async Task ClusterRejectsMissingAndCrossSlotKeysBeforeSending()
    {
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(Topology(seedServer!.Port));
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()] },
            TestContext.Current.CancellationToken
        );
        var script = new ValkeyScript("return 1");
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.ExecuteScriptAsync(script, [], [], TestContext.Current.CancellationToken)
        );
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.ExecuteScriptAsync(script, ["{a}:key", "{b}:key"], [], TestContext.Current.CancellationToken)
        );
        Assert.Equal("PONG", await cluster.PingAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ReplacementNodeStartsFreshAndDoesNotReplayTheFailedInvocation()
    {
        var script = new ValkeyScript("return ARGV[1]");
        await using var target = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                for (var attempt = 0; attempt < 2; attempt++)
                {
                    Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
                    await session.SendAsync("-NOSCRIPT Missing\r\n");
                }
                Assert.Equal("EVAL", (await session.ReadCommandAsync())[0]);
                if (index == 0)
                    session.Close();
                else
                    await session.SendAsync("+fresh\r\n");
            }
        );
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(Topology(target.Port));
        });
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()] },
            TestContext.Current.CancellationToken
        );
        await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            cluster.ExecuteScriptAsync(script, ["key"], ["old"], TestContext.Current.CancellationToken)
        );
        Assert.Equal(
            "fresh",
            (
                await cluster.ExecuteScriptAsync(script, ["key"], ["fresh"], TestContext.Current.CancellationToken)
            ).AsString()
        );
        await target.Session;
    }

    private static string Topology(int port) =>
        "*1\r\n%2\r\n+slots\r\n*2\r\n:0\r\n:16383\r\n+nodes\r\n*1\r\n%4\r\n+role\r\n+master\r\n+endpoint\r\n+127.0.0.1\r\n+port\r\n:"
        + port.ToString(CultureInfo.InvariantCulture)
        + "\r\n+health\r\n+online\r\n";
}
