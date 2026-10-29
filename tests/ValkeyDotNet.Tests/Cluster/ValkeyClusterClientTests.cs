using System.Globalization;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests.Cluster;

public sealed class ValkeyClusterClientTests
{
    [Fact]
    public void GetHashSlotUsesCrc16XmodemAndHashTags()
    {
        Assert.Equal(0x31C3 & 16_383, ValkeyClusterClient.GetHashSlot("123456789"));
        Assert.Equal(
            ValkeyClusterClient.GetHashSlot("user1000"),
            ValkeyClusterClient.GetHashSlot("{user1000}.following")
        );
        Assert.Equal(
            ValkeyClusterClient.GetHashSlot("user1000"),
            ValkeyClusterClient.GetHashSlot("{user1000}.followers")
        );
        Assert.NotEqual(ValkeyClusterClient.GetHashSlot("bar"), ValkeyClusterClient.GetHashSlot("foo{}{bar}"));
    }

    [Fact]
    public async Task ConnectAsyncPrefersShardsAndRoutesToTheSlotPrimary()
    {
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["GET", "cluster-key"], await session.ReadCommandAsync());
            await session.SendAsync("$5\r\nvalue\r\n");
        });
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(ShardsTopology(primary.Port));
        });

        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );

        Assert.Equal("value", await cluster.GetStringAsync("cluster-key", TestContext.Current.CancellationToken));
        await seed.Session;
        await primary.Session;
    }

    [Fact]
    public async Task ExecuteAsyncDeadlineLeavesTheRoutedConnectionUsable()
    {
        const string key = "cluster-deadline";
        var commandWritten = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseReply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        FakeValkeyServer? server = null;
        server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(ShardsTopology(server!.Port));
            Assert.Equal(["GET", key], await session.ReadCommandAsync());
            commandWritten.SetResult();
            await releaseReply.Task;
            await session.SendAsync("$4\r\nlate\r\n");
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
        });
        await using var clusterServer = server;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(server.Port),
            TestContext.Current.CancellationToken
        );
        ValkeyArgument routingKey = key;

        var command = cluster.ExecuteWithDeadlineAsync(
            routingKey,
            new ValkeyCommand("GET", routingKey),
            TimeSpan.FromMilliseconds(500),
            TestContext.Current.CancellationToken
        );
        await commandWritten.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        var failure = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(async () => await command);
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);

        releaseReply.SetResult();
        Assert.Equal("PONG", await cluster.PingAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ResponseDrainTimeoutReplacesTheStalledNodeConnectionForTheNextCommand()
    {
        const string key = "cluster-stalled-reply";
        var firstCommandWritten = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstConnectionRetired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var primary = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                Assert.Equal(["GET", key], await session.ReadCommandAsync());
                if (index == 0)
                {
                    firstCommandWritten.SetResult();
                    try
                    {
                        await session.ReadCommandAsync();
                    }
                    finally
                    {
                        firstConnectionRetired.SetResult();
                    }
                }
                else
                {
                    await session.SendAsync("$5\r\nfresh\r\n");
                }
            }
        );
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(ShardsTopology(primary.Port));
        });
        var options = Options(seed.Port);
        var seedOptions = options.SeedNodes[0];
        options = new ValkeyClusterOptions
        {
            SeedNodes =
            [
                new ValkeyClientOptions
                {
                    Host = seedOptions.Host,
                    Port = seedOptions.Port,
                    ConnectTimeout = seedOptions.ConnectTimeout,
                    ResponseDrainTimeout = TimeSpan.FromMilliseconds(500),
                },
            ],
        };
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            options,
            TestContext.Current.CancellationToken
        );
        ValkeyArgument routingKey = key;

        var command = cluster.ExecuteWithDeadlineAsync(
            routingKey,
            new ValkeyCommand("GET", routingKey),
            TimeSpan.FromMilliseconds(500),
            TestContext.Current.CancellationToken
        );
        await firstCommandWritten.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        var deadlineFailure = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(async () => await command);
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, deadlineFailure.DeliveryStatus);
        await firstConnectionRetired.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        Assert.Equal("fresh", await cluster.GetStringAsync(key, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ConnectAsyncReadsShardsMapsEncodedForResp2()
    {
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(FakeValkeyServer.HelloResp2);
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(ShardsTopology(seedServer!.Port, mapsAsArrays: true));
            Assert.Equal(["GET", "resp2-cluster-key"], await session.ReadCommandAsync());
            await session.SendAsync("$5\r\nvalue\r\n");
        });
        await using var seed = seedServer;
        var options = new ValkeyClusterOptions
        {
            SeedNodes =
            [
                new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = seed.Port,
                    Protocol = ValkeyProtocol.Resp2,
                },
            ],
        };

        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            options,
            TestContext.Current.CancellationToken
        );

        Assert.Equal("value", await cluster.GetStringAsync("resp2-cluster-key", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExecuteAsyncRemembersMovedRedirects()
    {
        const string key = "moved-key";
        var slot = ValkeyClusterClient.GetHashSlot(key);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["GET", key], await session.ReadCommandAsync());
            await session.SendAsync("$5\r\nfirst\r\n");
            Assert.Equal(["GET", key], await session.ReadCommandAsync());
            await session.SendAsync("$6\r\nsecond\r\n");
        });
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await SendFallbackTopologyAsync(session, Topology(seedServer!.Port));
            Assert.Equal(["GET", key], await session.ReadCommandAsync());
            // An empty redirect endpoint means the current host with the supplied port.
            await session.SendAsync($"-MOVED {slot} :{target.Port}\r\n");
        });
        await using var seed = seedServer;

        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );

        Assert.Equal("first", await cluster.GetStringAsync(key, TestContext.Current.CancellationToken));
        Assert.Equal("second", await cluster.GetStringAsync(key, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExecuteAsyncUsesAskingWithoutChangingTheSlotOwner()
    {
        const string key = "migrating-key";
        var slot = ValkeyClusterClient.GetHashSlot(key);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["ASKING"], await session.ReadCommandAsync());
            Assert.Equal(["GET", key], await session.ReadCommandAsync());
            await session.SendAsync("+OK\r\n$8\r\nimported\r\n");
        });
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await SendFallbackTopologyAsync(session, Topology(seedServer!.Port));
            Assert.Equal(["GET", key], await session.ReadCommandAsync());
            await session.SendAsync($"-ASK {slot} 127.0.0.1:{target.Port}\r\n");
            Assert.Equal(["GET", key], await session.ReadCommandAsync());
            await session.SendAsync("$5\r\nowner\r\n");
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );

        Assert.Equal("imported", await cluster.GetStringAsync(key, TestContext.Current.CancellationToken));
        Assert.Equal("owner", await cluster.GetStringAsync(key, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ConnectAsyncRejectsAnIncompleteSlotMap()
    {
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await SendFallbackTopologyAsync(session, SlotRange(0, 100, "127.0.0.1", seedPort: 6379));
        });

        var failure = await Assert.ThrowsAsync<ValkeyClusterException>(async () =>
            await ValkeyClusterClient.ConnectAsync(Options(seed.Port), TestContext.Current.CancellationToken)
        );

        Assert.Contains("usable cluster topology", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsyncBoundsRedirectLoops()
    {
        const string key = "looping-key";
        var slot = ValkeyClusterClient.GetHashSlot(key);
        FakeValkeyServer? seedServer = null;
        FakeValkeyServer? targetServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await SendFallbackTopologyAsync(session, Topology(seedServer!.Port));
            await session.ReadCommandAsync();
            await session.SendAsync($"-MOVED {slot} 127.0.0.1:{targetServer!.Port}\r\n");
        });
        targetServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync($"-MOVED {slot} 127.0.0.1:{seedServer.Port}\r\n");
        });
        await using var seed = seedServer;
        await using var target = targetServer;
        var options = Options(seed.Port);
        options = new ValkeyClusterOptions { SeedNodes = options.SeedNodes, MaxRedirects = 1 };
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            options,
            TestContext.Current.CancellationToken
        );

        var failure = await Assert.ThrowsAsync<ValkeyClusterException>(async () =>
            await cluster.GetStringAsync(key, TestContext.Current.CancellationToken)
        );

        Assert.Contains("limit of 1", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsyncRejectsARedirectForAnotherSlot()
    {
        const string key = "redirected-key";
        var slot = ValkeyClusterClient.GetHashSlot(key);
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await SendFallbackTopologyAsync(session, Topology(seedServer!.Port));
            await session.ReadCommandAsync();
            await session.SendAsync($"-MOVED {(slot + 1) % 16_384} 127.0.0.1:{seedServer.Port}\r\n");
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );

        var failure = await Assert.ThrowsAsync<ValkeyClusterException>(async () =>
            await cluster.GetStringAsync(key, TestContext.Current.CancellationToken)
        );

        Assert.Contains("routing key hashes", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ExecuteAsyncBoundsRetainedNodeConnections()
    {
        await using var primary = FakeValkeyServer.Start(_ => Task.CompletedTask);
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await SendFallbackTopologyAsync(session, Topology(primary.Port));
        });
        var options = Options(seed.Port);
        options = new ValkeyClusterOptions { SeedNodes = options.SeedNodes, MaxNodeConnections = 1 };
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            options,
            TestContext.Current.CancellationToken
        );

        var failure = await Assert.ThrowsAsync<ValkeyClusterException>(async () =>
            await cluster.GetStringAsync("another-node", TestContext.Current.CancellationToken)
        );

        Assert.Contains("total limit of 1", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ConnectAsyncMapsAnAnnouncedEndpoint()
    {
        ValkeyClusterEndpoint? announced = null;
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["GET", "mapped-key"], await session.ReadCommandAsync());
            await session.SendAsync("$6\r\nmapped\r\n");
        });
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync("*1\r\n" + Shard(0, 16_383, "private.cluster", primary.Port));
        });
        var options = Options(seed.Port);
        options = new ValkeyClusterOptions
        {
            SeedNodes = options.SeedNodes,
            EndpointMapper = endpoint =>
            {
                announced = endpoint;
                return new ValkeyClusterEndpoint("127.0.0.1", endpoint.Port);
            },
        };

        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            options,
            TestContext.Current.CancellationToken
        );

        Assert.Equal("mapped", await cluster.GetStringAsync("mapped-key", TestContext.Current.CancellationToken));
        Assert.Equal(new ValkeyClusterEndpoint("private.cluster", primary.Port), announced);
    }

    [Fact]
    public async Task ExecutePipelineAsyncGroupsByNodeAndPreservesInputOrder()
    {
        var lowKey = FindKey(0, 8_191);
        var highKey = FindKey(8_192, 16_383);
        var secondLowKey = FindKey(0, 8_191, lowKey);
        FakeValkeyServer? seedServer = null;
        await using var highPrimary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["GET", highKey], await session.ReadCommandAsync());
            await session.SendAsync("$3\r\ntwo\r\n");
        });
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(
                "*2\r\n"
                    + Shard(0, 8_191, "127.0.0.1", seedServer!.Port)
                    + Shard(8_192, 16_383, "127.0.0.1", highPrimary.Port)
            );
            Assert.Equal(["GET", lowKey], await session.ReadCommandAsync());
            Assert.Equal(["GET", secondLowKey], await session.ReadCommandAsync());
            await session.SendAsync("$3\r\none\r\n$5\r\nthree\r\n");
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );

        var replies = await cluster.ExecutePipelineWithDeadlineAsync(
            [
                new ValkeyClusterCommand(lowKey, new ValkeyCommand("GET", lowKey)),
                new ValkeyClusterCommand(highKey, new ValkeyCommand("GET", highKey)),
                new ValkeyClusterCommand(secondLowKey, new ValkeyCommand("GET", secondLowKey)),
            ],
            TimeSpan.FromSeconds(30),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(["one", "two", "three"], replies.Select(static reply => reply.AsString()));
    }

    [Fact]
    public async Task ConnectionsPerNodeAllowsIndependentCommandsToOverlap()
    {
        var arrivals = new[]
        {
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        FakeValkeyServer? server = null;
        server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                if (index == 0)
                {
                    Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
                    await session.SendAsync(ShardsTopology(server!.Port));
                }

                Assert.Equal("GET", (await session.ReadCommandAsync())[0]);
                arrivals[index].SetResult();
                await Task.WhenAll(arrivals.Select(static arrival => arrival.Task));
                await session.SendAsync(index == 0 ? "$4\r\nzero\r\n" : "$3\r\none\r\n");
            }
        );
        await using var clusterServer = server;
        var options = Options(server.Port);
        options = new ValkeyClusterOptions
        {
            SeedNodes = options.SeedNodes,
            ConnectionsPerNode = 2,
            MaxNodeConnections = 2,
        };
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            options,
            TestContext.Current.CancellationToken
        );

        var replies = await Task.WhenAll(
            cluster.GetStringAsync("parallel-a", TestContext.Current.CancellationToken),
            cluster.GetStringAsync("parallel-b", TestContext.Current.CancellationToken)
        );

        Assert.Equal(["one", "zero"], replies.Order(StringComparer.Ordinal));
        await server.Session;
    }

    [Fact]
    public async Task CancellationRemovesAnInvalidatedNodeConnectionFromThePool()
    {
        const string key = "cancelled-cluster-key";
        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        FakeValkeyServer? server = null;
        server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                if (index == 0)
                {
                    Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
                    await session.SendAsync(ShardsTopology(server!.Port));
                    Assert.Equal(["GET", key], await session.ReadCommandAsync());
                    written.SetResult();
                    await session.ReadCommandAsync();
                    return;
                }

                Assert.Equal(["GET", key], await session.ReadCommandAsync());
                await session.SendAsync("$9\r\nrecovered\r\n");
            }
        );
        await using var clusterServer = server;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(server.Port),
            TestContext.Current.CancellationToken
        );
        using var cancellation = new CancellationTokenSource();

        var cancelled = cluster.GetStringAsync(key, cancellation.Token);
        await written.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await cancelled);
        Assert.Equal("recovered", await cluster.GetStringAsync(key, TestContext.Current.CancellationToken));
        await server.Session;
    }

    [Fact]
    public async Task ExecutePipelineAsyncReturnsErrorsAndFollowsMovedAfterDraining()
    {
        const string errorKey = "pipeline-error";
        const string movedKey = "pipeline-moved";
        var slot = ValkeyClusterClient.GetHashSlot(movedKey);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["GET", movedKey], await session.ReadCommandAsync());
            await session.SendAsync("$5\r\nfirst\r\n");
            Assert.Equal(["GET", movedKey], await session.ReadCommandAsync());
            await session.SendAsync("$6\r\nsecond\r\n");
        });
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(ShardsTopology(seedServer!.Port));
            Assert.Equal(["GET", errorKey], await session.ReadCommandAsync());
            Assert.Equal(["GET", movedKey], await session.ReadCommandAsync());
            await session.SendAsync($"-WRONGTYPE expected\r\n-MOVED {slot} 127.0.0.1:{target.Port}\r\n");
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );

        var replies = await cluster.ExecutePipelineAsync(
            [
                new ValkeyClusterCommand(errorKey, new ValkeyCommand("GET", errorKey)),
                new ValkeyClusterCommand(movedKey, new ValkeyCommand("GET", movedKey)),
            ],
            TestContext.Current.CancellationToken
        );

        Assert.Equal("WRONGTYPE", replies[0].ToServerException().ErrorCode);
        Assert.Equal("first", replies[1].AsString());
        Assert.Equal("second", await cluster.GetStringAsync(movedKey, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ConnectAsyncValidatesClusterOptionsBeforeNetworking()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await ValkeyClusterClient.ConnectAsync(
                new ValkeyClusterOptions { SeedNodes = [] },
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await ValkeyClusterClient.ConnectAsync(
                new ValkeyClusterOptions { MaxRedirects = 17 },
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await ValkeyClusterClient.ConnectAsync(
                new ValkeyClusterOptions { ConnectionsPerNode = 17 },
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await ValkeyClusterClient.ConnectAsync(
                new ValkeyClusterOptions { ConnectionsPerNode = 2, MaxNodeConnections = 1 },
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await ValkeyClusterClient.ConnectAsync(
                new ValkeyClusterOptions { MaxNodeConnections = 0 },
                TestContext.Current.CancellationToken
            )
        );
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task FailedAskingSurfacesTheFollowingCommandResult(bool pipeline, bool commandError)
    {
        const string key = "{asking}:write";
        var slot = ValkeyClusterClient.GetHashSlot(key);
        var command = new ValkeyCommand("INCR", key);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["ASKING"], await session.ReadCommandAsync());
            Assert.Equal(["INCR", key], await session.ReadCommandAsync());
            // A NOSCRIPT/redirect-shaped error must not authorize another invocation after ASKING failed.
            await session.SendAsync(
                "-ERR ASKING refused\r\n" + (commandError ? "-NOSCRIPT No matching script.\r\n" : ":1\r\n")
            );
        });
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await SendFallbackTopologyAsync(session, Topology(seedServer!.Port));
            Assert.Equal(["INCR", key], await session.ReadCommandAsync());
            await session.SendAsync($"-ASK {slot} 127.0.0.1:{target.Port}\r\n");
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );
        async Task<RespValue> ExecuteAsync() =>
            pipeline
                ? (
                    await cluster.ExecutePipelineAsync(
                        [new ValkeyClusterCommand(key, command)],
                        TestContext.Current.CancellationToken
                    )
                )[0]
                : await cluster.ExecuteAsync(key, command, TestContext.Current.CancellationToken);
        if (commandError)
        {
            var failure = await Assert.ThrowsAsync<ValkeyAskingException>(ExecuteAsync);
            Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
            Assert.Equal("NOSCRIPT", Assert.IsType<ValkeyServerException>(failure.InnerException).ErrorCode);
        }
        else
        {
            Assert.Equal(1, (await ExecuteAsync()).AsInt64());
        }
        await target.Session;
        Assert.Single(target.ReceivedCommands, command => command[0] == "INCR");
    }

    [Theory]
    [InlineData(false, "MOVED")]
    [InlineData(false, "NOSCRIPT")]
    [InlineData(true, "MOVED")]
    [InlineData(true, "NOSCRIPT")]
    public async Task FailedAskingPreservesTheSocketAndItsPendingNeighbor(bool pipeline, string errorCode)
    {
        const string key = "{asking}:write";
        var slot = ValkeyClusterClient.GetHashSlot(key);
        var pairWritten = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseNeighbor = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        FakeValkeyServer? targetServer = null;
        targetServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["INCR", key], await session.ReadCommandAsync());
            await session.SendAsync($"-ASK {slot} 127.0.0.1:{targetServer!.Port}\r\n");
            Assert.Equal(["ASKING"], await session.ReadCommandAsync());
            Assert.Equal(["INCR", key], await session.ReadCommandAsync());
            pairWritten.TrySetResult();
            Assert.Equal(["GET", "neighbor"], await session.ReadCommandAsync());
            var commandError =
                errorCode == "MOVED"
                    ? $"-MOVED {slot} 127.0.0.1:{targetServer.Port}\r\n"
                    : "-NOSCRIPT No matching script.\r\n";
            await session.SendAsync("-ERR ASKING refused\r\n" + commandError);
            await releaseNeighbor.Task;
            await session.SendAsync("+neighbor-reply\r\n");
            Assert.Equal(["GET", "later"], await session.ReadCommandAsync());
            await session.SendAsync("+same-socket\r\n");
        });
        await using var target = targetServer;
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(ShardsTopology(target.Port));
            await session.ReadCommandAsync();
        });
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );
        async Task ExecuteFailureAsync()
        {
            var command = new ValkeyCommand("INCR", key);
            if (pipeline)
            {
                await cluster.ExecutePipelineAsync(
                    [new ValkeyClusterCommand(key, command)],
                    TestContext.Current.CancellationToken
                );
            }
            else
            {
                await cluster.ExecuteAsync(key, command, TestContext.Current.CancellationToken);
            }
        }
        var failing = Record.ExceptionAsync(ExecuteFailureAsync).AsTask();
        try
        {
            await pairWritten.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            var neighbor = cluster.GetStringAsync("neighbor", TestContext.Current.CancellationToken);
            var failure = await failing.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            Assert.NotNull(failure);
            Assert.IsType<ValkeyAskingException>(failure);
            Assert.Equal(
                ValkeyCommandDeliveryStatus.MayHaveBeenSent,
                Assert.IsAssignableFrom<IValkeyCommandFailure>(failure).DeliveryStatus
            );
            Assert.Equal(errorCode, Assert.IsType<ValkeyServerException>(failure.InnerException).ErrorCode);
            Assert.False(neighbor.IsCompleted);
            releaseNeighbor.TrySetResult();
            Assert.Equal(
                "neighbor-reply",
                await neighbor.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken)
            );
            Assert.Equal("same-socket", await cluster.GetStringAsync("later", TestContext.Current.CancellationToken));
            await target.Session;
            Assert.Single(target.ReceivedCommands, command => command[0] == "HELLO");
            Assert.Equal(2, target.ReceivedCommands.Count(command => command[0] == "INCR"));
            Assert.Single(target.ReceivedCommands, command => command[0] == "ASKING");
        }
        finally
        {
            releaseNeighbor.TrySetResult();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FounderCancellationOrDeadlineDoesNotCancelSharedNodeConnect(bool deadline)
    {
        var connecting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandshake = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            Assert.Equal("HELLO", (await session.ReadCommandAsync())[0]);
            connecting.TrySetResult();
            await releaseHandshake.Task;
            await session.SendAsync(FakeValkeyServer.HelloResp3);
            Assert.Equal(["GET", "joiner"], await session.ReadCommandAsync());
            await session.SendAsync("+joined\r\n");
        });
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(ShardsTopology(target.Port));
            await session.ReadCommandAsync();
        });
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()], MaxNodeConnections = 2 },
            TestContext.Current.CancellationToken
        );
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var founder = deadline
            ? cluster.ExecuteWithDeadlineAsync(
                "founder",
                new ValkeyCommand("GET", "founder"),
                TimeSpan.FromMilliseconds(500),
                cancellation.Token
            )
            : cluster.ExecuteAsync("founder", new ValkeyCommand("GET", "founder"), cancellation.Token);
        try
        {
            await connecting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            var joiner = cluster.GetStringAsync("joiner", TestContext.Current.CancellationToken);
            Assert.False(joiner.IsCompleted);
            if (deadline)
            {
                var failure = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() => founder);
                Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, failure.DeliveryStatus);
            }
            else
            {
                await cancellation.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => founder);
            }
            releaseHandshake.TrySetResult();
            Assert.Equal(
                "joined",
                await joiner.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken)
            );
            await target.Session;
            Assert.Single(target.ReceivedCommands, command => command[0] == "HELLO");
            Assert.DoesNotContain(target.ReceivedCommands, command => command[0] == "GET" && command[1] == "founder");
        }
        finally
        {
            releaseHandshake.TrySetResult();
        }
    }

    [Fact]
    public async Task SlowNodeConnectDoesNotBlockOtherRoutesOrTopologyRefresh()
    {
        var lowKey = FindKey(0, 8191);
        var highKey = FindKey(8192, 16383);
        var connecting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var slow = FakeValkeyServer.Start(async session =>
        {
            Assert.Equal("HELLO", (await session.ReadCommandAsync())[0]);
            connecting.TrySetResult();
            await release.Task;
            await session.SendAsync(FakeValkeyServer.HelloResp3);
            Assert.Equal(["GET", lowKey], await session.ReadCommandAsync());
            await session.SendAsync("+slow\r\n");
        });
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            var topology =
                "*2\r\n" + Shard(0, 8191, "127.0.0.1", slow.Port) + Shard(8192, 16383, "127.0.0.1", seedServer!.Port);
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(topology);
            Assert.Equal(["GET", highKey], await session.ReadCommandAsync());
            await session.SendAsync("+fast\r\n");
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(topology);
            await session.ReadCommandAsync();
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );
        var pending = cluster.GetStringAsync(lowKey, TestContext.Current.CancellationToken);
        try
        {
            await connecting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            Assert.Equal(
                "fast",
                await cluster
                    .GetStringAsync(highKey, TestContext.Current.CancellationToken)
                    .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
            );
            await cluster
                .RefreshTopologyAsync(TestContext.Current.CancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
        finally
        {
            release.TrySetResult();
        }
        Assert.Equal("slow", await pending);
    }

    [Fact]
    public async Task InFlightNodeReservationsCountTowardsTheCapAndAreReleasedOnFailure()
    {
        var lowKey = FindKey(0, 8191);
        var highKey = FindKey(8192, 16383);
        var connecting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var failing = FakeValkeyServer.Start(async session =>
        {
            await session.ReadCommandAsync();
            connecting.TrySetResult();
            await release.Task;
            session.Close();
        });
        await using var healthy = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["GET", highKey], await session.ReadCommandAsync());
            await session.SendAsync("+healthy\r\n");
        });
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(
                "*2\r\n" + Shard(0, 8191, "127.0.0.1", failing.Port) + Shard(8192, 16383, "127.0.0.1", healthy.Port)
            );
            await session.ReadCommandAsync();
        });
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()], MaxNodeConnections = 2 },
            TestContext.Current.CancellationToken
        );
        var pending = cluster.GetStringAsync(lowKey, TestContext.Current.CancellationToken);
        try
        {
            await connecting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            var failure = await Assert.ThrowsAsync<ValkeyClusterException>(() =>
                cluster.GetStringAsync(highKey, TestContext.Current.CancellationToken)
            );
            Assert.Contains("total limit of 2", failure.Message, StringComparison.Ordinal);
        }
        finally
        {
            release.TrySetResult();
        }
        await Assert.ThrowsAnyAsync<IOException>(() => pending);
        Assert.Equal("healthy", await cluster.GetStringAsync(highKey, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ConcurrentNodeAcquisitionSharesReservationsAndDisposeCancelsTheHandshake()
    {
        var connecting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await session.ReadCommandAsync();
            connecting.TrySetResult();
            await session.ReadCommandAsync();
        });
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(ShardsTopology(target.Port));
            await session.ReadCommandAsync();
        });
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()], MaxNodeConnections = 2 },
            TestContext.Current.CancellationToken
        );
        var first = cluster.GetStringAsync("key", TestContext.Current.CancellationToken);
        await connecting.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        var second = cluster.GetStringAsync("key", TestContext.Current.CancellationToken);
        Assert.False(second.IsCompleted);
        await cluster.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);
        await target.Session;
        Assert.Single(target.ReceivedCommands);
    }

    [Fact]
    public async Task PartialNodePoolFailureDisposesOpenedSocketsAndReleasesEveryReservation()
    {
        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var target = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                if (index == 0)
                {
                    await session.ExpectHandshakeAsync();
                    try
                    {
                        await session.ReadCommandAsync();
                    }
                    catch (IOException)
                    {
                        closed.TrySetResult();
                    }
                }
                else
                {
                    await session.ReadCommandAsync();
                    session.Close();
                }
            }
        );
        var lowKey = FindKey(0, 8191);
        var highKey = FindKey(8192, 16383);
        await using var healthy = FakeValkeyServer.StartMany(
            2,
            async (_, session) =>
            {
                await session.ExpectHandshakeAsync();
                Assert.Equal(["GET", highKey], await session.ReadCommandAsync());
                await session.SendAsync("+healthy\r\n");
            }
        );
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(
                "*2\r\n" + Shard(0, 8191, "127.0.0.1", target.Port) + Shard(8192, 16383, "127.0.0.1", healthy.Port)
            );
            await session.ReadCommandAsync();
        });
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            new ValkeyClusterOptions
            {
                SeedNodes = [seed.ClientOptions()],
                MaxNodeConnections = 3,
                ConnectionsPerNode = 2,
            },
            TestContext.Current.CancellationToken
        );
        await Assert.ThrowsAnyAsync<IOException>(() =>
            cluster.GetStringAsync(lowKey, TestContext.Current.CancellationToken)
        );
        await closed.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        Assert.Equal("healthy", await cluster.GetStringAsync(highKey, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("AUTH")]
    [InlineData("SELECT")]
    [InlineData("READONLY")]
    [InlineData("READWRITE")]
    [InlineData("ASKING")]
    public async Task ClusterRejectsSessionChangesBeforeAnyPipelineGroupIsWritten(string name)
    {
        FakeValkeyServer? seedServer = null;
        seedServer = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(ShardsTopology(seedServer!.Port));
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
        });
        await using var seed = seedServer;
        await using var cluster = await ValkeyClusterClient.ConnectAsync(
            Options(seed.Port),
            TestContext.Current.CancellationToken
        );
        await Assert.ThrowsAsync<ValkeyUnsupportedCommandException>(() =>
            cluster.ExecuteAsync("key", new ValkeyCommand(name), TestContext.Current.CancellationToken)
        );
        await Assert.ThrowsAsync<ValkeyUnsupportedCommandException>(() =>
            cluster.ExecutePipelineAsync(
                [
                    new ValkeyClusterCommand("key", new ValkeyCommand("INCR", "key")),
                    new ValkeyClusterCommand("key", new ValkeyCommand(name)),
                ],
                TestContext.Current.CancellationToken
            )
        );
        Assert.Equal("PONG", await cluster.PingAsync(TestContext.Current.CancellationToken));
        await seed.Session;
        Assert.DoesNotContain(seed.ReceivedCommands, command => command[0] is "INCR" || command[0] == name);
    }

    private static ValkeyClusterOptions Options(int seedPort) =>
        new()
        {
            SeedNodes =
            [
                new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = seedPort,
                    ConnectTimeout = TimeSpan.FromSeconds(30),
                },
            ],
        };

    private static string Topology(int seedPort) => SlotRange(0, 16_383, "127.0.0.1", seedPort);

    private static string ShardsTopology(int primaryPort, bool mapsAsArrays = false) =>
        "*1\r\n" + Shard(0, 16_383, "127.0.0.1", primaryPort, mapsAsArrays);

    private static string Shard(int start, int end, string host, int port, bool mapsAsArrays = false) =>
        (mapsAsArrays ? "*4\r\n" : "%2\r\n")
        + "+slots\r\n*2\r\n:"
        + start.ToString(CultureInfo.InvariantCulture)
        + "\r\n:"
        + end.ToString(CultureInfo.InvariantCulture)
        + "\r\n+nodes\r\n*1\r\n"
        + (mapsAsArrays ? "*8\r\n" : "%4\r\n")
        + "+role\r\n+master\r\n+endpoint\r\n+"
        + host
        + "\r\n+port\r\n:"
        + port.ToString(CultureInfo.InvariantCulture)
        + "\r\n+health\r\n+online\r\n";

    private static string FindKey(int minimumSlot, int maximumSlot, string? except = null)
    {
        for (var index = 0; ; index++)
        {
            var key = "slot-key-" + index.ToString(CultureInfo.InvariantCulture);
            var slot = ValkeyClusterClient.GetHashSlot(key);
            if (slot >= minimumSlot && slot <= maximumSlot && key != except)
                return key;
        }
    }

    private static async Task SendFallbackTopologyAsync(FakeValkeySession session, string topology)
    {
        Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
        await session.SendAsync("-ERR unknown command 'SHARDS'\r\n");
        Assert.Equal(["CLUSTER", "SLOTS"], await session.ReadCommandAsync());
        await session.SendAsync(topology);
    }

    private static string SlotRange(int start, int end, string host, int seedPort)
    {
        return "*1\r\n*3\r\n:"
            + start.ToString(CultureInfo.InvariantCulture)
            + "\r\n:"
            + end.ToString(CultureInfo.InvariantCulture)
            + "\r\n*3\r\n$"
            + host.Length.ToString(CultureInfo.InvariantCulture)
            + "\r\n"
            + host
            + "\r\n:"
            + seedPort.ToString(CultureInfo.InvariantCulture)
            + "\r\n$6\r\nnode-1\r\n";
    }
}
