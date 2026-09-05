using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests.Cluster;

public sealed partial class ValkeyClusterSubscriberTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task RelocationPreservesTlsAclClientNameAndResponseBounds(ValkeyProtocol protocol)
    {
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        var password = Guid.NewGuid().ToString();
        string[] expectedHello =
        [
            "HELLO",
            protocol == ValkeyProtocol.Resp2 ? "2" : "3",
            "AUTH",
            "test-user",
            password,
            "SETNAME",
            "recovery-tls",
        ];
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var oversized = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var target = FakeValkeyServer.Start(
            async session =>
            {
                Assert.Equal(expectedHello, await session.ExpectHandshakeAsync(Hello(protocol)));
                await session.ReadCommandAsync();
                await session.SendRawAsync([.. Ack(protocol, "x"u8.ToArray()), .. Message(protocol, "x"u8.ToArray())]);
                await oversized.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                await session.SendAsync("$1025\r\n");
                await session.ReadCommandAsync();
            },
            certificate
        );
        await using var origin = FakeValkeyServer.Start(
            async session =>
            {
                Assert.Equal(expectedHello, await session.ExpectHandshakeAsync(Hello(protocol)));
                await session.ReadCommandAsync();
                await session.SendRawAsync(Ack(protocol, "x"u8.ToArray()));
                await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            },
            certificate
        );
        await using var seed = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                Assert.Equal(expectedHello, await session.ExpectHandshakeAsync(Hello(protocol)));
                await session.ReadCommandAsync();
                await session.SendAsync(Topology(index == 0 ? origin.Port : target.Port));
                await session.ReadCommandAsync();
            },
            certificate
        );
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions
                {
                    SeedNodes =
                    [
                        new ValkeyClientOptions
                        {
                            Host = "127.0.0.1",
                            Port = seed.Port,
                            Protocol = protocol,
                            UseTls = true,
                            Username = "test-user",
                            Password = password,
                            ClientName = "recovery-tls",
                            MaxResponseBytes = 1024,
                            MaxResponseElements = 128,
                            MaxNestingDepth = 8,
                            CertificateValidationCallback = (_, presented, _, _) =>
                                presented?.GetCertHashString() == certificate.Thumbprint,
                        },
                    ],
                },
                EnableTopologyRecovery = true,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
            },
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        oversized.SetResult();
        await handle.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.IsType<ValkeyProtocolException>(handle.Failure);
        Assert.Equal(1, handle.SuccessfulRelocations);
        Assert.Equal(1, handle.ReconnectAttempts);
        await subscriber.DisposeAsync();
        await Task.WhenAll(origin.Session, target.Session, seed.Session).WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Fact]
    public async Task TotalRecoveryBudgetBoundsStalledDiscovery()
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var origin = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
            await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        });
        await using var seed = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                if (index == 0)
                {
                    await session.SendAsync(Topology(origin.Port));
                }
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = RecoveryOptions(seed.Port).Cluster,
                EnableTopologyRecovery = true,
                RecoveryTimeout = TimeSpan.FromMilliseconds(200),
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
            },
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        await handle.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.IsType<TimeoutException>(handle.Failure);
        Assert.False(handle.IsReconnecting);
        Assert.Equal(0, handle.SuccessfulReconnects);
        await subscriber.DisposeAsync();
        await seed.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, false)]
    [InlineData(ValkeyProtocol.Resp3, false)]
    [InlineData(ValkeyProtocol.Resp2, true)]
    [InlineData(ValkeyProtocol.Resp3, true)]
    public async Task InvalidShardRemovalStaysTerminalWithTopologyRecoveryEnabled(
        ValkeyProtocol protocol,
        bool wrongChannel
    )
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var origin = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(protocol, "x"u8.ToArray()));
            await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await session.SendRawAsync(
                Ack(protocol, wrongChannel ? "y"u8.ToArray() : "x"u8.ToArray(), "sunsubscribe", wrongChannel ? 0 : 1)
            );
            await session.ReadCommandAsync();
        });
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendAsync(Topology(origin.Port));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            RecoveryOptions(seed.Port, protocol),
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        await handle.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.IsType<ValkeyProtocolException>(handle.Failure);
        Assert.Equal(0, handle.ReconnectAttempts);
    }

    [Theory]
    [InlineData(false, 2)]
    [InlineData(true, 2)]
    [InlineData(false, 1)]
    public async Task RecoveryDiscoveryFallsBackToKnownPrimaryWithinEndpointBound(bool stalled, int limit)
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fallbackReads = 0;
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync([
                .. Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()),
                .. Message(ValkeyProtocol.Resp3, "x"u8.ToArray()),
            ]);
            await session.ReadCommandAsync();
        });
        await using var origin = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                var command = await session.ReadCommandAsync();
                if (index == 0)
                {
                    Assert.Equal(["SSUBSCRIBE", "x"], command);
                    await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
                    await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    return;
                }
                Assert.Equal(["CLUSTER", "SHARDS"], command);
                Interlocked.Increment(ref fallbackReads);
                await session.SendAsync(Topology(target.Port));
                await session.ReadCommandAsync();
            }
        );
        await using var seed = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                if (index == 0)
                {
                    await session.SendAsync(Topology(origin.Port));
                    await session.ReadCommandAsync();
                }
                else if (stalled)
                {
                    await session.ReadCommandAsync();
                }
            }
        );
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions
                {
                    SeedNodes =
                    [
                        new ValkeyClientOptions
                        {
                            Host = "127.0.0.1",
                            Port = seed.Port,
                            ConnectTimeout = TimeSpan.FromMilliseconds(500),
                        },
                    ],
                    MaxNodeConnections = 1,
                },
                EnableTopologyRecovery = true,
                MaxTopologyRefreshEndpoints = limit,
                MaxReconnectAttempts = 1,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
            },
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        if (limit == 1)
        {
            await handle.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            Assert.IsType<ValkeyConnectionException>(handle.Failure);
            Assert.Equal(0, fallbackReads);
        }
        else
        {
            await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
            Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
            Assert.Equal(1, fallbackReads);
            Assert.Equal(1, handle.SuccessfulRelocations);
        }
        await handle.DisposeAsync();
        await subscriber.DisposeAsync();
        await seed.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        if (limit > 1)
        {
            await Task.WhenAll(origin.Session, target.Session).WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        }
    }

    [Theory]
    [InlineData("handshake")]
    [InlineData("permission")]
    [InlineData("bounds")]
    public async Task RecoveryDiscoveryDoesNotRetryAuthenticationPermissionOrParserFailures(string failure)
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var origin = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
            await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        });
        await using var seed = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                if (index == 1 && failure == "handshake")
                {
                    await session.ReadCommandAsync();
                    await session.SendAsync("-WRONGPASS private-payload\r\n");
                }
                else
                {
                    await session.ExpectHandshakeAsync();
                    await session.ReadCommandAsync();
                    await session.SendAsync(
                        index == 0 ? Topology(origin.Port)
                        : failure == "bounds" ? "*2147483647\r\n"
                        : "-NOPERM private-payload\r\n"
                    );
                }
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(RecoveryOptions(seed.Port), TestToken);
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        await handle.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        if (failure == "bounds")
        {
            Assert.IsType<ValkeyProtocolException>(handle.Failure);
        }
        else
        {
            Assert.IsType<ValkeyServerException>(handle.Failure);
        }
        Assert.DoesNotContain("private-payload", handle.Failure!.ToString(), StringComparison.Ordinal);
        Assert.Equal(1, handle.ReconnectAttempts);
        Assert.Equal(0, handle.SuccessfulReconnects);
        await subscriber.DisposeAsync();
        await seed.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(failure == "handshake" ? 3 : 4, seed.ReceivedCommands.Count);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task RepeatedTopologyRecoveryKeepsOriginalBoundedQueueAndClosesEverySocket(ValkeyProtocol protocol)
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task ServeAsync(int cycle, FakeValkeySession session)
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync([.. Ack(protocol, "x"u8.ToArray()), .. Message(protocol, "x"u8.ToArray())]);
            if (cycle == 0)
            {
                await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            }
            if (cycle < 3)
            {
                await session.SendRawAsync(Ack(protocol, "x"u8.ToArray(), "sunsubscribe", 0));
                await session.ReadCommandAsync();
            }
            else
            {
                Assert.Equal(["SUNSUBSCRIBE", "x"], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(protocol, "x"u8.ToArray(), "sunsubscribe", 0));
                await session.ReadCommandAsync();
            }
        }
        await using var first = FakeValkeyServer.StartMany(2, (index, session) => ServeAsync(index * 2, session));
        await using var second = FakeValkeyServer.StartMany(2, (index, session) => ServeAsync(index * 2 + 1, session));
        await using var seed = FakeValkeyServer.StartMany(
            4,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                await session.ReadCommandAsync();
                await session.SendAsync(Topology(index % 2 == 0 ? first.Port : second.Port));
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = RecoveryOptions(seed.Port, protocol).Cluster,
                EnableTopologyRecovery = true,
                QueueCapacity = 1,
                MaxSubscriptions = 1,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
            },
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        while (handle.SuccessfulRelocations < 3 || handle.DroppedMessages < 3)
        {
            await Task.Delay(1, deadline.Token);
        }
        await handle.UnsubscribeAsync(TestToken);
        Assert.Equal(3, handle.ConnectionLosses);
        Assert.Equal(3, handle.ReconnectAttempts);
        Assert.Equal(3, handle.SuccessfulReconnects);
        Assert.Equal(3, handle.DroppedMessages);
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.False(await messages.MoveNextAsync());
        await subscriber.DisposeAsync();
        await Task.WhenAll(first.Session, second.Session, seed.Session).WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(8, seed.ReceivedCommands.Count);
        Assert.Equal(4, first.ReceivedCommands.Count);
        Assert.Equal(5, second.ReceivedCommands.Count);
    }
}
