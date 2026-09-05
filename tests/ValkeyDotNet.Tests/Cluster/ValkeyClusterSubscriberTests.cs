using System.Globalization;
using System.Text;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests.Cluster;

public sealed class ValkeyClusterSubscriberTests
{
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task RoutesBinaryShardChannelToMappedPrimaryAndKeepsDiscoveryOutOfSubscriptionMode(
        ValkeyProtocol protocol
    )
    {
        byte[] channel = [123, 120, 125, 0, 255];
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            var hello = await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(((int)protocol).ToString(CultureInfo.InvariantCulture), hello[1]);
            var command = await session.ReadBinaryCommandAsync();
            Assert.Equal("SSUBSCRIBE"u8.ToArray(), command[0]);
            Assert.Equal(channel, command[1]);
            await session.SendRawAsync(Ack(protocol, channel));
            await session.SendRawAsync(Message(protocol, channel));
            Assert.Equal("SUNSUBSCRIBE", (await session.ReadCommandAsync())[0]);
            await session.SendRawAsync(Ack(protocol, channel, "sunsubscribe", 0));
            await session.ReadCommandAsync();
        });
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(Topology(primary.Port, "announced.invalid"));
            await session.ReadCommandAsync();
        });
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
                        },
                    ],
                    EndpointMapper = endpoint => new ValkeyClusterEndpoint("127.0.0.1", endpoint.Port),
                },
                MaxSubscriptions = 1,
            },
            TestToken
        );
        var handle = await subscriber.SubscribeAsync(channel, TestToken);
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(channel, messages.Current.Channel.ToArray());
        Assert.True(messages.Current.IsSharded);
        await Assert.ThrowsAsync<ValkeyCapacityException>(() => subscriber.SubscribeAsync("other", TestToken));
        await handle.UnsubscribeAsync(TestToken);
        await handle.DisposeAsync();
        Assert.Equal(0, subscriber.SubscriptionCount);
        Assert.False(await messages.MoveNextAsync());
    }

    [Fact]
    public async Task InitialMovedRefreshesTopologyWithoutFollowingErrorEndpointText()
    {
        await using var moved = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendAsync("-MOVED 16287 untrusted.invalid:6379 private-payload\r\n");
            await session.ReadCommandAsync();
        });
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
            await session.ReadCommandAsync();
        });
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(Topology(moved.Port));
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(Topology(primary.Port));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(Options(seed.Port), TestToken);
        var handle = await subscriber.SubscribeAsync("x", TestToken);
        Assert.True(handle.IsConnected);
        await handle.DisposeAsync();
        Assert.Equal(0, subscriber.SubscriptionCount);
    }

    [Theory]
    [InlineData("ASK")]
    [InlineData("MOVED")]
    public async Task RedirectFailureIsBoundedAndSanitized(string kind)
    {
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("-" + kind + " 16287 untrusted.invalid:6379 private-payload\r\n");
            await session.ReadCommandAsync();
        });
        await using var seed = StartSeed(primary.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()], MaxRedirects = 0 },
            },
            TestToken
        );
        var error = await Assert.ThrowsAsync<ValkeyClusterException>(() => subscriber.SubscribeAsync("x", TestToken));
        Assert.DoesNotContain("private-payload", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("untrusted.invalid", error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, subscriber.SubscriptionCount);
    }

    [Fact]
    public async Task DisposalDuringAcknowledgementSettlesTheOperationAndReleasesSocket()
    {
        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            written.SetResult();
            await session.ReadCommandAsync();
        });
        await using var seed = StartSeed(primary.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(Options(seed.Port), TestToken);
        var subscribing = subscriber.SubscribeAsync("x", TestToken);
        await written.Task.WaitAsync(TestToken);
        await Task.WhenAll(subscriber.DisposeAsync().AsTask(), subscriber.DisposeAsync().AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => subscribing);
        Assert.Equal(0, subscriber.SubscriptionCount);
        await primary.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Fact]
    public async Task AcknowledgementDeadlineIsTypedAndDoesNotLeaveAnUnboundedReservation()
    {
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.ReadCommandAsync();
        });
        await using var seed = StartSeed(primary.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()] },
                OperationTimeout = TimeSpan.FromMilliseconds(250),
            },
            TestToken
        );
        var error = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() =>
            subscriber.SubscribeAsync("x", TestToken)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        Assert.Equal(0, subscriber.SubscriptionCount);
    }

    [Fact]
    public void OptionsRejectUnboundedOrInvalidSettings()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyClusterSubscriberOptions { MaxSubscriptions = 0 }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyClusterSubscriberOptions { MaxConcurrentOperations = 0 }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyClusterSubscriberOptions { QueueCapacity = 0 }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyClusterSubscriberOptions { OperationTimeout = TimeSpan.Zero }.Validate()
        );
        Assert.Throws<ArgumentException>(() =>
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions { SeedNodes = [new ValkeyClientOptions { Database = 1 }] },
            }.Validate()
        );
    }

    [Fact]
    public async Task BoundedAdmissionRejectsExcessWorkAndCancelsOnlyAWaitingOperation()
    {
        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            written.SetResult();
            await release.Task.WaitAsync(TestToken);
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
            await session.ReadCommandAsync();
        });
        await using var seed = StartSeed(primary.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()] },
                MaxConcurrentOperations = 2,
            },
            TestToken
        );
        try
        {
            var first = subscriber.SubscribeAsync("x", TestToken);
            await written.Task.WaitAsync(TestToken);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
            var waiting = subscriber.SubscribeAsync("waiting", cancellation.Token);
            await Assert.ThrowsAsync<ValkeyCapacityException>(() => subscriber.SubscribeAsync("excess", TestToken));
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
            release.SetResult();
            var handle = await first;
            Assert.True(handle.IsConnected);
            await handle.DisposeAsync();
            Assert.Equal(0, subscriber.SubscriptionCount);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task DuplicateClusterHandlesOwnIndependentConnectionsAndReleaseCapacity()
    {
        var firstClosed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var primary = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
                if (index == 0)
                {
                    try
                    {
                        await session.ReadCommandAsync();
                    }
                    finally
                    {
                        firstClosed.TrySetResult();
                    }
                    return;
                }
                await firstClosed.Task.WaitAsync(TestToken);
                await session.SendRawAsync(Message(ValkeyProtocol.Resp3, "x"u8.ToArray()));
                await session.ReadCommandAsync();
            }
        );
        await using var seed = StartSeed(primary.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(Options(seed.Port), TestToken);
        var first = await subscriber.SubscribeAsync("x", TestToken);
        var second = await subscriber.SubscribeAsync("x", TestToken);
        Assert.Equal(2, subscriber.SubscriptionCount);
        await first.DisposeAsync();
        await using var messages = second.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal("v"u8.ToArray(), messages.Current.Payload.ToArray());
        Assert.Equal(1, subscriber.SubscriptionCount);
        await subscriber.DisposeAsync();
        await second.DisposeAsync();
        Assert.False(await messages.MoveNextAsync());
    }

    [Fact]
    public async Task FailedUnsubscribeClosesOnlyItsDedicatedHandleAndReportsDeadline()
    {
        await using var primary = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
            Assert.Equal(["SUNSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.ReadCommandAsync();
        });
        await using var seed = StartSeed(primary.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()] },
                OperationTimeout = TimeSpan.FromMilliseconds(250),
            },
            TestToken
        );
        var handle = await subscriber.SubscribeAsync("x", TestToken);
        var error = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() => handle.UnsubscribeAsync(TestToken));
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        Assert.Equal(0, subscriber.SubscriptionCount);
        Assert.False(handle.IsConnected);
        await handle.DisposeAsync();
    }

    [Fact]
    public async Task LocalHandleDisposalBypassesFullLifecycleAdmission()
    {
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var primary = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                if (index == 1)
                {
                    blocked.SetResult();
                    await release.Task.WaitAsync(TestToken);
                }
                await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
                await session.ReadCommandAsync();
            }
        );
        await using var seed = StartSeed(primary.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()] },
                MaxConcurrentOperations = 1,
            },
            TestToken
        );
        try
        {
            var first = await subscriber.SubscribeAsync("x", TestToken);
            var pending = subscriber.SubscribeAsync("x", TestToken);
            await blocked.Task.WaitAsync(TestToken);
            await first.DisposeAsync();
            Assert.False(first.IsConnected);
            Assert.Equal(0, subscriber.SubscriptionCount);
            release.SetResult();
            var second = await pending;
            Assert.True(second.IsConnected);
            await second.DisposeAsync();
        }
        finally
        {
            release.TrySetResult();
        }
    }

    private static ValkeyClusterSubscriberOptions Options(int port) =>
        new()
        {
            Cluster = new ValkeyClusterOptions
            {
                SeedNodes = [new ValkeyClientOptions { Host = "127.0.0.1", Port = port }],
            },
        };

    private static FakeValkeyServer StartSeed(int primaryPort) =>
        FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(Topology(primaryPort));
            await session.ReadCommandAsync();
        });

    private static string Topology(int port, string host = "127.0.0.1") =>
        "*1\r\n%2\r\n+slots\r\n*2\r\n:0\r\n:16383\r\n+nodes\r\n*1\r\n%4\r\n+role\r\n+master\r\n+endpoint\r\n+"
        + host
        + "\r\n+port\r\n:"
        + port.ToString(CultureInfo.InvariantCulture)
        + "\r\n+health\r\n+online\r\n";

    private static string Hello(ValkeyProtocol protocol) =>
        protocol == ValkeyProtocol.Resp3 ? FakeValkeyServer.HelloResp3 : FakeValkeyServer.HelloResp2;

    private static byte[] Ack(ValkeyProtocol protocol, byte[] channel, string kind = "ssubscribe", int count = 1) =>
        [
            .. Encoding.ASCII.GetBytes(
                (protocol == ValkeyProtocol.Resp3 ? ">" : "*")
                    + "3\r\n$"
                    + kind.Length.ToString(CultureInfo.InvariantCulture)
                    + "\r\n"
                    + kind
                    + "\r\n$"
                    + channel.Length.ToString(CultureInfo.InvariantCulture)
                    + "\r\n"
            ),
            .. channel,
            .. Encoding.ASCII.GetBytes("\r\n:" + count.ToString(CultureInfo.InvariantCulture) + "\r\n"),
        ];

    private static byte[] Message(ValkeyProtocol protocol, byte[] channel) =>
        [
            .. Encoding.ASCII.GetBytes(
                (protocol == ValkeyProtocol.Resp3 ? ">" : "*")
                    + "3\r\n$8\r\nsmessage\r\n$"
                    + channel.Length.ToString(CultureInfo.InvariantCulture)
                    + "\r\n"
            ),
            .. channel,
            .. "\r\n$1\r\nv\r\n"u8,
        ];
}
