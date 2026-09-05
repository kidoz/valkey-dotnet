using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests.Cluster;

public sealed partial class ValkeyClusterSubscriberTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2, false)]
    [InlineData(ValkeyProtocol.Resp3, false)]
    [InlineData(ValkeyProtocol.Resp2, true)]
    [InlineData(ValkeyProtocol.Resp3, true)]
    public async Task TopologyRecoveryPreservesHandleStreamAndBinaryChannel(ValkeyProtocol protocol, bool transportLoss)
    {
        byte[] channel = [123, 120, 125, 0, 255, 13, 10];
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var replacement = FakeValkeyServer.Start(async session =>
        {
            await retired.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await session.ExpectHandshakeAsync(Hello(protocol));
            var command = await session.ReadBinaryCommandAsync();
            Assert.Equal("SSUBSCRIBE"u8.ToArray(), command[0]);
            Assert.Equal(channel, command[1]);
            await session.SendRawAsync([.. Ack(protocol, channel), .. Message(protocol, channel)]);
            Assert.Equal("SUNSUBSCRIBE", (await session.ReadCommandAsync())[0]);
            await session.SendRawAsync(Ack(protocol, channel, "sunsubscribe", 0));
            await session.ReadCommandAsync();
        });
        await using var origin = FakeValkeyServer.Start(async session =>
        {
            try
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                await session.ReadCommandAsync();
                await session.SendRawAsync([.. Ack(protocol, channel), .. Message(protocol, channel)]);
                await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                if (!transportLoss)
                {
                    await session.SendRawAsync(Ack(protocol, channel, "sunsubscribe", 0));
                    await session.ReadCommandAsync();
                }
            }
            finally
            {
                retired.TrySetResult();
            }
        });
        await using var seed = RecoverySeed(origin.Port, replacement.Port, protocol);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            RecoveryOptions(seed.Port, protocol),
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync(channel, TestToken);
        var completion = handle.Completion;
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        trigger.SetResult();
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal(channel, messages.Current.Channel.ToArray());
        Assert.True(messages.Current.IsSharded);
        Assert.Same(completion, handle.Completion);
        Assert.False(completion.IsCompleted);
        Assert.True(handle.IsConnected);
        Assert.Null(handle.Failure);
        Assert.Equal(1, handle.ConnectionLosses);
        Assert.Equal(1, handle.ReconnectAttempts);
        Assert.Equal(1, handle.SuccessfulReconnects);
        Assert.Equal(1, handle.SuccessfulRelocations);
        Assert.Equal(1, subscriber.SubscriptionCount);
        await handle.UnsubscribeAsync(TestToken);
        Assert.False(await messages.MoveNextAsync());
        Assert.Equal(0, subscriber.SubscriptionCount);
        await subscriber.DisposeAsync();
        await Task.WhenAll(origin.Session, replacement.Session, seed.Session)
            .WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, "ASK")]
    [InlineData(ValkeyProtocol.Resp3, "ASK")]
    [InlineData(ValkeyProtocol.Resp2, "MOVED")]
    [InlineData(ValkeyProtocol.Resp3, "MOVED")]
    public async Task RecoveryFollowsBoundedRedirectsBeforeRestoringStream(ValkeyProtocol protocol, string kind)
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redirected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await redirected.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await session.ExpectHandshakeAsync(Hello(protocol));
            if (kind == "ASK")
            {
                Assert.Equal(["ASKING"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
            }
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync([.. Ack(protocol, "x"u8.ToArray()), .. Message(protocol, "x"u8.ToArray())]);
            await session.ReadCommandAsync();
        });
        await using var origin = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
                if (index == 0)
                {
                    await session.SendRawAsync(Ack(protocol, "x"u8.ToArray()));
                    await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    return;
                }
                await session.SendAsync($"-{kind} 16287 127.0.0.1:{target.Port}\r\n");
                try
                {
                    await session.ReadCommandAsync();
                }
                finally
                {
                    redirected.TrySetResult();
                }
            }
        );
        await using var seed = FakeValkeyServer.StartMany(
            kind == "MOVED" ? 3 : 2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
                await session.SendAsync(Topology(index < 2 ? origin.Port : target.Port));
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            RecoveryOptions(seed.Port, protocol),
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal(1, handle.SuccessfulReconnects);
        Assert.Equal(1, handle.SuccessfulRelocations);
        await handle.DisposeAsync();
        await subscriber.DisposeAsync();
        await Task.WhenAll(origin.Session, target.Session, seed.Session).WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Theory]
    [InlineData("ASK")]
    [InlineData("MOVED")]
    [InlineData("ASK 0 private.invalid:6379")]
    [InlineData("NOPERM private-payload")]
    public async Task RecoveryRedirectExhaustionAndInvalidOrDeniedRepliesAreTerminal(string reply)
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var origin = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                if (index == 0)
                {
                    await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
                    await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    return;
                }
                await session.SendAsync(
                    "-" + (reply is "ASK" or "MOVED" ? reply + " 16287 private.invalid:6379" : reply) + "\r\n"
                );
                await session.ReadCommandAsync();
            }
        );
        await using var seed = RecoverySeed(origin.Port, origin.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            RecoveryOptions(seed.Port, maxRedirects: reply is "ASK" or "MOVED" ? 0 : 2),
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        await handle.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.NotNull(handle.Failure);
        Assert.DoesNotContain("private", handle.Failure.ToString(), StringComparison.Ordinal);
        Assert.Equal(1, handle.ReconnectAttempts);
        Assert.Equal(0, handle.SuccessfulReconnects);
        Assert.Equal(1, subscriber.SubscriptionCount);
        await handle.DisposeAsync();
        Assert.Equal(0, subscriber.SubscriptionCount);
    }

    [Theory]
    [InlineData("dispose")]
    [InlineData("unsubscribe")]
    [InlineData("owner")]
    public async Task RemovalDuringTopologyRefreshCancelsRecoveryWithoutResurrection(string removal)
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var refreshing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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
                else
                {
                    refreshing.TrySetResult();
                }
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(RecoveryOptions(seed.Port), TestToken);
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        trigger.SetResult();
        await refreshing.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.True(handle.IsReconnecting);
        var removing = removal switch
        {
            "dispose" => handle.DisposeAsync().AsTask(),
            "unsubscribe" => handle.UnsubscribeAsync(TestToken),
            _ => subscriber.DisposeAsync().AsTask(),
        };
        await removing.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        await handle.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Null(handle.Failure);
        Assert.False(handle.IsConnected);
        Assert.False(handle.IsReconnecting);
        Assert.Equal(0, handle.SuccessfulReconnects);
        Assert.Equal(0, subscriber.SubscriptionCount);
        await subscriber.DisposeAsync();
        await seed.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(33)]
    public async Task TopologyDiscoveryEndpointBoundIsValidated(int limit)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeyClusterSubscriber.ConnectAsync(
                new ValkeyClusterSubscriberOptions { MaxTopologyRefreshEndpoints = limit },
                TestToken
            )
        );
    }

    private static FakeValkeyServer RecoverySeed(
        int originalPort,
        int replacementPort,
        ValkeyProtocol protocol = ValkeyProtocol.Resp3
    ) =>
        FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
                await session.SendAsync(Topology(index == 0 ? originalPort : replacementPort));
                await session.ReadCommandAsync();
            }
        );

    private static ValkeyClusterSubscriberOptions RecoveryOptions(
        int seedPort,
        ValkeyProtocol protocol = ValkeyProtocol.Resp3,
        int maxRedirects = 2
    ) =>
        new()
        {
            Cluster = new ValkeyClusterOptions
            {
                SeedNodes =
                [
                    new ValkeyClientOptions
                    {
                        Host = "127.0.0.1",
                        Port = seedPort,
                        Protocol = protocol,
                    },
                ],
                MaxRedirects = maxRedirects,
            },
            EnableTopologyRecovery = true,
            InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
            MaxReconnectDelay = TimeSpan.FromMilliseconds(1),
        };
}
