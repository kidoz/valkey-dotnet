using System.Text;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

public sealed partial class ValkeySubscriberTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ShardedModeDeliversFragmentedBinaryMessagesAndSharesLocalHandles(ValkeyProtocol protocol)
    {
        byte[] name = [0, 255, 13, 10];
        byte[] payload = [255, 0, 128];
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            var command = await session.ReadBinaryCommandAsync();
            Assert.Equal("SSUBSCRIBE"u8.ToArray(), command[0]);
            Assert.Equal(name, command[1]);
            await session.SendRawAsync(Ack(protocol, "ssubscribe", name, 1));
            await deliver.Task.WaitAsync(TestToken);
            foreach (var value in Frame(protocol, "smessage", name, payload))
            {
                await session.SendRawAsync([value]);
            }
            Assert.Equal("SUNSUBSCRIBE", (await session.ReadCommandAsync())[0]);
            await session.SendRawAsync(Ack(protocol, "sunsubscribe", name, 0));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectShardAsync(server, protocol);
        var first = await subscriber.SubscribeShardedAsync(name, TestToken);
        var second = await subscriber.SubscribeShardedAsync(name, TestToken);
        await first.DisposeAsync();
        deliver.SetResult();
        await using var messages = second.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(name, messages.Current.Channel.ToArray());
        Assert.Equal(payload, messages.Current.Payload.ToArray());
        Assert.True(messages.Current.IsSharded);
        Assert.Null(messages.Current.Pattern);
        await second.UnsubscribeAsync(TestToken);
        Assert.False(await messages.MoveNextAsync());
        Assert.True(subscriber.IsConnected);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ShardedAndGlobalModesRejectMixingBeforeWriting(ValkeyProtocol protocol)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(protocol, "ssubscribe", "x", 1));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectShardAsync(server, protocol);
        await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.SubscribeAsync("never-sent", TestToken));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            subscriber.SubscribePatternAsync("never-sent*", TestToken)
        );
        await subscriber.SubscribeShardedAsync("x", TestToken);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ShardUnsubscriptionByServerIsAnExplicitTopologyFailure(ValkeyProtocol protocol)
    {
        var depart = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(protocol, "ssubscribe", "x", 1));
            await depart.Task.WaitAsync(TestToken);
            await session.SendRawAsync(Ack(protocol, "sunsubscribe", "x", 0));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectShardAsync(server, protocol, reconnect: true);
        var subscription = await subscriber.SubscribeShardedAsync("x", TestToken);
        depart.SetResult();
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.IsType<ValkeyClusterException>(subscriber.Failure);
        Assert.Equal(0, subscriber.ReconnectAttempts);
        await using var messages = subscription.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        await Assert.ThrowsAsync<ValkeyClusterException>(() => messages.MoveNextAsync().AsTask());
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ShardReconnectRestoresOnlyTheSameEndpoint(ValkeyProtocol protocol)
    {
        var lose = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(protocol, "ssubscribe", "x", 1));
                if (index == 0)
                {
                    await lose.Task.WaitAsync(TestToken);
                    session.Close();
                    return;
                }
                await session.SendRawAsync(Frame(protocol, "smessage", "x", "restored"));
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ConnectShardAsync(server, protocol, reconnect: true);
        var subscription = await subscriber.SubscribeShardedAsync("x", TestToken);
        lose.SetResult();
        await using var messages = subscription.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal("restored", Encoding.UTF8.GetString(messages.Current.Payload.Span));
        Assert.Equal(1, subscriber.SuccessfulReconnects);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, "message")]
    [InlineData(ValkeyProtocol.Resp3, "message")]
    [InlineData(ValkeyProtocol.Resp2, "pmessage")]
    [InlineData(ValkeyProtocol.Resp3, "pmessage")]
    public async Task ShardModeRejectsGlobalDeliveryFrames(ValkeyProtocol protocol, string kind)
    {
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(protocol, "ssubscribe", "x", 1));
            await deliver.Task.WaitAsync(TestToken);
            await session.SendRawAsync(Frame(protocol, kind, "x", "unexpected"));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectShardAsync(server, protocol);
        await subscriber.SubscribeShardedAsync("x", TestToken);
        deliver.SetResult();
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.IsType<ValkeyProtocolException>(subscriber.Failure);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ShardQueueOverflowIsBoundedAndDoesNotBlockUnsubscription(ValkeyProtocol protocol)
    {
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(protocol, "ssubscribe", "x", 1));
            await deliver.Task.WaitAsync(TestToken);
            for (var index = 0; index < 10; index++)
            {
                await session.SendRawAsync(Frame(protocol, "smessage", "x", "bounded"));
            }
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(protocol, "sunsubscribe", "x", 0));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = Connection(server, protocol),
                UseShardedPubSub = true,
                QueueCapacity = 1,
            },
            TestToken
        );
        var handle = await subscriber.SubscribeShardedAsync("x", TestToken);
        deliver.SetResult();
        await handle.UnsubscribeAsync(TestToken);
        Assert.Equal(9, handle.DroppedMessages);
        Assert.Equal(9, subscriber.DroppedMessages);
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.False(await messages.MoveNextAsync());
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task ShardedFramesRetainConfiguredResponseBounds(ValkeyProtocol protocol)
    {
        var send = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(protocol, "ssubscribe", "x", 1));
            await send.Task.WaitAsync(TestToken);
            await session.SendAsync(
                (protocol == ValkeyProtocol.Resp3 ? ">" : "*") + "3\r\n$8\r\nsmessage\r\n$1\r\nx\r\n$1024\r\n"
            );
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    Protocol = protocol,
                    MaxResponseBytes = 1024,
                },
                UseShardedPubSub = true,
            },
            TestToken
        );
        await subscriber.SubscribeShardedAsync("x", TestToken);
        send.SetResult();
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.IsType<ValkeyProtocolException>(subscriber.Failure);
    }

    private static Task<ValkeySubscriber> ConnectShardAsync(
        FakeValkeyServer server,
        ValkeyProtocol protocol,
        bool reconnect = false
    ) =>
        ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    Protocol = protocol,
                },
                UseShardedPubSub = true,
                EnableReconnect = reconnect,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(2),
            },
            TestToken
        );
}
