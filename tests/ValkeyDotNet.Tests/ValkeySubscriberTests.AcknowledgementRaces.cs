using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

public sealed partial class ValkeySubscriberTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2, 0)]
    [InlineData(ValkeyProtocol.Resp2, 1)]
    [InlineData(ValkeyProtocol.Resp2, 2)]
    [InlineData(ValkeyProtocol.Resp3, 0)]
    [InlineData(ValkeyProtocol.Resp3, 1)]
    [InlineData(ValkeyProtocol.Resp3, 2)]
    public async Task AcknowledgedSubscriptionsRetainBufferedMessagesAcrossImmediateClose(
        ValkeyProtocol protocol,
        int mode
    )
    {
        var kind = SubscriptionKind(mode);
        for (var cycle = 0; cycle < 32; cycle++)
        {
            await using var server = FakeValkeyServer.Start(async session =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal([kind.ToUpperInvariant(), ""], await session.ReadCommandAsync());
                var message =
                    mode == 1
                        ? Frame(protocol, "pmessage", "", "", "")
                        : Frame(protocol, mode == 2 ? "smessage" : "message", "", "");
                await session.SendRawAsync([.. Ack(protocol, kind, "", 1), .. message]);
                session.Close();
            });
            await using var subscriber = await ConnectModeAsync(server, protocol, mode);
            var handle = await SubscribeModeAsync(subscriber, mode);
            await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
            Assert.True(await messages.MoveNextAsync());
            Assert.Empty(messages.Current.Channel.ToArray());
            Assert.Empty(messages.Current.Payload.ToArray());
            Assert.Equal(mode == 2, messages.Current.IsSharded);
            Assert.Equal(mode == 1, messages.Current.Pattern.HasValue);
            await Assert.ThrowsAsync<ValkeyConnectionException>(() => messages.MoveNextAsync().AsTask());
            Assert.IsType<ValkeyConnectionException>(subscriber.Failure);
            await handle.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, 0)]
    [InlineData(ValkeyProtocol.Resp2, 1)]
    [InlineData(ValkeyProtocol.Resp2, 2)]
    [InlineData(ValkeyProtocol.Resp3, 0)]
    [InlineData(ValkeyProtocol.Resp3, 1)]
    [InlineData(ValkeyProtocol.Resp3, 2)]
    public async Task AcknowledgedUnsubscriptionsRemainSuccessfulAcrossImmediateClose(ValkeyProtocol protocol, int mode)
    {
        var kind = SubscriptionKind(mode);
        var unsubscribe =
            mode == 1 ? "punsubscribe"
            : mode == 2 ? "sunsubscribe"
            : "unsubscribe";
        for (var cycle = 0; cycle < 32; cycle++)
        {
            await using var server = FakeValkeyServer.Start(async session =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal([kind.ToUpperInvariant(), ""], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(protocol, kind, "", 1));
                Assert.Equal([unsubscribe.ToUpperInvariant(), ""], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(protocol, unsubscribe, "", 0));
                session.Close();
            });
            await using var subscriber = await ConnectModeAsync(server, protocol, mode);
            var handle = await SubscribeModeAsync(subscriber, mode);
            await handle.UnsubscribeAsync(TestToken);
            await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
            Assert.False(await messages.MoveNextAsync());
            await handle.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, 0)]
    [InlineData(ValkeyProtocol.Resp2, 1)]
    [InlineData(ValkeyProtocol.Resp2, 2)]
    [InlineData(ValkeyProtocol.Resp3, 0)]
    [InlineData(ValkeyProtocol.Resp3, 1)]
    [InlineData(ValkeyProtocol.Resp3, 2)]
    public async Task SubscriptionRejectionsRemainAuthoritativeAcrossImmediateClose(ValkeyProtocol protocol, int mode)
    {
        for (var cycle = 0; cycle < 32; cycle++)
        {
            await using var server = FakeValkeyServer.Start(async session =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal([SubscriptionKind(mode).ToUpperInvariant(), ""], await session.ReadCommandAsync());
                await session.SendAsync("-NOPERM private-server-detail\r\n");
                session.Close();
            });
            await using var subscriber = await ConnectModeAsync(server, protocol, mode);
            var failure = await Assert.ThrowsAsync<ValkeyServerException>(() => SubscribeModeAsync(subscriber, mode));
            Assert.Equal("NOPERM", failure.ErrorCode);
            Assert.DoesNotContain("private-server-detail", failure.ToString(), StringComparison.Ordinal);
            await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        }
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, 0)]
    [InlineData(ValkeyProtocol.Resp2, 1)]
    [InlineData(ValkeyProtocol.Resp2, 2)]
    [InlineData(ValkeyProtocol.Resp3, 0)]
    [InlineData(ValkeyProtocol.Resp3, 1)]
    [InlineData(ValkeyProtocol.Resp3, 2)]
    public async Task CloseBeforeAcknowledgementStillFailsSubscription(ValkeyProtocol protocol, int mode)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal([SubscriptionKind(mode).ToUpperInvariant(), ""], await session.ReadCommandAsync());
            session.Close();
        });
        await using var subscriber = await ConnectModeAsync(server, protocol, mode);
        await Assert.ThrowsAsync<ValkeyConnectionException>(() => SubscribeModeAsync(subscriber, mode));
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    private static string SubscriptionKind(int mode) =>
        mode == 1 ? "psubscribe"
        : mode == 2 ? "ssubscribe"
        : "subscribe";

    private static Task<ValkeySubscriber> ConnectModeAsync(
        FakeValkeyServer server,
        ValkeyProtocol protocol,
        int mode
    ) =>
        ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions { Connection = Connection(server, protocol), UseShardedPubSub = mode == 2 },
            TestToken
        );

    private static Task<ValkeySubscription> SubscribeModeAsync(ValkeySubscriber subscriber, int mode) =>
        mode switch
        {
            1 => subscriber.SubscribePatternAsync("", TestToken),
            2 => subscriber.SubscribeShardedAsync("", TestToken),
            _ => subscriber.SubscribeAsync("", TestToken),
        };
}
