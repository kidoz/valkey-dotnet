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
            var handle = await SubscribeModeAsync(subscriber, mode, TestToken);
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
            var handle = await SubscribeModeAsync(subscriber, mode, TestToken);
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
            var failure = await Assert.ThrowsAsync<ValkeyServerException>(() =>
                SubscribeModeAsync(subscriber, mode, TestToken)
            );
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
        await Assert.ThrowsAsync<ValkeyConnectionException>(() => SubscribeModeAsync(subscriber, mode, TestToken));
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, 0)]
    [InlineData(ValkeyProtocol.Resp2, 1)]
    [InlineData(ValkeyProtocol.Resp2, 2)]
    [InlineData(ValkeyProtocol.Resp3, 0)]
    [InlineData(ValkeyProtocol.Resp3, 1)]
    [InlineData(ValkeyProtocol.Resp3, 2)]
    public async Task DisposalSettlesUnacknowledgedSubscriptionWithoutWaitingForDeadline(
        ValkeyProtocol protocol,
        int mode
    )
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal([SubscriptionKind(mode).ToUpperInvariant(), ""], await session.ReadCommandAsync());
            received.SetResult();
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = Connection(server, protocol),
                UseShardedPubSub = mode == 2,
                OperationTimeout = TimeSpan.FromMinutes(1),
            },
            TestToken
        );
        var pending = SubscribeModeAsync(subscriber, mode, TestToken);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        await subscriber.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => pending.WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.False(subscriber.IsConnected);
        Assert.Null(subscriber.Failure);
        await server.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, 0)]
    [InlineData(ValkeyProtocol.Resp2, 1)]
    [InlineData(ValkeyProtocol.Resp2, 2)]
    [InlineData(ValkeyProtocol.Resp3, 0)]
    [InlineData(ValkeyProtocol.Resp3, 1)]
    [InlineData(ValkeyProtocol.Resp3, 2)]
    public async Task CallerCancellationStillTerminatesUnacknowledgedSubscription(ValkeyProtocol protocol, int mode)
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal([SubscriptionKind(mode).ToUpperInvariant(), ""], await session.ReadCommandAsync());
            received.SetResult();
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectModeAsync(server, protocol, mode);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var pending = SubscribeModeAsync(subscriber, mode, cancellation.Token);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        await cancellation.CancelAsync();
        var failure = await Assert.ThrowsAsync<ValkeyCommandCanceledException>(() =>
            pending.WaitAsync(TimeSpan.FromSeconds(5), TestToken)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
        Assert.Equal(cancellation.Token, failure.CancellationToken);
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.False(subscriber.IsConnected);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, 0)]
    [InlineData(ValkeyProtocol.Resp2, 1)]
    [InlineData(ValkeyProtocol.Resp2, 2)]
    [InlineData(ValkeyProtocol.Resp3, 0)]
    [InlineData(ValkeyProtocol.Resp3, 1)]
    [InlineData(ValkeyProtocol.Resp3, 2)]
    public async Task DeadlineStillTerminatesUnacknowledgedSubscription(ValkeyProtocol protocol, int mode)
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal([SubscriptionKind(mode).ToUpperInvariant(), ""], await session.ReadCommandAsync());
            received.SetResult();
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = Connection(server, protocol),
                UseShardedPubSub = mode == 2,
                OperationTimeout = TimeSpan.FromMilliseconds(500),
            },
            TestToken
        );
        var pending = SubscribeModeAsync(subscriber, mode, TestToken);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        var failure = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() =>
            pending.WaitAsync(TimeSpan.FromSeconds(5), TestToken)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.False(subscriber.IsConnected);
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

    private static Task<ValkeySubscription> SubscribeModeAsync(
        ValkeySubscriber subscriber,
        int mode,
        CancellationToken token
    ) =>
        mode switch
        {
            1 => subscriber.SubscribePatternAsync("", token),
            2 => subscriber.SubscribeShardedAsync("", token),
            _ => subscriber.SubscribeAsync("", token),
        };
}
