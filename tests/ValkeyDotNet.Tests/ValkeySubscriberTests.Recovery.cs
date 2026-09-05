using System.Net.Security;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

public sealed partial class ValkeySubscriberTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task RecoveryPreservesBinaryStreamsAndRestoresEachRegistrationOnce(ValkeyProtocol protocol)
    {
        byte[] channel = [0, 255, 13, 10];
        byte[] pattern = [0, 255, (byte)'*'];
        var disconnect = Signal();
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                var direct = await session.ReadBinaryCommandAsync();
                Assert.Equal(channel, direct[1]);
                await session.SendRawAsync(Ack(protocol, "subscribe", channel, 1));
                if (index == 1)
                {
                    // Delivery may interleave with restoration of the remaining registrations.
                    await session.SendRawAsync(Frame(protocol, "message", channel, new byte[] { 255, 0 }));
                }
                var matched = await session.ReadBinaryCommandAsync();
                Assert.Equal(pattern, matched[1]);
                await session.SendRawAsync(Ack(protocol, "psubscribe", pattern, 2));
                if (index == 0)
                {
                    await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    session.Close();
                    return;
                }
                await session.SendRawAsync(Frame(protocol, "pmessage", pattern, channel, "pattern"));
                Assert.Equal("UNSUBSCRIBE", (await session.ReadCommandAsync())[0]);
                await session.SendRawAsync(Ack(protocol, "unsubscribe", channel, 1));
                Assert.Equal("PUNSUBSCRIBE", (await session.ReadCommandAsync())[0]);
                await session.SendRawAsync(Ack(protocol, "punsubscribe", pattern, 0));
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ConnectRecoveringAsync(server, protocol);
        var first = await subscriber.SubscribeAsync(channel, TestToken);
        var duplicate = await subscriber.SubscribeAsync(channel, TestToken);
        var patterns = await subscriber.SubscribePatternAsync(pattern, TestToken);
        await using var messages = first.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        await using var duplicates = duplicate.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        await using var matches = patterns.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        disconnect.SetResult();
        await UntilAsync(() => subscriber.SuccessfulReconnects == 1);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.True(await duplicates.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal(new byte[] { 255, 0 }, messages.Current.Payload.ToArray());
        Assert.Equal(messages.Current.Payload.ToArray(), duplicates.Current.Payload.ToArray());
        Assert.True(await matches.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal(pattern, matches.Current.Pattern!.Value.ToArray());
        Assert.Equal(1, subscriber.ConnectionLosses);
        Assert.Equal(1, subscriber.ReconnectAttempts);
        Assert.Null(subscriber.Failure);
        Assert.False(subscriber.Completion.IsCompleted);
        await first.UnsubscribeAsync(TestToken);
        await duplicate.UnsubscribeAsync(TestToken);
        await patterns.UnsubscribeAsync(TestToken);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnsubscribeDuringReconnectRemovesIntentEvenWithARestoreAcknowledgementInFlight(bool inFlight)
    {
        var disconnect = Signal();
        var pause = Signal();
        var resume = Signal();
        var protocol = ValkeyProtocol.Resp3;
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                if (index == 0)
                {
                    await session.ExpectHandshakeAsync();
                    await session.ReadCommandAsync();
                    await session.SendRawAsync(Ack(protocol, "subscribe", "remove", 1));
                    await session.ReadCommandAsync();
                    await session.SendRawAsync(Ack(protocol, "subscribe", "keep", 2));
                    await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    session.Close();
                    return;
                }
                Assert.Equal("HELLO", (await session.ReadCommandAsync())[0]);
                if (inFlight)
                {
                    await session.SendAsync(Hello(protocol));
                    Assert.Equal(["SUBSCRIBE", "remove"], await session.ReadCommandAsync());
                }
                pause.SetResult();
                await resume.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                if (inFlight)
                {
                    await session.SendRawAsync(Ack(protocol, "subscribe", "remove", 1));
                    await session.SendRawAsync(Frame(protocol, "message", "remove", "discard"));
                }
                else
                {
                    await session.SendAsync(Hello(protocol));
                }
                Assert.Equal(["SUBSCRIBE", "keep"], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(protocol, "subscribe", "keep", inFlight ? 2 : 1));
                if (inFlight)
                {
                    Assert.Equal(["UNSUBSCRIBE", "remove"], await session.ReadCommandAsync());
                    await session.SendRawAsync(Ack(protocol, "unsubscribe", "remove", 1));
                }
                await session.SendRawAsync(Frame(protocol, "message", "keep", "restored"));
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ConnectRecoveringAsync(server, protocol);
        var removed = await subscriber.SubscribeAsync("remove", TestToken);
        var kept = await subscriber.SubscribeAsync("keep", TestToken);
        disconnect.SetResult();
        await pause.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.True(subscriber.IsReconnecting);
        var refused = await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            subscriber.SubscribeAsync("offline", TestToken)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, refused.DeliveryStatus);
        await removed.UnsubscribeAsync(TestToken);
        await using var removedMessages = removed.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.False(await removedMessages.MoveNextAsync());
        resume.SetResult();
        await UntilAsync(() => subscriber.SuccessfulReconnects == 1);
        await using var messages = kept.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal("restored"u8.ToArray(), messages.Current.Payload.ToArray());
        Assert.Equal(0, removed.DroppedMessages);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task DisconnectBeforeConfirmationDoesNotReplayThePendingSubscribe(ValkeyProtocol protocol)
    {
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal(["SUBSCRIBE", "confirmed"], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(protocol, "subscribe", "confirmed", 1));
                if (index == 0)
                {
                    Assert.Equal(["SUBSCRIBE", "unconfirmed"], await session.ReadCommandAsync());
                    session.Close();
                    return;
                }
                // A duplicate local handle after recovery must not result in another wire command.
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ConnectRecoveringAsync(server, protocol);
        await subscriber.SubscribeAsync("confirmed", TestToken);
        var error = await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            subscriber.SubscribeAsync("unconfirmed", TestToken)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        await UntilAsync(() => subscriber.SuccessfulReconnects == 1);
        await subscriber.SubscribeAsync("confirmed", TestToken);
        await subscriber.DisposeAsync();
        await server.Session;
        Assert.Equal(5, server.ReceivedCommands.Count);
    }

    [Theory]
    [InlineData("-WRONGPASS private-secret\r\n", true)]
    [InlineData("-NOPERM private-channel\r\n", false)]
    [InlineData(">3\r\n$9\r\nsubscribe\r\n$5\r\nwrong\r\n:1\r\n", false)]
    public async Task RejectedOrMalformedRestorationIsTerminalWithoutAnotherAttempt(string rejection, bool handshake)
    {
        var disconnect = Signal();
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(index == 1 && handshake ? rejection : Hello(ValkeyProtocol.Resp3));
                if (index == 1 && handshake)
                {
                    return;
                }
                await session.ReadCommandAsync();
                if (index == 1)
                {
                    await session.SendAsync(rejection);
                    await session.ReadCommandAsync();
                    return;
                }
                await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "x", 1));
                await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                session.Close();
            }
        );
        await using var subscriber = await ConnectRecoveringAsync(server, ValkeyProtocol.Resp3);
        var handle = await subscriber.SubscribeAsync("x", TestToken);
        disconnect.SetResult();
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.NotNull(subscriber.Failure);
        Assert.DoesNotContain("private-", subscriber.Failure.ToString(), StringComparison.Ordinal);
        Assert.Equal(1, subscriber.ReconnectAttempts);
        Assert.Equal(0, subscriber.SuccessfulReconnects);
        Assert.False(subscriber.IsConnected);
        Assert.False(subscriber.IsReconnecting);
        await handle.DisposeAsync();
    }

    [Fact]
    public async Task RestorationRetriesAreBoundedAndEveryAttemptUsesAnEmptyServerSubscriptionSet()
    {
        var disconnect = Signal();
        await using var server = FakeValkeyServer.StartMany(
            4,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                Assert.Equal(["SUBSCRIBE", "x"], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "x", 1));
                Assert.Equal(["PSUBSCRIBE", "y*"], await session.ReadCommandAsync());
                if (index == 0)
                {
                    await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "psubscribe", "y*", 2));
                    await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                }
                // Later candidates fail midway through restoration. No old ACK count may survive.
                session.Close();
            }
        );
        await using var subscriber = await ConnectRecoveringAsync(server, ValkeyProtocol.Resp3);
        await subscriber.SubscribeAsync("x", TestToken);
        await subscriber.SubscribePatternAsync("y*", TestToken);
        disconnect.SetResult();
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(3, subscriber.ReconnectAttempts);
        Assert.Equal(0, subscriber.SuccessfulReconnects);
        Assert.IsType<ValkeyConnectionException>(subscriber.Failure);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisposalCancelsBackoffOrAnUnansweredRestorationAndCompletesStreams(bool duringAcknowledgement)
    {
        var disconnect = Signal();
        var restoring = Signal();
        await using var server = FakeValkeyServer.StartMany(
            duringAcknowledgement ? 2 : 1,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                if (index == 0)
                {
                    await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "x", 1));
                    await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    session.Close();
                }
                else
                {
                    restoring.SetResult();
                    await session.ReadCommandAsync();
                }
            }
        );
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = server.ClientOptions(),
                EnableReconnect = true,
                InitialReconnectDelay = TimeSpan.FromSeconds(duringAcknowledgement ? 0.01 : 10),
                MaxReconnectDelay = TimeSpan.FromSeconds(10),
            },
            TestToken
        );
        var handle = await subscriber.SubscribeAsync("x", TestToken);
        disconnect.SetResult();
        await UntilAsync(() => subscriber.IsReconnecting);
        if (duringAcknowledgement)
        {
            await restoring.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        }
        await subscriber.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Null(subscriber.Failure);
        Assert.Equal(duringAcknowledgement ? 1 : 0, subscriber.ReconnectAttempts);
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.False(await messages.MoveNextAsync());
    }

    [Fact]
    public async Task RecoveryBudgetBoundsAnUnresponsiveHandshake()
    {
        var disconnect = Signal();
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                if (index == 1)
                {
                    await session.ReadCommandAsync();
                    await session.ReadCommandAsync();
                    return;
                }
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "x", 1));
                await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                session.Close();
            }
        );
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = server.ClientOptions(),
                EnableReconnect = true,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(10),
                RecoveryTimeout = TimeSpan.FromMilliseconds(300),
            },
            TestToken
        );
        await subscriber.SubscribeAsync("x", TestToken);
        disconnect.SetResult();
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.IsType<TimeoutException>(subscriber.Failure);
        Assert.Equal(1, subscriber.ReconnectAttempts);
    }

    [Fact]
    public async Task TlsCredentialsDatabaseAndClientNameAreReappliedOnRestoration()
    {
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        var disconnect = Signal();
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                Assert.Equal(
                    ["HELLO", "3", "AUTH", "user", "password", "SETNAME", "restoring"],
                    await session.ExpectHandshakeAsync()
                );
                Assert.Equal(["SELECT", "2"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
                await session.ReadCommandAsync();
                await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "x", 1));
                if (index == 0)
                {
                    await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    session.Close();
                }
                else
                {
                    await session.ReadCommandAsync();
                }
            },
            certificate
        );
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                EnableReconnect = true,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(10),
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    UseTls = true,
                    Username = "user",
                    Password = "password",
                    ClientName = "restoring",
                    Database = 2,
                    CertificateValidationCallback = (_, presented, _, errors) =>
                        presented?.GetCertHashString() == certificate.GetCertHashString()
                        && (errors & SslPolicyErrors.RemoteCertificateNameMismatch) == 0,
                },
            },
            TestToken
        );
        await subscriber.SubscribeAsync("x", TestToken);
        disconnect.SetResult();
        await UntilAsync(() => subscriber.SuccessfulReconnects == 1);
        Assert.True(subscriber.IsConnected);
    }

    [Fact]
    public async Task InvalidRecoveryBoundsAreRejectedBeforeNetworking()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(new ValkeySubscriberOptions { MaxReconnectAttempts = 0 }, TestToken)
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(
                new ValkeySubscriberOptions { InitialReconnectDelay = TimeSpan.Zero },
                TestToken
            )
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(
                new ValkeySubscriberOptions { MaxReconnectDelay = TimeSpan.FromDays(100) },
                TestToken
            )
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(new ValkeySubscriberOptions { RecoveryTimeout = TimeSpan.Zero }, TestToken)
        );
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task RepeatedRestorationKeepsOneStreamAndOneRegistrationPerConnection(ValkeyProtocol protocol)
    {
        const int cycles = 12;
        var proceed = Enumerable.Range(0, cycles).Select(_ => Signal()).ToArray();
        await using var server = FakeValkeyServer.StartMany(
            cycles + 1,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal(["SUBSCRIBE", "x"], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(protocol, "subscribe", "x", 1));
                await session.SendRawAsync(Frame(protocol, "message", "x", index));
                if (index < cycles)
                {
                    await proceed[index].Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    session.Close();
                }
                else
                {
                    await session.ReadCommandAsync();
                }
            }
        );
        await using var subscriber = await ConnectRecoveringAsync(server, protocol);
        var handle = await subscriber.SubscribeAsync("x", TestToken);
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        for (var cycle = 0; cycle <= cycles; cycle++)
        {
            Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
            Assert.Equal(
                System.Text.Encoding.ASCII.GetBytes(cycle.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                messages.Current.Payload.ToArray()
            );
            await UntilAsync(() => subscriber.SuccessfulReconnects == cycle);
            if (cycle < cycles)
            {
                proceed[cycle].SetResult();
            }
        }
        Assert.Equal(cycles, subscriber.ConnectionLosses);
        Assert.Equal(cycles, subscriber.ReconnectAttempts);
        Assert.Equal(0, subscriber.DroppedMessages);
        await subscriber.DisposeAsync();
        await server.Session;
        Assert.Equal((cycles + 1) * 2, server.ReceivedCommands.Count);
    }

    [Fact]
    public async Task CallerCancellationRemainsTerminalEvenWhenRecoveryIsEnabled()
    {
        var received = Signal();
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            received.SetResult();
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectRecoveringAsync(server, ValkeyProtocol.Resp3);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var pending = subscriber.SubscribeAsync("x", cancellation.Token);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        await cancellation.CancelAsync();
        await Assert.ThrowsAsync<ValkeyCommandCanceledException>(() => pending);
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(0, subscriber.ReconnectAttempts);
        Assert.IsType<ValkeyCommandCanceledException>(subscriber.Failure);
    }

    [Theory]
    [InlineData("$2048\r\n")]
    [InlineData(">17\r\n")]
    [InlineData(">1\r\n*1\r\n*1\r\n:0\r\n")]
    public async Task ReplacementReadersRetainAllParsingBounds(string frame)
    {
        var disconnect = Signal();
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                if (index == 0)
                {
                    await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "x", 1));
                    await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    session.Close();
                }
                else
                {
                    await session.SendAsync(frame);
                    await session.ReadCommandAsync();
                }
            }
        );
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                EnableReconnect = true,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(10),
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    MaxResponseBytes = 1024,
                    MaxResponseElements = 16,
                    MaxNestingDepth = 1,
                },
            },
            TestToken
        );
        await subscriber.SubscribeAsync("x", TestToken);
        disconnect.SetResult();
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.IsType<ValkeyProtocolException>(subscriber.Failure);
        Assert.Equal(1, subscriber.ReconnectAttempts);
    }

    private static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        while (!predicate())
        {
            await Task.Delay(5, timeout.Token);
        }
    }

    private static Task<ValkeySubscriber> ConnectRecoveringAsync(FakeValkeyServer server, ValkeyProtocol protocol) =>
        ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = Connection(server, protocol),
                EnableReconnect = true,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(10),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(40),
            },
            TestToken
        );
}
