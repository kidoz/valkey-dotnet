using System.Globalization;
using System.Net.Security;
using System.Security.Authentication;
using System.Text;
using ValkeyDotNet.Protocol;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

public sealed partial class ValkeySubscriberTests
{
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(ValkeyProtocol.Resp2, false)]
    [InlineData(ValkeyProtocol.Resp2, true)]
    [InlineData(ValkeyProtocol.Resp3, false)]
    [InlineData(ValkeyProtocol.Resp3, true)]
    public async Task SubscriptionsMatchAcknowledgementsAndDeliverBinaryMessages(ValkeyProtocol protocol, bool pattern)
    {
        byte[] name = [0, 255, 13, 10];
        byte[] channel = pattern ? [254, 0, 42] : name;
        byte[] payload = [255, 0, 13, 10, 128];
        var kind = pattern ? "psubscribe" : "subscribe";
        await using var server = FakeValkeyServer.Start(async session =>
        {
            var hello = await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(((int)protocol).ToString(CultureInfo.InvariantCulture), hello[1]);
            var command = await session.ReadBinaryCommandAsync();
            Assert.Equal(kind.ToUpperInvariant(), Encoding.ASCII.GetString(command[0]));
            Assert.Equal(name, command[1]);
            await session.SendRawAsync(Ack(protocol, kind, name, 1));
            var frame = pattern
                ? Frame(protocol, "pmessage", name, channel, payload)
                : Frame(protocol, "message", name, payload);
            // Fragment every byte, including binary payload and CRLF boundaries.
            foreach (var value in frame)
            {
                await session.SendRawAsync([value]);
            }

            Assert.Equal(pattern ? "PUNSUBSCRIBE" : "UNSUBSCRIBE", (await session.ReadCommandAsync())[0]);
            await session.SendRawAsync(Ack(protocol, pattern ? "punsubscribe" : "unsubscribe", name, 0));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectAsync(server, protocol);
        Assert.Equal(protocol, subscriber.NegotiatedProtocol);
        var subscription = pattern
            ? await subscriber.SubscribePatternAsync(name, TestToken)
            : await subscriber.SubscribeAsync(name, TestToken);
        await using var messages = subscription.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal(channel, messages.Current.Channel.ToArray());
        Assert.Equal(payload, messages.Current.Payload.ToArray());
        if (pattern)
        {
            Assert.Equal(name, messages.Current.Pattern!.Value.ToArray());
        }
        else
        {
            Assert.Null(messages.Current.Pattern);
        }

        await subscription.DisposeAsync();
        await subscription.DisposeAsync();
        Assert.False(await messages.MoveNextAsync());
        Assert.True(subscriber.IsConnected);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task DuplicateHandlesDoNotRemoveEachOthersServerSubscription(ValkeyProtocol protocol)
    {
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(["SUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(protocol, "subscribe", "x", 1));
            await deliver.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await session.SendRawAsync(Frame(protocol, "message", "x", "remaining"));
            Assert.Equal(["UNSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(protocol, "unsubscribe", "x", 0));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectAsync(server, protocol);
        var first = await subscriber.SubscribeAsync("x", TestToken);
        var second = await subscriber.SubscribeAsync("x", TestToken);
        await first.DisposeAsync();
        deliver.SetResult();
        await using var messages = second.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal("remaining", Encoding.UTF8.GetString(messages.Current.Payload.Span));
        await second.DisposeAsync();
        await subscriber.DisposeAsync();
        await server.Session;
        Assert.Equal(3, server.ReceivedCommands.Count);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task QueueOverflowDropsIncomingMessagesWithoutBlockingAcknowledgements(ValkeyProtocol protocol)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(protocol, "subscribe", "x", 1));
            for (var i = 0; i < 3; i++)
            {
                await session.SendRawAsync(Frame(protocol, "message", "x", i));
            }

            Assert.Equal(["PSUBSCRIBE", "x*"], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(protocol, "psubscribe", "x*", 2));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions { Connection = Connection(server, protocol), QueueCapacity = 1 },
            TestToken
        );
        var slow = await subscriber.SubscribeAsync("x", TestToken);
        await subscriber.SubscribePatternAsync("x*", TestToken);
        Assert.Equal(2, slow.DroppedMessages);
        Assert.Equal(2, subscriber.DroppedMessages);
        await using var messages = slow.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal("0", Encoding.UTF8.GetString(messages.Current.Payload.Span));
        await subscriber.DisposeAsync();
        Assert.False(await messages.MoveNextAsync());
        await slow.DisposeAsync();
    }

    [Theory]
    [InlineData(">3\r\n$9\r\nsubscribe\r\n$5\r\nwrong\r\n:1\r\n")]
    [InlineData(">3\r\n$9\r\nsubscribe\r\n$1\r\nx\r\n:2\r\n")]
    [InlineData("*3\r\n$9\r\nsubscribe\r\n$1\r\nx\r\n:1\r\n")]
    [InlineData(">3\r\n$7\r\nmessage\r\n$1\r\nx\r\n$1\r\na\r\n")]
    [InlineData(">3\r\n$9\r\nsubscribe\r\n_\r\n:1\r\n")]
    public async Task MalformedOrUnmatchedFramesTerminateTheSubscriber(string frame)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(frame);
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectAsync(server, ValkeyProtocol.Resp3);
        await Assert.ThrowsAsync<ValkeyProtocolException>(() => subscriber.SubscribeAsync("x", TestToken));
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.False(subscriber.IsConnected);
        Assert.IsType<ValkeyProtocolException>(subscriber.Failure);
    }

    [Fact]
    public async Task ServerRejectionIsSanitizedAndDoesNotPoisonTheNextSubscription()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("-NOPERM secret-channel private-payload\r\n");
            Assert.Equal(["SUBSCRIBE", "allowed"], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "allowed", 1));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ConnectAsync(server, ValkeyProtocol.Resp3);
        var error = await Assert.ThrowsAsync<ValkeyServerException>(() =>
            subscriber.SubscribeAsync("forbidden", TestToken)
        );
        Assert.Equal("NOPERM", error.ErrorCode);
        Assert.DoesNotContain("secret-channel", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("private-payload", error.ToString(), StringComparison.Ordinal);
        await subscriber.SubscribeAsync("allowed", TestToken);
        Assert.True(subscriber.IsConnected);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationOrTimeoutAfterWritingTerminatesAllSubscriptions(bool cancel)
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "active", 1));
            await session.ReadCommandAsync();
            received.SetResult();
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = server.ClientOptions(),
                OperationTimeout = TimeSpan.FromMilliseconds(cancel ? 5000 : 300),
            },
            TestToken
        );
        var active = await subscriber.SubscribeAsync("active", TestToken);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var waiting = subscriber.SubscribeAsync("waiting", cancellation.Token);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        if (cancel)
        {
            await cancellation.CancelAsync();
        }

        var error = await Record.ExceptionAsync(() => waiting);
        if (cancel)
        {
            Assert.IsType<ValkeyCommandCanceledException>(error);
        }
        else
        {
            Assert.IsType<ValkeyCommandTimeoutException>(error);
        }

        Assert.Equal(
            ValkeyCommandDeliveryStatus.MayHaveBeenSent,
            Assert.IsAssignableFrom<IValkeyCommandFailure>(error).DeliveryStatus
        );
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        await using var messages = active.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.NotNull(await Record.ExceptionAsync(() => messages.MoveNextAsync().AsTask()));
        await active.DisposeAsync();
        Assert.False(subscriber.IsConnected);
    }

    [Fact]
    public async Task AdmissionIsBoundedAndQueuedCancellationDoesNotCloseTheConnection()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledge = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            received.SetResult();
            await acknowledge.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "x", 1));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = server.ClientOptions(),
                MaxConcurrentOperations = 2,
                MaxSubscriptions = 1,
            },
            TestToken
        );
        var first = subscriber.SubscribeAsync("x", TestToken);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var queued = subscriber.SubscribeAsync("queued", cancellation.Token);
        await Assert.ThrowsAsync<ValkeyCapacityException>(() => subscriber.SubscribeAsync("overload", TestToken));
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued);
        Assert.True(subscriber.IsConnected);
        acknowledge.SetResult();
        await first;
        await Assert.ThrowsAsync<ValkeyCapacityException>(() => subscriber.SubscribeAsync("x", TestToken));
        await subscriber.DisposeAsync();
        await server.Session;
        Assert.Equal(2, server.ReceivedCommands.Count);
    }

    [Fact]
    public async Task DisposalSettlesAcknowledgementsAndQueuedOperations()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            received.SetResult();
            await session.ReadCommandAsync();
        });
        var subscriber = await ConnectAsync(server, ValkeyProtocol.Resp3);
        var first = subscriber.SubscribeAsync("x", TestToken);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        var second = subscriber.SubscribeAsync("y", TestToken);
        await Task.WhenAll(subscriber.DisposeAsync().AsTask(), subscriber.DisposeAsync().AsTask())
            .WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => first);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => second);
        Assert.Null(subscriber.Failure);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task SubscriberParsingRetainsByteElementAndDepthBounds(int bound)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(
                bound switch
                {
                    0 => "$2048\r\n",
                    1 => ">17\r\n",
                    _ => ">1\r\n*1\r\n*1\r\n:0\r\n",
                }
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
                    MaxResponseBytes = 1024,
                    MaxResponseElements = 16,
                    MaxNestingDepth = 1,
                },
            },
            TestToken
        );
        await Assert.ThrowsAsync<ValkeyProtocolException>(() => subscriber.SubscribeAsync("x", TestToken));
        Assert.False(subscriber.IsConnected);
    }

    [Fact]
    public async Task TlsAuthenticationDatabaseAndNegotiatedDowngradeAreHonored()
    {
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        await using var server = FakeValkeyServer.Start(
            async session =>
            {
                Assert.Equal(
                    ["HELLO", "3", "AUTH", "user", "password", "SETNAME", "subscriber"],
                    await session.ExpectHandshakeAsync(FakeValkeyServer.HelloResp2)
                );
                Assert.Equal(["SELECT", "2"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
                await session.ReadCommandAsync();
                await session.SendRawAsync(Ack(ValkeyProtocol.Resp2, "subscribe", "x", 1));
                await session.ReadCommandAsync();
            },
            certificate
        );
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    UseTls = true,
                    Username = "user",
                    Password = "password",
                    ClientName = "subscriber",
                    Database = 2,
                    CertificateValidationCallback = (_, presented, _, errors) =>
                        presented?.GetCertHashString() == certificate.GetCertHashString()
                        && (errors & SslPolicyErrors.RemoteCertificateNameMismatch) == 0,
                },
            },
            TestToken
        );
        Assert.Equal(ValkeyProtocol.Resp2, subscriber.NegotiatedProtocol);
        await subscriber.SubscribeAsync("x", TestToken);
    }

    [Fact]
    public async Task TlsUsesPlatformValidationByDefault()
    {
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        await using var server = FakeValkeyServer.Start(session => session.ReadCommandAsync(), certificate);
        await Assert.ThrowsAsync<AuthenticationException>(() =>
            ValkeySubscriber.ConnectAsync(
                new ValkeySubscriberOptions
                {
                    Connection = new ValkeyClientOptions
                    {
                        Host = "127.0.0.1",
                        Port = server.Port,
                        UseTls = true,
                    },
                },
                TestToken
            )
        );
    }

    [Fact]
    public async Task InvalidBoundsAreRejectedBeforeConnecting()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(new ValkeySubscriberOptions { QueueCapacity = 0 }, TestToken)
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(new ValkeySubscriberOptions { MaxSubscriptions = 0 }, TestToken)
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(new ValkeySubscriberOptions { MaxChannelBytes = 0 }, TestToken)
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(new ValkeySubscriberOptions { MaxConcurrentOperations = 0 }, TestToken)
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ValkeySubscriber.ConnectAsync(new ValkeySubscriberOptions { OperationTimeout = TimeSpan.Zero }, TestToken)
        );
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task EmptyChannelsAndPayloadsRemainDistinctFromNullAfterRemoteClose(ValkeyProtocol protocol)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(["SUBSCRIBE", ""], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(protocol, "subscribe", "", 1));
            await session.SendRawAsync(Frame(protocol, "message", "", ""));
            session.Close();
        });
        await using var subscriber = await ConnectAsync(server, protocol);
        var handle = await subscriber.SubscribeAsync("", TestToken);
        await subscriber.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.Empty(messages.Current.Channel.ToArray());
        Assert.Empty(messages.Current.Payload.ToArray());
        Assert.Null(messages.Current.Pattern);
        await Assert.ThrowsAsync<ValkeyConnectionException>(() => messages.MoveNextAsync().AsTask());
        Assert.IsType<ValkeyConnectionException>(subscriber.Failure);
        await handle.DisposeAsync();
    }

    [Fact]
    public async Task SubscriptionNamesAreCopiedAndLocalValidationDoesNotSend()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledge = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["SUBSCRIBE", "x"], await session.ReadCommandAsync());
            received.SetResult();
            await acknowledge.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "subscribe", "x", 1));
            await session.SendRawAsync(Frame(ValkeyProtocol.Resp3, "message", "x", "value"));
            Assert.Equal(["UNSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "unsubscribe", "x", 0));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions { Connection = server.ClientOptions(), MaxChannelBytes = 1 },
            TestToken
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => subscriber.SubscribeAsync("too-long", TestToken));
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => subscriber.SubscribeAsync("y", canceled.Token));
        byte[] name = [(byte)'x'];
        var pending = subscriber.SubscribeAsync(name, TestToken);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        name[0] = (byte)'y';
        acknowledge.SetResult();
        var handle = await pending;
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal("x", Encoding.UTF8.GetString(messages.Current.Channel.Span));
        await handle.DisposeAsync();
    }

    [Fact]
    public async Task RejectedAuthenticationCannotEchoCredentials()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync("-WRONGPASS test-user secret-password\r\n");
        });
        var error = await Assert.ThrowsAsync<ValkeyServerException>(() =>
            ValkeySubscriber.ConnectAsync(
                new ValkeySubscriberOptions
                {
                    Connection = new ValkeyClientOptions
                    {
                        Host = "127.0.0.1",
                        Port = server.Port,
                        Username = "test-user",
                        Password = "secret-password",
                    },
                },
                TestToken
            )
        );
        Assert.Equal("WRONGPASS", error.ErrorCode);
        Assert.DoesNotContain("test-user", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("secret-password", error.ToString(), StringComparison.Ordinal);
    }

    private static ValkeyClientOptions Connection(FakeValkeyServer server, ValkeyProtocol protocol) =>
        new()
        {
            Host = "127.0.0.1",
            Port = server.Port,
            Protocol = protocol,
        };

    private static Task<ValkeySubscriber> ConnectAsync(FakeValkeyServer server, ValkeyProtocol protocol) =>
        ValkeySubscriber.ConnectAsync(
            new ValkeySubscriberOptions { Connection = Connection(server, protocol) },
            TestToken
        );

    private static string Hello(ValkeyProtocol protocol) =>
        protocol == ValkeyProtocol.Resp2 ? FakeValkeyServer.HelloResp2 : FakeValkeyServer.HelloResp3;

    private static byte[] Frame(ValkeyProtocol protocol, string kind, params ValkeyArgument[] items)
    {
        var bytes = RespWriter.Encode(new ValkeyCommand(kind, items));
        // Commands normalize their first element; Pub/Sub frame kinds are lowercase.
        var upper = Encoding.ASCII.GetBytes(kind.ToUpperInvariant());
        var start = bytes.AsSpan().IndexOf(upper);
        Encoding.ASCII.GetBytes(kind).CopyTo(bytes, start);
        if (protocol == ValkeyProtocol.Resp3)
        {
            bytes[0] = (byte)'>';
        }

        return bytes;
    }

    private static byte[] Ack(ValkeyProtocol protocol, string kind, ValkeyArgument name, int count)
    {
        var bytes = Frame(protocol, kind, name);
        bytes[1] = (byte)'3';
        return [.. bytes, .. Encoding.ASCII.GetBytes(":" + count.ToString(CultureInfo.InvariantCulture) + "\r\n")];
    }
}
