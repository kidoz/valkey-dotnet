using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests.Cluster;

public sealed partial class ValkeyClusterSubscriberTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task AskUsesDedicatedSocketAndDoesNotChangeSlotOwnership(ValkeyProtocol protocol)
    {
        byte[] channel = [123, 120, 125, 0, 255, 13, 10];
        var slot = ValkeyClusterClient.GetHashSlot(channel);
        await using var importing = FakeValkeyServer.StartMany(
            2,
            async (_, session) =>
            {
                var hello = await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Contains("ask-client", hello);
                Assert.Equal(["ASKING"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
                var command = await session.ReadBinaryCommandAsync();
                Assert.Equal("SSUBSCRIBE"u8.ToArray(), command[0]);
                Assert.Equal(channel, command[1]);
                await session.SendRawAsync([.. Ack(protocol, channel), .. Message(protocol, channel)]);
                Assert.Equal("SUNSUBSCRIBE", (await session.ReadCommandAsync())[0]);
                await session.SendRawAsync(Ack(protocol, channel, "sunsubscribe", 0));
                await session.ReadCommandAsync();
            }
        );
        await using var origin = FakeValkeyServer.StartMany(
            2,
            async (_, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal("SSUBSCRIBE", (await session.ReadCommandAsync())[0]);
                await session.SendAsync($"-ASK {slot} importing.invalid:{importing.Port}\r\n");
                await session.ReadCommandAsync();
            }
        );
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(Topology(origin.Port));
            await session.ReadCommandAsync();
        });
        var mappedAsks = 0;
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
                            ClientName = "ask-client",
                        },
                    ],
                    MaxRedirects = 1,
                    EndpointMapper = endpoint =>
                    {
                        if (endpoint.Host == "importing.invalid")
                        {
                            mappedAsks++;
                        }
                        return new ValkeyClusterEndpoint("127.0.0.1", endpoint.Port);
                    },
                },
                MaxSubscriptions = 1,
            },
            TestToken
        );
        for (var attempt = 0; attempt < 2; attempt++)
        {
            await using var handle = await subscriber.SubscribeAsync(channel, TestToken);
            await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
            Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
            Assert.Equal(channel, messages.Current.Channel.ToArray());
            Assert.True(messages.Current.IsSharded);
            await handle.UnsubscribeAsync(TestToken);
            Assert.Equal(0, subscriber.SubscriptionCount);
        }
        Assert.Equal(2, mappedAsks);
        await subscriber.DisposeAsync();
        await Task.WhenAll(origin.Session, importing.Session, seed.Session)
            .WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(2, seed.ReceivedCommands.Count);
    }

    [Theory]
    [InlineData("ASK 0 private.invalid:6379")]
    [InlineData("ASK 16287 private.invalid:0")]
    [InlineData("ASK 16287 private.invalid:65536")]
    [InlineData("ASK 16287 secret@private.invalid:6379")]
    [InlineData("ASK 16287 private.invalid:6379 extra-secret")]
    [InlineData("ASK 16384 private.invalid:6379")]
    [InlineData("ASK -1 private.invalid:6379")]
    [InlineData("ASK 16287 private.invalid:no-port")]
    [InlineData("ASK")]
    [InlineData("ASK 16287 [[::1]:6379")]
    [InlineData("ASK 16287 [private.invalid]:6379")]
    public async Task InvalidAskNeverConnectsOrLeaksRedirectText(string redirect)
    {
        await using var origin = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("-" + redirect + "\r\n");
            await session.ReadCommandAsync();
        });
        await using var seed = StartSeed(origin.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(Options(seed.Port), TestToken);
        var error = await Assert.ThrowsAsync<ValkeyClusterException>(() => subscriber.SubscribeAsync("x", TestToken));
        Assert.DoesNotContain("private.invalid", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("secret", error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, subscriber.SubscriptionCount);
    }

    [Theory]
    [InlineData("-NOPERM private-secret\r\n", true)]
    [InlineData("+NOT-OK\r\n", false)]
    [InlineData(":1\r\n", false)]
    public async Task AskingMustSucceedBeforeSendingSubscription(string reply, bool serverError)
    {
        await using var importing = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["ASKING"], await session.ReadCommandAsync());
            await session.SendAsync(reply);
            await session.ReadCommandAsync();
        });
        await using var origin = StartAskOrigin(importing.Port);
        await using var seed = StartSeed(origin.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(Options(seed.Port), TestToken);
        if (serverError)
        {
            var error = await Assert.ThrowsAsync<ValkeyServerException>(() =>
                subscriber.SubscribeAsync("x", TestToken)
            );
            Assert.Equal("NOPERM", error.ErrorCode);
            Assert.DoesNotContain("private-secret", error.ToString(), StringComparison.Ordinal);
        }
        else
        {
            await Assert.ThrowsAsync<ValkeyProtocolException>(() => subscriber.SubscribeAsync("x", TestToken));
        }
        await importing.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(2, importing.ReceivedCommands.Count);
        Assert.Equal(0, subscriber.SubscriptionCount);
    }

    [Fact]
    public async Task AskRedirectLoopUsesTheSharedRedirectBound()
    {
        var port = 0;
        await using var origin = FakeValkeyServer.StartMany(
            3,
            async (attempt, session) =>
            {
                await session.ExpectHandshakeAsync();
                if (attempt != 0)
                {
                    Assert.Equal(["ASKING"], await session.ReadCommandAsync());
                    await session.SendAsync("+OK\r\n");
                }
                Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
                await session.SendAsync($"-ASK 16287 127.0.0.1:{port}\r\n");
                await session.ReadCommandAsync();
            }
        );
        port = origin.Port;
        await using var seed = StartSeed(port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()], MaxRedirects = 2 },
            },
            TestToken
        );
        await Assert.ThrowsAsync<ValkeyClusterException>(() => subscriber.SubscribeAsync("x", TestToken));
        await origin.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(8, origin.ReceivedCommands.Count);
        Assert.Equal(0, subscriber.SubscriptionCount);
    }

    [Fact]
    public async Task MovedAfterAskRefreshesAndClearsTheAskingState()
    {
        await using var final = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
            await session.ReadCommandAsync();
        });
        await using var importing = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["ASKING"], await session.ReadCommandAsync());
            await session.SendAsync("+OK\r\n");
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendAsync("-MOVED 16287 ignored.invalid:6379\r\n");
            await session.ReadCommandAsync();
        });
        await using var origin = StartAskOrigin(importing.Port);
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(Topology(origin.Port));
            Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
            await session.SendAsync(Topology(final.Port));
            await session.ReadCommandAsync();
        });
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(Options(seed.Port), TestToken);
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        Assert.True(handle.IsConnected);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task PendingAskingHonorsDeadlineCancellationAndDisposal(int stop)
    {
        var asking = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var importing = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["ASKING"], await session.ReadCommandAsync());
            asking.SetResult();
            await session.ReadCommandAsync();
        });
        await using var origin = StartAskOrigin(importing.Port);
        await using var seed = StartSeed(origin.Port);
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = new ValkeyClusterOptions { SeedNodes = [seed.ClientOptions()] },
                OperationTimeout = stop == 0 ? TimeSpan.FromMilliseconds(500) : TimeSpan.FromSeconds(30),
            },
            TestToken
        );
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var pending = subscriber.SubscribeAsync("x", cancellation.Token);
        await asking.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        if (stop == 0)
        {
            var error = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() =>
                pending.WaitAsync(TimeSpan.FromSeconds(5), TestToken)
            );
            Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, error.DeliveryStatus);
        }
        else if (stop == 1)
        {
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                pending.WaitAsync(TimeSpan.FromSeconds(5), TestToken)
            );
        }
        else
        {
            await subscriber.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken);
            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                pending.WaitAsync(TimeSpan.FromSeconds(5), TestToken)
            );
        }
        await importing.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(2, importing.ReceivedCommands.Count);
        Assert.Equal(0, subscriber.SubscriptionCount);
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task SameEndpointRecoveryRepeatsAskingBeforeRestoring(ValkeyProtocol protocol)
    {
        var disconnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var importing = FakeValkeyServer.StartMany(
            2,
            async (attempt, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal(["ASKING"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
                Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(protocol, "x"u8.ToArray()));
                if (attempt == 0)
                {
                    await disconnect.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
                    session.Close();
                }
                else
                {
                    await session.SendRawAsync(Message(protocol, "x"u8.ToArray()));
                    await session.ReadCommandAsync();
                }
            }
        );
        await using var origin = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendAsync($"-ASK 16287 127.0.0.1:{importing.Port}\r\n");
            await session.ReadCommandAsync();
        });
        await using var seed = StartSeed(origin.Port);
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
                },
                EnableReconnect = true,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(1),
            },
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        disconnect.SetResult();
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal(1, handle.SuccessfulReconnects);
        await handle.DisposeAsync();
        await importing.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        Assert.Equal(6, importing.ReceivedCommands.Count);
    }

    [Fact]
    public async Task AskPreservesTlsAclAndClientName()
    {
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        string[] expectedHello = ["HELLO", "3", "AUTH", "test-user", "test-password", "SETNAME", "ask-tls"];
        await using var importing = FakeValkeyServer.Start(
            async session =>
            {
                Assert.Equal(expectedHello, await session.ExpectHandshakeAsync());
                Assert.Equal(["ASKING"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
                Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
                await session.SendRawAsync(Ack(ValkeyProtocol.Resp3, "x"u8.ToArray()));
                await session.ReadCommandAsync();
            },
            certificate
        );
        await using var origin = FakeValkeyServer.Start(
            async session =>
            {
                Assert.Equal(expectedHello, await session.ExpectHandshakeAsync());
                await session.ReadCommandAsync();
                await session.SendAsync($"-ASK 16287 127.0.0.1:{importing.Port}\r\n");
                await session.ReadCommandAsync();
            },
            certificate
        );
        await using var seed = FakeValkeyServer.Start(
            async session =>
            {
                Assert.Equal(expectedHello, await session.ExpectHandshakeAsync());
                await session.ReadCommandAsync();
                await session.SendAsync(Topology(origin.Port));
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
                            UseTls = true,
                            Username = "test-user",
                            Password = "test-password",
                            ClientName = "ask-tls",
                            CertificateValidationCallback = (_, presented, _, _) =>
                                presented?.GetCertHashString() == certificate.Thumbprint,
                        },
                    ],
                },
            },
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        Assert.True(handle.IsConnected);
    }

    [Theory]
    [InlineData("ASK 16287 [::1]:6379", "::1")]
    [InlineData("ASK 16287 ::1:6379", "::1")]
    [InlineData("ASK 16287 :6379", "")]
    [InlineData("ASK 16287 node.example:6379", "node.example")]
    public void AskParserAcceptsSupportedEndpointForms(string text, string host)
    {
        var redirect = ValkeyDotNet.Cluster.ShardSubscriptionRedirectException.Parse(
            System.Text.Encoding.ASCII.GetBytes(text)
        );
        Assert.Equal(16287, redirect.Slot);
        Assert.Equal(host, redirect.Host);
        Assert.Equal(6379, redirect.Port);
        Assert.DoesNotContain(text, redirect.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(10)]
    [InlineData(127)]
    [InlineData(255)]
    public void AskParserRejectsNonAsciiAndControlBytes(byte value)
    {
        byte[] text = [.. "ASK 16287 private"u8, value, .. ".invalid:6379"u8];
        var error = Assert.Throws<ValkeyClusterException>(() =>
            ValkeyDotNet.Cluster.ShardSubscriptionRedirectException.Parse(text)
        );
        Assert.DoesNotContain("private", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AskParserRejectsOversizedRedirectBeforeDecoding()
    {
        var bytes = new byte[1025];
        Array.Fill(bytes, (byte)'a');
        "ASK "u8.CopyTo(bytes);
        Assert.Throws<ValkeyClusterException>(() =>
            ValkeyDotNet.Cluster.ShardSubscriptionRedirectException.Parse(bytes)
        );
    }

    private static FakeValkeyServer StartAskOrigin(int port) =>
        FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendAsync($"-ASK 16287 127.0.0.1:{port}\r\n");
            await session.ReadCommandAsync();
        });
}
