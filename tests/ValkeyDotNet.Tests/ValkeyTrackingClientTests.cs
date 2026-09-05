using System.Text;
using ValkeyDotNet.Protocol;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

public sealed class ValkeyTrackingClientTests
{
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Fact]
    public void RejectsResp2AndInvalidConfigurationBeforeConnecting()
    {
        Assert.Throws<ArgumentException>(() =>
            new ValkeyTrackingClient(
                new ValkeyConnectionOwnerOptions
                {
                    Connection = new ValkeyClientOptions { Protocol = ValkeyProtocol.Resp2 },
                }
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new TrackingSession(new ValkeyTrackingOptions { QueueCapacity = 0 })
        );
        Assert.Throws<ArgumentException>(() => new TrackingSession(new ValkeyTrackingOptions { Prefixes = ["a"] }));
        Assert.Throws<ArgumentException>(() =>
            new TrackingSession(new ValkeyTrackingOptions { Broadcast = true, Prefixes = ["a", "ab"] })
        );
        Assert.Throws<ArgumentException>(() =>
            new TrackingSession(new ValkeyTrackingOptions { Broadcast = true, Prefixes = ["", "b"] })
        );
        Assert.Throws<ArgumentException>(() =>
            new TrackingSession(new ValkeyTrackingOptions { Broadcast = true, Prefixes = [new byte[1024 * 1024 + 1]] })
        );
        Assert.Throws<ArgumentException>(() =>
            new TrackingSession(new ValkeyTrackingOptions { Broadcast = true, Prefixes = new ValkeyArgument[257] })
        );
    }

    [Fact]
    public async Task SnapshotsBinaryPrefixesAndEnablesTrackingBeforeCommands()
    {
        byte[] prefix = [0, 255, 13, 10];
        byte[] expected = [.. prefix];
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            var command = await session.ReadBinaryCommandAsync();
            Assert.Equal(
                ["CLIENT", "TRACKING", "ON", "NOLOOP", "BCAST", "PREFIX"],
                command[..^1].Select(Encoding.UTF8.GetString)
            );
            Assert.Equal(expected, command[^1]);
            await session.SendAsync("+OK\r\n");
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
            await session.ReadCommandAsync();
        });
        await using var client = Create(
            server,
            new ValkeyTrackingOptions
            {
                Broadcast = true,
                NoLoop = true,
                Prefixes = [prefix],
            }
        );
        prefix[0] = 99;
        Assert.Equal("PONG", (await client.ExecuteAsync(new ValkeyCommand("PING"), TestToken)).AsString());
    }

    [Fact]
    public async Task InterleavedBinaryInvalidationsDoNotConsumePipelineReplies()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await EnableAsync(session);
            Assert.Equal(["GET", "a"], await session.ReadCommandAsync());
            Assert.Equal(["GET", "b"], await session.ReadCommandAsync());
            await session.SendAsync("$1\r\nx\r\n>2\r\n$10\r\ninvalidate\r\n*2\r\n$0\r\n\r\n$4\r\n");
            foreach (var value in new byte[] { 0, 255, 13, 10, 13, 10 })
            {
                await session.SendRawAsync([value]);
            }
            await session.SendAsync(">2\r\n+other\r\n:1\r\n-ERR expected\r\n");
            await session.ReadCommandAsync();
        });
        await using var client = Create(server);
        var replies = await client.ExecutePipelineAsync(
            [new ValkeyCommand("GET", "a"), new ValkeyCommand("GET", "b")],
            TestToken
        );
        Assert.Equal("x", replies[0].AsString());
        Assert.Equal(RespType.SimpleError, replies[1].Type);
        await using var messages = client.ReadInvalidationsAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.False(messages.Current.InvalidateAll);
        Assert.Empty(messages.Current.Keys[0].ToArray());
        Assert.Equal(new byte[] { 0, 255, 13, 10 }, messages.Current.Keys[1].ToArray());
        Assert.Equal(1, client.InvalidationVersion);
    }

    [Theory]
    [InlineData("_\r\n", true)]
    [InlineData("*0\r\n", false)]
    public async Task DistinguishesFlushFromEmptyKeyBatch(string payload, bool all)
    {
        var tracking = new TrackingSession(new ValkeyTrackingOptions());
        tracking.OnPush(await ParseAsync(">2\r\n+invalidate\r\n" + payload));
        await using var messages = tracking.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(all, messages.Current.InvalidateAll);
        Assert.Empty(messages.Current.Keys);
    }

    [Fact]
    public async Task OverflowAndRepeatedResetNeverLoseInvalidateAll()
    {
        var tracking = new TrackingSession(new ValkeyTrackingOptions { QueueCapacity = 1 });
        var push = await ParseAsync(">2\r\n+invalidate\r\n*1\r\n$1\r\nk\r\n");
        tracking.OnPush(push);
        tracking.OnPush(push);
        tracking.OnPush(push);
        Assert.Equal(2, tracking.QueueOverflows);
        await using var messages = tracking.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.True(messages.Current.InvalidateAll);
        Assert.Equal(3, messages.Current.Version);
        tracking.OnPush(push);
        Assert.True(await messages.MoveNextAsync());
        Assert.False(messages.Current.InvalidateAll);
        tracking.Complete();
        tracking.Complete();
        Assert.True(await messages.MoveNextAsync());
        Assert.True(messages.Current.InvalidateAll);
        Assert.False(await messages.MoveNextAsync());
    }

    [Theory]
    [InlineData(">0\r\n")]
    [InlineData(">1\r\n:1\r\n")]
    [InlineData(">1\r\n+invalidate\r\n")]
    [InlineData(">3\r\n+invalidate\r\n_\r\n:1\r\n")]
    [InlineData(">2\r\n+invalidate\r\n$1\r\nx\r\n")]
    [InlineData(">2\r\n+invalidate\r\n*1\r\n_\r\n")]
    [InlineData(">2\r\n+invalidate\r\n*1\r\n:1\r\n")]
    public async Task MalformedPushFailsPendingCommandsAndInvalidatesEverything(string frame)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await EnableAsync(session);
            await session.ReadCommandAsync();
            await session.SendAsync(frame + "+PONG\r\n");
            await session.ReadCommandAsync();
        });
        await using var client = Create(server);
        await Assert.ThrowsAsync<ValkeyProtocolException>(() =>
            client.ExecuteAsync(new ValkeyCommand("PING"), TestToken)
        );
        await using var messages = client.ReadInvalidationsAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.True(messages.Current.InvalidateAll);
        Assert.Equal(ValkeyConnectionState.Disconnected, client.State);
    }

    [Fact]
    public async Task RestoresTrackingAndSessionSettingsWithoutReplayingFailedWrite()
    {
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                Assert.Equal(["HELLO", "3", "SETNAME", "tracked"], await session.ExpectHandshakeAsync());
                Assert.Equal(["SELECT", "1"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
                Assert.Equal(
                    ["CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "item:"],
                    await session.ReadCommandAsync()
                );
                await session.SendAsync("+OK\r\n");
                Assert.Equal(
                    index == 0 ? (string[])["INCR", "ambiguous"] : ["GET", "item:1"],
                    await session.ReadCommandAsync()
                );
                if (index == 0)
                {
                    session.Close();
                    return;
                }
                await session.SendAsync(">2\r\n+invalidate\r\n*1\r\n$6\r\nitem:1\r\n$1\r\nv\r\n");
                await session.ReadCommandAsync();
            }
        );
        await using var client = new ValkeyTrackingClient(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    Database = 1,
                    ClientName = "tracked",
                },
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(2),
            },
            new ValkeyTrackingOptions { Broadcast = true, Prefixes = ["item:"] }
        );
        var failure = await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            client.ExecuteAsync(new ValkeyCommand("INCR", "ambiguous"), TestToken)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
        await using var messages = client.ReadInvalidationsAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.True(messages.Current.InvalidateAll);
        Assert.Equal("v", (await client.ExecuteAsync(new ValkeyCommand("GET", "item:1"), TestToken)).AsString());
        Assert.True(await messages.MoveNextAsync());
        Assert.False(messages.Current.InvalidateAll);
        Assert.Equal("item:1"u8.ToArray(), messages.Current.Keys[0].ToArray());
    }

    [Theory]
    [InlineData("-NOPERM private-prefix\r\n", "NOPERM")]
    [InlineData("-ERR private-prefix\r\n", "ERR")]
    public async Task TrackingRejectionIsSanitizedAndTerminal(string reply, string code)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(reply);
        });
        await using var client = Create(server);
        var error = await Assert.ThrowsAsync<ValkeyServerException>(() => client.ConnectAsync(TestToken));
        Assert.Equal(code, error.ErrorCode);
        Assert.DoesNotContain("private-prefix", error.ToString(), StringComparison.Ordinal);
        Assert.Equal(ValkeyConnectionState.Faulted, client.State);
        Assert.Same(error, await Assert.ThrowsAsync<ValkeyServerException>(() => client.ConnectAsync(TestToken)));
    }

    [Theory]
    [InlineData(":1\r\n")]
    [InlineData("+not-OK\r\n")]
    public async Task RejectsMalformedTrackingAcknowledgement(string reply)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(reply);
        });
        await using var client = Create(server);
        await Assert.ThrowsAsync<ValkeyProtocolException>(() => client.ConnectAsync(TestToken));
        Assert.Equal(ValkeyConnectionState.Faulted, client.State);
    }

    [Fact]
    public async Task RejectsNegotiatedDowngradeBeforeSendingTracking()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(FakeValkeyServer.HelloResp2);
            await session.ReadCommandAsync();
        });
        await using var client = Create(server);
        await Assert.ThrowsAsync<ValkeyProtocolException>(() => client.ConnectAsync(TestToken));
        await server.Session;
        Assert.Single(server.ReceivedCommands);
    }

    [Fact]
    public async Task RejectsTrackingMutationInWholePipelineBeforeWriting()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await EnableAsync(session);
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
            await session.ReadCommandAsync();
        });
        await using var client = Create(server);
        foreach (
            var command in new[]
            {
                new ValkeyCommand("CLIENT", "tracking", "OFF"),
                new ValkeyCommand("CLIENT", "caching", "NO"),
                new ValkeyCommand("SELECT", "2"),
                new ValkeyCommand("AUTH", "redacted"),
            }
        )
        {
            await Assert.ThrowsAsync<ValkeyUnsupportedCommandException>(() =>
                client.ExecutePipelineAsync([new ValkeyCommand("INCR", "never-sent"), command], TestToken)
            );
        }
        Assert.Equal("PONG", (await client.ExecuteAsync(new ValkeyCommand("PING"), TestToken)).AsString());
    }

    [Fact]
    public async Task EnumerationCancellationDoesNotStopTrackingAndSecondReaderIsRejected()
    {
        var tracking = new TrackingSession(new ValkeyTrackingOptions());
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        await using var first = tracking.ReadAllAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);
        var pending = first.MoveNextAsync().AsTask();
        await using var second = tracking.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        await Assert.ThrowsAsync<InvalidOperationException>(() => second.MoveNextAsync().AsTask());
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        tracking.InvalidateAll();
        await using var replacement = tracking.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await replacement.MoveNextAsync());
        Assert.True(replacement.Current.InvalidateAll);
    }

    [Theory]
    [InlineData(">2\r\n+invalidate\r\n*1\r\n$1024\r\n")]
    [InlineData(">2\r\n+invalidate\r\n*100\r\n")]
    [InlineData(">2\r\n+invalidate\r\n*1\r\n*1\r\n*1\r\n")]
    public async Task ReplacementTrackingRetainsParserBounds(string frame)
    {
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await EnableAsync(session);
                await session.ReadCommandAsync();
                if (index == 0)
                {
                    session.Close();
                    return;
                }
                await session.SendAsync(frame);
                await session.ReadCommandAsync();
            }
        );
        await using var client = new ValkeyTrackingClient(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    MaxResponseBytes = 1024,
                    MaxResponseElements = 16,
                    MaxNestingDepth = 2,
                },
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(2),
            }
        );
        await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            client.ExecuteAsync(new ValkeyCommand("PING"), TestToken)
        );
        await Assert.ThrowsAsync<ValkeyProtocolException>(() =>
            client.ExecuteAsync(new ValkeyCommand("PING"), TestToken)
        );
    }

    [Fact]
    public async Task DeadlineDrainsLateReplyAndKeepsInvalidationDeliveryAlive()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await EnableAsync(session);
            Assert.Equal(["GET", "slow"], await session.ReadCommandAsync());
            received.SetResult();
            await release.Task.WaitAsync(TestToken);
            await session.SendAsync(">2\r\n+invalidate\r\n_\r\n$3\r\nold\r\n");
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
            await session.ReadCommandAsync();
        });
        await using var client = Create(server);
        await client.ConnectAsync(TestToken);
        try
        {
            var slow = client.ExecuteWithDeadlineAsync(
                new ValkeyCommand("GET", "slow"),
                TimeSpan.FromMilliseconds(200),
                TestToken
            );
            await received.Task.WaitAsync(TestToken);
            var error = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() => slow);
            Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
            var ping = client.ExecuteAsync(new ValkeyCommand("PING"), TestToken);
            release.SetResult();
            Assert.Equal("PONG", (await ping).AsString());
            await using var messages = client.ReadInvalidationsAsync(TestToken).GetAsyncEnumerator(TestToken);
            Assert.True(await messages.MoveNextAsync());
            Assert.True(messages.Current.InvalidateAll);
            Assert.Equal(ValkeyConnectionState.Connected, client.State);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task DisposalDuringTrackingAcknowledgementSettlesConnectAndCompletesStream()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            received.SetResult();
            await session.ReadCommandAsync();
        });
        await using var client = Create(server);
        var connecting = client.ConnectAsync(TestToken);
        await received.Task.WaitAsync(TestToken);
        await Task.WhenAll(client.DisposeAsync().AsTask(), client.DisposeAsync().AsTask());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => connecting);
        await using var messages = client.ReadInvalidationsAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.True(messages.Current.InvalidateAll);
        Assert.False(await messages.MoveNextAsync());
        Assert.Equal(ValkeyConnectionState.Disposed, client.State);
    }

    [Fact]
    public async Task CancellingWrittenCommandResetsTrackingWithoutReplayingIt()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await EnableAsync(session);
            await session.ReadCommandAsync();
            received.SetResult();
            await session.ReadCommandAsync();
        });
        await using var client = Create(server);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var command = client.ExecuteAsync(new ValkeyCommand("INCR", "ambiguous"), cancellation.Token);
        await received.Task.WaitAsync(TestToken);
        await cancellation.CancelAsync();
        var error = await Assert.ThrowsAsync<ValkeyCommandCanceledException>(() => command);
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        await using var messages = client.ReadInvalidationsAsync(TestToken).GetAsyncEnumerator(TestToken);
        Assert.True(await messages.MoveNextAsync());
        Assert.True(messages.Current.InvalidateAll);
    }

    [Fact]
    public async Task ReplacementReusesTlsAndAclBeforeEnablingTracking()
    {
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        var validations = 0;
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                Assert.Equal(
                    ["HELLO", "3", "AUTH", "tracking-user", "tracking-test-secret"],
                    await session.ExpectHandshakeAsync()
                );
                Assert.Equal(["CLIENT", "TRACKING", "ON"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
                await session.ReadCommandAsync();
                if (index == 0)
                {
                    session.Close();
                    return;
                }
                await session.SendAsync("+PONG\r\n");
                await session.ReadCommandAsync();
            },
            certificate
        );
        await using var client = new ValkeyTrackingClient(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    UseTls = true,
                    Username = "tracking-user",
                    Password = "tracking-test-secret",
                    CertificateValidationCallback = (_, presented, _, _) =>
                    {
                        Interlocked.Increment(ref validations);
                        return presented?.GetCertHashString() == certificate.Thumbprint;
                    },
                },
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(2),
            }
        );
        await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            client.ExecuteAsync(new ValkeyCommand("PING"), TestToken)
        );
        Assert.Equal("PONG", (await client.ExecuteAsync(new ValkeyCommand("PING"), TestToken)).AsString());
        Assert.Equal(2, validations);
    }

    [Fact]
    public async Task HandshakeRejectionDoesNotExposeEchoedCredentials()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ReadCommandAsync();
            await session.SendAsync("-WRONGPASS tracking-test-secret\r\n");
        });
        await using var client = new ValkeyTrackingClient(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    Password = "tracking-test-secret",
                },
            }
        );
        var error = await Assert.ThrowsAsync<ValkeyServerException>(() => client.ConnectAsync(TestToken));
        Assert.Equal("WRONGPASS", error.ErrorCode);
        Assert.DoesNotContain("tracking-test-secret", error.ToString(), StringComparison.Ordinal);
        Assert.Equal(ValkeyConnectionState.Faulted, client.State);
    }

    private static ValkeyTrackingClient Create(FakeValkeyServer server, ValkeyTrackingOptions? options = null) =>
        new(new ValkeyConnectionOwnerOptions { Connection = server.ClientOptions() }, options);

    private static async Task EnableAsync(FakeValkeySession session)
    {
        await session.ExpectHandshakeAsync();
        Assert.Equal(["CLIENT", "TRACKING", "ON"], await session.ReadCommandAsync());
        await session.SendAsync("+OK\r\n");
    }

    private static async Task<RespValue> ParseAsync(string frame)
    {
        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(frame));
        return await new RespReader(stream, 1024, 32, 8).ReadAsync(TestToken);
    }
}
