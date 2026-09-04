using System.Globalization;
using System.Security.Authentication;
using System.Security.Cryptography;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

/// <summary>
/// Deterministic client behaviour, driven byte-for-byte by a loopback server. No live Valkey needed:
/// these cover the paths a real server will not reproduce on demand — truncation, an abrupt close,
/// cancellation mid-read, and a hostile frame.
/// </summary>
public sealed class ValkeyClientTests
{
    [Fact]
    public async Task ConnectAsyncCompletesTheHandshake()
    {
        await using var server = FakeValkeyServer.Start(session => session.ExpectHandshakeAsync());
        var options = server.ClientOptions();

        await using var client = await ValkeyClient.ConnectAsync(
            new ValkeyClientOptions
            {
                Host = options.Host,
                Port = options.Port,
                ClientName = "unit-tests",
            },
            TestContext.Current.CancellationToken
        );

        Assert.Equal(ValkeyProtocol.Resp3, client.NegotiatedProtocol);
        Assert.Equal(RespType.Map, client.ServerInfo.Type);
        await server.Session;
        Assert.Equal(["HELLO", "3", "SETNAME", "unit-tests"], server.ReceivedCommands[0]);
    }

    [Fact]
    public async Task ConnectAsyncReportsTheProtocolTheServerChose()
    {
        // RESP3 was requested; the server answered in RESP2. Reporting Resp3 here would be a lie the
        // caller cannot detect.
        await using var server = FakeValkeyServer.Start(session =>
            session.ExpectHandshakeAsync(FakeValkeyServer.HelloResp2)
        );

        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(ValkeyProtocol.Resp2, client.NegotiatedProtocol);
    }

    [Fact]
    public async Task ConnectAsyncRejectsAHelloReplyWithoutAProtocol()
    {
        await using var server = FakeValkeyServer.Start(session =>
            session.ExpectHandshakeAsync("%1\r\n$6\r\nserver\r\n$6\r\nvalkey\r\n")
        );

        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await ValkeyClient.ConnectAsync(server.ClientOptions(), TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ConnectAsyncSendsCredentialsInTheHandshake()
    {
        await using var server = FakeValkeyServer.Start(session => session.ExpectHandshakeAsync());
        var options = new ValkeyClientOptions
        {
            Host = "127.0.0.1",
            Port = server.Port,
            Username = "app",
            Password = "s3cret",
            ClientName = "auth-tests",
        };

        await using (await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken))
        {
            await server.Session;
        }

        Assert.Equal(["HELLO", "3", "AUTH", "app", "s3cret", "SETNAME", "auth-tests"], server.ReceivedCommands[0]);
    }

    [Fact]
    public async Task ConnectAsyncAuthenticatesAsDefaultWhenOnlyAPasswordIsGiven()
    {
        await using var server = FakeValkeyServer.Start(session => session.ExpectHandshakeAsync());
        var options = new ValkeyClientOptions
        {
            Host = "127.0.0.1",
            Port = server.Port,
            Password = "s3cret",
        };

        await using (await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken))
        {
            await server.Session;
        }

        Assert.Equal(["HELLO", "3", "AUTH", "default", "s3cret"], server.ReceivedCommands[0]);
    }

    [Fact]
    public async Task ARejectedHandshakeDoesNotLeakCredentials()
    {
        await using var server = FakeValkeyServer.Start(session =>
            session.ExpectHandshakeAsync("-WRONGPASS invalid username-password pair or user is disabled\r\n")
        );
        var options = new ValkeyClientOptions
        {
            Host = "127.0.0.1",
            Port = server.Port,
            Username = "acl-user-x7",
            Password = "s3cret-x7",
        };

        var failure = await Assert.ThrowsAsync<ValkeyServerException>(async () =>
            await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken)
        );

        // The message is the library's; the stack trace is not, so assert on what this code controls.
        Assert.Equal("WRONGPASS", failure.ErrorCode);
        Assert.DoesNotContain("s3cret-x7", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("acl-user-x7", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ConnectAsyncPropagatesAHandshakeError()
    {
        await using var server = FakeValkeyServer.Start(session =>
            session.ExpectHandshakeAsync("-NOAUTH Authentication required\r\n")
        );

        var error = await Assert.ThrowsAsync<ValkeyServerException>(async () =>
            await ValkeyClient.ConnectAsync(server.ClientOptions(), TestContext.Current.CancellationToken)
        );

        Assert.Equal("NOAUTH", error.ErrorCode);
    }

    [Fact]
    public async Task ConnectAsyncTimesOutWhenTheServerNeverAnswers()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ReadCommandAsync();
            await session.ReadCommandAsync();
        });
        var options = new ValkeyClientOptions
        {
            Host = "127.0.0.1",
            Port = server.Port,
            ConnectTimeout = TimeSpan.FromMilliseconds(300),
        };

        await Assert.ThrowsAsync<TimeoutException>(async () =>
            await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ConnectAsyncValidatesOptionsBeforeTouchingTheNetwork()
    {
        // A timeout the timer cannot schedule used to reach CancelAfter, which threw from outside
        // the path that disposes the half-built socket.
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await ValkeyClient.ConnectAsync(
                new ValkeyClientOptions { ConnectTimeout = TimeSpan.MaxValue },
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await ValkeyClient.ConnectAsync(
                new ValkeyClientOptions { Protocol = (ValkeyProtocol)7 },
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task ConnectAsyncSelectsTheConfiguredDatabase()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("+OK\r\n");
        });
        var options = new ValkeyClientOptions
        {
            Host = "127.0.0.1",
            Port = server.Port,
            Database = 3,
        };

        await using (await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken))
        {
            await server.Session;
        }

        Assert.Equal(["SELECT", "3"], server.ReceivedCommands[1]);
    }

    [Fact]
    public async Task ExecuteAsyncDeliversPushFramesAndStillReturnsTheReply()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(">2\r\n+invalidate\r\n$3\r\nkey\r\n+PONG\r\n");
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        var pushes = new List<RespValue>();
        client.PushReceived += pushes.Add;

        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
        Assert.Equal("invalidate", Assert.Single(pushes).AsArray()[0].AsString());
    }

    [Fact]
    public async Task AFailingPushHandlerDoesNotDesynchronizeTheConnection()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(">1\r\n+invalidate\r\n+PONG\r\n");
            await session.ReadCommandAsync();
            await session.SendAsync("+PONG\r\n");
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        client.PushReceived += _ => throw new InvalidOperationException("handler bug");

        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task PushFramesAreDeliveredWhileTheConnectionIsIdle()
    {
        var sendPush = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseServer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await sendPush.Task;
            await session.SendAsync(">1\r\n+idle-push\r\n");
            await releaseServer.Task;
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        var received = new TaskCompletionSource<RespValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        client.PushReceived += received.SetResult;

        sendPush.SetResult();
        try
        {
            var push = await received.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
            Assert.Equal("idle-push", push.AsArray()[0].AsString());
        }
        finally
        {
            releaseServer.TrySetResult();
        }
    }

    [Theory]
    [InlineData("SUBSCRIBE")]
    [InlineData("UNSUBSCRIBE")]
    [InlineData("PSUBSCRIBE")]
    [InlineData("SSUBSCRIBE")]
    [InlineData("MONITOR")]
    [InlineData("RESET")]
    [InlineData("HELLO")]
    public async Task ExecuteAsyncRejectsConnectionStateCommandsBeforeWriting(string name)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        var rejection = await Assert.ThrowsAsync<ValkeyUnsupportedCommandException>(async () =>
            await client.ExecuteAsync(new ValkeyCommand(name, "channel"), TestContext.Current.CancellationToken)
        );

        Assert.Equal(name, rejection.Command);
        // The connection is untouched, so it still works for anything supported.
        await client.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands);
    }

    [Fact]
    public async Task ExecuteAsyncRejectsClientReplyButNotOtherClientSubcommands()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("+OK\r\n");
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        var rejection = await Assert.ThrowsAsync<ValkeyUnsupportedCommandException>(async () =>
            await client.ExecuteAsync(
                new ValkeyCommand("CLIENT", "reply", "off"),
                TestContext.Current.CancellationToken
            )
        );
        Assert.Equal("CLIENT REPLY", rejection.Command);

        var setname = await client.ExecuteAsync(
            new ValkeyCommand("CLIENT", "SETNAME", "still-fine"),
            TestContext.Current.CancellationToken
        );
        Assert.Equal("OK", setname.AsString());
    }

    [Fact]
    public async Task ExecutePipelineAsyncRejectsConnectionStateCommandsBeforeWriting()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        await Assert.ThrowsAsync<ValkeyUnsupportedCommandException>(async () =>
            await client.ExecutePipelineAsync(
                [new ValkeyCommand("GET", "a"), new ValkeyCommand("SUBSCRIBE", "news")],
                TestContext.Current.CancellationToken
            )
        );

        // Rejection happens before the batch is written, so the earlier command never reached the wire.
        await client.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands);
    }

    [Fact]
    public async Task ExecutePipelineAsyncReturnsErrorsInPlace()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("+OK\r\n-ERR broken\r\n");
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        var replies = await client.ExecutePipelineAsync(
            [new ValkeyCommand("SET", "a", "1"), new ValkeyCommand("INCR", "a")],
            TestContext.Current.CancellationToken
        );

        Assert.Equal("OK", replies[0].AsString());
        Assert.Equal(RespType.SimpleError, replies[1].Type);
        Assert.Throws<ValkeyServerException>(replies[1].ThrowIfError);
    }

    [Fact]
    public async Task ExecutePipelineAsyncRejectsABatchAboveThePendingRequestLimitBeforeWriting()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
        });
        var options = server.ClientOptions();
        await using var client = await ValkeyClient.ConnectAsync(
            new ValkeyClientOptions
            {
                Host = options.Host,
                Port = options.Port,
                ConnectTimeout = options.ConnectTimeout,
                MaxPendingRequests = 1,
            },
            TestContext.Current.CancellationToken
        );

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await client.ExecutePipelineAsync(
                [new ValkeyCommand("PING"), new ValkeyCommand("PING")],
                TestContext.Current.CancellationToken
            )
        );

        await client.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands);
    }

    [Fact]
    public async Task ExecutePipelineAsyncFailsWhenTheServerTruncatesTheBatch()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            for (var i = 0; i < 3; i++)
                await session.ReadCommandAsync();
            await session.SendAsync("+OK\r\n:1\r\n");
            session.Close();
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        await Assert.ThrowsAsync<ValkeyConnectionException>(async () =>
            await client.ExecutePipelineAsync(
                [new ValkeyCommand("SET", "a", "1"), new ValkeyCommand("INCR", "a"), new ValkeyCommand("GET", "a")],
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.PingAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ExecuteAsyncFailsWhenTheServerClosesMidReply()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("$100\r\ntruncated");
            session.Close();
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        var failure = await Assert.ThrowsAsync<ValkeyConnectionException>(async () =>
            await client.GetStringAsync("key", TestContext.Current.CancellationToken)
        );

        Assert.IsType<EndOfStreamException>(failure.InnerException);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.PingAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ExecuteAsyncInvalidatesTheConnectionOnAHostileFrame()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync("*100000000\r\n");
        });
        var options = new ValkeyClientOptions
        {
            Host = "127.0.0.1",
            Port = server.Port,
            MaxResponseBytes = 4096,
        };
        await using var client = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await client.ExecuteAsync(new ValkeyCommand("KEYS", "*"), TestContext.Current.CancellationToken)
        );
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.PingAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ExecuteAsyncInvalidatesTheConnectionWhenCancelledMidRead()
    {
        var written = new TaskCompletionSource();
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            written.SetResult();
            await session.ReadCommandAsync();
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        using var cancellation = new CancellationTokenSource();
        var pending = client.GetStringAsync("key", cancellation.Token);
        // Cancel only once the command is provably on the wire, so this exercises a cancelled read
        // rather than a cancelled queue wait.
        await written.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.PingAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ConcurrentCallersEachReceiveTheirOwnReply()
    {
        const int callers = 25;
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            var commands = new string[callers][];
            for (var i = 0; i < callers; i++)
                commands[i] = await session.ReadCommandAsync();

            // Holding every reply until every command arrives proves callers share one multiplexed
            // socket. A client that holds its write lock while awaiting a reply deadlocks here.
            for (var i = 0; i < callers; i++)
                await session.SendAsync($"+{commands[i][1]}\r\n");
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );

        var replies = await Task.WhenAll(
            Enumerable
                .Range(0, callers)
                .Select(index =>
                    client.ExecuteAsync(
                        new ValkeyCommand("ECHO", index.ToString(CultureInfo.InvariantCulture)),
                        TestContext.Current.CancellationToken
                    )
                )
        );

        for (var index = 0; index < callers; index++)
            Assert.Equal(index.ToString(CultureInfo.InvariantCulture), replies[index].AsString());
    }

    [Fact]
    public async Task CancellingAnEnqueuedCommandFailsEveryPendingCaller()
    {
        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseServer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.ReadCommandAsync();
            written.SetResult();
            await releaseServer.Task;
        });
        await using var client = await ValkeyClient.ConnectAsync(
            server.ClientOptions(),
            TestContext.Current.CancellationToken
        );
        using var cancellation = new CancellationTokenSource();

        var cancelled = client.GetStringAsync("cancelled", cancellation.Token);
        var collateral = client.GetStringAsync("collateral", TestContext.Current.CancellationToken);
        await written.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();

        try
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await cancelled);
            await Assert.ThrowsAsync<ValkeyConnectionException>(async () => await collateral);
            await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                await client.PingAsync(TestContext.Current.CancellationToken)
            );
        }
        finally
        {
            releaseServer.TrySetResult();
        }
    }

    [Fact]
    public async Task CancellingWhilePendingCapacityIsFullLeavesTheConnectionUsable()
    {
        var firstWritten = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var replyToFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            firstWritten.SetResult();
            await replyToFirst.Task;
            await session.SendAsync("+first\r\n");
            await session.ReadCommandAsync();
            await session.SendAsync("+PONG\r\n");
        });
        var options = server.ClientOptions();
        await using var client = await ValkeyClient.ConnectAsync(
            new ValkeyClientOptions
            {
                Host = options.Host,
                Port = options.Port,
                ConnectTimeout = options.ConnectTimeout,
                MaxPendingRequests = 1,
            },
            TestContext.Current.CancellationToken
        );

        var first = client.ExecuteAsync(new ValkeyCommand("ECHO", "first"), TestContext.Current.CancellationToken);
        await firstWritten.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        using var cancellation = new CancellationTokenSource();
        var waiting = client.PingAsync(cancellation.Token);
        await cancellation.CancelAsync();

        try
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await waiting);
        }
        finally
        {
            replyToFirst.TrySetResult();
        }
        Assert.Equal("first", (await first).AsString());
        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DisposeAsyncDuringAnInFlightCommandFaultsIt()
    {
        var written = new TaskCompletionSource();
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            written.SetResult();
            await session.ReadCommandAsync();
        });
        var client = await ValkeyClient.ConnectAsync(server.ClientOptions(), TestContext.Current.CancellationToken);

        var pending = client.GetStringAsync("key", TestContext.Current.CancellationToken);
        await written.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        await client.DisposeAsync();

        // The in-flight command must fail rather than hang; which transport failure surfaces depends
        // on how far the read had progressed.
        var failure = await Record.ExceptionAsync(async () => await pending);
        Assert.NotNull(failure);
        Assert.True(failure is ObjectDisposedException or ValkeyConnectionException, failure.ToString());
    }

    [Fact]
    public async Task EveryEntryPointThrowsAfterDispose()
    {
        await using var server = FakeValkeyServer.Start(session => session.ExpectHandshakeAsync());
        var client = await ValkeyClient.ConnectAsync(server.ClientOptions(), TestContext.Current.CancellationToken);
        await client.DisposeAsync();
        await client.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.PingAsync(TestContext.Current.CancellationToken)
        );
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.ExecutePipelineAsync([new ValkeyCommand("PING")], TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ConnectAsyncNegotiatesTlsWithAnExplicitlyTrustedCertificate()
    {
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        var expected = certificate.GetCertHashString(HashAlgorithmName.SHA256);
        await using var server = FakeValkeyServer.Start(
            async session =>
            {
                await session.ExpectHandshakeAsync();
                await session.ReadCommandAsync();
                await session.SendAsync("+PONG\r\n");
            },
            certificate
        );
        var options = new ValkeyClientOptions
        {
            Host = "127.0.0.1",
            Port = server.Port,
            UseTls = true,
            CertificateValidationCallback = (_, presented, _, _) =>
                presented is not null
                && string.Equals(
                    presented.GetCertHashString(HashAlgorithmName.SHA256),
                    expected,
                    StringComparison.Ordinal
                ),
        };

        await using var client = await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken);

        Assert.Equal("PONG", await client.PingAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ConnectAsyncRejectsAnUntrustedCertificateByDefault()
    {
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        await using var server = FakeValkeyServer.Start(session => session.ExpectHandshakeAsync(), certificate);
        var options = new ValkeyClientOptions
        {
            Host = "127.0.0.1",
            Port = server.Port,
            UseTls = true,
        };

        await Assert.ThrowsAsync<AuthenticationException>(async () =>
            await ValkeyClient.ConnectAsync(options, TestContext.Current.CancellationToken)
        );
    }
}
