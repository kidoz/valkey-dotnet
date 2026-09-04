using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests;

public sealed class ValkeyConnectionOwnerTests
{
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task NonListeningEndpointIsNotSentAndTheSameOwnerRecoversWhenItStarts()
    {
        // Bind without listening: reserve an exact local target that cannot accept a connection.
        using var endpoint = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        endpoint.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var port = ((IPEndPoint)endpoint.LocalEndPoint!).Port;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = port,
                    ConnectTimeout = TimeSpan.FromSeconds(1),
                },
                MaxConnectAttempts = 2,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(5),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(10),
            }
        );
        var failure = await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            owner.ExecuteAsync(new ValkeyCommand("INCR", "never-sent"), deadline.Token)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, failure.DeliveryStatus);
        // Kernels may refuse a bound/non-listening endpoint or silently drop its SYN packets.
        if (failure.InnerException is SocketException socketFailure)
            Assert.Equal(SocketError.ConnectionRefused, socketFailure.SocketErrorCode);
        else
            Assert.IsType<TimeoutException>(failure.InnerException);
        Assert.Equal(ValkeyConnectionState.Disconnected, owner.State);

        endpoint.Listen(1);
        var serving = ServeAsync();
        Assert.Equal("PONG", (await owner.ExecuteAsync(new ValkeyCommand("PING"), deadline.Token)).AsString());
        await serving;

        async Task ServeAsync()
        {
            using var accepted = await endpoint.AcceptAsync(deadline.Token);
            using var cancellation = deadline.Token.Register(accepted.Dispose);
            await using var stream = new NetworkStream(accepted, ownsSocket: false);
            var session = new FakeValkeySession(stream, []);
            await session.ExpectHandshakeAsync();
            // The earlier write was not queued offline or replayed after the endpoint started.
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
        }
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task RepeatedConcurrentLossSettlesEveryCallerAndKeepsOneConnectionPerCycle(ValkeyProtocol protocol)
    {
        const int cycles = 32;
        const int callers = 16;
        var activeSessions = 0;
        var peakSessions = 0;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        await using var server = FakeValkeyServer.StartMany(
            cycles,
            async (_, session) =>
            {
                var active = Interlocked.Increment(ref activeSessions);
                var peak = Volatile.Read(ref peakSessions);
                while (active > peak)
                {
                    var observed = Interlocked.CompareExchange(ref peakSessions, active, peak);
                    if (observed == peak)
                        break;
                    peak = observed;
                }
                try
                {
                    await session.ExpectHandshakeAsync(
                        protocol == ValkeyProtocol.Resp2 ? FakeValkeyServer.HelloResp2 : FakeValkeyServer.HelloResp3
                    );
                    for (var i = 0; i < callers; i++)
                    {
                        var command = await session.ReadCommandAsync();
                        Assert.Equal("ECHO", command[0]);
                        await session.SendAsync($"+{command[1]}\r\n");
                    }
                    for (var i = 0; i < callers; i++)
                        Assert.Equal(["INCR", "ambiguous"], await session.ReadCommandAsync());
                }
                finally
                {
                    // End this server-side session before permitting a replacement to arrive.
                    Interlocked.Decrement(ref activeSessions);
                    session.Close();
                }
            }
        );
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    Protocol = protocol,
                    MaxPendingRequests = callers,
                },
                MaxConcurrentOperations = callers,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(4),
            }
        );
        for (var cycle = 0; cycle < cycles; cycle++)
        {
            var replies = await Task.WhenAll(
                Enumerable
                    .Range(0, callers)
                    .Select(i => owner.ExecuteAsync(new ValkeyCommand("ECHO", i), deadline.Token))
            );
            Assert.Equal(
                Enumerable.Range(0, callers).Select(i => i.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                replies.Select(reply => reply.AsString())
            );
            var failures = Enumerable
                .Range(0, callers)
                .Select(_ =>
                    Assert.ThrowsAsync<ValkeyConnectionException>(() =>
                        owner.ExecuteAsync(new ValkeyCommand("INCR", "ambiguous"), deadline.Token)
                    )
                );
            foreach (var failure in await Task.WhenAll(failures))
                Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, failure.DeliveryStatus);
            Assert.Equal(ValkeyConnectionState.Disconnected, owner.State);
        }
        await owner.DisposeAsync();
        await server.Session;
        Assert.Equal(0, Volatile.Read(ref activeSessions));
        Assert.Equal(1, Volatile.Read(ref peakSessions));
        Assert.Equal(cycles, server.ReceivedCommands.Count(command => command[0] == "HELLO"));
        Assert.Equal(cycles * callers, server.ReceivedCommands.Count(command => command[0] == "INCR"));
    }

    [Fact]
    public async Task ConcurrentRecoveryKeepsOneReplacementAndRepeatsScopedTlsValidation()
    {
        const int callers = 16;
        using var certificate = FakeValkeyServer.CreateSelfSignedCertificate();
        var validations = 0;
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                if (index == 0)
                {
                    for (var i = 0; i < callers; i++)
                        Assert.Equal("GET", (await session.ReadCommandAsync())[0]);
                    session.Close();
                    return;
                }
                for (var i = 0; i < callers; i++)
                {
                    var command = await session.ReadCommandAsync();
                    Assert.Equal("GET", command[0]);
                    await session.SendAsync($"+{command[1]}\r\n");
                }
                await session.ReadCommandAsync();
            },
            certificate
        );
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    UseTls = true,
                    CertificateValidationCallback = (_, presented, _, _) =>
                    {
                        Interlocked.Increment(ref validations);
                        return presented?.GetCertHashString() == certificate.GetCertHashString();
                    },
                },
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(4),
            }
        );
        var replies = await Task.WhenAll(
                Enumerable
                    .Range(0, callers)
                    .Select(i => owner.ExecuteRetryableAsync(new ValkeyCommand("GET", i), TestToken))
            )
            .WaitAsync(TimeSpan.FromSeconds(30), TestToken);
        Assert.Equal(
            Enumerable.Range(0, callers).Select(i => i.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            replies.Select(r => r.AsString())
        );
        Assert.Equal(2, Volatile.Read(ref validations));
        await owner.DisposeAsync();
        await server.Session;
        Assert.Equal(2, server.ReceivedCommands.Count(c => c[0] == "HELLO"));
    }

    [Fact]
    public async Task DisposalDuringAnAdmittedCommandNeverClaimsNonDelivery()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            received.SetResult();
            await session.ReadCommandAsync();
        });
        await using var owner = new ValkeyConnectionOwner(Options(server));
        var command = owner.ExecuteAsync(new ValkeyCommand("INCR", "counter"), TestToken);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(10), TestToken);
        await owner.DisposeAsync();
        var error = await Record.ExceptionAsync(() => command);
        Assert.NotNull(error);
        Assert.False(error is IValkeyCommandFailure { DeliveryStatus: ValkeyCommandDeliveryStatus.NotSent });
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c[0] == "INCR");
    }

    [Fact]
    public async Task RecoveryDeadlineDoesNotEraseAnEarlierAmbiguousAttempt()
    {
        var reconnecting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                if (index == 0)
                {
                    await session.ExpectHandshakeAsync();
                    await session.ReadCommandAsync();
                    session.Close();
                }
                else
                {
                    await session.ReadCommandAsync();
                    reconnecting.SetResult();
                    await session.ReadCommandAsync();
                }
            }
        );
        await using var owner = new ValkeyConnectionOwner(Options(server));
        var retry = owner.ExecuteRetryableWithDeadlineAsync(
            new ValkeyCommand("GET", "key"),
            TimeSpan.FromSeconds(1),
            TestToken
        );
        await reconnecting.Task.WaitAsync(TimeSpan.FromSeconds(10), TestToken);
        Assert.Equal(ValkeyConnectionState.Reconnecting, owner.State);
        var error = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() => retry);
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        await owner.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c[0] == "GET");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationDoesNotReplayAndReplacementSupportsPipelinesAndScripts(bool deadline)
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                if (index == 0)
                {
                    Assert.Equal(["GET", "canceled"], await session.ReadCommandAsync());
                    received.SetResult();
                    await session.ReadCommandAsync();
                    return;
                }
                Assert.Equal(["PING"], await session.ReadCommandAsync());
                Assert.Equal(["GET", "wrong-type"], await session.ReadCommandAsync());
                await session.SendAsync("+PONG\r\n-WRONGTYPE wrong type\r\n");
                for (var i = 0; i < 2; i++)
                {
                    Assert.Equal("EVALSHA", (await session.ReadCommandAsync())[0]);
                    await session.SendAsync("-NOSCRIPT missing\r\n");
                }
                Assert.Equal(["EVAL", "return ARGV[1]", "0", "value"], await session.ReadCommandAsync());
                await session.SendAsync("+value\r\n");
                await session.ReadCommandAsync();
            }
        );
        await using var owner = new ValkeyConnectionOwner(Options(server));
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var operation = owner.ExecuteRetryableAsync(new ValkeyCommand("GET", "canceled"), canceled.Token);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(10), TestToken);
        await canceled.CancelAsync();
        var error = await Assert.ThrowsAsync<ValkeyCommandCanceledException>(() => operation);
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        ValkeyCommand[] batch = [new("PING"), new("GET", "wrong-type")];
        var replies = deadline
            ? await owner.ExecutePipelineWithDeadlineAsync(batch, TimeSpan.FromSeconds(10), TestToken)
            : await owner.ExecutePipelineAsync(batch, TestToken);
        Assert.Equal("PONG", replies[0].AsString());
        Assert.Throws<ValkeyServerException>(() => replies[1].ThrowIfError());
        var script = new ValkeyScript("return ARGV[1]");
        var result = deadline
            ? await owner.ExecuteScriptWithDeadlineAsync(script, [], ["value"], TimeSpan.FromSeconds(10), TestToken)
            : await owner.ExecuteScriptAsync(script, [], ["value"], TestToken);
        Assert.Equal("value", result.AsString());
        await owner.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c is ["GET", "canceled"]);
    }

    private static ValkeyConnectionOwnerOptions Options(FakeValkeyServer server, int capacity = 32, int retries = 1) =>
        new()
        {
            Connection = server.ClientOptions(),
            MaxConcurrentOperations = capacity,
            MaxCommandRetries = retries,
            InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
            MaxReconnectDelay = TimeSpan.FromMilliseconds(4),
        };

    [Fact]
    public async Task ConstructionIsLazyAndDisposalIsTerminalAndIdempotent()
    {
        await using var owner = new ValkeyConnectionOwner();
        Assert.Equal(ValkeyConnectionState.NeverConnected, owner.State);
        Assert.Equal("localhost", owner.Host);
        Assert.Equal(6379, owner.Port);
        var disposal = owner.DisposeAsync().AsTask();
        Assert.Same(disposal, owner.DisposeAsync().AsTask());
        await disposal;
        Assert.Equal(ValkeyConnectionState.Disposed, owner.State);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => owner.ConnectAsync(TestToken));
    }

    [Fact]
    public void InvalidOptionsFailBeforeConnecting()
    {
        Assert.Throws<ArgumentNullException>(() => new ValkeyConnectionOwnerOptions { Connection = null! }.Validate());
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnectionOwnerOptions { MaxConcurrentOperations = 0 }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnectionOwnerOptions { MaxConnectAttempts = 101 }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnectionOwnerOptions { MaxCommandRetries = -1 }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnectionOwnerOptions { MaxCommandRetries = 17 }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnectionOwnerOptions { InitialReconnectDelay = TimeSpan.Zero }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnectionOwnerOptions { MaxReconnectDelay = TimeSpan.FromMilliseconds(1) }.Validate()
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ValkeyConnectionOwnerOptions { MaxReconnectDelay = TimeSpan.MaxValue }.Validate()
        );
    }

    [Fact]
    public async Task ConcurrentCallersShareOneConnectionAndKeepTheirReplies()
    {
        const int count = 24;
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            for (var i = 0; i < count; i++)
            {
                var command = await session.ReadCommandAsync();
                Assert.Equal("ECHO", command[0]);
                await session.SendAsync($"+{command[1]}\r\n");
            }
            await session.ReadCommandAsync();
        });
        await using var owner = new ValkeyConnectionOwner(Options(server));
        var tasks = Enumerable
            .Range(0, count)
            .Select(async i =>
            {
                var reply = await owner.ExecuteAsync(new ValkeyCommand("ECHO", i), TestToken);
                Assert.Equal(i, int.Parse(reply.AsString()!, System.Globalization.CultureInfo.InvariantCulture));
            });
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30), TestToken);
        Assert.Equal(ValkeyConnectionState.Connected, owner.State);
        await owner.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c[0] == "HELLO");
    }

    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task FailedWritesAreNotReplayedAndReplacementReusesConnectionSettings(ValkeyProtocol protocol)
    {
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                var hello = await session.ExpectHandshakeAsync(
                    protocol == ValkeyProtocol.Resp2 ? FakeValkeyServer.HelloResp2 : FakeValkeyServer.HelloResp3
                );
                Assert.Equal(["HELLO", protocol == ValkeyProtocol.Resp2 ? "2" : "3", "SETNAME", "owner-tests"], hello);
                Assert.Equal(["SELECT", "2"], await session.ReadCommandAsync());
                await session.SendAsync("+OK\r\n");
                if (index == 0)
                {
                    Assert.Equal(["INCR", "counter"], await session.ReadCommandAsync());
                    session.Close();
                    return;
                }
                Assert.Equal(["PING"], await session.ReadCommandAsync());
                await session.SendAsync("+PONG\r\n");
                await session.ReadCommandAsync();
            }
        );
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    Protocol = protocol,
                    ClientName = "owner-tests",
                    Database = 2,
                },
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(4),
            }
        );
        var error = await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            owner.ExecuteAsync(new ValkeyCommand("INCR", "counter"), TestToken)
        );
        Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        Assert.Equal(ValkeyConnectionState.Disconnected, owner.State);
        Assert.Equal("PONG", (await owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken)).AsString());
        await owner.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c[0] == "INCR");
        Assert.Equal(2, server.ReceivedCommands.Count(c => c[0] == "HELLO"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExplicitRetryReplaysOnlyWithinItsLimit(bool recover)
    {
        await using var server = FakeValkeyServer.StartMany(
            2,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync();
                Assert.Equal(["GET", "key"], await session.ReadCommandAsync());
                if (recover && index == 1)
                {
                    await session.SendAsync("+value\r\n");
                    await session.ReadCommandAsync();
                }
                else
                    session.Close();
            }
        );
        await using var owner = new ValkeyConnectionOwner(Options(server));
        var operation = owner.ExecuteRetryableWithDeadlineAsync(
            new ValkeyCommand("GET", "key"),
            TimeSpan.FromSeconds(10),
            TestToken
        );
        if (recover)
            Assert.Equal("value", (await operation).AsString());
        else
        {
            var error = await Assert.ThrowsAsync<ValkeyConnectionException>(() => operation);
            Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, error.DeliveryStatus);
        }
        await owner.DisposeAsync();
        await server.Session;
        Assert.Equal(2, server.ReceivedCommands.Count(c => c[0] == "GET"));
    }

    [Fact]
    public async Task ZeroRetriesDisablesEvenExplicitReplay()
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            session.Close();
        });
        await using var owner = new ValkeyConnectionOwner(Options(server, retries: 0));
        await Assert.ThrowsAsync<ValkeyConnectionException>(() =>
            owner.ExecuteRetryableAsync(new ValkeyCommand("GET", "key"), TestToken)
        );
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c[0] == "GET");
    }

    [Fact]
    public async Task CanceledAndExpiredWaitersDoNotCancelSharedConnectionAndAdmissionIsBounded()
    {
        var handshake = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            Assert.Equal("HELLO", (await session.ReadCommandAsync())[0]);
            handshake.SetResult();
            await release.Task.WaitAsync(TestToken);
            await session.SendAsync(FakeValkeyServer.HelloResp3);
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
            await session.ReadCommandAsync();
        });
        await using var owner = new ValkeyConnectionOwner(Options(server, capacity: 2));
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        try
        {
            var abandoned = owner.ConnectAsync(canceled.Token);
            await handshake.Task.WaitAsync(TimeSpan.FromSeconds(10), TestToken);
            Assert.Equal(ValkeyConnectionState.Connecting, owner.State);
            var healthy = owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken);
            var capacity = await Assert.ThrowsAsync<ValkeyCapacityException>(() => owner.ConnectAsync(TestToken));
            Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, capacity.DeliveryStatus);
            await canceled.CancelAsync();
            var cancelError = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);
            Assert.IsNotType<ValkeyCommandCanceledException>(cancelError);
            var expired = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() =>
                owner.ExecuteWithDeadlineAsync(
                    new ValkeyCommand("INCR", "never-sent"),
                    TimeSpan.FromMilliseconds(50),
                    TestToken
                )
            );
            Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, expired.DeliveryStatus);
            release.TrySetResult();
            Assert.Equal("PONG", (await healthy.WaitAsync(TimeSpan.FromSeconds(10), TestToken)).AsString());
        }
        finally
        {
            release.TrySetResult();
        }
        await owner.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c[0] == "HELLO");
        Assert.DoesNotContain(server.ReceivedCommands, c => c[0] == "INCR");
    }

    [Fact]
    public async Task ConnectionAttemptsAreBoundedAndBackoffPersistsAcrossCycles()
    {
        var attempts = 0;
        await using var server = FakeValkeyServer.StartMany(
            6,
            async (_, session) =>
            {
                Assert.Equal("HELLO", (await session.ReadCommandAsync())[0]);
                Interlocked.Increment(ref attempts);
                session.Close();
            }
        );
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = server.ClientOptions(),
                MaxConnectAttempts = 3,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(40),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(80),
            }
        );
        var clock = Stopwatch.StartNew();
        for (var cycle = 1; cycle <= 2; cycle++)
        {
            var error = await Assert.ThrowsAsync<ValkeyConnectionException>(() => owner.ConnectAsync(TestToken));
            Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, error.DeliveryStatus);
            Assert.Equal(cycle * 3, Volatile.Read(ref attempts));
            Assert.Equal(ValkeyConnectionState.Disconnected, owner.State);
        }
        // Equal jitter gives >=20ms for the first pause and >=40ms for each of the next four.
        Assert.True(clock.Elapsed >= TimeSpan.FromMilliseconds(160));
        await server.Session;
    }

    [Theory]
    [InlineData("-WRONGPASS Authentication rejected\r\n", true)]
    [InlineData("+invalid-hello\r\n", false)]
    public async Task TerminalHandshakeFailuresAreNotRetried(string response, bool authentication)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(response);
        });
        await using var owner = new ValkeyConnectionOwner(Options(server));
        var first = await Record.ExceptionAsync(() => owner.ConnectAsync(TestToken));
        if (authentication)
            Assert.IsType<ValkeyServerException>(first);
        else
            Assert.IsType<ValkeyProtocolException>(first);
        Assert.Equal(ValkeyConnectionState.Faulted, owner.State);
        Assert.Same(first, await Record.ExceptionAsync(() => owner.ConnectAsync(TestToken)));
        await server.Session;
        Assert.Single(server.ReceivedCommands);
    }

    [Theory]
    [InlineData("-ERR rejected\r\n", true)]
    [InlineData("?malformed\r\n", false)]
    public async Task ExplicitRetryDoesNotReplayServerOrProtocolErrors(string response, bool serverError)
    {
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(response);
            await session.ReadCommandAsync();
        });
        await using var owner = new ValkeyConnectionOwner(Options(server));
        var error = await Record.ExceptionAsync(() =>
            owner.ExecuteRetryableAsync(new ValkeyCommand("GET", "key"), TestToken)
        );
        if (serverError)
            Assert.IsType<ValkeyServerException>(error);
        else
            Assert.IsType<ValkeyProtocolException>(error);
        await owner.DisposeAsync();
        await server.Session;
        Assert.Single(server.ReceivedCommands, c => c[0] == "GET");
    }

    [Fact]
    public async Task DisposalCancelsSharedConnectionAndSettlesEveryWaiter()
    {
        var handshake = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ReadCommandAsync();
            handshake.SetResult();
            await session.ReadCommandAsync();
        });
        await using var owner = new ValkeyConnectionOwner(Options(server));
        var waiters = Enumerable.Range(0, 16).Select(_ => owner.ConnectAsync(TestToken)).ToArray();
        await handshake.Task.WaitAsync(TimeSpan.FromSeconds(10), TestToken);
        await owner.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10), TestToken);
        foreach (var waiter in waiters)
            await Assert.ThrowsAsync<ObjectDisposedException>(() => waiter);
        Assert.Equal(ValkeyConnectionState.Disposed, owner.State);
        await server.Session;
    }

    [Fact]
    public async Task DeadlinesBeforeAdmissionStayNotSentAndLateRepliesDoNotMoveToAnotherCaller()
    {
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            Assert.Equal(["GET", "slow"], await session.ReadCommandAsync());
            received.SetResult();
            await release.Task.WaitAsync(TestToken);
            await session.SendAsync("+late\r\n");
            Assert.Equal(["PING"], await session.ReadCommandAsync());
            await session.SendAsync("+PONG\r\n");
            await session.ReadCommandAsync();
        });
        await using var owner = new ValkeyConnectionOwner(
            new ValkeyConnectionOwnerOptions
            {
                Connection = new ValkeyClientOptions
                {
                    Host = "127.0.0.1",
                    Port = server.Port,
                    MaxPendingRequests = 1,
                },
            }
        );
        try
        {
            var slow = owner.ExecuteRetryableWithDeadlineAsync(
                new ValkeyCommand("GET", "slow"),
                TimeSpan.FromMilliseconds(300),
                TestToken
            );
            await received.Task.WaitAsync(TimeSpan.FromSeconds(10), TestToken);
            var unsent = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() =>
                owner.ExecuteWithDeadlineAsync(
                    new ValkeyCommand("INCR", "not-sent"),
                    TimeSpan.FromMilliseconds(30),
                    TestToken
                )
            );
            Assert.Equal(ValkeyCommandDeliveryStatus.NotSent, unsent.DeliveryStatus);
            var expired = await Assert.ThrowsAsync<ValkeyCommandTimeoutException>(() => slow);
            Assert.Equal(ValkeyCommandDeliveryStatus.MayHaveBeenSent, expired.DeliveryStatus);
            Assert.Equal(ValkeyConnectionState.Connected, owner.State);
            var ping = owner.ExecuteAsync(new ValkeyCommand("PING"), TestToken);
            release.TrySetResult();
            Assert.Equal("PONG", (await ping.WaitAsync(TimeSpan.FromSeconds(10), TestToken)).AsString());
        }
        finally
        {
            release.TrySetResult();
        }
        await owner.DisposeAsync();
        await server.Session;
        Assert.Equal(3, server.ReceivedCommands.Count);
    }
}
