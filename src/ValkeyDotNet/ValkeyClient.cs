using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet;

/// <summary>
/// An asynchronous, dependency-free client for a single Valkey node.
/// The connection is safe for concurrent callers and matches multiplexed replies in wire order.
/// </summary>
public sealed class ValkeyClient : IAsyncDisposable
{
    private const long MaxOperationTimeoutMilliseconds = uint.MaxValue - 1;

    /// <summary>
    /// Commands that redefine what the connection is rather than returning one ordinary reply.
    /// The client owns the connection's protocol, database, and reply framing, so it rejects these
    /// instead of writing them and desynchronizing itself. See ExplainRejection for the specifics.
    /// </summary>
    private static readonly FrozenSet<string> ConnectionStateCommands = FrozenSet.ToFrozenSet(
        [
            "SUBSCRIBE",
            "UNSUBSCRIBE",
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
            "SSUBSCRIBE",
            "SUNSUBSCRIBE",
            "MONITOR",
            "RESET",
            "HELLO",
        ],
        StringComparer.Ordinal
    );

    private readonly ValkeyClientOptions _options;
    private readonly TcpClient _tcpClient;
    private readonly Stream _stream;
    private readonly RespReader _reader;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly SemaphoreSlim _pendingCapacity;
    private readonly ConcurrentQueue<TaskCompletionSource<RespValue>> _pendingResponses = new();
    private readonly CancellationTokenSource _readerShutdown = new();
    private Task? _responseLoop;
    private int _disposed;

    private ValkeyClient(ValkeyClientOptions options, TcpClient tcpClient, Stream stream)
    {
        _options = options;
        _tcpClient = tcpClient;
        _stream = stream;
        _pendingCapacity = new SemaphoreSlim(options.MaxPendingRequests, options.MaxPendingRequests);
        _reader = new RespReader(
            stream,
            options.MaxResponseBytes,
            options.MaxResponseElements,
            options.MaxNestingDepth
        );
        NegotiatedProtocol = options.Protocol;
    }

    /// <summary>The server metadata returned by the initial HELLO command.</summary>
    public RespValue ServerInfo { get; private set; } = RespValue.Null();

    /// <summary>The protocol the server reported in its HELLO reply, which may be a downgrade.</summary>
    public ValkeyProtocol NegotiatedProtocol { get; private set; }

    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    /// <summary>
    /// Raised when a RESP3 push frame is encountered while reading command responses.
    /// The client reads pushes continuously after the handshake completes.
    /// </summary>
    public event Action<RespValue>? PushReceived;

    public static async Task<ValkeyClient> ConnectAsync(
        ValkeyClientOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        options ??= new ValkeyClientOptions();
        options.Validate();

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.ConnectTimeout);
        var tcpClient = new TcpClient { NoDelay = true };

        try
        {
            await tcpClient.ConnectAsync(options.Host, options.Port, timeout.Token).ConfigureAwait(false);
            Stream stream = tcpClient.GetStream();
            if (options.UseTls)
            {
                var ssl = options.CertificateValidationCallback is null
                    ? new SslStream(stream, leaveInnerStreamOpen: false)
                    : new SslStream(stream, leaveInnerStreamOpen: false, options.CertificateValidationCallback);
                await ssl.AuthenticateAsClientAsync(
                        new SslClientAuthenticationOptions { TargetHost = options.Host },
                        timeout.Token
                    )
                    .ConfigureAwait(false);
                stream = ssl;
            }

            var client = new ValkeyClient(options, tcpClient, stream);
            try
            {
                await client.InitializeAsync(timeout.Token).ConfigureAwait(false);
                client.StartResponseLoop();
                return client;
            }
            catch
            {
                await client.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            tcpClient.Dispose();
            throw new TimeoutException(
                $"Connecting to {options.Host}:{options.Port} exceeded {options.ConnectTimeout}."
            );
        }
        catch
        {
            tcpClient.Dispose();
            throw;
        }
    }

    /// <summary>Executes any Valkey command and throws when the server returns an error reply.</summary>
    public async Task<RespValue> ExecuteAsync(ValkeyCommand command, CancellationToken cancellationToken = default) =>
        await ExecuteCoreAsync(command, timeout: null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Executes any Valkey command with an isolated deadline. Once the command is enqueued, expiry
    /// stops waiting for its reply but leaves the connection reader to drain that reply. The
    /// connection terminates if draining exceeds <see cref="ValkeyClientOptions.ResponseDrainTimeout"/>.
    /// </summary>
    public async Task<RespValue> ExecuteWithDeadlineAsync(
        ValkeyCommand command,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) => await ExecuteCoreAsync(command, ValidateOperationTimeout(timeout), cancellationToken).ConfigureAwait(false);

    private async Task<RespValue> ExecuteCoreAsync(
        ValkeyCommand command,
        TimeSpan? timeout,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(command);
        EnsureSupported(command);
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        var response = await SendMultiplexedAsync(RespWriter.Encode(command), timeout, cancellationToken)
            .ConfigureAwait(false);
        response.ThrowIfError();
        return response;
    }

    /// <summary>
    /// Writes all commands as one contiguous batch without awaiting individual replies. Error replies
    /// are returned in place so the batch remains synchronized; call ThrowIfError on each result.
    /// </summary>
    public async Task<IReadOnlyList<RespValue>> ExecutePipelineAsync(
        IEnumerable<ValkeyCommand> commands,
        CancellationToken cancellationToken = default
    ) => await ExecutePipelineCoreAsync(commands, timeout: null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Executes a contiguous command batch with an isolated deadline. Expiry never abandons unread
    /// positional replies; the background reader drains the entire batch before later replies, or
    /// terminates the connection after <see cref="ValkeyClientOptions.ResponseDrainTimeout"/>.
    /// </summary>
    public async Task<IReadOnlyList<RespValue>> ExecutePipelineWithDeadlineAsync(
        IEnumerable<ValkeyCommand> commands,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) =>
        await ExecutePipelineCoreAsync(commands, ValidateOperationTimeout(timeout), cancellationToken)
            .ConfigureAwait(false);

    private async Task<IReadOnlyList<RespValue>> ExecutePipelineCoreAsync(
        IEnumerable<ValkeyCommand> commands,
        TimeSpan? timeout,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(commands);
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        var commandList = commands.ToArray();
        if (commandList.Length == 0)
            return Array.Empty<RespValue>();
        if (commandList.Any(static command => command is null))
            throw new ArgumentException("A pipeline cannot contain a null command.", nameof(commands));
        if (commandList.Length > _options.MaxPendingRequests)
            throw new ArgumentException(
                $"A pipeline cannot exceed the configured {_options.MaxPendingRequests} pending requests.",
                nameof(commands)
            );
        foreach (var command in commandList)
            EnsureSupported(command);

        var payloads = new byte[commandList.Length][];
        for (var i = 0; i < commandList.Length; i++)
            payloads[i] = RespWriter.Encode(commandList[i]);
        return await SendMultiplexedAsync(payloads, timeout, cancellationToken).ConfigureAwait(false);
    }

    public async Task<string> PingAsync(CancellationToken cancellationToken = default) =>
        (await ExecuteAsync(new ValkeyCommand("PING"), cancellationToken).ConfigureAwait(false)).AsString()!;

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        var value = await ExecuteAsync(new ValkeyCommand("GET", key), cancellationToken).ConfigureAwait(false);
        return value.IsNull ? null : value.AsBytes().ToArray();
    }

    public async Task<string?> GetStringAsync(string key, CancellationToken cancellationToken = default) =>
        (await ExecuteAsync(new ValkeyCommand("GET", key), cancellationToken).ConfigureAwait(false)).AsString();

    public async Task<bool> SetAsync(
        string key,
        ReadOnlyMemory<byte> value,
        TimeSpan? expiry = null,
        bool onlyIfNotExists = false,
        bool onlyIfExists = false,
        CancellationToken cancellationToken = default
    )
    {
        if (onlyIfNotExists && onlyIfExists)
            throw new ArgumentException("NX and XX cannot be used together.");

        var arguments = new List<ValkeyArgument> { key, new(value) };
        if (expiry is { } duration)
        {
            if (duration <= TimeSpan.Zero || duration.TotalMilliseconds > long.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(expiry));
            arguments.Add("PX");
            arguments.Add((long)Math.Ceiling(duration.TotalMilliseconds));
        }
        if (onlyIfNotExists)
            arguments.Add("NX");
        if (onlyIfExists)
            arguments.Add("XX");

        var response = await ExecuteAsync(new ValkeyCommand("SET", arguments.ToArray()), cancellationToken)
            .ConfigureAwait(false);
        return !response.IsNull;
    }

    public Task<bool> SetStringAsync(
        string key,
        string value,
        TimeSpan? expiry = null,
        bool onlyIfNotExists = false,
        bool onlyIfExists = false,
        CancellationToken cancellationToken = default
    ) =>
        SetAsync(
            key,
            System.Text.Encoding.UTF8.GetBytes(value),
            expiry,
            onlyIfNotExists,
            onlyIfExists,
            cancellationToken
        );

    public async Task<long> DeleteAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        var arguments = keys.Select(static key => (ValkeyArgument)key).ToArray();
        if (arguments.Length == 0)
            return 0;
        return (
            await ExecuteAsync(new ValkeyCommand("DEL", arguments), cancellationToken).ConfigureAwait(false)
        ).AsInt64();
    }

    public async Task<long> IncrementAsync(
        string key,
        long amount = 1,
        CancellationToken cancellationToken = default
    ) =>
        (
            await ExecuteAsync(new ValkeyCommand("INCRBY", key, amount), cancellationToken).ConfigureAwait(false)
        ).AsInt64();

    public async Task<bool> HashSetAsync(
        string key,
        string field,
        ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default
    ) =>
        (
            await ExecuteAsync(new ValkeyCommand("HSET", key, field, new ValkeyArgument(value)), cancellationToken)
                .ConfigureAwait(false)
        ).AsInt64() == 1;

    public async Task<byte[]?> HashGetAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        var value = await ExecuteAsync(new ValkeyCommand("HGET", key, field), cancellationToken).ConfigureAwait(false);
        return value.IsNull ? null : value.AsBytes().ToArray();
    }

    public async ValueTask DisposeAsync()
    {
        InvalidateConnection(new ObjectDisposedException(nameof(ValkeyClient)));
        var responseLoop = Volatile.Read(ref _responseLoop);
        if (responseLoop is not null)
            await responseLoop.ConfigureAwait(false);
    }

    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        var arguments = new List<ValkeyArgument> { (int)_options.Protocol };
        if (_options.Password is not null)
        {
            arguments.Add("AUTH");
            arguments.Add(_options.Username ?? "default");
            arguments.Add(_options.Password);
        }
        if (_options.ClientName is not null)
        {
            arguments.Add("SETNAME");
            arguments.Add(_options.ClientName);
        }

        ServerInfo = await SendAndReadAsync(new ValkeyCommand("HELLO", arguments.ToArray()), cancellationToken)
            .ConfigureAwait(false);
        ServerInfo.ThrowIfError();
        if (ServerInfo.Type is not (RespType.Map or RespType.Array))
            throw new ValkeyProtocolException("HELLO returned an unexpected response type.");
        NegotiatedProtocol = ReadNegotiatedProtocol(ServerInfo);

        if (_options.Database != 0)
        {
            var select = await SendAndReadAsync(new ValkeyCommand("SELECT", _options.Database), cancellationToken)
                .ConfigureAwait(false);
            select.ThrowIfError();
        }
    }

    /// <summary>
    /// Reads the protocol the server actually selected. HELLO reports it as a map entry on RESP3 and
    /// as a flat key/value array on RESP2, and a server may answer with a lower version than asked.
    /// </summary>
    private static ValkeyProtocol ReadNegotiatedProtocol(RespValue serverInfo)
    {
        var reported = serverInfo.Type == RespType.Map ? FindProtocol(serverInfo.AsMap()) : FindProtocol(serverInfo);
        return reported switch
        {
            2 => ValkeyProtocol.Resp2,
            3 => ValkeyProtocol.Resp3,
            _ => throw new ValkeyProtocolException("HELLO did not report a supported protocol version."),
        };
    }

    private static long? FindProtocol(IReadOnlyList<KeyValuePair<RespValue, RespValue>> pairs)
    {
        foreach (var pair in pairs)
        {
            if (IsProtocolKey(pair.Key))
                return pair.Value.Type == RespType.Integer ? pair.Value.AsInt64() : null;
        }
        return null;
    }

    private static long? FindProtocol(RespValue flatArray)
    {
        var items = flatArray.AsArray();
        for (var i = 0; i + 1 < items.Count; i += 2)
        {
            if (IsProtocolKey(items[i]))
                return items[i + 1].Type == RespType.Integer ? items[i + 1].AsInt64() : null;
        }
        return null;
    }

    private static bool IsProtocolKey(RespValue key) =>
        key.Type is RespType.SimpleString or RespType.BlobString
        && string.Equals(key.AsString(), "proto", StringComparison.Ordinal);

    private static void EnsureSupported(ValkeyCommand command)
    {
        if (ConnectionStateCommands.Contains(command.Name))
            throw new ValkeyUnsupportedCommandException(command.Name, ExplainRejection(command.Name));
        if (IsClientReply(command))
            throw new ValkeyUnsupportedCommandException("CLIENT REPLY", ExplainRejection("CLIENT REPLY"));
    }

    private static bool IsClientReply(ValkeyCommand command) =>
        string.Equals(command.Name, "CLIENT", StringComparison.Ordinal)
        && command.ArgumentsSpan.Length > 0
        && Ascii.EqualsIgnoreCase(command.ArgumentsSpan[0].Bytes.Span, "REPLY"u8);

    private static string ExplainRejection(string name) =>
        name switch
        {
            "HELLO" => "the handshake belongs to ConnectAsync, and re-running it would leave "
                + $"{nameof(NegotiatedProtocol)}, the selected database, and the authenticated user misreported",
            "RESET" => "it discards the protocol, database, and authentication state established by ConnectAsync",
            "MONITOR" => "it turns the connection into an unsolicited stream of server events",
            "CLIENT REPLY" => "OFF and SKIP suppress replies, leaving the reader waiting for a frame "
                + "that never arrives",
            _ => "the subscribe family replies with push frames and puts the connection into subscriber mode, "
                + "which this client does not implement",
        };

    private async Task<RespValue> SendAndReadAsync(ValkeyCommand command, CancellationToken cancellationToken)
    {
        var payload = RespWriter.Encode(command);
        await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        return await ReadNonPushAsync(cancellationToken).ConfigureAwait(false);
    }

    private void StartResponseLoop() => _responseLoop = ReadResponsesAsync();

    private async Task<RespValue> SendMultiplexedAsync(
        byte[] payload,
        TimeSpan? timeout,
        CancellationToken cancellationToken
    )
    {
        var startedAt = Stopwatch.GetTimestamp();
        using var timeoutCancellation = timeout is { } duration ? new CancellationTokenSource(duration) : null;
        using var admissionCancellation = timeoutCancellation is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellation.Token);
        var admissionToken = admissionCancellation?.Token ?? cancellationToken;
        var pending = new TaskCompletionSource<RespValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationTokenRegistration cancellationRegistration = default;
        var capacityAcquired = false;
        var enqueued = false;
        var writeGateAcquired = false;

        try
        {
            await _writeGate.WaitAsync(admissionToken).ConfigureAwait(false);
            writeGateAcquired = true;
            await _pendingCapacity.WaitAsync(admissionToken).ConfigureAwait(false);
            capacityAcquired = true;
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            timeoutCancellation?.Token.ThrowIfCancellationRequested();

            _pendingResponses.Enqueue(pending);
            enqueued = true;
            if (cancellationToken.CanBeCanceled)
            {
                cancellationRegistration = cancellationToken.Register(
                    static state => ((SinglePendingCancellation)state!).Cancel(),
                    new SinglePendingCancellation(this, pending, cancellationToken)
                );
            }

            try
            {
                await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
                await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                CancelPending(pending, cancellationToken);
            }
            catch (IOException exception)
            {
                InvalidateConnection(new ValkeyConnectionException("The Valkey connection failed.", exception));
            }
            catch (SocketException exception)
            {
                InvalidateConnection(new ValkeyConnectionException("The Valkey connection failed.", exception));
            }
            catch (ObjectDisposedException) when (Volatile.Read(ref _disposed) != 0)
            {
                // Disposal has already completed every pending response with its terminal failure.
            }
        }
        catch (OperationCanceledException)
            when (!enqueued
                && timeoutCancellation?.IsCancellationRequested == true
                && !cancellationToken.IsCancellationRequested
            )
        {
            throw new ValkeyCommandTimeoutException(timeout!.Value, ValkeyCommandDeliveryStatus.NotSent);
        }
        finally
        {
            if (!enqueued && capacityAcquired)
                _pendingCapacity.Release();
            if (writeGateAcquired)
                _writeGate.Release();
        }

        try
        {
            return timeout is { } operationTimeout
                ? await AwaitWithDeadlineAsync(pending.Task, operationTimeout, startedAt).ConfigureAwait(false)
                : await pending.Task.ConfigureAwait(false);
        }
        finally
        {
            await cancellationRegistration.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task<IReadOnlyList<RespValue>> SendMultiplexedAsync(
        byte[][] payloads,
        TimeSpan? timeout,
        CancellationToken cancellationToken
    )
    {
        var startedAt = Stopwatch.GetTimestamp();
        using var timeoutCancellation = timeout is { } duration ? new CancellationTokenSource(duration) : null;
        using var admissionCancellation = timeoutCancellation is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellation.Token);
        var admissionToken = admissionCancellation?.Token ?? cancellationToken;
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var pending = new TaskCompletionSource<RespValue>[payloads.Length];
        for (var i = 0; i < pending.Length; i++)
            pending[i] = new TaskCompletionSource<RespValue>(TaskCreationOptions.RunContinuationsAsynchronously);

        CancellationTokenRegistration cancellationRegistration = default;
        var capacityAcquired = 0;
        var enqueued = false;
        var writeGateAcquired = false;
        try
        {
            await _writeGate.WaitAsync(admissionToken).ConfigureAwait(false);
            writeGateAcquired = true;
            for (; capacityAcquired < pending.Length; capacityAcquired++)
                await _pendingCapacity.WaitAsync(admissionToken).ConfigureAwait(false);
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            timeoutCancellation?.Token.ThrowIfCancellationRequested();

            foreach (var response in pending)
                _pendingResponses.Enqueue(response);
            enqueued = true;
            if (cancellationToken.CanBeCanceled)
            {
                cancellationRegistration = cancellationToken.Register(
                    static state => ((PendingCancellation)state!).Cancel(),
                    new PendingCancellation(this, pending, cancellationToken)
                );
            }

            try
            {
                foreach (var payload in payloads)
                    await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
                await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                CancelPending(pending, cancellationToken);
            }
            catch (IOException exception)
            {
                InvalidateConnection(new ValkeyConnectionException("The Valkey connection failed.", exception));
            }
            catch (SocketException exception)
            {
                InvalidateConnection(new ValkeyConnectionException("The Valkey connection failed.", exception));
            }
            catch (ObjectDisposedException) when (Volatile.Read(ref _disposed) != 0)
            {
                // Disposal has already completed every pending response with its terminal failure.
            }
        }
        catch (OperationCanceledException)
            when (!enqueued
                && timeoutCancellation?.IsCancellationRequested == true
                && !cancellationToken.IsCancellationRequested
            )
        {
            throw new ValkeyCommandTimeoutException(timeout!.Value, ValkeyCommandDeliveryStatus.NotSent);
        }
        finally
        {
            if (!enqueued && capacityAcquired > 0)
                _pendingCapacity.Release(capacityAcquired);
            if (writeGateAcquired)
                _writeGate.Release();
        }

        try
        {
            if (timeout is { } operationTimeout)
            {
                var responseTasks = new Task<RespValue>[pending.Length];
                for (var i = 0; i < pending.Length; i++)
                    responseTasks[i] = pending[i].Task;
                return await AwaitWithDeadlineAsync(Task.WhenAll(responseTasks), operationTimeout, startedAt)
                    .ConfigureAwait(false);
            }

            var responses = new RespValue[pending.Length];
            for (var i = 0; i < pending.Length; i++)
                responses[i] = await pending[i].Task.ConfigureAwait(false);
            return responses;
        }
        finally
        {
            await cancellationRegistration.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task ReadResponsesAsync()
    {
        try
        {
            while (true)
            {
                var response = await ReadNonPushAsync(_readerShutdown.Token).ConfigureAwait(false);
                if (!_pendingResponses.TryDequeue(out var pending))
                {
                    if (Volatile.Read(ref _disposed) != 0)
                        return;
                    throw new ValkeyProtocolException("The server returned a reply with no pending command.");
                }
                _pendingCapacity.Release();
                pending.TrySetResult(response);
            }
        }
        catch (OperationCanceledException) when (_readerShutdown.IsCancellationRequested)
        {
            // Connection disposal owns completion of every request still in the queue.
        }
        catch (ObjectDisposedException) when (Volatile.Read(ref _disposed) != 0)
        {
            // Disposing the stream may win the race with cancellation of its active read.
        }
        catch (IOException exception)
        {
            InvalidateConnection(new ValkeyConnectionException("The Valkey connection failed.", exception));
        }
        catch (SocketException exception)
        {
            InvalidateConnection(new ValkeyConnectionException("The Valkey connection failed.", exception));
        }
        catch (ValkeyProtocolException exception)
        {
            InvalidateConnection(exception);
        }
        catch (Exception exception)
        {
            InvalidateConnection(new ValkeyConnectionException("The Valkey response reader failed.", exception));
        }
    }

    private async Task<RespValue> ReadNonPushAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            var response = await _reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (response.Type != RespType.Push)
                return response;
            try
            {
                PushReceived?.Invoke(response);
            }
            catch
            { /* User callbacks cannot be allowed to corrupt the wire state. */
            }
        }
    }

    private void CancelPending(TaskCompletionSource<RespValue>[] pending, CancellationToken cancellationToken)
    {
        var cancelledAny = false;
        foreach (var response in pending)
            cancelledAny |= response.TrySetException(new ValkeyCommandCanceledException(cancellationToken));
        if (cancelledAny)
        {
            InvalidateConnection(
                new ValkeyConnectionException(
                    "A cancelled command made the Valkey connection unusable.",
                    new OperationCanceledException(cancellationToken)
                )
            );
        }
    }

    private void CancelPending(TaskCompletionSource<RespValue> pending, CancellationToken cancellationToken)
    {
        if (pending.TrySetException(new ValkeyCommandCanceledException(cancellationToken)))
        {
            InvalidateConnection(
                new ValkeyConnectionException(
                    "A cancelled command made the Valkey connection unusable.",
                    new OperationCanceledException(cancellationToken)
                )
            );
        }
    }

    internal static TimeSpan ValidateOperationTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero || timeout.TotalMilliseconds > MaxOperationTimeoutMilliseconds)
            throw new ArgumentOutOfRangeException(nameof(timeout));
        return timeout;
    }

    private async Task<T> AwaitWithDeadlineAsync<T>(Task<T> response, TimeSpan timeout, long startedAt)
    {
        var remaining = timeout - Stopwatch.GetElapsedTime(startedAt);
        if (remaining <= TimeSpan.Zero)
        {
            _ = DrainTimedOutResponseAsync(response);
            throw new ValkeyCommandTimeoutException(timeout, ValkeyCommandDeliveryStatus.MayHaveBeenSent);
        }

        try
        {
            return await response.WaitAsync(remaining).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _ = DrainTimedOutResponseAsync(response);
            throw new ValkeyCommandTimeoutException(timeout, ValkeyCommandDeliveryStatus.MayHaveBeenSent);
        }
    }

    private async Task DrainTimedOutResponseAsync(Task response)
    {
        try
        {
            await response.WaitAsync(_options.ResponseDrainTimeout, _readerShutdown.Token).ConfigureAwait(false);
            return;
        }
        catch (TimeoutException)
        {
            if (!response.IsCompleted)
            {
                InvalidateConnection(
                    new ValkeyConnectionException(
                        "The Valkey connection was terminated because a timed-out command reply did not arrive.",
                        new TimeoutException(
                            $"The retained response did not drain within {_options.ResponseDrainTimeout}."
                        )
                    )
                );
            }
        }
        catch (OperationCanceledException) when (_readerShutdown.IsCancellationRequested)
        {
            // Connection termination settles the retained response below.
        }
        catch
        {
            // The retained response reached another terminal state and its failure is now observed.
            return;
        }

        try
        {
            await response.ConfigureAwait(false);
        }
        catch
        {
            // The caller already observed its deadline. This continuation observes the eventual
            // terminal failure after connection invalidation settles the retained FIFO entry.
        }
    }

    private void InvalidateConnection(Exception failure)
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _readerShutdown.Cancel();
        try
        {
            _stream.Dispose();
        }
        finally
        {
            _tcpClient.Dispose();
            while (_pendingResponses.TryDequeue(out var pending))
            {
                _pendingCapacity.Release();
                pending.TrySetException(failure);
            }
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    private sealed class PendingCancellation(
        ValkeyClient client,
        TaskCompletionSource<RespValue>[] pending,
        CancellationToken cancellationToken
    )
    {
        public void Cancel() => client.CancelPending(pending, cancellationToken);
    }

    private sealed class SinglePendingCancellation(
        ValkeyClient client,
        TaskCompletionSource<RespValue> pending,
        CancellationToken cancellationToken
    )
    {
        public void Cancel() => client.CancelPending(pending, cancellationToken);
    }
}
