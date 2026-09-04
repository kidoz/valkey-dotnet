using System.Collections.Frozen;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using ValkeyDotNet.Internal;

namespace ValkeyDotNet;

/// <summary>
/// An asynchronous, dependency-free client for a single Valkey node.
/// The connection is safe for concurrent callers; commands are serialized in wire order.
/// </summary>
public sealed class ValkeyClient : IAsyncDisposable
{
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
    private readonly SemaphoreSlim _commandGate = new(1, 1);
    private int _disposed;

    private ValkeyClient(ValkeyClientOptions options, TcpClient tcpClient, Stream stream)
    {
        _options = options;
        _tcpClient = tcpClient;
        _stream = stream;
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

    /// <summary>
    /// Raised when a RESP3 push frame is encountered while reading command responses.
    /// This basic client does not run an idle background reader.
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
    public async Task<RespValue> ExecuteAsync(ValkeyCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        EnsureSupported(command);
        ThrowIfDisposed();
        await _commandGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            var response = await SendAndReadAsync(command, cancellationToken).ConfigureAwait(false);
            response.ThrowIfError();
            return response;
        }
        catch (OperationCanceledException)
        {
            await InvalidateAsync().ConfigureAwait(false);
            throw;
        }
        catch (IOException exception)
        {
            await InvalidateAsync().ConfigureAwait(false);
            throw new ValkeyConnectionException("The Valkey connection failed.", exception);
        }
        catch (SocketException exception)
        {
            await InvalidateAsync().ConfigureAwait(false);
            throw new ValkeyConnectionException("The Valkey connection failed.", exception);
        }
        catch (ValkeyProtocolException)
        {
            await InvalidateAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            _commandGate.Release();
        }
    }

    /// <summary>
    /// Writes all commands before reading their replies. Error replies are returned in place so every
    /// response can be drained without losing protocol synchronization; call ThrowIfError on each result.
    /// </summary>
    public async Task<IReadOnlyList<RespValue>> ExecutePipelineAsync(
        IEnumerable<ValkeyCommand> commands,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(commands);
        var commandList = commands.ToArray();
        if (commandList.Length == 0)
            return Array.Empty<RespValue>();
        if (commandList.Any(static command => command is null))
            throw new ArgumentException("A pipeline cannot contain a null command.", nameof(commands));
        foreach (var command in commandList)
            EnsureSupported(command);

        ThrowIfDisposed();
        await _commandGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            foreach (var command in commandList)
            {
                var payload = RespWriter.Encode(command);
                await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
            }
            await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);

            var responses = new RespValue[commandList.Length];
            for (var i = 0; i < responses.Length; i++)
                responses[i] = await ReadNonPushAsync(cancellationToken).ConfigureAwait(false);
            return responses;
        }
        catch (OperationCanceledException)
        {
            await InvalidateAsync().ConfigureAwait(false);
            throw;
        }
        catch (IOException exception)
        {
            await InvalidateAsync().ConfigureAwait(false);
            throw new ValkeyConnectionException("The Valkey pipeline failed.", exception);
        }
        catch (SocketException exception)
        {
            await InvalidateAsync().ConfigureAwait(false);
            throw new ValkeyConnectionException("The Valkey pipeline failed.", exception);
        }
        catch (ValkeyProtocolException)
        {
            await InvalidateAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            _commandGate.Release();
        }
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
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        await _stream.DisposeAsync().ConfigureAwait(false);
        _tcpClient.Dispose();
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

    private async ValueTask InvalidateAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        try
        {
            await _stream.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _tcpClient.Dispose();
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
