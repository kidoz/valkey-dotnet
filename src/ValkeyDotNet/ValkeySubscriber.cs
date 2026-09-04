using System.Diagnostics;
using System.Net.Security;
using System.Net.Sockets;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet;

/// <summary>
/// A dedicated RESP2/RESP3 channel and pattern subscriber. Connection loss is terminal; this version
/// does not reconnect, restore subscriptions, track keys, or route sharded subscriptions.
/// </summary>
public sealed class ValkeySubscriber : IAsyncDisposable
{
    private sealed class Registration(byte[] name, bool pattern)
    {
        internal byte[] Name { get; } = name;
        internal bool Pattern { get; } = pattern;
        internal List<ValkeySubscription> Handles { get; } = [];
    }

    private sealed class Pending(string kind, byte[] name, Action confirm)
    {
        internal string Kind { get; } = kind;
        internal byte[] Name { get; } = name;
        internal Action Confirm { get; } = confirm;
        internal TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private readonly ValkeySubscriberOptions _options;
    private readonly TcpClient _tcp;
    private readonly Stream _stream;
    private readonly RespReader _reader;
    private readonly object _sync = new();
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Dictionary<string, Registration> _registrations = new(StringComparer.Ordinal);
    private Task _readLoop = Task.CompletedTask;
    private Pending? _pending;
    private Exception? _failure;
    private bool _closed;
    private int _handles;
    private int _operations;
    private long _dropped;

    private ValkeySubscriber(ValkeySubscriberOptions options, TcpClient tcp, Stream stream)
    {
        _options = options;
        _tcp = tcp;
        _stream = stream;
        var connection = options.Connection;
        _reader = new RespReader(
            stream,
            connection.MaxResponseBytes,
            connection.MaxResponseElements,
            connection.MaxNestingDepth
        );
    }

    public ValkeyProtocol NegotiatedProtocol { get; private set; }
    public bool IsConnected
    {
        get
        {
            lock (_sync)
            {
                return !_closed;
            }
        }
    }

    /// <summary>The terminal failure, or null after normal disposal. No automatic reconnect occurs.</summary>
    public Exception? Failure
    {
        get
        {
            lock (_sync)
            {
                return _failure;
            }
        }
    }

    /// <summary>Completes normally when the socket reader stops; inspect Failure for the terminal cause.</summary>
    public Task Completion => _readLoop;

    /// <summary>Total dropped local deliveries across all handles, including disposed handles.</summary>
    public long DroppedMessages => Interlocked.Read(ref _dropped);

    public static async Task<ValkeySubscriber> ConnectAsync(
        ValkeySubscriberOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        options ??= new();
        options.Validate();
        var connection = options.Connection;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(connection.ConnectTimeout);
        var tcp = new TcpClient { NoDelay = true };
        Stream? stream = null;
        try
        {
            await tcp.ConnectAsync(connection.Host, connection.Port, timeout.Token).ConfigureAwait(false);
            stream = tcp.GetStream();
            if (connection.UseTls)
            {
                var ssl = connection.CertificateValidationCallback is null
                    ? new SslStream(stream, false)
                    : new SslStream(stream, false, connection.CertificateValidationCallback);
                stream = ssl;
                await ssl.AuthenticateAsClientAsync(
                        new SslClientAuthenticationOptions { TargetHost = connection.Host },
                        timeout.Token
                    )
                    .ConfigureAwait(false);
            }
            var subscriber = new ValkeySubscriber(options, tcp, stream);
            await subscriber.InitializeAsync(timeout.Token).ConfigureAwait(false);
            subscriber._readLoop = subscriber.ReadAsync();
            return subscriber;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            if (stream is not null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }

            tcp.Dispose();
            throw new TimeoutException("The subscriber connection timed out.");
        }
        catch
        {
            if (stream is not null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }

            tcp.Dispose();
            throw;
        }
    }

    private async Task InitializeAsync(CancellationToken token)
    {
        var connection = _options.Connection;
        var arguments = new List<ValkeyArgument> { (int)connection.Protocol };
        if (connection.Password is not null)
        {
            arguments.Add("AUTH");
            arguments.Add(connection.Username ?? "default");
            arguments.Add(connection.Password);
        }
        if (connection.ClientName is not null)
        {
            arguments.Add("SETNAME");
            arguments.Add(connection.ClientName);
        }
        var hello = await HandshakeCommandAsync(new ValkeyCommand("HELLO", arguments.ToArray()), token)
            .ConfigureAwait(false);
        if (hello.Type is not (RespType.Map or RespType.Array))
        {
            throw new ValkeyProtocolException("Unexpected subscriber handshake frame.");
        }

        NegotiatedProtocol = ValkeyClient.ReadNegotiatedProtocol(hello);
        if (connection.Database != 0)
        {
            var select = await HandshakeCommandAsync(new ValkeyCommand("SELECT", connection.Database), token)
                .ConfigureAwait(false);
            if (select.Type != RespType.SimpleString || !select.AsBytes().Span.SequenceEqual("OK"u8))
            {
                throw new ValkeyProtocolException("Unexpected subscriber database acknowledgement.");
            }
        }
    }

    private async Task<RespValue> HandshakeCommandAsync(ValkeyCommand command, CancellationToken token)
    {
        await _stream.WriteAsync(RespWriter.Encode(command), token).ConfigureAwait(false);
        await _stream.FlushAsync(token).ConfigureAwait(false);
        var reply = await _reader.ReadAsync(token).ConfigureAwait(false);
        ThrowServerError(reply);
        return reply;
    }

    public Task<ValkeySubscription> SubscribeAsync(
        ValkeyArgument channel,
        CancellationToken cancellationToken = default
    ) => SubscribeCoreAsync(channel, false, cancellationToken);

    public Task<ValkeySubscription> SubscribePatternAsync(
        ValkeyArgument pattern,
        CancellationToken cancellationToken = default
    ) => SubscribeCoreAsync(pattern, true, cancellationToken);

    private async Task<ValkeySubscription> SubscribeCoreAsync(
        ValkeyArgument name,
        bool pattern,
        CancellationToken cancellationToken
    )
    {
        if (name.Bytes.Length > _options.MaxChannelBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(name));
        }

        var started = await EnterAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Copy only after bounded admission. Never retain mutable caller storage.
            var bytes = name.Bytes.ToArray();
            var key = Key(bytes, pattern);
            ValkeySubscription handle;
            Registration registration;
            lock (_sync)
            {
                ThrowIfClosed();
                Remaining(started, cancellationToken);
                if (_handles >= _options.MaxSubscriptions)
                {
                    throw new ValkeyCapacityException("The subscriber's subscription capacity is full.");
                }

                handle = new ValkeySubscription(this, key, _options.QueueCapacity);
                if (_registrations.TryGetValue(key, out var existing))
                {
                    existing.Handles.Add(handle);
                    _handles++;
                    return handle;
                }
                registration = new Registration(bytes, pattern);
            }
            await ChangeAsync(
                    pattern ? "psubscribe" : "subscribe",
                    registration,
                    () =>
                    {
                        registration.Handles.Add(handle);
                        _registrations.Add(key, registration);
                        _handles++;
                    },
                    started,
                    cancellationToken
                )
                .ConfigureAwait(false);
            return handle;
        }
        finally
        {
            Exit();
        }
    }

    internal async Task UnsubscribeAsync(ValkeySubscription handle, CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (handle.Removed)
            {
                return;
            }
        }

        long started;
        try
        {
            started = await EnterAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            lock (_sync)
            {
                if (handle.Removed)
                {
                    return;
                }
            }

            throw;
        }
        try
        {
            Registration registration;
            lock (_sync)
            {
                if (handle.Removed)
                {
                    return;
                }

                ThrowIfClosed();
                Remaining(started, cancellationToken);
                registration = _registrations[handle.Key];
                if (registration.Handles.Count > 1)
                {
                    Remove(handle, registration);
                    return;
                }
            }
            await ChangeAsync(
                    registration.Pattern ? "punsubscribe" : "unsubscribe",
                    registration,
                    () => Remove(handle, registration),
                    started,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        finally
        {
            Exit();
        }
    }

    private void Remove(ValkeySubscription handle, Registration registration)
    {
        registration.Handles.Remove(handle);
        if (registration.Handles.Count == 0)
        {
            _registrations.Remove(handle.Key);
        }

        _handles--;
        handle.Removed = true;
        handle.Complete(null);
    }

    private async Task<long> EnterAsync(CancellationToken token)
    {
        var started = Stopwatch.GetTimestamp();
        token.ThrowIfCancellationRequested();
        lock (_sync)
        {
            ThrowIfClosed();
            if (_operations >= _options.MaxConcurrentOperations)
            {
                throw new ValkeyCapacityException("The subscriber's operation capacity is full.");
            }

            _operations++;
        }
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token, _shutdown.Token);
        timeout.CancelAfter(_options.OperationTimeout);
        try
        {
            await _gate.WaitAsync(timeout.Token).ConfigureAwait(false);
            return started;
        }
        catch (OperationCanceledException)
        {
            lock (_sync)
            {
                _operations--;
            }

            token.ThrowIfCancellationRequested();
            lock (_sync)
            {
                ThrowIfClosed();
            }

            throw new ValkeyCommandTimeoutException(_options.OperationTimeout, ValkeyCommandDeliveryStatus.NotSent);
        }
    }

    private TimeSpan Remaining(long started, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        var remaining = _options.OperationTimeout - Stopwatch.GetElapsedTime(started);
        if (remaining <= TimeSpan.Zero)
        {
            throw new ValkeyCommandTimeoutException(_options.OperationTimeout, ValkeyCommandDeliveryStatus.NotSent);
        }

        return remaining;
    }

    private void Exit()
    {
        _gate.Release();
        lock (_sync)
        {
            _operations--;
        }
    }

    private async Task ChangeAsync(
        string kind,
        Registration registration,
        Action confirm,
        long started,
        CancellationToken cancellationToken
    )
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        timeout.CancelAfter(Remaining(started, cancellationToken));
        var pending = new Pending(kind, registration.Name, confirm);
        lock (_sync)
        {
            ThrowIfClosed();
            cancellationToken.ThrowIfCancellationRequested();
            _pending = pending;
        }
        try
        {
            await _stream
                .WriteAsync(RespWriter.Encode(new ValkeyCommand(kind, registration.Name)), timeout.Token)
                .ConfigureAwait(false);
            await _stream.FlushAsync(timeout.Token).ConfigureAwait(false);
            await pending.Completion.Task.WaitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            lock (_sync)
            {
                if (_closed && !cancellationToken.IsCancellationRequested)
                {
                    throw _failure ?? new ObjectDisposedException(nameof(ValkeySubscriber));
                }
            }

            Exception error = cancellationToken.IsCancellationRequested
                ? new ValkeyCommandCanceledException(cancellationToken)
                : new ValkeyCommandTimeoutException(
                    _options.OperationTimeout,
                    ValkeyCommandDeliveryStatus.MayHaveBeenSent
                );
            Close(error);
            throw error;
        }
        catch (Exception error) when (error is IOException or SocketException)
        {
            var failure = new ValkeyConnectionException("The subscriber transport failed.", error);
            Close(failure);
            throw failure;
        }
        finally
        {
            // A terminal close may fault the acknowledgement after cancellation stopped its waiter.
            _ = pending.Completion.Task.Exception;
        }
    }

    private async Task ReadAsync()
    {
        try
        {
            while (true)
            {
                var frame = await _reader.ReadAsync(_shutdown.Token).ConfigureAwait(false);
                lock (_sync)
                {
                    if (_closed)
                    {
                        return;
                    }

                    if (frame.Type is RespType.SimpleError or RespType.BlobError)
                    {
                        if (_pending is null)
                        {
                            throw new ValkeyProtocolException("Unsolicited subscriber error.");
                        }

                        var rejected = _pending;
                        _pending = null;
                        try
                        {
                            ThrowServerError(frame);
                        }
                        catch (ValkeyServerException error)
                        {
                            rejected.Completion.TrySetException(error);
                        }
                        continue;
                    }
                    ProcessFrame(frame);
                }
            }
        }
        catch (Exception error)
        {
            Close(
                error is IOException or SocketException
                    ? new ValkeyConnectionException("The subscriber transport failed.", error)
                    : error
            );
        }
    }

    private void ProcessFrame(RespValue frame)
    {
        if (frame.Type != (NegotiatedProtocol == ValkeyProtocol.Resp3 ? RespType.Push : RespType.Array))
        {
            throw new ValkeyProtocolException("Unexpected subscriber frame type.");
        }

        var items = frame.AsArray();
        if (items.Count < 3 || items[0].Type != RespType.BlobString)
        {
            throw new ValkeyProtocolException("Malformed subscriber frame.");
        }

        var kindBytes = items[0].AsBytes();
        if (kindBytes.Length > 12)
        {
            throw new ValkeyProtocolException("Unknown subscriber frame kind.");
        }

        var kind = items[0].AsString();
        if (kind is "subscribe" or "psubscribe" or "unsubscribe" or "punsubscribe")
        {
            var pending = _pending;
            if (
                items.Count != 3
                || items[1].Type != RespType.BlobString
                || items[2].Type != RespType.Integer
                || items[2].AsInt64() < 0
                || pending is null
                || kind != pending.Kind
                || !items[1].AsBytes().Span.SequenceEqual(pending.Name)
            )
            {
                throw new ValkeyProtocolException("Mismatched subscription acknowledgement.");
            }

            var expectedCount = _registrations.Count + (kind is "subscribe" or "psubscribe" ? 1 : -1);
            if (items[2].AsInt64() != expectedCount)
            {
                throw new ValkeyProtocolException("Mismatched subscription count.");
            }

            pending.Confirm();
            _pending = null;
            pending.Completion.TrySetResult();
            return;
        }
        var pattern = kind == "pmessage";
        if (
            kind != "message" && !pattern
            || items.Count != (pattern ? 4 : 3)
            || items.Skip(1).Any(item => item.Type != RespType.BlobString)
        )
        {
            throw new ValkeyProtocolException("Malformed subscriber delivery.");
        }

        var name = items[1].AsBytes();
        if (
            name.Length > _options.MaxChannelBytes
            || !_registrations.TryGetValue(Key(name.Span, pattern), out var registration)
        )
        {
            throw new ValkeyProtocolException("Delivery has no confirmed subscription.");
        }

        var message = new ValkeyPubSubMessage(
            items[pattern ? 2 : 1].AsBytes(),
            items[pattern ? 3 : 2].AsBytes(),
            pattern ? name : (ReadOnlyMemory<byte>?)null
        );
        foreach (var handle in registration.Handles)
        {
            if (!handle.Deliver(message))
            {
                Interlocked.Increment(ref _dropped);
            }
        }
    }

    private static string Key(ReadOnlySpan<byte> name, bool pattern) =>
        (pattern ? "p" : "c") + Convert.ToBase64String(name);

    private static void ThrowServerError(RespValue frame)
    {
        if (frame.Type is not (RespType.SimpleError or RespType.BlobError))
        {
            return;
        }
        // Preserve only known protocol error categories, never arbitrary server text or payloads.
        var bytes = frame.AsBytes().Span;
        var separator = bytes.IndexOf((byte)' ');
        var code = separator < 0 ? bytes : bytes[..separator];
        var category =
            code.SequenceEqual("NOAUTH"u8) ? "NOAUTH"
            : code.SequenceEqual("WRONGPASS"u8) ? "WRONGPASS"
            : code.SequenceEqual("NOPERM"u8) ? "NOPERM"
            : "ERR";
        throw new ValkeyServerException(category + " Subscriber command rejected.");
    }

    private void ThrowIfClosed()
    {
        ObjectDisposedException.ThrowIf(_closed, this);
    }

    private void Close(Exception? error)
    {
        lock (_sync)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
            _failure = error;
            _pending?.Completion.TrySetException(error ?? new ObjectDisposedException(nameof(ValkeySubscriber)));
            _pending = null;
            foreach (var registration in _registrations.Values)
            {
                foreach (var handle in registration.Handles)
                {
                    handle.Removed = true;
                    handle.Complete(error);
                }
            }

            _registrations.Clear();
            _handles = 0;
        }
        _shutdown.Cancel();
        _stream.Dispose();
        _tcp.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        Close(null);
        await _readLoop.ConfigureAwait(false);
    }
}
