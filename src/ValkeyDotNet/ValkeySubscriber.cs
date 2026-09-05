using System.Diagnostics;
using System.Net.Sockets;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet;

/// <summary>
/// A dedicated RESP2/RESP3 channel and pattern subscriber with optional bounded restoration.
/// Sharded mode is opt-in and separate from global channels/patterns. Tracking uses a separate client.
/// </summary>
public sealed partial class ValkeySubscriber : IAsyncDisposable
{
    private sealed class Registration(byte[] name, bool pattern)
    {
        internal byte[] Name { get; } = name;
        internal bool Pattern { get; } = pattern;
        internal List<ValkeySubscription> Handles { get; } = [];
    }

    private sealed class Pending(string kind, Registration registration, Action confirm)
    {
        internal string Kind { get; } = kind;
        internal byte[] Name => Registration.Name;
        internal Registration Registration { get; } = registration;
        internal Action Confirm { get; } = confirm;
        internal TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private readonly ValkeySubscriberOptions _options;
    private Connection _connection;
    private readonly object _sync = new();
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Dictionary<string, Registration> _registrations = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Registration> _confirmed = new(StringComparer.Ordinal);
    private Task _readLoop = Task.CompletedTask;
    private Pending? _pending;
    private Exception? _failure;
    private bool _closed;
    private bool _recovering;
    private bool _connectionLossObserved;
    private Exception? _lastConnectionFailure;
    private long _connectionLosses;
    private long _reconnectAttempts;
    private long _successfulReconnects;
    private int _handles;
    private int _operations;
    private long _dropped;

    private ValkeySubscriber(ValkeySubscriberOptions options, Connection connection)
    {
        _options = options;
        _connection = connection;
    }

    public ValkeyProtocol NegotiatedProtocol
    {
        get
        {
            lock (_sync)
            {
                return _connection.Protocol;
            }
        }
    }

    public bool IsConnected
    {
        get
        {
            lock (_sync)
            {
                return !_closed && !_recovering;
            }
        }
    }

    /// <summary>The terminal failure, or null before terminal failure or after normal disposal.</summary>
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

    /// <summary>Completes normally after the reader and any recovery stop; inspect Failure for the terminal cause.</summary>
    public Task Completion => _readLoop;

    /// <summary>Total dropped local deliveries across all handles, including disposed handles.</summary>
    public long DroppedMessages => Interlocked.Read(ref _dropped);

    public bool IsReconnecting
    {
        get
        {
            lock (_sync)
            {
                return !_closed && _recovering;
            }
        }
    }

    /// <summary>Observed transport-loss intervals. Messages missed in these intervals cannot be counted or replayed.</summary>
    public long ConnectionLosses => Interlocked.Read(ref _connectionLosses);
    public long ReconnectAttempts => Interlocked.Read(ref _reconnectAttempts);
    public long SuccessfulReconnects => Interlocked.Read(ref _successfulReconnects);

    public static async Task<ValkeySubscriber> ConnectAsync(
        ValkeySubscriberOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        options ??= new();
        options.Validate();
        var connection = await Connection.OpenAsync(options.Connection, cancellationToken).ConfigureAwait(false);
        var subscriber = new ValkeySubscriber(options, connection);
        subscriber._readLoop = subscriber.ReadAsync();
        return subscriber;
    }

    public Task<ValkeySubscription> SubscribeAsync(
        ValkeyArgument channel,
        CancellationToken cancellationToken = default
    ) => SubscribeModeAsync(channel, false, false, cancellationToken);

    public Task<ValkeySubscription> SubscribePatternAsync(
        ValkeyArgument pattern,
        CancellationToken cancellationToken = default
    ) => SubscribeModeAsync(pattern, true, false, cancellationToken);

    /// <summary>Subscribes to one binary shard channel; requires UseShardedPubSub and a correctly routed node.</summary>
    public Task<ValkeySubscription> SubscribeShardedAsync(
        ValkeyArgument channel,
        CancellationToken cancellationToken = default
    ) => SubscribeModeAsync(channel, false, true, cancellationToken);

    private Task<ValkeySubscription> SubscribeModeAsync(
        ValkeyArgument name,
        bool pattern,
        bool sharded,
        CancellationToken cancellationToken
    )
    {
        if (sharded != _options.UseShardedPubSub)
        {
            throw new InvalidOperationException("Global and sharded subscriptions require separate subscriber modes.");
        }
        return SubscribeCoreAsync(name, pattern, cancellationToken);
    }

    private string SubscribeKind(bool pattern) =>
        _options.UseShardedPubSub ? "ssubscribe"
        : pattern ? "psubscribe"
        : "subscribe";

    private string UnsubscribeKind(bool pattern) =>
        _options.UseShardedPubSub ? "sunsubscribe"
        : pattern ? "punsubscribe"
        : "unsubscribe";

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

        lock (_sync)
        {
            ThrowIfClosed();
            ThrowIfRecovering();
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
                ThrowIfRecovering();
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
                    SubscribeKind(pattern),
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
            if (_recovering)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Remove(handle, _registrations[handle.Key]);
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
                if (_recovering || registration.Handles.Count > 1)
                {
                    Remove(handle, registration);
                    return;
                }
            }
            await ChangeAsync(
                    UnsubscribeKind(registration.Pattern),
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
        var pending = new Pending(kind, registration, confirm);
        Connection connection;
        lock (_sync)
        {
            ThrowIfClosed();
            ThrowIfRecovering();
            cancellationToken.ThrowIfCancellationRequested();
            connection = _connection;
            _pending = pending;
        }
        try
        {
            await connection
                .Stream.WriteAsync(RespWriter.Encode(new ValkeyCommand(kind, registration.Name)), timeout.Token)
                .ConfigureAwait(false);
            await connection.Stream.FlushAsync(timeout.Token).ConfigureAwait(false);
            await pending.Completion.Task.WaitAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (Exception) when (pending.Completion.Task.IsCompleted)
        {
            // The reader can process a reply and then close/cancel the socket before this writer
            // resumes. Preserve that settled outcome, including a sanitized server rejection,
            // instead of replacing it with the later flush failure or shutdown cancellation.
            await pending.Completion.Task.ConfigureAwait(false);
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
            if (!Disconnect(connection, failure))
            {
                Close(failure);
            }
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
        while (true)
        {
            var connection = _connection;
            try
            {
                while (true)
                {
                    var frame = await connection.Reader.ReadAsync(_shutdown.Token).ConfigureAwait(false);
                    ProcessResponse(frame);
                }
            }
            catch (Exception error)
            {
                var failure = error is IOException or SocketException
                    ? new ValkeyConnectionException("The subscriber transport failed.", error)
                    : error;
                if (
                    (error is IOException or SocketException || error is ObjectDisposedException && IsReconnecting)
                    && Disconnect(connection, failure)
                )
                {
                    if (await RecoverAsync().ConfigureAwait(false))
                    {
                        continue;
                    }
                }
                else
                {
                    Close(failure);
                }
                return;
            }
        }
    }

    private void ProcessResponse(RespValue frame, bool restoring = false)
    {
        lock (_sync)
        {
            if (_closed)
            {
                return;
            }
            if (_recovering && !restoring)
            {
                throw new IOException("The subscriber connection was retired.");
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
                    ThrowServerError(frame, _options.UseShardedPubSub);
                }
                catch (ValkeyServerException error)
                {
                    rejected.Completion.TrySetException(error);
                }
                return;
            }
            ProcessFrame(frame);
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
        if (_options.UseShardedPubSub && kind == "sunsubscribe" && _pending?.Kind != "sunsubscribe")
        {
            if (
                items.Count != 3
                || items[1].Type != RespType.BlobString
                || items[2].Type != RespType.Integer
                || items[1].AsBytes().Length > _options.MaxChannelBytes
                || items[2].AsInt64() != _confirmed.Count - 1
                || !_confirmed.ContainsKey(Key(items[1].AsBytes().Span, false))
            )
            {
                throw new ValkeyProtocolException("Malformed unsolicited shard unsubscription.");
            }
            throw new ValkeyClusterException(
                "The server removed a shard subscription; refresh topology and subscribe again."
            );
        }
        if (kind is "subscribe" or "psubscribe" or "unsubscribe" or "punsubscribe" or "ssubscribe" or "sunsubscribe")
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

            var subscribing = kind is "subscribe" or "psubscribe" or "ssubscribe";
            var expectedCount = _confirmed.Count + (subscribing ? 1 : -1);
            if (items[2].AsInt64() != expectedCount)
            {
                throw new ValkeyProtocolException("Mismatched subscription count.");
            }

            pending.Confirm();
            var key = Key(pending.Name, pending.Registration.Pattern);
            if (subscribing)
            {
                _confirmed.Add(key, pending.Registration);
            }
            else
            {
                _confirmed.Remove(key);
            }
            _pending = null;
            pending.Completion.TrySetResult();
            return;
        }
        var pattern = kind == "pmessage";
        if (
            (_options.UseShardedPubSub ? kind != "smessage" : kind != "message" && !pattern)
            || items.Count != (pattern ? 4 : 3)
            || items.Skip(1).Any(item => item.Type != RespType.BlobString)
        )
        {
            throw new ValkeyProtocolException("Malformed subscriber delivery.");
        }

        var name = items[1].AsBytes();
        if (
            name.Length > _options.MaxChannelBytes
            || !_confirmed.TryGetValue(Key(name.Span, pattern), out var registration)
        )
        {
            throw new ValkeyProtocolException("Delivery has no confirmed subscription.");
        }

        var message = new ValkeyPubSubMessage(
            items[pattern ? 2 : 1].AsBytes(),
            items[pattern ? 3 : 2].AsBytes(),
            pattern ? name : (ReadOnlyMemory<byte>?)null,
            _options.UseShardedPubSub
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

    private static void ThrowServerError(RespValue frame, bool sharded = false)
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
            : sharded && code.SequenceEqual("MOVED"u8) ? "MOVED"
            : sharded && code.SequenceEqual("ASK"u8) ? "ASK"
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
            _confirmed.Clear();
            _handles = 0;
        }
        _shutdown.Cancel();
        _connection.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        Close(null);
        await _readLoop.ConfigureAwait(false);
    }
}
