using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Security.Authentication;
using System.Security.Cryptography;
using ValkeyDotNet.Diagnostics;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet;

/// <summary>
/// Owns and replaces a standalone physical connection. Admission is bounded; ordinary commands,
/// scripts, and pipelines are never replayed. Connection establishment is shared by concurrent callers.
/// </summary>
public sealed class ValkeyConnectionOwner : IAsyncDisposable
{
    private readonly ValkeyConnectionOwnerOptions _options;
    private readonly TrackingSession? _tracking;
    private readonly object _sync = new();
    private readonly CancellationTokenSource _shutdown = new();
    private ValkeyClient? _client;
    private Task<ValkeyClient>? _connecting;
    private Task? _disposal;
    private ExceptionDispatchInfo? _terminalFailure;
    private ValkeyConnectionState _state;
    private bool _everConnected;
    private int _activeOperations;
    private int _failures;
    private long _lastFailureAt;
    private TimeSpan _reconnectDelay;

    /// <summary>Validates settings without opening a socket. Connections are opened on demand.</summary>
    public ValkeyConnectionOwner(ValkeyConnectionOwnerOptions? options = null)
    {
        _options = options ?? new ValkeyConnectionOwnerOptions();
        _options.Validate();
    }

    internal ValkeyConnectionOwner(ValkeyConnectionOwnerOptions options, TrackingSession tracking)
        : this(options)
    {
        _tracking = tracking;
    }

    /// <summary>Lifecycle snapshot, not a guarantee that the next network operation will succeed.</summary>
    public ValkeyConnectionState State
    {
        get
        {
            lock (_sync)
                return _state == ValkeyConnectionState.Connected && _client?.IsDisposed == true
                    ? ValkeyConnectionState.Disconnected
                    : _state;
        }
    }

    public string Host => _options.Connection.Host;
    public int Port => _options.Connection.Port;

    /// <summary>Warms the connection using bounded shared connection attempts.</summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default) =>
        _ = await RunAsync(static (_, _, _) => Task.FromResult(true), null, false, "connect", cancellationToken)
            .ConfigureAwait(false);

    public Task<RespValue> ExecuteAsync(ValkeyCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        return RunAsync(
            (client, _, token) => client.ExecuteAsync(command, token),
            null,
            false,
            "command",
            cancellationToken
        );
    }

    /// <summary>One isolated deadline covers acquisition and execution. The command is not replayed.</summary>
    public Task<RespValue> ExecuteWithDeadlineAsync(
        ValkeyCommand command,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(command);
        return RunAsync(
            (client, remaining, token) => client.ExecuteWithDeadlineAsync(command, remaining!.Value, token),
            ValkeyClient.ValidateOperationTimeout(timeout),
            false,
            "command",
            cancellationToken
        );
    }

    /// <summary>
    /// Authorizes replay after an ambiguous transport failure, up to MaxCommandRetries. Use only
    /// when the caller has established that repeating this exact operation is safe. No command-name
    /// classification is performed. Cancellation, protocol errors, timeouts and server errors are not retried.
    /// </summary>
    public Task<RespValue> ExecuteRetryableAsync(ValkeyCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        return RunAsync(
            (client, _, token) => client.ExecuteAsync(command, token),
            null,
            true,
            "command",
            cancellationToken
        );
    }

    /// <summary>Authorizes bounded transport replay within one acquisition-and-execution deadline.</summary>
    public Task<RespValue> ExecuteRetryableWithDeadlineAsync(
        ValkeyCommand command,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(command);
        return RunAsync(
            (client, remaining, token) => client.ExecuteWithDeadlineAsync(command, remaining!.Value, token),
            ValkeyClient.ValidateOperationTimeout(timeout),
            true,
            "command",
            cancellationToken
        );
    }

    /// <summary>Runs a pipeline once. A failed pipeline is never replayed.</summary>
    public Task<IReadOnlyList<RespValue>> ExecutePipelineAsync(
        IEnumerable<ValkeyCommand> commands,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(commands);
        return RunAsync(
            (client, _, token) => client.ExecutePipelineAsync(commands, token),
            null,
            false,
            "pipeline",
            cancellationToken
        );
    }

    public Task<RespValue> ExecuteScriptAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(script);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(arguments);
        return RunAsync(
            (client, _, token) => client.ExecuteScriptAsync(script, keys, arguments, token),
            null,
            false,
            "script",
            cancellationToken
        );
    }

    /// <summary>Runs a pipeline once with one deadline covering acquisition and execution.</summary>
    public Task<IReadOnlyList<RespValue>> ExecutePipelineWithDeadlineAsync(
        IEnumerable<ValkeyCommand> commands,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(commands);
        return RunAsync(
            (client, remaining, token) => client.ExecutePipelineWithDeadlineAsync(commands, remaining!.Value, token),
            ValkeyClient.ValidateOperationTimeout(timeout),
            false,
            "pipeline",
            cancellationToken
        );
    }

    /// <summary>Runs a script without transport replay, with one acquisition-and-execution deadline.</summary>
    public Task<RespValue> ExecuteScriptWithDeadlineAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(script);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(arguments);
        return RunAsync(
            (client, remaining, token) =>
                client.ExecuteScriptWithDeadlineAsync(script, keys, arguments, remaining!.Value, token),
            ValkeyClient.ValidateOperationTimeout(timeout),
            false,
            "script",
            cancellationToken
        );
    }

    private Task<T> RunAsync<T>(
        Func<ValkeyClient, TimeSpan?, CancellationToken, Task<T>> operation,
        TimeSpan? timeout,
        bool retryable,
        string kind,
        CancellationToken cancellationToken
    ) =>
        _options.EnableTelemetry
            ? RunWithTelemetryAsync(operation, timeout, retryable, kind, cancellationToken)
            : RunCoreAsync(operation, timeout, retryable, cancellationToken);

    private Task<T> RunWithTelemetryAsync<T>(
        Func<ValkeyClient, TimeSpan?, CancellationToken, Task<T>> operation,
        TimeSpan? timeout,
        bool retryable,
        string kind,
        CancellationToken cancellationToken
    ) =>
        OwnerDiagnostics.TrackOperationAsync(
            kind,
            () => RunCoreAsync(operation, timeout, retryable, cancellationToken)
        );

    private async Task<T> RunCoreAsync<T>(
        Func<ValkeyClient, TimeSpan?, CancellationToken, Task<T>> operation,
        TimeSpan? timeout,
        bool retryable,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            ThrowIfDisposed();
            if (_activeOperations >= _options.MaxConcurrentOperations)
                throw new ValkeyCapacityException();
            _activeOperations++;
        }
        var startedAt = Stopwatch.GetTimestamp();
        var priorAmbiguousAttempt = false;
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        try
        {
            for (var retry = 0; ; retry++)
            {
                linked.Token.ThrowIfCancellationRequested();
                if (timeout is { } budget)
                    _ = Remaining(budget, startedAt);
                var connection = GetConnectionTask();
                var client = timeout is { } duration
                    ? await connection.WaitAsync(Remaining(duration, startedAt), linked.Token).ConfigureAwait(false)
                    : await connection.WaitAsync(linked.Token).ConfigureAwait(false);
                var remaining = timeout is { } limit ? Remaining(limit, startedAt) : (TimeSpan?)null;
                try
                {
                    return await operation(client, remaining, linked.Token).ConfigureAwait(false);
                }
                catch (Exception exception)
                    when (!_shutdown.IsCancellationRequested
                        && (
                            exception is ValkeyConnectionException
                            || (exception is ObjectDisposedException && client.IsDisposed)
                        )
                    )
                {
                    priorAmbiguousAttempt |=
                        (exception as IValkeyCommandFailure)?.DeliveryStatus
                        == ValkeyCommandDeliveryStatus.MayHaveBeenSent;
                    await RetireAsync(client).ConfigureAwait(false);
                    if (retryable && retry < _options.MaxCommandRetries)
                        continue;
                    if (exception is ObjectDisposedException)
                        throw new ValkeyConnectionException(
                            "The Valkey connection closed before command admission.",
                            exception,
                            priorAmbiguousAttempt
                                ? ValkeyCommandDeliveryStatus.MayHaveBeenSent
                                : ValkeyCommandDeliveryStatus.NotSent
                        );
                    throw;
                }
                catch
                {
                    if (client.IsDisposed)
                        await RetireAsync(client).ConfigureAwait(false);
                    throw;
                }
            }
        }
        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
        {
            throw new ObjectDisposedException(nameof(ValkeyConnectionOwner));
        }
        catch (OperationCanceledException) when (priorAmbiguousAttempt)
        {
            throw new ValkeyCommandCanceledException(cancellationToken);
        }
        catch (TimeoutException exception) when (timeout is { })
        {
            throw new ValkeyCommandTimeoutException(
                timeout.Value,
                priorAmbiguousAttempt ? ValkeyCommandDeliveryStatus.MayHaveBeenSent
                    : exception is ValkeyCommandTimeoutException commandTimeout ? commandTimeout.DeliveryStatus
                    : ValkeyCommandDeliveryStatus.NotSent
            );
        }
        catch (ValkeyConnectionException exception)
            when (priorAmbiguousAttempt && exception.DeliveryStatus == ValkeyCommandDeliveryStatus.NotSent)
        {
            throw new ValkeyConnectionException("Valkey recovery failed after an earlier command attempt.", exception);
        }
        finally
        {
            lock (_sync)
                _activeOperations--;
        }
    }

    private Task<ValkeyClient> GetConnectionTask()
    {
        TaskCompletionSource<ValkeyClient> completion;
        lock (_sync)
        {
            ThrowIfDisposed();
            _terminalFailure?.Throw();
            if (_client is { IsDisposed: false } healthy)
                return Task.FromResult(healthy);
            if (_connecting is not null)
                return _connecting;
            if (_client is not null)
            {
                _client = null;
                RecordFailure();
            }
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _connecting = completion.Task;
            _state = _everConnected ? ValkeyConnectionState.Reconnecting : ValkeyConnectionState.Connecting;
        }
        _ = EstablishAsync(completion);
        _ = ObserveAsync(completion.Task);
        return completion.Task;
    }

    private async Task EstablishAsync(TaskCompletionSource<ValkeyClient> completion)
    {
        try
        {
            for (var attempt = 0; ; attempt++)
            {
                TimeSpan delay;
                lock (_sync)
                    delay = _reconnectDelay - Stopwatch.GetElapsedTime(_lastFailureAt);
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, _shutdown.Token).ConfigureAwait(false);
                _shutdown.Token.ThrowIfCancellationRequested();
                ValkeyClient candidate;
                try
                {
                    candidate = _options.EnableTelemetry
                        ? await OwnerDiagnostics.TrackConnectionAsync(ConnectPhysicalAsync).ConfigureAwait(false)
                        : await ConnectPhysicalAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                    when (exception is IOException or SocketException or TimeoutException or ValkeyConnectionException)
                {
                    lock (_sync)
                        RecordFailure();
                    if (attempt + 1 >= _options.MaxConnectAttempts)
                        throw new ValkeyConnectionException(
                            "The Valkey connection attempts were exhausted.",
                            exception,
                            ValkeyCommandDeliveryStatus.NotSent
                        );
                    continue;
                }
                bool accepted;
                bool reconnected;
                lock (_sync)
                {
                    reconnected = _everConnected;
                    accepted = _disposal is null && !candidate.IsDisposed;
                    if (accepted)
                    {
                        _client = candidate;
                        _everConnected = true;
                        _failures = 0;
                        _reconnectDelay = TimeSpan.Zero;
                        _state = ValkeyConnectionState.Connected;
                        _connecting = null;
                        completion.TrySetResult(candidate);
                    }
                }
                if (accepted)
                {
                    if (reconnected && _options.EnableTelemetry)
                        OwnerDiagnostics.Reconnected();
                    return;
                }
                await candidate.DisposeAsync().ConfigureAwait(false);
                _shutdown.Token.ThrowIfCancellationRequested();
                lock (_sync)
                    RecordFailure();
                if (attempt + 1 < _options.MaxConnectAttempts)
                    continue;
                throw new ValkeyConnectionException(
                    "The Valkey connection closed during initialization.",
                    new IOException("Connection closed."),
                    ValkeyCommandDeliveryStatus.NotSent
                );
            }
        }
        catch (Exception exception)
        {
            lock (_sync)
            {
                _connecting = null;
                if (_disposal is null)
                {
                    var terminal =
                        exception
                        is AuthenticationException
                            or ValkeyServerException
                            or ValkeyProtocolException
                            or ArgumentException;
                    if (terminal)
                        _terminalFailure = ExceptionDispatchInfo.Capture(exception);
                    _state = terminal ? ValkeyConnectionState.Faulted : ValkeyConnectionState.Disconnected;
                }
                completion.TrySetException(exception);
            }
        }
    }

    private Task<ValkeyClient> ConnectPhysicalAsync() =>
        ValkeyClient.ConnectCoreAsync(_options.Connection, _tracking, _shutdown.Token);

    private async Task RetireAsync(ValkeyClient client)
    {
        lock (_sync)
        {
            if (ReferenceEquals(_client, client))
            {
                _client = null;
                RecordFailure();
                if (_disposal is null)
                    _state = ValkeyConnectionState.Disconnected;
            }
        }
        await client.DisposeAsync().ConfigureAwait(false);
    }

    // Called under _sync. The capped equal-jitter delay persists between failed acquisition cycles.
    private void RecordFailure()
    {
        _failures = Math.Min(_failures + 1, 31);
        var cap = Math.Min(
            _options.MaxReconnectDelay.TotalMilliseconds,
            _options.InitialReconnectDelay.TotalMilliseconds * Math.Pow(2, _failures - 1)
        );
        _reconnectDelay = TimeSpan.FromMilliseconds(
            cap * (0.5 + RandomNumberGenerator.GetInt32(1_000_000) / 2_000_000d)
        );
        _lastFailureAt = Stopwatch.GetTimestamp();
    }

    public ValueTask DisposeAsync()
    {
        TaskCompletionSource completion;
        ValkeyClient? client;
        Task<ValkeyClient>? connecting;
        lock (_sync)
        {
            if (_disposal is not null)
                return new ValueTask(_disposal);
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _disposal = completion.Task;
            _state = ValkeyConnectionState.Disposed;
            client = _client;
            _client = null;
            connecting = _connecting;
        }
        _ = DisposeCoreAsync(completion, client, connecting);
        return new ValueTask(completion.Task);
    }

    private async Task DisposeCoreAsync(
        TaskCompletionSource completion,
        ValkeyClient? client,
        Task<ValkeyClient>? connecting
    )
    {
        try
        {
            await _shutdown.CancelAsync().ConfigureAwait(false);
            if (client is not null)
                await client.DisposeAsync().ConfigureAwait(false);
            if (connecting is not null)
                await ObserveAsync(connecting).ConfigureAwait(false);
            completion.TrySetResult();
        }
        catch (Exception exception)
        {
            completion.TrySetException(exception);
        }
    }

    private static async Task ObserveAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        { /* Every waiter observes its own failure; abandoned shared attempts are also observed. */
        }
    }

    private static TimeSpan Remaining(TimeSpan timeout, long startedAt)
    {
        var remaining = timeout - Stopwatch.GetElapsedTime(startedAt);
        return remaining > TimeSpan.Zero ? remaining : throw new TimeoutException("The operation deadline elapsed.");
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposal is not null, this);
}
