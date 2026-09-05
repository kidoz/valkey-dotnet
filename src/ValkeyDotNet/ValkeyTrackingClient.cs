using ValkeyDotNet.Protocol;

namespace ValkeyDotNet;

/// <summary>
/// A standalone RESP3 command connection with managed tracking and bounded invalidation delivery.
/// Replacement is on demand, uses the connection owner's bounded policy, and enables tracking before
/// commands run. This type provides transport notifications, not a local cache or consistency guarantee.
/// </summary>
public sealed class ValkeyTrackingClient : IAsyncDisposable
{
    private readonly ValkeyConnectionOwner _owner;
    private readonly TrackingSession _tracking;

    /// <summary>Validates and snapshots tracking settings without opening a connection.</summary>
    public ValkeyTrackingClient(
        ValkeyConnectionOwnerOptions? connectionOptions = null,
        ValkeyTrackingOptions? trackingOptions = null
    )
    {
        connectionOptions ??= new ValkeyConnectionOwnerOptions();
        connectionOptions.Validate();
        if (connectionOptions.Connection.Protocol != ValkeyProtocol.Resp3)
        {
            throw new ArgumentException("Managed client tracking requires RESP3.", nameof(connectionOptions));
        }
        _tracking = new TrackingSession(trackingOptions ?? new ValkeyTrackingOptions());
        _owner = new ValkeyConnectionOwner(connectionOptions, _tracking);
    }

    /// <summary>Connection lifecycle snapshot; silent network partitions require caller liveness checks.</summary>
    public ValkeyConnectionState State => _owner.State;

    /// <summary>
    /// Latest received invalidation/reset version, advanced before notifications are handed off.
    /// Consumers own synchronization between cache reads, fills, invalidations, and connection health.
    /// </summary>
    public long InvalidationVersion => _tracking.Version;

    /// <summary>Number of full-queue events replaced by conservative invalidate-all notifications.</summary>
    public long QueueOverflows => _tracking.QueueOverflows;

    /// <summary>
    /// Reads invalidations away from the socket reader. Only one enumeration may be active. Cancellation
    /// stops enumeration, not tracking; disposal sends a final invalidate-all and completes the stream.
    /// </summary>
    public IAsyncEnumerable<ValkeyInvalidation> ReadInvalidationsAsync(CancellationToken cancellationToken = default) =>
        _tracking.ReadAllAsync(cancellationToken);

    /// <summary>Connects and enables tracking before returning; also restores a lost connection on demand.</summary>
    public Task ConnectAsync(CancellationToken cancellationToken = default) => _owner.ConnectAsync(cancellationToken);

    /// <summary>Executes once on the tracked connection. Ambiguous writes are never replayed.</summary>
    public Task<RespValue> ExecuteAsync(ValkeyCommand command, CancellationToken cancellationToken = default) =>
        _owner.ExecuteAsync(command, cancellationToken);

    /// <summary>Executes once with an isolated acquisition-and-execution deadline.</summary>
    public Task<RespValue> ExecuteWithDeadlineAsync(
        ValkeyCommand command,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) => _owner.ExecuteWithDeadlineAsync(command, timeout, cancellationToken);

    /// <summary>Explicitly authorizes bounded transport replay of this exact caller-proven-safe command.</summary>
    public Task<RespValue> ExecuteRetryableAsync(
        ValkeyCommand command,
        CancellationToken cancellationToken = default
    ) => _owner.ExecuteRetryableAsync(command, cancellationToken);

    /// <summary>Explicitly authorizes bounded replay within one acquisition-and-execution deadline.</summary>
    public Task<RespValue> ExecuteRetryableWithDeadlineAsync(
        ValkeyCommand command,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) => _owner.ExecuteRetryableWithDeadlineAsync(command, timeout, cancellationToken);

    /// <summary>Executes a pipeline once, preserving positional errors and interleaved invalidations.</summary>
    public Task<IReadOnlyList<RespValue>> ExecutePipelineAsync(
        IEnumerable<ValkeyCommand> commands,
        CancellationToken cancellationToken = default
    ) => _owner.ExecutePipelineAsync(commands, cancellationToken);

    /// <summary>Executes a pipeline once with an isolated acquisition-and-execution deadline.</summary>
    public Task<IReadOnlyList<RespValue>> ExecutePipelineWithDeadlineAsync(
        IEnumerable<ValkeyCommand> commands,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) => _owner.ExecutePipelineWithDeadlineAsync(commands, timeout, cancellationToken);

    /// <summary>Executes a binary-safe script with bounded NOSCRIPT recovery, but no transport replay.</summary>
    public Task<RespValue> ExecuteScriptAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        CancellationToken cancellationToken = default
    ) => _owner.ExecuteScriptAsync(script, keys, arguments, cancellationToken);

    /// <summary>Executes a script without transport replay within one acquisition-and-execution deadline.</summary>
    public Task<RespValue> ExecuteScriptWithDeadlineAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) => _owner.ExecuteScriptWithDeadlineAsync(script, keys, arguments, timeout, cancellationToken);

    /// <summary>Closes the connection and completes invalidation delivery. Safe to call repeatedly.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _owner.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _tracking.Complete();
        }
    }
}
