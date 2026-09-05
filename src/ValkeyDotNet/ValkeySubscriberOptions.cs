namespace ValkeyDotNet;

/// <summary>Bounds, optional recovery, and connection settings for a dedicated Pub/Sub connection.</summary>
public sealed class ValkeySubscriberOptions
{
    public ValkeyClientOptions Connection { get; init; } = new();

    /// <summary>
    /// Uses SSUBSCRIBE/SUNSUBSCRIBE and smessage on this dedicated connection. Global channels and
    /// patterns cannot be mixed into this mode. Node routing remains the caller's responsibility.
    /// </summary>
    public bool UseShardedPubSub { get; init; }

    /// <summary>Maximum buffered messages per local subscription. Overflow drops the incoming message.</summary>
    public int QueueCapacity { get; init; } = 128;

    /// <summary>Maximum local handles, including duplicate channel or pattern subscriptions.</summary>
    public int MaxSubscriptions { get; init; } = 1024;

    public int MaxChannelBytes { get; init; } = 16 * 1024;
    public int MaxConcurrentOperations { get; init; } = 64;

    /// <summary>Bounds admission, writes, and acknowledgement. Expiry after writing terminates the subscriber.</summary>
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Restore confirmed subscriptions after transport loss. Disabled by default.</summary>
    public bool EnableReconnect { get; init; }

    public int MaxReconnectAttempts { get; init; } = 3;
    public TimeSpan InitialReconnectDelay { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan MaxReconnectDelay { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>Total budget per recovery cycle, including backoff, connect, and all restoration acknowledgements.</summary>
    public TimeSpan RecoveryTimeout { get; init; } = TimeSpan.FromSeconds(30);

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Connection);
        Connection.Validate();
        if (QueueCapacity is < 1 or > 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(QueueCapacity));
        }

        if (MaxSubscriptions is < 1 or > 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxSubscriptions));
        }

        if (MaxChannelBytes is < 1 or > 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxChannelBytes));
        }

        if (MaxConcurrentOperations is < 1 or > 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxConcurrentOperations));
        }

        if (OperationTimeout <= TimeSpan.Zero || OperationTimeout.TotalMilliseconds > uint.MaxValue - 1)
        {
            throw new ArgumentOutOfRangeException(nameof(OperationTimeout));
        }
        if (MaxReconnectAttempts is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxReconnectAttempts));
        }
        if (InitialReconnectDelay <= TimeSpan.Zero || InitialReconnectDelay > MaxReconnectDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(InitialReconnectDelay));
        }
        if (MaxReconnectDelay.TotalMilliseconds > uint.MaxValue - 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxReconnectDelay));
        }
        if (RecoveryTimeout <= TimeSpan.Zero || RecoveryTimeout.TotalMilliseconds > uint.MaxValue - 1)
        {
            throw new ArgumentOutOfRangeException(nameof(RecoveryTimeout));
        }
    }
}
