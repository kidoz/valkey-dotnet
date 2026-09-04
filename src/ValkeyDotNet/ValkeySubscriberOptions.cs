namespace ValkeyDotNet;

/// <summary>Bounds and connection settings for a dedicated, terminal Pub/Sub connection.</summary>
public sealed class ValkeySubscriberOptions
{
    public ValkeyClientOptions Connection { get; init; } = new();

    /// <summary>Maximum buffered messages per local subscription. Overflow drops the incoming message.</summary>
    public int QueueCapacity { get; init; } = 128;

    /// <summary>Maximum local handles, including duplicate channel or pattern subscriptions.</summary>
    public int MaxSubscriptions { get; init; } = 1024;

    public int MaxChannelBytes { get; init; } = 16 * 1024;
    public int MaxConcurrentOperations { get; init; } = 64;

    /// <summary>Bounds admission, writes, and acknowledgement. Expiry after writing terminates the subscriber.</summary>
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(5);

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
    }
}
