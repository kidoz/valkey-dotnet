namespace ValkeyDotNet;

/// <summary>Bounded admission, connection recovery, and explicitly authorized retry settings.</summary>
public sealed class ValkeyConnectionOwnerOptions
{
    /// <summary>Immutable connection settings reused for every physical connection.</summary>
    public ValkeyClientOptions Connection { get; init; } = new();

    /// <summary>Maximum admitted operations, including connected operations and reconnect waiters.</summary>
    public int MaxConcurrentOperations { get; init; } = 1024;

    /// <summary>Maximum connection attempts in one shared acquisition cycle.</summary>
    public int MaxConnectAttempts { get; init; } = 3;

    public TimeSpan InitialReconnectDelay { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan MaxReconnectDelay { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Maximum additional attempts for the explicitly retryable command methods only.</summary>
    public int MaxCommandRetries { get; init; } = 1;

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Connection);
        Connection.Validate();
        if (MaxConcurrentOperations is < 1 or > 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(MaxConcurrentOperations));
        if (MaxConnectAttempts is < 1 or > 100)
            throw new ArgumentOutOfRangeException(nameof(MaxConnectAttempts));
        if (MaxCommandRetries is < 0 or > 16)
            throw new ArgumentOutOfRangeException(nameof(MaxCommandRetries));
        if (InitialReconnectDelay < TimeSpan.FromMilliseconds(1) || InitialReconnectDelay > MaxReconnectDelay)
            throw new ArgumentOutOfRangeException(nameof(InitialReconnectDelay));
        if (MaxReconnectDelay.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(MaxReconnectDelay));
    }
}
