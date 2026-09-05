namespace ValkeyDotNet;

/// <summary>Discovery and resource bounds for independent slot-routed shard subscriptions.</summary>
public sealed class ValkeyClusterSubscriberOptions
{
    /// <summary>Seed discovery, endpoint mapping, and connection security/parser settings.</summary>
    public ValkeyClusterOptions Cluster { get; init; } = new();

    /// <summary>Maximum retained handles and dedicated subscription sockets, including failed handles.</summary>
    public int MaxSubscriptions { get; init; } = 256;
    public int MaxConcurrentOperations { get; init; } = 64;
    public int QueueCapacity { get; init; } = 128;
    public int MaxChannelBytes { get; init; } = 16 * 1024;

    /// <summary>Total admission, connection, topology-refresh, and acknowledgement budget per operation.</summary>
    public TimeSpan OperationTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Restores on the same endpoint after transport loss unless topology recovery is enabled.</summary>
    public bool EnableReconnect { get; init; }

    /// <summary>Refreshes routing after transport loss or server shard removal; implies EnableReconnect.</summary>
    public bool EnableTopologyRecovery { get; init; }

    /// <summary>Maximum distinct discovery endpoints tried per recovery refresh (1–32).</summary>
    public int MaxTopologyRefreshEndpoints { get; init; } = 4;
    public int MaxReconnectAttempts { get; init; } = 3;
    public TimeSpan InitialReconnectDelay { get; init; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan MaxReconnectDelay { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan RecoveryTimeout { get; init; } = TimeSpan.FromSeconds(30);

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Cluster);
        var seeds = Cluster.ValidateAndCopySeeds();
        if (seeds.Any(seed => seed.Database != 0))
        {
            throw new ArgumentException("Cluster subscriptions require database zero.");
        }
        if (MaxSubscriptions is < 1 or > 16_384)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxSubscriptions));
        }
        if (MaxConcurrentOperations is < 1 or > 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxConcurrentOperations));
        }
        if (MaxTopologyRefreshEndpoints is < 1 or > 32)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxTopologyRefreshEndpoints));
        }
        CreateSubscriberOptions(seeds[0]).Validate();
    }

    internal ValkeySubscriberOptions CreateSubscriberOptions(ValkeyClientOptions connection) =>
        new()
        {
            Connection = connection,
            UseShardedPubSub = true,
            MaxSubscriptions = 1,
            MaxConcurrentOperations = 1,
            QueueCapacity = QueueCapacity,
            MaxChannelBytes = MaxChannelBytes,
            OperationTimeout = OperationTimeout,
            EnableReconnect = EnableReconnect || EnableTopologyRecovery,
            MaxReconnectAttempts = MaxReconnectAttempts,
            InitialReconnectDelay = InitialReconnectDelay,
            MaxReconnectDelay = MaxReconnectDelay,
            RecoveryTimeout = RecoveryTimeout,
        };
}
