namespace ValkeyDotNet.Cluster;

// Only cluster-owned subscribers can resolve replacement endpoints. The callback never owns a reader.
internal sealed class ShardSubscriptionRecovery(
    int maxRedirects,
    Func<
        ValkeyClientOptions,
        ShardSubscriptionRedirectException?,
        CancellationToken,
        Task<(ValkeyClientOptions Options, bool Asking)>
    > resolveAsync
)
{
    internal int MaxRedirects { get; } = maxRedirects;
    internal Func<
        ValkeyClientOptions,
        ShardSubscriptionRedirectException?,
        CancellationToken,
        Task<(ValkeyClientOptions Options, bool Asking)>
    > ResolveAsync { get; } = resolveAsync;
}
