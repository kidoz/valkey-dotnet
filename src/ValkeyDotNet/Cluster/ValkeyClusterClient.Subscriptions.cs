using ValkeyDotNet.Cluster;

namespace ValkeyDotNet;

public sealed partial class ValkeyClusterClient
{
    // Only the dedicated cluster subscriber uses this snapshot. It never lends a command socket.
    internal ValkeyClientOptions GetSubscriptionNodeOptions(ReadOnlySpan<byte> channel)
    {
        ThrowIfDisposed();
        var slot = GetHashSlot(channel);
        var endpoint =
            Volatile.Read(ref _slots)[slot]
            ?? throw new ValkeyClusterException("No primary is known for the shard channel.");
        return CreateNodeOptions(endpoint);
    }

    internal ValkeyClientOptions GetSubscriptionRedirectOptions(
        ShardSubscriptionRedirectException redirect,
        ReadOnlySpan<byte> channel,
        ValkeyClientOptions source
    )
    {
        ThrowIfDisposed();
        if (redirect.Slot != GetHashSlot(channel))
        {
            throw new ValkeyClusterException("Shard subscription redirect does not match the channel slot.");
        }
        var host = redirect.Host.Length == 0 ? source.Host : redirect.Host;
        return CreateNodeOptions(CreateAnnouncedEndpoint(_options, host, redirect.Port));
    }
}
