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
}
