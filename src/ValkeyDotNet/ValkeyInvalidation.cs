namespace ValkeyDotNet;

/// <summary>A binary-key invalidation batch or a conservative invalidate-all notification.</summary>
public sealed class ValkeyInvalidation
{
    internal ValkeyInvalidation(long version, IReadOnlyList<ReadOnlyMemory<byte>> keys, bool invalidateAll)
    {
        Version = version;
        Keys = keys;
        InvalidateAll = invalidateAll;
    }

    /// <summary>Monotonically increasing notification version within one tracking client.</summary>
    public long Version { get; }

    /// <summary>
    /// True after server flush, detected connection loss, queue overflow, or disposal. All entries
    /// associated with this tracking client are then invalid, including entries not listed in Keys.
    /// </summary>
    public bool InvalidateAll { get; }

    /// <summary>Keys to evict, without text conversion. Empty for invalidate-all notifications.</summary>
    public IReadOnlyList<ReadOnlyMemory<byte>> Keys { get; }
}
