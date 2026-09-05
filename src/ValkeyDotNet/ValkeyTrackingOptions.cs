namespace ValkeyDotNet;

/// <summary>Immutable settings for same-connection RESP3 client tracking.</summary>
public sealed class ValkeyTrackingOptions
{
    /// <summary>Suppress invalidations caused by this connection's own writes. Defaults to false.</summary>
    public bool NoLoop { get; init; }

    /// <summary>Track matching writes without first reading keys. Defaults to false.</summary>
    public bool Broadcast { get; init; }

    /// <summary>
    /// Binary prefixes for broadcast mode. Empty means all keys. Prefixes must not overlap and are
    /// copied when the tracking client is constructed. At most 256 prefixes and 1 MiB total are accepted.
    /// </summary>
    public IReadOnlyList<ValkeyArgument> Prefixes { get; init; } = Array.Empty<ValkeyArgument>();

    /// <summary>
    /// Maximum buffered invalidation batches (default 256). Overflow replaces buffered batches with
    /// an invalidate-all notification, never silently discards the need to invalidate.
    /// </summary>
    public int QueueCapacity { get; init; } = 256;

    internal ValkeyCommand CreateCommand()
    {
        ArgumentNullException.ThrowIfNull(Prefixes);
        if (QueueCapacity is < 1 or > 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(nameof(QueueCapacity));
        }
        if (Prefixes.Count > 256 || (!Broadcast && Prefixes.Count != 0))
        {
            throw new ArgumentException("Tracking prefixes require broadcast mode and a maximum count of 256.");
        }

        var prefixes = new List<ReadOnlyMemory<byte>>(Prefixes.Count);
        var totalBytes = 0;
        foreach (var prefix in Prefixes)
        {
            if (prefix.Bytes.Length > 1024 * 1024 - totalBytes)
            {
                throw new ArgumentException("Tracking prefixes exceed the 1 MiB configuration limit.");
            }
            totalBytes += prefix.Bytes.Length;
            var copy = prefix.Bytes.ToArray();
            foreach (var previous in prefixes)
            {
                if (previous.Span.StartsWith(copy) || copy.AsSpan().StartsWith(previous.Span))
                {
                    throw new ArgumentException("Tracking prefixes must not overlap.");
                }
            }
            prefixes.Add(copy);
        }

        var arguments = new List<ValkeyArgument> { "TRACKING", "ON" };
        if (NoLoop)
        {
            arguments.Add("NOLOOP");
        }
        if (Broadcast)
        {
            arguments.Add("BCAST");
        }
        foreach (var prefix in prefixes)
        {
            arguments.Add("PREFIX");
            arguments.Add(prefix);
        }
        return new ValkeyCommand("CLIENT", arguments.ToArray());
    }
}
