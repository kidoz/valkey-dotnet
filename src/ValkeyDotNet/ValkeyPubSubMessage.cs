namespace ValkeyDotNet;

/// <summary>An opaque binary Pub/Sub delivery. Pattern is null for a direct channel subscription.</summary>
public sealed class ValkeyPubSubMessage
{
    internal ValkeyPubSubMessage(
        ReadOnlyMemory<byte> channel,
        ReadOnlyMemory<byte> payload,
        ReadOnlyMemory<byte>? pattern,
        bool isSharded = false
    )
    {
        Channel = channel;
        Payload = payload;
        Pattern = pattern;
        IsSharded = isSharded;
    }

    public ReadOnlyMemory<byte> Channel { get; }
    public ReadOnlyMemory<byte> Payload { get; }
    public ReadOnlyMemory<byte>? Pattern { get; }

    /// <summary>True for an smessage delivered by sharded Pub/Sub, rather than global Pub/Sub.</summary>
    public bool IsSharded { get; }
}
