namespace ValkeyDotNet;

/// <summary>An opaque binary Pub/Sub delivery. Pattern is null for a direct channel subscription.</summary>
public sealed class ValkeyPubSubMessage
{
    internal ValkeyPubSubMessage(
        ReadOnlyMemory<byte> channel,
        ReadOnlyMemory<byte> payload,
        ReadOnlyMemory<byte>? pattern
    )
    {
        Channel = channel;
        Payload = payload;
        Pattern = pattern;
    }

    public ReadOnlyMemory<byte> Channel { get; }
    public ReadOnlyMemory<byte> Payload { get; }
    public ReadOnlyMemory<byte>? Pattern { get; }
}
