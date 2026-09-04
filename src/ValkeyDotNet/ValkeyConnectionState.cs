namespace ValkeyDotNet;

/// <summary>A snapshot of a standalone connection owner's lifecycle.</summary>
public enum ValkeyConnectionState
{
    NeverConnected,
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
    Faulted,
    Disposed,
}
