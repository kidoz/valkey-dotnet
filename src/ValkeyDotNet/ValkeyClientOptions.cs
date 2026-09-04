using System.Net.Security;

namespace ValkeyDotNet;

/// <summary>Connection and protocol settings for <see cref="ValkeyClient"/>.</summary>
public sealed class ValkeyClientOptions
{
    /// <summary>
    /// The ceiling timer-backed timeout APIs accept. A longer timeout would throw after connection
    /// resources exist instead of being validated up front.
    /// </summary>
    private const long MaxTimerTimeoutMilliseconds = uint.MaxValue - 1;

    public string Host { get; init; } = "localhost";
    public int Port { get; init; } = 6379;
    public string? Username { get; init; }
    public string? Password { get; init; }
    public string? ClientName { get; init; }
    public int Database { get; init; }
    public ValkeyProtocol Protocol { get; init; } = ValkeyProtocol.Resp3;
    public bool UseTls { get; init; }
    public RemoteCertificateValidationCallback? CertificateValidationCallback { get; init; }
    public TimeSpan ConnectTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum time allowed to drain replies retained after an isolated operation deadline. If the
    /// replies do not arrive, the connection is terminated so its bounded pending queue cannot stay
    /// occupied indefinitely.
    /// </summary>
    public TimeSpan ResponseDrainTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum number of commands written to this connection whose replies have not yet been read.
    /// This bounds multiplexer bookkeeping and applies to ordinary calls and pipeline entries.
    /// </summary>
    public int MaxPendingRequests { get; init; } = 1024;

    public int MaxResponseBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// The maximum number of RESP values decoded from a single reply. It bounds the memory a reply
    /// occupies once decoded, which <see cref="MaxResponseBytes"/> alone does not: a three-byte
    /// element on the wire becomes a much larger object in managed memory.
    /// </summary>
    public int MaxResponseElements { get; init; } = 1024 * 1024;

    public int MaxNestingDepth { get; init; } = 128;

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Host);
        if (Port is < 1 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(Port));
        if (Database < 0)
            throw new ArgumentOutOfRangeException(nameof(Database));
        if (!Enum.IsDefined(Protocol))
            throw new ArgumentOutOfRangeException(nameof(Protocol));
        if (ConnectTimeout <= TimeSpan.Zero || ConnectTimeout.TotalMilliseconds > MaxTimerTimeoutMilliseconds)
            throw new ArgumentOutOfRangeException(nameof(ConnectTimeout));
        if (
            ResponseDrainTimeout <= TimeSpan.Zero
            || ResponseDrainTimeout.TotalMilliseconds > MaxTimerTimeoutMilliseconds
        )
            throw new ArgumentOutOfRangeException(nameof(ResponseDrainTimeout));
        if (MaxPendingRequests is < 1 or > 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(MaxPendingRequests));
        if (MaxResponseBytes < 1024)
            throw new ArgumentOutOfRangeException(nameof(MaxResponseBytes));
        if (MaxResponseElements < 16)
            throw new ArgumentOutOfRangeException(nameof(MaxResponseElements));
        if (MaxNestingDepth is < 1 or > 1024)
            throw new ArgumentOutOfRangeException(nameof(MaxNestingDepth));
        if (Username is not null && Password is null)
            throw new ArgumentException("A password is required when a username is supplied.");
    }
}
