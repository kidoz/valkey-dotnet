namespace ValkeyDotNet;

/// <summary>Discovery, connection, and redirect settings for <see cref="ValkeyClusterClient"/>.</summary>
public sealed class ValkeyClusterOptions
{
    /// <summary>
    /// Nodes tried in order during initial discovery. Their transport, authentication, protocol, and
    /// parser settings are also used for connections to discovered cluster nodes.
    /// </summary>
    public IReadOnlyList<ValkeyClientOptions> SeedNodes { get; init; } = [new ValkeyClientOptions()];

    /// <summary>The maximum number of MOVED or ASK redirects followed for one command.</summary>
    public int MaxRedirects { get; init; } = 5;

    /// <summary>
    /// Maximum number of node connections retained by this client. This bounds file-descriptor and
    /// memory growth when topology or redirect endpoints change repeatedly.
    /// </summary>
    public int MaxNodeConnections { get; init; } = 256;

    /// <summary>
    /// Multiplexed connections maintained for each active node. Values above one isolate independent
    /// work from response head-of-line blocking on a single RESP stream.
    /// </summary>
    public int ConnectionsPerNode { get; init; } = 1;

    /// <summary>
    /// Translates server-announced endpoints before connecting. Use this for private addresses,
    /// container hostnames, port forwarding, or a TLS hostname that differs from cluster metadata.
    /// The returned endpoint is validated before use.
    /// </summary>
    public Func<ValkeyClusterEndpoint, ValkeyClusterEndpoint>? EndpointMapper { get; init; }

    internal ValkeyClientOptions[] ValidateAndCopySeeds()
    {
        ArgumentNullException.ThrowIfNull(SeedNodes);
        if (SeedNodes.Count == 0)
            throw new ArgumentException("At least one cluster seed node is required.", nameof(SeedNodes));
        if (MaxRedirects is < 0 or > 16)
            throw new ArgumentOutOfRangeException(nameof(MaxRedirects));
        if (MaxNodeConnections is < 1 or > 16_384)
            throw new ArgumentOutOfRangeException(nameof(MaxNodeConnections));
        if (ConnectionsPerNode is < 1 or > 16)
            throw new ArgumentOutOfRangeException(nameof(ConnectionsPerNode));
        if (ConnectionsPerNode > MaxNodeConnections)
            throw new ArgumentException(
                "ConnectionsPerNode cannot exceed MaxNodeConnections.",
                nameof(ConnectionsPerNode)
            );

        var seeds = SeedNodes.ToArray();
        foreach (var seed in seeds)
        {
            if (seed is null)
                throw new ArgumentException("A cluster seed node cannot be null.", nameof(SeedNodes));
            seed.Validate();
        }
        return seeds;
    }
}
