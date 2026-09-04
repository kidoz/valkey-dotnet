namespace ValkeyDotNet;

/// <summary>
/// A host and port announced by a cluster node or returned by an endpoint mapping callback.
/// </summary>
public readonly record struct ValkeyClusterEndpoint(string Host, int Port);
