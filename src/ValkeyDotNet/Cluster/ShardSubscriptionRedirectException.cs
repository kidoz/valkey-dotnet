using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace ValkeyDotNet.Cluster;

// Internal routing data, never raw server text in Message or InnerException.
internal sealed class ShardSubscriptionRedirectException(int slot, string host, int port)
    : ValkeyException("ASK Shard subscription redirected.")
{
    internal int Slot { get; } = slot;
    internal string Host { get; } = host;
    internal int Port { get; } = port;

    internal static ShardSubscriptionRedirectException Parse(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length > 1024)
        {
            throw new ValkeyClusterException("The shard subscription redirect exceeds its size limit.");
        }
        foreach (var value in bytes)
        {
            if (value is < 32 or > 126)
            {
                throw new ValkeyClusterException("Malformed shard subscription redirect.");
            }
        }
        var parts = Encoding.ASCII.GetString(bytes).Split(' ');
        if (
            parts.Length != 3
            || parts[0] != "ASK"
            || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var slot)
            || slot is < 0 or >= 16384
        )
        {
            throw new ValkeyClusterException("Malformed shard subscription redirect.");
        }
        var separator = parts[2].LastIndexOf(':');
        if (
            separator < 0
            || !int.TryParse(
                parts[2].AsSpan(separator + 1),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var port
            )
            || port is < 1 or > 65535
        )
        {
            throw new ValkeyClusterException("Malformed shard subscription redirect endpoint.");
        }
        var host = parts[2][..separator];
        if (host.Contains('[', StringComparison.Ordinal) || host.Contains(']', StringComparison.Ordinal))
        {
            if (
                host.Length < 3
                || host[0] != '['
                || host[^1] != ']'
                || !IPAddress.TryParse(host[1..^1], out var address)
                || address.AddressFamily != AddressFamily.InterNetworkV6
            )
            {
                throw new ValkeyClusterException("Malformed shard subscription redirect host.");
            }
            host = host[1..^1];
        }
        if (host.Length != 0 && Uri.CheckHostName(host) == UriHostNameType.Unknown)
        {
            throw new ValkeyClusterException("Malformed shard subscription redirect host.");
        }
        return new(slot, host, port);
    }
}
