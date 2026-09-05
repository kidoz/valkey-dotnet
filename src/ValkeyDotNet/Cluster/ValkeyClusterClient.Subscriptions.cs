using System.Net.Sockets;
using ValkeyDotNet.Cluster;

namespace ValkeyDotNet;

public sealed partial class ValkeyClusterClient
{
    // Discovery sockets are temporary, serialized by the owning cluster subscriber, and never subscribed.
    internal async Task RefreshSubscriptionTopologyAsync(int maxEndpoints, CancellationToken token)
    {
        ThrowIfDisposed();
        var candidates = new List<ClusterEndpoint>(maxEndpoints) { _seedEndpoint };
        var seen = new HashSet<ClusterEndpoint> { _seedEndpoint };
        foreach (var slot in Volatile.Read(ref _slots))
        {
            if (candidates.Count == maxEndpoints)
            {
                break;
            }
            if (slot is { } endpoint && seen.Add(endpoint))
            {
                candidates.Add(endpoint);
            }
        }
        foreach (var endpoint in candidates)
        {
            token.ThrowIfCancellationRequested();
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
            deadline.CancelAfter(_nodeTemplate.ConnectTimeout);
            ValkeyClient? client = null;
            try
            {
                client = await ValkeyClient
                    .ConnectAsync(CreateNodeOptions(endpoint), deadline.Token)
                    .ConfigureAwait(false);
                var slots = await ReadTopologyAsync(
                        client,
                        endpoint,
                        _options,
                        _nodeTemplate.UseTls,
                        deadline.Token,
                        subscriptionRecovery: true
                    )
                    .ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                Volatile.Write(ref _slots, slots);
                return;
            }
            catch (ValkeyServerException error)
            {
                var category = error.ErrorCode is "NOAUTH" or "WRONGPASS" or "NOPERM" ? error.ErrorCode : "ERR";
                throw new ValkeyServerException(category + " Subscriber discovery rejected.");
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                // An unresponsive seed must not consume the entire recovery budget if other nodes are known.
            }
            catch (Exception error)
                when (error is IOException or SocketException or TimeoutException or ValkeyConnectionException)
            {
                token.ThrowIfCancellationRequested();
            }
            finally
            {
                if (client is not null)
                {
                    await client.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
        throw new ValkeyConnectionException(
            "No known cluster discovery endpoint was available.",
            new IOException("Topology refresh failed.")
        );
    }

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

    internal ValkeyClientOptions GetSubscriptionRedirectOptions(
        ShardSubscriptionRedirectException redirect,
        ReadOnlySpan<byte> channel,
        ValkeyClientOptions source
    )
    {
        ThrowIfDisposed();
        if (redirect.Slot != GetHashSlot(channel))
        {
            throw new ValkeyClusterException("Shard subscription redirect does not match the channel slot.");
        }
        var host = redirect.Host.Length == 0 ? source.Host : redirect.Host;
        return CreateNodeOptions(CreateAnnouncedEndpoint(_options, host, redirect.Port));
    }
}
