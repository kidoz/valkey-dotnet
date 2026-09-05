using ValkeyDotNet.Cluster;

namespace ValkeyDotNet;

/// <summary>
/// Routes sharded Pub/Sub to primaries using dedicated sockets, never ordinary command connections.
/// Each handle owns one socket. Initial MOVED refresh and ASK redirection are bounded;
/// established subscriptions can opt into bounded topology recovery with their original queues.
/// </summary>
public sealed class ValkeyClusterSubscriber : IAsyncDisposable
{
    private readonly ValkeyClusterSubscriberOptions _options;
    private readonly ValkeyClusterClient _cluster;
    private readonly object _sync = new();
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly SemaphoreSlim _topologyGate = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly HashSet<ValkeyShardedSubscription> _subscriptions = [];
    private int _operations;
    private Task? _disposal;

    private ValkeyClusterSubscriber(ValkeyClusterSubscriberOptions options, ValkeyClusterClient cluster)
    {
        _options = options;
        _cluster = cluster;
    }

    /// <summary>Discovers the complete primary slot map within one operation budget.</summary>
    public static async Task<ValkeyClusterSubscriber> ConnectAsync(
        ValkeyClusterSubscriberOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        options ??= new();
        options.Validate();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(options.OperationTimeout);
        try
        {
            var cluster = await ValkeyClusterClient.ConnectAsync(options.Cluster, timeout.Token).ConfigureAwait(false);
            return new ValkeyClusterSubscriber(options, cluster);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new ValkeyCommandTimeoutException(options.OperationTimeout, ValkeyCommandDeliveryStatus.NotSent);
        }
    }

    /// <summary>Number of retained handles, including terminal handles awaiting disposal.</summary>
    public int SubscriptionCount
    {
        get
        {
            lock (_sync)
            {
                return _subscriptions.Count;
            }
        }
    }

    /// <summary>
    /// Opens one independent, binary-safe shard subscription. Duplicate names intentionally use
    /// independent sockets. The initial attempt plus MaxRedirects combined MOVED/ASK retries are bounded.
    /// </summary>
    public Task<ValkeyShardedSubscription> SubscribeAsync(
        ValkeyArgument channel,
        CancellationToken cancellationToken = default
    )
    {
        if (channel.Bytes.Length > _options.MaxChannelBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(channel));
        }
        return RunAsync(token => SubscribeCoreAsync(channel, token), cancellationToken);
    }

    private async Task<ValkeyShardedSubscription> SubscribeCoreAsync(ValkeyArgument channel, CancellationToken token)
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            if (_subscriptions.Count >= _options.MaxSubscriptions)
            {
                throw new ValkeyCapacityException("The cluster subscriber's subscription capacity is full.");
            }
        }
        var name = channel.Bytes.ToArray();
        var node = _cluster.GetSubscriptionNodeOptions(name);
        var asking = false;
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await OpenSubscriptionAsync(name, node, asking, token).ConfigureAwait(false);
            }
            catch (ValkeyServerException error) when (error.ErrorCode == "MOVED")
            {
                if (attempt >= _options.Cluster.MaxRedirects)
                {
                    throw new ValkeyClusterException("Shard subscription topology-refresh attempts were exhausted.");
                }
                // Never follow endpoint text from a subscriber error. Reload validated discovery data.
                await RefreshRoutesAsync(token).ConfigureAwait(false);
                node = _cluster.GetSubscriptionNodeOptions(name);
                asking = false;
            }
            catch (ShardSubscriptionRedirectException redirect)
            {
                if (attempt >= _options.Cluster.MaxRedirects)
                {
                    throw new ValkeyClusterException("Shard subscription redirect attempts were exhausted.");
                }
                node = _cluster.GetSubscriptionRedirectOptions(redirect, name, node);
                asking = true;
            }
            catch (ValkeyServerException error) when (error.ErrorCode == "ASK")
            {
                throw new ValkeyClusterException("Malformed shard subscription ASK redirect.");
            }
        }
    }

    private async Task<ValkeyShardedSubscription> OpenSubscriptionAsync(
        byte[] name,
        ValkeyClientOptions node,
        bool asking,
        CancellationToken token
    )
    {
        var recovery = _options.EnableTopologyRecovery
            ? new ShardSubscriptionRecovery(
                _options.Cluster.MaxRedirects,
                async (source, redirect, recoveryToken) =>
                {
                    if (redirect is not null)
                    {
                        return (_cluster.GetSubscriptionRedirectOptions(redirect, name, source), true);
                    }
                    await RefreshRoutesAsync(recoveryToken).ConfigureAwait(false);
                    return (_cluster.GetSubscriptionNodeOptions(name), false);
                }
            )
            : null;
        var subscriber = await ValkeySubscriber
            .ConnectClusterAsync(_options.CreateSubscriberOptions(node), asking, recovery, token)
            .ConfigureAwait(false);
        var transferred = false;
        try
        {
            var subscription = await subscriber.SubscribeShardedAsync(name, token).ConfigureAwait(false);
            var handle = new ValkeyShardedSubscription(this, subscriber, subscription);
            lock (_sync)
            {
                ThrowIfDisposed();
                _subscriptions.Add(handle);
                transferred = true;
            }
            return handle;
        }
        finally
        {
            if (!transferred)
            {
                await subscriber.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Refreshes routing for future subscriptions; existing streams are not moved or duplicated.</summary>
    public async Task RefreshTopologyAsync(CancellationToken cancellationToken = default)
    {
        await RunAsync(
                async token =>
                {
                    await RefreshRoutesAsync(token).ConfigureAwait(false);
                    return true;
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    private async Task RefreshRoutesAsync(CancellationToken token)
    {
        // Independent of the lifecycle gate: unsubscribe may hold it while joining a recovery.
        await _topologyGate.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_options.EnableTopologyRecovery)
            {
                await _cluster
                    .RefreshSubscriptionTopologyAsync(_options.MaxTopologyRefreshEndpoints, token)
                    .ConfigureAwait(false);
            }
            else
            {
                await _cluster.RefreshTopologyAsync(token).ConfigureAwait(false);
            }
        }
        finally
        {
            _topologyGate.Release();
        }
    }

    internal async Task ReleaseAsync(
        ValkeyShardedSubscription handle,
        bool unsubscribe,
        CancellationToken cancellationToken
    )
    {
        Task? disposal;
        lock (_sync)
        {
            disposal = _disposal;
            if (disposal is null && !_subscriptions.Contains(handle))
            {
                return;
            }
        }
        if (disposal is not null)
        {
            await disposal.ConfigureAwait(false);
            return;
        }
        if (!unsubscribe)
        {
            // Local disposal must work even when lifecycle admission is full. Keep its reservation
            // until the physical reader/recovery has stopped; simultaneous unsubscribe settles on close.
            await handle.Subscriber.DisposeAsync().ConfigureAwait(false);
            lock (_sync)
            {
                _subscriptions.Remove(handle);
            }
            return;
        }
        try
        {
            await RunAsync(
                    async token =>
                    {
                        lock (_sync)
                        {
                            if (!_subscriptions.Contains(handle))
                            {
                                return true;
                            }
                        }
                        try
                        {
                            if (unsubscribe)
                            {
                                await handle.Subscription.UnsubscribeAsync(token).ConfigureAwait(false);
                            }
                        }
                        finally
                        {
                            await handle.Subscriber.DisposeAsync().ConfigureAwait(false);
                            lock (_sync)
                            {
                                _subscriptions.Remove(handle);
                            }
                        }
                        return true;
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (Exception) when (_shutdown.IsCancellationRequested)
        {
            Task closing;
            lock (_sync)
            {
                closing = _disposal!;
            }
            await closing.ConfigureAwait(false);
        }
    }

    private CancellationTokenSource CreateDeadline(CancellationToken token)
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(token, _shutdown.Token);
        timeout.CancelAfter(_options.OperationTimeout);
        return timeout;
    }

    private async Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken token)
    {
        using var deadline = CreateDeadline(token);
        var entered = false;
        try
        {
            await EnterAsync(deadline.Token).ConfigureAwait(false);
            entered = true;
            return await operation(deadline.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
        {
            throw new ObjectDisposedException(nameof(ValkeyClusterSubscriber));
        }
        catch (OperationCanceledException error)
            when (!token.IsCancellationRequested && deadline.IsCancellationRequested)
        {
            throw new ValkeyCommandTimeoutException(
                _options.OperationTimeout,
                error is IValkeyCommandFailure failure ? failure.DeliveryStatus : ValkeyCommandDeliveryStatus.NotSent
            );
        }
        finally
        {
            if (entered)
            {
                Exit();
            }
        }
    }

    private async Task EnterAsync(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        lock (_sync)
        {
            ThrowIfDisposed();
            if (_operations >= _options.MaxConcurrentOperations)
            {
                throw new ValkeyCapacityException("The cluster subscriber's operation capacity is full.");
            }
            _operations++;
        }
        try
        {
            await _gate.WaitAsync(token).ConfigureAwait(false);
        }
        catch
        {
            lock (_sync)
            {
                _operations--;
            }
            throw;
        }
    }

    private void Exit()
    {
        _gate.Release();
        lock (_sync)
        {
            _operations--;
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposal is not null, this);

    /// <summary>Cancels pending acquisition, closes all shard streams, and disposes discovery connections.</summary>
    public ValueTask DisposeAsync()
    {
        TaskCompletionSource completion;
        lock (_sync)
        {
            if (_disposal is not null)
            {
                return new ValueTask(_disposal);
            }
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _disposal = completion.Task;
        }
        _ = DisposeCoreAsync(completion);
        return new ValueTask(completion.Task);
    }

    private async Task DisposeCoreAsync(TaskCompletionSource completion)
    {
        try
        {
            await _shutdown.CancelAsync().ConfigureAwait(false);
            await _gate.WaitAsync().ConfigureAwait(false);
            try
            {
                ValkeyShardedSubscription[] snapshot;
                lock (_sync)
                {
                    snapshot = _subscriptions.ToArray();
                    _subscriptions.Clear();
                }
                // Signal every supervisor before joining any of them; discovery is shared and serialized.
                await Task.WhenAll(snapshot.Select(handle => handle.Subscriber.DisposeAsync().AsTask()))
                    .ConfigureAwait(false);
                await _cluster.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _gate.Release();
            }
            completion.TrySetResult();
        }
        catch (Exception error)
        {
            completion.TrySetException(error);
        }
    }
}
