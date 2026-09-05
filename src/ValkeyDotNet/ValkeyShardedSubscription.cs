namespace ValkeyDotNet;

/// <summary>
/// One cluster shard-channel stream with an independent socket and bounded queue. Dispose this
/// handle even after terminal failure to release its owner's capacity. Delivery is best-effort.
/// </summary>
public sealed class ValkeyShardedSubscription : IAsyncDisposable
{
    private readonly ValkeyClusterSubscriber _owner;
    internal ValkeySubscriber Subscriber { get; }
    internal ValkeySubscription Subscription { get; }

    internal ValkeyShardedSubscription(
        ValkeyClusterSubscriber owner,
        ValkeySubscriber subscriber,
        ValkeySubscription subscription
    )
    {
        _owner = owner;
        Subscriber = subscriber;
        Subscription = subscription;
    }

    public bool IsConnected => Subscriber.IsConnected;
    public bool IsReconnecting => Subscriber.IsReconnecting;
    public Exception? Failure => Subscriber.Failure;
    public Task Completion => Subscriber.Completion;
    public long DroppedMessages => Subscription.DroppedMessages;
    public long SuccessfulReconnects => Subscriber.SuccessfulReconnects;

    /// <summary>Reads binary shard deliveries away from the socket reader. Multiple readers compete.</summary>
    public IAsyncEnumerable<ValkeyPubSubMessage> ReadAllAsync(CancellationToken cancellationToken = default) =>
        Subscription.ReadAllAsync(cancellationToken);

    /// <summary>
    /// Sends SUNSUBSCRIBE then closes this socket. Once admitted, failure or cancellation also closes
    /// this handle; unrelated subscriptions are untouched. Before admission, cancellation leaves it active.
    /// </summary>
    public Task UnsubscribeAsync(CancellationToken cancellationToken = default) =>
        _owner.ReleaseAsync(this, true, cancellationToken);

    /// <summary>Closes this dedicated socket without waiting for a server acknowledgement.</summary>
    public async ValueTask DisposeAsync() =>
        await _owner.ReleaseAsync(this, false, CancellationToken.None).ConfigureAwait(false);
}
