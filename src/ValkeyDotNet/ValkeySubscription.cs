using System.Threading.Channels;

namespace ValkeyDotNet;

/// <summary>
/// One local subscription with an independent bounded queue. Enumerating messages does not run
/// application code on the socket reader. Dispose this handle to remove only its own registration.
/// </summary>
public sealed class ValkeySubscription : IAsyncDisposable
{
    private readonly ValkeySubscriber _owner;
    private readonly Channel<ValkeyPubSubMessage> _messages;
    private long _dropped;

    internal ValkeySubscription(ValkeySubscriber owner, string key, int capacity)
    {
        _owner = owner;
        Key = key;
        _messages = Channel.CreateBounded<ValkeyPubSubMessage>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false,
            }
        );
    }

    internal string Key { get; }
    internal bool Removed { get; set; }

    /// <summary>Incoming messages dropped because this handle's queue was full.</summary>
    public long DroppedMessages => Interlocked.Read(ref _dropped);

    /// <summary>
    /// Reads buffered deliveries. Multiple readers compete; they do not each receive a copy.
    /// Cancelling enumeration does not unsubscribe. Previously buffered messages drain before completion.
    /// </summary>
    public IAsyncEnumerable<ValkeyPubSubMessage> ReadAllAsync(CancellationToken cancellationToken = default) =>
        _messages.Reader.ReadAllAsync(cancellationToken);

    internal bool Deliver(ValkeyPubSubMessage message)
    {
        if (_messages.Writer.TryWrite(message))
        {
            return true;
        }

        Interlocked.Increment(ref _dropped);
        return false;
    }

    internal void Complete(Exception? error) => _messages.Writer.TryComplete(error);

    /// <summary>Removes this handle; only the last handle sends an unsubscribe command.</summary>
    public Task UnsubscribeAsync(CancellationToken cancellationToken = default) =>
        _owner.UnsubscribeAsync(this, cancellationToken);

    public async ValueTask DisposeAsync() => await UnsubscribeAsync().ConfigureAwait(false);
}
