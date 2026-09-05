using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ValkeyDotNet.Protocol;

/// <summary>Owned configuration and bounded handoff shared across sequential physical connections.</summary>
internal sealed class TrackingSession
{
    private readonly object _sync = new();
    private readonly Channel<ValkeyInvalidation> _messages;
    private long _version;
    private long _overflows;
    private bool _completed;
    private int _enumerating;

    internal TrackingSession(ValkeyTrackingOptions options)
    {
        EnableCommand = options.CreateCommand();
        _messages = Channel.CreateBounded<ValkeyInvalidation>(
            new BoundedChannelOptions(options.QueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false,
            }
        );
    }

    internal ValkeyCommand EnableCommand { get; }
    internal long Version => Interlocked.Read(ref _version);
    internal long QueueOverflows => Interlocked.Read(ref _overflows);

    internal async IAsyncEnumerable<ValkeyInvalidation> ReadAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        if (Interlocked.CompareExchange(ref _enumerating, 1, 0) != 0)
        {
            throw new InvalidOperationException("Only one invalidation reader may be active per tracking client.");
        }
        try
        {
            await foreach (var message in _messages.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return message;
            }
        }
        finally
        {
            Volatile.Write(ref _enumerating, 0);
        }
    }

    internal void OnPush(RespValue response)
    {
        var items = response.AsArray();
        if (items.Count == 0 || items[0].Type is not (RespType.SimpleString or RespType.BlobString))
        {
            throw new ValkeyProtocolException("The tracking connection received a malformed push kind.");
        }
        if (!items[0].AsBytes().Span.SequenceEqual("invalidate"u8))
        {
            // Other valid push kinds do not consume FIFO reply slots or become invalidations.
            return;
        }
        if (items.Count != 2)
        {
            throw new ValkeyProtocolException("An invalidation push must contain exactly two elements.");
        }
        if (items[1].IsNull)
        {
            InvalidateAll();
            return;
        }
        if (items[1].Type != RespType.Array)
        {
            throw new ValkeyProtocolException("Invalidation keys must be an array or null.");
        }
        var values = items[1].AsArray();
        // The RESP reader has already enforced byte, element, and nesting bounds.
        var keys = new ReadOnlyMemory<byte>[values.Count];
        for (var index = 0; index < values.Count; index++)
        {
            if (values[index].Type != RespType.BlobString)
            {
                throw new ValkeyProtocolException("An invalidation key must be a blob string.");
            }
            keys[index] = values[index].AsBytes();
        }
        Publish(Array.AsReadOnly(keys), false);
    }

    internal void InvalidateAll() => Publish(Array.Empty<ReadOnlyMemory<byte>>(), true);

    private void Publish(IReadOnlyList<ReadOnlyMemory<byte>> keys, bool invalidateAll)
    {
        lock (_sync)
        {
            if (_completed)
            {
                return;
            }
            var version = Interlocked.Increment(ref _version);
            if (!invalidateAll && _messages.Writer.TryWrite(new ValkeyInvalidation(version, keys, false)))
            {
                return;
            }
            if (!invalidateAll)
            {
                Interlocked.Increment(ref _overflows);
            }
            // A reset subsumes all queued keys. Producers serialize here; consumers never block them.
            while (_messages.Reader.TryRead(out _)) { }
            _messages.Writer.TryWrite(new ValkeyInvalidation(version, Array.Empty<ReadOnlyMemory<byte>>(), true));
        }
    }

    internal void Complete()
    {
        lock (_sync)
        {
            if (_completed)
            {
                return;
            }
            InvalidateAll();
            _completed = true;
            _messages.Writer.TryComplete();
        }
    }

    internal static void EnsureSupported(ValkeyCommand command)
    {
        if (command.Name is "AUTH" or "SELECT")
        {
            throw new ValkeyUnsupportedCommandException(
                command.Name,
                "tracking owns authentication and database state"
            );
        }
        if (command.Name == "CLIENT" && command.ArgumentsSpan.Length > 0)
        {
            var subcommand = command.ArgumentsSpan[0].Bytes.Span;
            if (
                System.Text.Ascii.EqualsIgnoreCase(subcommand, "TRACKING"u8)
                || System.Text.Ascii.EqualsIgnoreCase(subcommand, "CACHING"u8)
            )
            {
                throw new ValkeyUnsupportedCommandException(
                    "CLIENT TRACKING/CACHING",
                    "tracking configuration is immutable for this client"
                );
            }
        }
    }

    internal static void ThrowIfSetupError(RespValue response)
    {
        if (response.Type is RespType.SimpleError or RespType.BlobError)
        {
            var bytes = response.AsBytes().Span;
            var code =
                bytes.StartsWith("NOPERM "u8) ? "NOPERM"
                : bytes.StartsWith("WRONGPASS "u8) ? "WRONGPASS"
                : bytes.StartsWith("NOAUTH "u8) ? "NOAUTH"
                : "ERR";
            throw new ValkeyServerException($"{code} Tracking connection initialization was rejected.");
        }
    }
}
