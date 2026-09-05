using System.Globalization;
using System.Text;

namespace ValkeyDotNet.MigrationRelay;

// Only SELECT 0 and one or two expected RESTORE-ASKING frames; never a general RESP proxy.
internal static class RestoreAckLossRelay
{
    internal static async Task RunAsync(
        Stream sender,
        Stream destination,
        byte[] key,
        Action<string> report,
        CancellationToken token,
        byte[]? acknowledgedKey = null
    )
    {
        if (
            key.Length is < 1 or > 512
            || (
                acknowledgedKey is not null
                && (acknowledgedKey.Length is < 1 or > 512 || acknowledgedKey.AsSpan().SequenceEqual(key))
            )
        )
        {
            throw new ArgumentException("Expected one or two distinct bounded keys.");
        }
        var select = await ReadCommandAsync(sender, 2, token).ConfigureAwait(false);
        if (
            !select.Arguments[0].AsSpan().SequenceEqual("SELECT"u8)
            || !select.Arguments[1].AsSpan().SequenceEqual("0"u8)
        )
        {
            throw new InvalidOperationException("Expected SELECT 0.");
        }
        // Validate the entire expected request before forwarding even SELECT.
        var acknowledged = acknowledgedKey is null
            ? null
            : await ReadRestoreAsync(sender, acknowledgedKey, token).ConfigureAwait(false);
        var restore = await ReadRestoreAsync(sender, key, token).ConfigureAwait(false);
        await destination.WriteAsync(select.Wire, token).ConfigureAwait(false);
        await ReadOkAsync(destination, token).ConfigureAwait(false);
        await sender.WriteAsync("+OK\r\n"u8.ToArray(), token).ConfigureAwait(false);
        if (acknowledged is not null)
        {
            await destination.WriteAsync(acknowledged, token).ConfigureAwait(false);
            await ReadOkAsync(destination, token).ConfigureAwait(false);
            await sender.WriteAsync("+OK\r\n"u8.ToArray(), token).ConfigureAwait(false);
            report("RESTORE_ACK_FORWARDED");
        }
        await destination.WriteAsync(restore, token).ConfigureAwait(false);
        await ReadOkAsync(destination, token).ConfigureAwait(false);
        report("RESTORE_ACK_WITHHELD");
        // Keep the link open until MIGRATE's idle timeout, avoiding the server's non-timeout retry path.
        var extra = new byte[1];
        if (await sender.ReadAsync(extra, token).ConfigureAwait(false) != 0)
        {
            throw new InvalidOperationException("Unexpected extra transfer bytes.");
        }
        report("SENDER_CLOSED");
    }

    private static async Task<byte[]> ReadRestoreAsync(Stream sender, byte[] key, CancellationToken token)
    {
        var restore = await ReadCommandAsync(sender, 4, token).ConfigureAwait(false);
        if (
            !restore.Arguments[0].AsSpan().SequenceEqual("RESTORE-ASKING"u8)
            || !restore.Arguments[1].AsSpan().SequenceEqual(key)
            || !int.TryParse(
                Encoding.ASCII.GetString(restore.Arguments[2]),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var ttl
            )
            || ttl is < 1 or > 120000
            || restore.Arguments[3].Length is < 1 or > 8192
        )
        {
            throw new InvalidOperationException("Unexpected restore command, key, TTL, or payload bound.");
        }
        return restore.Wire;
    }

    private static async Task ReadOkAsync(Stream stream, CancellationToken token)
    {
        var reply = new byte[5];
        await stream.ReadExactlyAsync(reply, token).ConfigureAwait(false);
        if (!reply.AsSpan().SequenceEqual("+OK\r\n"u8))
        {
            throw new InvalidOperationException("Expected a complete success reply.");
        }
    }

    internal static async Task<(byte[][] Arguments, byte[] Wire)> ReadCommandAsync(
        Stream stream,
        int count,
        CancellationToken token
    )
    {
        if (count is not (2 or 4))
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }
        using var wire = new MemoryStream();
        var one = new byte[1];
        async Task<byte> ReadByteAsync()
        {
            if (wire.Length >= 16384)
            {
                throw new InvalidOperationException("Relay command byte budget exceeded.");
            }
            await stream.ReadExactlyAsync(one, token).ConfigureAwait(false);
            wire.WriteByte(one[0]);
            return one[0];
        }
        async Task<int> ReadLengthAsync(byte prefix, int maximum)
        {
            if (await ReadByteAsync().ConfigureAwait(false) != prefix)
            {
                throw new InvalidOperationException("Invalid relay frame prefix.");
            }
            var length = 0;
            var digits = 0;
            while (true)
            {
                var value = await ReadByteAsync().ConfigureAwait(false);
                if (value == '\r' && digits > 0)
                {
                    if (await ReadByteAsync().ConfigureAwait(false) != '\n')
                    {
                        throw new InvalidOperationException("Invalid relay frame delimiter.");
                    }
                    return length;
                }
                if (value is < (byte)'0' or > (byte)'9' || ++digits > 5)
                {
                    throw new InvalidOperationException("Invalid relay frame length.");
                }
                length = length * 10 + value - '0';
                if (length > maximum)
                {
                    throw new InvalidOperationException("Relay frame length exceeded its bound.");
                }
            }
        }
        if (await ReadLengthAsync((byte)'*', 4).ConfigureAwait(false) != count)
        {
            throw new InvalidOperationException("Unexpected relay command arity.");
        }
        var arguments = new byte[count][];
        for (var index = 0; index < count; index++)
        {
            var length = await ReadLengthAsync((byte)'$', 8192).ConfigureAwait(false);
            if (length > 16384 - wire.Length - 2)
            {
                throw new InvalidOperationException("Relay aggregate exceeded its byte budget.");
            }
            var bytes = new byte[length];
            await stream.ReadExactlyAsync(bytes, token).ConfigureAwait(false);
            wire.Write(bytes);
            if (
                await ReadByteAsync().ConfigureAwait(false) != '\r'
                || await ReadByteAsync().ConfigureAwait(false) != '\n'
            )
            {
                throw new InvalidOperationException("Invalid relay bulk delimiter.");
            }
            arguments[index] = bytes;
        }
        return (arguments, wire.ToArray());
    }
}
