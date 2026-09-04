using System.Buffers;
using System.Globalization;
using System.Numerics;
using System.Text;

namespace ValkeyDotNet.Internal;

internal sealed class RespReader
{
    /// <summary>
    /// The smallest element a RESP aggregate can contain is three bytes on the wire ("_\r\n" or
    /// "+\r\n"). A declared cardinality that cannot fit in the remaining byte budget is impossible,
    /// so it is rejected before anything is allocated for it.
    /// </summary>
    private const int MinimumElementBytes = 3;

    /// <summary>
    /// Aggregates grow to fit what actually arrives instead of trusting the declared count, so a
    /// small frame header cannot make the reader allocate a large collection up front.
    /// </summary>
    private const int MaximumInitialCapacity = 256;

    private readonly Stream _stream;
    private readonly byte[] _buffer = new byte[8192];
    private readonly int _maxResponseBytes;
    private readonly int _maxResponseElements;
    private readonly int _maxDepth;
    private int _offset;
    private int _length;
    private int _frameBytes;
    private int _frameElements;

    public RespReader(Stream stream, int maxResponseBytes, int maxResponseElements, int maxDepth)
    {
        _stream = stream;
        _maxResponseBytes = maxResponseBytes;
        _maxResponseElements = maxResponseElements;
        _maxDepth = maxDepth;
    }

    public async ValueTask<RespValue> ReadAsync(CancellationToken cancellationToken)
    {
        _frameBytes = 0;
        _frameElements = 0;
        var prefix = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
        return await ReadAfterPrefixAsync(prefix, 0, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<RespValue> ReadAfterPrefixAsync(byte prefix, int depth, CancellationToken cancellationToken)
    {
        if (depth > _maxDepth)
            throw new ValkeyProtocolException($"RESP nesting exceeds the configured limit of {_maxDepth}.");
        CountElement();

        return prefix switch
        {
            (byte)'+' => RespValue.Bytes(
                RespType.SimpleString,
                await ReadLineAsync(cancellationToken).ConfigureAwait(false)
            ),
            (byte)'-' => RespValue.Bytes(
                RespType.SimpleError,
                await ReadLineAsync(cancellationToken).ConfigureAwait(false)
            ),
            (byte)':' => RespValue.Integer(
                ParseInt64(await ReadAsciiLineAsync(cancellationToken).ConfigureAwait(false), "integer")
            ),
            (byte)'$' => await ReadBlobAsync(RespType.BlobString, nullable: true, cancellationToken)
                .ConfigureAwait(false),
            (byte)'*' => await ReadAggregateAsync(RespType.Array, nullable: true, depth, cancellationToken)
                .ConfigureAwait(false),
            (byte)'_' => await ReadNullAsync(cancellationToken).ConfigureAwait(false),
            (byte)'#' => await ReadBooleanAsync(cancellationToken).ConfigureAwait(false),
            (byte)',' => RespValue.Double(
                ParseDouble(await ReadAsciiLineAsync(cancellationToken).ConfigureAwait(false))
            ),
            (byte)'(' => ReadBigNumber(await ReadAsciiLineAsync(cancellationToken).ConfigureAwait(false)),
            (byte)'!' => await ReadBlobAsync(RespType.BlobError, nullable: false, cancellationToken)
                .ConfigureAwait(false),
            (byte)'=' => await ReadVerbatimAsync(cancellationToken).ConfigureAwait(false),
            (byte)'%' => await ReadMapAsync(RespType.Map, depth, cancellationToken).ConfigureAwait(false),
            (byte)'~' => await ReadAggregateAsync(RespType.Set, nullable: false, depth, cancellationToken)
                .ConfigureAwait(false),
            (byte)'>' => await ReadAggregateAsync(RespType.Push, nullable: false, depth, cancellationToken)
                .ConfigureAwait(false),
            (byte)'|' => await ReadAttributedAsync(depth, cancellationToken).ConfigureAwait(false),
            _ => throw new ValkeyProtocolException($"Unknown RESP type byte 0x{prefix:X2}."),
        };
    }

    private async ValueTask<RespValue> ReadBlobAsync(RespType type, bool nullable, CancellationToken cancellationToken)
    {
        var lengthText = await ReadAsciiLineAsync(cancellationToken).ConfigureAwait(false);
        if (lengthText == "?")
        {
            using var output = new MemoryStream();
            while (true)
            {
                if (await ReadByteAsync(cancellationToken).ConfigureAwait(false) != (byte)';')
                    throw new ValkeyProtocolException("A streamed string chunk must start with ';'.");
                var chunkLength = ParseChunkLength(await ReadAsciiLineAsync(cancellationToken).ConfigureAwait(false));
                if (chunkLength == 0)
                    break;
                var chunk = await ReadExactAsync(chunkLength, cancellationToken).ConfigureAwait(false);
                await ExpectCrLfAsync(cancellationToken).ConfigureAwait(false);
                output.Write(chunk);
            }
            return RespValue.Bytes(type, output.ToArray());
        }

        var length = ParseCount(lengthText, nullable, type);
        if (length < 0)
            return RespValue.Null();
        var value = await ReadExactAsync(length, cancellationToken).ConfigureAwait(false);
        await ExpectCrLfAsync(cancellationToken).ConfigureAwait(false);
        return RespValue.Bytes(type, value);
    }

    private async ValueTask<RespValue> ReadVerbatimAsync(CancellationToken cancellationToken)
    {
        var value = await ReadBlobAsync(RespType.VerbatimString, nullable: false, cancellationToken)
            .ConfigureAwait(false);
        var bytes = value.AsBytes();
        if (bytes.Length < 4 || bytes.Span[3] != (byte)':')
            throw new ValkeyProtocolException("A verbatim string must start with a three-byte format followed by ':'.");
        var format = Encoding.ASCII.GetString(bytes.Span[..3]);
        return RespValue.Bytes(RespType.VerbatimString, bytes[4..].ToArray(), format);
    }

    private async ValueTask<RespValue> ReadAggregateAsync(
        RespType type,
        bool nullable,
        int depth,
        CancellationToken cancellationToken
    )
    {
        var countText = await ReadAsciiLineAsync(cancellationToken).ConfigureAwait(false);
        if (countText == "?")
        {
            var streamed = new List<RespValue>();
            while (true)
            {
                var prefix = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
                if (prefix == (byte)'.')
                {
                    if ((await ReadLineAsync(cancellationToken).ConfigureAwait(false)).Length != 0)
                        throw new ValkeyProtocolException("Invalid streamed aggregate terminator.");
                    break;
                }
                streamed.Add(await ReadAfterPrefixAsync(prefix, depth + 1, cancellationToken).ConfigureAwait(false));
            }
            return RespValue.Items(type, streamed);
        }

        var count = ParseCount(countText, nullable, type);
        if (count < 0)
            return RespValue.Null();
        EnsureCardinalityFits(type, count, valuesPerEntry: 1);

        var values = new List<RespValue>(InitialCapacity(count));
        for (var i = 0; i < count; i++)
        {
            var prefix = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
            values.Add(await ReadAfterPrefixAsync(prefix, depth + 1, cancellationToken).ConfigureAwait(false));
        }
        return RespValue.Items(type, values);
    }

    private async ValueTask<RespValue> ReadMapAsync(RespType type, int depth, CancellationToken cancellationToken)
    {
        var countText = await ReadAsciiLineAsync(cancellationToken).ConfigureAwait(false);
        if (countText == "?")
        {
            var streamed = new List<KeyValuePair<RespValue, RespValue>>();
            while (true)
            {
                var prefix = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
                if (prefix == (byte)'.')
                {
                    if ((await ReadLineAsync(cancellationToken).ConfigureAwait(false)).Length != 0)
                        throw new ValkeyProtocolException("Invalid streamed map terminator.");
                    break;
                }
                streamed.Add(await ReadPairAsync(prefix, depth, cancellationToken).ConfigureAwait(false));
            }
            return RespValue.Pairs(type, streamed);
        }

        var count = ParseCount(countText, nullable: false, type);
        EnsureCardinalityFits(type, count, valuesPerEntry: 2);

        var pairs = new List<KeyValuePair<RespValue, RespValue>>(InitialCapacity(count));
        for (var i = 0; i < count; i++)
        {
            var prefix = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
            pairs.Add(await ReadPairAsync(prefix, depth, cancellationToken).ConfigureAwait(false));
        }
        return RespValue.Pairs(type, pairs);
    }

    private async ValueTask<KeyValuePair<RespValue, RespValue>> ReadPairAsync(
        byte keyPrefix,
        int depth,
        CancellationToken cancellationToken
    )
    {
        var key = await ReadAfterPrefixAsync(keyPrefix, depth + 1, cancellationToken).ConfigureAwait(false);
        var valuePrefix = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
        var value = await ReadAfterPrefixAsync(valuePrefix, depth + 1, cancellationToken).ConfigureAwait(false);
        return new(key, value);
    }

    private async ValueTask<RespValue> ReadAttributedAsync(int depth, CancellationToken cancellationToken)
    {
        var attributes = await ReadMapAsync(RespType.Map, depth, cancellationToken).ConfigureAwait(false);
        var prefix = await ReadByteAsync(cancellationToken).ConfigureAwait(false);
        var value = await ReadAfterPrefixAsync(prefix, depth + 1, cancellationToken).ConfigureAwait(false);
        var merged = attributes.AsMap().Concat(value.Attributes).ToArray();
        return value.WithAttributes(merged);
    }

    private async ValueTask<RespValue> ReadNullAsync(CancellationToken cancellationToken)
    {
        if ((await ReadLineAsync(cancellationToken).ConfigureAwait(false)).Length != 0)
            throw new ValkeyProtocolException("RESP3 null must be followed immediately by CRLF.");
        return RespValue.Null();
    }

    private async ValueTask<RespValue> ReadBooleanAsync(CancellationToken cancellationToken)
    {
        var text = await ReadAsciiLineAsync(cancellationToken).ConfigureAwait(false);
        return text switch
        {
            "t" => RespValue.Boolean(true),
            "f" => RespValue.Boolean(false),
            _ => throw new ValkeyProtocolException($"Invalid RESP3 boolean '{text}'."),
        };
    }

    private static RespValue ReadBigNumber(string text)
    {
        if (!BigInteger.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _))
            throw new ValkeyProtocolException($"Invalid RESP3 big number '{text}'.");
        return RespValue.Bytes(RespType.BigNumber, Encoding.ASCII.GetBytes(text));
    }

    private static double ParseDouble(string text) =>
        text switch
        {
            "inf" => double.PositiveInfinity,
            "-inf" => double.NegativeInfinity,
            "nan" => double.NaN,
            _ when double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) => value,
            _ => throw new ValkeyProtocolException($"Invalid RESP3 double '{text}'."),
        };

    private static long ParseInt64(string text, string kind) =>
        long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new ValkeyProtocolException($"Invalid RESP {kind} '{text}'.");

    /// <summary>
    /// Parses a declared length or cardinality. Only RESP2 null bulk strings and null arrays may use
    /// -1; every other aggregate and blob form requires a non-negative count.
    /// </summary>
    private static int ParseCount(string text, bool nullable, RespType type)
    {
        var value = ParseInt64(text, "length");
        if (value < -1 || value > int.MaxValue)
            throw new ValkeyProtocolException($"RESP length '{text}' is outside the supported range.");
        if (value < 0 && !nullable)
            throw new ValkeyProtocolException($"A RESP {type} cannot declare the null length '{text}'.");
        return (int)value;
    }

    private static int ParseChunkLength(string text)
    {
        var value = ParseInt64(text, "length");
        if (value < 0 || value > int.MaxValue)
            throw new ValkeyProtocolException($"A streamed chunk length '{text}' is outside the supported range.");
        return (int)value;
    }

    private static int InitialCapacity(int count) => Math.Min(count, MaximumInitialCapacity);

    /// <summary>
    /// Rejects a declared cardinality that the configured bounds cannot satisfy, before the reader
    /// allocates or reads anything for it.
    /// </summary>
    private void EnsureCardinalityFits(RespType type, int count, int valuesPerEntry)
    {
        var declared = (long)count * valuesPerEntry;
        if (declared > _maxResponseElements - _frameElements)
            throw new ValkeyProtocolException(
                $"A RESP {type} declares {count} entries, which exceeds the configured limit of "
                    + $"{_maxResponseElements} values per reply."
            );
        if (declared * MinimumElementBytes > _maxResponseBytes - _frameBytes)
            throw new ValkeyProtocolException(
                $"A RESP {type} declares {count} entries, which cannot fit in the configured limit of "
                    + $"{_maxResponseBytes} bytes."
            );
    }

    private async ValueTask<string> ReadAsciiLineAsync(CancellationToken cancellationToken) =>
        Encoding.ASCII.GetString(await ReadLineAsync(cancellationToken).ConfigureAwait(false));

    private async ValueTask<byte[]> ReadLineAsync(CancellationToken cancellationToken)
    {
        ArrayBufferWriter<byte>? output = null;
        while (true)
        {
            if (_offset == _length)
                await FillAsync(cancellationToken).ConfigureAwait(false);

            var available = _buffer.AsSpan(_offset, _length - _offset);
            var delimiter = available.IndexOf((byte)'\r');
            if (delimiter >= 0)
            {
                EnsureBytesFit(delimiter + 2);
                byte[] result;
                if (output is null)
                {
                    result = available[..delimiter].ToArray();
                }
                else
                {
                    output.Write(available[..delimiter]);
                    result = output.WrittenSpan.ToArray();
                }

                _offset += delimiter;
                CountBytes(delimiter);
                _offset++;
                CountBytes(1);
                if (await ReadByteAsync(cancellationToken).ConfigureAwait(false) != (byte)'\n')
                    throw new ValkeyProtocolException("RESP line ended with CR not followed by LF.");
                return result;
            }

            EnsureBytesFit(available.Length);
            output ??= new ArrayBufferWriter<byte>(Math.Min(available.Length * 2, _maxResponseBytes));
            output.Write(available);
            _offset = _length;
            CountBytes(available.Length);
        }
    }

    private async ValueTask<byte[]> ReadExactAsync(int count, CancellationToken cancellationToken)
    {
        if (count > _maxResponseBytes - _frameBytes)
            throw new ValkeyProtocolException($"RESP frame exceeds the configured limit of {_maxResponseBytes} bytes.");
        var result = new byte[count];
        for (var written = 0; written < count; )
        {
            if (_offset == _length)
                await FillAsync(cancellationToken).ConfigureAwait(false);
            var available = Math.Min(count - written, _length - _offset);
            _buffer.AsSpan(_offset, available).CopyTo(result.AsSpan(written));
            _offset += available;
            written += available;
            CountBytes(available);
        }
        return result;
    }

    private async ValueTask ExpectCrLfAsync(CancellationToken cancellationToken)
    {
        if (
            await ReadByteAsync(cancellationToken).ConfigureAwait(false) != (byte)'\r'
            || await ReadByteAsync(cancellationToken).ConfigureAwait(false) != (byte)'\n'
        )
            throw new ValkeyProtocolException("RESP bulk value is not followed by CRLF.");
    }

    private async ValueTask<byte> ReadByteAsync(CancellationToken cancellationToken)
    {
        if (_offset == _length)
            await FillAsync(cancellationToken).ConfigureAwait(false);
        CountBytes(1);
        return _buffer[_offset++];
    }

    private async ValueTask FillAsync(CancellationToken cancellationToken)
    {
        _length = await _stream.ReadAsync(_buffer, cancellationToken).ConfigureAwait(false);
        _offset = 0;
        if (_length == 0)
            throw new EndOfStreamException("The Valkey server closed the connection mid-response.");
    }

    private void CountBytes(int count)
    {
        _frameBytes = checked(_frameBytes + count);
        if (_frameBytes > _maxResponseBytes)
            throw new ValkeyProtocolException($"RESP frame exceeds the configured limit of {_maxResponseBytes} bytes.");
    }

    private void EnsureBytesFit(int count)
    {
        if (count > _maxResponseBytes - _frameBytes)
            throw new ValkeyProtocolException($"RESP frame exceeds the configured limit of {_maxResponseBytes} bytes.");
    }

    private void CountElement()
    {
        _frameElements++;
        if (_frameElements > _maxResponseElements)
            throw new ValkeyProtocolException(
                $"The reply contains more than the configured limit of {_maxResponseElements} values."
            );
    }
}
