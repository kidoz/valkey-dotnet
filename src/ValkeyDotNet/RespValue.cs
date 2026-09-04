using System.Globalization;
using System.Numerics;
using System.Text;

namespace ValkeyDotNet;

/// <summary>A lossless representation of a RESP2 or RESP3 reply.</summary>
public sealed class RespValue
{
    private const int PreviewCharacters = 48;
    private const int MaxUtf8BytesPerCharacter = 4;

    private readonly byte[]? _bytes;
    private readonly long _integer;
    private readonly double _double;
    private readonly bool _boolean;
    private readonly IReadOnlyList<RespValue>? _items;
    private readonly IReadOnlyList<KeyValuePair<RespValue, RespValue>>? _pairs;

    private RespValue(
        RespType type,
        byte[]? bytes = null,
        long integer = 0,
        double @double = 0,
        bool boolean = false,
        IReadOnlyList<RespValue>? items = null,
        IReadOnlyList<KeyValuePair<RespValue, RespValue>>? pairs = null,
        IReadOnlyList<KeyValuePair<RespValue, RespValue>>? attributes = null,
        string? verbatimFormat = null
    )
    {
        Type = type;
        _bytes = bytes;
        _integer = integer;
        _double = @double;
        _boolean = boolean;
        _items = items;
        _pairs = pairs;
        Attributes = attributes ?? Array.Empty<KeyValuePair<RespValue, RespValue>>();
        VerbatimFormat = verbatimFormat;
    }

    public RespType Type { get; }
    public bool IsNull => Type == RespType.Null;
    public string? VerbatimFormat { get; }
    public IReadOnlyList<KeyValuePair<RespValue, RespValue>> Attributes { get; }

    public ReadOnlyMemory<byte> AsBytes()
    {
        if (_bytes is null)
            throw WrongType("a string or error");
        return _bytes;
    }

    public string? AsString()
    {
        if (Type == RespType.Null)
            return null;
        if (_bytes is null)
            throw WrongType("a string or error");
        return Encoding.UTF8.GetString(_bytes);
    }

    public long AsInt64() => Type == RespType.Integer ? _integer : throw WrongType("an integer");

    public double AsDouble() => Type == RespType.Double ? _double : throw WrongType("a double");

    public bool AsBoolean() => Type == RespType.Boolean ? _boolean : throw WrongType("a boolean");

    public BigInteger AsBigInteger()
    {
        if (Type != RespType.BigNumber)
            throw WrongType("a big number");
        return BigInteger.Parse(AsString()!, CultureInfo.InvariantCulture);
    }

    public IReadOnlyList<RespValue> AsArray()
    {
        if (Type is not (RespType.Array or RespType.Set or RespType.Push))
            throw WrongType("an array, set, or push");
        return _items!;
    }

    public IReadOnlyList<KeyValuePair<RespValue, RespValue>> AsMap()
    {
        if (Type != RespType.Map)
            throw WrongType("a map");
        return _pairs!;
    }

    public ValkeyServerException ToServerException()
    {
        if (Type is not (RespType.SimpleError or RespType.BlobError))
            throw WrongType("an error");
        return new ValkeyServerException(AsString()!);
    }

    public void ThrowIfError()
    {
        if (Type is RespType.SimpleError or RespType.BlobError)
            throw ToServerException();
    }

    /// <summary>
    /// A short, bounded diagnostic form for logs. String and error payloads are reported as their
    /// type, byte length, and a truncated preview escaped to printable ASCII — never the whole value.
    /// </summary>
    public override string ToString() =>
        Type switch
        {
            RespType.Null => "(null)",
            RespType.Integer => _integer.ToString(CultureInfo.InvariantCulture),
            RespType.Double => _double.ToString("R", CultureInfo.InvariantCulture),
            RespType.Boolean => _boolean ? "true" : "false",
            RespType.Array or RespType.Set or RespType.Push => $"{Type}[{_items!.Count}]",
            RespType.Map => $"Map[{_pairs!.Count}]",
            _ => DescribePayload(),
        };

    private string DescribePayload()
    {
        if (_bytes is null)
            return Type.ToString();

        // Decode only what the preview can show: a payload may be megabytes, and a log line must not
        // carry — or pay for — the whole thing.
        var decodedBytes = Math.Min(_bytes.Length, PreviewCharacters * MaxUtf8BytesPerCharacter);
        var text = Encoding.UTF8.GetString(_bytes, 0, decodedBytes);
        var shown = Math.Min(text.Length, PreviewCharacters);

        var builder = new StringBuilder();
        builder.Append(Type).Append('(').Append(_bytes.Length).Append(") \"");
        for (var i = 0; i < shown; i++)
            AppendEscaped(builder, text[i]);
        builder.Append('"');
        if (shown < text.Length || decodedBytes < _bytes.Length)
            builder.Append('…');
        return builder.ToString();
    }

    /// <summary>
    /// Escapes to printable ASCII so a payload cannot inject newlines or control sequences into a
    /// log line, and so the truncation marker is never confused with payload content.
    /// </summary>
    private static void AppendEscaped(StringBuilder builder, char character)
    {
        switch (character)
        {
            case '\\':
                builder.Append("\\\\");
                break;
            case '"':
                builder.Append("\\\"");
                break;
            case '\r':
                builder.Append("\\r");
                break;
            case '\n':
                builder.Append("\\n");
                break;
            case '\t':
                builder.Append("\\t");
                break;
            default:
                if (character is >= ' ' and <= '~')
                    builder.Append(character);
                else
                    builder.Append("\\u").Append(((int)character).ToString("X4", CultureInfo.InvariantCulture));
                break;
        }
    }

    private InvalidOperationException WrongType(string expected) => new($"RESP value is {Type}, not {expected}.");

    internal static RespValue Null() => new(RespType.Null);

    internal static RespValue Bytes(RespType type, byte[] value, string? format = null) =>
        new(type, bytes: value, verbatimFormat: format);

    internal static RespValue Integer(long value) => new(RespType.Integer, integer: value);

    internal static RespValue Double(double value) => new(RespType.Double, @double: value);

    internal static RespValue Boolean(bool value) => new(RespType.Boolean, boolean: value);

    internal static RespValue Items(RespType type, IReadOnlyList<RespValue> values) => new(type, items: values);

    internal static RespValue Pairs(RespType type, IReadOnlyList<KeyValuePair<RespValue, RespValue>> values) =>
        new(type, pairs: values);

    internal RespValue WithAttributes(IReadOnlyList<KeyValuePair<RespValue, RespValue>> attributes) =>
        new(Type, _bytes, _integer, _double, _boolean, _items, _pairs, attributes, VerbatimFormat);
}
