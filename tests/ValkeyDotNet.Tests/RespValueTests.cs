using System.Text;

namespace ValkeyDotNet.Tests;

public sealed class RespValueTests
{
    [Fact]
    public void ToStringDescribesScalars()
    {
        Assert.Equal("(null)", RespValue.Null().ToString());
        Assert.Equal("42", RespValue.Integer(42).ToString());
        Assert.Equal("1.5", RespValue.Double(1.5).ToString());
        Assert.Equal("true", RespValue.Boolean(true).ToString());
    }

    [Fact]
    public void ToStringDescribesAggregatesByShapeOnly()
    {
        var items = new[] { RespValue.Integer(1), RespValue.Integer(2) };

        Assert.Equal("Array[2]", RespValue.Items(RespType.Array, items).ToString());
        Assert.Equal("Set[2]", RespValue.Items(RespType.Set, items).ToString());
        Assert.Equal("Push[2]", RespValue.Items(RespType.Push, items).ToString());
        Assert.Equal(
            "Map[1]",
            RespValue.Pairs(RespType.Map, [new(RespValue.Integer(1), RespValue.Integer(2))]).ToString()
        );
    }

    [Fact]
    public void ToStringReportsTypeAndLengthWithThePayload()
    {
        var value = RespValue.Bytes(RespType.BlobString, "hello world"u8.ToArray());

        Assert.Equal("BlobString(11) \"hello world\"", value.ToString());
    }

    [Fact]
    public void ToStringEscapesControlCharactersAndQuotes()
    {
        // Built byte-wise so the expectation is unambiguous: a, CR, LF, b, TAB, c, ", d, backslash, e, SOH.
        var payload = new byte[] { 0x61, 0x0D, 0x0A, 0x62, 0x09, 0x63, 0x22, 0x64, 0x5C, 0x65, 0x01 };

        var text = RespValue.Bytes(RespType.SimpleError, payload).ToString();

        Assert.Equal("SimpleError(11) \"a\\r\\nb\\tc\\\"d\\\\e\\u0001\"", text);
    }

    [Fact]
    public void ToStringTruncatesALargePayload()
    {
        var value = RespValue.Bytes(RespType.BlobString, Encoding.ASCII.GetBytes(new string('a', 64 * 1024)));

        var text = value.ToString();

        Assert.StartsWith("BlobString(65536) \"", text, StringComparison.Ordinal);
        Assert.EndsWith("…", text, StringComparison.Ordinal);
        Assert.True(text.Length < 100, $"diagnostic form was {text.Length} characters long");
    }

    [Fact]
    public void ToStringNeverEmitsNonPrintableCharacters()
    {
        var payload = new byte[256];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)i;

        var text = RespValue.Bytes(RespType.BlobString, payload).ToString();

        Assert.True(text.All(static character => character is (>= ' ' and <= '~') or '…'), text);
    }

    [Fact]
    public void AccessorsNameTheActualTypeOnAMismatch()
    {
        var value = RespValue.Integer(1);

        Assert.Contains(
            "Integer",
            Assert.Throws<InvalidOperationException>(() => value.AsMap()).Message,
            StringComparison.Ordinal
        );
        Assert.Throws<InvalidOperationException>(() => value.AsArray());
        Assert.Throws<InvalidOperationException>(() => value.AsBytes());
        Assert.Throws<InvalidOperationException>(() => value.AsDouble());
        Assert.Throws<InvalidOperationException>(() => value.AsBoolean());
        Assert.Throws<InvalidOperationException>(() => value.AsBigInteger());
    }

    [Fact]
    public void ThrowIfErrorOnlyThrowsForErrorTypes()
    {
        RespValue.Bytes(RespType.SimpleString, "OK"u8.ToArray()).ThrowIfError();

        Assert.Throws<ValkeyServerException>(RespValue.Bytes(RespType.SimpleError, "ERR x"u8.ToArray()).ThrowIfError);
        Assert.Throws<ValkeyServerException>(RespValue.Bytes(RespType.BlobError, "ERR x"u8.ToArray()).ThrowIfError);
    }
}
