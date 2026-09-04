using System.Globalization;
using System.Text;
using NSubstitute;
using ValkeyDotNet.Internal;

namespace ValkeyDotNet.Tests;

public sealed class RespReaderTests
{
    [Fact]
    public async Task ReadAsyncParsesResp2Values()
    {
        var reader = Reader("+OK\r\n-ERR wrong\r\n:42\r\n$5\r\nhello\r\n$-1\r\n*2\r\n:1\r\n$3\r\ntwo\r\n");

        Assert.Equal("OK", (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsString());
        Assert.Equal(RespType.SimpleError, (await reader.ReadAsync(TestContext.Current.CancellationToken)).Type);
        Assert.Equal(42L, (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsInt64());
        Assert.Equal("hello", (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsString());
        Assert.True((await reader.ReadAsync(TestContext.Current.CancellationToken)).IsNull);
        var array = (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsArray();
        Assert.Equal(1L, array[0].AsInt64());
        Assert.Equal("two", array[1].AsString());
    }

    [Fact]
    public async Task ReadAsyncParsesResp3Scalars()
    {
        var reader = Reader(
            "_\r\n#t\r\n#f\r\n,1.5\r\n,inf\r\n(3492890328409238509324850943850943825024385\r\n=15\r\ntxt:hello world\r\n!10\r\nERR broken\r\n"
        );

        Assert.True((await reader.ReadAsync(TestContext.Current.CancellationToken)).IsNull);
        Assert.True((await reader.ReadAsync(TestContext.Current.CancellationToken)).AsBoolean());
        Assert.False((await reader.ReadAsync(TestContext.Current.CancellationToken)).AsBoolean());
        Assert.Equal(1.5, (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsDouble());
        Assert.True(
            double.IsPositiveInfinity((await reader.ReadAsync(TestContext.Current.CancellationToken)).AsDouble())
        );
        Assert.Equal(
            "3492890328409238509324850943850943825024385",
            (await reader.ReadAsync(TestContext.Current.CancellationToken))
                .AsBigInteger()
                .ToString(CultureInfo.InvariantCulture)
        );
        var verbatim = await reader.ReadAsync(TestContext.Current.CancellationToken);
        Assert.Equal("txt", verbatim.VerbatimFormat);
        Assert.Equal("hello world", verbatim.AsString());
        Assert.Equal(RespType.BlobError, (await reader.ReadAsync(TestContext.Current.CancellationToken)).Type);
    }

    [Fact]
    public async Task ReadAsyncParsesResp3Aggregates()
    {
        var reader = Reader("%2\r\n+one\r\n:1\r\n+two\r\n:2\r\n~2\r\n+a\r\n+b\r\n>2\r\n+invalidate\r\n$3\r\nkey\r\n");

        var map = (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsMap();
        Assert.Equal(2, map.Count);
        Assert.Equal("two", map[1].Key.AsString());
        Assert.Equal(2L, map[1].Value.AsInt64());
        Assert.Equal(2, (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsArray().Count);
        var push = await reader.ReadAsync(TestContext.Current.CancellationToken);
        Assert.Equal(RespType.Push, push.Type);
        Assert.Equal("invalidate", push.AsArray()[0].AsString());
    }

    [Fact]
    public async Task ReadAsyncAttachesAttributes()
    {
        var value = await Reader("|1\r\n+ttl\r\n:10\r\n$5\r\nhello\r\n")
            .ReadAsync(TestContext.Current.CancellationToken);

        Assert.Equal("hello", value.AsString());
        var attribute = Assert.Single(value.Attributes);
        Assert.Equal("ttl", attribute.Key.AsString());
        Assert.Equal(10L, attribute.Value.AsInt64());
    }

    [Fact]
    public async Task ReadAsyncParsesStreamedValues()
    {
        var reader = Reader("$?\r\n;5\r\nhello\r\n;1\r\n!\r\n;0\r\n*?\r\n:1\r\n:2\r\n.\r\n%?\r\n+a\r\n:1\r\n.\r\n");

        Assert.Equal("hello!", (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsString());
        Assert.Equal(2, (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsArray().Count);
        Assert.Single((await reader.ReadAsync(TestContext.Current.CancellationToken)).AsMap());
    }

    [Fact]
    public async Task ReadAsyncRejectsMalformedData()
    {
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader("#x\r\n").ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData("&nope\r\n")] // Unknown type byte.
    [InlineData("=5\r\nhello\r\n")] // Verbatim string with no three-byte format.
    [InlineData("=3\r\ntxt\r\n")] // Verbatim string too short to carry a format and a colon.
    [InlineData("+OK\rX")] // CR not followed by LF.
    [InlineData("$?\r\n:5\r\n")] // Streamed chunk that does not start with ';'.
    [InlineData("$?\r\n;-1\r\n")] // Negative streamed chunk length.
    [InlineData("*?\r\n:1\r\n.x\r\n")] // Streamed aggregate terminator with trailing data.
    [InlineData("%?\r\n+a\r\n:1\r\n.x\r\n")] // Streamed map terminator with trailing data.
    [InlineData("_x\r\n")] // RESP3 null with a payload.
    public async Task ReadAsyncRejectsMalformedFrames(string payload)
    {
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData(":99999999999999999999\r\n")] // Beyond Int64.
    [InlineData(":not-a-number\r\n")]
    [InlineData("$9999999999\r\n")] // Beyond Int32, so unusable as a length.
    [InlineData("*99999999999\r\n")]
    [InlineData("%99999999999\r\n")]
    [InlineData(",not-a-double\r\n")]
    [InlineData("(not-a-big-number\r\n")]
    public async Task ReadAsyncRejectsNumbersOutsideTheSupportedRange(string payload)
    {
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData("!-1\r\n")] // Blob errors have no null form.
    [InlineData("=-1\r\n")]
    [InlineData("%-1\r\n")]
    [InlineData("~-1\r\n")]
    [InlineData(">-1\r\n")]
    [InlineData("|-1\r\n")] // Attributes cannot be null.
    [InlineData("$-2\r\n")] // -1 is the only negative length RESP defines.
    [InlineData("*-2\r\n")]
    public async Task ReadAsyncRejectsNullLengthsOnTypesThatCannotBeNull(string payload)
    {
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReadAsyncAcceptsNullLengthsOnlyForBulkStringsAndArrays()
    {
        var reader = Reader("$-1\r\n*-1\r\n");

        Assert.True((await reader.ReadAsync(TestContext.Current.CancellationToken)).IsNull);
        Assert.True((await reader.ReadAsync(TestContext.Current.CancellationToken)).IsNull);
    }

    [Theory]
    [InlineData("")]
    [InlineData("+OK")]
    [InlineData("-ERR broken")]
    [InlineData(":42")]
    [InlineData("_")]
    [InlineData("#")]
    [InlineData(",1.5")]
    [InlineData("(123")]
    [InlineData("$5\r\nhel")]
    [InlineData("$5\r\nhello")] // Payload complete, trailing CRLF missing.
    [InlineData("!10\r\nERR")]
    [InlineData("=9\r\ntxt:hel")]
    [InlineData("*2\r\n:1\r\n")]
    [InlineData("%1\r\n+key\r\n")]
    [InlineData("~1\r\n")]
    [InlineData(">1\r\n")]
    [InlineData("|1\r\n+ttl\r\n:10\r\n")] // Attributes parsed, attributed value missing.
    [InlineData("$?\r\n;5\r\nhel")]
    [InlineData("*?\r\n:1\r\n")]
    [InlineData("%?\r\n+a\r\n")]
    public async Task ReadAsyncRejectsTruncatedFrames(string payload)
    {
        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
            await Reader(payload).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData("*1000000\r\n")]
    [InlineData("*2147483647\r\n")]
    [InlineData("~2147483647\r\n")]
    [InlineData(">2147483647\r\n")]
    [InlineData("%1073741823\r\n")]
    public async Task ReadAsyncRejectsACardinalityTheByteLimitCannotHold(string payload)
    {
        // Every element costs at least three bytes on the wire, so these counts are impossible
        // inside the configured frame limit however the server follows them up.
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload, maxResponseBytes: 1024).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReadAsyncDoesNotAllocateForAnImpossibleCardinality()
    {
        var reader = Reader("*100000000\r\n", maxResponseBytes: 1024);

        var before = GC.GetAllocatedBytesForCurrentThread();
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await reader.ReadAsync(TestContext.Current.CancellationToken)
        );
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allocated < 1024 * 1024, $"rejecting a hostile count allocated {allocated} bytes");
    }

    [Fact]
    public async Task ReadAsyncRejectsMoreValuesThanTheElementLimit()
    {
        var payload = "*20\r\n" + string.Concat(Enumerable.Repeat(":1\r\n", 20));

        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload, maxResponseElements: 16).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReadAsyncRejectsMoreStreamedValuesThanTheElementLimit()
    {
        // A streamed aggregate declares no count, so the limit has to be charged per decoded value.
        var payload = "*?\r\n" + string.Concat(Enumerable.Repeat(":1\r\n", 40)) + ".\r\n";

        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload, maxResponseElements: 16).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReadAsyncChargesNestedAggregatesAgainstOneElementBudget()
    {
        var payload = string.Concat(Enumerable.Repeat("*1\r\n", 20)) + ":1\r\n";

        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload, maxResponseElements: 16, maxDepth: 64)
                .ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReadAsyncRejectsNestingBeyondTheDepthLimit()
    {
        var payload = string.Concat(Enumerable.Repeat("*1\r\n", 10)) + ":1\r\n";

        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload, maxDepth: 4).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReadAsyncRejectsABlobLargerThanTheByteLimit()
    {
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader("$4096\r\n", maxResponseBytes: 1024).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReadAsyncRejectsAFrameLargerThanTheByteLimitAcrossManyValues()
    {
        var payload = "*400\r\n" + string.Concat(Enumerable.Repeat("$3\r\nabc\r\n", 400));

        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await Reader(payload, maxResponseBytes: 1024).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReadAsyncResetsTheFrameBudgetBetweenReplies()
    {
        // The bounds are per reply, not per connection: two large-but-legal frames in a row must
        // both parse.
        var frame = "$600\r\n" + new string('x', 600) + "\r\n";
        var reader = Reader(frame + frame, maxResponseBytes: 1024);

        Assert.Equal(600, (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsBytes().Length);
        Assert.Equal(600, (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsBytes().Length);
    }

    [Fact]
    public async Task ReadAsyncUsesSubstitutedStream()
    {
        var payload = "+PONG\r\n"u8.ToArray();
        var offset = 0;
        var stream = Substitute.For<Stream>();
#pragma warning disable CA2012 // NSubstitute captures this ValueTask-returning call as configuration.
        stream
            .ReadAsync(Arg.Any<Memory<byte>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var destination = call.ArgAt<Memory<byte>>(0);
                var count = Math.Min(2, payload.Length - offset);
                if (count == 0)
                    return ValueTask.FromResult(0);
                payload.AsMemory(offset, count).CopyTo(destination);
                offset += count;
                return ValueTask.FromResult(count);
            });
#pragma warning restore CA2012

        var value = await new RespReader(stream, 1024, 1024, 8).ReadAsync(TestContext.Current.CancellationToken);

        Assert.Equal("PONG", value.AsString());
    }

    private static RespReader Reader(
        string value,
        int maxResponseBytes = 1024 * 1024,
        int maxResponseElements = 4096,
        int maxDepth = 32
    ) => new(new MemoryStream(Encoding.UTF8.GetBytes(value)), maxResponseBytes, maxResponseElements, maxDepth);
}
