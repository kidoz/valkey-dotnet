using System.Text;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet.Tests.Protocol;

public sealed class RespReaderBlobLengthTests
{
    [Theory]
    [InlineData("$4\r\n", "")]
    [InlineData("$0004\r\n", "")]
    [InlineData("$+4\r\n", "")]
    [InlineData("!4\r\n", "")]
    [InlineData("=8\r\ntxt:", "")]
    [InlineData("$?\r\n;4\r\n", ";0\r\n")]
    public async Task PreservesBinaryValuesAndFollowingReplyAcrossEverySplit(string header, string terminator)
    {
        ArgumentNullException.ThrowIfNull(header);
        byte[] expected = [0, 255, 13, 10];
        byte[] frame =
        [
            .. Encoding.ASCII.GetBytes(header),
            .. expected,
            .. "\r\n"u8,
            .. Encoding.ASCII.GetBytes(terminator),
            .. "+OK\r\n"u8,
        ];
        for (var split = 1; split <= frame.Length; split++)
        {
            await using var stream = new FragmentedStream(frame, split, frame.Length);
            var reader = new RespReader(stream, frame.Length, 32, 8);
            var value = await reader.ReadAsync(TestContext.Current.CancellationToken);
            Assert.Equal(expected, value.AsBytes().ToArray());
            Assert.Equal(
                header[0] == '!' ? RespType.BlobError
                    : header[0] == '=' ? RespType.VerbatimString
                    : RespType.BlobString,
                value.Type
            );
            if (header[0] == '=')
            {
                Assert.Equal("txt", value.VerbatimFormat);
            }
            Assert.Equal("OK", (await reader.ReadAsync(TestContext.Current.CancellationToken)).AsString());
            // Later reads must never overwrite an earlier caller's owned payload.
            Assert.Equal(expected, value.AsBytes().ToArray());
        }
    }

    [Theory]
    [InlineData("$0\r\n\r\n", false)]
    [InlineData("$-0\r\n\r\n", false)]
    [InlineData("$+0\r\n\r\n", false)]
    [InlineData("$-1\r\n", true)]
    [InlineData("$-01\r\n", true)]
    public async Task PreservesEmptyAndNullForms(string frame, bool isNull)
    {
        ArgumentNullException.ThrowIfNull(frame);
        foreach (var chunk in new[] { 1, frame.Length })
        {
            await using var stream = new FragmentedStream(Encoding.ASCII.GetBytes(frame), chunk, chunk);
            var value = await new RespReader(stream, frame.Length, 1, 0).ReadAsync(
                TestContext.Current.CancellationToken
            );
            Assert.Equal(isNull, value.IsNull);
            if (!isNull)
            {
                Assert.Empty(value.AsBytes().ToArray());
            }
        }
    }

    [Theory]
    [InlineData("$2147483647\r\n")]
    [InlineData("$2147483648\r\n")]
    [InlineData("$9223372036854775807\r\n")]
    [InlineData("$9223372036854775808\r\n")]
    [InlineData("$-9223372036854775809\r\n")]
    [InlineData("$-2\r\n")]
    [InlineData("!-1\r\n")]
    [InlineData("=-1\r\n")]
    [InlineData("$\r\n")]
    [InlineData("$+\r\n")]
    [InlineData("$ 1\r\n")]
    [InlineData("$1 \r\n")]
    [InlineData("$1x\r\n")]
    [InlineData("$1\rX")]
    [InlineData("$?\r\n;-1\r\n")]
    public async Task RejectsInvalidOrImpossibleLengthsBufferedAndFragmented(string frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        foreach (var chunk in new[] { 1, frame.Length })
        {
            await using var stream = new FragmentedStream(Encoding.ASCII.GetBytes(frame), chunk, chunk);
            var reader = new RespReader(stream, 128, 32, 8);
            await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
                await reader.ReadAsync(TestContext.Current.CancellationToken)
            );
        }
    }

    [Theory]
    [InlineData("$")]
    [InlineData("$12")]
    [InlineData("$12\r")]
    [InlineData("$4\r\nabc")]
    [InlineData("$4\r\nabcd\r")]
    public async Task RejectsTruncationAtHeaderPayloadAndTrailer(string frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        foreach (var chunk in new[] { 1, frame.Length })
        {
            await using var stream = new FragmentedStream(Encoding.ASCII.GetBytes(frame), chunk, chunk);
            await Assert.ThrowsAsync<EndOfStreamException>(async () =>
                await new RespReader(stream, 128, 32, 8).ReadAsync(TestContext.Current.CancellationToken)
            );
        }
    }

    [Theory]
    [InlineData("$4\r\nabcd\r\n")]
    [InlineData("$0004\r\nabcd\r\n")]
    [InlineData("*1\r\n$4\r\nabcd\r\n")]
    [InlineData("$?\r\n;4\r\nabcd\r\n;0\r\n")]
    public async Task AccountsForAllHeaderAndPayloadBytesAtEveryBudget(string frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        foreach (var chunk in new[] { 1, frame.Length })
        {
            for (var limit = 1; limit < frame.Length; limit++)
            {
                await using var stream = new FragmentedStream(Encoding.ASCII.GetBytes(frame), chunk, chunk);
                await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
                    await new RespReader(stream, limit, 32, 8).ReadAsync(TestContext.Current.CancellationToken)
                );
            }
            await using var exact = new FragmentedStream(Encoding.ASCII.GetBytes(frame + frame), chunk, chunk);
            var reader = new RespReader(exact, frame.Length, 32, 8);
            await reader.ReadAsync(TestContext.Current.CancellationToken);
            await reader.ReadAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task RejectsMaximumLengthBeforeAllocatingPayload()
    {
        await using var stream = new MemoryStream("$2147483647\r\n"u8.ToArray());
        var reader = new RespReader(stream, 128, 1, 0);
        var before = GC.GetAllocatedBytesForCurrentThread();
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await reader.ReadAsync(TestContext.Current.CancellationToken)
        );
        Assert.True(GC.GetAllocatedBytesForCurrentThread() - before < 1024 * 1024);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(1, 1)]
    [InlineData(2, 0)]
    public async Task BufferedBlobsRetainElementAndDepthLimits(int elements, int depth)
    {
        await using var stream = new MemoryStream("*1\r\n$1\r\na\r\n"u8.ToArray());
        await Assert.ThrowsAsync<ValkeyProtocolException>(async () =>
            await new RespReader(stream, 128, elements, depth).ReadAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task LongZeroPaddedLengthFallsBackAcrossTheConnectionBuffer()
    {
        var frame = "$" + new string('0', 9000) + "1\r\na\r\n";
        await using var stream = new MemoryStream(Encoding.ASCII.GetBytes(frame));
        var result = await new RespReader(stream, frame.Length, 1, 0).ReadAsync(TestContext.Current.CancellationToken);
        Assert.Equal("a", result.AsString());
    }

    [Fact]
    public async Task BufferedAndFragmentedPathsAgreeOnAllHeaderByteValues()
    {
        for (var value = 0; value <= byte.MaxValue; value++)
        {
            byte[] frame = [.. "$1"u8, (byte)value, .. "\r\na\r\n"u8];
            Assert.Equal(await OutcomeAsync(frame, frame.Length), await OutcomeAsync(frame, 1));
        }
    }

    [Fact]
    public async Task CancellationDuringHeaderFallbackReachesTheStream()
    {
        using var cancellation = new CancellationTokenSource();
        await using var stream = new FragmentedStream("$123\r\n"u8.ToArray(), 3, 1, cancellation);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await new RespReader(stream, 128, 32, 8).ReadAsync(cancellation.Token)
        );
    }

    private static async Task<string> OutcomeAsync(byte[] frame, int chunk)
    {
        await using var stream = new FragmentedStream(frame, chunk, chunk);
        try
        {
            var value = await new RespReader(stream, 128, 32, 8).ReadAsync(TestContext.Current.CancellationToken);
            return value.IsNull ? "null" : Convert.ToHexString(value.AsBytes().Span);
        }
        catch (ValkeyProtocolException exception)
        {
            return "protocol:" + exception.Message;
        }
        catch (EndOfStreamException exception)
        {
            return "eof:" + exception.Message;
        }
    }

    private sealed class FragmentedStream(
        byte[] frame,
        int firstRead,
        int laterReads,
        CancellationTokenSource? cancelAfterFirstRead = null
    ) : MemoryStream(frame)
    {
        private bool _first = true;

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default
        )
        {
            await Task.Yield();
            var count = Math.Min(buffer.Length, _first ? firstRead : laterReads);
            if (!_first && cancelAfterFirstRead is not null)
            {
                await cancelAfterFirstRead.CancelAsync();
            }
            _first = false;
            return await base.ReadAsync(buffer[..count], cancellationToken);
        }
    }
}
