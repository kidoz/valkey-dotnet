using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class GetReplyBenchmarks
{
    private readonly byte[] _payload = Enumerable.Range(0, 1024).Select(i => (byte)i).ToArray();
    private RespReader _reader = null!;

    [Params(1, 100)]
    public int ReplyCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        byte[] frame = [.. "$1024\r\n"u8, .. _payload, .. "\r\n"u8];
        _reader = new RespReader(new RepeatingReplyStream(frame), 4096, 1024, 16);
    }

    // Both cases retain the same owned payload/value/array graph. The delta isolates parsing
    // temporaries for synchronous, unfragmented bulk replies, not transport or async suspension.
    [Benchmark(Baseline = true)]
    public RespValue[] MaterializeReplies()
    {
        var values = new RespValue[ReplyCount];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = RespValue.Bytes(RespType.BlobString, _payload.ToArray());
        }
        return values;
    }

    [Benchmark]
    public async ValueTask<RespValue[]> ParseReplies()
    {
        var values = new RespValue[ReplyCount];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = await _reader.ReadAsync(default);
        }
        return values;
    }

    private sealed class RepeatingReplyStream(byte[] frame) : Stream
    {
        private int _offset;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            var count = Math.Min(buffer.Length, frame.Length - _offset);
            frame.AsSpan(_offset, count).CopyTo(buffer);
            _offset = (_offset + count) % frame.Length;
            return count;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(Read(buffer.Span));
        }

        public override void Flush() { }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
