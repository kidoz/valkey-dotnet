using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using ValkeyDotNet.Internal;

namespace ValkeyDotNet.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class ProtocolBenchmarks
{
    private static readonly ValkeyCommand SetCommand = new("SET", "benchmark:key", new byte[1024], "PX", 60_000);

    private static readonly byte[] Resp3Map =
        "%4\r\n+server\r\n+valkey\r\n+version\r\n+9.1.0\r\n+proto\r\n:3\r\n+role\r\n+master\r\n"u8.ToArray();

    private RespReader _reader = null!;

    [GlobalSetup]
    public void Setup() => _reader = new RespReader(new RepeatingFrameStream(Resp3Map), 4096, 1024, 16);

    [Benchmark]
    public byte[] EncodeSet() => RespWriter.Encode(SetCommand);

    [Benchmark]
    public ValueTask<RespValue> ParseResp3Map() => _reader.ReadAsync(default);

    private sealed class RepeatingFrameStream(byte[] frame) : Stream
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
            for (var written = 0; written < buffer.Length; )
            {
                var count = Math.Min(frame.Length - _offset, buffer.Length - written);
                frame.AsSpan(_offset, count).CopyTo(buffer[written..]);
                written += count;
                _offset = (_offset + count) % frame.Length;
            }
            return buffer.Length;
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
