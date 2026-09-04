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

    [Benchmark]
    public byte[] EncodeSet() => RespWriter.Encode(SetCommand);

    [Benchmark]
    public async Task<RespValue> ParseResp3Map()
    {
        var stream = new MemoryStream(Resp3Map, writable: false);
        var reader = new RespReader(stream, 4096, 1024, 16);
        return await reader.ReadAsync(default);
    }
}
