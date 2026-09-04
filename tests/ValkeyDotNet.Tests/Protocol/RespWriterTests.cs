using System.Text;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet.Tests.Protocol;

public sealed class RespWriterTests
{
    [Fact]
    public void EncodeIsBinarySafe()
    {
        var encoded = RespWriter.Encode(new ValkeyCommand("set", "key", new byte[] { 0, 13, 10, 255 }));
        var expectedPrefix = Encoding.ASCII.GetBytes("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$4\r\n");

        Assert.Equal(expectedPrefix, encoded[..expectedPrefix.Length]);
        Assert.Equal(new byte[] { 0, 13, 10, 255, 13, 10 }, encoded[expectedPrefix.Length..]);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(99)]
    [InlineData(100)]
    [InlineData(999)]
    [InlineData(1000)]
    public void EncodeWritesExactDecimalLengths(int length)
    {
        var encoded = RespWriter.Encode(new ValkeyCommand("SET", "key", new byte[length]));
        var marker = Encoding.ASCII.GetBytes($"${length}\r\n");

        Assert.Equal(marker, encoded.AsSpan(encoded.Length - length - 2 - marker.Length, marker.Length).ToArray());
        Assert.Equal((byte)'\r', encoded[^2]);
        Assert.Equal((byte)'\n', encoded[^1]);
    }
}
