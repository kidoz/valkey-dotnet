using System.Text;
using ValkeyDotNet.Internal;

namespace ValkeyDotNet.Tests;

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
}
