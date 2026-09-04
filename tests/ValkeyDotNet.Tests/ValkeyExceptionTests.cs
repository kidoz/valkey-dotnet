namespace ValkeyDotNet.Tests;

public sealed class ValkeyExceptionTests
{
    [Fact]
    public void ServerExceptionExposesErrorCode()
    {
        var error = new ValkeyServerException("WRONGTYPE operation against a key");

        Assert.Equal("WRONGTYPE", error.ErrorCode);
    }
}
