namespace ValkeyDotNet.Tests;

public sealed class ValkeyClientOptionsTests
{
    [Fact]
    public void DefaultsAreValid()
    {
        new ValkeyClientOptions().Validate();
    }

    [Fact]
    public void RejectsAProtocolOutsideTheEnum()
    {
        var options = new ValkeyClientOptions { Protocol = (ValkeyProtocol)99 };

        Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void RejectsANonPositiveConnectTimeout(int milliseconds)
    {
        var options = new ValkeyClientOptions { ConnectTimeout = TimeSpan.FromMilliseconds(milliseconds) };

        Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
    }

    [Fact]
    public void RejectsAConnectTimeoutTheTimerCannotSchedule()
    {
        // Beyond this, CancelAfter itself throws — from inside the connect path, where nothing has
        // been set up to clean up after it.
        var options = new ValkeyClientOptions { ConnectTimeout = TimeSpan.MaxValue };

        Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
    }

    [Fact]
    public void AcceptsTheLargestSchedulableConnectTimeout()
    {
        new ValkeyClientOptions { ConnectTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1) }.Validate();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public void RejectsAPortOutsideTheValidRange(int port)
    {
        Assert.Throws<ArgumentOutOfRangeException>(new ValkeyClientOptions { Port = port }.Validate);
    }

    [Fact]
    public void RejectsBoundsBelowTheirFloor()
    {
        Assert.Throws<ArgumentOutOfRangeException>(new ValkeyClientOptions { MaxResponseBytes = 1023 }.Validate);
        Assert.Throws<ArgumentOutOfRangeException>(new ValkeyClientOptions { MaxResponseElements = 15 }.Validate);
        Assert.Throws<ArgumentOutOfRangeException>(new ValkeyClientOptions { MaxNestingDepth = 0 }.Validate);
        Assert.Throws<ArgumentOutOfRangeException>(new ValkeyClientOptions { MaxNestingDepth = 1025 }.Validate);
        Assert.Throws<ArgumentOutOfRangeException>(new ValkeyClientOptions { Database = -1 }.Validate);
    }

    [Fact]
    public void RejectsAUsernameWithoutAPassword()
    {
        Assert.Throws<ArgumentException>(new ValkeyClientOptions { Username = "app" }.Validate);
    }

    [Fact]
    public void RejectsABlankHost()
    {
        Assert.Throws<ArgumentException>(new ValkeyClientOptions { Host = "  " }.Validate);
    }
}
