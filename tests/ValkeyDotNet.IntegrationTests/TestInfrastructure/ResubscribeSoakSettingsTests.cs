namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class ResubscribeSoakSettingsTests
{
    [Theory]
    [InlineData("")]
    [InlineData("19")]
    [InlineData("101")]
    [InlineData("-1")]
    [InlineData("+30")]
    [InlineData(" 30")]
    [InlineData("invalid")]
    [InlineData("2147483648")]
    public void RejectsInvalidOrUnboundedSoakCycles(string text)
    {
        Assert.Throws<InvalidOperationException>(() => ResubscribeSoakSettings.ParseCycles(text));
    }

    [Theory]
    [InlineData(null, 30)]
    [InlineData("20", 20)]
    [InlineData("100", 100)]
    public void AcceptsDefaultAndBoundarySoakCycles(string? text, int expected)
    {
        Assert.Equal(expected, ResubscribeSoakSettings.ParseCycles(text));
    }

    [Fact]
    public void CountsExactClientNamesWithoutPrefixOrFieldMatches()
    {
        const string clients =
            "id=1 name=owned ssub=1\r\nid=2 name=owned-extra\nid=3 othername=owned\nid=4 name=owned\n";
        Assert.Equal(2, ResubscribeSoakSettings.CountNamedClients(clients, "owned"));
        Assert.Equal(0, ResubscribeSoakSettings.CountNamedClients(clients, "absent"));
        Assert.Equal(0, ResubscribeSoakSettings.CountNamedClients("", "owned"));
    }
}
