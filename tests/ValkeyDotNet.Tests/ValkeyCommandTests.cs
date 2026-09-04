using System.Text;
using ValkeyDotNet.Internal;

namespace ValkeyDotNet.Tests;

public sealed class ValkeyCommandTests
{
    [Fact]
    public void NameIsUpperCased()
    {
        Assert.Equal("GET", new ValkeyCommand("get").Name);
    }

    [Theory]
    [InlineData("Σ")] // Non-ASCII: ASCII encoding would silently send '?'.
    [InlineData("GÉT")]
    [InlineData("GE T")]
    [InlineData("GET\r\n")] // CRLF would forge an extra protocol frame.
    [InlineData("GET\t")]
    [InlineData("GE\u0000T")] // A NUL would truncate the name for some readers.
    [InlineData("GET\u007f")]
    public void RejectsANameThatIsNotPrintableAscii(string name)
    {
        Assert.Throws<ArgumentException>(() => new ValkeyCommand(name));
    }

    [Theory]
    [InlineData("JSON.SET")] // Module commands stay usable.
    [InlineData("FT.SEARCH")]
    [InlineData("CLIENT")]
    public void AcceptsPrintableAsciiNames(string name)
    {
        Assert.Equal(name, new ValkeyCommand(name).Name);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void RejectsABlankName(string name)
    {
        Assert.Throws<ArgumentException>(() => new ValkeyCommand(name));
    }

    [Fact]
    public void RejectsANullName()
    {
        Assert.Throws<ArgumentNullException>(() => new ValkeyCommand(null!));
    }

    [Fact]
    public void ArgumentsAreCopiedFromTheCallersArray()
    {
        var arguments = new ValkeyArgument[] { "original" };
        var command = new ValkeyCommand("GET", arguments);

        arguments[0] = "swapped";

        Assert.Equal("original", Encoding.UTF8.GetString(command.Arguments[0].Bytes.Span));
    }

    [Fact]
    public void ArgumentsCannotBeMutatedThroughTheExposedList()
    {
        var command = new ValkeyCommand("GET", "key");

        // The backing array is not handed out, so a caller cannot cast the list back and rewrite the
        // command after it has been validated.
        Assert.IsNotType<ValkeyArgument[]>(command.Arguments);
        Assert.False(command.Arguments is IList<ValkeyArgument> { IsReadOnly: false });
    }

    [Fact]
    public void RejectsANullArgumentArray()
    {
        Assert.Throws<ArgumentNullException>(() => new ValkeyCommand("GET", null!));
    }

    [Fact]
    public void EncodesTheUpperCasedNameAsAscii()
    {
        var encoded = RespWriter.Encode(new ValkeyCommand("ping"));

        Assert.Equal("*1\r\n$4\r\nPING\r\n", Encoding.ASCII.GetString(encoded));
    }
}
