using System.Collections.ObjectModel;
using System.Text;

namespace ValkeyDotNet;

/// <summary>A command name and its binary-safe arguments.</summary>
public sealed class ValkeyCommand
{
    private readonly byte[] _name;
    private readonly ValkeyArgument[] _arguments;

    public ValkeyCommand(string name, params ValkeyArgument[] arguments)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(arguments);
        foreach (var character in name)
        {
            // Command names are ASCII on the wire. Encoding anything else would silently substitute
            // '?' and send a command the caller never wrote.
            if (character is < '!' or > '~')
                throw new ArgumentException(
                    "A command name must be printable ASCII with no whitespace or control characters.",
                    nameof(name)
                );
        }

        Name = name.ToUpperInvariant();
        _name = Encoding.ASCII.GetBytes(Name);
        _arguments = arguments.ToArray();
        Arguments = new ReadOnlyCollection<ValkeyArgument>(_arguments);
    }

    public string Name { get; }

    /// <summary>The arguments, in wire order. Genuinely read-only: the backing array is not exposed.</summary>
    public IReadOnlyList<ValkeyArgument> Arguments { get; }

    internal ReadOnlyMemory<byte> NameBytes => _name;
    internal ReadOnlySpan<ValkeyArgument> ArgumentsSpan => _arguments;
}
