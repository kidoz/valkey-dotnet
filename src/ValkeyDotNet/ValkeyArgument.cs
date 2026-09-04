using System.Globalization;
using System.Text;

namespace ValkeyDotNet;

/// <summary>A binary-safe command argument.</summary>
public readonly struct ValkeyArgument
{
    private readonly ReadOnlyMemory<byte> _value;

    public ValkeyArgument(ReadOnlyMemory<byte> value) => _value = value;

    public ReadOnlyMemory<byte> Bytes => _value;

    public static implicit operator ValkeyArgument(string value) =>
        new(Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))));

    public static implicit operator ValkeyArgument(byte[] value) =>
        new(value ?? throw new ArgumentNullException(nameof(value)));

    public static implicit operator ValkeyArgument(ReadOnlyMemory<byte> value) => new(value);

    public static implicit operator ValkeyArgument(int value) =>
        new(Encoding.ASCII.GetBytes(value.ToString(CultureInfo.InvariantCulture)));

    public static implicit operator ValkeyArgument(long value) =>
        new(Encoding.ASCII.GetBytes(value.ToString(CultureInfo.InvariantCulture)));

    public static implicit operator ValkeyArgument(double value) =>
        new(Encoding.ASCII.GetBytes(value.ToString("R", CultureInfo.InvariantCulture)));
}
