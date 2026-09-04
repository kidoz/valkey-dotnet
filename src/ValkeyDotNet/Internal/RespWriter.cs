using System.Buffers.Text;
using System.Diagnostics;

namespace ValkeyDotNet.Internal;

internal static class RespWriter
{
    public static byte[] Encode(ValkeyCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        var arguments = command.ArgumentsSpan;
        var argumentCount = checked(arguments.Length + 1);
        var length = GetArrayHeaderLength(argumentCount) + GetBulkStringLength(command.NameBytes.Length);
        foreach (var argument in arguments)
            length = checked(length + GetBulkStringLength(argument.Bytes.Length));

        var result = GC.AllocateUninitializedArray<byte>(length);
        var written = WriteArrayHeader(result, argumentCount);
        written += WriteBulkString(result.AsSpan(written), command.NameBytes.Span);
        foreach (var argument in arguments)
            written += WriteBulkString(result.AsSpan(written), argument.Bytes.Span);

        Debug.Assert(written == result.Length, "RESP command size calculation must exactly match the encoded payload.");
        return result;
    }

    private static int GetArrayHeaderLength(int count) => 1 + GetDecimalLength(count) + 2;

    private static int GetBulkStringLength(int length) => checked(1 + GetDecimalLength(length) + 2 + length + 2);

    private static int GetDecimalLength(int value) =>
        value switch
        {
            < 10 => 1,
            < 100 => 2,
            < 1_000 => 3,
            < 10_000 => 4,
            < 100_000 => 5,
            < 1_000_000 => 6,
            < 10_000_000 => 7,
            < 100_000_000 => 8,
            < 1_000_000_000 => 9,
            _ => 10,
        };

    private static int WriteArrayHeader(Span<byte> destination, int count)
    {
        destination[0] = (byte)'*';
        if (!Utf8Formatter.TryFormat(count, destination[1..], out var digits))
            throw new InvalidOperationException("The RESP command buffer was sized incorrectly.");
        WriteCrLf(destination[(digits + 1)..]);
        return digits + 3;
    }

    private static int WriteBulkString(Span<byte> destination, ReadOnlySpan<byte> value)
    {
        destination[0] = (byte)'$';
        if (!Utf8Formatter.TryFormat(value.Length, destination[1..], out var digits))
            throw new InvalidOperationException("The RESP command buffer was sized incorrectly.");

        var offset = digits + 1;
        WriteCrLf(destination[offset..]);
        offset += 2;
        value.CopyTo(destination[offset..]);
        offset += value.Length;
        WriteCrLf(destination[offset..]);
        return offset + 2;
    }

    private static void WriteCrLf(Span<byte> destination)
    {
        destination[0] = (byte)'\r';
        destination[1] = (byte)'\n';
    }
}
