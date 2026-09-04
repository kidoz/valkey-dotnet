using System.Buffers;
using System.Globalization;
using System.Text;

namespace ValkeyDotNet.Internal;

internal static class RespWriter
{
    private static readonly byte[] CrLf = "\r\n"u8.ToArray();

    public static byte[] Encode(ValkeyCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        var arguments = command.ArgumentsSpan;
        var writer = new ArrayBufferWriter<byte>();
        WriteAscii(writer, $"*{arguments.Length + 1}\r\n");
        WriteBulkString(writer, command.NameBytes.Span);
        foreach (var argument in arguments)
            WriteBulkString(writer, argument.Bytes.Span);
        return writer.WrittenSpan.ToArray();
    }

    private static void WriteBulkString(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        WriteAscii(writer, "$" + value.Length.ToString(CultureInfo.InvariantCulture) + "\r\n");
        writer.Write(value);
        writer.Write(CrLf);
    }

    private static void WriteAscii(IBufferWriter<byte> writer, string value) =>
        writer.Write(Encoding.ASCII.GetBytes(value));
}
