namespace ValkeyDotNet.Internal;

internal static class ClusterHashSlot
{
    public const int Count = 16_384;

    public static int Calculate(ReadOnlySpan<byte> key)
    {
        var hashInput = GetHashInput(key);
        ushort crc = 0;
        foreach (var value in hashInput)
        {
            crc ^= (ushort)(value << 8);
            for (var bit = 0; bit < 8; bit++)
                crc = (ushort)((crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1);
        }
        return crc & (Count - 1);
    }

    private static ReadOnlySpan<byte> GetHashInput(ReadOnlySpan<byte> key)
    {
        var openingBrace = key.IndexOf((byte)'{');
        if (openingBrace < 0)
            return key;

        var tagged = key[(openingBrace + 1)..];
        var closingBrace = tagged.IndexOf((byte)'}');
        return closingBrace > 0 ? tagged[..closingBrace] : key;
    }
}
