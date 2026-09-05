namespace ValkeyDotNet.Benchmarks;

internal sealed record AllocationSampleRow(string Type, string Stack, long Samples, long SampledBytes);

internal sealed class AllocationSampleSummary
{
    private readonly Dictionary<(string Type, string Stack), AllocationSampleRow> _rows = [];

    internal long Samples { get; private set; }
    internal long SamplesWithoutStacks { get; private set; }
    internal long SampledAllocationBytes { get; private set; }

    internal void Add(string type, long sampledBytes, string stack)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(stack);
        ArgumentOutOfRangeException.ThrowIfNegative(sampledBytes);
        var key = (type, stack);
        var previous = _rows.GetValueOrDefault(key);
        _rows[key] = new(
            type,
            stack,
            checked((previous?.Samples ?? 0) + 1),
            checked((previous?.SampledBytes ?? 0) + sampledBytes)
        );
        Samples++;
        SampledAllocationBytes = checked(SampledAllocationBytes + sampledBytes);
        if (stack.Length == 0)
        {
            SamplesWithoutStacks++;
        }
    }

    internal AllocationSampleRow[] Rows() =>
        _rows
            .Values.OrderByDescending(row => row.SampledBytes)
            .ThenBy(row => row.Type, StringComparer.Ordinal)
            .ThenBy(row => row.Stack, StringComparer.Ordinal)
            .ToArray();
}
