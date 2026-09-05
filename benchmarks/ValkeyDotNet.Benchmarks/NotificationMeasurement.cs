using System.Diagnostics;

namespace ValkeyDotNet.Benchmarks;

internal sealed record NotificationMeasurement(
    string Operation,
    string Protocol,
    int Concurrency,
    int Samples,
    double AcknowledgmentsPerSecond,
    double DeliveredOperationsPerSecond,
    double AcknowledgmentP50Microseconds,
    double AcknowledgmentP95Microseconds,
    double AcknowledgmentP99Microseconds,
    double DeliveryP50Microseconds,
    double DeliveryP95Microseconds,
    double DeliveryP99Microseconds,
    double AllocatedBytesPerOperation,
    double[] AcknowledgmentMicroseconds,
    double[] DeliveryMicroseconds
);

// A delivery can precede its command acknowledgment. Both intervals start at invocation.
internal sealed class NotificationSamples
{
    private readonly long[] _starts;
    private readonly long[] _acknowledgments;
    private readonly long[] _deliveries;
    private readonly TaskCompletionSource _delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _remaining;

    internal NotificationSamples(int count)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, 8192);
        _starts = new long[count];
        _acknowledgments = new long[count];
        _deliveries = new long[count];
        _remaining = count;
    }

    internal Task Delivered => _delivered.Task;

    internal void Begin(int index, long timestamp)
    {
        Validate(index, timestamp);
        if (Interlocked.CompareExchange(ref _starts[index], timestamp, 0) != 0)
        {
            throw new InvalidOperationException("Duplicate notification operation.");
        }
    }

    internal void Acknowledge(int index, long timestamp) => Record(_acknowledgments, index, timestamp);

    internal void Deliver(int index, long timestamp)
    {
        Record(_deliveries, index, timestamp);
        if (Interlocked.Decrement(ref _remaining) == 0)
        {
            _delivered.TrySetResult();
        }
    }

    internal NotificationMeasurement Summarize(
        NotificationOperation operation,
        ValkeyProtocol protocol,
        int concurrency,
        long start,
        long acknowledgmentsEnd,
        long allocatedBytes
    )
    {
        if (
            _starts.Any(value => value == 0)
            || _acknowledgments.Any(value => value == 0)
            || !_delivered.Task.IsCompletedSuccessfully
        )
        {
            throw new InvalidOperationException("Incomplete notification samples.");
        }
        var acknowledgmentSamples = Durations(_acknowledgments);
        var deliverySamples = Durations(_deliveries);
        var deliveredEnd = Math.Max(acknowledgmentsEnd, _deliveries.Max());
        return new NotificationMeasurement(
            operation.ToString(),
            protocol.ToString(),
            concurrency,
            _starts.Length,
            _starts.Length / Stopwatch.GetElapsedTime(start, acknowledgmentsEnd).TotalSeconds,
            _starts.Length / Stopwatch.GetElapsedTime(start, deliveredEnd).TotalSeconds,
            RoundTripMeasurements.Percentile(acknowledgmentSamples, 0.50),
            RoundTripMeasurements.Percentile(acknowledgmentSamples, 0.95),
            RoundTripMeasurements.Percentile(acknowledgmentSamples, 0.99),
            RoundTripMeasurements.Percentile(deliverySamples, 0.50),
            RoundTripMeasurements.Percentile(deliverySamples, 0.95),
            RoundTripMeasurements.Percentile(deliverySamples, 0.99),
            (double)allocatedBytes / _starts.Length,
            acknowledgmentSamples,
            deliverySamples
        );
    }

    private void Validate(int index, long timestamp)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _starts.Length);
        ArgumentOutOfRangeException.ThrowIfLessThan(timestamp, 1);
    }

    private void Record(long[] destination, int index, long timestamp)
    {
        Validate(index, timestamp);
        var start = Volatile.Read(ref _starts[index]);
        if (start == 0 || timestamp < start)
        {
            throw new InvalidOperationException("Notification observed before its operation started.");
        }
        if (Interlocked.CompareExchange(ref destination[index], timestamp, 0) != 0)
        {
            throw new InvalidOperationException("Duplicate notification or acknowledgment.");
        }
    }

    private double[] Durations(long[] ends)
    {
        var samples = new double[ends.Length];
        for (var index = 0; index < ends.Length; index++)
        {
            samples[index] = Stopwatch.GetElapsedTime(_starts[index], ends[index]).TotalMicroseconds;
        }
        Array.Sort(samples);
        return samples;
    }
}
