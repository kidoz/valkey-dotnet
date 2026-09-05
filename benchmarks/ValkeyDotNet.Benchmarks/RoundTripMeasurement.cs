using System.Diagnostics;

namespace ValkeyDotNet.Benchmarks;

internal sealed record RoundTripMeasurement(
    string Operation,
    string Protocol,
    int Concurrency,
    int Samples,
    int CommandsPerOperation,
    double OperationsPerSecond,
    double MeanMicroseconds,
    double P50Microseconds,
    double P95Microseconds,
    double P99Microseconds,
    double AllocatedBytesPerOperation,
    double[] LatencyMicroseconds
);

internal static class RoundTripMeasurements
{
    internal const int Iterations = 512;
    internal const int WarmupIterations = 64;

    internal static async Task<RoundTripMeasurement> MeasureAsync(
        RoundTripWorkload[] workers,
        ValkeyProtocol protocol,
        CancellationToken token
    )
    {
        if (workers.Length is not (1 or 8) || workers.Any(worker => worker.Operation != workers[0].Operation))
        {
            throw new ArgumentException(
                "The fixed profile requires one or eight homogeneous workers.",
                nameof(workers)
            );
        }
        var samples = new double[workers.Length * Iterations];
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = workers.Select((worker, index) => RunAsync(worker, index)).ToArray();
        var joined = Task.WhenAll(tasks);
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        var started = Stopwatch.GetTimestamp();
        start.SetResult();
        await joined;
        var elapsed = Stopwatch.GetElapsedTime(started).TotalSeconds;
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        var sorted = samples.Order().ToArray();
        var operation = workers[0].Operation;
        return new(
            operation.ToString(),
            protocol.ToString(),
            workers.Length,
            samples.Length,
            operation == RoundTripOperation.Pipeline100Get ? 100
                : operation == RoundTripOperation.AcquireReleaseCycle ? 2
                : 1,
            samples.Length / elapsed,
            samples.Average(),
            Percentile(sorted, 0.5),
            Percentile(sorted, 0.95),
            Percentile(sorted, 0.99),
            (double)allocated / samples.Length,
            samples
        );

        async Task RunAsync(RoundTripWorkload worker, int index)
        {
            await start.Task;
            for (var iteration = 0; iteration < Iterations; iteration++)
            {
                token.ThrowIfCancellationRequested();
                var before = Stopwatch.GetTimestamp();
                await worker.ExecuteAsync(token);
                samples[index * Iterations + iteration] = Stopwatch.GetElapsedTime(before).TotalMicroseconds;
            }
        }
    }

    internal static double Percentile(double[] sorted, double percentile)
    {
        if (sorted.Length == 0 || !double.IsFinite(percentile) || percentile is <= 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(percentile));
        }
        return sorted[(int)Math.Ceiling(percentile * sorted.Length) - 1];
    }
}
