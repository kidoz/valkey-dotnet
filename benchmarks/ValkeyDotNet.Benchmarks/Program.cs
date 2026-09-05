using BenchmarkDotNet.Running;

namespace ValkeyDotNet.Benchmarks;

public static class Program
{
    public static async Task Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        if (args.Length == 2 && args[0] == "--allocation-workload")
        {
            await AllocationWorkload.RunAsync(args[1]);
            return;
        }
        if (args.Length == 3 && args[0] == "--allocation-report")
        {
            await AllocationTraceReport.RunAsync(args[1], args[2]);
            return;
        }
        if (args.Length == 1 && args[0] == "--roundtrips")
        {
            await RoundTripBenchmarks.RunAsync();
            return;
        }
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
