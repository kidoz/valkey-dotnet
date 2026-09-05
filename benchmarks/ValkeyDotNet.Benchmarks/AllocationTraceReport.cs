using System.Text.Json;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;

namespace ValkeyDotNet.Benchmarks;

internal static class AllocationTraceReport
{
    internal static async Task RunAsync(string tracePath, string workloadPath)
    {
        using var metadata = JsonDocument.Parse(await File.ReadAllTextAsync(workloadPath));
        var root = metadata.RootElement;
        if (root.GetProperty("SchemaVersion").GetInt32() != 1 || !root.GetProperty("CleanupVerified").GetBoolean())
        {
            throw new InvalidOperationException("Expected a completed allocation workload report.");
        }
        var processId = root.GetProperty("ProcessId").GetInt32();
        var start = root.GetProperty("StartedUtc").GetDateTimeOffset();
        var end = root.GetProperty("EndedUtc").GetDateTimeOffset();
        var operations = root.GetProperty("Operations").GetInt32();
        if (processId <= 0 || operations <= 0 || end <= start)
        {
            throw new InvalidOperationException("Invalid allocation workload window or operation count.");
        }
        var summary = new AllocationSampleSummary();
        // Use a unique derived file; never overwrite an existing trace or report.
        var outputStem = Path.Combine("artifacts/performance", "allocation-stacks-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory("artifacts/performance");
        var etlx = TraceLog.CreateFromEventPipeDataFile(tracePath, outputStem + ".etlx");
        using var log = new TraceLog(etlx);
        if (
            log.SessionStartTime.ToUniversalTime() > start.UtcDateTime
            || log.SessionEndTime.ToUniversalTime() < end.UtcDateTime
        )
        {
            throw new InvalidOperationException("Trace does not cover the complete workload window.");
        }
        if (log.EventsLost != 0)
        {
            throw new InvalidOperationException("Trace lost events; allocation attribution is incomplete.");
        }
        foreach (var traceEvent in log.Events)
        {
            if (traceEvent is not GCAllocationTickTraceData data)
            {
                continue;
            }
            var timestamp = new DateTimeOffset(data.TimeStamp.ToUniversalTime());
            if (data.ProcessID != processId || timestamp < start || timestamp > end)
            {
                continue;
            }
            var frames = new List<string>();
            for (var frame = data.CallStack(); frame is not null && frames.Count < 64; frame = frame.Caller)
            {
                frames.Add(frame.CodeAddress.FullMethodName);
            }
            summary.Add(data.TypeName, data.AllocationAmount64, string.Join("\n", frames));
        }
        if (summary.Samples == 0)
        {
            throw new InvalidOperationException("No allocation samples matched this process/workload window.");
        }
        var report = new
        {
            SchemaVersion = 1,
            Workload = root.Clone(),
            Trace = Path.GetFileName(tracePath),
            log.EventsLost,
            summary.Samples,
            summary.SamplesWithoutStacks,
            summary.SampledAllocationBytes,
            StackDepthLimit = 64,
            CounterBytesPerOperation = (double)root.GetProperty("AllocatedBytes").GetInt64() / operations,
            Interpretation = "GC allocation-tick weights attributed to the sampled type/stack; estimates, not exact object counts or retained memory",
            Allocations = summary.Rows(),
        };
        await using var output = new FileStream(
            outputStem + ".json",
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None
        );
        await JsonSerializer.SerializeAsync(output, report);
        Console.WriteLine("Allocation stack report: " + outputStem + ".json");
    }
}
