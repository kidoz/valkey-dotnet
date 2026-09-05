using System.Runtime.InteropServices;
using System.Text.Json;

namespace ValkeyDotNet.Benchmarks;

internal static class AllocationWorkload
{
    internal static async Task RunAsync(string operationName)
    {
        RequireRelease();
        var operation = operationName switch
        {
            "Get" => RoundTripOperation.Get,
            "Pipeline100Get" => RoundTripOperation.Pipeline100Get,
            _ => throw new ArgumentException("Expected Get or Pipeline100Get.", nameof(operationName)),
        };
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var token = deadline.Token;
        await using var server = new OwnedBenchmarkServer();
        Console.WriteLine("Creating bounded local allocation server: " + server.Project);
        await server.StartAsync(token);
        await using var client = await ValkeyClient.ConnectAsync(server.Options(ValkeyProtocol.Resp3), token);
        if ((await client.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64() != 0)
        {
            throw new InvalidOperationException("The new allocation server is not empty.");
        }
        var serverInfo = (await client.ExecuteAsync(new ValkeyCommand("INFO", "SERVER"), token)).AsString();
        var workers = Enumerable
            .Range(0, 8)
            .Select(i => new RoundTripWorkload(client, server.Project, i, operation))
            .ToArray();
        foreach (var worker in workers)
        {
            await worker.SetupAsync(token);
        }
        await RunWorkersAsync(64);
        RequireValid();
        var iterations = operation == RoundTripOperation.Get ? 16384 : 512;
        // UTC boundaries select only the warm workload in an external EventPipe trace.
        // Profiling overhead makes this unsuitable for latency/throughput comparisons.
        var startedUtc = DateTimeOffset.UtcNow;
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        await RunWorkersAsync(iterations);
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        var endedUtc = DateTimeOffset.UtcNow;
        RequireValid();
        foreach (var worker in workers)
        {
            await worker.CleanupAsync(token);
        }
        if ((await client.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64() != 0)
        {
            throw new InvalidOperationException("Allocation workload left unexpected keys.");
        }
        await client.DisposeAsync();
        await server.DisposeAsync();
        var report = new
        {
            SchemaVersion = 1,
            Operation = operationName,
            ProcessId = Environment.ProcessId,
            StartedUtc = startedUtc,
            EndedUtc = endedUtc,
            Operations = iterations * workers.Length,
            CommandsPerOperation = operation == RoundTripOperation.Get ? 1 : 100,
            AllocatedBytes = allocated,
            Runtime = RuntimeInformation.FrameworkDescription,
            OS = RuntimeInformation.OSDescription,
            Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            LogicalProcessors = Environment.ProcessorCount,
            ServerInfo = serverInfo,
            server.ImageId,
            Protocol = "RESP3",
            Concurrency = workers.Length,
            PayloadBytes = 1024,
            WarmupPerWorker = 64,
            Topology = "standalone Docker loopback; one socket; one server CPU/128 MiB; no TLS",
            Scope = "warm workload; process-wide managed allocation including harness; not retained memory",
            CleanupVerified = true,
        };
        Directory.CreateDirectory("artifacts/performance");
        var path = "artifacts/performance/allocations-" + server.Project + ".json";
        await using var output = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        await JsonSerializer.SerializeAsync(output, report, cancellationToken: token);
        Console.WriteLine("Allocation workload report: " + path);

        Task RunWorkersAsync(int count) =>
            Task.WhenAll(
                workers.Select(async worker =>
                {
                    for (var i = 0; i < count; i++)
                    {
                        await worker.ExecuteAsync(token);
                    }
                })
            );

        void RequireValid()
        {
            if (workers.Any(worker => !worker.LastResultIsValid()))
            {
                throw new InvalidOperationException("Allocation workload returned invalid binary data.");
            }
        }
    }

    private static void RequireRelease()
    {
#if DEBUG
        throw new InvalidOperationException("Allocation profiling requires Release configuration.");
#endif
    }
}
