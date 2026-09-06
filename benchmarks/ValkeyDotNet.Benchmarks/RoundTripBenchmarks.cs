using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace ValkeyDotNet.Benchmarks;

internal static class RoundTripBenchmarks
{
    internal static async Task RunAsync()
    {
        RequireRelease();
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var server = new OwnedBenchmarkServer();
        Console.WriteLine("Creating bounded local benchmark server: " + server.Project);
        await server.StartAsync(token);
        string serverInfo;
        var results = new List<RoundTripMeasurement>();
        await using (var client = await ValkeyClient.ConnectAsync(server.Options(ValkeyProtocol.Resp3), token))
        {
            if ((await client.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64() != 0)
            {
                throw new InvalidOperationException("The new benchmark server is not empty.");
            }
            serverInfo = (await client.ExecuteAsync(new ValkeyCommand("INFO", "SERVER"), token)).AsString()!;
        }
        foreach (var protocol in new[] { ValkeyProtocol.Resp2, ValkeyProtocol.Resp3 })
        {
            foreach (var concurrency in new[] { 1, 8 })
            {
                // One physical multiplexed socket; concurrency measures caller/FIFO contention, not connection pooling.
                await using var client = await ValkeyClient.ConnectAsync(server.Options(protocol), token);
                foreach (var operation in Enum.GetValues<RoundTripOperation>())
                {
                    var workers = Enumerable
                        .Range(0, concurrency)
                        .Select(i => new RoundTripWorkload(client, server.Project, i, operation))
                        .ToArray();
                    foreach (var worker in workers)
                    {
                        await worker.SetupAsync(token);
                    }
                    await Task.WhenAll(
                        workers.Select(async worker =>
                        {
                            for (var iteration = 0; iteration < RoundTripMeasurements.WarmupIterations; iteration++)
                            {
                                await worker.ExecuteAsync(token);
                            }
                        })
                    );
                    RequireValid(workers);
                    await ValidateLeaseStatesAsync(workers, client, token);
                    var result = await RoundTripMeasurements.MeasureAsync(workers, protocol, token);
                    RequireValid(workers);
                    await ValidateLeaseStatesAsync(workers, client, token);
                    results.Add(result);
                    Console.WriteLine(
                        string.Create(
                            CultureInfo.InvariantCulture,
                            $"{protocol} c={concurrency} {operation}: {result.OperationsPerSecond:F1} ops/s; p50/p95/p99={result.P50Microseconds:F1}/{result.P95Microseconds:F1}/{result.P99Microseconds:F1} us; {result.AllocatedBytesPerOperation:F0} B/op"
                        )
                    );
                    foreach (var worker in workers)
                    {
                        await worker.CleanupAsync(token);
                    }
                    if ((await client.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64() != 0)
                    {
                        throw new InvalidOperationException("Benchmark workload left unexpected keys.");
                    }
                }
            }
        }
        await server.DisposeAsync();
        var report = new
        {
            SchemaVersion = 1,
            ProfileVersion = 2,
            TimestampUtc = DateTimeOffset.UtcNow,
            Runtime = RuntimeInformation.FrameworkDescription,
            OS = RuntimeInformation.OSDescription,
            Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            LogicalProcessors = Environment.ProcessorCount,
            ServerInfo = serverInfo,
            server.ImageId,
            Topology = "standalone Docker loopback; one client socket; server limited to one CPU/128 MiB",
            Tls = false,
            CachePayloadBytes = 1024,
            LockOwnerBytes = 16,
            IsolatedLeaseModel = "AcquireLease: one successful SET NX PX per distinct absent key; ReleaseLease: one warmed owner-checked script per pre-acquired key; 576 keys/worker, 120000 ms TTL; setup, validation and cleanup excluded",
            WarmupPerWorker = RoundTripMeasurements.WarmupIterations,
            MeasuredPerWorker = RoundTripMeasurements.Iterations,
            AllocationScope = "process-wide allocated bytes, client plus harness; not retained heap or server memory",
            LoadModel = "closed-loop; each worker awaits one operation; no open-loop coordinated-omission correction",
            CleanupVerified = true,
            Results = results,
        };
        Directory.CreateDirectory("artifacts/performance");
        var path = "artifacts/performance/roundtrips-" + server.Project + ".json";
        await using var output = new FileStream(
            path,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            4096,
            useAsync: true
        );
        await JsonSerializer.SerializeAsync(output, report, cancellationToken: token);
        Console.WriteLine("Benchmark report: " + path);
    }

    private static void RequireRelease()
    {
#if DEBUG
        throw new InvalidOperationException("Real-server benchmarks require Release configuration.");
#endif
    }

    private static void RequireValid(RoundTripWorkload[] workers)
    {
        if (workers.Any(worker => !worker.LastResultIsValid()))
        {
            throw new InvalidOperationException(
                "Benchmark workload preflight/final validation failed; results are invalid."
            );
        }
    }

    private static async Task ValidateLeaseStatesAsync(
        RoundTripWorkload[] workers,
        ValkeyClient client,
        CancellationToken token
    )
    {
        foreach (var worker in workers)
        {
            if (worker.IsolatedLease is not null)
            {
                await worker.IsolatedLease.ValidateStateAsync(client, token);
            }
        }
    }
}
