using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace ValkeyDotNet.Benchmarks;

internal static class NotificationBenchmarks
{
    internal static async Task RunAsync()
    {
        RequireRelease();
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var token = deadline.Token;
        await using var server = new OwnedBenchmarkServer();
        Console.WriteLine("Creating bounded local notification benchmark server: " + server.Project);
        await server.StartAsync(token);
        await using var observer = await ValkeyClient.ConnectAsync(server.Options(ValkeyProtocol.Resp3), token);
        var serverInfo = (await observer.ExecuteAsync(new ValkeyCommand("INFO", "SERVER"), token)).AsString();
        var results = new List<NotificationMeasurement>();
        await RequireEmptyAsync();
        foreach (var operation in Enum.GetValues<NotificationOperation>())
        {
            foreach (var protocol in new[] { ValkeyProtocol.Resp2, ValkeyProtocol.Resp3 })
            {
                if (operation != NotificationOperation.Publish && protocol != ValkeyProtocol.Resp3)
                {
                    continue;
                }
                foreach (var concurrency in new[] { 1, 8 })
                {
                    var result = await NotificationWorkload.RunAsync(
                        server.Options(protocol),
                        server.Project,
                        operation,
                        concurrency,
                        NotificationWorkload.WarmupIterations,
                        NotificationWorkload.Iterations,
                        token
                    );
                    results.Add(result);
                    await RequireEmptyAsync();
                    Console.WriteLine(
                        string.Create(
                            CultureInfo.InvariantCulture,
                            $"{protocol} c={concurrency} {operation}: ack/delivered={result.AcknowledgmentsPerSecond:F1}/{result.DeliveredOperationsPerSecond:F1} ops/s; delivery p50/p95/p99={result.DeliveryP50Microseconds:F1}/{result.DeliveryP95Microseconds:F1}/{result.DeliveryP99Microseconds:F1} us; {result.AllocatedBytesPerOperation:F0} B/op"
                        )
                    );
                }
            }
        }
        await observer.DisposeAsync();
        await server.DisposeAsync();
        var report = new
        {
            SchemaVersion = 1,
            TimestampUtc = DateTimeOffset.UtcNow,
            Runtime = RuntimeInformation.FrameworkDescription,
            OS = RuntimeInformation.OSDescription,
            Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            LogicalProcessors = Environment.ProcessorCount,
            ServerInfo = serverInfo,
            server.ImageId,
            Topology = "standalone Docker loopback; one shared writer socket and one subscriber/tracking socket; idle observer socket; server limited to one CPU/128 MiB",
            Tls = false,
            PayloadBytes = NotificationWorkload.PayloadBytes,
            QueueCapacity = NotificationWorkload.QueueCapacity,
            WarmupPerWorker = NotificationWorkload.WarmupIterations,
            MeasuredPerWorker = NotificationWorkload.Iterations,
            AllocationScope = "process-wide allocated bytes through delivery drain; clients plus validation/correlation harness; not retained heap or server memory; preparation excluded",
            LoadModel = "closed-loop command acknowledgments; one outstanding command per worker; deliveries consumed independently; no coordinated-omission correction",
            DeliveryInterval = "command invocation to async-enumerable observation, including network/server/queue/scheduling; not server-internal or local-cache eviction latency",
            TrackingModel = "unique keys written once; seeded and default GET tracking pre-registered outside timing; BCAST+PREFIX has no GET registration; batched keys individually correlated",
            CleanupVerified = true,
            Results = results,
        };
        Directory.CreateDirectory("artifacts/performance");
        var path = "artifacts/performance/notifications-" + server.Project + ".json";
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

        async Task RequireEmptyAsync()
        {
            if ((await observer.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64() != 0)
            {
                throw new InvalidOperationException("Notification workload left unexpected keys.");
            }
        }
    }

    private static void RequireRelease()
    {
#if DEBUG
        throw new InvalidOperationException("Notification benchmarks require Release configuration.");
#endif
    }
}
