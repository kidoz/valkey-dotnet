using ValkeyDotNet.Benchmarks;

namespace ValkeyDotNet.IntegrationTests;

public sealed class ValkeyRoundTripBenchmarkIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2)]
    [InlineData(ValkeyProtocol.Resp3)]
    public async Task OwnedRoundTripWorkloadsPreserveBinaryDataAndLockOutcomes(ValkeyProtocol protocol)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_ROUNDTRIP_TESTS") != "1")
        {
            Assert.Skip(
                "Set VALKEYDOTNET_RUN_ROUNDTRIP_TESTS=1 to verify benchmark workloads on a new owned Docker server."
            );
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        await using var server = new OwnedBenchmarkServer();
        await server.StartAsync(token);
        await using var client = await ValkeyClient.ConnectAsync(server.Options(protocol), token);
        foreach (var operation in Enum.GetValues<RoundTripOperation>())
        {
            var workers = Enumerable
                .Range(0, 8)
                .Select(i => new RoundTripWorkload(client, server.Project, i, operation))
                .ToArray();
            foreach (var worker in workers)
            {
                await worker.SetupAsync(token);
            }
            for (var round = 0; round < 3; round++)
            {
                await Task.WhenAll(workers.Select(worker => worker.ExecuteAsync(token)));
                foreach (var worker in workers)
                {
                    Assert.True(worker.LastResultIsValid(), operation.ToString());
                    Assert.Equal(16, worker.Owner.Length);
                    Assert.Equal(
                        worker.Payload,
                        (await client.ExecuteAsync(new ValkeyCommand("GET", worker.DataKeys[0]), token))
                            .AsBytes()
                            .ToArray()
                    );
                    Assert.InRange(
                        (await client.ExecuteAsync(new ValkeyCommand("PTTL", worker.DataKeys[0]), token)).AsInt64(),
                        1,
                        120000
                    );
                    var owner = await client.ExecuteAsync(new ValkeyCommand("GET", worker.LockKey), token);
                    if (operation == RoundTripOperation.AcquireReleaseCycle)
                    {
                        Assert.True(owner.IsNull);
                    }
                    else
                    {
                        Assert.Equal(worker.Owner, owner.AsBytes().ToArray());
                        Assert.InRange(
                            (await client.ExecuteAsync(new ValkeyCommand("PTTL", worker.LockKey), token)).AsInt64(),
                            1,
                            120000
                        );
                    }
                }
            }
            foreach (var worker in workers)
            {
                await worker.CleanupAsync(token);
            }
            Assert.Equal(0, (await client.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64());
        }
        await client.DisposeAsync();
        await server.DisposeAsync();
    }
}
