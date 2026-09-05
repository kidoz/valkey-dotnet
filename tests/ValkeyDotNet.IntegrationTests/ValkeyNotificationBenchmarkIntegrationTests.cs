using ValkeyDotNet.Benchmarks;

namespace ValkeyDotNet.IntegrationTests;

public sealed class ValkeyNotificationBenchmarkIntegrationTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2, 1)]
    [InlineData(ValkeyProtocol.Resp2, 8)]
    [InlineData(ValkeyProtocol.Resp3, 1)]
    [InlineData(ValkeyProtocol.Resp3, 8)]
    public async Task OwnedNotificationWorkloadsDeliverEveryBinaryIdentity(ValkeyProtocol protocol, int concurrency)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_RUN_NOTIFICATION_TESTS") != "1")
        {
            Assert.Skip("Set VALKEYDOTNET_RUN_NOTIFICATION_TESTS=1 to verify workloads on a new owned Docker server.");
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(2));
        var token = deadline.Token;
        await using var server = new OwnedBenchmarkServer();
        await server.StartAsync(token);
        foreach (var operation in Enum.GetValues<NotificationOperation>())
        {
            if (operation != NotificationOperation.Publish && protocol != ValkeyProtocol.Resp3)
            {
                continue;
            }
            var result = await NotificationWorkload.RunAsync(
                server.Options(protocol),
                server.Project,
                operation,
                concurrency,
                3,
                16,
                token
            );
            Assert.Equal(16 * concurrency, result.Samples);
            Assert.Equal(result.Samples, result.AcknowledgmentMicroseconds.Length);
            Assert.Equal(result.Samples, result.DeliveryMicroseconds.Length);
            Assert.Equal(operation.ToString(), result.Operation);
            Assert.Equal(protocol.ToString(), result.Protocol);
            Assert.Equal(concurrency, result.Concurrency);
            // No wall-clock performance thresholds: successful completion requires exact identity,
            // binary payload, acknowledgment, no-loss/reset and final value/cleanup validation.
            await using var observer = await ValkeyClient.ConnectAsync(server.Options(protocol), token);
            Assert.Equal(0, (await observer.ExecuteAsync(new ValkeyCommand("DBSIZE"), token)).AsInt64());
            Assert.Empty((await observer.ExecuteAsync(new ValkeyCommand("PUBSUB", "CHANNELS"), token)).AsArray());
        }
        await server.DisposeAsync();
    }
}
