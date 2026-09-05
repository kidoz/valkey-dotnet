using System.Globalization;
using ValkeyDotNet.Tests.TestInfrastructure;

namespace ValkeyDotNet.Tests.Cluster;

public sealed partial class ValkeyClusterSubscriberTests
{
    [Theory]
    [InlineData(ValkeyProtocol.Resp2, "fail")]
    [InlineData(ValkeyProtocol.Resp3, "fail")]
    [InlineData(ValkeyProtocol.Resp2, "loading")]
    [InlineData(ValkeyProtocol.Resp3, "loading")]
    public async Task RecoveryWaitsForAvailablePrimaryAndIgnoresFailedFormerMaster(
        ValkeyProtocol protocol,
        string health
    )
    {
        var trigger = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var target = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            Assert.Equal(["SSUBSCRIBE", "x"], await session.ReadCommandAsync());
            await session.SendRawAsync([.. Ack(protocol, "x"u8.ToArray()), .. Message(protocol, "x"u8.ToArray())]);
            await session.ReadCommandAsync();
        });
        await using var origin = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync(Hello(protocol));
            await session.ReadCommandAsync();
            await session.SendRawAsync(Ack(protocol, "x"u8.ToArray()));
            await trigger.Task.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
        });
        await using var seed = FakeValkeyServer.StartMany(
            3,
            async (index, session) =>
            {
                await session.ExpectHandshakeAsync(Hello(protocol));
                Assert.Equal(["CLUSTER", "SHARDS"], await session.ReadCommandAsync());
                await session.SendAsync(
                    index == 0
                        ? Topology(origin.Port)
                        : PrimaryHealthTopology(protocol, health, index == 2 ? target.Port : null)
                );
                await session.ReadCommandAsync();
            }
        );
        await using var subscriber = await ValkeyClusterSubscriber.ConnectAsync(
            new ValkeyClusterSubscriberOptions
            {
                Cluster = RecoveryOptions(seed.Port, protocol).Cluster,
                EnableTopologyRecovery = true,
                MaxTopologyRefreshEndpoints = 1,
                MaxReconnectAttempts = 2,
                InitialReconnectDelay = TimeSpan.FromMilliseconds(1),
            },
            TestToken
        );
        await using var handle = await subscriber.SubscribeAsync("x", TestToken);
        var completion = handle.Completion;
        await using var messages = handle.ReadAllAsync(TestToken).GetAsyncEnumerator(TestToken);
        trigger.SetResult();
        Assert.True(await messages.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), TestToken));
        Assert.Equal(2, handle.ReconnectAttempts);
        Assert.Equal(1, handle.SuccessfulRelocations);
        Assert.Same(completion, handle.Completion);
        Assert.False(completion.IsCompleted);
        Assert.Null(handle.Failure);
        await subscriber.DisposeAsync();
        await Task.WhenAll(seed.Session, origin.Session, target.Session).WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    [Theory]
    [InlineData("fail")]
    [InlineData("loading")]
    [InlineData("unexpected")]
    public async Task InitialDiscoveryRejectsUnavailableOrUnknownPrimaryHealth(string health)
    {
        await using var seed = FakeValkeyServer.Start(async session =>
        {
            await session.ExpectHandshakeAsync();
            await session.ReadCommandAsync();
            await session.SendAsync(PrimaryHealthTopology(ValkeyProtocol.Resp3, health, null));
            await session.ReadCommandAsync();
        });
        await Assert.ThrowsAsync<ValkeyClusterException>(() =>
            ValkeyClusterClient.ConnectAsync(RecoveryOptions(seed.Port).Cluster, TestToken)
        );
        await seed.Session.WaitAsync(TimeSpan.FromSeconds(5), TestToken);
    }

    private static string PrimaryHealthTopology(ValkeyProtocol protocol, string health, int? promotedPort)
    {
        var map = protocol == ValkeyProtocol.Resp2 ? "*4\r\n" : "%2\r\n";
        var nodeMap = protocol == ValkeyProtocol.Resp2 ? "*8\r\n" : "%4\r\n";
        return "*1\r\n"
            + map
            + "+slots\r\n*2\r\n:0\r\n:16383\r\n+nodes\r\n"
            + (promotedPort.HasValue ? "*2\r\n" : "*1\r\n")
            + nodeMap
            + "+role\r\n+master\r\n+health\r\n+"
            + health
            + "\r\n+endpoint\r\n+unavailable.invalid\r\n+port\r\n:1\r\n"
            + (
                promotedPort.HasValue
                    ? nodeMap
                        + "+role\r\n+master\r\n+health\r\n+online\r\n+endpoint\r\n+127.0.0.1\r\n+port\r\n:"
                        + promotedPort.Value.ToString(CultureInfo.InvariantCulture)
                        + "\r\n"
                    : ""
            );
    }
}
