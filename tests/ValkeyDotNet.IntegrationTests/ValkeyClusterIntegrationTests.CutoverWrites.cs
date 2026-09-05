using System.Globalization;
using ValkeyDotNet.IntegrationTests.TestInfrastructure;

namespace ValkeyDotNet.IntegrationTests;

public sealed partial class ValkeyClusterIntegrationTests
{
    private static async Task<(byte[] Expiring, byte[] Persistent)> VerifyCutoverWritesAsync(
        MigrationValkeyCluster cluster,
        ValkeyClusterClient first,
        ValkeyClient inspector,
        byte[] expiringKey,
        byte[] expiringValue,
        byte[] persistentKey,
        byte[] persistentValue,
        long originalExpiry,
        ValkeyProtocol protocol,
        CancellationToken token
    )
    {
        await using var second = await ValkeyClusterClient.ConnectAsync(cluster.Options(protocol), token);
        // Both clients retain exactly one source connection; obtain those exact IDs before pausing writes.
        var firstId = (await first.ExecuteAsync(expiringKey, new ValkeyCommand("CLIENT", "ID"), token)).AsInt64();
        var secondId = (await second.ExecuteAsync(persistentKey, new ValkeyCommand("CLIENT", "ID"), token)).AsInt64();
        Assert.True(firstId > 0 && secondId > 0);
        Assert.NotEqual(firstId, secondId);
        var firstText = firstId.ToString(CultureInfo.InvariantCulture);
        var secondText = secondId.ToString(CultureInfo.InvariantCulture);
        await cluster.CompleteAtomicSlotMigrationAcrossCutoverAsync(
            expiringKey,
            persistentKey,
            protocol,
            async bounded =>
            {
                await WritePairAsync(1, null, bounded);
                await VerifyValuesAsync(bounded);
            },
            async (release, bounded) =>
            {
                await WritePairAsync(2, release, bounded);
                // Verify the queued values before any later update could conceal their loss.
                await VerifyValuesAsync(bounded);
            },
            token
        );
        await VerifySlotOwnerAsync(cluster, ValkeyClusterClient.GetHashSlot(expiringKey), 1, protocol, token);
        await WritePairAsync(3, null, token);
        await VerifyValuesAsync(token);
        TestContext.Current.TestOutputHelper?.WriteLine(
            "writer_phases=before-pause,queued-across-cutover,after-cutover; total_updates=6; acknowledged=6; ambiguous=0; application_replays=0"
        );
        return (expiringValue, persistentValue);

        async Task VerifyValuesAsync(CancellationToken bounded)
        {
            await VerifyTransferredValuesAsync(
                first,
                expiringKey,
                expiringValue,
                persistentKey,
                persistentValue,
                originalExpiry,
                bounded
            );
            Assert.Equal(
                originalExpiry,
                (
                    await first.ExecuteAsync(expiringKey, new ValkeyCommand("PEXPIRETIME", expiringKey), bounded)
                ).AsInt64()
            );
        }

        async Task WritePairAsync(byte sequence, Func<Task>? release, CancellationToken bounded)
        {
            var nextExpiring = expiringValue.ToArray();
            var nextPersistent = persistentValue.ToArray();
            nextExpiring[^1] = sequence;
            nextPersistent[^1] = sequence;
            using var writes = CancellationTokenSource.CreateLinkedTokenSource(bounded);
            var observations = new[]
            {
                CutoverWriteObservation.ObserveAsync(
                    first.ExecuteAsync(
                        expiringKey,
                        new ValkeyCommand("SET", expiringKey, nextExpiring, "XX", "KEEPTTL"),
                        writes.Token
                    )
                ),
                CutoverWriteObservation.ObserveAsync(
                    second.ExecuteAsync(
                        persistentKey,
                        new ValkeyCommand("SET", persistentKey, nextPersistent, "XX", "KEEPTTL"),
                        writes.Token
                    )
                ),
            };
            try
            {
                if (release is not null)
                {
                    while (
                        !MigrationValkeyCluster.AreCutoverWritersBlocked(
                            (
                                await inspector.ExecuteAsync(
                                    new ValkeyCommand("CLIENT", "LIST", "ID", firstText, secondText),
                                    bounded
                                )
                            ).AsString()!,
                            firstText,
                            secondText,
                            cluster.Project
                        )
                    )
                    {
                        Assert.All(observations, observation => Assert.False(observation.IsCompleted));
                        await Task.Delay(20, bounded);
                    }
                    Assert.All(observations, observation => Assert.False(observation.IsCompleted));
                    TestContext.Current.TestOutputHelper?.WriteLine(
                        "source_write_pause=observed; exact_blocked_set_clients=2; pending_at_release=2"
                    );
                    await release();
                }
                var outcomes = await Task.WhenAll(observations);
                Assert.All(outcomes, outcome => Assert.Equal("acknowledged", outcome));
                expiringValue = nextExpiring;
                persistentValue = nextPersistent;
            }
            finally
            {
                // Failure cannot leave writers running outside the fixture: cancel, drain, and report every outcome.
                await writes.CancelAsync();
                var outcomes = await Task.WhenAll(observations);
                TestContext.Current.TestOutputHelper?.WriteLine(
                    $"sequence={sequence}; attempted=2; outcomes={string.Join(',', outcomes)}; application_replays=0"
                );
            }
        }
    }
}
