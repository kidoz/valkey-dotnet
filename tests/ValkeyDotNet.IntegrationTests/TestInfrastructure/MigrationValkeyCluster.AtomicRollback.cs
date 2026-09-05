using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    internal async Task FailAtomicSlotMigrationAfterSnapshotAsync(
        byte[] expiringKey,
        byte[] persistentKey,
        ValkeyProtocol protocol,
        Func<CancellationToken, Task> verifyDuringImport,
        CancellationToken token
    )
    {
        ValidateTransferKey(expiringKey);
        ValidateTransferKey(persistentKey);
        var slot = ValkeyClusterClient.GetHashSlot(expiringKey);
        if (
            !_enableMigrationDebug
            || slot != ValkeyClusterClient.GetHashSlot(persistentKey)
            || expiringKey.AsSpan().SequenceEqual(persistentKey)
        )
        {
            throw new InvalidOperationException(
                "Rollback requires two distinct same-slot keys and the owned debug fixture."
            );
        }
        var (sourceId, targetId) = await VerifyMigrationAsync(slot, 0, 1, 2, token);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(45));
        var bounded = deadline.Token;
        await using var source = await ValkeyClient.ConnectAsync(NodeOptions(0, protocol), bounded);
        await using var target = await ValkeyClient.ConnectAsync(NodeOptions(1, protocol), bounded);
        foreach (var client in new[] { source, target })
        {
            var capabilities = (
                await client.ExecuteAsync(
                    new ValkeyCommand("COMMAND", "INFO", "CLUSTER|MIGRATESLOTS", "CLUSTER|GETSLOTMIGRATIONS"),
                    bounded
                )
            ).AsArray();
            Assert.Equal(2, capabilities.Count);
            if (capabilities.Any(value => value.IsNull))
            {
                Assert.Skip("The owned server does not support atomic slot migration.");
            }
        }
        for (var index = 0; index < 3; index++)
        {
            Assert.Equal("", await CommandAsync(index, ["CLUSTER", "GETSLOTMIGRATIONS"], bounded));
            Assert.Equal(
                "enable-debug-command\nlocal",
                await CommandAsync(index, ["CONFIG", "GET", "enable-debug-command"], bounded)
            );
        }
        var number = slot.ToString(CultureInfo.InvariantCulture);
        // The upstream test hook holds the export before write pause/cutover, not the server process.
        // DEBUG is local-only; execute inside the exact verified source container, never over the host port.
        Assert.Equal("OK", await CommandAsync(0, ["DEBUG", "SLOTMIGRATION", "PREVENT-PAUSE", "1"], bounded));
        try
        {
            Assert.Equal(
                "OK",
                await CommandAsync(
                    0,
                    ["CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", number, number, "NODE", targetId],
                    bounded
                )
            );
            string? jobName = null;
            while (true)
            {
                var fields = await ReadJobAsync(source, bounded);
                var state = AtomicJobState(fields, "EXPORT", slot, sourceId, targetId, jobName);
                jobName ??= fields["name"];
                Assert.False(
                    state is "success" or "failed" or "cancelled",
                    "Export terminated before fault injection."
                );
                if (state == "waiting-to-pause")
                {
                    break;
                }
                await Task.Delay(50, bounded);
            }
            var imported = await ReadJobAsync(target, bounded);
            Assert.False(
                AtomicJobState(imported, "IMPORT", slot, sourceId, targetId, jobName)
                    is "success"
                        or "failed"
                        or "cancelled",
                "Import terminated before fault injection."
            );
            var keys = (
                await target.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "3"), bounded)
            ).AsArray();
            Assert.Equal(2, keys.Count);
            Assert.Contains(keys, key => key.AsBytes().Span.SequenceEqual(expiringKey));
            Assert.Contains(keys, key => key.AsBytes().Span.SequenceEqual(persistentKey));
            Assert.Equal("2", await CommandAsync(1, ["CLUSTER", "COUNTKEYSINSLOT", number], bounded));
            Assert.Equal(
                "MOVED",
                (
                    await Assert.ThrowsAsync<ValkeyServerException>(() =>
                        target.ExecuteAsync(new ValkeyCommand("GET", expiringKey), bounded)
                    )
                ).ErrorCode
            );
            await verifyDuringImport(bounded);

            // Only one owned EXPORT exists. Resolve and recheck its exact client ID before closing it.
            var clientId = ParseExportClientId(
                (await source.ExecuteAsync(new ValkeyCommand("CLIENT", "LIST", "FLAGS", "E"), bounded)).AsString()!
            );
            await VerifyNodeAsync(0, bounded);
            await VerifyNodeAsync(1, bounded);
            Assert.Equal(
                clientId,
                ParseExportClientId(
                    (await source.ExecuteAsync(new ValkeyCommand("CLIENT", "LIST", "FLAGS", "E"), bounded)).AsString()!
                )
            );
            Assert.Equal(
                1,
                (await source.ExecuteAsync(new ValkeyCommand("CLIENT", "KILL", "ID", clientId), bounded)).AsInt64()
            );

            while (true)
            {
                var exportState = AtomicJobState(
                    await ReadJobAsync(source, bounded),
                    "EXPORT",
                    slot,
                    sourceId,
                    targetId,
                    jobName
                );
                var importState = AtomicJobState(
                    await ReadJobAsync(target, bounded),
                    "IMPORT",
                    slot,
                    sourceId,
                    targetId,
                    jobName
                );
                Assert.False(exportState is "success" or "cancelled", "Export reached an unexpected terminal state.");
                Assert.False(importState is "success" or "cancelled", "Import reached an unexpected terminal state.");
                if (
                    exportState == "failed"
                    && importState == "failed"
                    && (
                        await target.ExecuteAsync(new ValkeyCommand("CLUSTER", "COUNTKEYSINSLOT", number), bounded)
                    ).AsInt64() == 0
                )
                {
                    break;
                }
                await Task.Delay(50, bounded);
            }
            Assert.Empty(
                (await source.ExecuteAsync(new ValkeyCommand("CLIENT", "LIST", "FLAGS", "E"), bounded)).AsString()!
            );
            Assert.Empty(
                (await target.ExecuteAsync(new ValkeyCommand("CLIENT", "LIST", "FLAGS", "i"), bounded)).AsString()!
            );
            Assert.Equal("PONG", await source.PingAsync(bounded));
            Assert.Equal("PONG", await target.PingAsync(bounded));
            await VerifyMigrationAsync(slot, 0, 1, 2, bounded);
            await WaitHealthyAsync(bounded);
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"atomic_job={jobName}; slot={slot}; provisional_keys=2; export_state=failed; import_state=failed; destination_keys_after_rollback=0; killed_export_clients=1"
            );
        }
        finally
        {
            // Restoration has its own bound; outer disposal still removes the owned cluster on failure.
            using var restoration = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            Assert.Equal(
                "OK",
                await CommandAsync(0, ["DEBUG", "SLOTMIGRATION", "PREVENT-PAUSE", "0"], restoration.Token)
            );
        }

        async Task<Dictionary<string, string>> ReadJobAsync(ValkeyClient client, CancellationToken cancellationToken)
        {
            return ReadAtomicJobFields(
                Assert.Single(
                    (
                        await client.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETSLOTMIGRATIONS"), cancellationToken)
                    ).AsArray()
                ),
                protocol
            );
        }
    }

    internal static string ParseExportClientId(string text)
    {
        if (text.Length > 16384)
        {
            throw new InvalidOperationException("Unexpected export client response size.");
        }
        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (lines.Length != 1)
        {
            throw new InvalidOperationException("Exactly one owned export client is required.");
        }
        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var part in lines[0].Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = part.IndexOf('=', StringComparison.Ordinal);
            if (separator <= 0 || !fields.TryAdd(part[..separator], part[(separator + 1)..]))
            {
                throw new InvalidOperationException("Invalid export client fields.");
            }
        }
        if (
            !fields.TryGetValue("id", out var id)
            || id.Length is < 1 or > 20
            || !id.All(char.IsAsciiDigit)
            || !ulong.TryParse(id, NumberStyles.None, CultureInfo.InvariantCulture, out var number)
            || number == 0
            || !fields.TryGetValue("flags", out var flags)
            || !flags.Contains('E', StringComparison.Ordinal)
        )
        {
            throw new InvalidOperationException("Invalid export client identity or flags.");
        }
        return id;
    }
}
