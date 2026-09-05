using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    internal async Task RunAtomicSlotMigrationAsync(
        int slot,
        int source,
        int target,
        ValkeyProtocol protocol,
        CancellationToken token
    )
    {
        var (sourceId, targetId) = await VerifyMigrationAsync(slot, source, target, 2, token);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(45));
        var bounded = deadline.Token;
        await using var exporter = await ValkeyClient.ConnectAsync(NodeOptions(source, protocol), bounded);
        await using var importer = await ValkeyClient.ConnectAsync(NodeOptions(target, protocol), bounded);
        foreach (var client in new[] { exporter, importer })
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
                Assert.Skip("The owned server does not support atomic slot migration commands.");
            }
            Assert.Empty(
                (await client.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETSLOTMIGRATIONS"), bounded)).AsArray()
            );
        }
        // Recheck exact owned identities immediately before starting the single job.
        await VerifyNodeAsync(source, bounded);
        await VerifyNodeAsync(target, bounded);
        Assert.Equal(sourceId, (await exporter.ExecuteAsync(new ValkeyCommand("CLUSTER", "MYID"), bounded)).AsString());
        Assert.Equal(targetId, (await importer.ExecuteAsync(new ValkeyCommand("CLUSTER", "MYID"), bounded)).AsString());
        var number = slot.ToString(CultureInfo.InvariantCulture);
        // OK is initiation only. Never replay this mutating command after an ambiguous failure.
        Assert.Equal(
            "OK",
            (
                await exporter.ExecuteAsync(
                    new ValkeyCommand("CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", number, number, "NODE", targetId),
                    bounded
                )
            ).AsString()
        );
        string? jobName = null;
        while (true)
        {
            var exported = await ReadAtomicJobAsync(exporter, "EXPORT", bounded);
            var imported = await ReadAtomicJobAsync(importer, "IMPORT", bounded);
            if (exported && imported)
            {
                break;
            }
            await Task.Delay(100, bounded);
        }
        await VerifyMigrationAsync(slot, source, target, 0, bounded, expectedTargetKeys: 2);
        await WaitHealthyAsync(bounded);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"atomic_job={jobName}; slot={slot}; export_state=success; import_state=success"
        );

        async Task<bool> ReadAtomicJobAsync(ValkeyClient client, string operation, CancellationToken cancellationToken)
        {
            var jobs = (
                await client.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETSLOTMIGRATIONS"), cancellationToken)
            ).AsArray();
            if (jobs.Count == 0)
            {
                return false;
            }
            var fields = ReadAtomicJobFields(Assert.Single(jobs), protocol);
            var success = ValidateAtomicJob(fields, operation, slot, sourceId, targetId, jobName);
            jobName ??= fields["name"];
            return success;
        }
    }

    private static Dictionary<string, string> ReadAtomicJobFields(RespValue job, ValkeyProtocol protocol)
    {
        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        // RESP2 has alternating fields; RESP3 has a map. Wire bounds remain the client's defaults.
        if (protocol == ValkeyProtocol.Resp3)
        {
            var pairs = job.AsMap();
            Assert.InRange(pairs.Count, 6, 32);
            foreach (var pair in pairs)
            {
                AddField(pair.Key, pair.Value);
            }
        }
        else
        {
            var values = job.AsArray();
            Assert.InRange(values.Count, 12, 64);
            Assert.Equal(0, values.Count % 2);
            for (var index = 0; index < values.Count; index += 2)
            {
                AddField(values[index], values[index + 1]);
            }
        }
        return fields;

        void AddField(RespValue key, RespValue value)
        {
            var name = key.AsString();
            Assert.NotNull(name);
            var text = name is "name" or "operation" or "slot_ranges" or "source_node" or "target_node" or "state"
                ? value.AsString()
                : "";
            Assert.NotNull(text);
            Assert.True(fields.TryAdd(name, text), "Duplicate atomic migration field.");
        }
    }

    internal static bool ValidateAtomicJob(
        IReadOnlyDictionary<string, string> fields,
        string operation,
        int slot,
        string sourceId,
        string targetId,
        string? expectedName
    )
    {
        var state = AtomicJobState(fields, operation, slot, sourceId, targetId, expectedName);
        if (state is "failed" or "cancelled")
        {
            throw new InvalidOperationException("The owned atomic migration failed or was cancelled.");
        }
        return state == "success";
    }

    internal static string AtomicJobState(
        IReadOnlyDictionary<string, string> fields,
        string operation,
        int slot,
        string sourceId,
        string targetId,
        string? expectedName
    )
    {
        if (
            !fields.TryGetValue("name", out var name)
            || name.Length != 40
            || !name.All(Uri.IsHexDigit)
            || (expectedName is not null && name != expectedName)
            || !fields.TryGetValue("operation", out var actualOperation)
            || actualOperation != operation
            || !fields.TryGetValue("source_node", out var source)
            || source != sourceId
            || !fields.TryGetValue("target_node", out var target)
            || target != targetId
            || !fields.TryGetValue("slot_ranges", out var range)
            || range != string.Create(CultureInfo.InvariantCulture, $"{slot}-{slot}")
            || !fields.TryGetValue("state", out var state)
            || string.IsNullOrEmpty(state)
        )
        {
            throw new InvalidOperationException("An unexpected atomic migration job was returned.");
        }
        return state;
    }
}
