using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    internal async Task CancelAtomicSlotMigrationBeforeTransferAsync(
        int slot,
        int source,
        int target,
        ValkeyProtocol protocol,
        CancellationToken token
    )
    {
        var (sourceId, targetId) = await VerifyMigrationAsync(slot, source, target, 2, token);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        var bounded = deadline.Token;
        // This administrative connection is private: MULTI state is never shared with other callers.
        await using var admin = await ValkeyClient.ConnectAsync(NodeOptions(source, protocol), bounded);
        var capabilities = (
            await admin.ExecuteAsync(
                new ValkeyCommand(
                    "COMMAND",
                    "INFO",
                    "CLUSTER|MIGRATESLOTS",
                    "CLUSTER|GETSLOTMIGRATIONS",
                    "CLUSTER|CANCELSLOTMIGRATIONS"
                ),
                bounded
            )
        ).AsArray();
        Assert.Equal(3, capabilities.Count);
        if (capabilities.Any(value => value.IsNull))
        {
            Assert.Skip("The owned server does not support atomic slot migration cancellation.");
        }
        // CANCELSLOTMIGRATIONS is node-wide; refuse to cancel any pre-existing job, on any member.
        for (var index = 0; index < 3; index++)
        {
            Assert.Equal("", await CommandAsync(index, ["CLUSTER", "GETSLOTMIGRATIONS"], bounded));
        }
        await VerifyNodeAsync(source, bounded);
        await VerifyNodeAsync(target, bounded);
        Assert.Equal(sourceId, (await admin.ExecuteAsync(new ValkeyCommand("CLUSTER", "MYID"), bounded)).AsString());
        var number = slot.ToString(CultureInfo.InvariantCulture);
        // EXEC prevents the async export from advancing between initiation, observation, and cancellation.
        // Never retry this batch after an ambiguous I/O failure; dispose the owned cluster instead.
        var replies = await admin.ExecutePipelineAsync(
            [
                new ValkeyCommand("MULTI"),
                new ValkeyCommand("CLUSTER", "MIGRATESLOTS", "SLOTSRANGE", number, number, "NODE", targetId),
                new ValkeyCommand("CLUSTER", "GETSLOTMIGRATIONS"),
                new ValkeyCommand("CLUSTER", "CANCELSLOTMIGRATIONS"),
                new ValkeyCommand("CLUSTER", "GETSLOTMIGRATIONS"),
                new ValkeyCommand("EXEC"),
            ],
            bounded
        );
        Assert.Equal(6, replies.Count);
        Assert.Equal("OK", replies[0].AsString());
        for (var index = 1; index <= 4; index++)
        {
            Assert.Equal("QUEUED", replies[index].AsString());
        }
        var executed = replies[5].AsArray();
        Assert.Equal(4, executed.Count);
        Assert.Equal("OK", executed[0].AsString());
        var active = ReadAtomicJobFields(Assert.Single(executed[1].AsArray()), protocol);
        var activeState = AtomicJobState(active, "EXPORT", slot, sourceId, targetId, null);
        Assert.False(
            activeState is "success" or "failed" or "cancelled",
            "The export must still be active before cancellation."
        );
        Assert.Equal("OK", executed[2].AsString());
        var cancelled = ReadAtomicJobFields(Assert.Single(executed[3].AsArray()), protocol);
        Assert.Equal("cancelled", AtomicJobState(cancelled, "EXPORT", slot, sourceId, targetId, active["name"]));
        Assert.Equal("PONG", await admin.PingAsync(bounded));
        // A later independent read must retain the terminal state, not merely the EXEC snapshot.
        var final = ReadAtomicJobFields(
            Assert.Single(
                (await admin.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETSLOTMIGRATIONS"), bounded)).AsArray()
            ),
            protocol
        );
        Assert.Equal("cancelled", AtomicJobState(final, "EXPORT", slot, sourceId, targetId, active["name"]));
        Assert.Equal("", await CommandAsync(target, ["CLUSTER", "GETSLOTMIGRATIONS"], bounded));
        await VerifyMigrationAsync(slot, source, target, 2, bounded);
        await WaitHealthyAsync(bounded);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"atomic_job={active["name"]}; slot={slot}; active_observed=true; export_state=cancelled; import_jobs=0"
        );
    }
}
