using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    internal static string? PausedRestoreClientId(string text)
    {
        if (text.Length > 16384)
        {
            throw new InvalidOperationException("Client-list observation exceeded its bound.");
        }
        string? result = null;
        foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var fields = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var command = fields.Single(field => field.StartsWith("cmd=", StringComparison.Ordinal));
            if (command != "cmd=restore-asking")
            {
                continue;
            }
            var id = fields.Single(field => field.StartsWith("id=", StringComparison.Ordinal))[3..];
            var flags = fields.Single(field => field.StartsWith("flags=", StringComparison.Ordinal))[6..];
            if (
                result is not null
                || !flags.Contains('b', StringComparison.Ordinal)
                || !ulong.TryParse(id, NumberStyles.None, CultureInfo.InvariantCulture, out var number)
                || number == 0
            )
            {
                throw new InvalidOperationException("A unique blocked RESTORE-ASKING client was not identified.");
            }
            result = id;
        }
        return result;
    }

    internal async Task TimeoutOwnedKeyBeforeRestoreAsync(byte[] key, ValkeyProtocol protocol, CancellationToken token)
    {
        ValidateTransferKey(key);
        var slot = ValkeyClusterClient.GetHashSlot(key);
        var (sourceId, targetId) = await VerifyMigrationAsync(slot, 0, 1, 2, token);
        var number = slot.ToString(CultureInfo.InvariantCulture);
        Assert.Contains(
            "[" + number + "->-" + targetId + "]",
            await CommandAsync(0, ["CLUSTER", "NODES"], token),
            StringComparison.Ordinal
        );
        Assert.Contains(
            "[" + number + "-<-" + sourceId + "]",
            await CommandAsync(1, ["CLUSTER", "NODES"], token),
            StringComparison.Ordinal
        );
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        var bounded = deadline.Token;
        await using var source = await ValkeyClient.ConnectAsync(NodeOptions(0, protocol), bounded);
        await using var target = await ValkeyClient.ConnectAsync(NodeOptions(1, protocol), bounded);
        var sourceKeys = (
            await source.ExecuteAsync(new ValkeyCommand("CLUSTER", "GETKEYSINSLOT", number, "3"), bounded)
        ).AsArray();
        Assert.Equal(2, sourceKeys.Count);
        Assert.Contains(sourceKeys, candidate => candidate.AsBytes().Span.SequenceEqual(key));
        Assert.DoesNotContain(
            "cmdstat_migrate:",
            (await source.ExecuteAsync(new ValkeyCommand("INFO", "COMMANDSTATS"), bounded)).AsString()!,
            StringComparison.Ordinal
        );
        await VerifyNodeAsync(0, bounded);
        await VerifyNodeAsync(1, bounded);
        Task<RespValue>? transfer = null;
        try
        {
            // WRITE keeps observation/admin commands available and expires even if this process dies.
            Assert.Equal(
                "OK",
                (await target.ExecuteAsync(new ValkeyCommand("CLIENT", "PAUSE", "30000", "WRITE"), bounded)).AsString()
            );
            transfer = source.ExecuteAsync(new ValkeyCommand("MIGRATE", Service(1), "6379", key, "0", "2000"), bounded);
            string? restoreId = null;
            using (var observation = CancellationTokenSource.CreateLinkedTokenSource(bounded))
            {
                observation.CancelAfter(TimeSpan.FromSeconds(2));
                while (restoreId is null)
                {
                    restoreId = PausedRestoreClientId(
                        (await target.ExecuteAsync(new ValkeyCommand("CLIENT", "LIST"), observation.Token)).AsString()!
                    );
                    if (restoreId is null)
                    {
                        Assert.False(transfer.IsCompleted, "MIGRATE ended before the blocked restore was observed.");
                        await Task.Delay(10, observation.Token);
                    }
                }
            }
            var error = await Assert.ThrowsAsync<ValkeyServerException>(() => transfer);
            Assert.Equal("IOERR", error.ErrorCode);
            Assert.Equal(ValkeyCommandDeliveryStatus.ReplyReceived, error.DeliveryStatus);
            Assert.Equal("IOERR error or timeout reading to target instance", error.Message);
            // Require the timed-out sender's exact destination socket to disappear while still paused.
            using (var closure = CancellationTokenSource.CreateLinkedTokenSource(bounded))
            {
                closure.CancelAfter(TimeSpan.FromSeconds(3));
                while (
                    (await target.ExecuteAsync(new ValkeyCommand("CLIENT", "LIST", "ID", restoreId), closure.Token))
                        .AsString()!
                        .Length != 0
                )
                {
                    await Task.Delay(10, closure.Token);
                }
            }
            Assert.Equal("PONG", await source.PingAsync(bounded));
            byte[] sentinel = [255, 0, 13, 10, 42];
            Assert.Equal(
                sentinel,
                (await source.ExecuteAsync(new ValkeyCommand("ECHO", sentinel), bounded)).AsBytes().ToArray()
            );
            var stats = (await source.ExecuteAsync(new ValkeyCommand("INFO", "COMMANDSTATS"), bounded)).AsString()!;
            Assert.StartsWith(
                "cmdstat_migrate:calls=1,",
                Assert.Single(stats.Split('\n'), line => line.StartsWith("cmdstat_migrate:", StringComparison.Ordinal)),
                StringComparison.Ordinal
            );
            Assert.Equal(
                0,
                (await target.ExecuteAsync(new ValkeyCommand("CLUSTER", "COUNTKEYSINSLOT", number), bounded)).AsInt64()
            );
        }
        finally
        {
            // Independent of the fault/caller deadline, including failure after PAUSE was written.
            try
            {
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await VerifyNodeAsync(1, cleanup.Token);
                await using var control = await ValkeyClient.ConnectAsync(NodeOptions(1, protocol), cleanup.Token);
                Assert.Equal(
                    "OK",
                    (await control.ExecuteAsync(new ValkeyCommand("CLIENT", "UNPAUSE"), cleanup.Token)).AsString()
                );
            }
            finally
            {
                await source.DisposeAsync();
                if (transfer is not null)
                {
                    try
                    {
                        await transfer;
                    }
                    catch (Exception error)
                        when (error is ValkeyException or OperationCanceledException or ObjectDisposedException)
                    {
                        // Observe the already asserted failure, or disposal after an earlier assertion failed.
                    }
                }
            }
        }
        // An unpause acknowledgement alone cannot exclude a delayed buffered restore.
        for (var sample = 0; sample < 3; sample++)
        {
            var replies = await target.ExecutePipelineAsync(
                [new ValkeyCommand("ASKING"), new ValkeyCommand("GET", key)],
                bounded
            );
            Assert.Equal("OK", replies[0].AsString());
            Assert.True(replies[1].IsNull);
            Assert.Equal(
                0,
                (await target.ExecuteAsync(new ValkeyCommand("CLUSTER", "COUNTKEYSINSLOT", number), bounded)).AsInt64()
            );
            await Task.Delay(100, bounded);
        }
        await VerifyMigrationAsync(slot, 0, 1, 2, bounded);
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"slot={slot}; blocked_restore_observed=true; transfer_socket_closed_before_unpause=true; caller_error=IOERR; delivery=ReplyReceived; same_connection_ping=PONG; migrate_calls=1; source_keys=2; destination_keys=0; replay=false; cutover=false; destination_unpaused=true"
        );
    }
}
