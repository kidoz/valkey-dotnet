namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    private async Task RunPausedCutoverAsync(
        ValkeyClient source,
        Func<CancellationToken, Task<bool>> verifyJobs,
        Func<Func<Task>, CancellationToken, Task> writeDuringPause,
        CancellationToken token
    )
    {
        // Fail closed on a different pause budget; never extend the server's automatic safety timeout.
        Assert.Equal(
            "cluster-manual-failover-timeout\n5000",
            await CommandAsync(0, ["CONFIG", "GET", "cluster-manual-failover-timeout"], token)
        );
        try
        {
            // Only the importing node needs this hook. DEBUG remains local-only inside the owned container.
            Assert.Equal("OK", await CommandAsync(1, ["DEBUG", "SLOTMIGRATION", "PREVENT-FAILOVER", "1"], token));
            Assert.Equal("OK", await CommandAsync(0, ["DEBUG", "SLOTMIGRATION", "PREVENT-PAUSE", "0"], token));
            using var pause = CancellationTokenSource.CreateLinkedTokenSource(token);
            pause.CancelAfter(TimeSpan.FromSeconds(5));
            while (true)
            {
                var info = (await source.ExecuteAsync(new ValkeyCommand("INFO", "CLIENTS"), pause.Token)).AsString()!;
                if (
                    info.Contains("\r\npaused_reason:slot_migration_in_progress\r\n", StringComparison.Ordinal)
                    && await verifyJobs(pause.Token)
                )
                {
                    Assert.Contains("\r\npaused_actions:write\r\n", info, StringComparison.Ordinal);
                    break;
                }
                await Task.Delay(20, pause.Token);
            }
            await writeDuringPause(
                async () =>
                {
                    Assert.True(await verifyJobs(pause.Token));
                    Assert.Equal(
                        "OK",
                        await CommandAsync(1, ["DEBUG", "SLOTMIGRATION", "PREVENT-FAILOVER", "0"], pause.Token)
                    );
                },
                pause.Token
            );
        }
        finally
        {
            using var restoration = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            Assert.Equal(
                "OK",
                await CommandAsync(1, ["DEBUG", "SLOTMIGRATION", "PREVENT-FAILOVER", "0"], restoration.Token)
            );
        }
    }

    internal static bool AreCutoverWritersBlocked(string text, string firstId, string secondId, string project)
    {
        if (text.Length > 16384 || firstId == secondId)
        {
            throw new InvalidOperationException("Invalid cutover writer observation.");
        }
        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (lines.Length != 2)
        {
            throw new InvalidOperationException("Exactly two known writer connections are required.");
        }
        var ids = new HashSet<string>(StringComparer.Ordinal);
        var blocked = true;
        foreach (var line in lines)
        {
            var fields = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var field in line.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            {
                var separator = field.IndexOf('=', StringComparison.Ordinal);
                if (separator <= 0 || !fields.TryAdd(field[..separator], field[(separator + 1)..]))
                {
                    throw new InvalidOperationException("Invalid writer client fields.");
                }
            }
            if (
                !fields.TryGetValue("id", out var id)
                || (id != firstId && id != secondId)
                || !ids.Add(id)
                || !fields.TryGetValue("name", out var name)
                || name != project
                || !fields.TryGetValue("cmd", out var command)
                || !fields.TryGetValue("flags", out var flags)
            )
            {
                throw new InvalidOperationException("Unexpected writer identity or fields.");
            }
            blocked &= command == "set" && flags.Contains('b', StringComparison.Ordinal);
        }
        return blocked;
    }
}
