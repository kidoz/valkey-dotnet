namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    private string? _originalPrimaryId;
    private string? _replicaId;

    private async Task AddReplicaAsync(CancellationToken token)
    {
        _originalPrimaryId = await CommandAsync(0, ["CLUSTER", "MYID"], token);
        _replicaId = await CommandAsync(3, ["CLUSTER", "MYID"], token);
        await DockerAsync(
            [
                "exec",
                _containers[0]!,
                "valkey-cli",
                "--cluster",
                "add-node",
                "node-4:6379",
                "node-1:6379",
                "--cluster-slave",
                "--cluster-master-id",
                _originalPrimaryId,
            ],
            token
        );
        using var readiness = CancellationTokenSource.CreateLinkedTokenSource(token);
        readiness.CancelAfter(TimeSpan.FromSeconds(45));
        while (!await ReplicaReadyAsync(readiness.Token) || !await HasOwnedMembershipAsync(readiness.Token))
        {
            await Task.Delay(100, readiness.Token);
        }
    }

    private async Task<bool> ReplicaReadyAsync(CancellationToken token)
    {
        var info = await CommandAsync(3, ["INFO", "REPLICATION"], token);
        return info.Contains("role:slave\r\n", StringComparison.Ordinal)
            && info.Contains("master_link_status:up\r\n", StringComparison.Ordinal)
            && info.Contains("master_sync_in_progress:0\r\n", StringComparison.Ordinal);
    }

    private async Task<bool> HasOwnedMembershipAsync(CancellationToken token)
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);
        for (var index = 0; index < 4; index++)
        {
            var id = await CommandAsync(index, ["CLUSTER", "MYID"], token);
            if (id.Length != 40 || !id.All(Uri.IsHexDigit) || !ids.Add(id))
            {
                throw new InvalidOperationException("Invalid owned failover node identities.");
            }
        }
        for (var index = 0; index < 4; index++)
        {
            var members = (await CommandAsync(index, ["CLUSTER", "NODES"], token)).Split(
                '\n',
                StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries
            );
            if (members.Length != 4 || !ids.SetEquals(members.Select(line => line.Split(' ')[0])))
            {
                return false;
            }
            var replica = members
                .Single(line => line.StartsWith(_replicaId + " ", StringComparison.Ordinal))
                .Split(' ');
            if (replica[3] != _originalPrimaryId || !replica[2].Split(',').Contains("slave", StringComparer.Ordinal))
            {
                return false;
            }
        }
        return true;
    }

    internal async Task StopOwnedPrimaryAsync(CancellationToken token)
    {
        if (_ports.Length != 4 || _originalPrimaryId is null || _replicaId is null)
        {
            throw new InvalidOperationException("Failover requires this fixture's initialized replica profile.");
        }
        await DiscoverAndVerifyAsync(token);
        if (
            await CommandAsync(0, ["CLUSTER", "MYID"], token) != _originalPrimaryId
            || await CommandAsync(3, ["CLUSTER", "MYID"], token) != _replicaId
            || !await HasOwnedMembershipAsync(token)
            || !await ReplicaReadyAsync(token)
        )
        {
            throw new InvalidOperationException(
                "Owned membership or replica readiness changed; refusing primary stop."
            );
        }
        await VerifyNodeAsync(0, token);
        await DockerAsync(["kill", "--signal", "KILL", _containers[0]!], token);
        await AssertPrimaryStoppedAsync(token);
    }

    internal async Task AssertPrimaryStoppedAsync(CancellationToken token)
    {
        await VerifyNodeAsync(0, token);
        Assert.Equal(
            "false",
            (await DockerAsync(["inspect", "--format", "{{.State.Running}}", _containers[0]!], token)).Trim()
        );
    }

    internal async Task WaitForPromotionAsync(int slot, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(45));
        while (true)
        {
            var agrees = true;
            for (var index = 1; index < 4; index++)
            {
                await using var client = await ValkeyClient.ConnectAsync(
                    NodeOptions(index, ValkeyProtocol.Resp3),
                    deadline.Token
                );
                var ranges = (
                    await client.ExecuteAsync(new ValkeyCommand("CLUSTER", "SLOTS"), deadline.Token)
                ).AsArray();
                var range = ranges
                    .Single(value => value.AsArray()[0].AsInt64() <= slot && value.AsArray()[1].AsInt64() >= slot)
                    .AsArray();
                agrees &= range[2].AsArray()[2].AsString() == _replicaId;
                if (index == 3)
                {
                    agrees &=
                        (await client.ExecuteAsync(new ValkeyCommand("ROLE"), deadline.Token)).AsArray()[0].AsString()
                        == "master";
                }
                var info = (await client.ExecuteAsync(new ValkeyCommand("CLUSTER", "INFO"), deadline.Token)).AsString();
                agrees &= info is not null && info.Contains("cluster_state:ok", StringComparison.Ordinal);
            }
            if (agrees)
            {
                return;
            }
            await Task.Delay(100, deadline.Token);
        }
    }
}
