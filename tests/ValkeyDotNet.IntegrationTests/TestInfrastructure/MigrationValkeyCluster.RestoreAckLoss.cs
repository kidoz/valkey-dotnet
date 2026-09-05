using System.Globalization;
using System.Text.Json;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed partial class MigrationValkeyCluster
{
    private const string RestoreRelayImage =
        "mcr.microsoft.com/dotnet/runtime:10.0@sha256:a365ce6a50b09176855d085c69da3fc1204a48432e36087e9a208f6e5860e235";
    private string? _restoreRelayId;
    private string[] _restoreRelayCommand = [];
    private string? _restoreRelayNetwork;
    private bool _restoreRelayRequested;
    private static string RelayDirectory => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "MigrationRelay"));

    internal async Task LoseOwnedRestoreAcknowledgmentAsync(
        byte[] key,
        ValkeyProtocol protocol,
        CancellationToken token,
        byte[]? acknowledgedKey = null
    )
    {
        ValidateTransferKey(key);
        if (acknowledgedKey is not null)
        {
            ValidateBulkTransferKeys(acknowledgedKey, key);
        }
        var slot = ValkeyClusterClient.GetHashSlot(key);
        var (sourceId, targetId) = await VerifyMigrationAsync(slot, 0, 1, acknowledgedKey is null ? 1 : 2, token);
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
        if (
            _restoreRelayRequested
            || !File.Exists(Path.Combine(RelayDirectory, "ValkeyDotNet.MigrationRelay.dll"))
            || RelayDirectory.Contains(',', StringComparison.Ordinal)
        )
        {
            throw new InvalidOperationException("A fresh built relay directory is required.");
        }
        var network = await VerifyRelayNetworkAsync(token);
        _restoreRelayNetwork = network;
        _restoreRelayCommand =
        [
            "/app/ValkeyDotNet.MigrationRelay.dll",
            Convert.ToBase64String(key),
            .. acknowledgedKey is null ? Array.Empty<string>() : [Convert.ToBase64String(acknowledgedKey)],
        ];
        _restoreRelayRequested = true;
        try
        {
            _restoreRelayId = (
                await DockerAsync(
                    [
                        "create",
                        "--name",
                        Project + "-restore-relay",
                        "--hostname",
                        "restore-relay",
                        "--network",
                        network,
                        "--network-alias",
                        "restore-relay",
                        "--label",
                        "com.docker.compose.project=" + Project,
                        "--label",
                        "com.docker.compose.service=restore-relay",
                        "--label",
                        "com.valkeydotnet.migration-token=" + _token,
                        "--memory",
                        "64m",
                        "--cpus",
                        "1",
                        "--pids-limit",
                        "64",
                        "--read-only",
                        "--cap-drop",
                        "ALL",
                        "--security-opt",
                        "no-new-privileges",
                        "--user",
                        "1654:1654",
                        "--mount",
                        "type=bind,source=" + RelayDirectory + ",target=/app,readonly",
                        "--env",
                        "VALKEYDOTNET_OWNED_RELAY=1",
                        "--env",
                        "DOTNET_EnableDiagnostics=0",
                        "--entrypoint",
                        "dotnet",
                        RestoreRelayImage,
                        .. _restoreRelayCommand,
                    ],
                    token
                )
            ).Trim();
            await VerifyRestoreRelayAsync(token);
            await DockerAsync(["start", _restoreRelayId], token);
            using var fault = CancellationTokenSource.CreateLinkedTokenSource(token);
            fault.CancelAfter(TimeSpan.FromSeconds(20));
            var bounded = fault.Token;
            while (
                !(await DockerAsync(["logs", _restoreRelayId], bounded)).Contains("READY\n", StringComparison.Ordinal)
            )
            {
                await Task.Delay(50, bounded);
            }
            await VerifyRestoreRelayAsync(bounded);
            await VerifyRelayNetworkAsync(bounded);
            await VerifyNodeAsync(0, bounded);
            await VerifyNodeAsync(1, bounded);
            await using var source = await ValkeyClient.ConnectAsync(NodeOptions(0, protocol), bounded);
            await using var target = await ValkeyClient.ConnectAsync(NodeOptions(1, protocol), bounded);
            await VerifyBulkKeysAsync(
                source,
                number,
                acknowledgedKey is null ? [key] : [acknowledgedKey, key],
                bounded
            );
            await VerifyBulkKeysAsync(target, number, [], bounded);
            Assert.DoesNotContain(
                "cmdstat_migrate:",
                (await source.ExecuteAsync(new ValkeyCommand("INFO", "COMMANDSTATS"), bounded)).AsString()!,
                StringComparison.Ordinal
            );
            var command = acknowledgedKey is null
                ? new ValkeyCommand("MIGRATE", "restore-relay", "6380", key, "0", "2000")
                : new ValkeyCommand("MIGRATE", "restore-relay", "6380", "", "0", "2000", "KEYS", acknowledgedKey, key);
            var error = await Assert.ThrowsAsync<ValkeyServerException>(() => source.ExecuteAsync(command, bounded));
            Assert.Equal("IOERR", error.ErrorCode);
            Assert.Equal(ValkeyCommandDeliveryStatus.ReplyReceived, error.DeliveryStatus);
            Assert.Equal("IOERR error or timeout reading to target instance", error.Message);
            Assert.Equal("PONG", await source.PingAsync(bounded));
            byte[] sentinel = [255, 0, 13, 10, 42];
            Assert.Equal(
                sentinel,
                (await source.ExecuteAsync(new ValkeyCommand("ECHO", sentinel), bounded)).AsBytes().ToArray()
            );
            Assert.Equal("0", (await DockerAsync(["wait", _restoreRelayId], bounded)).Trim());
            Assert.Equal(
                acknowledgedKey is null
                    ? "READY\nRESTORE_ACK_WITHHELD\nSENDER_CLOSED"
                    : "READY\nRESTORE_ACK_FORWARDED\nRESTORE_ACK_WITHHELD\nSENDER_CLOSED",
                (await DockerAsync(["logs", _restoreRelayId], bounded)).Trim()
            );
            var stats = (await source.ExecuteAsync(new ValkeyCommand("INFO", "COMMANDSTATS"), bounded)).AsString()!;
            Assert.StartsWith(
                "cmdstat_migrate:calls=1,",
                Assert.Single(stats.Split('\n'), line => line.StartsWith("cmdstat_migrate:", StringComparison.Ordinal)),
                StringComparison.Ordinal
            );
            await VerifyBulkKeysAsync(source, number, [key], bounded);
            await VerifyBulkKeysAsync(
                target,
                number,
                acknowledgedKey is null ? [key] : [acknowledgedKey, key],
                bounded
            );
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"slot={slot}; restore_ok_forwarded={(acknowledgedKey is null ? 0 : 1)}; restore_ok_withheld=1; source_error=IOERR; delivery=ReplyReceived; migrate_calls=1; source_keys=1; destination_keys={(acknowledgedKey is null ? 1 : 2)}; same_connection_ping=PONG; relay_exit=0; replay=false; cutover=false"
            );
        }
        finally
        {
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await DisposeRestoreRelayAsync(cleanup.Token);
        }
    }

    private async Task<string> VerifyRelayNetworkAsync(CancellationToken token)
    {
        await VerifyNodeAsync(0, token);
        using var node = JsonDocument.Parse(await DockerAsync(["inspect", _containers[0]!], token));
        var attachment = node.RootElement[0]
            .GetProperty("NetworkSettings")
            .GetProperty("Networks")
            .EnumerateObject()
            .Single();
        var id = attachment.Value.GetProperty("NetworkID").GetString()!;
        using var inspection = JsonDocument.Parse(await DockerAsync(["network", "inspect", id], token));
        var network = inspection.RootElement[0];
        var labels = network.GetProperty("Labels");
        var expected = _containers.Where(value => value is not null).ToHashSet(StringComparer.Ordinal);
        if (_restoreRelayId is not null)
        {
            expected.Add(_restoreRelayId);
        }
        if (
            network.GetProperty("Id").GetString() != id
            || labels.GetProperty("com.docker.compose.project").GetString() != Project
            || labels.GetProperty("com.valkeydotnet.migration-token").GetString() != _token
            || !expected.SetEquals(network.GetProperty("Containers").EnumerateObject().Select(item => item.Name))
        )
        {
            throw new InvalidOperationException("Relay requires exactly the owned private network members.");
        }
        return id;
    }

    private async Task VerifyRestoreRelayAsync(CancellationToken token)
    {
        if (_restoreRelayId is null || _restoreRelayId.Length != 64 || !_restoreRelayId.All(Uri.IsHexDigit))
        {
            throw new InvalidOperationException("An exact owned relay ID is required.");
        }
        using var inspection = JsonDocument.Parse(await DockerAsync(["inspect", _restoreRelayId], token));
        var relay = inspection.RootElement[0];
        var config = relay.GetProperty("Config");
        var labels = config.GetProperty("Labels");
        var host = relay.GetProperty("HostConfig");
        var mounts = relay.GetProperty("Mounts").EnumerateArray().ToArray();
        var ports = host.GetProperty("PortBindings");
        var capabilities = host.GetProperty("CapAdd");
        if (
            relay.GetProperty("Id").GetString() != _restoreRelayId
            || labels.GetProperty("com.docker.compose.project").GetString() != Project
            || labels.GetProperty("com.docker.compose.service").GetString() != "restore-relay"
            || labels.GetProperty("com.valkeydotnet.migration-token").GetString() != _token
            || config.GetProperty("Image").GetString() != RestoreRelayImage
            || config.GetProperty("Hostname").GetString() != "restore-relay"
            || config.GetProperty("User").GetString() != "1654:1654"
            || config.GetProperty("Entrypoint").GetArrayLength() != 1
            || config.GetProperty("Entrypoint")[0].GetString() != "dotnet"
            || !config
                .GetProperty("Cmd")
                .EnumerateArray()
                .Select(item => item.GetString())
                .SequenceEqual(_restoreRelayCommand)
            || host.GetProperty("Memory").GetInt64() != 64 * 1024 * 1024
            || host.GetProperty("NanoCpus").GetInt64() != 1_000_000_000
            || host.GetProperty("PidsLimit").GetInt64() != 64
            || !host.GetProperty("ReadonlyRootfs").GetBoolean()
            || host.GetProperty("Privileged").GetBoolean()
            || host.GetProperty("PublishAllPorts").GetBoolean()
            || host.GetProperty("NetworkMode").GetString() != _restoreRelayNetwork
            || relay.GetProperty("NetworkSettings").GetProperty("Networks").EnumerateObject().Count() != 1
            || (ports.ValueKind != JsonValueKind.Null && ports.EnumerateObject().Any())
            || (capabilities.ValueKind != JsonValueKind.Null && capabilities.GetArrayLength() != 0)
            || !host.GetProperty("CapDrop").EnumerateArray().Any(item => item.GetString() == "ALL")
            || !host.GetProperty("SecurityOpt").EnumerateArray().Any(item => item.GetString() == "no-new-privileges")
            || mounts.Length != 1
            || mounts[0].GetProperty("Type").GetString() != "bind"
            || mounts[0].GetProperty("Source").GetString() != RelayDirectory
            || mounts[0].GetProperty("Destination").GetString() != "/app"
            || mounts[0].GetProperty("RW").GetBoolean()
        )
        {
            throw new InvalidOperationException("Relay identity or isolation changed; refusing action.");
        }
    }

    private async Task DisposeRestoreRelayAsync(CancellationToken token)
    {
        if (!_restoreRelayRequested)
        {
            return;
        }
        var ids = (
            await DockerAsync(
                [
                    "ps",
                    "--all",
                    "--no-trunc",
                    "--quiet",
                    "--filter",
                    "label=com.docker.compose.project=" + Project,
                    "--filter",
                    "label=com.docker.compose.service=restore-relay",
                ],
                token
            )
        ).Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (ids.Length == 0 && _restoreRelayId is null)
        {
            _restoreRelayRequested = false;
            return;
        }
        if (ids.Length != 1 || (_restoreRelayId is not null && ids[0] != _restoreRelayId))
        {
            throw new InvalidOperationException("Relay cleanup identity is ambiguous.");
        }
        _restoreRelayId = ids[0];
        await VerifyRestoreRelayAsync(token);
        await DockerAsync(["rm", "--force", _restoreRelayId], token);
        _restoreRelayId = null;
        _restoreRelayRequested = false;
    }
}
