using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

// Owns three new primaries and optionally one replica; never accepts an external target.
internal sealed partial class MigrationValkeyCluster : IAsyncDisposable
{
    private static readonly string[] FailoverProfile = ["--profile", "failover"];
    private readonly string _token = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
    private readonly int[] _ports;
    private readonly string?[] _containers;
    private string? _dockerHost;
    private bool _created;

    internal MigrationValkeyCluster(bool includeReplica = false)
    {
        _ports = new int[includeReplica ? 4 : 3];
        _containers = new string?[_ports.Length];
        var reservations = new List<TcpListener>();
        try
        {
            for (var index = 0; index < _ports.Length; index++)
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                reservations.Add(listener);
                listener.Start();
                _ports[index] = ((IPEndPoint)listener.LocalEndpoint).Port;
            }
        }
        finally
        {
            foreach (var listener in reservations)
            {
                listener.Dispose();
            }
        }
        // Docker binds next. A collision fails creation; an existing listener is never stopped.
    }

    internal string Project =>
        (_ports.Length == 4 ? "valkey-dotnet-failover-tests-" : "valkey-dotnet-migration-tests-") + _token;

    internal static int ParseCycles(string? text)
    {
        if (
            !int.TryParse(text ?? "3", NumberStyles.Integer, CultureInfo.InvariantCulture, out var cycles)
            || cycles is < 1 or > 20
        )
        {
            throw new InvalidOperationException("VALKEYDOTNET_MIGRATION_CYCLES must be between 1 and 20.");
        }
        return cycles;
    }

    private static string Service(int index) => "node-" + (index + 1).ToString(CultureInfo.InvariantCulture);

    internal ValkeyClientOptions NodeOptions(int index, ValkeyProtocol protocol) =>
        new()
        {
            Host = "127.0.0.1",
            Port = _ports[index],
            Protocol = protocol,
            ConnectTimeout = TimeSpan.FromSeconds(2),
            ClientName = Project,
        };

    internal ValkeyClusterOptions Options(ValkeyProtocol protocol, int seed = 0) =>
        new()
        {
            SeedNodes = [NodeOptions(seed, protocol)],
            EndpointMapper = endpoint =>
            {
                for (var index = 0; index < _ports.Length; index++)
                {
                    if (endpoint.Host == Service(index) && endpoint.Port == 6379)
                    {
                        return new ValkeyClusterEndpoint("127.0.0.1", _ports[index]);
                    }
                }
                throw new InvalidOperationException("An endpoint outside the owned cluster was announced.");
            },
        };

    internal async Task StartNewAsync(CancellationToken token)
    {
        if (_created)
        {
            throw new InvalidOperationException("The migration project already exists.");
        }
        var context = Environment.GetEnvironmentVariable("DOCKER_CONTEXT");
        var host = !string.IsNullOrWhiteSpace(context)
            ? (
                await DockerAsync(["context", "inspect", context, "--format", "{{.Endpoints.docker.Host}}"], token)
            ).Trim()
            : Environment.GetEnvironmentVariable("DOCKER_HOST")
                ?? (await DockerAsync(["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], token)).Trim();
        if (
            !host.StartsWith("unix://", StringComparison.Ordinal)
            && !host.StartsWith("npipe:////./pipe/", StringComparison.Ordinal)
        )
        {
            throw new InvalidOperationException(
                "Migration tests require a local Unix-socket or named-pipe Docker endpoint."
            );
        }
        _dockerHost = host;
        _created = true;
        await ComposeAsync(["up", "-d", "--wait", "--wait-timeout", "45"], token);
        await DiscoverAndVerifyAsync(token);
        if (_containers.Any(id => id is null))
        {
            throw new InvalidOperationException("The migration cluster did not create all three owned primaries.");
        }
        await DockerAsync(
            [
                "exec",
                _containers[0]!,
                "valkey-cli",
                "--cluster",
                "create",
                "node-1:6379",
                "node-2:6379",
                "node-3:6379",
                "--cluster-replicas",
                "0",
                "--cluster-yes",
            ],
            token
        );
        await WaitHealthyAsync(token);
        if (_ports.Length == 4)
        {
            await AddReplicaAsync(token);
        }
    }

    internal async Task<string> CommandAsync(int node, string[] arguments, CancellationToken token)
    {
        await VerifyNodeAsync(node, token);
        return (
            await DockerAsync(["exec", _containers[node]!, "valkey-cli", "-e", "--raw", .. arguments], token)
        ).Trim();
    }

    internal async Task MoveEmptySlotAsync(int slot, int source, int target, CancellationToken token)
    {
        await BeginSlotMigrationAsync(slot, source, target, 0, token);
        await CompleteEmptySlotMigrationAsync(slot, source, target, token);
    }

    internal async Task BeginSlotMigrationAsync(
        int slot,
        int source,
        int target,
        int expectedSourceKeys,
        CancellationToken token
    )
    {
        var (sourceId, targetId) = await VerifyMigrationAsync(slot, source, target, expectedSourceKeys, token);
        var number = slot.ToString(CultureInfo.InvariantCulture);
        Assert.Equal("OK", await CommandAsync(target, ["CLUSTER", "SETSLOT", number, "IMPORTING", sourceId], token));
        Assert.Equal("OK", await CommandAsync(source, ["CLUSTER", "SETSLOT", number, "MIGRATING", targetId], token));
    }

    internal async Task CompleteEmptySlotMigrationAsync(int slot, int source, int target, CancellationToken token)
    {
        var (_, targetId) = await VerifyMigrationAsync(slot, source, target, 0, token);
        var number = slot.ToString(CultureInfo.InvariantCulture);
        // Both nodes are empty. Publish destination ownership before retiring the source.
        Assert.Equal("OK", await CommandAsync(target, ["CLUSTER", "SETSLOT", number, "NODE", targetId], token));
        Assert.Equal("OK", await CommandAsync(source, ["CLUSTER", "SETSLOT", number, "NODE", targetId], token));
        Assert.Equal(
            "OK",
            await CommandAsync(3 - source - target, ["CLUSTER", "SETSLOT", number, "NODE", targetId], token)
        );
        await WaitHealthyAsync(token);
    }

    private async Task<(string SourceId, string TargetId)> VerifyMigrationAsync(
        int slot,
        int source,
        int target,
        int expectedSourceKeys,
        CancellationToken token
    )
    {
        if (slot is < 0 or > 16383 || source is < 0 or > 2 || target is < 0 or > 2 || source == target)
        {
            throw new ArgumentOutOfRangeException(nameof(slot));
        }
        if (expectedSourceKeys is < 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(expectedSourceKeys));
        }
        if (!_created || _ports.Length != 3)
        {
            throw new InvalidOperationException("Migration requires this fixture's initialized three-primary profile.");
        }
        // Re-confirm every target immediately before fault injection, even after startup verification.
        await DiscoverAndVerifyAsync(token);
        var number = slot.ToString(CultureInfo.InvariantCulture);
        Assert.Equal(
            expectedSourceKeys.ToString(CultureInfo.InvariantCulture),
            await CommandAsync(source, ["CLUSTER", "COUNTKEYSINSLOT", number], token)
        );
        Assert.Equal("0", await CommandAsync(target, ["CLUSTER", "COUNTKEYSINSLOT", number], token));
        var sourceId = await CommandAsync(source, ["CLUSTER", "MYID"], token);
        var targetId = await CommandAsync(target, ["CLUSTER", "MYID"], token);
        var other = 3 - source - target;
        var otherId = await CommandAsync(other, ["CLUSTER", "MYID"], token);
        var ownedIds = new HashSet<string>(StringComparer.Ordinal) { sourceId, targetId, otherId };
        if (ownedIds.Count != 3 || ownedIds.Any(id => id.Length != 40 || !id.All(Uri.IsHexDigit)))
        {
            throw new InvalidOperationException("Owned cluster node identities are invalid.");
        }
        for (var index = 0; index < 3; index++)
        {
            var members = (await CommandAsync(index, ["CLUSTER", "NODES"], token)).Split(
                '\n',
                StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries
            );
            if (members.Length != 3 || !ownedIds.SetEquals(members.Select(line => line.Split(' ')[0])))
            {
                throw new InvalidOperationException(
                    "An external or unexpected cluster member was found; refusing migration."
                );
            }
        }
        return (sourceId, targetId);
    }

    private async Task WaitHealthyAsync(CancellationToken token)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var healthy = true;
            for (var index = 0; index < 3; index++)
            {
                var info = await CommandAsync(index, ["CLUSTER", "INFO"], token);
                healthy &=
                    info.Contains("cluster_state:ok", StringComparison.Ordinal)
                    && info.Contains("cluster_slots_ok:16384", StringComparison.Ordinal);
            }
            if (healthy)
            {
                return;
            }
            await Task.Delay(100, token);
        }
        throw new TimeoutException("The owned migration cluster did not become healthy.");
    }

    private async Task DiscoverAndVerifyAsync(CancellationToken token)
    {
        for (var index = 0; index < _ports.Length; index++)
        {
            var id = (await ComposeAsync(["ps", "--all", "--quiet", Service(index)], token)).Trim();
            if (id.Length == 0)
            {
                if (_containers[index] is not null)
                {
                    throw new InvalidOperationException("An owned migration container disappeared.");
                }
                continue; // Partial creation is cleaned up too.
            }
            if (_containers[index] is not null && _containers[index] != id)
            {
                throw new InvalidOperationException("An owned migration container was replaced.");
            }
            _containers[index] = id;
            await VerifyNodeAsync(index, token);
        }
    }

    private async Task VerifyNodeAsync(int index, CancellationToken token)
    {
        var id = _containers[index];
        if (id is null || id.Length != 64 || !id.All(Uri.IsHexDigit))
        {
            throw new InvalidOperationException("Refusing action without one exact owned container ID.");
        }
        using var inspection = JsonDocument.Parse(await DockerAsync(["inspect", id], token));
        var node = inspection.RootElement[0];
        var config = node.GetProperty("Config");
        var labels = config.GetProperty("Labels");
        var host = node.GetProperty("HostConfig");
        var binding = host.GetProperty("PortBindings").GetProperty("6379/tcp")[0];
        if (
            node.GetProperty("Id").GetString() != id
            || labels.GetProperty("com.docker.compose.project").GetString() != Project
            || labels.GetProperty("com.docker.compose.service").GetString() != Service(index)
            || labels.GetProperty("com.valkeydotnet.migration-token").GetString() != _token
            || config.GetProperty("Image").GetString() != "valkey/valkey:9.1"
            || config.GetProperty("Hostname").GetString() != Service(index)
            || host.GetProperty("Memory").GetInt64() != 128 * 1024 * 1024
            || host.GetProperty("NanoCpus").GetInt64() != 1_000_000_000
            || binding.GetProperty("HostIp").GetString() != "127.0.0.1"
            || binding.GetProperty("HostPort").GetString() != _ports[index].ToString(CultureInfo.InvariantCulture)
        )
        {
            throw new InvalidOperationException("Migration target identity or endpoint changed; refusing action.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_created)
        {
            return;
        }
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await DiscoverAndVerifyAsync(cleanup.Token);
        // Remove exact verified IDs, never a project-wide prune or unvalidated external endpoint.
        for (var index = 0; index < _containers.Length; index++)
        {
            var id = _containers[index];
            if (id is not null)
            {
                await DockerAsync(["rm", "--force", id], cleanup.Token);
                _containers[index] = null;
            }
        }
        if (
            (
                await DockerAsync(
                    ["ps", "--all", "--quiet", "--filter", "label=com.docker.compose.project=" + Project],
                    cleanup.Token
                )
            )
                .Trim()
                .Length != 0
        )
        {
            throw new InvalidOperationException(
                "Unexpected containers remain in the migration project; manual cleanup is required."
            );
        }
        var networks = (
            await DockerAsync(
                ["network", "ls", "--no-trunc", "--quiet", "--filter", "label=com.docker.compose.project=" + Project],
                cleanup.Token
            )
        ).Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (networks.Length > 1)
        {
            throw new InvalidOperationException("Unexpected migration networks; manual cleanup is required.");
        }
        foreach (var id in networks)
        {
            using var inspection = JsonDocument.Parse(await DockerAsync(["network", "inspect", id], cleanup.Token));
            var network = inspection.RootElement[0];
            var labels = network.GetProperty("Labels");
            if (
                network.GetProperty("Id").GetString() != id
                || labels.GetProperty("com.docker.compose.project").GetString() != Project
                || labels.GetProperty("com.valkeydotnet.migration-token").GetString() != _token
                || network.GetProperty("Containers").EnumerateObject().Any()
            )
            {
                throw new InvalidOperationException("Migration network ownership changed; manual cleanup is required.");
            }
            await DockerAsync(["network", "rm", id], cleanup.Token);
        }
        _created = false;
    }

    private Task<string> ComposeAsync(string[] arguments, CancellationToken token) =>
        DockerAsync(
            [
                "compose",
                "--file",
                Path.Combine(AppContext.BaseDirectory, "docker-compose.migration.yml"),
                "--project-name",
                Project,
                .. (_ports.Length == 4 ? FailoverProfile : Array.Empty<string>()),
                .. arguments,
            ],
            token
        );

    private async Task<string> DockerAsync(string[] arguments, CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo("docker")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            },
        };
        foreach (var argument in arguments)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }
        process.StartInfo.Environment["VALKEYDOTNET_MIGRATION_TOKEN"] = _token;
        process.StartInfo.Environment.Remove("COMPOSE_PROFILES");
        for (var index = 0; index < _ports.Length; index++)
        {
            process.StartInfo.Environment[
                "VALKEYDOTNET_MIGRATION_PORT_" + (index + 1).ToString(CultureInfo.InvariantCulture)
            ] = _ports[index].ToString(CultureInfo.InvariantCulture);
        }
        if (_dockerHost is not null)
        {
            process.StartInfo.Environment.Remove("DOCKER_CONTEXT");
            process.StartInfo.Environment["DOCKER_HOST"] = _dockerHost;
        }
        if (!process.Start())
        {
            throw new InvalidOperationException("Could not start Docker.");
        }
        var output = process.StandardOutput.ReadToEndAsync(timeout.Token);
        var error = process.StandardError.ReadToEndAsync(timeout.Token);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
            var result = await output;
            var details = await error;
            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException($"Docker failed for owned project {Project}: {details}");
            }
            return result;
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync(CancellationToken.None);
            }
            try
            {
                await Task.WhenAll(output, error);
            }
            catch (OperationCanceledException) { }
        }
    }
}
