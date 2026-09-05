using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace ValkeyDotNet.Benchmarks;

internal sealed class OwnedBenchmarkServer : IAsyncDisposable
{
    private const string Image = "valkey/valkey:9.1";
    private readonly string _nonce = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
    private string? _context;
    private string? _host;
    private string? _id;
    private bool _created;
    private bool _creationAttempted;
    internal string Project => "valkey-dotnet-bench-" + _nonce;
    internal int Port { get; private set; }
    internal string ImageId { get; private set; } = "";

    internal ValkeyClientOptions Options(ValkeyProtocol protocol) =>
        new()
        {
            Host = "127.0.0.1",
            Port = Port,
            Protocol = protocol,
            ClientName = Project,
            ConnectTimeout = TimeSpan.FromSeconds(2),
            MaxPendingRequests = 1024,
        };

    internal static void RequireLocalDocker(string endpoint)
    {
        if (
            !endpoint.StartsWith("unix://", StringComparison.Ordinal)
            && !endpoint.StartsWith("npipe://", StringComparison.Ordinal)
        )
        {
            throw new InvalidOperationException("Round-trip benchmarks require a local Docker daemon.");
        }
    }

    internal async Task StartAsync(CancellationToken token)
    {
        if (_created)
        {
            throw new InvalidOperationException("The owned benchmark server cannot be reused.");
        }
        _context = Environment.GetEnvironmentVariable("DOCKER_CONTEXT");
        _host = Environment.GetEnvironmentVariable("DOCKER_HOST");
        _context = string.IsNullOrWhiteSpace(_context) ? null : _context;
        _host = string.IsNullOrWhiteSpace(_host) ? null : _host;
        if (!string.IsNullOrWhiteSpace(_context))
        {
            _host = null;
            RequireLocalDocker(
                await DockerAsync(["context", "inspect", _context, "--format", "{{.Endpoints.docker.Host}}"], token)
            );
        }
        else if (string.IsNullOrWhiteSpace(_host))
        {
            _context = await DockerAsync(["context", "show"], token);
            RequireLocalDocker(
                await DockerAsync(["context", "inspect", _context, "--format", "{{.Endpoints.docker.Host}}"], token)
            );
        }
        else
        {
            RequireLocalDocker(_host);
        }
        // Existing endpoint variables are deliberately never read. Only this newly created ID is used.
        _creationAttempted = true;
        var created = await DockerAsync(
            [
                "create",
                "--name",
                Project,
                "--label",
                "valkeydotnet.benchmark=" + _nonce,
                "--memory",
                "128m",
                "--cpus",
                "1",
                "--pids-limit",
                "64",
                "--read-only",
                "--network",
                "bridge",
                "--tmpfs",
                "/data:rw,noexec,nosuid,size=16m",
                "--publish",
                "127.0.0.1::6379",
                Image,
                "valkey-server",
                "--save",
                "",
                "--appendonly",
                "no",
                "--enable-debug-command",
                "no",
            ],
            token
        );
        if (created.Length != 64 || !created.All(char.IsAsciiHexDigit))
        {
            throw new InvalidOperationException("Docker returned an invalid owned container identity.");
        }
        _id = created;
        _created = true;
        await VerifyOwnershipAsync(token);
        await DockerAsync(["start", _id], token);
        using (var inspection = JsonDocument.Parse(await DockerAsync(["inspect", _id], token)))
        {
            var node = inspection.RootElement[0];
            var bindings = node.GetProperty("NetworkSettings").GetProperty("Ports").GetProperty("6379/tcp");
            if (bindings.GetArrayLength() != 1 || bindings[0].GetProperty("HostIp").GetString() != "127.0.0.1")
            {
                throw new InvalidOperationException("Benchmark port is not exclusively loopback-bound.");
            }
            Port = int.Parse(bindings[0].GetProperty("HostPort").GetString()!, CultureInfo.InvariantCulture);
            ImageId = node.GetProperty("Image").GetString()!;
        }
        using var readiness = CancellationTokenSource.CreateLinkedTokenSource(token);
        readiness.CancelAfter(TimeSpan.FromSeconds(30));
        while (true)
        {
            readiness.Token.ThrowIfCancellationRequested();
            try
            {
                await using var client = await ValkeyClient.ConnectAsync(
                    Options(ValkeyProtocol.Resp3),
                    readiness.Token
                );
                if (await client.PingAsync(readiness.Token) == "PONG")
                {
                    break;
                }
            }
            catch (ValkeyConnectionException)
            {
                await Task.Delay(100, readiness.Token);
            }
        }
    }

    private async Task VerifyOwnershipAsync(CancellationToken token)
    {
        using var inspection = JsonDocument.Parse(await DockerAsync(["inspect", _id!], token));
        var node = inspection.RootElement[0];
        var config = node.GetProperty("Config");
        var host = node.GetProperty("HostConfig");
        var ports = host.GetProperty("PortBindings").GetProperty("6379/tcp");
        if (
            inspection.RootElement.GetArrayLength() != 1
            || node.GetProperty("Id").GetString() != _id
            || node.GetProperty("Name").GetString() != "/" + Project
            || config.GetProperty("Labels").GetProperty("valkeydotnet.benchmark").GetString() != _nonce
            || config.GetProperty("Image").GetString() != Image
            || host.GetProperty("Memory").GetInt64() != 128 * 1024 * 1024
            || host.GetProperty("NanoCpus").GetInt64() != 1_000_000_000
            || host.GetProperty("PidsLimit").GetInt64() != 64
            || !host.GetProperty("ReadonlyRootfs").GetBoolean()
            || host.GetProperty("Privileged").GetBoolean()
            || host.GetProperty("NetworkMode").GetString() is not ("bridge" or "default")
            || host.GetProperty("Tmpfs").GetProperty("/data").GetString() != "rw,noexec,nosuid,size=16m"
            || node.GetProperty("Mounts")
                .EnumerateArray()
                .Any(mount => mount.GetProperty("Type").GetString() != "tmpfs")
            || ports.GetArrayLength() != 1
            || ports[0].GetProperty("HostIp").GetString() != "127.0.0.1"
        )
        {
            throw new InvalidOperationException(
                "Benchmark container ownership or limits changed; refusing lifecycle action."
            );
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_creationAttempted)
        {
            return;
        }
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        if (!_created)
        {
            // A timed-out create may still have succeeded. Resolve only this nonce-labelled exact name.
            var ids = await DockerAsync(
                [
                    "ps",
                    "-a",
                    "--no-trunc",
                    "--filter",
                    "name=^/" + Project + "$",
                    "--filter",
                    "label=valkeydotnet.benchmark=" + _nonce,
                    "--format",
                    "{{.ID}}",
                ],
                cleanup.Token
            );
            if (ids.Length == 0)
            {
                _creationAttempted = false;
                return;
            }
            if (ids.Length != 64 || !ids.All(char.IsAsciiHexDigit))
            {
                throw new InvalidOperationException("Ambiguous benchmark container identity; refusing cleanup.");
            }
            _id = ids;
        }
        await VerifyOwnershipAsync(cleanup.Token);
        await DockerAsync(["rm", "--force", "--volumes", _id!], cleanup.Token);
        if (
            (
                await DockerAsync(
                    ["ps", "-a", "--no-trunc", "--filter", "id=" + _id, "--format", "{{.ID}}"],
                    cleanup.Token
                )
            ).Length != 0
        )
        {
            throw new InvalidOperationException("Owned benchmark container remains after cleanup.");
        }
        _created = false;
        _creationAttempted = false;
        Console.WriteLine("Removed owned benchmark container: " + Project);
    }

    private async Task<string> DockerAsync(string[] arguments, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(60));
        var info = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        if (_context is not null)
        {
            info.ArgumentList.Add("--context");
            info.ArgumentList.Add(_context);
            info.Environment.Remove("DOCKER_HOST");
        }
        else if (_host is not null)
        {
            info.Environment.Remove("DOCKER_CONTEXT");
            info.Environment["DOCKER_HOST"] = _host;
        }
        foreach (var argument in arguments)
        {
            info.ArgumentList.Add(argument);
        }
        using var process = Process.Start(info) ?? throw new InvalidOperationException("Could not start Docker.");
        var output = ReadBoundedAsync(process.StandardOutput, deadline.Token);
        var error = ReadBoundedAsync(process.StandardError, deadline.Token);
        try
        {
            await process.WaitForExitAsync(deadline.Token);
            await Task.WhenAll(output, error);
            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    "Owned benchmark Docker command failed; inspect local Docker configuration."
                );
            }
            return (await output).Trim();
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
            await deadline.CancelAsync();
            await ((Task)Task.WhenAll(output, error)).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    private static async Task<string> ReadBoundedAsync(StreamReader reader, CancellationToken token)
    {
        var text = new StringBuilder();
        var buffer = new char[4096];
        int count;
        while ((count = await reader.ReadAsync(buffer.AsMemory(), token)) != 0)
        {
            if (text.Length + count > 65536)
            {
                throw new InvalidOperationException("Docker command output exceeded the benchmark limit.");
            }
            text.Append(buffer, 0, count);
        }
        return text.ToString();
    }
}
