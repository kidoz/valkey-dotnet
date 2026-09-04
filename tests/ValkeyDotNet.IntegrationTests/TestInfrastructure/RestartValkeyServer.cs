using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

// Owns a fresh Compose project. Never accepts an external container, project, or endpoint.
internal sealed class RestartValkeyServer : IAsyncDisposable
{
    private readonly string _token = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
    private readonly string _version;
    private string? _dockerHost;
    private string? _container;
    private bool _created;

    internal RestartValkeyServer(string version)
    {
        if (version is not ("7.2" or "8.1" or "9.1"))
            throw new ArgumentOutOfRangeException(nameof(version));
        _version = version;
        using var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        Port = ((IPEndPoint)reservation.LocalEndpoint).Port;
        // Docker needs to bind it next. A port collision fails creation; no existing service is stopped.
    }

    internal int Port { get; }
    internal string Project => "valkey-dotnet-resilience-tests-" + _token;

    internal async Task StartNewAsync(CancellationToken cancellationToken)
    {
        if (_created)
            throw new InvalidOperationException("This fixture has already created its project.");
        var context = Environment.GetEnvironmentVariable("DOCKER_CONTEXT");
        var dockerHost = !string.IsNullOrWhiteSpace(context)
            ? (
                await DockerAsync(
                    ["context", "inspect", context, "--format", "{{.Endpoints.docker.Host}}"],
                    cancellationToken
                )
            ).Trim()
            : Environment.GetEnvironmentVariable("DOCKER_HOST")
                ?? (
                    await DockerAsync(
                        ["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
                        cancellationToken
                    )
                ).Trim();
        if (
            !dockerHost.StartsWith("unix://", StringComparison.Ordinal)
            && !dockerHost.StartsWith("npipe:////./pipe/", StringComparison.Ordinal)
        )
            throw new InvalidOperationException(
                "Restart tests require a local Unix-socket or named-pipe Docker endpoint."
            );
        _dockerHost = dockerHost;
        _created = true;
        await ComposeAsync(["up", "-d", "--wait", "--wait-timeout", "45"], cancellationToken);
        _container = (await ComposeAsync(["ps", "--all", "--quiet", "server"], cancellationToken)).Trim();
        await VerifyTargetAsync(cancellationToken);
    }

    internal async Task StopAsync(CancellationToken cancellationToken)
    {
        await VerifyTargetAsync(cancellationToken);
        await DockerAsync(["stop", "--time", "5", _container!], cancellationToken);
    }

    internal async Task RestartAsync(CancellationToken cancellationToken)
    {
        await VerifyTargetAsync(cancellationToken);
        await DockerAsync(["start", _container!], cancellationToken);
        // Poll a command, not only a TCP accept, before making a recovery assertion.
        for (var attempt = 0; attempt < 100; attempt++)
        {
            try
            {
                await using var probe = await ValkeyClient.ConnectAsync(
                    new ValkeyClientOptions
                    {
                        Host = "127.0.0.1",
                        Port = Port,
                        ConnectTimeout = TimeSpan.FromMilliseconds(200),
                    },
                    cancellationToken
                );
                if (await probe.PingAsync(cancellationToken) == "PONG")
                    return;
            }
            catch (Exception exception)
                when (exception is SocketException or IOException or TimeoutException or ValkeyConnectionException)
            { /* Bounded readiness polling for this newly restarted, owned server. */
            }
            await Task.Delay(100, cancellationToken);
        }
        throw new TimeoutException("The disposable restart server did not become ready.");
    }

    private async Task VerifyTargetAsync(CancellationToken cancellationToken)
    {
        if (_container is null || _container.Length != 64 || !_container.All(Uri.IsHexDigit))
            throw new InvalidOperationException("Refusing to operate without one exact container identifier.");
        using var inspection = JsonDocument.Parse(await DockerAsync(["inspect", _container], cancellationToken));
        var target = inspection.RootElement[0];
        var config = target.GetProperty("Config");
        var labels = config.GetProperty("Labels");
        var binding = target.GetProperty("HostConfig").GetProperty("PortBindings").GetProperty("6379/tcp")[0];
        if (
            target.GetProperty("Id").GetString() != _container
            || labels.GetProperty("com.docker.compose.project").GetString() != Project
            || labels.GetProperty("com.docker.compose.service").GetString() != "server"
            || labels.GetProperty("com.valkeydotnet.resilience-token").GetString() != _token
            || config.GetProperty("Image").GetString() != "valkey/valkey:" + _version
            || binding.GetProperty("HostIp").GetString() != "127.0.0.1"
            || binding.GetProperty("HostPort").GetString() != Port.ToString(CultureInfo.InvariantCulture)
        )
            throw new InvalidOperationException(
                "Disposable container identity or endpoint changed; refusing the action."
            );
    }

    public async ValueTask DisposeAsync()
    {
        if (!_created)
            return;
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        // Find a container even after a partially failed 'up', then validate it before deletion.
        var containers = (await ComposeAsync(["ps", "--all", "--quiet"], cleanup.Token)).Split(
            '\n',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries
        );
        if (containers.Length > 1)
            throw new InvalidOperationException($"Unexpected resources in {Project}; manual cleanup is required.");
        if (containers.Length == 1)
        {
            _container ??= containers[0];
            if (_container != containers[0])
                throw new InvalidOperationException("The disposable container was replaced; refusing cleanup.");
            await VerifyTargetAsync(cleanup.Token);
        }
        await ComposeAsync(["down", "--volumes", "--timeout", "5"], cleanup.Token);
        _created = false;
    }

    private Task<string> ComposeAsync(string[] arguments, CancellationToken cancellationToken) =>
        DockerAsync(
            [
                "compose",
                "--file",
                Path.Combine(AppContext.BaseDirectory, "docker-compose.resilience.yml"),
                "--project-name",
                Project,
                .. arguments,
            ],
            cancellationToken
        );

    private async Task<string> DockerAsync(string[] arguments, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        using var process = new Process();
        process.StartInfo = new ProcessStartInfo("docker")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var argument in arguments)
            process.StartInfo.ArgumentList.Add(argument);
        process.StartInfo.Environment["VALKEYDOTNET_RESILIENCE_VERSION"] = _version;
        process.StartInfo.Environment["VALKEYDOTNET_RESILIENCE_PORT"] = Port.ToString(CultureInfo.InvariantCulture);
        process.StartInfo.Environment["VALKEYDOTNET_RESILIENCE_TOKEN"] = _token;
        if (_dockerHost is not null)
        {
            // Freeze the checked daemon endpoint; later changes to the user's current context
            // cannot redirect lifecycle commands to a different Docker daemon.
            process.StartInfo.Environment.Remove("DOCKER_CONTEXT");
            process.StartInfo.Environment["DOCKER_HOST"] = _dockerHost;
        }
        if (!process.Start())
            throw new InvalidOperationException("Could not start Docker.");
        var output = process.StandardOutput.ReadToEndAsync(timeout.Token);
        var error = process.StandardError.ReadToEndAsync(timeout.Token);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
            var result = await output;
            var details = await error;
            if (process.ExitCode != 0)
                throw new InvalidOperationException($"Docker failed for disposable project {Project}: {details}");
            return result;
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync(CancellationToken.None);
            }
            // Observe canceled pipe reads as well as the process task.
            try
            {
                await Task.WhenAll(output, error);
            }
            catch (OperationCanceledException) { }
        }
    }
}
