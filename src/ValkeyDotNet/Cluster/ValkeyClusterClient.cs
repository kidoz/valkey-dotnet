using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using ValkeyDotNet.Cluster;

namespace ValkeyDotNet;

/// <summary>
/// Routes key-based commands across a Valkey Cluster using the cluster hash-slot map. Connections
/// are opened lazily and reused per primary node; MOVED and ASK redirects are followed with a
/// bounded retry count.
/// </summary>
public sealed partial class ValkeyClusterClient : IAsyncDisposable
{
    private const int MaxEndpointCharacters = 1_024;

    private sealed class NodePool
    {
        private int _next;

        public List<ValkeyClient> Clients { get; } = [];

        public ValkeyClient Select()
        {
            var selected = Clients[_next];
            _next = (_next + 1) % Clients.Count;
            return selected;
        }

        public int RemoveDisposed()
        {
            var removed = Clients.RemoveAll(static client => client.IsDisposed);
            _next = Clients.Count == 0 ? 0 : _next % Clients.Count;
            return removed;
        }
    }

    private readonly record struct ClusterEndpoint(string Host, int Port)
    {
        public static ClusterEndpoint Create(string host, int port)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(host);
            if (host.Length > MaxEndpointCharacters)
                throw new ArgumentException("A cluster endpoint host is too long.", nameof(host));
            if (port is < 1 or > 65535)
                throw new ArgumentOutOfRangeException(nameof(port));
            host = host.Trim();
            if (host.Length > 1 && host[0] == '[' && host[^1] == ']')
                host = host[1..^1];
            return new(host.TrimEnd('.').ToUpperInvariant(), port);
        }

        public override string ToString() =>
            Host.Contains(':', StringComparison.Ordinal) ? $"[{Host}]:{Port}" : $"{Host}:{Port}";
    }

    private readonly record struct ClusterRedirect(bool Asking, int Slot, ClusterEndpoint Endpoint);

    private readonly record struct PipelineItem(int Index, int Slot, ClusterEndpoint Endpoint, ValkeyCommand Command);

    private readonly ValkeyClusterOptions _options;
    private readonly ValkeyClientOptions _nodeTemplate;
    private readonly ClusterEndpoint _seedEndpoint;
    private readonly Dictionary<ClusterEndpoint, NodePool> _pools = [];
    private readonly SemaphoreSlim _clientGate = new(1, 1);
    private ClusterEndpoint?[] _slots;
    private int _connectionCount;
    private int _disposed;

    private ValkeyClusterClient(
        ValkeyClusterOptions options,
        ValkeyClientOptions nodeTemplate,
        ClusterEndpoint seedEndpoint,
        ValkeyClient seedClient,
        ClusterEndpoint?[] slots
    )
    {
        _options = options;
        _nodeTemplate = nodeTemplate;
        _seedEndpoint = seedEndpoint;
        var seedPool = new NodePool();
        seedPool.Clients.Add(seedClient);
        _pools.Add(seedEndpoint, seedPool);
        _connectionCount = 1;
        _slots = slots;
    }

    /// <summary>Connects to the first usable seed and loads the complete primary slot map.</summary>
    public static async Task<ValkeyClusterClient> ConnectAsync(
        ValkeyClusterOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        options ??= new ValkeyClusterOptions();
        var seeds = options.ValidateAndCopySeeds();
        Exception? lastFailure = null;

        foreach (var seed in seeds)
        {
            ValkeyClient? client = null;
            try
            {
                client = await ValkeyClient.ConnectAsync(seed, cancellationToken).ConfigureAwait(false);
                var endpoint = ClusterEndpoint.Create(seed.Host, seed.Port);
                var slots = await ReadTopologyAsync(client, endpoint, options, seed.UseTls, cancellationToken)
                    .ConfigureAwait(false);
                return new ValkeyClusterClient(options, seed, endpoint, client, slots);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (client is not null)
                    await client.DisposeAsync().ConfigureAwait(false);
                throw;
            }
            catch (Exception exception)
                when (exception
                        is ValkeyException
                            or IOException
                            or SocketException
                            or TimeoutException
                            or AuthenticationException
                )
            {
                lastFailure = exception;
                if (client is not null)
                    await client.DisposeAsync().ConfigureAwait(false);
            }
        }

        throw new ValkeyClusterException(
            "None of the configured seed nodes returned a usable cluster topology.",
            lastFailure!
        );
    }

    /// <summary>Calculates the Valkey Cluster slot for a binary key, including hash-tag handling.</summary>
    public static int GetHashSlot(ReadOnlySpan<byte> key) => ClusterHashSlot.Calculate(key);

    /// <summary>Calculates the Valkey Cluster slot for a UTF-8 key, including hash-tag handling.</summary>
    public static int GetHashSlot(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return ClusterHashSlot.Calculate(Encoding.UTF8.GetBytes(key));
    }

    /// <summary>
    /// Executes a command on the node responsible for <paramref name="routingKey"/>. Every key in a
    /// multi-key command must hash to that same slot; CROSSSLOT errors are returned by the server.
    /// </summary>
    public async Task<RespValue> ExecuteAsync(
        ValkeyArgument routingKey,
        ValkeyCommand command,
        CancellationToken cancellationToken = default
    ) => await ExecuteCoreAsync(routingKey, command, timeout: null, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Executes a routed command with one isolated deadline across connection acquisition and every
    /// bounded redirect attempt.
    /// </summary>
    public async Task<RespValue> ExecuteWithDeadlineAsync(
        ValkeyArgument routingKey,
        ValkeyCommand command,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) =>
        await ExecuteCoreAsync(routingKey, command, ValkeyClient.ValidateOperationTimeout(timeout), cancellationToken)
            .ConfigureAwait(false);

    private async Task<RespValue> ExecuteCoreAsync(
        ValkeyArgument routingKey,
        ValkeyCommand command,
        TimeSpan? timeout,
        CancellationToken cancellationToken,
        ValkeyScript? script = null
    )
    {
        ArgumentNullException.ThrowIfNull(command);
        ThrowIfDisposed();
        var startedAt = Stopwatch.GetTimestamp();

        var slot = ClusterHashSlot.Calculate(routingKey.Bytes.Span);
        var endpoint =
            Volatile.Read(ref _slots)[slot]
            ?? throw new ValkeyClusterException($"No primary node is known for cluster slot {slot}.");
        var asking = false;
        var attempted = false;

        for (var redirects = 0; ; redirects++)
        {
            var client = await GetClientWithinDeadlineAsync(
                    endpoint,
                    timeout,
                    startedAt,
                    attempted ? ValkeyCommandDeliveryStatus.MayHaveBeenSent : ValkeyCommandDeliveryStatus.NotSent,
                    cancellationToken
                )
                .ConfigureAwait(false);
            try
            {
                RespValue response;
                attempted = true;
                if (script is not null)
                {
                    response = await client
                        .ExecuteScriptCoreAsync(
                            script,
                            command,
                            asking,
                            timeout is { } scriptTimeout ? GetRemainingTimeout(scriptTimeout, startedAt) : null,
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                }
                else if (asking)
                {
                    var replies = timeout is { } operationTimeout
                        ? await client
                            .ExecutePipelineWithDeadlineAsync(
                                [new ValkeyCommand("ASKING"), command],
                                GetRemainingTimeout(operationTimeout, startedAt),
                                cancellationToken
                            )
                            .ConfigureAwait(false)
                        : await client
                            .ExecutePipelineAsync([new ValkeyCommand("ASKING"), command], cancellationToken)
                            .ConfigureAwait(false);
                    replies[0].ThrowIfError();
                    response = replies[1];
                    response.ThrowIfError();
                }
                else
                {
                    response = timeout is { } operationTimeout
                        ? await client
                            .ExecuteWithDeadlineAsync(
                                command,
                                GetRemainingTimeout(operationTimeout, startedAt),
                                cancellationToken
                            )
                            .ConfigureAwait(false)
                        : await client.ExecuteAsync(command, cancellationToken).ConfigureAwait(false);
                }
                return response;
            }
            catch (ValkeyServerException exception) when (exception.ErrorCode is "MOVED" or "ASK")
            {
                if (redirects >= _options.MaxRedirects)
                    throw new ValkeyClusterException(
                        $"The command exceeded the configured limit of {_options.MaxRedirects} cluster redirects.",
                        exception
                    );

                var redirect = ParseRedirect(exception, endpoint, _options);
                ValidateRedirectSlot(redirect, slot, exception);
                endpoint = redirect.Endpoint;
                asking = redirect.Asking;
                if (!asking)
                    UpdateSlot(slot, endpoint);
            }
            catch (Exception exception) when (exception is ValkeyConnectionException or ValkeyProtocolException)
            {
                await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
                throw;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (client.IsDisposed)
                    await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
                throw;
            }
            catch (ObjectDisposedException disposedException)
            {
                await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
                throw new ValkeyConnectionException("The cluster node connection was unavailable.", disposedException);
            }
        }
    }

    /// <summary>
    /// Groups commands by their current primary, pipelines each group, and runs independent node
    /// groups concurrently. Results retain input order and server errors remain in place. MOVED and
    /// ASK replies are followed individually after their initial node group has been fully drained.
    /// </summary>
    public async Task<IReadOnlyList<RespValue>> ExecutePipelineAsync(
        IEnumerable<ValkeyClusterCommand> commands,
        CancellationToken cancellationToken = default
    ) => await ExecutePipelineCoreAsync(commands, timeout: null, cancellationToken).ConfigureAwait(false);

    /// <summary>Executes all routed node groups and redirects within one isolated deadline.</summary>
    public async Task<IReadOnlyList<RespValue>> ExecutePipelineWithDeadlineAsync(
        IEnumerable<ValkeyClusterCommand> commands,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) =>
        await ExecutePipelineCoreAsync(commands, ValkeyClient.ValidateOperationTimeout(timeout), cancellationToken)
            .ConfigureAwait(false);

    private async Task<IReadOnlyList<RespValue>> ExecutePipelineCoreAsync(
        IEnumerable<ValkeyClusterCommand> commands,
        TimeSpan? timeout,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(commands);
        ThrowIfDisposed();
        var startedAt = Stopwatch.GetTimestamp();
        var commandList = commands.ToArray();
        if (commandList.Length == 0)
            return Array.Empty<RespValue>();
        if (commandList.Any(static command => command is null))
            throw new ArgumentException("A cluster pipeline cannot contain a null command.", nameof(commands));

        var slotMap = Volatile.Read(ref _slots);
        var groups = new Dictionary<ClusterEndpoint, List<PipelineItem>>();
        for (var index = 0; index < commandList.Length; index++)
        {
            var clusterCommand = commandList[index];
            var slot = ClusterHashSlot.Calculate(clusterCommand.RoutingKey.Bytes.Span);
            var endpoint =
                slotMap[slot] ?? throw new ValkeyClusterException($"No primary node is known for cluster slot {slot}.");
            if (!groups.TryGetValue(endpoint, out var group))
            {
                group = [];
                groups.Add(endpoint, group);
            }
            group.Add(new PipelineItem(index, slot, endpoint, clusterCommand.Command));
        }

        var responses = new RespValue[commandList.Length];
        var tasks = groups.Values.Select(group =>
            ExecutePipelineGroupAsync(group, responses, timeout, startedAt, cancellationToken)
        );
        await Task.WhenAll(tasks).ConfigureAwait(false);
        return responses;
    }

    /// <summary>Reloads all 16,384 slot assignments from the original seed endpoint.</summary>
    public async Task RefreshTopologyAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var client = await GetClientAsync(_seedEndpoint, cancellationToken).ConfigureAwait(false);
        try
        {
            var slots = await ReadTopologyAsync(
                    client,
                    _seedEndpoint,
                    _options,
                    _nodeTemplate.UseTls,
                    cancellationToken
                )
                .ConfigureAwait(false);
            Volatile.Write(ref _slots, slots);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (client.IsDisposed)
                await RemoveClientAsync(_seedEndpoint, client).ConfigureAwait(false);
            throw;
        }
        catch (Exception exception) when (exception is ValkeyConnectionException or ValkeyProtocolException)
        {
            await RemoveClientAsync(_seedEndpoint, client).ConfigureAwait(false);
            throw;
        }
        catch (ObjectDisposedException exception)
        {
            await RemoveClientAsync(_seedEndpoint, client).ConfigureAwait(false);
            throw new ValkeyConnectionException("The cluster seed connection was unavailable.", exception);
        }
    }

    public async Task<string> PingAsync(CancellationToken cancellationToken = default)
    {
        var client = await GetClientAsync(_seedEndpoint, cancellationToken).ConfigureAwait(false);
        try
        {
            return await client.PingAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (client.IsDisposed)
                await RemoveClientAsync(_seedEndpoint, client).ConfigureAwait(false);
            throw;
        }
        catch (Exception exception) when (exception is ValkeyConnectionException or ValkeyProtocolException)
        {
            await RemoveClientAsync(_seedEndpoint, client).ConfigureAwait(false);
            throw;
        }
        catch (ObjectDisposedException exception)
        {
            await RemoveClientAsync(_seedEndpoint, client).ConfigureAwait(false);
            throw new ValkeyConnectionException("The cluster seed connection was unavailable.", exception);
        }
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ValkeyArgument routingKey = key;
        var value = await ExecuteAsync(routingKey, new ValkeyCommand("GET", routingKey), cancellationToken)
            .ConfigureAwait(false);
        return value.IsNull ? null : value.AsBytes().ToArray();
    }

    public async Task<string?> GetStringAsync(string key, CancellationToken cancellationToken = default)
    {
        ValkeyArgument routingKey = key;
        return (
            await ExecuteAsync(routingKey, new ValkeyCommand("GET", routingKey), cancellationToken)
                .ConfigureAwait(false)
        ).AsString();
    }

    public async Task<bool> SetAsync(
        string key,
        ReadOnlyMemory<byte> value,
        TimeSpan? expiry = null,
        bool onlyIfNotExists = false,
        bool onlyIfExists = false,
        CancellationToken cancellationToken = default
    )
    {
        if (onlyIfNotExists && onlyIfExists)
            throw new ArgumentException("NX and XX cannot be used together.");

        ValkeyArgument routingKey = key;
        var arguments = new List<ValkeyArgument> { routingKey, new(value) };
        if (expiry is { } duration)
        {
            if (duration <= TimeSpan.Zero || duration.TotalMilliseconds > long.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(expiry));
            arguments.Add("PX");
            arguments.Add((long)Math.Ceiling(duration.TotalMilliseconds));
        }
        if (onlyIfNotExists)
            arguments.Add("NX");
        if (onlyIfExists)
            arguments.Add("XX");

        var response = await ExecuteAsync(routingKey, new ValkeyCommand("SET", arguments.ToArray()), cancellationToken)
            .ConfigureAwait(false);
        return !response.IsNull;
    }

    public Task<bool> SetStringAsync(
        string key,
        string value,
        TimeSpan? expiry = null,
        bool onlyIfNotExists = false,
        bool onlyIfExists = false,
        CancellationToken cancellationToken = default
    ) =>
        SetAsync(
            key,
            Encoding.UTF8.GetBytes(value ?? throw new ArgumentNullException(nameof(value))),
            expiry,
            onlyIfNotExists,
            onlyIfExists,
            cancellationToken
        );

    public async Task<long> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ValkeyArgument routingKey = key;
        return (
            await ExecuteAsync(routingKey, new ValkeyCommand("DEL", routingKey), cancellationToken)
                .ConfigureAwait(false)
        ).AsInt64();
    }

    public async Task<long> IncrementAsync(string key, long amount = 1, CancellationToken cancellationToken = default)
    {
        ValkeyArgument routingKey = key;
        return (
            await ExecuteAsync(routingKey, new ValkeyCommand("INCRBY", routingKey, amount), cancellationToken)
                .ConfigureAwait(false)
        ).AsInt64();
    }

    public async Task<bool> HashSetAsync(
        string key,
        string field,
        ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default
    )
    {
        ValkeyArgument routingKey = key;
        return (
                await ExecuteAsync(
                        routingKey,
                        new ValkeyCommand("HSET", routingKey, field, new ValkeyArgument(value)),
                        cancellationToken
                    )
                    .ConfigureAwait(false)
            ).AsInt64() == 1;
    }

    public async Task<byte[]?> HashGetAsync(string key, string field, CancellationToken cancellationToken = default)
    {
        ValkeyArgument routingKey = key;
        var value = await ExecuteAsync(routingKey, new ValkeyCommand("HGET", routingKey, field), cancellationToken)
            .ConfigureAwait(false);
        return value.IsNull ? null : value.AsBytes().ToArray();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _clientGate.WaitAsync().ConfigureAwait(false);
        ValkeyClient[] clients;
        try
        {
            clients = _pools.Values.SelectMany(static pool => pool.Clients).ToArray();
            _pools.Clear();
            _connectionCount = 0;
        }
        finally
        {
            _clientGate.Release();
        }

        foreach (var client in clients)
            await client.DisposeAsync().ConfigureAwait(false);
    }

    private async Task<ValkeyClient> GetClientAsync(ClusterEndpoint endpoint, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await _clientGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (!_pools.TryGetValue(endpoint, out var pool))
            {
                pool = new NodePool();
                _pools.Add(endpoint, pool);
            }

            _connectionCount -= pool.RemoveDisposed();

            var missing = _options.ConnectionsPerNode - pool.Clients.Count;
            if (missing > _options.MaxNodeConnections - _connectionCount)
                throw new ValkeyClusterException(
                    $"Opening {_options.ConnectionsPerNode} connections for a cluster node would exceed the "
                        + $"configured total limit of {_options.MaxNodeConnections}."
                );

            while (pool.Clients.Count < _options.ConnectionsPerNode)
            {
                var client = await ValkeyClient
                    .ConnectAsync(CreateNodeOptions(endpoint), cancellationToken)
                    .ConfigureAwait(false);
                pool.Clients.Add(client);
                _connectionCount++;
            }
            return pool.Select();
        }
        catch
        {
            if (_pools.TryGetValue(endpoint, out var pool) && pool.Clients.Count == 0)
                _pools.Remove(endpoint);
            throw;
        }
        finally
        {
            _clientGate.Release();
        }
    }

    private async Task<ValkeyClient> GetClientWithinDeadlineAsync(
        ClusterEndpoint endpoint,
        TimeSpan? timeout,
        long startedAt,
        ValkeyCommandDeliveryStatus deliveryStatus,
        CancellationToken cancellationToken
    )
    {
        if (timeout is not { } operationTimeout)
            return await GetClientAsync(endpoint, cancellationToken).ConfigureAwait(false);

        var remaining = GetRemainingTimeout(operationTimeout, startedAt, deliveryStatus);
        using var timeoutCancellation = new CancellationTokenSource(remaining);
        using var admissionCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCancellation.Token
        );
        try
        {
            return await GetClientAsync(endpoint, admissionCancellation.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (timeoutCancellation.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new ValkeyCommandTimeoutException(operationTimeout, deliveryStatus);
        }
    }

    private async ValueTask RemoveClientAsync(ClusterEndpoint endpoint, ValkeyClient client)
    {
        await _clientGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_pools.TryGetValue(endpoint, out var pool) && pool.Clients.Remove(client))
            {
                _connectionCount--;
                if (pool.Clients.Count == 0)
                    _pools.Remove(endpoint);
            }
        }
        finally
        {
            _clientGate.Release();
        }
        await client.DisposeAsync().ConfigureAwait(false);
    }

    private async Task ExecutePipelineGroupAsync(
        IReadOnlyList<PipelineItem> group,
        RespValue[] responses,
        TimeSpan? timeout,
        long startedAt,
        CancellationToken cancellationToken
    )
    {
        var endpoint = group[0].Endpoint;
        var client = await GetClientWithinDeadlineAsync(
                endpoint,
                timeout,
                startedAt,
                // Other node groups execute concurrently, so the batch as a whole may already have
                // reached Valkey even when this group is still acquiring its connection.
                ValkeyCommandDeliveryStatus.MayHaveBeenSent,
                cancellationToken
            )
            .ConfigureAwait(false);
        IReadOnlyList<RespValue> groupedResponses;
        try
        {
            groupedResponses = timeout is { } operationTimeout
                ? await client
                    .ExecutePipelineWithDeadlineAsync(
                        group.Select(static item => item.Command),
                        GetRemainingTimeout(operationTimeout, startedAt),
                        cancellationToken
                    )
                    .ConfigureAwait(false)
                : await client
                    .ExecutePipelineAsync(group.Select(static item => item.Command), cancellationToken)
                    .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is ValkeyConnectionException or ValkeyProtocolException)
        {
            await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
            throw;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (client.IsDisposed)
                await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
            throw;
        }
        catch (ObjectDisposedException exception)
        {
            await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
            throw new ValkeyConnectionException("The cluster node connection was unavailable.", exception);
        }

        for (var index = 0; index < group.Count; index++)
        {
            var item = group[index];
            var response = groupedResponses[index];
            responses[item.Index] = IsRedirect(response)
                ? await FollowPipelineRedirectAsync(item, response, timeout, startedAt, cancellationToken)
                    .ConfigureAwait(false)
                : response;
        }
    }

    private async Task<RespValue> FollowPipelineRedirectAsync(
        PipelineItem item,
        RespValue response,
        TimeSpan? timeout,
        long startedAt,
        CancellationToken cancellationToken
    )
    {
        var endpoint = item.Endpoint;
        for (var redirects = 0; IsRedirect(response); redirects++)
        {
            var exception = response.ToServerException();
            if (redirects >= _options.MaxRedirects)
                throw new ValkeyClusterException(
                    $"The command exceeded the configured limit of {_options.MaxRedirects} cluster redirects.",
                    exception
                );

            var redirect = ParseRedirect(exception, endpoint, _options);
            ValidateRedirectSlot(redirect, item.Slot, exception);
            endpoint = redirect.Endpoint;
            if (!redirect.Asking)
                UpdateSlot(item.Slot, endpoint);

            var client = await GetClientWithinDeadlineAsync(
                    endpoint,
                    timeout,
                    startedAt,
                    ValkeyCommandDeliveryStatus.MayHaveBeenSent,
                    cancellationToken
                )
                .ConfigureAwait(false);
            try
            {
                IReadOnlyList<RespValue> replies;
                if (timeout is { } operationTimeout)
                {
                    var remaining = GetRemainingTimeout(operationTimeout, startedAt);
                    replies = redirect.Asking
                        ? await client
                            .ExecutePipelineWithDeadlineAsync(
                                [new ValkeyCommand("ASKING"), item.Command],
                                remaining,
                                cancellationToken
                            )
                            .ConfigureAwait(false)
                        : await client
                            .ExecutePipelineWithDeadlineAsync([item.Command], remaining, cancellationToken)
                            .ConfigureAwait(false);
                }
                else
                {
                    replies = redirect.Asking
                        ? await client
                            .ExecutePipelineAsync([new ValkeyCommand("ASKING"), item.Command], cancellationToken)
                            .ConfigureAwait(false)
                        : await client.ExecutePipelineAsync([item.Command], cancellationToken).ConfigureAwait(false);
                }
                if (redirect.Asking)
                    replies[0].ThrowIfError();
                response = replies[replies.Count - 1];
            }
            catch (Exception failure) when (failure is ValkeyConnectionException or ValkeyProtocolException)
            {
                await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
                throw;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (client.IsDisposed)
                    await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
                throw;
            }
            catch (ObjectDisposedException disposedException)
            {
                await RemoveClientAsync(endpoint, client).ConfigureAwait(false);
                throw new ValkeyConnectionException("The cluster node connection was unavailable.", disposedException);
            }
        }
        return response;
    }

    private static bool IsRedirect(RespValue response) =>
        response.Type is RespType.SimpleError or RespType.BlobError
        && response.ToServerException().ErrorCode is "MOVED" or "ASK";

    private static TimeSpan GetRemainingTimeout(
        TimeSpan timeout,
        long startedAt,
        ValkeyCommandDeliveryStatus deliveryStatus = ValkeyCommandDeliveryStatus.MayHaveBeenSent
    )
    {
        var remaining = timeout - Stopwatch.GetElapsedTime(startedAt);
        return remaining > TimeSpan.Zero ? remaining : throw new ValkeyCommandTimeoutException(timeout, deliveryStatus);
    }

    private ValkeyClientOptions CreateNodeOptions(ClusterEndpoint endpoint) =>
        new()
        {
            Host = endpoint.Host,
            Port = endpoint.Port,
            Username = _nodeTemplate.Username,
            Password = _nodeTemplate.Password,
            ClientName = _nodeTemplate.ClientName,
            Database = _nodeTemplate.Database,
            Protocol = _nodeTemplate.Protocol,
            UseTls = _nodeTemplate.UseTls,
            CertificateValidationCallback = _nodeTemplate.CertificateValidationCallback,
            ConnectTimeout = _nodeTemplate.ConnectTimeout,
            ResponseDrainTimeout = _nodeTemplate.ResponseDrainTimeout,
            MaxPendingRequests = _nodeTemplate.MaxPendingRequests,
            MaxResponseBytes = _nodeTemplate.MaxResponseBytes,
            MaxResponseElements = _nodeTemplate.MaxResponseElements,
            MaxNestingDepth = _nodeTemplate.MaxNestingDepth,
        };

    private static async Task<ClusterEndpoint?[]> ReadTopologyAsync(
        ValkeyClient client,
        ClusterEndpoint source,
        ValkeyClusterOptions options,
        bool useTls,
        CancellationToken cancellationToken,
        bool subscriptionRecovery = false
    )
    {
        RespValue response;
        try
        {
            response = await client
                .ExecuteAsync(new ValkeyCommand("CLUSTER", "SHARDS"), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ValkeyServerException error) when (!subscriptionRecovery || error.ErrorCode == "ERR")
        {
            try
            {
                response = await client
                    .ExecuteAsync(new ValkeyCommand("CLUSTER", "SLOTS"), cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (ValkeyServerException exception)
            {
                if (subscriptionRecovery)
                {
                    throw;
                }
                throw new ValkeyClusterException(
                    "The seed node rejected both CLUSTER SHARDS and CLUSTER SLOTS discovery.",
                    exception
                );
            }

            return ParseSlotsTopology(response, source, options);
        }

        return ParseShardsTopology(response, source, options, useTls);
    }

    private static ClusterEndpoint?[] ParseShardsTopology(
        RespValue response,
        ClusterEndpoint source,
        ValkeyClusterOptions options,
        bool useTls
    )
    {
        try
        {
            var slots = new ClusterEndpoint?[ClusterHashSlot.Count];
            foreach (var shardValue in response.AsArray())
            {
                var shard = ReadMapLike(shardValue, "CLUSTER SHARDS shard");
                var ranges = RequireField(shard, "slots", "CLUSTER SHARDS shard").AsArray();
                if (ranges.Count == 0 || ranges.Count % 2 != 0)
                    throw new ValkeyClusterException("CLUSTER SHARDS returned an invalid slot-range list.");

                var nodes = RequireField(shard, "nodes", "CLUSTER SHARDS shard").AsArray();
                ClusterEndpoint? primary = null;
                foreach (var nodeValue in nodes)
                {
                    var node = ReadMapLike(nodeValue, "CLUSTER SHARDS node");
                    var role = ReadOptionalString(FindField(node, "role"));
                    if (
                        !string.Equals(role, "master", StringComparison.OrdinalIgnoreCase)
                        && !string.Equals(role, "primary", StringComparison.OrdinalIgnoreCase)
                    )
                        continue;

                    var host = FirstUsableEndpoint(
                        ReadOptionalString(FindField(node, "endpoint")),
                        ReadOptionalString(FindField(node, "hostname")),
                        ReadOptionalString(FindField(node, "ip"))
                    );
                    host ??= source.Host;
                    var portField = useTls
                        ? FindField(node, "tls-port") ?? FindField(node, "port")
                        : FindField(node, "port");
                    if (portField is null)
                        throw new ValkeyClusterException("CLUSTER SHARDS returned a primary with no port.");
                    var port = portField.AsInt64();
                    if (port is < 1 or > 65535)
                        throw new ValkeyClusterException($"CLUSTER SHARDS returned the invalid primary port {port}.");
                    primary = CreateAnnouncedEndpoint(options, host, (int)port);
                    break;
                }

                if (primary is null)
                    throw new ValkeyClusterException("CLUSTER SHARDS returned a shard with no primary node.");

                for (var index = 0; index < ranges.Count; index += 2)
                    AssignRange(slots, ReadSlot(ranges[index]), ReadSlot(ranges[index + 1]), primary.Value, "SHARDS");
            }

            EnsureCompleteTopology(slots, "SHARDS");
            return slots;
        }
        catch (InvalidOperationException exception)
        {
            throw new ValkeyClusterException("CLUSTER SHARDS returned an invalid response shape.", exception);
        }
    }

    private static ClusterEndpoint?[] ParseSlotsTopology(
        RespValue response,
        ClusterEndpoint source,
        ValkeyClusterOptions options
    )
    {
        try
        {
            var slots = new ClusterEndpoint?[ClusterHashSlot.Count];
            foreach (var rangeValue in response.AsArray())
            {
                var range = rangeValue.AsArray();
                if (range.Count < 3)
                    throw new ValkeyClusterException("CLUSTER SLOTS returned a range with no primary node.");

                var endpoint = ReadSlotsEndpoint(range[2], source, options);
                AssignRange(slots, ReadSlot(range[0]), ReadSlot(range[1]), endpoint, "SLOTS");
            }

            EnsureCompleteTopology(slots, "SLOTS");
            return slots;
        }
        catch (InvalidOperationException exception)
        {
            throw new ValkeyClusterException("CLUSTER SLOTS returned an invalid response shape.", exception);
        }
    }

    private static int ReadSlot(RespValue value)
    {
        var slot = value.AsInt64();
        return slot is >= 0 and < ClusterHashSlot.Count
            ? (int)slot
            : throw new ValkeyClusterException($"Cluster discovery returned the invalid slot number {slot}.");
    }

    private static ClusterEndpoint ReadSlotsEndpoint(
        RespValue value,
        ClusterEndpoint source,
        ValkeyClusterOptions options
    )
    {
        var parts = value.AsArray();
        if (parts.Count < 2)
            throw new ValkeyClusterException("CLUSTER SLOTS returned incomplete primary networking information.");

        var host = parts[0].IsNull ? null : parts[0].AsString();
        host = string.IsNullOrEmpty(host) ? source.Host : host;
        if (string.IsNullOrWhiteSpace(host) || host == "?")
            throw new ValkeyClusterException("CLUSTER SLOTS returned an unknown primary endpoint.");

        var port = parts[1].AsInt64();
        if (port is < 1 or > 65535)
            throw new ValkeyClusterException($"CLUSTER SLOTS returned the invalid primary port {port}.");
        return CreateAnnouncedEndpoint(options, host, (int)port);
    }

    private static ClusterRedirect ParseRedirect(
        ValkeyServerException exception,
        ClusterEndpoint source,
        ValkeyClusterOptions options
    )
    {
        if (exception.Message.Length > MaxEndpointCharacters)
            throw new ValkeyClusterException("The server returned an oversized cluster redirect.", exception);
        var parts = exception.Message.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (
            parts.Length != 3
            || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var slot)
            || slot is < 0 or >= ClusterHashSlot.Count
        )
            throw new ValkeyClusterException("The server returned a malformed cluster redirect.", exception);

        var separator = parts[2].LastIndexOf(':');
        if (
            separator < 0
            || !int.TryParse(
                parts[2].AsSpan(separator + 1),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var port
            )
            || port is < 1 or > 65535
        )
            throw new ValkeyClusterException("The server returned a malformed cluster redirect endpoint.", exception);

        var host = parts[2][..separator];
        if (host.Length == 0)
            host = source.Host;
        if (string.IsNullOrWhiteSpace(host) || host == "?")
            throw new ValkeyClusterException("The server redirected to an unknown cluster endpoint.", exception);

        return new ClusterRedirect(
            string.Equals(exception.ErrorCode, "ASK", StringComparison.Ordinal),
            slot,
            CreateAnnouncedEndpoint(options, host, port)
        );
    }

    private static void ValidateRedirectSlot(
        ClusterRedirect redirect,
        int expectedSlot,
        ValkeyServerException exception
    )
    {
        if (redirect.Slot != expectedSlot)
            throw new ValkeyClusterException(
                $"The server redirected slot {redirect.Slot}, but the routing key hashes to slot {expectedSlot}.",
                exception
            );
    }

    private static ClusterEndpoint CreateAnnouncedEndpoint(ValkeyClusterOptions options, string host, int port)
    {
        if (options.EndpointMapper is { } mapper)
        {
            var mapped = mapper(new ValkeyClusterEndpoint(host, port));
            host = mapped.Host;
            port = mapped.Port;
        }
        return ClusterEndpoint.Create(host, port);
    }

    private static IReadOnlyList<KeyValuePair<RespValue, RespValue>> ReadMapLike(RespValue value, string context)
    {
        if (value.Type == RespType.Map)
            return value.AsMap();

        var items = value.AsArray();
        if (items.Count % 2 != 0)
            throw new ValkeyClusterException($"{context} contains an odd number of map elements.");
        var pairs = new KeyValuePair<RespValue, RespValue>[items.Count / 2];
        for (var index = 0; index < items.Count; index += 2)
            pairs[index / 2] = new(items[index], items[index + 1]);
        return pairs;
    }

    private static RespValue RequireField(
        IReadOnlyList<KeyValuePair<RespValue, RespValue>> fields,
        string name,
        string context
    ) => FindField(fields, name) ?? throw new ValkeyClusterException($"{context} has no '{name}' field.");

    private static RespValue? FindField(IReadOnlyList<KeyValuePair<RespValue, RespValue>> fields, string name)
    {
        foreach (var field in fields)
        {
            if (string.Equals(field.Key.AsString(), name, StringComparison.OrdinalIgnoreCase))
                return field.Value;
        }
        return null;
    }

    private static string? ReadOptionalString(RespValue? value)
    {
        if (value is null || value.IsNull)
            return null;
        if (value.AsBytes().Length > MaxEndpointCharacters)
            throw new ValkeyClusterException("CLUSTER SHARDS returned an oversized text field.");
        return value.AsString();
    }

    private static string? FirstUsableEndpoint(params string?[] candidates)
    {
        foreach (var candidate in candidates)
        {
            if (!string.IsNullOrWhiteSpace(candidate) && candidate != "?")
                return candidate;
        }
        return null;
    }

    private static void AssignRange(
        ClusterEndpoint?[] slots,
        int start,
        int end,
        ClusterEndpoint endpoint,
        string command
    )
    {
        if (end < start)
            throw new ValkeyClusterException($"CLUSTER {command} returned an inverted slot range.");
        for (var slot = start; slot <= end; slot++)
        {
            if (slots[slot] is { } existing && existing != endpoint)
                throw new ValkeyClusterException($"CLUSTER {command} assigned slot {slot} to multiple primaries.");
            slots[slot] = endpoint;
        }
    }

    private static void EnsureCompleteTopology(ClusterEndpoint?[] slots, string command)
    {
        if (Array.FindIndex(slots, static endpoint => endpoint is null) is var missing and >= 0)
            throw new ValkeyClusterException($"CLUSTER {command} did not assign primary ownership for slot {missing}.");
    }

    private void UpdateSlot(int slot, ClusterEndpoint endpoint)
    {
        while (true)
        {
            var current = Volatile.Read(ref _slots);
            var updated = (ClusterEndpoint?[])current.Clone();
            updated[slot] = endpoint;
            if (ReferenceEquals(Interlocked.CompareExchange(ref _slots, updated, current), current))
                return;
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
