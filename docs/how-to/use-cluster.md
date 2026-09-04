# Use a Valkey Cluster

Configure one or more seed nodes; multiple seeds improve initial discovery availability. Every seed
should use the same authentication and TLS policy because the successful seed's connection settings
are reused for discovered primaries.

```csharp
await using var cluster = await ValkeyClusterClient.ConnectAsync(
    new ValkeyClusterOptions
    {
        SeedNodes =
        [
            new ValkeyClientOptions { Host = "valkey-a.example.com", Port = 6379, UseTls = true },
            new ValkeyClientOptions { Host = "valkey-b.example.com", Port = 6379, UseTls = true },
        ],
        MaxRedirects = 5,
        MaxNodeConnections = 256,
        ConnectionsPerNode = 2,
    }
);

await cluster.SetStringAsync("user:42", "online", TimeSpan.FromMinutes(5));
var value = await cluster.GetStringAsync("user:42");
```

One connection per node already multiplexes concurrent commands. Increase `ConnectionsPerNode` when
separate workloads—especially blocking commands—need isolation from FIFO response head-of-line
blocking; it is not required for ordinary request concurrency.

## Send a generic command

Supply the routing key separately. It tells the client which slot and primary should receive the
command.

```csharp
var key = (ValkeyArgument)"user:42";
var reply = await cluster.ExecuteAsync(key, new ValkeyCommand("HGETALL", key));
```

For a multi-key operation, use a hash tag so every key belongs to the same slot:

```csharp
var source = (ValkeyArgument)"{account:42}:pending";
var destination = (ValkeyArgument)"{account:42}:complete";

var moved = await cluster.ExecuteAsync(
    source,
    new ValkeyCommand("LMOVE", source, destination, "LEFT", "RIGHT")
);
```

The client follows bounded `MOVED` and `ASK` redirects automatically. It does not retry transport
failures, because a failed write may already have been applied.

## Pipeline across primaries

Pair every command with its routing key. Replies retain this input order even when node groups run
concurrently:

```csharp
var replies = await cluster.ExecutePipelineAsync(
    [
        new ValkeyClusterCommand("user:1", new ValkeyCommand("GET", "user:1")),
        new ValkeyClusterCommand("user:2", new ValkeyCommand("GET", "user:2")),
    ]
);

foreach (var reply in replies)
    reply.ThrowIfError();
```

## Translate announced endpoints

Clusters behind containers, private networks, port forwarding, or TLS gateways may announce an
endpoint the application cannot use directly. Translate only the host and port; authentication,
TLS validation, parser bounds, and timeouts still come from the successful seed:

```csharp
var options = new ValkeyClusterOptions
{
    SeedNodes = [new ValkeyClientOptions { Host = "cluster.example.com", UseTls = true }],
    EndpointMapper = announced => new ValkeyClusterEndpoint(
        announced.Host.Replace(".internal", ".example.com", StringComparison.OrdinalIgnoreCase),
        announced.Port),
};
```

Treat the mapper as part of the cluster trust boundary. It must not translate server-controlled
announcements to hosts outside the intended cluster, because discovered connections reuse the
successful seed's credentials and TLS policy.

All announced primary endpoints must be reachable from the application. When TLS or ACL credentials
are used, every discovered primary must support the same policy as the successful seed.
