# Run the live integration tests

The suite runs without a server by default; live tests report `[SKIP]`. To exercise them, give the
runner a disposable Valkey server.

## Start the test servers

```bash
just valkey-up
```

This brings up `docker-compose.yml` and waits until every container is healthy:

| Service | Line | Port |
|---|---|---|
| `valkey-9` | 9.x | 6379 |
| `valkey-8` | 8.x | 6380 |
| `valkey-7` | 7.x | 6381 |
| `valkey-acl` | 9.x with ACL users | 6382 |

Confirm what is actually running:

```bash
just valkey-versions
```

## Run against one server

```bash
just test-live                      # 127.0.0.1:6379, the 9.x server
just test-live 127.0.0.1:6380       # the 8.x server
```

The raw equivalent:

```bash
VALKEYDOTNET_ENDPOINT=127.0.0.1:6379 dotnet run --project tests/ValkeyDotNet.Tests
```

## Run against every maintained line

```bash
just test-matrix
```

Runs the suite once per line and fails if any run fails. Tests for features the server does not
implement skip with a reason rather than failing — on 8.x and 7.x you will see
`HashFieldExpirationWorksWhereTheServerSupportsIt [SKIP]`.

## Run against a three-primary cluster

```bash
just test-cluster
```

This uses `docker-compose.cluster.yml` to start and initialize three Valkey 9.1 primaries on ports
16379–16381. The nodes announce their container hostnames for node-to-node communication; the test's
`EndpointMapper` translates those names to host-published ports. It exercises `CLUSTER SHARDS`,
topology refresh, three slot ranges, and a pipeline distributed across the primaries.

Stop and remove it separately:

```bash
just cluster-down
```

For an externally managed disposable cluster, set comma-separated seeds and optionally a host used
to replace every announced hostname:

```bash
VALKEYDOTNET_CLUSTER_ENDPOINTS=127.0.0.1:16379,127.0.0.1:16380 \
VALKEYDOTNET_CLUSTER_MAPPED_HOST=127.0.0.1 \
dotnet run --project tests/ValkeyDotNet.Tests
```

## Tear down

```bash
just valkey-down
just cluster-down
```

Removes the containers and their volumes.

## Use a server you started yourself

Any reachable endpoint works:

```bash
docker run --rm -d --name valkey-scratch -p 6390:6379 valkey/valkey:9.1
just test-live 127.0.0.1:6390
docker stop valkey-scratch
```

**Never point the suite at a shared or production server.** The tests write and delete keys.

## Add a live test

Gate it so a checkout with no server still passes:

```csharp
var endpoint = Environment.GetEnvironmentVariable("VALKEYDOTNET_ENDPOINT");
if (string.IsNullOrWhiteSpace(endpoint))
    Assert.Skip("Set VALKEYDOTNET_ENDPOINT to run the live Valkey integration test.");
```

Give every key a unique prefix so parallel runs cannot collide, and delete what you create in a
`finally`:

```csharp
var key = "valkey-dotnet:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
```

If the test depends on a command that only some versions implement, probe rather than assuming:

```csharp
var info = await client.ExecuteAsync(new ValkeyCommand("COMMAND", "INFO", "HEXPIRE"), cancellationToken);
if (info.AsArray() is [{ IsNull: true }, ..])
    Assert.Skip("This server does not implement hash field expiration.");
```

## Related

- [Valkey compatibility](../reference/valkey-compatibility.md) — verified versions and behaviour
  differences.
- [Performance baseline](../reference/performance-baseline.md) — the benchmark suite, which needs no
  server.
