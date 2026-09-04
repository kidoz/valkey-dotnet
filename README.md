# ValkeyDotNet

[![Language](https://img.shields.io/badge/language-C%23-512BD4)](https://learn.microsoft.com/dotnet/csharp/)
[![.NET SDK](https://img.shields.io/badge/.NET%20SDK-10.0.400-512BD4)](https://github.com/kidoz/valkey-dotnet/blob/main/global.json)
[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/kidoz/valkey-dotnet/blob/main/LICENSE)

**ValkeyDotNet** — a .NET client library for [Valkey](https://valkey.io/), written entirely in C#
with RESP2, RESP3, TLS, pipelining, and cluster routing.

> ValkeyDotNet is an independent open-source project. It is not an official Valkey project and does
> not imply Valkey endorsement.

## Packages

| Package | Description |
|---|---|
| `ValkeyDotNet` | Managed Valkey client with its own RESP2/RESP3 protocol implementation. |

## Status

ValkeyDotNet 1.0.0 is the first stable release for standalone Valkey and primary-routed Valkey
Cluster workloads. Supported features are implemented and tested; unsupported behavior fails
explicitly. See the [changelog](CHANGELOG.md) for release notes.

ValkeyDotNet implements **its own** managed RESP protocol. The shipping library has **zero** runtime
package dependencies — no Rust core, no native library, no code generation, no third-party NuGet
package. It uses only the .NET base class library.

Implemented:

- RESP2 and RESP3, including maps, sets, attributes, push frames, verbatim strings, big numbers, and
  streamed values
- TCP and TLS, ACL authentication, client naming, and logical database selection
- Binary-safe generic commands, async cancellation, bounded pending work and response parsing, and
  command pipelining
- Convenience methods for common string, counter, and hash operations
- Concurrent callers multiplexed on one connection with FIFO reply matching
- `CLUSTER SHARDS` discovery with `CLUSTER SLOTS` fallback, CRC16/hash-tag routing, endpoint mapping,
  bounded `MOVED`/`ASK` handling, and slot-grouped cluster pipelines
- Configurable bounded connections per cluster node for head-of-line isolation

Deliberately not implemented: replica reads, cluster-wide scans, Sentinel discovery, general-purpose
pooling, automatic reconnect/retry, and subscription-mode Pub/Sub. See
[the connection model](docs/explanation/connection-model.md) for why, and
[why managed-only](docs/explanation/why-managed-only.md) for the positioning.

## Requirements

- .NET 10 SDK (pinned via `global.json`)
- C# 14
- A reachable Valkey server (default port 6379). Verified against Valkey 9.1, 8.1, and 7.2 —
  see [compatibility](docs/reference/valkey-compatibility.md).

## Installation

```xml
<PackageReference Include="ValkeyDotNet" Version="1.0.0" />
```

## Documentation

Documentation follows [Diátaxis](https://diataxis.fr/). Start at [`docs/`](docs/README.md).

- Tutorial: [Getting started](docs/tutorials/getting-started.md)
- How-to: [Connect over TLS](docs/how-to/connect-over-tls.md) ·
  [Use a cluster](docs/how-to/use-cluster.md) ·
  [Pipeline commands](docs/how-to/pipeline-commands.md) ·
  [Send any command](docs/how-to/send-any-command.md) ·
  [Handle errors](docs/how-to/handle-errors.md) ·
  [Run live integration tests](docs/how-to/run-live-integration-tests.md) ·
  [Publish a release](docs/how-to/publish-a-release.md)
- Reference: [`ValkeyClient`](docs/reference/valkey-client.md) ·
  [`ValkeyClusterClient`](docs/reference/valkey-cluster-client.md) ·
  [Client options](docs/reference/client-options.md) ·
  [RESP values](docs/reference/resp-values.md) ·
  [Exceptions](docs/reference/exceptions.md) ·
  [Valkey compatibility](docs/reference/valkey-compatibility.md) ·
  [Performance baseline](docs/reference/performance-baseline.md)
- Explanation: [Why managed-only](docs/explanation/why-managed-only.md) ·
  [Connection model](docs/explanation/connection-model.md)

## Usage

Connect, then use the typed convenience methods:

```csharp
using ValkeyDotNet;

await using var valkey = await ValkeyClient.ConnectAsync(
    new ValkeyClientOptions
    {
        Host = "localhost",
        Port = 6379,
        Protocol = ValkeyProtocol.Resp3,
        ClientName = "sample-app",
    }
);

await valkey.SetStringAsync("greeting", "hello", TimeSpan.FromMinutes(5));
Console.WriteLine(await valkey.GetStringAsync("greeting"));
```

Every other Valkey command goes through the generic API, which returns the reply losslessly:

```csharp
var raw = await valkey.ExecuteAsync(new ValkeyCommand("HGETALL", "user:42"));
foreach (var pair in raw.AsMap())
    Console.WriteLine($"{pair.Key.AsString()} = {pair.Value.AsString()}");
```

The generic command API is intentional: new server commands work immediately, while typed helpers can
grow without changing the transport or parser.

### Cluster routing

```csharp
await using var cluster = await ValkeyClusterClient.ConnectAsync(
    new ValkeyClusterOptions
    {
        SeedNodes =
        [
            new ValkeyClientOptions { Host = "valkey-a.example.com", Port = 6379, UseTls = true },
            new ValkeyClientOptions { Host = "valkey-b.example.com", Port = 6379, UseTls = true },
        ],
    }
);

await cluster.SetStringAsync("{user:42}:status", "online");
```

See [Use a Valkey Cluster](docs/how-to/use-cluster.md) for generic commands and hash tags.

### Pipelining

Pipelining sends a contiguous batch without waiting for individual replies:

```csharp
var replies = await valkey.ExecutePipelineAsync(
    [
        new ValkeyCommand("SET", "a", "1"),
        new ValkeyCommand("INCR", "a"),
        new ValkeyCommand("GET", "a"),
    ]
);

foreach (var reply in replies)
    reply.ThrowIfError();
```

Pipeline errors are **returned in place rather than thrown**, so the client can drain every reply and
preserve protocol synchronization. Check each reply; an unchecked result hides failures.

### Security

- TLS is opt-in via `UseTls`. Certificate validation uses the platform default unless a callback is
  explicitly supplied — and a supplied callback **replaces** platform validation entirely.
- Response size (`MaxResponseBytes`, 64 MiB), decoded value count (`MaxResponseElements`, 1 Mi), and
  nesting depth (`MaxNestingDepth`, 128) bound the parser so an unexpectedly large or malicious frame
  cannot exhaust the process. A declared aggregate cardinality is checked against both the element
  and byte budgets *before* anything is allocated for it, so an impossible header is rejected on
  sight rather than acted on.
- Commands that would redefine the connection — the subscribe family, `MONITOR`, `RESET`, `HELLO`,
  `CLIENT REPLY` — are rejected before they are written, leaving the connection usable.
- Cancellation during active I/O invalidates the connection, because the stream may sit between
  protocol frames. Reusing it would misattribute replies across callers.
- Passwords are sent only in the Valkey `HELLO ... AUTH` handshake. Use TLS on untrusted networks.
- `RespValue.ToString()` is bounded and escaped to printable ASCII, so a reply cannot flood or
  corrupt a log line.

## Build and test

Common tasks are exposed via [`just`](https://github.com/casey/just) (see `justfile`):

```bash
just            # list recipes
just ci         # format check, build, test
just build
just test
just pack
```

The equivalent raw commands:

```bash
dotnet tool restore
dotnet csharpier check .
dotnet build ValkeyDotNet.slnx
dotnet run --project tests/ValkeyDotNet.Tests
dotnet pack src/ValkeyDotNet -c Release -o artifacts
```

The solution targets .NET 10 with `TreatWarningsAsErrors` and `AnalysisLevel=latest-all`; every
suppressed analyzer rule carries a written justification in the owning `.csproj`. CSharpier owns all
formatting, including project files.

Live integration tests need a disposable server and are skipped otherwise. `dev/docker-compose.yml`
provides one container per maintained Valkey line:

```bash
just valkey-up            # start 9.x, 8.x, 7.x, and an ACL server
just test-live            # defaults to 127.0.0.1:6379
just test-matrix          # run the suite against every line
just test-cluster         # initialize and test a three-primary cluster
just valkey-down
just cluster-down
```

Run the BenchmarkDotNet performance and allocation suite in Release:

```bash
just bench
```

See [the performance baseline](docs/reference/performance-baseline.md) for current timing and
allocation numbers.

## License

Licensed under the [MIT License](https://github.com/kidoz/valkey-dotnet/blob/main/LICENSE).
Copyright (c) 2026 Aleksandr Pavlov.
