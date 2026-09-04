# Valkey compatibility

## Verified server versions

Every maintained Valkey release line is exercised by the live compatibility suite
(`tests/ValkeyDotNet.IntegrationTests/ValkeyCompatibilityTests.cs`).

| Line | Version verified | Test port | Result |
|---|---|---|---|
| 9.x | 9.1.2 | 6379 | Full suite passes, no skips |
| 8.x | 8.1.10 | 6380 | Passes; hash field expiration skipped (not implemented by the server) |
| 7.x | 7.2.14 | 6381 | Passes; hash field expiration skipped (not implemented by the server) |

Last verified 4 September 2026. Reproduce with `just valkey-up && just test-matrix`.

The disposable three-primary Valkey 9.1 cluster suite also passes on 4 September 2026, including
`CLUSTER SHARDS` discovery, endpoint mapping, routing across all three slot ranges, distributed
pipelining, topology refresh, and cleanup. Reproduce with `just test-cluster`.

## Command coverage

The client does not maintain a command allow-list. `ExecuteAsync` accepts any command name and any
binary-safe arguments, and `RespValue` represents any RESP2 or RESP3 reply, so **every current and
future Valkey command is reachable** without a library change. The typed methods on
[`ValkeyClient`](valkey-client.md) are ergonomics over that surface, not the supported set.

Practical consequence: commands added in Valkey 9 — `HEXPIRE`, `HPEXPIRE`, `HTTL`, `HPTTL`,
`HPERSIST`, `HEXPIRETIME`, `HPEXPIRETIME`, `HGETEX` — work today through `ExecuteAsync`:

```csharp
var set = await valkey.ExecuteAsync(
    new ValkeyCommand("HEXPIRE", "user:42", 100, "FIELDS", 1, "session")
);
Console.WriteLine(set.AsArray()[0].AsInt64()); // 1
```

## Version-dependent behaviour

These differences are server-side. The client surfaces them faithfully rather than papering over
them.

| Behaviour | 7.x | 8.x | 9.x | Client impact |
|---|---|---|---|---|
| Hash field expiration (`HEXPIRE` family) | absent | absent | present | Probe with `COMMAND INFO` before use. |
| `COMMAND INFO` subcommands in RESP3 | `Set` | `Set` | `Array` for commands without subcommands | None — `AsArray()` accepts `Array`, `Set`, and `Push`. |
| RESP protocol CRLF validation | lenient | lenient | strict | None — the writer always emits exact `\r\n` terminators. |
| `HGETEX` key permission | n/a | n/a | requires write | An ACL user with read-only rights now gets `NOPERM`. |

### Detecting a capability

Prefer probing the command over comparing versions — it survives backports and vendor builds:

```csharp
var info = await valkey.ExecuteAsync(new ValkeyCommand("COMMAND", "INFO", "HEXPIRE"));
bool supported = info.AsArray() is [{ IsNull: false }, ..];
```

To read the version instead:

```csharp
var server = await valkey.ExecuteAsync(new ValkeyCommand("INFO", "server"));
// parse the valkey_version: line
```

## Protocol coverage

| Feature | Supported |
|---|---|
| RESP2 | yes |
| RESP3 (`HELLO 3`) | yes, default |
| Maps, sets, attributes, push frames | yes |
| Verbatim strings, big numbers, doubles, booleans | yes |
| Binary-safe keys and values, including NUL, CR, and LF | yes |
| Pipelining | yes |
| Concurrent command multiplexing on one socket | yes; FIFO replies with one reader |
| Cluster primary routing and slot-grouped pipelines | yes; `CLUSTER SHARDS` with `CLUSTER SLOTS` fallback |
| TLS | yes |
| ACL authentication (`HELLO ... AUTH`) | yes |
| Client naming (`SETNAME`), `SELECT` | yes |

On RESP2 the server returns a flat array where RESP3 returns a map. `HGETALL` is the common case; see
[RESP values](resp-values.md).

## Not supported

These are client-side gaps, not server-version issues, and they apply to every Valkey version:

| Area | State |
|---|---|
| Sentinel discovery | absent |
| General-purpose connection pooling | absent; the cluster client has bounded per-node connections |
| Cluster replica reads and cluster-wide scans | absent |
| Subscription mode (`SUBSCRIBE`, `PSUBSCRIBE`, `MONITOR`) | rejected; it needs a dedicated subscriber state machine |
| Blocking commands (`BLPOP`, `XREAD BLOCK`) | reachable, but delay all later replies on the same connection |

See [the connection model](../explanation/connection-model.md) for why.

The unreleased `ValkeyConnectionOwner` provides standalone connection replacement with bounded
acquisition and explicit retry opt-in. The physical `ValkeyClient` remains terminal after failure.
See the [owner reference](connection-owner.md).

## Running the compatibility suite

```bash
just valkey-up        # start 9.x, 8.x, 7.x, and an ACL server
just test-matrix      # run the suite against each line
just valkey-versions  # confirm what is actually running
just valkey-down      # tear down
just test-cluster     # initialize and test a disposable three-primary 9.x cluster
just cluster-down     # tear down the cluster
```

Servers are defined in `dev/docker-compose.yml` with in-memory persistence disabled. They are disposable
test targets; never point the suite at a server holding data you care about.
