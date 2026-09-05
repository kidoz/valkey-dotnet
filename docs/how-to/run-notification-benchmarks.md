# Run publish and invalidation benchmarks

From the repository root, with .NET 10 and local Docker available:

```bash
just ci
just test-notification-workloads
just bench-notifications
```

The opt-in correctness suite covers every profile combination on four fresh owned servers, with
three warm-up and sixteen checked operations per caller. It has no performance thresholds.
The Release-only benchmark creates another disposable server. Neither command accepts an existing
endpoint. The CLI equivalent is
`dotnet run -c Release --project benchmarks/ValkeyDotNet.Benchmarks -- --notifications`.
The existing `--roundtrips` and BenchmarkDotNet codec suites are unchanged.

## Measurement contract

Eight rows use one/eight callers, 64 warm-up operations per caller and 512 measured operations per
caller. All callers share one physical writer connection. One separate physical connection consumes
notifications; the benchmark also keeps an idle observer connection for server metadata/cleanup.

| Operation | Protocol | One measured operation |
|---|---|---|
| Publish | RESP2 and RESP3 | PUBLISH one 1 KiB binary payload to one binary channel with exactly one subscriber. |
| TrackedInvalidation | RESP3 | SET PX one unique, previously read-tracked binary key to a 1 KiB value; observe that key's invalidation. |
| BroadcastInvalidation | RESP3 | The same mutation, with CLIENT TRACKING BCAST and one matching binary PREFIX, without pre-reading. |

Keys are seeded with ten-minute TTLs before tracking connects. Default tracking registers all keys
through GET before warm-up/timing. Each key is mutated once, so invalidations correlate unambiguously
even when a server batches multiple keys into one notification. This does **not** measure a hot-key
cache's repeated GET/re-registration or local eviction costs. Default read registration and BCAST
prefix behavior follow the [Valkey tracking contract](https://valkey.io/topics/client-side-caching/).

Payloads, commands, keys, correlation arrays and the measured workers' start gate are prepared
outside timing. Warm-up includes complete notification drain. Each caller waits only for its command
acknowledgment before sending again; notification consumption runs independently, with a bounded
8192-item queue. Reconnect is disabled for Pub/Sub; tracking has one connection attempt.

Two rates are reported: acknowledgments/second until all command workers finish, and delivered
operations/second until both acknowledgments and notification observations finish. For invalidation,
an operation is a **key mutation**, not an invalidation frame. Publish is one-to-one delivery, not a
fan-out benchmark. Each latency starts immediately before ExecuteAsync and ends at either reply
observation or async-enumerable delivery observation. Delivery may precede acknowledgment.

Reports include nearest-rank p50/p95/p99 for both latency series, their independently sorted samples
in microseconds, and process-wide managed allocated bytes per operation through delivery drain.
Allocation includes client and validation/correlation harness work, excludes preparation and final
value checks, and is not retained heap, peak memory, native allocations or server memory.
Receiver timestamps precede identity/payload validation, but that validation consumes CPU and can
affect following deliveries and throughput. Reply validation is inside the worker interval.

Missing deliveries time out; unknown, duplicate, malformed or pre-invocation identities, unexpected
tracking resets, non-monotonic versions, queue overflow, subscriber loss/recovery or incorrect command
acknowledgments fail the run. Final binary values and exact key/subscription cleanup are checked
outside the interval. Receiver tasks are canceled and joined on all execution paths.

## Safety and evidence

The suite reuses the [owned-server safety contract](run-roundtrip-benchmarks.md#safety-and-evidence):
local Docker only, random loopback port, `valkey/valkey:9.1`, one CPU, 128 MiB, 64 PIDs, read-only root,
16 MiB tmpfs, persistence/DEBUG disabled, no TLS, exact nonce/ID ownership checks, and no network
creation/removal. Each case has a 45-second deadline; the runner has five minutes. Independent owned
container cleanup remains bounded to 60 seconds, including on failure. Existing services are untouched.

Successful reports appear as `artifacts/performance/notifications-valkey-dotnet-bench-*.json` only
after verified container removal. Retain all runs and record CPU model plus `dotnet --info`.
The manual **Notification benchmarks** workflow runs correctness gates and uploads JSON/TRX and
runner metadata; it is not automatically dispatched and has no timing thresholds.

These are bounded, closed-loop observations, not BenchmarkDotNet statistical jobs or open-loop load
tests with coordinated-omission correction. Network/Docker/server/queue/scheduling time is included.
Short warm-up, fixed order, tiered JIT, GC, shared hosts and limited samples can affect tails. Repeat
on quiet target hardware. Do not interpret this as server-internal latency or guaranteed cache
freshness. Fan-out, slow consumers/overflow, cluster/sharded Pub/Sub, TLS, other versions/payloads,
recovery and prolonged-resource tests require separate profiles.

See [recorded notification observations](../reference/notification-performance.md).
