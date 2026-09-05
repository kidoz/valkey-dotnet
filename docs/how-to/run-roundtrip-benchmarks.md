# Run real-server cache and lock benchmarks

From the repository root, with .NET and local Docker available:

```bash
just ci
just test-roundtrip-workloads
just bench-roundtrips
```

The correctness suite creates two disposable standalone servers, one per RESP protocol. The
benchmark creates another server and executes a fixed Release-only profile. No existing endpoint
is accepted, and ordinary `just bench` still runs only the BenchmarkDotNet codec suites.

## Read the profile

The profile has 24 rows: six operations × RESP2/RESP3 × one/eight concurrent callers. Each row
uses one physical multiplexed client socket, 64 warm-up operations per caller, and 512 measured
operations per caller. Every caller awaits its operation before issuing another. Values are 1 KiB
binary payloads; owner tokens are 16 random bytes. TTL arguments are 120000 ms.

| Operation | One measured operation |
|---|---|
| Get | GET of a seeded 1 KiB value. |
| SetPx | SET of a 1 KiB value with PX. |
| ContendedSetNxPx | SET NX PX against an already-held key; expected null rejection. |
| AcquireReleaseCycle | Successful SET NX PX followed by an owner-checked release script; **two sequential round trips**. |
| ExtendLease | Owner-checked PEXPIRE script against a held lease. |
| Pipeline100Get | One pipeline of 100 distinct seeded keys, returning 100 KiB of values. |

Workers have distinct keys. Concurrency measures shared-socket/FIFO contention, not a race for one
lock, connection pooling, or cluster routing. Script caches are warmed before measurement;
NOSCRIPT cold-load costs are excluded. Commands, keys, payloads, tokens, and sample storage are
prepared outside the interval. Pipeline error inspection remains inside the measured operation.
Full binary/result validation runs before and after the interval, outside the timing/allocation
window; the separate correctness suite validates every operation for three concurrent rounds.

The report includes operation throughput, mean and nearest-rank p50/p95/p99 latency in microseconds,
managed allocated bytes per operation, command count per operation, and raw per-caller latency
samples. Pipeline throughput is **pipelines/second**, not keys/second. Cycle throughput is
**acquire/release cycles/second**, not individual commands/second.

## Interpret measurements carefully

This dedicated bounded sampler is not a BenchmarkDotNet statistical job. Per-operation timestamps
include async scheduling, FIFO waiting, socket/Docker transport, server processing, and response
decoding. Throughput uses total elapsed time until all workers finish. This is a closed-loop load
model without coordinated-omission correction; its tails do not predict overloaded open-loop traffic.

Allocation uses process-wide differences from
[GC.GetTotalAllocatedBytes](https://learn.microsoft.com/en-us/dotnet/api/system.gc.gettotalallocatedbytes?view=net-10.0)
because continuations and the reader may run on different threads. Counters are sampled outside
the timed window. Figures include managed client/harness allocations during the interval; they
exclude native and server allocations and are **not retained heap, peak memory, or leak evidence**.
The sampler does not force GC between rows, so collection and tiered-JIT history can affect results.

Repeat runs on an otherwise quiet machine and retain every result, including outliers. The short
warm-up, fixed case order, Docker VM, shared host services, and limited sample counts prevent these
numbers from being portable SLOs or release thresholds. Do not compare them directly with codec
microbenchmarks. Cluster/TLS, other payloads/concurrencies/versions, publishing/invalidation latency,
individually timed acquisition/release, and prolonged resource behavior need separate profiles.

## Safety and evidence

Each server uses `valkey/valkey:9.1`, a random loopback port, one CPU, 128 MiB memory, 64 PIDs,
read-only root, and a 16 MiB data tmpfs. Persistence and DEBUG are disabled. Only a local unix/npipe
Docker endpoint is accepted; effective context/host selection is frozen for subsequent commands.
The runner verifies exact container ID, nonce label, name, image name, limits, mounts, and port
binding before lifecycle actions. It uses Docker's bridge network; no network is created or removed.

The benchmark has a five-minute overall deadline, readiness has 30 seconds, Docker calls have
60 seconds, and independent cleanup has 60 seconds. Normal teardown deletes only exact owned keys
and verifies DBSIZE=0 after each row. Finally disposal verifies and force-removes only the owned
container and its anonymous volumes, including after failure. A timed-out create is reconciled by
the exact nonce-labelled name. Docker/ownership failures may require inspecting the printed project
before manual cleanup; never prune global resources. Cached images remain.

Successful runs write uniquely named JSON files under `artifacts/performance/`, only after verified
container cleanup. Reports contain runtime/OS/architecture, processor count, server INFO and image
ID, topology/TLS/payload/concurrency settings, allocation scope, and raw samples. Record CPU model
and `dotnet --info` alongside local runs. Failed runs do not publish a completed report.

The manual **Real-server round-trip benchmarks** workflow runs correctness gates first and uploads
JSON/TRX plus CPU/SDK environment files. It has no performance threshold and is not dispatched
automatically. See the [performance reference](../reference/performance-baseline.md) for recorded evidence.

For object/stack attribution, use the separate [allocation profiling guide](profile-allocations.md).
