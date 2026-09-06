# Real-server round-trip performance

Initial local reference observations from three complete runs on 6 September 2026 (Europe/Moscow;
JSON timestamps are UTC on 5 September). These are short, fixed-profile measurements, not production
SLOs, an optimization comparison, or release thresholds. The [run guide](../how-to/run-roundtrip-benchmarks.md)
defines the workload and measurement contract.

## Environment and scope

| Setting | Observed/configured value |
|---|---|
| Client hardware | Apple M4 Max, Arm64, 16 logical processors |
| Client software | macOS 26.6.2, .NET SDK 10.0.400, runtime 10.0.11, Release |
| Server | Valkey 9.1.2, Linux 7.0.12-linuxkit aarch64, Docker loopback |
| Server limits | One CPU, 128 MiB memory, 64 PIDs, 16 MiB data tmpfs |
| Topology / transport | Standalone, one multiplexed client socket, no TLS |
| Profile | RESP2/RESP3, concurrency 1/8, six workloads, 24 rows per run |
| Payload / owner | 1024-byte cache values, 16-byte random owner tokens |
| Sampling | 64 warm-up and 512 measured operations per worker per row |
| Allocation | Process-wide managed allocation delta, including client/harness; no forced GC |

Image ID: `sha256:475ee65cc75c327407458f5096cdd36954b3de3fc83f4c8ac31a4a8edecbf49e`.
Pre-existing host services remained running; the host was not a dedicated benchmark machine.

## RESP3, eight callers

Ranges below are the minimum and maximum of each metric across the three runs, not confidence
intervals or pooled percentiles. Every row has 4096 measured operations per run. All 72 rows,
including RESP2 and single-caller results, remain in the raw reports.

| Operation unit | Units/s | p50 µs | p99 µs | Managed B/unit |
|---|---:|---:|---:|---:|
| GET | 27,147–29,838 | 249–269 | 714–798 | 2,495–2,500 |
| SET PX | 28,781–30,885 | 241–259 | 525–730 | 2,479–2,481 |
| Held-key SET NX rejection | 32,334–33,724 | 225–231 | 382–413 | 1,444–1,447 |
| Acquire + owner-checked release cycle | 15,616–16,109 | 472–489 | 811–956 | 3,446–3,454 |
| Owner-checked extension | 29,792–31,442 | 239–252 | 440–455 | 2,138–2,140 |
| 100-key GET pipeline | 2,073–2,183 | 3,460–3,586 | 5,966–7,880 | 152,673–152,682 |

Each acquire/release unit contains two sequential round trips. Each pipeline unit returns 100 KiB
of values; its allocation is for the entire batch, not one key. Workers use distinct keys, so the
eight-caller profile exercises FIFO/socket contention rather than competing ownership of one lock.

The initial single-caller GET throughput varied from 2,828–2,934 ops/s in RESP2 and 4,028–5,228
ops/s in RESP3. The protocols always ran in that order with short warm-up; this is **not evidence
that RESP3 is inherently faster**. Process warm-up, scheduling, GC history, and host load confound
that comparison. No shipping code changed between these observations.

## Evidence and limitations

Local report filenames under `artifacts/performance/`:

- `roundtrips-valkey-dotnet-bench-1eaa42062c13444cbc793672ebfa25c0.json`
- `roundtrips-valkey-dotnet-bench-eee6c7807bac44de9b7df2dcc329859a.json`
- `roundtrips-valkey-dotnet-bench-1dc24b061c9a480e9ec1d4c24b6c943c.json`

All three reports contain 24 rows, correctly sized raw sample arrays, and verified cleanup.
The separate RESP2/RESP3 correctness suite passed all six workloads with eight callers across
three rounds; the final run took 1.687 seconds (`artifacts/performance/roundtrip-workloads.trx`).
All 442 unit tests and 192 server-free harness checks passed in Debug/Release. Seven owned
containers across two correctness runs and three benchmark runs were removed; no networks were
created or removed. Existing Docker resources remained. The manual workflow was added, not dispatched.

These allocations are not retained memory or leak evidence. Percentiles are closed-loop request
observations without coordinated-omission correction. The runner does not provide BenchmarkDotNet
confidence/error estimates or a statistically stable regression threshold. Separate acquisition/
release timings are added below; publishing/invalidation delivery has its own
[profile](notification-performance.md). Cold scripts, cluster/TLS, other versions/payloads,
same-key contention and long-running resource behavior remain outside this profile.

The measurements establish an end-to-end baseline for further allocation profiling; they do not
identify which objects dominate allocation or establish that any particular optimization is safe.
Subsequent [allocation profiling](allocation-profile.md) adds stack attribution and an isolated
codec control without changing this baseline or the shipping library.

## Isolated acquisition and release

Three additional complete runs on 6 September 2026 (5 September UTC) used profile version 2:
32 rows, adding `AcquireLease` and `ReleaseLease` in both protocols at one/eight callers. The original
six workload definitions and their historical tables above are retained. The client/runtime/server/
image/limits match the environment above; existing application and Valkey test services stayed up.
These are new workload observations, not a before/after optimization comparison.

Each worker has 576 distinct binary keys and a random 16-byte owner. Acquisition measures only a
successful SET NX PX on an absent key. Release measures the warmed public ExecuteScriptAsync path
for an owner-checked deletion of a pre-acquired key. Its command construction is included; seeding,
ownership/TTL checks, and cleanup are outside timing. TTL is 120000 ms. There is no per-operation
companion command. Script-cache warm-up and state checks affect cache/JIT history outside timing.
The [run guide](../how-to/run-roundtrip-benchmarks.md) specifies these boundaries.

Cells are minimum–maximum across three runs, rounded to whole units, not confidence intervals or
pooled percentiles. One unit is one acquisition or one release, unlike the two-command cycle above.

| Operation | RESP | Callers | Ops/s | Mean µs | p50 µs | p95 µs | p99 µs | Managed B/op |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| AcquireLease | 2 | 1 | 4575–4913 | 203–218 | 198–203 | 278–303 | 322–417 | 1736–1736 |
| AcquireLease | 2 | 8 | 17523–31873 | 250–454 | 233–304 | 331–1087 | 428–3197 | 1535–1631 |
| AcquireLease | 3 | 1 | 4763–4901 | 204–210 | 195–201 | 270–282 | 331–342 | 1736–1736 |
| AcquireLease | 3 | 8 | 26921–29272 | 273–297 | 244–270 | 395–437 | 628–768 | 1611–1625 |
| ReleaseLease | 2 | 1 | 3866–4640 | 215–259 | 204–232 | 278–421 | 401–758 | 2353–2353 |
| ReleaseLease | 2 | 8 | 12282–31459 | 254–581 | 239–346 | 334–1038 | 397–7917 | 2181–2248 |
| ReleaseLease | 3 | 1 | 4585–4831 | 207–218 | 199–210 | 270–284 | 336–401 | 2352–2352 |
| ReleaseLease | 3 | 8 | 27920–30111 | 265–286 | 249–259 | 354–422 | 498–790 | 2244–2248 |

The eight-caller RESP2 release p99 varied from 397 to 7917 µs. This spread prevents treating these
short shared-host runs as regression thresholds or protocol rankings. Process-wide managed B/op
includes the client and executing harness, excludes prepared inputs/final validation, and is not
retained memory. Different key lengths, key reuse, harness paths and per-case scheduling also mean
the isolated acquisition/release figures cannot simply be added to predict the cycle's cost.

Reports under `artifacts/performance/`:

- `roundtrips-valkey-dotnet-bench-f24e133b8bf44d828b8b2d7d62cf045e.json`
- `roundtrips-valkey-dotnet-bench-29fe655eff19402a9fa702ddedffa97c.json`
- `roundtrips-valkey-dotnet-bench-fa783c65de6f48a3808cd9d3862100f2.json`

Each report has `ProfileVersion=2`, 32 rows, sample arrays of 512/4096 entries, and verified cleanup.
Correctness gates preceded measurement: 484 unit tests, 217 server-free harness tests, and four live
protocol/concurrency cases passed (4.411 seconds). The live suite covers all original workloads and
all 576 isolated operations per worker, wrong-owner release rejection, held-key NX rejection,
binary ownership/TTL or absence, setup/exhaustion guards and DBSIZE=0 after cleanup. Seven owned
containers across correctness and measurement were removed; existing Docker inventory was unchanged.
The manual workflow uses the expanded profile but was not dispatched. No shipping code or runtime
dependency changed. NFR-PERF-002's requested operation set now has bounded local baseline coverage;
same-key contention, broader load/topology/version/TLS profiles and prolonged resource evidence
remain separate from that coverage and from production lock-safety claims.
