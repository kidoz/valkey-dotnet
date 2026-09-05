# GET and pipeline allocation observations

Initial local profiling on 6 September 2026 (Europe/Moscow), before shipping-library optimization.
The final section records the subsequent buffered bulk-length optimization and its comparison.
The [profiling guide](../how-to/profile-allocations.md) defines reproduction and safety controls.
The earlier [round-trip reference](roundtrip-performance.md) remains a separate, untraced baseline.

## Environment

Apple M4 Max, 16 logical processors, macOS 26.6.2 Arm64, .NET SDK 10.0.400/runtime 10.0.11,
Release, concurrent workstation GC. The live server was Valkey 9.1.2 in a fresh local Docker
container (one CPU, 128 MiB, no TLS), using the same image ID as the round-trip reference.
Existing host services remained running. The collector was dotnet-trace 9.0.661903 with
`gc-verbose`; offline analysis used TraceEvent 3.1.21, already in the benchmark dependency graph.
The shipping library remains dependency-free.

## Focused real-server traces

Both runs used RESP3, eight callers on one physical socket, prebuilt commands, 1 KiB binary values,
and 64 warm-up operations per caller. Setup, warm-up, final binary validation, and teardown were
excluded using the workload process ID and UTC window.

| Operation unit | Measured units | Counter B/unit | Allocation samples | Lost events / missing stacks |
|---|---:|---:|---:|---:|
| GET | 131072 | 2492.40 | 3130 | 0 / 0 |
| 100-key GET pipeline | 4096 | 152369.26 | 5949 | 0 / 0 |

These counters include managed client/harness allocations under profiling, not retained memory.
The pipeline figure covers 100 returned values, not one key. These are single observations, not
an optimization comparison or a new latency/throughput baseline.

Allocation stacks identified these paths:

| Allocated objects | Observed source |
|---|---|
| Returned payload `byte[]` | `RespReader.ReadExactAsync`; largest sampled category in both runs |
| Pending tasks/completion sources | `ValkeyClient.SendMultiplexedAsync` |
| Async state machines and semaphore wait machinery | Client wrappers, shared write admission, reader and benchmark wrapper |
| Encoded command `byte[]` | `RespWriter.Encode` |
| Reply objects | `RespValue.Bytes` / `RespReader.ReadBlobAsync` |
| Temporary length bytes and ASCII strings | `RespReader.ReadLineAsync` / `ReadAsciiLineAsync` |

Summed tick weights were 333660528 bytes for GET versus a 326683544-byte counter delta, and
634136808 bytes for pipelines versus 624104472 counter bytes. Sampling attributes each tick's
allocation interval to one observed type/stack; repeated allocation patterns can skew individual
categories. These weights are **not exact per-site byte totals or object counts**. In particular,
the pipeline trace overrepresents header temporaries relative to the exact codec control below.

## Paired codec control

BenchmarkDotNet 0.15.8 ShortRun: one launch, three warm-ups, three measured iterations. Both
methods allocate an identical array of owned binary payloads and `RespValue` objects.
`ParseReplies` additionally parses synchronous unfragmented `$1024` replies. The one-reply array
is a control-harness allocation, not part of the public single-GET result. There is no socket,
pending queue, async suspension or fragmented header in this control.

| Method | Replies | Mean | Error (99.9% CI half-width) | Allocated B/operation |
|---|---:|---:|---:|---:|
| MaterializeReplies | 1 | 45.04 ns | 1.827 ns | 1160 |
| ParseReplies | 1 | 192.71 ns | 31.387 ns | 1224 |
| MaterializeReplies | 100 | 4619.63 ns | 1723.856 ns | 113624 |
| ParseReplies | 100 | 18254.17 ns | 4836.262 ns | 120024 |

The measured difference is 64 bytes per reply, or 6400 bytes per 100 replies. Source inspection
matches two temporary representations of the four-byte length text: a byte array from
`ReadLineAsync` and an ASCII string from `ReadAsciiLineAsync`. This established the pre-optimization
unfragmented parsing overhead; `MaterializeReplies` is not a replacement parser. No reduction or
throughput improvement was implemented or measured at that stage. Timings are short-run local observations;
BenchmarkDotNet could not raise process priority on this host.

A second complete four-case run reproduced all four exact allocation figures. Its means were
44.70 / 182.10 ns for one reply and 4456.81 / 17539.53 ns for 100 replies (materialize / parse).
Run-to-run timing differences are not optimization gains; the shipping code was identical.

## Evidence

Local artifacts under `artifacts/performance/`:

- `allocation-get-20260906-1.nettrace` with
  `allocations-valkey-dotnet-bench-9fb5bf1f54dd41eabf6c2eb2f7b23efb.json` and
  `allocation-stacks-bfe5ccd3cc974e50822cc11763417115.json`.
- `allocation-pipeline-20260906-1.nettrace` with
  `allocations-valkey-dotnet-bench-99de4ee1cce34ccabd97ff7c8812db24.json` and
  `allocation-stacks-ed125bdd8dd9483d9e931f0cf03f92b7.json`.
- `get-reply-codec/`: full BenchmarkDotNet log, CSV, Markdown and HTML summaries. Exact allocations
  above are the memory-diagnoser allocated-byte totals divided by its operation counts.
- `get-reply-codec-repeat/`: second complete run, including full JSON with exact allocations.

The separate RESP2/RESP3 workload correctness checks passed (two cases, 1.349 seconds). All four
owned containers across correctness and profiling runs were removed. No Docker networks were
created or removed; existing container/network identities and healthy services remained intact.
The traces are local artifacts, not automatically uploaded or published.
All 442 unit tests and 196 server-free harness checks passed in Debug/Release, along with full
builds and `just ci`. The final analyzer reproduced both reports and rejected mismatched metadata;
Debug profiling and unsupported operation names were rejected before Docker creation.

Fragmented/streamed replies, other payloads and runtimes, cluster/TLS, retained-memory behavior,
and longer-running resource evidence remain outside this profile.

## Buffered bulk-length optimization

Implemented and measured later on 6 September 2026 in the same environment. The common complete
unsigned-decimal header is parsed in place, with an overflow guard before every multiply/add.
The fast path consumes nothing on a miss. Signed/null/streamed/non-decimal/overflowing/incomplete
headers still use the existing line parser, preserving its grammar, diagnostics and fragmentation
behavior. Blob strings, blob errors and verbatim strings share this path. Streamed chunk lengths
and aggregate/scalar parsing are unchanged.

Every header byte, including CRLF, is charged against the response budget before the existing
bounded payload allocator runs. Element/depth checks still precede blob parsing, and payload arrays
retain their independent ownership. No parser option/default, API, runtime dependency, cancellation
invalidation or retry behavior changed.

Fresh before/after BenchmarkDotNet ShortRun jobs used the unchanged paired control (one launch,
three warm-ups/measurements each). These are sequential jobs, not randomized interleaved samples.

| ParseReplies unit | Before mean ± error | After mean ± error | Before B/unit | After B/unit |
|---|---:|---:|---:|---:|
| One 1 KiB reply | 190.65 ± 28.52 ns | 146.70 ± 27.56 ns | 1224 | 1160 |
| 100 replies | 18728.55 ± 544.39 ns | 13688.99 ± 2091.95 ns | 120024 | 113624 |

Error is the half-width of the 99.9% confidence interval. The materialization controls stayed at
1160 / 113624 B; their means were 47.08 / 4958.43 ns before and 48.19 / 4625.26 ns after. Thus the
64 B/reply allocation delta disappeared for this unfragmented workload, without removing payload
or reply objects. Short-run timing reductions are local observations, not portable speedup guarantees.

The unchanged untraced real-server profile also completed all 24 rows before and after. Selected
eight-caller rows (one unit is a GET or a 100-key batch):

| Protocol / unit | Before → after B/unit | Before → after units/s | Before → after p99 µs |
|---|---:|---:|---:|
| RESP2 GET | 2496.36 → 2429.22 | 26831 → 25619 | 754 → 886 |
| RESP3 GET | 2477.71 → 2435.79 | 26298 → 29315 | 950 → 637 |
| RESP2 pipeline | 152683.27 → 146289.04 | 1677 → 2264 | 8501 → 6044 |
| RESP3 pipeline | 152648.69 → 146275.29 | 2177 → 2121 | 6648 → 6521 |

Process-wide allocations include async/gate/harness variability; fragmentation can still take the
fallback path. Throughput and tails moved in both directions on this shared host, including slower
RESP2 GET and RESP3 pipeline throughput. No general end-to-end speedup or CI threshold follows.

The 42 new regression cases cover binary payloads and next-reply ownership across every split,
null/empty/signed/zero-padded forms, malformed/overflow/truncated input, every smaller frame-byte
budget, element/depth limits, cancellation during fallback, a header larger than the 8 KiB buffer,
and buffered-versus-fragmented outcomes for all 256 possible inserted header bytes. A maximum
Int32 length is rejected before a large payload allocation. All 484 unit tests, 196 harness checks,
and both live RESP2/RESP3 workload checks passed; the live cases took 1.420 seconds.
Unit and harness checks passed in both Debug/Release; full builds and `just ci` also passed.
Maintainer review remains required for this untrusted-input parser change before release.

Evidence under `artifacts/performance/`:

- `blob-length-before/` and `blob-length-after/`: complete codec logs and full JSON reports.
- Before: `roundtrips-valkey-dotnet-bench-030bea491f4e4ef084966989fe442cfc.json`.
- After: `roundtrips-valkey-dotnet-bench-2639dbd8e3be4a36aa1023ee95736f7e.json`.
- `roundtrip-workloads.trx`: separate correctness run.

The four owned containers across the comparison and correctness runs were removed; no networks
were created or removed. Fragmented/signed/streamed forms remain correct but have no claimed
allocation improvement. Wider payload/runtime/version/TLS/cluster performance remains unmeasured.
