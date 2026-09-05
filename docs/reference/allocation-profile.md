# GET and pipeline allocation observations

Local profiling on 6 September 2026 (Europe/Moscow), with no shipping-library optimization.
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
`ReadLineAsync` and an ASCII string from `ReadAsciiLineAsync`. This establishes the current
unfragmented parsing overhead; `MaterializeReplies` is not a replacement parser. No reduction or
throughput improvement has been implemented or measured. Timings are short-run local observations;
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
