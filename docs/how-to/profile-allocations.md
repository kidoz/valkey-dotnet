# Profile GET and pipeline allocations

Run from the repository root with local Docker and the pinned SDK. This profile creates its own
bounded standalone server; it never accepts an existing application endpoint.

```bash
just ci
just test-roundtrip-workloads
dotnet build benchmarks/ValkeyDotNet.Benchmarks -c Release
dotnet tool install dotnet-trace --tool-path artifacts/diagnostic-tools --version 9.0.661903
mkdir -p artifacts/performance
allocation_trace_dir=$(mktemp -d artifacts/performance/allocation-trace.XXXXXX)
artifacts/diagnostic-tools/dotnet-trace collect --profile gc-verbose \
  --output "$allocation_trace_dir/get.nettrace" --show-child-io -- \
  dotnet benchmarks/ValkeyDotNet.Benchmarks/bin/Release/net10.0/ValkeyDotNet.Benchmarks.dll \
  --allocation-workload Get
```

Skip installation when that tool path already has version 9.0.661903. On Windows use the `.exe`
tool and a new, non-existing output path instead of the POSIX `mktemp` command.

Copy the completed `Allocation workload report:` JSON path from stdout, then run:

```bash
just allocation-report "$allocation_trace_dir/get.nettrace" 'artifacts/performance/allocations-REPLACE-WITH-PRINTED-NAME.json'
```

Repeat collection with `--allocation-workload Pipeline100Get` and a different trace filename.
Use its own printed JSON path for analysis. Do not pair traces and metadata from different runs.
Keep the `.nettrace`, workload JSON, and generated `allocation-stacks-*.json`/`.etlx` files together.
The analyzer runs offline after the traced process exits, so its own allocations are excluded.
It rejects lost events, incomplete time coverage, invalid metadata, and windows without matching
allocation samples. Stack rows retain type, up to 64 frames, sample count, and summed tick weights;
missing stacks are counted explicitly. Only analyze trusted local traces and metadata.

## Separate sampling from exact allocation deltas

The [.NET `gc-verbose` profile](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/dotnet-trace)
samples allocations. A tick's byte weight is attributed to its sampled type/stack, not an exact
count of objects allocated at that site. Repeated allocation patterns, thread-local sampling,
JIT inlining and limited sample counts can skew individual categories. The report preserves the
independent process-wide allocation counter; sampled weights need not sum to it. Do not infer
precise per-site savings, retained memory, or leaks from the trace.

Use the paired codec control to measure parsing overhead above the same owned reply graph:

```bash
just bench-get-replies
```

Both benchmark methods create a result array and independent binary payload/value objects.
`ParseReplies` reads synchronous, unfragmented `$1024` bulk replies; `MaterializeReplies` only
constructs that result graph. Compare allocated bytes within each reply count (1 or 100).
This control excludes network I/O, fragmented input, async suspension and pending-request state.
It is not a simulated end-to-end pipeline or a faster replacement implementation.

`just allocation-workload Get` (or `Pipeline100Get`) runs the focused workload without a profiler
when an untraced allocation-counter comparison is needed. Use normal
[round-trip benchmarks](run-roundtrip-benchmarks.md) for latency/throughput comparisons: traced
timings are not a performance baseline.

## Scope and cleanup

Each focused run uses RESP3, eight callers sharing one socket, 1024-byte values, 64 warm-up
operations per caller, and prebuilt commands. GET measures 16384 operations per caller (131072
total); pipeline measures 512 batches per caller (4096 batches, 409600 replies). Binary validation
runs before and after the measured window. Metadata includes UTC boundaries, process ID, operation
counts, environment, and cleanup. Window selection assumes a stable host wall clock.

The runner has a 90-second deadline and reuses the ownership/resource limits and independent
60-second cleanup described in the [round-trip guide](run-roundtrip-benchmarks.md). Only exact
owned keys are deleted, DBSIZE must return zero, and a completed report is written only after
verified container removal. Do not forcibly terminate the traced child; let its bounded run and
cleanup finish. Existing Docker resources remain untouched; diagnostic tools, images and local
trace artifacts remain cached. No trace is uploaded automatically.

See [recorded allocation observations](../reference/allocation-profile.md) for measured results.
