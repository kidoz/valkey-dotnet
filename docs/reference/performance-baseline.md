# Performance baseline

Measured protocol-code costs. These are **local** measurements of encoding and parsing only — they
contain no network round trip and are not throughput figures for a real Valkey deployment.

## Measurement environment

| Field | Value |
|---|---|
| Harness | BenchmarkDotNet 0.15.8, short run |
| Date | 4 September 2026 |
| Runtime | .NET 10.0.11 |
| Hardware | Apple M4 Max (Arm64) |

## Results

| Operation | Mean | Allocated per operation |
| --- | ---: | ---: |
| Encode a 1 KiB binary-safe `SET` command | 344.3 ns | 5.41 KiB |
| Parse a four-entry RESP3 map | 1.146 µs | 12.38 KiB |

The full unedited harness output, including error, standard deviation, and GC-generation columns, is
retained outside the published tree in `.agents/research/BENCHMARK_RECORD.md`.

## Reproducing

```bash
just bench
```

Equivalent to `dotnet run -c Release --project benchmarks/ValkeyDotNet.Benchmarks`. Release
configuration is required; a Debug run does not produce meaningful numbers.

Mean times are hardware-specific. Re-run the suite on the target deployment hardware before using
these figures for capacity planning. Allocation figures are stable across machines and are the more
useful regression signal.

## Known characteristics

Allocation currently exceeds what the protocol path requires. Both measured operations allocate
several times the payload size because encoding and parsing use temporary strings, arrays, and
`MemoryStream` instances rather than pooled buffers and span-based parsing.

This is the clearest optimization opportunity in the library, and it matters most for pipelined
workloads, where per-command allocation multiplies across the batch. Any change here must preserve
the frame-size and nesting bounds described in [Client options](client-options.md) and the binary
safety described in [RESP values](resp-values.md); a faster parser that drops either is a regression.
