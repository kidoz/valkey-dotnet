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
| Encode a 1 KiB binary-safe `SET` command | 63.32 ns | 1.09 KiB |
| Parse a four-entry RESP3 map | 506.25 ns | 1.15 KiB |

The harness summary, including error, standard deviation, and GC-generation columns, is retained
outside the published tree in `.agents/research/BENCHMARK_RECORD.md`.

## Reproducing

```bash
just bench --filter '*'
```

Equivalent to `dotnet run -c Release --project benchmarks/ValkeyDotNet.Benchmarks -- --filter '*'`.
Release configuration is required; a Debug run does not produce meaningful numbers.

Mean times are hardware-specific. Re-run the suite on the target deployment hardware before using
these figures for capacity planning. Allocation figures are stable across machines and are the more
useful regression signal.

## Known characteristics

Command encoding computes the exact wire length first and allocates only the returned payload. The
1.09 KiB allocation for the 1 KiB command is therefore the encoded command itself, including RESP
framing, with no temporary writer buffer or header strings.

The parser measurement is steady-state: one reader and its 8 KiB connection buffer are created in
global setup, then a repeating stream supplies frames. The reported allocation is the decoded reply
graph and temporary scalar parsing data, not per-connection setup. The previous 12.38 KiB figure
included a new reader and stream in every measured operation and is not directly comparable.

Remaining allocation work is concentrated in fragmented long lines, streamed strings, verbatim
strings, and scalar text parsing. Any change there must preserve the response-byte, decoded-element,
and nesting bounds described in [Client options](client-options.md) and the binary safety described
in [RESP values](resp-values.md).
