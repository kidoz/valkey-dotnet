# Publish and invalidation performance

Initial local observations from three complete runs on 6 September 2026 (Europe/Moscow; JSON
timestamps are 5 September UTC). These are bounded-profile observations, not production SLOs,
an optimization comparison or release thresholds. The
[run guide](../how-to/run-notification-benchmarks.md) defines the full measurement contract.

## Environment

| Setting | Observed/configured value |
|---|---|
| Client | Apple M4 Max, Arm64, 16 logical processors, macOS 26.6.2 |
| Runtime | .NET SDK 10.0.400, runtime 10.0.11, Release |
| Server | Valkey 9.1.2, Linux 7.0.12-linuxkit aarch64, standalone Docker loopback |
| Image ID | `sha256:475ee65cc75c327407458f5096cdd36954b3de3fc83f4c8ac31a4a8edecbf49e` |
| Limits | One CPU, 128 MiB memory, 64 PIDs, 16 MiB data tmpfs; persistence/DEBUG disabled |
| Connections | One shared writer, one notification consumer, one idle observer; no TLS/recovery |
| Load | One/eight callers; 64 warm-up and 512 measured operations per caller; 1 KiB binary values |
| Tracking | Unique binary keys, seeded/pre-registered outside timing; BCAST with one binary prefix |
| Queue | 8192 notifications, one consumer; complete warm-up and measured delivery drain |
| Host noise | Existing Valkey test fixtures and application services remained running; not a quiet dedicated host |

## Observations

Each cell is the minimum–maximum **across the three runs**, rounded to whole units. Percentile
ranges are ranges of per-run nearest-rank percentiles, not percentiles of pooled samples or
confidence intervals. Throughput is acknowledged commands or fully observed key/message deliveries
per second. Latency is invocation-to-consumer observation, in microseconds.

| Operation | RESP | Callers | Ack/s | Delivered/s | Delivery p50 µs | Delivery p95 µs | Delivery p99 µs | Managed B/op |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Publish | 2 | 1 | 2661–3793 | 2661–3793 | 259–352 | 338–570 | 473–2732 | 5208–5212 |
| Publish | 2 | 8 | 9974–21555 | 9974–21554 | 351–574 | 502–1996 | 1301–5361 | 4595–4742 |
| Publish | 3 | 1 | 2229–3715 | 2229–3715 | 258–373 | 357–921 | 560–2316 | 5208–5208 |
| Publish | 3 | 8 | 8983–22664 | 8983–22664 | 336–434 | 492–3931 | 880–8026 | 4672–4727 |
| Default tracking | 3 | 1 | 2954–3960 | 2954–3959 | 248–326 | 331–474 | 384–636 | 3856–3856 |
| Default tracking | 3 | 8 | 14577–24047 | 14577–24046 | 310–480 | 454–1040 | 983–2694 | 3210–3303 |
| BCAST tracking | 3 | 1 | 2840–3800 | 2840–3800 | 246–278 | 337–831 | 433–1481 | 3854–3856 |
| BCAST tracking | 3 | 8 | 21931–26611 | 21931–26610 | 276–311 | 364–645 | 506–952 | 2795–2809 |

Acknowledgment and delivered rates are nearly equal in these runs: the consumer kept up with this
small closed-loop load. This does not establish slow-consumer capacity or lossless behavior under
overload. Substantial run-to-run spread, especially the eight-caller Pub/Sub tails, prevents a
protocol-ranking or generalized throughput claim. Invalidation latency includes transport,
decoding, queueing and scheduling, not just server work or application cache eviction.

Managed allocation includes both connections and the running correlation/validation harness,
including per-notification objects, but excludes preconstructed inputs and final value validation.
It is process-wide allocation per operation, not retained memory or leak evidence. Batch shape and
concurrent scheduling can affect B/op; the results do not justify a general BCAST memory comparison.

## Evidence and coverage

All three reports under `artifacts/performance/` contain eight rows, metadata, acknowledgment and
delivery p50/p95/p99, both independently sorted sample series, and `CleanupVerified=true`:

- `notifications-valkey-dotnet-bench-76c5ed6c4ad0414493a4824bf0d71384.json`
- `notifications-valkey-dotnet-bench-32a82cb28f5146d88ebdb24ca876b431.json`
- `notifications-valkey-dotnet-bench-699f4834dd8e4700ab3d62ebde0ca7f5.json`

Correctness gates ran first: 484 library tests, 210 server-free harness tests, and four opt-in live
cases covering all eight combinations, with no performance assertions. The live cases passed in
1.650 seconds. All seven owned containers (four correctness, three benchmark) were removed; the
pre-existing container/network inventory remained unchanged. The manual workflow was added but not
dispatched. No shipping library code, API or runtime dependency changed.

NFR-PERF-002 now has bounded notification evidence alongside the existing cache/lock profiles.
Individually timed acquisition and release remain the next narrow benchmark gap. Wider server
versions, payloads, fan-out, cluster/TLS, open-loop load, overflow/recovery and prolonged-resource
profiles remain separate work; these results are not full performance acceptance.
