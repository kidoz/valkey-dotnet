# Resilience evidence

This page separates implemented experiments from executed evidence. It is not a cache/lock
production-readiness certification.

| Experiment | Bound and invariant | Evidence |
|---|---|---|
| Non-listening loopback endpoint | Two bounded connect attempts; `NotSent`; same owner succeeds when that exact endpoint begins listening; failed write is not replayed. | Deterministic suite passed locally. The observed macOS failure was timeout; refusal is accepted on kernels that report it. |
| Repeated loopback connection loss | RESP2/RESP3; 32 cycles each; 16 concurrent successful echoes followed by 16 ambiguous writes per cycle; all callers settle, replies match, no write replay, one server session per cycle. | Deterministic suite passed locally. |
| Live connection kill and replacement | Repeated `CLIENT KILL`; protocol, client name, database and script recovery; telemetry counts. | Existing live coverage previously passed Valkey 7.2, 8.1, and 9.1. Not a server restart. |
| Owned-container stop/start | Opt-in runner; default three cycles per protocol; ten-minute case deadline; 16 concurrent recovery calls; new server run ID, absent offline write, script recovery, one named owner connection, active-operation gauge zero. | Passed locally on Valkey 9.1.2 on 2026-09-05: RESP2 and RESP3, three cycles each, no failures or skips. |
| Restart resource samples | Post-GC heap growth ≤16 MiB and handle growth ≤32 from first completed cycle; thread-pool samples retained in TRX. | Heap smoke budget passed in the local restart run. macOS reported zero handles throughout, so this run provides no meaningful handle-growth evidence. No long-duration soak claim. |

## Local restart run — 2026-09-05

`just test-resilience` ran against fresh, ownership-validated Docker containers on macOS arm64,
using SDK 10.0.400, runtime .NET 10.0.11, and Valkey 9.1.2. The two cases completed in 9.307 seconds.
Each case verified a working script before injection, stopped only its own server, observed
`Disconnected` and a `NotSent` offline write, and recovered through the same owner after restart.
All six cycles verified a changed server run ID, matching concurrent replies, script recovery,
no offline write replay, one named owner connection, and zero active operations.

| Protocol | Post-GC heap samples (bytes, cycles 1–3) | Thread-pool threads | Queued work |
|---|---|---|---|
| RESP2 | 1,203,160; 1,158,208; 1,225,256 | 8 throughout | 0 throughout |
| RESP3 | 1,420,080; 1,347,936; 1,348,624 | 8 throughout | 0 throughout |

The local TRX record is `artifacts/resilience/restart-9.1.trx`. Both generated containers and their
Compose networks were removed; the two pre-existing stopped containers remained untouched.
Downloaded images remain cached. This short run establishes neither leak freedom nor restart
compatibility with Valkey 7.2/8.1; those versions were not exercised in this experiment.

Subscriber connection-loss restoration subsequently passed six live RESP2/RESP3 cases on Valkey
9.1.2, 8.1.10, and 7.2.14: three exact-ID connection kills per case, eighteen successful recoveries,
and verified cleanup. See [subscriber verification evidence](subscriber.md#verification-evidence).

DNS-resolution faults, abrupt primary failover, partitions, prolonged soak, and subscriber
server-restart restoration still need dedicated evidence. No transport retry policy was changed
to make these experiments pass.

Standalone RESP3 tracking also passed its live matrix on 2026-09-05: twelve cases across Valkey
9.1.2, 8.1.10, and 7.2.14, including nine exact-ID connection kills and successful on-demand tracking
restorations. Binary default/broadcast invalidations, NOLOOP, loss resets, replacement IDs, restored
prefix delivery, zero queue overflows, and writer isolation were verified. All three fresh owned
containers were removed with zero keys remaining; existing containers were unchanged. See the
[tracking evidence and limits](client-tracking.md#live-tracking-matrix--2026-09-05).
Tracking server restart, partitions, live TLS, and prolonged soak remain unverified.

Sharded Pub/Sub passed two live RESP2/RESP3 cases on 2026-09-05 against a fresh three-primary
Valkey 9.1.2 cluster. Binary delivery across all three slot ranges, independent duplicate handles,
final unsubscription, zero drops, and publisher isolation passed. Ownership-checked cleanup removed
only the three test containers and their network. This was a routing/lifecycle check, not a fault
experiment. Concurrent verification work also reproduced intermittent subscriber acknowledgement/
remote-close unit failures. A later shutdown-isolation change passed 370 unit cases and five
additional full Debug runs; this does not extend the live test's scope. See the
[sharded verification scope](cluster-subscriber.md#verification-scope).

See [Run isolated restart tests](../how-to/run-resilience-tests.md) for the experiment's safety and
cleanup contract.

## Slot-migration runner

`OwnedSlotMigrationPreservesShardedHandleAndStream` is an opt-in live RESP2/RESP3 experiment using
three newly owned Valkey 9.1 primaries. It moves one empty slot using legacy SETSLOT, retaining the
same sharded handle/stream and checking binary delivery, registration ownership, unrelated-channel
isolation, counters, final unsubscription, and resource cleanup. It is exposed by `just test-migration`
and the manual **Slot migration** workflow. The default is three moves per protocol, with a maximum
of twenty and a five-minute deadline per case.

On 2026-09-05, `just test-migration 3` passed both live cases on Valkey 9.1.2, with no failures or
skips: three moves per protocol, six successful relocations total. Each case retained the same
handle, enumerator, and completion task; binary delivery resumed after every move, the target had
one shard registration and the source had none, and the unrelated third-primary channel recorded
no connection loss. Each moving handle recorded three losses, three attempts, three successful
recoveries/relocations, and zero local queue drops. No library change was needed for this run.

The host was macOS arm64, SDK 10.0.400, runtime .NET 10.0.11; servers ran Linux aarch64 without TLS.
Each of the two sequentially created clusters had three primaries limited to 128 MiB and one CPU
per node, random loopback ports, and temporary data filesystems. Before teardown, all nodes had
zero keys, no shard channels, and no named application connections. Ownership-checked cleanup
removed all six test containers and both test networks; a subsequent Docker inventory confirmed
no test resources remained. Existing application container IDs and project-network IDs were retained.
The TRX record is `artifacts/resilience/migration.trx` (two cases, approximately 31 seconds total).
Per-move elapsed samples include Docker/admin checks and are not recovery-latency measurements.

`just ci` and the Release unit suite also passed all 435 cases, and all ten harness-only checks
passed in Release.
This establishes a small live legacy empty-slot migration smoke test, not general release readiness.
Atomic migration, nonempty-key transfer, forced ASK, primary failover, seed unavailability, TLS, and prolonged soak
remain separate evidence requirements. The [run guide](../how-to/run-slot-migration-tests.md)
describes ownership checks, opt-in controls, and cleanup limits.
