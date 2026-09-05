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

See [Run isolated restart tests](../how-to/run-resilience-tests.md) for the experiment's safety and
cleanup contract.
