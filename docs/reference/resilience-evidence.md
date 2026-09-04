# Resilience evidence

This page separates implemented experiments from executed evidence. It is not a cache/lock
production-readiness certification.

| Experiment | Bound and invariant | Evidence |
|---|---|---|
| Non-listening loopback endpoint | Two bounded connect attempts; `NotSent`; same owner succeeds when that exact endpoint begins listening; failed write is not replayed. | Deterministic suite passed locally. The observed macOS failure was timeout; refusal is accepted on kernels that report it. |
| Repeated loopback connection loss | RESP2/RESP3; 32 cycles each; 16 concurrent successful echoes followed by 16 ambiguous writes per cycle; all callers settle, replies match, no write replay, one server session per cycle. | Deterministic suite passed locally. |
| Live connection kill and replacement | Repeated `CLIENT KILL`; protocol, client name, database and script recovery; telemetry counts. | Existing live coverage previously passed Valkey 7.2, 8.1, and 9.1. Not a server restart. |
| Owned-container stop/start | Opt-in runner; default three cycles per protocol; ten-minute case deadline; 16 concurrent recovery calls; new server run ID, absent offline write, script recovery, one named owner connection, active-operation gauge zero. | Implemented; no executed real-server restart result recorded yet. |
| Restart resource samples | Post-GC heap growth ≤16 MiB and handle growth ≤32 from first completed cycle; thread-pool samples retained in TRX. | Implemented smoke-test budgets; not yet executed. No long-duration soak claim. |

DNS-resolution faults, abrupt primary failover, partitions, prolonged soak, and subscriber restoration
still need dedicated evidence. No transport retry policy was changed to make these tests pass.

See [Run isolated restart tests](../how-to/run-resilience-tests.md) for the experiment's safety and
cleanup contract.
