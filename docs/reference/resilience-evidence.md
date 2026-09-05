# Resilience evidence

This page separates implemented experiments from executed evidence. It is not a cache/lock
production-readiness certification.

## Atomic cutover queued writes — 2026-09-06

`just test-cutover-writes` passed both RESP2/RESP3 cases on Valkey 9.1.2 in 30.449 seconds, with no
failures or skips. Each used three fresh owned primaries with 128 MiB/one CPU limits, temporary
storage, and DEBUG=local. The Release client ran on macOS arm64 with SDK 10.0.400/.NET 10.0.11;
servers ran Linux aarch64 without TLS. See the [run guide](../how-to/run-cutover-writes-tests.md).

Two independent routed clients updated a 4 KiB expiring and a five-byte persistent binary string
before write pause, while queued across cutover, and after cutover. Each phase acknowledged both
SET XX KEEPTTL updates, with values checked before the next phase could mask a lost update.
All six updates per protocol were acknowledged; no ambiguous result or application replay occurred.
Original absolute expiration remained exact, positive TTL and persistence survived, and final
placement was target-only with all slot maps naming the target.

The test first observed a held post-snapshot export and both provisional keys, then used the
importer's local PREVENT-FAILOVER hook to hold ownership handoff. Source INFO confirmed migration
write pause; the correlated export reached `failover-granted` while its import stayed active.
Before release, read-only CLIENT LIST ID identified both exact original writer IDs with the owned
name, SET command, and blocked flags; both operations were still pending. This establishes actual
queued writes across handoff, not a timing-based assumption that writers overlapped migration.
The controlled stage follows the [upstream queued-client migration test](https://github.com/valkey-io/valkey/blob/9.1.2/tests/unit/cluster/cluster-migrateslots.tcl#L1273-L1316).

After release, both pending writes returned OK and both correlated migration jobs succeeded.
Original sharded handle/completion/enumerator delivery survived one relocation and one recovery
attempt with zero drops; the stationary stream had no connection loss or drops. Outcome recording
distinguishes acknowledged, not-sent, ambiguous, received-error, unexpected, and unclassified
cancellation results; typed cancellation keeps its ambiguity. Any non-acknowledged result fails the
healthy test. No transport replay or retry policy was added.

Pause observation, queued writes, release, and immediate value verification share five seconds;
the server's checked 5000 ms manual-failover setting is not extended. Shared migration work has
45 seconds after preflight. Independent ten-second finally blocks clear both hooks, and failed
write scopes cancel and drain both operations. Exact-ID observations are capped at 16 KiB and
reject duplicate/foreign identities and malformed fields. Teardown verified zero keys, shard
channels, and named clients, then removed all six owned containers/two networks. Evidence:
`artifacts/resilience/cutover-writes.trx`. The manual workflow was added, not dispatched; no shipping
API/dependency/parser/retry changes were needed.

All 442 unit tests and 181 harness checks passed in Debug/Release, and the new live cases skipped
without opt-in. Six repeat/shared cutover-writer, post-snapshot-writer, and rollback cases passed
in 86.174 seconds, with zero expiry shifts and drops. Evidence:
`artifacts/resilience/migration-after-cutover-writes.trx`. Across both live runs, twenty-four owned
containers/eight networks were removed; exact pre-existing Docker inventory remained and hostloom
services stayed healthy. Initial build attempts encountered MSBuild worker crashes; a single-worker
build with build servers disabled passed, followed by `just ci` with node reuse/build-server reuse
disabled. No warning suppression, project-setting change, staging, or commit was made.

This is a bounded healthy handoff with two distinct-key queued writes. It does not establish
sustained or same-key contention, simultaneous transport failure, ambiguous-result reconciliation,
uninterrupted publishing, short-lease correctness, performance, TLS, or other server versions.

## Atomic migration post-snapshot writes — 2026-09-05

`just test-atomic-writes` passed both RESP2/RESP3 cases on Valkey 9.1.2 in 25.657 seconds, with no
failures or skips. Each used three fresh owned 128 MiB/one-CPU primaries on local Docker with
DEBUG=local. The Release client ran on macOS arm64, SDK 10.0.400/.NET 10.0.11; servers ran Linux
aarch64 without TLS. The [run guide](../how-to/run-atomic-writes-tests.md) defines the fixed bounds.

The source EXPORT was held at `waiting-to-pause` using the upstream PREVENT-PAUSE hook, with a
correlated active IMPORT and both exact provisional binary keys present on the target. Two
independent routed clients then issued 32 paired rounds of SET XX KEEPTTL, one ordered writer per
key and at most two pending writes. All 64 replies were OK. Both final sequence-32 binary values,
source ownership, positive expiring TTL, exact original PEXPIRETIME, and persistent PTTL=-1 were
verified while migration remained held. Binary delivery on the original moving/stationary streams
also passed before hook release. This deterministic injection stage follows the
[upstream migration test hook](https://github.com/valkey-io/valkey/blob/9.1.2/tests/unit/cluster/cluster-migrateslots.tcl).

After release, both correlated jobs succeeded, all slot maps moved to the target, source membership
was empty, and both exact keys and final acknowledged values were retained at the target. Expiry
shift was zero in both protocols. The original moving handle/completion/enumerator survived one
relocation and one recovery attempt, with binary delivery and zero drops. The unrelated stream
reported no connection loss or drops. No write replay or export-client kill occurred.

The shared held-migration helper retains exact ownership/member/key checks and empty job histories.
Its 45-second budget includes connection setup, snapshot observation, writes, and completion; a
finally block clears the hook with an independent ten-second budget. Normal teardown verified zero
keys/shard channels/named clients and removed six containers/two networks. Evidence:
`artifacts/resilience/atomic-writes.trx`. The manual workflow was added, not dispatched. No shipping
API/dependency/parser/retry changes were needed.

All 442 unit tests and 165 server-free harness checks passed Debug/Release; `just ci` passed and
both live cases skipped without opt-in. A six-case repeat/shared-path regression (atomic writers,
healthy atomic migration, and post-snapshot link-failure rollback) passed in 79.808 seconds, with
zero expiry shifts and local drops. Evidence: `artifacts/resilience/migration-after-atomic-writes.trx`.
Across both live runs, all twenty-four owned containers/eight networks were removed. The exact
pre-existing Docker inventory remained and all three hostloom services stayed healthy. No commit
was made.

This proves bounded post-snapshot, pre-write-pause updates with two logical writers on distinct
keys. It does not establish same-key contention, writes spanning cutover, ambiguous write outcomes,
simultaneous faults, uninterrupted publishing, throughput/latency or memory performance, short-lease
correctness, TLS, other server versions, or production cache/lock safety.

## Bulk RESTORE acknowledgment loss — 2026-09-05

`just test-bulk-ack-loss` passed both RESP2/RESP3 cases on Valkey 9.1.2 in 31.853 seconds, with no
failures or skips. Each case used three fresh owned primaries and one isolated .NET relay on local
Docker. Primaries had 128 MiB/one CPU each and DEBUG disabled; the relay retained its 64 MiB/one CPU,
non-root, read-only, no-capabilities/no-published-ports configuration. The Release client ran on
macOS arm64 with SDK 10.0.400/.NET 10.0.11; servers ran Linux aarch64 without TLS. The pinned image
and exact bounds are documented in the [runner controls](../how-to/run-bulk-ack-loss-tests.md).

One two-key MIGRATE batch targeted an initially empty importing node. The relay validated all
expected frames, forwarded SELECT and the first RESTORE success, received and withheld the second
RESTORE success, then observed sender closure after the two-second source idle timeout. Its exact
phase log and zero exit status confirmed the injection. The source returned IOERR/ReplyReceived;
same-client PING and binary ECHO passed and commandstats showed exactly one MIGRATE call.

Independent reads and exact node-local key/count checks established mixed placement: the first
4 KiB binary value only at the target; the second five-byte binary value at both source and target.
Both keys started with 120-second TTLs. In both protocols, the moved key's absolute expiry shifted
+13 ms and the duplicate target copy's expiry shifted +14 ms; source duplicate expiry shifted zero.
Target expirations remained stable between observations and all TTLs stayed positive. Direct source
GET for the moved key returned ASK, routed GET found it, and mixed-key MGET returned TRYAGAIN.
All slot maps stayed source-owned. Original source-local and stationary sharded streams, completion
tasks, and enumerators survived with zero losses, attempts, relocations, or local drops.

This is partial acknowledgment followed by received IOERR, not all-key rollback or merely a
client-facing lost MIGRATE reply. It agrees with the
[server's per-key acknowledgment handling](https://github.com/valkey-io/valkey/blob/9.1.2/src/cluster.c#L579-L650).
No replay, overwrite, cutover, or winner selection ran. Source command processing stalls during
synchronous MIGRATE; post-fault delivery is not a latency guarantee. Concurrent mutation, other loss
positions, larger batches, other data types, TLS, other versions, and lock correctness remain separate.

Final teardown deleted only the three exact fixture copies, verified zero keys/shard channels/named
clients, and removed all eight owned containers/two networks. Evidence: `artifacts/resilience/bulk-ack-loss.trx`.
The manual workflow was added but not dispatched. No shipping API/dependency/parser limit/retry
behavior changed. The relay remains test-only, now with a fixed two-key mode in addition to its
single-key mode; it is not an arbitrary command proxy.

All 442 unit tests and 162 server-free harness checks passed Debug/Release, `just ci` passed, and
both new live cases skipped without opt-in. Six shared single-key acknowledgment-loss and two-key
BUSYKEY regressions passed in 83.890 seconds (`artifacts/resilience/reconciliation-after-bulk-ack-loss.trx`).
All reported zero drops; single-key destination expiry shifts were +13 ms for both protocols.
Across both live runs, twenty-eight owned containers/eight networks were removed. All pre-existing
Docker containers/networks remained and the three hostloom services stayed healthy. No commit was made.

## Bulk MIGRATE partial success — 2026-09-05

`just test-bulk-conflict` passed four cases on Valkey 9.1.2 in 52.675 seconds, with no failures or
skips: RESP2/RESP3, each with the conflict first and last in a two-key batch. Each case used a fresh
owned three-primary local Docker cluster, capped at 128 MiB/one CPU per node with DEBUG disabled.
The Release client ran on macOS arm64 with SDK 10.0.400/.NET 10.0.11; servers ran Linux aarch64
without TLS. No relay, debug hook, or network fault was used.

The source started with an expiring 4 KiB binary value and a persistent five-byte value in one slot.
The target held a different five-byte value for the second key, with a 90-second TTL. One two-key
MIGRATE without COPY/REPLACE returned outer ERR containing BUSYKEY and `DeliveryStatus=ReplyReceived`.
The same physical client subsequently passed PING and binary ECHO; command statistics confirmed
exactly one MIGRATE call. Source/target key and database counts changed from 2/1 to 1/2 in both orders.

Independent observations found the successful key only at the target, and both conflicting copies
unchanged. The moved key's absolute expiration shifted +1 ms in all four cases and stayed stable
between observations; the target conflict's expiration shifted zero and the source conflict stayed
persistent. Direct source GET for the moved key returned ASK, routed GET found the moved value,
routed conflict GET still saw the source copy, and mixed-key MGET returned TRYAGAIN. All slot maps
remained source-owned. Original source-local and stationary sharded streams, completion tasks, and
enumerators survived with zero losses, attempts, relocations, or local drops.

This is received-error partial success, consistent with the per-key reply handling in the
[Valkey 9.1.2 implementation](https://github.com/valkey-io/valkey/blob/9.1.2/src/cluster.c#L579-L619),
not an all-key rollback or production conflict-resolution policy. No batch replay, overwrite, or
cutover ran. Bulk IOERR/reply-loss ambiguity, concurrent writers, larger batches, other data types,
TLS, other versions, and lock-lease correctness remain separate evidence gaps.

Cleanup deleted only the three known fixture copies, verified zero keys/shard channels/named clients,
and removed all twelve owned containers/four networks. Evidence: `artifacts/resilience/bulk-conflict.trx`.
All 442 unit tests and 146 server-free harness checks passed Debug/Release, `just ci` passed, and the
four new live cases skipped without opt-in. The manual workflow was added but not dispatched.
No shipping API, dependency, parser limit, or retry behavior changed; ordinary migration helpers
retain their two-copy bound. See the [runner controls](../how-to/run-bulk-conflict-tests.md).

The shared membership-check regression passed four RESP2/RESP3 successful legacy-transfer and
single-key BUSYKEY cases in 59.026 seconds (`artifacts/resilience/migration-after-bulk-conflict.trx`).
Successful transfers retained the same stream through one relocation, with 0 ms/+1 ms expiry shifts;
conflict cases preserved source-local streams and expirations. All reported zero drops. Across both
live runs, twenty-four owned containers/eight networks were removed; all pre-existing Docker
containers/networks remained and the three hostloom services stayed healthy. No commit was made.

## RESTORE acknowledgment loss — 2026-09-05

`just test-restore-ack-loss` passed both RESP2/RESP3 cases on Valkey 9.1.2 in 33.285 seconds,
with no failures or skips. Each case used a fresh owned three-primary cluster and one test-only
.NET relay on local Docker. Primaries had 128 MiB/one CPU each and DEBUG disabled; the relay had
64 MiB/one CPU, no published ports or capabilities, a read-only filesystem, and a non-root user.
The Release client ran on macOS arm64 with SDK 10.0.400/.NET 10.0.11; containers ran Linux aarch64
without TLS. The pinned runtime image and isolation controls are in the
[runner guide](../how-to/run-restore-ack-loss-tests.md).

One binary key with a 4 KiB value and initial 120-second TTL started only at the source. The relay
forwarded SELECT 0 and one exact RESTORE-ASKING, received the real destination success reply,
and withheld it while keeping the sender socket open until the source's two-second idle timeout.
Its fixed phase log and zero exit status confirmed success before acknowledgment loss. The source
returned `ValkeyServerException`, `ErrorCode=IOERR`, `DeliveryStatus=ReplyReceived`, remained usable
for PING, and reported exactly one MIGRATE call. There was no application replay or server retry.

Two independent node-local reconciliation observations found the exact binary key and value at
both nodes. Source absolute expiration shifted by zero; destination absolute expiration differed
from source by +12 ms for RESP2 and +11 ms for RESP3, within the relative-TTL transfer tolerance,
and remained stable between observations. Each importing-node read used a fresh ASKING. All slot
maps and routed reads remained source-owned. Original source-local and stationary sharded handles,
completion tasks, and enumerators survived, delivered binary messages, and reported zero connection
losses, reconnect attempts, relocations, or local drops. No overwrite, cutover, or winner selection ran.

This separately establishes the duplicate-copy outcome allowed by the
[MIGRATE contract](https://valkey.io/commands/migrate/); the earlier source-only and client-facing
reply-loss experiments do not establish it. It is a single-key, no-concurrent-writer observation,
not bulk partial success, live TLS, other-version evidence, or a production conflict-resolution
policy. The source stalls during synchronous MIGRATE; post-fault delivery is not a latency guarantee.

The relay was removed under independent cleanup before copy reconciliation. Final teardown removed
only the two known fixture copies, verified zero keys/shard channels/named clients, and removed all
eight owned containers/two networks. The artifact is `artifacts/resilience/restore-ack-loss.trx`.
The manual workflow was added but not dispatched. No shipping library API, dependency, or retry
behavior changed.

All 442 unit tests and 136 server-free harness checks passed in Debug/Release, `just ci` passed,
and both new live cases skipped without opt-in. The shared BUSYKEY/source-only IOERR regression
passed four RESP2/RESP3 cases in 58.565 seconds, with retained source expiration and zero drops
(`artifacts/resilience/reconciliation-after-restore-ack-loss.trx`). Across both live runs, twenty
owned containers and six networks were removed. The before/after Docker inventory retained all
existing containers/networks, with the three hostloom services healthy. Cached images remain.

## Source-only MIGRATE IOERR — 2026-09-05

`just test-migrate-ioerr` passed both RESP2/RESP3 cases on Valkey 9.1.2 in 31.145 seconds, with no
failures or skips. Each case used a fresh owned three-primary cluster on local Docker, with
128 MiB/one CPU per node and DEBUG disabled. The Release client ran on macOS arm64 with SDK
10.0.400 and .NET 10.0.11; servers ran Linux aarch64 without TLS.

The owned destination's finite WRITE pause blocked RESTORE-ASKING during a single-key MIGRATE.
The runner observed one uniquely identified blocked restore connection before receiving IOERR
from the source's two-second server-side idle timeout. That exact destination socket disappeared
before explicit unpause. The source client reported `ValkeyServerException`, `ErrorCode=IOERR`,
and `DeliveryStatus=ReplyReceived`; subsequent PING and binary ECHO on the same physical connection
succeeded. Source command statistics confirmed one MIGRATE call, with no application replay.

The destination was unpaused under an independent cleanup deadline. Three subsequent GET/count
observations and final node-local placement checks found no destination key. Both original source
keys retained their binary values, with zero absolute-expiration shift for the 4 KiB expiring value
and the persistent key's expected TTL sentinel. All slot maps retained the source owner. Original
sharded handles/streams delivered binary messages after the fault with zero connection losses,
reconnect attempts, relocations, or local queue drops. No cutover or winner-selection policy ran.

The distinction is material: a received IOERR still permits either source-only or duplicate-copy
placement under the [MIGRATE contract](https://valkey.io/commands/migrate/). These independent
observations establish only the source-only, before-restore case. They are not evidence for a lost
RESTORE acknowledgment, an after-import duplicate copy, bulk partial success, concurrent mutation,
live TLS, or other server versions. The source's synchronous migration also stalls source commands
during the timeout; this is not a low-latency or uninterrupted-publishing claim.

Cleanup discarded only the two known source fixture keys, unsubscribed, verified zero keys/shard
channels/named clients, and removed all six owned containers/two networks. Evidence:
`artifacts/resilience/migrate-ioerr.trx`. All 442 unit tests and 111 harness checks passed in
Debug/Release, `just ci` passed, and both new cases skipped without opt-in. The manual workflow
was added but not dispatched. No shipping library code or retry policy changed. See
[runner controls](../how-to/run-migrate-ioerr-tests.md).

The shared-harness regression passed four RESP2/RESP3 legacy-transfer and pre-transfer-cancellation
cases in 54.264 seconds (`artifacts/resilience/migration-after-ioerr.trx`). Legacy expiry shifts were
0 ms/+1 ms; cancellation retained exact expiry; all cases reported zero drops. Across both live
runs, eighteen owned containers and six networks were removed. The before/after Docker inventory
retained all existing containers/networks, and the three hostloom services remained healthy.

## MIGRATE BUSYKEY conflict — 2026-09-05

`just test-busykey` passed both RESP2/RESP3 cases on Valkey 9.1.2 in 28.171 seconds, with no
failures or skips. Each case used a fresh owned three-primary cluster on local Docker, with
128 MiB/one CPU per node and DEBUG disabled. The client ran Release on macOS arm64 with SDK
10.0.400 and .NET 10.0.11; servers ran Linux aarch64 without TLS.

The same binary key existed at both nodes before transfer, with distinct 4 KiB/five-byte values
and 120/90-second initial TTLs. Exactly one single-key MIGRATE without REPLACE returned
`ValkeyServerException`, outer `ErrorCode=ERR`, and `DeliveryStatus=ReplyReceived`; the nested
destination error was BUSYKEY. This wrapping matches the
[Valkey 9.1.2 implementation](https://github.com/valkey-io/valkey/blob/9.1.2/src/cluster.c#L593-L605).
PING and binary ECHO then succeeded on that same physical connection. Source command statistics
confirmed one MIGRATE call; there was no replay or overwrite.

Independent node-local reads before and after rejection preserved both exact values and absolute
expirations, with zero PEXPIRETIME shift and positive TTL on each copy. All slot maps retained the
source owner. The same source-local and stationary sharded streams delivered binary sequences,
with zero losses, reconnect attempts, relocations, or local drops. Source/target shard registration
counts remained one/zero. Routed reads still returned only the source value, demonstrating why
reconciliation cannot rely on a routed GET alone.

The conflict was deliberately left unresolved until test teardown: no winner-selection policy,
REPLACE, or slot cutover was performed. Teardown deleted only the two known fixture copies,
drained/unsubscribed both streams, verified zero keys/channels/named clients, and removed all six
owned containers/two networks. Those deletions discard disposable test data, not production conflicts.
Evidence: `artifacts/resilience/busykey.trx`. The manual workflow was added but not dispatched;
see [runner controls](../how-to/run-busykey-tests.md).

This is received single-key conflict-rejection evidence, not server-to-server IOERR reconciliation,
bulk-transfer partial success, concurrent writers, a production merge policy, live TLS, or broader
version compatibility. No shipping library code or retry policy changed.

All 442 unit tests and 96 harness checks passed Debug/Release, `just ci` passed, and the two new
live cases skipped without opt-in. The existing successful-transfer regression also passed both
protocols in 38.715 seconds, with +1 ms/0 ms expiration shifts, one relocation each, and zero drops
(`artifacts/resilience/key-transfer-after-busykey.trx`). Across both live runs, twelve owned
containers and four networks were removed. The before/after inventory retained all existing Docker
containers/networks, and the three hostloom services remained healthy.

## Bounded resubscribe soak — 2026-09-05

`just test-resubscribe-soak` passed both RESP2/RESP3 cases on Valkey 9.1.2 in 182.409 seconds,
with no failures or skips. Each fresh three-primary cluster performed four warm-up and 30 measured
legacy empty-slot relocations: 68 successful relocations total. The client ran Release on Apple
M4 Max, macOS 26.6.2 arm64, SDK 10.0.400, and .NET 10.0.11; servers ran Linux aarch64 without TLS,
each limited to 128 MiB and one CPU. This was one sequential run, not a before/after benchmark.

Each cycle retained both original handles, completion tasks, and enumerators; delivered an exact
eight-byte binary sequence payload on each channel after recovery; and had zero local queue drops.
Every relocation required one reconnect attempt. The stationary channel reported no loss or attempt.
Settled per-node client counts matched the current slot owner on every cycle: nine named sockets
total, including three test inspectors, three publisher connections, the discovery seed, and two
subscription sockets. SHARDNUMSUB reported exactly two owner-local registrations total.
Final unsubscription reached EOF on both streams without extra buffered messages.

Resource samples include the fourth warm-up baseline and 30 measured cycles. All values below
are process-wide, including xUnit/output retention and Docker subprocess orchestration.

| Measurement | RESP2 | RESP3 |
|---|---:|---:|
| Relocation loop, including warm-up | 80.928 s | 82.575 s |
| Baseline post-GC heap | 2,782,592 bytes | 2,714,512 bytes |
| Maximum sampled post-GC heap | 2,782,592 bytes | 2,714,512 bytes |
| Final post-GC heap | 2,551,432 bytes | 2,714,248 bytes |
| Sampled working-set range | 107,642,880–122,585,088 bytes | 123,109,376–124,731,392 bytes |
| Sampled thread-pool threads | 6 | 6–7 |
| Maximum sampled queued work | 0 | 0 |

Both cases stayed within the 16 MiB post-GC growth smoke budget. macOS returned zero for process
handles, recorded as unsupported; the conditional +32 handle gate therefore supplies no local
handle-growth evidence. Working set increased and has no pass threshold. Queued work is not the
number of live tasks. Sampling follows recovery settlement, so transient connection peaks are not
measured. Full GC perturbs the workload, and these short runs do not exclude slow leaks.

Normal teardown verified zero keys, shard channels, and named application clients on every node,
then removed all six owned containers and two networks. The before/after Docker inventory retained
all existing containers and networks; the three existing hostloom services remained healthy.
Local evidence: `artifacts/resilience/resubscribe-soak.trx`. The manual workflow was added but not
dispatched. See [runner controls](../how-to/run-resubscribe-soak-tests.md).

All 442 unit tests and 93 harness checks passed in Debug and Release; `just ci` passed, builds
had no warnings, and both new live cases skipped without opt-in. No shipping library code changed.

This covers bounded, sequential sharded resubscription after slot-owner changes, not a reconnect
storm, hours-long soak, task-object counts, live TLS recovery, other server versions, standalone
subscriber restart, or server-to-server migration-error reconciliation. Publishing resumes only
after recovery; zero local drops is not a promise of replay or lossless delivery during an outage.

## Other recovery experiments

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

DNS-resolution faults, partitions, prolonged soak, and subscriber
server-restart restoration still need dedicated evidence. No transport retry policy was changed
to make the preceding experiments pass. Sharded primary-failover evidence is recorded below.

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
Atomic migration, nonempty-key transfer, forced ASK, TLS, and prolonged soak
remain separate evidence requirements. Primary failover and seed unavailability are covered below.
The [run guide](../how-to/run-slot-migration-tests.md)
describes ownership checks, opt-in controls, and cleanup limits.

## Primary-failover runner — 2026-09-05

`just test-failover` passed four Release cases on Valkey 9.1.2 with no failures or skips
(110.420 seconds total). Each case used a fresh three-primary cluster and one ready replica,
stopped only its verified primary with SIGKILL, and waited for natural promotion. The original
primary stayed stopped through all assertions; two cases used that primary as their only seed.
The host was macOS arm64, SDK 10.0.400 and .NET 10.0.11; Docker servers ran without TLS.

| Protocol | Seed | Promotion observed (ms) | Recovery attempts | Final check elapsed (ms) |
|---|---|---:|---:|---:|
| RESP2 | Surviving primary | 8,499 | 6 | 8,760 |
| RESP3 | Surviving primary | 8,553 | 7 | 9,687 |
| RESP2 | Stopped primary | 8,831 | 7 | 9,853 |
| RESP3 | Stopped primary | 7,457 | 6 | 7,936 |

Elapsed samples include Docker identity checks, server-election observation, and assertions; they
are not isolated recovery-latency measurements. Recovery used a 60-second deadline, 20 attempts,
and 500 ms initial / 2 second maximum backoff, not the default attempt budget.

Every case preserved the same handle, enumerator, and completion task; recorded one connection
loss and one successful recovery/relocation; resumed binary delivery; verified exactly one shard
registration on the promoted primary; and observed zero local queue drops. The unrelated channel
retained its connection and delivered before and after the fault. The publisher explicitly refreshed
through a surviving seed after promotion. No publications were attempted during the outage, and
no failed writes were replayed.

The initial four-case run failed. A diagnostic rerun showed a failed former primary still listed
as `master` before the promoted `online` master in `CLUSTER SHARDS`. Discovery had selected the
first master without inspecting health, exhausting reconnect attempts against the stopped endpoint.
The fix skips `fail`/`loading` primaries and treats an unavailable-primary map as transient only
for bounded subscription recovery. Unknown health remains terminal; initial discovery does not
install unavailable maps. This follows the [server health contract](https://valkey.io/commands/cluster-shards/).
Seven deterministic regressions and all 442 Debug/Release unit cases passed. Thirteen harness-only
checks passed, and all four fault cases skipped without opt-in. No retry limits, parser bounds,
TLS policy, or general command replay policy were relaxed. Maintainer review remains required
before release.

The passing TRX is `artifacts/resilience/failover.trx`; the pre-fix diagnostic failures and topology
snapshots are retained in `artifacts/resilience/failover-diagnostic.trx`. All sixteen containers and
four networks from the passing run were removed after ownership checks, with zero keys, shard
channels, or named application connections on surviving nodes before teardown. Failed-run resources
were also removed. Existing application containers and project networks were retained; images remain
cached. The manual **Primary failover** workflow was added but not dispatched.

The shared fixture also passed a post-fix migration regression: RESP2/RESP3, three moves each,
six successful relocations, zero drops, and cleanup of six additional containers/two networks
(29.023 seconds total). Its record is `artifacts/resilience/migration-after-failover.trx`.
Final Docker inventory contained no test resources; both original application container IDs and
all original application project-network IDs remained.

This verifies a single primary crash per case, not forced ASK, nonempty/atomic migration, partitions,
DNS failure, live TLS, other server versions, lock safety through failover, or prolonged resubscribe
soak. The [run guide](../how-to/run-primary-failover-tests.md) defines opt-in and cleanup controls.

## Native ASK migration — 2026-09-05

`just test-ask` passed RESP2/RESP3 against two fresh owned Valkey 9.1.2 three-primary clusters
(26.711 seconds total, no failures or skips). Host/runtime were macOS arm64, SDK 10.0.400 and
.NET 10.0.11; servers ran without TLS. Each node was limited to 128 MiB and one CPU.

The test held one slot in legacy IMPORTING/MIGRATING state with one binary key retained on the
source. Direct probes observed ASK at the source for the absent second key and MOVED at the
destination without ASKING. Each cluster client then completed SET/GET/GET for the second binary
key with exactly three additional source ASK errors and no additional destination MOVED errors,
demonstrating repeated ASKING and unchanged routing. A direct ASKING/GET pipeline succeeded and
the following unflagged GET was rejected, verifying one-shot admission. The retained source key
remained readable and all three topology views retained the source owner during migration.

Existing and newly established sharded subscriptions remained registered on the source, and the
original stream delivered without connection loss before cutover. After deleting the two known
test keys and verifying both nodes empty, destination-first cutover produced one loss, one attempt,
one recovery/relocation, and zero queue drops per original handle. The same enumerator and completion
task survived; binary delivery resumed, one destination registration replaced the source registration,
and the unrelated channel retained its connection. No runtime changes were needed for this increment.

This corrects the earlier blanket “live forced ASK outstanding” label: native command ASK now has
live evidence, but native sharded Pub/Sub stays on its source until cutover and does not naturally
exercise subscriber ASK. The server implementation explicitly distinguishes these paths in its
[routing logic](https://github.com/valkey-io/valkey/blob/9.1/src/cluster.c#L1126-L1139). Subscriber ASK
remains scripted compatibility coverage, not a claim that Valkey emitted that redirect live.

The initial record is `artifacts/resilience/ask-migration.trx`. A final run added an explicit zero
Pub/Sub ASK-error assertion and reran both ASK cases plus both three-cycle migration cases: all four
passed in 63.385 seconds (`artifacts/resilience/ask-and-migration.trx`). All 442 unit tests passed in
Debug and Release; 17 harness checks passed, and both new cases skipped without opt-in.
Cleanup verified zero keys, shard channels, and named application connections, then removed all
18 owned containers and six networks across both runs. Cached images and pre-existing application
resources were retained. The manual **ASK migration** workflow was added but not dispatched.

Nonempty-key MIGRATE, atomic migration, cross-version ASK behavior, live TLS, partitions, lock safety,
and long-duration soak remain unverified. These elapsed totals are test durations, not latency
benchmarks. The [run guide](../how-to/run-ask-migration-tests.md) specifies safety and cleanup limits.

## Nonempty-key migration — 2026-09-05

`just test-key-transfer` passed two Release cases on Valkey 9.1.2, RESP2 and RESP3, in 34.454 seconds
with no failures or skips. Each fresh three-primary cluster transferred two binary string keys
using two single-key MIGRATE calls: a 4 KiB byte-pattern value with a 120-second TTL, followed by a
small persistent value. Host/runtime were macOS arm64, SDK 10.0.400 and .NET 10.0.11, without TLS;
each Docker node was capped at 128 MiB and one CPU.

Both cases verified exact binary placement with node-local GETKEYSINSLOT, source removal, successful
ASK-routed reads of the transferred key, and TRYAGAIN for MGET spanning the transferred and retained
keys. The command connection remained usable after that server error. Values and expiration
metadata matched before transfer, between transfers, before cutover, and after cutover. Persistent
keys retained PTTL=-1; expiring keys retained positive TTL. Absolute expiration shifted by **+1 ms**
in both cases, within the explicit ±1 second tolerance for relative-TTL transfer/local clock skew.
This is metadata preservation evidence, not an observed expiry event or precise lock-lease proof.

Sharded delivery stayed on the source even after both data keys moved. Destination-first cutover
then produced one loss, one attempt, one successful recovery/relocation, and zero queue drops per
original handle. The same handle, enumerator, and completion task survived; one destination
registration replaced the source registration and the unrelated channel retained its connection.

The transfer helper accepts only fixture-namespaced binary keys of at most 512 bytes, derives the
destination from owned node indices, rechecks Docker identity/membership and migration markers,
and enforces a two-key total budget. It uses no COPY/REPLACE and never retries transfer failures.
MIGRATE has a two-second server idle timeout; client connect/recheck/transfer has a ten-second
deadline within the five-minute case budget. An I/O error can leave a copy at both nodes, so no
failure-reconciliation or atomic-slot-migration claim follows. See the
[MIGRATE contract](https://valkey.io/commands/migrate/).

All four known keys were deleted after verification. Every node had zero keys, shard channels, and
named application connections before ownership-checked cleanup removed six containers and two
networks. The record is `artifacts/resilience/key-transfer.trx`. All 442 unit tests passed in Debug
and Release, builds/formatting were clean, 20 harness checks passed, and both live cases skipped
without opt-in. No shipping code, dependency, parsing limit, or retry policy changed. The manual
**Nonempty key migration** workflow was added but not dispatched.

The shared-fixture regression passed both ASK cases and both three-cycle empty-slot cases in
66.056 seconds (`artifacts/resilience/migration-after-key-transfer.trx`), with twelve additional
containers and four networks cleaned up. Existing application resources were retained.

This is healthy single-key MIGRATE coverage for two strings per case. Bulk KEYS mode, other data
types, BUSYKEY/IOERR reconciliation, atomic migration, live TLS, cross-version faults, lock safety,
and prolonged soak remain separate evidence requirements. The
[run guide](../how-to/run-key-transfer-tests.md) defines the exact experiment and cleanup limits.

## Atomic slot migration — 2026-09-05

`just test-atomic-migration` passed both Release RESP2/RESP3 cases on Valkey 9.1.2 in 25.664 seconds,
with zero failures or skips. Each fresh three-primary cluster atomically moved one slot containing
two binary strings: a 4 KiB expiring value and a small persistent value. The host was macOS arm64,
SDK 10.0.400 and .NET 10.0.11; each node used a 128 MiB/one-CPU cap, without TLS.

Both source EXPORT and destination IMPORT jobs reached `success` with the same job identity,
verified node IDs, and exact single-slot range. RESP2 field arrays and RESP3 maps both passed.
The runner capability-probes the commands, refuses pre-existing jobs, sends MIGRATESLOTS once,
and polls under a 45-second deadline after membership preflight. Initial `OK` is not completion;
failed/cancelled jobs and unexpected identities fail without command replay. These checks follow
the [migration initiation contract](https://valkey.io/commands/cluster-migrateslots/) and
[job status contract](https://valkey.io/commands/cluster-getslotmigrations/).

Both cases verified exact destination key placement, empty source slot, agreement of all three
slot maps, byte-preserved values, positive expiring TTL, persistent PTTL=-1, and **zero milliseconds**
of PEXPIRETIME shift. The same sharded handle, enumerator, and completion task survived one loss,
one attempt, and one successful relocation with zero drops. Source/destination registrations,
resumed binary delivery, and the unrelated channel's unchanged connection passed.

All four test keys were deleted. Every node had zero keys, shard channels, and named application
connections before cleanup removed six owned containers and two networks. Evidence is
`artifacts/resilience/atomic-migration.trx`. All 442 unit tests and 40 harness checks passed in both
Debug and Release; both live cases skipped without opt-in. No shipping API, parser bounds, runtime
dependencies, or retry policy changed. The manual **Atomic slot migration** workflow was added,
not dispatched.

The shared-test legacy MIGRATE regression passed both protocols in 30.408 seconds, preserving
ASK/TRYAGAIN checks, +1 ms expiry shifts, and one relocation with zero drops. Its six additional
containers and two networks were cleaned up. Record: `artifacts/resilience/key-transfer-after-atomic.trx`.
The existing application containers and networks remained unchanged across both runs.

This is healthy completion evidence for one quiescent slot and two strings, not concurrent-write
atomicity, migration cancellation/failure recovery, other types or large datasets, cross-version
faults, TLS, lock-lease correctness, lossless Pub/Sub, performance, or prolonged soak. The
[run guide](../how-to/run-atomic-migration-tests.md) specifies the safety and cleanup limits.

## Pre-transfer atomic cancellation — 2026-09-05

`just test-atomic-cancellation` passed both Release RESP2/RESP3 cases on Valkey 9.1.2 in 26.280 seconds,
with no failures or skips. Each fresh three-primary cluster contained two source binary strings,
one expiring 4 KiB value and one persistent value. Host/runtime were macOS arm64, SDK 10.0.400,
and .NET 10.0.11; nodes were capped at 128 MiB and one CPU, without TLS.

A private administrative MULTI/EXEC transaction initiated a single-slot export, observed its active
job, cancelled it, and observed the same job in `cancelled` state. Every queue and execution reply
passed. An independent read retained the terminal state and PING succeeded on the same connection.
The transaction held asynchronous export progress until cancellation; this is pre-transfer evidence,
not cancellation racing with a completed job. The destination had no import job. Source cancellation
scope follows the [Valkey command contract](https://valkey.io/commands/cluster-cancelslotmigrations/).

Both cases retained exact source key placement and values, zero destination keys, positive TTL,
persistent PTTL=-1, and zero PEXPIRETIME shift. All three slot maps retained the source owner.
The same sharded handle, enumerator, and completion task continued binary delivery with zero losses,
attempts, relocations, or queue drops. Registrations remained source-local; the unrelated channel's
connection was unaffected.

The fixture refused pre-existing migration histories on all three nodes, verified owned Docker
identity/membership and the two-key budget, capability-probed commands, and never replayed the
transaction. Connection, capability checks, cancellation, and postchecks had a 30-second deadline
after membership preflight, within each five-minute case. No pause or debug setting was used.

All four known keys were deleted, every node had zero keys/shard channels/named application clients,
and six owned containers/two networks were removed. The record is
`artifacts/resilience/atomic-cancellation.trx`. All 442 unit tests and 51 harness checks passed in
Debug and Release; formatting/builds were clean and both new live cases skipped without opt-in.
No shipping code, parser bound, dependency, or retry policy changed. The manual **Atomic migration
cancellation** workflow was added but not dispatched.

The shared-code regression passed four healthy atomic/legacy MIGRATE cases across RESP2/RESP3
in 52.094 seconds, with zero failures or skips (`artifacts/resilience/migration-after-cancellation.trx`).
Atomic expiration shifts remained 0 ms, legacy shifts +1 ms, and every stream relocated once with
zero drops. Twelve additional containers and four networks were cleaned up. Existing application
containers and networks were retained across both runs.

Mid-transfer failure/late cancellation, partial-import cleanup, ambiguous EXEC outcomes, transfer-error
reconciliation, concurrent writes, TLS, other versions, and sustained soak remain unverified by this
run. This is not caller-token cancellation or lock-lease safety evidence. The
[run guide](../how-to/run-atomic-cancellation-tests.md) defines the exact experiment and safety limits.

## Post-snapshot atomic rollback — 2026-09-05

`just test-atomic-rollback` passed both Release RESP2/RESP3 cases on Valkey 9.1.2 in 25.422 seconds,
with zero failures or skips. Each fresh three-primary cluster contained two source binary strings,
a 4 KiB expiring value and a small persistent value. Host/runtime were macOS arm64, SDK 10.0.400
and .NET 10.0.11; nodes were capped at 128 MiB/one CPU, without TLS.

The source's local-only PREVENT-PAUSE debug hook held the export at `waiting-to-pause`. Both exact
provisional keys were observed at the destination with COUNTKEYSINSLOT=2 and a correlated active
IMPORT job. A direct destination GET still returned MOVED. Source values/TTL, all slot maps,
and binary sharded delivery passed while migration was held. This mirrors the hook used in the
[Valkey 9.1.2 tests](https://github.com/valkey-io/valkey/blob/9.1.2/tests/unit/cluster/cluster-migrateslots.tcl).

One exact export-client ID was resolved, rechecked, and closed per case. Both correlated jobs
became `failed`, destination slot-key count reached zero, and export/import clients disappeared.
No migration retry was attempted. Source binary values and original PEXPIRETIME survived (0 ms
shift), with positive expiring TTL and persistent PTTL=-1. Slot ownership and source registrations
remained unchanged. The same sharded handle/enumerator/completion task delivered after rollback
with zero losses, attempts, relocations, or drops; the unrelated channel was unaffected.

DEBUG was local-only in the separate owned rollback fixture and invoked inside the source container.
The hook was cleared in finally; normal fixtures retained DEBUG=no. The fixture overwrites inherited
debug-mode environment values, verifies owned identities/membership/two-key budget, and refuses
pre-existing migration histories. The client-ID parser rejects multiple records, duplicate fields,
invalid IDs, absent export flags, and input larger than 16 KiB. Admin work has a 45-second bound
after membership preflight; hook restoration has ten independent seconds, and fixture cleanup has
60 independent seconds. No shipping code, runtime dependency, parser bound, or retry policy changed.

All four known source keys were deleted; every node had zero keys/shard channels/named application
clients before six containers/two networks were removed. Evidence:
`artifacts/resilience/atomic-rollback.trx`. All 442 unit tests and 68 harness checks passed in Debug
and Release. Builds and formatting passed; both new live cases skipped without opt-in. The manual
**Atomic migration rollback** workflow was added, not dispatched.

The shared-fixture regression passed six legacy, healthy atomic, and early-cancellation cases
across both protocols in 77.961 seconds (`artifacts/resilience/migration-after-rollback.trx`).
All ordinary fixtures reported DEBUG=no. Eighteen more containers and six networks were removed;
the pre-existing hostloom services remained healthy and other application resources were unchanged.

This establishes provisional-import cleanup after a completed two-key snapshot and before cutover.
Interruption during a serialized value or partial snapshot, post-handoff failure, late administrative
cancellation, transfer-error reconciliation, concurrent writes, TLS, cross-version faults, lock safety,
and prolonged soak are separate evidence requirements. The
[run guide](../how-to/run-atomic-rollback-tests.md) defines the exact safety boundary.

## MIGRATE reply-loss reconciliation — 2026-09-05

`just test-migrate-reply-loss` passed both Release RESP2/RESP3 cases on Valkey 9.1.2 in 32.556 seconds,
with zero failures or skips. Each fresh three-primary cluster held two binary strings, a 4 KiB
expiring value and a small persistent value. Host/runtime were macOS arm64, SDK 10.0.400 and
.NET 10.0.11; nodes were capped at 128 MiB/one CPU, without TLS and with DEBUG disabled.

A single-use loopback relay forwarded HELLO, then confirmed and withheld exactly one MIGRATE
success reply before closing the connection. The caller received `ValkeyConnectionException` with
`MayHaveBeenSent`; subsequent PING on that physical client threw `ObjectDisposedException`.
Independent source/destination counts and exact binary key placement showed the expiring key
had transferred and the persistent key had not. ASK reads confirmed its bytes and TTL, while
mixed-key MGET returned TRYAGAIN. The lost-reply command was not replayed and no duplicate was deleted.

After reconciliation, a separate MIGRATE moved the remaining persistent key and normal cutover
relocated the original sharded handle/enumerator/completion task once, with one attempt and zero
drops. Source/target registrations, all slot maps, unrelated-channel isolation, and byte preservation
passed. Absolute expiration shifts were +1 ms in RESP2 and 0 ms in RESP3, within the ±1-second
legacy-transfer tolerance; persistent PTTL remained -1. This is metadata evidence, not a lease proof.

All four known keys were deleted and every node had zero keys/shard channels/named application
clients before six owned containers/two networks were removed. Evidence:
`artifacts/resilience/migrate-reply-loss.trx`. The legacy shared-code regression also passed both
protocols in 32.575 seconds, with +1 ms expiry shifts and six additional containers/two networks
cleaned up (`artifacts/resilience/key-transfer-after-reply-loss.trx`). Existing application resources
were retained. The manual **MIGRATE reply loss** workflow was added but not dispatched.

All 442 unit tests and 81 harness checks passed in Debug and Release; builds/formatting were clean
and both live cases skipped without opt-in. New relay checks cover binary forwarding, byte-at-a-time
success writes, unexpected replies, one-shot admission/arming, disposal, and both 64 KiB byte budgets.
The relay has two 4 KiB buffers, a ten-second lifetime, and bounded disposal. No shipping code,
runtime dependency, parser bound, or replay policy changed.

This covers client-facing loss after server success, not server-to-server IOERR, BUSYKEY, or
duplicate-copy reconciliation. The [MIGRATE contract](https://valkey.io/commands/migrate/) permits
different key placements after IOERR. Concurrent writers, other types, TLS, other versions, lock
safety, and sustained resubscribe soak remain separate. The
[run guide](../how-to/run-migrate-reply-loss-tests.md) specifies the experiment and safety limits.
