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
