# Run isolated writes queued across atomic cutover

Run from the repository root with local Docker available:

```bash
just test-cutover-writes
```

Each RESP2/RESP3 case creates three fresh Valkey 9.1 primaries, capped at 128 MiB/one CPU each,
with temporary storage and random loopback ports. Existing endpoints are never accepted.

## Interpret the checks

The test retains two binary strings in one slot: a 4 KiB expiring value and a five-byte persistent
value. Two independent routed clients each own one ordered writer. Original moving/stationary
sharded subscription streams are established before migration.

The shared owned debug fixture first holds EXPORT at `waiting-to-pause`, verifies a correlated
active IMPORT and both exact provisional destination keys, and acknowledges one paired update
(sequence 1). Values, expiration, and persistence are checked before proceeding.

The target's local-only `DEBUG SLOTMIGRATION PREVENT-FAILOVER 1` then holds the ownership handoff
while the source's PREVENT-PAUSE hook is released. Require the source to report migration-induced
write pause and the same export to reach `failover-granted`, with its import still active. This
controlled stage follows the [upstream queued-client migration test](https://github.com/valkey-io/valkey/blob/9.1.2/tests/unit/cluster/cluster-migrateslots.tcl#L1273-L1316).

Submit sequence-2 SET XX KEEPTTL updates through both routed clients. Read-only CLIENT LIST ID
inspection must identify both exact original connection IDs, the owned client name, command SET,
and blocked flags. Both operations must still be pending before the target hook is released.
Require both final replies to be OK and verify both sequence-2 values before a later update could
conceal their loss. Cluster redirect handling remains bounded; no application/transport replay
is added. Require both correlated migration jobs to succeed and all slot maps to name the target.

Finally acknowledge and verify a paired sequence-3 update after cutover. All three phases retain
the original absolute expiration and persistent PTTL=-1. Common teardown checks target-only key
placement, original sharded-stream relocation/delivery, and zero moving/stationary queue drops.

Each attempted write is classified as acknowledged, not sent, ambiguous, received error,
unexpected reply/failure, or cancellation without delivery status. Typed cancellation retains its
delivery ambiguity. A healthy run requires all six updates acknowledged; any other outcome fails
the test and is never silently retried. Failure cancellation drains both writers before leaving
their scope and logs only fixed outcome labels, not payloads or exception messages.

## Bounds and cleanup

The existing fixture enforces owned key prefixes, two distinct same-slot keys of at most 512 bytes,
exact container/member identities, empty migration histories, and capability probes. DEBUG is
local-only and called inside verified containers. Ordinary fixtures keep DEBUG disabled.

Each case has five minutes; held migration setup and completion share 45 seconds after membership
preflight. Pause observation, queued writes, release, and immediate verification share five seconds.
The source's default 5000 ms manual-failover configuration is checked and never extended. Each
hook has independent ten-second finally restoration; outer ownership-checked disposal still removes
the cluster if restoration fails. Docker commands and fixture cleanup have 60-second limits.

Normal cleanup deletes only the two known keys, unsubscribes, checks zero keys/shard channels/named
clients, and removes verified owned containers/network. Cached images remain. Never use global
Docker pruning to clean up these tests.

This is two queued writes crossing one healthy atomic handoff, not sustained contention, same-key
writers, simultaneous transport failure, ambiguous-result reconciliation, short-lease correctness,
uninterrupted publishing, a performance benchmark, TLS, or cross-version certification.

The manual **Atomic cutover queued writes** workflow uploads `artifacts/resilience/cutover-writes.trx`.
Normal tests skip unless `VALKEYDOTNET_RUN_CUTOVER_WRITES_TESTS=1`. See the
[resilience evidence](../reference/resilience-evidence.md) for executed results.
