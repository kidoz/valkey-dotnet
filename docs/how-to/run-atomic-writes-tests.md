# Run isolated atomic migration concurrent-writer tests

Run from the repository root with local Docker available:

```bash
just test-atomic-writes
```

Allow three fresh Valkey 9.1 primaries per RESP2/RESP3 case, each capped at 128 MiB and one CPU,
with temporary storage and random loopback ports. No existing endpoint is accepted.

## Interpret the checks

Each case establishes two same-slot binary keys (a 4 KiB expiring string and a five-byte persistent
string) and moving/stationary sharded streams. The owned debug fixture applies local-only
`DEBUG SLOTMIGRATION PREVENT-PAUSE 1` inside its verified source container. This hook is used by
the [upstream migration tests](https://github.com/valkey-io/valkey/blob/9.1.2/tests/unit/cluster/cluster-migrateslots.tcl);
it holds cutover without stopping ordinary command processing.

After starting exactly one migration, require an EXPORT in `waiting-to-pause`, a correlated active
IMPORT, and both exact provisional destination keys. Destination GET must still return MOVED.
Then issue 32 rounds of paired `SET XX KEEPTTL` calls through two independent routed clients,
one writer per key, with at most two pending writes. Each value retains binary null/CRLF/255 bytes
and carries its sequence in the last byte. Every round must acknowledge both writes with OK before
the next starts. There is no retry. Require the final sequence-32 bytes, unchanged absolute expiry,
persistent PTTL=-1, source-owned maps, and sharded delivery before releasing the hook.

Recheck that the same jobs remain held/nonterminal, release the hook, and require both correlated
jobs to succeed. Verify target-only placement and exact final acknowledged bytes, original
PEXPIRETIME, positive TTL, and persistence. All slot maps must name the target. The original
sharded handle/enumerator/completion must survive one relocation and deliver again; the unrelated
stream must remain connected. Neither stream may report local queue drops.

This is bounded **post-snapshot, pre-write-pause mutation** evidence. The writers are logically
concurrent; the test does not measure overlapping server execution, throughput, latency, or memory.
It does not cover same-key contention, writes spanning cutover, failed/ambiguous writes, simultaneous
faults, uninterrupted publishing, short lock leases, other data types, TLS, or other versions.

## Safety and cleanup

The shared held-migration helper retains the [rollback runner's ownership controls](run-atomic-rollback-tests.md):
two distinct owned-prefix keys of at most 512 bytes in one slot, exact container/member identities,
empty job histories, capability probes, and explicit DEBUG=local. Ordinary fixtures force DEBUG=no;
replica/debug combinations are rejected. No export client is killed in this test.

Each case has five minutes. After membership preflight, connection setup, snapshot observation,
all 64 writes, and migration completion share a 45-second deadline. A finally block clears the hook
with an independent ten-second deadline, including after failed setup. Fixture disposal still
removes the owned cluster if restoration fails. Docker commands and independent cleanup each have
60-second limits. Normal teardown deletes only the two known keys, unsubscribes, and verifies zero
keys, shard channels, and named application clients before removing verified containers/network.
Cached images remain. Never prune global Docker resources to clean up a failed case.

Ordinary tests skip unless `VALKEYDOTNET_RUN_ATOMIC_WRITES_TESTS=1`. The manual **Atomic migration
concurrent writes** workflow uploads `artifacts/resilience/atomic-writes.trx`. See the
[execution record](../reference/resilience-evidence.md) for observed results and remaining gaps.
