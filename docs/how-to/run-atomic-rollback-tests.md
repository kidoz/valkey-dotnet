# Run isolated atomic migration rollback tests

Run from the repository root with local Docker available:

```bash
just test-atomic-rollback
```

Allow three fresh Valkey 9.1 containers per RESP2/RESP3 case, each capped at 128 MiB and one CPU.
The runner uses random loopback ports and temporary storage, never an existing endpoint.

## Interpret the checks

Each case establishes binary sharded delivery and two same-slot source strings: a 4 KiB expiring
value and a small persistent value. The fixture enables local-only DEBUG at startup and applies
`DEBUG SLOTMIGRATION PREVENT-PAUSE 1` through valkey-cli inside the verified source container.
This upstream test hook holds the migration before write pause and ownership cutover, while
ordinary commands and Pub/Sub continue. It does not pause the server process. See the
[Valkey 9.1.2 migration tests](https://github.com/valkey-io/valkey/blob/9.1.2/tests/unit/cluster/cluster-migrateslots.tcl).

After MIGRATESLOTS starts one job, require the export to reach `waiting-to-pause` and the target
to report a matching active IMPORT. GETKEYSINSLOT must show both exact provisional binary keys on
the target, and COUNTKEYSINSLOT must report two. A direct GET must still return MOVED; the target
does not yet own the slot. Verify source values, TTL, all slot maps, and delivery while held.

The fixture then resolves the sole export client with a read-only `CLIENT LIST FLAGS E`, rechecks
its ID and Docker ownership, and sends `CLIENT KILL ID` for that exact ID once. Require both
correlated jobs to reach `failed`, target key count to reach zero, and export/import clients to
disappear. Source data and original absolute expiration must survive. All three slot maps must
retain the source owner, and the original sharded handle/enumerator/completion task must continue
delivery with zero losses, attempts, relocations, or queue drops. Check the unrelated channel too.

This is **post-snapshot, pre-cutover link-failure rollback**. Both snapshot keys have arrived; it is
not a test of interruption midway through a serialized value, a partially received snapshot,
post-handoff failure, late administrative cancellation, or lock safety. Valkey's
[atomic migration overview](https://valkey.io/topics/atomic-slot-migration/) describes the phases.

## Safety and cleanup

The fixture accepts only two distinct, owned-prefix keys of at most 512 bytes in the same slot.
It verifies container identities, resource limits, loopback ports, all cluster members, exact
source/target key counts, empty migration histories, and local-only debug configuration. It
capability-probes the migration commands. The PREVENT-PAUSE hook and export-client flags are
Valkey-specific test facilities; incompatible builds fail rather than falling back to a timing race.

The export-client selector has a 16 KiB input bound and requires one record, a positive numeric
ID, unique fields, and the export flag. No broad client-kill command, server stop, shared-server
mutation, or automatic migration replay is used. DEBUG stays disabled in ordinary fixtures;
the runner overwrites inherited debug-mode environment values and refuses the replica/debug combination.
DEBUG is invoked only inside the container, not through the published host port.

Each case has a five-minute deadline. Administrative connections, capability checks, migration,
verification, and rollback have 45 seconds after membership preflight. A finally block clears
PREVENT-PAUSE with an independent ten-second deadline. Outer fixture disposal still removes the
owned cluster if this fails. Docker commands and independent cleanup each have 60-second limits.

Normal teardown deletes only the two known source keys, unsubscribes, and verifies zero keys,
shard channels, and named application clients before removing verified containers and their empty
network. Cached images remain. After forced termination or ownership/Docker failure, inspect the
exact project printed in the output before manual cleanup; never prune global Docker resources.

The manual **Atomic migration rollback** workflow uploads `artifacts/resilience/atomic-rollback.trx`.
Ordinary tests skip unless `VALKEYDOTNET_RUN_ATOMIC_ROLLBACK_TESTS=1`. See
[executed evidence](../reference/resilience-evidence.md) for results and remaining gaps.
