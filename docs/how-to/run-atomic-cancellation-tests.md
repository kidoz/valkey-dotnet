# Run isolated atomic migration cancellation tests

Run from the repository root with local Docker available:

```bash
just test-atomic-cancellation
```

Allow three fresh Valkey 9.1 containers per RESP2/RESP3 case, each capped at 128 MiB and one CPU.
The runner uses random loopback ports and temporary storage, never an existing endpoint.

## Interpret the checks

Each case establishes binary sharded delivery and two source keys in one slot: a 4 KiB expiring
string and a small persistent string. On a private administrative connection, one MULTI/EXEC
transaction initiates MIGRATESLOTS, reads its active job, cancels it, and reads its terminal job.
The transaction prevents asynchronous export progress between those operations. The test checks
every OK/QUEUED response and every EXEC element, then verifies the same job is still `cancelled`
with an independent query. RESP2 field arrays and RESP3 maps are both exercised.

Require zero destination import jobs, both exact binary keys still on the source, byte-preserved
values, positive expiring TTL, persistent PTTL=-1, and unchanged PEXPIRETIME. All three slot maps
must retain the source owner. The original sharded handle, enumerator, and completion task must
remain usable with no loss, reconnect attempt, relocation, or queue drop. Source/destination
registrations and delivery on an unrelated channel are also checked. PING on the administrative
connection proves it exited transaction state and remains usable.

This is deliberate **pre-transfer cancellation**, not a race against a fast completed migration,
mid-transfer rollback, cancellation of a caller's CancellationToken, or recovery from an ambiguous
EXEC result. See the [server cancellation contract](https://valkey.io/commands/cluster-cancelslotmigrations/)
and [Valkey 9.1.2 migration implementation](https://github.com/valkey-io/valkey/blob/9.1.2/src/cluster_migrateslots.c).

## Safety and cleanup

CANCELSLOTMIGRATIONS affects all exports initiated on its source node. The fixture first verifies
owned Docker identities, resource limits, loopback ports, all cluster members, and exactly two
source/zero destination keys. It refuses pre-existing migration histories on any of the three
nodes. The single target ID comes from the verified cluster; no arbitrary cancellation endpoint
or job is accepted. Missing commands cause a capability-based skip before mutation.

The transaction is never replayed on error, timeout, or cancellation. It has no competing callers;
failure disposes that private connection and the owned cluster. No pause, debug option, or server
configuration change is used. Each case has a five-minute deadline; connection/capability checks,
the transaction, and post-cancellation verification have 30 seconds after membership preflight.
Docker commands and independent ownership-checked cleanup each have 60-second limits.

Normal teardown deletes only the two known keys and unsubscribes, checks zero keys, shard channels,
and named application connections on every node, then removes verified containers and their empty
owned network. Cached images remain. After forced termination or ownership/Docker failure, inspect
the exact project printed in the output before manual cleanup; never prune global Docker resources.

The manual **Atomic migration cancellation** workflow uploads `artifacts/resilience/atomic-cancellation.trx`.
Ordinary tests skip unless `VALKEYDOTNET_RUN_ATOMIC_CANCELLATION_TESTS=1`. Partial-transfer failures,
late cancellation, error reconciliation, concurrent writes, TLS, cross-version faults, and soak
remain separate evidence requirements. See [executed evidence](../reference/resilience-evidence.md).
