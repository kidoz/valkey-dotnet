# Run isolated atomic slot-migration tests

Run from the repository root with local Docker available:

```bash
just test-atomic-migration
```

Allow three disposable Valkey 9.1 containers, each capped at 128 MiB and one CPU. Each RESP2/RESP3
case creates a fresh three-primary cluster on random loopback ports with temporary data storage.
The runner never accepts an existing endpoint or externally supplied destination.

## Interpret the checks

The test establishes binary sharded delivery, then migrates one slot containing a 4 KiB expiring
string and a small persistent string. It probes both participating nodes for MIGRATESLOTS and
GETSLOTMIGRATIONS support and skips if either command is absent. Version text is recorded as
evidence, not used as the capability gate.

A single `CLUSTER MIGRATESLOTS SLOTSRANGE slot slot NODE target-id` starts the job. `OK` confirms
initiation, not completion. The runner polls both nodes until their EXPORT and IMPORT entries have
the same job identity and both report `success`. It validates the source, destination, and exact
single-slot range; failed/cancelled jobs, extra jobs, or changing identities fail the case.
RESP2 alternating fields and RESP3 maps are checked separately. See the
[initiation contract](https://valkey.io/commands/cluster-migrateslots/) and
[job-status contract](https://valkey.io/commands/cluster-getslotmigrations/).

After completion, all three slot maps must agree on the destination. Exact binary keys and values
must survive, the source must contain no slot keys, the persistent key must retain PTTL=-1, and
the expiring key must retain positive TTL and exactly the original PEXPIRETIME. The same sharded
handle, enumerator, and completion task must survive one relocation; source/destination registration
counts, resumed binary delivery, zero queue drops, and unrelated-channel isolation must pass.
This checks a quiescent two-string migration, not concurrent-write atomicity or lossless Pub/Sub.

## Safety and cleanup

Before initiation, the fixture rechecks owned Docker identities, resource limits, loopback ports,
cluster membership, and the exact two-source/zero-target key budget. Both nodes must have empty
migration histories. The destination comes only from the verified owned node ID. There is no legacy
fallback, command replay, or broad migration-cancellation command after an error or timeout.

Each case has a five-minute deadline. Connect, capability checks, initiation, job polling, and
post-migration health checks share a 45-second deadline after membership preflight. Subscriber
recovery has a 30-second/ten-attempt budget. Docker subprocesses have 60-second limits.

Normal teardown deletes only the two known keys, unsubscribes, and verifies zero keys, shard channels,
and named application connections on every node. Independent 60-second cleanup removes only the
verified owned containers and their empty network, including when a job fails or times out. Cached
images remain. After forced termination or an ownership/Docker failure, inspect the exact project
printed in the output before manual cleanup; never prune global Docker resources.

The manual **Atomic slot migration** workflow uploads `artifacts/resilience/atomic-migration.trx`.
Ordinary tests skip unless `VALKEYDOTNET_RUN_ATOMIC_MIGRATION_TESTS=1`. Migration cancellation,
transfer-error reconciliation, concurrent writes, other data types, bulk migration, TLS, other server
versions, and sustained soak remain separate work. See
[executed evidence](../reference/resilience-evidence.md).
