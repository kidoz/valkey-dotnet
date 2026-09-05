# Run isolated nonempty-key migration tests

Run the explicitly opt-in test from the repository root:

```bash
just test-key-transfer
```

Allow three disposable Valkey 9.1 containers, each capped at 128 MiB and one CPU. Each RESP2/RESP3
case creates a fresh three-primary cluster using random loopback ports and temporary data storage.
The fixture never accepts an existing endpoint or external MIGRATE destination.

## Interpret the checks

The test creates two binary keys in one hash slot: a 4 KiB byte-pattern string with a 120-second
TTL, and a small persistent string. It records the expiring key’s absolute expiration time,
verifies both values, and establishes sharded delivery before opening legacy migration state.

Two single-key MIGRATE calls transfer the expiring key first and the persistent key second.
Between calls, node-local GETKEYSINSLOT verifies exact binary placement, a direct source read
observes ASK, and mixed-key MGET reports TRYAGAIN. Individual cluster-client reads still return
the correct bytes. The command connection remains usable after TRYAGAIN. The same sharded stream
continues delivering from the source until destination-first cutover, then relocates once to the
destination; the unrelated channel retains its connection.

At each phase, byte values must match, the persistent key must report PTTL=-1, and the expiring key
must still have positive TTL. PEXPIRETIME may drift by at most one second from the original value,
allowing for relative-TTL transfer and local clock skew. This verifies expiration metadata, not
waiting until expiry or proving a precise distributed lease deadline. See the
[MIGRATE contract](https://valkey.io/commands/migrate/) and
[PEXPIRETIME reference](https://valkey.io/commands/pexpiretime/).

Cutover is refused unless the source has zero slot keys and the destination has the expected two.
After cutover, values/TTL, all three slot maps, unchanged handle/enumerator/completion task, one
destination registration, zero source registrations, and zero queue drops must pass. The test then
deletes only its two known keys and unsubscribes before ownership-checked cleanup.

## Safety and limits

Transfer keys must be at most 512 bytes and start with the fixture’s unique namespace. Node
identities, resource limits, loopback endpoints, cluster membership, exact key counts, and
importing/migrating markers are verified before transfer. MIGRATE uses a derived owned hostname,
port 6379, database zero, and a two-second server idle timeout. It never sends COPY or REPLACE.
A ten-second client deadline bounds connection, endpoint rechecks, and transfer after membership
preflight; the server idle timeout alone is not a total-duration limit. An IOERR or timeout fails
the test without replay: the key could then exist at both nodes. Do not interpret this runner as
migration-failure reconciliation logic.

Each case has a five-minute deadline. Subscriber recovery uses 30 seconds and ten attempts;
Docker subprocesses have 60-second limits. Cleanup gets an independent 60 seconds and removes
only verified container IDs and their empty owned network. Before normal teardown, all nodes
must have zero keys, shard channels, and named application connections. Cached images remain.
After forced termination or an ownership/Docker failure, inspect the exact project from the output
before manual cleanup; never prune global Docker resources.

The manual **Nonempty key migration** workflow uploads `artifacts/resilience/key-transfer.trx`.
Ordinary tests skip these cases unless `VALKEYDOTNET_RUN_KEY_TRANSFER_TESTS=1`. Bulk KEYS mode,
other value types, transfer-error reconciliation, atomic slot migration, TLS, cross-version behavior,
and prolonged soak are separate work. See [resilience evidence](../reference/resilience-evidence.md).
