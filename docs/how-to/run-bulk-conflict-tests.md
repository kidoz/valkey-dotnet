# Run isolated bulk MIGRATE conflict tests

From the repository root with local Docker available:

```bash
just test-bulk-conflict
```

Allow three fresh Valkey 9.1 primaries per case, each capped at 128 MiB and one CPU. Four cases
cover RESP2/RESP3 with the conflict first and last in the batch. The fixture accepts no existing
endpoint, publishes only loopback ports, and leaves DEBUG disabled. No relay or network fault is used.

## Interpret the checks

The source begins with two distinct binary keys in one slot: an expiring 4 KiB value and a small
persistent value. After opening importing/migrating markers, the target gets a different five-byte
value for the persistent key, with a 90-second TTL. SET NX prevents fixture setup from overwriting
any existing key. Source-local and stationary sharded streams prove normal delivery before transfer.

Exactly one `MIGRATE ... "" 0 2000 KEYS key1 key2` is sent, without COPY, REPLACE, auth, or replay.
Require outer ERR containing BUSYKEY, `DeliveryStatus=ReplyReceived`, successful PING/binary ECHO
on the same physical client, and one MIGRATE call in source command statistics.

A received batch error is not evidence of all-key rollback. The
[Valkey 9.1.2 implementation](https://github.com/valkey-io/valkey/blob/9.1.2/src/cluster.c#L579-L619)
handles individual restore replies and removes acknowledged keys even when another key conflicts.
Require independent key membership, slot counts, and database counts to change from source/target
2/1 to 1/2 in both batch orders. Verify the successful key exists only at the destination, while
both conflicting copies retain their distinct bytes and original TTL semantics. Inspect each copy
twice, with a fresh ASKING for every importing-node operation; do not use routed GET to infer the
hidden target conflict.

Require the moved key's absolute expiration to remain within one second of its original value and
stable between observations. The target conflict's absolute expiration must remain exact with a
positive TTL, and the source conflict must remain persistent. Direct source GET for the moved key
must return ASK; routed GET must find the moved value while routed GET for the conflict sees the
source copy. Mixed-key MGET must return TRYAGAIN. All slot maps must retain the source owner.

Original handles, completion tasks, and stream enumerators must deliver binary messages after the
batch, with zero losses, reconnect attempts, relocations, or local drops. Do not select a conflict
winner, retry the failed batch, or perform cutover. This is an observation/reconciliation test, not
a production migration API or a conflict-resolution policy.

## Bounds and cleanup

Only two distinct keys of at most 512 bytes each in the fixture's namespace and the same slot are
accepted. The helper verifies container identity, exact cluster membership, migration markers,
all three database counts, and exact source/target key sets before authorizing a batch. Only this
fixed-shape path permits three physical copies; ordinary migration helpers retain their two-copy
limit. Destination hostname and port come from the owned fixture, never caller input.

Each case has five minutes. Membership preflight, batch execution, and initial reconciliation share
a 15-second deadline; MIGRATE has a two-second server idle timeout. Docker operations and independent
fixture disposal have 60-second limits. Ambiguous ownership aborts rather than expanding cleanup.

Normal teardown deletes only the three known fixture copies, unsubscribes, and checks zero keys,
shard channels, and named clients before removing the owned containers and empty network. On failure,
ownership-checked fixture disposal still removes those disposable resources. Images stay cached.
After forced termination, inspect the exact printed project before manual cleanup; never prune Docker.

The live cases skip unless `VALKEYDOTNET_RUN_BULK_CONFLICT_TESTS=1`. The manual **Bulk MIGRATE conflict**
workflow uploads `artifacts/resilience/bulk-conflict.trx`. See the
[execution record](../reference/resilience-evidence.md) for measured scope. Bulk IOERR/reply loss,
concurrent mutation, larger batches, other data types, TLS, other versions, and production winner
selection remain separate work. No shipping API, dependency, parsing bound, or retry policy changes.
