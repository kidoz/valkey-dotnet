# Run isolated MIGRATE BUSYKEY tests

Run from the repository root with local Docker available:

```bash
just test-busykey
```

Allow three fresh Valkey 9.1 primaries per RESP2/RESP3 case, each capped at 128 MiB and one CPU.
The runner never accepts an existing endpoint and leaves DEBUG disabled.

## Interpret the checks

The test creates one owned binary key at the source with a 4 KiB value and a 120-second TTL.
After opening legacy migration state, it uses a same-connection ASKING/SET pipeline to create
the identical key at the importing destination with a different five-byte value and a 90-second
TTL. Both SET commands use NX; the conflict is intentional and limited to fresh fixture data.

Require one single-key MIGRATE without REPLACE, COPY, or KEYS to fail with a fully received server
reply. Valkey wraps the destination BUSYKEY in an outer ERR; require `ValkeyServerException`,
`ErrorCode=ERR`, and `DeliveryStatus=ReplyReceived`. The same physical client must still answer
PING and binary ECHO correctly. INFO COMMANDSTATS must show exactly one MIGRATE call.
This matches the [Valkey 9.1.2 error path](https://github.com/valkey-io/valkey/blob/9.1.2/src/cluster.c#L593-L605),
not a transport timeout or an ambiguous missing reply.

Read both nodes independently before and after rejection. Require the same exact binary key,
distinct unchanged values, exact original PEXPIRETIME on both nodes, and positive PTTL. Send a
fresh ASKING with each destination read in one pipeline. A routed GET alone is insufficient:
it sees the source value while hiding the conflicting destination copy.

Require unchanged source ownership on all three nodes and uninterrupted binary delivery on the
original source-local sharded stream and a stationary channel. Handles, completion tasks, and
enumerators remain unchanged, with zero losses, reconnect attempts, relocations, and local drops.

Treat the result as an **unresolved conflict**, not permission to choose a winner. The test performs
no automatic retry, REPLACE, slot cutover, or production conflict-resolution policy. This is not
server-to-server IOERR/duplicate-copy recovery or bulk MIGRATE partial-success coverage. See the
[MIGRATE contract](https://valkey.io/commands/migrate/) for the distinct I/O-error outcome.

## Safety and cleanup

Immediately before transfer, the fixture checks owned container identities, resource limits,
three-primary membership, migration markers, one key at each node, and exact binary key identity.
Keys must fit the owned namespace and 512-byte bound; the destination is fixed to the owned node.
The transfer has a two-second server idle timeout and a ten-second client/preflight deadline after
the initial membership checks. Each case has a five-minute deadline. Docker commands and independent
fixture cleanup have 60-second limits.

Only after all conflict-preservation assertions pass, normal teardown deletes the exact two fixture
copies independently, unsubscribes, disposes application clients, and verifies zero keys, shard
channels, and named clients on every node. Those deletions discard test data; they do not resolve
a production conflict. Removing the owned containers also discards their unfinished migration state.
Failed assertions still invoke ownership-checked fixture disposal. Cached images remain.

After forced termination or ambiguous ownership, inspect the exact printed project before manual
cleanup; never prune global Docker resources. Ordinary tests skip unless
`VALKEYDOTNET_RUN_BUSYKEY_TESTS=1`. The manual **MIGRATE BUSYKEY** workflow uploads
`artifacts/resilience/busykey.trx`. See the [execution record](../reference/resilience-evidence.md)
for measured results and limitations.
