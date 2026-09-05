# Run isolated bulk acknowledgment-loss tests

From the repository root with local Docker available:

```bash
just test-bulk-ack-loss
```

Each RESP2/RESP3 case creates three fresh Valkey 9.1 primaries and one test-only .NET relay on
their owned Docker network. Primaries have 128 MiB/one CPU each; the relay has 64 MiB/one CPU/64 PIDs,
a non-root user, read-only root, one read-only output-directory bind mount, and no published ports
or capabilities. DEBUG stays off. Image pin and host prerequisites match the
[single-key relay runner](run-restore-ack-loss-tests.md).

## Interpret the checks

The source starts with two distinct same-slot binary keys, containing 4 KiB and five-byte values
with initial 120-second TTLs. The target starts empty. Source-local and stationary sharded streams
prove normal delivery before opening legacy importing/migrating markers.

One two-key MIGRATE batch is sent to the relay, without COPY, REPLACE, auth, or replay. The relay
validates SELECT 0 and both expected RESTORE-ASKING frames before forwarding anything. It forwards
the real SELECT and first RESTORE success replies, then receives and withholds the second RESTORE
success reply. It keeps the sender socket open until the source's two-second idle timeout closes it.
Require the fixed phase log `RESTORE_ACK_FORWARDED`, `RESTORE_ACK_WITHHELD`, `SENDER_CLOSED` and zero
relay exit status; an error or premature close is not the intended experiment.

Require IOERR with `DeliveryStatus=ReplyReceived`, usable same-client PING/binary ECHO, and exactly
one MIGRATE call in source command statistics. The
[Valkey per-key acknowledgment path](https://github.com/valkey-io/valkey/blob/9.1.2/src/cluster.c#L579-L650)
can delete an acknowledged key before a later reply times out. Do not infer all-key rollback or
replay safety from the received batch error.

Reconcile each key independently in two observations: the first must exist only at the destination;
the second must retain identical copies at both nodes. Require exact binary values and key sets,
source/target counts 1/2, unchanged source expiration for the second key, positive TTLs, and stable
destination expirations within one second of the originals. Every importing-node read sends fresh
ASKING in the same pipeline. Direct source GET for the moved key returns ASK, routed GET finds it,
routed GET for the second key sees the source copy, and mixed-key MGET returns TRYAGAIN.

All slot maps stay source-owned. Original handles, completion tasks, and stream enumerators must
deliver binary messages with zero losses, attempts, relocations, or drops. No winner selection,
overwrite, replay, or cutover occurs. Synchronous MIGRATE stalls source commands during the timeout;
post-fault delivery is not a claim of uninterrupted low-latency publishing.

## Bounds and cleanup

The relay accepts one sender connection, closes its listener, and lives at most 30 seconds. It is
not a general RESP proxy: exactly SELECT 0 and one or two RESTORE-ASKING commands, exact ordered keys
of at most 512 bytes, TTL 1–120000 ms, and payload 1–8192 bytes. Parser limits remain 8 KiB per bulk,
16 KiB per flat command, and arity two/four; the two-key mode permits at most three command frames
(48 KiB conservative framing ceiling) and reads three five-byte success replies. Extra transfer
bytes are rejected, not forwarded. Encoded CLI keys are length-checked before decoding.

The fixture rechecks nonce-labelled identities, exact owned network membership, migration markers,
and source/target database counts and key sets before authorizing transfer. Only the two exact
same-slot owned keys are accepted; no pre-existing target copy is allowed in this mode. Existing
ordinary migration helpers keep their two-copy limit; reconciliation checks the three known copies.

Each case has five minutes. Fault work after relay start has 20 seconds. Relay removal in finally
has an independent 15-second deadline, with root fixture disposal as a fallback. Docker commands
and independent cluster disposal have 60-second limits. Ownership ambiguity fails closed for manual
inspection. Final teardown deletes only the three exact fixture copies, unsubscribes, checks zero
keys/shard channels/named clients, and removes the owned containers and empty network. Images stay
cached. After forced termination, inspect the exact printed project; never prune global resources.

Without `VALKEYDOTNET_RUN_BULK_ACK_LOSS_TESTS=1`, both live cases skip. The manual **Bulk RESTORE
acknowledgment loss** workflow uploads `artifacts/resilience/bulk-ack-loss.trx`. See the
[execution record](../reference/resilience-evidence.md) for measured scope. Concurrent writers,
other loss positions, larger batches, other data types, TLS, other server versions, and a production
conflict-resolution policy remain separate work. Shipping APIs, dependencies, and retry behavior
are unchanged.
