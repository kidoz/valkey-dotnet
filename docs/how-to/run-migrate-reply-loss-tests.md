# Run isolated MIGRATE reply-loss tests

Run from the repository root with local Docker available:

```bash
just test-migrate-reply-loss
```

Allow three fresh Valkey 9.1 containers per RESP2/RESP3 case, each capped at 128 MiB and one CPU.
The runner also owns one temporary loopback TCP relay. It never accepts an existing server endpoint.

## Interpret the checks

The test creates two binary string keys in one slot and establishes sharded delivery. It opens
legacy migration state, then sends one MIGRATE for the 4 KiB expiring key through a private proxy
connection to the source. After HELLO finishes, the relay is armed to withhold exactly `+OK\r\n`.
It confirms the complete success reply arrived, discards it, and closes the connection. The actual
server-to-server transfer has completed, but the calling client has not received that result.

Require `ValkeyConnectionException` with `DeliveryStatus=MayHaveBeenSent`; a subsequent PING on
the invalidated physical client must throw `ObjectDisposedException` before writing. Reconciliation
uses independent node-local key counts and exact binary placement, followed by ASK-routed reads
and value/TTL checks. It never replays that MIGRATE or deletes a supposed duplicate to resolve the
uncertainty. In this controlled case the transferred key is only at the destination, while the
second key remains at the source.

After those checks pass, the test transfers the distinct persistent key and completes the already
planned slot cutover. Mixed-key MGET must report TRYAGAIN during the intermediate phase; individual
reads and sharded delivery remain usable. The original stream must relocate once at cutover with
zero queue drops, while the unrelated channel retains its connection. Expiring PEXPIRETIME has
the same ±1-second tolerance as the [legacy transfer test](run-key-transfer-tests.md).

This is **client-facing reply loss after server success**, not server-to-server IOERR, BUSYKEY,
or duplicate-copy reconciliation. An IOERR can leave the key at both nodes or only at the source,
and requires separate observations and policy. See the [MIGRATE contract](https://valkey.io/commands/migrate/).
The test does not add a production migration/reconciliation API or automatic replay policy.

## Safety and cleanup

Before transfer the fixture validates owned container identities, cluster membership, migration
markers, source/target key counts, and the bounded owned-prefix key. The destination is the owned
node hostname; the relay connects only to the verified source's loopback port. It accepts one
connection and then closes its listener, with no reconnect path. A command's replay is never used
as a probe for its prior outcome.

The relay uses two fixed 4 KiB buffers, a 64 KiB total byte budget per direction, a ten-second
lifetime, and a five-second disposal wait. It forwards handshake bytes unchanged, checks success
across fragments, and fails on malformed or unexpected replies instead of claiming the fault
was injected correctly. It does not record command contents or credentials. DEBUG remains disabled.

Each case has a five-minute deadline; connection, fault injection, and initial count reconciliation
have ten seconds after membership/marker preflight. MIGRATE has a two-second server idle timeout.
Normal teardown deletes only the two known keys, unsubscribes, verifies zero keys/shard channels/
named application clients, and removes the owned containers and empty network. Docker commands
and independent fixture cleanup have 60-second limits. Cached images remain.

After forced termination or ownership/Docker failure, inspect the exact printed project before
manual cleanup; never prune global Docker resources. The manual **MIGRATE reply loss** workflow
uploads `artifacts/resilience/migrate-reply-loss.trx`. Ordinary tests skip unless
`VALKEYDOTNET_RUN_MIGRATE_REPLY_LOSS_TESTS=1`. See
[executed evidence](../reference/resilience-evidence.md) for results and limitations.
