# Run isolated MIGRATE IOERR tests

Run from the repository root with local Docker available:

```bash
just test-migrate-ioerr
```

Allow three fresh Valkey 9.1 primaries per RESP2/RESP3 case, each capped at 128 MiB and one CPU.
The runner never accepts an existing endpoint, does not alter host networking, and leaves DEBUG
disabled. It tests the **source-only** outcome of a server-to-server read timeout before RESTORE.

## Interpret the checks

The test creates two owned binary string keys: a 4 KiB expiring value and a small persistent value.
It establishes source-local and stationary sharded streams, then opens legacy migration state.
On the owned importing destination, CLIENT PAUSE WRITE delays writes for at most 30 seconds while
allowing administrative observations. The source sends exactly one single-key MIGRATE with a
two-second server idle timeout, without COPY, REPLACE, or replay.

Require a uniquely identified blocked RESTORE-ASKING connection at the destination before the
source returns `ValkeyServerException` with `ErrorCode=IOERR` and `DeliveryStatus=ReplyReceived`.
Require that exact destination connection to disappear **before unpausing**. This distinguishes
an abandoned queued restore from a transfer that might execute after the pause ends. PING and
binary ECHO must then succeed on the same source-side command connection; INFO COMMANDSTATS
must report one MIGRATE call.

`ReplyReceived` describes receipt of the error, not key placement. The
[MIGRATE contract](https://valkey.io/commands/migrate/) allows a key at the source alone or at both
nodes following IOERR. This runner independently verifies the former; it does not infer it from
the error code. [CLIENT PAUSE](https://valkey.io/commands/client-pause/) normally resumes buffered
commands, which is why unpausing without checking the transfer socket would be insufficient.

After unpausing, require three destination GET/slot-count observations 100 ms apart to remain
empty, followed by full node-local placement checks. Each destination GET sends a fresh ASKING
in the same pipeline. The source must retain both exact values, the original absolute expiration,
positive TTL, and persistent-key TTL sentinel. Slot ownership, handles, completion, enumerators,
and binary sharded delivery must remain source-local with zero losses, reconnect attempts,
relocations, or local drops. The test performs no cutover or conflict winner selection.

## Safety and cleanup

The fixture rechecks owned container identity, resource limits, membership, migration markers,
source/target key counts, and exact source key membership before pausing or authorizing transfer.
The key must fit the owned namespace and 512-byte bound. The destination hostname is fixed to
the owned node, and client-list observations are capped at 16 KiB with unique positive IDs.

Each case has five minutes. After initial membership/marker preflight, fault and reconciliation
work share a 15-second deadline. Blocked-restore observation has two seconds; socket-close
observation has three seconds. A finally block unpauses the verified destination through a fresh
control connection under an independent five-second deadline, closes the source client, and
observes any outstanding transfer task. The finite pause also expires after process termination.
If cleanup or ownership validation fails, the test fails rather than claiming success.

Normal teardown deletes only the two known source keys, unsubscribes, disposes application clients,
and verifies zero keys, shard channels, and named clients. Ownership-checked fixture disposal removes
only its containers and empty network, including after assertion failures; unfinished slot markers
disappear with those disposable containers. Docker commands and fixture disposal have independent
60-second limits. Cached images remain. After forced termination or ambiguous ownership, inspect
the exact printed project before manual cleanup; never prune global Docker resources.

Ordinary tests skip unless `VALKEYDOTNET_RUN_MIGRATE_IOERR_TESTS=1`. The manual **MIGRATE IOERR**
workflow uploads `artifacts/resilience/migrate-ioerr.trx`. See the
[execution record](../reference/resilience-evidence.md) for measured scope. Lost RESTORE acknowledgments
leaving duplicate copies, bulk partial success, concurrent writers, TLS, and other server versions
remain separate experiments.
