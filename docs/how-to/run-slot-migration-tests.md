# Run isolated slot-migration tests

Use this explicitly opt-in runner to verify established sharded subscriptions across real slot
ownership changes. Confirm that your local Docker daemon can host three disposable Valkey 9.1
primaries, each limited to 128 MiB and one CPU. Existing servers and configured application endpoints
are not accepted as migration targets.

From the repository root:

```bash
just test-migration       # three moves per protocol, each in its own fresh cluster
just test-migration 1     # one move per protocol
```

The runner exercises RESP2 and RESP3. Cycles must be 1–20; each protocol has a five-minute deadline.
Each Docker subprocess has a 60-second ceiling, within the test deadline. Image download can exhaust that
budget; pre-pull `valkey/valkey:9.1` if needed. Remote TCP/SSH Docker endpoints are rejected; the
checked local Unix-socket or named-pipe endpoint is frozen for the experiment.

The GitHub **Slot migration** workflow runs the same tests through manual dispatch only. It never
runs on push or pull request. TRX output, project names, server information, per-cycle recovery
counters, and cleanup confirmation are collected in `artifacts/resilience/migration.trx`.

## Interpret the test

The test proves binary SPUBLISH delivery before changing a slot, then alternates that empty slot
between two primaries. It uses the documented legacy IMPORTING/MIGRATING/NODE sequence from the
[Valkey cluster specification](https://valkey.io/topics/cluster-spec/#legacy-slot-migration).
There are no stored keys to copy with MIGRATE. Each move verifies that:

- the existing handle, enumerator, and completion task survive recovery;
- recovery/loss/relocation counters advance and another binary message arrives;
- the target has one shard registration and the source has none;
- an unrelated channel on the third primary remains connected and delivers messages;
- final unsubscribe drains both streams, with no keys, shard channels, or named application
  connections left before cleanup.

The test does not publish during the recovery gap and does not claim replay of missed messages.
It does not force or prove an ASK response, exercise nonempty-key migration, atomic slot migration,
promote a replica, test unavailable-seed recovery, or establish TLS or prolonged-soak evidence.
Per-cycle elapsed time includes Docker inspection/admin commands; it is not a recovery-latency benchmark.

## Ownership and cleanup

The fixture creates a random `valkey-dotnet-migration-tests-…` Compose project with random,
loopback-only published ports and temporary data filesystems. A port collision fails creation
without stopping the existing listener. Before topology mutation it checks exact container IDs,
project/service/token labels, image, hostname, resource bounds, port bindings, and the three-node
cluster membership. Endpoint mapping refuses nodes outside the fixture.

Cleanup has its own 60-second deadline, including after failed assertions or cancellation. It
removes only exact verified container IDs, then an ownership-verified empty network. Cached images
remain. If Docker is unavailable, ownership changes, or the process is forcibly terminated, cleanup
may require manual work. Inspect the exact project and labels reported in the test output before
removing resources; never use a global Docker prune.

For current execution status, see [migration evidence](../reference/resilience-evidence.md#slot-migration-runner).
