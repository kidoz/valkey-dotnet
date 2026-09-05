# Run isolated ASK-migration tests

Use the opt-in runner against newly owned local Docker clusters:

```bash
just test-ask
```

Allow capacity for three disposable Valkey 9.1 primaries, each limited to 128 MiB and one CPU.
The two RESP2/RESP3 cases each create their own cluster, bind random loopback ports, and store data
in temporary filesystems. No configured application endpoint or existing project is accepted.

## What the runner verifies

The test retains one binary key on the source and holds its slot in legacy IMPORTING/MIGRATING
state. A direct GET for a missing key must receive ASK naming the owned destination. A direct
destination GET without ASKING must receive MOVED. Three cluster-client operations (SET, GET, GET)
must succeed with binary data, produce three additional source ASK errors, and produce no additional
destination MOVED errors. This checks fresh ASKING on each request and unchanged routing. An
independent ASKING/GET pipeline followed by a rejected GET checks that ASKING is one-shot.

Sharded Pub/Sub is deliberately different: Valkey keeps channels on the source until SETSLOT NODE,
so native SSUBSCRIBE/SPUBLISH do not produce ASK in this intermediate state. The existing stream
must deliver without reconnecting, a new subscription must register on the source, and all three
slot maps must still identify that source. See the
[Valkey routing implementation](https://github.com/valkey-io/valkey/blob/9.1/src/cluster.c#L1126-L1139).

The runner deletes only its two known keys, verifies that both sides are empty, and completes
cutover destination-first. The original sharded handle, enumerator, and completion task must
survive one relocation, resume binary delivery, and have exactly one destination registration and
none at the source. The unrelated third-primary channel must retain its connection. Final cleanup
checks zero keys, shard channels, and named application connections.

## Bounds and cleanup

Each case has a five-minute deadline; subscriber recovery uses 30 seconds and at most ten attempts.
Docker subprocesses have a 60-second ceiling, and cleanup has an independent 60-second deadline.
The shared migration fixture verifies exact container IDs, ownership tokens, local daemon, resource
limits, ports, and cluster membership before slot changes. Cleanup removes only verified containers
and their empty owned network; cached images remain. On forced termination or Docker/ownership
failure, inspect the exact project from the output before manual cleanup; never use global pruning.

The manual **ASK migration** workflow uploads `artifacts/resilience/ask-migration.trx`. Normal tests
skip these cases unless `VALKEYDOTNET_RUN_ASK_TESTS=1`. This is command ASK and native Pub/Sub
cutover coverage, not a live subscriber-ASK injection, nonempty-key MIGRATE, atomic migration, TLS,
other server versions, or prolonged soak. Subscriber-ASK handling remains covered by scripted
loopback tests. See [resilience evidence](../reference/resilience-evidence.md) for executed results.
