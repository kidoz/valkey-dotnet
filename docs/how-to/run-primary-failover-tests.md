# Run isolated primary-failover tests

Use this explicitly opt-in runner to verify sharded subscription recovery after losing a primary.
Confirm that your local Docker daemon can host four disposable Valkey 9.1 containers, each capped
at 128 MiB and one CPU. Existing servers and configured application endpoints are never accepted
as fault targets.

From the repository root:

```bash
just test-failover
```

The four cases cover RESP2 and RESP3 with either a healthy discovery seed or the failed primary
itself as the seed. Every case creates a fresh three-primary cluster plus one replica of the
subscribed primary. After validating replication readiness, cluster membership, and container
ownership, it stops that primary with SIGKILL. The replica must win a server election; the runner
never issues a forced failover or recreates the subscription handle. See the
[Valkey replica-election specification](https://valkey.io/topics/cluster-spec/#replica-election-and-promotion).

## Interpret the results

Before the stop, the moving channel and an unrelated channel must deliver binary messages. After
the stop, the test requires all surviving nodes' slot maps to identify the former replica as the
new owner, its ROLE to be primary, and the cluster to be healthy. The same subscription handle,
enumerator, and completion task must survive, with one recorded loss and one successful relocation.
Binary delivery must resume, the promoted primary must have exactly one local shard registration,
and the unrelated channel must record no connection loss. The original primary stays stopped
through these checks, including both unavailable-seed cases.

The subscriber uses a 60-second recovery budget, at most 20 attempts, and 500 ms initial / 2 second
maximum backoff. This is not verification of every default retry budget. The publisher explicitly
refreshes its routing through a surviving seed after promotion; no failed publication is replayed.
No messages are published during the outage, so this does not measure missed deliveries or promise
replay, durable delivery, or cache-state reconstruction.

When diagnosing a failure, distinguish server promotion from subscriber recovery: promotion timing
and reconnect attempts are recorded separately. A failed former primary can remain in `CLUSTER
SHARDS` with role `master`; discovery must use its health field when choosing the new owner.

## Safety, limits, and cleanup

The runner reuses the migration fixture's `failover` Compose profile. Its random project starts
with `valkey-dotnet-failover-tests-`; ports bind only to loopback, persistence is disabled, and data
lives in temporary filesystems. Only local Unix-socket or named-pipe Docker endpoints are accepted;
the checked endpoint and selected Compose profile are fixed for the run.

Each case has a five-minute deadline. Replica readiness and promotion each have 45-second limits;
Docker subprocesses have a 60-second ceiling. Cleanup has an independent 60-second deadline and
removes only ownership-verified container IDs and an empty owned network, including the stopped
primary. Surviving nodes must have no keys, shard channels, or named application connections before
normal cleanup. Cached images remain. If ownership changes, Docker is unavailable, or the process
is forcibly terminated, inspect the exact project in the output before manual cleanup; never prune
global Docker resources.

The manual **Primary failover** workflow runs these cases and uploads
`artifacts/resilience/failover.trx`. Ordinary test runs skip the disruptive cases unless
`VALKEYDOTNET_RUN_FAILOVER_TESTS=1`. This is one primary crash per case, not prolonged soak,
partition/DNS fault coverage, multi-node failure, TLS, or cross-version certification.
See [resilience evidence](../reference/resilience-evidence.md) for executed results.
