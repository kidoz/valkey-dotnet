# Run isolated resubscribe soak tests

Run from the repository root with local Docker available:

```bash
just test-resubscribe-soak
```

Allow three fresh Valkey 9.1 primaries per RESP2/RESP3 case, each capped at 128 MiB and one CPU.
The runner never accepts an existing server endpoint. Use an optional count for a longer run:

```bash
just test-resubscribe-soak 100
```

Choose 20–100 measured relocations per protocol (default 30). Each case also performs four warm-up
relocations. The fixed 15-minute case deadline remains in effect at every count: a slower run can
time out instead of completing all requested cycles. This is a bounded, cycle-based soak, not an
hours-long stability certification or a throughput benchmark.

## Interpret the checks

The test repeatedly moves one empty slot between two primaries using legacy SETSLOT. A binary
sharded channel follows that slot; a second channel stays on the third primary. Keep the same two
handles, completion tasks, and async enumerators throughout. Every cycle must restore the moving
stream, deliver an exact eight-byte binary sequence payload on each channel, and leave the other
stream connected. The test publishes after recovery, not during its expected delivery gap.

Require one successful relocation and connection loss per cycle, bounded reconnect attempts, two
retained handles, and zero local queue drops. The two queues each have capacity eight; no background
publisher or unbounded sample collection is used. Final unsubscription drains both enumerators to
EOF, also catching unexpected buffered duplicate messages.

After each relocation, allow at most five seconds for server-side socket closure to settle. Require
exactly nine named connections across the cluster: three inspectors, three warmed publisher
connections, one retained discovery seed, and two dedicated shard sockets. Per-node counts must
match the current owner, and SHARDNUMSUB must report one registration per channel only at its owner.
Temporary recovery sockets are not sampled during connection establishment.

The fourth warm-up cycle sets the resource baseline. Each subsequent cycle records post-full-GC
managed heap, working set, process handles, thread-pool threads, and queued work. Require heap growth
of at most 16 MiB from baseline; require at most 32 additional handles when both samples are positive.
Treat zero handle counts as unsupported, not as evidence of zero handles. Working set and thread-pool
figures are diagnostic only; queued work is not a count of live tasks. These process-wide samples
include xUnit, retained test output, and Docker subprocess orchestration. Forced GC also perturbs the
workload. Use a dedicated process and the exact method filter for comparable measurements.

## Safety and cleanup

Every move reuses the owned-cluster fixture's container identity, resource-limit, endpoint mapping,
membership, and empty-slot checks. Only the printed fresh project is mutated. DEBUG remains disabled;
there are no replica stops, global CLIENT KILL commands, or external endpoints.

Each recovery uses a 30-second/ten-attempt budget with a 35-second observation deadline; message
delivery has five seconds. Docker commands and independent fixture cleanup have 60-second limits.
On success, require zero keys, shard channels, and named clients after disposing all application
connections. The fixture removes only its verified containers and empty network, including on a
failed assertion. Cached images remain. After forced process termination or ambiguous ownership,
inspect the exact printed project before manual cleanup; never prune global Docker resources.

Ordinary tests skip unless `VALKEYDOTNET_RUN_RESUBSCRIBE_SOAK=1` is set. The optional count comes from
`VALKEYDOTNET_RESUBSCRIBE_CYCLES` and is validated before creating Docker resources. The manual
**Resubscribe soak** workflow accepts the same count and uploads
`artifacts/resilience/resubscribe-soak.trx`. See the
[execution record](../reference/resilience-evidence.md) for measured scope and remaining gaps.
