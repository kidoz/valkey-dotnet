# Run bounded concurrent recovery tests

This is an explicitly opt-in disruptive test. It creates one fresh owned standalone Valkey server
per RESP protocol and closes only eight exact, freshly verified test-client IDs. It never accepts
an existing endpoint/container and does not stop or restart the server.

After confirming that your local Docker daemon may host disposable fault-test containers:

```bash
just test-concurrent-recovery       # 20 measured plus two warm-up cycles per protocol
just test-concurrent-recovery 100   # maximum bounded run, still five minutes per protocol
```

The manual **Concurrent recovery** workflow performs the same experiment on isolated runners after
server-free correctness gates. No automatic push/PR run injects these faults. Results and resource
summaries go to `artifacts/resilience/concurrent-recovery.trx`.

## Experiment contract

Four independent connection owners and four independent subscribers share one standalone server.
Each owner permits 16 admitted operations, with a 64-entry physical pending limit; each subscriber
has one binary channel/retained stream, an eight-message queue and one lifecycle operation slot.
Two additional physical clients control the experiment and sample resources. All ten connections
have exact fixture-derived names; worker connections use database 1 and the requested RESP protocol.

Before a cycle, binary Pub/Sub delivery and restored connection settings must pass. Container
ownership/limits and the exact client name/ID/database/protocol/subscription set are checked again.
A pipeline closes the eight worker IDs only. Before proceeding, all four owners must be disconnected
and all four subscribers must have observed loss without completing recovery. A configured one-second
subscriber backoff ceiling (500–1000 ms equal jitter) creates observable overlapping recovery windows;
this is not proof of simultaneous TCP connect syscalls or certification of the default backoff.

One start gate releases 64 distinct binary ECHO calls, 16 per owner. There is no transport replay:
calls begin only after the old owner sockets are known to be disconnected. Every reply must match
its caller. Each subscriber must restore the original stream, deliver the exact post-recovery
payload and report one loss/attempt/reconnect per cycle, with zero drops. No delivery is promised
during the disconnected interval. All eight worker IDs must change; server accepted-connection
statistics must increase by exactly eight, catching extra short-lived connections between samples.

This tests idle socket loss followed by concurrent acquisition and subscription recovery. It does
not inject failure into in-flight writes or measure ambiguous mutation outcomes, outage retries,
cluster failover/migration, tracking restoration, live TLS or cross-version restart behavior.

## Resource evidence and limits

A separate persistent client samples CLIENT LIST and process resources approximately every 20 ms
through fault/recovery. Each sample must have at most ten server-visible connections and at most
64 owner logical active operations; after each cycle these must settle to ten and zero respectively.
All 64 tracked command tasks and the sampler task are joined, including abort paths. Subscriber
queues must have zero drops and drain on unsubscribe.

The summary records sampled client/active-owner-operation maxima, live managed heap, working set,
process handles when supported, thread-pool threads and queued work. A zero handle count is reported
as unsupported. Sampling can miss short-lived peaks. The admitted-operation gauge is process-wide,
not physical FIFO occupancy; harness task counts are not counts of every library/runtime task.
Thread-pool work is not a task-object count. No process-wide transient socket/handle/task maximum
or leak-free claim follows from this experiment.

Post-GC managed heap and supported settled handle counts use the second warm-up cycle as baseline.
The smoke budgets are +16 MiB heap and +32 handles. Live heap, working set and thread-pool maxima are
recorded rather than assigned portable thresholds. All measurements include test-runner/harness
work, and full GC between cycles changes workload behavior. Even the maximum cycle count is bounded
resource evidence, not a prolonged production soak or throughput/latency benchmark.

## Abort and cleanup

The owned fixture accepts local unix/npipe Docker transports only, freezes daemon selection, verifies
container ID/name/nonce/image/limits/mounts/loopback binding, and uses `valkey/valkey:9.1` with one CPU,
128 MiB, 64 PIDs, read-only root and 16 MiB tmpfs. Persistence and DEBUG are disabled. It creates no
network and never prunes global resources. Existing endpoint environment variables are not targets.

Cycle counts outside 20–100 fail before creating a server. Each protocol has five minutes, each cycle
15 seconds, subscriber recovery ten seconds/one attempt, command deadlines five seconds, and client
observations two seconds. Failure, identity drift, extra connections, wrong replies or resource-budget
breach abort the case; the sampler and command tasks are settled before disposal. Normal completion
verifies empty channels, DBSIZE=0 and only the two inspectors remaining, then closes them. Independent
ownership-checked container removal has a 60-second cleanup budget on success and failure. Cached
images remain. Docker unavailability/ownership drift or forced process termination may need manual
inspection of the printed exact owned project; never substitute a shared target or global prune.

See [resilience evidence](../reference/resilience-evidence.md) for executed versus pending coverage.
