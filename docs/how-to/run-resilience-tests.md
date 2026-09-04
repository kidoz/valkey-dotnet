# Run isolated restart tests

This is a disruptive, explicitly opt-in test. It creates its own disposable Valkey container, stops
and starts that container repeatedly, then removes its Compose project. It never accepts an
existing container, project, or `VALKEYDOTNET_ENDPOINT` as a restart target.

Before running it, confirm that your local Docker daemon can host a disposable 128 MiB / one-CPU
container. The runner accepts only a local Unix-socket or local named-pipe Docker endpoint; remote
SSH/TCP contexts are rejected. It freezes the checked daemon endpoint for the experiment.

From the repository root:

```bash
just test-resilience                 # Valkey 9.1, three restarts for each of RESP2 and RESP3
just test-resilience 8.1 10           # ten restarts per protocol on Valkey 8.1
just test-resilience 7.2 25           # twenty-five per protocol on Valkey 7.2
```

Only Valkey 7.2, 8.1, and 9.1 are accepted. Cycles must be 1–100. Each protocol case has a ten-minute
abort deadline; more cycles do not extend it. The first image pull needs an available network and
can exhaust the bounded Docker-command startup time; pull the selected official image beforehand
if necessary.

The runner uses `dev/docker-compose.resilience.yml`, a random
`valkey-dotnet-resilience-tests-…` project, a loopback-only port, a 16 MiB temporary data filesystem,
and disabled persistence. A port collision fails creation instead of stopping the existing listener.
Before stop, start, or cleanup, it validates the exact container ID, project, service, random
ownership token, image, and host-port binding.

TRX results, including per-cycle resource samples, are written to
`artifacts/resilience/restart-<version>.trx`. The GitHub **Resilience** workflow provides the same
experiment through manual dispatch and uploads its TRX result. It does not run automatically on
push or pull request. Ordinary integration runs skip the restart cases.

## Interpret results

Each case proves a working connection and cached script before stopping the server. While it is
stopped, an ordinary write must fail with `NotSent`. After restart, the same owner must complete
16 concurrent echo operations with correctly matched replies, recover script execution, show a new
server run ID, and show that the offline write was not replayed. Exactly one named owner connection
must be visible, and its logical active-operation gauge must return to zero.

Post-cycle samples include post-GC managed heap, process handle count, thread-pool thread count,
and pending thread-pool work. The test asserts that the post-GC heap stays within 16 MiB and handles
within 32 of the first completed cycle. Those intentionally broad smoke-test budgets catch large
regressions; they do not prove leak freedom. Thread-pool values are recorded, not asserted. A
one-cycle run establishes only a baseline. This is not a latency/throughput benchmark or a
long-duration production soak.

## Cleanup and failures

Normal completion, failed assertions, and test cancellation dispose the owner and tear down the
validated project with an independent cleanup deadline. Downloaded images remain in Docker's cache.
If Docker becomes unavailable, the test process is forcibly terminated, or ownership validation
fails, automatic cleanup can be incomplete. The output identifies the exact generated project.
Inspect that project and its ownership labels before manually removing it; do not use a global
Docker prune or target another project's containers.

The ordinary server-free suite also runs 32 loopback connection-loss cycles per protocol with
16 concurrent callers. It verifies failure settlement, reply matching, and one connection per
cycle. A separate bound-but-not-listening socket test covers startup failure and recovery; the OS
may report connection refusal or timeout. These are scripted/local-kernel tests, not real-server
restart evidence.

See [resilience evidence](../reference/resilience-evidence.md) for executed versus pending coverage.
