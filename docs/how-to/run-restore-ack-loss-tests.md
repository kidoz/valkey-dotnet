# Run isolated RESTORE acknowledgment-loss tests

From the repository root with local Docker available:

```bash
just test-restore-ack-loss
```

Each RESP2/RESP3 case creates three fresh Valkey 9.1 primaries (128 MiB/one CPU each) and one
test-only .NET relay (64 MiB/one CPU). The relay lives on their owned Docker network, with no
published ports, host networking, privileged access, or added capabilities. It uses a read-only
filesystem, non-root user, 64-PID limit, and one read-only bind mount of the built relay output.
The runtime image is pinned to
`mcr.microsoft.com/dotnet/runtime:10.0@sha256:a365ce6a50b09176855d085c69da3fc1204a48432e36087e9a208f6e5860e235`.
The runner may pull that image; Docker must be able to read the workspace bind mount.

## Interpret the checks

The source has one binary key with a 4 KiB value and a 120-second initial TTL; the destination
starts empty. Legacy importing/migrating markers are established without changing slot ownership.
The source sends exactly one single-key MIGRATE to the relay, without COPY, REPLACE, or replay.

The relay accepts only SELECT 0 followed by RESTORE-ASKING for that exact key. It forwards the
destination's first success reply, then waits for and deliberately withholds the real RESTORE
success reply. The sender socket stays open until the source's two-second idle timeout closes it.
Closing early would exercise a different [server retry path](https://github.com/valkey-io/valkey/blob/9.1.2/src/cluster.c#L671-L698).
Require the source to receive IOERR with `DeliveryStatus=ReplyReceived`, remain usable for PING,
and report exactly one MIGRATE call in command statistics. Require the relay's fixed phase log
and successful exit to establish that RESTORE actually succeeded before the acknowledgment loss.

The [MIGRATE contract](https://valkey.io/commands/migrate/) permits source-only or duplicate-copy
placement after IOERR. Do not infer placement from the received error. This runner independently
checks the **duplicate-copy** outcome: exact node-local key membership and identical binary values
at both nodes in two observations, a fresh ASKING for each importing-node read, unchanged source
absolute expiration, and stable destination expiration. Relative-TTL transfer permits a one-second
source/destination expiry tolerance; measured shifts belong in the execution record.

All slot maps and routed reads must remain source-owned. The original source-local and stationary
sharded handles, completion tasks, and enumerators survive, deliver binary sequences, and report
zero losses, reconnect attempts, relocations, or local drops. There is no replay, cutover, overwrite,
or conflict winner selection. The synchronous source MIGRATE stalls source commands during its
timeout; delivery after the fault is not a claim of uninterrupted low-latency publishing.

## Bounds and cleanup

This is a single-connection fault tool, not a general RESP proxy. Its parser accepts only flat
arrays of two/four bulk strings, up to 8 KiB per bulk and 16 KiB per command, with strict CRLF and
unsigned length framing. Both commands are validated before either is forwarded. The fixed
RESTORE key is at most 512 bytes, TTL is 1–120000 ms, and payload is 1–8192 bytes. Replies must be
exact +OK frames. No scripts, credentials, alternate destinations, or arbitrary commands are accepted.
The listener closes after accepting one sender; process lifetime is capped at 30 seconds.

The fixture verifies its nonce-labelled containers, network membership, slot markers, exact key
membership/counts, and relay image/command/isolation before fault injection. Each case has five
minutes; fault work after container start has a 20-second deadline. Relay removal has an independent
15-second deadline in finally, with root fixture disposal as a fallback. Docker commands and cluster
disposal have independent 60-second limits. Ownership ambiguity fails closed for manual inspection.

Teardown deletes only the two exact fixture copies, unsubscribes, and verifies zero keys, shard
channels, and named clients. Only the owned containers and empty network are removed; images stay
cached. Forced termination may need manual cleanup of the exact printed project; never prune Docker.

Without `VALKEYDOTNET_RUN_RESTORE_ACK_LOSS_TESTS=1` the live cases skip. The manual **RESTORE
acknowledgment loss** workflow uploads `artifacts/resilience/restore-ack-loss.trx`. See the
[execution record](../reference/resilience-evidence.md) for measured scope. Bulk partial success,
concurrent mutation, TLS, other server versions, and a production conflict-resolution policy remain
separate work. No shipping library dependency, API, or retry behavior is changed by this test utility.
