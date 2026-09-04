# Script execution

`ValkeyScript` stores UTF-8 Lua source and its protocol-mandated SHA-1 identifier. Its constructor
rejects null or whitespace source. `Sha1` exposes the lowercase identifier used by `EVALSHA`.

## API

Both `ValkeyClient` and `ValkeyClusterClient` expose:

```csharp
Task<RespValue> ExecuteScriptAsync(
    ValkeyScript script,
    IReadOnlyList<ValkeyArgument> keys,
    IReadOnlyList<ValkeyArgument> arguments,
    CancellationToken cancellationToken = default);

Task<RespValue> ExecuteScriptWithDeadlineAsync(
    ValkeyScript script,
    IReadOnlyList<ValkeyArgument> keys,
    IReadOnlyList<ValkeyArgument> arguments,
    TimeSpan timeout,
    CancellationToken cancellationToken = default);
```

Keys and arguments are separate RESP arguments. Their bytes are borrowed: backing buffers must remain
unchanged until execution completes. Standalone scripts may have zero keys. Cluster scripts require
at least one key, route from the first key, and reject keys spanning multiple slots before writing.

`ValkeyScript.CreateCommand(keys, arguments)` produces a plain `EVAL` command for generic execution
and pipelines. It always includes source, so pipeline entries need no script-cache recovery and retain
the existing positional error behavior.

## Cache recovery

The fast path sends one `EVALSHA`. Only a `NOSCRIPT` error enters recovery. A connection-owned gate
serializes recovery for that script, including independently constructed objects with identical
source. The caller rechecks with `EVALSHA` inside the gate: another caller may already have loaded
the script. If the recheck also returns `NOSCRIPT`, it falls back to `EVAL` once. A successful recheck
is the result of that caller's invocation; it is never followed by another execution.

Recovery therefore sends at most two `EVALSHA` attempts and one `EVAL` per node attempt. Runtime
errors, permission errors, cancellation, timeouts, and transport failures are propagated. Even an
`EVAL` fallback that returns `NOSCRIPT` is not repeated. Repeated external cache flushes can require
another caller to reload later; the client does not loop indefinitely trying to stabilize the cache.

Coordination uses 16 lazily allocated semaphore stripes per physical connection, with no retained
dictionary of scripts. Unrelated scripts sharing a stripe serialize only on the recovery path.
Callers waiting for recovery support cancellation and the explicit deadline. There is no additional
limit on application callers; applications control concurrency as with the generic command API.

There is no persistent local assumption that a server has a script loaded. Each replacement
connection uses its own recovery gates. `SCRIPT FLUSH`, server cache eviction, and a newly routed
primary are discovered through `NOSCRIPT`. The server cache is volatile, as documented by
[Valkey](https://valkey.io/topics/eval-intro/).

## Cluster redirects and deadlines

Recovery happens on the physical connection selected by the cluster client. Each attempt on an
`ASK` target pipelines a fresh `ASKING` with that attempt, including the `EVAL` fallback. `MOVED`
updates the slot owner through the normal bounded redirect path. A failed transport is never replayed.

The explicit deadline covers recovery waits and all command attempts; the cluster version also
covers connection acquisition and redirects. An expired in-flight attempt retains its FIFO slot
under `ResponseDrainTimeout`. After an earlier attempt was sent, later recovery timeouts conservatively
report `MayHaveBeenSent`. Caller cancellation during I/O remains connection-fatal. See
[error handling](../how-to/handle-errors.md).

## Script contract

Lua source is trusted application code; application data belongs in `KEYS` and `ARGV`, never in
interpolated Lua. Every accessed database key must be declared in `KEYS`. Scripts must not fabricate
reserved routing or cache errors (`MOVED`, `ASK`, `NOSCRIPT`) after performing writes: those errors
instruct the driver to take another attempt. Ordinary runtime errors can follow partial script
effects and do not make retry safe.
