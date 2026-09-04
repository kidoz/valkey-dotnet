using System.Diagnostics;

namespace ValkeyDotNet;

public sealed partial class ValkeyClient
{
    // Fixed-size, connection-owned stripes coordinate even distinct script objects with the same
    // source. No dictionary retains an unbounded collection of caller-provided scripts.
    private readonly SemaphoreSlim?[] _scriptRecoveryGates = new SemaphoreSlim?[16];

    /// <summary>Executes EVALSHA with coordinated, bounded NOSCRIPT recovery on this connection.</summary>
    public Task<RespValue> ExecuteScriptAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(script);
        return ExecuteScriptCoreAsync(
            script,
            script.CreateCommand(keys, arguments, useHash: true),
            false,
            null,
            cancellationToken
        );
    }

    /// <summary>Applies one isolated deadline across script execution, recovery waits, and fallback.</summary>
    public Task<RespValue> ExecuteScriptWithDeadlineAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(script);
        return ExecuteScriptCoreAsync(
            script,
            script.CreateCommand(keys, arguments, useHash: true),
            false,
            ValidateOperationTimeout(timeout),
            cancellationToken
        );
    }

    internal async Task<RespValue> ExecuteScriptCoreAsync(
        ValkeyScript script,
        ValkeyCommand hashCommand,
        bool asking,
        TimeSpan? timeout,
        CancellationToken cancellationToken
    )
    {
        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            return await SendScriptAttemptAsync(hashCommand, asking, timeout, startedAt, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (ValkeyServerException exception) when (exception.ErrorCode == "NOSCRIPT")
        {
            // NOSCRIPT is a complete rejection. Only this error permits cache recovery.
        }

        var bucket = script.RecoveryBucket;
        var gate = Volatile.Read(ref _scriptRecoveryGates[bucket]);
        if (gate is null)
        {
            var candidate = new SemaphoreSlim(1, 1);
            gate = Interlocked.CompareExchange(ref _scriptRecoveryGates[bucket], candidate, null);
            if (gate is null)
                gate = candidate;
            else
                candidate.Dispose();
        }

        if (timeout is { } duration)
        {
            if (
                !await gate.WaitAsync(
                        RemainingScriptTimeout(duration, startedAt, previouslySent: true),
                        cancellationToken
                    )
                    .ConfigureAwait(false)
            )
                throw new ValkeyCommandTimeoutException(duration, ValkeyCommandDeliveryStatus.MayHaveBeenSent);
        }
        else
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Another caller may have loaded this script while we waited. This recheck also
            // executes this caller's own invocation, so a success must never be replayed.
            try
            {
                return await SendScriptAttemptAsync(
                        hashCommand,
                        asking,
                        timeout,
                        startedAt,
                        cancellationToken,
                        previouslySent: true
                    )
                    .ConfigureAwait(false);
            }
            catch (ValkeyServerException exception) when (exception.ErrorCode == "NOSCRIPT")
            {
                return await SendScriptAttemptAsync(
                        script.WithSource(hashCommand),
                        asking,
                        timeout,
                        startedAt,
                        cancellationToken,
                        previouslySent: true
                    )
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task<RespValue> SendScriptAttemptAsync(
        ValkeyCommand command,
        bool asking,
        TimeSpan? timeout,
        long startedAt,
        CancellationToken cancellationToken,
        bool previouslySent = false
    )
    {
        var remaining = timeout is { } duration
            ? RemainingScriptTimeout(duration, startedAt, previouslySent)
            : (TimeSpan?)null;
        try
        {
            if (!asking)
                return await ExecuteCoreAsync(command, remaining, cancellationToken).ConfigureAwait(false);

            var replies = await ExecutePipelineCoreAsync(
                    [new ValkeyCommand("ASKING"), command],
                    remaining,
                    cancellationToken
                )
                .ConfigureAwait(false);
            replies[0].ThrowIfError();
            replies[1].ThrowIfError();
            return replies[1];
        }
        catch (ValkeyCommandTimeoutException exception) when (timeout is not null)
        {
            throw new ValkeyCommandTimeoutException(
                timeout.Value,
                previouslySent ? ValkeyCommandDeliveryStatus.MayHaveBeenSent : exception.DeliveryStatus
            );
        }
    }

    private static TimeSpan RemainingScriptTimeout(TimeSpan timeout, long startedAt, bool previouslySent = false)
    {
        var remaining = timeout - Stopwatch.GetElapsedTime(startedAt);
        return remaining > TimeSpan.Zero
            ? remaining
            : throw new ValkeyCommandTimeoutException(
                timeout,
                previouslySent ? ValkeyCommandDeliveryStatus.MayHaveBeenSent : ValkeyCommandDeliveryStatus.NotSent
            );
    }
}
