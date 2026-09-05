using System.Net.Sockets;
using System.Security.Cryptography;
using ValkeyDotNet.Cluster;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet;

public sealed partial class ValkeySubscriber
{
    private void ThrowIfRecovering()
    {
        if (_recovering)
        {
            throw new ValkeyConnectionException(
                "The subscriber is restoring its connection; no new subscription was sent.",
                _lastConnectionFailure!,
                ValkeyCommandDeliveryStatus.NotSent
            );
        }
    }

    private bool Disconnect(Connection connection, Exception error)
    {
        lock (_sync)
        {
            if (_closed || !ReferenceEquals(connection, _connection))
            {
                return false;
            }
            if (_recovering)
            {
                return true;
            }
            if (_connectionLossObserved)
            {
                return false;
            }
            _connectionLossObserved = true;
            Interlocked.Increment(ref _connectionLosses);
            if (!_options.EnableReconnect)
            {
                return false;
            }
            _recovering = true;
            _lastConnectionFailure = error;
            _pending?.Completion.TrySetException(error);
            _pending = null;
            _confirmed.Clear();
        }
        // Retire the old socket before opening anything else. Only the reader starts recovery.
        connection.Dispose();
        return true;
    }

    private async Task<bool> RecoverAsync()
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);
        deadline.CancelAfter(_options.RecoveryTimeout);
        var acquired = false;
        var original = _connection.Options;
        try
        {
            // No replacement can be published while the old writer still owns the lifecycle gate.
            await _gate.WaitAsync(deadline.Token).ConfigureAwait(false);
            acquired = true;
            for (var attempt = 0; attempt < _options.MaxReconnectAttempts; attempt++)
            {
                var ceiling = Math.Min(
                    _options.MaxReconnectDelay.TotalMilliseconds,
                    _options.InitialReconnectDelay.TotalMilliseconds * Math.Pow(2, attempt)
                );
                await Task.Delay(
                        TimeSpan.FromMilliseconds(
                            ceiling * (0.5 + RandomNumberGenerator.GetInt32(1_000_001) / 2_000_000.0)
                        ),
                        deadline.Token
                    )
                    .ConfigureAwait(false);
                Interlocked.Increment(ref _reconnectAttempts);
                try
                {
                    await RestoreConnectionAsync(deadline.Token).ConfigureAwait(false);
                    var current = _connection.Options;
                    if (
                        original.Port != current.Port
                        || !string.Equals(original.Host, current.Host, StringComparison.OrdinalIgnoreCase)
                    )
                    {
                        Interlocked.Increment(ref _successfulRelocations);
                    }
                    return true;
                }
                catch (Exception error)
                    when (error is IOException or SocketException or TimeoutException
                        || _topologyRecovery is not null && error is ValkeyConnectionException
                    )
                {
                    _connection.Dispose();
                    lock (_sync)
                    {
                        _confirmed.Clear();
                        _lastConnectionFailure = error;
                    }
                    if (attempt + 1 == _options.MaxReconnectAttempts)
                    {
                        throw;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (!_shutdown.IsCancellationRequested)
        {
            Close(new TimeoutException("The subscriber recovery budget expired."));
        }
        catch (Exception error)
        {
            Close(
                error is IOException or SocketException
                    ? new ValkeyConnectionException("Subscriber recovery failed.", error)
                    : error
            );
        }
        finally
        {
            if (acquired)
            {
                _gate.Release();
            }
        }
        return false;
    }

    private async Task RestoreConnectionAsync(CancellationToken token)
    {
        var route = (_options.Connection, _asking);
        if (_topologyRecovery is not null)
        {
            route = await _topologyRecovery.ResolveAsync(_connection.Options, null, token).ConfigureAwait(false);
        }
        for (var redirectCount = 0; ; redirectCount++)
        {
            ShardSubscriptionRedirectException? redirect = null;
            try
            {
                var replacement = await Connection.OpenAsync(route.Item1, token, route.Item2).ConfigureAwait(false);
                try
                {
                    lock (_sync)
                    {
                        ThrowIfClosed();
                        _connection = replacement;
                        _confirmed.Clear();
                    }
                }
                catch
                {
                    replacement.Dispose();
                    throw;
                }
                await RestoreAsync(token).ConfigureAwait(false);
                return;
            }
            catch (ShardSubscriptionRedirectException error) when (_topologyRecovery is not null)
            {
                redirect = error;
            }
            catch (ValkeyServerException error) when (_topologyRecovery is not null && error.ErrorCode == "MOVED")
            {
                // Only validated discovery, never the MOVED error's endpoint, changes the slot map.
            }
            _connection.Dispose();
            if (redirectCount >= _topologyRecovery!.MaxRedirects)
            {
                throw new ValkeyClusterException("Shard subscription recovery redirects were exhausted.");
            }
            route = await _topologyRecovery.ResolveAsync(route.Item1, redirect, token).ConfigureAwait(false);
        }
    }

    private async Task RestoreAsync(CancellationToken token)
    {
        Registration[] snapshot;
        lock (_sync)
        {
            snapshot = _registrations.Values.ToArray();
        }
        foreach (var registration in snapshot)
        {
            lock (_sync)
            {
                ThrowIfClosed();
                if (registration.Handles.Count == 0)
                {
                    continue;
                }
            }
            await RestoreChangeAsync(SubscribeKind(registration.Pattern), registration, token).ConfigureAwait(false);
        }

        // Local unsubscribe remains available during recovery, even with an ACK in flight.
        // Reconcile those removals before advertising a fully restored connection.
        while (true)
        {
            Registration[] removed;
            lock (_sync)
            {
                ThrowIfClosed();
                removed = _confirmed.Values.Where(registration => registration.Handles.Count == 0).ToArray();
                if (removed.Length == 0)
                {
                    token.ThrowIfCancellationRequested();
                    _recovering = false;
                    _connectionLossObserved = false;
                    Interlocked.Increment(ref _successfulReconnects);
                    return;
                }
            }
            foreach (var registration in removed)
            {
                await RestoreChangeAsync(UnsubscribeKind(registration.Pattern), registration, token)
                    .ConfigureAwait(false);
            }
        }
    }

    private async Task RestoreChangeAsync(string kind, Registration registration, CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(_options.OperationTimeout);
        var pending = new Pending(kind, registration, static () => { });
        lock (_sync)
        {
            ThrowIfClosed();
            _pending = pending;
        }
        try
        {
            await _connection
                .Stream.WriteAsync(RespWriter.Encode(new ValkeyCommand(kind, registration.Name)), timeout.Token)
                .ConfigureAwait(false);
            await _connection.Stream.FlushAsync(timeout.Token).ConfigureAwait(false);
            // The supervisor itself is the sole reader during restoration; no second reader task.
            while (!pending.Completion.Task.IsCompleted)
            {
                ProcessResponse(
                    await _connection.Reader.ReadAsync(timeout.Token).ConfigureAwait(false),
                    restoring: true
                );
            }
            await pending.Completion.Task.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            throw new TimeoutException("A subscriber restoration acknowledgement timed out.");
        }
        finally
        {
            lock (_sync)
            {
                if (ReferenceEquals(_pending, pending))
                {
                    _pending = null;
                }
            }
            _ = pending.Completion.Task.Exception;
        }
    }
}
