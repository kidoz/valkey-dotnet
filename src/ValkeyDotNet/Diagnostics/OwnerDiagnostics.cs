using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net.Sockets;
using System.Security.Authentication;

namespace ValkeyDotNet.Diagnostics;

// One process-wide instrument set, no owner/endpoint registry or background export queue.
// All calls that can invoke a listener must stay outside owner and transport locks.
internal static class OwnerDiagnostics
{
    private static long _activeOperations;
    private static readonly Meter Meter = new(ValkeyDiagnostics.MeterName);
    private static readonly ActivitySource? Source = Create(() =>
        new ActivitySource(ValkeyDiagnostics.ActivitySourceName)
    );
    private static readonly Counter<long>? Operations = Create(() =>
        Meter.CreateCounter<long>("valkey.owner.operations", "{operation}")
    );
    private static readonly Counter<long>? OperationFailures = Create(() =>
        Meter.CreateCounter<long>("valkey.owner.operation.failures", "{operation}")
    );
    private static readonly Histogram<double>? OperationDuration = Create(() =>
        Meter.CreateHistogram<double>("valkey.owner.operation.duration", "s")
    );
    private static readonly Counter<long>? ConnectAttempts = Create(() =>
        Meter.CreateCounter<long>("valkey.owner.connection.attempts", "{attempt}")
    );
    private static readonly Counter<long>? ConnectFailures = Create(() =>
        Meter.CreateCounter<long>("valkey.owner.connection.failures", "{attempt}")
    );
    private static readonly Histogram<double>? ConnectDuration = Create(() =>
        Meter.CreateHistogram<double>("valkey.owner.connection.duration", "s")
    );
    private static readonly Counter<long>? Reconnects = Create(() =>
        Meter.CreateCounter<long>("valkey.owner.reconnects", "{connection}")
    );

    static OwnerDiagnostics()
    {
        _ = Create(() =>
            Meter.CreateObservableGauge(
                "valkey.owner.operations.active",
                () => Interlocked.Read(ref _activeOperations),
                "{operation}"
            )
        );
    }

    internal static async Task<T> TrackOperationAsync<T>(string kind, Func<Task<T>> operation)
    {
        var startedAt = Stopwatch.GetTimestamp();
        var parent = Activity.Current;
        var activity = StartActivity(kind, parent);
        string? error = null;
        Interlocked.Increment(ref _activeOperations);
        try
        {
            var result = await operation().ConfigureAwait(false);
            // Pipeline errors are still returned in place. Telemetry never turns them into exceptions.
            if (result is IReadOnlyList<RespValue> replies)
                for (var i = 0; i < replies.Count; i++)
                    if (replies[i].Type is RespType.SimpleError or RespType.BlobError)
                    {
                        error = "server";
                        break;
                    }
            return result;
        }
        catch (Exception exception)
        {
            error = Classify(exception);
            throw;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(startedAt).TotalSeconds;
            Interlocked.Decrement(ref _activeOperations);
            TagList tags = new() { { "valkey.operation.kind", kind } };
            if (error is not null)
                tags.Add("error.type", error);
            Add(Operations, tags);
            if (error is not null)
                Add(OperationFailures, tags);
            Record(OperationDuration, elapsed, tags);
            StopActivity(activity, parent, error);
        }
    }

    internal static async Task<ValkeyClient> TrackConnectionAsync(Func<Task<ValkeyClient>> connect)
    {
        var startedAt = Stopwatch.GetTimestamp();
        Add(ConnectAttempts, default);
        string? error = null;
        try
        {
            return await connect().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            error = Classify(exception);
            throw;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(startedAt).TotalSeconds;
            TagList tags = default;
            if (error is not null)
            {
                tags.Add("error.type", error);
                Add(ConnectFailures, tags);
            }
            Record(ConnectDuration, elapsed, tags);
        }
    }

    internal static void Reconnected() => Add(Reconnects, default);

    private static string Classify(Exception exception) =>
        exception switch
        {
            ValkeyCapacityException => "capacity",
            ValkeyProtocolException => "protocol",
            ValkeyServerException => "server",
            AuthenticationException => "authentication",
            ValkeyConnectionException or IOException or SocketException => "connection",
            TimeoutException => "timeout",
            OperationCanceledException => "canceled",
            ObjectDisposedException => "disposed",
            ArgumentException or ValkeyUnsupportedCommandException => "invalid_argument",
            _ => "_OTHER",
        };

    private static Activity? StartActivity(string kind, Activity? parent)
    {
        Activity? activity = null;
        try
        {
            if (Source?.HasListeners() != true)
                return null;
            var name = kind == "connect" ? "valkey.connect" : "valkey";
            var activityKind = kind == "connect" ? ActivityKind.Internal : ActivityKind.Client;
            KeyValuePair<string, object?>[] tags =
            [
                new("db.system.name", "valkey"),
                new("valkey.operation.kind", kind),
            ];
            activity =
                parent?.IdFormat == ActivityIdFormat.Hierarchical
                    ? Source.CreateActivity(name, activityKind, parentId: parent.Id, tags: tags)
                    : Source.CreateActivity(name, activityKind, parentContext: parent?.Context ?? default, tags: tags);
            activity?.Start();
            return activity;
        }
        catch
        {
            StopActivity(activity, parent, null);
            return null;
        }
    }

    private static void StopActivity(Activity? activity, Activity? parent, string? error)
    {
        try
        {
            if (error is not null && activity is not null)
            {
                activity.SetTag("error.type", error);
                activity.SetStatus(ActivityStatusCode.Error);
            }
            activity?.Dispose();
        }
        catch
        { /* A listener cannot replace an operation's result or original failure. */
        }
        finally
        {
            try
            {
                Activity.Current = parent;
            }
            catch
            { /* CurrentChanged is also application-owned code. */
            }
        }
    }

    private static T? Create<T>(Func<T> create)
        where T : class
    {
        try
        {
            return create();
        }
        catch
        {
            // Instrument publication and ActivitySource discovery can invoke throwing listeners.
            // A failed instrument stays unavailable, without poisoning the type initializer.
            return null;
        }
    }

    private static void Add(Counter<long>? counter, in TagList tags)
    {
        try
        {
            counter?.Add(1, tags);
        }
        catch
        { /* Best-effort diagnostics, never a transport failure. */
        }
    }

    private static void Record(Histogram<double>? histogram, double value, in TagList tags)
    {
        try
        {
            histogram?.Record(value, tags);
        }
        catch
        { /* Best-effort diagnostics, never a transport failure. */
        }
    }
}
