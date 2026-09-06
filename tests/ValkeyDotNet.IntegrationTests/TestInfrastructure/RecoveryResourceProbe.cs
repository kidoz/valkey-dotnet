using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed class RecoveryResourceProbe : IDisposable
{
    private readonly Process _process = Process.GetCurrentProcess();
    private readonly MeterListener _metrics = new();
    private long _active = -1;
    internal int Samples { get; private set; }
    internal int MaximumClients { get; private set; }
    internal long MaximumActiveOwnerOperations { get; private set; }
    internal long MaximumLiveHeap { get; private set; }
    internal long MaximumWorkingSet { get; private set; }
    internal int? MaximumHandles { get; private set; }
    internal int MaximumPoolThreads { get; private set; }
    internal long MaximumQueuedPoolWork { get; private set; }

    internal RecoveryResourceProbe()
    {
        _metrics.InstrumentPublished = (instrument, listener) =>
        {
            if (
                instrument.Meter.Name == ValkeyDiagnostics.MeterName
                && instrument.Name == "valkey.owner.operations.active"
            )
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _metrics.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Exchange(ref _active, value));
        _metrics.Start();
    }

    internal long ActiveOperations()
    {
        _metrics.RecordObservableInstruments();
        return Interlocked.Read(ref _active);
    }

    internal void Capture(int clients)
    {
        var active = ActiveOperations();
        if (
            clients is < 0 or > ConcurrentRecoverySettings.ExpectedClients
            || active is < 0 or > ConcurrentRecoverySettings.Participants * ConcurrentRecoverySettings.CallersPerOwner
        )
        {
            throw new InvalidOperationException("Observed recovery resources exceeded their configured bounds.");
        }
        Samples++;
        MaximumClients = Math.Max(MaximumClients, clients);
        MaximumActiveOwnerOperations = Math.Max(MaximumActiveOwnerOperations, active);
        MaximumLiveHeap = Math.Max(MaximumLiveHeap, GC.GetTotalMemory(forceFullCollection: false));
        _process.Refresh();
        MaximumWorkingSet = Math.Max(MaximumWorkingSet, _process.WorkingSet64);
        var handles = _process.HandleCount;
        if (handles > 0)
        {
            MaximumHandles = Math.Max(MaximumHandles ?? 0, handles);
        }
        MaximumPoolThreads = Math.Max(MaximumPoolThreads, ThreadPool.ThreadCount);
        MaximumQueuedPoolWork = Math.Max(MaximumQueuedPoolWork, ThreadPool.PendingWorkItemCount);
    }

    public void Dispose()
    {
        _metrics.Dispose();
        _process.Dispose();
    }
}
