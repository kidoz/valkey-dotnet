using System.Diagnostics;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal static class RecoveryHandleMeasurement
{
    internal const int DescriptorLimit = 4096;
    internal const int GrowthBudget = 32;
    internal static string Source => OperatingSystem.IsLinux() ? "linux-proc-self-fd" : "process-handle-count";

    internal static bool RequireLinux(string? value, bool isLinux)
    {
        if (value is not (null or "0" or "1"))
        {
            throw new InvalidOperationException("VALKEYDOTNET_REQUIRE_LINUX_HANDLES must be 0 or 1.");
        }
        if (value == "1" && !isLinux)
        {
            throw new InvalidOperationException("Linux descriptor evidence requires a Linux client process.");
        }
        return value == "1";
    }

    internal static int? Read(Process process)
    {
        if (OperatingSystem.IsLinux())
        {
            // Count names only: never follow descriptor targets or log their paths/content.
            return CountDescriptors(Directory.EnumerateFileSystemEntries("/proc/self/fd"));
        }
        process.Refresh();
        var count = process.HandleCount;
        return count > 0 ? count : null;
    }

    internal static int CountDescriptors(IEnumerable<string> entries)
    {
        var count = 0;
        foreach (var unused in entries)
        {
            if (++count > DescriptorLimit)
            {
                throw new InvalidOperationException("Linux descriptor observation exceeded 4096 entries.");
            }
        }
        if (count == 0)
        {
            throw new InvalidOperationException("Linux descriptor observation was empty.");
        }
        return count;
    }

    internal static void CheckAvailability(int? count, bool required)
    {
        if (count is <= 0 || (required && count is null))
        {
            throw new InvalidOperationException("Required handle observation is unavailable or invalid.");
        }
    }

    internal static void CheckGrowth(int? baseline, int? current)
    {
        CheckAvailability(baseline, required: false);
        CheckAvailability(current, required: baseline is not null);
        if (baseline is not null && current is not null && (long)current > (long)baseline + GrowthBudget)
        {
            throw new InvalidOperationException("Settled process handles exceeded the 32-handle growth budget.");
        }
    }
}
