using System.Diagnostics;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class RecoveryHandleMeasurementTests
{
    [Theory]
    [InlineData(null, false, false)]
    [InlineData("0", false, false)]
    [InlineData(null, true, false)]
    [InlineData("0", true, false)]
    [InlineData("1", true, true)]
    public void RequiredModeIsExplicit(string? value, bool linux, bool expected)
    {
        Assert.Equal(expected, RecoveryHandleMeasurement.RequireLinux(value, linux));
    }

    [Theory]
    [InlineData("1", false)]
    [InlineData("true", true)]
    [InlineData("", true)]
    [InlineData(" 1", true)]
    [InlineData("2", true)]
    public void InvalidOrUnsupportedModeFailsBeforeResources(string value, bool linux)
    {
        Assert.Throws<InvalidOperationException>(() => RecoveryHandleMeasurement.RequireLinux(value, linux));
    }

    [Fact]
    public void DescriptorEnumerationIsPositiveBoundedAndDisposed()
    {
        Assert.Equal(3, RecoveryHandleMeasurement.CountDescriptors(["0", "1", "2"]));
        Assert.Equal(4096, RecoveryHandleMeasurement.CountDescriptors(Enumerable.Repeat("fd", 4096)));
        Assert.Throws<InvalidOperationException>(() => RecoveryHandleMeasurement.CountDescriptors([]));
        var visited = 0;
        var disposed = false;
        Assert.Throws<InvalidOperationException>(() => RecoveryHandleMeasurement.CountDescriptors(Endless()));
        Assert.Equal(4097, visited);
        Assert.True(disposed);

        IEnumerable<string> Endless()
        {
            try
            {
                while (true)
                {
                    visited++;
                    yield return "fd";
                }
            }
            finally
            {
                disposed = true;
            }
        }
    }

    [Fact]
    public void ObservationErrorsAreNotConvertedToUnsupported()
    {
        Assert.Throws<IOException>(() => RecoveryHandleMeasurement.CountDescriptors(FailedRead()));

        static IEnumerable<string> FailedRead()
        {
            yield return "0";
            throw new IOException("Synthetic observation failure.");
        }
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData(0, true)]
    [InlineData(-1, false)]
    public void MissingRequiredOrInvalidCountsFail(int? count, bool required)
    {
        Assert.Throws<InvalidOperationException>(() => RecoveryHandleMeasurement.CheckAvailability(count, required));
    }

    [Fact]
    public void GrowthBudgetRejectsOverflowAndLossOfObservation()
    {
        RecoveryHandleMeasurement.CheckAvailability(null, required: false);
        RecoveryHandleMeasurement.CheckAvailability(1, required: true);
        RecoveryHandleMeasurement.CheckGrowth(null, null);
        RecoveryHandleMeasurement.CheckGrowth(100, 132);
        RecoveryHandleMeasurement.CheckGrowth(100, 90);
        RecoveryHandleMeasurement.CheckGrowth(int.MaxValue, int.MaxValue);
        Assert.Throws<InvalidOperationException>(() => RecoveryHandleMeasurement.CheckGrowth(100, 133));
        Assert.Throws<InvalidOperationException>(() => RecoveryHandleMeasurement.CheckGrowth(100, null));
        Assert.Throws<InvalidOperationException>(() => RecoveryHandleMeasurement.CheckGrowth(-1, 1));
    }

    [Fact]
    public void LinuxProbeObservesOpenDescriptorsWithoutDocker()
    {
        if (!OperatingSystem.IsLinux())
        {
            Assert.Skip("Requires Linux procfs, but no Docker daemon or Valkey server.");
        }
        using var process = Process.GetCurrentProcess();
        var before = RecoveryHandleMeasurement.Read(process);
        RecoveryHandleMeasurement.CheckAvailability(before, required: true);
        var files = new List<FileStream>();
        try
        {
            for (var index = 0; index < 64; index++)
            {
                files.Add(File.OpenRead("/dev/null"));
            }
            var held = RecoveryHandleMeasurement.Read(process);
            Assert.True(held >= 64);
            foreach (var file in files)
            {
                var descriptor = file.SafeFileHandle.DangerousGetHandle().ToInt64();
                Assert.Contains(
                    "/proc/self/fd/" + descriptor.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    Directory.EnumerateFileSystemEntries("/proc/self/fd")
                );
            }
            TestContext.Current.TestOutputHelper?.WriteLine($"Linux probe: before={before}; held_64_files={held}");
        }
        finally
        {
            foreach (var file in files)
            {
                file.Dispose();
            }
        }
        var after = RecoveryHandleMeasurement.Read(process);
        RecoveryHandleMeasurement.CheckAvailability(after, required: true);
        TestContext.Current.TestOutputHelper?.WriteLine($"Linux probe: after_disposal={after}");
    }
}
