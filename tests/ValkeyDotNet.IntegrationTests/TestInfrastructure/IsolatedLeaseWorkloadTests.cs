using System.Text;
using ValkeyDotNet.Benchmarks;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class IsolatedLeaseWorkloadTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PreparesBoundedDistinctBinaryKeysForEveryWarmupAndMeasurement(bool release)
    {
        var identities = new HashSet<string>(StringComparer.Ordinal);
        for (var worker = 0; worker < 8; worker++)
        {
            var workload = new IsolatedLeaseWorkload("valkey-dotnet-bench-test", worker, release);
            Assert.Equal(release, workload.IsRelease);
            Assert.Equal(16, workload.Owner.Length);
            Assert.Equal(576, workload.Keys.Length);
            Assert.Equal(0, workload.Executed);
            Assert.False(workload.ResultsAreValid());
            foreach (var key in workload.Keys)
            {
                Assert.True(identities.Add(Convert.ToHexString(key)));
                Assert.True(key.AsSpan().StartsWith("valkey-dotnet-bench-test:"u8));
                Assert.Contains('\0', Encoding.UTF8.GetString(key));
                Assert.Contains('\r', Encoding.UTF8.GetString(key));
                Assert.Contains('\n', Encoding.UTF8.GetString(key));
            }
        }
        Assert.Equal(8 * IsolatedLeaseWorkload.Capacity, identities.Count);
    }

    [Theory]
    [InlineData("foreign", 0)]
    [InlineData("valkey-dotnet-bench-test", -1)]
    [InlineData("valkey-dotnet-bench-test", 8)]
    public void RejectsUnscopedOrUnboundedProfiles(string prefix, int worker)
    {
        Assert.Throws<ArgumentException>(() => new IsolatedLeaseWorkload(prefix, worker, false));
    }

    [Fact]
    public void RejectsMissingAndOversizedPrefixes()
    {
        Assert.Throws<ArgumentNullException>(() => new IsolatedLeaseWorkload(null!, 0, false));
        Assert.Throws<ArgumentException>(() =>
            new IsolatedLeaseWorkload("valkey-dotnet-bench-" + new string('x', 80), 0, true)
        );
    }

    [Fact]
    public async Task RefusesExecutionBeforePreparationWithoutTouchingAClient()
    {
        var workload = new IsolatedLeaseWorkload("valkey-dotnet-bench-test", 0, false);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workload.ExecuteAsync(null!, TestContext.Current.CancellationToken)
        );
        Assert.Equal(0, workload.Executed);
        Assert.False(workload.ResultsAreValid());
    }
}
