using ValkeyDotNet.Benchmarks;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class RoundTripBenchmarkTests
{
    [Theory]
    [InlineData("")]
    [InlineData("tcp://127.0.0.1:2375")]
    [InlineData("ssh://host")]
    [InlineData("https://host")]
    public void BenchmarksRejectRemoteDockerEndpoints(string endpoint)
    {
        Assert.Throws<InvalidOperationException>(() => OwnedBenchmarkServer.RequireLocalDocker(endpoint));
    }

    [Fact]
    public void BenchmarksAcceptLocalSocketTransports()
    {
        OwnedBenchmarkServer.RequireLocalDocker("unix:///var/run/docker.sock");
        OwnedBenchmarkServer.RequireLocalDocker("npipe:////./pipe/docker_engine");
    }

    [Fact]
    public void LatencyPercentilesUseNearestRank()
    {
        double[] samples = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        Assert.Equal(5, RoundTripMeasurements.Percentile(samples, 0.5));
        Assert.Equal(10, RoundTripMeasurements.Percentile(samples, 0.95));
        Assert.Equal(10, RoundTripMeasurements.Percentile(samples, 0.99));
        Assert.Equal(1, RoundTripMeasurements.Percentile([1], 0.99));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1.1)]
    [InlineData(double.NaN)]
    public void RejectsInvalidPercentiles(double percentile)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RoundTripMeasurements.Percentile([1], percentile));
        Assert.Throws<ArgumentOutOfRangeException>(() => RoundTripMeasurements.Percentile([], 0.5));
    }

    [Fact]
    public async Task RejectsUnboundedWorkerProfilesWithoutStartingWork()
    {
        await Assert.ThrowsAsync<ArgumentException>(() =>
            RoundTripMeasurements.MeasureAsync([], ValkeyProtocol.Resp3, TestContext.Current.CancellationToken)
        );
    }
}
