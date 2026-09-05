using ValkeyDotNet.Benchmarks;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class AllocationSampleSummaryTests
{
    [Fact]
    public void AggregatesTickWeightsRatherThanObjectSizesOrCounts()
    {
        var summary = new AllocationSampleSummary();
        summary.Add("System.Byte[]", 100000, "ReadExactAsync");
        summary.Add("System.Byte[]", 120000, "ReadExactAsync");
        summary.Add("System.Byte[]", 80000, "Encode");
        Assert.Equal(3, summary.Samples);
        Assert.Equal(300000, summary.SampledAllocationBytes);
        Assert.Equal(0, summary.SamplesWithoutStacks);
        Assert.Equal(new AllocationSampleRow("System.Byte[]", "ReadExactAsync", 2, 220000), summary.Rows()[0]);
        Assert.Equal(2, summary.Rows().Length);
    }

    [Fact]
    public void KeepsDifferentTypesAndMissingStacksVisible()
    {
        var summary = new AllocationSampleSummary();
        summary.Add("System.Byte[]", 100, "");
        summary.Add("RespValue", 100, "");
        Assert.Equal(2, summary.SamplesWithoutStacks);
        Assert.Equal(2, summary.Rows().Length);
        Assert.Equal("RespValue", summary.Rows()[0].Type);
    }

    [Fact]
    public void RejectsNegativeWeightsWithoutRecordingSample()
    {
        var summary = new AllocationSampleSummary();
        Assert.Throws<ArgumentOutOfRangeException>(() => summary.Add("Type", -1, "Stack"));
        Assert.Equal(0, summary.Samples);
        Assert.Empty(summary.Rows());
    }

    [Fact]
    public void EmptySummaryContainsNoInventedSamples()
    {
        var summary = new AllocationSampleSummary();
        Assert.Equal(0, summary.SampledAllocationBytes);
        Assert.Equal(0, summary.SamplesWithoutStacks);
        Assert.Empty(summary.Rows());
    }
}
