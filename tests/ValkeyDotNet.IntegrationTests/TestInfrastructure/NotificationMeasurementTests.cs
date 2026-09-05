using System.Buffers.Binary;
using ValkeyDotNet.Benchmarks;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class NotificationMeasurementTests
{
    [Fact]
    public void DeliveryCanPrecedeAcknowledgmentAndSummaryExcludesMissingSamples()
    {
        var samples = new NotificationSamples(2);
        samples.Begin(0, 100_000_000);
        samples.Deliver(0, 110_000_000);
        samples.Acknowledge(0, 120_000_000);
        Assert.False(samples.Delivered.IsCompleted);
        Assert.Throws<InvalidOperationException>(() =>
            samples.Summarize(NotificationOperation.Publish, ValkeyProtocol.Resp3, 1, 90_000_000, 150_000_000, 100)
        );
        samples.Begin(1, 120_000_000);
        samples.Deliver(1, 160_000_000);
        samples.Acknowledge(1, 140_000_000);
        Assert.True(samples.Delivered.IsCompletedSuccessfully);
        var result = samples.Summarize(
            NotificationOperation.Publish,
            ValkeyProtocol.Resp3,
            1,
            90_000_000,
            150_000_000,
            100
        );
        Assert.Equal(50, result.AllocatedBytesPerOperation);
        Assert.Equal(result.DeliveryMicroseconds[0], result.DeliveryP50Microseconds);
        Assert.Equal(result.DeliveryMicroseconds[1], result.DeliveryP95Microseconds);
        Assert.Equal(result.DeliveryMicroseconds[1], result.DeliveryP99Microseconds);
        Assert.True(result.DeliveredOperationsPerSecond < result.AcknowledgmentsPerSecond);
    }

    [Fact]
    public void DuplicateAndPreInvocationEventsAreRejected()
    {
        var samples = new NotificationSamples(1);
        Assert.Throws<InvalidOperationException>(() => samples.Deliver(0, 5));
        Assert.Throws<InvalidOperationException>(() => samples.Acknowledge(0, 5));
        samples.Begin(0, 10);
        Assert.Throws<InvalidOperationException>(() => samples.Begin(0, 11));
        Assert.Throws<InvalidOperationException>(() => samples.Deliver(0, 9));
        samples.Deliver(0, 11);
        Assert.Throws<InvalidOperationException>(() => samples.Deliver(0, 12));
        samples.Acknowledge(0, 12);
        Assert.Throws<InvalidOperationException>(() => samples.Acknowledge(0, 13));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(8193)]
    public void SampleCountIsBounded(int count)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new NotificationSamples(count));
    }

    [Fact]
    public void SampleIndexesAndTimestampsAreBounded()
    {
        var samples = new NotificationSamples(1);
        Assert.Throws<ArgumentOutOfRangeException>(() => samples.Begin(-1, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => samples.Deliver(1, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => samples.Acknowledge(0, 0));
    }

    [Fact]
    public void BinaryIdentityRejectsUnknownTruncatedAndOutOfRangeKeys()
    {
        byte[] prefix = [0, 13, 10, 255];
        byte[] key = [.. prefix, 0, 0, 0, 1];
        Assert.Equal(1, NotificationWorkload.ReadId(key, prefix, 2));
        Assert.Throws<InvalidOperationException>(() => NotificationWorkload.ReadId(key, prefix, 1));
        Assert.Throws<InvalidOperationException>(() => NotificationWorkload.ReadId(key.AsSpan(1), prefix, 2));
        key[0] = 1;
        Assert.Throws<InvalidOperationException>(() => NotificationWorkload.ReadId(key, prefix, 2));
        key[0] = 0;
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(prefix.Length), -1);
        Assert.Throws<InvalidOperationException>(() => NotificationWorkload.ReadId(key, prefix, 2));
    }

    [Theory]
    [InlineData("foreign", 1, 1, 1, ValkeyProtocol.Resp3, 0)]
    [InlineData("valkey-dotnet-bench-test", 2, 1, 1, ValkeyProtocol.Resp3, 0)]
    [InlineData("valkey-dotnet-bench-test", 1, 0, 1, ValkeyProtocol.Resp3, 0)]
    [InlineData("valkey-dotnet-bench-test", 1, 65, 1, ValkeyProtocol.Resp3, 0)]
    [InlineData("valkey-dotnet-bench-test", 1, 1, 513, ValkeyProtocol.Resp3, 0)]
    [InlineData("valkey-dotnet-bench-test", 1, 1, 1, ValkeyProtocol.Resp2, 1)]
    [InlineData("valkey-dotnet-bench-test", 1, 1, 1, ValkeyProtocol.Resp3, 99)]
    public async Task InvalidWorkloadFailsBeforeOpeningConnection(
        string prefix,
        int concurrency,
        int warmup,
        int iterations,
        ValkeyProtocol protocol,
        int operation
    )
    {
        await Assert.ThrowsAsync<ArgumentException>(() =>
            NotificationWorkload.RunAsync(
                new ValkeyClientOptions { Protocol = protocol },
                prefix,
                (NotificationOperation)operation,
                concurrency,
                warmup,
                iterations,
                TestContext.Current.CancellationToken
            )
        );
    }
}
