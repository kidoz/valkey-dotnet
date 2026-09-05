namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class CutoverWriteTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesCutoverBeforeOwnedDebugClusterInitialization(bool debug)
    {
        await using var cluster = new MigrationValkeyCluster(enableMigrationDebug: debug);
        var key = System.Text.Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        var invoked = false;
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.CompleteAtomicSlotMigrationAcrossCutoverAsync(
                [.. key, 1],
                [.. key, 2],
                ValkeyProtocol.Resp3,
                _ =>
                {
                    invoked = true;
                    return Task.CompletedTask;
                },
                (_, _) =>
                {
                    invoked = true;
                    return Task.CompletedTask;
                },
                TestContext.Current.CancellationToken
            )
        );
        Assert.False(invoked);
    }

    [Theory]
    [InlineData("")]
    [InlineData("id=1 name=owned flags=b cmd=set")]
    [InlineData("id=1 name=owned flags=b cmd=set\nid=1 name=owned flags=b cmd=set")]
    [InlineData("id=1 name=owned flags=b cmd=set\nid=3 name=owned flags=b cmd=set")]
    [InlineData("id=1 name=external flags=b cmd=set\nid=2 name=owned flags=b cmd=set")]
    [InlineData("id=1 id=1 name=owned flags=b cmd=set\nid=2 name=owned flags=b cmd=set")]
    [InlineData("id=1 name=owned flags=b cmd=set\nid=2 name=owned cmd=set")]
    [InlineData("id=1 name=owned flags=b cmd=set\nid=2 name=owned flags=b cmd=set broken")]
    public void RejectsUnprovenWriterIdentities(string text)
    {
        Assert.Throws<InvalidOperationException>(() =>
            MigrationValkeyCluster.AreCutoverWritersBlocked(text, "1", "2", "owned")
        );
    }

    [Fact]
    public void RequiresBothExactWritersBlockedOnSetWithinResponseBound()
    {
        const string blocked = "id=2 name=owned flags=b cmd=set\nid=1 name=owned flags=b cmd=set";
        Assert.True(MigrationValkeyCluster.AreCutoverWritersBlocked(blocked, "1", "2", "owned"));
        Assert.False(
            MigrationValkeyCluster.AreCutoverWritersBlocked(
                blocked.Replace("flags=b", "flags=N", StringComparison.Ordinal),
                "1",
                "2",
                "owned"
            )
        );
        Assert.False(
            MigrationValkeyCluster.AreCutoverWritersBlocked(
                blocked.Replace("cmd=set", "cmd=client|id", StringComparison.Ordinal),
                "1",
                "2",
                "owned"
            )
        );
        Assert.Throws<InvalidOperationException>(() =>
            MigrationValkeyCluster.AreCutoverWritersBlocked(blocked, "1", "1", "owned")
        );
        Assert.Throws<InvalidOperationException>(() =>
            MigrationValkeyCluster.AreCutoverWritersBlocked(new string('x', 16385), "1", "2", "owned")
        );
    }

    [Theory]
    [InlineData(ValkeyCommandDeliveryStatus.NotSent, "not_sent")]
    [InlineData(ValkeyCommandDeliveryStatus.MayHaveBeenSent, "ambiguous")]
    [InlineData(ValkeyCommandDeliveryStatus.ReplyReceived, "reply_error")]
    public async Task AccountsForDeliveryCertaintyWithoutReplay(ValkeyCommandDeliveryStatus status, string expected)
    {
        Assert.Equal(
            expected,
            await CutoverWriteObservation.ObserveAsync(
                Task.FromException<RespValue>(
                    new ValkeyConnectionException("controlled failure", new IOException(), status)
                )
            )
        );
    }

    [Fact]
    public async Task DoesNotCallUnknownFailuresOrCancellationAcknowledged()
    {
        Assert.Equal(
            "unexpected_failure",
            await CutoverWriteObservation.ObserveAsync(Task.FromException<RespValue>(new InvalidOperationException()))
        );
        Assert.Equal(
            "cancelled_without_delivery_status",
            await CutoverWriteObservation.ObserveAsync(Task.FromCanceled<RespValue>(new CancellationToken(true)))
        );
        Assert.Equal(
            "reply_error",
            await CutoverWriteObservation.ObserveAsync(
                Task.FromException<RespValue>(new ValkeyServerException("ERR controlled rejection"))
            )
        );
    }

    [Fact]
    public async Task PreservesAmbiguityOfTypedCancellationOnCancelledTask()
    {
        Assert.Equal("ambiguous", await CutoverWriteObservation.ObserveAsync(CancelAsync()));

        static async Task<RespValue> CancelAsync()
        {
            await Task.Yield();
            throw new AmbiguousCancellationException();
        }
    }

    private sealed class AmbiguousCancellationException : OperationCanceledException, IValkeyCommandFailure
    {
        public AmbiguousCancellationException() { }

        public AmbiguousCancellationException(string message)
            : base(message) { }

        public AmbiguousCancellationException(string message, Exception innerException)
            : base(message, innerException) { }

        public ValkeyCommandDeliveryStatus DeliveryStatus => ValkeyCommandDeliveryStatus.MayHaveBeenSent;
    }
}
