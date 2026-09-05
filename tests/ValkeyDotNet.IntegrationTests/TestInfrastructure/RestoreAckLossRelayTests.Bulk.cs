using System.Text;
using ValkeyDotNet.MigrationRelay;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed partial class RestoreAckLossRelayTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(4096)]
    public async Task BulkForwardsFirstRestoreReplyAndWithholdsOnlyLast(int fragment)
    {
        var wire = BulkWire();
        using var sender = new DuplexStream(wire, fragment);
        using var destination = new DuplexStream("+OK\r\n+OK\r\n+OK\r\n"u8.ToArray(), fragment);
        var phases = new List<string>();
        await RestoreAckLossRelay.RunAsync(
            sender,
            destination,
            "last"u8.ToArray(),
            phases.Add,
            TestContext.Current.CancellationToken,
            "first"u8.ToArray()
        );
        Assert.Equal(wire, destination.Written);
        Assert.Equal("+OK\r\n+OK\r\n"u8.ToArray(), sender.Written);
        Assert.Equal(["RESTORE_ACK_FORWARDED", "RESTORE_ACK_WITHHELD", "SENDER_CLOSED"], phases);
    }

    [Theory]
    [InlineData("wrong-key")]
    [InlineData("wrong-command")]
    [InlineData("truncated")]
    [InlineData("oversized")]
    [InlineData("zero-ttl")]
    [InlineData("nested")]
    public async Task BulkValidatesLastRestoreBeforeForwardingAnyCommand(string variant)
    {
        var last = variant switch
        {
            "wrong-key" => Frame("RESTORE-ASKING"u8.ToArray(), "other"u8.ToArray(), "1000"u8.ToArray(), [255]),
            "wrong-command" => Frame("RESTORE"u8.ToArray(), "last"u8.ToArray(), "1000"u8.ToArray(), [255]),
            "truncated" => "*4\r\n$14\r\nRESTORE-ASKING\r\n"u8.ToArray(),
            "oversized" => "*4\r\n$8193\r\n"u8.ToArray(),
            "zero-ttl" => Frame("RESTORE-ASKING"u8.ToArray(), "last"u8.ToArray(), "0"u8.ToArray(), [255]),
            "nested" => "*4\r\n*4\r\n"u8.ToArray(),
            _ => throw new InvalidOperationException(),
        };
        using var sender = new DuplexStream([.. BulkPrefix(), .. last], 1);
        using var destination = new DuplexStream([], 1);
        var error = await Record.ExceptionAsync(() =>
            RestoreAckLossRelay.RunAsync(
                sender,
                destination,
                "last"u8.ToArray(),
                _ => { },
                TestContext.Current.CancellationToken,
                "first"u8.ToArray()
            )
        );
        Assert.True(error is InvalidOperationException or EndOfStreamException);
        Assert.Empty(sender.Written);
        Assert.Empty(destination.Written);
    }

    [Theory]
    [InlineData("+OK\r\n-ERR\r\n", false)]
    [InlineData("+OK\r\n+OK\r\n-ERR\r\n", false)]
    [InlineData("+OK\r\n+OK\r\n+OK\r\n", true)]
    public async Task BulkRejectsFailedRestoreOrExtraTransfer(string replies, bool extra)
    {
        using var sender = new DuplexStream(extra ? [.. BulkWire(), 42] : BulkWire(), 1);
        using var destination = new DuplexStream(Encoding.ASCII.GetBytes(replies), 1);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RestoreAckLossRelay.RunAsync(
                sender,
                destination,
                "last"u8.ToArray(),
                _ => { },
                TestContext.Current.CancellationToken,
                "first"u8.ToArray()
            )
        );
    }

    [Fact]
    public async Task BulkWaitsForSenderClosureInsteadOfClosingAfterFirstAcknowledgment()
    {
        using var sender = new DuplexStream(BulkWire(), 1, waitAtEnd: true);
        using var destination = new DuplexStream("+OK\r\n+OK\r\n+OK\r\n"u8.ToArray(), 1);
        await VerifySenderCloseWaitAsync(sender, destination);
    }

    private static async Task VerifySenderCloseWaitAsync(DuplexStream sender, DuplexStream destination)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(5));
        var withheld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var phases = new List<string>();
        var run = RestoreAckLossRelay.RunAsync(
            sender,
            destination,
            "last"u8.ToArray(),
            phase =>
            {
                phases.Add(phase);
                if (phase == "RESTORE_ACK_WITHHELD")
                {
                    withheld.TrySetResult();
                }
            },
            cancellation.Token,
            "first"u8.ToArray()
        );
        try
        {
            await withheld.Task.WaitAsync(cancellation.Token);
            Assert.False(run.IsCompleted);
            Assert.Equal("+OK\r\n+OK\r\n"u8.ToArray(), sender.Written);
        }
        finally
        {
            await cancellation.CancelAsync();
            try
            {
                await run;
                Assert.Fail("The relay must observe cancellation while awaiting sender closure.");
            }
            catch (OperationCanceledException)
            {
                Assert.True(cancellation.IsCancellationRequested);
            }
        }
        Assert.DoesNotContain("SENDER_CLOSED", phases);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BulkRelayRequiresInitializedOwnedPrimaries(bool replica)
    {
        await using var cluster = new MigrationValkeyCluster(replica);
        var prefix = Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.LoseOwnedRestoreAcknowledgmentAsync(
                [.. prefix, 0],
                ValkeyProtocol.Resp3,
                TestContext.Current.CancellationToken,
                [.. prefix, 255]
            )
        );
    }

    [Theory]
    [InlineData("duplicate")]
    [InlineData("oversized")]
    public async Task BulkRelayRejectsInvalidExpectedKeysBeforeReading(string variant)
    {
        using var sender = new DuplexStream([], 1);
        using var destination = new DuplexStream([], 1);
        await Assert.ThrowsAsync<ArgumentException>(() =>
            RestoreAckLossRelay.RunAsync(
                sender,
                destination,
                "last"u8.ToArray(),
                _ => { },
                TestContext.Current.CancellationToken,
                variant == "duplicate" ? "last"u8.ToArray() : new byte[513]
            )
        );
    }

    private static byte[] BulkPrefix() =>
        [
            .. Frame("SELECT"u8.ToArray(), "0"u8.ToArray()),
            .. Frame("RESTORE-ASKING"u8.ToArray(), "first"u8.ToArray(), "1000"u8.ToArray(), [255, 0, 13, 10]),
        ];

    private static byte[] BulkWire() =>
        [
            .. BulkPrefix(),
            .. Frame("RESTORE-ASKING"u8.ToArray(), "last"u8.ToArray(), "1000"u8.ToArray(), [0, 255, 10, 13]),
        ];
}
