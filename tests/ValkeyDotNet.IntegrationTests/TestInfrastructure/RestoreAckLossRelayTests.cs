using System.Globalization;
using System.Text;
using ValkeyDotNet.MigrationRelay;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed partial class RestoreAckLossRelayTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(4096)]
    public async Task ForwardsBinaryCommandsButOnlySelectAcknowledgment(int fragment)
    {
        byte[] key = [0, 255, 13, 10];
        var select = Frame("SELECT"u8.ToArray(), "0"u8.ToArray());
        var restore = Frame("RESTORE-ASKING"u8.ToArray(), key, "1000"u8.ToArray(), [255, 0, 13, 10]);
        using var sender = new DuplexStream([.. select, .. restore], fragment);
        using var destination = new DuplexStream("+OK\r\n+OK\r\n"u8.ToArray(), fragment);
        var phases = new List<string>();
        await RestoreAckLossRelay.RunAsync(sender, destination, key, phases.Add, TestContext.Current.CancellationToken);
        Assert.Equal("+OK\r\n"u8.ToArray(), sender.Written);
        Assert.Equal([.. select, .. restore], destination.Written);
        Assert.Equal(["RESTORE_ACK_WITHHELD", "SENDER_CLOSED"], phases);
    }

    [Theory]
    [InlineData("*2\r\n$-1\r\n")]
    [InlineData("*2\r\n$8193\r\n")]
    [InlineData("*2\r\n$9999999999\r\n")]
    [InlineData("*2\r\n$1\r\naXX")]
    [InlineData("*2\n")]
    [InlineData("*5\r\n")]
    [InlineData("*2\r\n*1\r\n")]
    [InlineData("*2\r\n$1\r\n")]
    [InlineData("*2\r\n$+1\r\n")]
    public async Task RejectsMalformedTruncatedNestedAndOversizedFrames(string wire)
    {
        using var stream = new DuplexStream(Encoding.ASCII.GetBytes(wire), 1);
        var error = await Record.ExceptionAsync(async () =>
        {
            await RestoreAckLossRelay.ReadCommandAsync(stream, 2, TestContext.Current.CancellationToken);
        });
        Assert.True(error is InvalidOperationException or EndOfStreamException);
    }

    [Fact]
    public async Task BoundsAggregateBeforeAllocatingAnotherBulk()
    {
        using var stream = new DuplexStream(Frame(new byte[8192], new byte[8192]), 4096);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RestoreAckLossRelay.ReadCommandAsync(stream, 2, TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData("-ERR\r\n", false)]
    [InlineData("+OK\r\n-ERR\r\n", false)]
    [InlineData("+OK\r\n+OK\r\n", true)]
    public async Task RejectsUnexpectedRepliesOrExtraTransferBytes(string replies, bool extra)
    {
        var wire = ValidWire();
        using var sender = new DuplexStream(extra ? [.. wire, 42] : wire, 1);
        using var destination = new DuplexStream(Encoding.ASCII.GetBytes(replies), 1);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RestoreAckLossRelay.RunAsync(
                sender,
                destination,
                "key"u8.ToArray(),
                _ => { },
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task RejectsWrongKeyBeforeForwardingAnyCommand()
    {
        using var sender = new DuplexStream(ValidWire(), 1);
        using var destination = new DuplexStream([], 1);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RestoreAckLossRelay.RunAsync(
                sender,
                destination,
                "other"u8.ToArray(),
                _ => { },
                TestContext.Current.CancellationToken
            )
        );
        Assert.Empty(destination.Written);
    }

    [Fact]
    public async Task CancellationStopsFraming()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        using var sender = new DuplexStream(ValidWire(), 1);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            RestoreAckLossRelay.ReadCommandAsync(sender, 2, cancellation.Token)
        );
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("120001")]
    [InlineData("")]
    [InlineData("1x")]
    public async Task RejectsInvalidTtlBeforeForwardingAnyCommand(string ttl)
    {
        using var sender = new DuplexStream(
            [
                .. Frame("SELECT"u8.ToArray(), "0"u8.ToArray()),
                .. Frame("RESTORE-ASKING"u8.ToArray(), "key"u8.ToArray(), Encoding.ASCII.GetBytes(ttl), [255, 0]),
            ],
            1
        );
        using var destination = new DuplexStream([], 1);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RestoreAckLossRelay.RunAsync(
                sender,
                destination,
                "key"u8.ToArray(),
                _ => { },
                TestContext.Current.CancellationToken
            )
        );
        Assert.Empty(destination.Written);
        Assert.Empty(sender.Written);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RelayFaultRequiresInitializedOwnedCluster(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        var key = Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.LoseOwnedRestoreAcknowledgmentAsync(
                key,
                ValkeyProtocol.Resp3,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task RelayFaultRejectsExternalKeyBeforeDockerAccess()
    {
        await using var cluster = new MigrationValkeyCluster();
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.LoseOwnedRestoreAcknowledgmentAsync(
                "external"u8.ToArray(),
                ValkeyProtocol.Resp2,
                TestContext.Current.CancellationToken
            )
        );
    }

    private static byte[] ValidWire() =>
        [
            .. Frame("SELECT"u8.ToArray(), "0"u8.ToArray()),
            .. Frame("RESTORE-ASKING"u8.ToArray(), "key"u8.ToArray(), "1000"u8.ToArray(), [255, 0]),
        ];

    private static byte[] Frame(params byte[][] arguments)
    {
        using var stream = new MemoryStream();
        stream.Write(Encoding.ASCII.GetBytes("*" + arguments.Length.ToString(CultureInfo.InvariantCulture) + "\r\n"));
        foreach (var argument in arguments)
        {
            stream.Write(
                Encoding.ASCII.GetBytes("$" + argument.Length.ToString(CultureInfo.InvariantCulture) + "\r\n")
            );
            stream.Write(argument);
            stream.Write("\r\n"u8);
        }
        return stream.ToArray();
    }

    private sealed class DuplexStream(byte[] input, int fragment, bool waitAtEnd = false) : Stream
    {
        private readonly MemoryStream _input = new(input);
        private readonly MemoryStream _output = new();
        internal byte[] Written => _output.ToArray();
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            _input.Read(buffer, offset, Math.Min(count, fragment));

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default
        )
        {
            if (waitAtEnd && _input.Position == _input.Length)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            return await _input.ReadAsync(buffer[..Math.Min(buffer.Length, fragment)], cancellationToken);
        }

        public override void Write(byte[] buffer, int offset, int count) => _output.Write(buffer, offset, count);

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default
        ) => _output.WriteAsync(buffer, cancellationToken);

        public override void Flush() { }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _input.Dispose();
                _output.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
