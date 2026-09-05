using System.Net;
using System.Net.Sockets;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

// A single-use, loopback-only test relay. It is not a general RESP proxy.
internal sealed class MigrationReplyLossProxy : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _lifetime;
    private readonly Task _run;
    private int _armed;
    private int _dropped;
    private int _disposed;

    internal MigrationReplyLossProxy(int sourcePort)
    {
        if (sourcePort is < 1 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(sourcePort));
        }
        _lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        _listener = new TcpListener(IPAddress.Loopback, 0);
        try
        {
            _listener.Start(1);
        }
        catch
        {
            _listener.Dispose();
            _lifetime.Dispose();
            throw;
        }
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _run = RunAsync(sourcePort);
    }

    internal int Port { get; }
    internal Task Completion => _run;
    internal int DroppedAcknowledgements => Volatile.Read(ref _dropped);

    // Call only after the library's HELLO handshake has completed, before sending MIGRATE.
    internal void Arm()
    {
        if (Interlocked.CompareExchange(ref _armed, 1, 0) != 0 || Volatile.Read(ref _disposed) != 0)
        {
            throw new InvalidOperationException("The reply-loss proxy can only be armed once before disposal.");
        }
    }

    private async Task RunAsync(int sourcePort)
    {
        using var downstream = await _listener.AcceptTcpClientAsync(_lifetime.Token);
        _listener.Stop(); // No second connection or reconnect is accepted.
        using var upstream = new TcpClient();
        await upstream.ConnectAsync(IPAddress.Loopback, sourcePort, _lifetime.Token);
        var client = downstream.GetStream();
        var server = upstream.GetStream();
        try
        {
            await Task.WhenAll(ForwardClientAsync(), ForwardServerAsync());
        }
        catch (OperationCanceledException) when (DroppedAcknowledgements == 1)
        {
            // The server pump deliberately closes the peer after withholding OK.
        }

        async Task ForwardClientAsync()
        {
            try
            {
                var buffer = new byte[4096];
                var total = 0;
                while (true)
                {
                    var count = await client.ReadAsync(buffer, _lifetime.Token);
                    if (count == 0)
                    {
                        throw new IOException("The test client closed before reply loss completed.");
                    }
                    total += count;
                    if (total > 65536)
                    {
                        throw new InvalidOperationException("Reply-loss request byte budget exceeded.");
                    }
                    await server.WriteAsync(buffer.AsMemory(0, count), _lifetime.Token);
                }
            }
            finally
            {
                await _lifetime.CancelAsync();
            }
        }

        async Task ForwardServerAsync()
        {
            try
            {
                var buffer = new byte[4096];
                var total = 0;
                var acknowledged = 0;
                while (true)
                {
                    var count = await server.ReadAsync(buffer, _lifetime.Token);
                    if (count == 0)
                    {
                        throw new IOException("The source closed before the expected success reply.");
                    }
                    total += count;
                    if (total > 65536)
                    {
                        throw new InvalidOperationException("Reply-loss response byte budget exceeded.");
                    }
                    if (Volatile.Read(ref _armed) == 0)
                    {
                        await client.WriteAsync(buffer.AsMemory(0, count), _lifetime.Token);
                        continue;
                    }
                    if (
                        acknowledged + count > 5
                        || !buffer.AsSpan(0, count).SequenceEqual("+OK\r\n"u8.Slice(acknowledged, count))
                    )
                    {
                        throw new InvalidOperationException("Expected exactly one MIGRATE success reply.");
                    }
                    acknowledged += count;
                    if (acknowledged == 5)
                    {
                        Interlocked.Exchange(ref _dropped, 1);
                        return; // Never forward any byte of the armed reply.
                    }
                }
            }
            finally
            {
                await _lifetime.CancelAsync();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }
        await _lifetime.CancelAsync();
        _listener.Stop();
        try
        {
            await _run.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (Exception error)
            when (error
                    is OperationCanceledException
                        or IOException
                        or SocketException
                        or ObjectDisposedException
                        or InvalidOperationException
            )
        {
            // Completion exposes relay failures to the test; teardown must still release the socket/CTS.
        }
        finally
        {
            _listener.Dispose();
            _lifetime.Dispose();
        }
    }
}
