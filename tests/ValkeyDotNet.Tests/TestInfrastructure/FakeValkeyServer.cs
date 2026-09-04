using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace ValkeyDotNet.Tests.TestInfrastructure;

/// <summary>
/// A scripted single-session Valkey server on loopback. It lets a test drive the exact byte sequence
/// a client sees — including hostile frames, truncation, and an abrupt close — without a real server.
/// </summary>
internal sealed class FakeValkeyServer : IAsyncDisposable
{
    public const string HelloResp3 =
        "%3\r\n$6\r\nserver\r\n$6\r\nvalkey\r\n$7\r\nversion\r\n$5\r\n9.1.0\r\n$5\r\nproto\r\n:3\r\n";

    public const string HelloResp2 =
        "*6\r\n$6\r\nserver\r\n$6\r\nvalkey\r\n$7\r\nversion\r\n$5\r\n9.1.0\r\n$5\r\nproto\r\n:2\r\n";

    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly List<string[]> _received = [];

    private FakeValkeyServer(
        TcpListener listener,
        X509Certificate2? certificate,
        int sessionCount,
        Func<int, FakeValkeySession, Task> handler
    )
    {
        _listener = listener;
        Session = RunAsync(certificate, sessionCount, handler);
    }

    /// <summary>The scripted handler's task. Transport faults are absorbed; assertion failures are not.</summary>
    public Task Session { get; }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    /// <summary>Every command the server read, in wire order. Read it after awaiting <see cref="Session"/>.</summary>
    public IReadOnlyList<string[]> ReceivedCommands => _received;

    public static FakeValkeyServer Start(Func<FakeValkeySession, Task> handler, X509Certificate2? certificate = null)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return new FakeValkeyServer(listener, certificate, 1, (_, session) => handler(session));
    }

    public static FakeValkeyServer StartMany(int sessionCount, Func<int, FakeValkeySession, Task> handler)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(sessionCount, 1);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return new FakeValkeyServer(listener, certificate: null, sessionCount, handler);
    }

    public ValkeyClientOptions ClientOptions() =>
        new()
        {
            Host = "127.0.0.1",
            Port = Port,
            ConnectTimeout = TimeSpan.FromSeconds(30),
        };

    /// <summary>
    /// A short-lived certificate for the TLS tests. The client trusts it by comparing the presented
    /// thumbprint, never by accepting whatever the peer offers.
    /// </summary>
    public static X509Certificate2 CreateSelfSignedCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=valkey-dotnet-tests",
            key,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1
        );
        var alternativeNames = new SubjectAlternativeNameBuilder();
        alternativeNames.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(alternativeNames.Build());

        using var selfSigned = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddHours(1)
        );
        // SslStream needs the private key attached, which only the PKCS#12 round trip guarantees.
        return X509CertificateLoader.LoadPkcs12(selfSigned.Export(X509ContentType.Pfx), password: null);
    }

    public async ValueTask DisposeAsync()
    {
        await _shutdown.CancelAsync();
        _listener.Dispose();
        await Session;
        _shutdown.Dispose();
    }

    private async Task RunAsync(
        X509Certificate2? certificate,
        int sessionCount,
        Func<int, FakeValkeySession, Task> handler
    )
    {
        var sessions = new List<Task>(sessionCount);
        try
        {
            for (var index = 0; index < sessionCount; index++)
            {
                var connection = await _listener.AcceptTcpClientAsync(_shutdown.Token);
                sessions.Add(RunSessionAsync(connection, certificate, index, handler));
            }
        }
        catch (OperationCanceledException)
        {
            // The test finished before every expected connection was opened.
        }
        catch (ObjectDisposedException)
        {
            // Disposal closes the listener underneath a pending accept.
        }

        await Task.WhenAll(sessions);
    }

    private async Task RunSessionAsync(
        TcpClient connection,
        X509Certificate2? certificate,
        int index,
        Func<int, FakeValkeySession, Task> handler
    )
    {
        SslStream? tls = null;
        try
        {
            connection.NoDelay = true;
            using var registration = _shutdown.Token.Register(connection.Dispose);

            Stream stream = connection.GetStream();
            if (certificate is not null)
            {
                tls = new SslStream(stream, leaveInnerStreamOpen: false);
                await tls.AuthenticateAsServerAsync(
                    certificate,
                    clientCertificateRequired: false,
                    checkCertificateRevocation: false
                );
                stream = tls;
            }

            await handler(index, new FakeValkeySession(stream, _received));
        }
        catch (OperationCanceledException)
        {
            // The test finished before the script did.
        }
        catch (ObjectDisposedException)
        {
            // The client, or shutdown, closed the socket underneath a pending read.
        }
        catch (IOException)
        {
            // Includes EndOfStreamException: several tests close the client mid-script on purpose.
        }
        catch (SocketException)
        {
            // Same, when the reset surfaces below the stream layer.
        }
        finally
        {
            if (tls is not null)
                await tls.DisposeAsync();
            connection.Dispose();
        }
    }
}

internal sealed class FakeValkeySession
{
    private readonly Stream _stream;
    private readonly List<string[]> _received;

    public FakeValkeySession(Stream stream, List<string[]> received)
    {
        _stream = stream;
        _received = received;
    }

    /// <summary>Reads the client's HELLO and answers it, so a script can get straight to the point.</summary>
    public async Task<string[]> ExpectHandshakeAsync(string reply = FakeValkeyServer.HelloResp3)
    {
        var hello = await ReadCommandAsync();
        Assert.Equal("HELLO", hello[0]);
        await SendAsync(reply);
        return hello;
    }

    public async Task<string[]> ReadCommandAsync()
    {
        var header = await ReadLineAsync();
        if (header.Length < 2 || header[0] != '*')
            throw new InvalidOperationException($"Expected a RESP array header, read '{header}'.");

        var count = int.Parse(header[1..], CultureInfo.InvariantCulture);
        var parts = new string[count];
        for (var i = 0; i < count; i++)
        {
            var lengthLine = await ReadLineAsync();
            if (lengthLine.Length < 2 || lengthLine[0] != '$')
                throw new InvalidOperationException($"Expected a bulk string header, read '{lengthLine}'.");

            var payload = new byte[int.Parse(lengthLine[1..], CultureInfo.InvariantCulture)];
            await _stream.ReadExactlyAsync(payload);
            await ReadLineAsync();
            parts[i] = Encoding.UTF8.GetString(payload);
        }

        lock (_received)
            _received.Add(parts);
        return parts;
    }

    public Task SendAsync(string payload) => SendRawAsync(Encoding.UTF8.GetBytes(payload));

    public async Task SendRawAsync(byte[] payload)
    {
        await _stream.WriteAsync(payload);
        await _stream.FlushAsync();
    }

    /// <summary>Closes the connection, letting a test exercise a remote close at an exact position.</summary>
    public void Close() => _stream.Dispose();

    private async Task<string> ReadLineAsync()
    {
        var builder = new StringBuilder();
        var current = new byte[1];
        while (true)
        {
            await _stream.ReadExactlyAsync(current);
            if (current[0] == (byte)'\r')
            {
                await _stream.ReadExactlyAsync(current);
                return builder.ToString();
            }
            builder.Append((char)current[0]);
        }
    }
}
