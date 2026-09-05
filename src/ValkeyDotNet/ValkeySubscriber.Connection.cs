using System.Net.Security;
using System.Net.Sockets;
using ValkeyDotNet.Protocol;

namespace ValkeyDotNet;

public sealed partial class ValkeySubscriber
{
    // One physical lifetime. Recovery publishes a new object only after the old writer has exited.
    private sealed class Connection : IDisposable
    {
        private readonly TcpClient _tcp;
        private int _disposed;

        private Connection(TcpClient tcp, Stream stream, ValkeyClientOptions options)
        {
            _tcp = tcp;
            Stream = stream;
            Reader = new RespReader(
                stream,
                options.MaxResponseBytes,
                options.MaxResponseElements,
                options.MaxNestingDepth
            );
        }

        internal Stream Stream { get; }
        internal RespReader Reader { get; }
        internal ValkeyProtocol Protocol { get; private set; }

        internal static async Task<Connection> OpenAsync(
            ValkeyClientOptions options,
            CancellationToken token,
            bool asking = false
        )
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
            timeout.CancelAfter(options.ConnectTimeout);
            var tcp = new TcpClient { NoDelay = true };
            Stream? stream = null;
            try
            {
                await tcp.ConnectAsync(options.Host, options.Port, timeout.Token).ConfigureAwait(false);
                stream = tcp.GetStream();
                if (options.UseTls)
                {
                    var ssl = options.CertificateValidationCallback is null
                        ? new SslStream(stream, false)
                        : new SslStream(stream, false, options.CertificateValidationCallback);
                    stream = ssl;
                    await ssl.AuthenticateAsClientAsync(
                            new SslClientAuthenticationOptions { TargetHost = options.Host },
                            timeout.Token
                        )
                        .ConfigureAwait(false);
                }
                var connection = new Connection(tcp, stream, options);
                await connection.InitializeAsync(options, timeout.Token).ConfigureAwait(false);
                if (asking)
                {
                    var reply = await connection
                        .HandshakeCommandAsync(new ValkeyCommand("ASKING"), timeout.Token)
                        .ConfigureAwait(false);
                    if (reply.Type != RespType.SimpleString || !reply.AsBytes().Span.SequenceEqual("OK"u8))
                    {
                        throw new ValkeyProtocolException("Unexpected ASKING acknowledgement.");
                    }
                }
                return connection;
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                if (stream is not null)
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }
                tcp.Dispose();
                throw new TimeoutException("The subscriber connection timed out.");
            }
            catch
            {
                if (stream is not null)
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }
                tcp.Dispose();
                throw;
            }
        }

        private async Task InitializeAsync(ValkeyClientOptions options, CancellationToken token)
        {
            var arguments = new List<ValkeyArgument> { (int)options.Protocol };
            if (options.Password is not null)
            {
                arguments.Add("AUTH");
                arguments.Add(options.Username ?? "default");
                arguments.Add(options.Password);
            }
            if (options.ClientName is not null)
            {
                arguments.Add("SETNAME");
                arguments.Add(options.ClientName);
            }
            var hello = await HandshakeCommandAsync(new ValkeyCommand("HELLO", arguments.ToArray()), token)
                .ConfigureAwait(false);
            if (hello.Type is not (RespType.Map or RespType.Array))
            {
                throw new ValkeyProtocolException("Unexpected subscriber handshake frame.");
            }
            Protocol = ValkeyClient.ReadNegotiatedProtocol(hello);
            if (options.Database != 0)
            {
                var select = await HandshakeCommandAsync(new ValkeyCommand("SELECT", options.Database), token)
                    .ConfigureAwait(false);
                if (select.Type != RespType.SimpleString || !select.AsBytes().Span.SequenceEqual("OK"u8))
                {
                    throw new ValkeyProtocolException("Unexpected subscriber database acknowledgement.");
                }
            }
        }

        private async Task<RespValue> HandshakeCommandAsync(ValkeyCommand command, CancellationToken token)
        {
            await Stream.WriteAsync(RespWriter.Encode(command), token).ConfigureAwait(false);
            await Stream.FlushAsync(token).ConfigureAwait(false);
            var reply = await Reader.ReadAsync(token).ConfigureAwait(false);
            ThrowServerError(reply);
            return reply;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _tcp.Dispose();
                Stream.Dispose();
            }
        }
    }
}
