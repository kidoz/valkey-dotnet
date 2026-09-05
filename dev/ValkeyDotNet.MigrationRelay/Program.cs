using System.Net;
using System.Net.Sockets;

namespace ValkeyDotNet.MigrationRelay;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        if (Environment.GetEnvironmentVariable("VALKEYDOTNET_OWNED_RELAY") != "1" || args.Length != 1)
        {
            throw new InvalidOperationException("This relay requires the owned Docker test harness.");
        }
        var key = Convert.FromBase64String(args[0]);
        if (key.Length is < 1 or > 512 || !key.AsSpan().StartsWith("{valkey-dotnet-migration-tests-"u8))
        {
            throw new InvalidOperationException("Invalid owned relay key.");
        }
        using var lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var listener = new TcpListener(IPAddress.Any, 6380);
        listener.Start(1);
        Console.WriteLine("READY");
        using var sender = await listener.AcceptTcpClientAsync(lifetime.Token).ConfigureAwait(false);
        listener.Stop();
        using var destination = new TcpClient();
        await destination.ConnectAsync("node-2", 6379, lifetime.Token).ConfigureAwait(false);
        await RestoreAckLossRelay
            .RunAsync(sender.GetStream(), destination.GetStream(), key, Console.WriteLine, lifetime.Token)
            .ConfigureAwait(false);
    }
}
