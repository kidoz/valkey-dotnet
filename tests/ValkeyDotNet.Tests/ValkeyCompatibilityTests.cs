using System.Globalization;
using System.Text;

namespace ValkeyDotNet.Tests;

/// <summary>
/// Live compatibility coverage across maintained Valkey release lines.
/// Skipped unless VALKEYDOTNET_ENDPOINT points at a disposable server; see docker-compose.yml.
/// </summary>
public sealed class ValkeyCompatibilityTests
{
    private static (string Host, int Port) RequireEndpoint()
    {
        var endpoint = Environment.GetEnvironmentVariable("VALKEYDOTNET_ENDPOINT");
        if (string.IsNullOrWhiteSpace(endpoint))
            Assert.Skip("Set VALKEYDOTNET_ENDPOINT to run the live Valkey compatibility tests.");

        var parts = endpoint.Split(':', 2);
        return (parts[0], parts.Length == 2 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 6379);
    }

    private static async Task<ValkeyClient> ConnectAsync(ValkeyProtocol protocol, CancellationToken cancellationToken)
    {
        var (host, port) = RequireEndpoint();
        return await ValkeyClient.ConnectAsync(
            new ValkeyClientOptions
            {
                Host = host,
                Port = port,
                Protocol = protocol,
                ClientName = "valkey-dotnet-compat",
            },
            cancellationToken
        );
    }

    private static string NewKey() =>
        "valkey-dotnet:compat:" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

    private static async Task<Version> ServerVersionAsync(ValkeyClient client, CancellationToken cancellationToken)
    {
        var info = await client.ExecuteAsync(new ValkeyCommand("INFO", "server"), cancellationToken);
        foreach (var line in info.AsString()!.Split('\n'))
        {
            if (!line.StartsWith("valkey_version:", StringComparison.Ordinal))
                continue;
            return Version.Parse(line["valkey_version:".Length..].Trim());
        }

        throw new InvalidOperationException("The server did not report valkey_version.");
    }

    private static async Task<bool> SupportsAsync(
        ValkeyClient client,
        string command,
        CancellationToken cancellationToken
    )
    {
        var reply = await client.ExecuteAsync(new ValkeyCommand("COMMAND", "INFO", command), cancellationToken);
        var entries = reply.AsArray();
        return entries.Count > 0 && !entries[0].IsNull;
    }

    [Fact]
    public async Task HandshakeNegotiatesResp3AndReportsServerMetadata()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var client = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken);

        Assert.Equal(ValkeyProtocol.Resp3, client.NegotiatedProtocol);

        // HELLO 3 replies with a map; the lossless value model exposes it directly.
        var serverInfo = client.ServerInfo.AsMap();
        var proto = serverInfo.Single(pair => pair.Key.AsString() == "proto");
        Assert.Equal(3L, proto.Value.AsInt64());

        var version = await ServerVersionAsync(client, cancellationToken);
        Assert.True(version.Major >= 7, $"Unexpected server version {version}.");
    }

    [Fact]
    public async Task Resp2HandshakeStillServesCommands()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var client = await ConnectAsync(ValkeyProtocol.Resp2, cancellationToken);

        Assert.Equal(ValkeyProtocol.Resp2, client.NegotiatedProtocol);
        Assert.Equal("PONG", await client.PingAsync(cancellationToken));

        // RESP2 has no map type: HGETALL comes back as a flat array of alternating fields and values.
        var key = NewKey();
        await client.HashSetAsync(key, "field", "value"u8.ToArray(), cancellationToken);
        try
        {
            var hash = await client.ExecuteAsync(new ValkeyCommand("HGETALL", key), cancellationToken);
            Assert.Equal(RespType.Array, hash.Type);
            var flat = hash.AsArray();
            Assert.Equal("field", flat[0].AsString());
            Assert.Equal("value", flat[1].AsString());
        }
        finally
        {
            await client.DeleteAsync([key], cancellationToken);
        }
    }

    [Fact]
    public async Task Resp3ReturnsHashesAsMaps()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var client = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken);

        var key = NewKey();
        await client.HashSetAsync(key, "field", "value"u8.ToArray(), cancellationToken);
        try
        {
            var hash = await client.ExecuteAsync(new ValkeyCommand("HGETALL", key), cancellationToken);
            Assert.Equal(RespType.Map, hash.Type);
            var pair = Assert.Single(hash.AsMap());
            Assert.Equal("field", pair.Key.AsString());
            Assert.Equal("value", pair.Value.AsString());
        }
        finally
        {
            await client.DeleteAsync([key], cancellationToken);
        }
    }

    [Fact]
    public async Task BinaryPayloadsRoundTripUnchanged()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var client = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken);

        // Every byte value, including NUL, CR, and LF, which naive framing would corrupt.
        var payload = new byte[256];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)i;

        var key = NewKey();
        try
        {
            Assert.True(await client.SetAsync(key, payload, cancellationToken: cancellationToken));
            var roundTripped = await client.GetAsync(key, cancellationToken);
            Assert.Equal(payload, roundTripped);
        }
        finally
        {
            await client.DeleteAsync([key], cancellationToken);
        }
    }

    [Fact]
    public async Task PipelineReturnsErrorsInPlaceAndKeepsConnectionUsable()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var client = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken);

        var key = NewKey();
        try
        {
            var replies = await client.ExecutePipelineAsync(
                [
                    new ValkeyCommand("SET", key, "not-a-number"),
                    new ValkeyCommand("INCR", key),
                    new ValkeyCommand("GET", key),
                ],
                cancellationToken
            );

            Assert.Equal(3, replies.Count);
            Assert.Equal("OK", replies[0].AsString());

            // The failure is returned in place so the reader can drain the whole batch.
            Assert.Equal(RespType.SimpleError, replies[1].Type);
            var failure = replies[1].ToServerException();
            Assert.Equal("ERR", failure.ErrorCode);

            Assert.Equal("not-a-number", replies[2].AsString());

            // The connection survived the error reply.
            Assert.Equal("PONG", await client.PingAsync(cancellationToken));
        }
        finally
        {
            await client.DeleteAsync([key], cancellationToken);
        }
    }

    [Fact]
    public async Task ServerErrorDoesNotInvalidateTheConnection()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var client = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken);

        var key = NewKey();
        await client.HashSetAsync(key, "field", "value"u8.ToArray(), cancellationToken);
        try
        {
            var failure = await Assert.ThrowsAsync<ValkeyServerException>(() =>
                client.IncrementAsync(key, 1, cancellationToken)
            );
            Assert.Equal("WRONGTYPE", failure.ErrorCode);

            Assert.Equal("PONG", await client.PingAsync(cancellationToken));
        }
        finally
        {
            await client.DeleteAsync([key], cancellationToken);
        }
    }

    [Fact]
    public async Task CommandInfoProjectsThroughAsArrayOnEveryLine()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var client = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken);

        // Valkey 9 changed COMMAND INFO subcommands from a Set to an Array in RESP3.
        // AsArray() accepts Array, Set, and Push, so the lossless model absorbs the change.
        var reply = await client.ExecuteAsync(new ValkeyCommand("COMMAND", "INFO", "GET"), cancellationToken);
        var entry = Assert.Single(reply.AsArray());
        var fields = entry.AsArray();

        Assert.Equal("get", fields[0].AsString());
        Assert.Equal(2L, fields[1].AsInt64()); // arity
        Assert.NotEmpty(fields[2].AsArray()); // flags: Set on some lines, Array on others
    }

    [Fact]
    public async Task HashFieldExpirationWorksWhereTheServerSupportsIt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var client = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken);

        if (!await SupportsAsync(client, "HEXPIRE", cancellationToken))
        {
            var version = await ServerVersionAsync(client, cancellationToken);
            Assert.Skip($"Valkey {version} does not implement hash field expiration.");
        }

        var key = NewKey();
        await client.HashSetAsync(key, "field", "value"u8.ToArray(), cancellationToken);
        try
        {
            // The generic command API reaches commands the typed surface does not wrap.
            var set = await client.ExecuteAsync(
                new ValkeyCommand("HEXPIRE", key, 100, "FIELDS", 1, "field"),
                cancellationToken
            );
            Assert.Equal(1L, set.AsArray()[0].AsInt64());

            var ttl = await client.ExecuteAsync(
                new ValkeyCommand("HTTL", key, "FIELDS", 1, "field"),
                cancellationToken
            );
            Assert.InRange(ttl.AsArray()[0].AsInt64(), 1L, 100L);

            var persisted = await client.ExecuteAsync(
                new ValkeyCommand("HPERSIST", key, "FIELDS", 1, "field"),
                cancellationToken
            );
            Assert.Equal(1L, persisted.AsArray()[0].AsInt64());

            Assert.Equal(
                "value",
                Encoding.UTF8.GetString((await client.HashGetAsync(key, "field", cancellationToken))!)
            );
        }
        finally
        {
            await client.DeleteAsync([key], cancellationToken);
        }
    }

    [Fact]
    public async Task ResponseSizeLimitIsEnforced()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var (host, port) = RequireEndpoint();

        var key = NewKey();
        await using (var writer = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken))
        {
            await writer.SetAsync(key, new byte[8192], cancellationToken: cancellationToken);
        }

        try
        {
            await using var bounded = await ValkeyClient.ConnectAsync(
                new ValkeyClientOptions
                {
                    Host = host,
                    Port = port,
                    MaxResponseBytes = 2048,
                },
                cancellationToken
            );

            await Assert.ThrowsAsync<ValkeyProtocolException>(() => bounded.GetAsync(key, cancellationToken));
        }
        finally
        {
            await using var cleanup = await ConnectAsync(ValkeyProtocol.Resp3, cancellationToken);
            await cleanup.DeleteAsync([key], cancellationToken);
        }
    }
}
