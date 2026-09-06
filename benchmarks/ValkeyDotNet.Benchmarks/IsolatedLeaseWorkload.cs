using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace ValkeyDotNet.Benchmarks;

// Each key is used once. No reset/acquire/release companion command enters the timed window.
internal sealed class IsolatedLeaseWorkload
{
    internal const int Capacity = RoundTripMeasurements.WarmupIterations + RoundTripMeasurements.Iterations;
    internal const int TtlMilliseconds = 120000;
    internal static readonly ValkeyScript Release = new(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
    );
    private readonly ValkeyCommand[] _acquisitions;
    private readonly ValkeyArgument[][] _scriptKeys;
    private readonly ValkeyArgument[] _arguments;
    private readonly RespValue?[] _replies;
    private int _executed;
    private bool _prepared;

    internal IsolatedLeaseWorkload(string prefix, int worker, bool release)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        if (
            !prefix.StartsWith("valkey-dotnet-bench-", StringComparison.Ordinal)
            || prefix.Length > 80
            || worker is < 0 or > 7
        )
        {
            throw new ArgumentException("Invalid bounded lease workload.");
        }
        IsRelease = release;
        Owner = RandomNumberGenerator.GetBytes(16);
        _arguments = [Owner];
        var stem = prefix + ":isolated-lease:" + worker.ToString(CultureInfo.InvariantCulture) + ":\0\r\n:";
        Keys = Enumerable
            .Range(0, Capacity)
            .Select(index => Encoding.UTF8.GetBytes(stem + index.ToString(CultureInfo.InvariantCulture)))
            .ToArray();
        _acquisitions = Keys.Select(key => new ValkeyCommand("SET", key, Owner, "NX", "PX", TtlMilliseconds)).ToArray();
        _scriptKeys = Keys.Select(key => new ValkeyArgument[] { key }).ToArray();
        _replies = new RespValue?[Capacity];
    }

    internal bool IsRelease { get; }
    internal byte[] Owner { get; }
    internal byte[][] Keys { get; }
    internal int Executed => _executed;

    internal async Task SetupAsync(ValkeyClient client, CancellationToken token)
    {
        if (_prepared || _executed != 0)
        {
            throw new InvalidOperationException("Lease workload setup cannot be repeated.");
        }
        foreach (var chunk in Keys.Chunk(100))
        {
            if (
                (
                    await client.ExecuteAsync(
                        new ValkeyCommand("EXISTS", chunk.Select(key => new ValkeyArgument(key)).ToArray()),
                        token
                    )
                ).AsInt64() != 0
            )
            {
                throw new InvalidOperationException("Lease workload requires absent keys.");
            }
        }
        if (IsRelease)
        {
            foreach (var chunk in _acquisitions.Chunk(100))
            {
                foreach (var reply in await client.ExecutePipelineAsync(chunk, token))
                {
                    if (reply.AsString() != "OK")
                    {
                        throw new InvalidOperationException("Lease pre-acquisition failed.");
                    }
                }
            }
        }
        _prepared = true;
    }

    internal async Task ExecuteAsync(ValkeyClient client, CancellationToken token)
    {
        if (!_prepared || _executed >= Capacity)
        {
            throw new InvalidOperationException("Lease workload is unprepared or exhausted.");
        }
        var index = _executed++;
        _replies[index] = IsRelease
            ? await client.ExecuteScriptAsync(Release, _scriptKeys[index], _arguments, token)
            : await client.ExecuteAsync(_acquisitions[index], token);
    }

    internal bool ResultsAreValid()
    {
        if (_executed == 0)
        {
            return false;
        }
        for (var index = 0; index < _executed; index++)
        {
            if (_replies[index] is not { } reply || (IsRelease ? reply.AsInt64() != 1 : reply.AsString() != "OK"))
            {
                return false;
            }
        }
        return true;
    }

    internal async Task ValidateStateAsync(ValkeyClient client, CancellationToken token)
    {
        var index = 0;
        foreach (var chunk in Keys.Chunk(100))
        {
            var replies = await client.ExecutePipelineAsync(
                chunk.SelectMany(key => new[] { new ValkeyCommand("GET", key), new ValkeyCommand("PTTL", key) }),
                token
            );
            for (var offset = 0; offset < chunk.Length; offset++, index++)
            {
                var held = IsRelease ? index >= _executed : index < _executed;
                var value = replies[offset * 2];
                var ttl = replies[offset * 2 + 1].AsInt64();
                if (
                    held
                        ? value.IsNull || !value.AsBytes().Span.SequenceEqual(Owner) || ttl is < 1 or > TtlMilliseconds
                        : !value.IsNull || ttl != -2
                )
                {
                    throw new InvalidOperationException("Lease ownership, expiration or deletion validation failed.");
                }
            }
        }
    }

    internal async Task CleanupAsync(ValkeyClient client, CancellationToken token)
    {
        foreach (var chunk in Keys.Chunk(100))
        {
            await client.ExecuteAsync(
                new ValkeyCommand("DEL", chunk.Select(key => new ValkeyArgument(key)).ToArray()),
                token
            );
        }
    }
}
