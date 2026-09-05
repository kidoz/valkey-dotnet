using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace ValkeyDotNet.Benchmarks;

internal enum RoundTripOperation
{
    Get,
    SetPx,
    ContendedSetNxPx,
    AcquireReleaseCycle,
    ExtendLease,
    Pipeline100Get,
}

internal sealed class RoundTripWorkload
{
    private static readonly ValkeyScript Release = new(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end"
    );
    private static readonly ValkeyScript Extend = new(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end"
    );
    private readonly ValkeyClient _client;
    private readonly ValkeyCommand _command;
    private readonly ValkeyCommand[] _pipeline;
    private readonly ValkeyArgument[] _lockKeys;
    private readonly ValkeyArgument[] _releaseArguments;
    private readonly ValkeyArgument[] _extendArguments;
    private RespValue? _lastReply;
    private IReadOnlyList<RespValue>? _lastPipeline;
    private RespValue? _acquired;

    internal RoundTripWorkload(ValkeyClient client, string prefix, int worker, RoundTripOperation operation)
    {
        if (
            !prefix.StartsWith("valkey-dotnet-bench-", StringComparison.Ordinal)
            || prefix.Length > 80
            || worker is < 0 or > 7
            || !Enum.IsDefined(operation)
        )
        {
            throw new ArgumentException("Invalid bounded benchmark workload.");
        }
        _client = client;
        Operation = operation;
        Payload = Enumerable.Range(0, 1024).Select(i => (byte)i).ToArray();
        Owner = RandomNumberGenerator.GetBytes(16);
        var stem = prefix + ":" + worker.ToString(CultureInfo.InvariantCulture) + ":";
        DataKeys = Enumerable
            .Range(0, 100)
            .Select(i => Encoding.UTF8.GetBytes(stem + i.ToString(CultureInfo.InvariantCulture)))
            .ToArray();
        LockKey = Encoding.UTF8.GetBytes(stem + "lock");
        _lockKeys = [LockKey];
        _releaseArguments = [Owner];
        _extendArguments = [Owner, 120000];
        _pipeline = DataKeys.Select(key => new ValkeyCommand("GET", key)).ToArray();
        _command = operation switch
        {
            RoundTripOperation.Get => new("GET", DataKeys[0]),
            RoundTripOperation.SetPx => new("SET", DataKeys[0], Payload, "PX", 120000),
            RoundTripOperation.ContendedSetNxPx or RoundTripOperation.AcquireReleaseCycle => new(
                "SET",
                LockKey,
                Owner,
                "NX",
                "PX",
                120000
            ),
            _ => new("PING"),
        };
    }

    internal RoundTripOperation Operation { get; }
    internal byte[] Payload { get; }
    internal byte[] Owner { get; }
    internal byte[][] DataKeys { get; }
    internal byte[] LockKey { get; }

    internal async Task SetupAsync(CancellationToken token)
    {
        var commands = DataKeys.Select(key => new ValkeyCommand("SET", key, Payload, "PX", 120000)).ToArray();
        foreach (var reply in await _client.ExecutePipelineAsync(commands, token))
        {
            reply.ThrowIfError();
        }
        if (Operation == RoundTripOperation.AcquireReleaseCycle)
        {
            await _client.ExecuteAsync(new ValkeyCommand("DEL", LockKey), token);
        }
        else
        {
            await _client.ExecuteAsync(new ValkeyCommand("SET", LockKey, Owner, "PX", 120000), token);
        }
    }

    internal async Task ExecuteAsync(CancellationToken token)
    {
        switch (Operation)
        {
            case RoundTripOperation.AcquireReleaseCycle:
                _acquired = await _client.ExecuteAsync(_command, token);
                _lastReply = await _client.ExecuteScriptAsync(Release, _lockKeys, _releaseArguments, token);
                break;
            case RoundTripOperation.ExtendLease:
                _lastReply = await _client.ExecuteScriptAsync(Extend, _lockKeys, _extendArguments, token);
                break;
            case RoundTripOperation.Pipeline100Get:
                _lastPipeline = await _client.ExecutePipelineAsync(_pipeline, token);
                foreach (var reply in _lastPipeline)
                {
                    reply.ThrowIfError();
                }
                break;
            default:
                _lastReply = await _client.ExecuteAsync(_command, token);
                break;
        }
    }

    // Outside measured intervals. Tests also verify each workload against the owned real server.
    internal bool LastResultIsValid() =>
        Operation switch
        {
            RoundTripOperation.Get => _lastReply is not null && _lastReply.AsBytes().Span.SequenceEqual(Payload),
            RoundTripOperation.SetPx => _lastReply?.AsString() == "OK",
            RoundTripOperation.ContendedSetNxPx => _lastReply?.IsNull == true,
            RoundTripOperation.AcquireReleaseCycle => _acquired?.AsString() == "OK" && _lastReply?.AsInt64() == 1,
            RoundTripOperation.ExtendLease => _lastReply?.AsInt64() == 1,
            RoundTripOperation.Pipeline100Get => _lastPipeline?.Count == 100
                && _lastPipeline.All(reply => reply.AsBytes().Span.SequenceEqual(Payload)),
            _ => false,
        };

    internal async Task CleanupAsync(CancellationToken token)
    {
        ValkeyArgument[] keys = [.. DataKeys.Select(key => new ValkeyArgument(key)), LockKey];
        await _client.ExecuteAsync(new ValkeyCommand("DEL", keys), token);
    }
}
