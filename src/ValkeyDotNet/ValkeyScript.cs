using System.Security.Cryptography;
using System.Text;

namespace ValkeyDotNet;

/// <summary>A reusable Lua script. Supply application data separately through KEYS and ARGV.</summary>
public sealed class ValkeyScript
{
    private readonly byte[] _source;
    private readonly ValkeyArgument _hashArgument;

    /// <summary>Creates a script from UTF-8 source. Never interpolate untrusted data into source.</summary>
    public ValkeyScript(string source)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        _source = Encoding.UTF8.GetBytes(source);
        var digest = SHA1.HashData(_source);
        Sha1 = Convert.ToHexStringLower(digest);
        RecoveryBucket = digest[0] & 15;
        _hashArgument = Sha1;
    }

    /// <summary>The SHA-1 identifier required by Valkey's EVALSHA protocol, not a security digest.</summary>
    public string Sha1 { get; }

    /// <summary>
    /// Builds a binary-safe EVAL command suitable for a pipeline. Pipeline execution does not need
    /// cache recovery. Argument memory is borrowed until execution completes.
    /// </summary>
    public ValkeyCommand CreateCommand(IReadOnlyList<ValkeyArgument> keys, IReadOnlyList<ValkeyArgument> arguments) =>
        CreateCommand(keys, arguments, useHash: false);

    internal ValkeyCommand CreateCommand(
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        bool useHash
    )
    {
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(arguments);
        var values = new ValkeyArgument[checked(2 + keys.Count + arguments.Count)];
        values[0] = useHash ? _hashArgument : new ValkeyArgument(_source);
        values[1] = keys.Count;
        for (var i = 0; i < keys.Count; i++)
            values[i + 2] = keys[i];
        for (var i = 0; i < arguments.Count; i++)
            values[i + 2 + keys.Count] = arguments[i];
        return new ValkeyCommand(useHash ? "EVALSHA" : "EVAL", values);
    }

    internal ValkeyCommand WithSource(ValkeyCommand hashCommand)
    {
        var values = hashCommand.ArgumentsSpan.ToArray();
        values[0] = new ValkeyArgument(_source);
        return new ValkeyCommand("EVAL", values);
    }

    internal int RecoveryBucket { get; }
}
