namespace ValkeyDotNet;

public sealed partial class ValkeyClusterClient
{
    /// <summary>Routes a script by its first key. All keys must share one cluster hash slot.</summary>
    public Task<RespValue> ExecuteScriptAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        CancellationToken cancellationToken = default
    ) => ExecuteRoutedScriptAsync(script, keys, arguments, null, cancellationToken);

    /// <summary>Applies one isolated deadline across routing, redirects, and script-cache recovery.</summary>
    public Task<RespValue> ExecuteScriptWithDeadlineAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) =>
        ExecuteRoutedScriptAsync(
            script,
            keys,
            arguments,
            ValkeyClient.ValidateOperationTimeout(timeout),
            cancellationToken
        );

    private Task<RespValue> ExecuteRoutedScriptAsync(
        ValkeyScript script,
        IReadOnlyList<ValkeyArgument> keys,
        IReadOnlyList<ValkeyArgument> arguments,
        TimeSpan? timeout,
        CancellationToken cancellationToken
    )
    {
        ArgumentNullException.ThrowIfNull(script);
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count == 0)
            throw new ArgumentException("A cluster script requires at least one routing key.", nameof(keys));
        var keySnapshot = keys.ToArray();
        var slot = GetHashSlot(keySnapshot[0].Bytes.Span);
        foreach (var key in keySnapshot)
            if (GetHashSlot(key.Bytes.Span) != slot)
                throw new ArgumentException("All script keys must share a cluster hash slot.", nameof(keys));
        return ExecuteCoreAsync(
            keySnapshot[0],
            script.CreateCommand(keySnapshot, arguments, useHash: true),
            timeout,
            cancellationToken,
            script
        );
    }
}
