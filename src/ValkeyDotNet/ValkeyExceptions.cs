namespace ValkeyDotNet;

public class ValkeyException : Exception
{
    public ValkeyException(string message)
        : base(message) { }

    public ValkeyException(string message, Exception innerException)
        : base(message, innerException) { }
}

public sealed class ValkeyProtocolException : ValkeyException
{
    public ValkeyProtocolException(string message)
        : base(message) { }
}

public sealed class ValkeyServerException : ValkeyException
{
    public ValkeyServerException(string message)
        : base(message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var separator = message.IndexOf(' ', StringComparison.Ordinal);
        ErrorCode = separator < 0 ? message : message[..separator];
    }

    public string ErrorCode { get; }
}

public sealed class ValkeyConnectionException : ValkeyException
{
    public ValkeyConnectionException(string message, Exception innerException)
        : base(message, innerException) { }
}

/// <summary>Cluster discovery, topology, or redirection failed.</summary>
public sealed class ValkeyClusterException : ValkeyException
{
    public ValkeyClusterException(string message)
        : base(message) { }

    public ValkeyClusterException(string message, Exception innerException)
        : base(message, innerException) { }
}

/// <summary>
/// A command was rejected before it reached the wire because it would change connection state the
/// client owns. The connection is untouched and remains usable.
/// </summary>
public sealed class ValkeyUnsupportedCommandException : ValkeyException
{
    public ValkeyUnsupportedCommandException(string command, string reason)
        : base($"{command} is not supported on a ValkeyClient connection: {reason}.")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(command);
        Command = command;
    }

    /// <summary>The rejected command, including its subcommand when that is what made it unsupported.</summary>
    public string Command { get; }
}
