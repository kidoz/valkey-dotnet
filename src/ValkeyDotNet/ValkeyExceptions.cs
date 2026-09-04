namespace ValkeyDotNet;

public class ValkeyException : Exception
{
    public ValkeyException(string message)
        : base(message) { }

    public ValkeyException(string message, Exception innerException)
        : base(message, innerException) { }
}

public sealed class ValkeyProtocolException : ValkeyException, IValkeyCommandFailure
{
    public ValkeyProtocolException(string message)
        : base(message) { }

    /// <inheritdoc />
    public ValkeyCommandDeliveryStatus DeliveryStatus => ValkeyCommandDeliveryStatus.MayHaveBeenSent;
}

public sealed class ValkeyServerException : ValkeyException, IValkeyCommandFailure
{
    public ValkeyServerException(string message)
        : base(message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var separator = message.IndexOf(' ', StringComparison.Ordinal);
        ErrorCode = separator < 0 ? message : message[..separator];
    }

    public string ErrorCode { get; }

    /// <inheritdoc />
    public ValkeyCommandDeliveryStatus DeliveryStatus => ValkeyCommandDeliveryStatus.ReplyReceived;
}

public sealed class ValkeyConnectionException : ValkeyException, IValkeyCommandFailure
{
    public ValkeyConnectionException(string message, Exception innerException)
        : this(message, innerException, ValkeyCommandDeliveryStatus.MayHaveBeenSent) { }

    public ValkeyConnectionException(
        string message,
        Exception innerException,
        ValkeyCommandDeliveryStatus deliveryStatus
    )
        : base(message, innerException)
    {
        if (!Enum.IsDefined(deliveryStatus))
            throw new ArgumentOutOfRangeException(nameof(deliveryStatus));
        DeliveryStatus = deliveryStatus;
    }

    /// <inheritdoc />
    public ValkeyCommandDeliveryStatus DeliveryStatus { get; }
}

/// <summary>A caller cancelled a command after its delivery became ambiguous.</summary>
public sealed class ValkeyCommandCanceledException : OperationCanceledException, IValkeyCommandFailure
{
    internal ValkeyCommandCanceledException(CancellationToken cancellationToken)
        : base("The command was cancelled after it may have reached Valkey.", cancellationToken) { }

    /// <inheritdoc />
    public ValkeyCommandDeliveryStatus DeliveryStatus => ValkeyCommandDeliveryStatus.MayHaveBeenSent;
}

/// <summary>A per-operation deadline elapsed.</summary>
public sealed class ValkeyCommandTimeoutException : TimeoutException, IValkeyCommandFailure
{
    internal ValkeyCommandTimeoutException(TimeSpan timeout, ValkeyCommandDeliveryStatus deliveryStatus)
        : base($"The Valkey command exceeded its {timeout} deadline.")
    {
        Timeout = timeout;
        DeliveryStatus = deliveryStatus;
    }

    /// <summary>The configured deadline.</summary>
    public TimeSpan Timeout { get; }

    /// <inheritdoc />
    public ValkeyCommandDeliveryStatus DeliveryStatus { get; }
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
