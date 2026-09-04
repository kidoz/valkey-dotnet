namespace ValkeyDotNet;

/// <summary>What the client can prove about command delivery when an operation fails.</summary>
public enum ValkeyCommandDeliveryStatus
{
    /// <summary>The client proved that no command bytes were written.</summary>
    NotSent,

    /// <summary>Command bytes may have reached Valkey, so application effects are unknown.</summary>
    MayHaveBeenSent,

    /// <summary>Valkey returned a complete reply for the command.</summary>
    ReplyReceived,
}
