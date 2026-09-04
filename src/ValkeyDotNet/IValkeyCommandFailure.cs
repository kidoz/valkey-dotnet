namespace ValkeyDotNet;

/// <summary>Exposes command-delivery certainty for failures that occur during command execution.</summary>
public interface IValkeyCommandFailure
{
    /// <summary>What the client can prove about delivery of the failed command.</summary>
    ValkeyCommandDeliveryStatus DeliveryStatus { get; }
}
