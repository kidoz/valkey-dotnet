namespace ValkeyDotNet;

/// <summary>A cluster pipeline command paired with the key used to route it.</summary>
public sealed class ValkeyClusterCommand
{
    public ValkeyClusterCommand(ValkeyArgument routingKey, ValkeyCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        RoutingKey = routingKey;
        Command = command;
    }

    public ValkeyArgument RoutingKey { get; }

    public ValkeyCommand Command { get; }
}
