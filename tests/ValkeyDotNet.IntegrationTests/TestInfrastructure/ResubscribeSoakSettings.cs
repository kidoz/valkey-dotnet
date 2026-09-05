using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal static class ResubscribeSoakSettings
{
    internal const int WarmupCycles = 4;
    internal const long HeapGrowthBudget = 16 * 1024 * 1024;
    internal const int HandleGrowthBudget = 32;

    internal static int ParseCycles(string? text)
    {
        if (
            !int.TryParse(text ?? "30", NumberStyles.None, CultureInfo.InvariantCulture, out var cycles)
            || cycles is < 20 or > 100
        )
        {
            throw new InvalidOperationException("VALKEYDOTNET_RESUBSCRIBE_CYCLES must be between 20 and 100.");
        }
        return cycles;
    }

    internal static int CountNamedClients(string clientList, string name) =>
        clientList
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Count(line => line.Split(' ').Contains("name=" + name, StringComparer.Ordinal));
}
