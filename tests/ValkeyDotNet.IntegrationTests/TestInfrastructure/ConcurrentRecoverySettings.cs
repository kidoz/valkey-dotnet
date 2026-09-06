using System.Globalization;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

internal sealed record RecoveryClient(long Id, string Name, int Database, int Protocol, int Subscriptions);

internal static class ConcurrentRecoverySettings
{
    internal const int Participants = 4;
    internal const int CallersPerOwner = 16;
    internal const int WarmupCycles = 2;
    internal const int ExpectedClients = Participants * 2 + 2;
    internal const long HeapGrowthBudget = 16 * 1024 * 1024;

    internal static int ParseCycles(string? value)
    {
        if (
            !int.TryParse(value ?? "20", NumberStyles.None, CultureInfo.InvariantCulture, out var cycles)
            || cycles is < 20 or > 100
        )
        {
            throw new InvalidOperationException("VALKEYDOTNET_CONCURRENT_RECOVERY_CYCLES must be between 20 and 100.");
        }
        return cycles;
    }

    internal static string Name(string project, string role, int index = 0)
    {
        if (
            !project.StartsWith("valkey-dotnet-bench-", StringComparison.Ordinal)
            || project.Length > 80
            || !project.All(character => char.IsAsciiLetterOrDigit(character) || character == '-')
            || role is not ("owner" or "subscriber" or "control" or "sampler")
            || index is < 0 or >= Participants
        )
        {
            throw new InvalidOperationException("Invalid owned recovery identity.");
        }
        return project + ":" + role + ":" + index.ToString(CultureInfo.InvariantCulture);
    }

    internal static RecoveryClient[] ParseClients(string value)
    {
        if (value.Length > 65536)
        {
            throw new InvalidOperationException("Client observation exceeded its bound.");
        }
        var lines = value.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (lines.Length > ExpectedClients)
        {
            throw new InvalidOperationException("Concurrent recovery exceeded its server-client bound.");
        }
        var clients = new List<RecoveryClient>();
        foreach (var line in lines)
        {
            var fields = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var field in line.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            {
                var equals = field.IndexOf('=', StringComparison.Ordinal);
                if (equals < 1 || !fields.TryAdd(field[..equals], field[(equals + 1)..]))
                {
                    throw new InvalidOperationException("Malformed client observation.");
                }
            }
            if (!fields.TryGetValue("name", out var name))
            {
                throw new InvalidOperationException("Missing client identity.");
            }
            var id = Number(fields, "id");
            if (id == 0 || clients.Any(client => client.Id == id))
            {
                throw new InvalidOperationException("Invalid or duplicate client ID.");
            }
            clients.Add(
                new RecoveryClient(
                    id,
                    name,
                    checked((int)Number(fields, "db")),
                    checked((int)Number(fields, "resp")),
                    checked((int)Number(fields, "sub"))
                )
            );
        }
        return clients.ToArray();
    }

    internal static long[] SelectTargets(RecoveryClient[] clients, string project, ValkeyProtocol protocol)
    {
        if (
            clients.Length != ExpectedClients
            || clients.Any(client => client.Id <= 0)
            || clients.Select(client => client.Id).Distinct().Count() != clients.Length
            || protocol is not (ValkeyProtocol.Resp2 or ValkeyProtocol.Resp3)
        )
        {
            throw new InvalidOperationException("Fault injection requires the exact steady-state client set.");
        }
        var expected = new HashSet<string>(StringComparer.Ordinal)
        {
            Name(project, "control"),
            Name(project, "sampler"),
        };
        var targets = new List<long>();
        foreach (var role in new[] { "owner", "subscriber" })
        {
            for (var index = 0; index < Participants; index++)
            {
                var name = Name(project, role, index);
                expected.Add(name);
                var matching = clients.Where(client => client.Name == name).ToArray();
                if (
                    matching.Length != 1
                    || matching[0].Database != 1
                    || matching[0].Protocol != (int)protocol
                    || matching[0].Subscriptions != (role == "subscriber" ? 1 : 0)
                )
                {
                    throw new InvalidOperationException("Fault target identity or restored settings do not match.");
                }
                targets.Add(matching[0].Id);
            }
        }
        if (
            clients.Any(client => !expected.Remove(client.Name))
            || expected.Count != 0
            || targets.Distinct().Count() != Participants * 2
        )
        {
            throw new InvalidOperationException("Unexpected or duplicate client identity; refusing injection.");
        }
        return targets.ToArray();
    }

    internal static long ConnectionsReceived(string info)
    {
        if (info.Length > 65536)
        {
            throw new InvalidOperationException("Server statistics exceeded their bound.");
        }
        const string prefix = "total_connections_received:";
        var fields = info.Split('\n', StringSplitOptions.TrimEntries)
            .Where(line => line.StartsWith(prefix, StringComparison.Ordinal))
            .ToArray();
        if (
            fields.Length != 1
            || !long.TryParse(
                fields[0].AsSpan(prefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var count
            )
        )
        {
            throw new InvalidOperationException("Invalid accepted-connection counter.");
        }
        return count;
    }

    private static long Number(Dictionary<string, string> fields, string key)
    {
        if (
            !fields.TryGetValue(key, out var value)
            || !long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var result)
        )
        {
            throw new InvalidOperationException("Invalid numeric client field.");
        }
        return result;
    }
}
