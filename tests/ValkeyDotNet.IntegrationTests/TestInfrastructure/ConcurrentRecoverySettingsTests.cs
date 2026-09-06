using ValkeyDotNet.Benchmarks;

namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

public sealed class ConcurrentRecoverySettingsTests
{
    private const string Project = "valkey-dotnet-bench-test";

    [Theory]
    [InlineData(null, 20)]
    [InlineData("20", 20)]
    [InlineData("100", 100)]
    public void CyclesAreExplicitlyBounded(string? value, int expected)
    {
        Assert.Equal(expected, ConcurrentRecoverySettings.ParseCycles(value));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("19")]
    [InlineData("101")]
    [InlineData(" 20")]
    [InlineData("+20")]
    [InlineData("invalid")]
    public void InvalidCycleCountsAreRejected(string value)
    {
        Assert.Throws<InvalidOperationException>(() => ConcurrentRecoverySettings.ParseCycles(value));
    }

    [Theory]
    [InlineData("foreign", "owner", 0)]
    [InlineData("valkey-dotnet-bench-bad name", "owner", 0)]
    [InlineData(Project, "other", 0)]
    [InlineData(Project, "owner", 4)]
    public void UnscopedIdentitiesAreRejected(string project, string role, int index)
    {
        Assert.Throws<InvalidOperationException>(() => ConcurrentRecoverySettings.Name(project, role, index));
    }

    [Fact]
    public void OnlyExactWorkerIdsCanBecomeFaultTargets()
    {
        var clients = SteadyClients();
        Assert.Equal(
            new long[] { 3, 4, 5, 6, 7, 8, 9, 10 },
            ConcurrentRecoverySettings.SelectTargets(clients, Project, ValkeyProtocol.Resp3)
        );
        Assert.Throws<InvalidOperationException>(() =>
            ConcurrentRecoverySettings.SelectTargets(clients[..^1], Project, ValkeyProtocol.Resp3)
        );
        Assert.Throws<InvalidOperationException>(() =>
            ConcurrentRecoverySettings.SelectTargets(clients, Project, ValkeyProtocol.Resp2)
        );
    }

    [Theory]
    [InlineData("foreign")]
    [InlineData("duplicate-name")]
    [InlineData("duplicate-id")]
    [InlineData("database")]
    [InlineData("subscription")]
    [InlineData("invalid-id")]
    public void ChangedFaultIdentityIsRejected(string mutation)
    {
        var clients = SteadyClients();
        clients[2] = mutation switch
        {
            "foreign" => clients[2] with { Name = "foreign" },
            "duplicate-name" => clients[2] with { Name = clients[3].Name },
            "duplicate-id" => clients[2] with { Id = clients[0].Id },
            "database" => clients[2] with { Database = 0 },
            "subscription" => clients[2] with { Subscriptions = 1 },
            _ => clients[2] with { Id = -1 },
        };
        Assert.Throws<InvalidOperationException>(() =>
            ConcurrentRecoverySettings.SelectTargets(clients, Project, ValkeyProtocol.Resp3)
        );
    }

    [Theory]
    [InlineData("id=1 name=x db=1 resp=3 sub=0 id=2")]
    [InlineData("id=0 name=x db=1 resp=3 sub=0")]
    [InlineData("id=-1 name=x db=1 resp=3 sub=0")]
    [InlineData("id=1 db=1 resp=3 sub=0")]
    [InlineData("id=1 name=x db=x resp=3 sub=0")]
    [InlineData("id=1 name=x db=1 resp=3 sub=0\nid=1 name=y db=1 resp=3 sub=0")]
    public void MalformedClientObservationsFailClosed(string value)
    {
        Assert.Throws<InvalidOperationException>(() => ConcurrentRecoverySettings.ParseClients(value));
    }

    [Fact]
    public void ObservationSizesAndClientCountsAreBounded()
    {
        Assert.Throws<InvalidOperationException>(() => ConcurrentRecoverySettings.ParseClients(new string('x', 65537)));
        Assert.Throws<InvalidOperationException>(() =>
            ConcurrentRecoverySettings.ParseClients(string.Join('\n', Enumerable.Repeat("x", 11)))
        );
        Assert.Equal("", Assert.Single(ConcurrentRecoverySettings.ParseClients("id=1 name= db=0 resp=2 sub=0")).Name);
    }

    [Fact]
    public void AcceptedConnectionCounterRejectsMissingDuplicateAndInvalidValues()
    {
        Assert.Equal(
            42,
            ConcurrentRecoverySettings.ConnectionsReceived("# Stats\r\ntotal_connections_received:42\r\n")
        );
        Assert.Throws<InvalidOperationException>(() => ConcurrentRecoverySettings.ConnectionsReceived(""));
        Assert.Throws<InvalidOperationException>(() =>
            ConcurrentRecoverySettings.ConnectionsReceived("total_connections_received:-1")
        );
        Assert.Throws<InvalidOperationException>(() =>
            ConcurrentRecoverySettings.ConnectionsReceived("total_connections_received:1\ntotal_connections_received:2")
        );
        Assert.Throws<InvalidOperationException>(() =>
            ConcurrentRecoverySettings.ConnectionsReceived(new string('x', 65537))
        );
    }

    [Fact]
    public async Task UnstartedContainerCannotAuthorizeFaultInjection()
    {
        await using var server = new OwnedBenchmarkServer();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            server.VerifyRunningOwnershipAsync(TestContext.Current.CancellationToken)
        );
    }

    private static RecoveryClient[] SteadyClients()
    {
        var clients = new List<RecoveryClient>
        {
            new(1, ConcurrentRecoverySettings.Name(Project, "control"), 0, 3, 0),
            new(2, ConcurrentRecoverySettings.Name(Project, "sampler"), 0, 3, 0),
        };
        foreach (var role in new[] { "owner", "subscriber" })
        {
            for (var index = 0; index < 4; index++)
            {
                clients.Add(
                    new RecoveryClient(
                        clients.Count + 1,
                        ConcurrentRecoverySettings.Name(Project, role, index),
                        1,
                        3,
                        role == "subscriber" ? 1 : 0
                    )
                );
            }
        }
        return clients.ToArray();
    }
}
