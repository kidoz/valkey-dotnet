namespace ValkeyDotNet.IntegrationTests.TestInfrastructure;

// Harness-only checks: these never start Docker or mutate a server.
public sealed class MigrationValkeyClusterTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesReplyLossBeforeOwnedClusterInitialization(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        var key = System.Text.Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.MigrateOwnedKeyWithLostReplyAsync(key, ValkeyProtocol.Resp3, TestContext.Current.CancellationToken)
        );
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.MigrateOwnedKeyWithLostReplyAsync(
                "unrelated"u8.ToArray(),
                ValkeyProtocol.Resp3,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task MigrationDebugIsExplicitLocalOnlyAndSeparateFromFailover()
    {
        await using var normal = new MigrationValkeyCluster();
        await using var rollback = new MigrationValkeyCluster(enableMigrationDebug: true);
        Assert.Equal("no", normal.MigrationDebugMode);
        Assert.Equal("local", rollback.MigrationDebugMode);
        Assert.StartsWith("valkey-dotnet-rollback-tests-", rollback.Project, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() =>
            new MigrationValkeyCluster(includeReplica: true, enableMigrationDebug: true)
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesAtomicRollbackBeforeOwnedDebugClusterInitialization(bool enableMigrationDebug)
    {
        await using var cluster = new MigrationValkeyCluster(enableMigrationDebug: enableMigrationDebug);
        var prefix = System.Text.Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.FailAtomicSlotMigrationAfterSnapshotAsync(
                [.. prefix, 1],
                [.. prefix, 2],
                ValkeyProtocol.Resp3,
                _ => Task.CompletedTask,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task RollbackRefusesUnscopedOrDuplicateKeys()
    {
        await using var cluster = new MigrationValkeyCluster(enableMigrationDebug: true);
        var key = System.Text.Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.FailAtomicSlotMigrationAfterSnapshotAsync(
                "external"u8.ToArray(),
                key,
                ValkeyProtocol.Resp2,
                _ => Task.CompletedTask,
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.FailAtomicSlotMigrationAfterSnapshotAsync(
                key,
                key,
                ValkeyProtocol.Resp2,
                _ => Task.CompletedTask,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesAtomicWritersBeforeOwnedDebugClusterInitialization(bool enableMigrationDebug)
    {
        await using var cluster = new MigrationValkeyCluster(enableMigrationDebug: enableMigrationDebug);
        var prefix = System.Text.Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        var invoked = false;
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.CompleteAtomicSlotMigrationAfterWritesAsync(
                [.. prefix, 1],
                [.. prefix, 2],
                ValkeyProtocol.Resp3,
                _ =>
                {
                    invoked = true;
                    return Task.CompletedTask;
                },
                TestContext.Current.CancellationToken
            )
        );
        Assert.False(invoked);
    }

    [Fact]
    public async Task AtomicWritersRefuseUnscopedDuplicateAndDifferentSlotKeys()
    {
        await using var cluster = new MigrationValkeyCluster(enableMigrationDebug: true);
        var key = System.Text.Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}");
        var other = System.Text.Encoding.UTF8.GetBytes("{" + cluster.Project + ":1}");
        Assert.NotEqual(ValkeyClusterClient.GetHashSlot(key), ValkeyClusterClient.GetHashSlot(other));
        var invoked = false;
        Task WriteAsync(CancellationToken token)
        {
            invoked = true;
            return Task.CompletedTask;
        }
        await Assert.ThrowsAsync<ArgumentException>(() =>
            cluster.CompleteAtomicSlotMigrationAfterWritesAsync(
                "external"u8.ToArray(),
                key,
                ValkeyProtocol.Resp2,
                WriteAsync,
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.CompleteAtomicSlotMigrationAfterWritesAsync(
                key,
                key,
                ValkeyProtocol.Resp2,
                WriteAsync,
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.CompleteAtomicSlotMigrationAfterWritesAsync(
                key,
                other,
                ValkeyProtocol.Resp2,
                WriteAsync,
                TestContext.Current.CancellationToken
            )
        );
        Assert.False(invoked);
    }

    [Theory]
    [InlineData("")]
    [InlineData("id=1 flags=N")]
    [InlineData("id=0 flags=E")]
    [InlineData("id=-1 flags=E")]
    [InlineData("id=+1 flags=E")]
    [InlineData("id=18446744073709551616 flags=E")]
    [InlineData("id=1 flags=E\nid=2 flags=E")]
    [InlineData("id=1 id=2 flags=E")]
    [InlineData("id=1 flags=E flags=N")]
    [InlineData("id=1")]
    [InlineData("flags=E")]
    [InlineData("id=1 flags=E malformed")]
    public void ExportClientSelectionRejectsAmbiguousOrInvalidIdentity(string text)
    {
        Assert.Throws<InvalidOperationException>(() => MigrationValkeyCluster.ParseExportClientId(text));
    }

    [Fact]
    public void ExportClientSelectionIsBoundedAndKeepsExactId()
    {
        Assert.Equal("123", MigrationValkeyCluster.ParseExportClientId("id=123 addr=127.0.0.1:6379 flags=E name=\n"));
        Assert.Throws<InvalidOperationException>(() =>
            MigrationValkeyCluster.ParseExportClientId(new string('x', 16385))
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesAtomicCancellationBeforeOwnedClusterInitialization(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.CancelAtomicSlotMigrationBeforeTransferAsync(
                0,
                0,
                1,
                ValkeyProtocol.Resp3,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Theory]
    [InlineData(-1, 0, 1)]
    [InlineData(16384, 0, 1)]
    [InlineData(0, -1, 1)]
    [InlineData(0, 0, 3)]
    [InlineData(0, 1, 1)]
    public async Task RefusesAtomicCancellationOutsideOwnedSlotAndNodes(int slot, int source, int target)
    {
        await using var cluster = new MigrationValkeyCluster();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cluster.CancelAtomicSlotMigrationBeforeTransferAsync(
                slot,
                source,
                target,
                ValkeyProtocol.Resp2,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Theory]
    [InlineData("cancelled")]
    [InlineData("failed")]
    [InlineData("success")]
    [InlineData("snapshot")]
    public void AtomicJobStatePreservesTerminalOutcomeWithoutTreatingCancellationAsSuccess(string state)
    {
        var fields = AtomicJobFields();
        fields["state"] = state;
        Assert.Equal(
            state,
            MigrationValkeyCluster.AtomicJobState(
                fields,
                "EXPORT",
                42,
                new string('b', 40),
                new string('c', 40),
                fields["name"]
            )
        );
        fields["target_node"] = new string('d', 40);
        Assert.Throws<InvalidOperationException>(() =>
            MigrationValkeyCluster.AtomicJobState(
                fields,
                "EXPORT",
                42,
                new string('b', 40),
                new string('c', 40),
                fields["name"]
            )
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesAtomicMigrationBeforeOwnedClusterInitialization(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.RunAtomicSlotMigrationAsync(0, 0, 1, ValkeyProtocol.Resp3, TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData(-1, 0, 1)]
    [InlineData(16384, 0, 1)]
    [InlineData(0, -1, 1)]
    [InlineData(0, 0, 3)]
    [InlineData(0, 1, 1)]
    public async Task RefusesAtomicMigrationOutsideOwnedSlotAndNodes(int slot, int source, int target)
    {
        await using var cluster = new MigrationValkeyCluster();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cluster.RunAtomicSlotMigrationAsync(
                slot,
                source,
                target,
                ValkeyProtocol.Resp2,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Theory]
    [InlineData("success", true)]
    [InlineData("snapshot", false)]
    public void AtomicJobRequiresSuccessfulTerminalState(string state, bool success)
    {
        var fields = AtomicJobFields();
        fields["state"] = state;
        Assert.Equal(
            success,
            MigrationValkeyCluster.ValidateAtomicJob(
                fields,
                "EXPORT",
                42,
                new string('b', 40),
                new string('c', 40),
                null
            )
        );
    }

    [Theory]
    [InlineData("name", "invalid")]
    [InlineData("operation", "IMPORT")]
    [InlineData("source_node", "unexpected")]
    [InlineData("target_node", "unexpected")]
    [InlineData("slot_ranges", "42-43")]
    [InlineData("slot_ranges", "42-42 44-44")]
    [InlineData("state", "failed")]
    [InlineData("state", "cancelled")]
    [InlineData("state", "")]
    [InlineData("state", null)]
    public void AtomicJobRejectsUnexpectedIdentityOrFailedState(string field, string? value)
    {
        var fields = AtomicJobFields();
        if (value is null)
        {
            fields.Remove(field);
        }
        else
        {
            fields[field] = value;
        }
        Assert.Throws<InvalidOperationException>(() =>
            MigrationValkeyCluster.ValidateAtomicJob(
                fields,
                "EXPORT",
                42,
                new string('b', 40),
                new string('c', 40),
                null
            )
        );
    }

    [Fact]
    public void AtomicJobCannotChangeIdentityDuringPolling()
    {
        Assert.Throws<InvalidOperationException>(() =>
            MigrationValkeyCluster.ValidateAtomicJob(
                AtomicJobFields(),
                "EXPORT",
                42,
                new string('b', 40),
                new string('c', 40),
                new string('d', 40)
            )
        );
    }

    private static Dictionary<string, string> AtomicJobFields() =>
        new(StringComparer.Ordinal)
        {
            ["name"] = new string('a', 40),
            ["operation"] = "EXPORT",
            ["slot_ranges"] = "42-42",
            ["source_node"] = new string('b', 40),
            ["target_node"] = new string('c', 40),
            ["state"] = "success",
        };

    [Theory]
    [InlineData(-1)]
    [InlineData(3)]
    public async Task RefusesCutoverWithUnexpectedTargetKeyBudget(int expectedTargetKeys)
    {
        await using var cluster = new MigrationValkeyCluster();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cluster.CompleteSlotMigrationAsync(0, 0, 1, expectedTargetKeys, TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task TransferKeysAreBoundedAndScopedToOwnedProject()
    {
        await using var cluster = new MigrationValkeyCluster();
        byte[] valid = [.. System.Text.Encoding.UTF8.GetBytes("{" + cluster.Project + ":0}"), 0, 255, 13, 10];
        cluster.ValidateTransferKey(valid);
        Assert.Throws<ArgumentException>(() => cluster.ValidateTransferKey("unrelated"u8.ToArray()));
        Assert.Throws<ArgumentException>(() => cluster.ValidateTransferKey([.. valid, .. new byte[512]]));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.MigrateOwnedKeyAsync(valid, 0, 1, 2, ValkeyProtocol.Resp3, TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(3)]
    public async Task RefusesMigrationWithUnexpectedKeyBudget(int expectedSourceKeys)
    {
        await using var cluster = new MigrationValkeyCluster();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cluster.BeginSlotMigrationAsync(0, 0, 1, expectedSourceKeys, TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesMigrationBeforeOwnedClusterInitialization(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.BeginSlotMigrationAsync(0, 0, 1, 0, TestContext.Current.CancellationToken)
        );
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.CompleteEmptySlotMigrationAsync(0, 0, 1, TestContext.Current.CancellationToken)
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RefusesPrimaryStopBeforeOwnedReplicaInitialization(bool includeReplica)
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            cluster.StopOwnedPrimaryAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task ReplicaProfileMapsFourthNodeAndKeepsSeedSelectionExplicit()
    {
        await using var cluster = new MigrationValkeyCluster(includeReplica: true);
        Assert.Equal(
            cluster.NodeOptions(1, ValkeyProtocol.Resp3).Port,
            Assert.Single(cluster.Options(ValkeyProtocol.Resp3, seed: 1).SeedNodes).Port
        );
        var mapped = cluster
            .Options(ValkeyProtocol.Resp3, seed: 1)
            .EndpointMapper!(new ValkeyClusterEndpoint("node-4", 6379));
        Assert.Equal(cluster.NodeOptions(3, ValkeyProtocol.Resp3).Port, mapped.Port);
        Assert.StartsWith("valkey-dotnet-failover-tests-", cluster.Project, StringComparison.Ordinal);
        Assert.Equal(
            4,
            Enumerable
                .Range(0, 4)
                .Select(index => cluster.NodeOptions(index, ValkeyProtocol.Resp3).Port)
                .Distinct()
                .Count()
        );
        Assert.Throws<InvalidOperationException>(() =>
            cluster.Options(ValkeyProtocol.Resp3).EndpointMapper!(new ValkeyClusterEndpoint("node-5", 6379))
        );
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("21")]
    [InlineData("invalid")]
    [InlineData("2147483648")]
    public void RejectsUnboundedOrInvalidCycleCount(string text)
    {
        Assert.Throws<InvalidOperationException>(() => MigrationValkeyCluster.ParseCycles(text));
    }

    [Fact]
    public void AcceptsDefaultAndBoundaryCycleCounts()
    {
        Assert.Equal(3, MigrationValkeyCluster.ParseCycles(null));
        Assert.Equal(1, MigrationValkeyCluster.ParseCycles("1"));
        Assert.Equal(20, MigrationValkeyCluster.ParseCycles("20"));
    }

    [Theory]
    [InlineData("external.invalid", 6379)]
    [InlineData("node-1", 6380)]
    [InlineData("node-4", 6379)]
    public async Task RejectsAnnouncementsOutsideOwnedNodes(string host, int port)
    {
        await using var cluster = new MigrationValkeyCluster();
        Assert.Throws<InvalidOperationException>(() =>
            cluster.Options(ValkeyProtocol.Resp3).EndpointMapper!(new ValkeyClusterEndpoint(host, port))
        );
    }

    [Fact]
    public async Task MapsOnlyOwnedNodesToDistinctLoopbackPorts()
    {
        await using var cluster = new MigrationValkeyCluster();
        var mapped = new HashSet<int>();
        for (var index = 0; index < 3; index++)
        {
            var node = cluster.NodeOptions(index, ValkeyProtocol.Resp2);
            var endpoint = cluster
                .Options(ValkeyProtocol.Resp2)
                .EndpointMapper!(new ValkeyClusterEndpoint("node-" + (index + 1), 6379));
            Assert.Equal("127.0.0.1", endpoint.Host);
            Assert.Equal(node.Port, endpoint.Port);
            Assert.Equal(ValkeyProtocol.Resp2, node.Protocol);
            Assert.True(mapped.Add(endpoint.Port));
        }
    }
}
