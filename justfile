set shell := ["bash", "-uc"]

solution := "ValkeyDotNet.slnx"
lib := "src/ValkeyDotNet"
tests := "tests/ValkeyDotNet.Tests"
integration_tests := "tests/ValkeyDotNet.IntegrationTests"
benchmarks := "benchmarks/ValkeyDotNet.Benchmarks"

# Disposable Valkey servers live under dev/.
compose := "dev/docker-compose.yml"
cluster_compose := "dev/docker-compose.cluster.yml"

# List available recipes.
default:
    @just --list

# Restore the local dotnet tools (CSharpier). Run once after cloning.
tools:
    dotnet tool restore

# Restore NuGet packages.
restore:
    dotnet restore {{ solution }}

# Build the solution.
build:
    dotnet build {{ solution }}

# Build in Release.
build-release:
    dotnet build -c Release {{ solution }}

# Run the server-free unit tests.
test:
    dotnet run --project {{ tests }}

# Run the live suite against a disposable server. Skips when no endpoint is set.
test-live endpoint="127.0.0.1:6379":
    VALKEYDOTNET_ENDPOINT={{ endpoint }} dotnet run --project {{ integration_tests }}

# Explicitly create and restart a fresh isolated Docker server; never targets an existing endpoint.
test-resilience $resilience_version="9.1" $resilience_cycles="3":
    @case "$resilience_version" in 9.1|8.1|7.2) ;; *) echo "Version must be 9.1, 8.1, or 7.2" >&2; exit 2 ;; esac
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_RESTART_TESTS=1 VALKEYDOTNET_RESILIENCE_VERSION="$resilience_version" \
    VALKEYDOTNET_RESILIENCE_CYCLES="$resilience_cycles" dotnet run --project {{ integration_tests }} -- \
    -method '*OwnedServerRestartsRecoverWithoutOfflineWriteReplayOrResourceGrowth' \
    -showLiveOutput -result-trx "artifacts/resilience/restart-$resilience_version.trx"

# Create a fresh local three-primary cluster, migrate one slot, and remove only owned resources.
test-migration $migration_cycles="3":
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_MIGRATION_TESTS=1 VALKEYDOTNET_MIGRATION_CYCLES="$migration_cycles" \
    dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedSlotMigrationPreservesShardedHandleAndStream' \
    -showLiveOutput -result-trx artifacts/resilience/migration.trx

# Repeatedly relocate retained streams on fresh clusters and sample settled resource bounds.
test-resubscribe-soak $resubscribe_cycles="30":
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_RESUBSCRIBE_SOAK=1 VALKEYDOTNET_RESUBSCRIBE_CYCLES="$resubscribe_cycles" \
    dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedResubscribeSoakPreservesStreamsAndBoundsSettledResources' \
    -showLiveOutput -result-trx artifacts/resilience/resubscribe-soak.trx

# Hold destination writes until one MIGRATE times out, then independently reconcile source-only placement.
test-migrate-ioerr:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_MIGRATE_IOERR_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedMigrateIoErrorBeforeRestorePreservesSourceDataAndStream' \
    -showLiveOutput -result-trx artifacts/resilience/migrate-ioerr.trx

# Confirm the first bulk transfer and lose the second RESTORE acknowledgment without replay.
test-bulk-ack-loss:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_BULK_ACK_LOSS_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedBulkAcknowledgmentLossReconcilesMovedAndDuplicateKeysWithoutReplay' \
    -showLiveOutput -result-trx artifacts/resilience/bulk-ack-loss.trx

# Reconcile a two-key MIGRATE's partial success with the conflict first and last.
test-bulk-conflict:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_BULK_CONFLICT_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedBulkConflictReconcilesPartialSuccessWithoutReplay' \
    -showLiveOutput -result-trx artifacts/resilience/bulk-conflict.trx

# Withhold one successful RESTORE acknowledgment and independently observe both retained copies.
test-restore-ack-loss:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_RESTORE_ACK_LOSS_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedLostRestoreAcknowledgmentPreservesDuplicateCopiesAndSourceStream' \
    -showLiveOutput -result-trx artifacts/resilience/restore-ack-loss.trx

# Reject one conflicting transfer and preserve both owned copies without REPLACE or replay.
test-busykey:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_BUSYKEY_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedBusyKeyRejectionPreservesConflictingCopiesAndSourceStream' \
    -showLiveOutput -result-trx artifacts/resilience/busykey.trx

# Lose one completed MIGRATE reply and reconcile node-local placement without replay.
test-migrate-reply-loss:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_MIGRATE_REPLY_LOSS_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedLostMigrateReplyIsReconciledWithoutReplay' \
    -showLiveOutput -result-trx artifacts/resilience/migrate-reply-loss.trx

# Break one owned export connection after snapshot import and verify rollback and source delivery.
test-atomic-rollback:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_ATOMIC_ROLLBACK_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedAtomicLinkFailureCleansImportedKeysAndPreservesSourceStream' \
    -showLiveOutput -result-trx artifacts/resilience/atomic-rollback.trx

# Cancel one owned atomic migration before transfer and verify unchanged data and sharded delivery.
test-atomic-cancellation:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_ATOMIC_CANCELLATION_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedAtomicCancellationPreservesSourceKeysExpiryAndShardStream' \
    -showLiveOutput -result-trx artifacts/resilience/atomic-cancellation.trx

# Atomically move one owned slot and verify job completion, binary data, expiry, and sharded delivery.
test-atomic-migration:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_ATOMIC_MIGRATION_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedAtomicMigrationPreservesBinaryKeysExpiryAndShardStream' \
    -showLiveOutput -result-trx artifacts/resilience/atomic-migration.trx

# Transfer two owned binary keys and verify expiry and sharded delivery through cutover.
test-key-transfer:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_KEY_TRANSFER_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedNonemptyMigrationPreservesBinaryKeysExpiryAndShardStream' \
    -showLiveOutput -result-trx artifacts/resilience/key-transfer.trx

# Verify native command ASK and sharded delivery during an owned legacy migration.
test-ask:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_ASK_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedMigrationForcesCommandAskWhileShardStreamWaitsForCutover' \
    -showLiveOutput -result-trx artifacts/resilience/ask-migration.trx

# Stop an owned primary and verify replica promotion with healthy and unavailable discovery seeds.
test-failover:
    mkdir -p artifacts/resilience
    VALKEYDOTNET_RUN_FAILOVER_TESTS=1 dotnet run --configuration Release --project {{ integration_tests }} -- \
    -method '*OwnedPrimaryFailoverPreservesShardedStream' \
    -showLiveOutput -result-trx artifacts/resilience/failover.trx

# Start every supported Valkey line and wait until each is healthy.
valkey-up:
    docker compose -f {{ compose }} up -d --wait

# Stop the test servers and drop their volumes.
valkey-down:
    docker compose -f {{ compose }} down -v

# Report the server version behind each test port.
valkey-versions:
    #!/usr/bin/env bash
    set -uo pipefail
    for port in 6379 6380 6381; do
        printf 'port %s: ' "$port"
        docker compose -f {{ compose }} exec -T "valkey-$( [ $port = 6379 ] && echo 9 || { [ $port = 6380 ] && echo 8 || echo 7; } )" \
            valkey-cli info server 2>/dev/null | tr -d '\r' | grep '^valkey_version' || echo 'unavailable'
    done

# Run the suite against every maintained Valkey line. Requires `just valkey-up`.
test-matrix: valkey-up
    #!/usr/bin/env bash
    set -uo pipefail
    status=0
    for port in 6379 6380 6381; do
        echo "=== Valkey on port $port ==="
        VALKEYDOTNET_ENDPOINT=127.0.0.1:$port dotnet run --project {{ integration_tests }} || status=1
    done
    exit $status

# Start a disposable three-primary cluster and initialize its slot map.
cluster-up:
    #!/usr/bin/env bash
    set -euo pipefail
    compose=(docker compose -f {{ cluster_compose }})
    "${compose[@]}" up -d --wait valkey-cluster-1 valkey-cluster-2 valkey-cluster-3
    "${compose[@]}" run --rm cluster-init
    services=(valkey-cluster-1 valkey-cluster-2 valkey-cluster-3)
    ports=(16379 16380 16381)
    for index in 0 1 2; do
        ready=0
        for _ in {1..40}; do
            if "${compose[@]}" exec -T "${services[$index]}" \
                valkey-cli -p "${ports[$index]}" cluster info 2>/dev/null \
                | tr -d '\r' | grep -q '^cluster_state:ok$'; then
                ready=1
                break
            fi
            sleep 0.25
        done
        if [ "$ready" -ne 1 ]; then
            echo "${services[$index]} did not reach cluster_state:ok" >&2
            exit 1
        fi
    done

# Run the live cluster suite. Announced container hostnames are mapped to host-published ports.
test-cluster: cluster-up
    VALKEYDOTNET_CLUSTER_ENDPOINTS=127.0.0.1:16379,127.0.0.1:16380,127.0.0.1:16381 \
    VALKEYDOTNET_CLUSTER_MAPPED_HOST=127.0.0.1 \
    dotnet run --project {{ integration_tests }}

# Stop the disposable cluster and remove its containers and networks.
cluster-down:
    docker compose -f {{ cluster_compose }} down -v

# Run the BenchmarkDotNet suite. Release only; results feed BENCHMARKS.md.
bench *args:
    dotnet run -c Release --project {{ benchmarks }} -- {{ args }}

# Format all C# and project files with CSharpier.
format: tools
    dotnet csharpier format .

# Fail if any file is not CSharpier-formatted.
format-check: tools
    dotnet csharpier check .

# Produce the NuGet package in ./artifacts.
pack:
    dotnet pack {{ lib }} -c Release -o artifacts

# Everything CI should enforce: formatting, a warning-free build, and the tests.
ci: format-check build test

# Remove build output and packaging artifacts.
clean:
    dotnet clean {{ solution }} || true
    rm -rf artifacts
    find . -type d -name bin -prune -exec rm -rf {} +
    find . -type d -name obj -prune -exec rm -rf {} +
    rm -rf BenchmarkDotNet.Artifacts
