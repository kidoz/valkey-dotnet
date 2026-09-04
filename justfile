set shell := ["bash", "-uc"]

solution := "ValkeyDotNet.slnx"
lib := "src/ValkeyDotNet"
tests := "tests/ValkeyDotNet.Tests"
benchmarks := "benchmarks/ValkeyDotNet.Benchmarks"

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

# Run the unit tests. Live-server tests are skipped.
test:
    dotnet run --project {{ tests }}

# Run the tests including the live round trip against a disposable server.
test-live endpoint="127.0.0.1:6379":
    VALKEYDOTNET_ENDPOINT={{ endpoint }} dotnet run --project {{ tests }}

# Start every supported Valkey line and wait until each is healthy.
valkey-up:
    docker compose up -d --wait

# Stop the test servers and drop their volumes.
valkey-down:
    docker compose down -v

# Report the server version behind each test port.
valkey-versions:
    #!/usr/bin/env bash
    set -uo pipefail
    for port in 6379 6380 6381; do
        printf 'port %s: ' "$port"
        docker compose exec -T "valkey-$( [ $port = 6379 ] && echo 9 || { [ $port = 6380 ] && echo 8 || echo 7; } )" \
            valkey-cli info server 2>/dev/null | tr -d '\r' | grep '^valkey_version' || echo 'unavailable'
    done

# Run the suite against every maintained Valkey line. Requires `just valkey-up`.
test-matrix: valkey-up
    #!/usr/bin/env bash
    set -uo pipefail
    status=0
    for port in 6379 6380 6381; do
        echo "=== Valkey on port $port ==="
        VALKEYDOTNET_ENDPOINT=127.0.0.1:$port dotnet run --project {{ tests }} || status=1
    done
    exit $status

# Start a disposable three-primary cluster and initialize its slot map.
cluster-up:
    #!/usr/bin/env bash
    set -euo pipefail
    compose=(docker compose -f docker-compose.cluster.yml)
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
    dotnet run --project {{ tests }}

# Stop the disposable cluster and remove its containers and networks.
cluster-down:
    docker compose -f docker-compose.cluster.yml down -v

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
