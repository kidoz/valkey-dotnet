using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;

namespace ValkeyDotNet.Benchmarks;

internal enum NotificationOperation
{
    Publish,
    TrackedInvalidation,
    BroadcastInvalidation,
}

internal static class NotificationWorkload
{
    internal const int PayloadBytes = 1024;
    internal const int QueueCapacity = 8192;
    internal const int WarmupIterations = 64;
    internal const int Iterations = 512;

    internal static async Task<NotificationMeasurement> RunAsync(
        ValkeyClientOptions options,
        string prefix,
        NotificationOperation operation,
        int concurrency,
        int warmupIterations,
        int iterations,
        CancellationToken token
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(prefix);
        if (
            !prefix.StartsWith("valkey-dotnet-bench-", StringComparison.Ordinal)
            || prefix.Length > 80
            || !Enum.IsDefined(operation)
            || concurrency is not (1 or 8)
            || warmupIterations is < 1 or > WarmupIterations
            || iterations is < 1 or > Iterations
            || (operation != NotificationOperation.Publish && options.Protocol != ValkeyProtocol.Resp3)
        )
        {
            throw new ArgumentException("Invalid bounded notification workload.");
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(45));
        var runToken = deadline.Token;
        await using var writer = await ValkeyClient.ConnectAsync(options, runToken);
        var warmCount = concurrency * warmupIterations;
        var count = warmCount + concurrency * iterations;
        var warm = new NotificationSamples(warmCount);
        var measured = new NotificationSamples(concurrency * iterations);
        var stem = Encoding.UTF8.GetBytes(prefix + ":notifications:\0\r\n:");
        var keys = new byte[count][];
        var payloads = new byte[count][];
        var commands = new ValkeyCommand[count];
        for (var index = 0; index < count; index++)
        {
            keys[index] = new byte[stem.Length + sizeof(int)];
            stem.CopyTo(keys[index], 0);
            BinaryPrimitives.WriteInt32BigEndian(keys[index].AsSpan(stem.Length), index);
            payloads[index] = Enumerable.Range(0, PayloadBytes).Select(value => (byte)value).ToArray();
            BinaryPrimitives.WriteInt32BigEndian(payloads[index], index);
            commands[index] =
                operation == NotificationOperation.Publish
                    ? new ValkeyCommand("PUBLISH", stem, payloads[index])
                    : new ValkeyCommand("SET", keys[index], payloads[index], "PX", 600000);
        }

        await using var subscriber =
            operation == NotificationOperation.Publish
                ? await ValkeySubscriber.ConnectAsync(
                    new ValkeySubscriberOptions { Connection = options, QueueCapacity = QueueCapacity },
                    runToken
                )
                : null;
        ValkeySubscription? subscription = null;
        await using var tracking =
            operation != NotificationOperation.Publish
                ? new ValkeyTrackingClient(
                    new ValkeyConnectionOwnerOptions { Connection = options, MaxConnectAttempts = 1 },
                    new ValkeyTrackingOptions
                    {
                        Broadcast = operation == NotificationOperation.BroadcastInvalidation,
                        Prefixes = operation == NotificationOperation.BroadcastInvalidation ? [stem] : [],
                        QueueCapacity = QueueCapacity,
                    }
                )
                : null;
        Task? receiver = null;
        using var receiving = CancellationTokenSource.CreateLinkedTokenSource(runToken);
        try
        {
            if (subscriber is not null)
            {
                subscription = await subscriber.SubscribeAsync(stem, runToken);
                receiver = ReceivePublishAsync(subscription);
            }
            else
            {
                ArgumentNullException.ThrowIfNull(tracking);
                foreach (var chunk in keys.Chunk(100))
                {
                    foreach (
                        var reply in await writer.ExecutePipelineAsync(
                            chunk.Select(key => new ValkeyCommand("SET", key, "seed", "PX", 600000)),
                            runToken
                        )
                    )
                    {
                        RequireOk(reply);
                    }
                }
                await tracking.ConnectAsync(runToken);
                if (operation == NotificationOperation.TrackedInvalidation)
                {
                    foreach (var chunk in keys.Chunk(100))
                    {
                        foreach (
                            var reply in await tracking.ExecutePipelineAsync(
                                chunk.Select(key => new ValkeyCommand("GET", key)),
                                runToken
                            )
                        )
                        {
                            if (reply.AsString() != "seed")
                            {
                                throw new InvalidOperationException("Tracking pre-registration failed.");
                            }
                        }
                    }
                }
                receiver = ReceiveInvalidationsAsync(tracking);
            }

            await SendPhaseAsync(warm, 0, warmupIterations);
            await warm.Delivered.WaitAsync(runToken);
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var sends = SendPhaseAsync(measured, warmCount, iterations, gate.Task);
            var allocated = GC.GetTotalAllocatedBytes(precise: true);
            var start = Stopwatch.GetTimestamp();
            gate.SetResult();
            await sends;
            var acknowledgmentsEnd = Stopwatch.GetTimestamp();
            await measured.Delivered.WaitAsync(runToken);
            allocated = GC.GetTotalAllocatedBytes(precise: true) - allocated;

            // Stop and join the consumer before disposing tracking (which emits a final reset).
            await receiving.CancelAsync();
            await receiver;
            if (
                (subscription?.DroppedMessages ?? 0) != 0
                || (subscriber?.ConnectionLosses ?? 0) != 0
                || (subscriber?.ReconnectAttempts ?? 0) != 0
                || (tracking?.QueueOverflows ?? 0) != 0
            )
            {
                throw new InvalidOperationException(
                    "Notification loss or connection recovery invalidates the measurement."
                );
            }
            var result = measured.Summarize(
                operation,
                options.Protocol,
                concurrency,
                start,
                acknowledgmentsEnd,
                allocated
            );
            if (subscription is not null)
            {
                await subscription.UnsubscribeAsync(runToken);
                var counts = await writer.ExecuteAsync(new ValkeyCommand("PUBSUB", "NUMSUB", stem), runToken);
                if (counts.AsArray()[1].AsInt64() != 0)
                {
                    throw new InvalidOperationException("Benchmark subscription was not removed.");
                }
            }
            if (tracking is not null)
            {
                await tracking.DisposeAsync();
                foreach (var chunk in keys.Chunk(100))
                {
                    var replies = await writer.ExecutePipelineAsync(
                        chunk.Select(key => new ValkeyCommand("GET", key)),
                        runToken
                    );
                    for (var index = 0; index < chunk.Length; index++)
                    {
                        var id = ReadId(chunk[index], stem, count);
                        if (!replies[index].AsBytes().Span.SequenceEqual(payloads[id]))
                        {
                            throw new InvalidOperationException("Final binary value validation failed.");
                        }
                    }
                    var deleted = await writer.ExecuteAsync(
                        new ValkeyCommand("DEL", chunk.Select(key => new ValkeyArgument(key)).ToArray()),
                        runToken
                    );
                    if (deleted.AsInt64() != chunk.Length)
                    {
                        throw new InvalidOperationException("Benchmark keys were not removed.");
                    }
                }
            }
            return result;
        }
        finally
        {
            await receiving.CancelAsync();
            if (receiver is not null)
            {
                await receiver;
            }
        }

        Task SendPhaseAsync(NotificationSamples samples, int offset, int perWorker, Task? gate = null)
        {
            return Task.WhenAll(
                Enumerable
                    .Range(0, concurrency)
                    .Select(async worker =>
                    {
                        if (gate is not null)
                        {
                            await gate.WaitAsync(runToken);
                        }
                        for (var iteration = 0; iteration < perWorker; iteration++)
                        {
                            var index = worker * perWorker + iteration;
                            samples.Begin(index, Stopwatch.GetTimestamp());
                            var reply = await writer.ExecuteAsync(commands[offset + index], runToken);
                            samples.Acknowledge(index, Stopwatch.GetTimestamp());
                            if (operation == NotificationOperation.Publish)
                            {
                                if (reply.AsInt64() != 1)
                                {
                                    throw new InvalidOperationException(
                                        "PUBLISH did not acknowledge exactly one subscriber."
                                    );
                                }
                            }
                            else
                            {
                                RequireOk(reply);
                            }
                        }
                    })
            );
        }

        void Deliver(int id, long timestamp)
        {
            if (id < warmCount)
            {
                warm.Deliver(id, timestamp);
            }
            else
            {
                measured.Deliver(id - warmCount, timestamp);
            }
        }

        async Task ReceivePublishAsync(ValkeySubscription source)
        {
            try
            {
                await foreach (var message in source.ReadAllAsync(receiving.Token))
                {
                    var timestamp = Stopwatch.GetTimestamp();
                    var id = ReadId(message.Payload.Span, [], count, PayloadBytes);
                    if (
                        !message.Channel.Span.SequenceEqual(stem)
                        || !message.Payload.Span.SequenceEqual(payloads[id])
                        || message.Pattern is not null
                        || message.IsSharded
                    )
                    {
                        throw new InvalidOperationException("Unexpected binary Pub/Sub delivery.");
                    }
                    Deliver(id, timestamp);
                }
            }
            catch (OperationCanceledException) when (receiving.IsCancellationRequested) { }
            catch
            {
                await deadline.CancelAsync();
                throw;
            }
        }

        async Task ReceiveInvalidationsAsync(ValkeyTrackingClient source)
        {
            long version = 0;
            try
            {
                await foreach (var invalidation in source.ReadInvalidationsAsync(receiving.Token))
                {
                    var timestamp = Stopwatch.GetTimestamp();
                    if (invalidation.InvalidateAll || invalidation.Version <= version || invalidation.Keys.Count == 0)
                    {
                        throw new InvalidOperationException("Unexpected invalidation reset or version.");
                    }
                    version = invalidation.Version;
                    foreach (var key in invalidation.Keys)
                    {
                        Deliver(ReadId(key.Span, stem, count), timestamp);
                    }
                }
            }
            catch (OperationCanceledException) when (receiving.IsCancellationRequested) { }
            catch
            {
                await deadline.CancelAsync();
                throw;
            }
        }
    }

    internal static int ReadId(ReadOnlySpan<byte> value, ReadOnlySpan<byte> prefix, int count, int? length = null)
    {
        if (
            value.Length != (length ?? prefix.Length + sizeof(int))
            || value.Length < prefix.Length + sizeof(int)
            || !value.StartsWith(prefix)
        )
        {
            throw new InvalidOperationException("Unexpected notification identity.");
        }
        var id = BinaryPrimitives.ReadInt32BigEndian(value[prefix.Length..]);
        if (id < 0 || id >= count)
        {
            throw new InvalidOperationException("Notification identity is out of range.");
        }
        return id;
    }

    private static void RequireOk(RespValue reply)
    {
        if (reply.AsString() != "OK")
        {
            throw new InvalidOperationException("Mutation was not acknowledged.");
        }
    }
}
