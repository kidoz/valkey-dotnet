# Subscribe to messages

Use a dedicated subscriber and a separate command connection for publishing:

```csharp
await using var subscriber = await ValkeySubscriber.ConnectAsync(
    new ValkeySubscriberOptions
    {
        Connection = new ValkeyClientOptions
        {
            Host = "localhost",
            MaxResponseBytes = 1024 * 1024,
        },
        QueueCapacity = 64,
        MaxSubscriptions = 16,
        EnableReconnect = true,
    },
    cancellationToken);

await using var subscription = await subscriber.SubscribeAsync("cache:invalidations", cancellationToken);
await using var publisher = await ValkeyClient.ConnectAsync(cancellationToken: cancellationToken);
await publisher.ExecuteAsync(
    new ValkeyCommand("PUBLISH", "cache:invalidations", new byte[] { 0, 255, 13, 10 }),
    cancellationToken);

await foreach (var message in subscription.ReadAllAsync(cancellationToken))
{
    // Decode and process message.Payload here; failures belong to your application.
    // message.Channel and message.Payload are binary memory, not text.
    break;
}

await subscription.UnsubscribeAsync(cancellationToken);
```

For patterns, use `SubscribePatternAsync("cache:*")`. Keep each processing consumer's own handle;
multiple readers of one handle divide its messages. Poll `DroppedMessages` to detect queue overflow
and apply your cache's missed-invalidation policy. Cancelling an enumeration does not unsubscribe.

With `EnableReconnect = true`, the existing streams survive a recoverable transport loss and confirmed
subscriptions are restored within the configured retry/time budgets. Watch `IsReconnecting` and
`ConnectionLosses`; recovery does not repair missed cache invalidations. New subscribe calls during
recovery fail with `NotSent`, while unsubscribing removes a handle from restoration intent.

If `subscriber.Completion` finishes, inspect `subscriber.Failure` and create a new subscriber
explicitly if appropriate. Recovery is disabled by default, and authentication/protocol failures
remain terminal when enabled. Messages published during downtime or dropped because a queue is full
are not recovered. Use [TLS settings](connect-over-tls.md) outside a trusted network.

To run the gated live recovery test, first provide an explicitly disposable endpoint. It uses
`CLIENT KILL ID` only for its own unique test subscriber and never kills by connection type:

```bash
VALKEYDOTNET_ENDPOINT=127.0.0.1:6379 VALKEYDOTNET_RUN_SUBSCRIBER_RECOVERY_TESTS=1 \
  dotnet run --project tests/ValkeyDotNet.IntegrationTests -- \
  -method '*SubscriberRestoresStreamsAfterItsOwnLiveConnectionIsKilled'
```

See [subscriber reference](../reference/subscriber.md) for lifecycle, limits, and failure semantics.
