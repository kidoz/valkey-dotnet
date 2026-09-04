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

On connection failure, inspect `subscriber.Failure` and create a new subscriber explicitly. This
version does not restore subscriptions. Messages published during downtime or dropped because a
queue is full are not recovered. Use [TLS settings](connect-over-tls.md) outside a trusted network.

See [subscriber reference](../reference/subscriber.md) for lifecycle, limits, and failure semantics.
