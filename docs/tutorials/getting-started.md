# Getting started

In this tutorial you will connect to a Valkey server, store and read a value, run a counter, and read
a raw reply. It takes about ten minutes and ends with a program you can run.

You need the .NET 10 SDK and Docker.

## 1. Start a Valkey server

```bash
docker run --rm -d --name valkey-tutorial -p 6379:6379 valkey/valkey:8
```

The server now listens on `127.0.0.1:6379`. You will remove it at the end.

## 2. Create a console project

```bash
dotnet new console -o ValkeyTutorial
cd ValkeyTutorial
dotnet add package ValkeyDotNet
```

## 3. Connect

Replace the contents of `Program.cs`:

```csharp
using ValkeyDotNet;

await using var valkey = await ValkeyClient.ConnectAsync(
    new ValkeyClientOptions
    {
        Host = "127.0.0.1",
        Port = 6379,
        ClientName = "tutorial",
    }
);

Console.WriteLine(await valkey.PingAsync());
```

Run it:

```bash
dotnet run
```

You will see `PONG`. The connection negotiated RESP3 and named itself `tutorial`, both in the single
`HELLO` handshake that `ConnectAsync` performs.

`await using` matters. The client owns a socket, and disposing it closes that socket.

## 4. Store and read a value

Add this below the `PingAsync` line:

```csharp
await valkey.SetStringAsync("greeting", "hello", TimeSpan.FromMinutes(5));
Console.WriteLine(await valkey.GetStringAsync("greeting"));
```

Run again. You will see `PONG` then `hello`. The value expires in five minutes because you passed an
expiry.

## 5. Count something

```csharp
await valkey.IncrementAsync("visits");
await valkey.IncrementAsync("visits", 10);
Console.WriteLine(await valkey.IncrementAsync("visits", 0));
```

Run again: `11`. `IncrementAsync` returns the value after the increment, so incrementing by `0` reads
the counter.

## 6. Read a raw reply

The convenience methods cover common cases. Every other Valkey command goes through `ExecuteAsync`,
which returns the reply exactly as the server sent it:

```csharp
await valkey.HashSetAsync("user:42", "name", "Ada"u8.ToArray());
await valkey.HashSetAsync("user:42", "role", "engineer"u8.ToArray());

var hash = await valkey.ExecuteAsync(new ValkeyCommand("HGETALL", "user:42"));
foreach (var pair in hash.AsMap())
    Console.WriteLine($"{pair.Key.AsString()} = {pair.Value.AsString()}");
```

Run again. The last two lines are:

```text
name = Ada
role = engineer
```

`AsMap()` works because you are on RESP3, where `HGETALL` returns a real map. On RESP2 the same
command returns a flat array — see [RESP values](../reference/resp-values.md).

## 7. Clean up

```csharp
await valkey.DeleteAsync(["greeting", "visits", "user:42"]);
```

Then stop the server:

```bash
docker stop valkey-tutorial
```

## What you did

You connected with an explicit handshake, used typed convenience methods, sent a raw command, and
read a lossless reply. That is the whole shape of the library: a small typed surface over a generic
command API.

Next:

- [Pipeline commands](../how-to/pipeline-commands.md) when you need throughput.
- [Connect over TLS](../how-to/connect-over-tls.md) before you touch a real server.
- [Connection model](../explanation/connection-model.md) to understand what one client can and
  cannot do.
