# `ValkeyClientOptions`

Connection and protocol settings passed to `ValkeyClient.ConnectAsync`. All members are `init`-only.
Options are validated when the connection is opened; an invalid combination throws before any socket
is created.

## Members

| Member | Type | Default | Meaning |
|---|---|---|---|
| `Host` | `string` | `"localhost"` | Server host name or address. Must be non-empty. |
| `Port` | `int` | `6379` | TCP port. Must be 1–65535. |
| `Username` | `string?` | `null` | ACL user name. Requires `Password`. |
| `Password` | `string?` | `null` | Sent in the `HELLO ... AUTH` handshake. |
| `ClientName` | `string?` | `null` | Sets the connection name via `HELLO ... SETNAME`. |
| `Database` | `int` | `0` | Logical database. A non-zero value issues `SELECT` after the handshake. Must be ≥ 0. |
| `Protocol` | `ValkeyProtocol` | `Resp3` | Protocol requested in `HELLO`. |
| `UseTls` | `bool` | `false` | Wraps the socket in an `SslStream`. |
| `CertificateValidationCallback` | `RemoteCertificateValidationCallback?` | `null` | Replaces platform certificate validation entirely when set. |
| `ConnectTimeout` | `TimeSpan` | 5 seconds | Bounds connect, TLS handshake, and the initial `HELLO`. Must be > 0 and ≤ 4294967294 ms (~49.7 days), the longest delay a timer can schedule. |
| `MaxResponseBytes` | `int` | 67108864 (64 MiB) | Maximum bytes in a single reply frame. Must be ≥ 1024. |
| `MaxResponseElements` | `int` | 1048576 (1 Mi) | Maximum number of RESP values decoded from a single reply. Must be ≥ 16. |
| `MaxNestingDepth` | `int` | `128` | Maximum RESP nesting depth. Must be 1–1024. |

## Validation

`ConnectAsync` calls `Validate()` before connecting. It throws:

| Condition | Exception |
|---|---|
| `Host` null, empty, or whitespace | `ArgumentException` |
| `Port` outside 1–65535 | `ArgumentOutOfRangeException` |
| `Database` negative | `ArgumentOutOfRangeException` |
| `Protocol` not a defined `ValkeyProtocol` value | `ArgumentOutOfRangeException` |
| `ConnectTimeout` ≤ `TimeSpan.Zero`, or longer than a timer can schedule | `ArgumentOutOfRangeException` |
| `MaxResponseBytes` < 1024 | `ArgumentOutOfRangeException` |
| `MaxResponseElements` < 16 | `ArgumentOutOfRangeException` |
| `MaxNestingDepth` outside 1–1024 | `ArgumentOutOfRangeException` |
| `Username` set while `Password` is null | `ArgumentException` |

Validation runs before any socket, timer, or TLS state is created, so an invalid option set costs
nothing and leaks nothing.

Passing `null` options to `ConnectAsync` uses a default-constructed instance.

## Bounds

`MaxResponseBytes`, `MaxResponseElements`, and `MaxNestingDepth` are enforced by the reader on every
frame. Exceeding any of them throws `ValkeyProtocolException` and invalidates the connection. They
exist so a hostile or malfunctioning server cannot exhaust process memory; see
[Connection model](../explanation/connection-model.md).

The three bounds cover different attacks:

| Bound | Stops |
|---|---|
| `MaxResponseBytes` | A frame that is simply too large — a claimed 40 GiB bulk string. |
| `MaxResponseElements` | A frame that is small on the wire but large once decoded. Every element costs at least three bytes on the wire but far more as a decoded object, so bytes alone do not bound memory. |
| `MaxNestingDepth` | Deep nesting that would otherwise overflow the stack. |

A declared aggregate cardinality is checked against both `MaxResponseElements` and the bytes left in
the frame budget **before** anything is allocated for it, so `*2147483647\r\n` is rejected on its
header rather than acted on.

## TLS

When `UseTls` is `true` and `CertificateValidationCallback` is `null`, the platform default chain and
host-name validation applies, with `TargetHost` set to `Host`. Supplying a callback replaces that
validation completely — the platform result is not consulted. See
[Connect over TLS](../how-to/connect-over-tls.md).
