# RESP values

`RespValue` is a lossless representation of a RESP2 or RESP3 reply. The client never guesses a
command-specific .NET type; it hands back what the server sent and lets the caller project it.

## `RespType`

| Member | RESP2 | RESP3 | Wire prefix |
|---|---|---|---|
| `Null` | yes (null bulk/array) | yes | `_` |
| `SimpleString` | yes | yes | `+` |
| `BlobString` | yes | yes | `$` |
| `VerbatimString` | — | yes | `=` |
| `SimpleError` | yes | yes | `-` |
| `BlobError` | — | yes | `!` |
| `Integer` | yes | yes | `:` |
| `Double` | — | yes | `,` |
| `BigNumber` | — | yes | `(` |
| `Boolean` | — | yes | `#` |
| `Array` | yes | yes | `*` |
| `Map` | — | yes | `%` |
| `Set` | — | yes | `~` |
| `Push` | — | yes | `>` |

On RESP2 a command that returns a map in RESP3 — `HGETALL`, `CONFIG GET`, `XPENDING` — returns a flat
`Array` of alternating keys and values instead. Code that calls `AsMap()` must either negotiate RESP3
or branch on `Type`.

## Properties

| Member | Type | Meaning |
|---|---|---|
| `Type` | `RespType` | The wire type. |
| `IsNull` | `bool` | `Type == RespType.Null`. |
| `VerbatimFormat` | `string?` | The three-character format hint of a verbatim string, e.g. `txt`, `mkd`. `null` otherwise. |
| `Attributes` | `IReadOnlyList<KeyValuePair<RespValue, RespValue>>` | RESP3 attribute metadata attached to the reply. Empty when absent. |

## Accessors

Each accessor throws `InvalidOperationException` when `Type` does not match. The message names the
actual type, so a mismatch is diagnosable without a debugger.

| Method | Valid for | Returns |
|---|---|---|
| `AsBytes()` | any string or error type | `ReadOnlyMemory<byte>` — the raw payload |
| `AsString()` | any string or error type, or `Null` | `string?` — UTF-8 decoded, `null` for `Null` |
| `AsInt64()` | `Integer` | `long` |
| `AsDouble()` | `Double` | `double` |
| `AsBoolean()` | `Boolean` | `bool` |
| `AsBigInteger()` | `BigNumber` | `BigInteger` |
| `AsArray()` | `Array`, `Set`, `Push` | `IReadOnlyList<RespValue>` |
| `AsMap()` | `Map` | `IReadOnlyList<KeyValuePair<RespValue, RespValue>>` |

`AsBytes()` is the only lossless accessor for values that are not valid UTF-8. Use it for arbitrary
binary payloads; `AsString()` will replace invalid sequences.

## Errors

| Method | Behaviour |
|---|---|
| `ThrowIfError()` | Throws `ValkeyServerException` when `Type` is `SimpleError` or `BlobError`; otherwise returns. |
| `ToServerException()` | Builds the `ValkeyServerException` without throwing. Throws `InvalidOperationException` when the value is not an error. |

`ThrowIfError()` is the intended call on every element of an `ExecutePipelineAsync` result, which
returns errors in place instead of throwing.

## `ToString()`

Produces a short, bounded diagnostic form for logs and debugging, never for parsing:

| Value | `ToString()` |
|---|---|
| Null | `(null)` |
| Integer, double, boolean | `42`, `1.5`, `true` |
| Array, set, push | `Array[3]`, `Set[2]`, `Push[2]` |
| Map | `Map[2]` |
| Any string or error | `BlobString(11) "hello world"` |

String and error payloads report their **type and byte length** followed by a preview of at most 48
characters. The preview is escaped to printable ASCII — `\r`, `\n`, `\t`, `\"`, `\\`, and `\uXXXX`
for everything else — and a trailing `…` marks truncation. So a large value cannot flood a log line,
and a payload cannot inject newlines or terminal control sequences into one.

`ToString()` is still derived from server data. It is bounded and escaped, not redacted: do not log
a reply that carries a secret just because the form is short.

## `ValkeyCommand` and `ValkeyArgument`

`ValkeyCommand(string name, params ValkeyArgument[] arguments)` upper-cases the name and requires it
to be printable ASCII — every character in `!`–`~`. Whitespace, control characters, and non-ASCII are
rejected with `ArgumentException` rather than encoded as `?`, which would send a command the caller
never wrote. Module names such as `JSON.SET` and `FT.SEARCH` are fine.

`Arguments` is a genuinely read-only view: the array passed to the constructor is copied, and the
copy is not handed out, so a caller cannot cast the list back and rewrite a validated command.

`ValkeyArgument` is a `readonly struct` wrapping `ReadOnlyMemory<byte>`, with implicit conversions
from `string` (UTF-8), `byte[]`, `ReadOnlyMemory<byte>`, `int`, `long`, and `double` (round-trip
`"R"` format, invariant culture). The `string` and `byte[]` conversions throw `ArgumentNullException`
on null rather than encoding an empty argument, because a silently empty argument would send a
different command than the caller wrote.
