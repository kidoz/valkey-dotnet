# Why a managed-only client

ValkeyDotNet takes no runtime dependency at all: not on a native library, not on a Rust core, not on
a third-party NuGet package. The shipping assembly uses only the .NET base class library. This page
explains why that constraint exists and what it costs.

## The constraint is the product

Valkey's own recommended .NET client, Valkey GLIDE, is built on a shared Rust core. That is a sound
architecture for a client family spanning many languages: the protocol, routing, and reconnection
logic are written once and reused. The cost lands on consumers and contributors as native artifacts
and a heavier toolchain.

ValkeyDotNet makes the opposite trade. It is a smaller library that does less, in exchange for:

- **Deployment that is just a DLL.** No native asset to match to a runtime identifier, no
  platform-specific package to forget, nothing to go wrong in a container built for an architecture
  you did not test.
- **Trimming and AOT that behave predictably.** Managed-only code is what the .NET linker is designed
  to reason about.
- **A build that needs only the .NET SDK.** `git clone && just build` works, with no second toolchain
  to install.
- **A supply chain you can audit.** Zero runtime dependencies means zero transitive packages to
  review and no third-party update cadence to track.

If you need cluster support, connection pooling, or a mature feature set today, use GLIDE. That is
not a hedge — it is the honest recommendation for those requirements.

## Why this is enforced rather than encouraged

A dependency-free library stays dependency-free only if adding one is treated as a design change. The
first `PackageReference` is always reasonable in isolation — a logging abstraction, a pooling helper,
a JSON serializer. It is also the moment the property is gone, because a dependency brings its own
dependencies and its own update pressure.

So the rule is absolute for `src/ValkeyDotNet`: no `PackageReference` entries. Test and benchmark
projects use packages freely; they do not ship.

Where a dependency would normally be reached for, the answer is to not need it:

- Replies are exposed as `RespValue` rather than deserialized, so no JSON library is required.
- Push frames use `Action<RespValue>` rather than a reactive abstraction.
- There is no logging abstraction; failures are exceptions with structured `ErrorCode`s, and
  observability will be `System.Diagnostics` primitives when it arrives.

## What it costs

Being honest about the trade:

- **Features arrive slowly.** Everything that GLIDE inherits from a shared core has to be written and
  tested here.
- **No cross-language behavioural parity.** Bug-for-bug compatibility with other Valkey clients is
  not a goal.
- **The allocation profile needs work.** The current protocol path allocates more than it should; see
  the [performance baseline](../reference/performance-baseline.md). A native core would have made
  this someone else's problem.

The bet is that for a large class of applications — a cache, a lock, a counter, a queue — a small
auditable managed client is worth more than breadth.

## Not an official client

ValkeyDotNet is an independent open-source project. It is not affiliated with or endorsed by the
Valkey project.

## Related

- [Connection model](connection-model.md) — the design that follows from the protocol.
- [Getting started](../tutorials/getting-started.md) — the library in practice.
