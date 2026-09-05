# ValkeyDotNet documentation

These docs follow [Diátaxis](https://diataxis.fr/). Every page belongs to exactly one of four
quadrants, because each serves a different need and mixing them makes all four worse.

| Quadrant | Serves | When you are | Pages |
|---|---|---|---|
| **[Tutorials](tutorials/)** | Learning | New, and want to get something working | [Getting started](tutorials/getting-started.md) |
| **[How-to guides](how-to/)** | A goal | Competent, and have a specific task | [Use a cluster](how-to/use-cluster.md), [Connect over TLS](how-to/connect-over-tls.md), [Pipeline commands](how-to/pipeline-commands.md), [Send any command](how-to/send-any-command.md), [Handle errors](how-to/handle-errors.md), [Run live tests](how-to/run-live-integration-tests.md), [Publish a release](how-to/publish-a-release.md) |
| **[Reference](reference/)** | Information | Working, and need a precise fact | [ValkeyClusterClient](reference/valkey-cluster-client.md), [ValkeyClient](reference/valkey-client.md), [Client options](reference/client-options.md), [RESP values](reference/resp-values.md), [Exceptions](reference/exceptions.md), [Valkey compatibility](reference/valkey-compatibility.md), [Performance baseline](reference/performance-baseline.md) |
| **[Explanation](explanation/)** | Understanding | Reflecting, and want to know why | [Why managed-only](explanation/why-managed-only.md), [Connection model](explanation/connection-model.md) |

Script execution: [Execute reusable scripts](how-to/execute-scripts.md) and
[script API reference](reference/scripts.md).

Standalone recovery: [Recover a standalone connection](how-to/recover-standalone-connections.md)
and [connection owner reference](reference/connection-owner.md).

Observability: [Collect owner telemetry](how-to/collect-telemetry.md) and
[diagnostics reference](reference/diagnostics.md).

Resilience: [Run isolated restart tests](how-to/run-resilience-tests.md) and
[executed versus pending evidence](reference/resilience-evidence.md).

Pub/Sub: [Subscribe to messages](how-to/subscribe-to-messages.md) and
[dedicated subscriber reference](reference/subscriber.md).

Sharded Pub/Sub: [cluster subscriber reference](reference/cluster-subscriber.md).

Client-side invalidations: [RESP3 tracking reference](reference/client-tracking.md).

## Writing docs here

Pick the quadrant before you write, and keep the page inside it:

- A **tutorial** is a lesson. It takes a beginner through a sequence that works. Every step is
  concrete, every step succeeds, and nothing is optional. Do not explain design decisions and do not
  offer alternatives — that is what the other quadrants are for.
- A **how-to guide** solves one real problem for someone who already knows the basics. It assumes
  competence, states a goal in its title, and can omit anything the reader can be trusted to know.
  It is not a lesson and does not need to be complete.
- **Reference** describes the machinery: types, options, defaults, limits, and behaviour. It is
  austere and structured, mirrors the shape of the code, and contains no instruction or opinion.
  Examples illustrate; they do not teach.
- **Explanation** discusses *why*. Context, alternatives considered, trade-offs, history. It is the
  only quadrant where an opinion belongs, and it must not become a substitute for reference.

The most common failure is a reference page that starts teaching, or a how-to guide that drifts into
justifying a design. If a page needs a second quadrant, link to it rather than absorbing it.
