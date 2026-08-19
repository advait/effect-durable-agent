type CanonicalSpanName = `${string}.${string}`;

const defineSpanNames = <const Names extends ReadonlyArray<CanonicalSpanName>>(
  names: Names,
): Names => names;

/**
 * Allowlist of meaningful spans owned by the reusable durable-agent runtime.
 *
 * Add a name only when the span:
 * - represents a recognizable agent operation in the end-to-end execution flow;
 * - owns non-trivial work whose duration, failure, or interruption is useful to diagnose; and
 * - has a lifetime that matches that work rather than acting as an instantaneous marker.
 *
 * Names are stable, lowercase, dot-separated semantic paths. Start with the owning subsystem and
 * move from general to specific, ending with the operation, for example `agent.command.submit`.
 * Describe domain work, not TypeScript symbols, class names, EDA prefixes, or helper mechanics.
 * Keep names low-cardinality: IDs, tool or sink names, providers, models, attempts, and outcomes are
 * span attributes, never name segments.
 *
 * Prefer `Effect.fn("...")` when a function owns the operation and `Effect.withSpan("...")` around
 * an existing effect boundary. Do not add spans for pure transforms, bookkeeping, condition checks,
 * retry-delay calculations, or other near-zero-width implementation steps. Use unnamed `Effect.fn`
 * when a function benefits from Effect structure but would not add a useful node to the trace.
 *
 * Application-specific operations belong in the application catalog, not this reusable catalog.
 */
export const SpanNames = defineSpanNames([
  "agent.command.submit",
  "agent.command.submit.wait",
  "agent.command.wait",
  "agent.compaction",
  "agent.events.follow",
  "agent.events.slice",
  "agent.events.stream",
  "agent.inference",
  "agent.messages.list",
  "agent.run",
  "agent.session.snapshot",
  "agent.sink.drain",
  "agent.tool",
  "agent.turn",
]);

/** Union of values admitted by the centralized EDA span-name catalog. */
export type SpanName = (typeof SpanNames)[number];
