import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";

import type { UsagePayload } from "../types/events";
import {
  EDATraceContext,
  type EDAEventTrace,
  type EDATraceAttributeValue,
  type EDATraceMetadata,
  makeEDAEventTrace,
  makeEDATraceContext,
  makeRootEDAEventTrace,
} from "../types/tracing";

export {
  EDATraceContext,
  EDARunTrace,
  EDATraceLink,
  EDATraceMetadata,
  EDAEventTrace,
  makeEDAEventTrace,
  makeEDARunTrace,
  makeEDATraceContext,
  makeEDATraceMetadataFromParent,
  makeRootEDAEventTrace,
  makeRootEDATraceMetadata,
} from "../types/tracing";

export { buildEDATraceparent, parseEDATraceparent } from "../types/tracing";

/** Attribute value types accepted by Effect span annotations after compaction. */
export type EdaSpanAttributeValue = EDATraceAttributeValue | undefined;

/** EDA span attribute map; undefined values are intentionally dropped before annotation. */
export type EdaSpanAttributes = Record<string, EdaSpanAttributeValue>;

export interface EDATracingOptions {
  readonly disableTracing?: boolean;
  readonly disableSpanPropagation?: boolean;
  readonly spanAttributes?: EdaSpanAttributes;
  readonly spanPrefix?: string;
}

export interface EDAExportedSpanLink {
  readonly attributes: Record<string, string | number | boolean>;
  readonly sampled: boolean;
  readonly spanId: string;
  readonly traceId: string;
}

export interface EDAExportedSpan {
  readonly attributes: Record<string, string | number | boolean>;
  readonly endedAtMs: number;
  readonly endedAtUnixNano: string;
  readonly kind: Tracer.SpanKind;
  readonly links: ReadonlyArray<EDAExportedSpanLink>;
  readonly name: string;
  readonly outcome: "ok" | "error" | "timeout" | "canceled";
  readonly parentSpanId?: string;
  readonly sampled: boolean;
  readonly spanId: string;
  readonly startedAtMs: number;
  readonly startedAtUnixNano: string;
  readonly statusCode: "OK" | "ERROR";
  readonly statusMessage?: string;
  readonly traceId: string;
}

interface EventLike {
  readonly event?: { readonly type?: unknown; readonly durability?: unknown };
  readonly type?: unknown;
}

interface PositionedEventLike {
  readonly position?: { readonly seq?: unknown; readonly subSeq?: unknown };
  readonly event?: { readonly type?: unknown; readonly durability?: unknown };
}

/** Compact undefined tracing attributes before handing them to Effect spans. */
export const compactSpanAttributes = (
  attributes: EdaSpanAttributes,
): Record<string, string | number | boolean> => {
  const compacted: Record<string, string | number | boolean> = {};
  for (const [key, value] of Object.entries(attributes)) {
    if (value !== undefined) {
      compacted[key] = value;
    }
  }
  return compacted;
};

const compactUnknownAttributes = (
  attributes: Readonly<Record<string, unknown>> | ReadonlyMap<string, unknown>,
): Record<string, string | number | boolean> => {
  const entries = attributes instanceof Map ? attributes.entries() : Object.entries(attributes);
  const compacted: Record<string, string | number | boolean> = {};
  for (const [key, value] of entries) {
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      compacted[key] = value;
    }
  }
  return compacted;
};

export const toEdaExternalSpan = (context: EDATraceContext): Tracer.ExternalSpan =>
  Tracer.externalSpan({
    traceId: context.traceId,
    spanId: context.spanId,
    sampled: context.sampled,
  });

export const edaTraceContextFromSpan = (span: Tracer.AnySpan): EDATraceContext =>
  makeEDATraceContext({
    traceId: span.traceId,
    spanId: span.spanId,
    sampled: span.sampled,
    tracestate: null,
  });

export const currentEDATraceContext: Effect.Effect<EDATraceContext, unknown> =
  Effect.currentSpan.pipe(Effect.map(edaTraceContextFromSpan));

export const currentOrRootEDAEventTrace: Effect.Effect<EDAEventTrace> = Effect.currentSpan.pipe(
  Effect.map((span) => makeEDAEventTrace(edaTraceContextFromSpan(span))),
  Effect.catch(() => Effect.sync(makeRootEDAEventTrace)),
);

export const withEdaTraceMetadata =
  (metadata: EDATraceMetadata) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    effect.pipe(Effect.annotateSpans(metadata.attributes));

/** Add EDA attributes to the currently active span. */
export const annotateEdaSpan = (attributes: EdaSpanAttributes): Effect.Effect<void> =>
  Effect.annotateCurrentSpan(compactSpanAttributes(attributes));

const nanosToMillis = (nanos: bigint): number => Number(nanos / 1_000_000n);

const outcomeFromExit = (exit: Exit.Exit<unknown, unknown>): EDAExportedSpan["outcome"] => {
  if (Exit.isSuccess(exit)) {
    return "ok";
  }
  return Cause.hasInterruptsOnly(exit.cause) ? "canceled" : "error";
};

const statusMessageFromExit = (exit: Exit.Exit<unknown, unknown>): string | undefined => {
  if (Exit.isSuccess(exit)) {
    return undefined;
  }
  const error = Cause.squash(exit.cause);
  return error instanceof Error ? error.message : String(error);
};

class ExportingNativeSpan extends Tracer.NativeSpan {
  readonly #emit: (span: EDAExportedSpan) => void;

  constructor(
    options: ConstructorParameters<typeof Tracer.NativeSpan>[0],
    emit: (span: EDAExportedSpan) => void,
  ) {
    super(options);
    this.#emit = emit;
  }

  override end(endTime: bigint, exit: Exit.Exit<unknown, unknown>): void {
    super.end(endTime, exit);
    // Workers can report the same clock tick for very fast operations. Such spans
    // add noise without describing execution, so keep them out of exported traces.
    if (endTime <= this.startTime) {
      return;
    }
    const parent = Option.getOrUndefined(this.parent);
    this.#emit({
      attributes: compactUnknownAttributes(this.attributes),
      endedAtMs: nanosToMillis(endTime),
      endedAtUnixNano: endTime.toString(),
      kind: this.kind,
      links: this.links.map((link) => ({
        attributes: compactUnknownAttributes(link.attributes),
        traceId: link.span.traceId,
        spanId: link.span.spanId,
        sampled: link.span.sampled,
      })),
      name: this.name,
      outcome: outcomeFromExit(exit),
      ...(parent === undefined ? {} : { parentSpanId: parent.spanId }),
      sampled: this.sampled,
      spanId: this.spanId,
      startedAtMs: nanosToMillis(this.startTime),
      startedAtUnixNano: this.startTime.toString(),
      statusCode: Exit.isSuccess(exit) ? "OK" : "ERROR",
      ...(statusMessageFromExit(exit) === undefined
        ? {}
        : { statusMessage: statusMessageFromExit(exit) }),
      traceId: this.traceId,
    });
  }
}

/** Create an Effect tracer that exports only spans with observable duration. */
export const makeEdaExportingTracer = (emit: (span: EDAExportedSpan) => void): Tracer.Tracer =>
  Tracer.make({
    span: (options) => new ExportingNativeSpan(options, emit),
  });

/** Summarize a durable/logical event collection for span attributes. */
export const eventBatchAttributes = (events: ReadonlyArray<EventLike>): EdaSpanAttributes => ({
  "eda.event.count": events.length,
  "eda.event.types": eventTypeSummary(events),
});

/** Summarize a committed positioned event collection for span attributes. */
export const committedBatchAttributes = (
  events: ReadonlyArray<PositionedEventLike>,
): EdaSpanAttributes => {
  const seqs = events
    .map((entry) => entry.position?.seq)
    .filter((seq): seq is number => typeof seq === "number");
  return {
    ...eventBatchAttributes(events),
    "eda.seq.min": seqs.length === 0 ? undefined : Math.min(...seqs),
    "eda.seq.max": seqs.length === 0 ? undefined : Math.max(...seqs),
  };
};

/** Stable, low-cardinality usage attributes for model lifecycle spans. */
export const usageAttributes = (usage: UsagePayload | undefined): EdaSpanAttributes => ({
  "eda.usage.input_tokens": usage?.inputTokens,
  "eda.usage.cached_input_tokens": usage?.cachedInputTokens,
  "eda.usage.output_tokens": usage?.outputTokens,
  "eda.usage.text_tokens": usage?.textTokens,
  "eda.usage.reasoning_tokens": usage?.reasoningTokens,
});

const eventTypeSummary = (events: ReadonlyArray<EventLike>): string | undefined => {
  if (events.length === 0) {
    return undefined;
  }
  const seen = new Set<string>();
  for (const event of events) {
    const type = event.event?.type ?? event.type;
    if (typeof type === "string") {
      seen.add(type);
    }
  }
  return seen.size === 0 ? undefined : Array.from(seen).join(",");
};
