import * as Schema from "effect/Schema";

/** 16-byte trace id encoded as 32 lowercase hex chars, excluding the all-zero sentinel. */
export const EDATraceId = Schema.String.check(Schema.isPattern(/^(?!0{32}$)[0-9a-f]{32}$/)).pipe(
  Schema.brand("EDATraceId"),
);
export type EDATraceId = typeof EDATraceId.Type;

/** 8-byte span id encoded as 16 lowercase hex chars, excluding the all-zero sentinel. */
export const EDASpanId = Schema.String.check(Schema.isPattern(/^(?!0{16}$)[0-9a-f]{16}$/)).pipe(
  Schema.brand("EDASpanId"),
);
export type EDASpanId = typeof EDASpanId.Type;

/** Primitive telemetry attribute values accepted by durable trace link metadata. */
export const EDATraceAttributeValue = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);
export type EDATraceAttributeValue = typeof EDATraceAttributeValue.Type;

/** Required attribute bag for trace links and boundary-provided span annotations. */
export const EDATraceAttributes = Schema.Record(Schema.String, EDATraceAttributeValue);
export type EDATraceAttributes = typeof EDATraceAttributes.Type;

/** W3C-compatible trace identity carried across EDA boundaries. */
export const EDATraceContext = Schema.Struct({
  traceId: EDATraceId,
  spanId: EDASpanId,
  sampled: Schema.Boolean,
  tracestate: Schema.NullOr(Schema.String),
});
export type EDATraceContext = typeof EDATraceContext.Type;

/** Causal link to another span context. */
export const EDATraceLink = Schema.Struct({
  context: EDATraceContext,
  attributes: EDATraceAttributes,
});
export type EDATraceLink = typeof EDATraceLink.Type;

/** Required trace metadata stamped on every EDA durable or ephemeral event. */
export const EDAEventTrace = Schema.Struct({
  span: EDATraceContext,
  links: Schema.Array(EDATraceLink),
});
export type EDAEventTrace = typeof EDAEventTrace.Type;

/** Required durable trace identity for one logical EDA run. */
export const EDARunTrace = Schema.Struct({
  root: EDATraceContext,
  links: Schema.Array(EDATraceLink),
});
export type EDARunTrace = typeof EDARunTrace.Type;

/** Required trace metadata accepted at EDA host/RPC boundaries. */
export const EDATraceMetadata = Schema.Struct({
  parent: Schema.NullOr(EDATraceContext),
  links: Schema.Array(EDATraceLink),
  attributes: EDATraceAttributes,
});
export type EDATraceMetadata = typeof EDATraceMetadata.Type;

const randomHex = (bytes: number): string => {
  const values = new Uint8Array(bytes);
  crypto.getRandomValues(values);
  return Array.from(values)
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
};

export const makeEDATraceContext = (input?: {
  readonly sampled?: boolean;
  readonly spanId?: string;
  readonly traceId?: string;
  readonly tracestate?: string | null;
}): EDATraceContext =>
  EDATraceContext.make({
    traceId: EDATraceId.make(input?.traceId ?? randomHex(16)),
    spanId: EDASpanId.make(input?.spanId ?? randomHex(8)),
    sampled: input?.sampled ?? true,
    tracestate: input?.tracestate ?? null,
  });

export const makeRootEDATraceMetadata = (): EDATraceMetadata =>
  EDATraceMetadata.make({ parent: null, links: [], attributes: {} });

export const makeEDATraceMetadataFromParent = (
  parent: EDATraceContext,
  input?: {
    readonly attributes?: EDATraceAttributes;
    readonly links?: ReadonlyArray<EDATraceLink>;
  },
): EDATraceMetadata =>
  EDATraceMetadata.make({
    parent,
    links: [...(input?.links ?? [])],
    attributes: input?.attributes ?? {},
  });

export const makeRootEDAEventTrace = (): EDAEventTrace =>
  EDAEventTrace.make({ span: makeEDATraceContext(), links: [] });

export const makeEDAEventTrace = (
  span: EDATraceContext,
  links: ReadonlyArray<EDATraceLink> = [],
): EDAEventTrace => EDAEventTrace.make({ span, links: [...links] });

export const makeEDARunTrace = (
  root: EDATraceContext = makeEDATraceContext(),
  links: ReadonlyArray<EDATraceLink> = [],
): EDARunTrace => EDARunTrace.make({ root, links: [...links] });

const TRACEPARENT_RE = /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;

export const parseEDATraceparent = (
  traceparent: string | null | undefined,
  tracestate: string | null = null,
): EDATraceContext | null => {
  const normalized = traceparent?.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  const match = TRACEPARENT_RE.exec(normalized);
  if (!match || match[1] === "ff") {
    return null;
  }
  const [, , traceId, spanId, flags] = match;
  if (traceId === undefined || spanId === undefined || flags === undefined) {
    return null;
  }
  try {
    return makeEDATraceContext({
      traceId,
      spanId,
      sampled: (Number.parseInt(flags, 16) & 1) === 1,
      tracestate,
    });
  } catch {
    return null;
  }
};

export const buildEDATraceparent = (context: EDATraceContext): string =>
  `00-${context.traceId}-${context.spanId}-${context.sampled ? "01" : "00"}`;
