import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import type { EDASessionStoreError } from "../services/session-store";
import {
  TranscriptCursor,
  TranscriptLogIncarnation,
  TranscriptOrder,
  TranscriptRecordViewFromEnvelope,
  TranscriptVersion,
  type TranscriptOrderKey,
  type TranscriptRecordView,
} from "./records";
import type { EDATranscriptStoreShape, TranscriptCursorMismatch } from "./store";
import {
  TranscriptActiveContent,
  TranscriptReadiness,
  transcriptCursorOf,
  type TranscriptWindow,
  type TranscriptWindowLimits,
  type TranscriptWindowSelector,
} from "./window";

const PositiveInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1));

/** Client-chosen correlation id echoed on the update that answers a request. */
export const TranscriptRequestId = Schema.NonEmptyString.check(Schema.isMaxLength(64));
export type TranscriptRequestId = typeof TranscriptRequestId.Type;

/**
 * The contiguous span of top-level records a subscriber holds, both ends inclusive. Absent when
 * the subscriber holds no top-level records (an empty session, or a window still building).
 */
export const TranscriptCoverage = Schema.Struct({
  from: TranscriptCursor,
  to: TranscriptCursor,
});
export type TranscriptCoverage = typeof TranscriptCoverage.Type;

/**
 * What a subscriber holds after its last complete update: the cut it was read at and the span it
 * covers. Every update is planned relative to this base, which is also persisted with the socket
 * so a hibernated subscription resumes without volatile state.
 *
 * `tail` is true when the coverage reaches the newest record of its cut; new history then joins
 * the coverage as it is committed. A subscriber that paged back past the tail sees `hasNewer`
 * instead and pages forward to rejoin it.
 */
export const TranscriptSubscriptionBase = Schema.Struct({
  incarnation: TranscriptLogIncarnation,
  version: TranscriptVersion,
  throughSeq: SequenceNumber,
  coverage: Schema.optionalKey(TranscriptCoverage),
  tail: Schema.Boolean,
});
export type TranscriptSubscriptionBase = typeof TranscriptSubscriptionBase.Type;

/**
 * One ordered, complete change to a subscriber's window: the only way a live client's window
 * changes.
 *
 * - `replace` discards everything the client holds and installs this window.
 * - `patch` is relative to the client's previous complete update: `records` and `dependencies`
 *   carry every record that changed or newly entered the window, `dependencyIds` lists the
 *   complete dependency membership of the new cut (the client drops any other dependency), and
 *   the client drops top-level records outside `coverage`.
 *
 * Either way the client ends at exactly the window read at `throughSeq`: a complete cut. When
 * present, `activeContent` replaces the client's active content completely (an empty array
 * clears it); when absent, active content is unchanged. A degraded update carries no live
 * content, and a client shows none while readiness is not `Ready`.
 */
export const TranscriptUpdate = Schema.Struct({
  updateSeq: PositiveInt,
  mode: Schema.Literals(["replace", "patch"]),
  incarnation: TranscriptLogIncarnation,
  version: TranscriptVersion,
  throughSeq: SequenceNumber,
  coverage: Schema.optionalKey(TranscriptCoverage),
  hasOlder: Schema.Boolean,
  hasNewer: Schema.Boolean,
  readiness: TranscriptReadiness,
  records: Schema.Array(TranscriptRecordViewFromEnvelope),
  dependencies: Schema.Array(TranscriptRecordViewFromEnvelope),
  dependencyIds: Schema.Array(Schema.NonEmptyString),
  activeContent: Schema.optionalKey(Schema.Array(TranscriptActiveContent)),
  requestId: Schema.optionalKey(TranscriptRequestId),
}).check(
  Schema.makeFilter(
    (update) =>
      [...update.records, ...update.dependencies].every(
        (record) => record.updatedSeq <= update.throughSeq,
      ) || "every record must be projected through throughSeq and none may be newer",
  ),
  Schema.makeFilter(
    (update) =>
      update.readiness._tag === "Ready" ||
      update.activeContent === undefined ||
      update.activeContent.length === 0 ||
      "a degraded update never carries live content",
  ),
  Schema.makeFilter((update) => {
    const dependencyIds = new Set(update.dependencyIds);
    return (
      update.dependencies.every((record) => dependencyIds.has(record.id)) ||
      "every sent dependency is a member of the cut"
    );
  }),
);
export type TranscriptUpdate = typeof TranscriptUpdate.Type;

/** Bounds of one subscription. Retained coverage never exceeds `limits.maxRecords`. */
export interface TranscriptSubscriptionPolicy {
  /** Top-level records of a fresh subscription (design decision 4). */
  readonly initialCount: number;
  /** Largest page a client may request at once. */
  readonly maxPageCount: number;
  readonly limits: TranscriptWindowLimits;
}

export const defaultTranscriptSubscriptionPolicy: TranscriptSubscriptionPolicy = {
  initialCount: 100,
  maxPageCount: 100,
  limits: { maxRecords: 300, maxBytes: 2 * 1_048_576, maxInlineBodyBytes: 16_384 },
};

/** A subscriber's request, decoded from its socket and handled strictly in arrival order. */
export type TranscriptSubscriptionRequest =
  /** Re-read the held coverage at a new cut, after committed changes. */
  | { readonly _tag: "Refresh" }
  /**
   * Start (or restart) a subscription. With `range` and `since` (what SSR rendered, or what a
   * reconnecting client last applied), the first update is a patch over that range; otherwise
   * it is a replacement of the latest `count` records.
   */
  | {
      readonly _tag: "Subscribe";
      readonly range?: TranscriptCoverage;
      readonly tail: boolean;
      readonly since?: SequenceNumber;
      readonly count?: number;
    }
  /** Extend the coverage by `count` records from its `before` or `after` edge. */
  | {
      readonly _tag: "Page";
      readonly direction: "before" | "after";
      readonly cursor: TranscriptCursor;
      readonly count: number;
    }
  /** Narrow the coverage to a span inside it, releasing the rest. */
  | { readonly _tag: "Retain"; readonly from: TranscriptCursor; readonly to: TranscriptCursor };

/** One window read at a single cut, and the base the update for it is relative to. */
export interface TranscriptSubscriptionRead {
  readonly window: TranscriptWindow;
  /** What the client holds; `undefined` makes the update a replacement. */
  readonly patchBase: TranscriptSubscriptionBase | undefined;
}

const orderKey = (cursor: TranscriptCursor): TranscriptOrderKey => ({
  anchorSeq: cursor.anchorSeq,
  ordinal: cursor.ordinal,
  recordId: cursor.recordId,
});

const sameCursor = (left: TranscriptCursor, right: TranscriptCursor): boolean =>
  left.incarnation === right.incarnation &&
  left.version === right.version &&
  TranscriptOrder(orderKey(left), orderKey(right)) === 0;

/**
 * A cursor after every possible record of one version, so a `Range` ending at it reads from a
 * point to the newest record. Never sent to clients.
 */
const tailCursor = (from: TranscriptCursor): TranscriptCursor => ({
  incarnation: from.incarnation,
  version: from.version,
  anchorSeq: SequenceNumber.make(Number.MAX_SAFE_INTEGER),
  ordinal: Number.MAX_SAFE_INTEGER,
  recordId: "￿",
});

/** The span a window's top-level records cover, or `undefined` when it has none. */
export const transcriptCoverageOf = (window: TranscriptWindow): TranscriptCoverage | undefined => {
  const first = window.records[0];
  const last = window.records[window.records.length - 1];
  const from = first === undefined ? undefined : transcriptCursorOf(window, first);
  const to = last === undefined ? undefined : transcriptCursorOf(window, last);
  return from === undefined || to === undefined ? undefined : { from, to };
};

const withinCoverage = (
  coverage: TranscriptCoverage | undefined,
  record: TranscriptRecordView,
): boolean => {
  if (coverage === undefined || !("position" in record) || record.position === undefined) {
    return false;
  }
  const key = { ...record.position, recordId: record.id };
  return (
    TranscriptOrder(orderKey(coverage.from), key) <= 0 &&
    TranscriptOrder(key, orderKey(coverage.to)) <= 0
  );
};

/** The base a client holds after applying an update for `window`. */
export const transcriptSubscriptionBaseOf = (
  window: TranscriptWindow,
): TranscriptSubscriptionBase => {
  const coverage = transcriptCoverageOf(window);
  return {
    incarnation: window.incarnation,
    version: window.version,
    throughSeq: window.throughSeq,
    ...(coverage === undefined ? {} : { coverage }),
    tail: !window.hasNewer,
  };
};

/**
 * Plan the one update that moves a client from `patchBase` to `window`.
 *
 * A patch sends a top-level record when it changed after the base cut or was outside the base
 * coverage. It sends a tool dependency when the tool changed or its assistant is sent, and any
 * other dependency (run groups, session context, pending users) when it changed or any top-level
 * record is sent. Everything else the client already holds unchanged from the base cut, because
 * the base cut was itself complete. A different incarnation or version, or a base cut ahead of
 * the window, makes the update a replacement.
 */
export const planTranscriptUpdate = (input: {
  readonly patchBase: TranscriptSubscriptionBase | undefined;
  readonly window: TranscriptWindow;
  readonly updateSeq: number;
  /** The live content captured at the same publication boundary as `window`. */
  readonly activeContent: ReadonlyArray<TranscriptActiveContent>;
  /**
   * Whether the client's active content must be replaced. A replacement always carries it;
   * otherwise the caller sends it only when it changed since its last send.
   */
  readonly includeActiveContent: boolean;
  readonly requestId?: TranscriptRequestId;
}): TranscriptUpdate => {
  const { window } = input;
  const base = input.patchBase;
  const patch =
    base !== undefined &&
    base.incarnation === window.incarnation &&
    base.version === window.version &&
    base.throughSeq <= window.throughSeq;
  const records = patch
    ? window.records.filter(
        (record) => record.updatedSeq > base.throughSeq || !withinCoverage(base.coverage, record),
      )
    : window.records;
  const sentIds = new Set(records.map((record) => record.id));
  const dependencies = patch
    ? window.dependencies.filter(
        (record) =>
          record.updatedSeq > base.throughSeq ||
          (record.kind === "tool" ? sentIds.has(record.assistantId) : records.length > 0),
      )
    : window.dependencies;
  const coverage = transcriptCoverageOf(window);
  const activeContent =
    patch && !input.includeActiveContent
      ? undefined
      : servedActiveContent(window, input.activeContent);
  return {
    updateSeq: input.updateSeq,
    mode: patch ? "patch" : "replace",
    incarnation: window.incarnation,
    version: window.version,
    throughSeq: window.throughSeq,
    ...(coverage === undefined ? {} : { coverage }),
    hasOlder: window.hasOlder,
    hasNewer: window.hasNewer,
    readiness: window.readiness,
    records,
    dependencies,
    dependencyIds: window.dependencies.map((record) => record.id),
    ...(activeContent === undefined ? {} : { activeContent }),
    ...(input.requestId === undefined ? {} : { requestId: input.requestId }),
  };
};

/**
 * Live content as served with a window: none on a degraded read, and never for an inference the
 * window already shows as committed or failed, so a stale live part cannot override a terminal.
 */
const servedActiveContent = (
  window: TranscriptWindow,
  activeContent: ReadonlyArray<TranscriptActiveContent>,
): ReadonlyArray<TranscriptActiveContent> => {
  if (window.readiness._tag !== "Ready") return [];
  const settled = new Set(
    window.records.flatMap((record) =>
      record.kind === "assistant" && record.lifecycle._tag !== "Streaming" ? [record.id] : [],
    ),
  );
  return activeContent.filter((content) => !settled.has(content.assistantId));
};

/**
 * Read the window that answers one request, at one cut.
 *
 * Helper reads (to find a page's far edge) may see other cuts; only the final read is served,
 * so the update is always one synchronous read. Coverage stays within `limits.maxRecords`:
 * paging back evicts the newest records, paging forward evicts the oldest, and a tail that
 * outgrows the bound keeps its newest records. A cursor minted for another incarnation or version
 * falls back to a replacement of the latest records.
 */
export const readTranscriptSubscription = (
  store: EDATranscriptStoreShape,
  base: TranscriptSubscriptionBase | undefined,
  request: TranscriptSubscriptionRequest,
  policy: TranscriptSubscriptionPolicy,
): Effect.Effect<TranscriptSubscriptionRead, EDASessionStoreError> => {
  const read = (selector: TranscriptWindowSelector) =>
    store.readTranscriptWindow(selector, policy.limits);
  const latest = read({
    _tag: "Latest",
    count: Math.min(policy.initialCount, policy.limits.maxRecords),
  }).pipe(Effect.map((window) => ({ window, patchBase: undefined })));

  const readCoverage = Effect.fnUntraced(function* (
    coverage: TranscriptCoverage | undefined,
    tail: boolean,
    overflowToNewest: boolean,
  ) {
    if (coverage === undefined) {
      return yield* read({
        _tag: "Latest",
        count: Math.min(policy.initialCount, policy.limits.maxRecords),
      });
    }
    if (!tail) return yield* read({ _tag: "Range", ...coverage });
    const window = yield* read({
      _tag: "Range",
      from: coverage.from,
      to: tailCursor(coverage.from),
    });
    // The span outgrew the bound: keep as many of its newest records as the Range read held.
    return window.hasNewer && overflowToNewest
      ? yield* read({ _tag: "Latest", count: Math.max(1, window.records.length) })
      : window;
  });

  const handle = Effect.fnUntraced(function* () {
    switch (request._tag) {
      case "Subscribe": {
        const count = Math.min(request.count ?? policy.initialCount, policy.limits.maxRecords);
        if (request.range === undefined) {
          const window = yield* read({ _tag: "Latest", count });
          return { window, patchBase: undefined };
        }
        const claimed: TranscriptSubscriptionBase = {
          incarnation: request.range.from.incarnation,
          version: request.range.from.version,
          throughSeq: request.since ?? SequenceNumber.make(0),
          coverage: request.range,
          tail: request.tail,
        };
        const window = yield* readCoverage(request.range, request.tail, true);
        return { window, patchBase: request.since === undefined ? undefined : claimed };
      }
      case "Refresh": {
        if (base === undefined) return yield* latest;
        const window = yield* readCoverage(base.coverage, base.tail, true);
        return { window, patchBase: base };
      }
      case "Page": {
        const coverage = base?.coverage;
        const edge = request.direction === "before" ? coverage?.from : coverage?.to;
        if (base === undefined || coverage === undefined || edge === undefined) {
          return base === undefined
            ? yield* latest
            : { window: yield* readCoverage(base.coverage, base.tail, true), patchBase: base };
        }
        if (!sameCursor(edge, request.cursor)) {
          return { window: yield* readCoverage(coverage, base.tail, true), patchBase: base };
        }
        const page = yield* read({
          _tag: "Page",
          direction: request.direction,
          cursor: request.cursor,
          count: Math.min(request.count, policy.maxPageCount, policy.limits.maxRecords),
        });
        const first = page.records[0];
        const last = page.records[page.records.length - 1];
        const pageFrom = first === undefined ? undefined : transcriptCursorOf(page, first);
        const pageTo = last === undefined ? undefined : transcriptCursorOf(page, last);
        if (pageFrom === undefined || pageTo === undefined) {
          return { window: yield* readCoverage(coverage, base.tail, true), patchBase: base };
        }
        if (request.direction === "before") {
          // A Range keeps its oldest records when it is over a bound, evicting the newest.
          const window = yield* readCoverage({ from: pageFrom, to: coverage.to }, base.tail, false);
          return { window, patchBase: base };
        }
        const from = yield* evictOldest(read, coverage.from, pageTo, policy.limits.maxRecords);
        const window = yield* readCoverage({ from, to: pageTo }, !page.hasNewer, false);
        return { window, patchBase: base };
      }
      case "Retain": {
        const coverage = base?.coverage;
        if (base === undefined || coverage === undefined) {
          return base === undefined
            ? yield* latest
            : { window: yield* readCoverage(base.coverage, base.tail, true), patchBase: base };
        }
        const inside =
          request.from.incarnation === base.incarnation &&
          request.from.version === base.version &&
          request.to.incarnation === base.incarnation &&
          request.to.version === base.version &&
          TranscriptOrder(orderKey(coverage.from), orderKey(request.from)) <= 0 &&
          TranscriptOrder(orderKey(request.from), orderKey(request.to)) <= 0 &&
          TranscriptOrder(orderKey(request.to), orderKey(coverage.to)) <= 0;
        const retained = inside ? { from: request.from, to: request.to } : coverage;
        const tail = base.tail && sameCursor(retained.to, coverage.to);
        return { window: yield* readCoverage(retained, tail, true), patchBase: base };
      }
    }
  });

  // A Latest read names no cursor, so it cannot mismatch.
  return handle().pipe(
    Effect.catchTag("TranscriptCursorMismatch", () =>
      latest.pipe(Effect.catchTag("TranscriptCursorMismatch", Effect.die)),
    ),
  );
};

/**
 * The oldest cursor a span ending at `to` may start from without exceeding `maxRecords`: the
 * current `from`, or later when the span would be over the bound.
 */
const evictOldest = Effect.fnUntraced(function* (
  read: (
    selector: TranscriptWindowSelector,
  ) => Effect.Effect<TranscriptWindow, EDASessionStoreError | TranscriptCursorMismatch>,
  from: TranscriptCursor,
  to: TranscriptCursor,
  maxRecords: number,
) {
  if (maxRecords <= 1) return to;
  const before = yield* read({
    _tag: "Page",
    direction: "before",
    cursor: to,
    count: maxRecords - 1,
  });
  const first = before.records[0];
  const oldest = first === undefined ? undefined : transcriptCursorOf(before, first);
  return oldest === undefined || TranscriptOrder(orderKey(oldest), orderKey(from)) <= 0
    ? from
    : oldest;
});
