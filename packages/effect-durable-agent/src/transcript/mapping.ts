import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { effectDurableAgentNamespace, SchemaVersion } from "../types/events/envelope";
import {
  TranscriptClassification,
  TranscriptDecorationKey,
  TranscriptFieldPath,
  type TranscriptVersion,
} from "./records";
import { computeTranscriptVersion, transcriptSemanticRevision } from "./version";

/**
 * Host decorations are data, not code. A host registers mappings from its own durable app events
 * to decorations on transcript records; EDA validates them at registration and executes them
 * with fixed bounds inside the projection.
 */

/** Bounds EDA enforces on every definition and every decoration it executes. */
export const transcriptMappingBounds = {
  maxMappings: 64,
  maxOperationsPerMapping: 16,
  maxPathLength: 16,
  /** Canonical JSON bytes of one decoration value or list entry. */
  maxValueBytes: 8_192,
  /**
   * Canonical JSON bytes of all decorations of one record, and of one decoration buffer. An
   * operation that would exceed it is skipped, so no record's row can approach storage value
   * limits whatever a host appends.
   */
  maxDecorationBytes: 262_144,
  /** Entries kept in one decoration list; further distinct entries are dropped. */
  maxListEntries: 64,
  maxUserFallbacks: 16,
} as const;

const MappingPath = TranscriptFieldPath.check(
  Schema.isMaxLength(transcriptMappingBounds.maxPathLength),
);

/** The app event a mapping reads: one host namespace, type and schema version. */
export const TranscriptMappedEvent = Schema.Struct({
  namespace: Schema.NonEmptyString,
  type: Schema.NonEmptyString,
  schemaVersion: SchemaVersion,
});
export type TranscriptMappedEvent = typeof TranscriptMappedEvent.Type;

/**
 * Which record a mapping decorates.
 *
 * User targets associate through a payload field: the submitting command's id, or the
 * command's idempotency key. A decoration that arrives before its target user record is held
 * and applied, in arrival order, when the record is created. `Session` decorates the
 * session-context singleton.
 */
export const TranscriptMappingTarget = Schema.TaggedUnion({
  UserByCommandId: { path: MappingPath },
  UserByIdempotencyKey: { path: MappingPath },
  Session: {},
});
export type TranscriptMappingTarget = typeof TranscriptMappingTarget.Type;

/** How a list entry is identified when it is replaced. */
export const TranscriptListEntryKey = Schema.TaggedUnion({
  /** A string id at this payload path (for example an attachment id). */
  Id: { path: MappingPath },
  /** A non-negative integer ordinal at this payload path; the list is kept in ordinal order. */
  Ordinal: { path: MappingPath },
});
export type TranscriptListEntryKey = typeof TranscriptListEntryKey.Type;

/** One declarative decoration operation. Every `from` path reads the authenticated event payload. */
export const TranscriptMappingOperation = Schema.TaggedUnion({
  /** Copy a payload value into a decoration; the latest event wins. */
  Copy: { from: MappingPath, to: TranscriptDecorationKey },
  /** Copy a payload value into a decoration only if the decoration is still absent. */
  FirstWins: { from: MappingPath, to: TranscriptDecorationKey },
  /** Insert or replace one entry of a decoration list, keyed by id or by ordinal. */
  ReplaceListEntry: {
    from: MappingPath,
    to: TranscriptDecorationKey,
    key: TranscriptListEntryKey,
  },
  /**
   * Classify the target user record, for example as an authenticated subagent notice. A record
   * holds one classification; the last one applied wins.
   */
  Classify: { classification: TranscriptClassification },
});
export type TranscriptMappingOperation = typeof TranscriptMappingOperation.Type;

/** One host mapping: an app event, the record it decorates, and the operations to apply. */
export const TranscriptMapping = Schema.Struct({
  /** Unique name; buffered decorations refer to it. */
  name: TranscriptDecorationKey,
  event: TranscriptMappedEvent,
  target: TranscriptMappingTarget,
  operations: Schema.NonEmptyArray(TranscriptMappingOperation).check(
    Schema.isMaxLength(transcriptMappingBounds.maxOperationsPerMapping),
  ),
});
export type TranscriptMapping = typeof TranscriptMapping.Type;

/**
 * A user decoration copied from a session decoration when the user record is created without
 * it, for example "the most recent author" for a message whose command carried no author event.
 *
 * Fallbacks apply in declaration order, after any parked decorations, and each one applies only
 * if the user decoration is still absent; so `[author ← lastAuthor, author ← owner]` means "the
 * latest author, else the session owner".
 */
export const TranscriptUserFallback = Schema.Struct({
  to: TranscriptDecorationKey,
  fromSession: TranscriptDecorationKey,
});
export type TranscriptUserFallback = typeof TranscriptUserFallback.Type;

/** The host's complete declarative transcript mapping data. */
export const TranscriptMappings = Schema.Struct({
  mappings: Schema.Array(TranscriptMapping).check(
    Schema.isMaxLength(transcriptMappingBounds.maxMappings),
  ),
  userFallbacks: Schema.Array(TranscriptUserFallback).check(
    Schema.isMaxLength(transcriptMappingBounds.maxUserFallbacks),
  ),
});
export type TranscriptMappings = typeof TranscriptMappings.Type;

/** Mappings for a host that registers no decorations. */
export const emptyTranscriptMappings: TranscriptMappings = { mappings: [], userFallbacks: [] };

/** Host mapping data rejected at registration. */
export class TranscriptDefinitionError extends Schema.TaggedError<TranscriptDefinitionError>()(
  "TranscriptDefinitionError",
  { message: Schema.String },
) {}

/** How a decoration key is written; one key has exactly one shape across all mappings. */
type DecorationShape = "copy" | "first-wins" | "list:Id" | "list:Ordinal";

const validationProblems = (mappings: TranscriptMappings): ReadonlyArray<string> => {
  const problems: Array<string> = [];
  const names = new Set<string>();
  const shapes = new Map<string, DecorationShape>();
  const sessionValueKeys = new Set<string>();
  const recordShape = (scope: "user" | "session", key: string, shape: DecorationShape) => {
    const scoped = `${scope}:${key}`;
    const existing = shapes.get(scoped);
    if (existing !== undefined && existing !== shape) {
      problems.push(`${scope} decoration "${key}" is written as both ${existing} and ${shape}`);
    }
    shapes.set(scoped, shape);
  };

  for (const mapping of mappings.mappings) {
    if (names.has(mapping.name)) problems.push(`duplicate mapping name "${mapping.name}"`);
    names.add(mapping.name);
    if (
      mapping.event.namespace === effectDurableAgentNamespace ||
      mapping.event.namespace === "__foreign"
    ) {
      problems.push(`mapping "${mapping.name}" may not read namespace ${mapping.event.namespace}`);
    }
    const scope = mapping.target._tag === "Session" ? "session" : "user";
    for (const operation of mapping.operations) {
      switch (operation._tag) {
        case "Copy":
        case "FirstWins":
          recordShape(scope, operation.to, operation._tag === "Copy" ? "copy" : "first-wins");
          if (scope === "session") sessionValueKeys.add(operation.to);
          break;
        case "ReplaceListEntry":
          recordShape(scope, operation.to, `list:${operation.key._tag}`);
          break;
        case "Classify":
          if (scope === "session") {
            problems.push(
              `mapping "${mapping.name}" classifies the session; only users can be classified`,
            );
          }
          break;
      }
    }
  }

  const fallbacks = new Set<string>();
  for (const fallback of mappings.userFallbacks) {
    const key = `${fallback.to}\u0000${fallback.fromSession}`;
    if (fallbacks.has(key)) {
      problems.push(`duplicate user fallback ${fallback.to} <- ${fallback.fromSession}`);
    }
    fallbacks.add(key);
    if (!sessionValueKeys.has(fallback.fromSession)) {
      problems.push(
        `user fallback reads session decoration "${fallback.fromSession}", which no mapping writes as a value`,
      );
    }
    const userShape = shapes.get(`user:${fallback.to}`);
    if (userShape !== undefined && userShape.startsWith("list:")) {
      problems.push(`user fallback writes "${fallback.to}", which is a list decoration`);
    }
  }
  return problems;
};

/**
 * A validated transcript interpretation: EDA's semantic revision plus host mappings, identified
 * by `version`. Only `makeTranscriptDefinition` constructs one.
 */
export class TranscriptDefinition {
  private constructor(
    readonly version: TranscriptVersion,
    readonly semanticRevision: number,
    readonly mappings: TranscriptMappings,
  ) {}

  /**
   * Validate host mapping data and derive its transcript version.
   *
   * Validation happens here, at registration, never during append: an accepted definition can
   * only degrade individual decorations at runtime (a missing field, an oversize value), never
   * event admission or other records.
   */
  static readonly make = Effect.fnUntraced(function* (input: unknown) {
    const mappings = yield* Schema.decodeUnknownEffect(TranscriptMappings)(input).pipe(
      Effect.mapError((error) => new TranscriptDefinitionError({ message: error.message })),
    );
    const problems = validationProblems(mappings);
    if (problems.length > 0) {
      return yield* new TranscriptDefinitionError({ message: problems.join("; ") });
    }
    return new TranscriptDefinition(
      computeTranscriptVersion(
        transcriptSemanticRevision,
        Schema.encodeSync(TranscriptMappings)(mappings),
      ),
      transcriptSemanticRevision,
      mappings,
    );
  });
}

/** Validate host mapping data and derive its transcript version; see `TranscriptDefinition.make`. */
export const makeTranscriptDefinition = TranscriptDefinition.make;
