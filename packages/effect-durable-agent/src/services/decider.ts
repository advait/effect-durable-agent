import * as Context from "effect/Context";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import type { ReducedState } from "../domain/reduced-state";
import type { EDACommand } from "../types/commands";
import type { EventId, SequenceNumber, SessionId } from "../types/core";
import { type DurableEventEnvelope, effectDurableAgentNamespace } from "../types/events/envelope";
import type { EDAReducerStateSnapshot } from "./reducer-registry";
import { eventSchemaNamespaces, storedJsonForm } from "./session-admission";
import type { EDAAppEvents } from "./session-store";

/** What a decider reads: the fold of every committed event at the moment of the decision. */
export interface EDADeciderView {
  readonly sessionId: SessionId;
  /** The runtime clock, read once. The same value stamps the events this decision commits. */
  readonly nowMs: number;
  readonly state: ReducedState;
  readonly reducerStates: EDAReducerStateSnapshot;
}

/** A durable event as a decider writes it. The runtime fills in the rest. */
export type EDAEventDraft<E extends DurableEventEnvelope = DurableEventEnvelope> = Omit<
  E,
  "eventId" | "sessionId" | "createdAtMs" | "trace" | "durability"
> & { readonly eventId?: EventId };

/** One item of an accepted decision: a command `submit` accepts, or an event draft. */
export type EDADeciderItem<E extends DurableEventEnvelope> = EDACommand | EDAEventDraft<E>;

/** A decider's answer. `Reject` commits nothing; `Accept` commits the marker and the items. */
export type EDADecision<E extends DurableEventEnvelope, Result, Rejection> =
  | {
      readonly _tag: "Accept";
      readonly items: ReadonlyArray<EDADeciderItem<E>>;
      readonly result: Result;
    }
  | { readonly _tag: "Reject"; readonly rejection: Rejection };

/** A registered decision: read the session's committed state and choose what to append. */
export interface EDADeciderDefinition<
  Name extends string,
  Input,
  E extends DurableEventEnvelope,
  Result,
  Rejection,
> {
  /** Stable. Matches /^[A-Za-z0-9._-]{1,128}$/ and does not start with `_eda.`. Stored in the log. */
  readonly name: Name;
  readonly input: Schema.Codec<Input, unknown, never, never>;
  /** The events this decider may emit. Drafts are encoded with it before the append. */
  readonly events: Schema.Codec<E, unknown, never, never>;
  /** Stored in the log and returned again on a retry. Keep it small and backward readable. */
  readonly result: Schema.Codec<Result, unknown, never, never>;
  readonly rejection: Schema.Codec<Rejection, unknown, never, never>;
  /** Required. 1 to 256 characters, no line feed. One acceptance per (name, key) in a session. */
  readonly idempotencyKey: (input: NoInfer<Input>) => string;
  /**
   * Pure and synchronous. No clock, no ids, no I/O. It runs inside the session's append gate.
   * Its types come from the schemas above, never from what it returns. TypeScript widens a
   * literal nested in the returned object (`payload: { to: "ai" }` checks as `string`), so build
   * nested values with their schema's `make` or write the literal `as const`.
   */
  readonly decide: (
    view: EDADeciderView,
    input: NoInfer<Input>,
  ) => EDADecision<NoInfer<E>, NoInfer<Result>, NoInfer<Rejection>>;
}

/** An accepted decision as the runtime sees it, with the draft type erased. */
export type EDAErasedDecision<Result, Rejection> =
  | {
      readonly _tag: "Accept";
      readonly items: ReadonlyArray<EDACommand | object>;
      readonly result: Result;
    }
  | { readonly _tag: "Reject"; readonly rejection: Rejection };

/**
 * A defined decider. Register it once on the runtime options (`deciders`), then call it with
 * `EDARuntime.decide`, a tool's `ctx.decide`, or the host's `decide` RPC.
 */
export interface EDADecider<
  Name extends string = string,
  Input = unknown,
  Result = unknown,
  Rejection = unknown,
> {
  readonly _tag: "EDADecider";
  readonly name: Name;
  readonly input: Schema.Codec<Input, unknown, never, never>;
  readonly result: Schema.Codec<Result, unknown, never, never>;
  readonly rejection: Schema.Codec<Rejection, unknown, never, never>;
  /** Namespaces the decider's `events` schema names. */
  readonly eventNamespaces: ReadonlyArray<string>;
  idempotencyKey(input: Input): string;
  decide(view: EDADeciderView, input: Input): EDAErasedDecision<Result, Rejection>;
  /** Encode one complete draft envelope with the decider's `events` schema; throws when it fails. */
  encodeEvent(event: unknown): unknown;
}

/** Decider names: stable identifiers that never contain the marker key's line feed. */
export const deciderNamePattern = /^[A-Za-z0-9._-]{1,128}$/;
/** Prefix reserved for framework deciders. */
export const edaReservedDeciderNamePrefix = "_eda.";
/** Longest idempotency key a decider may return. */
export const maxDeciderIdempotencyKeyLength = 256;
/** Largest encoded `result`, in UTF-8 bytes of its JSON. It is stored in the marker. */
export const maxDeciderResultBytes = 16_384;

/**
 * The marker's dedupe key. A decider name cannot hold a line feed, so the first one ends the name
 * and two different pairs never share a key. The longest is 128 + 1 + 256 = 385 characters.
 */
export const deciderMarkerDedupeKey = (decider: string, idempotencyKey: string): string =>
  `${decider}\n${idempotencyKey}`;

/** Why an idempotency key is unusable, or `undefined` when it is valid. */
export const deciderIdempotencyKeyViolation = (key: unknown): string | undefined => {
  if (typeof key !== "string") return `idempotency key is a ${typeof key}, not a string`;
  if (key.length === 0) return "idempotency key is empty";
  if (key.length > maxDeciderIdempotencyKeyLength)
    return `idempotency key is ${key.length} characters; the limit is ${maxDeciderIdempotencyKeyLength}`;
  if (key.includes("\n")) return "idempotency key contains a line feed";
  return undefined;
};

const deciderNameViolation = (name: string): string | undefined => {
  if (!deciderNamePattern.test(name)) return `must match ${String(deciderNamePattern)}`;
  if (name.startsWith(edaReservedDeciderNamePrefix))
    return `must not start with the reserved prefix ${edaReservedDeciderNamePrefix}`;
  return undefined;
};

export const EDADecider = {
  /**
   * Define a decider. Throws when the name is invalid or the `events` schema is not a union of
   * structs with literal namespaces, so registration can check every namespace it names.
   */
  define: <const Name extends string, Input, E extends DurableEventEnvelope, Result, Rejection>(
    definition: EDADeciderDefinition<Name, Input, E, Result, Rejection>,
  ): EDADecider<Name, Input, Result, Rejection> => {
    const violation = deciderNameViolation(definition.name);
    if (violation !== undefined) {
      throw new Error(`EDA decider name ${JSON.stringify(definition.name)} ${violation}`);
    }
    const encodeEvent = Schema.encodeUnknownSync(definition.events);
    return {
      _tag: "EDADecider",
      name: definition.name,
      input: definition.input,
      result: definition.result,
      rejection: definition.rejection,
      eventNamespaces: Array.from(new Set(eventSchemaNamespaces(definition.events.ast))),
      idempotencyKey: (input) => definition.idempotencyKey(input),
      decide: (view, input) => definition.decide(view, input),
      encodeEvent: (event) => encodeEvent(event),
    };
  },
};

/**
 * Encode a decider's result or rejection with its schema into JSON: what the acceptance marker
 * stores and what the host's `decide` RPC returns. An object property left `undefined` becomes
 * absent; any other value JSON would change is a failure.
 */
export const encodeDeciderJson = <A>(
  schema: Schema.Codec<A, unknown, never, never>,
  value: A,
): Result.Result<Schema.Json, string> => {
  try {
    return storedJsonForm(Schema.encodeUnknownSync(schema)(value), "encoded value");
  } catch (cause) {
    return Result.fail(`does not encode: ${String(cause)}`);
  }
};

/** What a call to a decider returns. */
export type EDADeciderOutcome<Result, Rejection> =
  | {
      readonly _tag: "Accepted";
      /** Decoded from the stored JSON, on the first call as on every replay. */
      readonly result: Result;
      /** Position of the acceptance marker. */
      readonly acceptedSeq: SequenceNumber;
      /** True when this call found an earlier acceptance and committed nothing. */
      readonly replayed: boolean;
    }
  | { readonly _tag: "Rejected"; readonly rejection: Rejection };

/** A decider call that could not run. Never a store error: the runtime stays usable. */
export class EDADeciderError extends Schema.TaggedError<EDADeciderError>()("EDADeciderError", {
  decider: Schema.String,
  reason: Schema.Literals(["unknown_decider", "invalid_input", "decide_threw", "invalid_output"]),
  message: Schema.String,
}) {}

/** The runtime's registered deciders by name. Empty unless the runtime options register some. */
export const EDADeciderRegistry = Context.Reference<ReadonlyMap<string, EDADecider>>(
  "effect-durable-agent/EDADeciderRegistry",
  { defaultValue: () => new Map() },
);

/**
 * Build the by-name registry the runtime and hosts share. Throws on a duplicate name or when a
 * decider's `events` schema names an app namespace that `appEvents` does not register; the
 * framework namespace is allowed for the event types app code may write.
 */
export const makeEDADeciderRegistry = (
  deciders: ReadonlyArray<EDADecider>,
  appEvents: EDAAppEvents | undefined,
): ReadonlyMap<string, EDADecider> => {
  const registered = new Set(appEvents === undefined ? [] : eventSchemaNamespaces(appEvents.ast));
  const byName = new Map<string, EDADecider>();
  for (const decider of deciders) {
    const violation = deciderNameViolation(decider.name);
    if (violation !== undefined) {
      throw new Error(`EDA decider name ${JSON.stringify(decider.name)} ${violation}`);
    }
    if (byName.has(decider.name)) {
      throw new Error(`Duplicate EDA decider name ${decider.name}`);
    }
    for (const namespace of decider.eventNamespaces) {
      if (namespace !== effectDurableAgentNamespace && !registered.has(namespace)) {
        throw new Error(
          `EDA decider ${decider.name} emits namespace ${namespace}, which the runtime's appEvents does not register`,
        );
      }
    }
    byName.set(decider.name, decider);
  }
  return byName;
};
