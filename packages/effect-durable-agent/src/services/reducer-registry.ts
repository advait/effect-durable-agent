import type { DurableEventEnvelope } from "../types/events";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { selectCommittedEvent, type CommittedDurableEvent } from "./session-store";

/** Input accepted by the reducer constructor before schema codecs are materialized. */
export interface EDAReducerDefinition<
  State,
  Name extends string = string,
  E = import("../types/events/durable").DecodedDurableEvent,
> {
  readonly name: Name;
  readonly initial: State;
  readonly stateSchema: Schema.Codec<State, unknown, never, never>;
  readonly schemaVersion?: number;
  readonly encode?: (state: State) => unknown;
  readonly decode?: (payload: unknown) => State;
  /** Return the next state without mutating the input; projections may be shared across sinks. */
  reduce(state: State, event: CommittedDurableEvent<E>): State;
}

/** App-specific pure reducer over the durable session event log. */
export interface EDAReducer<State = unknown, Name extends string = string> {
  readonly name: Name;
  readonly initial: State;
  readonly stateSchema: Schema.Codec<State, unknown, never, never>;
  readonly schemaVersion?: number;
  encode(state: State): unknown;
  decode(payload: unknown): State;
  /** Return the next state without mutating the input; projections may be shared across sinks. */
  reduce(state: State, event: CommittedDurableEvent): State;
}

/** Convenience constructor preserving a reducer's literal name and schema-derived state type. */
export const EDAReducer = {
  /** Select and narrow reducer input from one event schema; malformed registered events fail in the store. */
  forEvents: <E extends DurableEventEnvelope, State, const Name extends string>(
    events: Schema.Codec<E, unknown, never, never>,
    reducer: EDAReducerDefinition<State, Name, E>,
  ): EDAReducer<State, Name> =>
    EDAReducer.make({
      ...reducer,
      reduce: (state, entry) => {
        const selected = selectCommittedEvent(events, entry);
        return selected === undefined ? state : reducer.reduce(state, selected);
      },
    }),
  make: <State, const Name extends string = string>(
    reducer: EDAReducerDefinition<State, Name>,
  ): EDAReducer<State, Name> => ({
    ...reducer,
    encode: reducer.encode ?? ((state) => Schema.encodeUnknownSync(reducer.stateSchema)(state)),
    decode: reducer.decode ?? ((payload) => Schema.decodeUnknownSync(reducer.stateSchema)(payload)),
  }),
};

/** Snapshot of all app reducer states, keyed by reducer name. */
export type EDAReducerStateSnapshot = ReadonlyMap<string, unknown>;

interface EDASerializedReducerState {
  readonly name: string;
  readonly state: unknown;
}

type EDAReducerStateForName<
  Entry extends EDASerializedReducerState,
  Name extends Entry["name"],
> = Extract<Entry, { readonly name: Name }>["state"];

/** Exact reducer object required for every member of a serialized state union. */
export type EDAReducersFor<Entry extends EDASerializedReducerState> = {
  readonly [Name in Entry["name"]]: EDAReducer<EDAReducerStateForName<Entry, Name>, Name>;
};

/** Schema-backed app reducer registry used by both runtime folding and snapshot serialization. */
export interface EDATypedReducerRegistry<
  EntrySchema extends Schema.Codec<EDASerializedReducerState, unknown, never, never>,
> {
  readonly entrySchema: EntrySchema;
  readonly reducers: ReadonlyArray<EDAReducer>;
  readonly serialize: (snapshot: EDAReducerStateSnapshot) => ReadonlyArray<EntrySchema["Type"]>;
}

const reducerValues = (reducers: Readonly<Record<string, EDAReducer>>): ReadonlyArray<EDAReducer> =>
  Object.values(reducers);

/**
 * Register an app's complete reducer set against its public serialized-state union.
 *
 * Missing reducers, extra reducers, reducer-name drift, and state-schema drift are
 * rejected by TypeScript at the registration site.
 */
export const defineEDAReducerRegistry = <
  EntrySchema extends Schema.Codec<EDASerializedReducerState, unknown, never, never>,
>(
  entrySchema: EntrySchema,
  reducers: EDAReducersFor<EntrySchema["Type"]>,
): EDATypedReducerRegistry<EntrySchema> => {
  const values = reducerValues(reducers);
  const serializedStatesSchema = Schema.Array(entrySchema);
  return {
    entrySchema,
    reducers: values,
    serialize: (snapshot) =>
      Schema.decodeUnknownSync(serializedStatesSchema)(
        values.map((reducer) => ({
          name: reducer.name,
          state: snapshot.get(reducer.name) ?? reducer.initial,
        })),
      ),
  };
};

/** Registry that folds app-specific read-model state beside EDA reduced state. */
export interface EDAReducerRegistryShape {
  readonly reducers: ReadonlyArray<EDAReducer>;
  readonly initial: EDAReducerStateSnapshot;
  readonly reduce: (
    state: EDAReducerStateSnapshot,
    events: ReadonlyArray<CommittedDurableEvent>,
  ) => EDAReducerStateSnapshot;
}

/** Prefix reserved for framework-owned reducer checkpoints. */
export const edaReservedReducerNamePrefix = "_eda.";

/** Return the persisted schema version for one reducer. */
export const reducerSchemaVersion = (reducer: EDAReducer): number => reducer.schemaVersion ?? 1;

/** Encode one reducer state for durable checkpoint storage. */
export const encodeReducerState = <State, Name extends string>(
  reducer: EDAReducer<State, Name>,
  state: State,
): unknown => reducer.encode(state);

/** Decode one reducer state from durable checkpoint storage. */
export const decodeReducerState = <State, Name extends string>(
  reducer: EDAReducer<State, Name>,
  payload: unknown,
): State => reducer.decode(payload);

/** Registry for app-specific durable metadata reducers. */
export class EDAReducerRegistry extends Context.Service<
  EDAReducerRegistry,
  EDAReducerRegistryShape
>()("@effect-durable-agent/EDAReducerRegistry") {
  static readonly Empty = Layer.succeed(EDAReducerRegistry, makeReducerRegistry([]));

  static readonly Live = (reducers: ReadonlyArray<EDAReducer>) =>
    Layer.succeed(EDAReducerRegistry, makeReducerRegistry(reducers));
}

/** Read one typed reducer state from a snapshot. */
export const getEDAReducerState = <State, Name extends string>(
  snapshot: EDAReducerStateSnapshot,
  reducer: EDAReducer<State, Name>,
): State => {
  const state = snapshot.get(reducer.name);
  if (state === undefined) return reducer.initial;
  if (Schema.is(reducer.stateSchema)(state)) return state;
  throw new Error(`Invalid in-memory reducer state for ${reducer.name}`);
};

function makeReducerRegistry(reducers: ReadonlyArray<EDAReducer>): EDAReducerRegistryShape {
  validateReducerNames(reducers);
  const initial = new Map(reducers.map((reducer) => [reducer.name, reducer.initial]));
  return {
    reducers,
    initial,
    reduce: (state, events) => {
      if (events.length === 0 || reducers.length === 0) {
        return state;
      }
      const next = new Map(state);
      for (const event of events) {
        for (const reducer of reducers) {
          const current = next.get(reducer.name) ?? reducer.initial;
          next.set(reducer.name, reducer.reduce(current, event));
        }
      }
      return next;
    },
  };
}

function validateReducerNames(reducers: ReadonlyArray<EDAReducer>): void {
  const seen = new Set<string>();
  for (const reducer of reducers) {
    if (reducer.name.startsWith(edaReservedReducerNamePrefix)) {
      throw new Error(
        `Reducer name ${reducer.name} uses reserved EDA prefix ${edaReservedReducerNamePrefix}`,
      );
    }
    if (seen.has(reducer.name)) {
      throw new Error(`Duplicate EDA reducer name ${reducer.name}`);
    }
    seen.add(reducer.name);
  }
}
