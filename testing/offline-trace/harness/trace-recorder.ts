import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";

import { toJsonValue, type JsonValue } from "../json";

/** Event categories emitted by the offline trace harness. */
export type OfflineTraceKind =
  | "run.started"
  | "run.finished"
  | "model.request"
  | "model.part"
  | "model.finish"
  | "durable.event"
  | "live.event"
  | "verification.result";

/** Timestamped trace entry persisted into offline run artifacts. */
export interface OfflineTraceEvent {
  readonly atMs: number;
  readonly kind: OfflineTraceKind;
  readonly payload: JsonValue;
}

/** In-memory recorder API used by the trace harness and model wrapper. */
export interface OfflineTraceRecorderShape {
  readonly record: (kind: OfflineTraceKind, payload: unknown) => Effect.Effect<void>;
  readonly events: () => Effect.Effect<ReadonlyArray<OfflineTraceEvent>>;
}

/** Build an in-memory trace sink used by offline EDA validation runs. */
export const makeOfflineTraceRecorder: Effect.Effect<OfflineTraceRecorderShape> = Effect.gen(
  function* () {
    const events = yield* Ref.make<ReadonlyArray<OfflineTraceEvent>>([]);
    return {
      record: (kind, payload) =>
        Effect.gen(function* () {
          const atMs = yield* Clock.currentTimeMillis;
          const event = { atMs, kind, payload: toJsonValue(payload) } satisfies OfflineTraceEvent;
          yield* Ref.update(events, (existing) => [...existing, event]);
        }),
      events: () => Ref.get(events),
    };
  },
);

/** In-memory trace sink service for code paths that prefer Context-based access. */
export class OfflineTraceRecorder extends Context.Service<
  OfflineTraceRecorder,
  OfflineTraceRecorderShape
>()("@effect-durable-agent/testing/OfflineTraceRecorder") {
  static readonly Live = Layer.effect(OfflineTraceRecorder, makeOfflineTraceRecorder);
}
