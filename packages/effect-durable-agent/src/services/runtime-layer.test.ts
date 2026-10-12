import { assert, makeMethods } from "@effect/vitest";
import { describe, it } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Response from "effect/ai/Response";

import { SessionId } from "../types/core";
import { makeLanguageModelLayer } from "../testkit/layers";
import { IdGenerator } from "./id-generator";
import { ModelResolver } from "./model-resolver";
import { EDARuntime } from "./runtime";
import { makeEDARuntimeLayer } from "./runtime-layer";
import { EDASessionStore } from "./session-store";
import { SinkCheckpointStore } from "./sink-checkpoint-store";
import { SESSION_ID, command, modelSelection, usage } from "./session-state-control-testkit";

const sessionId = SessionId.make(SESSION_ID);

/** Wraps the live generator so a test can observe that the host-owned layer minted each identity. */
const countingIdGenerator = (minted: Array<string>) =>
  Layer.effect(
    IdGenerator,
    Effect.gen(function* () {
      const live = yield* IdGenerator;
      const record =
        <A extends string>(make: () => Effect.Effect<A>) =>
        () =>
          make().pipe(Effect.tap((id) => Effect.sync(() => minted.push(id))));
      return {
        makeSessionId: record(live.makeSessionId),
        makeEventId: record(live.makeEventId),
        makeCommandId: record(live.makeCommandId),
        makeRunId: record(live.makeRunId),
        makeRunRequestId: record(live.makeRunRequestId),
        makeTurnId: record(live.makeTurnId),
        makeInferenceId: record(live.makeInferenceId),
        makeToolCallId: record(live.makeToolCallId),
        makeMessageId: record(live.makeMessageId),
        makeCompactionId: record(live.makeCompactionId),
        makeSummaryId: record(live.makeSummaryId),
        makeCorrelationId: record(live.makeCorrelationId),
      };
    }),
  ).pipe(Layer.provide(IdGenerator.Live));

describe("makeEDARuntimeLayer", () => {
  makeMethods(it).effect("mints lifecycle identities from a host-owned IdGenerator layer", () => {
    const minted: Array<string> = [];
    return Effect.gen(function* () {
      const runtime = yield* EDARuntime;
      const terminal = yield* runtime.submitAndBlock(command);
      // The terminal command event is framework-minted, so it must come from the host layer.
      assert.include(minted, terminal.event.eventId);
    }).pipe(
      Effect.provide(
        makeEDARuntimeLayer({
          config: { modelSelection },
          idGeneratorLayer: countingIdGenerator(minted),
          sessionId,
          sessionStoreLayer: EDASessionStore.InMemory(sessionId),
          sinkCheckpointStoreLayer: SinkCheckpointStore.InMemory,
          modelResolverLayer: ModelResolver.Fixed.pipe(
            Layer.provide(
              makeLanguageModelLayer(
                Stream.make(
                  Response.makePart("finish", {
                    reason: "stop",
                    usage: usage(),
                    response: undefined,
                  }),
                ),
              ),
            ),
          ),
        }),
      ),
    );
  });
});
