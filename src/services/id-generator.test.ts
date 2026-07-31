import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { CommandId, EventId, RunId, SessionId } from "../types/core";
import { IdGenerator } from "./id-generator";

const UUID_A = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const UUID_B = "018f6bd5-2f2a-7b1e-9f1a-1f2e3d4c5b6a";
const UUID_C = "018f6bd5-2f2a-7b1e-af1a-1f2e3d4c5b6a";

describe("IdGenerator", () => {
  it("provides live ID minting as an Effect service", () => {
    const program = Effect.gen(function* () {
      const ids = yield* IdGenerator;
      return {
        sessionId: yield* ids.makeSessionId(),
        eventId: yield* ids.makeEventId(),
      };
    }).pipe(Effect.provide(IdGenerator.Live));

    const result = Effect.runSync(program);

    expect(Schema.is(SessionId)(result.sessionId)).toBe(true);
    expect(Schema.is(EventId)(result.eventId)).toBe(true);
    expect(result.eventId.at(14)).toBe("7");
    expect(result.sessionId).not.toBe(result.eventId);
  });

  it("provides deterministic test layers with the same service contract", () => {
    const program = Effect.gen(function* () {
      const ids = yield* IdGenerator;
      return {
        commandId: yield* ids.makeCommandId(),
        runId: yield* ids.makeRunId(),
        eventId: yield* ids.makeEventId(),
      };
    }).pipe(Effect.provide(IdGenerator.Deterministic([UUID_A, UUID_B, UUID_C])));

    const result = Effect.runSync(program);

    expect(result.commandId).toBe(CommandId.make(UUID_A));
    expect(result.runId).toBe(RunId.make(UUID_B));
    expect(result.eventId).toBe(EventId.make(UUID_C));
  });
});
