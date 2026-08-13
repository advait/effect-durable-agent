import * as Effect from "effect/Effect";

import { IdGenerator } from "effect-durable-agent/services/id-generator";
import { EventId } from "effect-durable-agent/types/core";

export const mintExampleEventId = (): Promise<EventId> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const ids = yield* IdGenerator;
      return yield* ids.makeEventId();
    }).pipe(Effect.provide(IdGenerator.Live)),
  );
