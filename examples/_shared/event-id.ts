import * as Effect from "effect/Effect";

import { IdGenerator } from "../../src/services/id-generator";
import { EventId } from "../../src/types/core";

export const mintExampleEventId = (): Promise<EventId> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const ids = yield* IdGenerator;
      return yield* ids.makeEventId();
    }).pipe(Effect.provide(IdGenerator.Live)),
  );
