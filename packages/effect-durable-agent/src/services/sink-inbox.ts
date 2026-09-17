import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";

import { SequenceNumber } from "../types/core";
import type { PositionedEvent } from "../types/events";

/** Durable payloads stay in the log; only bounded best-effort ephemeral work is retained. */
const ephemeralCapacity = 128;
const ephemeralByteCapacity = 256 * 1024;

/** An ephemeral update could not be measured as a JSON wire value. */
export class SinkEphemeralEncodingError extends Schema.TaggedErrorClass<SinkEphemeralEncodingError>()(
  "SinkEphemeralEncodingError",
  { cause: Schema.Defect() },
) {}

/** One ordered step on a sink's serialized delivery lane. */
type SinkWork =
  | { readonly _tag: "Durable"; readonly throughSeq: SequenceNumber }
  | { readonly _tag: "Ephemeral"; readonly event: PositionedEvent };

/**
 * Coalesces durable heads without losing ephemeral ordering barriers. Publication never
 * waits for delivery. Overflow drops the newest ephemeral update, never durable work.
 */
export const makeSinkInbox = Effect.fnUntraced(function* (initialHead: SequenceNumber) {
  const wake = yield* Effect.acquireRelease(Queue.sliding<void>(1), Queue.shutdown);
  let head = initialHead;
  const pending: Array<{ readonly event: PositionedEvent; readonly bytes: number }> = [];
  let pendingBytes = 0;

  const notify = (throughSeq: SequenceNumber) =>
    Effect.sync(() => {
      head = SequenceNumber.make(Math.max(head, throughSeq));
      Queue.offerUnsafe(wake, undefined);
    });

  const offerEphemeral = (event: PositionedEvent) =>
    Effect.try({
      try: () => {
        const bytes = new TextEncoder().encode(JSON.stringify(event)).byteLength;
        if (pending.length >= ephemeralCapacity || bytes > ephemeralByteCapacity - pendingBytes) {
          return false;
        }
        pending.push({ event, bytes });
        pendingBytes += bytes;
        Queue.offerUnsafe(wake, undefined);
        return true;
      },
      catch: (cause) => new SinkEphemeralEncodingError({ cause }),
    });

  const poll = (cursor: SequenceNumber): Effect.Effect<SinkWork | undefined> =>
    Effect.sync(() => {
      while (pending.length > 0) {
        const first = pending[0];
        if (first === undefined) break;
        if (first.event.position.seq > cursor) {
          return { _tag: "Durable", throughSeq: first.event.position.seq };
        }
        pending.shift();
        pendingBytes -= first.bytes;
        if (first.event.position.seq === cursor) {
          return { _tag: "Ephemeral", event: first.event };
        }
      }
      return cursor < head ? { _tag: "Durable", throughSeq: head } : undefined;
    });

  return { notify, offerEphemeral, poll, awaitWork: Queue.take(wake) };
});
