import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  comparePosition,
  CommandId,
  DurablePosition,
  durablePosition,
  EventId,
  Position,
  PositionOrder,
  SequenceNumber,
  SubSequenceNumber,
  SessionId,
} from "./core";

const VALID_UUID_V7 = "018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a";
const VALID_UUID_V4 = "018f6bd5-2f2a-4b1e-8f1a-1f2e3d4c5b6a";

describe("effect-durable-agent core types", () => {
  it("decodes framework IDs as UUIDv7-branded strings", () => {
    expect(Schema.decodeUnknownSync(EventId)(VALID_UUID_V7)).toBe(VALID_UUID_V7);
    expect(Schema.decodeUnknownSync(SessionId)(VALID_UUID_V7)).toBe(VALID_UUID_V7);
  });

  it("accepts migrated UUIDv4 session ids while keeping framework event ids UUIDv7-only", () => {
    expect(Schema.decodeUnknownSync(SessionId)(VALID_UUID_V4)).toBe(VALID_UUID_V4);
    expect(() => Schema.decodeUnknownSync(EventId)(VALID_UUID_V4)).toThrow();
    expect(() => Schema.decodeUnknownSync(EventId)("not-a-uuid")).toThrow();
    expect(() => Schema.decodeUnknownSync(SessionId)("not-a-uuid")).toThrow();
  });

  it("represents repeated framework IDs in JSON Schema", () => {
    const document = Schema.toJsonSchemaDocument(
      Schema.Struct({
        commandId: CommandId,
        eventId: EventId,
      }),
    );

    expect(document.schema).toMatchObject({
      properties: {
        commandId: { allOf: [{ format: "uuid" }] },
        eventId: { allOf: [{ format: "uuid" }] },
      },
    });
  });

  it("models durable positions as seq plus literal subSeq zero", () => {
    const position = durablePosition(SequenceNumber.make(3));

    expect(position).toEqual({ seq: 3, subSeq: 0 });
    expect(Schema.decodeUnknownSync(DurablePosition)(position)).toEqual(position);
    expect(() => Schema.decodeUnknownSync(DurablePosition)({ seq: 3, subSeq: 1 })).toThrow();
  });

  it("validates positions at the boundary", () => {
    expect(Schema.decodeUnknownSync(Position)({ seq: 3, subSeq: 1 })).toEqual({
      seq: 3,
      subSeq: 1,
    });

    expect(() => Schema.decodeUnknownSync(Position)({ seq: -1, subSeq: 0 })).toThrow();
    expect(() => Schema.decodeUnknownSync(Position)({ seq: 1.5, subSeq: 0 })).toThrow();
    expect(() => Schema.decodeUnknownSync(Position)({ seq: 1, subSeq: -1 })).toThrow();
  });

  it("orders positions lexicographically", () => {
    const left = Position.make({ seq: SequenceNumber.make(3), subSeq: SubSequenceNumber.make(2) });
    const right = Position.make({ seq: SequenceNumber.make(4), subSeq: SubSequenceNumber.make(0) });
    const laterAtSameSeq = Position.make({
      seq: SequenceNumber.make(3),
      subSeq: SubSequenceNumber.make(3),
    });

    expect(PositionOrder(left, right)).toBeLessThan(0);
    expect(PositionOrder(right, left)).toBeGreaterThan(0);
    expect(PositionOrder(left, laterAtSameSeq)).toBeLessThan(0);
    expect(PositionOrder(left, left)).toBe(0);
    expect(comparePosition(left, right)).toBe(PositionOrder(left, right));
  });
});
