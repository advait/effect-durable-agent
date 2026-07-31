import * as Prompt from "effect/unstable/ai/Prompt";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  InferenceRecordSchema,
  JsonCommandRecordSchema,
  JsonMessageRecordSchema,
  JsonPromptFilePart,
  RecoveryContinuationRecordSchema,
  RunRecordSchema,
  TokenConsumptionStateSchema,
  ToolCallRecordSchema,
  TurnRecordSchema,
} from "./reduced-state-schema";

describe("reduced state schemas", () => {
  it("encodes Prompt byte and URL file data as JSON strings", () => {
    const encode = Schema.encodeSync(JsonPromptFilePart);

    expect(
      encode(
        Prompt.filePart({
          data: new Uint8Array([104, 105]),
          mediaType: "text/plain",
        }),
      ).data,
    ).toBe("data:text/plain;base64,aGk=");
    expect(
      encode(
        Prompt.filePart({
          data: new URL("https://example.com/file.txt"),
          mediaType: "text/plain",
        }),
      ).data,
    ).toBe("https://example.com/file.txt");
  });

  it("retains canonical lifecycle identity checks", () => {
    const parse = Schema.decodeUnknownOption(JsonCommandRecordSchema);

    expect(
      Option.isSome(
        parse({
          commandId: "018f6bd5-2f2a-7b1e-8f1b-1f2e3d4c5b6a",
        }),
      ),
    ).toBe(true);
    expect(Option.isNone(parse({ commandId: "not-a-command-id" }))).toBe(true);
  });

  it("represents the combined snapshot lifecycle in JSON Schema", () => {
    expect(() =>
      Schema.toJsonSchemaDocument(
        Schema.Struct({
          commands: Schema.Array(JsonCommandRecordSchema),
          inferences: Schema.Array(InferenceRecordSchema),
          messages: Schema.Array(JsonMessageRecordSchema),
          recoveryContinuations: Schema.Array(RecoveryContinuationRecordSchema),
          runs: Schema.Array(RunRecordSchema),
          tokenConsumption: Schema.optionalKey(TokenConsumptionStateSchema),
          toolCalls: Schema.Array(ToolCallRecordSchema),
          turns: Schema.Array(TurnRecordSchema),
        }),
      ),
    ).not.toThrow();
  });
});
