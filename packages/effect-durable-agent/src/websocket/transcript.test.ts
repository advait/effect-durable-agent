import * as fc from "fast-check";
import { describe, expect, it } from "vite-plus/test";

import { SequenceNumber } from "../types/core";
import { defaultEDAWebSocketFlowControl } from "./messages";
import {
  EDA_TRANSCRIPT_PROTOCOL_MAJOR,
  edaTranscriptChunkFrameText,
  edaTranscriptNegotiation,
  negotiateEDATranscript,
  splitEDATranscriptUpdate,
  utf8ByteLength,
} from "./transcript";
import {
  applyEDATranscriptServerMessage,
  emptyEDATranscriptClientState,
} from "./transcript-client";
import {
  checkpointTranscriptDelivery,
  makeTranscriptDeliveryState,
  onTranscriptAck,
  restoreTranscriptDeliveryState,
} from "./transcript-delivery";

describe("transcript negotiation", () => {
  it("decodes only the stable envelope, before anything version-specific", () => {
    const hello = {
      _tag: "hello",
      protocolVersion: 99,
      somethingNew: { nested: true },
      negotiation: { ...edaTranscriptNegotiation, capabilities: ["window", "future-feature"] },
    };
    expect(negotiateEDATranscript(hello)._tag).toBe("Compatible");
    expect(
      negotiateEDATranscript({
        negotiation: {
          ...edaTranscriptNegotiation,
          protocolMajor: EDA_TRANSCRIPT_PROTOCOL_MAJOR + 1,
        },
      })._tag,
    ).toBe("MajorMismatch");
    expect(negotiateEDATranscript({ _tag: "hello" })._tag).toBe("Undecodable");
    expect(negotiateEDATranscript("ssr-data")._tag).toBe("Undecodable");
  });

  it("surfaces a major mismatch from the socket hello to the client", () => {
    const step = applyEDATranscriptServerMessage(
      emptyEDATranscriptClientState,
      JSON.stringify({
        _tag: "hello",
        negotiation: { ...edaTranscriptNegotiation, protocolMajor: 7 },
      }),
    );
    expect(step.outcome).toMatchObject({ _tag: "Hello", negotiation: { _tag: "MajorMismatch" } });
  });
});

describe("transcript chunking", () => {
  it("splits any update into frames within the limit that rejoin exactly", () => {
    fc.assert(
      fc.property(
        fc.string({ unit: "grapheme", maxLength: 3_000 }),
        fc.string({ unit: "binary", maxLength: 500 }),
        fc.integer({ min: 300, max: 2_000 }),
        (graphemes, binary, maxFrameBytes) => {
          const encoded = JSON.stringify({ text: graphemes, raw: binary, quote: '"\\\n\u0001' });
          const chunks = splitEDATranscriptUpdate(encoded, maxFrameBytes);
          expect(chunks.join("")).toBe(encoded);
          chunks.forEach((data, index) => {
            const frame = edaTranscriptChunkFrameText({
              frameId: 2 ** 40,
              updateSeq: 2 ** 40,
              index,
              count: chunks.length,
              data,
            });
            expect(utf8ByteLength(frame)).toBeLessThanOrEqual(maxFrameBytes);
            expect(JSON.parse(frame).data).toBe(data);
          });
        },
      ),
      { numRuns: 200 },
    );
  });

  it("discards an interrupted transfer instead of applying part of it", () => {
    const chunk = (frameId: number, updateSeq: number, index: number) =>
      edaTranscriptChunkFrameText({ frameId, updateSeq, index, count: 3, data: "{" });
    let state = emptyEDATranscriptClientState;
    const first = applyEDATranscriptServerMessage(state, chunk(1, 1, 0));
    expect(first.outcome._tag).toBe("Partial");
    expect(first.ack).toMatchObject({ frameId: 1, appliedUpdateSeq: 0 });
    state = first.state;
    const skipped = applyEDATranscriptServerMessage(state, chunk(2, 1, 2));
    expect(skipped.outcome._tag).toBe("Resubscribe");
    expect(skipped.state.assembling).toBeUndefined();
    expect(skipped.state.window).toBeUndefined();
  });
});

describe("transcript delivery acknowledgements", () => {
  const policy = { ...defaultEDAWebSocketFlowControl, maxInFlightFrames: 4 };
  const sent = {
    ...makeTranscriptDeliveryState(policy),
    nextFrameId: 4,
    inFlight: [1, 2, 3].map((frameId) => ({ frameId, sentAtMs: 0 })),
    nextUpdateSeq: 3,
    completedUpdateSeq: 1,
    completedThroughSeq: SequenceNumber.make(10),
    transfer: { updateSeq: 2, throughSeq: SequenceNumber.make(12), count: 5, nextIndex: 2 },
    staged: ["c", "d", "e"],
  };

  it("frees capacity on receipts but applies only complete updates", () => {
    const receipt = onTranscriptAck(sent, {
      _tag: "ack",
      frameId: 3,
      durableThroughSeq: 10,
      appliedUpdateSeq: 1,
    });
    expect(receipt._tag).toBe("Applied");
    if (receipt._tag === "Applied") expect(receipt.state.inFlight).toEqual([]);
    expect(
      onTranscriptAck(sent, {
        _tag: "ack",
        frameId: 3,
        durableThroughSeq: 12,
        appliedUpdateSeq: 2,
      }),
    ).toMatchObject({ _tag: "ProtocolError" });
    expect(onTranscriptAck(sent, { _tag: "ack", frameId: 4, durableThroughSeq: 0 })).toMatchObject({
      _tag: "ProtocolError",
    });
  });

  it("cannot resume a transfer whose staged chunks died with the isolate", () => {
    expect(restoreTranscriptDeliveryState(policy, checkpointTranscriptDelivery(sent))._tag).toBe(
      "TransferLost",
    );
    const { transfer: _transfer, ...idle } = sent;
    const restored = restoreTranscriptDeliveryState(
      policy,
      checkpointTranscriptDelivery({ ...idle, staged: [] }),
    );
    expect(restored._tag).toBe("Restored");
  });
});
