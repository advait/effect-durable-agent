import * as Schema from "effect/Schema";

import { SequenceNumber, type InferenceId, type TurnId } from "../types/core";
import type { PositionedEvent } from "../types/events";
import {
  InferenceCompletedEvent,
  InferenceFailedEvent,
  InferenceStartedEvent,
  TurnCompletedEvent,
  TurnFailedEvent,
  TurnStoppedEvent,
} from "../types/events/durable";
import {
  ReasoningDeltaEvent,
  TextDeltaEvent,
  ToolParamsDeltaEvent,
  ToolParamsStartEvent,
} from "../types/events/ephemeral";
import { transcriptRecordId } from "./records";
import type { TranscriptActiveContent, TranscriptActivePart } from "./window";

/**
 * The accumulated value of the inference streaming now, folded from the published live stream.
 *
 * Text and reasoning deltas carry only a `providerPartId`; they belong to the session's one
 * active inference, which is the latest `InferenceStarted` without a terminal. That association
 * is sound because `SessionState` publishes every durable and ephemeral event under one append
 * gate, in commit order: an inference's start is published before its first delta and its
 * terminal after its last. A session runs at most one inference at a time.
 *
 * The value is retired on every inference terminal (commit, partial plus failure, empty success
 * and recovery failure all end in `InferenceCompleted` or `InferenceFailed`), and on its turn's
 * terminal as a backstop. It is volatile: an isolate loss loses it, and recovery records the
 * inference as failed (design decision 6).
 */
export interface TranscriptActiveContentState {
  /** Highest durable seq this accumulator has folded; the publication side of the capture fence. */
  readonly publishedThroughSeq: SequenceNumber;
  /** Increases whenever the served active content changes; lets subscribers skip unchanged sends. */
  readonly revision: number;
  readonly inference: ActiveInference | undefined;
}

interface ActiveInference {
  readonly inferenceId: InferenceId;
  readonly turnId: TurnId;
  readonly parts: ReadonlyArray<TranscriptActivePart>;
}

/** The accumulator before anything is published in this isolate. */
export const emptyTranscriptActiveContentState: TranscriptActiveContentState = {
  publishedThroughSeq: SequenceNumber.make(0),
  revision: 0,
  inference: undefined,
};

const isInferenceStarted = Schema.is(InferenceStartedEvent);
const isInferenceCompleted = Schema.is(InferenceCompletedEvent);
const isInferenceFailed = Schema.is(InferenceFailedEvent);
const isTurnCompleted = Schema.is(TurnCompletedEvent);
const isTurnFailed = Schema.is(TurnFailedEvent);
const isTurnStopped = Schema.is(TurnStoppedEvent);
const isTextDelta = Schema.is(TextDeltaEvent);
const isReasoningDelta = Schema.is(ReasoningDeltaEvent);
const isToolParamsStart = Schema.is(ToolParamsStartEvent);
const isToolParamsDelta = Schema.is(ToolParamsDeltaEvent);

/** Fold one published event, in publication order, into the active-content accumulator. */
export const foldTranscriptActiveContent = (
  state: TranscriptActiveContentState,
  positioned: PositionedEvent,
): TranscriptActiveContentState => {
  const event = positioned.event;
  if (event.durability === "durable") {
    const published: TranscriptActiveContentState = {
      ...state,
      publishedThroughSeq: SequenceNumber.make(
        Math.max(state.publishedThroughSeq, positioned.position.seq),
      ),
    };
    if (isInferenceStarted(event)) {
      return withInference(published, {
        inferenceId: event.payload.inferenceId,
        turnId: event.payload.turnId,
        parts: [],
      });
    }
    const active = published.inference;
    if (active === undefined) return published;
    const retires =
      ((isInferenceCompleted(event) || isInferenceFailed(event)) &&
        event.payload.inferenceId === active.inferenceId) ||
      ((isTurnCompleted(event) || isTurnFailed(event) || isTurnStopped(event)) &&
        event.payload.turnId === active.turnId);
    return retires ? withInference(published, undefined) : published;
  }

  const active = state.inference;
  if (active === undefined) return state;
  if (isTextDelta(event)) {
    return appendText(
      state,
      active,
      "Text",
      `text:${event.payload.providerPartId}`,
      event.payload.delta,
    );
  }
  if (isReasoningDelta(event)) {
    return appendText(
      state,
      active,
      "Reasoning",
      `reasoning:${event.payload.providerPartId}`,
      event.payload.delta,
    );
  }
  if (isToolParamsStart(event)) {
    const id = `tool:${event.payload.toolCallId}`;
    if (active.parts.some((part) => part.id === id)) return state;
    return withInference(state, {
      ...active,
      parts: [
        ...active.parts,
        {
          id,
          part: {
            _tag: "ToolCall",
            providerPartId: event.payload.providerPartId,
            toolName: event.payload.toolName,
            providerExecuted: event.payload.providerExecuted,
          },
          paramsText: "",
        },
      ],
    });
  }
  if (isToolParamsDelta(event)) {
    const id = `tool:${event.payload.toolCallId}`;
    const delta = event.payload.delta;
    if (delta.length === 0 || !active.parts.some((part) => part.id === id)) return state;
    return withInference(state, {
      ...active,
      parts: active.parts.map((part) =>
        part.id === id ? { ...part, paramsText: (part.paramsText ?? "") + delta } : part,
      ),
    });
  }
  return state;
};

const appendText = (
  state: TranscriptActiveContentState,
  active: ActiveInference,
  tag: "Text" | "Reasoning",
  id: string,
  delta: string,
): TranscriptActiveContentState => {
  if (delta.length === 0) return state;
  const index = active.parts.findIndex((part) => part.id === id);
  const existing = index === -1 ? undefined : active.parts[index];
  const text =
    existing !== undefined && existing.part._tag === tag ? existing.part.text + delta : delta;
  const part: TranscriptActivePart = { id, part: { _tag: tag, text } };
  return withInference(state, {
    ...active,
    parts:
      index === -1
        ? [...active.parts, part]
        : active.parts.map((candidate, candidateIndex) =>
            candidateIndex === index ? part : candidate,
          ),
  });
};

const withInference = (
  state: TranscriptActiveContentState,
  inference: ActiveInference | undefined,
): TranscriptActiveContentState =>
  state.inference === undefined && inference === undefined
    ? state
    : { ...state, revision: state.revision + 1, inference };

/** The active content to serve, in the window's shape; empty when nothing is streaming. */
export const transcriptActiveContentOf = (
  state: TranscriptActiveContentState,
): ReadonlyArray<TranscriptActiveContent> =>
  state.inference === undefined
    ? []
    : [
        {
          assistantId: transcriptRecordId.assistant(state.inference.inferenceId),
          parts: state.inference.parts,
        },
      ];
