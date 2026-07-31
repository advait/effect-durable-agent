import * as Option from "effect/Option";
import * as Prompt from "effect/unstable/ai/Prompt";

import { assertNever } from "./assert-never";
import type { InferenceId, RunId, ToolCallId, TurnId } from "../types/core";
import type {
  FailurePayload,
  InferenceCompletedPayload,
  InferenceFailedPayload,
  ProviderPartId,
  ReasoningDeltaPayload,
  TextDeltaPayload,
  ToolCallCreatedPayload,
  ToolCallRejectedPayload,
  ToolName,
  ToolParamsDeltaPayload,
  ToolParamsEndPayload,
  ToolParamsStartPayload,
  UsagePayload,
} from "../types/events";

/** Stable lifecycle identity for one provider stream inference. */
export interface InferenceIdentity {
  readonly runId: RunId;
  readonly turnId: TurnId;
  readonly inferenceId: InferenceId;
}

/** Whether the inference still accepts stream parts or has crossed a terminal boundary. */
export type InferencePhase = "streaming" | "sealed";

/** Local lifecycle for speculative tool-parameter drafts seen before finish sealing. */
export type ToolDraftStatus = "streaming" | "ended" | "staged-created" | "staged-rejected";

/** In-memory speculative state for one provider-streamed tool call. */
export interface ToolDraft extends InferenceIdentity {
  readonly providerPartId: ProviderPartId;
  readonly toolCallId: ToolCallId;
  readonly toolName: ToolName;
  readonly providerExecuted: boolean;
  readonly paramsText: string;
  readonly status: ToolDraftStatus;
}

interface ToolDecisionFields extends InferenceIdentity {
  readonly providerPartId: ProviderPartId;
  readonly toolCallId: ToolCallId;
  readonly toolName: ToolName;
  readonly params: unknown;
  readonly providerExecuted: boolean;
}

/** Staged decision for a schema-valid final tool call. */
export interface ToolCallCreatedDecision extends ToolDecisionFields {
  readonly _tag: "ToolCallCreatedDecision";
}

/** Staged decision for a final tool call that cannot execute. */
export interface ToolCallRejectedDecision extends ToolDecisionFields {
  readonly _tag: "ToolCallRejectedDecision";
  readonly reason: "unknown-tool" | "invalid-params";
  readonly error: FailurePayload;
  readonly modelFeedback: string;
}

/** Staged durable tool decision emitted after the owning inference is sealed. */
export type ToolCallDecision = ToolCallCreatedDecision | ToolCallRejectedDecision;

/** Pure fold state for one provider inference stream. */
export interface InferenceState extends InferenceIdentity {
  readonly phase: InferencePhase;
  readonly assistantText: string;
  readonly reasoningText: string;
  readonly responseMetadata: Option.Option<unknown>;
  readonly drafts: ReadonlyMap<ProviderPartId, ToolDraft>;
  readonly decisions: ReadonlyArray<ToolCallDecision>;
}

/** Effectful shell's validation result for a final provider tool-call part. */
export type ToolValidation =
  | { readonly _tag: "ValidToolParams"; readonly params: unknown }
  | { readonly _tag: "UnknownTool" }
  | { readonly _tag: "InvalidToolParams"; readonly message: string };

/** Non-delta stream boundaries that currently affect ordering/tracing but not state. */
export type StreamBoundaryType = "text-start" | "text-end" | "reasoning-start" | "reasoning-end";

/** Provider stream part normalized into data-only input for `step`. */
export type InferenceStepInput =
  | { readonly type: StreamBoundaryType; readonly providerPartId: ProviderPartId }
  | { readonly type: "text-delta"; readonly providerPartId: ProviderPartId; readonly delta: string }
  | {
      readonly type: "reasoning-delta";
      readonly providerPartId: ProviderPartId;
      readonly delta: string;
    }
  | {
      readonly type: "tool-params-start";
      readonly providerPartId: ProviderPartId;
      readonly toolCallId: ToolCallId;
      readonly toolName: ToolName;
      readonly providerExecuted: boolean;
    }
  | {
      readonly type: "tool-params-delta";
      readonly providerPartId: ProviderPartId;
      readonly delta: string;
    }
  | { readonly type: "tool-params-end"; readonly providerPartId: ProviderPartId }
  | {
      readonly type: "tool-call";
      readonly providerPartId: ProviderPartId;
      readonly toolCallId: ToolCallId;
      readonly toolName: ToolName;
      readonly params: unknown;
      readonly providerExecuted: boolean;
      readonly validation: ToolValidation;
    }
  | { readonly type: "response-metadata"; readonly responseMetadata: unknown }
  | {
      readonly type: "finish";
      readonly finishReason?: string;
      readonly usage?: UsagePayload;
      readonly finishMetadata?: unknown;
    }
  | { readonly type: "error"; readonly error: FailurePayload };

/** Live-only payloads emitted by pure inference-state transitions. */
export type EphemeralPayload =
  | { readonly type: "TextDelta"; readonly payload: TextDeltaPayload }
  | { readonly type: "ReasoningDelta"; readonly payload: ReasoningDeltaPayload }
  | { readonly type: "ToolParamsStart"; readonly payload: ToolParamsStartPayload }
  | { readonly type: "ToolParamsDelta"; readonly payload: ToolParamsDeltaPayload }
  | { readonly type: "ToolParamsEnd"; readonly payload: ToolParamsEndPayload };

/** Durable payloads emitted together when finish seals an inference. */
export type FinalizePayload =
  | { readonly type: "InferenceCompleted"; readonly payload: InferenceCompletedPayload }
  | { readonly type: "ToolCallCreated"; readonly payload: ToolCallCreatedPayload }
  | { readonly type: "ToolCallRejected"; readonly payload: ToolCallRejectedPayload };

/** Data-only emission interpreted by `InferenceRunner` after a transition. */
export type InferenceEmission =
  | { readonly kind: "ephemeral"; readonly event: EphemeralPayload }
  | { readonly kind: "finalize"; readonly events: ReadonlyArray<FinalizePayload> }
  | { readonly kind: "fail"; readonly payload: InferenceFailedPayload };

/** Result of one pure provider-part transition. */
export interface InferenceStepResult {
  readonly next: InferenceState;
  readonly emissions: ReadonlyArray<InferenceEmission>;
}

/** Initial empty fold state for a newly committed inference. */
export const initialInferenceState = (identity: InferenceIdentity): InferenceState => ({
  ...identity,
  phase: "streaming",
  assistantText: "",
  reasoningText: "",
  responseMetadata: Option.none(),
  drafts: new Map(),
  decisions: [],
});

/** Pure transition for one provider stream part. */
export const step = (state: InferenceState, input: InferenceStepInput): InferenceStepResult => {
  if (state.phase === "sealed") {
    return { next: state, emissions: [] };
  }

  switch (input.type) {
    case "text-start":
    case "text-end":
    case "reasoning-start":
    case "reasoning-end":
      return { next: state, emissions: [] };
    case "text-delta":
      return recordTextDelta(state, input);
    case "reasoning-delta":
      return recordReasoningDelta(state, input);
    case "tool-params-start":
      return recordToolParamsStart(state, input);
    case "tool-params-delta":
      return recordToolParamsDelta(state, input);
    case "tool-params-end":
      return recordToolParamsEnd(state, input);
    case "tool-call":
      return recordToolCall(state, input);
    case "response-metadata":
      return {
        next: { ...state, responseMetadata: Option.some(input.responseMetadata) },
        emissions: [],
      };
    case "finish":
      return finalizeInference(state, input);
    case "error":
      return failInference(state, input.error);
    default:
      return assertNever(input, "inference step input");
  }
};

const recordTextDelta = (
  state: InferenceState,
  input: Extract<InferenceStepInput, { readonly type: "text-delta" }>,
): InferenceStepResult => {
  const next = { ...state, assistantText: `${state.assistantText}${input.delta}` };
  return {
    next,
    emissions: [
      {
        kind: "ephemeral",
        event: {
          type: "TextDelta",
          payload: { providerPartId: input.providerPartId, delta: input.delta },
        },
      },
    ],
  };
};

const recordReasoningDelta = (
  state: InferenceState,
  input: Extract<InferenceStepInput, { readonly type: "reasoning-delta" }>,
): InferenceStepResult => {
  const next = { ...state, reasoningText: `${state.reasoningText}${input.delta}` };
  return {
    next,
    emissions: [
      {
        kind: "ephemeral",
        event: {
          type: "ReasoningDelta",
          payload: { providerPartId: input.providerPartId, delta: input.delta },
        },
      },
    ],
  };
};

const recordToolParamsStart = (
  state: InferenceState,
  input: Extract<InferenceStepInput, { readonly type: "tool-params-start" }>,
): InferenceStepResult => {
  if (state.drafts.has(input.providerPartId)) {
    return { next: state, emissions: [] };
  }

  const draft: ToolDraft = {
    runId: state.runId,
    turnId: state.turnId,
    inferenceId: state.inferenceId,
    providerPartId: input.providerPartId,
    toolCallId: input.toolCallId,
    toolName: input.toolName,
    providerExecuted: input.providerExecuted,
    paramsText: "",
    status: "streaming",
  };
  const next = { ...state, drafts: new Map(state.drafts).set(input.providerPartId, draft) };
  return {
    next,
    emissions: [
      {
        kind: "ephemeral",
        event: {
          type: "ToolParamsStart",
          payload: {
            providerPartId: input.providerPartId,
            toolCallId: input.toolCallId,
            toolName: input.toolName,
            providerExecuted: input.providerExecuted,
          },
        },
      },
    ],
  };
};

const recordToolParamsDelta = (
  state: InferenceState,
  input: Extract<InferenceStepInput, { readonly type: "tool-params-delta" }>,
): InferenceStepResult => {
  const draft = state.drafts.get(input.providerPartId);
  if (draft === undefined || draft.status !== "streaming") {
    return { next: state, emissions: [] };
  }

  const nextDraft = { ...draft, paramsText: `${draft.paramsText}${input.delta}` };
  const next = { ...state, drafts: new Map(state.drafts).set(input.providerPartId, nextDraft) };
  return {
    next,
    emissions: [
      {
        kind: "ephemeral",
        event: {
          type: "ToolParamsDelta",
          payload: {
            providerPartId: input.providerPartId,
            toolCallId: draft.toolCallId,
            delta: input.delta,
          },
        },
      },
    ],
  };
};

const recordToolParamsEnd = (
  state: InferenceState,
  input: Extract<InferenceStepInput, { readonly type: "tool-params-end" }>,
): InferenceStepResult => {
  const draft = state.drafts.get(input.providerPartId);
  if (draft === undefined || draft.status !== "streaming") {
    return { next: state, emissions: [] };
  }

  const nextDraft = { ...draft, status: "ended" as const };
  const next = { ...state, drafts: new Map(state.drafts).set(input.providerPartId, nextDraft) };
  return {
    next,
    emissions: [
      {
        kind: "ephemeral",
        event: {
          type: "ToolParamsEnd",
          payload: { providerPartId: input.providerPartId, toolCallId: draft.toolCallId },
        },
      },
    ],
  };
};

const recordToolCall = (
  state: InferenceState,
  input: Extract<InferenceStepInput, { readonly type: "tool-call" }>,
): InferenceStepResult => {
  const draft = state.drafts.get(input.providerPartId);
  const toolCallId = draft?.toolCallId ?? input.toolCallId;
  const decision = makeToolCallDecision(state, input, toolCallId);
  const nextDraft = draft === undefined ? undefined : markDraft(draft, decision);
  const nextDrafts =
    nextDraft === undefined
      ? state.drafts
      : new Map(state.drafts).set(input.providerPartId, nextDraft);
  return {
    next: {
      ...state,
      drafts: nextDrafts,
      decisions: upsertDecision(state.decisions, decision),
    },
    emissions: [],
  };
};

const makeToolCallDecision = (
  state: InferenceState,
  input: Extract<InferenceStepInput, { readonly type: "tool-call" }>,
  toolCallId: ToolCallId,
): ToolCallDecision => {
  const fields = {
    runId: state.runId,
    turnId: state.turnId,
    inferenceId: state.inferenceId,
    providerPartId: input.providerPartId,
    toolCallId,
    toolName: input.toolName,
    params: input.params,
    providerExecuted: input.providerExecuted,
  };

  switch (input.validation._tag) {
    case "ValidToolParams":
      return { ...fields, _tag: "ToolCallCreatedDecision", params: input.validation.params };
    case "UnknownTool":
      return rejectToolCall(fields, "unknown-tool", `Unknown tool: ${input.toolName}`);
    case "InvalidToolParams":
      return rejectToolCall(fields, "invalid-params", input.validation.message);
    default:
      return assertNever(input.validation, "tool validation");
  }
};

const rejectToolCall = (
  fields: ToolDecisionFields,
  reason: ToolCallRejectedDecision["reason"],
  message: string,
): ToolCallRejectedDecision => ({
  ...fields,
  _tag: "ToolCallRejectedDecision",
  reason,
  error: { message },
  modelFeedback: `Tool ${fields.toolName} arguments were rejected: ${message}`,
});

const markDraft = (draft: ToolDraft, decision: ToolCallDecision): ToolDraft => ({
  ...draft,
  status: decision._tag === "ToolCallCreatedDecision" ? "staged-created" : "staged-rejected",
});

const upsertDecision = (
  decisions: ReadonlyArray<ToolCallDecision>,
  decision: ToolCallDecision,
): ReadonlyArray<ToolCallDecision> => [
  ...decisions.filter((existing) => existing.providerPartId !== decision.providerPartId),
  decision,
];

const finalizeInference = (
  state: InferenceState,
  input: Extract<InferenceStepInput, { readonly type: "finish" }>,
): InferenceStepResult => ({
  next: { ...state, phase: "sealed" },
  emissions: [
    {
      kind: "finalize",
      events: [
        ...state.decisions.flatMap(decisionToPayloads),
        {
          type: "InferenceCompleted",
          payload: {
            runId: state.runId,
            turnId: state.turnId,
            inferenceId: state.inferenceId,
            ...(input.finishReason === undefined ? {} : { finishReason: input.finishReason }),
            ...(input.usage === undefined ? {} : { usage: input.usage }),
            ...(Option.isNone(state.responseMetadata)
              ? {}
              : { responseMetadata: state.responseMetadata.value }),
            ...(input.finishMetadata === undefined ? {} : { finishMetadata: input.finishMetadata }),
          },
        },
      ],
    },
  ],
});

const failInference = (state: InferenceState, error: FailurePayload): InferenceStepResult => ({
  next: { ...state, phase: "sealed" },
  emissions: [
    {
      kind: "fail",
      payload: {
        runId: state.runId,
        turnId: state.turnId,
        inferenceId: state.inferenceId,
        error,
      },
    },
  ],
});

const decisionToPayloads = (decision: ToolCallDecision): ReadonlyArray<FinalizePayload> => {
  const payload = {
    runId: decision.runId,
    turnId: decision.turnId,
    inferenceId: decision.inferenceId,
    toolCallId: decision.toolCallId,
    promptPart: toolCallPromptPart(decision),
  };

  if (decision._tag === "ToolCallRejectedDecision") {
    return [
      {
        type: "ToolCallRejected",
        payload: { ...payload, promptPart: toolRejectedPromptPart(decision) },
      },
    ];
  }

  return [{ type: "ToolCallCreated", payload }];
};

const toolCallPromptPart = (decision: ToolCallDecision): ToolCallCreatedPayload["promptPart"] => ({
  ...Prompt.toolCallPart({
    id: decision.providerPartId,
    name: decision.toolName,
    params: decision.params,
    providerExecuted: decision.providerExecuted,
  }),
  id: decision.providerPartId,
  name: decision.toolName,
});

const toolRejectedPromptPart = (
  decision: ToolCallRejectedDecision,
): ToolCallRejectedPayload["promptPart"] => ({
  ...Prompt.toolResultPart({
    id: decision.providerPartId,
    name: decision.toolName,
    isFailure: true,
    result: {
      message: decision.error.message,
      reason: decision.reason,
      modelFeedback: decision.modelFeedback,
    },
  }),
  id: decision.providerPartId,
  name: decision.toolName,
  isFailure: true,
  result: {
    message: decision.error.message,
    reason: decision.reason,
    modelFeedback: decision.modelFeedback,
  },
});
