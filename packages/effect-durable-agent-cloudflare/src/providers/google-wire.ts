/**
 * The subset of the Gemini API (`v1beta` `generateContent` and `streamGenerateContent`) that EDA's
 * Google adapter sends and reads. Requests are built by EDA, so they are plain types; responses are
 * parsed with these schemas at the HTTP boundary. Unknown response fields (such as the `p` padding
 * on streamed chunks) are dropped by decoding.
 */
import * as Schema from "effect/Schema";

/** `thinkingConfig.thinkingLevel` values the Gemini 4 models accept (`minimal` is rejected). */
export type GoogleThinkingLevel = "low" | "medium" | "high" | "xhigh";

/** `toolConfig.functionCallingConfig.mode`. */
export type GoogleFunctionCallingMode = "AUTO" | "ANY" | "NONE" | "VALIDATED";

/** One request part. Exactly one data field is set; `thought` and `thoughtSignature` annotate it. */
export interface GoogleRequestPart {
  readonly text?: string;
  readonly thought?: boolean;
  readonly thoughtSignature?: string;
  readonly inlineData?: { readonly mimeType: string; readonly data: string };
  readonly fileData?: { readonly mimeType: string; readonly fileUri: string };
  readonly functionCall?: { readonly name: string; readonly args: unknown; readonly id?: string };
  readonly functionResponse?: {
    readonly name: string;
    readonly response: Readonly<Record<string, unknown>>;
    readonly id?: string;
  };
}

/** One turn of `contents`. Tool results go back in `user` turns. */
export interface GoogleContent {
  readonly role: "user" | "model";
  readonly parts: ReadonlyArray<GoogleRequestPart>;
}

export interface GoogleFunctionDeclaration {
  readonly name: string;
  readonly description?: string;
  /** JSON Schema for the arguments object (mutually exclusive with OpenAPI `parameters`). */
  readonly parametersJsonSchema: unknown;
}

export interface GoogleGenerationConfig {
  readonly maxOutputTokens?: number;
  readonly thinkingConfig?: {
    readonly thinkingLevel?: GoogleThinkingLevel;
    /** Return thought summaries as parts with `thought: true`. */
    readonly includeThoughts?: boolean;
  };
  readonly responseMimeType?: string;
  readonly responseJsonSchema?: unknown;
}

export interface GoogleToolConfig {
  readonly functionCallingConfig?: {
    readonly mode?: GoogleFunctionCallingMode;
    readonly allowedFunctionNames?: ReadonlyArray<string>;
  };
}

/** `POST /v1beta/models/{model}:generateContent` (and `:streamGenerateContent`) body. */
export interface GoogleGenerateContentRequest {
  readonly systemInstruction?: { readonly parts: ReadonlyArray<{ readonly text: string }> };
  readonly tools?: ReadonlyArray<{
    readonly functionDeclarations: ReadonlyArray<GoogleFunctionDeclaration>;
  }>;
  readonly toolConfig?: GoogleToolConfig;
  readonly contents: ReadonlyArray<GoogleContent>;
  readonly generationConfig?: GoogleGenerationConfig;
}

const ResponsePart = Schema.Struct({
  text: Schema.optionalKey(Schema.String),
  thought: Schema.optionalKey(Schema.Boolean),
  thoughtSignature: Schema.optionalKey(Schema.String),
  functionCall: Schema.optionalKey(
    Schema.Struct({
      name: Schema.String,
      args: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
      id: Schema.optionalKey(Schema.String),
    }),
  ),
});
export type GoogleResponsePart = typeof ResponsePart.Type;

const UsageMetadata = Schema.Struct({
  promptTokenCount: Schema.optionalKey(Schema.Number),
  /** Prompt tokens read from the implicit cache; a subset of `promptTokenCount`. */
  cachedContentTokenCount: Schema.optionalKey(Schema.Number),
  candidatesTokenCount: Schema.optionalKey(Schema.Number),
  /** Thinking tokens, billed as output and not part of `candidatesTokenCount`. */
  thoughtsTokenCount: Schema.optionalKey(Schema.Number),
  toolUsePromptTokenCount: Schema.optionalKey(Schema.Number),
  totalTokenCount: Schema.optionalKey(Schema.Number),
});
export type GoogleUsageMetadata = typeof UsageMetadata.Type;

/**
 * A full response, and also each streamed SSE chunk: chunks carry new parts only, and
 * `usageMetadata` cumulatively, so the last chunk's usage is final.
 */
export const GoogleGenerateContentResponse = Schema.Struct({
  candidates: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        content: Schema.optionalKey(
          Schema.Struct({ parts: Schema.optionalKey(Schema.Array(ResponsePart)) }),
        ),
        finishReason: Schema.optionalKey(Schema.String),
        finishMessage: Schema.optionalKey(Schema.String),
      }),
    ),
  ),
  /** Set when the prompt itself was blocked; there are no candidates then. */
  promptFeedback: Schema.optionalKey(
    Schema.Struct({
      blockReason: Schema.optionalKey(Schema.String),
      blockReasonMessage: Schema.optionalKey(Schema.String),
    }),
  ),
  usageMetadata: Schema.optionalKey(UsageMetadata),
  modelVersion: Schema.optionalKey(Schema.String),
  responseId: Schema.optionalKey(Schema.String),
});
export type GoogleGenerateContentResponse = typeof GoogleGenerateContentResponse.Type;

/** Error body of a failed request, and of an error sent as an SSE event. */
export const GoogleErrorBody = Schema.Struct({
  error: Schema.Struct({
    code: Schema.optionalKey(Schema.Number),
    message: Schema.optionalKey(Schema.String),
    status: Schema.optionalKey(Schema.String),
  }),
});
export type GoogleErrorBody = typeof GoogleErrorBody.Type;

/** One streamed SSE `data` payload. */
export const GoogleStreamChunk = Schema.Union([GoogleErrorBody, GoogleGenerateContentResponse]);
