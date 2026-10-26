import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";

import { EventId, SequenceNumber, SessionId, ToolCallId, RunId, TurnId } from "../types/core";
import { DurableEventEnvelope, ToolName } from "../types/events";
import type { CommittedDurableEvent, EDASessionStoreError } from "./session-store";

/**
 * What startup recovery does with a call to this tool that had started when the runtime died.
 *
 * - `fail` (default): record the call as failed; the model sees the interruption and decides.
 * - `reinvoke`: run the handler again with the same tool-call id and params, passing
 *   {@link EDAToolExecutionContext.recovery}. Declare it only when a second invocation of the
 *   same call is safe: it must attach to, or return the outcome of, the original execution
 *   rather than repeat its side effects (for example an idempotency key derived from the
 *   tool-call id). Recovery re-invokes at most {@link maxToolCallAttempts} times in total.
 */
export type ToolRecoveryPolicy = "fail" | "reinvoke";

/** Effect AI tool annotation declaring a tool's {@link ToolRecoveryPolicy}. */
export const ToolRecovery = Context.Reference<ToolRecoveryPolicy>(
  "effect-durable-agent/ToolRecovery",
  { defaultValue: () => "fail" },
);

/** Total execution attempts, including the first, that recovery allows one tool call. */
export const maxToolCallAttempts = 3;

/** The recovery policy a toolkit declares for `toolName`; `fail` for unknown tools. */
export const toolRecoveryPolicy = (
  toolkit: EDAModelToolkit | undefined,
  toolName: string,
): ToolRecoveryPolicy => {
  const tool = toolkit?.tools[toolName];
  return tool === undefined ? "fail" : Context.get(tool.annotations, ToolRecovery);
};

/** Runtime parameter schema associated with a provider-visible tool name. */
export type ToolParamsSchema = Schema.Top;

/** Provider-visible Effect AI toolkit with handlers available for local execution. */
export type EDAModelToolkit = Toolkit.WithHandler<Record<string, Tool.Any>>;

/** Framework context passed to product tool handlers without model-visible params. */
export interface EDAToolExecutionContext {
  /** Framework-owned tool-call identity for this execution. */
  readonly toolCallId: ToolCallId;
  /** Run and turn that durably created this tool call. */
  readonly runId: RunId;
  readonly turnId: TurnId;
  /** Read typed facts from this session journal, optionally only those after `afterSeq`. */
  readonly queryEvents: <E extends DurableEventEnvelope>(
    schema: Schema.Codec<E, unknown, never, never>,
    options?: { readonly limit?: number; readonly afterSeq?: SequenceNumber },
  ) => Effect.Effect<ReadonlyArray<CommittedDurableEvent<E>>, EDASessionStoreError>;
  /** Session identity fixed for this runtime instance. */
  readonly sessionId: SessionId;
  /** Mint an app durable event id for tool-emitted facts. */
  readonly makeEventId: () => Effect.Effect<EventId>;
  /** Emit an app durable event through `SessionState`, never directly through the store. */
  readonly emitDurable: (
    event: DurableEventEnvelope,
  ) => Effect.Effect<CommittedDurableEvent, EDASessionStoreError>;
  /**
   * Present when startup recovery re-invokes a call that had started before the runtime died
   * (its tool declares `ToolRecovery: "reinvoke"`). Facts the first attempt committed are in the
   * journal after `firstStartedSeq`.
   */
  readonly recovery?: {
    /** This attempt, 2 or more. */
    readonly attempt: number;
    readonly firstStartedAtMs: number;
    readonly firstStartedSeq: SequenceNumber;
  };
}

/** Registry contract used by model streaming and post-finish tool execution. */
export interface EDAToolRegistryShape {
  /** Look up the parameter schema for a registered tool name. */
  readonly getParamsSchema: (toolName: ToolName) => Effect.Effect<ToolParamsSchema | undefined>;
  /** Build provider-visible tool definitions while keeping EDA in charge of lifecycle events. */
  readonly getModelToolkit: () => Effect.Effect<EDAModelToolkit | undefined>;
  /** Execute a tool. EDA records the durable lifecycle around this effect. */
  readonly execute: (
    toolName: ToolName,
    params: unknown,
    context: EDAToolExecutionContext,
  ) => Effect.Effect<unknown, unknown>;
}

/** Build a handler-backed Effect Toolkit from tools and handlers. */
export const makeEDAToolkit = <const Tools extends ReadonlyArray<Tool.Any>>(
  tools: Tools,
  handlers: Toolkit.HandlersFrom<Toolkit.ToolsByName<Tools>>,
): Effect.Effect<EDAModelToolkit> => {
  const toolkit = Toolkit.make(...tools);
  return toolkit.pipe(
    Effect.provide(toolkit.toLayer(handlers)),
    Effect.map((withHandlers) => withHandlers as unknown as EDAModelToolkit),
  );
};

const makeToolRegistry = (toolkit: EDAModelToolkit | undefined): EDAToolRegistryShape => ({
  getParamsSchema: (toolName) => Effect.succeed(toolkit?.tools[toolName]?.parametersSchema),
  getModelToolkit: () => Effect.succeed(toolkit),
  execute: (toolName, params, _context) =>
    Effect.gen(function* () {
      if (toolkit === undefined || toolkit.tools[toolName] === undefined) {
        return yield* Effect.fail(new Error(`No handler registered for tool ${toolName}`));
      }

      const results = yield* (yield* toolkit.handle(toolName, params)).pipe(Stream.runCollect);
      const final = Array.from(results)
        .filter((result) => !result.preliminary)
        .at(-1);
      if (final === undefined) {
        return yield* Effect.fail(new Error(`Tool ${toolName} completed without a final result`));
      }
      if (final.isFailure) {
        return yield* Effect.fail(final.result);
      }
      return final.encodedResult;
    }) as Effect.Effect<unknown, unknown>,
});

const toolkitFromSchemas = (
  schemas: ReadonlyMap<string, ToolParamsSchema>,
): EDAModelToolkit | undefined => {
  if (schemas.size === 0) {
    return undefined;
  }

  const tools = Object.fromEntries(
    Array.from(schemas.entries()).map(([name, schema]) => [
      name,
      Tool.dynamic(name, { parameters: schema }),
    ]),
  );

  return {
    tools,
    handle: ((name: string) =>
      Effect.die(
        new Error(`No test handler registered for schema-only tool ${name}`),
      )) as unknown as EDAModelToolkit["handle"],
  };
};

/** Registry of Effect AI tools available to the model and EDA lifecycle executor. */
export class EDAToolRegistry extends Context.Service<EDAToolRegistry, EDAToolRegistryShape>()(
  "@effect-durable-agent/EDAToolRegistry",
) {
  static readonly Empty = Layer.succeed(EDAToolRegistry, makeToolRegistry(undefined));

  static readonly FromToolkit = (toolkit: EDAModelToolkit) =>
    Layer.succeed(EDAToolRegistry, makeToolRegistry(toolkit));

  static readonly FromToolkitEffect = <E, R>(toolkit: Effect.Effect<EDAModelToolkit, E, R>) =>
    Layer.effect(
      EDAToolRegistry,
      toolkit.pipe(Effect.map((withHandlers) => makeToolRegistry(withHandlers))),
    );

  static readonly FromShape = (shape: EDAToolRegistryShape) =>
    Layer.succeed(EDAToolRegistry, shape);

  static readonly FromShapeEffect = <E, R>(shape: Effect.Effect<EDAToolRegistryShape, E, R>) =>
    Layer.effect(EDAToolRegistry, shape);

  /** Test/convenience schema-only tools. Runtime execution will fail unless a real Toolkit is provided. */
  static readonly FromSchemas = (schemas: ReadonlyMap<string, ToolParamsSchema>) =>
    Layer.succeed(EDAToolRegistry, makeToolRegistry(toolkitFromSchemas(schemas)));
}
