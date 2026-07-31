import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import { EventId, SessionId, ToolCallId } from "../types/core";
import { DurableEventEnvelope, ToolName } from "../types/events";
import type { CommittedDurableEvent, EDASessionStoreError } from "./session-store";

/** Runtime parameter schema associated with a provider-visible tool name. */
export type ToolParamsSchema = Schema.Top;

/** Provider-visible Effect AI toolkit with handlers available for local execution. */
export type EDAModelToolkit = Toolkit.WithHandler<Record<string, Tool.Any>>;

/** Framework context passed to product tool handlers without model-visible params. */
export interface EDAToolExecutionContext {
  /** Framework-owned tool-call identity for this execution. */
  readonly toolCallId: ToolCallId;
  /** Session identity fixed for this runtime instance. */
  readonly sessionId: SessionId;
  /** Mint an app durable event id for tool-emitted facts. */
  readonly makeEventId: () => Effect.Effect<EventId>;
  /** Emit an app durable event through `SessionState`, never directly through the store. */
  readonly emitDurable: (
    event: DurableEventEnvelope,
  ) => Effect.Effect<CommittedDurableEvent, EDASessionStoreError>;
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
