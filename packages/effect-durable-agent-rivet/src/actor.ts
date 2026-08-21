import * as Schema from "effect/Schema";
import { actor } from "rivetkit";
import { db } from "rivetkit/db";

import type { DurableTranscriptMessage } from "effect-durable-agent/domain/message-transcript";
import type { EDASessionSnapshot } from "effect-durable-agent/services/session-query";
import {
  CommittedDurableEvent,
  type CommittedDurableEvent as CommittedDurableEventValue,
} from "effect-durable-agent/services/session-store";
import type { EDASubmittable } from "effect-durable-agent/services/session-state";
import type { CommittedCommandTerminalEvent } from "effect-durable-agent/services/runtime";
import {
  EDACommand,
  type EDACommand as EDACommandValue,
} from "effect-durable-agent/types/commands";
import { CommandId, SequenceNumber, SessionId } from "effect-durable-agent/types/core";
import {
  DurableEventEnvelope,
  EDADurableEvent,
  decodeUnknownEDADurableEventSync,
  effectDurableAgentNamespace,
} from "effect-durable-agent/types/events";
import { EDATraceMetadata, makeRootEDATraceMetadata } from "effect-durable-agent/types/tracing";
import { EDAWebSocketAttachment, SubscriberId } from "effect-durable-agent/host/websocket-protocol";
import {
  EDASessionRivetHost,
  type EDARivetWebSocketState,
  type EDASessionRivetHostOptions,
} from "./runtime";

/** Raw action input decoded at the Rivet transport boundary. */
export interface EDARivetCommandActionInput {
  readonly command: unknown;
  readonly trace?: unknown;
}

/** Raw batch action input decoded at the Rivet transport boundary. */
export interface EDARivetBatchActionInput {
  readonly items: ReadonlyArray<unknown>;
  readonly trace?: unknown;
}

/** Raw wait action input decoded at the Rivet transport boundary. */
export interface EDARivetBlockActionInput {
  readonly afterSeq?: number;
  readonly commandId: string;
  readonly trace?: unknown;
}

/** Optional action metadata for scoped reads and destruction. */
export interface EDARivetScopedActionInput {
  readonly trace?: unknown;
}

/** Connection params accepted when opening the EDA raw WebSocket. */
export interface EDARivetConnectionParams {
  readonly afterSeq?: number;
  readonly trace?: unknown;
}

/** Context supplied to an app-owned Rivet connection authorization hook. */
export interface EDARivetAuthorizationContext {
  readonly params: unknown;
  readonly request?: Request;
  readonly sessionId: SessionId;
}

/** Reject the returned promise (or throw) to deny all actions and WebSockets on a connection. */
export type EDARivetAuthorize = (context: EDARivetAuthorizationContext) => void | Promise<void>;

/** App-owned options, excluding actor-owned database and keep-awake wiring. */
export type CreateEDASessionRivetActorOptions = Omit<
  EDASessionRivetHostOptions,
  "keepAwake" | "storage"
> & {
  /** Rivet action timeout; defaults to an effectively unbounded safe integer for blocking APIs. */
  readonly actionTimeoutMs?: number;
  readonly authorize?: EDARivetAuthorize;
};

const EDARivetSubmittable = Schema.Union([EDACommand, DurableEventEnvelope]);

/** Encode a domain command for the Rivet action transport. */
export const encodeEDARivetCommand = (command: EDACommandValue): unknown =>
  Schema.encodeSync(EDACommand)(command);

/** Encode a command/app-event batch for the Rivet action transport. */
export const encodeEDARivetSubmittables = (
  items: ReadonlyArray<EDASubmittable>,
): ReadonlyArray<unknown> => items.map((item) => Schema.encodeSync(EDARivetSubmittable)(item));

/** Decode one committed event returned across the Rivet action transport. */
export const decodeEDARivetCommittedEvent = (input: unknown): CommittedDurableEventValue =>
  decodeCommittedEvent(input);

/**
 * Create a native Rivet Actor definition for EDA sessions.
 *
 * One stable actor key maps to one EDA `SessionId`. Rivet owns placement,
 * actor-local SQLite, lifecycle, and transport; core EDA owns all agent semantics.
 */
export const createEDASessionRivetActor = (options: CreateEDASessionRivetActorOptions) => {
  const { actionTimeoutMs = Number.MAX_SAFE_INTEGER, authorize, ...hostOptions } = options;
  return actor({
    options: {
      actionTimeout: actionTimeoutMs,
      canHibernateWebSocket: true,
    },
    createState: () => ({}),
    createVars: (c) => ({
      host: new EDASessionRivetHost({
        ...hostOptions,
        keepAwake: (promise) => c.keepAwake(promise),
        storage: c.db,
      }),
    }),
    createConnState: (c, params: unknown): EDARivetWebSocketState => {
      const decoded = decodeConnectionParams(params);
      return EDAWebSocketAttachment.make({
        kind: "eda-events-v1",
        sessionId: sessionIdFromActorKey(c.key),
        subscriberId: SubscriberId.make(crypto.randomUUID()),
        lastAckedSeq: SequenceNumber.make(decoded.afterSeq ?? 0),
        trace: decodeTrace(decoded.trace),
      });
    },
    db: db(),
    onBeforeConnect:
      authorize === undefined
        ? undefined
        : async (c, params: unknown) => {
            await authorize({
              params,
              ...(c.request === undefined ? {} : { request: c.request }),
              sessionId: sessionIdFromActorKey(c.key),
            });
          },
    onWake: async (c) => {
      await c.vars.host.wake(sessionIdFromActorKey(c.key));
    },
    onSleep: async (c) => {
      await c.vars.host.dispose();
    },
    onDestroy: async (c) => {
      await c.vars.host.dispose();
    },
    onWebSocket: async (c, webSocket) => {
      await c.vars.host.acceptEventWebSocket({
        attachment: c.conn.state,
        persistAck: (attachment) => {
          c.conn.state = attachment;
        },
        webSocket,
      });
    },
    actions: {
      submit: async (c, input: EDARivetCommandActionInput): Promise<unknown> =>
        encodeCommitted(
          await c.vars.host.submit({
            command: decodeCommand(input.command),
            sessionId: sessionIdFromActorKey(c.key),
            trace: decodeTrace(input.trace),
          }),
        ),
      submitBatch: async (c, input: EDARivetBatchActionInput): Promise<ReadonlyArray<unknown>> => {
        const committed = await c.vars.host.submitBatch({
          items: decodeSubmittables(input.items),
          sessionId: sessionIdFromActorKey(c.key),
          trace: decodeTrace(input.trace),
        });
        return committed.map(encodeCommitted);
      },
      submitAndBlock: async (c, input: EDARivetCommandActionInput): Promise<unknown> =>
        encodeCommitted(
          await c.vars.host.submitAndBlock({
            command: decodeCommand(input.command),
            sessionId: sessionIdFromActorKey(c.key),
            trace: decodeTrace(input.trace),
          }),
        ),
      blockOnCommand: async (c, input: EDARivetBlockActionInput): Promise<unknown> =>
        encodeCommitted(
          await c.vars.host.blockOnCommand({
            ...(input.afterSeq === undefined
              ? {}
              : { afterSeq: SequenceNumber.make(input.afterSeq) }),
            commandId: CommandId.make(input.commandId),
            sessionId: sessionIdFromActorKey(c.key),
            trace: decodeTrace(input.trace),
          }),
        ),
      snapshot: async (c, input: EDARivetScopedActionInput = {}): Promise<EDASessionSnapshot> =>
        encodeSnapshot(
          await c.vars.host.snapshot({
            sessionId: sessionIdFromActorKey(c.key),
            trace: decodeTrace(input.trace),
          }),
        ),
      messages: async (
        c,
        input: EDARivetScopedActionInput = {},
      ): Promise<ReadonlyArray<DurableTranscriptMessage>> =>
        c.vars.host.messages({
          sessionId: sessionIdFromActorKey(c.key),
          trace: decodeTrace(input.trace),
        }),
      destroySession: async (c, input: EDARivetScopedActionInput = {}): Promise<void> => {
        await c.vars.host.destroy({
          sessionId: sessionIdFromActorKey(c.key),
          trace: decodeTrace(input.trace),
        });
      },
    },
  });
};

const sessionIdFromActorKey = (key: ReadonlyArray<string>): SessionId => {
  if (key.length !== 1 || key[0] === undefined) {
    throw new Error("EDA Rivet Actors require exactly one SessionId actor key component");
  }
  return SessionId.make(key[0]);
};

const decodeConnectionParams = (input: unknown): EDARivetConnectionParams => {
  if (input === undefined || input === null) {
    return {};
  }
  const schema = Schema.Struct({
    afterSeq: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
    trace: Schema.optionalKey(Schema.Unknown),
  });
  return Schema.decodeUnknownSync(schema)(input);
};

const decodeTrace = (input: unknown): EDATraceMetadata =>
  input === undefined
    ? makeRootEDATraceMetadata()
    : Schema.decodeUnknownSync(EDATraceMetadata)(input);

const decodeCommand = (input: unknown): EDACommandValue =>
  Schema.decodeUnknownSync(EDACommand)(input);

const decodeSubmittables = (input: ReadonlyArray<unknown>): ReadonlyArray<EDASubmittable> =>
  input.map((item) => Schema.decodeUnknownSync(EDARivetSubmittable)(item));

const decodeCommittedEvent = (input: unknown): CommittedDurableEventValue => {
  const committed = Schema.decodeUnknownSync(CommittedDurableEvent)(input);
  return CommittedDurableEvent.make({
    ...committed,
    event:
      committed.event.namespace === effectDurableAgentNamespace
        ? decodeUnknownEDADurableEventSync(committed.event)
        : committed.event,
  });
};

const encodeCommitted = (
  committed: CommittedDurableEventValue | CommittedCommandTerminalEvent,
): unknown => ({
  position: committed.position,
  event:
    committed.event.namespace === effectDurableAgentNamespace
      ? Schema.encodeSync(EDADurableEvent)(
          Schema.decodeUnknownSync(EDADurableEvent)(committed.event),
        )
      : Schema.encodeSync(DurableEventEnvelope)(committed.event),
});

const encodeSnapshot = (snapshot: EDASessionSnapshot): EDASessionSnapshot => ({
  ...snapshot,
  reducerStates: new Map(snapshot.reducerStates),
  state: {
    ...snapshot.state,
    commands: new Map(
      Array.from(snapshot.state.commands, ([commandId, record]) => [
        commandId,
        encodeCommandCarrier(record),
      ]),
    ),
    commandQueues: {
      ...snapshot.state.commandQueues,
      activeControlCommands:
        snapshot.state.commandQueues.activeControlCommands.map(encodeCommandCarrier),
      pendingCommands: snapshot.state.commandQueues.pendingCommands.map(encodeCommandCarrier),
      queuedCommands: snapshot.state.commandQueues.queuedCommands.map(encodeCommandCarrier),
    },
  },
});

const encodeCommandCarrier = <A extends { readonly command?: EDACommandValue }>(value: A): A =>
  value.command === undefined
    ? value
    : {
        ...value,
        // SAFETY: the transport value is intentionally encoded but preserves the
        // command's structural fields; consumers decode at the package boundary.
        command: Schema.encodeSync(EDACommand)(value.command) as EDACommandValue,
      };
