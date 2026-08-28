import type * as Effect from "effect/Effect";

import type { EDASessionSnapshot } from "effect-durable-agent/services/session-query";
import type {
  EDAWebSocketClientFrame,
  EDAWebSocketServerFrameInput,
} from "effect-durable-agent/websocket";
import type { SequenceNumber } from "effect-durable-agent/types/core";

/** Initial state and cursor selected when an app-projected WebSocket is accepted. */
export interface EDAWebSocketProjectionInitial<State extends object> {
  readonly afterSeq: SequenceNumber;
  readonly state: State;
}

/** One encoded app frame and the projection state that produced it. */
export interface EDAWebSocketProjectionEncoded<State extends object> {
  readonly frame: string;
  readonly state: State;
}

/**
 * App-owned wire projection hosted directly by the EDA Durable Object.
 *
 * The host persists `State` beside EDA delivery state, so an app can retain a
 * stable public protocol without placing a resident WebSocket bridge in front
 * of the hibernating object.
 */
export interface EDAWebSocketProjection<State extends object> {
  /** Stable identifier persisted in WebSocket attachments. */
  readonly id: string;
  /** Decode one app-protocol client message into EDA's delivery ACK protocol. */
  readonly decodeClientMessage: (
    message: string,
  ) => Effect.Effect<EDAWebSocketClientFrame, unknown>;
  /** Restore app-owned connection state after isolate eviction. */
  readonly decodeState: (encoded: unknown) => Effect.Effect<State, unknown>;
  /** Encode app-owned connection state for Cloudflare's attachment boundary. */
  readonly encodeState: (state: State) => unknown;
  /** Project one EDA server frame while updating app-owned connection state. */
  readonly encodeServerFrame: (
    frame: EDAWebSocketServerFrameInput,
    state: State,
  ) => EDAWebSocketProjectionEncoded<State>;
  /** Seed the projection and select its default replay cursor from a durable snapshot. */
  readonly initialize: (input: {
    readonly requestedAfterSeq?: SequenceNumber;
    readonly snapshot: EDASessionSnapshot;
  }) => EDAWebSocketProjectionInitial<State>;
}
