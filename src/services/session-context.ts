import * as Context from "effect/Context";
import * as Layer from "effect/Layer";

import { SessionId } from "../types/core";

/** Runtime-scoped session identity dependency. */
export interface SessionContextShape {
  /** The one session this runtime instance owns; fixed for the runtime lifetime. */
  readonly sessionId: SessionId;
}

/**
 * Identity of the session a runtime instance is bound to. One runtime = one
 * session, so services read the session here instead of threading `sessionId`
 * through every input struct.
 */
export class SessionContext extends Context.Service<SessionContext, SessionContextShape>()(
  "@effect-durable-agent/SessionContext",
) {
  static readonly Live = (sessionId: SessionId) => Layer.succeed(SessionContext, { sessionId });
}
