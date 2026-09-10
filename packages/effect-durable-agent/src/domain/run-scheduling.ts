import * as Schema from "effect/Schema";

import type { CommandId, SessionId } from "../types/core";

/**
 * Identity of the admitted work selected by dispatch or recovery for a new run.
 * This is not a durable request identity: recovery can resolve the same command again.
 */
export interface RunSchedulingInput {
  readonly sessionId: SessionId;
  readonly commandId: CommandId;
}

/**
 * Permission to start the selected run in this session's current runtime.
 * Only immediate local execution is supported; this value is never persisted.
 */
export const LocalRunResolution = Schema.TaggedStruct("Local", {});
export type LocalRunResolution = typeof LocalRunResolution.Type;
