import { DurableObject } from "cloudflare:workers";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { EDAWebSocketClientFrame } from "effect-durable-agent/websocket";
import {
  EDASessionDurableObject,
  EDAWebSocketProjectionId,
  type EDAWebSocketProjection,
} from "../../dist/index.js";

import {
  conformanceHostOptions,
  conformanceRuntimeBuilds,
  conformanceWorker,
} from "../../../../testing/host-conformance/fixture";
import {
  runTranscriptProbe,
  type TranscriptProbeResult,
} from "../../../../testing/host-conformance/transcript-probe";

const ProjectionState = Schema.Struct({ frameCount: Schema.Int });
type ProjectionState = typeof ProjectionState.Type;

const ProjectedAck = Schema.Struct({
  durableThroughSeq: Schema.Int,
  frameId: Schema.Int,
  type: Schema.Literal("ack"),
});

const conformanceProjection: EDAWebSocketProjection<ProjectionState> = {
  id: EDAWebSocketProjectionId.make("conformance-projection-v1"),
  decodeClientMessage: (message) =>
    Effect.try({
      try: () => JSON.parse(message) as unknown,
      catch: (error) => error,
    }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(ProjectedAck)),
      Effect.flatMap((frame) =>
        Schema.decodeUnknownEffect(EDAWebSocketClientFrame)({
          _tag: "ack",
          durableThroughSeq: frame.durableThroughSeq,
          frameId: frame.frameId,
        }),
      ),
    ),
  decodeState: Schema.decodeUnknownEffect(ProjectionState),
  encodeState: Schema.encodeUnknownSync(ProjectionState),
  encodeServerFrame: (frame, state) => ({
    _tag: "Send",
    frame: JSON.stringify({ ...frame, _tag: undefined, type: frame._tag }),
    state: { frameCount: state.frameCount + 1 },
  }),
  initialize: ({ requestedAfterSeq, snapshot }) => ({
    afterSeq: requestedAfterSeq ?? snapshot.state.lastSeq,
    state: { frameCount: 0 },
  }),
};

export class EDAConformanceSession extends EDASessionDurableObject<object, ProjectionState> {
  constructor(ctx: DurableObjectState, env: object) {
    super(ctx, env, { ...conformanceHostOptions(), webSocketProjection: conformanceProjection });
  }

  async runtimeBuilds(): Promise<number> {
    return conformanceRuntimeBuilds.count;
  }
}

/** Runs storage-level transcript scenarios against this object's real storage. */
export class EDATranscriptProbe extends DurableObject {
  async probe(step: string): Promise<TranscriptProbeResult> {
    return runTranscriptProbe(this.ctx.storage, step);
  }
}

export default conformanceWorker;
