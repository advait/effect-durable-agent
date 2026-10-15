import { DurableObject } from "cloudflare:workers";
import { EDASessionCell } from "../../dist/index.js";

import {
  conformanceHostOptions,
  conformanceRuntimeBuilds,
  conformanceWorker,
} from "../../../../testing/host-conformance/fixture";
import {
  runTranscriptProbe,
  type TranscriptProbeResult,
} from "../../../../testing/host-conformance/transcript-probe";

export class EDAConformanceSession extends EDASessionCell {
  constructor(ctx: DurableObjectState, env: object) {
    super(ctx, env, conformanceHostOptions());
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
