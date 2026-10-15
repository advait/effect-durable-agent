import { DurableObject } from "cloudflare:workers";
import { EDASessionCell } from "../../dist/index.js";

import {
  conformanceHostOptions,
  conformanceIncarnationKey,
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

  async kvIncarnation(): Promise<unknown> {
    return this.ctx.storage.kv.get(conformanceIncarnationKey) ?? null;
  }

  /**
   * Test-only: arm the platform alarm the way a release before recorded wakeups did (no wakeup
   * row), and leave builder work pending so the builder also wants the alarm.
   */
  async armUnrecordedAlarm(delayMs: number): Promise<void> {
    this.ctx.storage.sql.exec("DELETE FROM _eda_alarm_wakeups");
    this.ctx.storage.sql.exec(
      "INSERT INTO _eda_transcript_meta (version, role, cursor, status) VALUES (?, 'retired', 0, 'ok')",
      "0".repeat(64),
    );
    await this.ctx.storage.setAlarm(Date.now() + delayMs);
  }
}

/** Runs storage-level transcript scenarios against this object's real storage. */
export class EDATranscriptProbe extends DurableObject {
  async probe(step: string): Promise<TranscriptProbeResult> {
    return runTranscriptProbe(this.ctx.storage, step);
  }
}

export default conformanceWorker;
