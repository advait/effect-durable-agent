import { EDASessionDurableObject } from "../../packages/cloudflare/dist/index.js";

import { conformanceHostOptions, conformanceWorker } from "./fixture";

export class EDAConformanceSession extends EDASessionDurableObject {
  constructor(ctx: DurableObjectState, env: object) {
    super(ctx, env, conformanceHostOptions());
  }
}

export default conformanceWorker;
