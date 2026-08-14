import { EDASessionCell } from "../../packages/celld/dist/index.js";

import { conformanceHostOptions, conformanceWorker } from "./fixture";

export class EDAConformanceSession extends EDASessionCell {
  constructor(ctx: DurableObjectState, env: object) {
    super(ctx, env, conformanceHostOptions());
  }
}

export default conformanceWorker;
