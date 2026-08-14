import { EDASessionDurableObject } from "../../dist/index.js";

import {
  conformanceHostOptions,
  conformanceWorker,
} from "../../../../testing/host-conformance/fixture";

export class EDAConformanceSession extends EDASessionDurableObject {
  constructor(ctx: DurableObjectState, env: object) {
    super(ctx, env, conformanceHostOptions());
  }
}

export default conformanceWorker;
