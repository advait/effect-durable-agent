import { DurableObject } from "cloudflare:workers";
import type { EDAWebSocketProjection } from "./durable-object";
import { EDASessionDurableObject, getEDASessionDurableObjectByName } from "./durable-object";

interface TestProjectionState {
  readonly cursor: number;
}

declare class ProjectedSession extends EDASessionDurableObject<object, TestProjectionState> {
  readonly projection: EDAWebSocketProjection<TestProjectionState>;
  productRpc(): Promise<string>;
}

declare const namespace: DurableObjectNamespace<ProjectedSession>;

const stub = getEDASessionDurableObjectByName(namespace, "session-id");

const productRpcResult: Promise<string> = stub.productRpc();
void productRpcResult;

declare class UnrelatedDurableObject extends DurableObject<object> {
  unrelatedRpc(): Promise<string>;
}

declare const unrelatedNamespace: DurableObjectNamespace<UnrelatedDurableObject>;

// @ts-expect-error An unrelated namespace must not satisfy the EDA session helper contract.
getEDASessionDurableObjectByName(unrelatedNamespace, "session-id");
