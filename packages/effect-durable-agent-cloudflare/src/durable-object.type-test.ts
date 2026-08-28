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
