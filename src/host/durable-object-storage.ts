import type { SessionId } from "../types/core";

/**
 * Minimal synchronous SQL cursor surface used by Cloudflare Durable Object storage.
 *
 * This type is host-only on purpose: framework services should depend on
 * `EDASessionStore`, not Cloudflare's SQL API.
 */
export interface DurableObjectSqlCursor<Row> {
  readonly one: () => Row;
  readonly toArray: () => Array<Row>;
}

/** Minimal synchronous SQL executor surface used by Cloudflare Durable Object storage. */
export interface DurableObjectSqlStorage {
  readonly exec: <Row = Record<string, unknown>>(
    query: string,
    ...bindings: ReadonlyArray<unknown>
  ) => DurableObjectSqlCursor<Row>;
}

/**
 * Durable Object storage capabilities needed by EDA SQL adapters.
 *
 * The `transactionSync` callback must remain purely synchronous. Adapter code
 * may run SQL here; model calls, live publish, and Effect suspension must stay
 * outside the callback.
 */
export interface DurableObjectSessionStorage {
  readonly sql: DurableObjectSqlStorage;
  readonly transactionSync: <A>(closure: () => A) => A;
}

/**
 * Construction options for one session-scoped Durable Object session store.
 *
 * One Durable Object SQLite database represents one EDA session, so `sessionId`
 * is fixed at layer construction and not a method parameter.
 */
export interface DurableObjectSessionStoreOptions {
  readonly sessionId: SessionId;
  readonly storage: DurableObjectSessionStorage;
}
