import { DatabaseSync, type SQLInputValue } from "node:sqlite";

import type { DurableObjectAlarmStorage } from "../durable-object-keepalive";
import type {
  DurableObjectSessionStorage,
  DurableObjectSqlCursor,
} from "../durable-object-storage";

/**
 * Test-only Durable Object storage over real SQLite, with nested `transactionSync` modeled by
 * savepoints: an inner transaction that throws rolls back only its own writes, as workerd's
 * nested transactions do. Unlike the pattern-matching SQL fakes, every query runs for real.
 *
 * `faults` lets a test throw from a chosen statement, to inject projection failures at any point.
 * With `nestedTransactions: false` it rejects nesting the way celld does.
 */
export class SqliteDurableObjectStorage
  implements DurableObjectSessionStorage, DurableObjectAlarmStorage
{
  readonly db = new DatabaseSync(":memory:");

  constructor(readonly options: { readonly nestedTransactions?: boolean } = {}) {}

  /** Current `transactionSync` nesting depth; 0 outside any transaction. */
  depth = 0;
  alarm: number | null = null;
  /** Called before every statement; throw to fail that statement. */
  faults: ((query: string) => void) | undefined;
  /** Statements executed, for counting index use or write volume. */
  statements = 0;

  readonly sql = {
    exec: <Row = Record<string, unknown>>(
      query: string,
      ...bindings: ReadonlyArray<unknown>
    ): DurableObjectSqlCursor<Row> => {
      this.faults?.(query);
      this.statements += 1;
      const rows = asRows<Row>(this.db.prepare(query).all(...bindings.map(sqlInput)));
      return {
        one: () => {
          const [row, ...rest] = rows;
          if (row === undefined || rest.length > 0) {
            throw new Error(`expected one row, got ${rows.length}`);
          }
          return row;
        },
        toArray: () => rows,
      };
    },
  };

  transactionSync = <A>(closure: () => A): A => {
    if (this.depth > 0 && this.options.nestedTransactions === false) {
      throw new Error("storage.transaction: cannot start a transaction within a transaction");
    }
    const savepoint = `eda_test_${this.depth}`;
    this.db.exec(this.depth === 0 ? "BEGIN" : `SAVEPOINT ${savepoint}`);
    this.depth += 1;
    try {
      const result = closure();
      this.depth -= 1;
      this.db.exec(this.depth === 0 ? "COMMIT" : `RELEASE ${savepoint}`);
      return result;
    } catch (error) {
      this.depth -= 1;
      this.db.exec(
        this.depth === 0 ? "ROLLBACK" : `ROLLBACK TO ${savepoint}; RELEASE ${savepoint}`,
      );
      throw error;
    }
  };

  getAlarm = (): number | null => this.alarm;
  setAlarm = (scheduledTimeMs: number): void => {
    this.alarm = scheduledTimeMs;
  };
  deleteAlarm = (): void => {
    this.alarm = null;
  };

  /** Rows of one table, for assertions. */
  rows<Row = Record<string, unknown>>(
    query: string,
    ...bindings: Array<SQLInputValue>
  ): Array<Row> {
    return asRows<Row>(this.db.prepare(query).all(...bindings));
  }
}

/** Test-only: rows are trusted to have the shape each query selects. */
const isRow = <Row>(_row: unknown): _row is Row => true;

const asRows = <Row>(rows: ReadonlyArray<unknown>): Array<Row> => {
  const typed: Array<Row> = [];
  for (const row of rows) if (isRow<Row>(row)) typed.push(row);
  return typed;
};

const sqlInput = (value: unknown): SQLInputValue => {
  if (
    value === null ||
    typeof value === "number" ||
    typeof value === "string" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  throw new Error(`unsupported SQL binding ${typeof value}`);
};
