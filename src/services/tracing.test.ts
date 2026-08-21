import * as Context from "effect/Context";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";
import { describe, expect, it } from "vite-plus/test";

import { makeEdaExportingTracer, type EDAExportedSpan } from "./tracing";

const makeSpan = (startTime: bigint, spans: Array<EDAExportedSpan>) =>
  makeEdaExportingTracer((span) => spans.push(span)).span({
    name: "agent.test",
    parent: Option.none(),
    annotations: Context.empty(),
    links: [],
    startTime,
    kind: "internal",
    root: true,
    sampled: true,
  });

describe("makeEdaExportingTracer", () => {
  it("does not export spans with zero duration", () => {
    const spans: Array<EDAExportedSpan> = [];
    const span = makeSpan(1_000n, spans);

    span.end(1_000n, Exit.succeed(undefined));

    expect(spans).toEqual([]);
  });

  it("exports spans with observable duration", () => {
    const spans: Array<EDAExportedSpan> = [];
    const span = makeSpan(1_000n, spans);

    span.end(2_000n, Exit.succeed(undefined));

    expect(spans).toHaveLength(1);
    expect(spans[0]).toMatchObject({
      name: "agent.test",
      startedAtUnixNano: "1000",
      endedAtUnixNano: "2000",
    });
  });

  it("keeps native span and link export total for hostile attributes", () => {
    const hostileOwnKeys = new Proxy(
      {},
      {
        ownKeys() {
          throw new Error("OWNKEYS_SENTINEL");
        },
      },
    );
    const hostileGetter = new Proxy(
      { errorMessage: "unreachable" },
      {
        get() {
          throw new Error("GET_SENTINEL");
        },
      },
    );

    for (const hostile of [hostileOwnKeys, hostileGetter]) {
      const spans: Array<EDAExportedSpan> = [];
      const span = makeSpan(1_000n, spans);
      Object.defineProperty(span, "attributes", { value: hostile });
      span.addLinks([
        {
          attributes: hostile,
          span: Tracer.externalSpan({
            sampled: true,
            spanId: "1111111111111111",
            traceId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          }),
        },
      ]);

      expect(() => span.end(2_000n, Exit.succeed(undefined))).not.toThrow();
      expect(spans).toHaveLength(1);
      expect(spans[0]).toMatchObject({
        attributes: {},
        links: [{ attributes: {} }],
      });
    }
  });

  it("bounds native attribute snapshots", () => {
    const spans: Array<EDAExportedSpan> = [];
    const span = makeSpan(1_000n, spans);
    for (let index = 0; index < 300; index += 1) {
      span.attribute(`attribute.${index}`, index);
    }

    span.end(2_000n, Exit.succeed(undefined));

    expect(Object.keys(spans[0]?.attributes ?? {})).toHaveLength(256);
  });
});
