import * as Context from "effect/Context";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
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
});
