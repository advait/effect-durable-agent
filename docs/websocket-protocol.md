# effect-durable-agent WebSocket live-event protocol

Status: current
Runtime: effect-durable-agent host with app-specific event bindings
Last reviewed: 2026-08-14

Current scope: WebSocket-only live transport. The implemented slice includes protocol schemas,
Cloudflare/celld hibernation handling, native Rivet raw-WebSocket handling, ACK-window subscriber
flow control, `4008` lag close, bounded active-turn ephemeral replay in `LiveEventBus`, and
reconnect-safe `EDARuntime.eventsAfter(...)`. Items called out as future hardening below are not
current behavior. This document replaces the earlier SSE-oriented live-event idea.

## 0. Objectives

The protocol exists to satisfy these objectives, in priority order:

1. **Durable correctness.** Durable `seq` remains the only durable resume cursor. A client that reconnects from the last acknowledged durable `seq` can recover durable facts through `eventsAfter(seq)`.
2. **Turn-scoped ephemeral continuity.** Live-only ephemerals are retained only for the currently open turn. A committed turn terminal (`TurnCompleted`, `TurnStopped`, or `TurnFailed`) is the checkpoint that makes prior ephemerals droppable because durable message/lifecycle facts now explain the visible result. A reconnecting client receives durable history through the latest checkpoint, then receives the active turn's buffered ephemerals, then follows new live events.
3. **Bounded memory per subscriber and per session.** A slow browser must not move unbounded memory from the active-turn ephemeral buffer or `LiveEventBus` into either an Effect queue or Cloudflare's opaque WebSocket send buffer. The design must state hard bounds for session-level active-turn ephemeral retention plus subscriber queue/in-flight bytes.
4. **Subscriber isolation.** Slow-client lag closes only the lagging socket. It never backpressures `SessionState`, the durable append path, `LiveEventBus`, other subscribers, model streams, tools, or sinks.
5. **Explicit failure semantics.** Expected subscriber failures are typed `Schema.TaggedErrorClass` values (`SubscriberLagged`, `SubscriberProtocolError`, etc.) and are mapped once at the host boundary to WebSocket close codes/reasons.
6. **Effect-native resource ownership.** Each subscriber is one scoped workflow made of queues, refs, streams, deferred cancellation, and forked fibers. Closing the WebSocket closes the scope; closing the scope closes the WebSocket. No detached global fibers, raw timers, or hidden promises own protocol correctness.
7. **Host hibernation fit.** Cloudflare/celld use `ctx.acceptWebSocket` and
   `serializeAttachment`; Rivet uses raw `onWebSocket` and `c.conn.state`. Both persist only the
   small subscriber identity, trace, and last acknowledged durable cursor.
8. **Operational clarity.** Observability must expose subscriber id, session id, current durable ack, in-flight frame count, queue size, close reason, lag reason, active turn id, and active-turn ephemeral buffer size.
9. **Protocol evolution.** The frame shape should be versioned and batch-capable from day one, while allowing the first implementation to send one event per frame.

Non-objectives:

- Provide exactly-once delivery over WebSocket. Durable events are at-least-once across reconnect and clients must de-duplicate by position/event id.
- Detect kernel/TCP-level delivery. The Workers WebSocket API gives `send(...) => void` and no reliable drain signal; application-level ACKs are the durability/flow-control signal.
- Preserve ephemerals indefinitely. Active-turn ephemerals are retained only until the turn terminal checkpoint commits; completed-turn ephemerals are replaced by durable facts.
- Keep SSE as a supported production live transport.

## 1. Important constraints and conclusions

### 1.1 `ws.send` is an enqueue, not a backpressure boundary

In Workers/Durable Objects, `WebSocket.send(...)` is synchronous and returns `void`. The installed Workers types document that it enqueues data, and they do not expose a readable `bufferedAmount` or drain effect for server-side flow control. Therefore EDA must assume that `send` can buffer below the JavaScript layer.

Consequence: an Effect `Queue.dropping` before `ws.send` is necessary but insufficient. If the sender fiber drains that queue into `ws.send` without a second gate, unbounded memory can simply move into the platform's opaque socket buffer.

### 1.2 ACK-window flow control is the explicit backpressure boundary

The server sends at most `maxInFlightFrames` unacknowledged frames. A frame leaves the in-flight set only when the browser sends an application ACK after applying the frame to client state. If ACKs stop, the sender stops draining the bounded subscriber queue. The eager pump continues to drain the shared live/replay source into that bounded queue. When the queue fills, `Queue.offer` returns `false`, the workflow fails with `SubscriberLagged`, and the host closes only that socket.

This bounds opaque WebSocket buffering to approximately:

```text
max opaque WS bytes <= maxInFlightFrames * maxFrameBytes
```

and bounds Effect-side event buffering to:

```text
max queued events <= subscriberBufferCapacityEvents
max unsent event bytes ~= subscriberBufferCapacityEvents * average encoded event size
```

### 1.3 Batching is intentional, not delegated to TCP

TCP may coalesce bytes, but it does not remove WebSocket message overhead, JavaScript `message` event overhead, JSON parsing overhead, or ACK bookkeeping. Cloudflare's WebSocket guidance recommends batching logical messages because each WebSocket message has processing overhead. EDA frames are therefore **batch-capable**.

The initial implementation may set `maxFrameEvents = 1` for simplicity. The protocol still uses an `events` array so later micro-batching by count/bytes/time does not change the wire contract.

### 1.4 Turn terminals are the dynamic ephemeral-retention boundary

EDA needs reconnect continuity for the active turn without retaining live deltas forever. The retention boundary is the turn terminal durable event:

- While a turn is open, `LiveEventBus` keeps a **turn-scoped ephemeral replay buffer** containing positioned ephemerals emitted for that active turn.
- When `TurnCompleted`, `TurnStopped`, or `TurnFailed` is published, `LiveEventBus` drops the buffer for that turn.
- A later reconnect never receives pre-terminal ephemerals for completed turns; it receives the durable `AssistantMessageCommitted` / `AssistantPartialCommitted` / lifecycle terminal facts instead.
- A reconnect during an open turn receives durable replay plus the active turn's buffered ephemerals, even when those ephemerals have `position.seq <= afterSeq`. `afterSeq` is durable-only and cannot prove which live-only ephemerals the client applied before disconnect.

This is a natural `LiveEventBus` responsibility because the buffer is live-only transport state. `SessionState` remains the authority for durable commits, reduced state, active turn identity, and ephemeral position allocation. `LiveEventBus` consumes the already-positioned ordered stream and maintains a best-effort reconnect buffer from those facts; it does not allocate positions, mutate durable state, decide turn lifecycle, or become a correctness source after process loss.

Memory safety still matters. Current behavior caps active-turn retention by event count (`activeTurnEphemeralReplayCapacity = 4096`), keeps the newest events, and records an `overflowed` flag in `LiveEventBus.activeTurnReplay()`. It does not yet emit an explicit client reset/snapshot event when overflow happens. Coalesced retention or overflow reset signaling is future hardening.

## 2. Wire protocol overview

Transport: WebSocket over the authenticated session events route.

Encoding: UTF-8 JSON text frames only. Binary client frames are rejected with close code `1003` (unsupported data) or `1002` (protocol error), depending on host convenience.

Protocol version: `1`.

Server-to-client frames:

- `hello` — connection accepted and flow-control parameters announced.
- `events` — one ordered frame containing one or more EDA `PositionedEvent`s.
- `heartbeat` — idle keepalive/status frame emitted when no event frame has been sent for the configured heartbeat interval.
- `lagged` — optional final application frame before close when the server intentionally lag-closes.
- `error` — optional final application frame before close for protocol/internal errors.

Client-to-server frames:

- `ack` — cumulative acknowledgement for delivered/applied event frames.
- `pong` — optional response to an application `ping` if one is introduced later. Protocol v1 does not require app-level ping/pong because Cloudflare handles WebSocket control ping/pong automatically.

The route-level resume parameter is `afterSeq`. The server may adjust it downward to the socket attachment's `lastAckedSeq` on hibernation restore. The authoritative resume cursor is always the server-side last acknowledged durable `seq`.

### 2.1 App-specific typed event binding

The transport owns the frame protocol and framework event schemas. Each app registers only its custom event union:

```ts
import * as Schema from "effect/Schema";

import { makeEDAWebSocketWireProtocol } from "./host/websocket-wire";
import { DurableEventEnvelope } from "./types/events";

const CounterIncrementedEvent = Schema.Struct({
  ...DurableEventEnvelope.fields,
  namespace: Schema.Literal("counter"),
  type: Schema.Literal("CounterIncremented"),
  schemaVersion: Schema.Literal(1),
  payload: Schema.Struct({ amount: Schema.Number }),
});

export const counterWebSocketProtocol = makeEDAWebSocketWireProtocol({
  appEvents: CounterIncrementedEvent,
});

export const CounterWebSocketServerFrame = counterWebSocketProtocol.wire.serverFrame;
export type CounterWebSocketServerFrame = typeof CounterWebSocketServerFrame.Type;
```

`makeEDAWebSocketWireProtocol` automatically unions the app events with all EDA durable and ephemeral events. The returned value makes the two schema directions explicit:

- `domain.event`, `domain.positionedEvent`, `domain.eventsFrame`, and `domain.serverFrame`
  describe decoded in-memory values. Schema transformations have already run on their `Type` side.
- `wire.event`, `wire.positionedEvent`, `wire.eventsFrame`, and `wire.serverFrame` describe
  the UTF-8 JSON representation. Browser and other external consumers decode with these schemas
  when they need validated wire values without hydrating domain transformations.
- `wire.clientFrame` validates client-to-server ACK messages.
- `encodeServerFrame` accepts only the exact app-bound domain server-frame type and serializes it to
  one JSON text message.
- `host` is the adapter passed as `webSocketProtocol` to the EDA host. It validates and encodes the
  host's broad event envelope against the registered app/framework union at runtime.

For example, a transformed message content field may be a prompt-part array in
`domain.serverFrame.Type` and a string in `wire.serverFrame.Type`. TypeScript rejects passing the
wire frame to `encodeServerFrame`; callers must decode external input through the domain schema
before treating it as an in-memory value. Both surfaces are derived from the same event schemas, so
there is still one logical contract rather than parallel hand-maintained models. Switching on
`event.type` narrows `event.payload` to that event's exact schema on either surface.

The EDA host binding is therefore explicit:

```ts
const options = {
  // Other host options omitted.
  webSocketProtocol: counterWebSocketProtocol.host,
};
```

The app event union is the registration point: adding a custom event there changes the WebSocket union everywhere it is imported. Consumers that use an exhaustive event policy or `assertNever` then receive a TypeScript error until they explicitly apply or ignore the new event.

Hosts without custom events use the strict framework-only protocol. The default host encoder does not accept an arbitrary `EventEnvelope`; an app event must be registered explicitly or outbound encoding fails. This prevents an omitted app binding from silently degrading payloads back to `unknown`.

This API separation does not change protocol version `1` or any serialized frame shape. It makes the
existing domain-to-wire transformation direction visible in TypeScript and tests it through a full
domain frame → JSON text → wire/domain decode round trip.

## 3. Schema sketch

Frame variants are plain `Schema.Struct` values with literal `_tag` fields. Client and server protocols compose those variants with `Schema.Union`. This keeps the wire contract data-only; class constructors are reserved for domain values that benefit from behavior.

```ts
import * as Schema from "effect/Schema";

import { SequenceNumber } from "effect-durable-agent/types/core";
import { PositionedEvent } from "effect-durable-agent/types/events";

export const EDA_WS_PROTOCOL_VERSION = 1 as const;

export const SubscriberId = Schema.NonEmptyString.pipe(Schema.brand("SubscriberId"));
export type SubscriberId = typeof SubscriberId.Type;

export const FrameId = Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)).pipe(
  Schema.brand("FrameId"),
);
export type FrameId = typeof FrameId.Type;

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

export const FlowControlConfig = Schema.Struct({
  ackTimeoutMs: PositiveInt,
  heartbeatIntervalMs: PositiveInt,
  maxFrameBytes: PositiveInt,
  maxFrameEvents: PositiveInt,
  maxInFlightFrames: PositiveInt,
  subscriberBufferCapacityEvents: PositiveInt,
});
export type FlowControlConfig = typeof FlowControlConfig.Type;

export const HelloFrame = Schema.Struct({
  _tag: Schema.Literal("hello"),
  protocolVersion: Schema.Literal(1),
  subscriberId: SubscriberId,
  resumeSeq: SequenceNumber,
  flowControl: FlowControlConfig,
});

export const EventsFrame = Schema.Struct({
  _tag: Schema.Literal("events"),
  frameId: FrameId,
  // Ordered by Position. The first implementation may send one event per frame.
  events: Schema.NonEmptyArray(PositionedEvent),
  // Max durable seq included in this frame, or the previous durable seq when the frame contains only ephemerals.
  durableThroughSeq: SequenceNumber,
});

export const HeartbeatFrame = Schema.Struct({
  _tag: Schema.Literal("heartbeat"),
  serverTimeMs: UnixEpochMillis,
  // Latest durable seq the server has emitted on this connection. Clients may use
  // it for liveness/status, but heartbeats do not advance ACK frame ids.
  durableThroughSeq: SequenceNumber,
});

export const LaggedFrame = Schema.Struct({
  _tag: Schema.Literal("lagged"),
  resumeSeq: SequenceNumber,
  reason: Schema.Literals(["buffer-overflow", "ack-timeout"]),
});

export const ErrorFrame = Schema.Struct({
  _tag: Schema.Literal("error"),
  message: Schema.String,
});

export const ServerFrame = Schema.Union([
  HelloFrame,
  EventsFrame,
  HeartbeatFrame,
  LaggedFrame,
  ErrorFrame,
]);
export type ServerFrame = typeof ServerFrame.Type;

export const AckFrame = Schema.Struct({
  _tag: Schema.Literal("ack"),
  frameId: FrameId,
  // Highest durable seq the client has applied to its local state.
  durableThroughSeq: SequenceNumber,
});

export const ClientFrame = Schema.Union([AckFrame]);
export type ClientFrame = typeof ClientFrame.Type;
```

Notes:

- `_tag` strings are intentionally short because they appear in every frame.
- `events` frames are cumulative-ACKed by `frameId`: ACKing `N` acknowledges all frames with `frameId <= N`.
- `heartbeat` frames are idle keepalives. They have no `frameId`, do not count against the in-flight ACK window, and must not be ACKed.
- `durableThroughSeq` in an ACK must be monotonic and must not exceed the maximum durable `seq` sent through that frame. Violations are protocol errors.
- Frame IDs are per-subscriber, start at `1`, and reset on reconnect.

## 4. Default policy values

Current production defaults are bounded while leaving enough headroom for fast model deltas and larger durable tool/message events observed in replay:

```ts
const defaultFlowControl = {
  subscriberBufferCapacityEvents: 2_048,
  maxInFlightFrames: 16,
  maxFrameEvents: 1,
  maxFrameBytes: 256 * 1024,
  ackTimeoutMs: 30_000,
  heartbeatIntervalMs: 10_000,
};
```

Later tuning can change only configuration:

- Increase `maxFrameEvents` to `10-25` for high-frequency token deltas.
- Add `maxFrameDelayMs = 25-50` if micro-batching by time is needed.
- Keep `maxFrameBytes` below any practical browser/Worker frame-size concern and below the close-reason/attachment constraints.

The bounded-memory invariant is the target for current and future tests:

```text
Events retained by EDA subscriber workflow <= subscriberBufferCapacityEvents + maxInFlightFrames * maxFrameEvents
Opaque WS bytes intentionally enqueued <= maxInFlightFrames * maxFrameBytes
```

## 5. Connection lifecycle

### 5.1 Handshake

1. HTTP route authenticates the request exactly like the former session events route.
2. Non-`Upgrade: websocket` requests return `426 Upgrade Required`.
3. Route resolves the session Durable Object by session id and forwards the upgrade request to the DO.
4. The concrete EDA DO creates a `WebSocketPair`, calls `ctx.acceptWebSocket(server)`, stores a small attachment, and returns the client socket in a `101` response.
5. The host starts or restores the subscriber workflow.
6. Server sends `hello` as the first application frame.

Attachment shape:

```ts
const EDAWebSocketAttachment = Schema.Struct({
  kind: Schema.Literal("eda-events-v1"),
  sessionId: SessionId,
  subscriberId: SubscriberId,
  lastAckedSeq: SequenceNumber,
});
```

The attachment must stay well under Cloudflare's 16 KiB attachment limit. It is updated only when ACKs advance `lastAckedSeq`.

### 5.2 Normal streaming

1. Server obtains the merged reconnect/live stream from `EDARuntime.eventsAfter(lastAckedSeq)`. That stream must include durable replay plus the active-turn ephemeral replay buffer before following new live events.
2. Eager pump subscribes/replays and offers positioned events into `Queue.dropping`.
3. Sender builds one `events` frame from the queue when the in-flight window has capacity.
4. If no event is available for `heartbeatIntervalMs`, sender emits a `heartbeat` frame with the current server time and latest sent durable seq. Heartbeats are liveness/status frames only: they do not enter `inFlight` and are not ACKed.
5. Sender calls `ws.send(JSON.stringify(frame))` inside a small transport boundary effect.
6. Sender records event-frame metadata in `inFlight` and starts/refreshes the oldest-frame ACK deadline.
7. Client validates and applies events in order.
8. Client sends `ack` after applying an `events` frame to local state.
9. Server validates ACK, frees in-flight frames through `frameId`, advances `lastAckedSeq` if durable seq advanced, and updates the socket attachment.

### 5.3 Lag close

A subscriber is lagged when either:

- the eager pump cannot offer a new event into the bounded queue (`buffer-overflow`), or
- the oldest in-flight frame is not ACKed within `ackTimeoutMs` (`ack-timeout`).

The workflow fails with `SubscriberLagged`, the host optionally sends a final `lagged` frame if the socket is still open, then closes the socket with application code `4008` and a short reason such as:

```text
lag;seq=123
```

Close reasons must stay short because WebSocket close reasons have a small protocol limit.

### 5.4 Cancellation / close

If the client closes the socket, the host completes a `closed` signal and interrupts the subscriber scope. The Effect stream is interrupted with `Stream.interruptWhen(closed)`, which interrupts in-progress pulls. Queue resources and fibers are cleaned up by scope finalizers.

If the Effect workflow completes or fails first, the finalizer closes the WebSocket unless it is already closing/closed.

### 5.5 Hibernation restore

When a Durable Object hibernates, in-memory subscriber fibers disappear but accepted WebSockets and attachments remain. On any wake event (client message, new RPC, alarm), the concrete DO/runtime host must:

1. Re-run normal lightweight constructor/migration setup.
2. Inspect `ctx.getWebSockets()`.
3. For each socket with `kind: "eda-events-v1"`, ensure a subscriber workflow exists.
4. Start that workflow from `attachment.lastAckedSeq`.

If durable events were committed while the subscriber workflow was absent, `eventsAfter(lastAckedSeq)` replays durable catch-up before following live events. If a turn is still open, the active-turn ephemeral replay buffer is included in the reconnect prefix. If a turn terminal committed while the subscriber workflow was absent, the old ephemeral buffer has been dropped and clients recover through durable facts only.

## 6. Effect-native runtime design

### 6.1 Boundary split

Keep protocol logic out of Cloudflare callback code. The host adapter should translate platform events into small Effect queues/deferreds, while the subscriber workflow owns policy.

```ts
interface EDAEventWebSocketTransport {
  readonly send: (frame: ServerFrame) => Effect.Effect<void, SubscriberSendFailed>;
  readonly incoming: Queue.Queue<ClientFrame, SubscriberProtocolError>;
  readonly closed: Deferred.Deferred<void>;
  readonly persistAck: (seq: SequenceNumber) => Effect.Effect<void>;
}
```

The Cloudflare adapter implementation may call `ws.send`, `ws.close`, `ws.serializeAttachment`, and signal `closed`, but it should not contain queue overflow, ACK-window, or replay policy. Active-turn ephemeral replay is likewise not transport-adapter state; it belongs in `LiveEventBus` so WebSocket subscribers, tests, and any future live transport consume the same reconnect source.

### 6.2 Typed subscriber errors

Expected failures are typed and serializable:

```ts
export class SubscriberLagged extends Schema.TaggedErrorClass<SubscriberLagged>()(
  "SubscriberLagged",
  {
    subscriberId: SubscriberId,
    lastAckedSeq: SequenceNumber,
    reason: Schema.Literals(["buffer-overflow", "ack-timeout"]),
  },
) {}

export class SubscriberProtocolError extends Schema.TaggedErrorClass<SubscriberProtocolError>()(
  "SubscriberProtocolError",
  {
    subscriberId: SubscriberId,
    message: Schema.String,
  },
) {}

export class SubscriberSendFailed extends Schema.TaggedErrorClass<SubscriberSendFailed>()(
  "SubscriberSendFailed",
  {
    subscriberId: SubscriberId,
    cause: Schema.Defect,
  },
) {}
```

Mapping to close codes happens once:

| Error | Code | Reason |
| --- | ---: | --- |
| `SubscriberLagged` | `4008` | `lag;seq=<lastAckedSeq>` |
| `SubscriberProtocolError` | `1002` | `protocol` |
| `SubscriberSendFailed` | `1011` | `send-failed` |
| client normal close | no server override / `1000` | `normal` |
| host shutdown | future use of `1001` | `going-away` |

Defects are not converted into ordinary lag. They are logged as defects and close with `1011` if the socket is still open.

### 6.3 Workflow sketch

Pseudo-code shape, not final API:

```ts
const runWebSocketSubscriber = Effect.fn("EDAWebSocketSubscriber.run")(function* (input) {
  const outbound = yield* Queue.dropping<PositionedEvent>(input.policy.subscriberBufferCapacityEvents);
  const closed = input.transport.closed;
  const lastAckedSeq = yield* Ref.make(input.resumeSeq);
  const nextFrameId = yield* Ref.make(FrameId.make(1));
  const inFlight = yield* Ref.make<ReadonlyArray<SentFrame>>([]);

  yield* input.transport.send(new HelloFrame({
    protocolVersion: 1,
    subscriberId: input.subscriberId,
    resumeSeq: input.resumeSeq,
    flowControl: input.policy,
  }));

  const source = yield* input.runtime.eventsAfter(input.resumeSeq);

  const pump = source.pipe(
    Stream.interruptWhen(Deferred.await(closed)),
    Stream.runForEach((event) =>
      Queue.offer(outbound, event).pipe(
        Effect.flatMap((accepted) =>
          accepted
            ? Effect.void
            : Effect.gen(function* () {
                const seq = yield* Ref.get(lastAckedSeq);
                yield* new SubscriberLagged({
                  subscriberId: input.subscriberId,
                  lastAckedSeq: seq,
                  reason: "buffer-overflow",
                });
              }),
        ),
      ),
    ),
  );

  const ackLoop = readAckFrames(input.transport.incoming, inFlight, lastAckedSeq, input.transport.persistAck);
  const sender = sendFrames(outbound, inFlight, nextFrameId, lastAckedSeq, input.transport, input.policy);
  const timeout = watchAckTimeouts(inFlight, lastAckedSeq, input.policy);

  yield* Effect.raceFirst(
    pump,
    Effect.raceFirst(sender, Effect.raceFirst(ackLoop, timeout)),
  ).pipe(
    Effect.ensuring(Deferred.succeed(closed, undefined).pipe(Effect.ignore)),
  );
}).pipe(Effect.scoped);
```

Design details:

- The pump is eager and non-suspending at the subscriber edge because the queue is dropping. It isolates `LiveEventBus` and replay source from the client.
- The sender is the only place that drains `outbound`. It waits when `inFlight.length >= maxInFlightFrames`.
- ACK handling is cumulative. It removes all sent frames with `frameId <= ack.frameId`, then advances `lastAckedSeq` to `max(current, ack.durableThroughSeq)` after validation.
- `watchAckTimeouts` observes only the oldest in-flight frame. No in-flight frames means no timeout.
- `persistAck` updates WebSocket attachment. It is intentionally not a durable database write.
- `Effect.raceFirst` is used because whichever branch fails/completes should tear down the subscriber; losers are interrupted and finalizers run.

### 6.4 Sender batching algorithm

Initial algorithm with one event per frame:

1. Wait until in-flight window has capacity.
2. `Queue.take(outbound)`.
3. Encode a candidate frame.
4. If encoded bytes exceed `maxFrameBytes`, fail with `SubscriberProtocolError` or `SubscriberSendFailed` depending on whether the event is impossible to send or encoding failed unexpectedly. Large individual event payloads should be rare because durable event payloads are already size-capped.
5. Send frame.
6. Add to in-flight metadata: `{ frameId, durableThroughSeq, sentAtMs, maxDurableSeqAllowed }`.

Future micro-batch algorithm:

1. Take first event immediately.
2. Pull `Queue.takeUpTo(outbound, maxFrameEvents - 1)`.
3. Keep appending while encoded size remains `<= maxFrameBytes`.
4. If time-based batching is introduced, use Effect `Clock`/`TestClock`, not raw `setTimeout`.

Batch metadata:

- `durableThroughSeq` is the max durable event `seq` in the frame, or current `lastAckedSeq` for ephemeral-only frames.
- Frame event order must already be `Position` order. The subscriber does not sort the ongoing live stream; reconnect prefix sorting stays in `EDASessionQuery.eventsAfter`.

## 7. Client obligations

The browser client must:

1. Open WebSocket with its current durable cursor: `?afterSeq=<lastDurableAppliedSeq>`.
2. Wait for `hello` and validate `protocolVersion === 1`.
3. Ignore `heartbeat` frames for state application. They only prove the socket is alive and report the server's latest emitted durable seq.
4. Process `events` frames in `frameId` order. If a frame id skips unexpectedly, close and reconnect from the last applied durable seq.
5. Apply events to local state idempotently:
   - Drop durable events with `position.seq <= lastDurableAppliedSeq`.
   - Apply ephemerals only if they are relevant to the current live turn/session view.
6. ACK only after events are validated and applied to client state:

```json
{ "_tag": "ack", "frameId": 42, "durableThroughSeq": 123 }
```

7. Do not ACK `heartbeat` frames; they have no `frameId` and do not affect backpressure.
8. On close `4008`, reconnect from the sequence in close reason if present, otherwise from local `lastDurableAppliedSeq`.
9. Use bounded exponential backoff for abnormal reconnects.

ACK after state application, not merely after receipt, is intentional: it backpressures on a busy browser main thread as well as on network delay.

## 8. `LiveEventBus` active-turn ephemeral replay buffer

The WebSocket subscriber protocol depends on a live-only reconnect source owned by `LiveEventBus` with these semantics:

```ts
interface ActiveTurnEphemeralReplay {
  readonly turnId?: TurnId;
  readonly events: ReadonlyArray<PositionedEvent>; // ephemerals only, ordered by Position
  readonly overflowed: boolean;
}

interface LiveEventBusShape {
  readonly publish: (event: PositionedEvent) => Effect.Effect<boolean>;
  readonly subscribeQueue: () => Effect.Effect<PubSub.Subscription<PositionedEvent>, never, Scope.Scope>;
  readonly activeTurnReplay: () => Effect.Effect<ActiveTurnEphemeralReplay>;
}
```

Ownership rules:

1. `SessionState` still emits every durable and ephemeral event through `LiveEventBus.publish` after assigning authoritative positions and folding durable state.
2. `LiveEventBus.publish` updates its active-turn buffer from the positioned event in the same live-bus critical section as PubSub fanout, with no suspending work between buffer mutation and publish:
   - `TurnStarted` opens or resets the active-turn buffer for that `turnId`.
   - active-turn ephemerals append to the buffer in `Position` order.
   - `TurnCompleted`, `TurnStopped`, or `TurnFailed` publishes the terminal and closes the current turn buffer, dropping its ephemerals at that checkpoint.
3. Reconnect setup subscribes to `LiveEventBus`, captures a durable replay head, reads durable replay, reads `liveBus.activeTurnReplay()`, drains pending live events, sorts this finite prefix by `Position`, drops duplicate durable events at or below the replay head, and emits all active-turn ephemerals from the buffer regardless of `afterSeq`.
4. Clients must treat reconnect-prefix ephemerals as a reconstruction of the current open turn. They should reset or de-duplicate active-turn live accumulators before applying buffered ephemerals, because durable `afterSeq` alone cannot encode which ephemerals were previously applied.
5. Once the turn terminal durable event is observed, clients should retire live accumulators for that turn and rely on durable transcript/lifecycle state.

This changes `LiveEventBus` from "only PubSub fanout" to "live delivery hub": it still is not durable authority, but it is the natural owner of live-only retention and subscriber replay state. The shared `PubSub` remains just one implementation detail inside that service.

Implemented decision: raw-delta retention with a conservative event-count cap. This gives reconnecting clients the same live event sequence for the open turn while bounding process memory. Coalesced retention would bound memory more semantically but requires snapshot-shaped ephemeral schemas and client reset semantics; that belongs to a future live-state extension.

## 9. Observability

Every subscriber run should have a span such as `eda.ws.subscriber` with attributes:

- `sessionId`
- `subscriberId`
- `resumeSeq`
- `lastAckedSeq`
- `queueCapacityEvents`
- `queueSize` where cheap/available
- `inFlightFrames`
- `maxInFlightFrames`
- `closeCode`
- `closeReason`
- `lagReason`

Logs:

- connect/restore with resume seq
- ACK advancement
- lag close with queue size/in-flight details
- protocol error with sanitized reason
- send defect with `Schema.Defect`

Metrics later:

- active subscribers
- lag closes by reason
- ack latency histogram
- frames sent
- events sent
- reconnect count

## 10. Security and boundary validation

- Authenticate and authorize before upgrade. Do not accept a WebSocket and then authenticate inside the protocol.
- Decode every inbound client frame with `Schema.decodeUnknownEffect(ClientFrame)` after JSON parse.
- Reject binary frames.
- Treat unknown `_tag`, malformed JSON, non-monotonic ACKs, ACKs for unsent future frames, and `durableThroughSeq` beyond sent durable max as `SubscriberProtocolError`.
- Do not expose raw internal error details in `error` frames or close reasons.

Future hardening: cap inbound text frame size before `JSON.parse`; ACK frames should be tiny.

## 11. Testing status and future hardening

Current tests cover the implemented slice:

1. **Normal subscriber delivery:** fake runtime stream emits events; fake transport records frames; ACKs advance `lastAckedSeq` and attachment persistence is called.
2. **Overflow lag:** with no ACKs and a tiny subscriber buffer, the pump fails with `SubscriberLagged`.
3. **Active-turn ephemeral replay:** reconnect during an open turn emits buffered ephemerals even when their durable anchor `seq <= afterSeq`.
4. **Turn terminal checkpoint:** committing `TurnCompleted`/`TurnStopped`/`TurnFailed` drops prior buffered ephemerals; subsequent reconnect receives durable facts only for that completed turn.
5. **Merged reconnect ordering:** durable replay, buffered live events, and active-turn ephemerals sort into a reconnect-safe prefix.
6. **Cloudflare host flow:** fake DO WebSocket upgrade streams frames and ACK updates `serializeAttachment`.
7. **Integration flow:** fake WebSocket-style route/runtime tests exercise the WebSocket-only session event path.

Future hardening tests should add:

- ACK-gated send assertions that the sender stops draining `outbound` while the in-flight window is full.
- ACK timeout with `TestClock.adjust(ackTimeoutMs)` producing `SubscriberLagged { reason: "ack-timeout" }`.
- Protocol errors for malformed ACKs, ACKs for unsent future frames, and durable seq advancement beyond sent frame boundaries.
- Oversized outbound frame behavior and inbound text-frame byte cap before JSON parse.
- Explicit client reducer reset/de-duplication behavior after active-turn replay overflow.

Shared conformance tests exercise live delivery, ACK handling, and explicit-cursor reconnects
against real workerd, celld, and Rivet Engine processes. A future hardening case should keep one
ACKed host WebSocket alive across host-native hibernation and prove attachment/connection-state
restoration directly.

## 12. Implemented artifacts and remaining work

Implemented artifacts:

1. `packages/effect-durable-agent/src/host/websocket-wire.ts` defines the app-event-parameterized public wire schemas and encoder.
2. `packages/effect-durable-agent/src/host/websocket-protocol.ts` defines host frame schemas, attachment schema, flow-control config, close-code constants, and encode/decode helpers.
3. `packages/effect-durable-agent/src/services/live-event-bus.ts` owns bounded active-turn ephemeral replay: open on `TurnStarted`, append active-turn ephemerals, drop on `TurnCompleted`/`TurnStopped`/`TurnFailed`, expose `activeTurnReplay()`.
4. `packages/effect-durable-agent/src/services/session-query.ts` / `EDARuntime.eventsAfter(...)` merge durable replay, active-turn ephemerals, already-buffered live events, and follow-live delivery.
5. `packages/effect-durable-agent/src/services/websocket-subscriber.ts` defines typed subscriber errors and `runWebSocketSubscriber(...)` over `EDARuntime.eventsAfter`, `Queue.dropping`, ACK refs, scoped cancellation, and ACK timeouts.
6. `packages/effect-durable-agent-cloudflare/src/durable-object-runtime.ts` adapts Durable Object WebSockets to the transport, persists ACKs in `serializeAttachment`, restores accepted sockets, and maps typed errors to close codes for Cloudflare and celld.
7. `packages/effect-durable-agent-cloudflare/src/durable-object.ts` and the consuming application's routes expose a WebSocket-only events endpoint with `426` for non-upgrade requests.
8. `packages/effect-durable-agent-rivet/src/runtime.ts` adapts Rivet raw WebSockets to the same
   subscriber workflow and persists ACK state in `c.conn.state`.
9. Tests cover typed custom-event bindings, normal subscriber delivery, subscriber overflow lag,
   active-turn replay retention/drop, reconnect ordering, and real host streaming.

Remaining hardening:

1. Browser/client-side protocol helper with ordered apply, ACK-after-apply, active-turn replay reset/de-dupe, duplicate durable drop, and reconnect from last durable seq.
2. Inbound text-frame byte cap before JSON parse.
3. Real-host hibernation coverage that verifies persisted ACK attachment/connection state without
   supplying an explicit reconnect cursor.
4. Focused ACK-timeout and protocol-error tests.
5. Micro-batching (`maxFrameEvents > 1`, optional frame delay) once token volume needs it.
6. Explicit client-visible behavior for active-turn replay overflow.
7. Broader host conformance coverage for lag/timeout failure paths.
