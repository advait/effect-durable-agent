# WebSocket live-event protocol

Status: current

Protocol version: 1

Attachment version: 2
Last reviewed: 2026-08-28

EDA uses a pull-based, ACK-clocked WebSocket protocol. Core owns the wire
contract and pure delivery state machine. The Cloudflare adapter owns sockets,
attachments, hibernation restoration, and interpretation of delivery actions.

## Correctness model

- Durable event sequence is the reconnect cursor. Delivery is at least once
  across a new connection; clients de-duplicate durable events by position or
  event id.
- Each connection has monotonically increasing frame ids.
- At most `maxInFlightFrames` event frames may be sent without a cumulative
  client ACK.
- ACKs are accepted only for persisted in-flight frame receipts. The rule is
  identical before and after Durable Object hibernation.
- Ephemeral events are live-only. They may be buffered while an isolate is
  active but are not persisted in the WebSocket attachment.

The store is the durable replay buffer. Core reads bounded event pages only
when a new socket connects, a durable gap is observed, or an ACK reopens the
window.

## Wire frames

All messages are UTF-8 JSON text. The existing version-1 client wire shape is
unchanged by attachment version 2.

Server frames:

- `hello`: protocol version, subscriber id, resume cursor, and flow-control
  policy.
- `events`: non-empty ordered event array, frame id, and the highest durable
  sequence covered by the frame.
- `lagged`: optional final frame before close code `4008`.
- `error`: optional sanitized protocol/internal error.
- `pong`: application-level response for hosts without automatic response.
- `heartbeat`: deprecated decode-only compatibility variant. Servers do not
  schedule or send heartbeats.

Client frames:

- `ack`: cumulative frame id and highest durable sequence applied by the
  client.
- `ping`: exact application ping string used by the Cloudflare automatic
  response path.

Clients ACK only after validating and applying an events frame:

```json
{ "_tag": "ack", "frameId": 42, "durableThroughSeq": 123 }
```

Duplicate ACKs that do not advance the durable cursor are ignored. The server
protocol-closes ACKs that reference an unsent frame, move the cursor backward,
advance the cursor from a duplicate frame, or exceed the durable boundary of
the acknowledged receipts.

## Flow control

Current defaults:

```ts
{
  ackTimeoutMs: 30_000,
  maxFrameBytes: 256 * 1024,
  maxFrameEvents: 1,
  maxInFlightFrames: 16,
  pingIntervalMs: 30_000,
  subscriberBufferCapacityEvents: 2_048
}
```

ACK timeout is checked lazily on protocol transitions. There is no resident
timer. This is deliberate: a timer would keep a Durable Object ineligible for
hibernation. Actual encoded frame bytes are checked by the host after the
app-specific encoder runs.

## Hibernation attachment

Cloudflare permits 16,384 bytes of structured-clone attachment data per
accepted WebSocket. The adapter validates both encoding and restoration with
Effect Schema:

```ts
{
  kind: "eda-events-v2",
  sessionId,
  subscriberId,
  trace,
  delivery: {
    lastAckedSeq,
    lastAckedFrameId,
    lastSentFrameId,
    nextFrameId,
    sentDurableThroughSeq,
    inFlight: [{ frameId, durableThroughSeq, sentAtMs }]
  },
  projection?: { id, state }
}
```

The host persists this complete checkpoint before every tracked send and after
every applied ACK. Persistence is an explicit delivery action ordered before
`Send`; failure closes the socket without sending the corresponding frame.
Persisting only `lastAckedSeq` is insufficient because a post-hibernation ACK
could not otherwise be validated against the frames actually sent.

An optional app-owned projection stores its stable id and schema-encoded
connection state in the same attachment. The host initializes that state from
the authoritative session snapshot, persists it before projected frames become
client-visible, and decodes it after hibernation. A missing or unknown
projection closes the socket instead of silently reverting to the raw EDA wire
protocol.

An attachment is decoded when a hibernated socket first appears through
`ctx.getWebSockets()` or sends a message. Missing, malformed, or obsolete
attachments are closed. Deployments terminate open WebSocket requests, so the
adapter does not carry a live migration path for version-1 attachments.

## Cloudflare hibernation rules

The Durable Object base class uses:

- `ctx.acceptWebSocket(server)`, never the standard server-side
  `WebSocket.accept()`;
- `ctx.getWebSockets()` to discover accepted sockets after isolate eviction;
- `serializeAttachment` / `deserializeAttachment` for delivery restoration;
- `ctx.setWebSocketAutoResponse` for the exact app ping/pong pair.

The implementation owns no `setTimeout`, `setInterval`, resident subscriber
fiber, or outbound WebSocket. An idle accepted socket therefore does not by
itself prevent hibernation. Native WebSocket protocol pings are handled by the
platform; browser clients use the configured application ping, which receives
an automatic response without waking the object.

Cloudflare can return sockets in a closing state from `getWebSockets()`.
Send/close failures are contained per socket, and explicit close remains
necessary for compatibility dates before automatic close-handshake handling.

## Ownership and event flow

```text
EDA append/publish
  -> SessionEventObserver (infallible injected port)
  -> EDAWebSocketConnectionManager (Cloudflare)
  -> core delivery transition
  -> Persist / Send / ReadEventPage / Close actions
  -> Cloudflare socket and attachment APIs
```

`SessionEventObserver.onEvent` has an error channel of `never`. The
Cloudflare implementation catches and logs platform defects before returning
to EDA. No callback registry or runtime listener registration is exposed.

Files:

- `effect-durable-agent/src/websocket/protocol.ts`: app-bound wire schemas and
  encoders.
- `effect-durable-agent/src/websocket/messages.ts`: domain frame schemas,
  constants, and decoding.
- `effect-durable-agent/src/websocket/delivery.ts`: pure checkpointed delivery
  machine and action algebra.
- `effect-durable-agent/src/services/session-event-observer.ts`: injected
  infallible publish observer.
- `effect-durable-agent-cloudflare/src/websocket/attachment.ts`: attachment
  schema and codec.
- `effect-durable-agent-cloudflare/src/websocket/connection-manager.ts`:
  Cloudflare WebSocket ownership and action interpreter.

## App event binding

Applications with custom events build an encoder from the same core module:

```ts
import { makeEDAWebSocketWireProtocol } from "effect-durable-agent/websocket";

const protocol = makeEDAWebSocketWireProtocol({
  appEvents: MyAppEvent,
});

const options = {
  webSocketProtocol: protocol.host,
};
```

The app event schema is unioned with framework events. An unregistered custom
event fails outbound encoding rather than degrading to an unknown payload.

## App wire projection

An application that already exposes a stable client protocol can register one
`EDAWebSocketProjection`. The Worker authenticates the upgrade and sets
`x-eda-websocket-projection` only on its internal request to the Durable Object.
The object still accepts and owns the client socket directly; there is no
Worker-side socket pair or outbound bridge.

The projection owns only app wire concerns: its initial snapshot projection,
state codec, server-frame encoder, and client-ACK decoder. EDA retains socket
acceptance, replay, flow control, attachment persistence, auto-response, and
hibernation restoration. Raw EDA clients remain the default when the internal
selection header is absent.

## Client obligations

1. Authenticate before upgrade and connect with
   `?afterSeq=<lastDurableAppliedSeq>`.
2. Validate `hello.protocolVersion === 1`.
3. Apply event frames in frame-id order and idempotently.
4. ACK after application, not merely receipt.
5. Send the advertised application ping while idle; do not ACK pong.
6. On close `4008`, reconnect from the local durable cursor using bounded
   exponential backoff.

Inbound JSON is schema-decoded. Binary, malformed, and unknown frames are
closed as protocol errors. Error frames and close reasons never expose raw
internal failures.
