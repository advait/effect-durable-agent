# WebSocket live-event protocol

Status: current

Protocol version: 1

Attachment version: 2
Last reviewed: 2026-10-15

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

Clients ACK only after validating and applying an events frame (raw event sockets ignore the
optional `appliedUpdateSeq`, which belongs to transcript sockets):

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
    inFlight: [{ frameId, durableThroughSeq, sentAtMs }],
    suppressed?: [{ fromFrameId, throughFrameId, durableThroughSeq }]
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

Projection initialization and any cold-start recovery complete before the host
calls `ctx.acceptWebSocket`. Immediately after acceptance, the connection
manager registers the socket and serializes its attachment before its first
await. This prevents recovery fanout from observing an accepted socket with no
attachment. Projection ids are schema-backed non-empty brands. A projection
may return `SuppressAndAck` for an internal-only events frame; the host removes
only that exact receipt from window capacity and persists it separately until
all earlier client-visible frames are ACKed. It never uses a cumulative client
ACK on the app's behalf.

## Transcript-window stream

A socket upgraded with `?stream=transcript` carries the ordered transcript stream of
`docs/transcript-history.md` §9 instead of raw events. It shares the transport above: the hello,
ACK receipts and their 16-frame window, the lazy ACK timeout (`lagged` + `4008`), the application
ping and auto-response, and per-socket attachments. A host enables it by passing
`transcriptSockets: { transcriptStore }` (on `EDASessionDurableObject`, `transcriptSockets: {}`
suffices: `transcriptStore` defaults to the object's own Durable Object transcript store); without
it the upgrade is rejected with 400. On `EDASessionDurableObject`, a session the host has not
enabled for transcripts (`transcript.enabled`) is refused with 409, and an upgrade naming a session
other than the object's with 400. When a rebuilt transcript version activates, every transcript
socket, held or hibernated, is re-read and receives one complete replacement in the new version. Any other
`stream` value keeps the raw event stream. The
projection header cannot be combined with it. Accepting a transcript socket never prepares the
model runtime: windows are storage-level reads.

Wire schemas live in `effect-durable-agent/src/websocket/transcript.ts`; the update contract in
`effect-durable-agent/src/transcript/subscription.ts`.

### Negotiation

The hello adds a stable envelope that clients decode before anything version-specific
(`negotiateEDATranscript`), and that hosts also embed in SSR data (`edaTranscriptNegotiation`):

```json
{
  "_tag": "hello",
  "protocolVersion": 1,
  "subscriberId": "…",
  "resumeSeq": 0,
  "flowControl": { "…": "…" },
  "negotiation": { "protocolMajor": 1, "wireVersion": 1, "capabilities": ["window", "page", "retain", "chunked-updates", "active-content"] }
}
```

A different `protocolMajor` (or no decodable envelope) means the client performs one bounded reload.
Unknown capabilities and unknown fields are ignored; unknown record kinds, parts and readiness decode
as explicit `unknown` representations.

### Requests

Each request is answered by exactly one update, in arrival order (at most 4 may be outstanding, and
a client frame may be at most 2,048 UTF-8 bytes):

- `subscribe { requestId?, range?, tail?, since?, count? }`: with `range` (`{ from, to }` cursors),
  `tail` and `since` (what SSR rendered, or what a reconnecting client last applied), the first
  update is a patch over that range; otherwise it replaces the window with the latest `count`
  records (default 100).
- `page { requestId?, direction, cursor, count }`: `cursor` is the coverage's `from` (`before`) or
  `to` (`after`); at most 100 records per page. A cursor that is not the current edge (the coverage
  moved while the request was in flight) is answered with a re-read of the current coverage.
- `retain { requestId?, from, to }`: narrows the coverage to a span inside it.

Coverage is contiguous and holds at most 300 top-level records: paging back evicts the newest,
paging forward the oldest. A subscriber whose coverage reaches the newest record (`hasNewer: false`)
follows the tail; new history joins its coverage as it commits. A cursor from another incarnation or
version is answered with a replacement of the latest records.

### Updates

```json
{ "_tag": "transcript", "frameId": 7, "update": { "updateSeq": 3, "mode": "patch", "…": "…" } }
```

A `TranscriptUpdate` carries `updateSeq` (per socket, from 1), `mode`, `incarnation`, `version`,
`throughSeq`, `coverage`, `hasOlder`, `hasNewer`, `readiness`, `records`, `dependencies`,
`dependencyIds`, optional `activeContent` and the answered `requestId`.

- `replace`: discard everything held and install this window.
- `patch`: relative to the previous complete update. Upsert `records` and `dependencies` by id, drop
  top-level records outside `coverage`, and keep only the dependencies listed in `dependencyIds`.

Either way the client then holds exactly the window read at `throughSeq`: a complete cut. Committed
batches are coalesced to the boundary a read returns; a batch is never split across updates.
`activeContent`, when present, replaces live content completely (`[]` clears it); it is published at
most once per display cadence (50 ms), never as raw deltas, and is absent from degraded updates.
The update that commits or fails an inference also retires its live content: a window read is served
only at exactly the cut the live-content accumulator has folded, so a read that resolves behind or
ahead of publication is re-read rather than paired with live parts from another cut.

An update larger than `maxFrameBytes` is sent as `transcript-chunk` frames:

```json
{ "_tag": "transcript-chunk", "frameId": 8, "updateSeq": 4, "index": 0, "count": 37, "data": "…" }
```

The `data` of all chunks concatenated in order is the JSON of one update; the client applies it only
after chunk `count - 1`. Each chunk is a frame in the flow-control window, so a slow receiver
receives at most 16 before ACKing. Encoded updates above 8 MiB close the socket with `1011`; staging is also
bounded to 32 MiB across the object's sockets, and a socket whose chunked update would exceed that is
closed with `4009` to resubscribe later. Clients reconnect after a `4009` with the same bounded
exponential backoff as after `4008`.

### Acknowledgements

```json
{ "_tag": "ack", "frameId": 8, "durableThroughSeq": 120, "appliedUpdateSeq": 3 }
```

`frameId` is the cumulative transport receipt that frees flow-control capacity. `appliedUpdateSeq`
(and `durableThroughSeq`, that update's `throughSeq`) is the applied cut: it advances only when an
update is complete and may never name an update whose frames were not all sent.

### Hibernation

The attachment kind `eda-transcript-v1` stores the transport receipts, the applied cut, the
subscription base (incarnation, version, `throughSeq`, coverage, tail) and unanswered requests. An
idle socket that wakes re-reads its coverage at the next opportunity and continues with patches.
Chunk data is never persisted: a socket that wakes mid-transfer is closed with `4009`, and the client
discards the partial transfer, reconnects, and resubscribes from what it last applied. Destroying
the session drops live content and closes every transcript socket with `4009`, including sockets
still hibernated (recognised by their `eda-transcript-v1` attachment without being restored).

The applied cut's `throughSeq` is not monotonic (a new incarnation or a building window may lower
it); the server validates that `appliedUpdateSeq` never moves backwards, names only a fully sent
update, and that `durableThroughSeq` matches the last completed update when it names it.

### Client reference store

`effect-durable-agent/websocket` exports pure helpers so hosts only render:
`applyEDATranscriptServerMessage` (chunk assembly, complete-cut application, coverage eviction, ACKs,
and a `Resubscribe` outcome for gaps or broken transfers), `edaTranscriptClientFromWindow` (SSR
handoff), `edaTranscriptClientReconnected`, and the `subscribe`/`page`/`retain` request builders.

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
