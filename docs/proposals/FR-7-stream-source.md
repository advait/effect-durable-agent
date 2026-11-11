# FR-7. A long-lived stream source on a headless actor

Status: proposal. Recommended not to build.

## The request

A managed home for a long-lived inbound stream such as Salesforce SSE. The sketch is an
`EDAStreamSource` with `connect(cursor)`, `cursorOf` and `route(item)`, hosted on an
`EDAProcessDurableObject`, a headless actor with reducers, sinks and timers and no model.

## What the source shows

EDA has no such actor. Every host type is a session. The residency claim is true, and EDA
documents it. In-flight outbound work does not keep a Durable Object resident, and sessions were
evicted 70 to 140 s after their last inbound event (`spec.md` §16). EDA's answer for its own work is the
keep-alive heartbeat alarm.

## Is it general

Not on the evidence. Judge it by consumers.

- Gia has no long-lived inbound stream. Slack reaches it through HTTP events. Its other actors,
  the capacity coordinator and the subagent coordinator, are plain Durable Objects with their own
  storage, and they are not asking to become EDA actors.
- The docs-site connector is the only consumer. The request itself calls it "reasonable, and
  arguably product code".
- The other named users (Slack Socket Mode, GitHub event streams, provider batch polling) are
  hypothetical here.

It is also a different product. EDA is "one durable state machine per session" with a model loop
at the center. A stream source is keyed by a principal, not a session. It fans in to many
sessions. Its hard parts are the reconnect and cursor rules of one upstream protocol. The docs-site
models show how specific those rules are. The confirmed watermark may advance only after a
completed listing, never from the stream. A framework `cursorOf` would hide exactly the rule that
matters.

## Designs considered

| Design | Verdict |
| --- | --- |
| A. `EDAStreamSource` on a new headless host type. | Rejected. One consumer, and a second kind of actor to maintain on two hosts. |
| B. Run the connector as an ordinary EDA session that never receives `SubmitMessage`. | Possible once FR-1, FR-4 and FR-5 exist. Not recommended yet. |
| C. Keep the connector as product code. | Chosen. |

**About B.** An EDA session with no tools and no model calls already works
(`examples/004-celld-no-tools`). After [FR-4](FR-4-sink-wakeups.md) it would have durable wakeups,
and after [FR-5](FR-5-event-dedupe-keys.md) idempotent facts. A sink could hold the stream under
the keep-alive lease. That is a lot of session machinery (commands, runs, transcript, model
runtime construction on every wake) carried for none of its use. If a second stream consumer ever
appears, start from B and see what is missing, before designing A.

## What EDA should do instead

Make the connector's two touch points with a session good.

- Inbound facts need idempotent append. That is FR-5.
- Outbound work and timers need a wakeup. That is FR-4 and FR-6.

With those, the connector shrinks to what is specific to Salesforce, which is the token, the SSE
connection and the two cursors.

## Priority and size

Not planned. It would be large. It needs a new host type, alarm ownership, storage and
conformance on workerd and celld. Revisit when a second consumer exists.
