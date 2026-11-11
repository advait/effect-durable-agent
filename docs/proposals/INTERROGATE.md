# Adversarial review of FR-1 and FR-5

Status: record of a review. The specs it reviewed are proposals.

Two models tried to break the first drafts of [FR-1](FR-1-deciders.md) and
[FR-5](FR-5-event-dedupe-keys.md) at commit `b10756b`. One was Claude Opus 5.5 and one was
gpt-6-astra. Each worked in its own checkout, prototyped the proposed changes there, and had to
prove every finding with a run against the real code. The author of the specs then reran every
command. A finding is listed here only if the rerun showed the failure.

## Intent the drafts had to meet

1. An implementer can land both features from the files without redesigning.
2. Every claim about EDA 0.1.0 and about gia-cf is true.
3. The stated invariants hold. There is at most one event per dedupe triple. A decision reads the
   fold of the committed log, and nothing interleaves before its append. A retry never decides
   twice. No decider error leaves the runtime fatal. The marker and its items commit together.
4. The migration and rollback claims hold on the real stores.
5. The Gia adoption plan works against the real Gia code.

## Verified findings

Six findings survived. Two were found by both reviewers.

| # | Finding | Found by | Breaks | What changed |
| --- | --- | --- | --- | --- |
| 1 | The namespace allowlist left out `SessionConfigured`. The rule as drafted rejects the seed batch of every new web, cron, Slack and subagent session in Gia. | Both | Intent 2 and 5 | FR-1 "Namespace rule". `SessionConfigured` is on the list. Gia's full test suite against the packed build now decides the list. The pin-only Gia PR is no longer called a no-op. |
| 2 | The marker key `JSON.stringify([decider, idempotencyKey])` can exceed `EventDedupeKey`'s 512 characters for a legal 256-character key. | Both | Intent 1 | FR-1 "The acceptance marker". The key is the name, a line feed, and the idempotency key. Names are limited to `[A-Za-z0-9._-]`. The longest key is 385 characters. |
| 3 | A schema-valid draft over the store's row cap fails as a store error. The runtime goes fatal and the next small `submit` fails. | Opus | Intent 3 | FR-1 "Size limit". Core checks event size before the append, for `submit` and `decide`, as a non-fatal error. |
| 4 | A draft that the decider's `events` schema accepts and the registered `appEvents` schema refuses fails as a store error. The runtime goes fatal. | astra | Intent 3 | FR-1 "Two schemas". `SessionState` checks every app event with the registered schema before the append, as a non-fatal error. |
| 5 | In 0.1.0, one batch that holds the same event twice is folded and published twice. Live state then differs from replay. | astra | Intent 3 | FR-5 "A defect in 0.1.0 that this feature would widen". `publishFreshDurables` keeps one entry per `seq`. A new test covers both stores. |
| 6 | Gia's metadata reducer replaces the metadata on every seed. The extra seed event that the cron plan accepted erases a Slack thread binding. | astra | Intent 5 | FR-5 "Gia adoption". Cron keeps its derived event ids for one release and adds the key. The ids go later. |

Findings 3 and 5 are defects in 0.1.0 itself. They exist without either feature.

## The runs

Paths are relative to each reviewer's EDA checkout. Build core first with
`pnpm --filter effect-durable-agent run build`.

**1. Allowlist.** Opus branch `adversary/fr1-fr5-interrogate` at `b2c303a`.

```text
$ pnpm --filter effect-durable-agent exec vp test run src/services/adversary-fr1-namespace-rule.test.ts
 × accepts Gia's Slack/cron/subagent seed (SessionConfigured + system prompt + submit)
```

astra branch `review/docs-chat-frs-adversarial` at `121ac4d`.

```text
$ pnpm --filter effect-durable-agent exec vp test run src/services/gia-adoption-fr-proposal-review.test.ts
Gia model seed {"namespace":"effect-durable-agent","type":"SessionConfigured","proposedAdmission":"framework_event"}
```

**2. Marker key.** astra.

```text
$ pnpm --filter effect-durable-agent exec vp test run src/services/session-state-fr-proposal-review.test.ts
marker key encoding {"nameLength":19,"keyLength":256,"markerKeyLength":538,"markerKeyAccepted":false}
```

**3. Oversize draft.** Opus, on the real SQLite store.

```text
$ pnpm --filter effect-durable-agent-cloudflare exec vp test run src/adversary-fr1-decider-fatal.test.ts
decide outcome: ..."EDASessionStoreError"... fact_json serialized JSON is 1500011 bytes; hard cap is 1500000 bytes
following submit: ..."EDASessionStoreError"... (the same message)
 × a schema-valid draft the store refuses leaves the runtime usable
```

**4. Two schemas.** astra, same file as finding 2.

```text
decider schema mismatch {"deciderSchemaAccepted":true,"invalidExit":"Failure","subsequentValidExit":"Failure","invalidIsStoreError":true,"subsequentIsStoreError":true,"logRows":0}
```

**5. Same-batch repeat.** astra, on the in-memory store and on SQLite.

```text
$ pnpm --filter effect-durable-agent-cloudflare exec vp test run src/durable-object-fr-proposal-review.test.ts
SQLite same-batch collapse {"returnedSeqs":[1,1],"logRows":1,"liveCount":2,"replayCount":1,"publishedSeqs":[1,1]}
```

**6. Cron seed.** astra, same file as finding 1.

```text
Gia cron seed retry {"beforeMode":"thread","beforeThread":"1715000001.000001","afterMode":"proactive","afterThread":null,"metadataRows":2}
```

## What held

- **The migration and the rollback read.** Both reviewers ran migration 4's two statements on a
  schema-3 SQLite log. The partial unique index rejected a second row with the same triple. The
  0.1.0 store code read every row afterwards and appended.
- **The atomicity argument**, once finding 5 is fixed. Every durable write goes through
  `commitDurableEntriesWithinGate`, and `state` is written only inside the gate. Opus checked this
  by reading the code, not by a run.
- **The design choice.** Neither reviewer produced a run that favored compare-and-append over
  deciders, or hash-derived ids over dedupe keys.
- astra's 88 control tests passed (existing admission, recovery, sink replay, storage and
  transcript fingerprints).

## Dropped

Nothing was dropped. Every reported finding reproduced. Both reviewers reported only what they
had run.

## Where the repros are

The repro tests are local commits in the two reviewers' checkouts. They are not pushed and not in
this branch, because this branch changes no source. The implementer should lift them into the
suites named in each spec's test section.
