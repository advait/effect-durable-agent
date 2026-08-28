# Changelog

All notable changes to Effect Durable Agent will be documented in this file.

## Unreleased

## 0.1.0-alpha.7

- Let Cloudflare hosts project application WebSocket protocols inside the EDA Durable Object while
  preserving hibernation, strict delivery acknowledgements, and cold recovery
- Persist Effect Schema-encoded application projection state in backward-compatible WebSocket
  attachments
- Preserve client-visible ACK ordering when projections suppress internal frames and compact long
  suppressed runs within Cloudflare attachment limits
- Validate projection selection before accepting sockets and preserve concrete application RPC
  methods on typed Durable Object stubs

## 0.1.0-alpha.6

- Reorganize the host boundary around a platform-neutral session runtime and focused Cloudflare
  controller, provider, RPC, and storage adapters
- Persist complete WebSocket delivery checkpoints as versioned Effect Schema attachments and
  restore strict ACK validation after Durable Object hibernation
- Keep idle WebSockets hibernation-eligible by using append-driven fanout, Cloudflare automatic
  ping responses, and no resident subscriber fiber or heartbeat timer
- Avoid cold-start recovery deadlocks by preparing the session runtime outside per-socket queues
  and flushing deferred durable catch-up after startup repair completes
- Make reducer host configuration heterogeneous and fully typed without `any`, with schema-backed
  codecs materialized by `EDAReducer.make`
- Make native tracing attribute export total for hostile maps and values

## 0.1.0-alpha.5

- Make WebSocket delivery hibernation-native and persist durable resume cursors
- Patch Effect-aware lint plugin loading in the npm publish workflow
- Publish only the core package; Cloudflare and celld remained at `0.1.0-alpha.4` pending their
  first authenticated maintainer publish

## 0.1.0-alpha.4

- Let applications project instruction and data messages from current reducer state into each model prompt
- Build prompts only after turn input selection is committed into durable reducer state
- Split the host-neutral runtime, Cloudflare Durable Objects host, and celld host into lockstep packages
- Add shared real-runtime conformance tests for workerd and celld, including RPC, persistence,
  hard-restart recovery, durable idempotency, WebSocket resume/ACK, and warm/cold session recreation
- Encode Effect Schema values explicitly at Durable Object RPC boundaries

## 0.1.0-alpha.3

- Publish the state-projected prompt context and WebSocket schema changes from the unpublished `0.1.0-alpha.2` package
- Pin prerelease publication to the npm `alpha` dist-tag

## 0.1.0-alpha.2

- Let applications project instruction and data messages from current reducer state into each model prompt
- Build prompts only after turn input selection is committed into durable reducer state
- Separate WebSocket protocol schemas into explicit domain and wire surfaces
- Add a typed domain-frame encoder, a host adapter for app-event narrowing, and transformation
  round-trip coverage

## 0.1.0-alpha.1

- Publish the first public prerelease as `effect-durable-agent`
- Ship compiled ESM and TypeScript declarations for every supported subpath
- Add isolated package-consumer validation for Cloudflare Workers
- Add MIT licensing and trusted-publishing release automation
