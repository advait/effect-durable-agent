# Changelog

All notable changes to Effect Durable Agent will be documented in this file.

## Unreleased

## 0.1.0-alpha.5

- Add the native `effect-durable-agent-rivet` host with actor-local SQLite, Rivet lifecycle and
  keep-awake integration, typed actions, and the EDA resumable WebSocket protocol
- Run the same real-process host conformance contract against Rivet Engine, workerd, and celld
- Add a dedicated Rivet example and extend lockstep version, package-consumer, and publishing checks

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
