# Changelog

All notable changes to Effect Durable Agent will be documented in this file.

## Unreleased

- Let applications project instruction and data messages from current reducer state into each model prompt
- Build prompts only after turn input selection is committed into durable reducer state

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
