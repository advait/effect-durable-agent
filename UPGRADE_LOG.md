# Dependency Upgrade Log

**Date:** 2026-10-12
**Project:** Effect Durable Agent

## Summary

- Updated the aligned Effect v4 package set from `4.0.0-beta.102` to stable `4.0.3`
- Updated Effect tsgo from `0.39.0` to `0.51.1`, which requires Oxlint `1.87.0` and
  `oxlint-tsgolint` `7.0.2003`
- Kept Vitest 4.1: `@effect/vitest` 4 declares Vitest 5, but `@cloudflare/vitest-pool-workers`
  (pinned at 0.18.8; 0.23 is the latest) supports only Vitest 4.1. The root manifest allows that
  single peer range; all suites run.

## Migration notes

- `effect/unstable/<area>` imports are now `effect/<area>`.
- `Schema.TaggedErrorClass` is `Schema.TaggedError`; `LanguageModel.Service` is the
  `LanguageModel.LanguageModel` interface; `Crypto.randomUUIDv7` is a function.
- `onExcessProperty: "preserve"` was removed upstream. Durable decoding now retains unknown payload
  fields explicitly through `decodeRetainingExcess`.
- The `effecttsgo/unstable-api-usage` diagnostic is off: Effect AI and HTTP remain
  `@stability unstable`, and the exact Effect pin owns that risk.
- Validation: build, lint, typecheck, unit, property, replay-memory, integration and isolated
  Worker package checks.

---


**Date:** 2026-07-30
**Project:** Effect Durable Agent

## Summary

- Updated the aligned Effect v4 package set from `4.0.0-beta.78` to `4.0.0-beta.102`
- Removed local dependency patches after verifying both fixes are present upstream

## Updates

### `effect`: `4.0.0-beta.78` → `4.0.0-beta.102`

- Includes the `MutableList` invariant fix from
  [Effect-TS/effect#6514](https://github.com/Effect-TS/effect/pull/6514)
- Replaces the local patch required for safe interrupted `PubSub` subscribers
- Validation: library tests, typecheck, lint, package build, and isolated Worker consumer

### `@effect/ai-openai`: `4.0.0-beta.78` → `4.0.0-beta.102`

- Includes the Responses API system-message `input_text` encoding fix
- Replaces the local OpenAI language-model patch
- Validation: library tests, OpenAI smoke test, package build, and isolated Worker consumer
