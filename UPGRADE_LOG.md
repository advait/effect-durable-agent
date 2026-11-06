# Dependency Upgrade Log

**Date:** 2026-11-06
**Project:** Effect Durable Agent

## Summary

- Updated the aligned Effect v4 package set from `4.0.3` to `4.1.2`, and changed its peers from an
  exact pin to `~4.1.2`
- Kept Effect tsgo `0.51.1`, Oxlint `1.87.0` and Vitest 4.1 (`@effect/vitest` 4.1.2 still declares
  Vitest 5; the root manifest's single peer allowance still applies)

## Migration notes

- `effect/Crypto` is gone: `IdGenerator.Live` uses `SecureRandom.layerWebCrypto` and
  `SecureRandom.uuidV7()` from `effect/crypto`, which cannot fail.
- `Response.ResponseMetadataPart` no longer has `request`.
- `Prompt.FilePartEncoded["data"]` adds `{ _tag: "Uint8Array", base64 }` and `{ _tag: "URL", href }`.
- Validation: build, lint, typecheck, unit, property, replay-memory, integration and isolated
  Worker package checks (`pnpm run ci`).

---

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
