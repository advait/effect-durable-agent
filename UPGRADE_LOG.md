# Dependency Upgrade Log

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
