# EDA real integration tests

This directory is for local-only integration tests that exercise the real `effect-durable-agent` runtime stack with a real model provider. They do not deploy anything to production.

Run the real OpenAI suite explicitly:

```bash
EDA_REAL_INTEGRATION=true \
OPENAI_API_KEY=... \
EDA_OPENAI_MODEL=gpt-4.1-mini \
mise run test -- testing/integration/runtime-real.test.ts
```

Optional:

- `OPENAI_API_URL=...` to point the OpenAI-compatible client at a local/provider-compatible endpoint.
- `EDA_OPENAI_MODEL=...` to override the default model.

The suite is skipped unless `EDA_REAL_INTEGRATION=true` and `OPENAI_API_KEY` is present. This keeps normal local checks deterministic while making the real integration path an explicit developer action.
