# EDA offline trace harness

This folder contains the non-Cloudflare, non-web integration harness for
`effect-durable-agent`. All EDA-specific harness code stays inside this package so the runtime
remains self-contained.

## Run a real OpenAI trace

```bash
OPENAI_API_KEY=... pnpm exec tsx testing/offline-trace/node/run.ts \
  --scenario no-tools \
  --out .eda-traces/no-tools-001
```

Optional flags:

- `--scenario no-tools | multi-turn | prefix-cache | framework-tool | parallel-tools | rejected-tool`
- `--model <model-id>` (defaults to `EDA_OPENAI_MODEL` or `gpt-4.1-mini`)
- `--out <directory>`

If `OPENAI_API_KEY` is absent, the CLI writes a blocked report (`blocked.json` and `summary.md`) instead of pretending a real model run occurred.

## Artifacts

A successful run writes:

```text
.eda-traces/<run>/
  run.json
  summary.md
  trace.jsonl
  durable-events.jsonl
  live-events.jsonl
  prompts/
    attempt-0.json
    attempt-0.sha256
```

- `trace.jsonl` contains `model.request`, `model.part`, `model.finish`, durable/live event, and verification records.
- `durable-events.jsonl` is the committed replay source.
- `live-events.jsonl` captures live durable and ephemeral delivery.
- `prompts/*.json` is the canonical model-facing prompt message array.
- `prompts/*.sha256` is the stable prompt hash used by prefix verification.

## Verification intent

The prompt-prefix verifier proves the EDA-side invariant: later prompts preserve earlier model-facing message arrays exactly as a prefix. Cache metrics are reported separately from provider usage; a provider may report zero cache reads even when EDA preserved the prefix correctly.

For OpenAI Responses API, the provider cache is sensitive to the wire shape of system messages.
`@effect/ai-openai` sends `Prompt.SystemMessage` as
`content: [{ type: "input_text", text }]` instead of a plain string; real `prefix-cache` runs should
therefore report non-zero cached input tokens once the cache warms.

Provider-executed tools remain unsupported in this slice. Framework-owned tools are traced through durable `ToolCallCreated` / `ToolCallStarted` / `ToolCallCompleted` or failure/cancellation events.

## Scenario coverage

- `no-tools`: one no-tool turn that proves the harness can capture a real provider stream.
- `multi-turn`: four queued user commands that verify longer transcript continuity and prompt-prefix preservation across follow-up turns.
- `prefix-cache`: three turns with a large durable system prompt first in provider context, sized above OpenAI's prompt-cache threshold so real runs can report cached input tokens as well as EDA prefix metrics.
- `framework-tool`: one framework-owned tool call plus continuation from the tool result.
- `parallel-tools`: two framework-owned tool calls emitted in one model attempt; tests prove both handlers start concurrently while continuation prompts preserve the model's original tool-call/result order.
- `rejected-tool`: invalid tool parameters, durable rejection, and corrective prompt feedback.
