# Durable model selection

Hosts supply `modelResolverLayer` instead of a captured `modelLayer`. The
`ModelResolver` service resolves a persisted `ModelSelectionPayload` to an
Effect AI `LanguageModel.Service` at each inference or compaction call. Shared
provider transports belong to the resolver layer. Provider-specific validation,
model catalogs, and reasoning settings belong to the host adapter.

`SessionConfigured` is a creation fact, not a switch command. The first
configuration wins; a legacy session without it is initialized by its first
`RunStarted`. `RunStarted` remains the immutable execution snapshot. Runtime
defaults are only used before durable selection exists. Restart recovery
inherits the interrupted run's selection, including opaque provider settings.
There is deliberately no model-switch command in this release.

`ModelResolver.Fixed` is a convenience for deterministic tests and fixed-model
hosts; such hosts must ensure their recorded selection matches their fixed
provider. Multi-model hosts use a real resolver. The Cloudflare OpenAI resolver
shares its gateway/client transport and builds a model from each selection.

## Cloudflare providers

`makeEDAMultiProviderModelResolverLayer` (`effect-durable-agent-cloudflare/providers`) serves
every provider a host configures. Each provider gets one client for the runtime lifetime. Its
requests go through EDA's provider transport: the Cloudflare AI Gateway binding when the host
passes one, and `fetch` otherwise. The host's `resolve` turns each recorded selection into
`{ provider, modelId, config }`, and the resolver builds that provider's model.

A prompt for one provider can hold parts another provider wrote. A text or reasoning part with no
text whose metadata names only other providers holds nothing the selected provider can use, such
as Gemini's signed empty answer part. The resolver's `preparePrompt` removes those parts, and an
assistant message they leave empty, before the provider's own preparation. The Anthropic and
OpenAI adapters already drop reasoning that lacks their own metadata. Anthropic rejects an empty
text block and an empty assistant message.

| Provider | Subpath | Adapter | Config helper | Prompt preparation | Compaction |
| --- | --- | --- | --- | --- | --- |
| OpenAI | `/openai` | `@effect/ai-openai` (Responses API) | none | replays native compaction items | `/responses/compact` |
| Anthropic | `/anthropic` | `@effect/ai-anthropic` | `anthropicAgentModelConfig` | EDA cache hints become `cache_control`; replays native compaction blocks | on-demand compaction (`compact-2026-09-04`) |
| Google | `/google` | EDA's own Gemini API adapter | `googleAgentModelConfig` | none | prompt-prefix summary |

Effect has no v4 Google provider, so `/google` calls the Gemini API
(`v1beta/models/{model}:streamGenerateContent?alt=sse` and `:generateContent`) directly and
parses responses with a small schema. Through the AI Gateway it uses the `google-ai-studio`
provider and sends the key in `x-goog-api-key`. Gemini differs from the other two in these ways:

- Effort maps to `thinkingConfig.thinkingLevel` one to one: `low`, `medium` and `high`. Gemini 4
  cannot turn thinking off and rejects `minimal`. It returns 400 when `thinkingBudget` is sent
  with `thinkingLevel`, so EDA sends only `thinkingLevel`. Gemini's `xhigh` is not an effort.
- A thought signature stays on the part Gemini sent it on. EDA stores it as
  `options.google.thoughtSignature` on the committed prompt part and sends it back on the same
  part. A signed part is never merged with its neighbours; only unsigned streamed pieces of one
  part are joined. Gemini rejects a function call replayed without its signature. In a parallel
  call only the first call carries one. A streamed answer's signature arrives on an empty text
  part at the end. EDA commits it as an empty text part with the signature and sends it back as it
  came.
- Gemini reports some provider failures as finish reasons, such as `MALFORMED_FUNCTION_CALL`,
  `TOO_MANY_TOOL_CALLS` and `MISSING_THOUGHT_SIGNATURE`. EDA fails the inference with an
  `AiError` that keeps Gemini's `finishMessage`, and commits no tool call from that response.
  Length, safety and recitation stops end the response as they do on the other providers.
- A file part's data reaches the adapter as a string. A base64 `data:` URL goes inline with its
  payload and media type. A string with a URL scheme, such as a Gemini Files URI, goes as
  `fileData.fileUri`. Bare base64 goes inline.
- Caching is implicit, for prompts of 4,096 tokens or more, with no write charge. EDA keeps every
  request's `contents` an unchanged prefix of the next request's. Only the results of one step's
  calls share a turn. Usage reports `cachedContentTokenCount` as cached input and no cache writes.
  Thinking tokens are output.
- Gemini has no compaction endpoint. `makeEDANativeCompactionExecutorLayer` compacts Google
  sessions with `promptPrefixSummaryCompactionExecutor`. That executor re-sends an earlier
  request with the summary request appended as its own turn, so the call reads the conversation
  from the cache.

Token consumption now contains `byModel` buckets keyed by provider and model ID.
Input, cached input, cache-write input, output, text, and reasoning counters are
preserved. Text and reasoning are output breakdowns. Costs and pricing tiers
belong to application reducers, not the execution framework.

Compaction completion and known-usage failure events include model selection
and provider usage. Calls interrupted before usage is persisted remain unknown;
EDA does not invent usage estimates. Old compaction events remain readable.

Framework checkpoint schema 7 rebuilds older checkpoints from retained
events, including the schema-5 aggregate-usage format and schema 6. Conversation history is preserved; the old aggregate usage shape and
the `modelLayer` host option are removed. All in-repository host examples and
consumers use the new resolver contract.
