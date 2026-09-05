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

Token consumption now contains `byModel` buckets keyed by provider and model ID.
Input, cached input, cache-write input, output, text, and reasoning counters are
preserved. Text and reasoning are output breakdowns. Costs and pricing tiers
belong to application reducers, not the execution framework.

Compaction completion and known-usage failure events include model selection
and provider usage. Calls interrupted before usage is persisted remain unknown;
EDA does not invent usage estimates. Old compaction events remain readable.

Framework checkpoint schema 6 rebuilds schema-5 checkpoints from retained
events. Conversation history is preserved; the old aggregate usage shape and
the `modelLayer` host option are removed. All in-repository host examples and
consumers use the new resolver contract.
