# 005 — Rivet no-tools host

This is the smallest EDA session hosted as a native [Rivet Actor](https://rivet.dev/actors/): one
stable actor key per `SessionId`, actor-local SQLite event history, lifecycle recovery, and the EDA
resume/ACK WebSocket protocol.

## Run locally

Install matching package versions and provide an OpenAI key:

```sh
pnpm add effect-durable-agent@0.1.0-alpha.5 effect-durable-agent-rivet@0.1.0-alpha.5 rivetkit@2.3.10
export OPENAI_API_KEY='...'
```

Start the server with a local Rivet Engine:

```sh
RIVET_RUN_ENGINE=1 pnpm exec tsx server.ts
```

In another terminal, submit an idempotent command through the typed Rivet client:

```sh
pnpm exec tsx client.ts
```

The session actor is addressed by the EDA `SessionId`. Retrying the client preserves the original
command admission because `rivet-example:first-message` is a durable idempotency key.

## Deploy

Rivet can run on Rivet Compute or on a self-hosted engine. For Rivet Compute, package the server in
the application image and run:

```sh
npx @rivetkit/cli deploy
```

Production clients should use the public endpoint and public/connection token supplied by the
deployment. Keep secret runner tokens server-side. See Rivet's
[deployment guide](https://rivet.dev/docs/deploy/) for the supported targets.
