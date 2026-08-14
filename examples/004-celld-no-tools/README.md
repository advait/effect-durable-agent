# 004 — celld no-tools host

This is the smallest deployable EDA session on [celld](https://celld.dev/): one named cell, Durable
Objects-compatible SQLite state, and a tiny HTTP command facade.

## Configure

Install matching package versions and make `esbuild` and `celld` available on `PATH`:

```sh
pnpm add effect-durable-agent@0.1.0-alpha.5 effect-durable-agent-celld@0.1.0-alpha.5
```

celld reads the fleet location from `--bucket` or `CELLD_BUCKET`. Provider secrets can be passed as
cell variables without committing them to Wrangler configuration:

```sh
export CELLD_BUCKET=s3://my-eda-fleet
export CELLD_VAR_OPENAI_API_KEY='...'
```

The normal AWS credential chain is used for `s3://` buckets. A `gs://` bucket uses Google
Application Default Credentials.

## Validate and deploy

From this example directory:

```sh
celld deploy wrangler.jsonc --dry-run
celld deploy wrangler.jsonc
```

Start a celld node against the same bucket after deployment:

```sh
celld --bucket "$CELLD_BUCKET" --listen 127.0.0.1:8080
```

Nodes load a deployment at startup, so restart an existing node to serve a newly deployed version.

## Exercise the session

```sh
curl -X POST \
  http://127.0.0.1:8080/sessions/018f6bd5-2f2a-7b1e-8f1a-1f2e3d4c5b6a/messages \
  -H 'content-type: application/json' \
  -d '{"text":"Say pong in one sentence.","idempotencyKey":"celld-demo-1"}'
```

Retrying the same idempotency key returns the original durable admission rather than running the
command twice. `GET` on the same URL returns the committed transcript.

## Generated bindings

`worker-configuration.d.ts` is generated from `wrangler.jsonc`. Wrangler does not infer secret
bindings from env files, so the authored `env.d.ts` augments `Env` with `OPENAI_API_KEY`, while
`.dev.vars.example` documents its local name:

```sh
pnpm exec wrangler types worker-configuration.d.ts \
  --config wrangler.jsonc \
  --env-file .dev.vars.example \
  --include-runtime=false
```
