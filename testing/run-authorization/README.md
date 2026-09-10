# Run authorization conformance

This fixture uses the production Durable Object session adapter and a separate
SQLite-backed manual authorizer. It verifies the protocol without consuming a
model API or implementing production capacity policy.

Build the workspace with `pnpm run build`. Set a random `STAGING_TOKEN` in the
fixture's ignored `.dev.vars` file and write the same value to a private token
file. Start the fixture with:

```sh
pnpm exec wrangler dev --config testing/run-authorization/wrangler.jsonc --local \
  --ip 127.0.0.1 --port 34903 --persist-to /tmp/eda-authorization-state \
  --define EDA_AUTHORIZER_OFFLINE:true
```

If the installed workerd rejects the configured compatibility date, pass its
reported maximum date with `--compatibility-date`. Use the same persistence path
on every restart. Run each verification mode with:

```sh
node testing/run-authorization/verify.mjs MODE http://127.0.0.1:34903 \
  /path/to/private-token /tmp/eda-authorization-verification.json
```

The modes form one sequence and share the verification state file:

1. `offline`: persist a request while the authorizer rejects delivery; no run starts.
2. Restart with `EDA_AUTHORIZER_OFFLINE:false`, then run `handoff`. The driver
   observes only the authorizer until the session alarm delivers the original ID.
3. Restart again and run `hydration`: the acknowledged request survives unchanged.
4. `queue`: grant three queued runs, reject duplicate grants, and verify full and
   cursor-based WebSocket replay.
5. `controls`: stop, cancel, and interrupt waiting requests; old grants stay stale.
6. Restart with `EDA_CONFORMANCE_BLOCK_MODEL:true`, then run `block` to leave an
   authorized run unfinished.
7. Restart with `EDA_CONFORMANCE_BLOCK_MODEL:false`, then run `recover`. Recovery
   requires fresh authorization and links the replacement to its interrupted run.

For remote staging, deploy the same candidate under a unique Worker name with
`workers_dev` enabled, install the token with `wrangler secret put STAGING_TOKEN`,
and replace local restarts with redeployments using the same flags and Worker
name. All routes fail closed without the token. Delete the isolated Worker after
verification; never reuse an application Worker name or bindings.
