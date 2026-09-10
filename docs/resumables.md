# Durable external work

A tool can open a durable waiting handle with `context.openResumable({ kind, title }, requestEvents)`.
EDA appends `ResumableOpened` and the extension events returned by `requestEvents` in one commit.
The callback may mint identifiers and construct events, but must not perform external I/O. A
repeated call by the same tool returns the original handle without invoking the callback again.
Use a durable sink to deliver the extension request after this commit.

Opening a handle ends the originating run at its next tool boundary, even if the external work
finishes early. Pending user messages stay queued while a handle is open. The parent has no active
run while waiting, so a host can release run capacity on the ordinary `RunCompleted` event.

A trusted extension calls `runtime.resolveResumable({ resumableId, result }, appEvents)` to settle
work. The Cloudflare host exposes the equivalent `resolveResumable` RPC with `sessionId`,
`resolution`, optional `events`, and trace metadata. Settlement appends the application result,
`ResumableResolved`, and one internal `ResumeResumable` admission atomically. Its new run uses the
configured run scheduler, including deferred capacity admission. Repeated settlement returns the
same command identity. Ordinary command submission rejects `ResumeResumable`.

Results are bounded text (16,384 characters) projected as untrusted external context. They do not
forge a user transcript message. Store large artifacts outside the event log and include references
in the result. Compaction includes settled result text in its source and discards completed handles
only after their covered context is rebased.

Stop and interrupt admission cancel outstanding handles under the same append gate. They also
cancel a pending internal continuation. Cancellation wins over later external results; a result
already delivered cannot create another continuation. `cancelResumable` is also available to a
trusted extension. External cancellation itself belongs to the host's durable sink/orchestrator:
`ResumableCancelled` is an obligation to stop that work, not proof the remote worker has stopped.

The current primitive opens one automatic continuation per tool call and permits at most 16 open
handles. Manual waits, deadlines, batch joins, and child session policy belong to later layers.
Gia's initial integration implements a single-child delegation on top of this primitive; EDA does
not create children, assign owners, route workspaces, or manage provider credentials.

Reduced-state checkpoint version 8 adds resumables. Version 7 checkpoints are rebuilt from their
retained durable log. Tests exercise every committed prefix of open, settle, and resumed execution,
as well as early completion, Stop/interrupt races, failed atomic append, and duplicate delivery.
