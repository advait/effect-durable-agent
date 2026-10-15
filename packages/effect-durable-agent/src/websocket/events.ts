/**
 * The raw live-event WebSocket contract alone: frames, delivery rules and messages, without the
 * transcript-window stream. Browser clients of the event socket import this entry so their
 * bundles do not carry the transcript protocol's schemas, which `effect-durable-agent/websocket`
 * also exports.
 */
export * from "./delivery";
export * from "./messages";
export * from "./protocol";
