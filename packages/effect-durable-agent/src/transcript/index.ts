/**
 * EDA's transcript contract: the user-visible history of a session as typed records derived from
 * the event log, read in bounded windows. See `docs/transcript-history.md`.
 */
export { planTranscriptBatch } from "./interpreter";
export * from "./mapping";
export * from "./plan";
export * from "./records";
export * from "./store";
export * from "./version";
export * from "./window";
export {
  buildingTranscriptWindow,
  inactiveTranscriptWindow,
  readTranscriptWindowFrom,
  transcriptReadiness,
  type TranscriptWindowSource,
} from "./window-read";
export * from "./active-content";
export * from "./subscription";
export { resolveTranscriptBody, type ResolvedTranscriptBody } from "./body";
