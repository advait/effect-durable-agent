/**
 * Test-only transcript fixtures for store adapters: the scenario corpus (one log per
 * interpretation rule), its Gia-shaped mappings, the deterministic log writer, and the in-memory
 * reference projection. An adapter proves its tables and transactions by projecting every
 * scenario and comparing against `projectBatches`.
 */
export { giaLikeTestMappings, transcriptScenarios } from "../transcript/transcript-scenarios";
export type { TranscriptScenario } from "../transcript/transcript-scenarios";
export {
  TestLog,
  ids,
  normalizedProjection,
  projectBatches,
  testDefinition,
  testId,
  testSessionId,
} from "../transcript/transcript-testkit";
