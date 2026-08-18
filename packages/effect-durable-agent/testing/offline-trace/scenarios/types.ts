import type { EDACommand } from "../../../src/types/commands";
import type { ModelSelectionPayload, SystemPromptText } from "../../../src/types/events";
import type { EDAModelToolkit } from "../../../src/services/tool-registry";

/** Declarative offline scenario run against the real EDA runtime graph. */
export interface OfflineTraceScenario {
  readonly name: string;
  readonly description: string;
  readonly modelSelection: ModelSelectionPayload;
  readonly systemPrompt?: SystemPromptText;
  readonly commands: ReadonlyArray<EDACommand>;
  readonly toolkit?: EDAModelToolkit;
}
