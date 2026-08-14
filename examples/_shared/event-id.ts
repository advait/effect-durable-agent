import { EventId } from "effect-durable-agent/types/core";

export const mintExampleEventId = async (): Promise<EventId> => EventId.make(crypto.randomUUID());
