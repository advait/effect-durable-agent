export const json = (body: unknown, status = 200): Response =>
  Response.json(body, {
    headers: { "Cache-Control": "no-store" },
    status,
  });

export const parseJsonObject = async (request: Request): Promise<Record<string, unknown>> => {
  const body = await request.json().catch(() => undefined);
  return typeof body === "object" && body !== null && !Array.isArray(body)
    ? (body as Record<string, unknown>)
    : {};
};

export const requiredString = (body: Record<string, unknown>, key: string): string | undefined => {
  const value = body[key];
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
};

export const optionalString = (value: string | undefined): string | undefined => {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
};

export const pathParam = (pathname: string, pattern: RegExp): string | null => {
  const match = pattern.exec(pathname);
  return match === null ? null : decodeURIComponent(match[1]!);
};
