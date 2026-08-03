export interface LogQueryParams {
  service?: string;
  level?: string;
  since?: Date;
  until?: Date;
  attributes: Record<string, string>;
  q?: string;
  limit: number;
  cursor?: { timestamp: Date; id: number };
}

export interface QueryValidationResult {
  valid: boolean;
  params?: LogQueryParams;
  error?: string;
}

const VALID_LEVELS = ["debug", "info", "warn", "error"];
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

export function validateQueryParams(
  query: Record<string, unknown>
): QueryValidationResult {
  const result: LogQueryParams = {
    attributes: {},
    limit: DEFAULT_LIMIT,
  };

  if (query.service !== undefined) {
    if (typeof query.service !== "string" || query.service.trim() === "") {
      return { valid: false, error: "service must be a non-empty string" };
    }
    result.service = query.service;
  }

  // level
  if (query.level !== undefined) {
    if (
      typeof query.level !== "string" ||
      !VALID_LEVELS.includes(query.level)
    ) {
      return { valid: false, error: `invalid level: '${query.level}'` };
    }
    result.level = query.level;
  }

  // since
  if (query.since !== undefined) {
    if (typeof query.since !== "string") {
      return { valid: false, error: "since must be a valid timestamp" };
    }
    const parsed = new Date(query.since);
    if (isNaN(parsed.getTime())) {
      return { valid: false, error: `invalid since timestamp: '${query.since}'` };
    }
    result.since = parsed;
  }

  // until
  if (query.until !== undefined) {
    if (typeof query.until !== "string") {
      return { valid: false, error: "until must be a valid timestamp" };
    }
    const parsed = new Date(query.until);
    if (isNaN(parsed.getTime())) {
      return { valid: false, error: `invalid until timestamp: '${query.until}'` };
    }
    result.until = parsed;
  }

  if (result.since && result.until && result.until.getTime() < result.since.getTime()) {
    return { valid: false, error: "until must not be earlier than since" };
  }

  // attr.<key> 
  for (const [key, value] of Object.entries(query)) {
    if (key.startsWith("attr.")) {
      const attrKey = key.slice("attr.".length);
      if (attrKey === "" || typeof value !== "string") {
        return { valid: false, error: `invalid attribute filter: '${key}'` };
      }
      result.attributes[attrKey] = value;
    }
  }

  if (query.q !== undefined) {
    if (typeof query.q !== "string") {
      return { valid: false, error: "q must be a string" };
    }
    result.q = query.q;
  }

  // limit
  if (query.limit !== undefined) {
    if (typeof query.limit !== "string" || !/^\d+$/.test(query.limit)) {
      return { valid: false, error: `limit must be a positive integer` };
    }
    const parsedLimit = parseInt(query.limit, 10);
    if (parsedLimit < 1 || parsedLimit > MAX_LIMIT) {
      return {
        valid: false,
        error: `limit must be between 1 and ${MAX_LIMIT}`,
      };
    }
    result.limit = parsedLimit;
  }

  // cursor
  if (query.cursor !== undefined) {
    if (typeof query.cursor !== "string") {
      return { valid: false, error: "invalid cursor" };
    }
    const decoded = decodeCursor(query.cursor);
    if (!decoded) {
      return { valid: false, error: "invalid or malformed cursor" };
    }
    result.cursor = decoded;
  }

  return { valid: true, params: result };
}

export function encodeCursor(timestamp: Date, id: number): string {
  const payload = JSON.stringify({ timestamp: timestamp.toISOString(), id });
  return Buffer.from(payload, "utf-8").toString("base64url");
}

function decodeCursor(cursor: string): { timestamp: Date; id: number } | null {
  try {
    const decoded = Buffer.from(cursor, "base64url").toString("utf-8");
    const parsed = JSON.parse(decoded);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      typeof parsed.timestamp !== "string" ||
      typeof parsed.id !== "number"
    ) {
      return null;
    }
    const ts = new Date(parsed.timestamp);
    if (isNaN(ts.getTime())) {
      return null;
    }
    return { timestamp: ts, id: parsed.id };
  } catch {
    return null;
  }
}