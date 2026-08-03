export type LogLevel = "debug" | "info" | "warn" | "error";

const VALID_LEVELS: LogLevel[] = ["debug", "info", "warn", "error"];
const MAX_FUTURE_MS = 5 * 60 * 1000; 

export interface RawLogEntry {
  timestamp?: unknown;
  level?: unknown;
  service?: unknown;
  message?: unknown;
  attributes?: unknown;
}

export interface ValidatedLogEntry {
  timestamp: Date;
  level: LogLevel;
  service: string;
  message: string;
  attributes: Record<string, string | number | boolean>;
}

export interface ValidationResult {
  valid: boolean;
  entry?: ValidatedLogEntry;
  reason?: string;
}

export function validateLogEntry(raw: unknown): ValidationResult {
  
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return { valid: false, reason: "log entry must be an object" };
  }

  const entry = raw as RawLogEntry;

  //timestamp
  if (entry.timestamp === undefined || entry.timestamp === null) {
    return { valid: false, reason: "timestamp is required" };
  }
  if (typeof entry.timestamp !== "string") {
    return { valid: false, reason: "timestamp must be a string" };
  }
  const parsedDate = new Date(entry.timestamp);
  if (isNaN(parsedDate.getTime())) {
    return { valid: false, reason: `invalid timestamp: '${entry.timestamp}'` };
  }
  const now = Date.now();
  if (parsedDate.getTime() - now > MAX_FUTURE_MS) {
    return { valid: false, reason: "timestamp is more than five minutes in the future" };
  }

  //level
  if (entry.level === undefined || entry.level === null) {
    return { valid: false, reason: "level is required" };
  }
  if (typeof entry.level !== "string" || !VALID_LEVELS.includes(entry.level as LogLevel)) {
    return { valid: false, reason: `invalid level: '${entry.level}'` };
  }

  //service
  if (entry.service === undefined || entry.service === null) {
    return { valid: false, reason: "service is required" };
  }
  if (typeof entry.service !== "string" || entry.service.trim() === "") {
    return { valid: false, reason: "service must be a non-empty string" };
  }

  // message
  if (entry.message === undefined || entry.message === null) {
    return { valid: false, reason: "message is required" };
  }
  if (typeof entry.message !== "string" || entry.message.trim() === "") {
    return { valid: false, reason: "message must be a non-empty string" };
  }

  //attributes 
  let attributes: Record<string, string | number | boolean> = {};
  if (entry.attributes !== undefined && entry.attributes !== null) {
    if (
      typeof entry.attributes !== "object" ||
      Array.isArray(entry.attributes)
    ) {
      return { valid: false, reason: "attributes must be a flat object" };
    }
    for (const [key, value] of Object.entries(entry.attributes)) {
      if (
        typeof value !== "string" &&
        typeof value !== "number" &&
        typeof value !== "boolean"
      ) {
        return {
          valid: false,
          reason: `attribute '${key}' must be a string, number, or boolean`,
        };
      }
    }
    attributes = entry.attributes as Record<string, string | number | boolean>;
  }

  return {
    valid: true,
    entry: {
      timestamp: parsedDate,
      level: entry.level as LogLevel,
      service: entry.service,
      message: entry.message,
      attributes,
    },
  };
}