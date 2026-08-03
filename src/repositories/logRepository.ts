import { db } from "../db/index.js";
import { logs } from "../db/schema.js";
import type { ValidatedLogEntry } from "../validation/logValidation.js";
import type { LogQueryParams } from "../validation/queryValidation.js";
import { and, or, eq, lt, gte, ilike, sql, desc } from "drizzle-orm";

export async function insertLogs(entries: ValidatedLogEntry[]): Promise<void> {
  if (entries.length === 0) {
    return;
  }

  await db.insert(logs).values(
    entries.map((entry) => ({
      timestamp: entry.timestamp,
      level: entry.level,
      service: entry.service,
      message: entry.message,
      attributes: entry.attributes,
    }))
  );
}

export interface LogRow {
  id: number;
  timestamp: Date;
  level: string;
  service: string;
  message: string;
  attributes: Record<string, unknown>;
}

export async function queryLogs(params: LogQueryParams): Promise<LogRow[]> {
  const conditions = [];

  
  if (params.service) {
    conditions.push(eq(logs.service, params.service));
  }
  if (params.level) {
    conditions.push(eq(logs.level, params.level));
  }

  // since inclusive، until exclusive
  if (params.since) {
    conditions.push(gte(logs.timestamp, params.since));
  }
  if (params.until) {
    conditions.push(lt(logs.timestamp, params.until));
  }

  // case-insensitive 
  if (params.q) {
    conditions.push(ilike(logs.message, `%${params.q}%`));
  }

  // attr.<key>=value 
  for (const [key, value] of Object.entries(params.attributes)) {
    conditions.push(sql`${logs.attributes}->>${key} = ${value}`);
  }

  // Cursor pagination:
  if (params.cursor) {
    const { timestamp, id } = params.cursor;
    conditions.push(
      or(
        lt(logs.timestamp, timestamp),
        and(eq(logs.timestamp, timestamp), lt(logs.id, id))
      )
    );
  }

  const whereClause = conditions.length > 0 ? and(...conditions) : undefined;

  const rows = await db
    .select()
    .from(logs)
    .where(whereClause)
    .orderBy(desc(logs.timestamp), desc(logs.id))
    .limit(params.limit + 1); 

  return rows as LogRow[];
}