import { db } from "../db/index.js";
import { logs } from "../db/schema.js";
import type { ValidatedLogEntry } from "../validation/logValidation.js";
import type { LogQueryParams } from "../validation/queryValidation.js";
import { and, or, eq, lt, gte, ilike, sql, desc } from "drizzle-orm";
import type { AggregateQueryParams } from "../validation/queryValidation.js";

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




export interface AggregateRow {
  bucketStart: Date;
  group: string | null;
  count: number;
}

export async function queryAggregate(
  params: AggregateQueryParams
): Promise<AggregateRow[]> {
  const conditions = [
    gte(logs.timestamp, params.since),
    lt(logs.timestamp, params.until),
  ];

  if (params.service) {
    conditions.push(eq(logs.service, params.service));
  }
  if (params.level) {
    conditions.push(eq(logs.level, params.level));
  }
  if (params.q) {
    conditions.push(ilike(logs.message, `%${params.q}%`));
  }
  for (const [key, value] of Object.entries(params.attributes)) {
    conditions.push(sql`${logs.attributes}->>${key} = ${value}`);
  }

  const bucketExpr = sql`to_timestamp(floor(extract(epoch FROM ${logs.timestamp}) / ${params.bucketSeconds}) * ${params.bucketSeconds})`;

  const groupExpr =
    params.groupBy === "service"
      ? sql`${logs.service}`
      : params.groupBy === "level"
      ? sql`${logs.level}`
      : sql`NULL::text`;

  const rows = await db
    .select({
      bucketStart: bucketExpr.as("bucket_start"),
      group: groupExpr.as("group_value"),
      count: sql<number>`count(*)`.as("count"),
    })
    .from(logs)
    .where(and(...conditions))
    .groupBy(sql`1`, sql`2`)
    .orderBy(sql`1`);

  return rows.map((row) => ({
    bucketStart: new Date(row.bucketStart as unknown as string),
    group: row.group as string | null,
    count: Number(row.count),
  }));
}