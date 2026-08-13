import { db, pool } from "../db/index.js";
import { logs } from "../db/schema.js";
import type { ValidatedLogEntry } from "../validation/logValidation.js";
import type { LogQueryParams } from "../validation/queryValidation.js";
import { and, or, eq, lt, gte, ilike, sql, desc } from "drizzle-orm";
import type { AggregateQueryParams } from "../validation/queryValidation.js";
import { from as copyFrom } from "pg-copy-streams";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

function csvField(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

const FLUSH_MAX_WAIT_MS = 100;
const FLUSH_MAX_BATCH_SIZE = 5000;
const MAX_QUEUE_SIZE = 200_000;
const MAX_RETRY_ATTEMPTS = 3;

let pendingEntries: ValidatedLogEntry[] = [];
let isFlushing = false;
let flushTimer: NodeJS.Timeout | null = null;

export class QueueFullError extends Error {
  constructor() {
    super("ingest queue is full, try again shortly");
    this.name = "QueueFullError";
  }
}

export function enqueueLogs(entries: ValidatedLogEntry[]): void {
  if (entries.length === 0) return;

  if (pendingEntries.length + entries.length > MAX_QUEUE_SIZE) {
    throw new QueueFullError();
  }

  pendingEntries.push(...entries);

  if (pendingEntries.length >= FLUSH_MAX_BATCH_SIZE) {
    void triggerFlush();
  } else if (!flushTimer) {
    flushTimer = setTimeout(() => void triggerFlush(), FLUSH_MAX_WAIT_MS);
  }
}

async function triggerFlush(): Promise<void> {
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }

  if (isFlushing || pendingEntries.length === 0) return;

  isFlushing = true;
  const batch = pendingEntries;
  pendingEntries = [];

  await flushBatchWithRetry(batch, 0);

  isFlushing = false;

  if (pendingEntries.length >= FLUSH_MAX_BATCH_SIZE) {
    void triggerFlush();
  } else if (pendingEntries.length > 0 && !flushTimer) {
    flushTimer = setTimeout(() => void triggerFlush(), FLUSH_MAX_WAIT_MS);
  }
}

async function flushBatchWithRetry(
  batch: ValidatedLogEntry[],
  attempt: number
): Promise<void> {
  try {
    const client = await pool.connect();
    try {
      const ingestStream = client.query(
        copyFrom(
          `COPY logs (timestamp, level, service, message, attributes) FROM STDIN WITH (FORMAT csv)`
        )
      );

      let csvData = "";
      for (const e of batch) {
        csvData +=
          [
            csvField(e.timestamp.toISOString()),
            csvField(e.level),
            csvField(e.service),
            csvField(e.message),
            csvField(JSON.stringify(e.attributes)),
          ].join(",") + "\n";
      }

      const sourceStream = Readable.from([csvData]);
      await pipeline(sourceStream, ingestStream);
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(
      `Flush attempt ${attempt + 1} failed for batch of ${batch.length}:`,
      err
    );

    if (attempt + 1 >= MAX_RETRY_ATTEMPTS) {
      console.error(
        `Dropping batch of ${batch.length} logs after ${MAX_RETRY_ATTEMPTS} failed attempts.`
      );
      return;
    }

    await new Promise((r) => setTimeout(r, 50 * (attempt + 1)));
    return flushBatchWithRetry(batch, attempt + 1);
  }
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

  if (params.since) {
    conditions.push(gte(logs.timestamp, params.since));
  }
  if (params.until) {
    conditions.push(lt(logs.timestamp, params.until));
  }

  if (params.q) {
    conditions.push(ilike(logs.message, `%${params.q}%`));
  }

  for (const [key, value] of Object.entries(params.attributes)) {
    conditions.push(sql`${logs.attributes}->>${key} = ${value}`);
  }

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
