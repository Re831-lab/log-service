import { Router, type Request, type Response } from "express";
import { validateLogEntry, type ValidatedLogEntry } from "../validation/logValidation.js";
import { validateQueryParams, encodeCursor } from "../validation/queryValidation.js";
import { insertLogs, queryLogs } from "../repositories/logRepository.js";
import { validateAggregateParams } from "../validation/queryValidation.js";
import { queryAggregate } from "../repositories/logRepository.js";

export const logsRouter = Router();

interface RejectedEntry {
  index: number;
  reason: string;
}

logsRouter.post("/logs", async (req: Request, res: Response) => {
  const body = req.body;

  if (
    typeof body !== "object" ||
    body === null ||
    !Array.isArray(body.logs)
  ) {
    return res.status(400).json({
      error: "request body must be an object with a 'logs' array",
    });
  }

  const rawLogs: unknown[] = body.logs;
  const validEntries: ValidatedLogEntry[] = [];
  const rejected: RejectedEntry[] = [];
  const now = Date.now();

  rawLogs.forEach((rawLog, index) => {
    const result = validateLogEntry(rawLog, now);
    if (result.valid && result.entry) {
      validEntries.push(result.entry);
    } else {
      rejected.push({ index, reason: result.reason ?? "invalid log entry" });
    }
  });

  if (validEntries.length === 0) {
    return res.status(400).json({
      accepted: 0,
      rejected,
    });
  }

  try {
    await insertLogs(validEntries);
  } catch (err) {
    console.error("Failed to insert logs:", err);
    return res.status(500).json({ error: "failed to persist logs" });
  }

  return res.status(200).json({
    accepted: validEntries.length,
    rejected,
  });
});

logsRouter.get("/logs", async (req: Request, res: Response) => {
  const validation = validateQueryParams(req.query as Record<string, unknown>);

  if (!validation.valid || !validation.params) {
    return res.status(400).json({ error: validation.error });
  }

  const params = validation.params;
  const rows = await queryLogs(params);

  const hasMore = rows.length > params.limit;
  const pageRows = hasMore ? rows.slice(0, params.limit) : rows;

  const nextCursor = hasMore
    ? encodeCursor(
        pageRows[pageRows.length - 1].timestamp,
        pageRows[pageRows.length - 1].id
      )
    : null;

  return res.status(200).json({
    logs: pageRows.map((row) => ({
      id: String(row.id),
      timestamp: row.timestamp.toISOString(),
      level: row.level,
      service: row.service,
      message: row.message,
      attributes: row.attributes,
    })),
    next_cursor: nextCursor,
  });
});

logsRouter.get("/logs/aggregate", async (req: Request, res: Response) => {
  const validation = validateAggregateParams(req.query as Record<string, unknown>);

  if (!validation.valid || !validation.params) {
    return res.status(400).json({ error: validation.error });
  }

  const rows = await queryAggregate(validation.params);

  return res.status(200).json({
    buckets: rows.map((row) => ({
      start: row.bucketStart.toISOString(),
      group: row.group,
      count: row.count,
    })),
  });
});