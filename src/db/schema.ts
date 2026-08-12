import { pgTable, bigserial, timestamp, text, jsonb, index } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

export const logs = pgTable(
  "logs",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    timestamp: timestamp("timestamp", { withTimezone: true }).notNull(),
    level: text("level").notNull(),
    service: text("service").notNull(),
    message: text("message").notNull(),
    attributes: jsonb("attributes").notNull().default({}),
  },
  (table) => [
    index("idx_logs_timestamp_id").on(table.timestamp.desc(), table.id.desc()),
    index("idx_logs_service_timestamp").on(table.service, table.timestamp.desc()),
    index("idx_logs_level_timestamp").on(table.level, table.timestamp.desc()),
    index("idx_logs_attr_user_id").using("btree", sql`(${table.attributes}->>'user_id')`),
    index("idx_logs_attr_region").using("btree", sql`(${table.attributes}->>'region')`),
  ]
);