import express from "express";
import "dotenv/config";
import { logsRouter } from "./routes/logs.js";
import { db } from "./db/index.js";
import { sql } from "drizzle-orm";

const app = express();
const PORT = process.env.PORT ? parseInt(process.env.PORT) : 8080;

app.use(express.json());

let isReady = false;

app.get("/health", (req, res) => {
  if (isReady) {
    res.status(200).json({ status: "ok" });
  } else {
    res.status(503).json({ status: "not ready" });
  }
});

app.use(logsRouter);

app.use(
  (
    err: unknown,
    req: express.Request,
    res: express.Response,
    next: express.NextFunction
  ) => {
    if (err instanceof SyntaxError && "body" in err) {
      return res.status(400).json({ error: "malformed JSON in request body" });
    }
    console.error(err);
    res.status(500).json({ error: "internal server error" });
  }
);

async function start() {
  try {
    await db.execute(sql`SELECT 1`);
    isReady = true;
    console.log("Database connection established.");

    app.listen(PORT, () => {
      console.log(`Server listening on port ${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
}

start();