import "./db.js";
import express from "express";
import cors from "cors";
import { raceRouter } from "./src/routes/race.js";
import { runPredictionFeatureLogMigrations } from "./prediction-feature-log.js";

const app = express();
const port = process.env.PORT || 3001;
const host = process.env.HOST || "0.0.0.0";

runPredictionFeatureLogMigrations();

app.use(cors());
app.use(express.json());

app.get("/api/health", (_req, res) => {
  res.json({ ok: true });
});

app.use("/api", raceRouter);

app.use((err, _req, res, _next) => {
  const status = err?.statusCode || 500;
  const payload = {
    error: err?.code || "internal_error",
    message: err?.message || "Unexpected server error"
  };

  if (err?.debug) {
    payload.debug = err.debug;

    console.error("[RACE_PARSE_DEBUG] Parsing failed");
    console.error(`[RACE_PARSE_DEBUG] stage=${err.debug.stage} bodies=${err.debug.foundRacerBodyCount}`);

    if (Array.isArray(err.debug.rows)) {
      for (const row of err.debug.rows) {
        console.error(`[RACE_PARSE_DEBUG] raw row ${row.rowIndex}:`, row.raw);
        console.error(`[RACE_PARSE_DEBUG] parsed row ${row.rowIndex}:`, row.parsed);
        console.error(
          `[RACE_PARSE_DEBUG] row ${row.rowIndex} avgSt source: ${row.raw?.avgStSource || "unknown"}`
        );
      }
    }

    if (Array.isArray(err.debug.failedRows) && err.debug.failedRows.length > 0) {
      for (const failed of err.debug.failedRows) {
        console.error(
          `[RACE_PARSE_DEBUG] row ${failed.rowIndex} missing fields: ${failed.missingFields.join(", ")}`
        );
      }
    }
  } else {
    console.error(err);
  }

  res.status(status).json(payload);
});

app.listen(port, host, () => {
  console.log(`Backend listening on http://${host}:${port}`);
});
