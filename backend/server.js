import "./db.js";
import express from "express";
import cors from "cors";
import { raceRouter } from "./src/routes/race.js";
import { fetchKyoteiBiyoriRaceData } from "./src/services/kyoteibiyori.js";
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

function toNullableNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toPositiveInt(value, fallback = null) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

function buildEmptyOriginalExhibitionRows() {
  return Array.from({ length: 6 }, (_, index) => ({
    lane: index + 1,
    boatNumber: index + 1,
    lapTime: null,
    lapTimeRaw: null,
    lapSource: null,
    straightTime: null,
    straightSource: null,
    measured: false
  }));
}

function buildOriginalExhibitionRows(kyoteiBiyori = null) {
  const byLane = kyoteiBiyori?.byLane instanceof Map ? kyoteiBiyori.byLane : new Map();
  const fieldSources = kyoteiBiyori?.fieldSources && typeof kyoteiBiyori.fieldSources === "object" ? kyoteiBiyori.fieldSources : {};
  return buildEmptyOriginalExhibitionRows().map((base) => {
    const row = byLane.get(base.lane) || {};
    const laneSources = fieldSources[base.lane] || fieldSources[String(base.lane)] || {};
    const lapTime = toNullableNumber(row?.lapTime);
    const lapTimeRaw = toNullableNumber(row?.lapTimeRaw ?? row?.lapRaw ?? row?.lapTime);
    const straightTime = toNullableNumber(row?.straightTime ?? row?.nobiashi ?? row?.__nobiashi);
    const lapSource = row?.lapSource || row?.lapTimeDetail?.source || laneSources?.lapTime || laneSources?.lapTimeRaw || null;
    const straightSource =
      row?.straightTimeDetail?.source ||
      row?.nobiashiDetail?.source ||
      laneSources?.straightTime ||
      laneSources?.nobiashi ||
      laneSources?.__nobiashi ||
      null;
    return {
      ...base,
      lapTime,
      lapTimeRaw,
      lapSource,
      straightTime,
      straightSource,
      measured: lapTime !== null || straightTime !== null,
      lapStatus: lapTime !== null ? "ok" : "not_measured",
      straightStatus: straightTime !== null ? "ok" : "not_measured"
    };
  });
}

app.get("/api/race/original-exhibition", async (req, res) => {
  const date = String(req.query?.date || "");
  const venueId = toPositiveInt(req.query?.venueId, null);
  const raceNo = toPositiveInt(req.query?.raceNo, null);
  const timeoutMs = Math.max(1200, Math.min(toPositiveInt(req.query?.timeoutMs, 3500), 6000));
  if (!date || !venueId || !raceNo) {
    return res.status(400).json({
      ok: false,
      error: "missing_required_query_params",
      message: "date, venueId, and raceNo are required",
      rows: buildEmptyOriginalExhibitionRows()
    });
  }

  try {
    const kyoteiBiyori = await fetchKyoteiBiyoriRaceData({
      date,
      venueId,
      raceNo,
      timeoutMs
    });
    const rows = buildOriginalExhibitionRows(kyoteiBiyori);
    const measuredCount = rows.filter((row) => row.measured).length;
    console.info("[ORIGINAL_EXHIBITION_FETCH]", JSON.stringify({
      date,
      venueId,
      raceNo,
      ok: !!kyoteiBiyori?.ok,
      measuredCount,
      source: "kyoteibiyori_original_exhibition",
      actual_fetch_paths: kyoteiBiyori?.diagnostics?.actual_fetch_paths || []
    }));
    res.json({
      ok: !!kyoteiBiyori?.ok,
      optional: true,
      measuredCount,
      fetchedAt: new Date().toISOString(),
      rows,
      source: {
        kind: "kyoteibiyori_original_exhibition",
        url: kyoteiBiyori?.url || null,
        triedUrls: Array.isArray(kyoteiBiyori?.triedUrls) ? kyoteiBiyori.triedUrls : [],
        actualFetchPaths: kyoteiBiyori?.diagnostics?.actual_fetch_paths || [],
        requestOritenOk: kyoteiBiyori?.diagnostics?.fetch_results?.request_oriten_kaiseki_custom?.ok === true
      },
      diagnostics: {
        fieldSources: kyoteiBiyori?.fieldSources || {},
        fieldDiagnostics: kyoteiBiyori?.fieldDiagnostics || null,
        fetch: kyoteiBiyori?.diagnostics?.fetch_results || null,
        parse: kyoteiBiyori?.diagnostics?.parse_results || null
      },
      error: kyoteiBiyori?.ok ? null : kyoteiBiyori?.error || kyoteiBiyori?.fallbackReason || "not_measured_or_not_fetched"
    });
  } catch (err) {
    console.info("[ORIGINAL_EXHIBITION_FETCH]", JSON.stringify({
      date,
      venueId,
      raceNo,
      ok: false,
      source: "kyoteibiyori_original_exhibition",
      error: String(err?.message || err)
    }));
    res.json({
      ok: false,
      optional: true,
      measuredCount: 0,
      fetchedAt: new Date().toISOString(),
      rows: buildEmptyOriginalExhibitionRows(),
      source: {
        kind: "kyoteibiyori_original_exhibition",
        url: null,
        triedUrls: [],
        actualFetchPaths: []
      },
      diagnostics: null,
      error: String(err?.message || err)
    });
  }
});

app.use("/api", raceRouter);

app.use((err, _req, res, _next) => {
  const status = err?.statusCode || 500;
  const debugMode =
    process.env.NODE_ENV !== "production" ||
    process.env.DEBUG_API_ERRORS === "1";
  const payload = {
    ok: false,
    status,
    error: err?.code || "internal_error",
    message: err?.message || "Unexpected server error",
    where: err?.where || "server",
    route: err?.route || null
  };

  if (err?.details && typeof err.details === "object") {
    payload.details = err.details;
  }

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

  if (debugMode && err?.stack) {
    payload.stack = String(err.stack);
  }

  res.status(status).json(payload);
});

app.listen(port, host, () => {
  console.log(`Backend listening on http://${host}:${port}`);
});
