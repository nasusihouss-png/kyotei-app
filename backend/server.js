import "./db.js";
import express from "express";
import cors from "cors";
import { raceRouter } from "./src/routes/race.js";
import { fetchKyoteiBiyoriRaceData } from "./src/services/kyoteibiyori.js";
import { runHistoryBackfill } from "./src/services/race-history-backfill.js";
import { fetchRaceTendencies } from "./src/services/racer-tendencies.js";
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
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const text = value.trim();
    if (!text || text === "-") return null;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toPositiveInt(value, fallback = null) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

const originalExhibitionCache = new Map();
const originalExhibitionPendingRaceKeys = new Set();
const ORIGINAL_EXHIBITION_CACHE_LIMIT = 200;
const raceConditionsCache = new Map();
const RACE_CONDITIONS_CACHE_LIMIT = 300;

function buildOriginalExhibitionRaceKey({ date, venueId, raceNo }) {
  return `${String(date || "")}|${Number(venueId)}|${Number(raceNo)}`;
}

function buildOpenApiDatePath(date) {
  const normalized = String(date || "").replace(/-/g, "");
  if (/^\d{8}$/.test(normalized)) return `${normalized.slice(0, 4)}/${normalized}.json`;
  return "today.json";
}

function getOpenApiRaceRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.races)) return payload.races;
  if (Array.isArray(payload?.data)) return payload.data;
  const found = [];
  const visit = (value) => {
    if (Array.isArray(value)) {
      value.forEach(visit);
      return;
    }
    if (!value || typeof value !== "object") return;
    if (value.race_stadium_number !== undefined && value.race_number !== undefined) {
      found.push(value);
      return;
    }
    Object.values(value).forEach(visit);
  };
  visit(payload);
  return found;
}

function buildEmptyRaceConditions() {
  return {
    windDirection: null,
    windSpeed: null,
    waveHeight: null,
    weather: null,
    temperature: null,
    waterTemperature: null
  };
}

function normalizeRaceConditionsFromSource(source = {}) {
  const root = source && typeof source === "object" ? source : {};
  const nested = root.conditions || root.raceConditions || {};
  return {
    windDirection:
      nested.windDirection ??
      root.windDirection ??
      root.wind_direction ??
      root.windDir ??
      root.race_wind_direction ??
      root.race_wind_direction_number ??
      null,
    windSpeed: toNullableNumber(nested.windSpeed ?? nested.wind ?? root.windSpeed ?? root.wind_speed ?? root.race_wind ?? root.wind),
    waveHeight: toNullableNumber(nested.waveHeight ?? nested.wave ?? root.waveHeight ?? root.wave_height ?? root.race_wave ?? root.wave),
    weather: nested.weather ?? root.weather ?? root.race_weather ?? root.race_weather_number ?? null,
    temperature: toNullableNumber(nested.temperature ?? root.temperature ?? root.race_temperature),
    waterTemperature: toNullableNumber(nested.waterTemperature ?? root.waterTemperature ?? root.water_temperature ?? root.race_water_temperature)
  };
}

function setRaceConditionsCache(raceKey, payload) {
  raceConditionsCache.set(raceKey, cloneJson(payload));
  while (raceConditionsCache.size > RACE_CONDITIONS_CACHE_LIMIT) {
    const oldestKey = raceConditionsCache.keys().next().value;
    raceConditionsCache.delete(oldestKey);
  }
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function setOriginalExhibitionCache(raceKey, payload) {
  originalExhibitionCache.set(raceKey, cloneJson(payload));
  while (originalExhibitionCache.size > ORIGINAL_EXHIBITION_CACHE_LIMIT) {
    const oldestKey = originalExhibitionCache.keys().next().value;
    originalExhibitionCache.delete(oldestKey);
  }
}

function buildEmptyOriginalExhibitionRows() {
  return Array.from({ length: 6 }, (_, index) => ({
    boat: index + 1,
    lane: index + 1,
    boatNumber: index + 1,
    exST: null,
    exhibitionSt: null,
    exhibitionST: null,
    exTime: null,
    exhibitionTime: null,
    lapTime: null,
    lapTimeRaw: null,
    lapSource: null,
    straightTime: null,
    straightSource: null,
    turnTime: null,
    turnSource: null,
    sourceRaw: {
      exST: null,
      exTime: null,
      lapTime: null,
      straightTime: null,
      turnTime: null
    },
    parsed: {
      exST: null,
      exTime: null,
      lapTime: null,
      straightTime: null,
      turnTime: null
    },
    warnings: [],
    measured: false
  }));
}

const ORIGINAL_EXHIBITION_VALID_RANGES = {
  exST: [-0.3, 0.5],
  exTime: [6, 8.5],
  lapTime: [30, 45],
  straightTime: [5, 9],
  turnTime: [4, 8],
  motor2Rate: [0, 100]
};

function validateOriginalExhibitionValue(field, value, warnings = []) {
  const n = toNullableNumber(value);
  if (n === null) return null;
  const range = ORIGINAL_EXHIBITION_VALID_RANGES[field];
  if (!range) return n;
  if (n < range[0] || n > range[1]) {
    warnings.push(`${field} ${n.toFixed(2)} rejected: out of valid range`);
    if (field === "exST" && n >= 1) {
      warnings.push(`${field} ${n.toFixed(2)} rejected: likely decimal parsing error from .01`);
    }
    return null;
  }
  return n;
}

function buildOriginalExhibitionRows(kyoteiBiyori = null) {
  const byLane = kyoteiBiyori?.byLane instanceof Map ? kyoteiBiyori.byLane : new Map();
  const fieldSources = kyoteiBiyori?.fieldSources && typeof kyoteiBiyori.fieldSources === "object" ? kyoteiBiyori.fieldSources : {};
  return buildEmptyOriginalExhibitionRows().map((base) => {
    const row = byLane.get(base.lane) || {};
    const laneSources = fieldSources[base.lane] || fieldSources[String(base.lane)] || {};
    const parserWarnings = Array.isArray(row?.parserWarnings) ? [...row.parserWarnings] : [];
    const lapTime = validateOriginalExhibitionValue("lapTime", row?.lapTime, parserWarnings);
    const lapTimeRaw = validateOriginalExhibitionValue("lapTime", row?.lapTimeRaw ?? row?.lapRaw ?? row?.lapTime, parserWarnings);
    const straightTime = validateOriginalExhibitionValue("straightTime", row?.straightTime ?? row?.nobiashi ?? row?.__nobiashi, parserWarnings);
    const turnTime = validateOriginalExhibitionValue("turnTime", row?.turnTime ?? row?.mawariashi ?? row?.__mawariashi, parserWarnings);
    const signedExST = toNullableNumber(row?.exhibitionStSignedValue);
    const rawExST = toNullableNumber(row?.exST ?? row?.exhibitionSt ?? row?.exhibitionST);
    const exSTRawCandidate = signedExST ?? (String(row?.exhibitionStFlag || "").toUpperCase() === "F" && rawExST !== null && rawExST > 0 ? -Math.abs(rawExST) : rawExST);
    const exST = validateOriginalExhibitionValue("exST", exSTRawCandidate, parserWarnings);
    const exTimeRaw = toNullableNumber(row?.exTime ?? row?.exhibitionTime);
    const exTime = validateOriginalExhibitionValue("exTime", exTimeRaw !== null && exTimeRaw > 0 ? exTimeRaw : null, parserWarnings);
    const lapSource = row?.lapSource || row?.lapTimeDetail?.source || laneSources?.lapTime || laneSources?.lapTimeRaw || null;
    const straightSource =
      row?.straightTimeDetail?.source ||
      row?.nobiashiDetail?.source ||
      laneSources?.straightTime ||
      laneSources?.nobiashi ||
      laneSources?.__nobiashi ||
      null;
    const turnSource =
      row?.turnTimeDetail?.source ||
      row?.mawariashiDetail?.source ||
      laneSources?.turnTime ||
      laneSources?.mawariashi ||
      laneSources?.__mawariashi ||
      null;
    return {
      ...base,
      exST,
      exhibitionSt: exST,
      exhibitionST: exST,
      exTime,
      exhibitionTime: exTime,
      exhibitionStRaw: row?.exhibitionStRaw ?? null,
      exhibitionStFlag: row?.exhibitionStFlag ?? null,
      exhibitionStSignedValue: toNullableNumber(row?.exhibitionStSignedValue),
      lapTime,
      lapTimeRaw,
      lapSource,
      straightTime,
      straightSource,
      turnTime,
      turnSource,
      sourceRaw: row?.sourceRaw || {
        exST: row?.exhibitionStRaw ?? null,
        exTime: row?.exhibitionTime ?? null,
        lapTime: row?.lapTimeRaw ?? row?.lapTime ?? null,
        straightTime: row?.straightTime ?? row?.nobiashi ?? row?.__nobiashi ?? null,
        turnTime: row?.turnTime ?? row?.mawariashi ?? row?.__mawariashi ?? null
      },
      parsed: {
        exST,
        exTime,
        lapTime,
        straightTime,
        turnTime
      },
      warnings: [...new Set(parserWarnings)],
      measured: lapTime !== null || straightTime !== null || turnTime !== null,
      exSTStatus: exST !== null ? "ok" : "not_measured",
      exTimeStatus: exTime !== null ? "ok" : "not_measured",
      lapStatus: lapTime !== null ? "ok" : "not_measured",
      straightStatus: straightTime !== null ? "ok" : "not_measured",
      turnStatus: turnTime !== null ? "ok" : "not_measured"
    };
  });
}

async function handleRaceExhibitionRequest(req, res) {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  const date = String(req.query?.date || "");
  const venueId = toPositiveInt(req.query?.venueId, null);
  const raceNo = toPositiveInt(req.query?.raceNo, null);
  const timeoutMs = Math.max(1200, Math.min(toPositiveInt(req.query?.timeoutMs, 45000), 45000));
  const forceRaw = String(req.query?.force || req.query?.forceExhibition || req.query?.forceRefresh || "").toLowerCase();
  const forceExhibition = forceRaw === "1" || forceRaw === "true";
  if (!date || !venueId || !raceNo) {
    const debug = {
      date,
      venueId,
      raceNo,
      forceExhibition,
      validation: "date, venueId, and raceNo are required"
    };
    return res.status(400).json({
      ok: false,
      backendConnected: true,
      exhibitionFetchRoute: "none",
      playwrightStarted: false,
      playwrightFinished: false,
      playwrightError: null,
      error: "missing_required_query_params",
      message: "date, venueId, and raceNo are required",
      rows: buildEmptyOriginalExhibitionRows(),
      debug,
      diagnostics: debug
    });
  }

  const raceKey = buildOriginalExhibitionRaceKey({ date, venueId, raceNo });
  if (!forceExhibition) {
    const cachedPayload = originalExhibitionCache.get(raceKey);
    if (cachedPayload) {
      const payload = cloneJson(cachedPayload);
      const debug = {
        ...(payload.debug || payload.diagnostics || {}),
        raceKey,
        cacheHit: true,
        source: "cache",
        exhibitionFetchRoute: "cache"
      };
      payload.fetchedAt = new Date().toISOString();
      payload.exhibitionFetchRoute = "cache";
      payload.source = {
        ...(payload.source || {}),
        source: "cache",
        cache: true,
        forceExhibition: false
      };
      payload.debug = debug;
      payload.diagnostics = debug;
      return res.json(payload);
    }
  }

  if (originalExhibitionPendingRaceKeys.has(raceKey)) {
    const debug = {
      date,
      venueId,
      raceNo,
      raceKey,
      forceExhibition,
      exhibitionFetchRoute: "pending",
      playwrightStarted: false,
      playwrightFinished: false,
      playwrightError: null,
      error: "Exhibition fetch already running. Please wait."
    };
    return res.json({
      ok: false,
      optional: true,
      backendConnected: true,
      exhibitionFetchRoute: "pending",
      playwrightStarted: false,
      playwrightFinished: false,
      playwrightError: null,
      measuredCount: 0,
      fetchedAt: new Date().toISOString(),
      rows: buildEmptyOriginalExhibitionRows(),
      source: {
        kind: "kyoteibiyori_original_exhibition",
        source: "pending",
        cache: false,
        forceExhibition
      },
      debug,
      diagnostics: debug,
      error: "Exhibition fetch already running. Please wait."
    });
  }

  originalExhibitionPendingRaceKeys.add(raceKey);
  try {
    const kyoteiBiyori = await fetchKyoteiBiyoriRaceData({
      date,
      venueId,
      raceNo,
      timeoutMs,
      forceExhibition
    });
    const requestDiagnostics = kyoteiBiyori?.diagnostics || {};
    const rows = buildOriginalExhibitionRows(kyoteiBiyori);
    const measuredCount = rows.filter((row) => row.measured).length;
    const sourceExhibitionTablePreview = rows.map((row) => ({
      boat: row.boat,
      exST: row.exST,
      exTime: row.exTime,
      lapTime: row.lapTime,
      straightTime: row.straightTime,
      turnTime: row.turnTime
    }));
    const parserWarningsPreview = rows.map((row) => ({
      boat: row.boat,
      sourceRaw: row.sourceRaw,
      parsed: row.parsed,
      warnings: row.warnings || []
    }));
    const debug = {
      ...(requestDiagnostics || {}),
      raceKey,
      exhibitionFetchRoute: requestDiagnostics?.exhibitionFetchRoute || "none",
      playwrightStarted: requestDiagnostics?.playwrightStarted === true,
      playwrightFinished: requestDiagnostics?.playwrightFinished === true,
      playwrightError: requestDiagnostics?.playwrightError || null,
      fieldSources: kyoteiBiyori?.fieldSources || {},
      fieldDiagnostics: kyoteiBiyori?.fieldDiagnostics || null,
      fetch: kyoteiBiyori?.diagnostics?.fetch_results || null,
      parse: kyoteiBiyori?.diagnostics?.parse_results || null,
      sourceExhibitionTablePreview,
      parserWarningsPreview
    };
    console.info("[ORIGINAL_EXHIBITION_FETCH]", JSON.stringify({
      date,
      venueId,
      raceNo,
      ok: !!kyoteiBiyori?.ok,
      measuredCount,
      forceExhibition,
      exhibitionFetchRoute: requestDiagnostics?.exhibitionFetchRoute || "none",
      playwrightStarted: requestDiagnostics?.playwrightStarted === true,
      playwrightFinished: requestDiagnostics?.playwrightFinished === true,
      playwrightError: requestDiagnostics?.playwrightError || null,
      source: "kyoteibiyori_original_exhibition",
      actual_fetch_paths: kyoteiBiyori?.diagnostics?.actual_fetch_paths || []
    }));
    const payload = {
      ok: !!kyoteiBiyori?.ok,
      optional: true,
      backendConnected: true,
      exhibitionFetchRoute: debug.exhibitionFetchRoute,
      playwrightStarted: debug.playwrightStarted,
      playwrightFinished: debug.playwrightFinished,
      playwrightError: debug.playwrightError,
      measuredCount,
      fetchedAt: new Date().toISOString(),
      rows,
      source: {
        kind: "kyoteibiyori_original_exhibition",
        source: "network",
        cache: false,
        url: kyoteiBiyori?.url || null,
        triedUrls: Array.isArray(kyoteiBiyori?.triedUrls) ? kyoteiBiyori.triedUrls : [],
        actualFetchPaths: kyoteiBiyori?.diagnostics?.actual_fetch_paths || [],
        requestOritenOk: kyoteiBiyori?.diagnostics?.fetch_results?.request_oriten_kaiseki_custom?.ok === true,
        forceExhibition
      },
      debug,
      diagnostics: debug,
      error: kyoteiBiyori?.ok ? null : kyoteiBiyori?.error || kyoteiBiyori?.fallbackReason || "not_measured_or_not_fetched"
    };
    if (payload.ok && measuredCount > 0) {
      setOriginalExhibitionCache(raceKey, payload);
    }
    res.json(payload);
  } catch (err) {
    const debug = {
      date,
      venueId,
      raceNo,
      raceKey,
      forceExhibition,
      exhibitionFetchRoute: "none",
      playwrightStarted: false,
      playwrightFinished: false,
      playwrightError: String(err?.message || err),
      error: String(err?.message || err)
    };
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
      backendConnected: true,
      exhibitionFetchRoute: debug.exhibitionFetchRoute,
      playwrightStarted: debug.playwrightStarted,
      playwrightFinished: debug.playwrightFinished,
      playwrightError: debug.playwrightError,
      measuredCount: 0,
      fetchedAt: new Date().toISOString(),
      rows: buildEmptyOriginalExhibitionRows(),
      source: {
        kind: "kyoteibiyori_original_exhibition",
        source: "error",
        cache: false,
        url: null,
        triedUrls: [],
        actualFetchPaths: []
      },
      debug,
      diagnostics: debug,
      error: String(err?.message || err)
    });
  } finally {
    originalExhibitionPendingRaceKeys.delete(raceKey);
  }
}

app.get("/api/race/exhibition", handleRaceExhibitionRequest);
app.get("/api/race/original-exhibition", handleRaceExhibitionRequest);

app.get("/api/race/conditions", async (req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  const date = String(req.query?.date || "");
  const venueId = toPositiveInt(req.query?.venueId, null);
  const raceNo = toPositiveInt(req.query?.raceNo, null);
  const forceRaw = String(req.query?.force || "").toLowerCase();
  const force = forceRaw === "1" || forceRaw === "true";
  const raceKey = buildOriginalExhibitionRaceKey({ date, venueId, raceNo });
  if (!date || !venueId || !raceNo) {
    return res.status(400).json({
      ok: false,
      rows: [],
      conditions: buildEmptyRaceConditions(),
      error: "date, venueId, and raceNo are required",
      debug: { date, venueId, raceNo, source: "none" }
    });
  }
  if (!force && raceConditionsCache.has(raceKey)) {
    const payload = cloneJson(raceConditionsCache.get(raceKey));
    return res.json({
      ...payload,
      source: "cache",
      fetchedAt: new Date().toISOString(),
      debug: { ...(payload.debug || {}), cacheHit: true, source: "cache" }
    });
  }
  const url = `https://boatraceopenapi.github.io/previews/v2/${buildOpenApiDatePath(date)}`;
  try {
    const response = await fetch(url, { cache: "no-store" });
    const payload = await response.json().catch(() => null);
    if (!response.ok || !payload) {
      throw new Error(`openapi preview fetch failed (${response.status})`);
    }
    const rows = getOpenApiRaceRows(payload);
    const raceRow = rows.find((row) =>
      Number(row?.race_stadium_number) === Number(venueId) &&
      Number(row?.race_number) === Number(raceNo)
    ) || null;
    const conditions = normalizeRaceConditionsFromSource(raceRow || {});
    const ok = !!raceRow;
    const result = {
      ok,
      rows: raceRow ? [raceRow] : [],
      conditions,
      source: "openapi_previews",
      fetchedAt: new Date().toISOString(),
      debug: {
        raceKey,
        url,
        rowsCount: rows.length,
        matched: !!raceRow,
        source: "openapi_previews"
      },
      error: raceRow ? null : "race conditions not found"
    };
    if (raceRow) setRaceConditionsCache(raceKey, result);
    return res.json(result);
  } catch (error) {
    return res.json({
      ok: false,
      rows: [],
      conditions: buildEmptyRaceConditions(),
      source: "error",
      fetchedAt: new Date().toISOString(),
      debug: {
        raceKey,
        url,
        source: "openapi_previews",
        error: String(error?.message || error)
      },
      error: String(error?.message || error)
    });
  }
});

app.get("/api/race/history-backfill", async (req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  const date = String(req.query?.date || "");
  const targetRace = String(req.query?.targetRace || "");
  const targetRaceMatch = targetRace.match(/^(\d+)-(\d+)$/);
  const venueId = toPositiveInt(req.query?.venueId, targetRaceMatch ? Number(targetRaceMatch[1]) : null);
  const raceNo = toPositiveInt(req.query?.raceNo, targetRaceMatch ? Number(targetRaceMatch[2]) : null);
  const months = Math.max(1, Math.min(toPositiveInt(req.query?.months, 6), 24));
  const forceRaw = String(req.query?.force || "").toLowerCase();
  const force = forceRaw === "1" || forceRaw === "true";
  const allVenuesRaw = String(req.query?.allVenues || "").toLowerCase();
  const allVenues = allVenuesRaw === "1" || allVenuesRaw === "true";
  if (!date || !venueId) {
    return res.status(400).json({
      ok: false,
      attempted: false,
      fetchedRaceCount: 0,
      fetchedEntryCount: 0,
      fetchedResultCount: 0,
      skippedCount: 0,
      errors: [{ error: "date and venueId are required" }]
    });
  }
  if (allVenues && !raceNo) {
    return res.status(400).json({
      ok: false,
      attempted: false,
      allVenues: true,
      fetchedRaceCount: 0,
      fetchedEntryCount: 0,
      fetchedResultCount: 0,
      skippedCount: 0,
      errors: [{ error: "raceNo or targetRace is required for all-venue target-racer backfill" }]
    });
  }
  try {
    if (allVenues) {
      const tendencyResult = await fetchRaceTendencies({
        date,
        venueId,
        raceNo,
        months,
        force,
        backfill: true,
        allVenues: true
      });
      return res.json({
        ...(tendencyResult.historyBackfill || {}),
        tendencyRows: tendencyResult.rows || [],
        tendencyDebug: tendencyResult.debug || {}
      });
    }
    const result = await runHistoryBackfill({ date, venueId, months, force });
    return res.json(result);
  } catch (error) {
    return res.json({
      ok: false,
      attempted: true,
      allVenues,
      date,
      venueId,
      raceNo,
      months,
      fetchedRaceCount: 0,
      fetchedEntryCount: 0,
      fetchedResultCount: 0,
      skippedCount: 0,
      errors: [{ error: String(error?.message || error) }]
    });
  }
});

app.get("/api/race/tendencies", async (req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  const date = String(req.query?.date || "");
  const venueId = toPositiveInt(req.query?.venueId, null);
  const raceNo = toPositiveInt(req.query?.raceNo, null);
  const months = Math.max(1, Math.min(toPositiveInt(req.query?.months, 6), 24));
  const forceRaw = String(req.query?.force || "").toLowerCase();
  const force = forceRaw === "1" || forceRaw === "true";
  const backfillRaw = String(req.query?.backfill || "").toLowerCase();
  const backfill = backfillRaw === "1" || backfillRaw === "true";
  const allVenuesRaw = String(req.query?.allVenues || "").toLowerCase();
  const allVenues = allVenuesRaw === "1" || allVenuesRaw === "true";
  if (!date || !venueId || !raceNo) {
    return res.status(400).json({
      ok: false,
      periodMonths: months,
      rows: [],
      source: "none",
      error: "date, venueId, and raceNo are required",
      debug: { date, venueId, raceNo, months }
    });
  }
  try {
    const result = await fetchRaceTendencies({ date, venueId, raceNo, months, force, backfill, allVenues });
    return res.json(result);
  } catch (error) {
    return res.json({
      ok: false,
      periodMonths: months,
      rows: [],
      source: "error",
      error: String(error?.message || error),
      debug: { date, venueId, raceNo, months, backfill, allVenues }
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
  console.log(`Backend API running on http://localhost:${port}`);
  console.log(`Backend listening on http://${host}:${port}`);
});
