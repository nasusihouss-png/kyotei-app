import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import axios from "axios";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const DEFAULT_HISTORY_DIR = path.resolve(__dirname, "../../data/history");
const pendingBackfills = new Map();

const TECHNIQUE_BY_NUMBER = {
  1: "\u9003\u3052",
  2: "\u5dee\u3057",
  3: "\u307e\u304f\u308a",
  4: "\u307e\u304f\u308a\u5dee\u3057",
  5: "\u629c\u304d",
  6: "\u6075\u307e\u308c"
};

function toNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function toInteger(value, fallback = null) {
  const number = toNumber(value, null);
  return Number.isInteger(number) ? number : fallback;
}

function toBoat(value, fallback = null) {
  const number = toInteger(value, null);
  return number !== null && number >= 1 && number <= 6 ? number : fallback;
}

function firstValue(row = {}, fields = []) {
  for (const field of fields) {
    const value = row?.[field];
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
}

function normalizeRacerId(value) {
  if (value === null || value === undefined || value === "") return "";
  return String(value).normalize("NFKC").trim();
}

function normalizeRacerName(value) {
  if (value === null || value === undefined || value === "") return "";
  return String(value).normalize("NFKC").replace(/[\s\u3000]+/g, "").trim();
}

function racerKey(target = {}) {
  const id = normalizeRacerId(firstValue(target, [
    "racerId",
    "racer_id",
    "racer_number",
    "registrationNo",
    "registration_no",
    "touban",
    "racerNo",
    "playerId"
  ]));
  const name = normalizeRacerName(firstValue(target, [
    "racerName",
    "racer_name",
    "name",
    "player_name",
    "playerName"
  ]));
  return id || name || "";
}

function normalizeTargetRacers(targetRacers = []) {
  const byKey = new Map();
  for (const row of Array.isArray(targetRacers) ? targetRacers : []) {
    const id = normalizeRacerId(firstValue(row, [
      "racerId",
      "racer_id",
      "racer_number",
      "registrationNo",
      "registration_no",
      "touban",
      "racerNo",
      "playerId"
    ]));
    const name = normalizeRacerName(firstValue(row, [
      "racerName",
      "racer_name",
      "name",
      "player_name",
      "playerName"
    ]));
    const key = id || name;
    if (!key || byKey.has(key)) continue;
    byKey.set(key, {
      boat: toBoat(firstValue(row, ["boat", "boatNumber", "lane", "entry"]), null),
      course: toBoat(firstValue(row, ["course", "entryCourse", "lane", "boat"]), null),
      racerId: id || null,
      racerName: firstValue(row, ["racerName", "racer_name", "name", "player_name", "playerName"]) || null,
      racerNameNormalized: name,
      key
    });
  }
  return [...byKey.values()];
}

function findTargetForEntry(entry = {}, targets = []) {
  const entryId = normalizeRacerId(entry?.racerId);
  const entryName = normalizeRacerName(entry?.racerName);
  return targets.find((target) => {
    if (target.racerId && entryId) return target.racerId === entryId;
    if (target.racerId && !entryId && target.racerNameNormalized && entryName) {
      return target.racerNameNormalized === entryName;
    }
    if (!target.racerId && target.racerNameNormalized && entryName) {
      return target.racerNameNormalized === entryName;
    }
    return false;
  }) || null;
}

export function normalizeHistoryDate(value) {
  if (value === null || value === undefined || value === "") return null;
  const text = String(value)
    .normalize("NFKC")
    .trim()
    .replace(/[\u5e74\u6708./]/g, "-")
    .replace(/\u65e5/g, "");
  const match = text.match(/^(\d{4})(\d{2})(\d{2})(?:\D|$)/)
    || text.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:\D|$)/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return date.toISOString().slice(0, 10);
}

export function subtractHistoryMonths(dateText, months) {
  const normalized = normalizeHistoryDate(dateText);
  const date = new Date(`${normalized || ""}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  const day = date.getUTCDate();
  date.setUTCDate(1);
  date.setUTCMonth(date.getUTCMonth() - Math.max(1, Number(months || 6)));
  const lastDay = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0)).getUTCDate();
  date.setUTCDate(Math.min(day, lastDay));
  return date.toISOString().slice(0, 10);
}

function dateKeys(periodStart, periodEnd) {
  const start = new Date(`${periodStart}T00:00:00Z`);
  const end = new Date(`${periodEnd}T00:00:00Z`);
  const rows = [];
  for (const cursor = new Date(start); cursor < end; cursor.setUTCDate(cursor.getUTCDate() + 1)) {
    rows.push(cursor.toISOString().slice(0, 10));
  }
  return rows;
}

function allVenuesMarkerPath({ date, dataDir = DEFAULT_HISTORY_DIR } = {}) {
  const normalized = normalizeHistoryDate(date);
  if (!normalized) return null;
  return path.join(dataDir, `${normalized.replace(/-/g, "")}-allVenues.json`);
}

export function normalizeWinningDecision(value) {
  const numeric = toInteger(value, null);
  if (numeric !== null && TECHNIQUE_BY_NUMBER[numeric]) return TECHNIQUE_BY_NUMBER[numeric];
  const text = String(value || "").normalize("NFKC").replace(/\s+/g, "").toLowerCase();
  if (!text) return null;
  if (text.includes("\u307e\u304f\u308a\u5dee\u3057") || text.includes("\u6372\u308a\u5dee\u3057") || text.includes("makuri-sashi") || text.includes("makurisashi")) return TECHNIQUE_BY_NUMBER[4];
  if (text.includes("\u9003\u3052") || text.includes("nige") || text.includes("escape")) return TECHNIQUE_BY_NUMBER[1];
  if (text.includes("\u5dee\u3057") || text.includes("sashi")) return TECHNIQUE_BY_NUMBER[2];
  if (text.includes("\u307e\u304f\u308a") || text.includes("\u6372\u308a") || text.includes("makuri")) return TECHNIQUE_BY_NUMBER[3];
  if (text.includes("\u629c\u304d") || text.includes("nuki")) return TECHNIQUE_BY_NUMBER[5];
  if (text.includes("\u6075\u307e\u308c") || text.includes("megumare")) return TECHNIQUE_BY_NUMBER[6];
  return null;
}

function resultRaceRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.races)) return payload.races;
  if (Array.isArray(payload?.data)) return payload.data;
  return [];
}

export function normalizeBackfilledResultRace(row = {}) {
  const date = normalizeHistoryDate(firstValue(row, ["date", "race_date", "raceDate"]));
  const venueId = toInteger(firstValue(row, ["venueId", "venue_id", "stadium_number", "race_stadium_number"]), null);
  const raceNo = toInteger(firstValue(row, ["raceNo", "race_no", "number", "race_number"]), null);
  const boats = Array.isArray(row?.boats)
    ? row.boats
    : row?.boats && typeof row.boats === "object"
      ? Object.values(row.boats)
      : Array.isArray(row?.entries)
        ? row.entries
        : [];
  const entries = boats
    .map((boatRow, index) => {
      const boat = toBoat(firstValue(boatRow, ["boat", "boatNumber", "racer_boat_number", "lane", "frame"]), index + 1);
      const course = toBoat(firstValue(boatRow, ["course", "entryCourse", "entry_course", "racer_course_number"]), boat);
      const racerIdValue = firstValue(boatRow, ["racerId", "racer_id", "racer_number", "registrationNo", "registration_no", "touban", "racerNo", "playerId"]);
      const racerNameValue = firstValue(boatRow, ["racerName", "racer_name", "name", "player_name", "playerName"]);
      return {
        boat,
        course,
        courseSource: firstValue(boatRow, ["course", "entryCourse", "entry_course", "racer_course_number"]) == null
          ? "boat_fallback"
          : "actual",
        racerId: racerIdValue === null ? null : String(racerIdValue).normalize("NFKC").trim() || null,
        racerName: racerNameValue === null ? null : String(racerNameValue).trim() || null,
        finishPosition: toInteger(firstValue(boatRow, ["finishPosition", "finish", "rank", "arrival", "resultRank", "racer_place_number", "\u7740\u9806"]), null),
        startTiming: toNumber(firstValue(boatRow, ["startTiming", "start_timing", "racer_start_timing"]), null)
      };
    })
    .filter((entry) => entry.boat !== null && (entry.racerId || entry.racerName));
  const winnerEntry = entries.find((entry) => entry.finishPosition === 1);
  const result = row?.result && typeof row.result === "object" ? row.result : {};
  const winnerBoat = toBoat(
    firstValue(row, ["winnerBoat", "winner_boat", "finish1", "finish_1"])
      ?? firstValue(result, ["winnerBoat", "winner_boat", "finish1", "finish_1"]),
    winnerEntry?.boat ?? null
  );
  const winnerCourse = toBoat(
    firstValue(row, ["winnerCourse", "winner_course"])
      ?? firstValue(result, ["winnerCourse", "winner_course"]),
    winnerEntry?.course ?? winnerBoat
  );
  const winningDecision = normalizeWinningDecision(firstValue(row, [
    "winnerDecision",
    "winningDecision",
    "winningTechnique",
    "kimarite",
    "decision",
    "winMethod",
    "technique_number",
    "race_technique_number",
    "\u6c7a\u307e\u308a\u624b"
  ]) ?? firstValue(result, [
    "winnerDecision",
    "winningDecision",
    "winningTechnique",
    "kimarite",
    "decision",
    "winMethod",
    "\u6c7a\u307e\u308a\u624b"
  ]));
  if (!date || !Number.isInteger(venueId) || !Number.isInteger(raceNo) || entries.length === 0) return null;
  return {
    date,
    venueId,
    raceNo,
    entries,
    result: {
      winnerBoat,
      winnerCourse,
      winningDecision
    }
  };
}

export function getHistoryCachePath({ date, venueId, dataDir = DEFAULT_HISTORY_DIR } = {}) {
  const normalized = normalizeHistoryDate(date);
  if (!normalized || !Number.isInteger(toInteger(venueId, null))) return null;
  return path.join(dataDir, `${normalized.replace(/-/g, "")}-${Number(venueId)}.json`);
}

export function readHistoryCacheFile(filePath) {
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
    return {
      ...parsed,
      races: (Array.isArray(parsed?.races) ? parsed.races : []).map(normalizeBackfilledResultRace).filter(Boolean)
    };
  } catch {
    return null;
  }
}

function writeHistoryCacheFile(filePath, payload) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
  fs.writeFileSync(tempPath, JSON.stringify(payload, null, 2), "utf8");
  if (fs.existsSync(filePath)) fs.rmSync(filePath, { force: true });
  fs.renameSync(tempPath, filePath);
}

function readAllVenueMarker({ date, dataDir = DEFAULT_HISTORY_DIR } = {}) {
  const filePath = allVenuesMarkerPath({ date, dataDir });
  if (!filePath || !fs.existsSync(filePath)) return null;
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function writeAllVenueMarker({ date, dataDir = DEFAULT_HISTORY_DIR, source = null, venueIds = [] } = {}) {
  const filePath = allVenuesMarkerPath({ date, dataDir });
  if (!filePath) return null;
  writeHistoryCacheFile(filePath, {
    version: 1,
    date: normalizeHistoryDate(date),
    allVenues: true,
    fetchedAt: new Date().toISOString(),
    source,
    venueIds
  });
  return filePath;
}

function loadHistoryCacheForDate({ date, dataDir = DEFAULT_HISTORY_DIR } = {}) {
  const normalized = normalizeHistoryDate(date);
  if (!normalized || !fs.existsSync(dataDir)) return { races: [], files: [], errors: [] };
  const compact = normalized.replace(/-/g, "");
  const races = [];
  const files = [];
  const errors = [];
  for (const fileName of fs.readdirSync(dataDir)) {
    if (!new RegExp(`^${compact}-\\d+\\.json$`).test(fileName)) continue;
    const filePath = path.join(dataDir, fileName);
    const cached = readHistoryCacheFile(filePath);
    if (!cached) {
      errors.push({ file: filePath, error: "invalid_history_cache" });
      continue;
    }
    files.push(filePath);
    races.push(...cached.races);
  }
  return { races, files, errors };
}

export function loadHistoryCache({
  periodStart,
  periodEnd,
  dataDir = DEFAULT_HISTORY_DIR
} = {}) {
  if (!fs.existsSync(dataDir)) return { races: [], files: [], errors: [] };
  const races = [];
  const files = [];
  const errors = [];
  for (const fileName of fs.readdirSync(dataDir)) {
    if (!/^\d{8}-\d+\.json$/.test(fileName)) continue;
    const filePath = path.join(dataDir, fileName);
    const cached = readHistoryCacheFile(filePath);
    if (!cached) {
      errors.push({ file: filePath, error: "invalid_history_cache" });
      continue;
    }
    const matching = cached.races.filter((race) =>
      race.date >= periodStart && race.date < periodEnd
    );
    if (matching.length > 0 || (cached.date >= periodStart && cached.date < periodEnd)) {
      files.push(filePath);
      races.push(...matching);
    }
  }
  const byKey = new Map(races.map((race) => [`${race.date}|${race.venueId}|${race.raceNo}`, race]));
  return { races: [...byKey.values()], files, errors };
}

export function historyCacheRacesToRows(races = []) {
  return (Array.isArray(races) ? races : []).flatMap((race) =>
    (Array.isArray(race?.entries) ? race.entries : []).map((entry) => ({
      raceDate: race?.date ?? null,
      venueId: race?.venueId ?? null,
      raceNo: race?.raceNo ?? null,
      ...entry,
      winnerBoat: race?.result?.winnerBoat ?? null,
      winnerCourse: race?.result?.winnerCourse ?? null,
      winnerDecision: race?.result?.winningDecision ?? null
    }))
  );
}

function writeVenueGroupedHistory({ date, races, source, dataDir = DEFAULT_HISTORY_DIR } = {}) {
  const normalizedDate = normalizeHistoryDate(date);
  const byVenue = new Map();
  for (const race of Array.isArray(races) ? races : []) {
    if (!Number.isInteger(toInteger(race?.venueId, null))) continue;
    const rows = byVenue.get(Number(race.venueId)) || [];
    rows.push(race);
    byVenue.set(Number(race.venueId), rows);
  }
  for (const [venueId, venueRaces] of byVenue.entries()) {
    const filePath = getHistoryCachePath({ date: normalizedDate, venueId, dataDir });
    writeHistoryCacheFile(filePath, {
      version: 1,
      date: normalizedDate,
      venueId,
      fetchedAt: new Date().toISOString(),
      source,
      allVenues: true,
      races: venueRaces
    });
  }
  return [...byVenue.keys()].sort((a, b) => a - b);
}

function scanTargetRacerHistory(races = [], targets = []) {
  const targetRows = normalizeTargetRacers(targets);
  const matchedByRacer = Object.fromEntries(targetRows.map((target) => [target.key, 0]));
  let scannedRaceCount = 0;
  let scannedEntryCount = 0;
  let matchedEntryCount = 0;
  const matchedRaceKeys = new Set();
  const scannedVenueIds = new Set();
  for (const race of Array.isArray(races) ? races : []) {
    scannedRaceCount += 1;
    if (Number.isInteger(toInteger(race?.venueId, null))) scannedVenueIds.add(Number(race.venueId));
    for (const entry of Array.isArray(race?.entries) ? race.entries : []) {
      scannedEntryCount += 1;
      const target = findTargetForEntry(entry, targetRows);
      if (!target) continue;
      matchedEntryCount += 1;
      matchedByRacer[target.key] = (matchedByRacer[target.key] || 0) + 1;
      matchedRaceKeys.add(`${race.date}|${race.venueId}|${race.raceNo}`);
    }
  }
  return {
    targetRacerCount: targetRows.length,
    scannedVenueIds,
    scannedRaceCount,
    scannedEntryCount,
    matchedEntryCount,
    matchedRaceCount: matchedRaceKeys.size,
    matchedByRacer
  };
}

async function fetchResultDay(date, { httpGet, timeoutMs }) {
  const compact = date.replace(/-/g, "");
  const year = compact.slice(0, 4);
  const urls = [
    `https://boatraceopenapi.github.io/results/v3/${year}/${compact}.json`,
    `https://boatraceopenapi.github.io/results/v2/${year}/${compact}.json`
  ];
  const errors = [];
  for (const url of urls) {
    try {
      const response = await httpGet(url, {
        timeout: timeoutMs,
        responseType: "json",
        validateStatus: (status) => status >= 200 && status < 300
      });
      return { payload: response?.data, url, errors };
    } catch (error) {
      errors.push({ url, error: String(error?.message || error) });
    }
  }
  return { payload: null, url: null, errors };
}

async function mapWithConcurrency(items, concurrency, task) {
  const results = new Array(items.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (nextIndex < items.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await task(items[index], index);
    }
  });
  await Promise.all(workers);
  return results;
}

async function performHistoryBackfill({
  date,
  venueId,
  months = 6,
  force = false,
  dataDir = DEFAULT_HISTORY_DIR,
  httpGet = axios.get,
  timeoutMs = 15000,
  concurrency = 4
} = {}) {
  const targetDate = normalizeHistoryDate(date);
  const normalizedVenueId = toInteger(venueId, null);
  const periodMonths = Math.max(1, Math.min(24, toInteger(months, 6)));
  const periodStart = subtractHistoryMonths(targetDate, periodMonths);
  if (!targetDate || !periodStart || !Number.isInteger(normalizedVenueId)) {
    throw new Error("date and venueId are required");
  }
  const dates = dateKeys(periodStart, targetDate);
  const outcomes = await mapWithConcurrency(dates, Math.max(1, Math.min(8, toInteger(concurrency, 4))), async (raceDate) => {
    const filePath = getHistoryCachePath({ date: raceDate, venueId: normalizedVenueId, dataDir });
    if (!force && filePath && fs.existsSync(filePath)) {
      const cached = readHistoryCacheFile(filePath);
      if (cached) return { date: raceDate, source: "cache", races: cached.races, filePath, errors: [] };
    }
    const fetched = await fetchResultDay(raceDate, { httpGet, timeoutMs });
    if (!fetched.payload) {
      return { date: raceDate, source: "error", races: [], filePath, errors: fetched.errors };
    }
    const races = resultRaceRows(fetched.payload)
      .map(normalizeBackfilledResultRace)
      .filter((race) => race?.venueId === normalizedVenueId);
    const payload = {
      version: 1,
      date: raceDate,
      venueId: normalizedVenueId,
      fetchedAt: new Date().toISOString(),
      source: fetched.url,
      races
    };
    writeHistoryCacheFile(filePath, payload);
    return { date: raceDate, source: "network", races, filePath, errors: fetched.errors };
  });
  const networkRows = outcomes.filter((row) => row.source === "network");
  const networkRaces = networkRows.flatMap((row) => row.races);
  const errors = outcomes
    .filter((row) => row.source === "error")
    .flatMap((row) => row.errors.map((error) => ({ date: row.date, ...error })));
  const result = {
    ok: errors.length < outcomes.length,
    attempted: true,
    date: targetDate,
    venueId: normalizedVenueId,
    months: periodMonths,
    dateRangeStart: periodStart,
    dateRangeEnd: targetDate,
    dateRangeEndExclusive: true,
    fetchedRaceCount: networkRaces.length,
    fetchedEntryCount: networkRaces.reduce((sum, race) => sum + race.entries.length, 0),
    fetchedResultCount: networkRaces.filter((race) => race.result?.winnerBoat !== null).length,
    skippedCount: outcomes.filter((row) => row.source === "cache" || (row.source === "network" && row.races.length === 0)).length,
    cacheHitCount: outcomes.filter((row) => row.source === "cache").length,
    cacheWriteCount: networkRows.length,
    errors,
    source: "boatraceopenapi_results_v3_v2"
  };
  return result;
}

async function performTargetRacerHistoryBackfill({
  date,
  months = 6,
  force = false,
  targetRacers = [],
  dataDir = DEFAULT_HISTORY_DIR,
  httpGet = axios.get,
  timeoutMs = 15000,
  concurrency = 4
} = {}) {
  const targetDate = normalizeHistoryDate(date);
  const periodMonths = Math.max(1, Math.min(24, toInteger(months, 6)));
  const periodStart = subtractHistoryMonths(targetDate, periodMonths);
  const normalizedTargets = normalizeTargetRacers(targetRacers);
  if (!targetDate || !periodStart) throw new Error("date is required");
  if (normalizedTargets.length === 0) throw new Error("target racers are required for all-venue history backfill");
  const dates = dateKeys(periodStart, targetDate);
  const outcomes = await mapWithConcurrency(
    dates,
    Math.max(1, Math.min(8, toInteger(concurrency, 4))),
    async (raceDate) => {
      const marker = !force ? readAllVenueMarker({ date: raceDate, dataDir }) : null;
      if (marker) {
        const cached = loadHistoryCacheForDate({ date: raceDate, dataDir });
        const scan = scanTargetRacerHistory(cached.races, normalizedTargets);
        return {
          date: raceDate,
          source: "cache",
          races: cached.races,
          venueIds: [...scan.scannedVenueIds],
          errors: cached.errors,
          scan
        };
      }
      const fetched = await fetchResultDay(raceDate, { httpGet, timeoutMs });
      if (!fetched.payload) {
        return {
          date: raceDate,
          source: "error",
          races: [],
          venueIds: [],
          errors: fetched.errors,
          scan: scanTargetRacerHistory([], normalizedTargets)
        };
      }
      const races = resultRaceRows(fetched.payload)
        .map(normalizeBackfilledResultRace)
        .filter(Boolean);
      const venueIds = writeVenueGroupedHistory({ date: raceDate, races, source: fetched.url, dataDir });
      writeAllVenueMarker({ date: raceDate, dataDir, source: fetched.url, venueIds });
      const scan = scanTargetRacerHistory(races, normalizedTargets);
      return {
        date: raceDate,
        source: "network",
        races,
        venueIds,
        errors: fetched.errors,
        scan
      };
    }
  );
  const errors = outcomes
    .filter((row) => row.source === "error")
    .flatMap((row) => row.errors.map((error) => ({ date: row.date, ...error })));
  const aggregateMatchedByRacer = Object.fromEntries(normalizedTargets.map((target) => [target.key, 0]));
  const scannedVenueIds = new Set();
  let scannedRaceCount = 0;
  let scannedEntryCount = 0;
  let matchedEntryCount = 0;
  let matchedRaceCount = 0;
  for (const outcome of outcomes) {
    for (const venueId of outcome.venueIds || []) scannedVenueIds.add(Number(venueId));
    scannedRaceCount += outcome.scan?.scannedRaceCount || 0;
    scannedEntryCount += outcome.scan?.scannedEntryCount || 0;
    matchedEntryCount += outcome.scan?.matchedEntryCount || 0;
    matchedRaceCount += outcome.scan?.matchedRaceCount || 0;
    for (const [key, count] of Object.entries(outcome.scan?.matchedByRacer || {})) {
      aggregateMatchedByRacer[key] = (aggregateMatchedByRacer[key] || 0) + Number(count || 0);
    }
  }
  return {
    ok: errors.length < outcomes.length,
    attempted: true,
    allVenues: true,
    date: targetDate,
    months: periodMonths,
    dateRangeStart: periodStart,
    dateRangeEnd: targetDate,
    dateRangeEndExclusive: true,
    targetRacerCount: normalizedTargets.length,
    targetRacers: normalizedTargets.map((target) => ({
      boat: target.boat,
      course: target.course,
      racerId: target.racerId,
      racerName: target.racerName,
      key: target.key
    })),
    scannedDateCount: outcomes.length,
    scannedVenueCount: scannedVenueIds.size,
    scannedRaceCount,
    scannedEntryCount,
    matchedEntryCount,
    matchedRaceCount,
    matchedByRacer: aggregateMatchedByRacer,
    fetchedRaceCount: outcomes
      .filter((row) => row.source === "network")
      .reduce((sum, row) => sum + (row.scan?.scannedRaceCount || 0), 0),
    fetchedEntryCount: outcomes
      .filter((row) => row.source === "network")
      .reduce((sum, row) => sum + (row.scan?.scannedEntryCount || 0), 0),
    fetchedResultCount: outcomes
      .filter((row) => row.source === "network")
      .reduce((sum, row) => sum + row.races.filter((race) => race.result?.winnerBoat !== null).length, 0),
    skippedCount: outcomes.filter((row) => row.source === "cache").length,
    cacheHitCount: outcomes.filter((row) => row.source === "cache").length,
    cacheWriteCount: outcomes.filter((row) => row.source === "network").length,
    errors,
    source: "boatraceopenapi_results_v3_v2_all_venues"
  };
}

export async function runHistoryBackfill(options = {}) {
  const targetDate = normalizeHistoryDate(options?.date);
  const venueId = toInteger(options?.venueId, null);
  const months = Math.max(1, Math.min(24, toInteger(options?.months, 6)));
  const allVenues = options?.allVenues === true;
  const targetKey = allVenues
    ? normalizeTargetRacers(options?.targetRacers).map((target) => target.key).join(",")
    : String(venueId);
  const pendingKey = `${targetDate}|${allVenues ? "all" : venueId}|${months}|${targetKey}`;
  if (pendingBackfills.has(pendingKey)) return pendingBackfills.get(pendingKey);
  const pending = (allVenues
    ? performTargetRacerHistoryBackfill(options)
    : performHistoryBackfill(options)
  ).finally(() => pendingBackfills.delete(pendingKey));
  pendingBackfills.set(pendingKey, pending);
  return pending;
}
