import axios from "axios";
import db from "../../db.js";
import {
  historyCacheRacesToRows,
  loadHistoryCache,
  runHistoryBackfill
} from "./race-history-backfill.js";

const tendencyCache = new Map();
const TENDENCY_CACHE_LIMIT = 300;

const RACER_ID_FIELDS = [
  "racerId",
  "racer_id",
  "registration_no",
  "registrationNo",
  "racer_number",
  "touban",
  "racerNo",
  "playerId"
];
const RACER_NAME_FIELDS = ["racerName", "racer_name", "name", "player_name", "playerName"];
const DECISION_FIELDS = [
  "winnerDecision",
  "decisionType",
  "winningTechnique",
  "winning_technique",
  "kimarite",
  "decision",
  "decision_type",
  "winMethod",
  "\u6c7a\u307e\u308a\u624b"
];
const FINISH_POSITION_FIELDS = ["finishPosition", "finish", "rank", "arrival", "resultRank", "\u7740\u9806"];
const WINNER_BOAT_FIELDS = ["winnerBoat", "winner_boat", "finish1", "finish_1"];

function toNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function toInteger(value, fallback = null) {
  const number = toNumber(value, null);
  return Number.isInteger(number) ? number : fallback;
}

function toCourse(value, fallback = null) {
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

function normalizeDateKey(value) {
  if (value === null || value === undefined || value === "") return null;
  const text = String(value)
    .normalize("NFKC")
    .trim()
    .replace(/[年月./]/g, "-")
    .replace(/日/g, "");
  const compactMatch = text.match(/^(\d{4})(\d{2})(\d{2})(?:\D|$)/);
  const separatedMatch = text.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:\D|$)/);
  const match = compactMatch || separatedMatch;
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

function resolveBoat(row = {}, fallback = null) {
  return toCourse(firstValue(row, [
    "boat",
    "boatNumber",
    "racer_boat_number",
    "lane",
    "teiban",
    "frame",
    "entry"
  ]), fallback);
}

function resolveCourse(row = {}, fallbackBoat = null) {
  const actualCourse = toCourse(firstValue(row, [
    "course",
    "actualCourse",
    "entryCourse",
    "entry_course",
    "racer_course_number"
  ]), null);
  if (actualCourse !== null) {
    return { course: actualCourse, courseSource: "actual" };
  }
  const boatFallback = toCourse(firstValue(row, [
    "lane",
    "teiban",
    "boat",
    "frame",
    "boatNumber",
    "racer_boat_number",
    "entry"
  ]), fallbackBoat);
  if (boatFallback !== null) {
    return { course: boatFallback, courseSource: "boat_fallback" };
  }
  return {
    course: null,
    courseSource: "unknown"
  };
}

function roundRate(value) {
  return value === null || value === undefined || !Number.isFinite(Number(value))
    ? null
    : Number(Number(value).toFixed(4));
}

function subtractMonths(dateText, months) {
  const normalizedDate = normalizeDateKey(dateText);
  const date = new Date(`${normalizedDate || ""}T00:00:00Z`);
  if (Number.isNaN(date.getTime())) return null;
  const day = date.getUTCDate();
  date.setUTCDate(1);
  date.setUTCMonth(date.getUTCMonth() - months);
  const lastDayOfTargetMonth = new Date(Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth() + 1,
    0
  )).getUTCDate();
  date.setUTCDate(Math.min(day, lastDayOfTargetMonth));
  return date.toISOString().slice(0, 10);
}

function openApiDatePath(date) {
  const normalized = String(date || "").replace(/-/g, "");
  const today = new Date().toISOString().slice(0, 10);
  if (date === today) return "today.json";
  return /^\d{8}$/.test(normalized) ? `${normalized.slice(0, 4)}/${normalized}.json` : "today.json";
}

function findOpenApiRaceRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.races)) return payload.races;
  if (Array.isArray(payload?.data)) return payload.data;
  const rows = [];
  const visit = (value) => {
    if (Array.isArray(value)) {
      value.forEach(visit);
      return;
    }
    if (!value || typeof value !== "object") return;
    if (value.race_stadium_number !== undefined && value.race_number !== undefined) {
      rows.push(value);
      return;
    }
    Object.values(value).forEach(visit);
  };
  visit(payload);
  return rows;
}

function normalizeTargetRows(rows = [], source = "unknown") {
  return (Array.isArray(rows) ? rows : [])
    .map((row, index) => {
      const boat = resolveBoat(row, index + 1);
      const course = resolveCourse(row, boat);
      return {
        boat,
        course: course.course,
        courseSource: course.courseSource,
        coursePredicted: course.courseSource !== "actual",
        racerId: normalizeRacerId(firstValue(row, RACER_ID_FIELDS)),
        racerName: String(firstValue(row, RACER_NAME_FIELDS) || "").trim(),
        targetSource: source
      };
    })
    .filter((row) =>
      Number.isInteger(row.boat) &&
      row.boat >= 1 &&
      row.boat <= 6 &&
      (row.racerId || normalizeRacerName(row.racerName))
    )
    .sort((a, b) => a.boat - b.boat);
}

function getLocalTargetRows({ date, venueId, raceNo }) {
  return db.prepare(`
    SELECT e.lane, e.registration_no, e.name, e.entry_course
    FROM entries e
    INNER JOIN races r ON r.race_id = e.race_id
    WHERE r.race_date = ? AND r.venue_id = ? AND r.race_no = ?
    ORDER BY e.lane
  `).all(String(date), Number(venueId), Number(raceNo));
}

async function getOpenApiTargetRows({ date, venueId, raceNo }) {
  const url = `https://boatraceopenapi.github.io/programs/v2/${openApiDatePath(date)}`;
  const response = await axios.get(url, {
    timeout: 15000,
    responseType: "json",
    validateStatus: (status) => status >= 200 && status < 300
  });
  const race = findOpenApiRaceRows(response.data).find((row) =>
    Number(row?.race_stadium_number) === Number(venueId) &&
    Number(row?.race_number) === Number(raceNo)
  );
  const boats = Array.isArray(race?.boats)
    ? race.boats
    : race?.boats && typeof race.boats === "object"
      ? Object.entries(race.boats).map(([boat, row]) => ({
          ...(row || {}),
          racer_boat_number: row?.racer_boat_number ?? Number(boat)
        }))
      : [];
  return {
    rows: boats,
    url
  };
}

function classifyDecisionType(value) {
  const text = String(value || "").normalize("NFKC").replace(/\s+/g, "").toLowerCase();
  if (!text) return null;
  if (text.includes("\u307e\u304f\u308a\u5dee\u3057") || text.includes("\u6372\u308a\u5dee\u3057") || text.includes("makuri-sashi") || text.includes("makurisashi")) return "makuriSashi";
  if (text.includes("\u9003\u3052") || text.includes("nige") || text.includes("escape")) return "escape";
  if (text.includes("\u5dee\u3057") || text.includes("sashi")) return "sashi";
  if (text.includes("\u307e\u304f\u308a") || text.includes("\u6372\u308a") || text.includes("makuri")) return "makuri";
  if (text.includes("\u629c\u304d") || text.includes("nuki")) return "nuki";
  if (text.includes("\u6075\u307e\u308c") || text.includes("megumare")) return "megumare";
  return null;
}

function resolveFinishPosition(row = {}, boat = null) {
  const explicit = toInteger(firstValue(row, FINISH_POSITION_FIELDS), null);
  if (explicit !== null && explicit >= 1 && explicit <= 6) return explicit;
  const targetBoat = toCourse(boat, resolveBoat(row, null));
  if (targetBoat === null) return null;
  for (const [index, fields] of [
    [1, ["finish1", "finish_1"]],
    [2, ["finish2", "finish_2"]],
    [3, ["finish3", "finish_3"]]
  ]) {
    if (toCourse(firstValue(row, fields), null) === targetBoat) return index;
  }
  return null;
}

function normalizeHistoryRow(row = {}) {
  const boat = resolveBoat(row, null);
  const preservedCourseSource = ["actual", "boat_fallback", "unknown"].includes(row?.courseSource)
    ? row.courseSource
    : row?.courseSource === "predicted"
      ? "boat_fallback"
      : null;
  const course = preservedCourseSource
    ? { course: toCourse(row?.course, boat), courseSource: preservedCourseSource }
    : resolveCourse(row, boat);
  const winnerBoat = toCourse(firstValue(row, WINNER_BOAT_FIELDS), null);
  const finishPosition = resolveFinishPosition(row, boat);
  const winnerDecision = firstValue(row, DECISION_FIELDS);
  return {
    ...row,
    raceId: firstValue(row, ["raceId", "race_id"]) || null,
    raceDate: normalizeDateKey(firstValue(row, ["raceDate", "race_date", "date", "raceDay", "\u958b\u50ac\u65e5"])),
    venueId: toInteger(firstValue(row, ["venueId", "venue_id", "stadiumNumber", "race_stadium_number"]), null),
    raceNo: toInteger(firstValue(row, ["raceNo", "race_no", "raceNumber", "race_number"]), null),
    racerId: normalizeRacerId(firstValue(row, RACER_ID_FIELDS)),
    racerName: String(firstValue(row, RACER_NAME_FIELDS) || "").trim(),
    racerNameNormalized: normalizeRacerName(firstValue(row, RACER_NAME_FIELDS)),
    boat,
    course: course.course,
    courseSource: course.courseSource,
    finishPosition,
    winnerBoat,
    winnerCourse: toCourse(firstValue(row, ["winnerCourse", "winner_course"]), winnerBoat),
    winnerDecision,
    winningTechnique: winnerDecision,
    decisionClass: classifyDecisionType(winnerDecision),
    avgST: toNumber(firstValue(row, ["avgST", "avg_st", "averageStartTiming", "avgStartTiming"]), null),
    startTiming: toNumber(firstValue(row, [
      "startTiming",
      "start_timing",
      "racer_start_timing",
      "actualStartTiming"
    ]), null),
    targetWon: winnerBoat !== null
      ? winnerBoat === boat
      : finishPosition !== null
        ? finishPosition === 1
        : null,
    resultExists: winnerBoat !== null || finishPosition !== null
  };
}

function dateInHistoryRange(value, periodStart, periodEnd) {
  const date = normalizeDateKey(value);
  return date !== null && date >= periodStart && date < periodEnd;
}

function getHistoryDataset({ periodStart, periodEnd }) {
  const raceRows = db.prepare(`
    SELECT race_id, race_date, venue_id, race_no
    FROM races
    ORDER BY race_date DESC
  `).all().filter((row) => dateInHistoryRange(row?.race_date, periodStart, periodEnd));
  const rawEntryRows = db.prepare(`
    SELECT
      r.race_id,
      r.race_date,
      r.venue_id,
      r.race_no,
      e.lane,
      e.registration_no,
      e.name,
      e.entry_course,
      e.avg_st
    FROM entries e
    INNER JOIN races r ON r.race_id = e.race_id
    ORDER BY r.race_date DESC
  `).all()
    .filter((row) => dateInHistoryRange(row?.race_date, periodStart, periodEnd));
  const resultRows = db.prepare(`
    SELECT
      r.race_id,
      r.race_date,
      r.venue_id,
      r.race_no,
      re.finish_1,
      re.finish_2,
      re.finish_3,
      re.decision_type
    FROM results re
    INNER JOIN races r ON r.race_id = re.race_id
    ORDER BY r.race_date DESC
  `).all().filter((row) => dateInHistoryRange(row?.race_date, periodStart, periodEnd));
  const raceKey = (row = {}) => [
    normalizeDateKey(firstValue(row, ["raceDate", "race_date", "date"])),
    toInteger(firstValue(row, ["venueId", "venue_id"]), null),
    toInteger(firstValue(row, ["raceNo", "race_no"]), null)
  ].join("|");
  const resultByRaceKey = new Map(resultRows.map((row) => [raceKey(row), row]));
  const resultByRaceId = new Map(resultRows.map((row) => [String(row?.race_id || ""), row]).filter(([key]) => key));
  const bareEntries = rawEntryRows.map(normalizeHistoryRow);
  const entriesByRaceKey = new Map();
  for (const entry of bareEntries) {
    const key = raceKey(entry);
    const entries = entriesByRaceKey.get(key) || [];
    entries.push(entry);
    entriesByRaceKey.set(key, entries);
  }
  const entryRows = bareEntries.map((entry) => {
    const result = resultByRaceKey.get(raceKey(entry)) || resultByRaceId.get(String(entry.raceId || "")) || null;
    if (!result) return entry;
    const winnerBoat = toCourse(firstValue(result, WINNER_BOAT_FIELDS), null);
    const winnerEntry = (entriesByRaceKey.get(raceKey(entry)) || []).find((row) => row.boat === winnerBoat);
    return normalizeHistoryRow({
      ...entry,
      ...result,
      winnerCourse: winnerEntry?.course ?? winnerBoat
    });
  });
  const cachedHistory = loadHistoryCache({ periodStart, periodEnd });
  const cachedEntryRows = historyCacheRacesToRows(cachedHistory.races).map(normalizeHistoryRow);
  const cachedRaceRows = cachedHistory.races.map((race) => ({
    race_date: race.date,
    venue_id: race.venueId,
    race_no: race.raceNo,
    historySource: "history_cache"
  }));
  const cachedResultRows = cachedHistory.races
    .filter((race) => race?.result?.winnerBoat !== null && race?.result?.winnerBoat !== undefined)
    .map((race) => ({
      race_date: race.date,
      venue_id: race.venueId,
      race_no: race.raceNo,
      finish_1: race.result.winnerBoat,
      winner_course: race.result.winnerCourse,
      decision_type: race.result.winningDecision,
      historySource: "history_cache"
    }));
  const mergeNonNull = (current = {}, next = {}) => {
    const merged = { ...current };
    for (const [key, value] of Object.entries(next)) {
      if (value !== null && value !== undefined && value !== "") merged[key] = value;
    }
    return merged;
  };
  const mergeByKey = (rows, keyOf, normalize = (row) => row) => {
    const byKey = new Map();
    for (const row of rows) {
      const key = keyOf(row);
      if (!key || key.startsWith("null|")) continue;
      byKey.set(key, normalize(mergeNonNull(byKey.get(key), row)));
    }
    return [...byKey.values()];
  };
  return {
    raceRows: mergeByKey([...raceRows, ...cachedRaceRows], raceKey),
    entryRows: mergeByKey(
      [...entryRows, ...cachedEntryRows],
      (row) => `${raceKey(row)}|${resolveBoat(row, null)}`,
      normalizeHistoryRow
    ),
    resultRows: mergeByKey([...resultRows, ...cachedResultRows], raceKey),
    cacheFiles: cachedHistory.files,
    cacheErrors: cachedHistory.errors,
    cachedRaceCount: cachedHistory.races.length,
    cachedEntryCount: cachedEntryRows.length,
    cachedResultCount: cachedResultRows.length
  };
}

export function matchHistoryRowsForTarget(target = {}, historyRows = []) {
  const rows = (Array.isArray(historyRows) ? historyRows : []).map(normalizeHistoryRow);
  const targetRacerId = normalizeRacerId(firstValue(target, RACER_ID_FIELDS));
  const targetRacerName = normalizeRacerName(firstValue(target, RACER_NAME_FIELDS));
  if (targetRacerId) {
    const racerIdRows = rows.filter((row) => row.racerId === targetRacerId);
    if (racerIdRows.length > 0) return { rows: racerIdRows, matchMethod: "racerId" };
  }
  if (targetRacerName) {
    const racerNameRows = rows.filter((row) =>
      row.racerNameNormalized === targetRacerName &&
      (!targetRacerId || !row.racerId)
    );
    if (racerNameRows.length > 0) return { rows: racerNameRows, matchMethod: "racerName" };
  }
  return { rows: [], matchMethod: "none" };
}

function buildMatchedHistorySample(target = {}, row = {}, matchMethod = "none") {
  const targetCourse = toCourse(target?.course, toCourse(target?.boat, null));
  const courseMatch = row.course !== null && row.course === targetCourse;
  const includedInAggregation = courseMatch;
  const exclusionReason = includedInAggregation ? null : "course_mismatch";
  const rateExclusionReason = !includedInAggregation
    ? exclusionReason
    : !row.resultExists
      ? "missing_result"
      : row.decisionClass === null
        ? "missing_decision"
        : null;
  return {
    targetBoat: target?.boat ?? null,
    targetCourse,
    targetRacerName: target?.racerName || null,
    targetRacerId: target?.racerId || null,
    matchedRaceDate: row.raceDate,
    matchedVenueId: row.venueId,
    matchedRaceNo: row.raceNo,
    matchedBoat: row.boat,
    matchedCourse: row.course,
    matchedCourseSource: row.courseSource,
    matchedRacerName: row.racerName || null,
    matchedRacerId: row.racerId || null,
    matchMethod,
    courseMatch,
    finishPosition: row.finishPosition,
    winnerBoat: row.winnerBoat,
    winnerCourse: row.winnerCourse,
    winnerDecision: row.winnerDecision || null,
    targetWon: row.targetWon,
    includedInAggregation,
    exclusionReason,
    rateIncludedInAggregation: rateExclusionReason === null,
    rateExclusionReason
  };
}

export function calculateRacerCourseTendency(target, historyRows = [], matchMethod = "none") {
  const course = toCourse(target?.course, toCourse(target?.boat, null));
  const racerHistory = (Array.isArray(historyRows) ? historyRows : []).map(normalizeHistoryRow);
  const matching = racerHistory.filter((row) => row.course === course);
  const withResult = matching.filter((row) => row.resultExists);
  const withTechnique = withResult.filter((row) => row.decisionClass !== null);
  const allCourseWithResult = racerHistory.filter((row) => row.resultExists);
  const allCourseWithTechnique = allCourseWithResult.filter((row) => row.decisionClass !== null);
  const courseStartRows = matching.filter((row) => row.startTiming !== null);
  const allCourseActualStartRows = racerHistory.filter((row) => row.startTiming !== null);
  const allCourseStartRows = allCourseActualStartRows.length > 0
    ? allCourseActualStartRows.map((row) => row.startTiming)
    : racerHistory.filter((row) => row.avgST !== null).map((row) => row.avgST);
  const matchedHistorySamples = racerHistory.map((row) => buildMatchedHistorySample(target, row, matchMethod));
  const counts = {
    escape: 0,
    beatenBySashi: 0,
    beatenByMakuri: 0,
    beatenByMakuriSashi: 0,
    sashi: 0,
    makuri: 0,
    makuriSashi: 0,
    boatFallbackCourseHistoryCount: matching.filter((row) => row.courseSource === "boat_fallback").length,
    unknownCourseHistoryCount: racerHistory.filter((row) => row.courseSource === "unknown").length
  };
  const allCourseCounts = {
    win: 0,
    sashi: 0,
    makuri: 0,
    makuriSashi: 0
  };
  for (const row of withTechnique) {
    const method = row.decisionClass;
    const won = row.targetWon === true;
    if (course === 1) {
      if (won && method === "escape") counts.escape += 1;
      if (row.targetWon === false && method === "sashi") counts.beatenBySashi += 1;
      if (row.targetWon === false && method === "makuri") counts.beatenByMakuri += 1;
      if (row.targetWon === false && method === "makuriSashi") counts.beatenByMakuriSashi += 1;
    } else if (won) {
      if (method === "sashi") counts.sashi += 1;
      if (method === "makuri") counts.makuri += 1;
      if (method === "makuriSashi") counts.makuriSashi += 1;
    }
  }
  for (const row of allCourseWithResult) {
    if (row.targetWon === true) allCourseCounts.win += 1;
  }
  for (const row of allCourseWithTechnique) {
    if (row.targetWon !== true) continue;
    if (row.decisionClass === "sashi") allCourseCounts.sashi += 1;
    if (row.decisionClass === "makuri") allCourseCounts.makuri += 1;
    if (row.decisionClass === "makuriSashi") allCourseCounts.makuriSashi += 1;
  }
  const rate = (count) => matching.length > 0 && withTechnique.length > 0
    ? roundRate(count / matching.length)
    : null;
  const allCourseResultRate = (count) => racerHistory.length > 0 && allCourseWithResult.length > 0
    ? roundRate(count / racerHistory.length)
    : null;
  const allCourseTechniqueRate = (count) => racerHistory.length > 0 && allCourseWithTechnique.length > 0
    ? roundRate(count / racerHistory.length)
    : null;
  const sampleStatus = matching.length >= 10
    ? "ok"
    : matching.length >= 3
      ? "small_sample"
      : matching.length >= 1
        ? "very_small_sample"
        : "insufficient_history";
  return {
    ...target,
    matchMethod,
    sampleStatus,
    allCourseLast6mRaceCount: racerHistory.length,
    courseSpecificLast6mRaceCount: matching.length,
    last6mRaceCount: matching.length,
    allCourseWinRate: allCourseResultRate(allCourseCounts.win),
    allCourseSashiRate: allCourseTechniqueRate(allCourseCounts.sashi),
    allCourseMakuriRate: allCourseTechniqueRate(allCourseCounts.makuri),
    allCourseMakuriSashiRate: allCourseTechniqueRate(allCourseCounts.makuriSashi),
    allCourseAvgST: allCourseStartRows.length > 0
      ? Number((allCourseStartRows.reduce((sum, value) => sum + value, 0) / allCourseStartRows.length).toFixed(3))
      : null,
    escapeRate: course === 1 ? rate(counts.escape) : null,
    beatenBySashiRate: course === 1 ? rate(counts.beatenBySashi) : null,
    beatenByMakuriRate: course === 1 ? rate(counts.beatenByMakuri) : null,
    beatenByMakuriSashiRate: course === 1 ? rate(counts.beatenByMakuriSashi) : null,
    sashiRate: course >= 2 && course <= 4 ? rate(counts.sashi) : null,
    makuriRate: course >= 2 && course <= 4 ? rate(counts.makuri) : null,
    makuriSashiRate: course >= 2 && course <= 4 ? rate(counts.makuriSashi) : null,
    avgST: courseStartRows.length > 0
      ? Number((courseStartRows.reduce((sum, row) => sum + row.startTiming, 0) / courseStartRows.length).toFixed(3))
      : null,
    lateStartRate: null,
    earlyStartRate: null,
    debug: {
      matchMethod,
      matchedHistoryEntryCount: racerHistory.length,
      matchedCourseEntryCount: matching.length,
      matchedResultCount: withResult.length,
      matchedTechniqueCount: withTechnique.length,
      allCourseMatchedResultCount: allCourseWithResult.length,
      allCourseMatchedTechniqueCount: allCourseWithTechnique.length,
      boatFallbackCourseHistoryCount: counts.boatFallbackCourseHistoryCount,
      predictedCourseHistoryCount: counts.boatFallbackCourseHistoryCount,
      unknownCourseHistoryCount: counts.unknownCourseHistoryCount,
      allCourseLast6mRaceCount: racerHistory.length,
      courseSpecificLast6mRaceCount: matching.length,
      matchedHistorySamples,
      actualStartTimingAvailable: allCourseActualStartRows.length > 0
    }
  };
}

function cacheSet(key, value) {
  tendencyCache.set(key, structuredClone(value));
  while (tendencyCache.size > TENDENCY_CACHE_LIMIT) {
    tendencyCache.delete(tendencyCache.keys().next().value);
  }
}

export async function fetchRaceTendencies({
  date,
  venueId,
  raceNo,
  months = 6,
  force = false,
  backfill = false,
  allVenues = false
} = {}) {
  const targetDate = normalizeDateKey(date);
  const periodMonths = Math.max(1, Math.min(24, toInteger(months, 6)));
  const cacheKey = `${targetDate}|${venueId}|${raceNo}|${periodMonths}`;
  if (!force && !backfill && tendencyCache.has(cacheKey)) {
    const cached = structuredClone(tendencyCache.get(cacheKey));
    return {
      ...cached,
      source: "cache",
      debug: { ...(cached.debug || {}), cacheHit: true }
    };
  }
  const periodStart = subtractMonths(targetDate, periodMonths);
  if (!periodStart || !targetDate) throw new Error("invalid tendency date");
  let historyBackfill = {
    attempted: false,
    allVenues: allVenues === true,
    targetRacerCount: 0,
    scannedDateCount: 0,
    scannedVenueCount: 0,
    scannedRaceCount: 0,
    scannedEntryCount: 0,
    matchedEntryCount: 0,
    matchedByRacer: {},
    fetchedRaceCount: 0,
    fetchedEntryCount: 0,
    fetchedResultCount: 0,
    skippedCount: 0,
    errors: []
  };
  let targetRows = normalizeTargetRows(getLocalTargetRows({ date: targetDate, venueId, raceNo }), "local_entries");
  let targetSource = "local_entries";
  let targetUrl = null;
  let targetError = null;
  if (targetRows.length < 6) {
    try {
      const openApi = await getOpenApiTargetRows({ date: targetDate, venueId, raceNo });
      const openApiRows = normalizeTargetRows(openApi.rows, "openapi_programs");
      if (openApiRows.length > 0) {
        const byBoat = new Map(openApiRows.map((row) => [row.boat, row]));
        targetRows.forEach((row) => byBoat.set(row.boat, row));
        targetRows = [...byBoat.values()].sort((a, b) => a.boat - b.boat);
        targetSource = targetRows.some((row) => row.targetSource === "local_entries")
          ? "local_entries+openapi_programs"
          : "openapi_programs";
      }
      targetUrl = openApi.url;
    } catch (error) {
      targetError = String(error?.message || error);
    }
  }
  if (backfill) {
    try {
      historyBackfill = await runHistoryBackfill({
        date: targetDate,
        venueId,
        months: periodMonths,
        force,
        allVenues,
        targetRacers: targetRows
      });
    } catch (error) {
      historyBackfill = {
        ...historyBackfill,
        ok: false,
        attempted: true,
        targetRacerCount: targetRows.length,
        errors: [{ error: String(error?.message || error) }]
      };
    }
  }
  const history = getHistoryDataset({
    periodStart,
    periodEnd: targetDate
  });
  const matchedHistoryCountByRacer = {};
  const matchedHistoryCountByBoat = {};
  const matchedHistorySamples = [];
  const matchedHistorySamplesByBoat = {};
  const courseSpecificMatchedCountByBoat = {};
  const allCourseMatchedCountByBoat = {};
  const sampleStatusByBoat = {};
  const targetRacers = [];
  const rows = targetRows.map((target) => {
    const match = matchHistoryRowsForTarget(target, history.entryRows);
    const racerKey = target.racerId || normalizeRacerName(target.racerName) || `boat-${target.boat}`;
    matchedHistoryCountByRacer[racerKey] = match.rows.length;
    matchedHistoryCountByBoat[String(target.boat)] = match.rows.length;
    targetRacers.push({
      boat: target.boat,
      course: target.course,
      courseSource: target.courseSource,
      racerId: target.racerId || null,
      racerName: target.racerName || null,
      matchMethod: match.matchMethod
    });
    const tendency = calculateRacerCourseTendency(target, match.rows, match.matchMethod);
    courseSpecificMatchedCountByBoat[String(target.boat)] = tendency.courseSpecificLast6mRaceCount;
    allCourseMatchedCountByBoat[String(target.boat)] = tendency.allCourseLast6mRaceCount;
    sampleStatusByBoat[String(target.boat)] = tendency.sampleStatus;
    const samples = tendency?.debug?.matchedHistorySamples || [];
    matchedHistorySamples.push(...samples);
    matchedHistorySamplesByBoat[String(target.boat)] = samples;
    return tendency;
  });
  const allInsufficient = rows.length > 0 && rows.every((row) => row.sampleStatus === "insufficient_history");
  const actualStartTimingAvailable = history.entryRows.some((row) => row?.startTiming !== null);
  const historySummary = {
    historyTotalRaceCount: history.raceRows.length,
    historyTotalEntryCount: history.entryRows.length,
    historyTotalResultCount: history.resultRows.length,
    dateRangeStart: periodStart,
    dateRangeEnd: targetDate
  };
  const response = {
    ok: targetRows.length > 0,
    periodMonths,
    periodStart,
    periodEnd: targetDate,
    source: targetRows.length > 0 ? `local_result_history+${targetSource}` : "unavailable",
    rows,
    actualStartTimingAvailable,
    historyBackfill,
    warning: allInsufficient
      ? "直近6か月の履歴データが不足しているため、戦法率は未算出です。"
      : null,
    error: targetRows.length > 0 ? null : targetError || "target race entries unavailable",
    debug: {
      cacheHit: false,
      targetSource,
      targetUrl,
      targetError,
      targetRowsCount: targetRows.length,
      targetRacers,
      historySource: history.cachedRaceCount > 0
        ? `local races + entries + result + history cache${historyBackfill?.allVenues ? " (all venues)" : ""}`
        : "local races + entries + result",
      historyBackfill,
      historySummary,
      historyTotalRaceCount: history.raceRows.length,
      historyTotalEntryCount: history.entryRows.length,
      historyTotalResultCount: history.resultRows.length,
      historyCacheFileCount: history.cacheFiles.length,
      historyCacheRaceCount: history.cachedRaceCount,
      historyCacheEntryCount: history.cachedEntryCount,
      historyCacheResultCount: history.cachedResultCount,
      historyCacheErrors: history.cacheErrors,
      matchedHistoryCountByRacer,
      matchedHistoryCountByBoat,
      matchedHistorySamples,
      matchedHistorySamplesByBoat,
      courseSpecificMatchedCountByBoat,
      allCourseMatchedCountByBoat,
      sampleStatusByBoat,
      dateRangeStart: periodStart,
      dateRangeEnd: targetDate,
      dateRangeEndExclusive: true,
      actualStartTimingAvailable
    }
  };
  if (response.ok) cacheSet(cacheKey, response);
  return response;
}
