import {
  applyRaceFlowScenarioAdjustments,
  applyRaceFlowTicketDecisionCompatibility,
  buildRaceFlowScenarioModel,
  buildRaceFlowScenarioTickets
} from "./race-flow-scenario-engine.js";
import {
  DEFAULT_SCORING_CONFIG as BASE_DEFAULT_SCORING_CONFIG,
  getVenueScoringConfig,
  mergeScoringConfig,
  motorStrengthLabel,
  weightedAverageFromWeights
} from "./scoring-config.js";

export const DEFAULT_SCORING_CONFIG = BASE_DEFAULT_SCORING_CONFIG;

const BOATS = [1, 2, 3, 4, 5, 6];
const CLASS_SCORE = { 1: 1, 2: 0.72, 3: 0.44, 4: 0.24, A1: 1, A2: 0.72, B1: 0.44, B2: 0.24 };

function finiteNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function percent01(value, fallback = 0.5) {
  const n = finiteNumber(value, null);
  if (n === null) return fallback;
  return clamp(n / 100, 0, 1);
}

function pointRate01(value, fallback = 0.5) {
  const n = finiteNumber(value, null);
  if (n === null) return fallback;
  return clamp((n - 1) / 7, 0, 1);
}

function startTimingScore(value, fallback = 0.5) {
  const st = finiteNumber(value, null);
  if (st === null) return fallback;
  return clamp((0.28 - st) / 0.22, 0, 1);
}

function getBoatNumber(row = {}) {
  return finiteNumber(row.racer_boat_number ?? row.boatNumber ?? row.lane ?? row.boat, null);
}

function getPreviewBoatRows(preview = {}) {
  const boats = preview?.boats;
  if (Array.isArray(boats)) return boats;
  if (boats && typeof boats === "object") {
    return Object.entries(boats).map(([boatKey, row]) => ({
      ...(row || {}),
      racer_boat_number: row?.racer_boat_number ?? finiteNumber(boatKey, null),
      boatKey
    }));
  }
  return [];
}

function byBoatNumber(rows = []) {
  const map = new Map();
  for (const row of rows) {
    const boat = getBoatNumber(row);
    if (Number.isInteger(boat) && boat >= 1 && boat <= 6) map.set(boat, row);
  }
  return map;
}

function getPreviewBoatByNumber(preview = {}, boatNumber) {
  const boats = preview?.boats;
  if (!boats) return null;
  if (Array.isArray(boats)) {
    return boats.find((row) => getBoatNumber(row) === boatNumber) || null;
  }
  if (typeof boats === "object") {
    const direct = boats[String(boatNumber)];
    if (direct) {
      return {
        ...direct,
        racer_boat_number: direct?.racer_boat_number ?? boatNumber,
        boatKey: String(boatNumber)
      };
    }
  }
  return null;
}

function validCourse(value) {
  const n = finiteNumber(value, null);
  return Number.isInteger(n) && n >= 1 && n <= 6;
}

function validExhibitionTime(value) {
  const n = finiteNumber(value, null);
  return n !== null && n > 0;
}

function validStartTiming(value) {
  const n = parseStartTimingValue(value);
  return n !== null && n > -0.3 && n < 1;
}

function parseStartTimingValue(value) {
  const direct = finiteNumber(value, null);
  if (direct !== null) return direct;
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text || text === "-") return null;
  const normalized = text
    .replace(/[０-９]/g, (char) => String.fromCharCode(char.charCodeAt(0) - 0xfee0))
    .replace(/[．]/g, ".")
    .replace(/\s+/g, "");
  const match = normalized.match(/^([FL])?([+-]?(?:\d+(?:\.\d+)?|\.\d+))/i);
  if (!match) return null;
  const flag = String(match[1] || "").toUpperCase();
  const numericText = match[2].startsWith(".") ? `0${match[2]}` : match[2];
  const num = Number(numericText);
  if (!Number.isFinite(num)) return null;
  return flag === "F" && num > 0 ? -Math.abs(num) : num;
}

function firstStartTiming(...values) {
  for (const value of values) {
    const num = parseStartTimingValue(value);
    if (num !== null) return num;
  }
  return null;
}

function firstExhibitionTime(...values) {
  for (const value of values) {
    const num = finiteNumber(value, null);
    if (num !== null && num > 0) return num;
  }
  return null;
}

function getOpenApiPreviewField(row = {}, field) {
  if (!row || typeof row !== "object") return null;
  return row[field];
}

export function inspectPreviewExhibitionStatus(preview = {}) {
  const rows = getPreviewBoatRows(preview);
  const perBoat = {};
  for (const boat of BOATS) {
    const row = getPreviewBoatByNumber(preview, boat) || byBoatNumber(rows).get(boat) || null;
    const rawExhibitionTime = getOpenApiPreviewField(row, "racer_exhibition_time");
    const rawCourse = getOpenApiPreviewField(row, "racer_course_number");
    perBoat[String(boat)] = {
      hasPreviewRow: !!row,
      racer_exhibition_time: rawExhibitionTime ?? null,
      racer_course_number: rawCourse ?? null,
      racer_start_timing: getOpenApiPreviewField(row, "racer_start_timing") ?? null
    };
  }
  const completeShape = rows.length >= 6;
  const allExhibitionTimeZeroOrNull = completeShape && BOATS.every((boat) => {
    const raw = perBoat[String(boat)]?.racer_exhibition_time;
    return raw === null || raw === undefined || raw === "" || Number(raw) === 0;
  });
  const allCourseNull = completeShape && BOATS.every((boat) => {
    const raw = perBoat[String(boat)]?.racer_course_number;
    return raw === null || raw === undefined || raw === "";
  });
  return {
    completeShape,
    allExhibitionTimeZeroOrNull,
    allCourseNull,
    exhibitionNotRun: completeShape && allExhibitionTimeZeroOrNull && allCourseNull,
    perBoat
  };
}

export function isExhibitionAvailable(preview = {}) {
  preview = preview || {};
  if (inspectPreviewExhibitionStatus(preview).exhibitionNotRun) return false;
  const rows = getPreviewBoatRows(preview);
  if (rows.length < 6) return false;
  const timeComplete = BOATS.every((boat) => {
    const row = rows.find((item) => getBoatNumber(item) === boat);
    return row && validExhibitionTime(getOpenApiPreviewField(row, "racer_exhibition_time"));
  });
  const courseComplete = BOATS.every((boat) => {
    const row = rows.find((item) => getBoatNumber(item) === boat);
    return row && validCourse(getOpenApiPreviewField(row, "racer_course_number"));
  });
  const stComplete = BOATS.every((boat) => {
    const row = rows.find((item) => getBoatNumber(item) === boat);
    return row && validStartTiming(getOpenApiPreviewField(row, "racer_start_timing"));
  });
  return timeComplete || courseComplete || stComplete;
}

export function buildExhibitionFeatures(preview = {}) {
  preview = preview || {};
  const rows = getPreviewBoatRows(preview);
  const map = byBoatNumber(rows);
  const feature = {
    status: "pre_exhibition",
    entryCourseByBoat: null,
    exhibitionTimeByBoat: null,
    exhibitionStartByBoat: null,
    weather: null,
    usedFields: [],
    sourceByField: {
      entry_course: "openapi_previews.racer_course_number",
      exhibition_time: "openapi_previews.racer_exhibition_time",
      exhibition_st: "openapi_previews.racer_start_timing",
      weather: "openapi_previews.race_weather"
    },
    diagnostics: {
      boatShape: Array.isArray(preview?.boats) ? "array" : preview?.boats && typeof preview.boats === "object" ? "object_by_boat_number" : "missing",
      rowCount: rows.length,
      exhibitionStatus: inspectPreviewExhibitionStatus(preview),
      perBoat: {}
    }
  };
  const conditions = preview?.conditions || preview?.raceConditions || {};
  const wind = finiteNumber(preview.race_wind ?? preview.windSpeed ?? conditions.windSpeed ?? conditions.wind, null);
  const wave = finiteNumber(preview.race_wave ?? preview.waveHeight ?? conditions.waveHeight ?? conditions.wave, null);
  const weather = preview.race_weather ?? conditions.weather ?? null;
  const windDirection = preview.race_wind_direction ?? preview.race_wind_direction_number ?? conditions.windDirection ?? null;
  if (wind !== null || wave !== null || weather != null || windDirection != null) {
    feature.weather = {
      wind,
      windSpeed: wind,
      wave,
      waveHeight: wave,
      weather,
      weatherNumber: finiteNumber(preview.race_weather_number, null),
      windDirection,
      windDirectionNumber: finiteNumber(preview.race_wind_direction_number, null)
    };
    feature.usedFields.push("weather");
  }
  if (feature.diagnostics.exhibitionStatus.exhibitionNotRun) {
    feature.diagnostics.reason = "preview_all_exhibition_time_zero_or_null_and_course_null";
    if (feature.usedFields.length > 0) feature.status = "exhibition_reflected";
    return feature;
  }
  if (rows.length < 6) {
    if (feature.usedFields.length > 0) feature.status = "exhibition_reflected";
    return feature;
  }

  const coursePairs = BOATS.map((boat) => {
    const row = getPreviewBoatByNumber(preview, boat) || map.get(boat);
    const value = getOpenApiPreviewField(row, "racer_course_number");
    return [boat, validCourse(value) ? finiteNumber(value) : null, row || null];
  });
  const timePairs = BOATS.map((boat) => {
    const row = getPreviewBoatByNumber(preview, boat) || map.get(boat);
    const value = getOpenApiPreviewField(row, "racer_exhibition_time");
    return [boat, validExhibitionTime(value) ? finiteNumber(value) : null, row || null];
  });
  const stPairs = BOATS.map((boat) => {
    const row = getPreviewBoatByNumber(preview, boat) || map.get(boat);
    const value = getOpenApiPreviewField(row, "racer_start_timing");
    return [boat, validStartTiming(value) ? parseStartTimingValue(value) : null, row || null];
  });
  for (const boat of BOATS) {
    feature.diagnostics.perBoat[String(boat)] = {
      hasPreviewRow: !!(getPreviewBoatByNumber(preview, boat) || map.get(boat)),
      rawCourse: (getPreviewBoatByNumber(preview, boat) || map.get(boat))?.racer_course_number ?? null,
      rawExhibitionTime: (getPreviewBoatByNumber(preview, boat) || map.get(boat))?.racer_exhibition_time ?? null,
      rawStartTiming: (getPreviewBoatByNumber(preview, boat) || map.get(boat))?.racer_start_timing ?? null
    };
  }

  if (coursePairs.some(([, value]) => value !== null)) {
    feature.entryCourseByBoat = Object.fromEntries(coursePairs.map(([boat, value]) => [boat, value]));
    feature.usedFields.push("entry_course");
  }
  if (timePairs.some(([, value]) => value !== null)) {
    feature.exhibitionTimeByBoat = Object.fromEntries(timePairs.map(([boat, value]) => [boat, value]));
    feature.usedFields.push("exhibition_time");
  }
  if (stPairs.some(([, value]) => value !== null)) {
    feature.exhibitionStartByBoat = Object.fromEntries(stPairs.map(([boat, value]) => [boat, value]));
    feature.usedFields.push("exhibition_st");
  }

  if (feature.usedFields.length > 0) feature.status = "exhibition_reflected";
  return feature;
}

function normalizeRaceConditionsForPrediction(program = {}, preview = {}) {
  const fromProgram = program?.conditions || program?.raceConditions || {};
  const fromPreview = preview?.conditions || preview?.raceConditions || {};
  return {
    windDirection:
      fromProgram.windDirection ??
      fromPreview.windDirection ??
      program?.windDirection ??
      program?.race_wind_direction ??
      preview?.race_wind_direction ??
      preview?.race_wind_direction_number ??
      null,
    windSpeed: finiteNumber(
      fromProgram.windSpeed ?? fromProgram.wind ?? fromPreview.windSpeed ?? fromPreview.wind ?? program?.windSpeed ?? program?.race_wind ?? preview?.windSpeed ?? preview?.race_wind,
      null
    ),
    waveHeight: finiteNumber(
      fromProgram.waveHeight ?? fromProgram.wave ?? fromPreview.waveHeight ?? fromPreview.wave ?? program?.waveHeight ?? program?.race_wave ?? preview?.waveHeight ?? preview?.race_wave,
      null
    ),
    weather:
      fromProgram.weather ??
      fromPreview.weather ??
      program?.weather ??
      program?.race_weather ??
      preview?.race_weather ??
      preview?.race_weather_number ??
      null,
    temperature: finiteNumber(fromProgram.temperature ?? fromPreview.temperature ?? program?.temperature ?? preview?.temperature, null),
    waterTemperature: finiteNumber(fromProgram.waterTemperature ?? fromPreview.waterTemperature ?? program?.waterTemperature ?? preview?.waterTemperature, null)
  };
}

function mergeRaceConditionsIntoPreview(preview = null, conditions = {}) {
  const base = preview && typeof preview === "object" ? preview : {};
  return {
    ...base,
    conditions,
    raceConditions: conditions,
    race_wind: base.race_wind ?? conditions.windSpeed,
    race_wave: base.race_wave ?? conditions.waveHeight,
    race_weather: base.race_weather ?? conditions.weather,
    race_wind_direction: base.race_wind_direction ?? conditions.windDirection
  };
}

export function shrinkRate(localRate, sampleSize, priorRate, k = DEFAULT_SCORING_CONFIG.shrinkK) {
  const local = finiteNumber(localRate, null);
  const prior = finiteNumber(priorRate, null);
  const n = Math.max(0, finiteNumber(sampleSize, 0));
  if (local === null && prior === null) return null;
  if (local === null) return prior;
  if (prior === null) return local;
  return ((n * local) + (k * prior)) / (n + k);
}

function normalizeProgramBoats(program = {}) {
  return (Array.isArray(program?.boats) ? program.boats : [])
    .map((row) => {
      const boat = getBoatNumber(row);
      const course = finiteNumber(row.racer_course_number ?? row.entryCourse ?? row.course ?? boat, boat);
      const playerTendency = normalizePlayerTendency(row, boat, course);
      return {
        raw: row,
        boat,
        course,
        name: row.racer_name ?? row.name ?? `Lane-${boat}`,
        racerNumber: row.racer_number ?? row.registrationNo ?? null,
        classNumber: row.racer_class_number ?? row.classNumber ?? null,
        nationalWinRatePoint: finiteNumber(row.racer_national_top_1_percent, null),
        national2Rate: finiteNumber(row.racer_national_top_2_percent, null),
        localWinRatePoint: finiteNumber(row.racer_local_top_1_percent, null),
        local2Rate: finiteNumber(row.racer_local_top_2_percent, null),
        motorNo: row.racer_assigned_motor_number ?? row.motorNo ?? row.motor_no ?? row.motorNumber ?? null,
        motor2Rate: finiteNumber(row.racer_assigned_motor_top_2_percent ?? row.motor2Rate ?? row.motor_2rate, null),
        motorRankAtVenue: finiteNumber(row.motorRankAtVenue ?? row.motor_rank_at_venue ?? row.motorRank ?? row.motor_rank, null),
        motorPercentileAtVenue: finiteNumber(row.motorPercentileAtVenue ?? row.motor_percentile_at_venue ?? row.motorPercentile ?? row.motor_percentile, null),
        motorStrengthLabel: row.motorStrengthLabel ?? row.motor_strength_label ?? null,
        boat2Rate: finiteNumber(row.racer_assigned_boat_top_2_percent, null),
        averageStartTiming: finiteNumber(row.racer_average_start_timing, null),
        exST: firstStartTiming(row.racer_start_timing, row.exST, row.exSt, row.startTiming, row.exhibitionStSignedValue, row.exhibitionStRaw, row.exhibitionSTRaw, row.exhibitionSt, row.exhibitionST),
        exTime: firstExhibitionTime(row.racer_exhibition_time, row.exTime, row.exhibitionTime, row.exhibition_time),
        lapTime: finiteNumber(row.racer_lap_time ?? row.lapTime ?? row.lap_time ?? row.kyoteiBiyoriLapTime ?? row.kyoteibiyori_lap_time, null),
        straightTime: finiteNumber(row.racer_straight_time ?? row.straightTime ?? row.straight_time ?? row.kyoteiBiyoriStraightTime ?? row.kyoteibiyori_straight_time, null),
        turnTime: finiteNumber(row.racer_turn_time ?? row.turnTime ?? row.turn_time ?? row.kyoteiBiyoriTurnTime ?? row.kyoteibiyori_turn_time, null),
        playerTendency,
        racerCourseStats: playerTendency,
        techniqueStats: {
          last6mRaceCount: playerTendency.last6mRaceCount,
          courseSpecificLast6mRaceCount: playerTendency.courseSpecificLast6mRaceCount,
          allCourseLast6mRaceCount: playerTendency.allCourseLast6mRaceCount,
          sampleStatus: playerTendency.sampleStatus,
          allCourseWinRate: playerTendency.allCourseWinRate,
          allCourseSashiRate: playerTendency.allCourseSashiRate,
          allCourseMakuriRate: playerTendency.allCourseMakuriRate,
          allCourseMakuriSashiRate: playerTendency.allCourseMakuriSashiRate,
          allCourseAvgST: playerTendency.allCourseAvgST,
          escapeRate: playerTendency.escapeRate,
          beatenBySashiRate: playerTendency.beatenBySashiRate,
          beatenByMakuriRate: playerTendency.beatenByMakuriRate,
          beatenByMakuriSashiRate: playerTendency.beatenByMakuriSashiRate,
          nigashiRate: playerTendency.nigashiRate,
          sashiRate: playerTendency.sashiRate,
          makuriRate: playerTendency.makuriRate,
          makuriSashiRate: playerTendency.makuriSashiRate,
          avgStartTiming: playerTendency.avgStartTiming,
          lateStartRate: playerTendency.lateStartRate,
          earlyStartRate: playerTendency.earlyStartRate,
          course6TrifectaRate: playerTendency.course6TrifectaRate
        },
        flyingCount: finiteNumber(row.racer_flying_count, 0),
        fStatus: row.fStatus ?? row.F ?? row.f_status ?? row.flyingStatus ?? null,
        lateCount: finiteNumber(row.racer_late_count, 0)
      };
    })
    .filter((row) => Number.isInteger(row.boat) && row.boat >= 1 && row.boat <= 6)
    .sort((a, b) => a.boat - b.boat);
}

function normalizePercentile(value) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  return clamp(Math.abs(n) > 1 ? n / 100 : n, 0, 1);
}

function enrichMotorRanking(boats = []) {
  const validRateRows = boats
    .filter((boat) => finiteNumber(boat.motor2Rate, null) !== null)
    .sort((a, b) => finiteNumber(b.motor2Rate, 0) - finiteNumber(a.motor2Rate, 0));
  const fallbackRankByBoat = new Map(validRateRows.map((boat, index) => [boat.boat, index + 1]));
  const count = Math.max(validRateRows.length, 1);
  return boats.map((boat) => {
    const explicitRank = finiteNumber(boat.motorRankAtVenue, null);
    const fallbackRank = fallbackRankByBoat.get(boat.boat) ?? null;
    const rank = explicitRank ?? fallbackRank;
    const explicitPercentile = normalizePercentile(boat.motorPercentileAtVenue);
    const fallbackPercentile = rank !== null && count > 1 ? clamp((count - rank) / (count - 1), 0, 1) : normalizePercentile(boat.motor2Rate);
    const percentile = explicitPercentile ?? fallbackPercentile ?? 0.5;
    return {
      ...boat,
      motorRankAtVenue: rank,
      motorPercentileAtVenue: percentile,
      motorStrengthLabel: boat.motorStrengthLabel || motorStrengthLabel(percentile),
      motorRankSource: explicitRank !== null || explicitPercentile !== null ? "entry_motor_rank" : fallbackRank !== null ? "race_motor2Rate_rank" : "missing"
    };
  });
}

function laneBias01(course) {
  return ({ 1: 1, 2: 0.68, 3: 0.56, 4: 0.48, 5: 0.34, 6: 0.24 })[course] ?? 0.4;
}

function zScores(valuesByBoat) {
  const entries = Object.entries(valuesByBoat || {}).map(([boat, value]) => [boat, finiteNumber(value, null)]).filter(([, value]) => value !== null);
  const mean = entries.reduce((sum, [, value]) => sum + value, 0) / Math.max(1, entries.length);
  const variance = entries.reduce((sum, [, value]) => sum + ((value - mean) ** 2), 0) / Math.max(1, entries.length);
  const sd = Math.sqrt(variance) || 1;
  return Object.fromEntries(entries.map(([boat, value]) => [boat, (value - mean) / sd]));
}

const EXHIBITION_DELTA_CONTEXT = {
  exST: 0.16,
  exTime: 0.22,
  lapTime: 1.1,
  straightTime: 0.45,
  turnTime: 0.5,
  motorRank: 1,
  motor2Rate: 28
};

function venueFeatureContextMultiplier(venueId, field) {
  const id = Number(venueId) || 0;
  const attackVenues = new Set([3, 5, 9, 13, 17, 20, 21, 24]);
  const innerVenues = new Set([2, 4, 10, 11, 14, 16, 18, 22]);
  if (attackVenues.has(id)) {
    if (field === "straightTime") return 0.82;
    if (field === "turnTime") return 0.9;
    if (field === "lapTime") return 0.95;
  }
  if (innerVenues.has(id)) {
    if (field === "straightTime") return 1.12;
    if (field === "turnTime") return 0.92;
    if (field === "lapTime") return 0.88;
  }
  return 1;
}

function featureScoreModel(valuesByBoat = {}, { lowerBetter = true, field = "value", venueId = null } = {}) {
  const entries = Object.entries(valuesByBoat)
    .map(([boat, value]) => [String(boat), finiteNumber(value, null)])
    .filter(([, value]) => value !== null);
  if (entries.length === 0) {
    return { scores: {}, ranks: {}, deltasFromBest: {}, percentiles: {}, venueNormalizedScores: {}, rawValues: {}, count: 0 };
  }
  const sorted = [...entries].sort((a, b) => lowerBetter ? a[1] - b[1] : b[1] - a[1]);
  const rawValues = Object.fromEntries(entries);
  const best = sorted[0][1];
  const values = entries.map(([, value]) => value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = max - min;
  const expectedSpread = (EXHIBITION_DELTA_CONTEXT[field] || Math.max(spread, 1)) * venueFeatureContextMultiplier(venueId, field);
  const ranks = Object.fromEntries(sorted.map(([boat], index) => [boat, index + 1]));
  const deltasFromBest = Object.fromEntries(entries.map(([boat, value]) => [
    boat,
    lowerBetter ? value - best : best - value
  ]));
  const percentiles = Object.fromEntries(entries.map(([boat]) => [
    boat,
    entries.length <= 1 ? 0.5 : clamp((entries.length - ranks[boat]) / (entries.length - 1), 0, 1)
  ]));
  const raceScores = Object.fromEntries(entries.map(([boat, value]) => [
    boat,
    spread === 0 ? 0.5 : lowerBetter ? clamp((max - value) / spread, 0, 1) : clamp((value - min) / spread, 0, 1)
  ]));
  const deltaScores = Object.fromEntries(entries.map(([boat]) => [
    boat,
    expectedSpread > 0 ? clamp(1 - Math.max(0, deltasFromBest[boat]) / expectedSpread, 0, 1) : raceScores[boat]
  ]));
  const venueNormalizedScores = Object.fromEntries(entries.map(([boat]) => [
    boat,
    clamp(raceScores[boat] * 0.48 + percentiles[boat] * 0.34 + deltaScores[boat] * 0.18, 0, 1)
  ]));
  return {
    scores: venueNormalizedScores,
    raceScores,
    ranks,
    deltasFromBest,
    deltaScores,
    percentiles,
    venueNormalizedScores,
    rawValues,
    best,
    spread,
    expectedSpread,
    count: entries.length
  };
}

function lowerTimeFeatureScores(valuesByBoat = {}, options = {}) {
  return featureScoreModel(valuesByBoat, { ...options, lowerBetter: true });
}

function highValueFeatureScores(valuesByBoat = {}, options = {}) {
  return featureScoreModel(valuesByBoat, { ...options, lowerBetter: false });
}

function weightedFeatureAverage(parts = {}, weights = {}) {
  let weighted = 0;
  let weightTotal = 0;
  const used = {};
  for (const [field, weight] of Object.entries(weights)) {
    const value = parts[field];
    if (value === null || value === undefined) continue;
    weighted += Number(value) * Number(weight || 0);
    weightTotal += Number(weight || 0);
    used[field] = value;
  }
  return {
    score: weightTotal > 0 ? clamp(weighted / weightTotal, 0, 1) : null,
    used
  };
}

const HEAD_FEATURE_WEIGHTS = BASE_DEFAULT_SCORING_CONFIG.scoringCoefficients.headScore;
const RESIDUAL_FEATURE_WEIGHTS = BASE_DEFAULT_SCORING_CONFIG.scoringCoefficients.partnerResidualScore;
const FOUR_BENEFICIARY_FEATURE_WEIGHTS = {
  turnTime: BASE_DEFAULT_SCORING_CONFIG.scoringCoefficients.fourHeadOpportunity.boat4TurnTime,
  straightTime: BASE_DEFAULT_SCORING_CONFIG.scoringCoefficients.fourHeadOpportunity.boat4StraightTime,
  motorRank: BASE_DEFAULT_SCORING_CONFIG.scoringCoefficients.fourHeadOpportunity.boat4MotorRank,
  motor2Rate: 8,
  exST: 8,
  exTime: 6,
  lapTime: 8
};

function venueWeightMultiplier(venueId, field, mode = "head") {
  const id = Number(venueId) || 0;
  const attackVenues = new Set([3, 5, 9, 13, 17, 20, 21, 24]);
  const innerVenues = new Set([2, 4, 10, 11, 14, 16, 18, 22]);
  if (mode === "residual") {
    if (innerVenues.has(id) && ["lapTime", "turnTime", "motor2Rate"].includes(field)) return 1.12;
    if (attackVenues.has(id) && ["straightTime", "motor2Rate"].includes(field)) return 1.08;
  }
  if (mode === "fourBeneficiary") {
    if (attackVenues.has(id) && ["straightTime", "turnTime", "motor2Rate"].includes(field)) return 1.14;
    if (innerVenues.has(id) && field === "straightTime") return 0.9;
  }
  if (mode === "head") {
    if (attackVenues.has(id) && field === "straightTime") return 1.12;
    if (innerVenues.has(id) && ["lapTime", "turnTime"].includes(field)) return 1.08;
  }
  return 1;
}

function applyVenueWeightMultipliers(weights = {}, venueId = null, mode = "head") {
  return Object.fromEntries(
    Object.entries(weights).map(([field, weight]) => [field, weight * venueWeightMultiplier(venueId, field, mode)])
  );
}

function roleFeatureWeights(boat, venueId = null) {
  if (boat === 1) return applyVenueWeightMultipliers({ exST: 18, exTime: 8, lapTime: 18, turnTime: 18, motorRank: 16, motor2Rate: 8 }, venueId, "head");
  if (boat === 2) return applyVenueWeightMultipliers({ exST: 18, turnTime: 20, lapTime: 14, motorRank: 16, motor2Rate: 8, exTime: 6 }, venueId, "head");
  if (boat === 3) return applyVenueWeightMultipliers({ exST: 22, straightTime: 22, exTime: 8, turnTime: 14, motorRank: 16, motor2Rate: 8 }, venueId, "head");
  if (boat === 4) return applyVenueWeightMultipliers({ exST: 16, straightTime: 18, turnTime: 20, motorRank: 18, motor2Rate: 8, lapTime: 8 }, venueId, "fourBeneficiary");
  return { straightTime: 12, lapTime: 12, turnTime: 12, motorRank: 18, motor2Rate: 8 };
}

function buildOriginalExhibitionFeatureScores(boats = [], exhibition = {}, options = {}) {
  const venueId = options.venueId ?? options.stadiumNumber ?? null;
  const values = {
    exST: Object.fromEntries(boats.map((boat) => [boat.boat, exhibition.exhibitionStartByBoat?.[boat.boat] ?? boat.exST ?? null])),
    exTime: Object.fromEntries(boats.map((boat) => [boat.boat, exhibition.exhibitionTimeByBoat?.[boat.boat] ?? boat.exTime ?? null])),
    lapTime: Object.fromEntries(boats.map((boat) => [boat.boat, boat.lapTime ?? null])),
    straightTime: Object.fromEntries(boats.map((boat) => [boat.boat, boat.straightTime ?? null])),
    turnTime: Object.fromEntries(boats.map((boat) => [boat.boat, boat.turnTime ?? null])),
    motorRank: Object.fromEntries(boats.map((boat) => [boat.boat, boat.motorPercentileAtVenue ?? null])),
    motor2Rate: Object.fromEntries(boats.map((boat) => [boat.boat, boat.motor2Rate ?? null]))
  };
  const fieldScores = {
    exST: lowerTimeFeatureScores(values.exST, { field: "exST", venueId }),
    exTime: lowerTimeFeatureScores(values.exTime, { field: "exTime", venueId }),
    lapTime: lowerTimeFeatureScores(values.lapTime, { field: "lapTime", venueId }),
    straightTime: lowerTimeFeatureScores(values.straightTime, { field: "straightTime", venueId }),
    turnTime: lowerTimeFeatureScores(values.turnTime, { field: "turnTime", venueId }),
    motorRank: highValueFeatureScores(values.motorRank, { field: "motorRank", venueId }),
    motor2Rate: highValueFeatureScores(values.motor2Rate, { field: "motor2Rate", venueId })
  };
  const byBoat = {};
  for (const boat of boats) {
    const key = String(boat.boat);
    const parts = {
      exST: Object.prototype.hasOwnProperty.call(fieldScores.exST.scores, key) ? fieldScores.exST.scores[key] : null,
      exTime: Object.prototype.hasOwnProperty.call(fieldScores.exTime.scores, key) ? fieldScores.exTime.scores[key] : null,
      lapTime: Object.prototype.hasOwnProperty.call(fieldScores.lapTime.scores, key) ? fieldScores.lapTime.scores[key] : null,
      straightTime: Object.prototype.hasOwnProperty.call(fieldScores.straightTime.scores, key) ? fieldScores.straightTime.scores[key] : null,
      turnTime: Object.prototype.hasOwnProperty.call(fieldScores.turnTime.scores, key) ? fieldScores.turnTime.scores[key] : null,
      motorRank: Object.prototype.hasOwnProperty.call(fieldScores.motorRank.scores, key) ? fieldScores.motorRank.scores[key] : null,
      motor2Rate: Object.prototype.hasOwnProperty.call(fieldScores.motor2Rate.scores, key) ? fieldScores.motor2Rate.scores[key] : null
    };
    const role = weightedFeatureAverage(parts, roleFeatureWeights(boat.boat, venueId));
    const headFeature = weightedFeatureAverage(parts, applyVenueWeightMultipliers(HEAD_FEATURE_WEIGHTS, venueId, "head"));
    const residualFeature = weightedFeatureAverage(parts, applyVenueWeightMultipliers(RESIDUAL_FEATURE_WEIGHTS, venueId, "residual"));
    const fourBeneficiaryFeature = weightedFeatureAverage(parts, applyVenueWeightMultipliers(FOUR_BENEFICIARY_FEATURE_WEIGHTS, venueId, "fourBeneficiary"));
    const venueNormalizedMetrics = Object.fromEntries(Object.keys(parts).map((field) => [
      field,
      {
        raw: values[field]?.[boat.boat] ?? null,
        rank: fieldScores[field]?.ranks?.[key] ?? null,
        deltaFromBest: fieldScores[field]?.deltasFromBest?.[key] ?? null,
        raceScore: fieldScores[field]?.raceScores?.[key] ?? null,
        venueDayNormalizedScore: fieldScores[field]?.venueNormalizedScores?.[key] ?? null,
        percentile: fieldScores[field]?.percentiles?.[key] ?? null
      }
    ]));
    byBoat[key] = {
      boat: boat.boat,
      values: Object.fromEntries(Object.keys(parts).map((field) => [field, values[field]?.[boat.boat] ?? null])),
      ranks: Object.fromEntries(Object.keys(parts).map((field) => [field, fieldScores[field]?.ranks?.[key] ?? null])),
      scores: parts,
      venueNormalizedMetrics,
      roleScore: role.score,
      headFeatureScore: headFeature.score,
      residualFeatureScore: residualFeature.score,
      fourBeneficiaryFeatureScore: fourBeneficiaryFeature.score,
      roleUsedFields: Object.keys(role.used)
    };
  }
  const originalCounts = {
    lapTime: fieldScores.lapTime.count,
    straightTime: fieldScores.straightTime.count,
    turnTime: fieldScores.turnTime.count
  };
  return {
    byBoat,
    fieldCounts: Object.fromEntries(Object.entries(fieldScores).map(([field, model]) => [field, model.count])),
    originalCounts,
    allOriginalExhibitionTimesComplete: originalCounts.lapTime >= 6 && originalCounts.straightTime >= 6 && originalCounts.turnTime >= 6,
    venueNormalizedMetrics: BOATS.map((boat) => byBoat[String(boat)]).filter(Boolean).map((row) => ({
      boat: row.boat,
      ...row.venueNormalizedMetrics
    })),
    preview: BOATS.map((boat) => byBoat[String(boat)]).filter(Boolean).map((row) => ({
      boat: row.boat,
      exST: row.values.exST,
      exSTScore: row.scores.exST,
      exTime: row.values.exTime,
      exTimeScore: row.scores.exTime,
      lapTime: row.values.lapTime,
      lapTimeScore: row.scores.lapTime,
      straightTime: row.values.straightTime,
      straightTimeScore: row.scores.straightTime,
      turnTime: row.values.turnTime,
      turnTimeScore: row.scores.turnTime,
      motor2Rate: row.values.motor2Rate,
      motorRankAtVenue: boats.find((boatRow) => boatRow.boat === row.boat)?.motorRankAtVenue ?? null,
      motorPercentileAtVenue: boats.find((boatRow) => boatRow.boat === row.boat)?.motorPercentileAtVenue ?? null,
      motorStrengthLabel: boats.find((boatRow) => boatRow.boat === row.boat)?.motorStrengthLabel ?? null,
      motorRankScore: row.scores.motorRank,
      motor2RateScore: row.scores.motor2Rate,
      venueNormalizedMetrics: row.venueNormalizedMetrics,
      roleScore: row.roleScore,
      headFeatureScore: row.headFeatureScore,
      residualFeatureScore: row.residualFeatureScore,
      fourBeneficiaryFeatureScore: row.fourBeneficiaryFeatureScore,
      roleUsedFields: row.roleUsedFields
    }))
  };
}

function enrichExhibitionFeaturesFromBoats(exhibition = {}, boats = []) {
  const next = { ...(exhibition || {}) };
  const exTimePairs = boats.map((boat) => [boat.boat, exhibition?.exhibitionTimeByBoat?.[boat.boat] ?? boat.exTime ?? null]);
  const exStPairs = boats.map((boat) => [boat.boat, exhibition?.exhibitionStartByBoat?.[boat.boat] ?? boat.exST ?? null]);
  if (exTimePairs.some(([, value]) => finiteNumber(value, null) !== null)) {
    next.exhibitionTimeByBoat = Object.fromEntries(exTimePairs.map(([boat, value]) => [boat, finiteNumber(value, null)]));
    next.usedFields = [...new Set([...(next.usedFields || []), "exhibition_time"])];
    next.sourceByField = { ...(next.sourceByField || {}), exhibition_time: "canonicalRaceData.entries.exTime" };
  }
  if (exStPairs.some(([, value]) => finiteNumber(value, null) !== null)) {
    next.exhibitionStartByBoat = Object.fromEntries(exStPairs.map(([boat, value]) => [boat, finiteNumber(value, null)]));
    next.usedFields = [...new Set([...(next.usedFields || []), "exhibition_st"])];
    next.sourceByField = { ...(next.sourceByField || {}), exhibition_st: "canonicalRaceData.entries.exST" };
  }
  if ((next.usedFields || []).length > 0) next.status = "exhibition_reflected";
  return next;
}

function optionalRate01(value) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  return clamp(Math.abs(n) <= 1 ? n : n / 100, 0, 1);
}

function getVenueLaneBias(config, stadiumNumber, lane) {
  const source = config?.venueLaneBias;
  if (!source) return null;
  if (Array.isArray(source)) {
    return source.find((row) => Number(row?.venue ?? row?.stadiumNumber) === Number(stadiumNumber) && Number(row?.lane) === Number(lane)) || null;
  }
  return source?.[`${stadiumNumber}-${lane}`] || source?.[String(stadiumNumber)]?.[String(lane)] || null;
}

function pickFiniteFrom(source = {}, row = {}, keys = []) {
  for (const key of keys) {
    const value = source?.[key] ?? row?.[key];
    const n = finiteNumber(value, null);
    if (n !== null) return n;
  }
  return null;
}

function normalizePlayerTendency(row = {}, boat = null, course = null) {
  const source =
    row?.playerTendency && typeof row.playerTendency === "object"
      ? row.playerTendency
      : row?.racerCourseStats && typeof row.racerCourseStats === "object"
        ? row.racerCourseStats
        : {};
  return {
    boat,
    racerId: source.racerId ?? source.racer_id ?? row.racer_number ?? row.racerId ?? null,
    course,
    last6mRaceCount: pickFiniteFrom(source, row, ["last6mRaceCount", "last_6m_race_count"]),
    courseSpecificLast6mRaceCount: pickFiniteFrom(source, row, ["courseSpecificLast6mRaceCount", "course_specific_last_6m_race_count", "last6mRaceCount", "last_6m_race_count"]),
    allCourseLast6mRaceCount: pickFiniteFrom(source, row, ["allCourseLast6mRaceCount", "all_course_last_6m_race_count"]),
    sampleStatus: source.sampleStatus ?? source.sample_status ?? row.sampleStatus ?? row.sample_status ?? null,
    allCourseWinRate: pickFiniteFrom(source, row, ["allCourseWinRate", "all_course_win_rate"]),
    allCourseSashiRate: pickFiniteFrom(source, row, ["allCourseSashiRate", "all_course_sashi_rate"]),
    allCourseMakuriRate: pickFiniteFrom(source, row, ["allCourseMakuriRate", "all_course_makuri_rate"]),
    allCourseMakuriSashiRate: pickFiniteFrom(source, row, ["allCourseMakuriSashiRate", "all_course_makuri_sashi_rate"]),
    allCourseAvgST: pickFiniteFrom(source, row, ["allCourseAvgST", "all_course_avg_st"]),
    escapeRate: pickFiniteFrom(source, row, ["escapeRate", "escape_rate", "course1EscapeRate"]),
    beatenBySashiRate: pickFiniteFrom(source, row, ["beatenBySashiRate", "beaten_by_sashi_rate"]),
    beatenByMakuriRate: pickFiniteFrom(source, row, ["beatenByMakuriRate", "beaten_by_makuri_rate"]),
    beatenByMakuriSashiRate: pickFiniteFrom(source, row, ["beatenByMakuriSashiRate", "beaten_by_makuri_sashi_rate"]),
    nigashiRate: pickFiniteFrom(source, row, ["nigashiRate", "nigashi_rate", "course2NigashiRate"]),
    sashiRate: pickFiniteFrom(source, row, ["sashiRate", "sashi_rate", "course2SashiRate"]),
    makuriRate: pickFiniteFrom(source, row, ["makuriRate", "makuri_rate"]),
    makuriSashiRate: pickFiniteFrom(source, row, ["makuriSashiRate", "makuri_sashi_rate", "makurisashiRate", "makurisashi_rate"]),
    course6TrifectaRate: pickFiniteFrom(source, row, ["course6TrifectaRate", "course6_trifecta_rate"]),
    avgStartTiming: pickFiniteFrom(source, row, ["avgST", "avg_st", "avgStartTiming", "avg_start_timing"]),
    lateStartRate: pickFiniteFrom(source, row, ["lateStartRate", "late_start_rate", "delayRate", "delay_rate"]),
    earlyStartRate: pickFiniteFrom(source, row, ["earlyStartRate", "early_start_rate"]),
    exhibitionToRealStartGap: pickFiniteFrom(source, row, ["exhibitionToRealStartGap", "exhibition_to_real_start_gap"]),
    fCount: pickFiniteFrom(source, row, ["fCount", "f_count", "flyingCount", "racer_flying_count"]),
    localWinRate: pickFiniteFrom(source, row, ["localWinRate", "local_win_rate", "racer_local_top_1_percent"]),
    laneQuinellaRate: pickFiniteFrom(source, row, ["laneQuinellaRate", "lane_quinella_rate", "lane2RenRate", "lane2renScore", "lane2renAvg"]),
    laneTrifectaRate: pickFiniteFrom(source, row, ["laneTrifectaRate", "lane_trifecta_rate", "lane3RenRate", "lane3renScore", "lane3renAvg"])
  };
}

function rateDelta(value, center = 0.5) {
  const n = optionalRate01(value);
  return n === null ? 0 : n - center;
}

function positiveRateLift(value, threshold = 0.5) {
  const n = optionalRate01(value);
  return n === null ? 0 : Math.max(0, n - threshold);
}

function tendencySampleStatus(tendency = {}) {
  const explicit = String(tendency?.sampleStatus || "").trim();
  if (["ok", "small_sample", "very_small_sample", "insufficient_history"].includes(explicit)) {
    return explicit;
  }
  const count = finiteNumber(
    tendency?.courseSpecificLast6mRaceCount ?? tendency?.last6mRaceCount,
    null
  );
  if (count === null) return "unknown";
  if (count >= 10) return "ok";
  if (count >= 3) return "small_sample";
  if (count >= 1) return "very_small_sample";
  return "insufficient_history";
}

function tendencySampleWeight(tendency = {}) {
  return ({
    ok: 1,
    small_sample: 0.35,
    very_small_sample: 0.12,
    insufficient_history: 0,
    unknown: 1
  })[tendencySampleStatus(tendency)] ?? 0;
}

function tendencyCanDriveUpset(tendency = {}) {
  return tendencySampleWeight(tendency) >= 0.35;
}

function weightedPositiveRateLift(tendency = {}, field, threshold = 0.5) {
  return positiveRateLift(tendency?.[field], threshold) * tendencySampleWeight(tendency);
}

function allCourseReferenceWeight(tendency = {}) {
  const count = finiteNumber(tendency?.allCourseLast6mRaceCount, 0);
  if (count >= 10) return 0.15;
  if (count >= 3) return 0.08;
  if (count >= 1) return 0.03;
  return 0;
}

function buildStartReliabilityContribution({ boat = {}, tendency = {}, featureRow = {}, course = null, config = {} } = {}) {
  const avgStart = tendency.avgStartTiming ?? tendency.allCourseAvgST ?? boat.averageStartTiming;
  const avgSTScore = avgStart !== null && avgStart !== undefined ? startTimingScore(avgStart, 0.5) : 0.5;
  const lateStartRate = optionalRate01(tendency.lateStartRate) ?? 0;
  const earlyStartRate = optionalRate01(tendency.earlyStartRate) ?? 0;
  const fCount = Math.max(0, finiteNumber(boat.flyingCount ?? tendency.fCount, 0));
  const fStatusText = String(boat.fStatus ?? "").toUpperCase();
  const hasFRisk = fCount > 0 || /\bF|Ｆ/.test(fStatusText);
  const exSTScore = finiteNumber(featureRow?.scores?.exST, null);
  const exhibitionGoodButHistoryPoor =
    exSTScore !== null &&
    exSTScore >= 0.72 &&
    (avgSTScore <= 0.42 || lateStartRate >= 0.18);
  const weights = config.professionalFactorWeights || {};
  const headAdjustment =
    (avgSTScore - 0.5) * finiteNumber(weights.startReliabilityHead, 0.11) +
    Math.max(0, earlyStartRate - 0.18) * finiteNumber(weights.earlyStartAttack, 0.05) * (course >= 3 ? 1 : 0.45) -
    Math.max(0, lateStartRate - 0.12) * (course <= 2 ? 0.12 : 0.08) -
    (hasFRisk ? finiteNumber(weights.fRiskHeadPenalty, 0.08) : 0) -
    (exhibitionGoodButHistoryPoor ? finiteNumber(weights.exhibitionHistoryContradictionPenalty, 0.06) : 0);
  const wallAdjustment =
    (avgSTScore - 0.5) * 0.1 -
    Math.max(0, lateStartRate - 0.12) * 0.16 -
    (hasFRisk ? 0.05 : 0);
  const warning =
    exhibitionGoodButHistoryPoor
      ? "展示STは良いが平均ST/出遅れ率に不安があり、過信を抑制"
      : hasFRisk && earlyStartRate >= 0.18
        ? "早仕掛け傾向とFリスクがあり、攻め評価を抑制"
        : null;
  return {
    avgSTScore,
    lateStartRate,
    earlyStartRate,
    fCount,
    hasFRisk,
    exhibitionGoodButHistoryPoor,
    headAdjustment,
    wallAdjustment,
    warning
  };
}

function buildScores(boats, exhibition, config, featureScores = buildOriginalExhibitionFeatureScores(boats, exhibition)) {
  const timeZ = exhibition.exhibitionTimeByBoat ? zScores(exhibition.exhibitionTimeByBoat) : {};
  const lapValues = Object.fromEntries(boats.filter((boat) => boat.lapTime !== null).map((boat) => [boat.boat, boat.lapTime]));
  const lapZ = Object.keys(lapValues).length >= 2 ? zScores(lapValues) : {};
  const straightValues = Object.fromEntries(boats.filter((boat) => boat.straightTime !== null).map((boat) => [boat.boat, boat.straightTime]));
  const straightZ = Object.keys(straightValues).length >= 2 ? zScores(straightValues) : {};
  const turnValues = Object.fromEntries(boats.filter((boat) => boat.turnTime !== null).map((boat) => [boat.boat, boat.turnTime]));
  const turnZ = Object.keys(turnValues).length >= 2 ? zScores(turnValues) : {};
  const courses = exhibition.entryCourseByBoat || Object.fromEntries(boats.map((boat) => [boat.boat, boat.boat]));
  return boats.map((boat) => {
    const course = finiteNumber(courses[boat.boat], boat.boat);
    const tendency = boat.playerTendency || {};
    const venueBias = getVenueLaneBias(config, boat.raw?.race_stadium_number, course) || getVenueLaneBias(config, config?.stadiumNumber, course);
    const venueBiasBoost = venueBias
      ? (
        (optionalRate01(venueBias.winRate) ?? laneBias01(course)) -
        laneBias01(course)
      ) * 0.12
      : 0;
    const techniqueBoost = (
      rateDelta(tendency.escapeRate) * (course === 1 ? 0.12 : 0) +
      positiveRateLift(tendency.sashiRate, 0.12) * (course === 2 ? 0.28 : 0) -
      rateDelta(tendency.nigashiRate) * (course === 2 ? 0.05 : 0) +
      positiveRateLift(tendency.makuriRate, 0.1) * ([3, 4].includes(course) ? 0.24 : 0) +
      positiveRateLift(tendency.makuriSashiRate, 0.08) * ([3, 4, 5].includes(course) ? 0.26 : 0) +
      rateDelta(tendency.laneQuinellaRate) * 0.05 +
      rateDelta(tendency.laneTrifectaRate) * 0.04 -
      positiveRateLift(tendency.beatenBySashiRate, 0.15) * (course === 1 ? 0.18 : 0) -
      positiveRateLift(tendency.beatenByMakuriRate, 0.1) * (course === 1 ? 0.2 : 0) -
      positiveRateLift(tendency.beatenByMakuriSashiRate, 0.08) * (course === 1 ? 0.18 : 0)
    ) * tendencySampleWeight(tendency);
    const allCourseReferenceBoost = (
      rateDelta(tendency.allCourseWinRate, 1 / 6) * 0.05 +
      positiveRateLift(tendency.allCourseSashiRate, 0.08) * (course === 2 ? 0.06 : 0) +
      positiveRateLift(tendency.allCourseMakuriRate, 0.08) * ([3, 4].includes(course) ? 0.06 : 0) +
      positiveRateLift(tendency.allCourseMakuriSashiRate, 0.06) * ([3, 4].includes(course) ? 0.06 : 0)
    ) * allCourseReferenceWeight(tendency);
    const tendencyStartValue = tendency.avgStartTiming ?? tendency.allCourseAvgST;
    const tendencyStartWeight = tendency.avgStartTiming !== null && tendency.avgStartTiming !== undefined
      ? tendencySampleWeight(tendency)
      : allCourseReferenceWeight(tendency);
    const startTendencyBoost =
      tendencyStartValue !== null && tendencyStartValue !== undefined
        ? (startTimingScore(tendencyStartValue, 0.5) - 0.5) * (course === 1 ? 0.08 : 0.04) * tendencyStartWeight
        : 0;
    const lateRatePenalty = weightedPositiveRateLift(tendency, "lateStartRate", 0.12) * (course === 1 ? 0.18 : 0.08);
    const lapBoost = Object.prototype.hasOwnProperty.call(lapZ, String(boat.boat))
      ? clamp(-Number(lapZ[String(boat.boat)] || 0) / 2, -0.4, 0.4) * 0.08
      : 0;
    const straightBoost = Object.prototype.hasOwnProperty.call(straightZ, String(boat.boat))
      ? clamp(-Number(straightZ[String(boat.boat)] || 0) / 2, -0.4, 0.4) * (course >= 3 ? 0.1 : 0.04)
      : 0;
    const turnBoost = Object.prototype.hasOwnProperty.call(turnZ, String(boat.boat))
      ? clamp(-Number(turnZ[String(boat.boat)] || 0) / 2, -0.4, 0.4) * ([2, 3, 4, 5].includes(course) ? 0.1 : 0.05)
      : 0;
    const featureRow = featureScores.byBoat[String(boat.boat)] || {};
    const roleScore = finiteNumber(featureRow.roleScore, null);
    const firstPlaceDampening = boat.boat >= 5
      ? finiteNumber(config.originalExhibitionWeights?.outsideFirstPlaceDampening, 0.42)
      : 1;
    const roleFeatureBoost = roleScore === null
      ? 0
      : (roleScore - 0.5) * finiteNumber(config.originalExhibitionWeights?.roleFeatureBoost, 0.26) * firstPlaceDampening;
    const motorRankScore = finiteNumber(featureRow?.scores?.motorRank, normalizePercentile(boat.motorPercentileAtVenue) ?? 0.5);
    const motorRankBoost = (motorRankScore - 0.5) * finiteNumber(config.professionalFactorWeights?.motorRankHead, 0.18) * firstPlaceDampening;
    const exhibitionExcellent = (
      finiteNumber(featureRow?.scores?.exST, 0.5) >= 0.72 ||
      finiteNumber(featureRow?.scores?.exTime, 0.5) >= 0.72
    ) && (
      finiteNumber(featureRow?.scores?.turnTime, 0.5) >= 0.66 ||
      finiteNumber(featureRow?.scores?.straightTime, 0.5) >= 0.66
    );
    const weakMotorHeadPenalty = motorRankScore < 0.28 && !exhibitionExcellent ? (0.28 - motorRankScore) * 0.1 : 0;
    const startReliability = buildStartReliabilityContribution({ boat, tendency, featureRow, course, config });
    const startReliabilityBoost = startReliability.headAdjustment;
    const base =
      config.baseWeights.laneBias * laneBias01(course) +
      config.baseWeights.class * (CLASS_SCORE[boat.classNumber] ?? 0.48) +
      config.baseWeights.nationalWinRatePoint * pointRate01(boat.nationalWinRatePoint) +
      config.baseWeights.localWinRatePoint * pointRate01(boat.localWinRatePoint) +
      config.baseWeights.motor2Rate * percent01(boat.motor2Rate) +
      config.baseWeights.boat2Rate * percent01(boat.boat2Rate) +
      config.baseWeights.averageStartTiming * startTimingScore(boat.averageStartTiming) -
      config.baseWeights.flyingPenalty * Math.max(0, boat.flyingCount) -
      config.baseWeights.latePenalty * Math.max(0, boat.lateCount);
    const exhibitionTimeBoost = exhibition.exhibitionTimeByBoat
      ? config.exhibitionWeights.exhibitionTimeZ * clamp(-Number(timeZ[String(boat.boat)] ?? 0) / 2, -0.5, 0.5)
      : 0;
    const exhibitionStBoost = exhibition.exhibitionStartByBoat
      ? config.exhibitionWeights.exhibitionStartTiming * (startTimingScore(exhibition.exhibitionStartByBoat[boat.boat]) - 0.5)
      : 0;
    const entryBoost = exhibition.entryCourseByBoat
      ? config.exhibitionWeights.entryCourse * (laneBias01(course) - laneBias01(boat.boat))
      : 0;
    const makuriBoost = exhibition.exhibitionTimeByBoat && course >= 3
      ? computeMakuriAlertBoost(boat.boat, exhibition.exhibitionTimeByBoat, config)
      : 0;
    return {
      ...boat,
      course,
      featureScores: featureRow,
      score: base + exhibitionTimeBoost + exhibitionStBoost + entryBoost + makuriBoost + lapBoost + straightBoost + turnBoost + roleFeatureBoost + motorRankBoost + startReliabilityBoost + techniqueBoost + allCourseReferenceBoost + startTendencyBoost + venueBiasBoost - lateRatePenalty - weakMotorHeadPenalty,
      scoreParts: { base, exhibitionTimeBoost, exhibitionStBoost, entryBoost, makuriBoost, lapBoost, straightBoost, turnBoost, roleFeatureBoost, motorRankBoost, startReliabilityBoost, weakMotorHeadPenalty, techniqueBoost, allCourseReferenceBoost, startTendencyBoost, lateRatePenalty, venueBiasBoost },
      professionalFactors: {
        motorRankScore,
        motorRankBoost,
        motorRankSource: boat.motorRankSource,
        motorStrengthLabel: boat.motorStrengthLabel,
        startReliability,
        weakMotorHeadPenalty
      }
    };
  });
}

function computeMakuriAlertBoost(boatNumber, timesByBoat, config) {
  const own = finiteNumber(timesByBoat?.[boatNumber], null);
  if (own === null) return 0;
  const inside = BOATS.filter((boat) => boat < boatNumber).map((boat) => finiteNumber(timesByBoat?.[boat], null)).filter((v) => v !== null);
  if (inside.length === 0) return 0;
  const fastestInside = Math.min(...inside);
  const diff = fastestInside - own;
  if (diff < config.makuriAlertSeconds) return 0;
  return config.exhibitionWeights.makuriAlert * clamp(diff / 0.12, 0, 1);
}

export function softmax(rows, scoreKey = "score") {
  const max = Math.max(...rows.map((row) => finiteNumber(row?.[scoreKey], 0)));
  const expRows = rows.map((row) => ({ row, exp: Math.exp(finiteNumber(row?.[scoreKey], 0) - max) }));
  const total = expRows.reduce((sum, item) => sum + item.exp, 0) || 1;
  return expRows.map((item) => ({ ...item.row, probability: item.exp / total }));
}

export function plackettLuceTrifecta(scoredBoats) {
  const combos = [];
  for (const first of scoredBoats) {
    const p1Rows = softmax(scoredBoats);
    const p1 = p1Rows.find((row) => row.boat === first.boat)?.probability ?? 0;
    const restAfterFirst = scoredBoats.filter((row) => row.boat !== first.boat);
    for (const second of restAfterFirst) {
      const p2Rows = softmax(restAfterFirst);
      const p2 = p2Rows.find((row) => row.boat === second.boat)?.probability ?? 0;
      const restAfterSecond = restAfterFirst.filter((row) => row.boat !== second.boat);
      for (const third of restAfterSecond) {
        const p3Rows = softmax(restAfterSecond);
        const p3 = p3Rows.find((row) => row.boat === third.boat)?.probability ?? 0;
        combos.push({
          combo: `${first.boat}-${second.boat}-${third.boat}`,
          boats: [first.boat, second.boat, third.boat],
          probability: p1 * p2 * p3
        });
      }
    }
  }
  return combos.sort((a, b) => b.probability - a.probability);
}

export function marginalizeTickets(trifecta) {
  const exacta = new Map();
  const trio = new Map();
  const quinella = new Map();
  for (const row of trifecta) {
    const [a, b, c] = row.boats;
    const exactaKey = `${a}-${b}`;
    const trioKey = [a, b, c].sort((x, y) => x - y).join("=");
    const quinellaKey = [a, b].sort((x, y) => x - y).join("=");
    exacta.set(exactaKey, (exacta.get(exactaKey) || 0) + row.probability);
    trio.set(trioKey, (trio.get(trioKey) || 0) + row.probability);
    quinella.set(quinellaKey, (quinella.get(quinellaKey) || 0) + row.probability);
  }
  const mapRows = (map) => [...map.entries()].map(([combo, probability]) => ({ combo, probability })).sort((a, b) => b.probability - a.probability);
  return {
    trifecta,
    exacta: mapRows(exacta),
    trio: mapRows(trio),
    quinella: mapRows(quinella)
  };
}

export function buildTurnScenario(prediction) {
  const top = prediction.tickets.trifecta.slice(0, 8);
  const head = prediction.firstPlaceProbabilities[0];
  const hasExhibition = prediction.exhibition.status === "exhibition_reflected";
  const boat1 = prediction.scoredBoats.find((boat) => boat.boat === 1);
  const featureByBoat = prediction.featureScores?.byBoat || {};
  const scoreOf = (boat, field) => finiteNumber(featureByBoat[String(boat)]?.scores?.[field], null);
  const boat1FootText = scoreOf(1, "lapTime") !== null && scoreOf(1, "turnTime") !== null && scoreOf(1, "lapTime") >= 0.68 && scoreOf(1, "turnTime") >= 0.68
    ? "1号艇は周回とまわり足が上位で、イン残し評価を上げます。"
    : "";
  const boat3AttackText = scoreOf(3, "exST") !== null && scoreOf(3, "straightTime") !== null && scoreOf(3, "exST") >= 0.68 && scoreOf(3, "straightTime") >= 0.68
    ? "3号艇はSTと直線が良く、センター攻めの可能性があります。"
    : "";
  const boat4TurnText = scoreOf(4, "turnTime") !== null && scoreOf(4, "turnTime") >= 0.68
    ? "4号艇はまわり足が良く、3が攻めた後のまくり差し展開に注意。"
    : "";
  const boat1TendencyText = tendencyCanDriveUpset(boat1?.playerTendency) && optionalRate01(boat1?.playerTendency?.escapeRate) >= 0.55
    ? "1号艇は直近6か月の逃げ率が高く、メイン逃げ本線。"
    : "";
  const sparseTendencyText = prediction.tendencySummary?.sparse === true
    ? "直近6か月のコース別戦法データはサンプル不足のため、展示ST・展示タイム・周回・直線・まわり足を中心に評価しています。"
    : "";
  const tendencyAttackText = prediction.scoredBoats
    .filter((boat) => boat.boat >= 2 && boat.boat <= 4)
    .map((boat) => {
      const tendency = boat.playerTendency || {};
      if (!tendencyCanDriveUpset(tendency)) return "";
      if (boat.boat === 2 && optionalRate01(tendency.sashiRate) >= 0.16) return "2号艇の差し率が高く、1Mの差し抜けを警戒。";
      if (boat.boat === 3 && optionalRate01(tendency.makuriRate) >= 0.16) return "3号艇のまくり率が高く、センター攻めを評価。";
      if (boat.boat === 4 && optionalRate01(tendency.makuriSashiRate) >= 0.14) return "4号艇のまくり差し率が高く、展開突きを警戒。";
      return "";
    })
    .find(Boolean) || "";
  const outsideAlert = prediction.scoredBoats
    .filter((boat) => boat.boat >= 3 && (boat.scoreParts.makuriBoost > 0 || boat.scoreParts.roleFeatureBoost > 0.02))
    .sort((a, b) => (b.scoreParts.makuriBoost + b.scoreParts.roleFeatureBoost) - (a.scoreParts.makuriBoost + a.scoreParts.roleFeatureBoost))[0];
  const mainMethod = head?.boat === 1 ? "逃げ" : head?.course <= 2 ? "差し" : "まくり差し";
  const counterMethod = outsideAlert ? "まくり/まくり差し" : "差し残し";
  return {
    main: {
      title: "本線シナリオ",
      text: `${head?.boat ?? "-"}号艇の${mainMethod}が中心。${hasExhibition ? "展示反映済みの隊形" : "枠なり前提"}で、1Mは${top[0]?.combo ?? "-"}を軸に見る。${boat1TendencyText}${boat1FootText}${sparseTendencyText}`,
      tickets: top.slice(0, 2).map((row) => row.combo)
    },
    counter: {
      title: "対抗シナリオ",
      text: outsideAlert
        ? `${outsideAlert.boat}号艇の展示気配が内側より強く、${counterMethod}の一撃を警戒。${tendencyAttackText}${boat3AttackText || boat4TurnText}`
        : `${boat1?.averageStartTiming == null ? "平均ST不明の艇があり" : "内側のST差次第で"}2・3着争いが入れ替わる可能性。`,
      tickets: top.slice(2, 5).map((row) => row.combo)
    },
    upset: {
      title: "穴シナリオ",
      text: prediction.exhibition.weather?.wind >= 5
        ? "風が強めならターン出口で隊形が崩れ、外の連絡みまで押さえる筋。"
        : "地力差が小さい場合は2M以降の入れ替わりで中穴決着の可能性。",
      tickets: top.slice(5, 8).map((row) => row.combo)
    }
  };
}

function expandTicketPatterns(patterns, baseTickets = [], limit = 6) {
  const baseSet = new Set(baseTickets.map((row) => row.combo));
  const rows = [];
  const seen = new Set();
  for (const pattern of patterns) {
    const [first, second, third] = pattern;
    const thirds = third === "flow" ? BOATS.filter((boat) => boat !== first && boat !== second) : [third];
    for (const tail of thirds) {
      const combo = `${first}-${second}-${tail}`;
      if (first === second || first === tail || second === tail || seen.has(combo) || baseSet.has(combo)) continue;
      seen.add(combo);
      rows.push({ combo, boats: [first, second, tail], sourcePattern: `${first}-${second}-流し` });
      if (rows.length >= limit) return rows;
    }
  }
  return rows;
}

function getBoat(prediction, boatNumber) {
  return prediction.scoredBoats.find((boat) => boat.boat === boatNumber) || {};
}

function rateScore(value, fallback = 0.5) {
  const n = optionalRate01(value);
  return n === null ? fallback : n;
}

function buildScenarioRow({
  prediction,
  scenarioName,
  attacker,
  baseScore,
  upsetScore,
  description,
  patterns,
  reasons
}) {
  const basicTickets = prediction.tickets.trifecta.slice(0, 6);
  return {
    scenarioName,
    attacker,
    probabilityScore: Math.round(clamp(baseScore, 0, 1) * 100),
    upsetScore: Math.round(clamp(upsetScore, 0, 1) * 100),
    description,
    reasons: reasons.filter(Boolean),
    recommendedExtraTickets: expandTicketPatterns(patterns, basicTickets, 6)
  };
}

export function buildDevelopmentScenarios(prediction = {}) {
  const firstRows = Array.isArray(prediction.firstPlaceProbabilities) ? prediction.firstPlaceProbabilities : [];
  const p = (boat) => firstRows.find((row) => row.boat === boat)?.probability ?? 0;
  const boat1 = getBoat(prediction, 1);
  const boat2 = getBoat(prediction, 2);
  const boat3 = getBoat(prediction, 3);
  const boat4 = getBoat(prediction, 4);
  const boat5 = getBoat(prediction, 5);
  const boat6 = getBoat(prediction, 6);
  const exSt = prediction.exhibition?.exhibitionStartByBoat || {};
  const exTime = prediction.exhibition?.exhibitionTimeByBoat || {};
  const timeZ = prediction.exhibition?.exhibitionTimeByBoat ? zScores(exTime) : {};
  const lapValues = Object.fromEntries(prediction.scoredBoats.filter((boat) => boat.lapTime !== null).map((boat) => [boat.boat, boat.lapTime]));
  const lapZ = Object.keys(lapValues).length >= 2 ? zScores(lapValues) : {};
  const straightValues = Object.fromEntries(prediction.scoredBoats.filter((boat) => boat.straightTime !== null).map((boat) => [boat.boat, boat.straightTime]));
  const straightZ = Object.keys(straightValues).length >= 2 ? zScores(straightValues) : {};
  const turnValues = Object.fromEntries(prediction.scoredBoats.filter((boat) => boat.turnTime !== null).map((boat) => [boat.boat, boat.turnTime]));
  const turnZ = Object.keys(turnValues).length >= 2 ? zScores(turnValues) : {};
  const featureByBoat = prediction.featureScores?.byBoat || {};
  const fs = (boat, field, fallback = 0.5) => {
    const value = featureByBoat[String(boat)]?.scores?.[field];
    return value === null || value === undefined ? fallback : Number(value);
  };
  const roleFs = (boat, fallback = 0.5) => {
    const value = featureByBoat[String(boat)]?.roleScore;
    return value === null || value === undefined ? fallback : Number(value);
  };
  const good = (boat, field, threshold = 0.68) => fs(boat, field, 0.5) >= threshold;
  const weak = (boat, field, threshold = 0.32) => fs(boat, field, 0.5) <= threshold;
  const t1 = boat1.playerTendency || {};
  const t2 = boat2.playerTendency || {};
  const t3 = boat3.playerTendency || {};
  const t4 = boat4.playerTendency || {};
  const t1UpsetEligible = tendencyCanDriveUpset(t1);
  const t2UpsetEligible = tendencyCanDriveUpset(t2);
  const t3UpsetEligible = tendencyCanDriveUpset(t3);
  const t4UpsetEligible = tendencyCanDriveUpset(t4);
  const lift = (tendency, field, threshold) => weightedPositiveRateLift(tendency, field, threshold);
  const upsetLift = (tendency, field, threshold) =>
    tendencyCanDriveUpset(tendency) ? weightedPositiveRateLift(tendency, field, threshold) : 0;
  const t1BeatenBySashi = optionalRate01(t1.beatenBySashiRate);
  const t1BeatenByMakuri = optionalRate01(t1.beatenByMakuriRate);
  const t1BeatenByMakuriSashi = optionalRate01(t1.beatenByMakuriSashiRate);
  const t2SashiRate = optionalRate01(t2.sashiRate);
  const t3MakuriRate = optionalRate01(t3.makuriRate);
  const t3MakuriSashiRate = optionalRate01(t3.makuriSashiRate);
  const t4MakuriRate = optionalRate01(t4.makuriRate);
  const t4MakuriSashiRate = optionalRate01(t4.makuriSashiRate);
  const boat1EscapeWeak = t1UpsetEligible && optionalRate01(t1.escapeRate) !== null && optionalRate01(t1.escapeRate) < 0.42;
  const boat1LateRisk = t1UpsetEligible && optionalRate01(t1.lateStartRate) !== null && optionalRate01(t1.lateStartRate) >= 0.18;
  const boat2LateRisk = t2UpsetEligible && optionalRate01(t2.lateStartRate) !== null && optionalRate01(t2.lateStartRate) >= 0.18;
  const boat1SashiVulnerable = t1UpsetEligible && t1BeatenBySashi !== null && t1BeatenBySashi >= 0.16;
  const boat1MakuriVulnerable = t1UpsetEligible && t1BeatenByMakuri !== null && t1BeatenByMakuri >= 0.1;
  const boat1MakuriSashiVulnerable = t1UpsetEligible && t1BeatenByMakuriSashi !== null && t1BeatenByMakuriSashi >= 0.08;
  const boat2LegacySashiUpset =
    t2UpsetEligible &&
    optionalRate01(t2.nigashiRate) !== null &&
    optionalRate01(t2.nigashiRate) < 0.42 &&
    t2SashiRate !== null &&
    t2SashiRate > 0.55;
  const boat2SashiUpset = boat2LegacySashiUpset || (boat1SashiVulnerable && t2SashiRate !== null && t2SashiRate >= 0.16);
  const boat3AttackReady =
    t3UpsetEligible &&
    ((t3MakuriRate !== null && t3MakuriRate >= 0.16) || (t3MakuriSashiRate !== null && t3MakuriSashiRate >= 0.14)) &&
    (startTimingScore(exSt[3], 0.5) > 0.62 || Number(straightZ["3"] || 0) < -0.45 || (good(3, "exST") && good(3, "straightTime")));
  const boat4DevelopSashiReady = boat3AttackReady && (t4UpsetEligible && t4MakuriSashiRate !== null && t4MakuriSashiRate >= 0.14 || (good(4, "straightTime") && good(4, "turnTime")));
  const boat1LapTurnWeak = weak(1, "lapTime") || weak(1, "turnTime");
  const boat1LapTurnStrong = good(1, "lapTime") && good(1, "turnTime");
  const boat1TrustLow = p(1) < 0.34 || boat1EscapeWeak || boat1LateRisk || boat2LateRisk || boat1SashiVulnerable || boat1MakuriVulnerable || boat1MakuriSashiVulnerable || boat1LapTurnWeak || (prediction.exhibition?.exhibitionStartByBoat && startTimingScore(exSt[1], 0.5) < 0.45) || Number(timeZ["1"] || 0) > 0.6 || Number(lapZ["1"] || 0) > 0.6;
  const boat3StraightStStrong = good(3, "exST") && good(3, "straightTime");
  const boat3TurnStrong = good(3, "turnTime");
  const boat4StraightTurnStrong = good(4, "straightTime") && good(4, "turnTime");
  const boat5SecondThirdStrong = good(5, "lapTime") && (good(5, "straightTime") || good(5, "turnTime"));
  const boat6SecondThirdStrong = good(6, "lapTime") && (good(6, "straightTime") || good(6, "turnTime"));
  const sortedByScore = [...prediction.scoredBoats].sort((a, b) => b.score - a.score);
  const scoreGapSmall = Math.abs((sortedByScore[0]?.score ?? 0) - (sortedByScore[2]?.score ?? 0)) < 0.12;
  const scenarios = [
    buildScenarioRow({
      prediction,
      scenarioName: "イン逃げ成功シナリオ",
      attacker: 1,
      baseScore: p(1) + lift(t1, "escapeRate", 0.5) * 0.35 + (roleFs(1) - 0.5) * 0.22 + (boat1LapTurnStrong ? 0.08 : 0) + (t1.avgStartTiming !== null && t1.avgStartTiming !== undefined ? (startTimingScore(t1.avgStartTiming, 0.5) - 0.5) * 0.18 * tendencySampleWeight(t1) : 0) - lift(t1, "beatenBySashiRate", 0.15) * 0.42 - lift(t1, "beatenByMakuriRate", 0.1) * 0.48 - lift(t1, "beatenByMakuriSashiRate", 0.08) * 0.44 - (boat1LateRisk ? 0.12 : 0) - (boat2LateRisk ? 0.06 : 0) - (boat1LapTurnWeak ? 0.1 : 0),
      upsetScore: boat1TrustLow ? 0.2 : 0.05,
      description: "1号艇が先マイして内有利を保つ本線展開。",
      patterns: [[1, 2, "flow"], [1, 3, "flow"]],
      reasons: [t1UpsetEligible && lift(t1, "escapeRate", 0.5) > 0 ? "1号艇は直近6か月の逃げ率が高く、メイン逃げ本線。" : null, boat1LapTurnStrong ? "1号艇は周回とまわり足が上位で、イン残し評価を上げます。" : null, boat1SashiVulnerable ? "1号艇の差され率が高く、逃げ信頼度を下げます。" : null, boat1MakuriVulnerable ? "1号艇のまくられ率が高く、センター攻めを警戒します。" : null, boat1MakuriSashiVulnerable ? "1号艇のまくり差され率が高く、外の差し抜けを警戒します。" : null, boat1LapTurnWeak ? "1号艇の周回またはまわり足が弱く、逃げ信頼度を下げます。" : null, boat1LateRisk ? "1号艇の出遅れ率が高い" : null, boat2LateRisk ? "2号艇の出遅れ率が高く、1号艇の壁信頼度を下げます。" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "2号艇差しシナリオ",
      attacker: 2,
      baseScore: p(2) + lift(t2, "sashiRate", 0.12) * 0.72 + lift(t1, "beatenBySashiRate", 0.12) * 0.62 + (roleFs(2) - 0.5) * 0.18 + (Number(turnZ["2"] || 0) < -0.45 || good(2, "turnTime") ? 0.12 : 0) + (boat1TrustLow ? 0.12 : 0),
      upsetScore: (boat1TrustLow ? 0.22 : 0.08) + upsetLift(t2, "sashiRate", 0.12) * 0.9 + upsetLift(t1, "beatenBySashiRate", 0.12) * 0.8 + (boat2SashiUpset ? 0.16 : 0) + (good(2, "turnTime") ? 0.12 : 0),
      description: "1号艇の踏み込みが甘い場合、2号艇の差し抜けや1残しを警戒。",
      patterns: [[2, 1, "flow"], [2, 3, "flow"], [2, 4, "flow"]],
      reasons: [boat1TrustLow ? "1号艇の信頼度が低め" : null, boat2LegacySashiUpset ? "2号艇の逃がし率が低く差し率が高い" : null, boat1SashiVulnerable && t2UpsetEligible && t2SashiRate !== null && t2SashiRate >= 0.16 ? "1号艇は直近6か月で差され率が高く、2号艇は差し率も高いため、2差し警戒。" : null, good(2, "turnTime") || Number(turnZ["2"] || 0) < -0.45 ? "2号艇のまわり足が良く、差しと2着残りを上げます。" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "3号艇まくりシナリオ",
      attacker: 3,
      baseScore: p(3) + lift(t3, "makuriRate", 0.12) * 0.72 + lift(t1, "beatenByMakuriRate", 0.08) * 0.58 + (boat3StraightStStrong ? 0.18 : startTimingScore(exSt[3], 0.5) > 0.65 ? 0.12 : 0) + (good(3, "straightTime") || Number(straightZ["3"] || 0) < -0.5 ? 0.14 : 0),
      upsetScore: (boat1TrustLow ? 0.18 : 0.08) + upsetLift(t3, "makuriRate", 0.12) * 0.85 + upsetLift(t1, "beatenByMakuriRate", 0.08) * 0.75 + (boat3AttackReady ? 0.18 : 0) + (boat3StraightStStrong ? 0.12 : 0) + (scoreGapSmall ? 0.12 : 0) + (boat2LateRisk ? 0.1 : 0),
      description: "3号艇が先に握ると内が抵抗して隊形が崩れる可能性。直線が良ければまくり切りも警戒。",
      patterns: [[3, 1, "flow"], [3, 4, "flow"], [3, 5, "flow"]],
      reasons: [t3UpsetEligible && t3MakuriRate !== null && t3MakuriRate >= 0.16 ? "3号艇はまくり率が高く、展示STと直線も良いためセンター攻めを評価。" : null, boat1MakuriVulnerable ? "1号艇のまくられ率が高く、3号艇の攻めを強めます。" : null, boat3StraightStStrong ? "3号艇はSTと直線が良く、センター攻めの可能性があります。" : null, boat2LateRisk ? "2号艇の出遅れ率が高く、3号艇の攻め筋が広がります。" : null, Number(straightZ["3"] || 0) < -0.5 ? "3号艇の直線が速い" : null, scoreGapSmall ? "上位評価の差が小さい" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "3号艇まくり差しシナリオ",
      attacker: 3,
      baseScore: p(3) + lift(t3, "makuriSashiRate", 0.1) * 0.72 + lift(t1, "beatenByMakuriSashiRate", 0.06) * 0.56 + (Number(timeZ["3"] || 0) < -0.5 ? 0.1 : 0) + (boat3TurnStrong || Number(turnZ["3"] || 0) < -0.45 ? 0.14 : 0),
      upsetScore: (boat1TrustLow ? 0.18 : 0.08) + upsetLift(t3, "makuriSashiRate", 0.1) * 0.75 + upsetLift(t1, "beatenByMakuriSashiRate", 0.06) * 0.7 + (Number(timeZ["3"] || 0) < -0.5 ? 0.2 : 0) + (boat3TurnStrong ? 0.14 : 0),
      description: "3号艇が握りながら差し場を拾う展開。1残しと4連動を重視。",
      patterns: [[3, 1, "flow"], [3, 4, "flow"], [1, 3, "flow"]],
      reasons: [t3UpsetEligible && t3MakuriSashiRate !== null && t3MakuriSashiRate >= 0.14 ? "3号艇のまくり差し率が高い" : null, boat1MakuriSashiVulnerable ? "1号艇のまくり差され率が高く、3号艇の差し抜けを警戒します。" : null, Number(timeZ["3"] || 0) < -0.5 ? "3号艇の展示タイムが良い" : null, boat3TurnStrong || Number(turnZ["3"] || 0) < -0.45 ? "3号艇のまわり足が良く、まくり差しと2・3着残りを上げます。" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "4号艇まくりシナリオ",
      attacker: 4,
      baseScore: p(4) + lift(t4, "makuriRate", 0.1) * 0.68 + lift(t1, "beatenByMakuriRate", 0.08) * 0.48 + (good(4, "straightTime") ? 0.12 : 0) + (startTimingScore(exSt[4], 0.5) > 0.65 ? 0.12 : 0),
      upsetScore: (startTimingScore(exSt[4], 0.5) > 0.65 ? 0.18 : 0.08) + upsetLift(t4, "makuriRate", 0.1) * 0.75 + upsetLift(t1, "beatenByMakuriRate", 0.08) * 0.65 + (good(4, "straightTime") ? 0.12 : 0) + (boat1TrustLow ? 0.12 : 0) + (boat2LateRisk ? 0.1 : 0),
      description: "4号艇のカド攻めで内が流れる展開。",
      patterns: [[4, 1, "flow"], [4, 3, "flow"], [4, 5, "flow"]],
      reasons: [t4UpsetEligible && t4MakuriRate !== null && t4MakuriRate >= 0.14 ? "4号艇のまくり率が高い" : null, boat1MakuriVulnerable ? "1号艇のまくられ率が高く、4号艇の攻めも警戒します。" : null, startTimingScore(exSt[4], 0.5) > 0.65 ? "4号艇の展示STが早い" : null, good(4, "straightTime") ? "4号艇の直線が良く、カド攻めを上げます。" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "4号艇まくり差しシナリオ",
      attacker: 4,
      baseScore: p(4) + lift(t4, "makuriSashiRate", 0.1) * 0.75 + lift(t1, "beatenByMakuriSashiRate", 0.06) * 0.56 + (Number(timeZ["4"] || 0) < -0.45 ? 0.1 : 0) + (boat4StraightTurnStrong ? 0.18 : Number(straightZ["4"] || 0) < -0.5 ? 0.1 : 0),
      upsetScore: (boat1TrustLow ? 0.18 : 0.08) + upsetLift(t4, "makuriSashiRate", 0.1) * 0.82 + upsetLift(t1, "beatenByMakuriSashiRate", 0.06) * 0.72 + (boat4DevelopSashiReady ? 0.22 : p(3) > p(2) ? 0.1 : 0) + (scoreGapSmall ? 0.1 : 0) + (boat4StraightTurnStrong ? 0.18 : Number(straightZ["4"] || 0) < -0.5 ? 0.16 : 0),
      description: "3号艇が攻めて内を動かし、4号艇が差し場を突く形に注意。直線が良い外艇は抜け出しもある。",
      patterns: [[4, 3, "flow"], [4, 1, "flow"], [3, 4, "flow"]],
      reasons: [boat4DevelopSashiReady && t4UpsetEligible ? "3号艇攻めから4号艇まくり差し率が生きる" : p(3) > p(2) ? "3号艇攻めから4号艇差しの形があり得る" : null, boat1MakuriSashiVulnerable ? "1号艇のまくり差され率が高く、4号艇の展開突きを警戒します。" : null, boat4StraightTurnStrong ? "4号艇はまわり足が良く、3が攻めた後の展開突きに注意。" : null, Number(straightZ["4"] || 0) < -0.5 ? "4号艇の直線が速い" : null, scoreGapSmall ? "1着候補が割れている" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "5号艇展開突きシナリオ",
      attacker: 5,
      baseScore: p(5) + percent01(boat5.motor2Rate, 0.4) * 0.12 + (boat5SecondThirdStrong ? 0.16 : Number(lapZ["5"] || 0) < -0.6 ? 0.1 : 0),
      upsetScore: (Number(lapZ["5"] || 0) < -0.6 || good(5, "lapTime") ? 0.25 : 0.1) + (Number(straightZ["5"] || 0) < -0.6 || good(5, "straightTime") ? 0.2 : 0) + (good(5, "turnTime") ? 0.12 : 0) + (percent01(boat5.motor2Rate, 0.4) > 0.45 ? 0.12 : 0),
      description: "内の攻め合いが長引くと5号艇が展開を突いて連に絡む筋。",
      patterns: [[1, 5, "flow"], [3, 5, "flow"], [4, 5, "flow"]],
      reasons: [boat5SecondThirdStrong ? "5号艇は周回と伸びが良く、2・3着穴で評価。" : null, Number(lapZ["5"] || 0) < -0.6 ? "5号艇のLap Timeが良い" : null, Number(straightZ["5"] || 0) < -0.6 ? "5号艇の直線が速い" : null, percent01(boat5.motor2Rate, 0.4) > 0.45 ? "5号艇のモーター2連率が高い" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "6号艇展開突きシナリオ",
      attacker: 6,
      baseScore: p(6) + percent01(boat6.motor2Rate, 0.4) * 0.1 + rateScore(boat6.techniqueStats?.course6TrifectaRate, 0.45) * 0.12 + (boat6SecondThirdStrong ? 0.14 : Number(straightZ["6"] || 0) < -0.6 ? 0.08 : 0),
      upsetScore: (Number(lapZ["6"] || 0) < -0.6 || good(6, "lapTime") ? 0.22 : 0.08) + (Number(straightZ["6"] || 0) < -0.6 || good(6, "straightTime") ? 0.18 : 0) + (good(6, "turnTime") ? 0.1 : 0) + (percent01(boat6.motor2Rate, 0.4) > 0.45 ? 0.12 : 0),
      description: "外枠でも機力や残り足が上位なら、崩れた展開で3着穴まで。",
      patterns: [[1, 6, "flow"], [3, 6, "flow"], [4, 6, "flow"]],
      reasons: [boat6SecondThirdStrong ? "6号艇は周回と伸びが良く、頭より2・3着穴で評価。" : null, Number(lapZ["6"] || 0) < -0.6 ? "6号艇のLap Timeが良い" : null, Number(straightZ["6"] || 0) < -0.6 ? "6号艇の直線が速い" : null, percent01(boat6.motor2Rate, 0.4) > 0.45 ? "6号艇のモーター2連率が高い" : null]
    })
  ];
  const upsetScenarios = scenarios
    .filter((row) => row.attacker !== 1 && row.upsetScore >= 28 && row.recommendedExtraTickets.length > 0)
    .sort((a, b) => b.upsetScore - a.upsetScore || b.probabilityScore - a.probabilityScore);
  const extraTickets = [];
  const seen = new Set();
  for (const scenario of upsetScenarios) {
    const ticket = scenario.recommendedExtraTickets.find((row) => !seen.has(row.combo));
    if (!ticket) continue;
    seen.add(ticket.combo);
    extraTickets.push({ ...ticket, scenarioName: scenario.scenarioName, upsetScore: scenario.upsetScore });
    if (extraTickets.length >= 6) break;
  }
  for (const scenario of upsetScenarios) {
    for (const ticket of scenario.recommendedExtraTickets) {
      if (seen.has(ticket.combo)) continue;
      seen.add(ticket.combo);
      extraTickets.push({ ...ticket, scenarioName: scenario.scenarioName, upsetScore: scenario.upsetScore });
      if (extraTickets.length >= 6) break;
    }
    if (extraTickets.length >= 6) break;
  }
  return {
    scenarios,
    upsetScenarios,
    upsetAlert: upsetScenarios[0]?.description || "",
    upsetReasons: upsetScenarios.flatMap((row) => row.reasons).filter((reason, index, arr) => reason && arr.indexOf(reason) === index),
    extraTickets
  };
}

function buildTendencySummary(scoredBoats = []) {
  const fields = [
    "escapeRate",
    "beatenBySashiRate",
    "beatenByMakuriRate",
    "beatenByMakuriSashiRate",
    "sashiRate",
    "makuriRate",
    "makuriSashiRate",
    "allCourseWinRate",
    "allCourseSashiRate",
    "allCourseMakuriRate",
    "allCourseMakuriSashiRate",
    "allCourseAvgST",
    "avgStartTiming",
    "lateStartRate",
    "earlyStartRate"
  ];
  const preview = scoredBoats.map((boat) => {
    const tendency = boat.playerTendency || {};
    return {
      boat: boat.boat,
      course: boat.course,
      last6mRaceCount: tendency.last6mRaceCount ?? null,
      courseSpecificLast6mRaceCount: tendency.courseSpecificLast6mRaceCount ?? tendency.last6mRaceCount ?? null,
      allCourseLast6mRaceCount: tendency.allCourseLast6mRaceCount ?? null,
      sampleStatus: tendencySampleStatus(tendency),
      sampleWeight: tendencySampleWeight(tendency),
      allCourseWinRate: optionalRate01(tendency.allCourseWinRate),
      allCourseSashiRate: optionalRate01(tendency.allCourseSashiRate),
      allCourseMakuriRate: optionalRate01(tendency.allCourseMakuriRate),
      allCourseMakuriSashiRate: optionalRate01(tendency.allCourseMakuriSashiRate),
      allCourseAvgST: finiteNumber(tendency.allCourseAvgST, null),
      escapeRate: optionalRate01(tendency.escapeRate),
      beatenBySashiRate: optionalRate01(tendency.beatenBySashiRate),
      beatenByMakuriRate: optionalRate01(tendency.beatenByMakuriRate),
      beatenByMakuriSashiRate: optionalRate01(tendency.beatenByMakuriSashiRate),
      sashiRate: optionalRate01(tendency.sashiRate),
      makuriRate: optionalRate01(tendency.makuriRate),
      makuriSashiRate: optionalRate01(tendency.makuriSashiRate),
      avgST: finiteNumber(tendency.avgStartTiming, null),
      lateStartRate: optionalRate01(tendency.lateStartRate),
      earlyStartRate: optionalRate01(tendency.earlyStartRate),
      techniqueBoost: finiteNumber(boat.scoreParts?.techniqueBoost, 0),
      allCourseReferenceBoost: finiteNumber(boat.scoreParts?.allCourseReferenceBoost, 0),
      startTendencyBoost: finiteNumber(boat.scoreParts?.startTendencyBoost, 0),
      lateRatePenalty: finiteNumber(boat.scoreParts?.lateRatePenalty, 0)
    };
  });
  const rowsWithData = preview.filter((row) =>
    fields.some((field) => row[field === "avgStartTiming" ? "avgST" : field] !== null)
  ).length;
  const okRows = preview.filter((row) => row.sampleStatus === "ok").length;
  const weightedRows = preview.filter((row) => row.sampleWeight > 0).length;
  const sparse = preview.some((row) =>
    ["small_sample", "very_small_sample", "insufficient_history"].includes(row.sampleStatus)
  );
  return {
    available: rowsWithData > 0 && weightedRows > 0,
    complete: rowsWithData >= 6 && okRows >= 6,
    sparse,
    okRows,
    weightedRows,
    rowsWithData,
    preview
  };
}

function buildCoefficientContributionByBoat(scoredBoats = [], featureScores = {}, scoringConfig = DEFAULT_SCORING_CONFIG) {
  return scoredBoats.map((boat) => {
    const scoreParts = boat.scoreParts || {};
    const featureRow = featureScores.byBoat?.[String(boat.boat)] || {};
    const headWeights = scoringConfig.scoringCoefficients?.headScore || {};
    const residualWeights = scoringConfig.scoringCoefficients?.partnerResidualScore || {};
    const headContribution = weightedAverageFromWeights(featureRow.scores || {}, headWeights, 0.5);
    const residualContribution = weightedAverageFromWeights(featureRow.scores || {}, residualWeights, 0.5);
    return {
      boat: boat.boat,
      totalScore: finiteNumber(boat.score, 0),
      motorRankContribution: finiteNumber(scoreParts.motorRankBoost, 0),
      motorRankScore: boat.professionalFactors?.motorRankScore ?? featureRow.scores?.motorRank ?? null,
      motorStrengthLabel: boat.motorStrengthLabel ?? boat.professionalFactors?.motorStrengthLabel ?? null,
      startReliabilityContribution: finiteNumber(scoreParts.startReliabilityBoost, 0),
      startReliability: boat.professionalFactors?.startReliability || null,
      venueBiasContribution: finiteNumber(scoreParts.venueBiasBoost, 0),
      conditionContribution: finiteNumber(boat.raceFlowAdjustment, 0),
      majorScoreParts: {
        base: finiteNumber(scoreParts.base, 0),
        roleFeatureBoost: finiteNumber(scoreParts.roleFeatureBoost, 0),
        motorRankBoost: finiteNumber(scoreParts.motorRankBoost, 0),
        startReliabilityBoost: finiteNumber(scoreParts.startReliabilityBoost, 0),
        techniqueBoost: finiteNumber(scoreParts.techniqueBoost, 0),
        venueBiasBoost: finiteNumber(scoreParts.venueBiasBoost, 0),
        weakMotorHeadPenalty: finiteNumber(scoreParts.weakMotorHeadPenalty, 0),
        lateRatePenalty: finiteNumber(scoreParts.lateRatePenalty, 0)
      },
      headCoefficientScore: headContribution.score,
      residualCoefficientScore: residualContribution.score
    };
  });
}

function expectedHeadFromScenario(scenario = null) {
  if (!scenario) return null;
  const id = String(scenario.id || "");
  if (id === "escape_1") return 1;
  if (id === "sashi_2") return 2;
  if (id === "makuri_3" || id === "makuri_sashi_3") return 3;
  if (id === "makuriSashi_4" || id === "four_beneficiary" || id === "second_wave_4") return 4;
  const firstPattern = Array.isArray(scenario.patterns) ? scenario.patterns[0] : null;
  return Number.isInteger(firstPattern?.[0]) ? firstPattern[0] : null;
}

export function checkPredictionConsistency(prediction = {}) {
  const issues = [];
  const warnings = [];
  const mainScenario =
    prediction.mainScenarioGroup ||
    prediction.raceFlowScenario?.mainScenarioGroup ||
    prediction.raceFlowScenario?.mainScenario ||
    null;
  const derivedScenario =
    prediction.derivedScenarioGroup ||
    prediction.raceFlowScenario?.derivedScenarioGroup ||
    null;
  const groupedMainTickets = prediction.ticketGroups && Array.isArray(prediction.ticketGroups.mainTickets)
    ? prediction.ticketGroups.mainTickets
    : null;
  const visibleTicketRows = prediction.ticketGroups
    ? [
      ...safeArray(prediction.ticketGroups.mainTickets),
      ...safeArray(prediction.ticketGroups.secondaryTickets),
      ...safeArray(prediction.ticketGroups.upsetTickets),
      ...safeArray(prediction.ticketGroups.referenceTickets)
    ]
    : [];
  const mainTickets = groupedMainTickets !== null
    ? groupedMainTickets
    : Array.isArray(prediction.tickets?.trifecta)
      ? prediction.tickets.trifecta.slice(0, 6)
      : [];
  const mainHeads = [...new Set(mainTickets.map((ticket) => Number(ticket?.boats?.[0])).filter(Number.isFinite))];
  const expectedHead = expectedHeadFromScenario(mainScenario);
  if (expectedHead && mainHeads.length > 0 && !mainHeads.includes(expectedHead)) {
    issues.push(`mainScenario ${mainScenario.id} expects ${expectedHead}-head, but main tickets start with ${mainHeads.join("/")}`);
  }
  const fourHeadScore = finiteNumber(
    prediction.raceFlowScenario?.scoreByBoat?.["4"]?.headScore ?? prediction.raceFlowScenario?.scoreByBoat?.[4]?.headScore,
    0
  );
  const fourBenefitScore = finiteNumber(
    prediction.raceFlowScenario?.scenarioFamilies?.find((row) => row.id === "four_beneficiary")?.score01,
    0
  );
  const hasFourTicket = mainTickets.some((ticket) => Number(ticket?.boats?.[0]) === 4) ||
    visibleTicketRows.some((ticket) => Number(ticket?.boats?.[0]) === 4) ||
    (Array.isArray(prediction.extraTickets) && prediction.extraTickets.some((ticket) => String(ticket?.combo || "").startsWith("4-")));
  if ((fourHeadScore >= 0.52 || fourBenefitScore >= 0.54) && !hasFourTicket) {
    warnings.push("4-head warning is high but 4-head tickets are not reflected in main or upset tickets");
  }
  const confidenceScore = finiteNumber(prediction.confidenceScore ?? prediction.confidence?.score, null);
  if (confidenceScore !== null && confidenceScore < 45) {
    warnings.push("見送り推奨: confidence is low, tickets should be treated as reference");
  }
  const topScenarioScore = finiteNumber(mainScenario?.score01, null);
  const derivedScenarioScore = finiteNumber(derivedScenario?.score01, null);
  if (topScenarioScore !== null && derivedScenarioScore !== null && Math.abs(topScenarioScore - derivedScenarioScore) < 0.04) {
    warnings.push("見送り推奨: main and derived scenario scores are close");
  }
  return {
    ok: issues.length === 0,
    referenceOnly: warnings.some((warning) => warning.includes("見送り推奨")),
    expectedHead,
    mainHeads,
    issues,
    warnings
  };
}

function ticketBoats(ticket = {}) {
  const fromArray = Array.isArray(ticket?.boats)
    ? ticket.boats.map((value) => Number(value))
    : [];
  const fromCombo = String(ticket?.combo ?? ticket?.ticket ?? ticket?.selection ?? "")
    .split("-")
    .map((value) => Number(value));
  const boats = (fromArray.length >= 3 ? fromArray : fromCombo)
    .filter((value) => Number.isInteger(value) && value >= 1 && value <= 6)
    .slice(0, 3);
  const unique = new Set(boats);
  return boats.length === 3 && unique.size === 3 ? boats : [];
}

function ticketCombo(ticket = {}) {
  const boats = ticketBoats(ticket);
  return boats.length === 3 ? boats.join("-") : String(ticket?.combo ?? ticket?.ticket ?? "");
}

function getScenarioRows(prediction = {}) {
  return [
    ...safeArray(prediction.raceFlowScenario?.scenarioFamilies),
    ...safeArray(prediction.raceFlowScenario?.scenarios)
  ];
}

function scenarioAliasesForHead(head) {
  if (head === 1) return ["escape_1"];
  if (head === 2) return ["sashi_2"];
  if (head === 3) return ["makuri_3", "makuri_sashi_3"];
  if (head === 4) return ["four_beneficiary", "makuriSashi_4", "second_wave_4"];
  if (head === 5) return ["outer_follow_5", "outside_follow_5_6"];
  if (head === 6) return ["outer_follow_6", "outside_follow_5_6"];
  return [];
}

function bestScenarioForHead(prediction = {}, head) {
  const aliases = new Set(scenarioAliasesForHead(head));
  const rows = getScenarioRows(prediction)
    .filter((row) => aliases.has(row?.id))
    .sort((a, b) => score01(b?.score, 0) - score01(a?.score, 0));
  return rows[0] || null;
}

function topScenarioRank(prediction = {}, scenarioId) {
  const rows = getScenarioRows(prediction)
    .slice()
    .sort((a, b) => score01(b?.score, 0) - score01(a?.score, 0));
  const index = rows.findIndex((row) => row?.id === scenarioId);
  return index < 0 ? 99 : index + 1;
}

function flowBoatScore(prediction = {}, boat, field, fallback = 0.5) {
  const byBoat = prediction.raceFlowScenario?.scoreByBoat || {};
  const raw = byBoat[String(boat)] || byBoat[boat] || {};
  const splitRows = prediction.headPartnerSplitPreview || prediction.raceFlowScenario?.headPartnerSplit || [];
  const split = safeArray(splitRows).find((row) => Number(row?.boat) === Number(boat)) || {};
  return score01(raw?.[field] ?? split?.[field], fallback);
}

function liveFeatureSupport(prediction = {}, canonicalRaceData = null, boat, fallback = 0.5) {
  const entry = safeArray(canonicalRaceData?.entries)
    .find((row) => Number(row?.boat ?? row?.lane) === Number(boat)) || {};
  const scores = {
    exST: featureScore01(prediction, boat, "exST", null),
    exTime: featureScore01(prediction, boat, "exTime", null),
    lapTime: featureScore01(prediction, boat, "lapTime", null),
    straightTime: featureScore01(prediction, boat, "straightTime", null),
    turnTime: featureScore01(prediction, boat, "turnTime", null),
    motorRank: featureScore01(prediction, boat, "motorRank", null),
    motor2Rate: featureScore01(prediction, boat, "motor2Rate", null)
  };
  const values = Object.entries(scores)
    .map(([field, value]) => {
      if (value !== null && value !== undefined) return Number(value);
      if (field === "motor2Rate" && entry.motor2Rate !== null && entry.motor2Rate !== undefined) {
        return percent01(entry.motor2Rate, fallback);
      }
      return null;
    })
    .filter((value) => Number.isFinite(value));
  if (values.length === 0) return fallback;
  return clamp(values.reduce((sum, value) => sum + value, 0) / values.length);
}

function partnerVenueSupport(prediction = {}, head, second, decisionScenarioId = null) {
  const residual = prediction.raceFlowScenario?.decisionResidualScores || {};
  if (head === 4) {
    const exacta = residual.venue4ExactaRates?.[`${head}-${second}`];
    const secondRate = residual.venue4SecondRates?.[String(second)];
    return {
      score: score01(exacta ?? secondRate, 0.5),
      known: exacta !== null && exacta !== undefined || secondRate !== null && secondRate !== undefined
    };
  }
  if (head === 3 && second === 1) {
    const raw = decisionScenarioId === "makuri_sashi_3"
      ? residual.venueMakuriSashiBoat1SecondRate
      : residual.venueMakuriBoat1SecondRate;
    return { score: score01(raw, 0.5), known: raw !== null && raw !== undefined };
  }
  if (head === 2 && second === 1) {
    const raw = residual.venueSashiBoat1SecondRate;
    return { score: score01(raw, 0.5), known: raw !== null && raw !== undefined };
  }
  return { score: 0.5, known: false };
}

function ticketDecisionSupport(ticket = {}) {
  return score01(ticket?.decisionCompatibilityScore ?? ticket?.ticketDecisionCompatibilityScore, 0.5);
}

function gradeFromScore(score, gradeCap = null) {
  let grade = score >= 72 ? "A" : score >= 60 ? "B" : score >= 48 ? "C" : "reject";
  const order = { A: 3, B: 2, C: 1, reject: 0 };
  if (gradeCap && order[grade] > order[gradeCap]) grade = gradeCap;
  return grade;
}

function formatTicketReason(reasons = []) {
  return reasons.filter(Boolean).slice(0, 3).join(" / ");
}

export function evaluateTicketPlausibility(ticket = {}, finalPrediction = {}, canonicalRaceData = null) {
  const boats = ticketBoats(ticket);
  const combo = boats.join("-");
  const reasons = [];
  const rejectReasons = [];
  const gradeCaps = [];
  if (boats.length !== 3) {
    return {
      ...ticket,
      ticket: combo || String(ticket?.combo ?? ticket?.ticket ?? "-"),
      combo: combo || String(ticket?.combo ?? ticket?.ticket ?? "-"),
      boats,
      plausible: false,
      score: 0,
      plausibilityScore: 0,
      grade: "reject",
      reasons,
      rejectReasons: ["invalid trifecta combo"]
    };
  }

  const [head, second, third] = boats;
  const scenario = bestScenarioForHead(finalPrediction, head);
  const scenarioId = scenario?.id || ticket?.scenarioId || ticket?.decisionScenarioId || null;
  const scenarioSupport = Math.max(score01(scenario?.score, 0), score01(ticket?.upsetScore, 0));
  const scenarioRank = scenarioId ? topScenarioRank(finalPrediction, scenarioId) : 99;
  const headScore = flowBoatScore(finalPrediction, head, "headScore", 0.35);
  const attackerScore = flowBoatScore(finalPrediction, head, "attackerScore", 0.35);
  const beneficiaryScore = flowBoatScore(finalPrediction, head, "beneficiaryScore", 0.35);
  const headTriggerScore = flowBoatScore(finalPrediction, head, "scenarioTriggerScore", 0.35);
  const firstProbability = firstPlaceProbability01(finalPrediction, head, 0.03);
  const liveHeadSupport = liveFeatureSupport(finalPrediction, canonicalRaceData, head, 0.5);
  const headSupport = clamp(Math.max(
    headScore,
    scenarioSupport,
    attackerScore * 0.9,
    beneficiaryScore * (head === 4 ? 1.05 : 0.82),
    headTriggerScore * 0.82,
    firstProbability * 1.6,
    liveHeadSupport * 0.78
  ));

  if (headScore >= 0.54) reasons.push(`${head}頭スコア`);
  if (scenarioSupport >= 0.54) reasons.push(scenarioId === "four_beneficiary" ? "4展開拾い" : `${head}頭シナリオ`);
  if (head === 4 && beneficiaryScore >= 0.56) reasons.push("4 beneficiary");
  if (liveHeadSupport >= 0.62) reasons.push(`${head}足色`);
  if (firstProbability >= 0.22) reasons.push(`${head}一着確率`);
  const validatedHead =
    finalPrediction.headValidation?.byBoat?.[String(head)] ||
    finalPrediction.finalPrediction?.debug?.headValidation?.byBoat?.[String(head)] ||
    null;
  if (validatedHead?.status === "partner_only" || validatedHead?.status === "rejected") {
    if (head >= 5 || validatedHead.status === "rejected") {
      rejectReasons.push(`head validation failed: ${head} is ${validatedHead.status}`);
    } else {
      gradeCaps.push("C");
      reasons.push("頭検証は条件付き");
    }
  }

  if (head >= 5 && headSupport < 0.66) {
    rejectReasons.push("5・6号艇の頭は強い根拠が不足");
  } else if (headSupport < 0.42) {
    rejectReasons.push("head gate failed: head support is weak");
  } else if (headSupport < 0.5) {
    gradeCaps.push("C");
    reasons.push("頭は条件付き");
  }

  if (!scenarioId || scenarioSupport < 0.36) {
    rejectReasons.push("scenario gate failed: top scenario support is weak");
  } else if (scenarioRank > 3 && scenarioSupport < 0.55) {
    rejectReasons.push("scenario gate failed: ticket does not match top scenarios");
  } else if (scenarioSupport < 0.48) {
    gradeCaps.push("C");
    reasons.push("シナリオ薄め");
  }

  const secondScore = flowBoatScore(finalPrediction, second, "secondScore", 0.42);
  const secondResidual = flowBoatScore(finalPrediction, second, "residualScore", 0.42);
  const secondLive = liveFeatureSupport(finalPrediction, canonicalRaceData, second, 0.5);
  const venue = partnerVenueSupport(finalPrediction, head, second, scenarioId);
  const partnerSupport = clamp(
    secondScore * 0.42 +
    secondResidual * 0.24 +
    secondLive * 0.16 +
    venue.score * 0.12 +
    ticketPartnerScore01(finalPrediction, head, second) * 0.06
  );
  if (secondScore >= 0.56 || secondResidual >= 0.56) reasons.push(`${second}残り`);
  if (venue.known && venue.score >= 0.56) reasons.push(`会場${head}-${second}`);
  if (secondLive >= 0.62) reasons.push(`${second}相手足`);
  if (partnerSupport < 0.38) rejectReasons.push("partner gate failed: second boat support is weak");
  else if (partnerSupport < 0.5) gradeCaps.push("C");

  const thirdScore = flowBoatScore(finalPrediction, third, "thirdScore", 0.4);
  const thirdResidual = flowBoatScore(finalPrediction, third, "residualScore", 0.4);
  const thirdLive = liveFeatureSupport(finalPrediction, canonicalRaceData, third, 0.5);
  const thirdSupport = clamp(thirdScore * 0.46 + thirdResidual * 0.28 + thirdLive * 0.26);
  if (thirdScore >= 0.55 || thirdResidual >= 0.55 || thirdLive >= 0.62) reasons.push(`${third}三着根拠`);
  if (thirdSupport < 0.34) rejectReasons.push("third gate failed: third boat support is weak");
  else if (thirdSupport < 0.47) gradeCaps.push("C");

  const residualScores = finalPrediction.raceFlowScenario?.decisionResidualScores || {};
  const boat1Residual = score01(residualScores.boat1ResidualAfterAttackScore, flowBoatScore(finalPrediction, 1, "residualScore", 0.5));
  const boat3Residual = score01(residualScores.boat3ResidualScore, flowBoatScore(finalPrediction, 3, "residualScore", 0.5));
  const boat5Follow = score01(residualScores.boat5LinkedFollowScore, flowBoatScore(finalPrediction, 5, "residualScore", 0.5));
  const boat6Follow = score01(residualScores.boat6LinkedFollowScore, flowBoatScore(finalPrediction, 6, "residualScore", 0.5));
  const insideCollapse = score01(residualScores.insideCollapseScore, clamp(1 - boat1Residual));
  const boat1KeepSupport = (boat1Residual * 0.46) +
    (featureScore01(finalPrediction, 1, "lapTime", 0.5) * 0.18) +
    (featureScore01(finalPrediction, 1, "turnTime", 0.5) * 0.18) +
    (featureScore01(finalPrediction, 1, "motorRank", featureScore01(finalPrediction, 1, "motor2Rate", 0.5)) * 0.18);
  const boat3KeepSupport = (boat3Residual * 0.5) +
    (featureScore01(finalPrediction, 3, "lapTime", 0.5) * 0.18) +
    (featureScore01(finalPrediction, 3, "turnTime", 0.5) * 0.2) +
    (featureScore01(finalPrediction, 3, "motorRank", 0.5) * 0.12);
  const venue43 = partnerVenueSupport(finalPrediction, 4, 3, "four_beneficiary");
  const venue41 = partnerVenueSupport(finalPrediction, 4, 1, "four_beneficiary");
  const venue45 = partnerVenueSupport(finalPrediction, 4, 5, "four_beneficiary");
  const venue46 = partnerVenueSupport(finalPrediction, 4, 6, "four_beneficiary");
  const venue31 = partnerVenueSupport(finalPrediction, 3, 1, scenarioId);
  const venue21 = partnerVenueSupport(finalPrediction, 2, 1, scenarioId);
  let specialScoreBoost = 0;

  if (head === 4) {
    if (scenarioId !== "four_beneficiary" && scenarioId !== "makuriSashi_4" && scenarioSupport < 0.58) {
      gradeCaps.push("C");
      reasons.push("4頭は条件付き");
    }
    if (second === 3) {
      const venueNotRare = !venue43.known || venue43.score >= 0.43;
      const allow43 = boat3Residual >= 0.58 &&
        boat3KeepSupport >= 0.58 &&
        venueNotRare;
      if (!allow43) {
        rejectReasons.push("4-3 rejected: 3 residual/lap-turn or venue support is weak");
      } else {
        reasons.push("3残り強");
      }
    }
    if (second === 1) {
      if (boat1KeepSupport >= 0.56 || venue41.score >= 0.54) {
        reasons.push("1残り強");
        specialScoreBoost += 0.08;
      } else {
        gradeCaps.push("C");
        reasons.push("1残り条件付き");
      }
    }
    if (second === 5) {
      const outsideSupported = boat1Residual <= 0.54 && insideCollapse >= 0.52 && (boat5Follow >= 0.52 || venue45.score >= 0.54);
      if (!outsideSupported) {
        gradeCaps.push("C");
        reasons.push("5外追走は条件付き");
      } else {
        reasons.push("5外追走");
      }
    }
    if (second === 6) {
      const outsideSupported = boat1Residual <= 0.5 && insideCollapse >= 0.56 && (boat6Follow >= 0.54 || venue46.score >= 0.54);
      if (!outsideSupported) {
        rejectReasons.push("4-6 rejected: inside collapse and boat6 follow support are insufficient");
      } else {
        reasons.push("6外追走");
      }
    }
  }

  if (head === 3 && second === 1) {
    const allow31 = boat1Residual >= 0.55 && boat1KeepSupport >= 0.54 && (!venue31.known || venue31.score >= 0.44);
    if (!allow31) {
      gradeCaps.push("C");
      reasons.push("3-1は1残り弱めで降格");
      if (boat1Residual < 0.42 && venue31.known && venue31.score < 0.4) {
        rejectReasons.push("3-1 rejected: boat1 residual and venue makuri 1-second support are weak");
      }
    } else {
      reasons.push("3攻め後の1残り");
    }
  }

  if (head === 2) {
    const sashi2 = scenarioScore01(finalPrediction, "sashi_2", 0.4);
    if (second === 1) {
      const allow21 = sashi2 >= 0.5 && boat1Residual >= 0.52 && (!venue21.known || venue21.score >= 0.44);
      if (!allow21) {
        gradeCaps.push(sashi2 < 0.43 ? "reject" : "C");
        reasons.push("2差しは条件付き");
      } else {
        reasons.push("2差し+1残り");
      }
    } else if (sashi2 < 0.44) {
      gradeCaps.push("C");
      reasons.push("2頭シナリオ薄め");
    }
  }

  const decisionSupport = ticketDecisionSupport(ticket);
  if (decisionSupport < 0.38 && Math.max(headSupport, scenarioSupport) < 0.68) {
    rejectReasons.push("venue combo gate failed: decision-conditioned combo support is rare");
  } else if (decisionSupport < 0.48) {
    gradeCaps.push("C");
    reasons.push("会場相性薄め");
  }

  const mainScenario = finalPrediction.mainScenarioGroup || finalPrediction.raceFlowScenario?.mainScenarioGroup || null;
  const derivedScenario = finalPrediction.derivedScenarioGroup || finalPrediction.raceFlowScenario?.derivedScenarioGroup || null;
  const expectedMainHead = expectedHeadFromScenario(mainScenario);
  const expectedDerivedHead = expectedHeadFromScenario(derivedScenario);
  if (
    expectedMainHead &&
    head !== expectedMainHead &&
    head !== expectedDerivedHead &&
    scenarioSupport < 0.58
  ) {
    gradeCaps.push("C");
    reasons.push("説明本線とは別筋");
  }
  if (finalPrediction.finalScenarioConsistencyCheck?.ok === false) {
    gradeCaps.push("C");
    reasons.push("整合性チェック注意");
  }

  const score = Math.round(clamp(
    headSupport * 0.28 +
    scenarioSupport * 0.2 +
    partnerSupport * 0.18 +
    thirdSupport * 0.12 +
    decisionSupport * 0.1 +
    liveHeadSupport * 0.07 +
    score01(ticket?.probability, 0.02) * 0.05 +
    specialScoreBoost
  ) * 100);
  const cap = gradeCaps.includes("reject")
    ? "reject"
    : gradeCaps.includes("C")
      ? "C"
      : gradeCaps.includes("B")
        ? "B"
        : null;
  const grade = rejectReasons.length > 0 ? "reject" : gradeFromScore(score, cap);
  const plausible = grade !== "reject";
  return {
    ...ticket,
    ticket: combo,
    combo,
    boats,
    plausible,
    score,
    plausibilityScore: score,
    grade,
    scenarioId,
    gateScores: {
      headSupport: roundNumber(headSupport * 100, 1),
      scenarioSupport: roundNumber(scenarioSupport * 100, 1),
      partnerSupport: roundNumber(partnerSupport * 100, 1),
      thirdSupport: roundNumber(thirdSupport * 100, 1),
      decisionSupport: roundNumber(decisionSupport * 100, 1),
      liveHeadSupport: roundNumber(liveHeadSupport * 100, 1)
    },
    reasons: reasons.filter(Boolean).filter((reason, index, arr) => arr.indexOf(reason) === index),
    rejectReasons,
    displayReason: plausible
      ? formatTicketReason(reasons)
      : formatTicketReason(rejectReasons)
  };
}

function sortEvaluatedTickets(a, b) {
  const gradeOrder = { A: 3, B: 2, C: 1, reject: 0 };
  return (gradeOrder[b.grade] || 0) - (gradeOrder[a.grade] || 0) ||
    Number(b.score || 0) - Number(a.score || 0) ||
    Number(b.probability || 0) - Number(a.probability || 0);
}

function collectTicketCandidates(prediction = {}) {
  const rows = [];
  const seen = new Set();
  const add = (ticket, source) => {
    const combo = ticketCombo(ticket);
    if (!combo || seen.has(combo)) return;
    const boats = ticketBoats({ ...ticket, combo });
    if (boats.length !== 3) return;
    seen.add(combo);
    rows.push({ ...ticket, combo, boats, source: ticket?.source || source });
  };
  safeArray(prediction.tickets?.trifecta).slice(0, 36).forEach((ticket) => add(ticket, "probability"));
  safeArray(prediction.raceFlowExtraTickets).forEach((ticket) => add(ticket, "raceFlow"));
  safeArray(prediction.extraTickets).forEach((ticket) => add(ticket, "development"));
  for (const scenario of safeArray(prediction.developmentScenarios)) {
    for (const ticket of safeArray(scenario.recommendedExtraTickets)) {
      add({ ...ticket, scenarioName: scenario.scenarioName, upsetScore: scenario.upsetScore }, "developmentScenario");
    }
  }
  return rows;
}

export function buildTicketPlausibilityGroups(prediction = {}, canonicalRaceData = null) {
  const evaluatedTickets = collectTicketCandidates(prediction)
    .map((ticket) => evaluateTicketPlausibility(ticket, prediction, canonicalRaceData))
    .sort(sortEvaluatedTickets);
  const plausible = evaluatedTickets.filter((ticket) => ticket.plausible);
  const rejectedTickets = evaluatedTickets
    .filter((ticket) => !ticket.plausible)
    .slice(0, 24)
    .map((ticket) => ({
      ticket: ticket.combo,
      rejectedBecause: ticket.rejectReasons,
      score: ticket.score,
      scenarioId: ticket.scenarioId
    }));
  const mainScenario = prediction.mainScenarioGroup || prediction.raceFlowScenario?.mainScenarioGroup || null;
  const derivedScenario = prediction.derivedScenarioGroup || prediction.raceFlowScenario?.derivedScenarioGroup || null;
  const mainScenarioScore = score01(mainScenario?.score, null);
  const derivedScenarioScore = score01(derivedScenario?.score, null);
  const scenarioClose = mainScenarioScore !== null &&
    derivedScenarioScore !== null &&
    Math.abs(mainScenarioScore - derivedScenarioScore) < 0.035;
  const consistencyFailed = prediction.finalScenarioConsistencyCheck?.ok === false;
  const dataWarnings = safeArray(prediction.raceFlowScenario?.dataWarnings);
  const hasA = plausible.some((ticket) => ticket.grade === "A");
  const noBuyReasons = [
    !hasA ? "A評価の買い目なし" : null,
    mainScenarioScore !== null && mainScenarioScore < 0.43 ? "本線シナリオ信頼度が低い" : null,
    scenarioClose ? "主要シナリオの差が小さい" : null,
    consistencyFailed ? "説明と買い目の整合性チェック注意" : null,
    dataWarnings.length > 0 ? "入力データに不足または矛盾あり" : null
  ].filter(Boolean);
  const noBuyRecommended = noBuyReasons.length > 0;
  const mainTickets = noBuyRecommended
    ? []
    : plausible.filter((ticket) => ticket.grade === "A").slice(0, 6);
  const secondaryTickets = noBuyRecommended
    ? []
    : plausible.filter((ticket) => ticket.grade === "B").slice(0, 6);
  const upsetTickets = noBuyRecommended
    ? []
    : plausible.filter((ticket) => ticket.grade === "C").slice(0, 8);
  const referenceTickets = noBuyRecommended
    ? plausible.slice(0, 6).map((ticket) => ({ ...ticket, referenceOnly: true }))
    : [];
  const summary = {
    noBuyRecommended,
    referenceOnly: noBuyRecommended,
    noBuyReason: noBuyRecommended
      ? "見送り推奨: レースの軸が割れており、無理に買うレースではありません。"
      : "",
    noBuyReasons,
    mainCount: mainTickets.length,
    secondaryCount: secondaryTickets.length,
    upsetCount: upsetTickets.length,
    referenceCount: referenceTickets.length,
    rejectedCount: rejectedTickets.length,
    evaluatedCount: evaluatedTickets.length
  };
  return {
    mainTickets,
    secondaryTickets,
    upsetTickets,
    referenceTickets,
    rejectedTickets,
    evaluatedTickets: evaluatedTickets.slice(0, 36),
    noBuyRecommended,
    referenceOnly: noBuyRecommended,
    noBuyReason: summary.noBuyReason,
    noBuyReasons,
    summary
  };
}

function qualityLabelFromCount(count, total = 6, highRatio = 0.84, mediumRatio = 0.5) {
  const ratio = total > 0 ? count / total : 0;
  if (ratio >= highRatio) return "high";
  if (ratio >= mediumRatio) return "medium";
  return "low";
}

function combineQuality(labels = []) {
  const scores = { high: 2, medium: 1, low: 0 };
  const avg = labels.reduce((sum, label) => sum + (scores[label] ?? 0), 0) / Math.max(1, labels.length);
  if (avg >= 1.55) return "high";
  if (avg >= 0.85) return "medium";
  return "low";
}

function buildDataQualityReport(prediction = {}) {
  const boats = safeArray(prediction.scoredBoats);
  const total = Math.max(1, Math.min(6, boats.length || 6));
  const countField = (field) => boats.filter((boat) => boat?.[field] !== null && boat?.[field] !== undefined).length;
  const exSTCount = Object.values(prediction.exhibition?.exhibitionStartByBoat || {}).filter((value) => finiteNumber(value, null) !== null).length;
  const exTimeCount = Object.values(prediction.exhibition?.exhibitionTimeByBoat || {}).filter((value) => finiteNumber(value, null) !== null).length;
  const lapTimeCount = countField("lapTime");
  const straightTimeCount = countField("straightTime");
  const turnTimeCount = countField("turnTime");
  const motor2RateCount = countField("motor2Rate");
  const motorRankCount = boats.filter((boat) =>
    finiteNumber(boat?.motorRankAtVenue, null) !== null ||
    finiteNumber(boat?.motorPercentileAtVenue, null) !== null ||
    finiteNumber(prediction.featureScores?.byBoat?.[String(boat.boat)]?.scores?.motorRank, null) !== null
  ).length;
  const tendencyRows = safeArray(prediction.tendencySummary?.preview);
  const tendencyOkOrSmallCount = tendencyRows.filter((row) => ["ok", "small_sample"].includes(row?.sampleStatus)).length;
  const tendencyAnyCount = tendencyRows.filter((row) => row?.sampleWeight > 0).length;
  const decisionSamples = prediction.raceFlowScenario?.decisionResidualScores?.sampleWeights || {};
  const venueSampleCount = Object.values(decisionSamples).filter((value) => finiteNumber(value, 0) >= 0.35).length;
  const venueAvailable = prediction.raceFlowScenario?.quality?.venueAvailable === true || venueSampleCount > 0;
  const conditions = prediction.race?.conditions || prediction.exhibition?.conditions || {};
  const conditionAvailable = conditions?.available === true ||
    ["windDirection", "windSpeed", "waveHeight", "weather", "temperature", "waterTemperature"].some((field) =>
      conditions?.[field] !== null && conditions?.[field] !== undefined
    );
  const exhibitionQuality = combineQuality([
    qualityLabelFromCount(exSTCount, total),
    qualityLabelFromCount(exTimeCount, total),
    qualityLabelFromCount(lapTimeCount, total),
    qualityLabelFromCount(straightTimeCount, total),
    qualityLabelFromCount(turnTimeCount, total)
  ]);
  const tendencyQuality = tendencyOkOrSmallCount >= 4 ? "high" : tendencyAnyCount >= 3 ? "medium" : "low";
  const venueBiasQuality = venueAvailable && venueSampleCount >= 2 ? "high" : venueAvailable ? "medium" : "low";
  const motorQuality = combineQuality([
    qualityLabelFromCount(motor2RateCount, total),
    qualityLabelFromCount(motorRankCount, total)
  ]);
  const conditionQuality = conditionAvailable ? "medium" : "low";
  const overall = combineQuality([exhibitionQuality, tendencyQuality, venueBiasQuality, motorQuality, conditionQuality]);
  const warnings = [
    exhibitionQuality === "low" ? "展示データ品質が低く、足色評価を弱めます。" : null,
    tendencyQuality === "low" ? "戦法データのサンプルが不足しているため、傾向は強く使いません。" : null,
    venueBiasQuality === "low" ? "会場バイアスのサンプルが薄く、組み合わせ補正は控えめです。" : null,
    motorQuality === "low" ? "モーター評価データが不足しています。" : null,
    conditionQuality === "low" ? "水面条件が未取得のため、風波補正なしで評価します。" : null
  ].filter(Boolean);
  return {
    exhibitionQuality,
    tendencyQuality,
    venueBiasQuality,
    motorQuality,
    conditionQuality,
    overall,
    counts: {
      exSTCount,
      exTimeCount,
      lapTimeCount,
      straightTimeCount,
      turnTimeCount,
      motor2RateCount,
      motorRankCount,
      tendencyOkOrSmallCount,
      tendencyAnyCount,
      venueSampleCount,
      conditionAvailable: conditionAvailable ? 1 : 0,
      total
    },
    warnings
  };
}

function strongSupportList(items = []) {
  return items.filter((item) => item.ok).map((item) => item.reason);
}

function buildHeadValidation(prediction = {}, dataQuality = {}, scoringConfig = DEFAULT_SCORING_CONFIG) {
  const thresholds = scoringConfig.ticketGateThresholds || {};
  const rows = BOATS.map((boat) => {
    const scenario = bestScenarioForHead(prediction, boat);
    const scenarioSupport = score01(scenario?.score, 0);
    const headScore = flowBoatScore(prediction, boat, "headScore", 0.35);
    const attackTriggerScore = flowBoatScore(prediction, boat, "scenarioTriggerScore", 0.35);
    const attackerScore = flowBoatScore(prediction, boat, "attackerScore", 0.35);
    const beneficiaryScore = flowBoatScore(prediction, boat, "beneficiaryScore", 0.35);
    const residualScore = flowBoatScore(prediction, boat, "residualScore", 0.35);
    const startReliability = score01(prediction.scoredBoats?.find((row) => row.boat === boat)?.professionalFactors?.startReliability?.avgSTScore, featureScore01(prediction, boat, "exST", 0.5));
    const motorRank = featureScore01(prediction, boat, "motorRank", featureScore01(prediction, boat, "motor2Rate", 0.5));
    const lapTurn = (featureScore01(prediction, boat, "lapTime", 0.5) + featureScore01(prediction, boat, "turnTime", 0.5)) / 2;
    const straight = featureScore01(prediction, boat, "straightTime", 0.5);
    const tendency = prediction.scoredBoats?.find((row) => row.boat === boat)?.playerTendency || {};
    const tendencySupported = ["ok", "small_sample"].includes(tendencySampleStatus(tendency));
    const supports = strongSupportList([
      { ok: headScore >= 0.58, reason: "headScore" },
      { ok: scenarioSupport >= 0.56, reason: "scenario" },
      { ok: Math.max(attackTriggerScore, attackerScore, beneficiaryScore) >= 0.58, reason: "attack/benefit" },
      { ok: motorRank >= 0.62, reason: "motor" },
      { ok: startReliability >= 0.62, reason: "start" },
      { ok: lapTurn >= 0.62 || straight >= 0.66, reason: "exhibition balance" },
      { ok: tendencySupported, reason: "tendency sample" }
    ]);
    let validationScore = clamp(
      headScore * 0.26 +
      scenarioSupport * 0.22 +
      Math.max(attackerScore, beneficiaryScore) * 0.16 +
      motorRank * 0.12 +
      startReliability * 0.1 +
      lapTurn * 0.1 +
      straight * 0.04
    );
    const rejectReasons = [];
    let status = "upset";
    if (supports.length >= 3 && validationScore >= 0.56) status = "main";
    if (supports.length < 2 || validationScore < 0.44) status = "partner_only";
    if (boat >= 5) {
      const outsideScenario = Math.max(scenarioScore01(prediction, "outer_follow_5", 0), scenarioScore01(prediction, "outer_follow_6", 0));
      const insideCollapse = score01(prediction.raceFlowScenario?.decisionResidualScores?.insideCollapseScore, 0.45);
      const live = liveFeatureSupport(prediction, null, boat, 0.5);
      const outsideMin = finiteNumber(thresholds.outsideHeadMinSupport, 0.66);
      if (outsideScenario < 0.58 || insideCollapse < 0.58 || live < outsideMin) {
        status = "partner_only";
        rejectReasons.push("outside head requires strong outside scenario, inside collapse, and live support");
      }
    }
    if (boat === 3 && attackTriggerScore >= 0.62 && residualScore < 0.48 && featureScore01(prediction, 3, "turnTime", 0.5) < 0.56) {
      status = status === "main" ? "upset" : status;
      rejectReasons.push("3 is attack trigger, but residual/turn support is weak for head");
    }
    if (dataQuality.overall === "low" && status === "main" && validationScore < 0.66) {
      status = "upset";
      rejectReasons.push("data quality is low, so head confidence is capped");
    }
    return {
      boat,
      status,
      validatedAsHead: status === "main" || status === "upset",
      validationScore: roundNumber(validationScore * 100, 1),
      supports,
      rejectReasons,
      scenarioId: scenario?.id || null,
      scenarioSupport: roundNumber(scenarioSupport * 100, 1),
      headScore: roundNumber(headScore * 100, 1),
      attackTriggerScore: roundNumber(attackTriggerScore * 100, 1),
      beneficiaryScore: roundNumber(beneficiaryScore * 100, 1),
      residualScore: roundNumber(residualScore * 100, 1),
      startReliability: roundNumber(startReliability * 100, 1),
      motorRank: roundNumber(motorRank * 100, 1),
      lapTurn: roundNumber(lapTurn * 100, 1)
    };
  }).sort((a, b) => b.validationScore - a.validationScore);
  return {
    rows,
    byBoat: Object.fromEntries(rows.map((row) => [String(row.boat), row])),
    main: rows.filter((row) => row.status === "main"),
    upset: rows.filter((row) => row.status === "upset"),
    partnerOnly: rows.filter((row) => row.status === "partner_only")
  };
}

function buildPartnerValidation(prediction = {}) {
  const rows = [];
  for (const head of BOATS) {
    for (const partner of BOATS) {
      if (partner === head) continue;
      const venue = partnerVenueSupport(prediction, head, partner, bestScenarioForHead(prediction, head)?.id || null);
      const secondScore = flowBoatScore(prediction, partner, "secondScore", 0.42);
      const residualScore = flowBoatScore(prediction, partner, "residualScore", 0.42);
      const live = liveFeatureSupport(prediction, null, partner, 0.5);
      let score = clamp(secondScore * 0.42 + residualScore * 0.26 + live * 0.2 + venue.score * 0.12);
      const reasons = [];
      const rejectReasons = [];
      if (secondScore >= 0.56) reasons.push(`${partner} secondScore`);
      if (residualScore >= 0.56) reasons.push(`${partner} residual`);
      if (live >= 0.62) reasons.push(`${partner} live support`);
      if (venue.known && venue.score >= 0.56) reasons.push(`venue ${head}-${partner}`);
      if (head === 4 && partner === 3 && residualScore < 0.58) {
        score = Math.min(score, 0.44);
        rejectReasons.push("4-3 requires boat3 residual after triggering attack");
      }
      if (head === 3 && partner === 1 && score01(prediction.raceFlowScenario?.decisionResidualScores?.boat1ResidualAfterAttackScore, 0.5) < 0.5) {
        score = Math.min(score, 0.5);
        rejectReasons.push("3-1 requires boat1 residual after attack");
      }
      rows.push({
        head,
        partner,
        score: roundNumber(score * 100, 1),
        grade: score >= 0.62 ? "A" : score >= 0.52 ? "B" : score >= 0.42 ? "C" : "reject",
        reasons,
        rejectReasons
      });
    }
  }
  const byHead = Object.fromEntries(BOATS.map((head) => [
    String(head),
    rows.filter((row) => row.head === head).sort((a, b) => b.score - a.score)
  ]));
  return { rows, byHead };
}

function detectCommonWrongCaseWarnings(prediction = {}, headValidation = {}) {
  const residual = prediction.raceFlowScenario?.decisionResidualScores || {};
  const boat1Residual = score01(residual.boat1ResidualAfterAttackScore, flowBoatScore(prediction, 1, "residualScore", 0.5));
  const boat2Wall = scoreByBoatFromRows(prediction.wallScorePreview || [], 2, "wallScore", 0.5);
  const boat3Trigger = flowBoatScore(prediction, 3, "scenarioTriggerScore", 0.5);
  const boat3Residual = flowBoatScore(prediction, 3, "residualScore", 0.5);
  const boat4Beneficiary = flowBoatScore(prediction, 4, "beneficiaryScore", 0.5);
  const notes = [];
  if (boat1Residual < 0.52 && boat2Wall < 0.48 && boat3Trigger >= 0.58 && boat4Beneficiary >= 0.58) {
    notes.push("1号艇が強く見えても、2の壁が弱く3が攻め、4が展開を拾う形に注意。");
  }
  if (boat1Residual >= 0.64 && featureScore01(prediction, 1, "motorRank", featureScore01(prediction, 1, "motor2Rate", 0.5)) >= 0.62) {
    notes.push("外の気配が良くても、1号艇の残り足とモーターが強く内残りを評価。");
  }
  if (boat3Trigger >= 0.62 && boat3Residual < 0.5 && boat4Beneficiary >= 0.62) {
    notes.push("3号艇は攻めの起点評価。3頭より4号艇の展開拾いを上に見ます。");
  }
  const outsideHead = safeArray(headValidation.rows).filter((row) => row.boat >= 5 && row.status !== "partner_only");
  if (outsideHead.length > 0) {
    notes.push("5・6頭は内崩れと外追走が揃う場合だけ。通常は2・3着候補中心です。");
  }
  return notes;
}

function buildFinalExplanation(finalView = {}) {
  const buyLabel = finalView.buyDecision === "buy" ? "買い" : finalView.buyDecision === "light" ? "軽め" : "見送り";
  const main = finalView.mainScenario?.label || finalView.mainScenario?.id || "-";
  const ticketText = safeArray(finalView.mainTickets).length > 0
    ? safeArray(finalView.mainTickets).map((ticket) => ticket.combo).join(" / ")
    : safeArray(finalView.referenceTickets).map((ticket) => ticket.combo).join(" / ") || "-";
  const warnings = safeArray(finalView.warnings).slice(0, 3).join(" / ");
  return {
    summary: `${buyLabel}判定。中心シナリオは ${main}、買い目は ${ticketText}。`,
    raceFlow: [
      finalView.mainScenario?.reasons?.[0] || null,
      finalView.secondaryScenario?.reasons?.[0] || null,
      ...safeArray(finalView.commonCaseWarnings)
    ].filter(Boolean).join(" "),
    ticket: safeArray(finalView.ticketReasoning).slice(0, 4).map((row) => `${row.ticket}: ${row.reason}`).join(" / "),
    warnings
  };
}

function buildFinalPredictionView(prediction = {}, confidence = null, scoringConfig = DEFAULT_SCORING_CONFIG) {
  const dataQuality = prediction.dataQuality || buildDataQualityReport(prediction);
  const headValidation = prediction.headValidation || buildHeadValidation(prediction, dataQuality, scoringConfig);
  const partnerValidation = prediction.partnerValidation || buildPartnerValidation(prediction);
  const mainScenario = prediction.mainScenarioGroup || prediction.raceFlowScenario?.mainScenarioGroup || prediction.raceFlowScenario?.mainScenario || null;
  const secondaryScenario = prediction.derivedScenarioGroup || prediction.raceFlowScenario?.derivedScenarioGroup || prediction.raceFlowScenario?.secondaryScenario || null;
  const upsetScenario = prediction.raceFlowScenario?.upsetScenario || null;
  const mainTickets = safeArray(prediction.ticketGroups?.mainTickets);
  const secondaryTickets = safeArray(prediction.ticketGroups?.secondaryTickets);
  const upsetTickets = safeArray(prediction.ticketGroups?.upsetTickets);
  const referenceTickets = safeArray(prediction.ticketGroups?.referenceTickets);
  const rejectedTickets = safeArray(prediction.ticketGroups?.rejectedTickets);
  const scenarioClose = score01(mainScenario?.score, null) !== null &&
    score01(secondaryScenario?.score, null) !== null &&
    Math.abs(score01(mainScenario?.score, 0) - score01(secondaryScenario?.score, 0)) < (scoringConfig.buyDecisionThresholds?.scenarioCloseGap ?? 0.035);
  const consistency = checkPredictionConsistency(prediction);
  const confidenceScore = finiteNumber(confidence?.score, null);
  const noA = mainTickets.length === 0;
  const passReasons = [
    noA ? "A評価の本線券なし" : null,
    confidenceScore !== null && confidenceScore < (scoringConfig.buyDecisionThresholds?.passConfidence ?? 40) ? "信頼度が低い" : null,
    scenarioClose && confidenceScore !== null && confidenceScore < 58 ? "シナリオが割れている" : null,
    dataQuality.overall === "low" && noA ? "データ品質が低い" : null,
    consistency.ok === false ? "説明と買い目の整合性に注意" : null
  ].filter(Boolean);
  const lightReasons = [
    noA && (secondaryTickets.length > 0 || upsetTickets.length > 0 || referenceTickets.length > 0) ? "強いA券はないが参考候補あり" : null,
    confidenceScore !== null && confidenceScore < (scoringConfig.buyDecisionThresholds?.buyConfidence ?? 58) ? "信頼度は中位" : null,
    dataQuality.overall === "medium" ? "データ品質は中位" : null
  ].filter(Boolean);
  const buyDecision = passReasons.length > 0
    ? "pass"
    : lightReasons.length > 0
      ? "light"
      : "buy";
  const headCandidates = headValidation.rows.filter((row) => row.status === "main" || row.status === "upset").slice(0, 4);
  const partnerCandidates = safeArray(partnerValidation.byHead?.[String(headCandidates[0]?.boat || expectedHeadFromScenario(mainScenario) || 1)]).slice(0, 5);
  const thirdCandidates = safeArray(prediction.raceFlowScenario?.partnerCandidates || prediction.headPartnerSplitPreview)
    .slice()
    .sort((a, b) => score01(b?.thirdScore, 0) - score01(a?.thirdScore, 0))
    .slice(0, 5);
  const commonCaseWarnings = detectCommonWrongCaseWarnings(prediction, headValidation);
  const warnings = [
    ...safeArray(dataQuality.warnings),
    ...safeArray(confidence?.warnings),
    ...safeArray(prediction.ticketGroups?.noBuyReasons),
    ...commonCaseWarnings,
    ...safeArray(consistency.warnings)
  ].filter(Boolean).filter((warning, index, arr) => arr.indexOf(warning) === index);
  const ticketReasoning = [
    ...mainTickets,
    ...secondaryTickets,
    ...upsetTickets,
    ...referenceTickets
  ].map((ticket) => ({
    ticket: ticket.combo,
    grade: ticket.grade || (ticket.referenceOnly ? "reference" : "-"),
    scenario: ticket.scenarioId || ticket.scenarioName || "-",
    reason: ticket.displayReason || safeArray(ticket.reasons).join(" / ") || "-"
  }));
  const finalView = {
    dataQuality,
    mainScenario,
    secondaryScenario,
    upsetScenario,
    headCandidates,
    partnerCandidates,
    thirdCandidates,
    mainTickets,
    secondaryTickets,
    upsetTickets,
    referenceTickets,
    rejectedTickets,
    buyDecision,
    confidence,
    warnings,
    explanation: null,
    ticketReasoning,
    commonCaseWarnings,
    debug: {
      dataQualityReport: dataQuality,
      scenarioScores: prediction.raceFlowScenario?.scenarioFamilies || prediction.raceFlowScenario?.scenarios || [],
      headValidation,
      partnerValidation,
      ticketPlausibility: prediction.ticketGroups || null,
      factorContributionByBoat: prediction.coefficientContributionByBoat || [],
      consistencyCheck: consistency
    }
  };
  finalView.explanation = buildFinalExplanation(finalView);
  return finalView;
}

export function buildRacePrediction(program = {}, preview = null, config = DEFAULT_SCORING_CONFIG) {
  const stadiumNumber = finiteNumber(program.race_stadium_number, null);
  const scoringConfig = getVenueScoringConfig(mergeScoringConfig({
    ...config,
    stadiumNumber
  }), stadiumNumber);
  const boats = enrichMotorRanking(normalizeProgramBoats(program));
  const raceConditions = normalizeRaceConditionsForPrediction(program, preview || {});
  const previewWithConditions = mergeRaceConditionsIntoPreview(preview || {}, raceConditions);
  const exhibition = enrichExhibitionFeaturesFromBoats(buildExhibitionFeatures(previewWithConditions), boats);
  exhibition.conditions = raceConditions;
  const featureScores = buildOriginalExhibitionFeatureScores(boats, exhibition, { venueId: scoringConfig.stadiumNumber });
  const baseScoredBoats = buildScores(boats, exhibition, scoringConfig, featureScores).sort((a, b) => a.boat - b.boat);
  const venueBiasSource =
    scoringConfig.venueLaneBias ||
    scoringConfig.venueBias ||
    program.venue_scenario_bias ||
    program.venueBias ||
    program.venueBiasProfile ||
    previewWithConditions.venue_scenario_bias ||
    previewWithConditions.venueBias ||
    null;
  const raceFlowScenario = buildRaceFlowScenarioModel({
    entries: baseScoredBoats,
    featureScores,
    venueBias: venueBiasSource,
    stadiumNumber: scoringConfig.stadiumNumber,
    raceConditions,
    scoringConfig
  });
  const scoredBoats = applyRaceFlowScenarioAdjustments(baseScoredBoats, raceFlowScenario).sort((a, b) => a.boat - b.boat);
  const tendencySummary = buildTendencySummary(scoredBoats);
  const firstPlaceProbabilities = softmax(scoredBoats)
    .map((row) => ({ boat: row.boat, course: row.course, probability: row.probability }))
    .sort((a, b) => b.probability - a.probability);
  const trifecta = plackettLuceTrifecta(scoredBoats);
  const decisionCompatibleTickets = applyRaceFlowTicketDecisionCompatibility(trifecta, raceFlowScenario);
  const tickets = marginalizeTickets(decisionCompatibleTickets.tickets);
  const prediction = {
    race: {
      date: program.race_date ?? null,
      stadiumNumber: finiteNumber(program.race_stadium_number, null),
      raceNumber: finiteNumber(program.race_number, null),
      closedAt: program.race_closed_at ?? null,
      conditions: raceConditions
    },
    exhibition,
    featureScores,
    featureScorePreview: featureScores.preview,
    venueNormalizedExhibitionMetrics: featureScores.venueNormalizedMetrics,
    currentScoringWeights: scoringConfig.scoringCoefficients,
    currentScoringConfig: scoringConfig,
    venueOverrideApplied: scoringConfig.venueOverrideApplied || null,
    coefficientContributionByBoat: buildCoefficientContributionByBoat(scoredBoats, featureScores, scoringConfig),
    motorRankContribution: scoredBoats.map((boat) => ({
      boat: boat.boat,
      motorNo: boat.motorNo ?? null,
      motorRankAtVenue: boat.motorRankAtVenue ?? null,
      motorPercentileAtVenue: boat.motorPercentileAtVenue ?? null,
      motorStrengthLabel: boat.motorStrengthLabel ?? null,
      contribution: boat.scoreParts?.motorRankBoost ?? 0
    })),
    startReliabilityContribution: scoredBoats.map((boat) => ({
      boat: boat.boat,
      contribution: boat.scoreParts?.startReliabilityBoost ?? 0,
      detail: boat.professionalFactors?.startReliability || null
    })),
    raceFlowScenario,
    raceFlowScenarioPreview: raceFlowScenario.scenarioFamilies || raceFlowScenario.scenarios,
    scenarioFamilyPreview: raceFlowScenario.scenarioFamilies || [],
    mainScenarioGroup: raceFlowScenario.mainScenarioGroup || null,
    derivedScenarioGroup: raceFlowScenario.derivedScenarioGroup || null,
    wallScorePreview: raceFlowScenario.wallScores,
    headPartnerSplitPreview: raceFlowScenario.headPartnerSplit,
    fourHeadPartnerDecision: raceFlowScenario.fourHeadPartnerDecision || null,
    venueBiasTable: raceFlowScenario.venueBiasTable || null,
    ticketAdjustmentLog: raceFlowScenario.ticketAdjustmentLog,
    ticketDecisionCompatibilityPreview: decisionCompatibleTickets.preview,
    conditionAdjustmentLog: raceFlowScenario.conditionAdjustmentLog,
    venueBiasContribution: scoredBoats.map((boat) => ({ boat: boat.boat, contribution: boat.scoreParts?.venueBiasBoost ?? 0 })),
    conditionContribution: raceFlowScenario.conditionAdjustmentLog,
    tendencySummary,
    tendencyScorePreview: tendencySummary.preview,
    scoredBoats,
    firstPlaceProbabilities,
    tickets,
    freshness: {
      fetchedAt: new Date().toISOString(),
      apiUpdateNote: "Open API is updated roughly every 30 minutes.",
      statsBuiltAt: null,
      statsPeriod: null
    }
  };
  const scenario = buildTurnScenario(prediction);
  const development = buildDevelopmentScenarios({ ...prediction, scenario });
  const scenarioScorePreview = development.scenarios.map((row) => ({
    scenarioName: row.scenarioName,
    attacker: row.attacker,
    probabilityScore: row.probabilityScore,
    upsetScore: row.upsetScore,
    reasons: row.reasons
  }));
  const raceFlowExtraTickets = buildRaceFlowScenarioTickets(raceFlowScenario, tickets.trifecta.slice(0, 12), 6);
  const extraTickets = [];
  const seenExtraTickets = new Set();
  for (const ticket of [...raceFlowExtraTickets, ...development.extraTickets]) {
    if (!ticket?.combo || seenExtraTickets.has(ticket.combo)) continue;
    seenExtraTickets.add(ticket.combo);
    extraTickets.push(ticket);
    if (extraTickets.length >= 6) break;
  }
  const finalPrediction = {
    ...prediction,
    scenario,
    developmentScenarios: development.scenarios,
    scenarioScorePreview,
    raceFlowExtraTickets,
    upsetScenarios: development.upsetScenarios,
    upsetAlert: development.upsetAlert,
    upsetReasons: development.upsetReasons,
    extraTickets
  };
  finalPrediction.dataQuality = buildDataQualityReport(finalPrediction);
  finalPrediction.headValidation = buildHeadValidation(finalPrediction, finalPrediction.dataQuality, scoringConfig);
  finalPrediction.partnerValidation = buildPartnerValidation(finalPrediction);
  finalPrediction.finalScenarioConsistencyCheck = checkPredictionConsistency(finalPrediction);
  const ticketGroups = buildTicketPlausibilityGroups(finalPrediction, program);
  finalPrediction.ticketGroups = ticketGroups;
  finalPrediction.ticketPlausibilityPreview = ticketGroups.evaluatedTickets;
  finalPrediction.ticketPlausibilitySummary = ticketGroups.summary;
  finalPrediction.rejectedTickets = ticketGroups.rejectedTickets;
  finalPrediction.tickets = {
    ...finalPrediction.tickets,
    plausibleTrifecta: ticketGroups.mainTickets,
    secondaryTrifecta: ticketGroups.secondaryTickets,
    upsetTrifecta: ticketGroups.upsetTickets,
    referenceTrifecta: ticketGroups.referenceTickets
  };
  const legacyExtraCandidates = ticketGroups.noBuyRecommended
    ? [
      ...ticketGroups.referenceTickets,
      ...ticketGroups.evaluatedTickets.filter((ticket) => ticket.plausible && Number(ticket?.boats?.[0]) !== 1)
    ]
    : [
      ...ticketGroups.mainTickets.filter((ticket) => Number(ticket?.boats?.[0]) !== 1),
      ...ticketGroups.secondaryTickets.filter((ticket) => Number(ticket?.boats?.[0]) !== 1),
      ...ticketGroups.upsetTickets
    ];
  const legacyExtraSeen = new Set();
  finalPrediction.extraTickets = legacyExtraCandidates
    .filter((ticket) => {
      if (Number(ticket?.boats?.[0]) === 1 || !ticket?.combo || legacyExtraSeen.has(ticket.combo)) return false;
      legacyExtraSeen.add(ticket.combo);
      return true;
    })
    .slice(0, 6);
  finalPrediction.finalScenarioConsistencyCheck = checkPredictionConsistency(finalPrediction);
  const confidence = buildConfidenceScore(finalPrediction);
  finalPrediction.confidence = confidence;
  finalPrediction.confidenceScore = confidence.score;
  finalPrediction.finalPrediction = buildFinalPredictionView(finalPrediction, confidence, scoringConfig);
  finalPrediction.buyDecision = finalPrediction.finalPrediction.buyDecision;
  finalPrediction.warnings = finalPrediction.finalPrediction.warnings;
  finalPrediction.mainTickets = finalPrediction.finalPrediction.mainTickets;
  finalPrediction.secondaryTickets = finalPrediction.finalPrediction.secondaryTickets;
  finalPrediction.upsetTickets = finalPrediction.finalPrediction.upsetTickets;
  finalPrediction.referenceTickets = finalPrediction.finalPrediction.referenceTickets;
  finalPrediction.headCandidates = finalPrediction.finalPrediction.headCandidates;
  finalPrediction.partnerCandidates = finalPrediction.finalPrediction.partnerCandidates;
  finalPrediction.thirdCandidates = finalPrediction.finalPrediction.thirdCandidates;
  finalPrediction.explanation = finalPrediction.finalPrediction.explanation;
  finalPrediction.ticketReasoning = finalPrediction.finalPrediction.ticketReasoning;
  finalPrediction.coefficientWarning = finalPrediction.finalScenarioConsistencyCheck.warnings;
  return finalPrediction;
}

export function buildConfidenceScore(prediction = {}) {
  const firstRows = Array.isArray(prediction.firstPlaceProbabilities) ? prediction.firstPlaceProbabilities : [];
  const scoredBoats = Array.isArray(prediction.scoredBoats) ? prediction.scoredBoats : [];
  const boat1 = scoredBoats.find((boat) => boat.boat === 1) || {};
  const boat2 = scoredBoats.find((boat) => boat.boat === 2) || {};
  const boat3 = scoredBoats.find((boat) => boat.boat === 3) || {};
  const boat4 = scoredBoats.find((boat) => boat.boat === 4) || {};
  const boat1First = firstRows.find((row) => row.boat === 1)?.probability ?? 0;
  const counterFirst = firstRows.find((row) => row.boat !== 1)?.probability ?? 0;
  const outsideHead = firstRows
    .filter((row) => row.boat >= 5)
    .reduce((sum, row) => sum + Number(row.probability || 0), 0);
  const topScores = [...scoredBoats].sort((a, b) => b.score - a.score);
  const topScoreGap = topScores.length >= 3 ? (topScores[0].score - topScores[2].score) : 0;
  const topSplitPenalty = Math.max(0, counterFirst - boat1First + 0.04);
  const opponentsStable = firstRows
    .filter((row) => row.boat >= 2 && row.boat <= 4)
    .reduce((sum, row) => sum + Number(row.probability || 0), 0);
  const fHolderPenalty = scoredBoats
    .filter((boat) => boat.boat >= 2)
    .reduce((sum, boat) => sum + Math.max(0, Number(boat.flyingCount || 0)) * (boat.boat >= 5 ? 3 : 1.5), 0);
  const groupedExtraTicketCount = prediction.ticketGroups
    ? safeArray(prediction.ticketGroups.secondaryTickets).length + safeArray(prediction.ticketGroups.upsetTickets).length
    : null;
  const developmentPenalty = groupedExtraTicketCount !== null
    ? Math.min(8, groupedExtraTicketCount * 1.2)
    : Array.isArray(prediction.extraTickets)
      ? Math.min(8, prediction.extraTickets.length * 1.2)
      : 0;
  const extraTicketCount = groupedExtraTicketCount !== null
    ? groupedExtraTicketCount
    : Array.isArray(prediction.extraTickets) ? prediction.extraTickets.length : 0;
  const boat1EscapeSupport =
    weightedPositiveRateLift(boat1.playerTendency, "escapeRate", 0.55) * 16 +
    weightedPositiveRateLift(boat2.playerTendency, "nigashiRate", 0.55) * 12;
  const boat1VulnerabilityPenalty =
    weightedPositiveRateLift(boat1.playerTendency, "beatenBySashiRate", 0.15) * 24 +
    weightedPositiveRateLift(boat1.playerTendency, "beatenByMakuriRate", 0.1) * 28 +
    weightedPositiveRateLift(boat1.playerTendency, "beatenByMakuriSashiRate", 0.08) * 24;
  const boat1LateRatePenalty = weightedPositiveRateLift(boat1.playerTendency, "lateStartRate", 0.12) * 28;
  const boat2WallLatePenalty = weightedPositiveRateLift(boat2.playerTendency, "lateStartRate", 0.12) * 16;
  const clearAttackBoat =
    tendencyCanDriveUpset(boat3.playerTendency) && (
      positiveRateLift(boat3.playerTendency?.makuriRate, 0.56) > 0 ||
      positiveRateLift(boat3.playerTendency?.makuriSashiRate, 0.56) > 0
    ) ||
    tendencyCanDriveUpset(boat4.playerTendency) && (
      positiveRateLift(boat4.playerTendency?.makuriRate, 0.56) > 0 ||
      positiveRateLift(boat4.playerTendency?.makuriSashiRate, 0.56) > 0
    );
  const tooManyUpsetsPenalty = Math.max(0, extraTicketCount - 3) * 1.4;
  const entryUnconfirmed = prediction.exhibition?.entryCourseByBoat ? 0 : 1;
  const boat1ExhibitionStScore = prediction.exhibition?.exhibitionStartByBoat
    ? startTimingScore(prediction.exhibition.exhibitionStartByBoat[1], 0.45)
    : 0.5;
  const timeZ = prediction.exhibition?.exhibitionTimeByBoat ? zScores(prediction.exhibition.exhibitionTimeByBoat) : {};
  const boat1ExhibitionTimeScore = prediction.exhibition?.exhibitionTimeByBoat
    ? clamp(0.5 + (-Number(timeZ["1"] || 0) * 0.18), 0, 1)
    : 0.5;
  const originalFeatureComplete = prediction.featureScores?.allOriginalExhibitionTimesComplete === true;
  const originalFeatureQualityAdjustment = originalFeatureComplete ? 6 : -3;
  const tendencyAvailable = prediction.tendencySummary?.available === true;
  const tendencyComplete = prediction.tendencySummary?.complete === true;
  const tendencySparse = prediction.tendencySummary?.sparse === true;
  const tendencyQualityAdjustment = tendencyComplete ? 4 : tendencyAvailable && !tendencySparse ? 1 : tendencyAvailable ? 0 : -2;
  const raceFlowDataWarnings = Array.isArray(prediction.raceFlowScenario?.dataWarnings)
    ? prediction.raceFlowScenario.dataWarnings
    : [];
  const raceFlowQualityAdjustment = finiteNumber(prediction.raceFlowScenario?.quality?.confidenceAdjustment, 0);
  const dataQualityOverall = prediction.dataQuality?.overall || prediction.finalPrediction?.dataQuality?.overall || "medium";
  const dataQualityAdjustment = dataQualityOverall === "high" ? 3 : dataQualityOverall === "medium" ? 0 : -7;
  const motorScore = percent01(boat1.motor2Rate, 0.45);
  const score =
    (boat1First * 38) +
    (boat1ExhibitionStScore * 8) +
    (boat1ExhibitionTimeScore * 8) +
    (motorScore * 10) +
    (opponentsStable * 10) +
    ((1 - outsideHead) * 8) +
    (clamp(topScoreGap / 0.35, 0, 1) * 12) -
    (topSplitPenalty * 18) -
    (entryUnconfirmed * 4) -
    fHolderPenalty -
    developmentPenalty +
    boat1EscapeSupport -
    boat1VulnerabilityPenalty -
    boat1LateRatePenalty -
    boat2WallLatePenalty -
    tooManyUpsetsPenalty +
    originalFeatureQualityAdjustment +
    tendencyQualityAdjustment +
    raceFlowQualityAdjustment +
    dataQualityAdjustment;
  const warnings = [];
  if (entryUnconfirmed) warnings.push("進入未確定のため直前展示で再確認");
  if (outsideHead >= 0.18) warnings.push("5・6号艇の頭浮上余地あり");
  if (topSplitPenalty > 0.08) warnings.push("1着候補が割れている");
  if (fHolderPenalty > 0) warnings.push("F持ちの影響を評価に反映");
  if (boat1EscapeSupport > 0) warnings.push("1号艇逃げ率と2号艇逃がし率を信頼度に反映");
  if (boat1VulnerabilityPenalty > 0) warnings.push("1号艇の差され・まくられ傾向を信頼度に反映");
  if (boat1LateRatePenalty > 0) warnings.push("1号艇の出遅れ率が高い");
  if (boat2WallLatePenalty > 0) warnings.push("2号艇の出遅れ率が高く、1号艇の壁信頼度を抑制");
  if (clearAttackBoat) warnings.push("攻め艇が明確で穴候補あり");
  if (tooManyUpsetsPenalty > 0) warnings.push("穴候補が多く本線信頼度を抑制");
  if (originalFeatureComplete) {
    warnings.push("周回・直線・まわり足データを6艇分反映");
  } else {
    warnings.push("周回・直線・まわり足データ未取得のため、展示ST・展示タイム・モーター中心で予想");
  }
  if (tendencySparse) {
    warnings.push("直近6か月のコース別戦法データはサンプル不足のため、展示ST・展示タイム・周回・直線・まわり足を中心に評価しています。");
  } else if (tendencyAvailable) {
    warnings.push("直近6か月のコース別戦法データを予想に反映");
  } else {
    warnings.push("直近6か月の戦法データ未取得のため、展示・モーター中心で予想");
  }
  for (const warning of raceFlowDataWarnings) {
    if (warning && !warnings.includes(warning)) warnings.push(warning);
  }
  for (const warning of safeArray(prediction.dataQuality?.warnings)) {
    if (warning && !warnings.includes(warning)) warnings.push(warning);
  }
  const consistencyWarnings = Array.isArray(prediction.finalScenarioConsistencyCheck?.warnings)
    ? prediction.finalScenarioConsistencyCheck.warnings
    : [];
  for (const warning of consistencyWarnings) {
    if (warning && !warnings.includes(warning)) warnings.push(warning);
  }
  if (prediction.ticketGroups?.noBuyRecommended) {
    const warning = prediction.ticketGroups.noBuyReason || "見送り推奨: 買い目Aがなく参考扱い";
    if (!warnings.includes(warning)) warnings.push(warning);
  }
  if (prediction.exhibition?.status !== "exhibition_reflected") warnings.push("展示前の出走表ベース予想");
  const roundedScore = Math.round(clamp(score, 0, 100));
  const waterUnstable = prediction.conditionAdjustmentLog?.some((row) => row?.type === "wind" && row?.level === "strong" || row?.type === "wave" && row?.level === "strong");
  if (
    roundedScore < 45 ||
    topScoreGap < 0.06 ||
    waterUnstable && roundedScore < 58 ||
    prediction.finalScenarioConsistencyCheck?.ok === false
  ) {
    const warning = "見送り推奨: 係数診断上、強い本線推薦ではなく参考買い目扱い";
    if (!warnings.includes(warning)) warnings.push(warning);
  }
  return {
    score: roundedScore,
    warnings,
    factors: {
      boat1First,
      counterFirst,
      boat1ExhibitionStScore,
      boat1ExhibitionTimeScore,
      motorScore,
      opponentsStable,
      outsideHead,
      topScoreGap,
      entryUnconfirmed,
      fHolderPenalty,
      developmentPenalty,
      boat1EscapeSupport,
      boat1VulnerabilityPenalty,
      boat1LateRatePenalty,
      boat2WallLatePenalty,
      clearAttackBoat,
      extraTicketCount,
      tooManyUpsetsPenalty,
      originalFeatureComplete,
      originalFeatureQualityAdjustment,
      tendencyAvailable,
      tendencyComplete,
      tendencyQualityAdjustment,
      dataQualityOverall,
      dataQualityAdjustment,
      raceFlowQualityAdjustment,
      waterUnstable,
      finalScenarioConsistencyCheck: prediction.finalScenarioConsistencyCheck || null
    }
  };
}

function roundNumber(value, digits = 1) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  const scale = 10 ** digits;
  return Math.round(n * scale) / scale;
}

function score01(value, fallback = 0.5) {
  const n = finiteNumber(value, null);
  if (n === null) return fallback;
  return clamp(Math.abs(n) > 1 ? n / 100 : n, 0, 1);
}

function scoreByBoatFromRows(rows = [], boat, field, fallback = 0.5) {
  const row = (Array.isArray(rows) ? rows : []).find((item) => Number(item?.boat) === Number(boat));
  return score01(row?.[field], fallback);
}

function scenarioScore01(prediction = {}, scenarioId, fallback = 0.5) {
  const scenario = [
    ...(Array.isArray(prediction.raceFlowScenario?.scenarioFamilies) ? prediction.raceFlowScenario.scenarioFamilies : []),
    ...(Array.isArray(prediction.raceFlowScenario?.scenarios) ? prediction.raceFlowScenario.scenarios : [])
  ]
    .find((row) => row?.id === scenarioId);
  return score01(scenario?.score, fallback);
}

function featureScore01(prediction = {}, boat, field, fallback = 0.5) {
  const row = prediction.featureScores?.byBoat?.[String(boat)] || {};
  return score01(row?.scores?.[field], fallback);
}

function firstPlaceProbability01(prediction = {}, boat, fallback = 0.04) {
  const row = (Array.isArray(prediction.firstPlaceProbabilities) ? prediction.firstPlaceProbabilities : [])
    .find((item) => Number(item?.boat) === Number(boat));
  return score01(row?.probability, fallback);
}

function ticketPartnerScore01(prediction = {}, head, partner) {
  const rows = Array.isArray(prediction.tickets?.trifecta) ? prediction.tickets.trifecta : [];
  const total = rows
    .filter((ticket) => Array.isArray(ticket?.boats) && Number(ticket.boats[0]) === Number(head) && Number(ticket.boats[1]) === Number(partner))
    .reduce((sum, ticket) => sum + Number(ticket.probability || 0), 0);
  return clamp(total * 12, 0, 1);
}

function buildBoat4PartnerRanking(prediction = {}) {
  const splitRows = Array.isArray(prediction.headPartnerSplitPreview) ? prediction.headPartnerSplitPreview : [];
  return [1, 2, 3, 5, 6]
    .map((partner) => {
      const residual =
        (scoreByBoatFromRows(splitRows, partner, "secondScore", 0.45) * 0.6) +
        (scoreByBoatFromRows(splitRows, partner, "thirdScore", 0.45) * 0.25) +
        (ticketPartnerScore01(prediction, 4, partner) * 0.15);
      return {
        partner,
        score: roundNumber(clamp(residual) * 100, 1)
      };
    })
    .sort((a, b) => b.score - a.score);
}

function buildBoat4ConditionScore(prediction = {}, parts = {}) {
  const conditions = prediction.race?.conditions || {};
  const windSpeed = finiteNumber(conditions.windSpeed ?? conditions.wind_speed, null);
  const waveHeight = finiteNumber(conditions.waveHeight ?? conditions.wave_height, null);
  const windDirection = String(conditions.windDirection ?? conditions.wind_direction ?? "").toLowerCase();
  const tailwind = /tail|追|順/.test(windDirection);
  const headwind = /head|向|against/.test(windDirection);
  const crosswind = /cross|横/.test(windDirection);
  let score = 0.5;
  const notes = [];

  if (windSpeed !== null && windSpeed >= 7) {
    score -= 0.08;
    notes.push("風速7m以上で全体の再現性をやや抑制");
  } else if (windSpeed !== null && windSpeed >= 5) {
    score -= 0.04;
    notes.push("風速5m以上で展開の安定度を少し抑制");
  }
  if (waveHeight !== null && waveHeight >= 8) {
    score += (parts.boat4TurnTimeScore * 0.09) + (parts.boat4LapTimeScore * 0.07) - 0.04;
    notes.push("波高8cm以上でまわり足・周回の安定を重視");
  } else if (waveHeight !== null && waveHeight >= 5) {
    score += (parts.boat4TurnTimeScore * 0.06) + (parts.boat4LapTimeScore * 0.04);
    notes.push("波高5cm以上でターン安定を加点");
  }
  if (tailwind) {
    score += Math.max(0, parts.boat3TriggerScore - 0.5) * 0.12;
    notes.push("追い風気味でセンター攻めの起点を少し加点");
  }
  if (headwind) {
    score += ((parts.boat4TurnTimeScore + parts.boat4LapTimeScore) / 2 - 0.5) * 0.1;
    notes.push("向かい風気味で周回・まわり足を重視");
  }
  if (crosswind) {
    score -= 0.03;
    score += Math.max(0, parts.boat4TurnTimeScore - 0.55) * 0.08;
    notes.push("横風で再現性を抑え、ターン安定を評価");
  }
  return {
    score: clamp(score),
    notes
  };
}

function boat4MakuriSashiReferenceScore(boat4 = {}) {
  const tendency = boat4?.playerTendency || boat4?.racerCourseStats || {};
  if (!tendencyCanDriveUpset(tendency)) return 0.5;
  return optionalRate01(tendency.makuriSashiRate) ?? 0.5;
}

function buildBoat4RecommendedTickets({
  scoreLabel,
  components,
  partnerRanking,
  prediction
}) {
  const tickets = [];
  const reasons = [];
  const add = (combo, reason) => {
    if (tickets.includes(combo)) return;
    tickets.push(combo);
    reasons.push({ action: "promote", target: combo, reason });
  };
  const highOrMedium = scoreLabel === "高" || scoreLabel === "中";
  if (highOrMedium) {
    add("4-1-flow", "4頭時は1残りを最優先相手に評価");
    add("4-2-flow", "2差し切りより4の展開拾いを上に見る形");
  }
  const partner5 = partnerRanking.find((row) => row.partner === 5)?.score ?? 0;
  if (scoreLabel === "高" && partner5 >= 48) {
    add("4-5-flow", "5号艇の追走スコアが一定以上");
  }
  const partner3 = partnerRanking.find((row) => row.partner === 3)?.score ?? 0;
  const boat3TurnScore = featureScore01(prediction, 3, "turnTime", 0.5);
  const boat3LapScore = featureScore01(prediction, 3, "lapTime", 0.5);
  const allow43 =
    components.boat3ResidualScore >= 0.58 &&
    (boat3TurnScore >= 0.62 || boat3LapScore >= 0.62) &&
    partner3 >= 44;
  if (allow43) {
    add("4-3-flow", "3号艇の残り足と会場相手評価が残るため4-3も押さえ");
  } else if (highOrMedium) {
    reasons.push({
      action: "demote",
      target: "4-3-flow",
      reason: "3号艇は攻めの起点寄りで、4頭時は4-1 / 4-2を優先"
    });
  }
  return {
    tickets: tickets.slice(0, 4),
    reasons
  };
}

function buildBoat4OpportunityRow(program = {}, prediction = {}, confidence = buildConfidenceScore(prediction), config = {}) {
  const scoringConfig = mergeScoringConfig(config || {});
  const opportunityWeights = scoringConfig.scoringCoefficients?.fourHeadOpportunity || {};
  const boat4 = (Array.isArray(prediction.scoredBoats) ? prediction.scoredBoats : []).find((row) => row.boat === 4) || {};
  const wallRows = Array.isArray(prediction.wallScorePreview) ? prediction.wallScorePreview : [];
  const splitRows = Array.isArray(prediction.headPartnerSplitPreview) ? prediction.headPartnerSplitPreview : [];
  const boat4HeadPotentialScore = Math.max(
    scoreByBoatFromRows(splitRows, 4, "headScore", 0.42),
    firstPlaceProbability01(prediction, 4, 0.04) * 1.8
  );
  const boat3TriggerScore = Math.max(
    scenarioScore01(prediction, "makuri_3", 0.45),
    scenarioScore01(prediction, "makuri_sashi_3", 0.45),
    scoreByBoatFromRows(splitRows, 3, "scenarioTriggerScore", 0.45)
  );
  const boat3ResidualScore = (
    scoreByBoatFromRows(splitRows, 3, "secondScore", 0.45) +
    scoreByBoatFromRows(splitRows, 3, "thirdScore", 0.45)
  ) / 2;
  const boat4BeneficiaryScore = scoreByBoatFromRows(splitRows, 4, "beneficiaryScore", 0.5);
  const threeAttackFourBenefitScore = clamp(
    boat3TriggerScore * (
      boat4BeneficiaryScore * 0.68 +
      featureScore01(prediction, 4, "turnTime", 0.5) * 0.17 +
      featureScore01(prediction, 4, "straightTime", 0.5) * 0.15
    )
  );
  const sashi2Score = scenarioScore01(prediction, "sashi_2", 0.5);
  const twoSashiFailureScore = clamp(1 - sashi2Score);
  const boat2WallScore = scoreByBoatFromRows(wallRows, 2, "wallScore", 0.52);
  const boat2WallWeaknessScore = clamp(1 - boat2WallScore);
  const boat4TurnTimeScore = featureScore01(prediction, 4, "turnTime", 0.5);
  const boat4StraightTimeScore = featureScore01(prediction, 4, "straightTime", 0.5);
  const boat4LapTimeScore = featureScore01(prediction, 4, "lapTime", 0.5);
  const boat4MotorRankScore = featureScore01(prediction, 4, "motorRank", 0.5);
  const boat4Motor2RateScore = featureScore01(prediction, 4, "motor2Rate", 0.5);
  const boat4MakuriSashiRateScore = boat4MakuriSashiReferenceScore(boat4);
  const venue4HeadComboBiasScore =
    score01(config?.boat4HeadComboBias ?? config?.venue4HeadComboBias, null) ??
    (prediction.raceFlowScenario?.quality?.venueAvailable ? 0.54 : 0.5);
  const condition = buildBoat4ConditionScore(prediction, {
    boat3TriggerScore,
    boat4TurnTimeScore,
    boat4LapTimeScore
  });
  const coefficientScore = weightedAverageFromWeights({
    boat3AttackTrigger: boat3TriggerScore,
    boat2SashiFailure: twoSashiFailureScore,
    boat1CollapseRisk: clamp(1 - scoreByBoatFromRows(splitRows, 1, "residualScore", 0.5)),
    boat4TurnTime: boat4TurnTimeScore,
    boat4StraightTime: boat4StraightTimeScore,
    boat4MotorRank: boat4MotorRankScore,
    boat4MakuriSashiTendency: boat4MakuriSashiRateScore,
    venue4HeadBias: venue4HeadComboBiasScore,
    conditionAdjustment: condition.score
  }, opportunityWeights, 0.5);
  const rawScore = clamp(
    coefficientScore.score * 0.74 +
    boat4HeadPotentialScore * 0.1 +
    threeAttackFourBenefitScore * 0.1 +
    boat4BeneficiaryScore * 0.06
  );
  const boat4HeadOpportunityScore = roundNumber(clamp(rawScore) * 100, 1);
  const strengthLabel =
    boat4HeadOpportunityScore >= 60 ? "高" :
      boat4HeadOpportunityScore >= 48 ? "中" :
        "低";
  const confidenceScore = roundNumber(clamp((boat4HeadOpportunityScore / 100) * 0.7 + (Number(confidence.score || 0) / 100) * 0.3) * 100, 0);
  const partnerRanking = buildBoat4PartnerRanking(prediction);
  const ticketModel = buildBoat4RecommendedTickets({
    scoreLabel: strengthLabel,
    components: {
      boat3ResidualScore
    },
    partnerRanking,
    prediction
  });
  const mainReason =
    threeAttackFourBenefitScore >= 0.58
      ? "3号艇が攻めの起点になりそうですが、3自身が残すより4号艇が展開を拾う形を高く見ます。"
      : twoSashiFailureScore >= 0.52 && boat4TurnTimeScore >= 0.58
        ? "2号艇の差し切り評価が低く、4号艇のまわり足・直線が良いため4頭警戒。"
        : boat4HeadOpportunityScore >= 48
          ? "4号艇の足色と展開受けが噛み合えば、4頭まで押さえたいレースです。"
          : "4頭条件は薄めで、基本は相手候補までの評価です。";
  const caution = [
    strengthLabel === "低" ? "4頭推奨度は低め。無理に頭固定しない。" : null,
    ticketModel.reasons.some((row) => row.action === "demote" && row.target === "4-3-flow")
      ? "4頭の場合、この場では4-3より4-1 / 4-2を優先します。"
      : null,
    confidenceScore < 48 ? "全体信頼度が低く、見送り含めて検討。" : null,
    ...condition.notes
  ].filter(Boolean).join(" / ");

  const components = {
    boat4HeadPotentialScore,
    threeAttackFourBenefitScore,
    twoSashiFailureScore,
    boat2WallScore,
    boat2WallWeaknessScore,
    boat3TriggerScore,
    boat3ResidualScore,
    boat4BeneficiaryScore,
    venue4HeadComboBiasScore,
    conditionScore: condition.score,
    boat4TurnTimeScore,
    boat4StraightTimeScore,
    boat4LapTimeScore,
    boat4MotorRankScore,
    boat4Motor2RateScore,
    boat4MakuriSashiRateScore,
    coefficientScore: coefficientScore.score
  };

  return {
    stadiumNumber: finiteNumber(program.race_stadium_number, null),
    raceNumber: finiteNumber(program.race_number, null),
    closedAt: program.race_closed_at ?? null,
    boat4HeadOpportunityScore,
    confidence: confidenceScore,
    strengthLabel,
    mainReason,
    recommendedTickets: ticketModel.tickets,
    caution: caution || "大きな注意点なし",
    exhibitionStatus: prediction.exhibition?.status || "unknown",
    debug: {
      ...Object.fromEntries(Object.entries(components).map(([key, value]) => [key, roundNumber(value * 100, 1)])),
      venue4HeadPartnerRanking: partnerRanking,
      ticketPromotionDemotionReasons: ticketModel.reasons,
      conditionNotes: condition.notes,
      coefficientWeights: opportunityWeights,
      confidenceWarnings: confidence.warnings
    },
    prediction
  };
}

export function buildBoat4OpportunityRanking(programs = [], previewsByRaceKey = {}, options = {}) {
  const config = mergeScoringConfig(options.config || {});
  const rows = (Array.isArray(programs) ? programs : [])
    .map((program) => {
      try {
        const key = `${program.race_stadium_number}-${program.race_number}`;
        const prediction = buildRacePrediction(program, previewsByRaceKey[key] || null, config);
        if (!Array.isArray(prediction.scoredBoats) || prediction.scoredBoats.length !== 6) return null;
        const confidence = buildConfidenceScore(prediction);
        return buildBoat4OpportunityRow(program, prediction, confidence, config);
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) =>
      Number(b.boat4HeadOpportunityScore || 0) - Number(a.boat4HeadOpportunityScore || 0) ||
      Number(b.confidence || 0) - Number(a.confidence || 0) ||
      Number(a.stadiumNumber || 99) - Number(b.stadiumNumber || 99) ||
      Number(a.raceNumber || 99) - Number(b.raceNumber || 99)
    );
  return rows
    .slice(0, Math.max(1, Number(options.limit || 60)))
    .map((row, index) => ({ ...row, rank: index + 1 }));
}

export function buildTodayRanking(programs = [], previewsByRaceKey = {}, options = {}) {
  const config = mergeScoringConfig(options.config || {});
  const rows = (Array.isArray(programs) ? programs : [])
    .map((program) => {
      try {
        const key = `${program.race_stadium_number}-${program.race_number}`;
        const prediction = buildRacePrediction(program, previewsByRaceKey[key] || null, config);
        if (!Array.isArray(prediction.scoredBoats) || prediction.scoredBoats.length !== 6) return null;
        const confidence = buildConfidenceScore(prediction);
        const topTickets = safeArray(prediction.ticketGroups?.mainTickets).length > 0
          ? safeArray(prediction.ticketGroups.mainTickets)
          : safeArray(prediction.ticketGroups?.referenceTickets).length > 0
            ? safeArray(prediction.ticketGroups.referenceTickets)
            : prediction.tickets.trifecta.slice(0, 6);
        const head = prediction.firstPlaceProbabilities[0] || null;
        const counter = prediction.firstPlaceProbabilities.find((row) => row.boat !== head?.boat) || null;
        const insideCandidate =
          head?.boat === 1 && confidence.score >= 55
            ? {
              axis: 1,
              opponents: topTickets
                .flatMap((row) => row.boats.slice(1))
                .filter((boat, index, arr) => arr.indexOf(boat) === index)
                .slice(0, 3)
            }
            : null;
        return {
          stadiumNumber: finiteNumber(program.race_stadium_number, null),
          raceNumber: finiteNumber(program.race_number, null),
          closedAt: program.race_closed_at ?? null,
          confidenceScore: confidence.score,
          attention: confidence.warnings,
          confidenceFactors: confidence.factors,
          mainHead: head,
          counterHead: counter,
          tickets: topTickets.slice(0, 6),
          scenario: prediction.scenario,
          upsetAlert: prediction.upsetAlert,
          upsetReasons: prediction.upsetReasons,
          extraTickets: prediction.extraTickets,
          ticketGroups: prediction.ticketGroups,
          developmentScenarios: prediction.developmentScenarios,
          insideCandidate,
          exhibitionStatus: prediction.exhibition.status,
          prediction
        };
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .sort((a, b) => b.confidenceScore - a.confidenceScore || Number(a.stadiumNumber || 99) - Number(b.stadiumNumber || 99) || Number(a.raceNumber || 99) - Number(b.raceNumber || 99));
  return rows.slice(0, Math.max(1, Number(options.limit || 20)));
}

export function screenInsideEscapeCandidates(programs = [], previewsByRaceKey = {}, config = DEFAULT_SCORING_CONFIG) {
  return programs
    .map((program) => {
      const key = `${program.race_stadium_number}-${program.race_number}`;
      const prediction = buildRacePrediction(program, previewsByRaceKey[key] || null, config);
      const boat1 = prediction.scoredBoats.find((boat) => boat.boat === 1);
      const firstProb = prediction.firstPlaceProbabilities.find((row) => row.boat === 1)?.probability ?? 0;
      const wallRisk = prediction.scoredBoats
        .filter((boat) => boat.boat >= 2)
        .reduce((max, boat) => Math.max(max, boat.scoreParts.makuriBoost + Math.max(0, boat.score - (boat1?.score ?? 0))), 0);
      const score = clamp((firstProb * 100) + (boat1?.score ?? 0) * 10 - wallRisk * 12, 0, 100);
      const opponents = prediction.tickets.trifecta
        .filter((row) => row.boats[0] === 1)
        .flatMap((row) => row.boats.slice(1))
        .filter((boat, index, arr) => arr.indexOf(boat) === index)
        .slice(0, 3);
      const recommended = prediction.tickets.trifecta
        .filter((row) => row.boats[0] === 1)
        .slice(0, 5);
      return {
        raceNumber: finiteNumber(program.race_number, null),
        closedAt: program.race_closed_at ?? null,
        score,
        exhibitionStatus: prediction.exhibition.status,
        axis: 1,
        opponents,
        recommended,
        prediction
      };
    })
    .filter((row) => row.score >= config.insideCandidateThreshold && row.recommended.length === 5)
    .sort((a, b) => b.score - a.score);
}

