export const DEFAULT_SCORING_CONFIG = {
  shrinkK: 24,
  baseWeights: {
    laneBias: 0.28,
    class: 0.2,
    nationalWinRatePoint: 0.34,
    localWinRatePoint: 0.16,
    motor2Rate: 0.13,
    boat2Rate: 0.05,
    averageStartTiming: 0.16,
    flyingPenalty: 0.08,
    latePenalty: 0.04
  },
  exhibitionWeights: {
    exhibitionTimeZ: 0.18,
    exhibitionStartTiming: 0.08,
    entryCourse: 0.16,
    windBias: 0.04,
    makuriAlert: 0.1
  },
  originalExhibitionWeights: {
    roleFeatureBoost: 0.26,
    outsideFirstPlaceDampening: 0.42
  },
  screeningWeights: {
    boat1Strength: 0.26,
    startTrust: 0.16,
    courseEscape: 0.18,
    localFit: 0.1,
    venueInsideAdvantage: 0.14,
    weakWall: 0.12,
    wind: 0.04
  },
  makuriAlertSeconds: 0.07,
  insideCandidateThreshold: 62
};

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
  const n = finiteNumber(value, null);
  return n !== null && n > -0.3 && n < 1;
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
  if (feature.diagnostics.exhibitionStatus.exhibitionNotRun) {
    feature.diagnostics.reason = "preview_all_exhibition_time_zero_or_null_and_course_null";
    return feature;
  }
  if (rows.length < 6) return feature;

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
    return [boat, validStartTiming(value) ? finiteNumber(value) : null, row || null];
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

  const wind = finiteNumber(preview.race_wind ?? preview.windSpeed, null);
  const wave = finiteNumber(preview.race_wave ?? preview.waveHeight, null);
  if (feature.usedFields.length > 0 && (wind !== null || wave !== null || preview.race_weather_number != null)) {
    feature.weather = {
      wind,
      wave,
      weatherNumber: finiteNumber(preview.race_weather_number, null),
      windDirectionNumber: finiteNumber(preview.race_wind_direction_number, null)
    };
    feature.usedFields.push("weather");
  }
  if (feature.usedFields.length > 0) feature.status = "exhibition_reflected";
  return feature;
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
        motor2Rate: finiteNumber(row.racer_assigned_motor_top_2_percent ?? row.motor2Rate ?? row.motor_2rate, null),
        boat2Rate: finiteNumber(row.racer_assigned_boat_top_2_percent, null),
        averageStartTiming: finiteNumber(row.racer_average_start_timing, null),
        exST: finiteNumber(row.racer_start_timing ?? row.exST ?? row.exhibitionSt ?? row.exhibitionST, null),
        exTime: finiteNumber(row.racer_exhibition_time ?? row.exTime ?? row.exhibitionTime, null),
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
        lateCount: finiteNumber(row.racer_late_count, 0)
      };
    })
    .filter((row) => Number.isInteger(row.boat) && row.boat >= 1 && row.boat <= 6)
    .sort((a, b) => a.boat - b.boat);
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

function lowerTimeFeatureScores(valuesByBoat = {}) {
  const entries = Object.entries(valuesByBoat)
    .map(([boat, value]) => [String(boat), finiteNumber(value, null)])
    .filter(([, value]) => value !== null);
  if (entries.length === 0) return { scores: {}, ranks: {}, count: 0 };
  const sorted = [...entries].sort((a, b) => a[1] - b[1]);
  const min = sorted[0][1];
  const max = sorted[sorted.length - 1][1];
  const spread = max - min;
  return {
    scores: Object.fromEntries(entries.map(([boat, value]) => [
      boat,
      spread === 0 ? 0.5 : clamp((max - value) / spread, 0, 1)
    ])),
    ranks: Object.fromEntries(sorted.map(([boat], index) => [boat, index + 1])),
    count: entries.length
  };
}

function highValueFeatureScores(valuesByBoat = {}) {
  const entries = Object.entries(valuesByBoat)
    .map(([boat, value]) => [String(boat), finiteNumber(value, null)])
    .filter(([, value]) => value !== null);
  if (entries.length === 0) return { scores: {}, ranks: {}, count: 0 };
  const sorted = [...entries].sort((a, b) => b[1] - a[1]);
  const min = Math.min(...entries.map(([, value]) => value));
  const max = Math.max(...entries.map(([, value]) => value));
  const spread = max - min;
  return {
    scores: Object.fromEntries(entries.map(([boat, value]) => [
      boat,
      spread === 0 ? 0.5 : clamp((value - min) / spread, 0, 1)
    ])),
    ranks: Object.fromEntries(sorted.map(([boat], index) => [boat, index + 1])),
    count: entries.length
  };
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

function roleFeatureWeights(boat) {
  if (boat === 1) return { exST: 0.18, exTime: 0.16, lapTime: 0.24, turnTime: 0.24, motor2Rate: 0.18 };
  if (boat === 2) return { exST: 0.24, turnTime: 0.3, lapTime: 0.2, motor2Rate: 0.26 };
  if (boat === 3) return { exST: 0.28, straightTime: 0.3, exTime: 0.18, turnTime: 0.24 };
  if (boat === 4) return { exST: 0.24, straightTime: 0.28, turnTime: 0.28, motor2Rate: 0.2 };
  return { straightTime: 0.3, lapTime: 0.25, turnTime: 0.25, motor2Rate: 0.2 };
}

function buildOriginalExhibitionFeatureScores(boats = [], exhibition = {}) {
  const values = {
    exST: Object.fromEntries(boats.map((boat) => [boat.boat, exhibition.exhibitionStartByBoat?.[boat.boat] ?? boat.exST ?? null])),
    exTime: Object.fromEntries(boats.map((boat) => [boat.boat, exhibition.exhibitionTimeByBoat?.[boat.boat] ?? boat.exTime ?? null])),
    lapTime: Object.fromEntries(boats.map((boat) => [boat.boat, boat.lapTime ?? null])),
    straightTime: Object.fromEntries(boats.map((boat) => [boat.boat, boat.straightTime ?? null])),
    turnTime: Object.fromEntries(boats.map((boat) => [boat.boat, boat.turnTime ?? null])),
    motor2Rate: Object.fromEntries(boats.map((boat) => [boat.boat, boat.motor2Rate ?? null]))
  };
  const fieldScores = {
    exST: lowerTimeFeatureScores(values.exST),
    exTime: lowerTimeFeatureScores(values.exTime),
    lapTime: lowerTimeFeatureScores(values.lapTime),
    straightTime: lowerTimeFeatureScores(values.straightTime),
    turnTime: lowerTimeFeatureScores(values.turnTime),
    motor2Rate: highValueFeatureScores(values.motor2Rate)
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
      motor2Rate: Object.prototype.hasOwnProperty.call(fieldScores.motor2Rate.scores, key) ? fieldScores.motor2Rate.scores[key] : null
    };
    const role = weightedFeatureAverage(parts, roleFeatureWeights(boat.boat));
    byBoat[key] = {
      boat: boat.boat,
      values: Object.fromEntries(Object.keys(parts).map((field) => [field, values[field]?.[boat.boat] ?? null])),
      ranks: Object.fromEntries(Object.keys(parts).map((field) => [field, fieldScores[field]?.ranks?.[key] ?? null])),
      scores: parts,
      roleScore: role.score,
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
      motor2RateScore: row.scores.motor2Rate,
      roleScore: row.roleScore,
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
      score: base + exhibitionTimeBoost + exhibitionStBoost + entryBoost + makuriBoost + lapBoost + straightBoost + turnBoost + roleFeatureBoost + techniqueBoost + allCourseReferenceBoost + startTendencyBoost + venueBiasBoost - lateRatePenalty,
      scoreParts: { base, exhibitionTimeBoost, exhibitionStBoost, entryBoost, makuriBoost, lapBoost, straightBoost, turnBoost, roleFeatureBoost, techniqueBoost, allCourseReferenceBoost, startTendencyBoost, lateRatePenalty, venueBiasBoost }
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

export function buildRacePrediction(program = {}, preview = null, config = DEFAULT_SCORING_CONFIG) {
  const scoringConfig = {
    ...DEFAULT_SCORING_CONFIG,
    ...config,
    baseWeights: { ...DEFAULT_SCORING_CONFIG.baseWeights, ...(config?.baseWeights || {}) },
    exhibitionWeights: { ...DEFAULT_SCORING_CONFIG.exhibitionWeights, ...(config?.exhibitionWeights || {}) },
    originalExhibitionWeights: { ...DEFAULT_SCORING_CONFIG.originalExhibitionWeights, ...(config?.originalExhibitionWeights || {}) },
    screeningWeights: { ...DEFAULT_SCORING_CONFIG.screeningWeights, ...(config?.screeningWeights || {}) },
    stadiumNumber: finiteNumber(program.race_stadium_number, null)
  };
  const boats = normalizeProgramBoats(program);
  const exhibition = enrichExhibitionFeaturesFromBoats(preview ? buildExhibitionFeatures(preview) : buildExhibitionFeatures(null), boats);
  const featureScores = buildOriginalExhibitionFeatureScores(boats, exhibition);
  const scoredBoats = buildScores(boats, exhibition, scoringConfig, featureScores).sort((a, b) => a.boat - b.boat);
  const tendencySummary = buildTendencySummary(scoredBoats);
  const firstPlaceProbabilities = softmax(scoredBoats)
    .map((row) => ({ boat: row.boat, course: row.course, probability: row.probability }))
    .sort((a, b) => b.probability - a.probability);
  const trifecta = plackettLuceTrifecta(scoredBoats);
  const tickets = marginalizeTickets(trifecta);
  const prediction = {
    race: {
      date: program.race_date ?? null,
      stadiumNumber: finiteNumber(program.race_stadium_number, null),
      raceNumber: finiteNumber(program.race_number, null),
      closedAt: program.race_closed_at ?? null
    },
    exhibition,
    featureScores,
    featureScorePreview: featureScores.preview,
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
  return {
    ...prediction,
    scenario,
    developmentScenarios: development.scenarios,
    scenarioScorePreview,
    upsetScenarios: development.upsetScenarios,
    upsetAlert: development.upsetAlert,
    upsetReasons: development.upsetReasons,
    extraTickets: development.extraTickets
  };
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
  const developmentPenalty = Array.isArray(prediction.extraTickets)
    ? Math.min(8, prediction.extraTickets.length * 1.2)
    : 0;
  const extraTicketCount = Array.isArray(prediction.extraTickets) ? prediction.extraTickets.length : 0;
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
    tendencyQualityAdjustment;
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
  if (prediction.exhibition?.status !== "exhibition_reflected") warnings.push("展示前の出走表ベース予想");
  return {
    score: Math.round(clamp(score, 0, 100)),
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
      tendencyQualityAdjustment
    }
  };
}

export function buildTodayRanking(programs = [], previewsByRaceKey = {}, options = {}) {
  const config = {
    ...DEFAULT_SCORING_CONFIG,
    ...(options.config || {})
  };
  const rows = (Array.isArray(programs) ? programs : [])
    .map((program) => {
      try {
        const key = `${program.race_stadium_number}-${program.race_number}`;
        const prediction = buildRacePrediction(program, previewsByRaceKey[key] || null, config);
        if (!Array.isArray(prediction.scoredBoats) || prediction.scoredBoats.length !== 6) return null;
        const confidence = buildConfidenceScore(prediction);
        const topTickets = prediction.tickets.trifecta.slice(0, 6);
        while (topTickets.length < 6 && prediction.tickets.trifecta[topTickets.length]) {
          topTickets.push(prediction.tickets.trifecta[topTickets.length]);
        }
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

