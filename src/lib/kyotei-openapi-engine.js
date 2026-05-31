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
      return {
        raw: row,
        boat,
        course: boat,
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
        lapTime: finiteNumber(row.racer_lap_time ?? row.lapTime ?? row.lap_time ?? row.kyoteiBiyoriLapTime ?? row.kyoteibiyori_lap_time, null),
        straightTime: finiteNumber(row.racer_straight_time ?? row.straightTime ?? row.straight_time ?? row.kyoteiBiyoriStraightTime ?? row.kyoteibiyori_straight_time, null),
        techniqueStats: {
          escapeRate: finiteNumber(row.escapeRate ?? row.escape_rate ?? row.course1EscapeRate ?? null, null),
          nigashiRate: finiteNumber(row.nigashiRate ?? row.nigashi_rate ?? row.course2NigashiRate ?? null, null),
          sashiRate: finiteNumber(row.sashiRate ?? row.sashi_rate ?? row.course2SashiRate ?? null, null),
          makuriRate: finiteNumber(row.makuriRate ?? row.makuri_rate ?? null, null),
          makuriSashiRate: finiteNumber(row.makuriSashiRate ?? row.makuri_sashi_rate ?? null, null),
          course6TrifectaRate: finiteNumber(row.course6TrifectaRate ?? row.course6_trifecta_rate ?? null, null)
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

function optionalRate01(value) {
  const n = finiteNumber(value, null);
  return n === null ? null : percent01(n, 0.5);
}

function getVenueLaneBias(config, stadiumNumber, lane) {
  const source = config?.venueLaneBias;
  if (!source) return null;
  if (Array.isArray(source)) {
    return source.find((row) => Number(row?.venue ?? row?.stadiumNumber) === Number(stadiumNumber) && Number(row?.lane) === Number(lane)) || null;
  }
  return source?.[`${stadiumNumber}-${lane}`] || source?.[String(stadiumNumber)]?.[String(lane)] || null;
}

function buildScores(boats, exhibition, config) {
  const timeZ = exhibition.exhibitionTimeByBoat ? zScores(exhibition.exhibitionTimeByBoat) : {};
  const lapValues = Object.fromEntries(boats.filter((boat) => boat.lapTime !== null).map((boat) => [boat.boat, boat.lapTime]));
  const lapZ = Object.keys(lapValues).length >= 2 ? zScores(lapValues) : {};
  const straightValues = Object.fromEntries(boats.filter((boat) => boat.straightTime !== null).map((boat) => [boat.boat, boat.straightTime]));
  const straightZ = Object.keys(straightValues).length >= 2 ? zScores(straightValues) : {};
  const courses = exhibition.entryCourseByBoat || Object.fromEntries(boats.map((boat) => [boat.boat, boat.boat]));
  return boats.map((boat) => {
    const course = finiteNumber(courses[boat.boat], boat.boat);
    const venueBias = getVenueLaneBias(config, boat.raw?.race_stadium_number, course) || getVenueLaneBias(config, config?.stadiumNumber, course);
    const venueBiasBoost = venueBias
      ? (
        (optionalRate01(venueBias.winRate) ?? laneBias01(course)) -
        laneBias01(course)
      ) * 0.12
      : 0;
    const techniqueBoost =
      ((optionalRate01(boat.techniqueStats.escapeRate) ?? 0.5) - 0.5) * (course === 1 ? 0.08 : 0) +
      ((optionalRate01(boat.techniqueStats.sashiRate) ?? 0.5) - 0.5) * (course === 2 ? 0.07 : 0) +
      ((optionalRate01(boat.techniqueStats.makuriRate) ?? 0.5) - 0.5) * ([3, 4].includes(course) ? 0.07 : 0) +
      ((optionalRate01(boat.techniqueStats.makuriSashiRate) ?? 0.5) - 0.5) * ([3, 4, 5].includes(course) ? 0.07 : 0);
    const lapBoost = Object.prototype.hasOwnProperty.call(lapZ, String(boat.boat))
      ? clamp(-Number(lapZ[String(boat.boat)] || 0) / 2, -0.4, 0.4) * 0.08
      : 0;
    const straightBoost = Object.prototype.hasOwnProperty.call(straightZ, String(boat.boat))
      ? clamp(-Number(straightZ[String(boat.boat)] || 0) / 2, -0.4, 0.4) * (course >= 3 ? 0.1 : 0.04)
      : 0;
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
      score: base + exhibitionTimeBoost + exhibitionStBoost + entryBoost + makuriBoost + lapBoost + straightBoost + techniqueBoost + venueBiasBoost,
      scoreParts: { base, exhibitionTimeBoost, exhibitionStBoost, entryBoost, makuriBoost, lapBoost, straightBoost, techniqueBoost, venueBiasBoost }
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
  const outsideAlert = prediction.scoredBoats
    .filter((boat) => boat.boat >= 3 && boat.scoreParts.makuriBoost > 0)
    .sort((a, b) => b.scoreParts.makuriBoost - a.scoreParts.makuriBoost)[0];
  const mainMethod = head?.boat === 1 ? "逃げ" : head?.course <= 2 ? "差し" : "まくり差し";
  const counterMethod = outsideAlert ? "まくり/まくり差し" : "差し残し";
  return {
    main: {
      title: "本線シナリオ",
      text: `${head?.boat ?? "-"}号艇の${mainMethod}が中心。${hasExhibition ? "展示反映済みの隊形" : "枠なり前提"}で、1Mは${top[0]?.combo ?? "-"}を軸に見る。`,
      tickets: top.slice(0, 2).map((row) => row.combo)
    },
    counter: {
      title: "対抗シナリオ",
      text: outsideAlert
        ? `${outsideAlert.boat}号艇の展示気配が内側より強く、${counterMethod}の一撃を警戒。`
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
  const boat1TrustLow = p(1) < 0.34 || (prediction.exhibition?.exhibitionStartByBoat && startTimingScore(exSt[1], 0.5) < 0.45) || Number(timeZ["1"] || 0) > 0.6 || Number(lapZ["1"] || 0) > 0.6;
  const sortedByScore = [...prediction.scoredBoats].sort((a, b) => b.score - a.score);
  const scoreGapSmall = Math.abs((sortedByScore[0]?.score ?? 0) - (sortedByScore[2]?.score ?? 0)) < 0.12;
  const scenarios = [
    buildScenarioRow({
      prediction,
      scenarioName: "イン逃げ成功シナリオ",
      attacker: 1,
      baseScore: p(1) + rateScore(boat1.techniqueStats?.escapeRate, 0.5) * 0.2,
      upsetScore: boat1TrustLow ? 0.2 : 0.05,
      description: "1号艇が先マイして内有利を保つ本線展開。",
      patterns: [[1, 2, "flow"], [1, 3, "flow"]],
      reasons: []
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "2号艇差しシナリオ",
      attacker: 2,
      baseScore: p(2) + rateScore(boat2.techniqueStats?.sashiRate, 0.5) * 0.24 + (boat1TrustLow ? 0.12 : 0),
      upsetScore: (boat1TrustLow ? 0.35 : 0.12) + (rateScore(boat2.techniqueStats?.nigashiRate, 0.5) < 0.42 ? 0.18 : 0),
      description: "1号艇の踏み込みが甘い場合、2号艇の差し抜けや1残しを警戒。",
      patterns: [[2, 1, "flow"], [2, 3, "flow"], [2, 4, "flow"]],
      reasons: [boat1TrustLow ? "1号艇の信頼度が低め" : null, rateScore(boat2.techniqueStats?.sashiRate, 0.5) > 0.58 ? "2号艇の差し率が高い" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "3号艇まくりシナリオ",
      attacker: 3,
      baseScore: p(3) + rateScore(boat3.techniqueStats?.makuriRate, 0.5) * 0.22 + (startTimingScore(exSt[3], 0.5) > 0.65 ? 0.12 : 0) + (Number(straightZ["3"] || 0) < -0.5 ? 0.1 : 0),
      upsetScore: (boat1TrustLow ? 0.25 : 0.12) + (startTimingScore(exSt[3], 0.5) > 0.65 ? 0.22 : 0) + (scoreGapSmall ? 0.12 : 0) + (Number(straightZ["3"] || 0) < -0.5 ? 0.18 : 0),
      description: "3号艇が先に握ると内が抵抗して隊形が崩れる可能性。直線が良ければまくり切りも警戒。",
      patterns: [[3, 1, "flow"], [3, 4, "flow"], [3, 5, "flow"]],
      reasons: [startTimingScore(exSt[3], 0.5) > 0.65 ? "3号艇の展示STが早い" : null, Number(straightZ["3"] || 0) < -0.5 ? "3号艇の直線が速い" : null, scoreGapSmall ? "上位評価の差が小さい" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "3号艇まくり差しシナリオ",
      attacker: 3,
      baseScore: p(3) + rateScore(boat3.techniqueStats?.makuriSashiRate, 0.5) * 0.24 + (Number(timeZ["3"] || 0) < -0.5 ? 0.1 : 0),
      upsetScore: (boat1TrustLow ? 0.22 : 0.1) + (Number(timeZ["3"] || 0) < -0.5 ? 0.2 : 0),
      description: "3号艇が握りながら差し場を拾う展開。1残しと4連動を重視。",
      patterns: [[3, 1, "flow"], [3, 4, "flow"], [1, 3, "flow"]],
      reasons: [Number(timeZ["3"] || 0) < -0.5 ? "3号艇の展示タイムが良い" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "4号艇まくりシナリオ",
      attacker: 4,
      baseScore: p(4) + rateScore(boat4.techniqueStats?.makuriRate, 0.5) * 0.2 + (startTimingScore(exSt[4], 0.5) > 0.65 ? 0.12 : 0),
      upsetScore: (startTimingScore(exSt[4], 0.5) > 0.65 ? 0.22 : 0.1) + (boat1TrustLow ? 0.2 : 0),
      description: "4号艇のカド攻めで内が流れる展開。",
      patterns: [[4, 1, "flow"], [4, 3, "flow"], [4, 5, "flow"]],
      reasons: [startTimingScore(exSt[4], 0.5) > 0.65 ? "4号艇の展示STが早い" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "4号艇まくり差しシナリオ",
      attacker: 4,
      baseScore: p(4) + rateScore(boat4.techniqueStats?.makuriSashiRate, 0.5) * 0.26 + (Number(timeZ["4"] || 0) < -0.45 ? 0.1 : 0) + (Number(straightZ["4"] || 0) < -0.5 ? 0.1 : 0),
      upsetScore: (boat1TrustLow ? 0.24 : 0.12) + (p(3) > p(2) ? 0.12 : 0) + (scoreGapSmall ? 0.1 : 0) + (Number(straightZ["4"] || 0) < -0.5 ? 0.16 : 0),
      description: "3号艇が攻めて内を動かし、4号艇が差し場を突く形に注意。直線が良い外艇は抜け出しもある。",
      patterns: [[4, 3, "flow"], [4, 1, "flow"], [3, 4, "flow"]],
      reasons: [p(3) > p(2) ? "3号艇攻めから4号艇差しの形があり得る" : null, Number(straightZ["4"] || 0) < -0.5 ? "4号艇の直線が速い" : null, scoreGapSmall ? "1着候補が割れている" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "5号艇展開突きシナリオ",
      attacker: 5,
      baseScore: p(5) + percent01(boat5.motor2Rate, 0.4) * 0.15 + (Number(lapZ["5"] || 0) < -0.6 ? 0.12 : 0) + (Number(straightZ["5"] || 0) < -0.6 ? 0.12 : 0),
      upsetScore: (Number(lapZ["5"] || 0) < -0.6 ? 0.25 : 0.1) + (Number(straightZ["5"] || 0) < -0.6 ? 0.2 : 0) + (percent01(boat5.motor2Rate, 0.4) > 0.45 ? 0.12 : 0),
      description: "内の攻め合いが長引くと5号艇が展開を突いて連に絡む筋。",
      patterns: [[5, 1, "flow"], [5, 3, "flow"], [5, 4, "flow"]],
      reasons: [Number(lapZ["5"] || 0) < -0.6 ? "5号艇のLap Timeが良い" : null, Number(straightZ["5"] || 0) < -0.6 ? "5号艇の直線が速い" : null, percent01(boat5.motor2Rate, 0.4) > 0.45 ? "5号艇のモーター2連率が高い" : null]
    }),
    buildScenarioRow({
      prediction,
      scenarioName: "6号艇展開突きシナリオ",
      attacker: 6,
      baseScore: p(6) + percent01(boat6.motor2Rate, 0.4) * 0.15 + rateScore(boat6.techniqueStats?.course6TrifectaRate, 0.45) * 0.12 + (Number(straightZ["6"] || 0) < -0.6 ? 0.1 : 0),
      upsetScore: (Number(lapZ["6"] || 0) < -0.6 ? 0.22 : 0.08) + (Number(straightZ["6"] || 0) < -0.6 ? 0.18 : 0) + (percent01(boat6.motor2Rate, 0.4) > 0.45 ? 0.12 : 0),
      description: "外枠でも機力や残り足が上位なら、崩れた展開で3着穴まで。",
      patterns: [[6, 1, "flow"], [6, 3, "flow"], [6, 4, "flow"]],
      reasons: [Number(lapZ["6"] || 0) < -0.6 ? "6号艇のLap Timeが良い" : null, Number(straightZ["6"] || 0) < -0.6 ? "6号艇の直線が速い" : null, percent01(boat6.motor2Rate, 0.4) > 0.45 ? "6号艇のモーター2連率が高い" : null]
    })
  ];
  const upsetScenarios = scenarios
    .filter((row) => row.attacker !== 1 && row.upsetScore >= 28 && row.recommendedExtraTickets.length > 0)
    .sort((a, b) => b.upsetScore - a.upsetScore || b.probabilityScore - a.probabilityScore);
  const extraTickets = [];
  const seen = new Set();
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
export function buildRacePrediction(program = {}, preview = null, config = DEFAULT_SCORING_CONFIG) {
  const scoringConfig = {
    ...DEFAULT_SCORING_CONFIG,
    ...config,
    baseWeights: { ...DEFAULT_SCORING_CONFIG.baseWeights, ...(config?.baseWeights || {}) },
    exhibitionWeights: { ...DEFAULT_SCORING_CONFIG.exhibitionWeights, ...(config?.exhibitionWeights || {}) },
    screeningWeights: { ...DEFAULT_SCORING_CONFIG.screeningWeights, ...(config?.screeningWeights || {}) },
    stadiumNumber: finiteNumber(program.race_stadium_number, null)
  };
  const boats = normalizeProgramBoats(program);
  const exhibition = preview ? buildExhibitionFeatures(preview) : buildExhibitionFeatures(null);
  const scoredBoats = buildScores(boats, exhibition, scoringConfig).sort((a, b) => a.boat - b.boat);
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
  return {
    ...prediction,
    scenario,
    developmentScenarios: development.scenarios,
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
  const entryUnconfirmed = prediction.exhibition?.entryCourseByBoat ? 0 : 1;
  const boat1ExhibitionStScore = prediction.exhibition?.exhibitionStartByBoat
    ? startTimingScore(prediction.exhibition.exhibitionStartByBoat[1], 0.45)
    : 0.5;
  const timeZ = prediction.exhibition?.exhibitionTimeByBoat ? zScores(prediction.exhibition.exhibitionTimeByBoat) : {};
  const boat1ExhibitionTimeScore = prediction.exhibition?.exhibitionTimeByBoat
    ? clamp(0.5 + (-Number(timeZ["1"] || 0) * 0.18), 0, 1)
    : 0.5;
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
    developmentPenalty;
  const warnings = [];
  if (entryUnconfirmed) warnings.push("進入未確定のため直前展示で再確認");
  if (outsideHead >= 0.18) warnings.push("5・6号艇の頭浮上余地あり");
  if (topSplitPenalty > 0.08) warnings.push("1着候補が割れている");
  if (fHolderPenalty > 0) warnings.push("F持ちの影響を評価に反映");
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
      developmentPenalty
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

