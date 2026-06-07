import {
  decisionSampleWeight,
  getDecisionConditionedStats,
  getEstimatedVenueBias,
  getHeadDecisionComboStats,
  weightedDecisionRate
} from "./venue-bias-engine.js";
import { DEFAULT_SCORING_CONFIG, mergeScoringConfig } from "./scoring-config.js";
import { getVenueProfile } from "./venue-profile.js";

const BOATS = [1, 2, 3, 4, 5, 6];

const SCENARIO_LABELS = {
  escape_1: "イン逃げ成功",
  sashi_2: "2号艇差し",
  makuri_3: "3号艇まくり",
  makuri_sashi_3: "3号艇まくり差し",
  second_wave_4: "4号艇まくり差し",
  outside_follow_5_6: "5・6号艇展開突き"
};

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function finiteNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 3) {
  const n = finiteNumber(value, 0);
  const scale = 10 ** digits;
  return Math.round(n * scale) / scale;
}

function roundScore(value) {
  return round(clamp(value) * 100, 1);
}

function optionalRate01(value) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  return clamp(n > 1 ? n / 100 : n, 0, 1);
}

function positiveRate(tendency, field, threshold = 0.1) {
  const rate = optionalRate01(tendency?.[field]);
  if (rate === null) return 0;
  return Math.max(0, rate - threshold) * tendencySampleWeight(tendency);
}

function startTimingScore(value, fallback = 0.5) {
  const st = finiteNumber(value, null);
  if (st === null) return fallback;
  return clamp((0.28 - st) / 0.22, 0, 1);
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
    small_sample: 0.38,
    very_small_sample: 0.12,
    insufficient_history: 0,
    unknown: 0.18
  })[tendencySampleStatus(tendency)] ?? 0;
}

function tendencyHasSignal(tendency = {}) {
  if (!tendency || typeof tendency !== "object") return false;
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
    "courseWinRate",
    "courseQuinellaRate",
    "courseTrifectaRate",
    "recentWinRate",
    "recentQuinellaRate",
    "recentTrifectaRate",
    "localVenueWinRate",
    "localVenueQuinellaRate",
    "localVenueTrifectaRate"
  ];
  const count = finiteNumber(
    tendency.courseSpecificLast6mRaceCount ?? tendency.last6mRaceCount ?? tendency.allCourseLast6mRaceCount,
    0
  );
  return count > 0 || fields.some((field) => tendency[field] !== null && tendency[field] !== undefined);
}

function mergeTendency(row = {}) {
  const direct = {};
  for (const field of [
    "escapeRate",
    "beatenBySashiRate",
    "beatenByMakuriRate",
    "beatenByMakuriSashiRate",
    "sashiRate",
    "makuriRate",
    "makuriSashiRate",
    "avgST",
    "avgStartTiming",
    "lateStartRate",
    "earlyStartRate",
    "sampleStatus",
    "last6mRaceCount",
    "courseSpecificLast6mRaceCount",
    "allCourseLast6mRaceCount",
    "allCourseWinRate",
    "allCourseSashiRate",
    "allCourseMakuriRate",
    "allCourseMakuriSashiRate",
    "allCourseAvgST",
    "courseWinRate",
    "courseQuinellaRate",
    "courseTrifectaRate",
    "recentWinRate",
    "recentQuinellaRate",
    "recentTrifectaRate",
    "localVenueWinRate",
    "localVenueQuinellaRate",
    "localVenueTrifectaRate"
  ]) {
    if (row[field] !== null && row[field] !== undefined) direct[field] = row[field];
  }
  return {
    ...(row.techniqueStats || {}),
    ...(row.racerCourseStats || {}),
    ...(row.playerTendency || {}),
    ...direct
  };
}

function boatNumber(row = {}) {
  return finiteNumber(row.boat ?? row.boatNumber ?? row.lane ?? row.entry ?? row.racer_boat_number, null);
}

function buildBoatMap(entries = []) {
  const map = new Map();
  for (const row of safeArray(entries)) {
    const boat = boatNumber(row);
    if (Number.isInteger(boat) && boat >= 1 && boat <= 6) map.set(boat, row);
  }
  return map;
}

function featureScore(row = {}, featureScores = {}, field, fallback = 0.5) {
  const boat = boatNumber(row);
  const fromRow = row?.featureScores?.scores?.[field];
  if (fromRow !== null && fromRow !== undefined) return clamp(Number(fromRow), 0, 1);
  const fromAll = featureScores?.byBoat?.[String(boat)]?.scores?.[field];
  if (fromAll !== null && fromAll !== undefined) return clamp(Number(fromAll), 0, 1);
  return fallback;
}

function roleScore(row = {}, featureScores = {}, fallback = 0.5) {
  const boat = boatNumber(row);
  const fromRow = row?.featureScores?.roleScore;
  if (fromRow !== null && fromRow !== undefined) return clamp(Number(fromRow), 0, 1);
  const fromAll = featureScores?.byBoat?.[String(boat)]?.roleScore;
  if (fromAll !== null && fromAll !== undefined) return clamp(Number(fromAll), 0, 1);
  return fallback;
}

function featureAggregateScore(row = {}, featureScores = {}, key, fallback = 0.5) {
  const boat = boatNumber(row);
  const fromRow = row?.featureScores?.[key];
  if (fromRow !== null && fromRow !== undefined) return clamp(Number(fromRow), 0, 1);
  const fromAll = featureScores?.byBoat?.[String(boat)]?.[key];
  if (fromAll !== null && fromAll !== undefined) return clamp(Number(fromAll), 0, 1);
  return fallback;
}

function reliabilitySampleMultiplier(tendency = {}) {
  return ({
    ok: 1,
    small_sample: 0.86,
    very_small_sample: 0.72,
    insufficient_history: 0.58,
    unknown: 0.76
  })[tendencySampleStatus(tendency)] ?? 0.72;
}

function weightedPresentScore(parts = []) {
  let weighted = 0;
  let total = 0;
  const used = {};
  for (const [field, value, weight] of parts) {
    const n = optionalRate01(value);
    if (n === null) continue;
    weighted += n * weight;
    total += weight;
    used[field] = n;
  }
  return {
    score: total > 0 ? weighted / total : null,
    used
  };
}

function buildReliabilityProfile({ tendency = {}, lapTime = 0.5, turnTime = 0.5, motorRank = 0.5 } = {}) {
  const rateScore = weightedPresentScore([
    ["courseQuinellaRate", tendency.courseQuinellaRate, 0.2],
    ["courseTrifectaRate", tendency.courseTrifectaRate, 0.28],
    ["recentQuinellaRate", tendency.recentQuinellaRate, 0.12],
    ["recentTrifectaRate", tendency.recentTrifectaRate, 0.14],
    ["localVenueQuinellaRate", tendency.localVenueQuinellaRate, 0.1],
    ["localVenueTrifectaRate", tendency.localVenueTrifectaRate, 0.12],
    ["courseWinRate", tendency.courseWinRate, 0.04]
  ]);
  const footScore = clamp((lapTime * 0.32) + (turnTime * 0.36) + (motorRank * 0.32));
  const sampleMultiplier = reliabilitySampleMultiplier(tendency);
  const rawScore = rateScore.score === null
    ? clamp(0.47 + (footScore - 0.5) * 0.45)
    : clamp((rateScore.score * 0.58 + footScore * 0.42) * sampleMultiplier + 0.5 * (1 - sampleMultiplier));
  return {
    score: rawScore,
    rateScore: rateScore.score,
    footScore,
    sampleMultiplier,
    usedRates: rateScore.used,
    source: Object.keys(rateScore.used).length > 0 ? "course_recent_local_rates" : "foot_motor_fallback"
  };
}

function normalizeVenueNode(node, course) {
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {
    return node.find((row) => Number(row?.course ?? row?.lane ?? row?.boat) === course) || null;
  }
  return node[String(course)] || node[course] || null;
}

function venueCourseBias(venueBias, stadiumNumber, course) {
  if (!venueBias || typeof venueBias !== "object") return null;
  const candidates = [
    normalizeVenueNode(venueBias, course),
    normalizeVenueNode(venueBias.courses, course),
    normalizeVenueNode(venueBias.lanes, course),
    normalizeVenueNode(venueBias[String(stadiumNumber)], course),
    normalizeVenueNode(venueBias?.[String(stadiumNumber)]?.courses, course),
    normalizeVenueNode(venueBias?.[String(stadiumNumber)]?.lanes, course),
    venueBias[`${stadiumNumber}-${course}`],
    venueBias[`${stadiumNumber}:${course}`]
  ];
  return candidates.find((row) => row && typeof row === "object") || null;
}

function venueRate(venueBias, stadiumNumber, course, fields = []) {
  const row = venueCourseBias(venueBias, stadiumNumber, course);
  if (!row) return null;
  for (const field of fields) {
    const rate = optionalRate01(row[field]);
    if (rate !== null) return rate;
  }
  return null;
}

function decisionRate(row = {}, field, fallback = 0.5) {
  return weightedDecisionRate(row, field, fallback);
}

function comboRate(row = {}, group, key, fallback = 0.5) {
  const rates = row?.[group] && typeof row[group] === "object" ? row[group] : {};
  const raw = optionalRate01(rates?.[key]);
  if (raw === null) return fallback;
  return fallback + (raw - fallback) * decisionSampleWeight(row?.sampleCount);
}

function residualVenueBlend(...values) {
  const present = values.filter((value) => value !== null && value !== undefined && Number.isFinite(Number(value)));
  if (!present.length) return 0.5;
  return present.reduce((sum, value) => sum + Number(value), 0) / present.length;
}

function decisionCompatibilityMultiplier(score, floor = 0.62, ceiling = 1.24) {
  return clamp(floor + clamp(score) * (ceiling - floor), floor, ceiling);
}

function normalizeRaceConditions(source = {}) {
  const root = source && typeof source === "object" ? source : {};
  const text = String(root.windDirection ?? root.wind_direction ?? root.windDir ?? "");
  const windSpeed = finiteNumber(root.windSpeed ?? root.wind ?? root.race_wind, null);
  const waveHeight = finiteNumber(root.waveHeight ?? root.wave ?? root.race_wave, null);
  const tideLevel = finiteNumber(root.tideLevel ?? root.tide ?? root.tide_level ?? root.race_tide_level, null);
  const tideDirection = root.tideDirection ?? root.tide_direction ?? root.race_tide_direction ?? null;
  const tidePhase = root.tidePhase ?? root.tide_phase ?? root.race_tide_phase ?? null;
  const waterType = root.waterType ?? root.water_type ?? root.race_water_type ?? null;
  const directionLower = text.toLowerCase();
  const isTailwind = /tail|追/.test(directionLower);
  const isHeadwind = /head|向/.test(directionLower);
  const isCrosswind = /cross|横/.test(directionLower);
  const windLevel = windSpeed === null ? 0 : windSpeed >= 7 ? 1 : windSpeed >= 5 ? 0.58 : 0;
  const waveLevel = waveHeight === null ? 0 : waveHeight >= 8 ? 1 : waveHeight >= 5 ? 0.58 : 0;
  const tideLevelRisk = tideLevel === null ? 0 : tideLevel >= 120 ? 0.8 : tideLevel >= 80 ? 0.45 : 0;
  return {
    windDirection: root.windDirection ?? root.wind_direction ?? root.windDir ?? null,
    windSpeed,
    waveHeight,
    weather: root.weather ?? null,
    temperature: finiteNumber(root.temperature ?? root.race_temperature, null),
    waterTemperature: finiteNumber(root.waterTemperature ?? root.water_temperature ?? root.race_water_temperature, null),
    tideLevel,
    tideDirection,
    tidePhase,
    waterType,
    tideLevelRisk,
    windLevel,
    waveLevel,
    isTailwind,
    isHeadwind,
    isCrosswind,
    available: windSpeed !== null || waveHeight !== null || root.weather !== null || root.windDirection !== null || tideLevel !== null || tideDirection !== null || tidePhase !== null || waterType !== null
  };
}

function boatMetrics(row = {}, featureScores = {}) {
  const tendency = mergeTendency(row);
  const tendencyStart = tendency.avgStartTiming ?? tendency.avgST ?? tendency.allCourseAvgST;
  const lateRate = optionalRate01(tendency.lateStartRate);
  const earlyRate = optionalRate01(tendency.earlyStartRate);
  const fCount = Math.max(0, finiteNumber(row.flyingCount ?? row.fCount ?? row.F, 0));
  const fStatusText = String(row.fStatus ?? row.F ?? "").toUpperCase();
  const lapTime = featureScore(row, featureScores, "lapTime", 0.5);
  const straightTime = featureScore(row, featureScores, "straightTime", 0.5);
  const turnTime = featureScore(row, featureScores, "turnTime", 0.5);
  const motorRank = featureScore(row, featureScores, "motorRank", row.motorPercentileAtVenue ?? 0.5);
  const reliability = buildReliabilityProfile({ tendency, lapTime, turnTime, motorRank });
  return {
    boat: boatNumber(row),
    course: finiteNumber(row.course ?? row.lane ?? row.boat, boatNumber(row)),
    exST: featureScore(row, featureScores, "exST", 0.5),
    exTime: featureScore(row, featureScores, "exTime", 0.5),
    lapTime,
    straightTime,
    turnTime,
    motorRank,
    motor2Rate: featureScore(row, featureScores, "motor2Rate", optionalRate01(row.motor2Rate) ?? 0.5),
    roleScore: roleScore(row, featureScores, 0.5),
    headFeatureScore: featureAggregateScore(row, featureScores, "headFeatureScore", roleScore(row, featureScores, 0.5)),
    residualFeatureScore: featureAggregateScore(row, featureScores, "residualFeatureScore", roleScore(row, featureScores, 0.5)),
    fourBeneficiaryFeatureScore: featureAggregateScore(row, featureScores, "fourBeneficiaryFeatureScore", roleScore(row, featureScores, 0.5)),
    tendency,
    courseWinRate: optionalRate01(tendency.courseWinRate),
    courseQuinellaRate: optionalRate01(tendency.courseQuinellaRate),
    courseTrifectaRate: optionalRate01(tendency.courseTrifectaRate),
    recentWinRate: optionalRate01(tendency.recentWinRate),
    recentQuinellaRate: optionalRate01(tendency.recentQuinellaRate),
    recentTrifectaRate: optionalRate01(tendency.recentTrifectaRate),
    localVenueWinRate: optionalRate01(tendency.localVenueWinRate),
    localVenueQuinellaRate: optionalRate01(tendency.localVenueQuinellaRate),
    localVenueTrifectaRate: optionalRate01(tendency.localVenueTrifectaRate),
    reliabilityScore: reliability.score,
    reliabilityProfile: reliability,
    sampleStatus: tendencySampleStatus(tendency),
    sampleWeight: tendencySampleWeight(tendency),
    startHistoryScore: startTimingScore(tendencyStart, 0.5),
    lateRate: lateRate ?? 0.08,
    earlyRate: earlyRate ?? 0.08,
    fCount,
    hasFRisk: fCount > 0 || /\bF|Ｆ/.test(fStatusText),
    flyingPenalty: fCount * 0.08,
    raw: row
  };
}

function wallScore(metric = {}, scoringConfig = DEFAULT_SCORING_CONFIG) {
  const startWeight = scoringConfig?.scoringCoefficients?.headScore?.exST ? 0.14 : 0.12;
  return clamp(
    0.1 +
    metric.exST * 0.18 +
    metric.straightTime * 0.1 +
    metric.turnTime * 0.1 +
    metric.motorRank * 0.1 +
    metric.motor2Rate * 0.1 +
    metric.reliabilityScore * 0.08 +
    (metric.courseQuinellaRate ?? 0.5) * 0.05 +
    (metric.courseTrifectaRate ?? 0.5) * 0.06 +
    metric.startHistoryScore * Math.min(startWeight, 0.1) +
    (1 - metric.lateRate) * 0.16 +
    Math.max(0, metric.earlyRate - 0.16) * 0.04 -
    metric.flyingPenalty -
    (metric.hasFRisk ? 0.04 : 0)
  );
}

function blockingScore(metric = {}) {
  return clamp(
    0.12 +
    metric.exST * 0.2 +
    metric.straightTime * 0.16 +
    metric.turnTime * 0.14 +
    metric.motorRank * 0.12 +
    metric.reliabilityScore * 0.2 +
    (metric.courseQuinellaRate ?? 0.5) * 0.1 +
    (metric.courseTrifectaRate ?? 0.5) * 0.14 +
    metric.startHistoryScore * 0.08 -
    Math.max(0, metric.lateRate - 0.12) * 0.14 -
    metric.flyingPenalty -
    (metric.hasFRisk ? 0.05 : 0)
  );
}

function boat3WallAttackScore(metric = {}) {
  return clamp(
    0.12 +
    metric.exST * 0.24 +
    metric.straightTime * 0.22 +
    metric.turnTime * 0.08 +
    metric.motorRank * 0.14 +
    metric.motor2Rate * 0.08 +
    positiveRate(metric.tendency, "makuriRate", 0.08) * 0.32 +
    positiveRate(metric.tendency, "makuriSashiRate", 0.07) * 0.26 +
    metric.startHistoryScore * 0.1 -
    Math.max(0, metric.lateRate - 0.12) * 0.12 -
    (metric.hasFRisk ? 0.06 : 0)
  );
}

function supportCount(checks = []) {
  return checks.filter(Boolean).length;
}

function scenarioRow({ id, attacker, score, triggerScore, beneficiaries = [], partners = [], reasons = [], patterns = [] }) {
  return {
    id,
    label: SCENARIO_LABELS[id] || id,
    attacker,
    score: roundScore(score),
    score01: round(clamp(score)),
    triggerScore: roundScore(triggerScore ?? score),
    beneficiaries,
    partners,
    patterns,
    reasons: reasons.filter(Boolean)
  };
}

function expandPattern(pattern = [], baseTicketCombos = new Set()) {
  const [first, second, third] = pattern;
  const rows = [];
  const thirds = third === "flow"
    ? BOATS.filter((boat) => boat !== first && boat !== second)
    : [third];
  for (const t of thirds) {
    if (!Number.isInteger(t) || t < 1 || t > 6 || t === first || t === second) continue;
    const combo = `${first}-${second}-${t}`;
    if (baseTicketCombos.has(combo)) continue;
    rows.push(combo);
  }
  return rows;
}

function buildHeadCandidateRows(scoreByBoat = {}) {
  return Object.values(scoreByBoat)
    .filter((row) => row.canBeHead)
    .sort((a, b) => b.headScore - a.headScore || a.boat - b.boat)
    .slice(0, 4)
    .map((row) => ({ boat: row.boat, score: roundScore(row.headScore), reasons: row.headReasons }));
}

function buildPartnerRows(scoreByBoat = {}) {
  return Object.values(scoreByBoat)
    .sort((a, b) => Math.max(b.secondScore, b.thirdScore) - Math.max(a.secondScore, a.thirdScore) || a.boat - b.boat)
    .slice(0, 5)
    .map((row) => ({
      boat: row.boat,
      secondScore: roundScore(row.secondScore),
      thirdScore: roundScore(row.thirdScore),
      reasons: row.partnerReasons
    }));
}

function buildDangerRows(scoreByBoat = {}) {
  return Object.values(scoreByBoat)
    .filter((row) => !row.canBeHead && (row.scenarioTriggerScore >= 0.5 || row.beneficiaryScore >= 0.48 || row.boat >= 5 && row.thirdScore >= 0.55))
    .sort((a, b) => Math.max(b.scenarioTriggerScore, b.beneficiaryScore, b.thirdScore) - Math.max(a.scenarioTriggerScore, a.beneficiaryScore, a.thirdScore))
    .slice(0, 4)
    .map((row) => ({
      boat: row.boat,
      reason: row.dangerReason,
      triggerScore: roundScore(row.scenarioTriggerScore),
      beneficiaryScore: roundScore(row.beneficiaryScore)
    }));
}

export function buildRaceFlowScenarioModel({
  entries = [],
  featureScores = {},
  venueBias = null,
  stadiumNumber = null,
  raceConditions = null,
  venueProfile = null,
  scoringConfig = DEFAULT_SCORING_CONFIG
} = {}) {
  const config = mergeScoringConfig(scoringConfig || {});
  const effectiveVenueBias = venueBias || getEstimatedVenueBias(stadiumNumber);
  const map = buildBoatMap(entries);
  const metrics = {};
  for (const boat of BOATS) {
    metrics[boat] = boatMetrics(map.get(boat) || { boat }, featureScores);
  }

  const tendencyAvailable = Object.values(metrics).some((row) => tendencyHasSignal(row.tendency));
  const venueAvailable = !!effectiveVenueBias || BOATS.some((boat) => venueCourseBias(effectiveVenueBias, stadiumNumber, boat));
  const dataWarnings = [];
  if (!tendencyAvailable) dataWarnings.push("戦法データが不足しているため、展開シナリオは展示・足色中心で評価");
  if (!venueAvailable) dataWarnings.push("会場バイアスが未取得のため、全国共通の展開評価で補正");
  const conditions = normalizeRaceConditions(raceConditions || {});
  const resolvedVenueProfile = venueProfile || getVenueProfile(stadiumNumber, conditions);
  const roughWaterSensitivity = finiteNumber(resolvedVenueProfile.roughWaterSensitivity, 0.5);
  const tideInstabilityLevel = resolvedVenueProfile.hasTideInfluence ? finiteNumber(conditions.tideLevelRisk, 0) : 0;
  const roughWaterLevel = clamp(
    (conditions.waveLevel * 0.75) +
    (conditions.windLevel * 0.18) +
    (tideInstabilityLevel * 0.18) +
    Math.max(0, roughWaterSensitivity - 0.5) * 0.16,
    0,
    1
  );

  const walls = {
    2: wallScore(metrics[2], config),
    3: wallScore(metrics[3], config),
    4: wallScore(metrics[4], config)
  };
  const blockings = {
    2: blockingScore(metrics[2]),
    3: blockingScore(metrics[3]),
    4: blockingScore(metrics[4])
  };
  const boat3AttackWall = boat3WallAttackScore(metrics[3]);

  const vEscape = venueRate(effectiveVenueBias, stadiumNumber, 1, ["escapeRate", "nigeRate", "winRate"]) ?? optionalRate01(effectiveVenueBias?.headRates?.["1"] ?? effectiveVenueBias?.headRates?.[1]) ?? 0.5;
  const vSashi2 = venueRate(effectiveVenueBias, stadiumNumber, 2, ["sashiRate", "decisionSashiRate"]) ?? optionalRate01(effectiveVenueBias?.headRates?.["2"] ?? effectiveVenueBias?.headRates?.[2]) ?? 0.5;
  const vMakuri3 = venueRate(effectiveVenueBias, stadiumNumber, 3, ["makuriRate", "decisionMakuriRate"]) ?? optionalRate01(effectiveVenueBias?.headRates?.["3"] ?? effectiveVenueBias?.headRates?.[3]) ?? 0.5;
  const vMakuriSashi3 = venueRate(effectiveVenueBias, stadiumNumber, 3, ["makuriSashiRate", "decisionMakuriSashiRate"]) ?? optionalRate01(effectiveVenueBias?.headRates?.["3"] ?? effectiveVenueBias?.headRates?.[3]) ?? 0.5;
  const vMakuriSashi4 = venueRate(effectiveVenueBias, stadiumNumber, 4, ["makuriSashiRate", "decisionMakuriSashiRate"]) ?? optionalRate01(effectiveVenueBias?.headRates?.["4"] ?? effectiveVenueBias?.headRates?.[4]) ?? 0.5;
  const venueFeatureWeights = effectiveVenueBias?.venueFeatureWeights || {};
  const decisionConditionedStats = getDecisionConditionedStats(effectiveVenueBias, stadiumNumber);
  const headDecisionComboStats = getHeadDecisionComboStats(effectiveVenueBias, stadiumNumber);
  const venueMakuriBoat1Second = decisionRate(decisionConditionedStats.makuri, "boat1SecondRate", 0.5);
  const venueMakuriSashiBoat1Second = decisionRate(decisionConditionedStats.makuriSashi, "boat1SecondRate", 0.5);
  const venueSashiBoat1Second = decisionRate(decisionConditionedStats.sashi, "boat1SecondRate", 0.5);
  const venueMakuriInsideResidual = decisionRate(decisionConditionedStats.makuri, "insideResidualRate", 0.5);
  const venueMakuriOutsideLinked = decisionRate(decisionConditionedStats.makuri, "outsideLinkedRate", 0.5);
  const venueMakuriSashiOutsideLinked = decisionRate(decisionConditionedStats.makuriSashi, "outsideLinkedRate", 0.5);
  const venue4MakuriSashi = headDecisionComboStats?.["4"]?.makuriSashi || {};
  const venue4Second1 = comboRate(venue4MakuriSashi, "secondRates", "1", 0.5);
  const venue4Second2 = comboRate(venue4MakuriSashi, "secondRates", "2", 0.5);
  const venue4Second3 = comboRate(venue4MakuriSashi, "secondRates", "3", 0.5);
  const venue4Second5 = comboRate(venue4MakuriSashi, "secondRates", "5", 0.5);
  const venue4Second6 = comboRate(venue4MakuriSashi, "secondRates", "6", 0.5);
  const venue4Exacta1 = comboRate(venue4MakuriSashi, "exactaRates", "4-1", 0.5);
  const venue4Exacta2 = comboRate(venue4MakuriSashi, "exactaRates", "4-2", 0.5);
  const venue4Exacta3 = comboRate(venue4MakuriSashi, "exactaRates", "4-3", 0.5);
  const venue4Exacta5 = comboRate(venue4MakuriSashi, "exactaRates", "4-5", 0.5);
  const venue4Exacta6 = comboRate(venue4MakuriSashi, "exactaRates", "4-6", 0.5);

  const b1 = metrics[1];
  const b2 = metrics[2];
  const b3 = metrics[3];
  const b4 = metrics[4];
  const b5 = metrics[5];
  const b6 = metrics[6];

  const b1Escape = positiveRate(b1.tendency, "escapeRate", 0.42);
  const b1BeatenBySashi = positiveRate(b1.tendency, "beatenBySashiRate", 0.08);
  const b1BeatenByMakuri = positiveRate(b1.tendency, "beatenByMakuriRate", 0.06);
  const b1BeatenByMakuriSashi = positiveRate(b1.tendency, "beatenByMakuriSashiRate", 0.05);
  const weakBoat2Wall = 1 - Math.max(walls[2], blockings[2] * 0.92);
  const weakBoat3Wall = 1 - Math.max(walls[3], blockings[3] * 0.96);
  const boat1WeakFoot = Math.max(0, 0.52 - b1.lapTime) + Math.max(0, 0.52 - b1.turnTime);
  const roughWaterStability1 = roughWaterLevel * Math.max(0, ((b1.lapTime + b1.turnTime) / 2) - 0.55) * 0.12;
  const headwindEscapeDrag = conditions.isHeadwind
    ? conditions.windLevel * (Math.max(0, 0.56 - b1.exST) + Math.max(0, 0.56 - b1.turnTime)) * 0.09
    : 0;
  const tailwindCenterAttack = conditions.isTailwind ? conditions.windLevel * 0.08 : 0;
  const crosswindStability = conditions.isCrosswind ? conditions.windLevel * 0.04 : 0;
  const waveAggressionPenalty = roughWaterLevel * 0.08;
  const conditionAdjustmentLog = [];
  const venueBoat1Residual = residualVenueBlend(
    venueMakuriBoat1Second,
    venueMakuriSashiBoat1Second,
    venueSashiBoat1Second,
    venueMakuriInsideResidual
  );
  const boat1ResidualAfterAttackScore = clamp(
    0.12 +
    b1.lapTime * 0.18 +
    b1.turnTime * 0.2 +
    b1.motorRank * 0.12 +
    b1.motor2Rate * 0.1 +
    b1.reliabilityScore * 0.12 +
    b1Escape * 0.42 +
    venueBoat1Residual * 0.2 +
    roughWaterLevel * Math.max(0, ((b1.lapTime + b1.turnTime) / 2) - 0.55) * 0.08 +
    conditions.isHeadwind * conditions.windLevel * Math.max(0, b1.turnTime - 0.55) * 0.05 -
    b1BeatenByMakuri * 0.28 -
    b1BeatenByMakuriSashi * 0.24 -
    boat1WeakFoot * 0.08
  );

  const escape1 = clamp(
    0.24 +
    b1.exST * 0.13 +
    b1.exTime * 0.09 +
    b1.lapTime * 0.15 +
    b1.turnTime * 0.16 +
    b1.motorRank * 0.1 +
    b1.motor2Rate * 0.08 +
    b1Escape * 0.55 +
    walls[2] * 0.12 +
    (vEscape - 0.5) * 0.12 -
    b1BeatenBySashi * 0.65 -
    b1BeatenByMakuri * 0.65 -
    b1BeatenByMakuriSashi * 0.62 -
    weakBoat2Wall * 0.08 -
    boat1WeakFoot * 0.12 +
    roughWaterStability1 -
    headwindEscapeDrag +
    crosswindStability * Math.max(0, b1.turnTime - 0.5)
  );

  const sashi2Support = supportCount([
    b2.turnTime >= 0.64,
    b2.exST >= 0.62,
    positiveRate(b2.tendency, "sashiRate", 0.1) > 0,
    b1BeatenBySashi > 0,
    weakBoat2Wall < 0.46,
    vSashi2 >= 0.56
  ]);
  const sashi2 = clamp(
    0.13 +
    b2.exST * 0.17 +
    b2.turnTime * 0.24 +
    b2.lapTime * 0.08 +
    positiveRate(b2.tendency, "sashiRate", 0.08) * 0.95 +
    b1BeatenBySashi * 0.72 +
    Math.max(0, 0.55 - b1.turnTime) * 0.14 +
    conditions.isHeadwind * conditions.windLevel * b2.turnTime * 0.05 +
    roughWaterLevel * Math.max(0, b2.turnTime - 0.55) * 0.07 +
    (vSashi2 - 0.5) * 0.16 -
    Math.max(0, b2.lateRate - 0.12) * b2.sampleWeight * 0.18
  );

  const makuri3Support = supportCount([
    b3.exST >= 0.64,
    b3.straightTime >= 0.64,
    positiveRate(b3.tendency, "makuriRate", 0.08) > 0,
    b1BeatenByMakuri > 0,
    weakBoat2Wall >= 0.42,
    vMakuri3 >= 0.56
  ]);
  const makuri3 = clamp((
    0.1 +
    b3.exST * 0.2 +
    b3.straightTime * 0.24 +
    b3.motorRank * 0.12 +
    boat3AttackWall * 0.1 +
    b3.startHistoryScore * 0.08 +
    positiveRate(b3.tendency, "makuriRate", 0.08) * 0.9 +
    b1BeatenByMakuri * 0.7 +
    weakBoat2Wall * 0.22 +
    weakBoat3Wall * 0.08 +
    tailwindCenterAttack * ((b3.exST + b3.straightTime) / 2) -
    roughWaterLevel * Math.max(0, 0.58 - b3.turnTime) * 0.09 -
    (conditions.isCrosswind ? conditions.windLevel * Math.max(0, 0.58 - b3.turnTime) * 0.06 : 0) -
    (vMakuri3 - 0.5) * 0.16 -
    Math.max(0, 0.45 - b3.turnTime) * 0.06
  ) * 0.86);

  const makuriSashi3Support = supportCount([
    b3.turnTime >= 0.62,
    b3.straightTime >= 0.56,
    positiveRate(b3.tendency, "makuriSashiRate", 0.07) > 0,
    b1BeatenByMakuriSashi > 0,
    weakBoat2Wall >= 0.34,
    vMakuriSashi3 >= 0.56
  ]);
  const makuriSashi3 = clamp(
    0.11 +
    b3.exST * 0.13 +
    b3.straightTime * 0.12 +
    b3.turnTime * 0.27 +
    positiveRate(b3.tendency, "makuriSashiRate", 0.07) * 0.92 +
    b1BeatenByMakuriSashi * 0.72 +
    weakBoat2Wall * 0.13 +
    roughWaterLevel * Math.max(0, b3.turnTime - 0.55) * 0.07 +
    (conditions.isCrosswind ? conditions.windLevel * Math.max(0, b3.turnTime - 0.55) * 0.05 : 0) +
    (vMakuriSashi3 - 0.5) * 0.16
  );

  const attack3 = Math.max(makuri3, makuriSashi3);
  const boat3ResidualScore = clamp(
    0.12 +
    b3.lapTime * 0.14 +
    b3.turnTime * 0.18 +
    b3.motorRank * 0.12 +
    b3.motor2Rate * 0.08 +
    b3.reliabilityScore * 0.12 +
    positiveRate(b3.tendency, "makuriRate", 0.08) * 0.18 +
    positiveRate(b3.tendency, "makuriSashiRate", 0.07) * 0.14 +
    venue4Second3 * 0.12 +
    venue4Exacta3 * 0.16 -
    Math.max(0, attack3 - 0.56) * 0.08 +
    roughWaterLevel * Math.max(0, b3.turnTime - 0.55) * 0.06
  );
  const boat2ResidualAfterFlowScore = clamp(
    0.18 +
    b2.turnTime * 0.18 +
    b2.lapTime * 0.1 +
    walls[2] * 0.12 +
    b2.reliabilityScore * 0.12 +
    positiveRate(b2.tendency, "sashiRate", 0.08) * 0.14 +
    venue4Second2 * 0.12 +
    venue4Exacta2 * 0.14
  );
  const boat5LinkedFollowScore = clamp(
    0.14 +
    b5.lapTime * 0.13 +
    b5.straightTime * 0.14 +
    b5.turnTime * 0.08 +
    b5.motorRank * 0.14 +
    b5.motor2Rate * 0.1 +
    b5.reliabilityScore * 0.12 +
    Math.max(venue4Second5, venue4Exacta5) * 0.16 +
    Math.max(venueMakuriOutsideLinked, venueMakuriSashiOutsideLinked) * 0.12
  );
  const boat6LinkedFollowScore = clamp(
    0.12 +
    b6.lapTime * 0.12 +
    b6.straightTime * 0.14 +
    b6.turnTime * 0.08 +
    b6.motorRank * 0.14 +
    b6.motor2Rate * 0.1 +
    b6.reliabilityScore * 0.12 +
    Math.max(venueMakuriOutsideLinked, venueMakuriSashiOutsideLinked) * 0.12
  );
  const boat4ObstructionRiskFromBoat3 = clamp(
    blockings[3] * 0.26 +
    walls[3] * 0.16 +
    b3.reliabilityScore * 0.22 +
    b3.exST * 0.08 +
    b3.straightTime * 0.08 +
    b3.turnTime * 0.08 +
    (b3.courseTrifectaRate ?? 0.5) * 0.16 -
    weakBoat3Wall * 0.16 -
    Math.max(0, attack3 - 0.62) * 0.08
  );
  const b4DirectSupport = supportCount([
    b4.exST >= 0.6,
    b4.straightTime >= 0.62,
    b4.turnTime >= 0.62,
    positiveRate(b4.tendency, "makuriSashiRate", 0.07) > 0,
    attack3 >= 0.5,
    vMakuriSashi4 >= 0.56
  ]);
  const makuriSashi4 = clamp(
    0.06 +
    b4.headFeatureScore * 0.18 +
    b4.exST * 0.1 +
    b4.straightTime * 0.14 +
    b4.turnTime * 0.2 +
    b4.motorRank * 0.14 +
    b4.motor2Rate * 0.1 +
    b4.reliabilityScore * 0.1 +
    positiveRate(b4.tendency, "makuriSashiRate", 0.07) * 0.6 +
    weakBoat3Wall * 0.08 +
    (vMakuriSashi4 - 0.5) * 0.12 -
    boat4ObstructionRiskFromBoat3 * 0.1
  );
  const secondWave4 = clamp(
    0.08 +
    b4.fourBeneficiaryFeatureScore * 0.16 +
    b4.exST * 0.08 +
    b4.straightTime * 0.16 * (venueFeatureWeights.straight ?? 1) +
    b4.turnTime * 0.2 * (venueFeatureWeights.turn ?? 1) +
    b4.motorRank * 0.14 +
    b4.motor2Rate * 0.08 +
    b4.reliabilityScore * 0.12 +
    positiveRate(b4.tendency, "makuriSashiRate", 0.07) * 0.78 +
    attack3 * 0.28 +
    (1 - boat1ResidualAfterAttackScore) * 0.16 +
    weakBoat3Wall * 0.08 +
    b1BeatenByMakuriSashi * 0.42 +
    roughWaterLevel * Math.max(0, b4.turnTime - 0.55) * 0.1 +
    conditions.isTailwind * conditions.windLevel * Math.max(0, b4.straightTime - 0.55) * 0.04 +
    (vMakuriSashi4 - 0.5) * 0.15 * (venueFeatureWeights.fourBeneficiary ?? 1) -
    boat4ObstructionRiskFromBoat3 * 0.16
  );

  const outside5 = clamp(0.1 + b5.lapTime * 0.17 + b5.straightTime * 0.16 + b5.turnTime * 0.11 + b5.motorRank * 0.12 + b5.motor2Rate * 0.08 + Math.max(sashi2, attack3, secondWave4) * 0.18 - waveAggressionPenalty * 0.35);
  const outside6 = clamp(0.08 + b6.lapTime * 0.15 + b6.straightTime * 0.15 + b6.turnTime * 0.1 + b6.motorRank * 0.12 + b6.motor2Rate * 0.08 + Math.max(sashi2, attack3, secondWave4) * 0.15 - waveAggressionPenalty * 0.42);
  const outsideFollow = Math.max(outside5, outside6);
  if (conditions.windLevel > 0) {
    conditionAdjustmentLog.push({
      type: "wind",
      level: conditions.windLevel >= 1 ? "strong" : "medium",
      windSpeed: conditions.windSpeed,
      windDirection: conditions.windDirection,
      note: conditions.isTailwind
        ? "追い風気味のためSTと直線が良いセンター攻めを軽く加点"
        : conditions.isHeadwind
          ? "向かい風気味のため差し・ターン安定を軽く重視"
          : "強めの風のためターン安定と信頼度を軽く調整"
    });
  }
  if (conditions.waveLevel > 0) {
    conditionAdjustmentLog.push({
      type: "wave",
      level: conditions.waveLevel >= 1 ? "strong" : "medium",
      waveHeight: conditions.waveHeight,
      note: "波高あり。外の一撃頭を少し抑え、周回・まわり足の安定を重視"
    });
  }

  if (tideInstabilityLevel > 0 || resolvedVenueProfile.hasTideInfluence) {
    conditionAdjustmentLog.push({
      type: "tide",
      level: tideInstabilityLevel >= 0.7 ? "strong" : tideInstabilityLevel > 0 ? "medium" : "reference",
      tideLevel: conditions.tideLevel,
      tideDirection: conditions.tideDirection,
      tidePhase: conditions.tidePhase,
      waterType: conditions.waterType ?? resolvedVenueProfile.waterType,
      note: "潮の影響がある水面では、外の一撃よりも周回・まわり足と残り足を軽く重視"
    });
  }

  const scoreByBoat = {};
  const headScore1 = escape1;
  const headScore2 = sashi2 * (sashi2Support >= 2 ? 0.95 : 0.62);
  const headScore3 = Math.max(makuri3 * (makuri3Support >= 2 ? 0.94 : 0.62), makuriSashi3 * (makuriSashi3Support >= 2 ? 0.9 : 0.62));
  const headScore4Raw = Math.max(makuriSashi4 * 0.86, secondWave4 * (b4DirectSupport >= 3 ? 0.84 : b4DirectSupport >= 2 ? 0.66 : 0.44));
  const headScore4 = b4DirectSupport >= 2 ? headScore4Raw : Math.min(headScore4Raw, 0.42);
  const outsideHeadCap = 0.31;
  const headScore5 = Math.min(outside5 * 0.35, outsideHeadCap);
  const headScore6 = Math.min(outside6 * 0.32, outsideHeadCap - 0.02);

  const beneficiary4 = clamp(
    0.08 +
    attack3 * 0.3 +
    b4.turnTime * 0.17 +
    b4.straightTime * 0.13 +
    b4.motorRank * 0.11 +
    b4.reliabilityScore * 0.08 +
    positiveRate(b4.tendency, "makuriSashiRate", 0.07) * 0.38 -
    boat4ObstructionRiskFromBoat3 * 0.18
  );
  const beneficiary5 = outside5;
  const beneficiary6 = outside6;
  const attackerScores = {
    1: escape1,
    2: sashi2,
    3: attack3,
    4: makuriSashi4,
    5: headScore5,
    6: headScore6
  };
  const beneficiaryScores = {
    1: clamp(escape1 * 0.42 + b1.reliabilityScore * 0.18 + b1.lapTime * 0.18 + b1.turnTime * 0.18),
    2: clamp(sashi2 * 0.36 + walls[2] * 0.24 + b2.reliabilityScore * 0.16 + b2.turnTime * 0.14),
    3: clamp(makuriSashi3 * 0.3 + b3.reliabilityScore * 0.18 + b3.turnTime * 0.18 + b3.straightTime * 0.12),
    4: beneficiary4,
    5: beneficiary5,
    6: beneficiary6
  };
  const residualScores = {
    1: boat1ResidualAfterAttackScore,
    2: boat2ResidualAfterFlowScore,
    3: boat3ResidualScore,
    4: clamp(b4.residualFeatureScore * 0.5 + b4.reliabilityScore * 0.16 + b4.turnTime * 0.18 + b4.motorRank * 0.16 + b4.motor2Rate * 0.1 + secondWave4 * 0.12),
    5: boat5LinkedFollowScore,
    6: boat6LinkedFollowScore
  };
  const insideCollapseScore = clamp(1 - boat1ResidualAfterAttackScore + Math.max(0, attack3 - 0.56) * 0.25 + weakBoat2Wall * 0.12);
  const fourHeadPartnerDecision = {
    prefer41: boat1ResidualAfterAttackScore >= 0.58 || b1.reliabilityScore >= 0.62 || venue4Exacta1 >= 0.58,
    prefer45: insideCollapseScore >= 0.54 && boat5LinkedFollowScore >= 0.52 && b5.reliabilityScore >= 0.48,
    prefer46: insideCollapseScore >= 0.58 && boat6LinkedFollowScore >= 0.52 && b6.reliabilityScore >= 0.48,
    allow43: boat3ResidualScore >= 0.62 && insideCollapseScore >= 0.5 && venue4Exacta3 >= 0.46 || boat4ObstructionRiskFromBoat3 >= 0.62 && boat3ResidualScore >= 0.58,
    insideCollapseScore: roundScore(insideCollapseScore),
    boat4ObstructionRiskFromBoat3: roundScore(boat4ObstructionRiskFromBoat3),
    why: []
  };
  if (fourHeadPartnerDecision.prefer41) fourHeadPartnerDecision.why.push("1 residual/motor/lap-turn supports 4-1");
  if (fourHeadPartnerDecision.prefer45) fourHeadPartnerDecision.why.push("inside collapse + boat5 outside follow supports 4-5");
  if (fourHeadPartnerDecision.prefer46) fourHeadPartnerDecision.why.push("inside collapse + boat6 outside follow supports 4-6");
  if (!fourHeadPartnerDecision.allow43) fourHeadPartnerDecision.why.push("4-3 requires boat3 residual; default is demoted");

  const rows = [
    {
      boat: 1,
      headScore: headScore1,
      attackerScore: attackerScores[1],
      secondScore: clamp(0.18 + escape1 * 0.2 + sashi2 * 0.14 + attack3 * boat1ResidualAfterAttackScore * 0.24 + b1.reliabilityScore * 0.14 + b1.lapTime * 0.12 + b1.turnTime * 0.12 + venue4Exacta1 * 0.06),
      thirdScore: clamp(0.18 + escape1 * 0.14 + Math.max(sashi2, attack3, secondWave4) * boat1ResidualAfterAttackScore * 0.2 + b1.reliabilityScore * 0.16 + b1.lapTime * 0.11 + b1.turnTime * 0.11 + venue4Second1 * 0.05),
      scenarioTriggerScore: escape1,
      beneficiaryScore: beneficiaryScores[1],
      residualScore: residualScores[1],
      supportFactors: supportCount([b1.lapTime >= 0.62, b1.turnTime >= 0.62, b1Escape > 0, walls[2] >= 0.55]),
      canBeHead: headScore1 >= 0.42,
      headReasons: [
        b1.lapTime >= 0.62 && b1.turnTime >= 0.62 ? "周回とまわり足が上位" : null,
        b1Escape > 0 ? "逃げ率の支えあり" : null
      ].filter(Boolean),
      partnerReasons: ["1残し評価"],
      dangerReason: "1着信頼が割れた場合の残し"
    },
    {
      boat: 2,
      headScore: headScore2,
      attackerScore: attackerScores[2],
      secondScore: clamp(0.2 + sashi2 * 0.26 + boat2ResidualAfterFlowScore * 0.1 + b2.reliabilityScore * 0.14 + b2.turnTime * 0.18 + walls[2] * 0.12 + b2.lapTime * 0.08),
      thirdScore: clamp(0.2 + sashi2 * 0.16 + boat2ResidualAfterFlowScore * 0.09 + b2.reliabilityScore * 0.16 + b2.turnTime * 0.14 + b2.lapTime * 0.1 + walls[2] * 0.07),
      scenarioTriggerScore: sashi2,
      beneficiaryScore: beneficiaryScores[2],
      residualScore: residualScores[2],
      supportFactors: sashi2Support,
      canBeHead: headScore2 >= 0.48 && sashi2Support >= 2,
      headReasons: [
        b2.turnTime >= 0.64 ? "まわり足が良く差し向き" : null,
        positiveRate(b2.tendency, "sashiRate", 0.08) > 0 ? "差し率の支えあり" : null
      ].filter(Boolean),
      partnerReasons: ["2差し・2着残り"],
      dangerReason: "差し筋はあるが頭支持が不足"
    },
    {
      boat: 3,
      headScore: headScore3,
      attackerScore: attackerScores[3],
      secondScore: clamp(0.18 + attack3 * 0.18 + boat3ResidualScore * 0.22 + b3.reliabilityScore * 0.16 + b3.straightTime * 0.1 + b3.turnTime * 0.13),
      thirdScore: clamp(0.19 + attack3 * 0.16 + boat3ResidualScore * 0.2 + b3.reliabilityScore * 0.18 + b3.straightTime * 0.1 + b3.turnTime * 0.13),
      scenarioTriggerScore: attack3,
      beneficiaryScore: beneficiaryScores[3],
      residualScore: residualScores[3],
      supportFactors: Math.max(makuri3Support, makuriSashi3Support),
      canBeHead: headScore3 >= 0.5 && Math.max(makuri3Support, makuriSashi3Support) >= 2,
      headReasons: [
        b3.exST >= 0.64 && b3.straightTime >= 0.64 ? "STと直線が良い" : null,
        b3.turnTime >= 0.62 ? "まくり差しの足もある" : null
      ].filter(Boolean),
      partnerReasons: ["センター攻め後の2・3着"],
      dangerReason: "攻め足はあるが頭までは条件不足"
    },
    {
      boat: 4,
      headScore: headScore4,
      attackerScore: attackerScores[4],
      secondScore: clamp(0.22 + secondWave4 * 0.32 + beneficiary4 * 0.18 + b4.reliabilityScore * 0.14 + b4.turnTime * 0.12 + b4.straightTime * 0.1),
      thirdScore: clamp(0.24 + secondWave4 * 0.28 + beneficiary4 * 0.18 + b4.reliabilityScore * 0.16 + b4.turnTime * 0.12 + b4.straightTime * 0.1),
      scenarioTriggerScore: secondWave4,
      beneficiaryScore: beneficiaryScores[4],
      residualScore: residualScores[4],
      supportFactors: b4DirectSupport,
      canBeHead: headScore4 >= 0.5 && b4DirectSupport >= 2 && (
        attack3 >= 0.5 && Math.max(makuri3Support, makuriSashi3Support) >= 2 ||
        positiveRate(b4.tendency, "makuriSashiRate", 0.07) > 0
      ),
      headReasons: [
        attack3 >= 0.5 ? "3攻め後の差し場あり" : null,
        b4.turnTime >= 0.62 ? "まわり足上位" : null,
        b4.straightTime >= 0.62 ? "直線も支え" : null
      ].filter(Boolean),
      partnerReasons: ["3攻め後のまくり差し・相手筆頭"],
      dangerReason: "展開の恩恵は大きいが頭条件は限定"
    },
    {
      boat: 5,
      headScore: headScore5,
      attackerScore: attackerScores[5],
      secondScore: clamp(0.13 + outside5 * 0.2 + boat5LinkedFollowScore * 0.14 + b5.reliabilityScore * 0.12 + b5.lapTime * 0.1 + b5.straightTime * 0.1),
      thirdScore: clamp(0.18 + outside5 * 0.3 + boat5LinkedFollowScore * 0.16 + b5.reliabilityScore * 0.18 + b5.lapTime * 0.12 + b5.straightTime * 0.12),
      scenarioTriggerScore: outside5,
      beneficiaryScore: beneficiaryScores[5],
      residualScore: residualScores[5],
      supportFactors: supportCount([b5.lapTime >= 0.62, b5.straightTime >= 0.62, b5.turnTime >= 0.62]),
      canBeHead: false,
      headReasons: [],
      partnerReasons: ["頭より2・3着の展開突き"],
      dangerReason: "足色は穴相手向き。頭へは過大評価しない"
    },
    {
      boat: 6,
      headScore: headScore6,
      attackerScore: attackerScores[6],
      secondScore: clamp(0.12 + outside6 * 0.22 + b6.reliabilityScore * 0.1 + b6.lapTime * 0.1 + b6.straightTime * 0.1),
      thirdScore: clamp(0.18 + outside6 * 0.34 + b6.reliabilityScore * 0.18 + b6.lapTime * 0.12 + b6.straightTime * 0.12),
      scenarioTriggerScore: outside6,
      beneficiaryScore: beneficiaryScores[6],
      residualScore: residualScores[6],
      supportFactors: supportCount([b6.lapTime >= 0.62, b6.straightTime >= 0.62, b6.turnTime >= 0.62]),
      canBeHead: false,
      headReasons: [],
      partnerReasons: ["頭より3着穴の展開突き"],
      dangerReason: "外枠は頭ではなく相手穴評価"
    }
  ];

  for (const row of rows) {
    const metric = metrics[row.boat] || {};
    Object.assign(row, {
      reliabilityScore: metric.reliabilityScore,
      reliabilityProfile: metric.reliabilityProfile,
      wallScore: row.boat === 2 || row.boat === 3 || row.boat === 4 ? walls[row.boat] : null,
      blockingScore: row.boat === 2 || row.boat === 3 || row.boat === 4 ? blockings[row.boat] : null,
      courseWinRate: metric.courseWinRate,
      courseQuinellaRate: metric.courseQuinellaRate,
      courseTrifectaRate: metric.courseTrifectaRate,
      recentQuinellaRate: metric.recentQuinellaRate,
      recentTrifectaRate: metric.recentTrifectaRate,
      localVenueQuinellaRate: metric.localVenueQuinellaRate,
      localVenueTrifectaRate: metric.localVenueTrifectaRate
    });
    scoreByBoat[row.boat] = row;
  }

  const scenarios = [
    scenarioRow({
      id: "escape_1",
      attacker: 1,
      score: escape1,
      beneficiaries: [1, 2, 3],
      partners: [2, 3, 4],
      patterns: [[1, 2, "flow"], [1, 3, "flow"], [1, 4, "flow"]],
      reasons: [
        b1.lapTime >= 0.62 && b1.turnTime >= 0.62 ? "1号艇は周回とまわり足が上位で、イン残し評価を上げます。" : null,
        b1BeatenBySashi > 0 || b1BeatenByMakuri > 0 ? "1号艇の被差し・被まくり傾向は逃げ信頼度を抑えます。" : null
      ]
    }),
    scenarioRow({
      id: "sashi_2",
      attacker: 2,
      score: sashi2,
      beneficiaries: [1, 2, 3],
      partners: [1, 3, 4],
      patterns: [[2, 1, "flow"], [2, 3, "flow"], [2, 4, "flow"]],
      reasons: [
        b2.turnTime >= 0.64 ? "2号艇はまわり足が良く、差しと2着残りを上げます。" : null,
        positiveRate(b2.tendency, "sashiRate", 0.08) > 0 ? "2号艇の差し率がサンプル重み込みでプラス。" : null,
        b1BeatenBySashi > 0 ? "1号艇の差され率があり、2差し警戒。" : null
      ]
    }),
    scenarioRow({
      id: "makuri_3",
      attacker: 3,
      score: makuri3,
      beneficiaries: [3, 4, 5],
      partners: [1, 4, 5],
      patterns: [[3, 1, "flow"], [3, 4, "flow"]],
      reasons: [
        b3.exST >= 0.64 && b3.straightTime >= 0.64 ? "3号艇はSTと直線が良く、センター攻めの可能性があります。" : null,
        weakBoat2Wall >= 0.42 ? "2号艇の壁が弱く、3号艇の攻め筋が広がります。" : null,
        positiveRate(b3.tendency, "makuriRate", 0.08) > 0 ? "3号艇のまくり率を軽く加点。" : null
      ]
    }),
    scenarioRow({
      id: "makuri_sashi_3",
      attacker: 3,
      score: makuriSashi3,
      beneficiaries: [1, 3, 4],
      partners: [1, 4, 5],
      patterns: [[3, 1, "flow"], [3, 4, "flow"], [1, 3, "flow"]],
      reasons: [
        b3.turnTime >= 0.62 ? "3号艇はまわり足が良く、まくり差しと2・3着残りを上げます。" : null,
        b1BeatenByMakuriSashi > 0 ? "1号艇のまくり差され率を警戒。" : null
      ]
    }),
    scenarioRow({
      id: "second_wave_4",
      attacker: 4,
      score: secondWave4,
      beneficiaries: [4, 1, 3],
      partners: [1, 3, 5],
      patterns: [[4, 1, "flow"], [4, 3, "flow"], [3, 4, "flow"], [1, 4, "flow"]],
      reasons: [
        attack3 >= 0.5 ? "3号艇が攻めると4号艇に差し場が生まれます。" : null,
        b4.turnTime >= 0.62 ? "4号艇はまわり足が良く、3が攻めた後のまくり差し展開に注意。" : null,
        b4.straightTime >= 0.62 ? "4号艇の直線も展開突きの支え。" : null
      ]
    }),
    scenarioRow({
      id: "outside_follow_5_6",
      attacker: null,
      score: outsideFollow,
      beneficiaries: [5, 6],
      partners: [5, 6],
      patterns: [[1, 5, "flow"], [1, 6, "flow"], [3, 5, "flow"], [4, 5, "flow"]],
      reasons: [
        outside5 >= 0.58 ? "5号艇は周回・直線を2・3着穴で評価。" : null,
        outside6 >= 0.56 ? "6号艇は頭より3着穴で評価。" : null,
        "5・6号艇は頭候補に過大評価せず、相手穴中心です。"
      ]
    })
  ].sort((a, b) => b.score - a.score);

  const fourBeneficiaryPatterns = [];
  if (fourHeadPartnerDecision.prefer41) fourBeneficiaryPatterns.push([4, 1, "flow"]);
  if (boat2ResidualAfterFlowScore >= 0.52 || venue4Exacta2 >= 0.54) fourBeneficiaryPatterns.push([4, 2, "flow"]);
  if (fourHeadPartnerDecision.prefer45) fourBeneficiaryPatterns.push([4, 5, "flow"]);
  if (fourHeadPartnerDecision.prefer46) fourBeneficiaryPatterns.push([4, 6, "flow"]);
  if (fourHeadPartnerDecision.allow43) fourBeneficiaryPatterns.push([4, 3, "flow"]);
  if (!fourBeneficiaryPatterns.length) {
    fourBeneficiaryPatterns.push(insideCollapseScore >= 0.56 ? [4, 5, "flow"] : [4, 1, "flow"]);
  }
  const scenarioFamilies = [
    scenarioRow({
      id: "escape_1",
      attacker: 1,
      score: escape1,
      beneficiaries: [1, 2, 3],
      partners: [2, 3, 4],
      patterns: [[1, 2, "flow"], [1, 3, "flow"], [1, 4, "flow"]],
      reasons: ["1 direct escape family"]
    }),
    scenarioRow({
      id: "sashi_2",
      attacker: 2,
      score: sashi2,
      beneficiaries: [1, 2, 3],
      partners: [1, 3, 4],
      patterns: [[2, 1, "flow"], [2, 3, "flow"], [2, 4, "flow"]],
      reasons: ["2 sashi direct head family"]
    }),
    scenarioRow({
      id: "makuri_3",
      attacker: 3,
      score: makuri3,
      beneficiaries: [3, 4, 5],
      partners: [1, 4, 5],
      patterns: [[3, 1, "flow"], [3, 4, "flow"], [3, 5, "flow"]],
      reasons: ["3 attacks and wins directly"]
    }),
    scenarioRow({
      id: "makuriSashi_4",
      attacker: 4,
      score: makuriSashi4,
      beneficiaries: [1, 2, 5],
      partners: [1, 2, 5],
      patterns: [[4, 1, "flow"], [4, 2, "flow"], [4, 5, "flow"]],
      reasons: ["4 attacks directly by makuri-sashi"]
    }),
    scenarioRow({
      id: "four_beneficiary",
      attacker: 3,
      score: secondWave4,
      triggerScore: attack3,
      beneficiaries: [4, 5, 6],
      partners: fourBeneficiaryPatterns.map((pattern) => pattern[1]),
      patterns: fourBeneficiaryPatterns,
      reasons: fourHeadPartnerDecision.why
    }),
    scenarioRow({
      id: "outer_follow_5",
      attacker: null,
      score: outside5,
      beneficiaries: [5],
      partners: [1, 3, 4],
      patterns: [[3, 5, "flow"], [4, 5, "flow"], [1, 5, "flow"]],
      reasons: ["5 follows when inside shape collapses"]
    }),
    scenarioRow({
      id: "outer_follow_6",
      attacker: null,
      score: outside6,
      beneficiaries: [6],
      partners: [1, 3, 4],
      patterns: [[3, 6, "flow"], [4, 6, "flow"], [1, 6, "flow"]],
      reasons: ["6 follows only as outside residual"]
    })
  ].sort((a, b) => b.score - a.score);
  const mainScenarioGroup = scenarioFamilies.find((row) => ["escape_1", "sashi_2", "makuri_3", "makuriSashi_4"].includes(row.id)) || scenarioFamilies[0] || null;
  const derivedScenarioGroup = scenarioFamilies.find((row) => row.id === "four_beneficiary" && row.score >= 45) ||
    scenarioFamilies.find((row) => row.id === "outer_follow_5" && row.score >= 50) ||
    scenarioFamilies.find((row) => row.id === "outer_follow_6" && row.score >= 50) ||
    null;

  const headCandidates = buildHeadCandidateRows(scoreByBoat);
  const partnerCandidates = buildPartnerRows(scoreByBoat);
  const dangerousButNotHead = buildDangerRows(scoreByBoat);
  const topScenario = scenarios[0];
  const secondaryScenario = scenarios.find((row) => row.id !== topScenario?.id) || null;
  const upsetScenario = scenarios.find((row) => row.id !== "escape_1" && row.score >= 52) || null;
  const lowReliabilityGoodExhibition = Object.values(metrics).find((metric) =>
    metric.exST >= 0.66 &&
    (metric.exTime >= 0.62 || metric.straightTime >= 0.64) &&
    metric.reliabilityScore < 0.44 &&
    metric.motorRank < 0.55
  );
  const reliabilityExplanations = [
    boat4ObstructionRiskFromBoat3 >= 0.62
      ? "3号艇のコース別3連対率・壁評価が高く、4号艇の展開拾いは3を越え切れるかが条件です。"
      : null,
    lowReliabilityGoodExhibition
      ? `${lowReliabilityGoodExhibition.boat}号艇は展示気配は良いですが、コース別連対・3連対の信頼度が低いため頭評価は控えめです。`
      : null
  ].filter(Boolean);
  const explanations = [
    topScenario ? `本線展開は「${topScenario.label}」。${topScenario.reasons[0] || "展示足と進入のバランスから評価しています。"}` : null,
    secondaryScenario ? `対抗展開は「${secondaryScenario.label}」。${secondaryScenario.reasons[0] || "相手候補の連動を見ます。"}` : null,
    upsetScenario && upsetScenario.id !== topScenario?.id ? `穴展開は「${upsetScenario.label}」。${upsetScenario.reasons[0] || "隊形が崩れた時だけ押さえます。"}` : null,
    dangerousButNotHead.length > 0 ? `危険だが頭ではない艇: ${dangerousButNotHead.map((row) => `${row.boat}号艇`).join("、")}。相手穴中心で扱います。` : null,
    ...reliabilityExplanations,
    ...conditionAdjustmentLog.map((row) => row.note),
    !tendencyAvailable ? "戦法データ不足のため、展示ST・展示タイム・周回・直線・まわり足を中心に展開評価しています。" : null
  ].filter(Boolean);

  const wallScores = [2, 3, 4].map((boat) => ({
    boat,
    wallScore: roundScore(walls[boat]),
    blockingScore: roundScore(blockings[boat]),
    reliabilityScore: roundScore(metrics[boat].reliabilityScore),
    wallAttackScore: boat === 3 ? roundScore(boat3AttackWall) : null,
    sampleStatus: metrics[boat].sampleStatus,
    lateStartRate: metrics[boat].lateRate,
    earlyStartRate: metrics[boat].earlyRate,
    motorRank: roundScore(metrics[boat].motorRank),
    hasFRisk: metrics[boat].hasFRisk
  }));
  const headPartnerSplit = Object.values(scoreByBoat)
    .sort((a, b) => a.boat - b.boat)
    .map((row) => ({
      boat: row.boat,
      headScore: roundScore(row.headScore),
      attackerScore: roundScore(row.attackerScore),
      secondScore: roundScore(row.secondScore),
      thirdScore: roundScore(row.thirdScore),
      scenarioTriggerScore: roundScore(row.scenarioTriggerScore),
      beneficiaryScore: roundScore(row.beneficiaryScore),
      residualScore: roundScore(row.residualScore),
      reliabilityScore: roundScore(row.reliabilityScore),
      wallScore: row.wallScore === null ? null : roundScore(row.wallScore),
      blockingScore: row.blockingScore === null ? null : roundScore(row.blockingScore),
      courseQuinellaRate: row.courseQuinellaRate,
      courseTrifectaRate: row.courseTrifectaRate,
      supportFactors: row.supportFactors,
      canBeHead: row.canBeHead,
      dangerReason: row.dangerReason
    }));

  const ticketAdjustmentLog = [];
  if (escape1 < 0.48) {
    ticketAdjustmentLog.push({
      action: "demote",
      target: "1-head-heavy",
      reason: "1号艇の逃げ信頼度が低く、2/3/4頭の警戒を上げます。"
    });
  }
  for (const row of dangerousButNotHead) {
    ticketAdjustmentLog.push({
      action: "demote",
      target: `${row.boat}-head`,
      reason: row.reason
    });
  }
  const makuriSampleWeight = decisionSampleWeight(decisionConditionedStats.makuri?.sampleCount);
  const makuriSashiSampleWeight = decisionSampleWeight(decisionConditionedStats.makuriSashi?.sampleCount);
  const sashiSampleWeight = decisionSampleWeight(decisionConditionedStats.sashi?.sampleCount);
  const boat4MakuriSashiSampleWeight = decisionSampleWeight(venue4MakuriSashi?.sampleCount);
  const decisionResidualScores = {
    boat1ResidualAfterAttackScore: roundScore(boat1ResidualAfterAttackScore),
    boat3ResidualScore: roundScore(boat3ResidualScore),
    boat2ResidualAfterFlowScore: roundScore(boat2ResidualAfterFlowScore),
    boat5LinkedFollowScore: roundScore(boat5LinkedFollowScore),
    boat6LinkedFollowScore: roundScore(boat6LinkedFollowScore),
    insideCollapseScore: roundScore(insideCollapseScore),
    boat4ObstructionRiskFromBoat3: roundScore(boat4ObstructionRiskFromBoat3),
    fourHeadPartnerDecision,
    venueMakuriBoat1SecondRate: roundScore(venueMakuriBoat1Second),
    venueMakuriSashiBoat1SecondRate: roundScore(venueMakuriSashiBoat1Second),
    venueSashiBoat1SecondRate: roundScore(venueSashiBoat1Second),
    venue4SecondRates: {
      "1": roundScore(venue4Second1),
      "2": roundScore(venue4Second2),
      "3": roundScore(venue4Second3),
      "5": roundScore(venue4Second5),
      "6": roundScore(venue4Second6)
    },
    venue4ExactaRates: {
      "4-1": roundScore(venue4Exacta1),
      "4-2": roundScore(venue4Exacta2),
      "4-3": roundScore(venue4Exacta3),
      "4-5": roundScore(venue4Exacta5),
      "4-6": roundScore(venue4Exacta6)
    },
    sampleWeights: {
      makuri: round(makuriSampleWeight, 3),
      makuriSashi: round(makuriSashiSampleWeight, 3),
      sashi: round(sashiSampleWeight, 3),
      boat4MakuriSashi: round(boat4MakuriSashiSampleWeight, 3)
    }
  };
  if (makuriSampleWeight >= 0.35 && venueMakuriBoat1Second < 0.44 && boat1ResidualAfterAttackScore < 0.55) {
    ticketAdjustmentLog.push({
      action: "demote",
      target: "3-1-flow",
      scenarioId: "makuri_3",
      reason: "まくり決着時の1号艇2着残りが低く、1の残留足も強くないため3-1を本線から下げます"
    });
  } else if (makuriSampleWeight >= 0.35 && (venueMakuriBoat1Second >= 0.58 || boat1ResidualAfterAttackScore >= 0.64)) {
    ticketAdjustmentLog.push({
      action: "keep",
      target: "3-1-flow",
      scenarioId: "makuri_3",
      reason: "まくり決着でも1号艇が2着に残る根拠があるため3-1を維持します"
    });
  }
  if (boat4MakuriSashiSampleWeight >= 0.35 && venue4Exacta3 < 0.42 && boat3ResidualScore < 0.54) {
    ticketAdjustmentLog.push({
      action: "demote",
      target: "4-3-flow",
      scenarioId: "four_beneficiary",
      reason: "4頭時の4-3が薄く、3号艇は攻めの起点後に残る評価が弱いため4-3を下げます"
    });
  }
  if (boat4MakuriSashiSampleWeight >= 0.35 && (venue4Exacta1 >= 0.56 || boat1ResidualAfterAttackScore >= 0.62)) {
    ticketAdjustmentLog.push({
      action: "promote",
      target: "4-1-flow",
      scenarioId: "four_beneficiary",
      reason: "4頭時に1号艇残りの根拠があるため4-1を相手上位に評価します"
    });
  } else if (insideCollapseScore >= 0.56 && boat1ResidualAfterAttackScore < 0.52) {
    ticketAdjustmentLog.push({
      action: "demote",
      target: "4-1-flow",
      scenarioId: "four_beneficiary",
      reason: "4頭時でも1号艇の残留評価が弱く、4-1より外連動を優先します"
    });
  }
  if (boat4MakuriSashiSampleWeight >= 0.35 && venue4Exacta5 >= 0.56 && boat5LinkedFollowScore >= 0.52) {
    ticketAdjustmentLog.push({
      action: "promote",
      target: "4-5-flow",
      scenarioId: "four_beneficiary",
      reason: "4頭時の外連動と5号艇の追走評価があるため4-5系を少し評価します"
    });
  }
  if (boat4MakuriSashiSampleWeight >= 0.35 && venue4Exacta6 >= 0.54 && boat6LinkedFollowScore >= 0.52) {
    ticketAdjustmentLog.push({
      action: "promote",
      target: "4-6-flow",
      scenarioId: "four_beneficiary",
      reason: "内側が崩れる4頭展開で6号艇の追走評価があり4-6系も候補に入れます"
    });
  }
  if (
    makuriSampleWeight > 0 && makuriSampleWeight < 0.35 ||
    makuriSashiSampleWeight > 0 && makuriSashiSampleWeight < 0.35 ||
    sashiSampleWeight > 0 && sashiSampleWeight < 0.35 ||
    boat4MakuriSashiSampleWeight > 0 && boat4MakuriSashiSampleWeight < 0.35
  ) {
    ticketAdjustmentLog.push({
      action: "reference_only",
      target: "decision-conditioned venue bias",
      reason: "決まり手別の会場サンプルが少ないため、出目補正は参考程度に抑えます"
    });
  }
  const decisionBiasExplanations = ticketAdjustmentLog
    .filter((row) =>
      ["3-1-flow", "4-3-flow", "4-1-flow", "4-5-flow", "4-6-flow", "decision-conditioned venue bias"].includes(row.target)
    )
    .map((row) => row.reason)
    .filter(Boolean)
    .slice(0, 4);

  return {
    available: safeArray(entries).length > 0,
    dataWarnings,
    scenarios,
    scenarioFamilies,
    mainScenarioGroup,
    derivedScenarioGroup,
    mainScenario: topScenario || null,
    secondaryScenario,
    upsetScenario,
    wallScores,
    headCandidates,
    partnerCandidates,
    dangerousButNotHead,
    headPartnerSplit,
    scoreByBoat: Object.fromEntries(
      Object.entries(scoreByBoat).map(([boat, row]) => [boat, {
        boat: row.boat,
        headScore: row.headScore,
        attackerScore: row.attackerScore,
        secondScore: row.secondScore,
        thirdScore: row.thirdScore,
        scenarioTriggerScore: row.scenarioTriggerScore,
        beneficiaryScore: row.beneficiaryScore,
        residualScore: row.residualScore,
        reliabilityScore: row.reliabilityScore,
        wallScore: row.wallScore,
        blockingScore: row.blockingScore,
        courseWinRate: row.courseWinRate,
        courseQuinellaRate: row.courseQuinellaRate,
        courseTrifectaRate: row.courseTrifectaRate,
        recentQuinellaRate: row.recentQuinellaRate,
        recentTrifectaRate: row.recentTrifectaRate,
        localVenueQuinellaRate: row.localVenueQuinellaRate,
        localVenueTrifectaRate: row.localVenueTrifectaRate,
        reliabilityProfile: row.reliabilityProfile,
        supportFactors: row.supportFactors,
        canBeHead: row.canBeHead,
        dangerReason: row.dangerReason
      }])
    ),
    ticketAdjustmentLog,
    fourHeadPartnerDecision,
    venueBiasTable: {
      headRates: effectiveVenueBias?.headRates || null,
      scenarioFollowerBias: effectiveVenueBias?.scenarioFollowerBias || null,
      head4SecondBias: effectiveVenueBias?.head4SecondBias || null,
      head3SecondBias: effectiveVenueBias?.head3SecondBias || null,
      head2SecondBias: effectiveVenueBias?.head2SecondBias || null,
      source: effectiveVenueBias?.source || null
    },
    decisionConditionedStats,
    headDecisionComboStats,
    decisionResidualScores,
    venueProfile: resolvedVenueProfile,
    normalizedConditions: {
      ...conditions,
      roughWaterLevel: round(roughWaterLevel, 3)
    },
    conditionAdjustmentLog,
    explanations: [...explanations, ...decisionBiasExplanations],
    quality: {
      tendencyAvailable,
      venueAvailable,
      conditionAvailable: conditions.available,
      confidenceAdjustment: dataWarnings.length * -1.5 - (conditions.windLevel > 0 || conditions.waveLevel > 0 || tideInstabilityLevel > 0 ? 0.8 : 0)
    }
  };
}

export function applyRaceFlowScenarioAdjustments(entries = [], model = {}) {
  const byBoat = model?.scoreByBoat || {};
  return safeArray(entries).map((row) => {
    const boat = boatNumber(row);
    const scores = byBoat[String(boat)] || byBoat[boat] || {};
    const head = finiteNumber(scores.headScore, 0.5);
    const second = finiteNumber(scores.secondScore, 0.5);
    const third = finiteNumber(scores.thirdScore, 0.5);
    const trigger = finiteNumber(scores.scenarioTriggerScore, 0.5);
    const beneficiary = finiteNumber(scores.beneficiaryScore, 0.5);
    const outsideDampening = boat >= 5 ? 0.35 : 1;
    let adjustment = (
      (head - 0.5) * 0.16 * outsideDampening +
      (trigger - 0.5) * 0.045 * outsideDampening +
      (beneficiary - 0.5) * 0.035 +
      (Math.max(second, third) - 0.5) * 0.02
    );
    if (boat >= 5) adjustment = Math.min(adjustment, 0.035);
    adjustment = clamp(adjustment, -0.12, boat >= 5 ? 0.035 : 0.14);
    return {
      ...row,
      baseScoreBeforeRaceFlow: row.score,
      score: finiteNumber(row.score, 0) + adjustment,
      raceFlowAdjustment: round(adjustment, 4),
      raceFlowHeadScore: roundScore(head),
      raceFlowSecondScore: roundScore(second),
      raceFlowThirdScore: roundScore(third),
      raceFlowScenarioTriggerScore: roundScore(trigger),
      raceFlowBeneficiaryScore: roundScore(beneficiary),
      raceFlowResidualScore: roundScore(finiteNumber(scores.residualScore, 0.5)),
      raceFlowAttackerScore: roundScore(finiteNumber(scores.attackerScore, head)),
      raceFlowReliabilityScore: roundScore(finiteNumber(scores.reliabilityScore, 0.5)),
      raceFlowWallScore: scores.wallScore === null ? null : roundScore(finiteNumber(scores.wallScore, 0.5)),
      raceFlowBlockingScore: scores.blockingScore === null ? null : roundScore(finiteNumber(scores.blockingScore, 0.5)),
      reliabilityScore: finiteNumber(scores.reliabilityScore, row.reliabilityScore ?? null),
      wallScore: scores.wallScore ?? row.wallScore ?? null,
      blockingScore: scores.blockingScore ?? row.blockingScore ?? null,
      courseWinRate: scores.courseWinRate ?? row.courseWinRate ?? null,
      courseQuinellaRate: scores.courseQuinellaRate ?? row.courseQuinellaRate ?? null,
      courseTrifectaRate: scores.courseTrifectaRate ?? row.courseTrifectaRate ?? null,
      raceFlowCanBeHead: scores.canBeHead === true
    };
  });
}

function scenarioById(model = {}, id) {
  return safeArray(model?.scenarioFamilies).find((row) => row?.id === id) ||
    safeArray(model?.scenarios).find((row) => row?.id === id) ||
    null;
}

function scenarioDecisionForTicket(model = {}, head) {
  if (head === 2) return { scenarioId: "sashi_2", decision: "sashi" };
  if (head === 4) {
    const fourBenefit = finiteNumber(scenarioById(model, "four_beneficiary")?.score, 0);
    const directFour = finiteNumber(scenarioById(model, "makuriSashi_4")?.score, 0);
    return fourBenefit >= directFour
      ? { scenarioId: "four_beneficiary", decision: "makuriSashi" }
      : { scenarioId: "makuriSashi_4", decision: "makuriSashi" };
  }
  if (head === 3) {
    const makuri = finiteNumber(scenarioById(model, "makuri_3")?.score, 0);
    const makuriSashi = finiteNumber(scenarioById(model, "makuri_sashi_3")?.score, 0);
    return makuri >= makuriSashi
      ? { scenarioId: "makuri_3", decision: "makuri" }
      : { scenarioId: "makuri_sashi_3", decision: "makuriSashi" };
  }
  if (head === 1) return { scenarioId: "escape_1", decision: "escape" };
  return { scenarioId: "outside_follow_5_6", decision: null };
}

function residualScore01(model = {}, field, fallback = 0.5) {
  return optionalRate01(model?.decisionResidualScores?.[field]) ?? fallback;
}

function decisionStat(model = {}, decision) {
  return model?.decisionConditionedStats?.[decision] || {};
}

function headDecisionStat(model = {}, head, decision) {
  return model?.headDecisionComboStats?.[String(head)]?.[decision] || {};
}

function rateFromStats(row = {}, group, key, fallback = 0.5) {
  const raw = row?.[group]?.[key];
  const rate = optionalRate01(raw);
  if (rate === null) return fallback;
  return fallback + (rate - fallback) * decisionSampleWeight(row?.sampleCount);
}

function blendSupport(liveScore, venueScore, venueWeight = 0.5) {
  return clamp(0.5 + (liveScore - 0.5) * 0.72 + (venueScore - 0.5) * venueWeight);
}

export function scoreRaceFlowTicketDecisionCompatibility(ticket = {}, model = {}) {
  const boats = Array.isArray(ticket?.boats)
    ? ticket.boats.map((value) => Number(value))
    : String(ticket?.combo || "").split("-").map((value) => Number(value));
  const [head, second] = boats;
  const scenario = scenarioDecisionForTicket(model, head);
  const reasons = [];
  let support = 0.5;
  let floor = 0.74;
  let ceiling = 1.18;
  const boat1Residual = residualScore01(model, "boat1ResidualAfterAttackScore", 0.5);
  const boat3Residual = residualScore01(model, "boat3ResidualScore", 0.5);
  const boat2Residual = residualScore01(model, "boat2ResidualAfterFlowScore", 0.5);
  const boat5Follow = residualScore01(model, "boat5LinkedFollowScore", 0.5);
  const boat6Follow = residualScore01(model, "boat6LinkedFollowScore", 0.5);

  if (head === 3 && second === 1) {
    const stats = decisionStat(model, scenario.decision || "makuri");
    const venue = decisionRate(stats, "boat1SecondRate", 0.5);
    support = blendSupport(boat1Residual, venue, 0.9);
    floor = 0.62;
    ceiling = 1.22;
    if (support < 0.46) reasons.push("3頭時の1残り根拠が弱いため3-1を降格");
    if (support > 0.6) reasons.push("3頭時でも1が残る根拠があるため3-1を維持");
  } else if (head === 3 && second >= 4) {
    const stats = decisionStat(model, scenario.decision || "makuri");
    support = decisionRate(stats, "outsideLinkedRate", 0.5);
    floor = 0.86;
    ceiling = 1.16;
    if (support > 0.58) reasons.push("3攻め後の外連動が出やすく外相手を加点");
  } else if (head === 2 && second === 1) {
    const stats = decisionStat(model, "sashi");
    const venue = decisionRate(stats, "boat1SecondRate", 0.5);
    support = blendSupport(boat1Residual, venue, 0.85);
    floor = 0.68;
    ceiling = 1.18;
    if (support < 0.46) reasons.push("差し決着時の1残りが薄く2-1を降格");
    if (support > 0.58) reasons.push("差し決着時の1残りがあり2-1を維持");
  } else if (head === 4 && second === 3) {
    const stats = headDecisionStat(model, 4, "makuriSashi");
    const venue = Math.max(
      rateFromStats(stats, "secondRates", "3", 0.5),
      rateFromStats(stats, "exactaRates", "4-3", 0.5)
    );
    support = blendSupport(boat3Residual, venue, 0.95);
    floor = 0.58;
    ceiling = 1.18;
    if (support < 0.48) reasons.push("3は攻めの起点後に残る根拠が弱く4-3を降格");
    if (support > 0.6) reasons.push("3の残り足と4-3傾向があり4-3を維持");
  } else if (head === 4 && second === 1) {
    const stats = headDecisionStat(model, 4, "makuriSashi");
    const venue = Math.max(
      rateFromStats(stats, "secondRates", "1", 0.5),
      rateFromStats(stats, "exactaRates", "4-1", 0.5)
    );
    support = blendSupport(boat1Residual, venue, 0.95);
    floor = 0.76;
    ceiling = 1.24;
    if (support > 0.58) reasons.push("4頭時の1残り根拠があり4-1を加点");
  } else if (head === 4 && second === 2) {
    const stats = headDecisionStat(model, 4, "makuriSashi");
    const venue = Math.max(
      rateFromStats(stats, "secondRates", "2", 0.5),
      rateFromStats(stats, "exactaRates", "4-2", 0.5)
    );
    support = blendSupport(boat2Residual, venue, 0.8);
    floor = 0.8;
    ceiling = 1.18;
    if (support > 0.58) reasons.push("4頭時の2残りがあり4-2を加点");
  } else if (head === 4 && second === 5) {
    const stats = headDecisionStat(model, 4, "makuriSashi");
    const venue = Math.max(
      rateFromStats(stats, "secondRates", "5", 0.5),
      rateFromStats(stats, "exactaRates", "4-5", 0.5)
    );
    support = blendSupport(boat5Follow, venue, 0.85);
    floor = 0.82;
    ceiling = 1.2;
    if (support > 0.58) reasons.push("4頭時の外連動があり4-5を少し加点");
  } else if (head === 4 && second === 6) {
    const stats = headDecisionStat(model, 4, "makuriSashi");
    const venue = Math.max(
      rateFromStats(stats, "secondRates", "6", 0.5),
      rateFromStats(stats, "exactaRates", "4-6", 0.5)
    );
    support = blendSupport(boat6Follow, venue, 0.82);
    floor = 0.8;
    ceiling = 1.18;
    if (support > 0.58) reasons.push("4頭時の内崩れと6号艇の追走評価があり4-6を加点");
  }

  const multiplier = decisionCompatibilityMultiplier(support, floor, ceiling);
  return {
    combo: ticket?.combo || boats.join("-"),
    scenarioId: scenario.scenarioId,
    decision: scenario.decision,
    ticketDecisionCompatibilityScore: roundScore(support),
    multiplier: round(multiplier, 4),
    reasons
  };
}

export function applyRaceFlowTicketDecisionCompatibility(trifecta = [], model = {}) {
  const adjusted = safeArray(trifecta).map((ticket) => {
    const compatibility = scoreRaceFlowTicketDecisionCompatibility(ticket, model);
    return {
      ...ticket,
      probability: finiteNumber(ticket?.probability, 0) * compatibility.multiplier,
      decisionCompatibilityScore: compatibility.ticketDecisionCompatibilityScore,
      decisionCompatibilityMultiplier: compatibility.multiplier,
      decisionCompatibilityReasons: compatibility.reasons,
      decisionScenarioId: compatibility.scenarioId,
      decisionMethod: compatibility.decision
    };
  });
  const total = adjusted.reduce((sum, ticket) => sum + finiteNumber(ticket.probability, 0), 0);
  const normalized = total > 0
    ? adjusted.map((ticket) => ({ ...ticket, probability: ticket.probability / total }))
    : adjusted;
  const preview = normalized
    .filter((ticket) => safeArray(ticket.decisionCompatibilityReasons).length > 0 || ticket.decisionCompatibilityMultiplier !== 1)
    .sort((a, b) => Math.abs(1 - b.decisionCompatibilityMultiplier) - Math.abs(1 - a.decisionCompatibilityMultiplier))
    .slice(0, 24)
    .map((ticket) => ({
      combo: ticket.combo,
      decisionMethod: ticket.decisionMethod,
      scenarioId: ticket.decisionScenarioId,
      ticketDecisionCompatibilityScore: ticket.decisionCompatibilityScore,
      multiplier: ticket.decisionCompatibilityMultiplier,
      reasons: ticket.decisionCompatibilityReasons
    }));
  return { tickets: normalized.sort((a, b) => b.probability - a.probability), preview };
}

export function buildRaceFlowScenarioTickets(model = {}, baseTickets = [], limit = 6) {
  const baseTicketCombos = new Set(safeArray(baseTickets).map((ticket) => ticket?.combo).filter(Boolean));
  const rows = [];
  const seen = new Set(baseTicketCombos);
  const scenarioSource = safeArray(model?.scenarioFamilies).length > 0
    ? safeArray(model?.scenarioFamilies)
    : safeArray(model?.scenarios);
  const scenarios = scenarioSource
    .filter((scenario) => scenario.id !== "escape_1" && scenario.score >= 54)
    .sort((a, b) => b.score - a.score);
  const maxRowsPerScenario = Math.max(2, Math.ceil(limit / 2));
  for (const scenario of scenarios) {
    let scenarioRowsAdded = 0;
    for (const pattern of safeArray(scenario.patterns)) {
      const candidates = expandPattern(pattern, baseTicketCombos)
        .map((combo) => ({
          combo,
          compatibility: scoreRaceFlowTicketDecisionCompatibility({
            combo,
            boats: combo.split("-").map((value) => Number(value))
          }, model)
        }))
        .sort((a, b) => b.compatibility.multiplier - a.compatibility.multiplier);
      for (const { combo, compatibility } of candidates) {
        if (seen.has(combo)) continue;
        if (compatibility.multiplier < 0.82) {
          model.ticketAdjustmentLog?.push?.({
            action: "demote",
            target: combo,
            scenarioId: scenario.id,
            ticketDecisionCompatibilityScore: compatibility.ticketDecisionCompatibilityScore,
            reason: compatibility.reasons[0] || "decision-conditioned combo compatibility is low"
          });
          continue;
        }
        seen.add(combo);
        rows.push({
          combo,
          boats: combo.split("-").map((value) => Number(value)),
          probability: round(clamp((scenario.score / 100) * 0.07 * compatibility.multiplier), 4),
          sourcePattern: scenario.label,
          scenarioId: scenario.id,
          scenarioName: scenario.label,
          upsetScore: scenario.score,
          decisionCompatibilityScore: compatibility.ticketDecisionCompatibilityScore,
          decisionCompatibilityMultiplier: compatibility.multiplier,
          decisionCompatibilityReasons: compatibility.reasons
        });
        model.ticketAdjustmentLog?.push?.({
          action: "promote",
          target: combo,
          scenarioId: scenario.id,
          reason: `${scenario.label}の展開スコアが高いため追加候補へ昇格`
        });
        scenarioRowsAdded += 1;
        if (rows.length >= limit) return rows;
        if (scenarioRowsAdded >= maxRowsPerScenario) break;
      }
      if (scenarioRowsAdded >= maxRowsPerScenario) break;
    }
  }
  return rows;
}
