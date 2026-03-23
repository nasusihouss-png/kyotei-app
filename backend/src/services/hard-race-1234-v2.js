import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEBUG_ROOT = path.resolve(__dirname, "../../debug/hard-race-1234");
const FIXED_COMBOS = ["1-2-3", "1-2-4", "1-3-2", "1-3-4", "1-4-2", "1-4-3"];
const FIXED_TOP4_COUNT = 4;
const HARD_RACE_V2_WEIGHTS = {
  opponent_pairs: { pair23: 0.31, pair24: 0.34, pair34: 0.35 },
  opponent_roles: {
    lane2_second: 0.34,
    lane3_attack: 0.33,
    lane4_develop: 0.33
  },
  combo_multiplier: {
    "1-2-3": 1.03,
    "1-2-4": 1.16,
    "1-3-2": 0.98,
    "1-3-4": 1.01,
    "1-4-2": 1.05,
    "1-4-3": 0.97
  }
};
const HARD_RACE_V2_DECISION_THRESHOLDS = {
  buy4: {
    p1_escape_min: 0.53,
    fixed1234_total_probability_min: 0.31,
    outside_break_risk_max: 0.22,
    top4_fixed1234_probability_min: 0.25,
    top4_share_within_fixed1234_min: 0.7
  },
  buy6: {
    p1_escape_min: 0.53,
    fixed1234_total_probability_min: 0.31,
    outside_break_risk_max: 0.22
  },
  borderline: {
    p1_escape_min: 0.45,
    fixed1234_total_probability_min: 0.26
  }
};
const VENUE_INNER_BIAS = {
  1: 0.63, 2: 0.64, 3: 0.51, 4: 0.58, 5: 0.62, 6: 0.64, 7: 0.71, 8: 0.67,
  9: 0.57, 10: 0.68, 11: 0.64, 12: 0.69, 13: 0.62, 14: 0.56, 15: 0.7, 16: 0.63,
  17: 0.61, 18: 0.7, 19: 0.73, 20: 0.67, 21: 0.68, 22: 0.64, 23: 0.66, 24: 0.76
};

function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 4) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(Number(value).toFixed(digits));
}

function asField(value, source = null, missingReason = null) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return { value: null, source, missing_reason: missingReason || "precomputed feature missing" };
  }
  return { value: Number(value), source, missing_reason: null };
}

function getFieldValue(row, ...keys) {
  for (const key of keys) {
    const n = toNum(row?.[key], null);
    if (n !== null) return n;
  }
  return null;
}

function getKyoteiFieldSource(fieldSources = {}, lane, candidates = []) {
  const laneSources = fieldSources?.[String(lane)] || fieldSources?.[lane] || {};
  for (const key of candidates) {
    if (laneSources?.[key]) return laneSources[key];
  }
  return null;
}

function buildArtifactDir({ date, venueId, raceNo }) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  return path.resolve(DEBUG_ROOT, `${String(date).replace(/-/g, "")}_${String(venueId).padStart(2, "0")}_${String(raceNo)}_${stamp}`);
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeArtifact(filePath, body) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, body, "utf8");
  return filePath;
}

function saveArtifacts({ dir, raw, fetchedUrls, normalizedData, scores }) {
  ensureDir(dir);
  const rawSavedPaths = {};
  for (const [key, value] of Object.entries(raw || {})) {
    if (value === null || value === undefined || value === "") continue;
    const extension = typeof value === "string" && (value.startsWith("<") || value.includes("<html")) ? "html" : "json";
    rawSavedPaths[key] = writeArtifact(
      path.resolve(dir, "raw", `${key}.${extension}`),
      extension === "json" ? JSON.stringify(value, null, 2) : String(value)
    );
  }
  const parsedSavedPaths = {
    fetched_urls: writeArtifact(path.resolve(dir, "parsed", "fetched-urls.json"), JSON.stringify(fetchedUrls, null, 2)),
    normalized_data: writeArtifact(path.resolve(dir, "parsed", "normalized-data.json"), JSON.stringify(normalizedData, null, 2)),
    hard_race_scores: writeArtifact(path.resolve(dir, "parsed", "hard-race-1234.json"), JSON.stringify(scores, null, 2))
  };
  return { rawSavedPaths, parsedSavedPaths };
}

async function fetchOfficialRaceResult({ date, venueId, raceNo, timeoutMs = 6000 }) {
  const hd = String(date).replace(/-/g, "");
  const jcd = String(venueId).padStart(2, "0");
  const rno = Number(raceNo);
  const url = `https://www.boatrace.jp/owpc/pc/race/raceresult?rno=${rno}&jcd=${jcd}&hd=${hd}`;
  try {
    const { data } = await axios.get(url, {
      timeout: timeoutMs,
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
      }
    });
    const $ = cheerio.load(data);
    let combo = null;
    $("table tr").each((_, tr) => {
      if (combo) return;
      const cells = $(tr).children("th,td");
      const heading = normalizeText(cells.eq(0).text());
      if (!heading.includes("3") && !heading.includes("三")) return;
      const rowText = normalizeText($(tr).text());
      const match = rowText.match(/([1-6])\D+([1-6])\D+([1-6])/);
      if (match) combo = `${match[1]}-${match[2]}-${match[3]}`;
    });
    return {
      ok: true,
      url,
      raw: data,
      result: combo ? { top3: combo.split("-").map((item) => Number(item)), combo } : null
    };
  } catch (error) {
    return { ok: false, url, raw: null, result: null, error: String(error?.message || error) };
  }
}

function buildSourceSummary({ data, normalizedLanes, resultFetch, fallbackSummary = null }) {
  const kyotei = data?.source?.kyotei_biyori || {};
  const official = data?.source?.official_fetch_status || {};
  const supplementalCoveredLanes = normalizedLanes.filter((lane) => {
    const f = lane?.features || {};
    return [
      f.sashi_rate?.value,
      f.makuri_rate?.value,
      f.makurisashi_rate?.value,
      f.stability_rate?.value,
      f.breakout_rate?.value,
      f.course_3rentai_rate?.value
    ].some((value) => value !== null);
  }).length;

  return {
    primary: {
      source: "boatrace",
      racelist: official?.racelist || "unknown",
      beforeinfo: official?.beforeinfo || "unknown",
      result_fetch_ok: !!resultFetch?.ok,
      result_available: !!resultFetch?.result
    },
    supplement: {
      source: "kyoteibiyori",
      fetch_ok: !!kyotei?.ok,
      fallback_used: !!kyotei?.fallback_used,
      covered_lanes: supplementalCoveredLanes,
      failed_fields: Array.isArray(kyotei?.field_diagnostics?.failed_fields) ? kyotei.field_diagnostics.failed_fields : [],
      populated_fields: Array.isArray(kyotei?.field_diagnostics?.populated_fields) ? kyotei.field_diagnostics.populated_fields : []
    },
    source_priority: [
      "boatrace > kyoteibiyori",
      "official overlap wins",
      "kyoteibiyori only supplements missing traits",
      fallbackSummary?.used ? "fallback applied to unresolved hard-race metrics" : "fallback not needed"
    ],
    hard_race_fallback: fallbackSummary || {
      used: false,
      fields: [],
      details: {}
    },
    fetch_timings: data?.source?.fetch_timings || data?.source?.timings || {}
  };
}

function buildMissingFieldDetails(normalized) {
  const details = {};
  for (const laneRow of normalized?.lanes || []) {
    const lane = Number(laneRow?.lane);
    for (const [key, field] of Object.entries(laneRow?.features || {})) {
      if (field?.value !== null) continue;
      details[`lane${lane}.${key}`] = {
        reason: field?.missing_reason || "api field missing",
        source: field?.source || null,
        lane,
        field: key
      };
    }
  }
  return details;
}

function getMetricReason({ fieldName, missingFields = [], missingFieldDetails = {}, normalized }) {
  const related = missingFields.filter((key) => {
    if (!key.startsWith("lane")) return false;
    if (fieldName === "boat1_escape_trust") {
      return /\.national_win_rate$|\.local_win_rate$|\.avg_st$|\.f_count$|\.l_count$|\.motor_2ren$|\.motor_3ren$|\.boat_2ren$|\.boat_3ren$/.test(key);
    }
    if (fieldName === "opponent_234_fit") {
      return /\.course_3rentai_rate$|\.lane_3rentai_rate$|\.avg_st$|\.motor_2ren$|\.motor_3ren$|\.boat_2ren$|\.stability_rate$|\.breakout_rate$|\.sashi_rate$|\.makuri_rate$|\.makurisashi_rate$|\.zentsuke_tendency$/.test(key);
    }
    if (fieldName === "pair23_fit") {
      return /^lane(2|3)\./.test(key);
    }
    if (fieldName === "pair24_fit") {
      return /^lane(2|4)\./.test(key);
    }
    if (fieldName === "pair34_fit") {
      return /^lane(3|4)\./.test(key);
    }
    if (fieldName === "kill_escape_risk" || fieldName === "shape_shuffle_risk" || fieldName === "makuri_risk") {
      return /\.makuri_rate$|\.makurisashi_rate$/.test(key);
    }
    if (fieldName === "outside_break_risk") {
      return /\.course_3rentai_rate$|\.avg_st$|\.motor_2ren$/.test(key);
    }
    if (fieldName === "box_hit_score") {
      return /^lane[1-6]\./.test(key);
    }
    if (fieldName === "shape_focus_score" || fieldName === "top4_fixed1234_probability" || /^p_/.test(fieldName)) {
      return /^lane(2|3|4)\./.test(key);
    }
    return false;
  });
  if (related.length > 0) {
    const reasons = [...new Set(related.map((key) => missingFieldDetails?.[key]?.reason || "missing source data"))];
    return reasons.includes("missing source data") ? "missing source data" : reasons[0];
  }
  const lanes = Array.isArray(normalized?.lanes) ? normalized.lanes : [];
  if (lanes.length < 6) return "api field missing";
  return "not calculated";
}

function buildMetricStatus({ computed, missingFieldDetails, normalized }) {
  const missingFields = Array.isArray(computed?.missing_fields) ? computed.missing_fields : [];
  const metricValues = computed?.scores || {};
  const result = {};
  for (const fieldName of [
    "hard_race_score",
    "boat1_escape_trust",
    "opponent_234_fit",
    "pair23_fit",
    "pair24_fit",
    "pair34_fit",
    "kill_escape_risk",
    "shape_shuffle_risk",
    "makuri_risk",
    "outside_break_risk",
    "box_hit_score",
    "shape_focus_score",
    "fixed1234_total_probability",
    "top4_fixed1234_probability",
    "fixed1234_shape_concentration",
    "head_prob_1",
    "head_prob_2",
    "head_prob_3",
    "head_prob_4",
    "head_prob_5",
    "head_prob_6",
    "outside_head_risk",
    "outside_2nd_risk",
    "outside_3rd_risk",
    "outside_box_break_risk",
    "p_123",
    "p_124",
    "p_132",
    "p_134",
    "p_142",
    "p_143"
  ]) {
    const value = metricValues?.[fieldName] ?? null;
    result[fieldName] = {
      value,
      status: value === null ? "missing" : "calculated",
      reason: value === null ? getMetricReason({ fieldName, missingFields, missingFieldDetails, normalized }) : null
    };
  }
  return result;
}

function normalizeLane({ racer, kyoteiByLane, fieldSources }) {
  const lane = Number(racer?.lane);
  const supplement = kyoteiByLane?.get(lane) || {};
  const kyoteiSource = (keys) => getKyoteiFieldSource(fieldSources, lane, keys);

  return {
    lane,
    name: racer?.name || null,
    class: racer?.class || null,
    entry_course: toNum(racer?.entryCourse ?? racer?.lane, lane),
    branch: racer?.branch || null,
    features: {
      national_win_rate: asField(toNum(racer?.nationwideWinRate, null), "boatrace", "api field missing"),
      local_win_rate: asField(toNum(racer?.localWinRate, null), "boatrace", "api field missing"),
      avg_st: asField(toNum(racer?.avgSt, null), "boatrace", "api field missing"),
      f_count: asField(toNum(racer?.fHoldCount, 0), "boatrace", "api field missing"),
      l_count: asField(toNum(racer?.lHoldCount, null), "boatrace", "api field missing"),
      motor_2ren: asField(getFieldValue(racer, "motor2Rate", "motor2ren"), "boatrace", "api field missing"),
      motor_3ren: asField(getFieldValue(racer, "motor3Rate", "motor3ren"), "boatrace", "api field missing"),
      boat_2ren: asField(getFieldValue(racer, "boat2Rate", "boat2ren"), "boatrace", "api field missing"),
      boat_3ren: asField(getFieldValue(racer, "boat3Rate", "boat3ren"), "boatrace", "api field missing"),
      sashi_rate: asField(getFieldValue(supplement, "sashiRate", "sashi_rate"), kyoteiSource(["sashiRate", "sashi_rate"]) || null, "missing source data"),
      makuri_rate: asField(getFieldValue(supplement, "makuriRate", "makuri_rate"), kyoteiSource(["makuriRate", "makuri_rate"]) || null, "missing source data"),
      makurisashi_rate: asField(
        getFieldValue(supplement, "makurisashiRate", "makurisashi_rate", "makuriSashiRate"),
        kyoteiSource(["makurisashiRate", "makurisashi_rate", "makuriSashiRate"]) || null,
        "missing source data"
      ),
      late_start_rate: asField(
        getFieldValue(supplement, "lateStartRate", "late_start_rate", "delayRate", "delay_rate"),
        kyoteiSource(["lateStartRate", "late_start_rate", "delayRate", "delay_rate"]) || null,
        "missing source data"
      ),
      stability_rate: asField(getFieldValue(supplement, "stabilityRate", "stability_rate"), kyoteiSource(["stabilityRate", "stability_rate"]) || null, "missing source data"),
      breakout_rate: asField(getFieldValue(supplement, "breakoutRate", "breakout_rate"), kyoteiSource(["breakoutRate", "breakout_rate"]) || null, "missing source data"),
      course_3rentai_rate: asField(
        getFieldValue(supplement, "lane3RenRate", "lane3renScore", "lane3renAvg", "lane3renRate", "course3RentaiRate", "course_3rentai_rate") ??
          getFieldValue(racer, "lane3RenRate", "lane3renScore", "lane3renAvg", "lane3renRate", "course3RentaiRate", "course_3rentai_rate"),
        kyoteiSource(["lane3RenRate", "lane3renScore", "lane3renAvg", "lane3renRate"]) || null,
        "missing source data"
      ),
      lane_3rentai_rate: asField(
        getFieldValue(supplement, "lane3RenRate", "lane3renScore", "lane3renAvg", "lane3renRate") ??
          getFieldValue(racer, "lane3RenRate", "lane3renScore", "lane3renAvg", "lane3renRate"),
        kyoteiSource(["lane3RenRate", "lane3renScore", "lane3renAvg", "lane3renRate"]) || null,
        "missing source data"
      ),
      zentsuke_tendency: asField(
        getFieldValue(supplement, "zentsukeTendency", "zentsuke_tendency"),
        kyoteiSource(["zentsukeTendency", "zentsuke_tendency"]) || null,
        "missing source data"
      )
    }
  };
}

function normalizeRaceData({ data, resultFetch }) {
  const racers = Array.isArray(data?.racers) ? data.racers : [];
  const kyoteiByLane =
    data?.source?.kyotei_biyori?.request_diagnostics?.merged_by_lane instanceof Map
      ? data.source.kyotei_biyori.request_diagnostics.merged_by_lane
      : data?.source?.kyotei_biyori?.byLane instanceof Map
        ? data.source.kyotei_biyori.byLane
        : new Map();
  const fieldSources = data?.source?.kyotei_biyori?.field_sources || data?.source?.kyotei_biyori?.fieldSources || {};

  return {
    race: {
      date: data?.race?.date || null,
      venue_id: toNum(data?.race?.venueId, null),
      venue_name: data?.race?.venueName || null,
      race_no: toNum(data?.race?.raceNo, null)
    },
    result: resultFetch?.result || null,
    venue: {
      inside_bias: asField(round(VENUE_INNER_BIAS[toNum(data?.race?.venueId, null)] ?? 0.62, 3), "derived_local", "api field missing")
    },
    source_priority: {
      primary: "boatrace",
      supplement: "kyoteibiyori",
      overlap_policy: "official_first"
    },
    lanes: racers.map((racer) => normalizeLane({ racer, kyoteiByLane, fieldSources }))
  };
}

function missingCoreBoatraceFields(normalized) {
  const requiredByLane = ["national_win_rate", "local_win_rate", "avg_st", "f_count", "motor_2ren", "boat_2ren"];
  const missing = [];
  for (const laneRow of normalized?.lanes || []) {
    for (const key of requiredByLane) {
      if (laneRow?.features?.[key]?.value === null) missing.push(`lane${laneRow.lane}.${key}`);
    }
  }
  return missing;
}

function scoreBlend(values) {
  const present = values.filter((row) => row && row.value !== null && row.weight > 0);
  if (!present.length) return null;
  const totalWeight = present.reduce((sum, row) => sum + row.weight, 0);
  return totalWeight > 0 ? present.reduce((sum, row) => sum + row.value * row.weight, 0) / totalWeight : null;
}

function percentFromRate(value, scale = 10) {
  return value === null ? null : clamp(0, 100, value * scale);
}

function inverseStScore(avgSt) {
  return avgSt === null ? null : clamp(0, 100, ((0.24 - avgSt) / 0.11) * 100);
}

function lowerBetterScore(value, min = 0, max = 30) {
  return value === null ? null : clamp(0, 100, ((max - value) / Math.max(1, max - min)) * 100);
}

function normalizedPercent(value, min = 0, max = 100) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return null;
  return clamp(0, 1, (Number(value) - min) / Math.max(1e-9, max - min));
}

function invertNormalized(value, min = 0, max = 100) {
  const normalized = normalizedPercent(value, min, max);
  return normalized === null ? null : clamp(0, 1, 1 - normalized);
}

function weightedAverage(values) {
  const present = values.filter((row) => row && row.value !== null && Number.isFinite(Number(row.weight)) && row.weight > 0);
  if (!present.length) return null;
  const totalWeight = present.reduce((sum, row) => sum + row.weight, 0);
  return totalWeight > 0 ? present.reduce((sum, row) => sum + row.value * row.weight, 0) / totalWeight : null;
}

function normalizeWeights(weightMap) {
  const entries = Object.entries(weightMap || {}).filter(([, value]) => Number.isFinite(Number(value)) && Number(value) > 0);
  const total = entries.reduce((sum, [, value]) => sum + Number(value), 0);
  if (!(total > 0)) return Object.fromEntries(entries.map(([key]) => [key, 0]));
  return Object.fromEntries(entries.map(([key, value]) => [key, Number(value) / total]));
}

function toProbabilityMap(scoreMap) {
  const entries = Object.entries(scoreMap || {}).map(([key, value]) => [key, Math.max(0.0001, Number(value) || 0.0001)]);
  const total = entries.reduce((sum, [, value]) => sum + value, 0);
  if (!(total > 0)) return Object.fromEntries(entries.map(([key]) => [key, 0]));
  return Object.fromEntries(entries.map(([key, value]) => [key, round(value / total, 4)]));
}

function laneFitScore(row, lane) {
  const f = row?.features || {};
  const course3 = f.course_3rentai_rate?.value;
  const lane3 = f.lane_3rentai_rate?.value;
  const stScore = inverseStScore(f.avg_st?.value);
  const motor2 = f.motor_2ren?.value;
  const motor3 = f.motor_3ren?.value;
  const boat2 = f.boat_2ren?.value;
  const stability = f.stability_rate?.value;
  const breakout = f.breakout_rate?.value;
  const sashi = f.sashi_rate?.value;
  const makuri = f.makuri_rate?.value;
  const makurisashi = f.makurisashi_rate?.value;
  const zentsukePenalty = f.zentsuke_tendency?.value === null ? null : clamp(0, 100, 100 - f.zentsuke_tendency.value);

  if (lane === 2) {
    return scoreBlend([
      { value: course3, weight: 0.2 }, { value: lane3, weight: 0.16 }, { value: stScore, weight: 0.18 },
      { value: motor2, weight: 0.14 }, { value: motor3, weight: 0.08 }, { value: boat2, weight: 0.08 },
      { value: sashi, weight: 0.1 }, { value: stability, weight: 0.04 }, { value: zentsukePenalty, weight: 0.02 }
    ]);
  }
  if (lane === 3) {
    return scoreBlend([
      { value: course3, weight: 0.18 }, { value: lane3, weight: 0.14 }, { value: stScore, weight: 0.16 },
      { value: motor2, weight: 0.12 }, { value: motor3, weight: 0.1 }, { value: makuri, weight: 0.13 },
      { value: makurisashi, weight: 0.11 }, { value: breakout, weight: 0.04 }, { value: stability, weight: 0.02 }
    ]);
  }
  return scoreBlend([
    { value: course3, weight: 0.22 }, { value: lane3, weight: 0.18 }, { value: stScore, weight: 0.12 },
    { value: motor2, weight: 0.14 }, { value: motor3, weight: 0.08 }, { value: boat2, weight: 0.08 },
    { value: breakout, weight: 0.08 }, { value: stability, weight: 0.06 }, { value: zentsukePenalty, weight: 0.04 }
  ]);
}

function pairFitScore({ leftFit, rightFit, leftSupport = [], rightSupport = [], balancePenalty = 0.12 }) {
  const balance = leftFit === null || rightFit === null ? null : clamp(0, 100, 100 - Math.abs(leftFit - rightFit));
  return scoreBlend([
    { value: leftFit, weight: 0.3 },
    { value: rightFit, weight: 0.3 },
    { value: scoreBlend(leftSupport.map((value) => ({ value, weight: 1 }))), weight: 0.18 },
    { value: scoreBlend(rightSupport.map((value) => ({ value, weight: 1 }))), weight: 0.14 },
    { value: balance, weight: balancePenalty }
  ]);
}

function estimateBoat1EscapeAffinity(boat1, challenger) {
  if (!boat1 || !challenger) return null;
  const boat1St = inverseStScore(boat1?.features?.avg_st?.value ?? null);
  const challengerSt = inverseStScore(challenger?.features?.avg_st?.value ?? null);
  return scoreBlend([
    { value: normalizedPercent(boat1?.features?.local_win_rate?.value ?? null, 3, 8) * 100, weight: 0.28 },
    { value: normalizedPercent(boat1?.features?.motor_2ren?.value ?? null, 20, 60) * 100, weight: 0.2 },
    { value: normalizedPercent(boat1?.features?.boat_2ren?.value ?? null, 20, 60) * 100, weight: 0.14 },
    { value: normalizedPercent(challenger?.features?.course_3rentai_rate?.value ?? null, 20, 80) * 100, weight: 0.12 },
    { value: clamp(0, 100, 100 - Math.max(0, (challengerSt || 50) - (boat1St || 50))), weight: 0.16 },
    { value: clamp(0, 100, 100 - Math.max(0, (challenger?.features?.motor_2ren?.value ?? 40) - (boat1?.features?.motor_2ren?.value ?? 40))), weight: 0.1 }
  ]);
}

function fallbackPairFit({ laneA, laneB, boat1 }) {
  return round(scoreBlend([
    { value: laneA?.features?.course_3rentai_rate?.value ?? null, weight: 0.22 },
    { value: laneB?.features?.course_3rentai_rate?.value ?? null, weight: 0.22 },
    { value: inverseStScore(laneA?.features?.avg_st?.value ?? null), weight: 0.12 },
    { value: inverseStScore(laneB?.features?.avg_st?.value ?? null), weight: 0.12 },
    { value: laneA?.features?.motor_2ren?.value ?? null, weight: 0.1 },
    { value: laneB?.features?.motor_2ren?.value ?? null, weight: 0.1 },
    { value: estimateBoat1EscapeAffinity(boat1, laneA), weight: 0.06 },
    { value: estimateBoat1EscapeAffinity(boat1, laneB), weight: 0.06 }
  ]), 1);
}

function buildFallbackTracker() {
  return {
    used: false,
    items: [],
    byField: {}
  };
}

function markFallback(tracker, field, formula) {
  tracker.used = true;
  tracker.byField[field] = formula;
  tracker.items.push({ field, type: "fallback", formula });
}

function resolveMetricWithFallback({ field, primaryValue, fallbackValue, fallbackFormula, tracker }) {
  if (primaryValue !== null) {
    return { value: primaryValue, sourceType: "primary", fallbackUsed: false };
  }
  if (fallbackValue !== null) {
    markFallback(tracker, field, fallbackFormula);
    return { value: fallbackValue, sourceType: "fallback", fallbackUsed: true };
  }
  return { value: null, sourceType: "missing", fallbackUsed: false };
}

function summarizeMetricSource(primary, resolved) {
  if (resolved?.fallbackUsed) return `${primary} + fallback`;
  if (resolved?.value !== null) return primary;
  return `${primary} missing`;
}

function buildHeadCandidate(laneRow, laneFit, venueInsideBias) {
  const lane = laneRow?.lane;
  const laneBias = lane === 1 ? venueInsideBias : clamp(0, 100, 74 - ((lane - 1) * 9));
  const stScore = inverseStScore(laneRow?.features?.avg_st?.value ?? null);
  const attackScore = scoreBlend([
    { value: laneFit, weight: 0.24 },
    { value: percentFromRate(laneRow?.features?.national_win_rate?.value ?? null), weight: 0.18 },
    { value: percentFromRate(laneRow?.features?.local_win_rate?.value ?? null), weight: 0.15 },
    { value: stScore, weight: 0.14 },
    { value: laneRow?.features?.motor_2ren?.value ?? null, weight: 0.1 },
    { value: laneRow?.features?.boat_2ren?.value ?? null, weight: 0.08 },
    { value: laneRow?.features?.course_3rentai_rate?.value ?? null, weight: 0.05 },
    { value: laneRow?.features?.breakout_rate?.value ?? null, weight: 0.03 },
    { value: laneRow?.features?.makuri_rate?.value ?? null, weight: 0.03 }
  ]);
  return {
    lane,
    score: round(scoreBlend([
      { value: attackScore, weight: 0.78 },
      { value: laneBias, weight: 0.22 }
    ]), 1)
  };
}

function comboProbability({ combo, secondWeights, thirdWeights, pair23Fit, pair24Fit, pair34Fit }) {
  const [head, second, third] = combo.split("-").map((item) => Number(item));
  if (head !== 1 || !secondWeights?.[second] || !thirdWeights?.[third]) return 0;
  const pairFit =
    second === 2 && third === 3 ? pair23Fit
      : second === 2 && third === 4 ? pair24Fit
        : second === 3 && third === 2 ? pair23Fit
          : second === 3 && third === 4 ? pair34Fit
            : second === 4 && third === 2 ? pair24Fit
              : pair34Fit;
  const multiplier = HARD_RACE_V2_WEIGHTS.combo_multiplier[combo] || 1;
  return secondWeights[second] * thirdWeights[third] * Math.max(0.01, (pairFit || 45) / 100) * multiplier;
}

function computeScores(normalized, sourceSummary) {
  const lanes = new Map((normalized?.lanes || []).map((row) => [row.lane, row]));
  const boat1 = lanes.get(1);
  const lane2 = lanes.get(2);
  const lane3 = lanes.get(3);
  const lane4 = lanes.get(4);
  const lane5 = lanes.get(5);
  const lane6 = lanes.get(6);
  const missingPrimary = missingCoreBoatraceFields(normalized);
  const missingFieldDetails = buildMissingFieldDetails(normalized);

  if (!boat1 || missingPrimary.length > 0) {
    const missingFields = !boat1 ? ["race.boat1"] : missingPrimary;
    const emptyScores = {
      hard_race_score: null,
      boat1_escape_trust: null,
      opponent_234_fit: null,
      pair23_fit: null,
      pair24_fit: null,
      pair34_fit: null,
      outside_break_risk: null,
      kill_escape_risk: null,
      shape_shuffle_risk: null,
      makuri_risk: null,
      box_hit_score: null,
      shape_focus_score: null,
      fixed1234_total_probability: null,
      top4_fixed1234_probability: null,
      fixed1234_shape_concentration: null,
      head_prob_1: null,
      head_prob_2: null,
      head_prob_3: null,
      head_prob_4: null,
      head_prob_5: null,
      head_prob_6: null,
      outside_head_risk: null,
      outside_2nd_risk: null,
      outside_3rd_risk: null,
      outside_box_break_risk: null,
      p_123: null,
      p_124: null,
      p_132: null,
      p_134: null,
      p_142: null,
      p_143: null
    };
    const metricDetails = buildMetricStatus({
      computed: {
        scores: emptyScores,
        missing_fields: missingFields
      },
      missingFieldDetails,
      normalized
    });
    return {
      scores: emptyScores,
      features: {},
      fixed1234_matrix: {},
      fixed1234_top4: [],
      suggested_shape: null,
      data_status: "DATA_ERROR",
      confidence_status: "DATA_ERROR",
      decision: "DATA_ERROR",
      decision_reason: "missing source data",
      missing_fields: missingFields,
      missing_field_details: missingFieldDetails,
      metric_status: metricDetails,
      fallback_used: { used: false, fields: [], details: {} },
      head_candidates: [],
      head_opponents: [],
      head_candidate_ranking: [],
      outside_danger_scenarios: [],
      hard_mode: { active: false },
      open_mode: { active: false }
    };
  }

  const fallbackTracker = buildFallbackTracker();
  const boat1Features = boat1.features;
  const venueInsideBias = normalized?.venue?.inside_bias?.value !== null ? normalized.venue.inside_bias.value * 100 : null;

  const fit2 = laneFitScore(lane2, 2);
  const fit3 = laneFitScore(lane3, 3);
  const fit4 = laneFitScore(lane4, 4);
  const fit5 = laneFitScore(lane5, 4);
  const fit6 = laneFitScore(lane6, 4);

  const lane1BaseStrength = weightedAverage([
    { value: normalizedPercent(boat1Features.local_win_rate.value, 4, 8.5), weight: 0.35 },
    { value: normalizedPercent(boat1Features.national_win_rate.value, 4, 8.5), weight: 0.25 },
    { value: normalizedPercent(boat1Features.motor_2ren.value, 20, 60), weight: 0.18 },
    { value: normalizedPercent(boat1Features.boat_2ren.value, 20, 60), weight: 0.12 },
    { value: normalizedPercent(venueInsideBias, 50, 76), weight: 0.1 }
  ]);
  const stEdge = weightedAverage([
    { value: invertNormalized(boat1Features.avg_st.value, 0.11, 0.24), weight: 0.7 },
    { value: normalizedPercent(weightedAverage([
      { value: lane2?.features?.avg_st?.value ?? null, weight: 0.4 },
      { value: lane3?.features?.avg_st?.value ?? null, weight: 0.35 },
      { value: lane4?.features?.avg_st?.value ?? null, weight: 0.25 }
    ]), 0.11, 0.24), weight: 0.3 }
  ]);
  const startStability = weightedAverage([
    { value: invertNormalized((boat1Features.f_count.value || 0) * 0.75 + (boat1Features.l_count.value || 0) * 0.4, 0, 2.5), weight: 0.55 },
    { value: invertNormalized(boat1Features.avg_st.value, 0.11, 0.24), weight: 0.45 }
  ]);
  const motorBoatEdge = weightedAverage([
    { value: normalizedPercent(boat1Features.motor_2ren.value, 20, 60), weight: 0.5 },
    { value: normalizedPercent(boat1Features.boat_2ren.value, 20, 60), weight: 0.3 },
    { value: normalizedPercent(boat1Features.motor_3ren.value, 25, 70), weight: 0.12 },
    { value: normalizedPercent(boat1Features.boat_3ren.value, 25, 70), weight: 0.08 }
  ]);
  const localCourseBias = normalizedPercent(venueInsideBias, 50, 76);
  const flSafety = weightedAverage([
    { value: invertNormalized(boat1Features.f_count.value || 0, 0, 2), weight: 0.7 },
    { value: invertNormalized(boat1Features.l_count.value || 0, 0, 2), weight: 0.3 }
  ]);

  const pair23Primary = round(pairFitScore({
    leftFit: fit2,
    rightFit: fit3,
    leftSupport: [lane2?.features?.sashi_rate?.value, lane2?.features?.stability_rate?.value, lane2?.features?.lane_3rentai_rate?.value],
    rightSupport: [lane3?.features?.makurisashi_rate?.value, lane3?.features?.lane_3rentai_rate?.value, lane3?.features?.motor_3ren?.value]
  }), 1);
  const pair24Primary = round(pairFitScore({
    leftFit: fit2,
    rightFit: fit4,
    leftSupport: [lane2?.features?.sashi_rate?.value, lane2?.features?.lane_3rentai_rate?.value, lane2?.features?.motor_2ren?.value],
    rightSupport: [lane4?.features?.breakout_rate?.value, lane4?.features?.lane_3rentai_rate?.value, lane4?.features?.stability_rate?.value]
  }), 1);
  const pair34Primary = round(pairFitScore({
    leftFit: fit3,
    rightFit: fit4,
    leftSupport: [lane3?.features?.makuri_rate?.value, lane3?.features?.makurisashi_rate?.value, lane3?.features?.lane_3rentai_rate?.value],
    rightSupport: [lane4?.features?.breakout_rate?.value, lane4?.features?.stability_rate?.value, lane4?.features?.lane_3rentai_rate?.value]
  }), 1);
  const pair23Resolved = resolveMetricWithFallback({
    field: "pair23_fit",
    primaryValue: pair23Primary,
    fallbackValue: fallbackPairFit({ laneA: lane2, laneB: lane3, boat1 }),
    fallbackFormula: "boatrace course_3rentai + avgST + motor + boat1 escape affinity",
    tracker: fallbackTracker
  });
  const pair24Resolved = resolveMetricWithFallback({
    field: "pair24_fit",
    primaryValue: pair24Primary,
    fallbackValue: fallbackPairFit({ laneA: lane2, laneB: lane4, boat1 }),
    fallbackFormula: "boatrace course_3rentai + avgST + motor + boat1 escape affinity",
    tracker: fallbackTracker
  });
  const pair34Resolved = resolveMetricWithFallback({
    field: "pair34_fit",
    primaryValue: pair34Primary,
    fallbackValue: fallbackPairFit({ laneA: lane3, laneB: lane4, boat1 }),
    fallbackFormula: "boatrace course_3rentai + avgST + motor + boat1 escape affinity",
    tracker: fallbackTracker
  });
  const pair23Fit = pair23Resolved.value;
  const pair24Fit = pair24Resolved.value;
  const pair34Fit = pair34Resolved.value;

  const lane2SecondRemain = round(scoreBlend([
    { value: fit2, weight: 0.29 },
    { value: lane2?.features?.sashi_rate?.value, weight: 0.27 },
    { value: lane2?.features?.lane_3rentai_rate?.value, weight: 0.18 },
    { value: lane2?.features?.stability_rate?.value, weight: 0.12 },
    { value: lane2?.features?.motor_2ren?.value, weight: 0.14 }
  ]), 1);
  const lane3AttackRemain = round(scoreBlend([
    { value: fit3, weight: 0.23 },
    { value: pair23Fit, weight: 0.12 },
    { value: lane3?.features?.makuri_rate?.value, weight: 0.24 },
    { value: lane3?.features?.makurisashi_rate?.value, weight: 0.2 },
    { value: lane3?.features?.lane_3rentai_rate?.value, weight: 0.11 },
    { value: inverseStScore(lane3?.features?.avg_st?.value ?? null), weight: 0.1 }
  ]), 1);
  const lane4DevelopRemain = round(scoreBlend([
    { value: fit4, weight: 0.2 },
    { value: pair24Fit, weight: 0.18 },
    { value: pair34Fit, weight: 0.16 },
    { value: lane4?.features?.breakout_rate?.value, weight: 0.22 },
    { value: lane4?.features?.lane_3rentai_rate?.value, weight: 0.14 },
    { value: lane4?.features?.stability_rate?.value, weight: 0.1 }
  ]), 1);
  const pairSupportFit = round(scoreBlend([
    { value: pair23Fit, weight: 0.34 },
    { value: pair24Fit, weight: 0.33 },
    { value: pair34Fit, weight: 0.33 }
  ]), 1);
  const opponent234Fit = round(scoreBlend([
    { value: pairSupportFit, weight: 0.58 },
    { value: lane2SecondRemain, weight: 0.14 },
    { value: lane3AttackRemain, weight: 0.14 },
    { value: lane4DevelopRemain, weight: 0.14 }
  ]), 1);

  const killEscapePrimary = round(scoreBlend([
    { value: normalizedPercent(lane3?.features?.makuri_rate?.value ?? null, 20, 80) * 100, weight: 0.28 },
    { value: normalizedPercent(lane3?.features?.makurisashi_rate?.value ?? null, 15, 75) * 100, weight: 0.14 },
    { value: normalizedPercent(lane4?.features?.breakout_rate?.value ?? null, 20, 80) * 100, weight: 0.2 },
    { value: normalizedPercent(lane3?.features?.motor_2ren?.value ?? null, 20, 60) * 100, weight: 0.08 },
    { value: normalizedPercent(lane4?.features?.motor_2ren?.value ?? null, 20, 60) * 100, weight: 0.08 },
    { value: normalizedPercent(lane3?.features?.course_3rentai_rate?.value ?? null, 20, 80) * 100, weight: 0.08 },
    { value: normalizedPercent(lane4?.features?.course_3rentai_rate?.value ?? null, 20, 80) * 100, weight: 0.08 },
    { value: invertNormalized(boat1Features.avg_st.value, 0.11, 0.24) === null ? null : (1 - invertNormalized(boat1Features.avg_st.value, 0.11, 0.24)) * 100, weight: 0.06 }
  ]), 1);
  const killEscapeFallback = round(scoreBlend([
    { value: lane3AttackRemain, weight: 0.36 },
    { value: lane4DevelopRemain, weight: 0.32 },
    { value: fit5, weight: 0.08 },
    { value: fit6, weight: 0.08 },
    { value: normalizedPercent(lane5?.features?.course_3rentai_rate?.value ?? null, 20, 75) * 100, weight: 0.08 },
    { value: normalizedPercent(lane6?.features?.course_3rentai_rate?.value ?? null, 20, 75) * 100, weight: 0.08 }
  ]), 1);
  const killEscapeResolved = resolveMetricWithFallback({
    field: "kill_escape_risk",
    primaryValue: killEscapePrimary,
    fallbackValue: killEscapeFallback,
    fallbackFormula: "lane3 attack + lane4 develop + outside head entry pressure",
    tracker: fallbackTracker
  });
  const killEscapeRisk = killEscapeResolved.value;

  const shapeShufflePrimary = round(scoreBlend([
    { value: normalizedPercent(lane3?.features?.makurisashi_rate?.value ?? null, 15, 75) * 100, weight: 0.26 },
    { value: normalizedPercent(lane4?.features?.breakout_rate?.value ?? null, 20, 80) * 100, weight: 0.24 },
    { value: lane3?.features?.lane_3rentai_rate?.value ?? null, weight: 0.12 },
    { value: lane4?.features?.lane_3rentai_rate?.value ?? null, weight: 0.12 },
    { value: clamp(0, 100, Math.abs((fit3 || 50) - (fit2 || 50)) * 1.1), weight: 0.14 },
    { value: clamp(0, 100, Math.abs((fit4 || 50) - (fit2 || 50)) * 1.1), weight: 0.12 }
  ]), 1);
  const shapeShuffleFallback = round(scoreBlend([
    { value: clamp(0, 100, Math.abs((pair23Fit || 50) - (pair24Fit || 50)) * 1.15), weight: 0.26 },
    { value: clamp(0, 100, Math.abs((pair24Fit || 50) - (pair34Fit || 50)) * 1.05), weight: 0.22 },
    { value: clamp(0, 100, Math.abs((fit3 || 50) - (fit4 || 50))), weight: 0.18 },
    { value: lane3?.features?.course_3rentai_rate?.value ?? null, weight: 0.16 },
    { value: lane4?.features?.course_3rentai_rate?.value ?? null, weight: 0.18 }
  ]), 1);
  const shapeShuffleResolved = resolveMetricWithFallback({
    field: "shape_shuffle_risk",
    primaryValue: shapeShufflePrimary,
    fallbackValue: shapeShuffleFallback,
    fallbackFormula: "lane3 attack + lane4 develop + outside head entry pressure",
    tracker: fallbackTracker
  });
  const shapeShuffleRisk = shapeShuffleResolved.value;

  const outsideHeadRisk = round(scoreBlend([
    { value: fit5, weight: 0.32 },
    { value: fit6, weight: 0.32 },
    { value: lane5?.features?.breakout_rate?.value, weight: 0.1 },
    { value: lane6?.features?.breakout_rate?.value, weight: 0.1 },
    { value: inverseStScore(lane5?.features?.avg_st?.value ?? null), weight: 0.08 },
    { value: inverseStScore(lane6?.features?.avg_st?.value ?? null), weight: 0.08 }
  ]), 1);
  const outsideSecondRisk = round(clamp(
    0,
    1,
    weightedAverage([
      { value: normalizedPercent(weightedAverage([
        { value: lane5?.features?.course_3rentai_rate?.value ?? null, weight: 0.34 },
        { value: lane6?.features?.course_3rentai_rate?.value ?? null, weight: 0.34 },
        { value: inverseStScore(lane5?.features?.avg_st?.value ?? null), weight: 0.16 },
        { value: inverseStScore(lane6?.features?.avg_st?.value ?? null), weight: 0.16 }
      ]), 20, 80), weight: 0.72 },
      { value: normalizedPercent(weightedAverage([
        { value: lane5?.features?.motor_2ren?.value ?? null, weight: 0.5 },
        { value: lane6?.features?.motor_2ren?.value ?? null, weight: 0.5 }
      ]), 20, 60), weight: 0.28 }
    ]) || 0
  ), 4);
  const outsideThirdRisk = round(clamp(
    0,
    1,
    weightedAverage([
      { value: normalizedPercent(weightedAverage([
        { value: lane5?.features?.lane_3rentai_rate?.value ?? null, weight: 0.4 },
        { value: lane6?.features?.lane_3rentai_rate?.value ?? null, weight: 0.4 },
        { value: lane5?.features?.stability_rate?.value ?? null, weight: 0.1 },
        { value: lane6?.features?.stability_rate?.value ?? null, weight: 0.1 }
      ]), 20, 80), weight: 0.8 },
      { value: normalizedPercent(weightedAverage([
        { value: lane5?.features?.boat_2ren?.value ?? null, weight: 0.5 },
        { value: lane6?.features?.boat_2ren?.value ?? null, weight: 0.5 }
      ]), 20, 60), weight: 0.2 }
    ]) || 0
  ), 4);
  const outsideBoxBreakRisk = round(clamp(
    0,
    1,
    weightedAverage([
      { value: normalizedPercent(outsideHeadRisk, 15, 75), weight: 0.42 },
      { value: outsideSecondRisk, weight: 0.33 },
      { value: outsideThirdRisk, weight: 0.1 },
      { value: normalizedPercent(weightedAverage([
        { value: lane5?.features?.zentsuke_tendency?.value ?? null, weight: 0.5 },
        { value: lane6?.features?.zentsuke_tendency?.value ?? null, weight: 0.5 }
      ]), 0, 100), weight: 0.07 },
      { value: normalizedPercent(clamp(0, 100, 100 - ((pair23Fit || 50) * 0.45 + (pair24Fit || 50) * 0.55)), 0, 100), weight: 0.08 }
    ]) || 0
  ), 4);
  const outsideBreakRisk = round(clamp(
    0,
    1,
    weightedAverage([
      { value: normalizedPercent(outsideHeadRisk, 15, 75), weight: 0.45 },
      { value: outsideSecondRisk, weight: 0.35 },
      { value: outsideThirdRisk, weight: 0.08 },
      { value: outsideBoxBreakRisk, weight: 0.12 }
    ]) || 0
  ), 4);

  const laneDelayRisk = weightedAverage([
    { value: normalizedPercent(lane2?.features?.late_start_rate?.value ?? null, 0, 30), weight: 0.34 },
    { value: normalizedPercent(lane3?.features?.late_start_rate?.value ?? null, 0, 30), weight: 0.33 },
    { value: normalizedPercent(lane4?.features?.late_start_rate?.value ?? null, 0, 30), weight: 0.33 }
  ]);
  const p1EscapeScore = weightedAverage([
    { value: lane1BaseStrength, weight: 0.31 },
    { value: stEdge, weight: 0.17 },
    { value: startStability, weight: 0.11 },
    { value: motorBoatEdge, weight: 0.12 },
    { value: localCourseBias, weight: 0.1 },
    { value: flSafety, weight: 0.07 },
    { value: laneDelayRisk === null ? null : 1 - laneDelayRisk, weight: 0.05 },
    { value: killEscapeRisk === null ? null : 1 - (killEscapeRisk / 100), weight: 0.21 },
    { value: outsideHeadRisk === null ? null : 1 - (outsideHeadRisk / 100), weight: 0.06 }
  ]);
  const p1Escape = round(clamp(0, 1, p1EscapeScore ?? 0), 4);
  const boat1EscapeTrust = round((p1Escape || 0) * 100, 1);
  const makuriRisk = round(scoreBlend([
    { value: killEscapeRisk, weight: 0.64 },
    { value: shapeShuffleRisk, weight: 0.36 }
  ]), 1);
  const boxHitScore = round(clamp(
    0,
    1,
    (p1Escape || 0) * clamp(0.35, 1, 0.68 + (normalizedPercent(pairSupportFit, 45, 75) || 0) * 0.22 + (normalizedPercent(opponent234Fit, 45, 75) || 0) * 0.05 - (outsideBoxBreakRisk || 0) * 0.18 - ((killEscapeRisk || 0) / 100) * 0.11)
  ), 4);

  const headProbScoreMap = {
    1: Math.max(0.0001, (p1Escape || 0) * 100),
    2: Math.max(0.0001, (buildHeadCandidate(lane2, fit2, venueInsideBias || 62)?.score || 0) * (1 + (lane2SecondRemain || 0) / 220)),
    3: Math.max(0.0001, (buildHeadCandidate(lane3, fit3, venueInsideBias || 62)?.score || 0) * (1 + (lane3AttackRemain || 0) / 180)),
    4: Math.max(0.0001, (buildHeadCandidate(lane4, fit4, venueInsideBias || 62)?.score || 0) * (1 + (lane4DevelopRemain || 0) / 200)),
    5: Math.max(0.0001, (buildHeadCandidate(lane5, fit5, venueInsideBias || 62)?.score || 0) * (1 + (outsideHeadRisk || 0) / 160)),
    6: Math.max(0.0001, (buildHeadCandidate(lane6, fit6, venueInsideBias || 62)?.score || 0) * (1 + (outsideHeadRisk || 0) / 160))
  };
  const headProbMap = toProbabilityMap(headProbScoreMap);

  const secondWeights = normalizeWeights({
    2: Math.max(0.0001, lane2SecondRemain || 0.0001),
    3: Math.max(0.0001, lane3AttackRemain || 0.0001),
    4: Math.max(0.0001, lane4DevelopRemain || 0.0001)
  });
  const thirdConditionalWeights = {
    "2": normalizeWeights({
      3: Math.max(0.0001, weightedAverage([
        { value: pair23Fit, weight: 0.44 },
        { value: fit3, weight: 0.24 },
        { value: lane3?.features?.lane_3rentai_rate?.value ?? null, weight: 0.18 },
        { value: lane3?.features?.makurisashi_rate?.value ?? null, weight: 0.14 }
      ]) || 0.0001),
      4: Math.max(0.0001, weightedAverage([
        { value: pair24Fit, weight: 0.48 },
        { value: fit4, weight: 0.22 },
        { value: lane4?.features?.lane_3rentai_rate?.value ?? null, weight: 0.18 },
        { value: lane4?.features?.breakout_rate?.value ?? null, weight: 0.12 }
      ]) || 0.0001)
    }),
    "3": normalizeWeights({
      2: Math.max(0.0001, weightedAverage([
        { value: pair23Fit, weight: 0.46 },
        { value: fit2, weight: 0.22 },
        { value: lane2?.features?.lane_3rentai_rate?.value ?? null, weight: 0.18 },
        { value: lane2?.features?.stability_rate?.value ?? null, weight: 0.14 }
      ]) || 0.0001),
      4: Math.max(0.0001, weightedAverage([
        { value: pair34Fit, weight: 0.48 },
        { value: fit4, weight: 0.22 },
        { value: lane4?.features?.lane_3rentai_rate?.value ?? null, weight: 0.16 },
        { value: lane4?.features?.breakout_rate?.value ?? null, weight: 0.14 }
      ]) || 0.0001)
    }),
    "4": normalizeWeights({
      2: Math.max(0.0001, weightedAverage([
        { value: pair24Fit, weight: 0.5 },
        { value: fit2, weight: 0.22 },
        { value: lane2?.features?.lane_3rentai_rate?.value ?? null, weight: 0.16 },
        { value: lane2?.features?.sashi_rate?.value ?? null, weight: 0.12 }
      ]) || 0.0001),
      3: Math.max(0.0001, weightedAverage([
        { value: pair34Fit, weight: 0.48 },
        { value: fit3, weight: 0.2 },
        { value: lane3?.features?.lane_3rentai_rate?.value ?? null, weight: 0.16 },
        { value: lane3?.features?.makurisashi_rate?.value ?? null, weight: 0.16 }
      ]) || 0.0001)
    })
  };

  const fixed1234Matrix = Object.fromEntries(
    FIXED_COMBOS.map((combo) => {
      const [, second, third] = combo.split("-").map((item) => Number(item));
      const probability = (p1Escape || 0) * (secondWeights[second] || 0) * (thirdConditionalWeights[String(second)]?.[third] || 0);
      return [combo, round(probability, 4)];
    })
  );
  const p123 = fixed1234Matrix["1-2-3"] || 0;
  const p124 = fixed1234Matrix["1-2-4"] || 0;
  const p132 = fixed1234Matrix["1-3-2"] || 0;
  const p134 = fixed1234Matrix["1-3-4"] || 0;
  const p142 = fixed1234Matrix["1-4-2"] || 0;
  const p143 = fixed1234Matrix["1-4-3"] || 0;
  const fixed1234Top4 = Object.entries(fixed1234Matrix)
    .map(([combo, probability]) => ({ combo, probability }))
    .sort((a, b) => (b.probability || 0) - (a.probability || 0))
    .slice(0, FIXED_TOP4_COUNT);
  const fixedTop4Set = new Set(fixed1234Top4.map((row) => row.combo));
  const top4Fixed1234Probability = round(fixed1234Top4.reduce((sum, row) => sum + (row.probability || 0), 0), 4);
  const fixed1234TotalProbability = round(Object.values(fixed1234Matrix).reduce((sum, value) => sum + (value || 0), 0), 4);
  const fixed1234ShapeConcentration = round(fixed1234TotalProbability > 0 ? top4Fixed1234Probability / fixed1234TotalProbability : null, 4);
  const refinedShapeFocusScore = fixed1234ShapeConcentration;
  const suggestedShape =
    p124 >= p123 && p124 >= p134 && p124 >= p142
      ? "1-24-234"
      : fixed1234Top4[0]?.combo === "1-4-2" || fixed1234Top4[0]?.combo === "1-4-3"
        ? "1-24-234"
        : fixed1234Top4[0]?.combo === "1-3-2" || fixed1234Top4[0]?.combo === "1-3-4"
          ? "1-34-234"
          : "1-23-234";

  const supplementMissing = [];
  for (const lane of normalized?.lanes || []) {
    for (const key of ["sashi_rate", "makuri_rate", "makurisashi_rate", "stability_rate", "breakout_rate", "course_3rentai_rate", "lane_3rentai_rate"]) {
      if (lane?.features?.[key]?.value === null) supplementMissing.push(`lane${lane.lane}.${key}`);
    }
  }
  const criticalPartialFields = [];
  if (pair23Resolved.sourceType === "missing" || pair23Resolved.fallbackUsed) criticalPartialFields.push("pair23_fit");
  if (pair24Resolved.sourceType === "missing" || pair24Resolved.fallbackUsed) criticalPartialFields.push("pair24_fit");
  if (pair34Resolved.sourceType === "missing" || pair34Resolved.fallbackUsed) criticalPartialFields.push("pair34_fit");
  if (killEscapeResolved.sourceType === "missing" || killEscapeResolved.fallbackUsed) criticalPartialFields.push("kill_escape_risk");
  if (shapeShuffleResolved.sourceType === "missing" || shapeShuffleResolved.fallbackUsed) criticalPartialFields.push("shape_shuffle_risk");
  const unresolvedCriticalFields = [];
  if (pair23Resolved.sourceType === "missing") unresolvedCriticalFields.push("pair23_fit");
  if (pair24Resolved.sourceType === "missing") unresolvedCriticalFields.push("pair24_fit");
  if (pair34Resolved.sourceType === "missing") unresolvedCriticalFields.push("pair34_fit");
  if (killEscapeResolved.sourceType === "missing") unresolvedCriticalFields.push("kill_escape_risk");
  if (shapeShuffleResolved.sourceType === "missing") unresolvedCriticalFields.push("shape_shuffle_risk");
  const dataStatus =
    unresolvedCriticalFields.length > 0
      ? "DATA_ERROR"
      : supplementMissing.length > 0 || criticalPartialFields.length > 0
        ? "PARTIAL"
        : "OK";
  const mode = (headProbMap[1] || 0) < 0.44 || ((killEscapeRisk || 0) / 100) > 0.58 || (outsideBoxBreakRisk || 0) > 0.34 ? "OPEN" : "HARD";

  let decision = "SKIP";
  const buy4Thresholds = HARD_RACE_V2_DECISION_THRESHOLDS.buy4;
  const buy6Thresholds = HARD_RACE_V2_DECISION_THRESHOLDS.buy6;
  const borderlineThresholds = HARD_RACE_V2_DECISION_THRESHOLDS.borderline;
  const pairGateStrong = (pairSupportFit || 0) >= 56 && (pair24Fit || 0) >= 50;
  const pairGateBorderline = (pairSupportFit || 0) >= 52;
  const top2Fixed1234Probability = round(
    Object.values(fixed1234Matrix)
      .sort((a, b) => (b || 0) - (a || 0))
      .slice(0, 2)
      .reduce((sum, value) => sum + (value || 0), 0),
    4
  );
  const buy6GatePassed =
    (headProbMap[1] || 0) >= buy6Thresholds.p1_escape_min &&
    (fixed1234TotalProbability || 0) >= buy6Thresholds.fixed1234_total_probability_min &&
    (outsideBreakRisk || 1) <= buy6Thresholds.outside_break_risk_max &&
    (outsideBoxBreakRisk || 1) <= 0.3 &&
    pairGateStrong &&
    top2Fixed1234Probability >= 0.14;

  if (
    buy6GatePassed &&
    (top4Fixed1234Probability || 0) >= buy4Thresholds.top4_fixed1234_probability_min &&
    (fixed1234ShapeConcentration || 0) >= buy4Thresholds.top4_share_within_fixed1234_min
  ) {
    decision = "BUY-4";
  } else if (buy6GatePassed) {
    decision = "BUY-6";
  } else if (
    (headProbMap[1] || 0) >= borderlineThresholds.p1_escape_min &&
    (fixed1234TotalProbability || 0) >= borderlineThresholds.fixed1234_total_probability_min &&
    pairGateBorderline
  ) {
    decision = "BORDERLINE";
  }
  if (mode === "OPEN") decision = "SKIP";
  if (dataStatus === "DATA_ERROR") decision = "DATA_ERROR";

  const actualCombo = normalized?.result?.combo || null;
  const yBox6 = actualCombo && FIXED_COMBOS.includes(actualCombo) ? 1 : 0;
  const yTop4 = actualCombo && fixedTop4Set.has(actualCombo) ? 1 : 0;
  const yBuy6 = yBox6;
  const yBuy4 = yTop4;
  const falseNegativeCase = false;

  const headCandidates = Object.entries(headProbMap)
    .map(([lane, probability]) => ({ lane: Number(lane), probability, score: round(probability * 100, 1) }))
    .sort((a, b) => (b.probability || 0) - (a.probability || 0));
  const openHeadCandidates = headCandidates.slice(0, 2);
  const openOpponentCandidates = (normalized?.lanes || [])
    .filter((laneRow) => !openHeadCandidates.some((head) => head.lane === laneRow.lane))
    .map((laneRow) => ({
      lane: laneRow.lane,
      probability: null,
      score: round(scoreBlend([
        { value: laneFitScore(laneRow, laneRow.lane <= 4 ? laneRow.lane : 4), weight: 0.34 },
        { value: laneRow?.features?.course_3rentai_rate?.value ?? null, weight: 0.22 },
        { value: laneRow?.features?.lane_3rentai_rate?.value ?? null, weight: 0.18 },
        { value: inverseStScore(laneRow?.features?.avg_st?.value ?? null), weight: 0.14 },
        { value: laneRow?.features?.motor_2ren?.value ?? null, weight: 0.12 }
      ]), 1)
    }))
    .sort((a, b) => (b.score || 0) - (a.score || 0))
    .slice(0, 3);

  const decisionReason =
    dataStatus === "DATA_ERROR"
      ? "critical hard-race metrics could not be determined"
      : mode === "OPEN"
      ? "Open Race / Chaos Mode triggered"
      : dataStatus === "PARTIAL"
        ? "missing source data"
        : decision === "BUY-4"
          ? "hard mode rank strong and six-ticket concentration passed"
        : decision === "BUY-6"
            ? "hard mode six-ticket gate passed"
            : decision === "BORDERLINE"
              ? "box fit is present but buy gate is still short"
              : "1-234-234 fit is not strong enough";

  const missingFields = [
    ...supplementMissing,
    ...criticalPartialFields.map((field) => `computed.${field}`),
    ...unresolvedCriticalFields.map((field) => `unresolved.${field}`)
  ];
  const fallbackSummary = {
    used: fallbackTracker.used,
    fields: Object.keys(fallbackTracker.byField),
    details: fallbackTracker.byField
  };
  const outsideDangerScenarios = [
    { label: "5,6 head", risk: round((outsideHeadRisk || 0) / 100, 4) },
    { label: "5,6 2nd", risk: outsideSecondRisk },
    { label: "5,6 3rd", risk: outsideThirdRisk },
    { label: "box break", risk: outsideBoxBreakRisk }
  ].sort((a, b) => (b.risk || 0) - (a.risk || 0));
  const raceRankScore = round(scoreBlend([
    { value: (headProbMap[1] || 0) * 100, weight: 0.24 },
    { value: pairSupportFit, weight: 0.26 },
    { value: fixed1234TotalProbability * 100, weight: 0.18 },
    { value: top4Fixed1234Probability * 100, weight: 0.14 },
    { value: (1 - (outsideBoxBreakRisk || 0)) * 100, weight: 0.1 },
    { value: (1 - (killEscapeRisk || 0) / 100) * 100, weight: 0.08 }
  ]), 1);
  const hardRaceRank =
    dataStatus === "DATA_ERROR"
      ? "DATA_ERROR"
      : raceRankScore >= 58 && fixed1234TotalProbability >= 0.26
        ? "A"
        : raceRankScore >= 50 && fixed1234TotalProbability >= 0.2
          ? "B"
          : "SKIP";
  const operationalPick =
    dataStatus === "DATA_ERROR"
      ? "見送り"
      : mode === "OPEN"
        ? "穴モード"
        : ["BUY-4", "BUY-6"].includes(decision)
          ? "買う"
          : "見送り";
  const hardRaceScore = round(scoreBlend([
    { value: boat1EscapeTrust, weight: 0.34 },
    { value: pairSupportFit, weight: 0.28 },
    { value: opponent234Fit, weight: 0.08 },
    { value: fixed1234TotalProbability * 100, weight: 0.18 },
    { value: top4Fixed1234Probability * 100, weight: 0.06 },
    { value: (1 - (outsideBreakRisk || 0)) * 100, weight: 0.06 }
  ]), 1);
  const computed = {
    scores: {
      hard_race_score: hardRaceScore,
      boat1_escape_trust: boat1EscapeTrust,
      opponent_234_fit: opponent234Fit,
      pair23_fit: pair23Fit,
      pair24_fit: pair24Fit,
      pair34_fit: pair34Fit,
      outside_break_risk: outsideBreakRisk,
      kill_escape_risk: killEscapeRisk,
      shape_shuffle_risk: shapeShuffleRisk,
      makuri_risk: makuriRisk,
      box_hit_score: boxHitScore,
      shape_focus_score: refinedShapeFocusScore,
      fixed1234_total_probability: fixed1234TotalProbability,
      top4_fixed1234_probability: top4Fixed1234Probability,
      fixed1234_shape_concentration: fixed1234ShapeConcentration,
      head_prob_1: headProbMap[1] || 0,
      head_prob_2: headProbMap[2] || 0,
      head_prob_3: headProbMap[3] || 0,
      head_prob_4: headProbMap[4] || 0,
      head_prob_5: headProbMap[5] || 0,
      head_prob_6: headProbMap[6] || 0,
      outside_head_risk: round((outsideHeadRisk || 0) / 100, 4),
      outside_2nd_risk: outsideSecondRisk,
      outside_3rd_risk: outsideThirdRisk,
      outside_box_break_risk: outsideBoxBreakRisk,
      p_123: p123,
      p_124: p124,
      p_132: p132,
      p_134: p134,
      p_142: p142,
      p_143: p143
    },
    features: {
      lane2_fit: round(fit2, 2),
      lane3_fit: round(fit3, 2),
      lane4_fit: round(fit4, 2),
      pair_support_fit: pairSupportFit,
      race_rank_score: raceRankScore,
      lane2_second_remain: lane2SecondRemain,
      lane3_attack_remain: lane3AttackRemain,
      lane4_develop_remain: lane4DevelopRemain,
      p1_escape: p1Escape,
      p1_escape_score: boat1EscapeTrust,
      lane1_base_strength: round((lane1BaseStrength || 0) * 100, 1),
      st_edge: round((stEdge || 0) * 100, 1),
      start_stability: round((startStability || 0) * 100, 1),
      motor_boat_edge: round((motorBoatEdge || 0) * 100, 1),
      local_course_bias: round((localCourseBias || 0) * 100, 1),
      fl_safety: round((flSafety || 0) * 100, 1),
      outside_head_risk: round((outsideHeadRisk || 0) / 100, 4),
      outside_2nd_risk: outsideSecondRisk,
      outside_3rd_risk: outsideThirdRisk,
      outside_box_break_risk: outsideBoxBreakRisk,
      top2_fixed1234_probability: top2Fixed1234Probability,
      supplement_missing_count: supplementMissing.length,
      top4_share_within_fixed1234: fixed1234ShapeConcentration,
      second_candidate_scores: {
        lane2_sashi_nokori: lane2SecondRemain,
        lane3_seme_nokori: lane3AttackRemain,
        lane4_tenkai_nokori: lane4DevelopRemain
      },
      conditional_probabilities: {
        p_1st_1: p1Escape,
        p_2nd_given_1st1: secondWeights,
        p_3rd_given_1st1_2nd: thirdConditionalWeights
      },
      head_probabilities: headProbMap,
      evaluation_targets: {
        y_box6: yBox6,
        y_top4: yTop4,
        y_buy6: yBuy6,
        y_buy4: yBuy4
      },
      false_negative_review: {
        actual_combo: actualCombo,
        false_negative_case: falseNegativeCase,
        matched_pattern: actualCombo === "1-2-4"
      },
      boat1_escape_breakdown: {
        formula: "0.30*lane1_base_strength + 0.18*st_edge + 0.14*start_stability + 0.10*motor_boat_edge + 0.10*local_course_bias + 0.08*fl_safety - 0.18*kill_escape_risk - 0.07*outside_head_risk - 0.05*shape_shuffle_risk",
        source_summary: {
          pair23_fit: summarizeMetricSource("kyoteibiyori/boatrace mix", pair23Resolved),
          pair24_fit: summarizeMetricSource("kyoteibiyori/boatrace mix", pair24Resolved),
          pair34_fit: summarizeMetricSource("kyoteibiyori/boatrace mix", pair34Resolved),
          kill_escape_risk: summarizeMetricSource("kyoteibiyori/boatrace mix", killEscapeResolved),
          shape_shuffle_risk: summarizeMetricSource("kyoteibiyori/boatrace mix", shapeShuffleResolved)
        }
      },
      operational_policy: {
        candidate_decisions: ["BUY-4", "BUY-6", "BORDERLINE", "SKIP"],
        mode,
        operational_pick: operationalPick
      },
      decision_gates: {
        pair_support_fit: pairSupportFit,
        pair_gate_strong: pairGateStrong,
        pair_gate_borderline: pairGateBorderline,
        opponent_234_fit_support: opponent234Fit
      },
      reviewed_coefficients: {
        weights: HARD_RACE_V2_WEIGHTS,
        thresholds: HARD_RACE_V2_DECISION_THRESHOLDS
      },
      source_summary: {
        ...(sourceSummary || {}),
        hard_race_fallback: fallbackSummary
      }
    },
    fixed1234_matrix: fixed1234Matrix,
    fixed1234_top4: fixed1234Top4,
    suggested_shape: suggestedShape,
    data_status: dataStatus,
    confidence_status: dataStatus,
    decision,
    hard_race_rank: hardRaceRank,
    decision_reason: decisionReason,
    operational_pick: operationalPick,
    missing_fields: missingFields,
    fallback_used: fallbackSummary,
    head_candidate_ranking: headCandidates,
    head_candidates: openHeadCandidates,
    head_opponents: openOpponentCandidates,
    outside_danger_scenarios: outsideDangerScenarios,
    hard_mode: {
      active: mode === "HARD",
      p1_escape: p1Escape,
      head_prob_1: headProbMap[1] || 0,
      fixed1234_total_probability: fixed1234TotalProbability,
      top2_fixed1234_probability: top2Fixed1234Probability,
      top4_fixed1234_probability: top4Fixed1234Probability,
      top4_share_within_fixed1234: fixed1234ShapeConcentration,
      decision
    },
    open_mode: {
      active: mode === "OPEN",
      trigger: mode === "OPEN" ? ((headProbMap[1] || 0) < 0.44 ? "head_prob_1 low" : (outsideBoxBreakRisk || 0) > 0.34 ? "outside_box_break_risk high" : "kill_escape_risk high") : null,
      alert_label: mode === "OPEN" ? "荒れ注意" : null,
      head_candidates: openHeadCandidates,
      head_opponents: openOpponentCandidates
    }
  };
  return {
    ...computed,
    missing_field_details: missingFieldDetails,
    metric_status: buildMetricStatus({
      computed,
      missingFieldDetails,
      normalized
    })
  };
}
export async function buildHardRace1234Response({ data, date, venueId, raceNo, artifactCollector = null }) {
  const artifactDir = buildArtifactDir({ date, venueId, raceNo });
  const startedAt = Date.now();
  let resultFetch = { ok: false, url: null, raw: null, result: null, error: null };
  let normalizedData = null;
  let sourceSummary = null;
  let computed = null;
  let saved = { rawSavedPaths: {}, parsedSavedPaths: {} };
  const fetchedUrls = {
    boatrace: {
      racelist: data?.source?.racelistUrl || null,
      beforeinfo: data?.source?.beforeinfoUrl || null,
      raceresult: null
    },
    kyoteibiyori: {
      primary: data?.source?.kyotei_biyori?.url || null,
      tried_urls: Array.isArray(data?.source?.kyotei_biyori?.tried_urls) ? data.source.kyotei_biyori.tried_urls : []
    }
  };

  try {
    resultFetch = await fetchOfficialRaceResult({ date, venueId, raceNo });
    fetchedUrls.boatrace.raceresult = resultFetch?.url || null;
    normalizedData = normalizeRaceData({ data, resultFetch });
    sourceSummary = buildSourceSummary({ data, normalizedLanes: normalizedData.lanes, resultFetch });
    computed = computeScores(normalizedData, sourceSummary);
    sourceSummary = buildSourceSummary({ data, normalizedLanes: normalizedData.lanes, resultFetch, fallbackSummary: computed.fallback_used });
    saved = saveArtifacts({
      dir: artifactDir,
      raw: {
        ...(artifactCollector?.raw || {}),
        boatrace_raceresult: resultFetch?.raw || null
      },
      fetchedUrls,
      normalizedData,
      scores: computed
    });
  } catch (error) {
    normalizedData = normalizedData || normalizeRaceData({ data, resultFetch });
    sourceSummary = sourceSummary || buildSourceSummary({ data, normalizedLanes: normalizedData.lanes, resultFetch });
    const missingFieldDetails = buildMissingFieldDetails(normalizedData);
    computed = {
      scores: {
        boat1_escape_trust: null,
        opponent_234_fit: null,
        hard_race_score: null,
        pair23_fit: null,
        pair24_fit: null,
        pair34_fit: null,
        outside_break_risk: null,
        kill_escape_risk: null,
        shape_shuffle_risk: null,
        makuri_risk: null,
        box_hit_score: null,
        shape_focus_score: null,
        fixed1234_total_probability: null,
        top4_fixed1234_probability: null,
        fixed1234_shape_concentration: null,
        head_prob_1: null,
        head_prob_2: null,
        head_prob_3: null,
        head_prob_4: null,
        head_prob_5: null,
        head_prob_6: null,
        outside_head_risk: null,
        outside_2nd_risk: null,
        outside_3rd_risk: null,
        outside_box_break_risk: null,
        p_123: null,
        p_124: null,
        p_132: null,
        p_134: null,
        p_142: null,
        p_143: null
      },
      features: {
        source_summary: sourceSummary
      },
      fixed1234_matrix: {},
      fixed1234_top4: [],
      suggested_shape: null,
      data_status: "DATA_ERROR",
      confidence_status: "DATA_ERROR",
      decision: "DATA_ERROR",
      decision_reason: String(error?.message || error || "hard race build failed"),
      missing_fields: ["hard_race_build"],
      fallback_used: { used: false, fields: [], details: {} },
      head_candidates: [],
      head_opponents: [],
      head_candidate_ranking: [],
      outside_danger_scenarios: [],
      hard_mode: { active: false },
      open_mode: { active: false },
      missing_field_details: {
        ...missingFieldDetails,
        hard_race_build: {
          reason: "not calculated",
          source: "hard-race-1234",
          lane: null,
          field: "hard_race_build"
        }
      },
      metric_status: {
        boat1_escape_trust: { value: null, status: "missing", reason: "not calculated" },
        opponent_234_fit: { value: null, status: "missing", reason: "not calculated" },
        hard_race_score: { value: null, status: "missing", reason: "not calculated" },
        pair23_fit: { value: null, status: "missing", reason: "not calculated" },
        pair24_fit: { value: null, status: "missing", reason: "not calculated" },
        pair34_fit: { value: null, status: "missing", reason: "not calculated" },
        kill_escape_risk: { value: null, status: "missing", reason: "not calculated" },
        shape_shuffle_risk: { value: null, status: "missing", reason: "not calculated" },
        makuri_risk: { value: null, status: "missing", reason: "not calculated" },
        outside_break_risk: { value: null, status: "missing", reason: "not calculated" },
        box_hit_score: { value: null, status: "missing", reason: "not calculated" },
        shape_focus_score: { value: null, status: "missing", reason: "not calculated" },
        fixed1234_total_probability: { value: null, status: "missing", reason: "not calculated" },
        top4_fixed1234_probability: { value: null, status: "missing", reason: "not calculated" },
        fixed1234_shape_concentration: { value: null, status: "missing", reason: "not calculated" },
        head_prob_1: { value: null, status: "missing", reason: "not calculated" },
        head_prob_2: { value: null, status: "missing", reason: "not calculated" },
        head_prob_3: { value: null, status: "missing", reason: "not calculated" },
        head_prob_4: { value: null, status: "missing", reason: "not calculated" },
        head_prob_5: { value: null, status: "missing", reason: "not calculated" },
        head_prob_6: { value: null, status: "missing", reason: "not calculated" },
        outside_head_risk: { value: null, status: "missing", reason: "not calculated" },
        outside_2nd_risk: { value: null, status: "missing", reason: "not calculated" },
        outside_3rd_risk: { value: null, status: "missing", reason: "not calculated" },
        outside_box_break_risk: { value: null, status: "missing", reason: "not calculated" },
        p_123: { value: null, status: "missing", reason: "not calculated" },
        p_124: { value: null, status: "missing", reason: "not calculated" },
        p_132: { value: null, status: "missing", reason: "not calculated" },
        p_134: { value: null, status: "missing", reason: "not calculated" },
        p_142: { value: null, status: "missing", reason: "not calculated" },
        p_143: { value: null, status: "missing", reason: "not calculated" }
      }
    };
    sourceSummary = buildSourceSummary({ data, normalizedLanes: normalizedData.lanes, resultFetch, fallbackSummary: computed.fallback_used });
  }

  const fetchTimings = {
    ...(data?.source?.fetch_timings || data?.source?.timings || {}),
    hard_race_total_ms: Date.now() - startedAt
  };
  return {
    race_no: toNum(raceNo, null),
    status: computed.decision === "DATA_ERROR" ? "DATA_ERROR" : "FETCHED",
    data_status: computed.data_status,
    confidence_status: computed.confidence_status || computed.data_status,
    hard_race_score: computed.scores.hard_race_score,
    boat1_escape_trust: computed.scores.boat1_escape_trust,
    opponent_234_fit: computed.scores.opponent_234_fit,
    pair23_fit: computed.scores.pair23_fit,
    pair24_fit: computed.scores.pair24_fit,
    pair34_fit: computed.scores.pair34_fit,
    outside_break_risk: computed.scores.outside_break_risk,
    kill_escape_risk: computed.scores.kill_escape_risk,
    shape_shuffle_risk: computed.scores.shape_shuffle_risk,
    makuri_risk: computed.scores.makuri_risk,
    box_hit_score: computed.scores.box_hit_score,
    shape_focus_score: computed.scores.shape_focus_score,
    fixed1234_total_probability: computed.scores.fixed1234_total_probability,
    top4_fixed1234_probability: computed.scores.top4_fixed1234_probability,
    fixed1234_shape_concentration: computed.scores.fixed1234_shape_concentration,
    head_prob_1: computed.scores.head_prob_1,
    head_prob_2: computed.scores.head_prob_2,
    head_prob_3: computed.scores.head_prob_3,
    head_prob_4: computed.scores.head_prob_4,
    head_prob_5: computed.scores.head_prob_5,
    head_prob_6: computed.scores.head_prob_6,
    outside_head_risk: computed.scores.outside_head_risk,
    outside_2nd_risk: computed.scores.outside_2nd_risk,
    outside_3rd_risk: computed.scores.outside_3rd_risk,
    outside_box_break_risk: computed.scores.outside_box_break_risk,
    p_123: computed.scores.p_123,
    p_124: computed.scores.p_124,
    p_132: computed.scores.p_132,
    p_134: computed.scores.p_134,
    p_142: computed.scores.p_142,
    p_143: computed.scores.p_143,
    fixed1234_matrix: computed.fixed1234_matrix,
    fixed1234_top4: computed.fixed1234_top4,
    suggested_shape: computed.suggested_shape,
    hard_race_rank: computed.hard_race_rank || null,
    operational_pick: computed.operational_pick || null,
    decision: computed.decision,
    decision_reason: computed.decision_reason,
    fallback_used: computed.fallback_used,
    head_candidates: computed.head_candidates,
    head_opponents: computed.head_opponents,
    head_candidate_ranking: computed.head_candidate_ranking,
    outside_danger_scenarios: computed.outside_danger_scenarios,
    hard_mode: computed.hard_mode,
    open_mode: computed.open_mode,
    missing_fields: computed.missing_fields,
    missing_field_details: computed.missing_field_details || {},
    metric_status: computed.metric_status || {},
    source_summary: sourceSummary,
    fetched_urls: fetchedUrls,
    fetch_timings: fetchTimings,
    raw_saved_paths: saved.rawSavedPaths,
    parsed_saved_paths: saved.parsedSavedPaths,
    normalized_data: normalizedData,
    features: computed.features,
    scores: computed.scores,
    screeningDebug: {
      fetch_success: data?.source?.official_fetch_status?.racelist === "success",
      parse_success: Array.isArray(normalizedData?.lanes) && normalizedData.lanes.length === 6,
      score_success: computed.decision !== "DATA_ERROR",
      data_status: computed.data_status,
      confidence_status: computed.confidence_status || computed.data_status,
      decision_reason: computed.decision_reason,
      source_summary: sourceSummary,
      fetched_urls: fetchedUrls,
      fetch_timings: fetchTimings,
      raw_saved_paths: saved.rawSavedPaths,
      normalized_data_path: saved.parsedSavedPaths.normalized_data || null,
      hard_race_scores_path: saved.parsedSavedPaths.hard_race_scores || null,
      fixed1234_matrix: computed.fixed1234_matrix,
      scores: computed.scores,
      features: computed.features,
      metric_status: computed.metric_status || {},
      missing_field_details: computed.missing_field_details || {},
      optional_fields_missing: computed.data_status === "PARTIAL" ? computed.missing_fields : [],
      missing_required_scores: computed.decision === "DATA_ERROR" ? computed.missing_fields : []
    }
  };
}

