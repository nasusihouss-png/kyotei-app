import fs from "fs";
import path from "path";
import axios from "axios";
import * as cheerio from "cheerio";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEBUG_ROOT = path.resolve(__dirname, "../../debug/hard-race-1234");
const FIXED_COMBOS = ["1-2-3", "1-2-4", "1-3-2", "1-3-4", "1-4-2", "1-4-3"];
const FIXED_TOP4_COUNT = 4;
const HARD_RACE_V2_WEIGHTS = {
  opponent_pairs: { pair23: 0.29, pair24: 0.34, pair34: 0.37 },
  opponent_roles: {
    lane2_second: 0.31,
    lane3_attack: 0.31,
    lane4_develop: 0.38
  },
  kill_escape: {
    lane3_makuri: 0.46,
    lane3_makurisashi: 0.14,
    lane3_breakout: 0.12,
    lane3_st: 0.14,
    lane3_motor2: 0.14,
    lane4_makuri: 0.08,
    lane4_makurisashi: 0.12,
    lane4_breakout: 0.34,
    lane4_st: 0.18,
    lane4_motor2: 0.1,
    lane4_lane3rentai: 0.18,
    lane3_trigger: 65,
    lane4_trigger: 69,
    lane3_pressure: 0.78,
    lane4_pressure: 0.4,
    boat1_vulnerability: 0.46
  },
  boat1_escape_adjustment: {
    kill_penalty_trigger: 26,
    kill_penalty_rate: 0.12,
    shape_penalty_trigger: 48,
    shape_penalty_rate: 0.03,
    shape_penalty_cap: 2,
    outside_head_penalty_trigger: 20,
    outside_head_penalty_rate: 0.18,
    outside_head_penalty_cap: 6
  },
  shape_shuffle: {
    lane3_makurisashi: 0.24,
    lane4_breakout: 0.24,
    lane3_lane3rentai: 0.14,
    lane4_lane3rentai: 0.14,
    fit_gap_vs2_lane3: 0.11,
    fit_gap_vs2_lane4: 0.1,
    pair_gap: 0.07
  },
  outside_break: {
    head: 0.45,
    second: 0.4,
    third: 0.15,
    pair23_guard: 0.02,
    pair24_guard: 0.03
  },
  box_hit: {
    boat1_escape_trust: 0.28,
    opponent_234_fit: 0.42,
    outside_guard: 0.16,
    kill_escape_guard: 0.08,
    shape_shuffle_guard: 0.06,
    scale: 0.84
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
    boat1_escape_trust: 44,
    opponent_234_fit: 58,
    outside_break_risk_max: 20,
    box_hit_score_min: 0.56,
    fixed1234_total_probability_min: 0.34,
    top4_fixed1234_probability_min: 0.27,
    shape_focus_score_min: 0.6,
    shape_concentration_min: 0.62
  },
  buy6: {
    boat1_escape_trust: 44,
    opponent_234_fit: 58,
    outside_break_risk_max: 20,
    fixed1234_total_probability_min: 0.34,
    box_hit_score_min: 0.56
  },
  borderline: {
    boat1_escape_trust: 40,
    opponent_234_fit: 52,
    outside_break_risk_max: 26,
    fixed1234_total_probability_min: 0.28,
    box_hit_score_min: 0.5
  },
  false_negative_review: {
    opponent_234_fit_high: 56,
    outside_break_risk_low: 24,
    pair24_fit_high: 58,
    pair34_fit_high: 57,
    p124_high: 0.055,
    fixed1234_total_probability_high: 0.31
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

function normalizeText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function asField(value, source = null, missingReason = null) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return { value: null, source, missing_reason: missingReason || "api field missing" };
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
      if (!/3連単/.test(heading)) return;
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

function buildSourceSummary({ data, normalizedLanes, resultFetch }) {
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
      "kyoteibiyori only supplements missing traits"
    ],
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
      decision: "DATA_ERROR",
      decision_reason: "missing source data",
      missing_fields: missingFields,
      missing_field_details: missingFieldDetails,
      metric_status: metricDetails
    };
  }

  const boat1Features = boat1.features;
  const baseBoat1EscapeTrust = round(scoreBlend([
    { value: percentFromRate(boat1Features.national_win_rate.value), weight: 0.23 },
    { value: percentFromRate(boat1Features.local_win_rate.value), weight: 0.16 },
    { value: inverseStScore(boat1Features.avg_st.value), weight: 0.17 },
    { value: lowerBetterScore((boat1Features.f_count.value || 0) * 10 + (boat1Features.l_count.value || 0) * 8), weight: 0.11 },
    { value: boat1Features.motor_2ren.value, weight: 0.13 },
    { value: boat1Features.motor_3ren.value, weight: 0.06 },
    { value: boat1Features.boat_2ren.value, weight: 0.05 },
    { value: boat1Features.boat_3ren.value, weight: 0.03 },
    { value: normalized?.venue?.inside_bias?.value !== null ? normalized.venue.inside_bias.value * 100 : null, weight: 0.06 }
  ]), 1);

  const fit2 = laneFitScore(lane2, 2);
  const fit3 = laneFitScore(lane3, 3);
  const fit4 = laneFitScore(lane4, 4);
  const fit5 = laneFitScore(lane5, 4);
  const fit6 = laneFitScore(lane6, 4);
  const pair23Fit = round(pairFitScore({
    leftFit: fit2,
    rightFit: fit3,
    leftSupport: [lane2?.features?.sashi_rate?.value, lane2?.features?.stability_rate?.value, lane2?.features?.lane_3rentai_rate?.value],
    rightSupport: [lane3?.features?.makurisashi_rate?.value, lane3?.features?.lane_3rentai_rate?.value, lane3?.features?.motor_3ren?.value]
  }), 1);
  const pair24Fit = round(pairFitScore({
    leftFit: fit2,
    rightFit: fit4,
    leftSupport: [lane2?.features?.sashi_rate?.value, lane2?.features?.lane_3rentai_rate?.value, lane2?.features?.motor_2ren?.value],
    rightSupport: [lane4?.features?.breakout_rate?.value, lane4?.features?.lane_3rentai_rate?.value, lane4?.features?.stability_rate?.value]
  }), 1);
  const pair34Fit = round(pairFitScore({
    leftFit: fit3,
    rightFit: fit4,
    leftSupport: [lane3?.features?.makuri_rate?.value, lane3?.features?.makurisashi_rate?.value, lane3?.features?.lane_3rentai_rate?.value],
    rightSupport: [lane4?.features?.breakout_rate?.value, lane4?.features?.stability_rate?.value, lane4?.features?.lane_3rentai_rate?.value]
  }), 1);
  const lane2SecondRemain = round(scoreBlend([
    { value: fit2, weight: 0.3 },
    { value: lane2?.features?.sashi_rate?.value, weight: 0.24 },
    { value: lane2?.features?.lane_3rentai_rate?.value, weight: 0.18 },
    { value: lane2?.features?.stability_rate?.value, weight: 0.12 },
    { value: lane2?.features?.motor_2ren?.value, weight: 0.16 }
  ]), 1);
  const lane3AttackRemain = round(scoreBlend([
    { value: fit3, weight: 0.26 },
    { value: pair23Fit, weight: 0.12 },
    { value: lane3?.features?.makuri_rate?.value, weight: 0.2 },
    { value: lane3?.features?.makurisashi_rate?.value, weight: 0.2 },
    { value: lane3?.features?.lane_3rentai_rate?.value, weight: 0.12 },
    { value: inverseStScore(lane3?.features?.avg_st?.value ?? null), weight: 0.1 }
  ]), 1);
  const lane4DevelopRemain = round(scoreBlend([
    { value: fit4, weight: 0.22 },
    { value: pair24Fit, weight: 0.18 },
    { value: pair34Fit, weight: 0.18 },
    { value: lane4?.features?.breakout_rate?.value, weight: 0.18 },
    { value: lane4?.features?.lane_3rentai_rate?.value, weight: 0.14 },
    { value: lane4?.features?.stability_rate?.value, weight: 0.1 }
  ]), 1);
  const opponent234Fit = round(scoreBlend([
    { value: lane2SecondRemain, weight: HARD_RACE_V2_WEIGHTS.opponent_roles.lane2_second },
    { value: lane3AttackRemain, weight: HARD_RACE_V2_WEIGHTS.opponent_roles.lane3_attack },
    { value: lane4DevelopRemain, weight: HARD_RACE_V2_WEIGHTS.opponent_roles.lane4_develop },
    { value: pair23Fit, weight: HARD_RACE_V2_WEIGHTS.opponent_pairs.pair23 * 0.35 },
    { value: pair24Fit, weight: HARD_RACE_V2_WEIGHTS.opponent_pairs.pair24 * 0.45 },
    { value: pair34Fit, weight: HARD_RACE_V2_WEIGHTS.opponent_pairs.pair34 * 0.5 }
  ]), 1);

  const lane3KillBase = scoreBlend([
    { value: lane3?.features?.makuri_rate?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane3_makuri },
    { value: lane3?.features?.makurisashi_rate?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane3_makurisashi },
    { value: lane3?.features?.breakout_rate?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane3_breakout },
    { value: inverseStScore(lane3?.features?.avg_st?.value ?? null), weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane3_st },
    { value: lane3?.features?.motor_2ren?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane3_motor2 }
  ]);
  const lane4KillBase = scoreBlend([
    { value: lane4?.features?.makuri_rate?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane4_makuri },
    { value: lane4?.features?.makurisashi_rate?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane4_makurisashi },
    { value: lane4?.features?.breakout_rate?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane4_breakout },
    { value: inverseStScore(lane4?.features?.avg_st?.value ?? null), weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane4_st },
    { value: lane4?.features?.motor_2ren?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane4_motor2 },
    { value: lane4?.features?.lane_3rentai_rate?.value, weight: HARD_RACE_V2_WEIGHTS.kill_escape.lane4_lane3rentai }
  ]);
  const boat1Vulnerability = clamp(0, 100, 68 - (baseBoat1EscapeTrust ?? 68));
  const killEscapeRisk = round(clamp(
    0,
    100,
    Math.max(0, (lane3KillBase || 0) - HARD_RACE_V2_WEIGHTS.kill_escape.lane3_trigger) * HARD_RACE_V2_WEIGHTS.kill_escape.lane3_pressure +
      Math.max(0, (lane4KillBase || 0) - HARD_RACE_V2_WEIGHTS.kill_escape.lane4_trigger) * HARD_RACE_V2_WEIGHTS.kill_escape.lane4_pressure +
      boat1Vulnerability * HARD_RACE_V2_WEIGHTS.kill_escape.boat1_vulnerability
  ), 1);
  const shapeShuffleRisk = round(scoreBlend([
    { value: lane3?.features?.makurisashi_rate?.value, weight: HARD_RACE_V2_WEIGHTS.shape_shuffle.lane3_makurisashi },
    { value: lane4?.features?.breakout_rate?.value, weight: HARD_RACE_V2_WEIGHTS.shape_shuffle.lane4_breakout },
    { value: lane3?.features?.lane_3rentai_rate?.value, weight: HARD_RACE_V2_WEIGHTS.shape_shuffle.lane3_lane3rentai },
    { value: lane4?.features?.lane_3rentai_rate?.value, weight: HARD_RACE_V2_WEIGHTS.shape_shuffle.lane4_lane3rentai },
    { value: clamp(0, 100, Math.abs((fit3 || 50) - (fit2 || 50)) * 1.2), weight: HARD_RACE_V2_WEIGHTS.shape_shuffle.fit_gap_vs2_lane3 },
    { value: clamp(0, 100, Math.abs((fit4 || 50) - (fit2 || 50)) * 1.2), weight: HARD_RACE_V2_WEIGHTS.shape_shuffle.fit_gap_vs2_lane4 },
    { value: clamp(0, 100, Math.abs((pair23Fit || 50) - (pair24Fit || 50))), weight: HARD_RACE_V2_WEIGHTS.shape_shuffle.pair_gap }
  ]), 1);
  const boat1KillPenalty = round(
    Math.max(
      0,
      ((killEscapeRisk || 0) - HARD_RACE_V2_WEIGHTS.boat1_escape_adjustment.kill_penalty_trigger) *
        HARD_RACE_V2_WEIGHTS.boat1_escape_adjustment.kill_penalty_rate
    ),
    1
  );
  const boat1ShapePenalty = round(
    clamp(
      0,
      HARD_RACE_V2_WEIGHTS.boat1_escape_adjustment.shape_penalty_cap,
      ((shapeShuffleRisk || 0) - HARD_RACE_V2_WEIGHTS.boat1_escape_adjustment.shape_penalty_trigger) *
        HARD_RACE_V2_WEIGHTS.boat1_escape_adjustment.shape_penalty_rate
    ),
    1
  );
  const outsideHeadBoat1Penalty = round(
    clamp(
      0,
      HARD_RACE_V2_WEIGHTS.boat1_escape_adjustment.outside_head_penalty_cap,
      ((scoreBlend([
        { value: fit5, weight: 0.4 },
        { value: fit6, weight: 0.4 },
        { value: lane5?.features?.breakout_rate?.value, weight: 0.1 },
        { value: lane6?.features?.breakout_rate?.value, weight: 0.1 }
      ]) || 0) - HARD_RACE_V2_WEIGHTS.boat1_escape_adjustment.outside_head_penalty_trigger) *
        HARD_RACE_V2_WEIGHTS.boat1_escape_adjustment.outside_head_penalty_rate
    ),
    1
  );
  const boat1EscapeTrust = round(
    clamp(0, 100, (baseBoat1EscapeTrust || 0) - (boat1KillPenalty || 0) - (boat1ShapePenalty || 0) - (outsideHeadBoat1Penalty || 0)),
    1
  );
  const makuriRisk = round(scoreBlend([
    { value: killEscapeRisk, weight: 0.62 },
    { value: shapeShuffleRisk, weight: 0.38 }
  ]), 1);

  const outsideHead = scoreBlend([
    { value: fit5, weight: 0.36 },
    { value: fit6, weight: 0.36 },
    { value: lane5?.features?.breakout_rate?.value, weight: 0.1 },
    { value: lane6?.features?.breakout_rate?.value, weight: 0.1 },
    { value: inverseStScore(lane5?.features?.avg_st?.value ?? null), weight: 0.04 },
    { value: inverseStScore(lane6?.features?.avg_st?.value ?? null), weight: 0.04 }
  ]);
  const outsideSecond = scoreBlend([
    { value: lane5?.features?.course_3rentai_rate?.value, weight: 0.22 },
    { value: lane6?.features?.course_3rentai_rate?.value, weight: 0.22 },
    { value: lane5?.features?.lane_3rentai_rate?.value, weight: 0.12 },
    { value: lane6?.features?.lane_3rentai_rate?.value, weight: 0.12 },
    { value: inverseStScore(lane5?.features?.avg_st?.value ?? null), weight: 0.12 },
    { value: inverseStScore(lane6?.features?.avg_st?.value ?? null), weight: 0.12 },
    { value: lane5?.features?.motor_2ren?.value, weight: 0.1 },
    { value: lane6?.features?.motor_2ren?.value, weight: 0.1 }
  ]);
  const outsideThird = scoreBlend([
    { value: lane5?.features?.course_3rentai_rate?.value, weight: 0.2 },
    { value: lane6?.features?.course_3rentai_rate?.value, weight: 0.2 },
    { value: lane5?.features?.lane_3rentai_rate?.value, weight: 0.16 },
    { value: lane6?.features?.lane_3rentai_rate?.value, weight: 0.16 },
    { value: lane5?.features?.stability_rate?.value, weight: 0.1 },
    { value: lane6?.features?.stability_rate?.value, weight: 0.1 },
    { value: lane5?.features?.boat_2ren?.value, weight: 0.04 },
    { value: lane6?.features?.boat_2ren?.value, weight: 0.04 }
  ]);
  const outsideBreakRisk = round(clamp(
    0,
    100,
    (outsideHead || 0) * HARD_RACE_V2_WEIGHTS.outside_break.head +
      (outsideSecond || 0) * HARD_RACE_V2_WEIGHTS.outside_break.second +
      (outsideThird || 0) * HARD_RACE_V2_WEIGHTS.outside_break.third +
      Math.max(0, 68 - (pair23Fit || 52)) * HARD_RACE_V2_WEIGHTS.outside_break.pair23_guard +
      Math.max(0, 68 - (pair24Fit || 52)) * HARD_RACE_V2_WEIGHTS.outside_break.pair24_guard
  ), 1);

  const fixedBase = scoreBlend([
    { value: boat1EscapeTrust, weight: HARD_RACE_V2_WEIGHTS.box_hit.boat1_escape_trust },
    { value: opponent234Fit, weight: HARD_RACE_V2_WEIGHTS.box_hit.opponent_234_fit },
    { value: clamp(0, 100, 100 - outsideBreakRisk), weight: HARD_RACE_V2_WEIGHTS.box_hit.outside_guard },
    { value: clamp(0, 100, 100 - (killEscapeRisk || 50)), weight: HARD_RACE_V2_WEIGHTS.box_hit.kill_escape_guard },
    { value: clamp(0, 100, 100 - (shapeShuffleRisk || 50)), weight: HARD_RACE_V2_WEIGHTS.box_hit.shape_shuffle_guard }
  ]);
  const boxHitScore = round(clamp(0, 0.9, ((fixedBase || 0) / 100) * HARD_RACE_V2_WEIGHTS.box_hit.scale), 4);

  const secondWeights = {
    2: Math.max(0.01, (fit2 || 35) * 0.55 + (lane2?.features?.sashi_rate?.value || 35) * 0.35 + (lane2?.features?.stability_rate?.value || 45) * 0.1),
    3: Math.max(0.01, (fit3 || 35) * 0.5 + (lane3?.features?.makuri_rate?.value || 35) * 0.28 + (lane3?.features?.makurisashi_rate?.value || 35) * 0.22),
    4: Math.max(0.01, (fit4 || 35) * 0.52 + (lane4?.features?.breakout_rate?.value || 35) * 0.26 + (lane4?.features?.stability_rate?.value || 45) * 0.22)
  };
  const thirdWeights = {
    2: Math.max(0.01, (fit2 || 35) * 0.38 + (lane2?.features?.lane_3rentai_rate?.value || 40) * 0.42 + (lane2?.features?.stability_rate?.value || 45) * 0.2),
    3: Math.max(0.01, (fit3 || 35) * 0.4 + (lane3?.features?.lane_3rentai_rate?.value || 40) * 0.32 + (lane3?.features?.makurisashi_rate?.value || 35) * 0.28),
    4: Math.max(0.01, (fit4 || 35) * 0.44 + (lane4?.features?.lane_3rentai_rate?.value || 40) * 0.4 + (lane4?.features?.breakout_rate?.value || 35) * 0.16)
  };

  const rawMatrix = Object.fromEntries(
    FIXED_COMBOS.map((combo) => [combo, comboProbability({ combo, secondWeights, thirdWeights, pair23Fit, pair24Fit, pair34Fit })])
  );
  const rawTotal = Object.values(rawMatrix).reduce((sum, value) => sum + value, 0);
  const fixed1234Matrix = Object.fromEntries(
    Object.entries(rawMatrix).map(([combo, raw]) => [combo, round(rawTotal > 0 ? (raw / rawTotal) * boxHitScore : 0, 4)])
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
  const shapeFocusScore = round(fixed1234Top4.reduce((sum, row) => sum + (row.probability || 0), 0), 4);
  const top4Fixed1234Probability = round(fixed1234Top4.reduce((sum, row) => sum + (row.probability || 0), 0), 4);
  const fixed1234TotalProbability = boxHitScore;
  const fixed1234ShapeConcentration = round(boxHitScore > 0 ? shapeFocusScore / boxHitScore : null, 4);
  const strongestShapeShare = round(boxHitScore > 0 ? (fixed1234Top4[0]?.probability || 0) / boxHitScore : null, 4);
  const top2ShapeShare = round(
    boxHitScore > 0 ? fixed1234Top4.slice(0, 2).reduce((sum, row) => sum + (row.probability || 0), 0) / boxHitScore : null,
    4
  );
  const lane4CoverageShare = round(boxHitScore > 0 ? (p124 + p142 + p143) / boxHitScore : null, 4);
  const refinedShapeFocusScore = round(clamp(
    0,
    0.95,
    ((fixed1234ShapeConcentration || 0) * 0.55) +
      ((strongestShapeShare || 0) * 0.16) +
      ((top2ShapeShare || 0) * 0.14) +
      ((lane4CoverageShare || 0) * 0.15)
  ), 4);
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
  const dataStatus = supplementMissing.length > 0 ? "PARTIAL" : "OK";

  let decision = "SKIP";
  const buy4Thresholds = HARD_RACE_V2_DECISION_THRESHOLDS.buy4;
  const buy6Thresholds = HARD_RACE_V2_DECISION_THRESHOLDS.buy6;
  const borderlineThresholds = HARD_RACE_V2_DECISION_THRESHOLDS.borderline;
  const falseNegativeBuy6Rescue =
    (opponent234Fit || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.opponent_234_fit_high &&
    (outsideBreakRisk || 100) <= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.outside_break_risk_low &&
    (fixed1234TotalProbability || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.fixed1234_total_probability_high &&
    (
      (pair24Fit || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.pair24_fit_high ||
      (pair34Fit || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.pair34_fit_high ||
      (p124 || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.p124_high
    );
  const buy6GatePassed =
    boat1EscapeTrust >= buy6Thresholds.boat1_escape_trust &&
    opponent234Fit >= buy6Thresholds.opponent_234_fit &&
    outsideBreakRisk <= buy6Thresholds.outside_break_risk_max &&
    fixed1234TotalProbability >= buy6Thresholds.fixed1234_total_probability_min &&
    boxHitScore >= buy6Thresholds.box_hit_score_min;

  if (
    buy6GatePassed &&
    top4Fixed1234Probability >= buy4Thresholds.top4_fixed1234_probability_min &&
    fixed1234ShapeConcentration >= buy4Thresholds.shape_concentration_min &&
    refinedShapeFocusScore >= buy4Thresholds.shape_focus_score_min
  ) {
    decision = "BUY-4";
  } else if (buy6GatePassed || falseNegativeBuy6Rescue) {
    decision = "BUY-6";
  } else if (
    boat1EscapeTrust >= borderlineThresholds.boat1_escape_trust &&
    opponent234Fit >= borderlineThresholds.opponent_234_fit &&
    outsideBreakRisk <= borderlineThresholds.outside_break_risk_max &&
    fixed1234TotalProbability >= borderlineThresholds.fixed1234_total_probability_min &&
    boxHitScore >= borderlineThresholds.box_hit_score_min
  ) {
    decision = "BORDERLINE";
  }

  const actualCombo = normalized?.result?.combo || null;
  const yBox6 = actualCombo && FIXED_COMBOS.includes(actualCombo) ? 1 : 0;
  const yTop4 = actualCombo && fixedTop4Set.has(actualCombo) ? 1 : 0;
  const yBuy6 = yBox6;
  const yBuy4 = yTop4;
  const falseNegativeCase =
    actualCombo === "1-2-4" &&
    (opponent234Fit || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.opponent_234_fit_high &&
    (outsideBreakRisk || 100) <= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.outside_break_risk_low &&
    !["BUY-6", "BUY-4"].includes(decision);

  const decisionReason =
    dataStatus === "PARTIAL"
      ? "missing source data"
      : decision === "BUY-4"
        ? "box hit and top-4 focus both passed"
        : decision === "BUY-6"
          ? falseNegativeBuy6Rescue
            ? "rescued by strong 1-234-234 hit pattern despite prior skip tendency"
            : "box-hit gate passed for 1-234-234 six-ticket buy"
          : decision === "BORDERLINE"
            ? "box fit is present but buy gate is still short"
            : "1-234-234 fit is not strong enough";

  const missingFields = supplementMissing;
  const computed = {
    scores: {
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
      lane2_second_remain: lane2SecondRemain,
      lane3_attack_remain: lane3AttackRemain,
      lane4_develop_remain: lane4DevelopRemain,
      outside_head_risk_base: round(outsideHead, 2),
      outside_second_risk_base: round(outsideSecond, 2),
      outside_third_risk_base: round(outsideThird, 2),
      supplement_missing_count: supplementMissing.length,
      evaluation_targets: {
        y_box6: yBox6,
        y_top4: yTop4,
        y_buy6: yBuy6,
        y_buy4: yBuy4
      },
      false_negative_review: {
        actual_combo: actualCombo,
        false_negative_case: falseNegativeCase,
        matched_pattern: actualCombo === "1-2-4",
        opponent_234_fit_high: (opponent234Fit || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.opponent_234_fit_high,
        outside_break_risk_low: (outsideBreakRisk || 100) <= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.outside_break_risk_low,
        pair24_fit_high: (pair24Fit || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.pair24_fit_high,
        p_124_high: (p124 || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.p124_high,
        fixed1234_total_probability_high: (fixed1234TotalProbability || 0) >= HARD_RACE_V2_DECISION_THRESHOLDS.false_negative_review.fixed1234_total_probability_high,
        buy6_rescue_thresholds_met: buy6GatePassed,
        buy6_rescue_triggered: falseNegativeBuy6Rescue
      },
      boat1_escape_breakdown: {
        raw_base_trust: baseBoat1EscapeTrust,
        kill_escape_penalty: boat1KillPenalty,
        shape_shuffle_penalty: boat1ShapePenalty,
        outside_head_penalty: outsideHeadBoat1Penalty
      },
      operational_policy: {
        candidate_decisions: ["BUY-4", "BUY-6", "BORDERLINE"],
        sort_by: "box_hit_score_desc",
        adopt_top_n: "3-5"
      },
      reviewed_coefficients: {
        weights: HARD_RACE_V2_WEIGHTS,
        thresholds: HARD_RACE_V2_DECISION_THRESHOLDS
      },
      source_summary: sourceSummary
    },
    fixed1234_matrix: fixed1234Matrix,
    fixed1234_top4: fixed1234Top4,
    suggested_shape: suggestedShape,
    data_status: dataStatus,
    decision,
    hard_race_rank: decision === "BUY-4" ? "A" : ["BUY-6", "BORDERLINE"].includes(decision) ? "B" : "SKIP",
    decision_reason: decisionReason,
    missing_fields: missingFields
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
      decision: "DATA_ERROR",
      decision_reason: String(error?.message || error || "hard race build failed"),
      missing_fields: ["hard_race_build"],
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
        p_123: { value: null, status: "missing", reason: "not calculated" },
        p_124: { value: null, status: "missing", reason: "not calculated" },
        p_132: { value: null, status: "missing", reason: "not calculated" },
        p_134: { value: null, status: "missing", reason: "not calculated" },
        p_142: { value: null, status: "missing", reason: "not calculated" },
        p_143: { value: null, status: "missing", reason: "not calculated" }
      }
    };
  }

  const fetchTimings = {
    ...(data?.source?.fetch_timings || data?.source?.timings || {}),
    hard_race_total_ms: Date.now() - startedAt
  };
  return {
    race_no: toNum(raceNo, null),
    status: computed.decision === "DATA_ERROR" ? "DATA_ERROR" : "FETCHED",
    data_status: computed.data_status,
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
    decision: computed.decision,
    decision_reason: computed.decision_reason,
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
