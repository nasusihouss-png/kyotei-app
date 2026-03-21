import fs from "fs";
import path from "path";
import axios from "axios";
import * as cheerio from "cheerio";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEBUG_ROOT = path.resolve(__dirname, "../../debug/hard-race-1234");
const FIXED_COMBOS = ["1-2-3", "1-2-4", "1-3-2", "1-3-4", "1-4-2", "1-4-3"];
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
    ]
  };
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

function computeScores(normalized, sourceSummary) {
  const lanes = new Map((normalized?.lanes || []).map((row) => [row.lane, row]));
  const boat1 = lanes.get(1);
  const lane2 = lanes.get(2);
  const lane3 = lanes.get(3);
  const lane4 = lanes.get(4);
  const lane5 = lanes.get(5);
  const lane6 = lanes.get(6);
  const missingPrimary = missingCoreBoatraceFields(normalized);

  if (!boat1 || missingPrimary.length > 0) {
    return {
      scores: {
        boat1_escape_trust: null,
        opponent_234_fit: null,
        outside_break_risk: null,
        makuri_risk: null,
        fixed1234_total_probability: null,
        top4_fixed1234_probability: null,
        fixed1234_shape_concentration: null
      },
      features: {},
      fixed1234_matrix: {},
      fixed1234_top4: [],
      suggested_shape: null,
      data_status: "DATA_ERROR",
      decision: "DATA_ERROR",
      decision_reason: "missing source data",
      missing_fields: missingPrimary
    };
  }

  const boat1Features = boat1.features;
  const boat1EscapeTrust = round(scoreBlend([
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
  const opponent234Fit = round(scoreBlend([
    { value: fit2, weight: 0.38 },
    { value: fit3, weight: 0.34 },
    { value: fit4, weight: 0.28 }
  ]), 1);

  const makuriRisk = round(scoreBlend([
    { value: lane3?.features?.makuri_rate?.value, weight: 0.35 },
    { value: lane3?.features?.makurisashi_rate?.value, weight: 0.2 },
    { value: lane4?.features?.makuri_rate?.value, weight: 0.24 },
    { value: lane4?.features?.makurisashi_rate?.value, weight: 0.11 },
    { value: clamp(0, 100, 100 - (boat1EscapeTrust ?? 50)), weight: 0.1 }
  ]), 1);

  const outsideHead = scoreBlend([
    { value: laneFitScore(lane5, 4), weight: 0.5 },
    { value: laneFitScore(lane6, 4), weight: 0.5 }
  ]);
  const outsideSecond = scoreBlend([
    { value: lane5?.features?.course_3rentai_rate?.value, weight: 0.26 },
    { value: lane6?.features?.course_3rentai_rate?.value, weight: 0.26 },
    { value: inverseStScore(lane5?.features?.avg_st?.value ?? null), weight: 0.14 },
    { value: inverseStScore(lane6?.features?.avg_st?.value ?? null), weight: 0.14 },
    { value: lane5?.features?.motor_2ren?.value, weight: 0.1 },
    { value: lane6?.features?.motor_2ren?.value, weight: 0.1 }
  ]);
  const outsideBreakRisk = round(clamp(
    0,
    100,
    (outsideHead || 0) * 0.48 +
      (outsideSecond || 0) * 0.32 +
      Math.max(0, 65 - (fit2 || 50)) * 0.08 +
      Math.max(0, 65 - (fit3 || 50)) * 0.07 +
      Math.max(0, 65 - (fit4 || 50)) * 0.05
  ), 1);

  const fixedBase = scoreBlend([
    { value: boat1EscapeTrust, weight: 0.42 },
    { value: opponent234Fit, weight: 0.34 },
    { value: clamp(0, 100, 100 - outsideBreakRisk), weight: 0.18 },
    { value: clamp(0, 100, 100 - (makuriRisk || 50)), weight: 0.06 }
  ]);
  const fixed1234TotalProbability = round(clamp(0, 0.86, ((fixedBase || 0) / 100) * 0.63), 4);

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

  const rawMatrix = {};
  for (const combo of FIXED_COMBOS) {
    const [, second, third] = combo.split("-").map((part) => Number(part));
    rawMatrix[combo] = secondWeights[second] * thirdWeights[third];
  }
  const rawTotal = Object.values(rawMatrix).reduce((sum, value) => sum + value, 0);
  const fixed1234Matrix = Object.fromEntries(
    Object.entries(rawMatrix).map(([combo, raw]) => [combo, round(rawTotal > 0 ? (raw / rawTotal) * fixed1234TotalProbability : 0, 4)])
  );
  const fixed1234Top4 = Object.entries(fixed1234Matrix)
    .map(([combo, probability]) => ({ combo, probability }))
    .sort((a, b) => (b.probability || 0) - (a.probability || 0))
    .slice(0, 4);
  const top4Fixed1234Probability = round(fixed1234Top4.reduce((sum, row) => sum + (row.probability || 0), 0), 4);
  const fixed1234ShapeConcentration = round(fixed1234TotalProbability > 0 ? top4Fixed1234Probability / fixed1234TotalProbability : null, 4);
  const suggestedShape =
    fixed1234Top4[0]?.combo === "1-4-2" || fixed1234Top4[0]?.combo === "1-4-3"
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
  if (boat1EscapeTrust >= 68 && opponent234Fit >= 63 && outsideBreakRisk <= 32 && fixed1234TotalProbability >= 0.42) {
    decision = fixed1234ShapeConcentration >= 0.86 ? "BUY-4" : "BUY-6";
  } else if (boat1EscapeTrust >= 60 && opponent234Fit >= 55 && outsideBreakRisk <= 42 && fixed1234TotalProbability >= 0.34) {
    decision = "BORDERLINE";
  }

  const decisionReason =
    dataStatus === "PARTIAL"
      ? "missing source data"
      : decision === "BUY-4"
        ? "strong 1-234-234 concentration"
        : decision === "BUY-6"
          ? "boat1 escape and 2/3/4 fit passed the six-ticket gate"
          : decision === "BORDERLINE"
            ? "base 1-234-234 fit is present but not concentrated enough"
            : "1-234-234 fit is not strong enough";

  return {
    scores: {
      boat1_escape_trust: boat1EscapeTrust,
      opponent_234_fit: opponent234Fit,
      outside_break_risk: outsideBreakRisk,
      makuri_risk: makuriRisk,
      fixed1234_total_probability: fixed1234TotalProbability,
      top4_fixed1234_probability: top4Fixed1234Probability,
      fixed1234_shape_concentration: fixed1234ShapeConcentration
    },
    features: {
      lane2_fit: round(fit2, 2),
      lane3_fit: round(fit3, 2),
      lane4_fit: round(fit4, 2),
      outside_head_risk_base: round(outsideHead, 2),
      outside_second_risk_base: round(outsideSecond, 2),
      supplement_missing_count: supplementMissing.length,
      source_summary: sourceSummary
    },
    fixed1234_matrix: fixed1234Matrix,
    fixed1234_top4: fixed1234Top4,
    suggested_shape: suggestedShape,
    data_status: dataStatus,
    decision,
    decision_reason: decisionReason,
    missing_fields: supplementMissing
  };
}

export async function buildHardRace1234Response({ data, date, venueId, raceNo, artifactCollector = null }) {
  const artifactDir = buildArtifactDir({ date, venueId, raceNo });
  const resultFetch = await fetchOfficialRaceResult({ date, venueId, raceNo });
  const normalizedData = normalizeRaceData({ data, resultFetch });
  const sourceSummary = buildSourceSummary({ data, normalizedLanes: normalizedData.lanes, resultFetch });
  const computed = computeScores(normalizedData, sourceSummary);
  const fetchedUrls = {
    boatrace: {
      racelist: data?.source?.racelistUrl || null,
      beforeinfo: data?.source?.beforeinfoUrl || null,
      raceresult: resultFetch?.url || null
    },
    kyoteibiyori: {
      primary: data?.source?.kyotei_biyori?.url || null,
      tried_urls: Array.isArray(data?.source?.kyotei_biyori?.tried_urls) ? data.source.kyotei_biyori.tried_urls : []
    }
  };
  const saved = saveArtifacts({
    dir: artifactDir,
    raw: {
      ...(artifactCollector?.raw || {}),
      boatrace_raceresult: resultFetch?.raw || null
    },
    fetchedUrls,
    normalizedData,
    scores: computed
  });

  return {
    race_no: toNum(raceNo, null),
    status: computed.decision === "DATA_ERROR" ? "DATA_ERROR" : "FETCHED",
    data_status: computed.data_status,
    boat1_escape_trust: computed.scores.boat1_escape_trust,
    opponent_234_fit: computed.scores.opponent_234_fit,
    outside_break_risk: computed.scores.outside_break_risk,
    makuri_risk: computed.scores.makuri_risk,
    fixed1234_total_probability: computed.scores.fixed1234_total_probability,
    top4_fixed1234_probability: computed.scores.top4_fixed1234_probability,
    fixed1234_shape_concentration: computed.scores.fixed1234_shape_concentration,
    fixed1234_matrix: computed.fixed1234_matrix,
    fixed1234_top4: computed.fixed1234_top4,
    suggested_shape: computed.suggested_shape,
    decision: computed.decision,
    decision_reason: computed.decision_reason,
    missing_fields: computed.missing_fields,
    source_summary: sourceSummary,
    fetched_urls: fetchedUrls,
    raw_saved_paths: saved.rawSavedPaths,
    parsed_saved_paths: saved.parsedSavedPaths,
    normalized_data: normalizedData,
    features: computed.features,
    scores: computed.scores,
    screeningDebug: {
      fetch_success: true,
      parse_success: true,
      score_success: computed.decision !== "DATA_ERROR",
      data_status: computed.data_status,
      decision_reason: computed.decision_reason,
      source_summary: sourceSummary,
      fetched_urls: fetchedUrls,
      raw_saved_paths: saved.rawSavedPaths,
      normalized_data_path: saved.parsedSavedPaths.normalized_data,
      hard_race_scores_path: saved.parsedSavedPaths.hard_race_scores,
      fixed1234_matrix: computed.fixed1234_matrix,
      scores: computed.scores,
      features: computed.features,
      optional_fields_missing: computed.missing_fields,
      missing_required_scores: computed.decision === "DATA_ERROR" ? computed.missing_fields : []
    }
  };
}
