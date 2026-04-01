import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getVenueEscapeFailPressure,
  getVenueLaneBiasScore,
  getVenueScenarioContext,
  getVenueStyleMatchScore,
  VENUE_INNER_BIAS
} from "./venue-scenario-bias.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEBUG_ROOT = path.resolve(__dirname, "../../debug/hard-race-1234");
const FIXED_COMBOS = ["1-2-3", "1-2-4", "1-3-2", "1-3-4", "1-4-2", "1-4-3"];
export const HARD_RACE_API_RESPONSE_KEYS = [
  "boat1_head_pre",
  "boat1_escape_trust",
  "fit_234_index",
  "outside_break_risk_pre",
  "hard_race_index",
  "scenario_repro_score",
  "box_hit_score",
  "fixed1234_shape_concentration",
  "decision",
  "decision_reason"
];
function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 4) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(Number(value).toFixed(digits));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function buildArtifactDir({ date, venueId, raceNo }) {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  return path.resolve(DEBUG_ROOT, `${String(date).replace(/-/g, "")}_${String(venueId).padStart(2, "0")}_${String(raceNo)}_${stamp}`);
}

function writeArtifact(filePath, body) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, body, "utf8");
  return filePath;
}

function saveArtifacts({ dir, fetchedUrls, normalizedData, scores }) {
  ensureDir(dir);
  return {
    rawSavedPaths: {},
    parsedSavedPaths: {
      fetched_urls: writeArtifact(path.resolve(dir, "parsed", "fetched-urls.json"), JSON.stringify(fetchedUrls, null, 2)),
      normalized_data: writeArtifact(path.resolve(dir, "parsed", "normalized-data.json"), JSON.stringify(normalizedData, null, 2)),
      hard_race_scores: writeArtifact(path.resolve(dir, "parsed", "hard-race-1234.json"), JSON.stringify(scores, null, 2))
    }
  };
}

function safeNorm(value, min, max) {
  if (!Number.isFinite(Number(value))) return null;
  return clamp(0, 1, (Number(value) - min) / Math.max(1e-9, max - min));
}

function invNorm(value, min, max) {
  const v = safeNorm(value, min, max);
  return v === null ? null : 1 - v;
}

function weightedAverage(values) {
  const present = values.filter((row) => row && Number.isFinite(Number(row.value)) && Number.isFinite(Number(row.weight)) && row.weight > 0);
  if (!present.length) return null;
  const totalWeight = present.reduce((sum, row) => sum + row.weight, 0);
  return totalWeight > 0 ? present.reduce((sum, row) => sum + row.value * row.weight, 0) / totalWeight : null;
}

function scoreBlend(values) {
  const v = weightedAverage(values);
  return v === null ? null : clamp(0, 100, v);
}

function venueMotorStReadiness(features = {}) {
  return scoreBlend([
    {
      value: safeNorm(weightedAverage([{ value: features.motor_2ren?.value, weight: 0.58 }, { value: features.boat_2ren?.value, weight: 0.42 }]), 20, 60) === null
        ? null
        : safeNorm(weightedAverage([{ value: features.motor_2ren?.value, weight: 0.58 }, { value: features.boat_2ren?.value, weight: 0.42 }]), 20, 60) * 100,
      weight: 0.62
    },
    {
      value: invNorm(features.avg_st?.value, 0.11, 0.24) === null
        ? null
        : invNorm(features.avg_st?.value, 0.11, 0.24) * 100,
      weight: 0.38
    }
  ]);
}

function courseRateByLane(snapshot, lane) {
  if (!snapshot || typeof snapshot !== "object") return null;
  if (lane === 1) {
    return toNum(snapshot.course1_win_rate, null) ?? toNum(snapshot.course1_2rate, null);
  }
  if (lane === 2) return toNum(snapshot.course2_2rate, null);
  if (lane === 3) return toNum(snapshot.course3_3rate, null);
  if (lane === 4) return toNum(snapshot.course4_3rate, null);
  return toNum(snapshot.course_fit_score, null) === null
    ? null
    : clamp(0, 100, 45 + Number(snapshot.course_fit_score) * 7);
}

function normalizeWeights(weightMap) {
  const entries = Object.entries(weightMap || {}).filter(([, value]) => Number.isFinite(Number(value)) && Number(value) > 0);
  const total = entries.reduce((sum, [, value]) => sum + Number(value), 0);
  if (!(total > 0)) return Object.fromEntries(entries.map(([key]) => [key, 0]));
  return Object.fromEntries(entries.map(([key, value]) => [key, Number(value) / total]));
}

function normalizeProbabilities(probMap) {
  const entries = Object.entries(probMap || {}).map(([key, value]) => [key, Math.max(0.000001, Number(value) || 0.000001)]);
  const total = entries.reduce((sum, [, value]) => sum + value, 0);
  if (!(total > 0)) return Object.fromEntries(entries.map(([key]) => [key, 0]));
  const normalized = Object.fromEntries(entries.map(([key, value]) => [key, value / total]));
  const rounded = {};
  let running = 0;
  entries.forEach(([key], index) => {
    if (index === entries.length - 1) {
      rounded[key] = round(Math.max(0, 1 - running), 4);
    } else {
      rounded[key] = round(normalized[key], 4);
      running += rounded[key];
    }
  });
  return rounded;
}

function normalizeComboMatrix(matrix) {
  const entries = Object.entries(matrix || {}).map(([combo, probability]) => [combo, Math.max(0.000001, Number(probability) || 0.000001)]);
  const total = entries.reduce((sum, [, value]) => sum + value, 0);
  if (!(total > 0)) return Object.fromEntries(entries.map(([combo]) => [combo, 0]));
  const normalized = {};
  let running = 0;
  entries.forEach(([combo, value], index) => {
    if (index === entries.length - 1) {
      normalized[combo] = round(Math.max(0, 1 - running), 4);
    } else {
      normalized[combo] = round(value / total, 4);
      running += normalized[combo];
    }
  });
  return normalized;
}

function asField(value, source, missingReason, options = {}) {
  if (!Number.isFinite(Number(value))) {
    return {
      value: null,
      source,
      missing_reason: missingReason,
      source_priority: options?.source_priority || null,
      coverage: options?.coverage ?? 0,
      fallback_used: !!options?.fallback_used,
      required: options?.required === true,
      status: options?.status || null
    };
  }
  return {
    value: Number(value),
    source,
    missing_reason: null,
    source_priority: options?.source_priority || null,
    coverage: options?.coverage ?? 1,
    fallback_used: !!options?.fallback_used,
    required: options?.required === true,
    status: options?.status || "ok"
  };
}

function buildFallbackTracker() {
  return {
    used: false,
    byField: {}
  };
}

function resolveMetricWithFallback({ field, primaryValue, fallbackValue, fallbackFormula, tracker }) {
  if (Number.isFinite(Number(primaryValue))) {
    return { value: Number(primaryValue), sourceType: "snapshot", fallbackUsed: false };
  }
  if (Number.isFinite(Number(fallbackValue))) {
    tracker.used = true;
    tracker.byField[field] = { field, source: "estimated", formula: fallbackFormula };
    return { value: Number(fallbackValue), sourceType: "estimated", fallbackUsed: true };
  }
  return { value: null, sourceType: "missing", fallbackUsed: false };
}

function buildSourceSummary({ data, normalizedLanes, fallbackSummary, missingFields }) {
  const coverage = normalizedLanes.reduce(
    (acc, laneRow) => {
      const fields = Object.values(laneRow?.features || {});
      acc.total_fields += fields.length;
      acc.ready_fields += fields.filter((field) => field?.value !== null).length;
      return acc;
    },
    { total_fields: 0, ready_fields: 0 }
  );
  return {
    mode: "pure_inference",
    inference_source:
      data?.source?.refresh_meta?.refresh_attempted === true
        ? "latest_public_data_auto_refresh"
        : "local_precomputed_only",
    snapshot: {
      race: "snapshot",
      entries: "snapshot",
      feature_snapshot: data?.source?.local_snapshots?.feature_snapshot ? "snapshot" : "missing",
      prediction_feature_event_snapshot: data?.source?.local_snapshots?.prediction_feature_event_snapshot ? "snapshot" : "missing",
      prediction_log_snapshot: data?.source?.local_snapshots?.prediction_log_snapshot ? "snapshot" : "missing",
      coverage,
      coverage_report: data?.source?.coverage_report_summary ? "snapshot" : "missing"
    },
    fallback: {
      used: !!fallbackSummary?.used,
      fields: Object.keys(fallbackSummary?.byField || {}),
      details: fallbackSummary?.byField || {}
    },
    estimated_fields: Object.keys(fallbackSummary?.byField || {}),
    missing_fields: Array.isArray(missingFields) ? missingFields : [],
    broken_fields_required: Array.isArray(missingFields) ? missingFields.filter((field) => /^snapshot\./.test(String(field))) : [],
    broken_fields_optional: Array.isArray(missingFields) ? missingFields.filter((field) => !/^snapshot\./.test(String(field))) : [],
    coverage_report_summary: data?.source?.coverage_report_summary || null
  };
}

function getCoverageMeta(snapshot, fieldName) {
  const coverage = snapshot?.coverage_report && typeof snapshot.coverage_report === "object"
    ? snapshot.coverage_report[fieldName]
    : null;
  return coverage && typeof coverage === "object" ? coverage : null;
}

function predictionValueFromCoverage(meta, fallbackValue = null) {
  if (meta?.status === "ok" || meta?.status === "fallback") {
    if (Number.isFinite(Number(meta?.normalized))) return Number(meta.normalized);
    if (Number.isFinite(Number(meta?.value))) return Number(meta.value);
  }
  return Number.isFinite(Number(fallbackValue)) ? Number(fallbackValue) : null;
}

function normalizeRaceData({ data }) {
  const venueId = toInt(data?.race?.venueId, null);
  const venueScenario = getVenueScenarioContext(venueId);
  const lanes = (Array.isArray(data?.racers) ? data.racers : []).map((racer) => {
    const lane = toInt(racer?.lane, null);
    const snapshot =
      racer?.featureSnapshot && typeof racer.featureSnapshot === "object"
        ? racer.featureSnapshot
        : racer?.playerSnapshot?.feature_snapshot && typeof racer.playerSnapshot.feature_snapshot === "object"
          ? racer.playerSnapshot.feature_snapshot
          : {};
    const meta = (fieldName) => getCoverageMeta(snapshot, fieldName);
    const courseRate = courseRateByLane(snapshot, lane);
    const attackQuality = weightedAverage([
      { value: safeNorm(snapshot.entry_advantage_score, 0, 14) === null ? null : safeNorm(snapshot.entry_advantage_score, 0, 14) * 100, weight: 0.35 },
      { value: safeNorm(snapshot.motor_total_score, 0, 18) === null ? null : safeNorm(snapshot.motor_total_score, 0, 18) * 100, weight: 0.25 },
      { value: invNorm(racer?.avgSt, 0.11, 0.24) === null ? null : invNorm(racer?.avgSt, 0.11, 0.24) * 100, weight: 0.2 },
      { value: courseRate, weight: 0.2 }
    ]);
    const outerEntryTendency = weightedAverage([
      { value: safeNorm(snapshot.entry_advantage_score, 0, 14) === null ? null : safeNorm(snapshot.entry_advantage_score, 0, 14) * 100, weight: 0.45 },
      { value: safeNorm(snapshot.course_change_score, 0, 10) === null ? null : safeNorm(snapshot.course_change_score, 0, 10) * 100, weight: 0.25 },
      { value: invNorm(racer?.avgSt, 0.11, 0.24) === null ? null : invNorm(racer?.avgSt, 0.11, 0.24) * 100, weight: 0.3 }
    ]);
    return {
      lane,
      name: racer?.name || null,
      class: racer?.class || null,
      features: {
        national_win_rate: asField(racer?.nationwideWinRate, "entry_snapshot", "snapshot.racer.national_win_rate"),
        local_win_rate: asField(racer?.localWinRate, "entry_snapshot", "snapshot.racer.local_win_rate"),
        avg_st: asField(racer?.avgSt, "entry_snapshot", "snapshot.racer.avg_st"),
        f_count: asField(racer?.fHoldCount ?? 0, "entry_snapshot", "snapshot.racer.f_count"),
        l_count: asField(racer?.lHoldCount, "player_snapshot", "snapshot.racer.l_count"),
        motor_2ren: asField(racer?.motor2Rate, "entry_snapshot", "snapshot.racer.motor_2ren"),
        boat_2ren: asField(racer?.boat2Rate, "entry_snapshot", "snapshot.racer.boat_2ren"),
        motor_3ren: asField(snapshot?.motor3_rate ?? snapshot?.motor3Rate ?? meta("motor_3ren")?.value, meta("motor_3ren")?.source || "feature_snapshot", "snapshot.feature.motor_3ren", meta("motor_3ren") || {}),
        boat_3ren: asField(snapshot?.boat3_rate ?? snapshot?.boat3Rate ?? meta("boat_3ren")?.value, meta("boat_3ren")?.source || "feature_snapshot", "snapshot.feature.boat_3ren", meta("boat_3ren") || {}),
        course_1_head_rate: asField(snapshot?.course1_win_rate ?? meta("course_1_head_rate")?.value, meta("course_1_head_rate")?.source || "feature_snapshot", "snapshot.feature.course1_win_rate", meta("course_1_head_rate") || {}),
        course_1_2ren_rate: asField(snapshot?.course1_2rate ?? meta("course_1_2ren_rate")?.value, meta("course_1_2ren_rate")?.source || "feature_snapshot", "snapshot.feature.course1_2rate", meta("course_1_2ren_rate") || {}),
        lane_1st_rate: asField(meta("lane_1st_rate")?.value, meta("lane_1st_rate")?.source || "feature_snapshot", "snapshot.coverage.lane_1st_rate", meta("lane_1st_rate") || {}),
        lane_2ren_rate: asField(meta("lane_2ren_rate")?.value, meta("lane_2ren_rate")?.source || "feature_snapshot", "snapshot.coverage.lane_2ren_rate", meta("lane_2ren_rate") || {}),
        lane_3ren_rate: asField(meta("lane_3ren_rate")?.value, meta("lane_3ren_rate")?.source || "feature_snapshot", "snapshot.coverage.lane_3ren_rate", meta("lane_3ren_rate") || {}),
        lane_course_rate: asField(courseRate, "feature_snapshot", `snapshot.feature.course_rate_lane${lane}`),
        course_fit_score: asField(snapshot?.course_fit_score, "feature_snapshot", "snapshot.feature.course_fit_score"),
        motor_total_score: asField(snapshot?.motor_total_score, "feature_snapshot", "snapshot.feature.motor_total_score"),
        entry_advantage_score: asField(snapshot?.entry_advantage_score, "feature_snapshot", "snapshot.feature.entry_advantage_score"),
        start_rank: asField(snapshot?.st_rank, "feature_snapshot", "snapshot.feature.st_rank"),
        exhibition_rank: asField(snapshot?.exhibition_rank, "feature_snapshot", "snapshot.feature.exhibition_rank"),
        exhibition_st: asField(
          predictionValueFromCoverage(meta("exhibition_st"), racer?.exhibitionSt),
          meta("exhibition_st")?.source || "snapshot.racer.exhibition_st",
          "snapshot.coverage.exhibition_st",
          meta("exhibition_st") || {}
        ),
        exhibition_time: asField(
          predictionValueFromCoverage(meta("exhibition_time"), racer?.exhibitionTime),
          meta("exhibition_time")?.source || "snapshot.racer.exhibition_time",
          "snapshot.coverage.exhibition_time",
          meta("exhibition_time") || {}
        ),
        lap_time: asField(
          predictionValueFromCoverage(meta("lapTime"), racer?.kyoteiBiyoriLapTime ?? racer?.lapTime),
          meta("lapTime")?.source || "snapshot.racer.lap_time",
          "snapshot.coverage.lapTime",
          meta("lapTime") || {}
        ),
        stability_rate: asField(meta("stability_rate")?.value, meta("stability_rate")?.source || "estimated_from_snapshot", "snapshot.coverage.stability_rate", meta("stability_rate") || {}),
        breakout_rate: asField(meta("breakout_rate")?.value, meta("breakout_rate")?.source || "estimated_from_snapshot", "snapshot.coverage.breakout_rate", meta("breakout_rate") || {}),
        sashi_rate: asField(meta("sashi_rate")?.value, meta("sashi_rate")?.source || "estimated_from_snapshot", "snapshot.coverage.sashi_rate", meta("sashi_rate") || {}),
        makuri_rate: asField(meta("makuri_rate")?.value, meta("makuri_rate")?.source || "estimated_from_snapshot", "snapshot.coverage.makuri_rate", meta("makuri_rate") || {}),
        makurisashi_rate: asField(meta("makurisashi_rate")?.value, meta("makurisashi_rate")?.source || "estimated_from_snapshot", "snapshot.coverage.makurisashi_rate", meta("makurisashi_rate") || {}),
        zentsuke_tendency: asField(meta("zentsuke_tendency")?.value, meta("zentsuke_tendency")?.source || "estimated_from_snapshot", "snapshot.coverage.zentsuke_tendency", meta("zentsuke_tendency") || {}),
        attack_quality: asField(attackQuality, "estimated_from_snapshot", "snapshot.derived.attack_quality"),
        outer_entry_tendency: asField(outerEntryTendency, "estimated_from_snapshot", "snapshot.derived.outer_entry_tendency")
      }
    };
  });

  return {
    race: {
      date: data?.race?.date || null,
      venue_id: venueId,
      venue_name: data?.race?.venueName || null,
      race_no: toInt(data?.race?.raceNo, null)
    },
    venue: {
      inside_bias: asField(toNum(VENUE_INNER_BIAS[venueId] ?? 0.62, null) * 100, "stored_lookup", "snapshot.venue.inside_bias"),
      one_course_trust: asField(venueScenario.one_course_trust, "stored_lookup", "snapshot.venue.one_course_trust"),
      two_course_sashi_remain_rate: asField(venueScenario.two_course_sashi_remain_rate, "stored_lookup", "snapshot.venue.two_course_sashi_remain_rate"),
      three_course_attack_success_rate: asField(venueScenario.three_course_attack_success_rate, "stored_lookup", "snapshot.venue.three_course_attack_success_rate"),
      four_course_develop_sashi_rate: asField(venueScenario.four_course_develop_sashi_rate, "stored_lookup", "snapshot.venue.four_course_develop_sashi_rate"),
      outer_renyuu_entry_rate: asField(venueScenario.outer_renyuu_entry_rate, "stored_lookup", "snapshot.venue.outer_renyuu_entry_rate"),
      lane56_renyuu_intrusion_rate: asField(venueScenario.lane56_renyuu_intrusion_rate, "stored_lookup", "snapshot.venue.lane56_renyuu_intrusion_rate"),
      escape_fail_total_risk: asField(venueScenario.escape_fail_pattern?.total_risk, "stored_lookup", "snapshot.venue.escape_fail_total_risk"),
      profile: venueScenario
    },
    lanes
  };
}

function laneStyleScenarioScore(laneRow, normalized) {
  const lane = laneRow?.lane;
  const f = laneRow?.features || {};
  const venueProfile = normalized?.venue?.profile || getVenueScenarioContext(normalized?.race?.venue_id);
  const venueInsideBias = normalized?.venue?.inside_bias?.value ?? 62;
  const venueLaneBias = getVenueLaneBiasScore(venueProfile, lane);
  const laneStyleCode =
    lane === 1 ? "nige" : lane === 2 ? "sashi" : lane === 3 ? "makuri" : lane === 4 ? "makurisashi" : "outside_entry";
  const venueStyleFit = getVenueStyleMatchScore(venueProfile, lane, laneStyleCode);
  const styleFit =
    lane === 1
      ? scoreBlend([
          { value: f.course_1_head_rate?.value, weight: 0.42 },
          { value: f.lane_1st_rate?.value, weight: 0.24 },
          { value: f.stability_rate?.value, weight: 0.18 },
          { value: venueInsideBias, weight: 0.16 }
        ])
      : lane === 2
        ? scoreBlend([
            { value: f.sashi_rate?.value, weight: 0.34 },
            { value: f.lane_2ren_rate?.value, weight: 0.28 },
            { value: f.lane_course_rate?.value, weight: 0.22 },
            { value: f.stability_rate?.value, weight: 0.16 }
          ])
        : lane === 3
          ? scoreBlend([
              { value: f.makuri_rate?.value, weight: 0.28 },
              { value: f.makurisashi_rate?.value, weight: 0.24 },
              { value: f.attack_quality?.value, weight: 0.24 },
              { value: f.lane_1st_rate?.value, weight: 0.12 },
              { value: f.lane_3ren_rate?.value, weight: 0.12 }
            ])
          : lane === 4
            ? scoreBlend([
                { value: f.breakout_rate?.value, weight: 0.28 },
                { value: f.zentsuke_tendency?.value, weight: 0.2 },
                { value: f.attack_quality?.value, weight: 0.22 },
                { value: f.lane_2ren_rate?.value, weight: 0.14 },
                { value: f.lane_3ren_rate?.value, weight: 0.16 }
              ])
            : scoreBlend([
                { value: f.outer_entry_tendency?.value, weight: 0.26 },
                { value: f.makuri_rate?.value, weight: 0.18 },
              { value: f.lane_2ren_rate?.value, weight: 0.16 },
              { value: f.lane_3ren_rate?.value, weight: 0.18 },
              { value: f.motor_total_score?.value, weight: 0.22 }
            ]);
  const venueEscapeFailFit = lane === 1
    ? clamp(0, 100, 100 - getVenueEscapeFailPressure(venueProfile, 1))
    : weightedAverage([
        { value: getVenueEscapeFailPressure(venueProfile, lane), weight: 0.56 },
        { value: f.attack_quality?.value, weight: 0.24 },
        { value: f.lane_course_rate?.value, weight: 0.2 }
      ]);

  const recentPerformanceSupport = scoreBlend([
    { value: safeNorm(f.national_win_rate?.value, 4, 8.5) === null ? null : safeNorm(f.national_win_rate?.value, 4, 8.5) * 100, weight: 0.34 },
    { value: safeNorm(f.local_win_rate?.value, 4, 8.5) === null ? null : safeNorm(f.local_win_rate?.value, 4, 8.5) * 100, weight: 0.28 },
    { value: safeNorm(f.motor_total_score?.value, 0, 18) === null ? null : safeNorm(f.motor_total_score?.value, 0, 18) * 100, weight: 0.2 },
    { value: invNorm(f.avg_st?.value, 0.11, 0.24) === null ? null : invNorm(f.avg_st?.value, 0.11, 0.24) * 100, weight: 0.18 }
  ]);

  return scoreBlend([
    { value: styleFit, weight: 0.42 },
    { value: venueLaneBias, weight: 0.14 },
    { value: venueStyleFit, weight: 0.12 },
    { value: venueEscapeFailFit, weight: 0.08 },
    { value: f.lane_course_rate?.value, weight: 0.1 },
    { value: safeNorm(f.motor_3ren?.value, 25, 75) === null ? null : safeNorm(f.motor_3ren?.value, 25, 75) * 100, weight: 0.08 },
    { value: safeNorm(f.motor_total_score?.value, 0, 18) === null ? null : safeNorm(f.motor_total_score?.value, 0, 18) * 100, weight: 0.1 },
    { value: recentPerformanceSupport, weight: 0.12 }
  ]);
}

function describeHardScenario({ laneScores = {}, headProb1 = 0, outsideBreakRisk = 0 }) {
  const ranked = Object.entries(laneScores)
    .map(([lane, score]) => ({ lane: Number(String(lane).replace("lane", "")), score: Number(score) || 0 }))
    .sort((a, b) => b.score - a.score);
  const top = ranked[0] || { lane: 1, score: 0 };
  if (top.lane === 1) {
    return headProb1 >= 0.55 && outsideBreakRisk <= 0.24 ? "escape_repro" : "escape_with_pressure";
  }
  if (top.lane === 2) return "boat2_sashi_repro";
  if (top.lane === 3) return "boat3_attack_repro";
  if (top.lane === 4) return "boat4_cado_repro";
  return "outside_mix_repro";
}

function buildMissingFieldDetails(normalized) {
  const details = {};
  for (const laneRow of normalized?.lanes || []) {
    const lane = Number(laneRow?.lane);
    for (const [key, field] of Object.entries(laneRow?.features || {})) {
      if (field?.value !== null) continue;
      details[`lane${lane}.${key}`] = {
        reason: field?.missing_reason || "precomputed feature missing",
        source: field?.source || null,
        status: field?.status || null,
        required: field?.required === true,
        lane,
        field: key
      };
    }
  }
  return details;
}

function computeScores(normalized) {
  const lanes = new Map((normalized?.lanes || []).map((row) => [row.lane, row]));
  const venueProfile = normalized?.venue?.profile || getVenueScenarioContext(normalized?.race?.venue_id);
  const boat1 = lanes.get(1);
  const lane2 = lanes.get(2);
  const lane3 = lanes.get(3);
  const lane4 = lanes.get(4);
  const lane5 = lanes.get(5);
  const lane6 = lanes.get(6);
  const missingFieldDetails = buildMissingFieldDetails(normalized);
  const fallbackTracker = buildFallbackTracker();
  const missingFields = [];
  const requiredCoverageMissingFields = Object.entries(missingFieldDetails)
    .filter(([, detail]) => detail?.required === true)
    .map(([field]) => field);
  const optionalCoverageMissingFields = Object.entries(missingFieldDetails)
    .filter(([, detail]) => detail?.required !== true)
    .map(([field]) => field);

  if (!boat1 || !lane2 || !lane3 || !lane4 || !lane5 || !lane6) {
    return {
      scores: {},
      features: {},
      fixed1234_matrix: {},
      fixed1234_top4: [],
      suggested_shape: null,
      data_status: "BROKEN_PIPELINE",
      confidence_status: "BROKEN_PIPELINE",
      decision: "SKIP",
      decision_reason: "lane snapshots are incomplete",
      hard_race_rank: "BROKEN_PIPELINE",
      fallback_used: fallbackTracker,
      missing_fields: ["snapshot.lanes"],
      missing_field_details: missingFieldDetails,
      metric_status: {}
    };
  }

  const f1 = boat1.features;
  const lane1CourseHeadResolved = resolveMetricWithFallback({
    field: "lane1_course_head_rate",
    primaryValue: f1.course_1_head_rate.value,
    fallbackValue: f1.course_1_2ren_rate.value,
    fallbackFormula: "fallback to lane1 2ren rate",
    tracker: fallbackTracker
  });
  const lane1CourseHead = lane1CourseHeadResolved.value;
  if (!Number.isFinite(lane1CourseHead)) missingFields.push("lane1.course_1_head_rate");
  const lane1MotorStReadiness = venueMotorStReadiness(f1);
  const lane1VenueHeadBonus = clamp(
    0,
    100,
    50 +
      toNum(venueProfile?.lane1_head_boost, 0) * 1.9 +
      ((lane1MotorStReadiness || 0) >= 52 ? toNum(venueProfile?.lane1_motor_st_synergy_boost, 0) * 2.1 : 0)
  );

  const lane1Strength = scoreBlend([
    { value: safeNorm(lane1CourseHead, 20, 80) === null ? null : safeNorm(lane1CourseHead, 20, 80) * 100, weight: 0.24 },
    { value: safeNorm(f1.national_win_rate.value, 4, 8.5) === null ? null : safeNorm(f1.national_win_rate.value, 4, 8.5) * 100, weight: 0.14 },
    { value: safeNorm(f1.local_win_rate.value, 4, 8.5) === null ? null : safeNorm(f1.local_win_rate.value, 4, 8.5) * 100, weight: 0.13 },
    { value: invNorm(f1.avg_st.value, 0.11, 0.24) === null ? null : invNorm(f1.avg_st.value, 0.11, 0.24) * 100, weight: 0.12 },
    { value: invNorm((f1.f_count.value || 0) * 0.8 + (f1.l_count.value || 0) * 0.4, 0, 2.5) === null ? null : invNorm((f1.f_count.value || 0) * 0.8 + (f1.l_count.value || 0) * 0.4, 0, 2.5) * 100, weight: 0.1 },
    { value: safeNorm(weightedAverage([{ value: f1.motor_2ren.value, weight: 0.6 }, { value: f1.boat_2ren.value, weight: 0.4 }]), 20, 60) === null ? null : safeNorm(weightedAverage([{ value: f1.motor_2ren.value, weight: 0.6 }, { value: f1.boat_2ren.value, weight: 0.4 }]), 20, 60) * 100, weight: 0.14 },
    { value: normalized?.venue?.inside_bias?.value ?? null, weight: 0.08 },
    { value: venueProfile?.one_course_trust, weight: 0.08 },
    { value: clamp(0, 100, 100 - getVenueEscapeFailPressure(venueProfile, 1)), weight: 0.05 },
    { value: clamp(0, 100, 50 + toNum(venueProfile?.venue_escape_bias, 0) * 2 + toNum(venueProfile?.venue_inside_stability, 0) * 1.4), weight: 0.03 },
    { value: lane1VenueHeadBonus, weight: 0.07 }
  ]);

  const laneRemainScore = (laneRow, role = "generic") => {
    const f = laneRow.features;
    const lane = laneRow?.lane;
    const venueLaneBias = getVenueLaneBiasScore(venueProfile, lane);
    const venueStyleFit = getVenueStyleMatchScore(
      venueProfile,
      lane,
      role === "attack" ? "makuri" : role === "develop" ? "makurisashi" : role === "outside" ? "outside_entry" : "sashi"
    );
    return scoreBlend([
      { value: f.lane_course_rate.value, weight: 0.3 },
      { value: f.attack_quality.value, weight: role === "attack" ? 0.24 : 0.16 },
      { value: safeNorm(f.motor_total_score.value, 0, 18) === null ? null : safeNorm(f.motor_total_score.value, 0, 18) * 100, weight: 0.16 },
      { value: invNorm(f.avg_st.value, 0.11, 0.24) === null ? null : invNorm(f.avg_st.value, 0.11, 0.24) * 100, weight: 0.18 },
      { value: safeNorm(f.entry_advantage_score.value, 0, 14) === null ? null : safeNorm(f.entry_advantage_score.value, 0, 14) * 100, weight: 0.12 },
      { value: venueLaneBias, weight: 0.04 },
      { value: venueStyleFit, weight: 0.04 },
      {
        value: clamp(
          0,
          100,
          50 +
            (role === "attack" ? toNum(venueProfile?.venue_makuri_bias, 0) * 2 : 0) +
            (role === "develop" ? toNum(venueProfile?.venue_makurizashi_bias, 0) * 2 : 0) +
            (role === "second" ? toNum(venueProfile?.venue_sashi_bias, 0) * 1.8 : 0) +
            (role === "outside" ? toNum(venueProfile?.venue_outer_3rd_bias, 0) * 1.6 : 0) +
            (role === "second" ? toNum(venueProfile?.lane2_second_boost, 0) * 1.5 : 0) +
            (role === "attack" ? toNum(venueProfile?.lane3_attack_boost, 0) * 1.7 + toNum(venueProfile?.lane3_second_boost, 0) * 0.9 : 0) +
            (role === "develop" ? toNum(venueProfile?.lane4_develop_boost, 0) * 1.9 : 0) +
            (role === "outside" ? toNum(venueProfile?.volatility_boost, 0) * 0.9 - toNum(venueProfile?.lane56_head_penalty, 0) * 0.6 : 0) +
            toNum(venueProfile?.venue_start_importance, 0) * (role === "attack" || role === "develop" ? 0.8 : 0.3)
        ),
        weight: 0.02
      }
    ]);
  };

  const lane2SecondRemain = laneRemainScore(lane2, "second");
  const lane3AttackRemain = laneRemainScore(lane3, "attack");
  const lane4DevelopRemain = laneRemainScore(lane4, "develop");
  const lane5Pressure = laneRemainScore(lane5, "outside");
  const lane6Pressure = laneRemainScore(lane6, "outside");
  const lane1ScenarioRepro = laneStyleScenarioScore(boat1, normalized);
  const lane2ScenarioRepro = laneStyleScenarioScore(lane2, normalized);
  const lane3ScenarioRepro = laneStyleScenarioScore(lane3, normalized);
  const lane4ScenarioRepro = laneStyleScenarioScore(lane4, normalized);
  const lane5ScenarioRepro = laneStyleScenarioScore(lane5, normalized);
  const lane6ScenarioRepro = laneStyleScenarioScore(lane6, normalized);
  const scenarioReproScore = scoreBlend([
    { value: lane1ScenarioRepro, weight: 0.26 },
    { value: lane2ScenarioRepro, weight: 0.14 },
    { value: lane3ScenarioRepro, weight: 0.2 },
    { value: lane4ScenarioRepro, weight: 0.18 },
    { value: lane5ScenarioRepro, weight: 0.11 },
    { value: lane6ScenarioRepro, weight: 0.11 }
  ]);
  const laneScenarioScores = {
    lane1: round(lane1ScenarioRepro, 1),
    lane2: round(lane2ScenarioRepro, 1),
    lane3: round(lane3ScenarioRepro, 1),
    lane4: round(lane4ScenarioRepro, 1),
    lane5: round(lane5ScenarioRepro, 1),
    lane6: round(lane6ScenarioRepro, 1)
  };

  const pairFit = (left, right) => scoreBlend([
    { value: left, weight: 0.42 },
    { value: right, weight: 0.42 },
    { value: clamp(0, 100, 100 - Math.abs((left || 50) - (right || 50))), weight: 0.16 }
  ]);

  const pair23 = resolveMetricWithFallback({
    field: "pair23_fit",
    primaryValue: pairFit(lane2SecondRemain, lane3AttackRemain),
    fallbackValue: lane2?.features?.lane_course_rate?.value,
    fallbackFormula: "lane2 course rate fallback",
    tracker: fallbackTracker
  });
  const pair24 = resolveMetricWithFallback({
    field: "pair24_fit",
    primaryValue: pairFit(lane2SecondRemain, lane4DevelopRemain),
    fallbackValue: lane4?.features?.lane_course_rate?.value,
    fallbackFormula: "lane4 course rate fallback",
    tracker: fallbackTracker
  });
  const pair34 = resolveMetricWithFallback({
    field: "pair34_fit",
    primaryValue: pairFit(lane3AttackRemain, lane4DevelopRemain),
    fallbackValue: weightedAverage([{ value: lane3?.features?.lane_course_rate?.value, weight: 0.5 }, { value: lane4?.features?.lane_course_rate?.value, weight: 0.5 }]),
    fallbackFormula: "lane3/lane4 course rate fallback",
    tracker: fallbackTracker
  });

  const pairSupportFit = scoreBlend([
    { value: pair23.value, weight: 0.34 },
    { value: pair24.value, weight: 0.33 },
    { value: pair34.value, weight: 0.33 }
  ]);
  const opponent234Fit = scoreBlend([
    { value: pairSupportFit, weight: 0.38 },
    { value: lane2SecondRemain, weight: 0.14 },
    { value: lane3AttackRemain, weight: 0.14 },
    { value: lane4DevelopRemain, weight: 0.14 },
    { value: venueProfile?.two_course_sashi_remain_rate, weight: 0.06 },
    { value: venueProfile?.three_course_attack_success_rate, weight: 0.07 },
    { value: venueProfile?.four_course_develop_sashi_rate, weight: 0.07 }
  ]);

  const killEscapeRisk = resolveMetricWithFallback({
    field: "kill_escape_risk",
    primaryValue: scoreBlend([
      { value: lane3AttackRemain, weight: 0.36 },
      { value: lane4DevelopRemain, weight: 0.17 },
      { value: lane3?.features?.makuri_rate?.value, weight: 0.14 },
      { value: lane3?.features?.makurisashi_rate?.value, weight: 0.08 },
      { value: lane4?.features?.breakout_rate?.value, weight: 0.05 },
      { value: lane3ScenarioRepro, weight: 0.12 },
      { value: lane4ScenarioRepro, weight: 0.08 }
    ]),
    fallbackValue: scoreBlend([{ value: lane3?.features?.lane_course_rate?.value, weight: 0.6 }, { value: lane4?.features?.lane_course_rate?.value, weight: 0.4 }]),
    fallbackFormula: "lane3/lane4 course rate fallback",
    tracker: fallbackTracker
  }).value;

  const shapeShuffleRisk = resolveMetricWithFallback({
    field: "shape_shuffle_risk",
    primaryValue: scoreBlend([
      { value: clamp(0, 100, Math.abs((lane2SecondRemain || 50) - (lane3AttackRemain || 50)) * 1.05), weight: 0.21 },
      { value: clamp(0, 100, Math.abs((lane2SecondRemain || 50) - (lane4DevelopRemain || 50)) * 1.05), weight: 0.18 },
      { value: clamp(0, 100, Math.abs((lane3AttackRemain || 50) - (lane4DevelopRemain || 50))), weight: 0.12 },
      { value: lane3?.features?.makurisashi_rate?.value, weight: 0.14 },
      { value: lane4?.features?.breakout_rate?.value, weight: 0.12 },
      { value: lane4?.features?.zentsuke_tendency?.value, weight: 0.08 },
      { value: clamp(0, 100, Math.abs((lane3ScenarioRepro || 50) - (lane4ScenarioRepro || 50))), weight: 0.15 }
    ]),
    fallbackValue: clamp(0, 100, Math.abs((pair23.value || 50) - (pair24.value || 50))),
    fallbackFormula: "pair fit divergence fallback",
    tracker: fallbackTracker
  }).value;

  const outsideHeadRisk = scoreBlend([
    { value: lane5Pressure, weight: 0.24 },
    { value: lane6Pressure, weight: 0.24 },
    { value: lane5?.features?.makuri_rate?.value, weight: 0.12 },
    { value: lane6?.features?.makuri_rate?.value, weight: 0.12 },
    { value: lane5ScenarioRepro, weight: 0.09 },
    { value: lane6ScenarioRepro, weight: 0.09 },
    { value: venueProfile?.outer_renyuu_entry_rate, weight: 0.08 },
    { value: clamp(0, 100, 50 + toNum(venueProfile?.venue_outer_3rd_bias, 0) * 1.8 - toNum(venueProfile?.venue_escape_bias, 0) * 0.7 - toNum(venueProfile?.lane56_head_penalty, 0) * 1.8 + toNum(venueProfile?.volatility_boost, 0) * 0.5), weight: 0.06 }
  ]);
  const outside2ndRisk = clamp(0, 1, (scoreBlend([
    { value: lane5Pressure, weight: 0.3 },
    { value: lane6Pressure, weight: 0.3 },
    { value: lane5?.features?.outer_entry_tendency?.value, weight: 0.16 },
    { value: lane6?.features?.outer_entry_tendency?.value, weight: 0.16 },
    { value: venueProfile?.outer_renyuu_entry_rate, weight: 0.08 },
    { value: clamp(0, 100, 50 + toNum(venueProfile?.volatility_boost, 0) * 1.1), weight: 0.06 }
  ]) || 0) / 100);
  const outside3rdRisk = clamp(0, 1, (scoreBlend([
    { value: lane5?.features?.lane_course_rate?.value, weight: 0.32 },
    { value: lane6?.features?.lane_course_rate?.value, weight: 0.32 },
    { value: lane5?.features?.motor_total_score?.value, weight: 0.14 },
    { value: lane6?.features?.motor_total_score?.value, weight: 0.14 },
    { value: venueProfile?.lane56_renyuu_intrusion_rate, weight: 0.06 },
    { value: clamp(0, 100, 50 + toNum(venueProfile?.venue_outer_3rd_bias, 0) * 2.2 + toNum(venueProfile?.volatility_boost, 0) * 1.2), weight: 0.02 }
  ]) || 0) / 100);
  const outsideBoxBreakRisk = clamp(0, 1, weightedAverage([
    { value: (outsideHeadRisk || 0) / 100, weight: 0.48 },
    { value: outside2ndRisk, weight: 0.32 },
    { value: outside3rdRisk, weight: 0.2 }
  ]) || 0);
  const outsideBreakRisk = clamp(0, 1, weightedAverage([
    { value: (outsideHeadRisk || 0) / 100, weight: 0.46 },
    { value: outside2ndRisk, weight: 0.32 },
    { value: outside3rdRisk, weight: 0.06 },
    { value: outsideBoxBreakRisk, weight: 0.16 }
  ]) || 0);

  const p1Score = weightedAverage([
    { value: lane1Strength, weight: 0.35 },
    { value: lane1ScenarioRepro, weight: 0.11 },
    { value: 100 - (killEscapeRisk || 0), weight: 0.18 },
    { value: 100 - (shapeShuffleRisk || 0), weight: 0.08 },
    { value: 100 - (outsideHeadRisk || 0), weight: 0.06 },
    { value: pairSupportFit, weight: 0.1 },
    { value: scenarioReproScore, weight: 0.04 },
    { value: 100 - (lane3?.features?.makuri_rate?.value || 0), weight: 0.04 },
    { value: 100 - (lane4?.features?.breakout_rate?.value || 0), weight: 0.04 },
    { value: clamp(0, 100, 50 + toNum(venueProfile?.venue_123_box_tightness, 0) * 1.6), weight: 0.04 },
    { value: lane1VenueHeadBonus, weight: 0.06 }
  ]);
  const p1Escape = clamp(0, 1, (p1Score || 0) / 100);
  const hardScenario = describeHardScenario({
    laneScores: laneScenarioScores,
    headProb1: p1Escape,
    outsideBreakRisk
  });
  const boat1EscapeTrust = round(p1Escape * 100, 1);
  const boat1HeadPre = round(p1Escape, 4);

  const headProbMap = normalizeProbabilities({
    1: Math.max(0.001, p1Escape),
    2: Math.max(0.001, (lane2SecondRemain || 1) / 100 * 0.52),
    3: Math.max(0.001, (lane3AttackRemain || 1) / 100 * (0.62 + Math.max(0, toNum(venueProfile?.lane3_attack_boost, 0)) * 0.01)),
    4: Math.max(0.001, (lane4DevelopRemain || 1) / 100 * (0.56 + Math.max(0, toNum(venueProfile?.lane4_develop_boost, 0)) * 0.008)),
    5: Math.max(0.001, (outsideHeadRisk || 1) / 100 * Math.max(0.08, 0.24 - Math.max(0, toNum(venueProfile?.lane56_head_penalty, 0)) * 0.01)),
    6: Math.max(0.001, (outsideHeadRisk || 1) / 100 * Math.max(0.06, 0.2 - Math.max(0, toNum(venueProfile?.lane56_head_penalty, 0)) * 0.01))
  });

  const secondWeights = normalizeWeights({
    2: Math.max(0.0001, lane2SecondRemain || 0),
    3: Math.max(0.0001, lane3AttackRemain || 0),
    4: Math.max(0.0001, lane4DevelopRemain || 0)
  });
  const thirdConditionalWeights = {
    "2": normalizeWeights({ 3: Math.max(0.0001, pair23.value || 0), 4: Math.max(0.0001, pair24.value || 0) }),
    "3": normalizeWeights({ 2: Math.max(0.0001, pair23.value || 0), 4: Math.max(0.0001, pair34.value || 0) }),
    "4": normalizeWeights({ 2: Math.max(0.0001, pair24.value || 0), 3: Math.max(0.0001, pair34.value || 0) })
  };

  const fixed1234RawMatrix = {};
  for (const combo of FIXED_COMBOS) {
    const [, second, third] = combo.split("-").map(Number);
    fixed1234RawMatrix[combo] = round((headProbMap[1] || 0) * (secondWeights[second] || 0) * (thirdConditionalWeights[String(second)]?.[third] || 0), 6);
  }
  const fixed1234TotalProbability = round(Object.values(fixed1234RawMatrix).reduce((sum, value) => sum + (value || 0), 0), 4);
  const fixed1234Matrix = normalizeComboMatrix(fixed1234RawMatrix);
  const fixed1234Top4 = Object.entries(fixed1234Matrix)
    .map(([combo, probability]) => ({ combo, probability }))
    .sort((a, b) => b.probability - a.probability)
    .slice(0, 4);
  const top4Fixed1234Probability = round(fixed1234Top4.reduce((sum, row) => sum + row.probability, 0), 4);
  const fixed1234ShapeConcentration = round(top4Fixed1234Probability, 4);
  const boxHitScore = fixed1234TotalProbability;
  const shapeFocusScore = fixed1234ShapeConcentration;
  const fit234Index = round(opponent234Fit, 1);
  const outsideBreakRiskPre = round(outsideBreakRisk, 4);
  const hardRaceIndex = round(scoreBlend([
    { value: boat1EscapeTrust, weight: 0.27 },
    { value: pairSupportFit, weight: 0.15 },
    { value: opponent234Fit, weight: 0.16 },
    { value: fixed1234TotalProbability * 100, weight: 0.14 },
    { value: (1 - outsideBreakRisk) * 100, weight: 0.08 },
    { value: scenarioReproScore, weight: 0.08 },
    { value: venueProfile?.one_course_trust, weight: 0.05 },
    { value: clamp(0, 100, 100 - toNum(venueProfile?.escape_fail_pattern?.total_risk, 28)), weight: 0.04 },
    { value: weightedAverage([
      { value: venueProfile?.two_course_sashi_remain_rate, weight: 0.34 },
      { value: venueProfile?.three_course_attack_success_rate, weight: 0.33 },
      { value: venueProfile?.four_course_develop_sashi_rate, weight: 0.33 }
    ]), weight: 0.03 },
    { value: clamp(0, 100, 50 + toNum(venueProfile?.venue_123_box_tightness, 0) * 1.5 - toNum(venueProfile?.venue_outside_break_risk, 0) * 1.2), weight: 0.1 },
    { value: clamp(0, 100, 50 + toNum(venueProfile?.lane1_head_boost, 0) * 1.5 + toNum(venueProfile?.lane2_second_boost, 0) * 0.6 + toNum(venueProfile?.lane3_attack_boost, 0) * 0.6 + toNum(venueProfile?.lane4_develop_boost, 0) * 0.5 + toNum(venueProfile?.volatility_boost, 0) * 0.4), weight: 0.04 }
  ]), 1);

  const unresolved = [];
  if (!Number.isFinite(lane1CourseHead)) unresolved.push("snapshot.feature.lane1_course_head_rate");
  if (!Number.isFinite(pair23.value)) unresolved.push("computed.pair23_fit");
  if (!Number.isFinite(pair24.value)) unresolved.push("computed.pair24_fit");
  if (!Number.isFinite(pair34.value)) unresolved.push("computed.pair34_fit");
  if (!Number.isFinite(killEscapeRisk)) unresolved.push("computed.kill_escape_risk");
  if (!Number.isFinite(shapeShuffleRisk)) unresolved.push("computed.shape_shuffle_risk");

  const dataStatus = unresolved.length > 0 || requiredCoverageMissingFields.length > 0
    ? "BROKEN_PIPELINE"
    : fallbackTracker.used || optionalCoverageMissingFields.length > 0
      ? "FALLBACK"
      : "READY";
  const buyPolicy = venueProfile?.buyPolicy || null;
  const insideHeadTight = buyPolicy?.code === "inside_head_focus";
  const attack34Capture = buyPolicy?.code === "attack_34_capture";
  const wideCoverage = buyPolicy?.code === "wide_coverage_watch";

  const decision =
    dataStatus === "BROKEN_PIPELINE"
      ? "SKIP"
      : boat1HeadPre >= (insideHeadTight ? 0.6 : 0.58) &&
        hardRaceIndex >= (insideHeadTight ? 71 : attack34Capture ? 68 : 70) &&
        fit234Index >= (insideHeadTight ? 64 : attack34Capture ? 61 : 63) &&
        outsideBreakRiskPre <= (wideCoverage ? 0.2 : 0.18)
        ? "BUY-4"
        : boat1HeadPre >= (insideHeadTight ? 0.54 : 0.52) &&
          hardRaceIndex >= (wideCoverage ? 59 : 61) &&
          fit234Index >= (attack34Capture ? 55 : 56) &&
          outsideBreakRiskPre <= (wideCoverage ? 0.28 : 0.24)
          ? "BUY-6"
          : boat1HeadPre >= (wideCoverage ? 0.42 : 0.45) &&
            hardRaceIndex >= (wideCoverage ? 52 : 54) &&
            fit234Index >= (attack34Capture ? 49 : 50) &&
            outsideBreakRiskPre <= (wideCoverage ? 0.36 : 0.32)
          ? "BORDERLINE"
          : "SKIP";

  const hardRaceRank =
    dataStatus === "BROKEN_PIPELINE"
      ? "BROKEN_PIPELINE"
      : boat1HeadPre >= 0.58 && hardRaceIndex >= 68
        ? "A"
        : boat1HeadPre >= 0.48 && hardRaceIndex >= 58
          ? "B"
          : "SKIP";

  return {
    scores: {
      hard_race_score: hardRaceIndex,
      boat1_head_pre: boat1HeadPre,
      hard_race_index: hardRaceIndex,
      boat1_escape_trust: boat1EscapeTrust,
      fit_234_index: fit234Index,
      opponent_234_fit: round(opponent234Fit, 1),
      pair23_fit: round(pair23.value, 1),
      pair24_fit: round(pair24.value, 1),
      pair34_fit: round(pair34.value, 1),
      kill_escape_risk: round(killEscapeRisk, 1),
      shape_shuffle_risk: round(shapeShuffleRisk, 1),
      makuri_risk: round(weightedAverage([{ value: killEscapeRisk, weight: 0.65 }, { value: shapeShuffleRisk, weight: 0.35 }]), 1),
      outside_head_risk: round((outsideHeadRisk || 0) / 100, 4),
      outside_2nd_risk: round(outside2ndRisk, 4),
      outside_3rd_risk: round(outside3rdRisk, 4),
      outside_box_break_risk: round(outsideBoxBreakRisk, 4),
      outside_break_risk_pre: outsideBreakRiskPre,
      outside_break_risk: round(outsideBreakRisk, 4),
      head_prob_1: headProbMap[1],
      head_prob_2: headProbMap[2],
      head_prob_3: headProbMap[3],
      head_prob_4: headProbMap[4],
      head_prob_5: headProbMap[5],
      head_prob_6: headProbMap[6],
      box_hit_score: round(boxHitScore, 4),
      shape_focus_score: round(shapeFocusScore, 4),
      fixed1234_total_probability: fixed1234TotalProbability,
      top4_fixed1234_probability: top4Fixed1234Probability,
      fixed1234_shape_concentration: fixed1234ShapeConcentration,
      p_123: fixed1234Matrix["1-2-3"] || 0,
      p_124: fixed1234Matrix["1-2-4"] || 0,
      p_132: fixed1234Matrix["1-3-2"] || 0,
      p_134: fixed1234Matrix["1-3-4"] || 0,
      p_142: fixed1234Matrix["1-4-2"] || 0,
      p_143: fixed1234Matrix["1-4-3"] || 0
    },
    features: {
      p1_escape: round(p1Escape, 4),
      p1_formula: "pre-race blend of lane1 course-head rate, nationwide/local win rate, avg ST, F/L safety, motor/boat strength, venue bias, 3/4-course pressure, and outside head pressure",
      scenario_repro_score: round(scenarioReproScore, 1),
      lane_scenario_repro_scores: laneScenarioScores,
      venue_scenario_bias: venueProfile,
      top4_share_within_fixed1234: round(fixed1234ShapeConcentration, 4),
      conditional_probabilities: {
        p_1st_1: round(headProbMap[1], 4),
        p_2nd_given_1st1: secondWeights,
        p_3rd_given_1st1_2nd: thirdConditionalWeights,
        fixed1234_raw_total_probability: fixed1234TotalProbability,
        fixed1234_normalized_matrix: fixed1234Matrix
      },
      evaluation_targets: {
        y_box6: 0,
        y_top4: 0,
        y_buy6: 0,
        y_buy4: 0
      },
      buyPolicy,
      venueBiasProfile: venueProfile?.venueBiasProfile || venueProfile?.venue_bias_profile || null,
      venueAdjustmentReason: Array.isArray(venueProfile?.venueAdjustmentReason) ? venueProfile.venueAdjustmentReason : []
    },
    fixed1234_matrix: fixed1234Matrix,
    fixed1234_top4: fixed1234Top4,
    suggested_shape: fixed1234Top4[0]?.combo === "1-4-2" || fixed1234Top4[0]?.combo === "1-4-3"
      ? "1-24-234"
      : fixed1234Top4[0]?.combo === "1-3-2" || fixed1234Top4[0]?.combo === "1-3-4"
        ? "1-34-234"
        : "1-23-234",
    data_status: dataStatus,
    confidence_status: dataStatus,
    decision,
    hardScenario,
    hardScenarioScore: round(scenarioReproScore, 1),
    scenario_repro_score: round(scenarioReproScore, 1),
    decision_reason:
      dataStatus === "BROKEN_PIPELINE"
        ? "precomputed features are incomplete"
        : dataStatus === "FALLBACK"
          ? "some metrics use stored-feature fallback formulas"
          : decision === "BUY-4"
            ? "pre-race 1-head confidence is high and the 234 fit is dense enough for four-ticket operation"
            : decision === "BUY-6"
              ? "pre-race hard-race gate passed with enough 234 support"
              : decision === "BORDERLINE"
                ? "boat 1 head remains viable before exhibition, but shape density is still modest"
                : "pre-race outside pressure or low 1-head probability keeps this out of hard-race range",
    hard_race_rank: hardRaceRank,
    operational_pick:
      decision === "BUY-4" || decision === "BUY-6"
        ? "買う"
        : decision === "BORDERLINE"
          ? "穴候補"
          : "見送り",
    fallback_used: fallbackTracker,
    head_candidate_ranking: Object.entries(headProbMap).map(([lane, probability]) => ({ lane: Number(lane), probability, score: round(probability * 100, 1) })).sort((a, b) => b.probability - a.probability),
    head_candidates: Object.entries(headProbMap).map(([lane, probability]) => ({ lane: Number(lane), probability, score: round(probability * 100, 1) })).sort((a, b) => b.probability - a.probability).slice(0, 2),
    head_opponents: [
      { lane: 2, score: round(lane2SecondRemain, 1) },
      { lane: 3, score: round(lane3AttackRemain, 1) },
      { lane: 4, score: round(lane4DevelopRemain, 1) }
    ].sort((a, b) => b.score - a.score),
    outside_danger_scenarios: [
      { label: "5,6 head", risk: round((outsideHeadRisk || 0) / 100, 4) },
      { label: "5,6 2nd", risk: round(outside2ndRisk, 4) },
      { label: "5,6 3rd", risk: round(outside3rdRisk, 4) },
      { label: "box break", risk: round(outsideBoxBreakRisk, 4) }
    ],
    buyPolicy,
    venueBiasProfile: venueProfile?.venueBiasProfile || venueProfile?.venue_bias_profile || null,
    venueAdjustmentReason: Array.isArray(venueProfile?.venueAdjustmentReason) ? venueProfile.venueAdjustmentReason : [],
    hard_mode: { active: decision === "BUY-4" || decision === "BUY-6" },
    open_mode: { active: decision === "SKIP" && dataStatus !== "BROKEN_PIPELINE" },
    missing_fields: [...requiredCoverageMissingFields, ...optionalCoverageMissingFields, ...unresolved],
    missing_field_details: missingFieldDetails,
    metric_status: {}
  };
}

export async function buildHardRace1234Response({ data, date, venueId, raceNo }) {
  const artifactDir = buildArtifactDir({ date, venueId, raceNo });
  const normalizedData = normalizeRaceData({ data });
  const computed = computeScores(normalizedData);
  const sourceSummary = buildSourceSummary({
    data,
    normalizedLanes: normalizedData.lanes,
    fallbackSummary: computed.fallback_used,
    missingFields: computed.missing_fields
  });
  const fetchedUrls = {
    boatrace: {},
    kyoteibiyori: {}
  };
  const saved = saveArtifacts({
    dir: artifactDir,
    fetchedUrls,
    normalizedData,
    scores: computed
  });

  return {
    race_no: toInt(raceNo, null),
    status: computed.data_status,
    data_status: computed.data_status,
    confidence_status: computed.confidence_status,
    hardScenario: computed.hardScenario || null,
    hardScenarioScore: computed.hardScenarioScore ?? computed.features?.scenario_repro_score ?? null,
    scenario_repro_score: computed.scenario_repro_score ?? computed.features?.scenario_repro_score ?? null,
    ...computed.scores,
    boat1_head_pre: computed.scores?.boat1_head_pre ?? null,
    boat1_escape_trust: computed.scores?.boat1_escape_trust ?? null,
    fit_234_index: computed.scores?.fit_234_index ?? computed.scores?.opponent_234_fit ?? null,
    outside_break_risk_pre: computed.scores?.outside_break_risk_pre ?? computed.scores?.outside_break_risk ?? null,
    hard_race_index: computed.scores?.hard_race_index ?? computed.scores?.hard_race_score ?? null,
    box_hit_score: computed.scores?.box_hit_score ?? null,
    fixed1234_shape_concentration: computed.scores?.fixed1234_shape_concentration ?? null,
    fixed1234_matrix: computed.fixed1234_matrix,
    fixed1234_top4: computed.fixed1234_top4,
    suggested_shape: computed.suggested_shape,
    hard_race_rank: computed.hard_race_rank,
    operational_pick: computed.operational_pick,
    decision: computed.decision,
    decision_reason: computed.decision_reason,
    fallback_used: {
      used: !!computed.fallback_used?.used,
      fields: Object.keys(computed.fallback_used?.byField || {}),
      details: computed.fallback_used?.byField || {}
    },
    head_candidates: computed.head_candidates,
    head_opponents: computed.head_opponents,
    head_candidate_ranking: computed.head_candidate_ranking,
    outside_danger_scenarios: computed.outside_danger_scenarios,
    hard_mode: computed.hard_mode,
    open_mode: computed.open_mode,
    missing_fields: computed.missing_fields,
    missing_field_details: computed.missing_field_details,
    metric_status: computed.metric_status,
    source_summary: sourceSummary,
    fetched_urls: fetchedUrls,
    fetch_timings: {
      hard_race_total_ms: 0,
      pure_inference: true
    },
    raw_saved_paths: saved.rawSavedPaths,
    parsed_saved_paths: saved.parsedSavedPaths,
    normalized_data: normalizedData,
    venue_scenario_bias: normalizedData?.venue?.profile || null,
    venueBiasProfile: computed.venueBiasProfile || normalizedData?.venue?.profile?.venueBiasProfile || null,
    buyPolicy: computed.buyPolicy || normalizedData?.venue?.profile?.buyPolicy || null,
    venueAdjustmentReason: computed.venueAdjustmentReason || normalizedData?.venue?.profile?.venueAdjustmentReason || [],
    features: {
      ...computed.features,
      source_summary: sourceSummary
    },
    scores: computed.scores,
    screeningDebug: {
      fetch_success: true,
      parse_success: Array.isArray(normalizedData?.lanes) && normalizedData.lanes.length === 6,
      score_success: computed.data_status !== "BROKEN_PIPELINE",
      pure_inference: true,
      data_status: computed.data_status,
      decision_reason: computed.decision_reason,
      source_summary: sourceSummary,
      fetched_urls: fetchedUrls,
      normalized_data_path: saved.parsedSavedPaths.normalized_data || null,
      hard_race_scores_path: saved.parsedSavedPaths.hard_race_scores || null,
      optional_fields_missing: computed.data_status === "FALLBACK" ? computed.missing_fields : [],
      missing_required_scores: computed.data_status === "BROKEN_PIPELINE" ? computed.missing_fields : []
    }
  };
}
