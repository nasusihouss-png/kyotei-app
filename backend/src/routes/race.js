import { Router } from "express";
import { getRaceData } from "../services/boatrace.js";
import { saveRace } from "../../save-race.js";
import { buildRaceFeatures } from "../../feature-engine.js";
import { rankRace } from "../../score-engine.js";
import { analyzeRacePattern } from "../../race-pattern-engine.js";
import { applyCoursePerformanceFeatures } from "../../course-performance-engine.js";
import { applyMotorPerformanceFeatures } from "../../motor-performance-engine.js";
import { applyMotorTrendFeatures } from "../../motor-trend-engine.js";
import { applyEntryDynamicsFeatures } from "../../entry-dynamics-engine.js";
import { analyzeExpectedValue } from "../../odds-engine.js";
import { buildBetPlan } from "../../bankroll-engine.js";
import { evaluateRaceRisk } from "../../race-risk-engine.js";
import { savePredictionLog } from "../../save-prediction-log.js";
import { saveRaceResult } from "../../save-result.js";
import {
  buildRaceIdFromParts,
  compareActualTop3VsPredictedBets,
  normalizeFinishOrder
} from "../../result-utils.js";
import { markSettlementHits } from "../../save-settlement-log.js";
import db from "../../db.js";
import { applyVenueAdjustments } from "../../venue-adjustment-engine.js";
import { saveFeatureSnapshots } from "../../save-feature-snapshots.js";
import { simulateTrifectaProbabilities } from "../../monte-carlo-engine.js";
import { analyzeRaceIndexes } from "../../race-index-engine.js";
import { estimateRaceOutcomeProbabilities } from "../../race-outcome-probability-engine.js";
import { buildTicketStrategy } from "../../ticket-strategy-engine.js";
import { analyzeHeadAndPartners } from "../../head-partner-selection-engine.js";
import { analyzeRaceFlow } from "../../race-flow-engine.js";
import { analyzePlayerStartProfiles } from "../../player-start-profile-engine.js";
import { evaluatePartnerPrecision } from "../../partner-precision-engine.js";
import { evaluateLane2Wall } from "../../wall-evaluation-engine.js";
import { evaluateHeadConfidence } from "../../head-confidence-engine.js";
import { evaluateHeadPrecision } from "../../head-precision-engine.js";
import { generateTicketsV2 } from "../../ticket-generation-v2-engine.js";
import { analyzeHitQuality } from "../../hit-quality-engine.js";
import { analyzePreRaceForm } from "../../pre-race-form-engine.js";
import { analyzeRoleCandidates } from "../../candidate-role-engine.js";
import { refineRaceRiskWithStructure } from "../../risk-structure-engine.js";
import { analyzeRaceStructure } from "../../race-structure-engine.js";
import { optimizeTickets } from "../../ticket-optimization-engine.js";
import { decideRaceSelection } from "../../race-selection-engine.js";
import { buildStakeAllocationPlan } from "../../stake-allocation-engine.js";
import { analyzeExhibitionAI } from "../../exhibition-ai-engine.js";
import { detectValue } from "../../value-detection-engine.js";
import { detectMarketTraps } from "../../market-trap-detector.js";
import {
  analyzeVenueBias,
  applyVenueBiasToRisk,
  applyVenueBiasToStructure
} from "../../venue-bias-engine.js";
import {
  createPlacedBet,
  createPlacedBets,
  updatePlacedBet,
  deletePlacedBet,
  listPlacedBets,
  enforceRecommendationOnlyForBets,
  settlePlacedBetsForRace,
  getPlacedBetSummaries
} from "../../placed-bet-service.js";
import { runSelfLearning } from "../../self-learning-engine.js";
import { saveRaceStartDisplaySnapshot, saveRaceStartDisplayResult } from "../../race-start-display-store.js";
import { attachPredictionFeatureLogSettlement, savePredictionFeatureLog } from "../../prediction-feature-log.js";
import { buildScenarioSuggestions } from "../../scenario-suggestion-engine.js";
import { buildBetExplainability, buildRaceExplainability } from "../../explainability-engine.js";
import { applyContenderSynergy } from "../../contender-synergy-engine.js";
import {
  getManualLapEvaluation,
  saveManualLapEvaluation
} from "../../manual-lap-evaluation-store.js";
import {
  getActiveLearningWeights,
  getLatestLearningRun,
  rollbackLearningWeights,
  applyLearningBatchManually,
  runContinuousLearningIfNeeded
} from "../../learning-weight-engine.js";
import {
  buildVerifiedLearningRows,
  ensureVerificationRecordColumns,
  getPredictionSnapshot,
  listLatestVerificationRecords,
  listPredictionSnapshots,
  mapPredictionSnapshotRow
} from "../../prediction-snapshot-store.js";

export const raceRouter = Router();

db.exec(`
  CREATE TABLE IF NOT EXISTS race_verification_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id TEXT NOT NULL,
    race_date TEXT,
    venue_code INTEGER,
    venue_name TEXT,
    race_no INTEGER,
    verified_at TEXT DEFAULT CURRENT_TIMESTAMP,
    predicted_top3 TEXT,
    actual_top3 TEXT,
    hit_miss TEXT,
    mismatch_categories_json TEXT,
    verification_summary_json TEXT,
    is_hidden_from_results INTEGER NOT NULL DEFAULT 0,
    is_invalid_verification INTEGER NOT NULL DEFAULT 0,
    exclude_from_learning INTEGER NOT NULL DEFAULT 0,
    invalid_reason TEXT,
    invalidated_at TEXT
  );
`);

function ensureVerificationLogColumns() {
  const cols = db.prepare("PRAGMA table_info(race_verification_logs)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("race_date")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN race_date TEXT");
  if (!names.has("venue_code")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN venue_code INTEGER");
  if (!names.has("venue_name")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN venue_name TEXT");
  if (!names.has("race_no")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN race_no INTEGER");
  if (!names.has("is_hidden_from_results")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN is_hidden_from_results INTEGER NOT NULL DEFAULT 0");
  if (!names.has("is_invalid_verification")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN is_invalid_verification INTEGER NOT NULL DEFAULT 0");
  if (!names.has("exclude_from_learning")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN exclude_from_learning INTEGER NOT NULL DEFAULT 0");
  if (!names.has("invalid_reason")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN invalid_reason TEXT");
  if (!names.has("invalidated_at")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN invalidated_at TEXT");
  ensureVerificationRecordColumns();
}

ensureVerificationLogColumns();

function parseBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const text = String(value).trim().toLowerCase();
  if (["1", "true", "on", "yes", "y"].includes(text)) return true;
  if (["0", "false", "off", "no", "n"].includes(text)) return false;
  return fallback;
}

function resolveRecommendationOnlyMode(req) {
  const envEnabled = parseBooleanFlag(process.env.RECOMMENDATION_ONLY, false);
  const requestEnabled = parseBooleanFlag(
    req?.body?.recommendation_only ??
      req?.query?.recommendation_only ??
      req?.headers?.["x-recommendation-only"],
    false
  );
  // request can tighten to ON, but cannot disable server-level ON
  return envEnabled || requestEnabled;
}

function safeJsonParse(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toNullableNum(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function pct(numerator, denominator) {
  if (!denominator) return 0;
  return Number(((numerator / denominator) * 100).toFixed(2));
}

function normalizeRecommendation(value) {
  const raw = String(value || "").trim().toUpperCase().replace(/\s+/g, " ");
  if (raw === "FULL BET") return "FULL BET";
  if (raw === "SMALL BET") return "SMALL BET";
  if (raw === "MICRO BET") return "MICRO BET";
  if (raw === "SKIP") return "SKIP";
  return "UNKNOWN";
}

function initBucket() {
  return {
    raceIds: new Set(),
    totalBetAmount: 0,
    totalBetCount: 0,
    hitCount: 0,
    totalPayout: 0,
    totalProfitLoss: 0,
    evSum: 0,
    evCount: 0
  };
}

function finalizeBucket(bucket) {
  return {
    total_races: bucket.raceIds.size,
    total_bets: bucket.totalBetAmount,
    total_bet_count: bucket.totalBetCount,
    hit_rate: pct(bucket.hitCount, bucket.totalBetCount),
    recovery_rate: pct(bucket.totalPayout, bucket.totalBetAmount),
    total_profit_loss: bucket.totalProfitLoss,
    average_ev_of_placed_bets: bucket.evCount
      ? Number((bucket.evSum / bucket.evCount).toFixed(4))
      : 0
  };
}

function sumRows(rows, pick) {
  return (Array.isArray(rows) ? rows : []).reduce((acc, row) => acc + toNum(row?.[pick]), 0);
}

function calcRates({ bet, payout, hit, total }) {
  return {
    hit_rate: pct(hit, total),
    recovery_rate: pct(payout, bet)
  };
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function dateKey(value) {
  const text = String(value || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{8}$/.test(text)) return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  return null;
}

function localTodayKey() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function getDateMode(targetDate) {
  const target = dateKey(targetDate);
  const today = localTodayKey();
  if (!target) return { target, today, mode: "unknown", isFuture: false, isSameDay: false };
  if (target > today) return { target, today, mode: "future", isFuture: true, isSameDay: false };
  if (target === today) return { target, today, mode: "same_day", isFuture: false, isSameDay: true };
  return { target, today, mode: "past", isFuture: false, isSameDay: false };
}

function normalizeModeValue(mode) {
  const raw = String(mode || "").toUpperCase();
  if (raw === "FULL_BET") return "FULL_BET";
  if (raw === "FULL BET") return "FULL_BET";
  if (raw === "SMALL_BET") return "SMALL_BET";
  if (raw === "SMALL BET") return "SMALL_BET";
  if (raw === "MICRO_BET") return "MICRO_BET";
  if (raw === "MICRO BET") return "MICRO_BET";
  if (raw === "SKIP") return "SKIP";
  return "UNKNOWN";
}

function denormalizeModeValue(mode) {
  const m = normalizeModeValue(mode);
  if (m === "FULL_BET") return "FULL BET";
  if (m === "SMALL_BET") return "SMALL BET";
  if (m === "MICRO_BET") return "MICRO BET";
  if (m === "SKIP") return "SKIP";
  return "UNKNOWN";
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeSavedBetSnapshotItems(items) {
  return safeArray(items)
    .map((row) => {
      const combo = normalizeCombo(row?.combo ?? row);
      if (!combo || combo.split("-").length !== 3) return null;
      return {
        ...(row && typeof row === "object" ? row : {}),
        combo,
        prob: Number.isFinite(Number(row?.prob)) ? Number(row.prob) : null,
        odds: Number.isFinite(Number(row?.odds)) ? Number(row.odds) : null,
        ev: Number.isFinite(Number(row?.ev)) ? Number(row.ev) : null,
        ticket_type: row?.ticket_type || "backup",
        recommended_bet: Number.isFinite(Number(row?.recommended_bet))
          ? Number(row.recommended_bet)
          : Number.isFinite(Number(row?.bet))
            ? Number(row.bet)
            : 100,
        explanation_tags: Array.isArray(row?.explanation_tags) ? [...row.explanation_tags] : [],
        explanation_summary: row?.explanation_summary || null,
        trap_flags: Array.isArray(row?.trap_flags) ? [...row.trap_flags] : [],
        explanation_reasons: Array.isArray(row?.explanation_reasons) ? [...row.explanation_reasons] : []
      };
    })
    .filter(Boolean);
}

function buildFeatureContributionComponents(row) {
  const f = row?.features || {};
  const exhibitionRank = toNum(f.exhibition_rank, 6);
  const stRank = toNum(f.st_rank, 6);
  const expectedActualStRank = toNum(f.expected_actual_st_rank, 6);
  return {
    player_score_component: Number((
      toNum(f.class_score, 0) * 4 +
      toNum(f.nationwide_win_rate, 0) * 1.8 +
      toNum(f.local_win_rate, 0) * 2.2 +
      toNum(f.local_minus_nation, 0) * 1.2
    ).toFixed(3)),
    motor_score_component: Number((
      toNum(f.motor2_rate, 0) * 0.32 +
      toNum(f.boat2_rate, 0) * 0.18 +
      toNum(f.motor_total_score, 0) +
      toNum(f.motor_trend_score, 0)
    ).toFixed(3)),
    exhibition_score_component: Number((
      (exhibitionRank === 1 ? 8 : exhibitionRank === 2 ? 4 : 0) +
      toNum(f.tilt_bonus, 0) +
      toNum(f.course_fit_score, 0)
    ).toFixed(3)),
    start_st_score_component: Number((
      toNum(f.st_inv, 0) * 24 +
      toNum(f.expected_actual_st_inv, 0) * 16 +
      (stRank === 1 ? 7 : stRank === 2 ? 3 : 0) +
      (expectedActualStRank === 1 ? 4 : expectedActualStRank === 2 ? 2 : 0)
    ).toFixed(3)),
    formation_pattern_bias_component: Number(toNum(f.escape_second_place_bias_score, 0).toFixed(3)),
    left_neighbor_bias_component: Number((
      Math.max(0, toNum(f.display_time_delta_vs_left, 0)) * 10 +
      Math.max(0, toNum(f.avg_st_rank_delta_vs_left, 0)) * 1.5 +
      toNum(f.slit_alert_flag, 0) * 6
    ).toFixed(3)),
    f_hold_bias_component: Number((-toNum(f.f_hold_caution_penalty, 0)).toFixed(3)),
    scenario_bias_component: Number((
      toNum(f.entry_advantage_score, 0) +
      toNum(f.contender_bonus, 0) +
      toNum(f.venue_lane_adjustment, 0)
    ).toFixed(3))
  };
}

function confidenceBandLabel(value, thresholds = {}) {
  const n = toNum(value, null);
  if (!Number.isFinite(n)) return null;
  const highMin = toNum(thresholds?.highMin ?? thresholds?.high_min, 80);
  const mediumMin = toNum(thresholds?.mediumMin ?? thresholds?.medium_min, 60);
  if (n >= highMin) return "high";
  if (n >= mediumMin) return "medium";
  return "low";
}

function normalizeParticipationState(value) {
  const text = String(value || "").trim().toLowerCase();
  if (text === "recommended" || text === "participate" || text === "full_bet" || text === "full bet") return "recommended";
  if (text === "watch" || text === "small_bet" || text === "small bet" || text === "micro_bet" || text === "micro bet") {
    return "watch";
  }
  if (text === "not_recommended" || text === "skip") return "not_recommended";
  return null;
}

function scenarioMatchBucketLabel(value) {
  const n = toNum(value, null);
  if (!Number.isFinite(n)) return null;
  if (n >= 70) return "high";
  if (n >= 55) return "medium";
  return "low";
}

function overlapBucketLabel(overlapLanes) {
  const count = Array.isArray(overlapLanes) ? overlapLanes.length : 0;
  if (count >= 2) return "strong";
  if (count === 1) return "partial";
  return "none";
}

function fHoldZoneLabel(ranking) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const inside = rows.some((row) => {
    const lane = toInt(row?.racer?.lane, null);
    return lane >= 1 && lane <= 3 && toNum(row?.features?.f_hold_bias_applied, 0) === 1;
  });
  const outside = rows.some((row) => {
    const lane = toInt(row?.racer?.lane, null);
    return lane >= 4 && toNum(row?.features?.f_hold_bias_applied, 0) === 1;
  });
  if (inside && outside) return "mixed";
  if (inside) return "inside";
  if (outside) return "outside";
  return "none";
}

function getSegmentCorrectionValue(learningWeights, type, key, field) {
  if (!type || !key || !field) return 0;
  const segment = learningWeights?.segmented_corrections?.[String(type)]?.[String(key)];
  const values = segment?.correction_values || {};
  return toNum(values?.[field], 0);
}

function rowsHaveFHold(ranking) {
  return (Array.isArray(ranking) ? ranking : []).some((row) => toNum(row?.features?.f_hold_bias_applied, 0) === 1);
}

function buildSegmentCorrectionUsageSummary({
  learningWeights,
  race,
  entryMeta,
  escapePatternAnalysis,
  scenarioSuggestions,
  contenderSignals,
  ranking,
  confidenceScores
}) {
  const calibrationThresholds = learningWeights?.confidence_calibration || {};
  const segments = [
    ["venue", toInt(race?.venueId, null)],
    ["predicted_entry_pattern", Array.isArray(entryMeta?.predicted_entry_order) ? entryMeta.predicted_entry_order.join("-") : null],
    ["actual_entry_pattern", Array.isArray(entryMeta?.actual_entry_order) ? entryMeta.actual_entry_order.join("-") : null],
    ["entry_change_present", entryMeta?.entry_changed ? "changed" : "unchanged"],
    ["entry_type", entryMeta?.entry_change_type || null],
    ["formation_pattern", escapePatternAnalysis?.formation_pattern || null],
    ["scenario_type", scenarioSuggestions?.scenario_type || null],
    ["scenario_match_bucket", scenarioMatchBucketLabel(scenarioSuggestions?.scenario_confidence)],
    ["has_f_hold", rowsHaveFHold(ranking) ? "yes" : "no"],
    ["f_hold_zone", fHoldZoneLabel(ranking)],
    ["motor_exhibition_overlap_bucket", overlapBucketLabel(contenderSignals?.overlap_lanes)],
    ["participation_decision_state", normalizeParticipationState(confidenceScores?.participation_state_seed)],
    ["head_confidence_band", confidenceBandLabel(confidenceScores?.head_confidence_calibrated ?? confidenceScores?.head_fixed_confidence_pct, calibrationThresholds)],
    ["bet_confidence_band", confidenceBandLabel(confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct, calibrationThresholds)]
  ];
  const used = segments
    .map(([type, key]) => {
      const segment = learningWeights?.segmented_corrections?.[String(type)]?.[String(key)];
      if (!segment) return null;
      return {
        type,
        key: String(key),
        sample_count: toNum(segment?.sample_count, 0)
      };
    })
    .filter(Boolean);
  return {
    segment_count: used.length,
    segments: used
  };
}

function buildFinalRecommendedBetsSnapshot({
  recommendedBets,
  optimizedTickets
}) {
  const normalizedOptimized = normalizeSavedBetSnapshotItems(optimizedTickets);

  if (normalizedOptimized.length > 0) {
    return {
      snapshot_source: "optimized_tickets",
      items: normalizedOptimized
        .sort((a, b) => (Number.isFinite(b?.prob) ? b.prob : -1) - (Number.isFinite(a?.prob) ? a.prob : -1))
    };
  }

  const normalizedRecommended = normalizeSavedBetSnapshotItems(recommendedBets);

  return {
    snapshot_source: normalizedRecommended.length > 0 ? "bet_plan_recommended_bets" : "missing_final_recommended_bets_snapshot",
    items: normalizedRecommended
      .sort((a, b) => (Number.isFinite(b?.prob) ? b.prob : -1) - (Number.isFinite(a?.prob) ? a.prob : -1))
  };
}

function insertVerificationRecord({
  raceId,
  raceMeta,
  predictedTop3Text,
  actualTop3Text,
  hitMiss,
  mismatchCategories,
  summary
}) {
  return db.prepare(
    `
    INSERT INTO race_verification_logs (
      race_id,
      race_date,
      venue_code,
      venue_name,
      race_no,
      prediction_snapshot_id,
      verified_against_snapshot_id,
      verification_status,
      verification_reason,
      confirmed_result,
      head_hit,
      bet_hit,
      learning_ready,
      predicted_top3,
      actual_top3,
      hit_miss,
      mismatch_categories_json,
      verification_summary_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `
  ).run(
    raceId,
    raceMeta?.race_date || null,
    Number.isFinite(Number(raceMeta?.venue_id)) ? Number(raceMeta.venue_id) : null,
    raceMeta?.venue_name || null,
    Number.isFinite(Number(raceMeta?.race_no)) ? Number(raceMeta.race_no) : null,
    Number.isFinite(Number(summary?.prediction_snapshot_id)) ? Number(summary.prediction_snapshot_id) : null,
    Number.isFinite(Number(summary?.verified_against_snapshot_id)) ? Number(summary.verified_against_snapshot_id) : null,
    summary?.verification_status || null,
    summary?.verification_reason || null,
    summary?.confirmed_result_canonical || null,
    summary?.head_correct === true ? 1 : summary?.head_correct === false ? 0 : null,
    summary?.hit_miss === "HIT" ? 1 : summary?.hit_miss === "MISS" ? 0 : null,
    summary?.learning_ready ? 1 : 0,
    predictedTop3Text || null,
    actualTop3Text || null,
    hitMiss || null,
    JSON.stringify(Array.isArray(mismatchCategories) ? mismatchCategories : []),
    JSON.stringify(summary || {})
  );
}

function buildMismatchAnalysis({
  predictedTop3,
  actualTop3,
  predictedBets,
  predictionJson,
  raceRisk,
  featureLog
}) {
  const categories = [];
  const predictedCombo = predictedTop3.length === 3 ? predictedTop3.join("-") : null;
  const actualCombo = actualTop3.length === 3 ? actualTop3.join("-") : null;
  const recBets = safeArray(predictedBets)
    .map((b) => {
      const raw =
        typeof b === "string" || Array.isArray(b)
          ? b
          : b?.combo ?? b?.selection ?? null;
      return normalizeCombo(raw);
    })
    .filter((x) => x && x.split("-").length === 3);
  const recBetSet = new Set(recBets);
  const hitMatchFound = !!(actualCombo && recBetSet.has(actualCombo));
  const hitMiss = actualCombo ? (hitMatchFound ? "HIT" : "MISS") : "PENDING";

  const headCorrect =
    predictedTop3.length > 0 && actualTop3.length > 0 && Number(predictedTop3[0]) === Number(actualTop3[0]);
  if (headCorrect) categories.push("HEAD_HIT");
  else categories.push("HEAD_MISS");

  const secondThirdCorrect =
    predictedTop3.length >= 3 &&
    actualTop3.length >= 3 &&
    Number(predictedTop3[1]) === Number(actualTop3[1]) &&
    Number(predictedTop3[2]) === Number(actualTop3[2]);
  if (!secondThirdCorrect) categories.push("PARTNER_MISS");

  const entryChanged = !!predictionJson?.entry_changed;
  if (entryChanged && hitMiss === "MISS") categories.push("ENTRY_CHANGE_IMPACT");

  const reasonCodes = safeArray(raceRisk?.skip_reason_codes).map((x) => String(x || "").toUpperCase());
  if (reasonCodes.includes("CHAOS_HIGH") || reasonCodes.includes("ARE_INDEX_HIGH")) {
    if (hitMiss === "MISS") categories.push("CHAOS_UNDERESTIMATED");
  }

  const ranked = safeArray(predictionJson?.ranking);
  const topRank = ranked[0] || {};
  const topFeature = topRank?.features || {};
  if (!headCorrect && Number(topFeature?.exhibition_rank) === 1) categories.push("EXHIBITION_WEIGHT_BIAS");
  if (!headCorrect && Number(topFeature?.motor_total_score) >= 12) categories.push("MOTOR_WEIGHT_BIAS");
  if (!headCorrect && Number(topFeature?.class_score) >= 8) categories.push("PLAYER_WEIGHT_BIAS");

  if (recBets.length > 0 && actualCombo) {
    const hasHitTicket = recBetSet.has(actualCombo);
    if (!hasHitTicket && recBets.length <= 2) categories.push("UNDERSPREAD");
    if (!hasHitTicket && recBets.length >= 8) categories.push("OVERSPREAD");
  }

  if (featureLog?.entry_changed && hitMiss === "MISS" && !categories.includes("ENTRY_CHANGE_IMPACT")) {
    categories.push("ENTRY_CHANGE_IMPACT");
  }

  return {
    hit_miss: hitMiss,
    head_correct: headCorrect,
    second_third_correct: secondThirdCorrect,
    categories: [...new Set(categories)],
    predicted_combo: predictedCombo,
    actual_combo: actualCombo,
    verified_against_bets: [...recBetSet],
    confirmed_result_canonical: actualCombo,
    hit_match_found: hitMatchFound
  };
}

function parseTop3FromCombo(comboLike) {
  const digits = String(comboLike || "")
    .match(/[1-6]/g)
    || [];
  if (digits.length < 3) return [];
  return digits.slice(0, 3).map((x) => Number(x));
}

function downgradeModeForEntryChange(mode, severity) {
  const m = normalizeModeValue(mode);
  if (severity === "none") return m;
  if (severity === "high") {
    if (m === "FULL_BET") return "SMALL_BET";
    if (m === "SMALL_BET") return "MICRO_BET";
    return m;
  }
  if (severity === "medium") {
    if (m === "FULL_BET") return "SMALL_BET";
    return m;
  }
  return m;
}

function buildEntryOrderMeta(racers) {
  const rows = Array.isArray(racers) ? racers : [];
  const predicted_entry_order = rows
    .map((r) => toInt(r?.lane))
    .filter((v) => Number.isInteger(v) && v >= 1 && v <= 6)
    .sort((a, b) => a - b);
  const actual_entry_order = rows
    .map((r) => {
      const lane = toInt(r?.lane);
      const entry = toInt(r?.entryCourse, lane);
      return {
        lane,
        entry
      };
    })
    .filter((x) => Number.isInteger(x.lane))
    .sort((a, b) => {
      if (a.entry !== b.entry) return a.entry - b.entry;
      return a.lane - b.lane;
    })
    .map((x) => x.lane);

  const laneToEntry = new Map(
    rows
      .map((r) => [toInt(r?.lane), toInt(r?.entryCourse, toInt(r?.lane))])
      .filter((x) => Number.isInteger(x[0]))
  );

  let changedCount = 0;
  let maxShift = 0;
  for (const lane of predicted_entry_order) {
    const entry = laneToEntry.get(lane);
    if (!Number.isInteger(entry)) continue;
    if (entry !== lane) changedCount += 1;
    maxShift = Math.max(maxShift, Math.abs(entry - lane));
  }

  const lane1Entry = laneToEntry.get(1);
  const outerInvasion = rows.some((r) => {
    const lane = toInt(r?.lane);
    const entry = toInt(r?.entryCourse, lane);
    return Number.isInteger(lane) && Number.isInteger(entry) && lane >= 4 && entry <= 3;
  });
  const entry_changed = changedCount > 0;

  let entry_change_type = "none";
  if (entry_changed) {
    if (lane1Entry !== 1) entry_change_type = "lane1_lost_inside";
    else if (outerInvasion) entry_change_type = "outer_invasion";
    else if (changedCount >= 3) entry_change_type = "multi_shift";
    else entry_change_type = "minor_shift";
  }

  let severity = "none";
  if (entry_change_type === "lane1_lost_inside" || maxShift >= 2 || changedCount >= 3) severity = "high";
  else if (entry_change_type === "outer_invasion" || changedCount >= 2) severity = "medium";
  else if (entry_changed) severity = "low";

  return {
    predicted_entry_order,
    actual_entry_order,
    entry_changed,
    entry_change_type,
    changed_count: changedCount,
    max_shift: maxShift,
    severity
  };
}

function applyEntryChangeToDecision(raceDecision, entryMeta) {
  const meta = entryMeta || {};
  const original = raceDecision || {};
  const baseMode = normalizeModeValue(original.mode || "UNKNOWN");
  const downgradedMode = downgradeModeForEntryChange(baseMode, meta.severity || "none");
  const confidencePenalty =
    meta.severity === "high" ? 12 : meta.severity === "medium" ? 7 : meta.severity === "low" ? 3 : 0;
  const confidence = Math.max(0, Number(toNum(original.confidence, 0) - confidencePenalty).toFixed(2));
  const reasonCodes = Array.isArray(original.reason_codes) ? [...original.reason_codes] : [];
  if (meta.entry_changed && !reasonCodes.includes("ENTRY_ORDER_CHANGED")) {
    reasonCodes.push("ENTRY_ORDER_CHANGED");
  }
  const summary = meta.entry_changed
    ? `${original.summary || ""}${original.summary ? " / " : ""}進入変化考慮`
    : original.summary || "";

  return {
    ...original,
    mode: downgradedMode,
    confidence,
    reason_codes: reasonCodes,
    summary,
    entry_change_adjusted: !!meta.entry_changed
  };
}

function buildStartDisplaySignatureFromRacers(racers) {
  const rows = Array.isArray(racers) ? racers : [];
  return rows
    .map((r) => ({
      lane: toInt(r?.lane, null),
      entry: toInt(r?.entryCourse, toInt(r?.lane, null))
    }))
    .filter((x) => Number.isInteger(x.lane))
    .sort((a, b) => {
      if (a.entry !== b.entry) return a.entry - b.entry;
      return a.lane - b.lane;
    })
    .map((x) => x.lane)
    .join("-");
}

function ensureEntrySnapshotColumns() {
  const cols = db.prepare("PRAGMA table_info(entries)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("f_hold_count")) {
    db.exec("ALTER TABLE entries ADD COLUMN f_hold_count INTEGER");
  }
}

function loadRaceSnapshotFromDb({ date, venueId, raceNo }) {
  ensureEntrySnapshotColumns();
  const nullableNum = (value) => {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  };
  const raceId = buildRaceIdFromParts({ date, venueId, raceNo });
  if (!raceId) return null;

  const raceRow = db
    .prepare(
      `
      SELECT race_id, race_date, venue_id, venue_name, race_no, race_name, weather, wind_speed, wind_dir, wave_height
      FROM races
      WHERE race_id = ?
      LIMIT 1
    `
    )
    .get(raceId);
  if (!raceRow) return null;

  const entryRows = db
    .prepare(
      `
      SELECT
        lane,
        registration_no,
        name,
        class,
        branch,
        age,
        weight,
        avg_st,
        nationwide_win_rate,
        local_win_rate,
        motor2_rate,
        boat2_rate,
        exhibition_time,
        exhibition_st,
        f_hold_count,
        entry_course,
        tilt
      FROM entries
      WHERE race_id = ?
      ORDER BY lane ASC
    `
    )
    .all(raceId);
  if (!Array.isArray(entryRows) || entryRows.length !== 6) return null;

  return {
    source: {
      cache: {
        hit: false,
        fallback: "db_snapshot"
      }
    },
    race: {
      date: raceRow.race_date,
      venueId: toInt(raceRow.venue_id, null),
      venueName: raceRow.venue_name || null,
      raceNo: toInt(raceRow.race_no, null),
      raceName: raceRow.race_name || null,
      weather: raceRow.weather || null,
      windSpeed: nullableNum(raceRow.wind_speed),
      windDirection: raceRow.wind_dir || null,
      waveHeight: nullableNum(raceRow.wave_height)
    },
    racers: entryRows.map((r) => ({
      lane: toInt(r.lane, null),
      registrationNo: toInt(r.registration_no, null),
      name: r.name || null,
      class: r.class || null,
      branch: r.branch || null,
      age: toInt(r.age, null),
      weight: nullableNum(r.weight),
      avgSt: nullableNum(r.avg_st),
      nationwideWinRate: nullableNum(r.nationwide_win_rate),
      localWinRate: nullableNum(r.local_win_rate),
      motor2Rate: nullableNum(r.motor2_rate),
      boat2Rate: nullableNum(r.boat2_rate),
      exhibitionTime: nullableNum(r.exhibition_time),
      exhibitionSt: nullableNum(r.exhibition_st),
      fHoldCount: toInt(r.f_hold_count, 0),
      entryCourse: toInt(r.entry_course, null),
      tilt: nullableNum(r.tilt)
    }))
  };
}

async function resolveRaceDataForList({
  date,
  venueId,
  raceNo,
  allowRefresh,
  refreshTimeoutMs
}) {
  const snapshot = loadRaceSnapshotFromDb({ date, venueId, raceNo });
  if (snapshot) {
    return {
      data: snapshot,
      usedCachedData: true,
      partialData: false,
      warning: null
    };
  }
  if (!allowRefresh) {
    return {
      data: null,
      usedCachedData: false,
      partialData: true,
      warning: "snapshot_not_found"
    };
  }
  try {
    const fresh = await getRaceData({
      date,
      venueId,
      raceNo,
      timeoutMs: refreshTimeoutMs
    });
    return {
      data: fresh,
      usedCachedData: false,
      partialData: false,
      warning: null
    };
  } catch (err) {
    return {
      data: null,
      usedCachedData: false,
      partialData: true,
      warning: err?.message || "refresh_failed"
    };
  }
}

function loadStartSignatureTrendContext() {
  try {
    const summary = db
      .prepare(
        `
        SELECT
          COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hits,
          COALESCE(COUNT(*), 0) AS races
        FROM prediction_feature_logs
        WHERE hit_flag IN (0, 1)
      `
      )
      .get();
    const globalRaces = toNum(summary?.races, 0);
    const globalHits = toNum(summary?.hits, 0);
    const globalHitRate = globalRaces > 0 ? (globalHits / globalRaces) * 100 : 0;

    const bySignatureRows = db
      .prepare(
        `
        SELECT
          start_display_signature,
          COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hits,
          COALESCE(COUNT(*), 0) AS races
        FROM prediction_feature_logs
        WHERE hit_flag IN (0, 1)
          AND start_display_signature IS NOT NULL
          AND TRIM(start_display_signature) <> ''
        GROUP BY start_display_signature
      `
      )
      .all();
    const bySignature = new Map(
      bySignatureRows.map((row) => [
        String(row.start_display_signature),
        {
          races: toNum(row.races, 0),
          hits: toNum(row.hits, 0),
          hit_rate: toNum(row.races, 0) > 0 ? Number(((toNum(row.hits, 0) / toNum(row.races, 0)) * 100).toFixed(2)) : 0
        }
      ])
    );
    return {
      globalHitRate: Number(globalHitRate.toFixed(2)),
      globalRaces,
      bySignature
    };
  } catch {
    return {
      globalHitRate: 0,
      globalRaces: 0,
      bySignature: new Map()
    };
  }
}

function analyzeStartSignals(racers, entryMeta, signatureTrendContext) {
  const rows = Array.isArray(racers) ? racers : [];
  const stRows = rows
    .map((r) => ({
      lane: toInt(r?.lane, null),
      st: Number(r?.exhibitionST)
    }))
    .filter((x) => Number.isInteger(x.lane) && Number.isFinite(x.st) && x.st >= 0)
    .sort((a, b) => a.st - b.st);

  const signature = buildStartDisplaySignatureFromRacers(rows);
  const fastest = stRows[0] || null;
  const second = stRows[1] || null;
  const slowCount = stRows.filter((x) => x.st >= 0.23).length;
  const minSt = fastest ? fastest.st : null;
  const maxSt = stRows.length ? stRows[stRows.length - 1].st : null;
  const spread = Number.isFinite(minSt) && Number.isFinite(maxSt) ? Number((maxSt - minSt).toFixed(4)) : null;
  const topGap =
    fastest && second && Number.isFinite(fastest.st) && Number.isFinite(second.st)
      ? Number((second.st - fastest.st).toFixed(4))
      : null;

  let stabilityScore = 50;
  if (fastest) {
    if (fastest.lane <= 2) stabilityScore += 12;
    else if (fastest.lane <= 4) stabilityScore += 7;
    else stabilityScore += 2;
  }
  if (Number.isFinite(spread)) {
    if (spread > 0.16) stabilityScore -= 10;
    else if (spread > 0.12) stabilityScore -= 6;
    else if (spread < 0.07) stabilityScore += 6;
  }
  if (Number.isFinite(topGap)) {
    if (topGap >= 0.06) stabilityScore += 4;
    else if (topGap <= 0.015) stabilityScore -= 3;
  }
  stabilityScore -= Math.min(15, slowCount * 5);
  if ((entryMeta?.severity || "none") === "high") stabilityScore -= 7;
  else if ((entryMeta?.severity || "none") === "medium") stabilityScore -= 4;

  const signatureTrend = signatureTrendContext?.bySignature?.get(signature) || null;
  const globalHitRate = toNum(signatureTrendContext?.globalHitRate, 0);
  let signatureAdjustment = 0;
  if (signatureTrend && signatureTrend.races >= 20) {
    const delta = signatureTrend.hit_rate - globalHitRate;
    if (delta >= 8) signatureAdjustment += 5;
    else if (delta >= 4) signatureAdjustment += 2;
    else if (delta <= -8) signatureAdjustment -= 6;
    else if (delta <= -4) signatureAdjustment -= 3;
  }
  stabilityScore = clamp(0, 100, stabilityScore + signatureAdjustment);

  return {
    signature,
    fastest_st_lane: fastest?.lane ?? null,
    st_spread: spread,
    delayed_boat_count: slowCount,
    top_st_gap: topGap,
    stability_score: Number(stabilityScore.toFixed(2)),
    signature_trend: signatureTrend
      ? {
          races: signatureTrend.races,
          hit_rate: signatureTrend.hit_rate,
          global_hit_rate: globalHitRate
        }
      : null
  };
}

function applyStartSignalToDecision(raceDecision, startSignals, entryMeta) {
  const original = raceDecision || {};
  const currentMode = normalizeModeValue(original.mode || "UNKNOWN");
  const s = toNum(startSignals?.stability_score, 50);
  const severeEntry = (entryMeta?.severity || "none") === "high";

  let mode = currentMode;
  if (s < 34) mode = downgradeModeForEntryChange(mode, "high");
  else if (s < 44) mode = downgradeModeForEntryChange(mode, "medium");
  if (severeEntry && s < 46) mode = downgradeModeForEntryChange(mode, "high");

  const baseConfidence = toNum(original.confidence, 0);
  const confidenceAdjust = (s - 50) * 0.18 + (severeEntry ? -3 : 0);
  const confidence = clamp(0, 100, Number((baseConfidence + confidenceAdjust).toFixed(2)));

  const reasonCodes = Array.isArray(original.reason_codes) ? [...original.reason_codes] : [];
  if (s < 44 && !reasonCodes.includes("START_SIGNAL_UNSTABLE")) reasonCodes.push("START_SIGNAL_UNSTABLE");
  if (s >= 65 && !reasonCodes.includes("START_SIGNAL_STRONG")) reasonCodes.push("START_SIGNAL_STRONG");

  return {
    ...original,
    mode,
    confidence,
    reason_codes: reasonCodes
  };
}

function computeRecommendationScore({
  raceDecision,
  raceStructure,
  startSignals,
  entryMeta,
  race,
  learningWeights,
  contenderSignals,
  escapePatternAnalysis,
  scenarioSuggestions,
  ranking,
  attackScenarioAnalysis
}) {
  const lw = learningWeights || {};
  const confidence = toNum(raceDecision?.confidence, 0);
  const headStability = toNum(raceStructure?.head_stability_score, 50);
  const chaosRisk = toNum(raceStructure?.chaos_risk_score, 50);
  const startStability = toNum(startSignals?.stability_score, 50);
  const mode = normalizeModeValue(raceDecision?.mode || "UNKNOWN");
  const modeBase = mode === "FULL_BET" ? 12 : mode === "SMALL_BET" ? 8 : mode === "MICRO_BET" ? 4 : 0;
  const baseEntryPenalty = toNum(lw?.entry_changed_penalty, 6);
  const entryPenalty =
    (entryMeta?.severity || "none") === "high"
      ? baseEntryPenalty
      : (entryMeta?.severity || "none") === "medium"
        ? baseEntryPenalty * 0.55
        : 0;
  const startWeight = clamp(0.8, 1.2, toNum(lw?.start_signal_weight, 1));
  const venueWeight = clamp(0.8, 1.25, toNum(lw?.venue_correction_weight, 1));
  const gradeWeight = clamp(0.8, 1.25, toNum(lw?.grade_correction_weight, 1));
  const venueAdj =
    toNum(lw?.venue_score_adjustments?.[String(toInt(race?.venueId, 0))], 0) * venueWeight;
  const gradeKey = String(race?.grade || race?.raceGrade || "").trim();
  const gradeAdj = toNum(lw?.grade_score_adjustments?.[gradeKey], 0) * gradeWeight;
  const sigAdj = toNum(lw?.start_signature_score_adjustments?.[String(startSignals?.signature || "")], 0);
  const contenderConcentration = toNum(contenderSignals?.contender_concentration, 0);
  const overlapCount = toNum((contenderSignals?.overlap_lanes || []).length, 0);
  const predictedEntryPattern = Array.isArray(entryMeta?.predicted_entry_order) ? entryMeta.predicted_entry_order.join("-") : null;
  const actualEntryPattern = Array.isArray(entryMeta?.actual_entry_order) ? entryMeta.actual_entry_order.join("-") : null;
  const overlapBucket = overlapBucketLabel(contenderSignals?.overlap_lanes);
  const attackScenarioBoost =
    toNum(attackScenarioAnalysis?.attack_scenario_applied, 0) === 1
      ? clamp(-3, 6, (toNum(attackScenarioAnalysis?.attack_scenario_score, 0) - 54) * 0.11)
      : 0;
  const segmentRecommendationAdj =
    getSegmentCorrectionValue(lw, "venue", toInt(race?.venueId, null), "recommendation_score_adjustment") +
    getSegmentCorrectionValue(lw, "predicted_entry_pattern", predictedEntryPattern, "recommendation_score_adjustment") +
    getSegmentCorrectionValue(lw, "actual_entry_pattern", actualEntryPattern, "recommendation_score_adjustment") +
    getSegmentCorrectionValue(lw, "entry_change_present", entryMeta?.entry_changed ? "changed" : "unchanged", "entry_changed_penalty_delta") +
    getSegmentCorrectionValue(lw, "entry_type", entryMeta?.entry_change_type || null, "entry_changed_penalty_delta") +
    getSegmentCorrectionValue(lw, "formation_pattern", escapePatternAnalysis?.formation_pattern || null, "pattern_strength_adjustment") +
    getSegmentCorrectionValue(lw, "scenario_type", scenarioSuggestions?.scenario_type || null, "recommendation_score_adjustment") +
    getSegmentCorrectionValue(lw, "motor_exhibition_overlap_bucket", overlapBucket, "motor_lap_overlap_adjustment") +
    getSegmentCorrectionValue(lw, "has_f_hold", rowsHaveFHold(ranking) ? "yes" : "no", "caution_penalty_correction");
  const score = clamp(
    0,
    100,
    modeBase +
      confidence * 0.34 +
      headStability * 0.22 +
      startStability * 0.24 * startWeight +
      (100 - chaosRisk) * 0.2 -
      entryPenalty +
      contenderConcentration * 0.08 +
      overlapCount * 2.2 +
      attackScenarioBoost +
      venueAdj +
      gradeAdj +
      sigAdj +
      segmentRecommendationAdj
  );
  return Number(score.toFixed(2));
}

const ESCAPE_FORMATION_PATTERN_TABLE = {
  inside_lead: {
    label: "inside_lead",
    second_place_bias: { "1-2": 3, "1-3": 0, "1-4": 0, "1-5": -4, "1-6": 0 }
  },
  one_two_lead: {
    label: "one_two_lead",
    second_place_bias: { "1-2": 9, "1-3": 9, "1-4": 3, "1-5": -4, "1-6": 0 }
  },
  slow_line_lead: {
    label: "slow_line_lead",
    second_place_bias: { "1-2": 6, "1-3": 0, "1-4": -4, "1-5": -4, "1-6": -4 }
  },
  one_delayed: {
    label: "one_delayed",
    second_place_bias: { "1-2": 3, "1-3": -4, "1-4": 0, "1-5": 0, "1-6": 0 }
  },
  three_attacks_first: {
    label: "three_attacks_first",
    second_place_bias: { "1-2": 0, "1-3": 3, "1-4": -4, "1-5": -4, "1-6": 3 }
  },
  middle_bulge: {
    label: "middle_bulge",
    second_place_bias: { "1-2": 0, "1-3": 6, "1-4": -4, "1-5": -4, "1-6": 0 }
  },
  no_wall: {
    label: "no_wall",
    second_place_bias: { "1-2": -3, "1-3": 3, "1-4": 0, "1-5": 0, "1-6": 0 }
  },
  two_three_delayed: {
    label: "two_three_delayed",
    second_place_bias: { "1-2": -3, "1-3": -7, "1-4": 6, "1-5": 6, "1-6": -3 }
  },
  dash_lead: {
    label: "dash_lead",
    second_place_bias: { "1-2": 9, "1-3": 0, "1-4": 9, "1-5": 9, "1-6": 9 }
  },
  middle_dent: {
    label: "middle_dent",
    second_place_bias: { "1-2": 9, "1-3": 9, "1-4": 9, "1-5": 6, "1-6": 3 }
  },
  outside_lead: {
    label: "outside_lead",
    second_place_bias: { "1-2": 0, "1-3": -4, "1-4": 9, "1-5": 6, "1-6": 9 }
  }
};

function buildEscapeSecondPlaceCandidateScores(ranking) {
  return (Array.isArray(ranking) ? ranking : [])
    .map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      if (!Number.isInteger(lane) || lane <= 1) return null;
      const f = row?.features || {};
      const laneBias = lane === 2 ? 7 : lane === 3 ? 6 : lane === 4 ? 5 : lane === 5 ? 3 : 2;
      const score =
        toNum(row?.score, 0) * 0.1 +
        (7 - toNum(f.exhibition_rank, 6)) * 4 +
        (7 - toNum(f.expected_actual_st_rank ?? f.st_rank, 6)) * 4.5 +
        toNum(f.entry_advantage_score, 0) * 0.8 +
        toNum(f.slit_alert_flag, 0) * 7 -
        toNum(f.f_hold_caution_penalty, 0) * 0.6 +
        laneBias;
      return {
        lane,
        score: Number(score.toFixed(2))
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score);
}

function classifyEscapeFormationPattern({ primaryLane, secondaryLane, ranking }) {
  const lane2 = (Array.isArray(ranking) ? ranking : []).find((row) => toInt(row?.racer?.lane, null) === 2) || null;
  const lane3 = (Array.isArray(ranking) ? ranking : []).find((row) => toInt(row?.racer?.lane, null) === 3) || null;
  const lane4 = (Array.isArray(ranking) ? ranking : []).find((row) => toInt(row?.racer?.lane, null) === 4) || null;
  const lane5 = (Array.isArray(ranking) ? ranking : []).find((row) => toInt(row?.racer?.lane, null) === 5) || null;
  const lane6 = (Array.isArray(ranking) ? ranking : []).find((row) => toInt(row?.racer?.lane, null) === 6) || null;

  const slit2 = toNum(lane2?.features?.slit_alert_flag, 0);
  const slit3 = toNum(lane3?.features?.slit_alert_flag, 0);
  const slit4 = toNum(lane4?.features?.slit_alert_flag, 0);
  const expectedStRank2 = toNum(lane2?.features?.expected_actual_st_rank ?? lane2?.features?.st_rank, 6);
  const expectedStRank3 = toNum(lane3?.features?.expected_actual_st_rank ?? lane3?.features?.st_rank, 6);
  const expectedStRank4 = toNum(lane4?.features?.expected_actual_st_rank ?? lane4?.features?.st_rank, 6);
  const expectedStRank5 = toNum(lane5?.features?.expected_actual_st_rank ?? lane5?.features?.st_rank, 6);
  const expectedStRank6 = toNum(lane6?.features?.expected_actual_st_rank ?? lane6?.features?.st_rank, 6);
  const exDelta3 = toNum(lane3?.features?.display_time_delta_vs_left, 0);
  const exDelta4 = toNum(lane4?.features?.display_time_delta_vs_left, 0);
  const exDelta5 = toNum(lane5?.features?.display_time_delta_vs_left, 0);
  const lane2Delayed = expectedStRank2 >= 4 || toNum(lane2?.features?.f_hold_bias_applied, 0) === 1;
  const lane3Delayed = expectedStRank3 >= 4 || toNum(lane3?.features?.f_hold_bias_applied, 0) === 1;
  const middleDentSignal = expectedStRank2 >= 4 && expectedStRank3 >= 4;
  const outsideLeadSignal = primaryLane >= 4 && secondaryLane >= 5;
  const dashLeadSignal = primaryLane >= 4 || slit4 === 1 || exDelta4 >= 0.1 || exDelta5 >= 0.1;
  const noWallSignal = primaryLane === 3 && slit2 === 0 && expectedStRank2 >= 3;
  const threeAttackSignal = primaryLane === 3 && (slit3 === 1 || exDelta3 >= 0.1);

  if (outsideLeadSignal || (primaryLane >= 5 && expectedStRank5 <= expectedStRank3 && expectedStRank6 <= 4)) return "outside_lead";
  if (middleDentSignal) return "middle_dent";
  if (lane2Delayed && lane3Delayed && (primaryLane >= 4 || secondaryLane >= 4)) return "two_three_delayed";
  if (dashLeadSignal && (primaryLane >= 4 || secondaryLane >= 4)) return "dash_lead";
  if (threeAttackSignal && secondaryLane === 6) return "three_attacks_first";
  if (threeAttackSignal) return "middle_bulge";
  if (noWallSignal) return "no_wall";
  if (lane2Delayed && primaryLane === 2) return "slow_line_lead";
  if (toNum((Array.isArray(ranking) ? ranking : [])[0]?.features?.f_hold_bias_applied, 0) === 1) return "one_delayed";
  if (primaryLane === 2 && secondaryLane === 3) return "one_two_lead";
  return "inside_lead";
}

function getEscapeSecondPlaceBiasScore(biasByCombo, lane) {
  const comboKey = `1-${lane}`;
  return Number.isFinite(Number(biasByCombo?.[comboKey])) ? Number(biasByCombo[comboKey]) : 0;
}

function analyzeEscapeFormationLayer({ ranking, racePattern, indexes }) {
  const escapeIndex = toNum(indexes?.escape_index, 0);
  const sashiIndex = toNum(indexes?.sashi_index, 0);
  const makuriIndex = toNum(indexes?.makuri_index, 0);
  const makurizashiIndex = toNum(indexes?.makurizashi_index, 0);
  const chaosIndex = toNum(indexes?.chaos_index, 0);
  const dominantEscape =
    String(racePattern || "") === "escape" &&
    escapeIndex >= 60 &&
    escapeIndex >= Math.max(sashiIndex, makuriIndex, makurizashiIndex) + 4 &&
    chaosIndex < 72;
  const candidates = buildEscapeSecondPlaceCandidateScores(ranking);
  const primaryLane = candidates[0]?.lane ?? 2;
  const secondaryLane = candidates[1]?.lane ?? 3;
  const patternKey = classifyEscapeFormationPattern({ primaryLane, secondaryLane, ranking });
  const pattern = ESCAPE_FORMATION_PATTERN_TABLE[patternKey] || ESCAPE_FORMATION_PATTERN_TABLE.inside_lead;
  const topGap = toNum(candidates[0]?.score, 0) - toNum(candidates[1]?.score, 0);
  const confidence = clamp(
    0.25,
    0.95,
    0.48 + Math.max(0, escapeIndex - 60) * 0.006 + topGap * 0.01 - Math.max(0, chaosIndex - 55) * 0.004
  );

  return {
    dominant_escape: dominantEscape,
    formation_pattern: patternKey,
    formation_pattern_label: pattern.label,
    escape_pattern_applied: dominantEscape,
    escape_pattern_confidence: Number(confidence.toFixed(4)),
    formation_pattern_clarity_score: Number((confidence * 100).toFixed(2)),
    escape_second_place_bias_json: pattern.second_place_bias,
    second_place_candidate_scores: candidates
  };
}

function applyEscapeFormationBiasToRanking(ranking, escapePatternAnalysis, learningWeights = {}, race = null) {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!escapePatternAnalysis?.escape_pattern_applied) {
    return rows.map((row, idx) => ({
      ...row,
      rank: idx + 1,
      features: {
        ...(row?.features || {}),
        formation_pattern: escapePatternAnalysis?.formation_pattern || null,
        escape_pattern_applied: 0,
        escape_pattern_confidence: toNum(escapePatternAnalysis?.escape_pattern_confidence, 0),
        escape_second_place_bias: 0,
        escape_second_place_bias_score: 0
      }
    }));
  }

  const biasByLane = escapePatternAnalysis.escape_second_place_bias_json || {};
  const learnedPatternAdj = getSegmentCorrectionValue(
    learningWeights,
    "formation_pattern",
    escapePatternAnalysis?.formation_pattern || null,
    "second_place_bias_correction"
  );
  const learnedVenueAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "second_place_bias_correction"
  );
  return [...rows]
    .map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      const baseBiasScore = lane !== 1
        ? getEscapeSecondPlaceBiasScore(biasByLane, lane)
        : 0;
      const slitBoost = Math.max(0, toNum(row?.features?.slit_alert_flag, 0)) * 0.8;
      const nextBiasScore = Number((baseBiasScore + slitBoost + learnedPatternAdj + learnedVenueAdj).toFixed(2));
      const nextScore = Number((toNum(row?.score, 0) + nextBiasScore).toFixed(4));
      return {
        ...row,
        score: nextScore,
        features: {
          ...(row?.features || {}),
          formation_pattern: escapePatternAnalysis.formation_pattern,
          escape_pattern_applied: 1,
          escape_pattern_confidence: toNum(escapePatternAnalysis.escape_pattern_confidence, 0),
          escape_second_place_bias: baseBiasScore,
          escape_second_place_bias_score: nextBiasScore
        }
      };
    })
    .sort((a, b) => toNum(b?.score, 0) - toNum(a?.score, 0))
    .map((row, idx) => ({
      ...row,
      rank: idx + 1
    }));
}

const ATTACK_SCENARIO_DICTIONARY = {
  two_sashi: {
    label: "2差し警戒",
    lane_bias: { 2: 4.8, 3: 1.1, 1: -1.8 },
    partner_bias: { 1: 1.2, 3: 0.9, 4: 0.4 }
  },
  three_makuri: {
    label: "3捲り本線",
    lane_bias: { 3: 5.2, 4: 1.4, 1: -2.4, 2: -1.3 },
    partner_bias: { 4: 1.1, 5: 0.8, 2: 0.4 }
  },
  three_makuri_sashi: {
    label: "3捲り差し候補",
    lane_bias: { 3: 4.4, 2: 1.5, 4: 0.9, 1: -1.5 },
    partner_bias: { 2: 1.1, 4: 0.9, 5: 0.5 }
  },
  four_cado_makuri: {
    label: "4カド捲り注意",
    lane_bias: { 4: 5.4, 5: 1.4, 1: -2.6, 2: -1.4, 3: -0.8 },
    partner_bias: { 5: 1.1, 3: 0.9, 6: 0.7 }
  },
  four_cado_makuri_sashi: {
    label: "4カド捲り差し候補",
    lane_bias: { 4: 4.6, 3: 1.4, 2: 1.1, 5: 0.8, 1: -1.7 },
    partner_bias: { 3: 1.1, 5: 0.9, 2: 0.8 }
  }
};

function laneRowFromRanking(ranking, lane) {
  return (Array.isArray(ranking) ? ranking : []).find((row) => toInt(row?.racer?.lane, null) === lane) || null;
}

function getExpectedStRank(row) {
  return toNum(row?.features?.expected_actual_st_rank ?? row?.features?.st_rank, 6);
}

function getAttackScenarioReasonTags(type, evidence = {}) {
  const tags = [];
  if (!type) return tags;
  if (toNum(evidence?.boat1_weakness, 0) >= 58) tags.push("BOAT1_WEAKNESS");
  if (toNum(evidence?.wall_weakness, 0) >= 55) tags.push("WALL_WEAK");
  if (toNum(evidence?.slit_support, 0) >= 55) tags.push("SLIT_SUPPORT");
  if (toNum(evidence?.inside_balance, 0) >= 55) tags.push("INSIDE_BALANCE");
  if (toNum(evidence?.partial_collapse, 0) >= 55) tags.push("PARTIAL_COLLAPSE");
  if (toNum(evidence?.cado_advantage, 0) >= 55) tags.push("CADO_ADVANTAGE");
  if (String(type).includes("makuri")) tags.push("ATTACK_SCENARIO");
  if (String(type).includes("sashi")) tags.push("SASHI_SHAPE");
  return [...new Set(tags)];
}

function analyzeAttackScenarioLayer({
  ranking,
  raceIndexes,
  raceFlow,
  wallEvaluation,
  entryMeta,
  escapePatternAnalysis,
  playerStartProfile
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const lane1 = laneRowFromRanking(rows, 1);
  const lane2 = laneRowFromRanking(rows, 2);
  const lane3 = laneRowFromRanking(rows, 3);
  const lane4 = laneRowFromRanking(rows, 4);
  const lane5 = laneRowFromRanking(rows, 5);
  const byLane = playerStartProfile?.by_lane || {};
  const p2 = byLane["2"] || {};
  const p3 = byLane["3"] || {};
  const p4 = byLane["4"] || {};
  const predictedEntryOrder = Array.isArray(entryMeta?.predicted_entry_order) ? entryMeta.predicted_entry_order : [];
  const actualEntryOrder = Array.isArray(entryMeta?.actual_entry_order) ? entryMeta.actual_entry_order : [];
  const lane4Course = actualEntryOrder[3] ?? predictedEntryOrder[3] ?? 4;
  const nigeProbPct = toNum(raceFlow?.nige_prob, 0) * 100;
  const sashiProbPct = toNum(raceFlow?.sashi_prob, 0) * 100;
  const makuriProbPct = toNum(raceFlow?.makuri_prob, 0) * 100;
  const makurizashiProbPct = toNum(raceFlow?.makurizashi_prob, 0) * 100;
  const boat1Weakness = clamp(
    0,
    100,
    100 - nigeProbPct +
      toNum(lane1?.features?.f_hold_caution_penalty, 0) * 6 +
      Math.max(0, getExpectedStRank(lane1) - 3) * 7 +
      Math.max(0, 58 - toNum(lane1?.score, 58)) * 0.5
  );
  const wallStrength = toNum(wallEvaluation?.wall_strength, 50);
  const wallBreakRisk = toNum(wallEvaluation?.wall_break_risk, 50);
  const wallWeakness = clamp(0, 100, wallBreakRisk * 0.9 + Math.max(0, 58 - wallStrength) * 0.7);
  const lane2InsideBalance = clamp(
    0,
    100,
    toNum(p2.sashi_style_score, 50) * 0.45 +
      toNum(p2.start_attack_score, 50) * 0.22 +
      Math.max(0, 7 - getExpectedStRank(lane2)) * 7 +
      Math.max(0, toNum(lane2?.features?.entry_advantage_score, 0)) * 4 +
      toNum(lane2?.features?.slit_alert_flag, 0) * 16 -
      toNum(lane2?.features?.f_hold_caution_penalty, 0) * 4
  );
  const lane3Attack = clamp(
    0,
    100,
    toNum(p3.makuri_style_score, 50) * 0.46 +
      toNum(p3.start_attack_score, 50) * 0.28 +
      Math.max(0, 7 - getExpectedStRank(lane3)) * 7 +
      Math.max(0, toNum(lane3?.features?.display_time_delta_vs_left, 0)) * 90 +
      toNum(lane3?.features?.slit_alert_flag, 0) * 18 -
      toNum(lane3?.features?.f_hold_caution_penalty, 0) * 5
  );
  const lane4Attack = clamp(
    0,
    100,
    toNum(p4.makuri_style_score, 50) * 0.44 +
      toNum(p4.start_attack_score, 50) * 0.3 +
      Math.max(0, 7 - getExpectedStRank(lane4)) * 7 +
      Math.max(0, toNum(lane4?.features?.display_time_delta_vs_left, 0)) * 90 +
      toNum(lane4?.features?.slit_alert_flag, 0) * 18 +
      Math.max(0, toNum(lane4?.features?.kado_bonus, 0)) * 12 -
      toNum(lane4?.features?.f_hold_caution_penalty, 0) * 5
  );
  const slitSupport3 = clamp(
    0,
    100,
    toNum(lane3?.features?.slit_alert_flag, 0) * 45 +
      Math.max(0, toNum(lane3?.features?.display_time_delta_vs_left, 0)) * 110 +
      Math.max(0, toNum(lane3?.features?.avg_st_rank_delta_vs_left, 0)) * 10
  );
  const slitSupport4 = clamp(
    0,
    100,
    toNum(lane4?.features?.slit_alert_flag, 0) * 45 +
      Math.max(0, toNum(lane4?.features?.display_time_delta_vs_left, 0)) * 110 +
      Math.max(0, toNum(lane4?.features?.avg_st_rank_delta_vs_left, 0)) * 10
  );
  const partialCollapse = clamp(
    0,
    100,
    Math.max(0, wallBreakRisk - 42) * 1.2 +
      Math.max(0, 4 - getExpectedStRank(lane2)) * 4 +
      Math.max(0, 4 - getExpectedStRank(lane3)) * 2
  );
  const cadoAdvantage = clamp(
    0,
    100,
    (lane4Course === 4 ? 28 : lane4Course >= 4 ? 20 : 8) +
      Math.max(0, toNum(lane4?.features?.kado_bonus, 0)) * 14 +
      slitSupport4 * 0.34
  );
  const attackVsEscapePenalty = escapePatternAnalysis?.escape_pattern_applied ? 7 : 0;

  const scores = {
    two_sashi: clamp(
      0,
      100,
      lane2InsideBalance * 0.48 +
        boat1Weakness * 0.24 +
        wallStrength * 0.16 +
        sashiProbPct * 0.2 -
        attackVsEscapePenalty
    ),
    three_makuri: clamp(
      0,
      100,
      lane3Attack * 0.42 +
        wallWeakness * 0.22 +
        slitSupport3 * 0.18 +
        makuriProbPct * 0.22 +
        Math.max(0, toNum(raceIndexes?.makuri_index, 0) - 50) * 0.35 -
        attackVsEscapePenalty
    ),
    three_makuri_sashi: clamp(
      0,
      100,
      lane3Attack * 0.34 +
        partialCollapse * 0.24 +
        slitSupport3 * 0.12 +
        makurizashiProbPct * 0.28 +
        Math.max(0, toNum(raceIndexes?.makurizashi_index, 0) - 50) * 0.32 -
        attackVsEscapePenalty
    ),
    four_cado_makuri: clamp(
      0,
      100,
      lane4Attack * 0.38 +
        cadoAdvantage * 0.24 +
        wallWeakness * 0.18 +
        slitSupport4 * 0.18 +
        makuriProbPct * 0.18 -
        attackVsEscapePenalty
    ),
    four_cado_makuri_sashi: clamp(
      0,
      100,
      lane4Attack * 0.3 +
        cadoAdvantage * 0.2 +
        partialCollapse * 0.2 +
        slitSupport4 * 0.12 +
        makurizashiProbPct * 0.24 -
        attackVsEscapePenalty
    )
  };

  const ordered = Object.entries(scores)
    .map(([type, score]) => ({ type, score: Number(score.toFixed(2)) }))
    .sort((a, b) => b.score - a.score);
  const best = ordered[0] || { type: null, score: 0 };
  const second = ordered[1] || { type: null, score: 0 };
  const evidence = {
    boat1_weakness: Number(boat1Weakness.toFixed(2)),
    wall_weakness: Number(wallWeakness.toFixed(2)),
    wall_strength: Number(wallStrength.toFixed(2)),
    inside_balance: Number(lane2InsideBalance.toFixed(2)),
    slit_support: Number(Math.max(slitSupport3, slitSupport4).toFixed(2)),
    partial_collapse: Number(partialCollapse.toFixed(2)),
    cado_advantage: Number(cadoAdvantage.toFixed(2))
  };
  const attackScenarioApplied =
    best.score >= 58 &&
    best.score >= second.score + 4 &&
    !escapePatternAnalysis?.escape_pattern_applied;
  const scenarioType = attackScenarioApplied ? best.type : null;
  return {
    attack_scenario_type: scenarioType,
    attack_scenario_label: scenarioType ? ATTACK_SCENARIO_DICTIONARY[scenarioType]?.label || scenarioType : null,
    attack_scenario_score: Number(best.score.toFixed(2)),
    attack_scenario_reason_tags: getAttackScenarioReasonTags(scenarioType, evidence),
    attack_scenario_applied: attackScenarioApplied ? 1 : 0,
    two_sashi_score: Number(scores.two_sashi.toFixed(2)),
    three_makuri_score: Number(scores.three_makuri.toFixed(2)),
    three_makuri_sashi_score: Number(scores.three_makuri_sashi.toFixed(2)),
    four_cado_makuri_score: Number(scores.four_cado_makuri.toFixed(2)),
    four_cado_makuri_sashi_score: Number(scores.four_cado_makuri_sashi.toFixed(2)),
    scenario_candidates: ordered,
    evidence
  };
}

function applyAttackScenarioBiasToRanking(ranking, attackScenarioAnalysis) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const type = attackScenarioAnalysis?.attack_scenario_type || null;
  const config = type ? ATTACK_SCENARIO_DICTIONARY[type] : null;
  if (!config || !toNum(attackScenarioAnalysis?.attack_scenario_applied, 0)) {
    return rows.map((row, idx) => ({
      ...row,
      rank: idx + 1,
      features: {
        ...(row?.features || {}),
        attack_scenario_type: type,
        attack_scenario_applied: 0,
        attack_scenario_score: toNum(attackScenarioAnalysis?.attack_scenario_score, 0),
        attack_scenario_bias_score: 0
      }
    }));
  }
  const multiplier = clamp(0.35, 1, toNum(attackScenarioAnalysis?.attack_scenario_score, 0) / 72);
  return [...rows]
    .map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      const baseLaneBias = Number.isFinite(Number(config.lane_bias?.[lane])) ? Number(config.lane_bias[lane]) : 0;
      const partnerBias = Number.isFinite(Number(config.partner_bias?.[lane])) ? Number(config.partner_bias[lane]) : 0;
      const appliedBias = Number((baseLaneBias * multiplier + partnerBias * multiplier * 0.55).toFixed(2));
      return {
        ...row,
        score: Number((toNum(row?.score, 0) + appliedBias).toFixed(4)),
        features: {
          ...(row?.features || {}),
          attack_scenario_type: type,
          attack_scenario_applied: 1,
          attack_scenario_score: toNum(attackScenarioAnalysis?.attack_scenario_score, 0),
          attack_scenario_bias_score: appliedBias
        }
      };
    })
    .sort((a, b) => toNum(b?.score, 0) - toNum(a?.score, 0))
    .map((row, idx) => ({
      ...row,
      rank: idx + 1
    }));
}

function getAttackScenarioTargetLanes(type) {
  if (type === "two_sashi") return { head: [2], partner: [1, 3, 4] };
  if (type === "three_makuri") return { head: [3], partner: [4, 5, 2] };
  if (type === "three_makuri_sashi") return { head: [3], partner: [2, 4, 5] };
  if (type === "four_cado_makuri") return { head: [4], partner: [5, 3, 6] };
  if (type === "four_cado_makuri_sashi") return { head: [4], partner: [3, 5, 2] };
  return { head: [], partner: [] };
}

function applyAttackScenarioBiasToTickets(tickets, attackScenarioAnalysis) {
  const rows = Array.isArray(tickets) ? tickets : [];
  if (!toNum(attackScenarioAnalysis?.attack_scenario_applied, 0)) return rows;
  const targets = getAttackScenarioTargetLanes(attackScenarioAnalysis?.attack_scenario_type);
  const scoreFactor = clamp(0.2, 1, toNum(attackScenarioAnalysis?.attack_scenario_score, 0) / 72);
  return [...rows]
    .map((row) => {
      const combo = normalizeCombo(row?.combo);
      const lanes = combo ? combo.split("-").map((n) => toInt(n, null)).filter(Number.isInteger) : [];
      const headBonus = targets.head.includes(lanes[0]) ? 0.02 * scoreFactor : 0;
      const partnerBonus = targets.partner.includes(lanes[1]) ? 0.012 * scoreFactor : 0;
      const thirdBonus = targets.partner.includes(lanes[2]) ? 0.007 * scoreFactor : 0;
      const prob = Number.isFinite(Number(row?.prob)) ? Number(row.prob) : null;
      const totalBonus = Number((headBonus + partnerBonus + thirdBonus).toFixed(4));
      return {
        ...row,
        prob: Number.isFinite(prob) ? Number((prob + totalBonus).toFixed(4)) : prob,
        attack_scenario_bias_score: totalBonus,
        attack_scenario_type: attackScenarioAnalysis?.attack_scenario_type || null
      };
    })
    .sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0));
}

function getAttackScenarioHeadLane(type) {
  if (type === "two_sashi") return 2;
  if (type === "three_makuri" || type === "three_makuri_sashi") return 3;
  if (type === "four_cado_makuri" || type === "four_cado_makuri_sashi") return 4;
  return null;
}

function buildHeadScenarioBalanceAnalysis({
  ranking,
  raceFlow,
  headSelection,
  attackScenarioAnalysis,
  escapePatternAnalysis
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const lane1 = laneRowFromRanking(rows, 1);
  const lane1Score = toNum(lane1?.score, 56);
  const nigeProbPct = toNum(raceFlow?.nige_prob, 0) * 100;
  const boat1Weakness = toNum(attackScenarioAnalysis?.evidence?.boat1_weakness, 0);
  const escapeConfidence = toNum(escapePatternAnalysis?.escape_pattern_confidence, 0);
  const attackType = attackScenarioAnalysis?.attack_scenario_type || null;
  const attackHeadLane = getAttackScenarioHeadLane(attackType);
  const attackScenarioScore = toNum(attackScenarioAnalysis?.attack_scenario_score, 0);
  const secondaryHeads = Array.isArray(headSelection?.secondary_heads) ? headSelection.secondary_heads : [];
  const lane1Support =
    (toInt(headSelection?.main_head, null) === 1 ? 10 : 0) +
    (secondaryHeads.includes(1) ? 6 : 0) +
    Math.max(0, lane1Score - 56) * 0.32;
  const survivalResidualScore = clamp(
    0,
    100,
    nigeProbPct * 0.66 +
      escapeConfidence * 0.24 +
      lane1Support -
      boat1Weakness * 0.2
  );
  const attackDominanceMargin = attackScenarioScore - survivalResidualScore;
  const scenarioCandidates = Array.isArray(attackScenarioAnalysis?.scenario_candidates)
    ? attackScenarioAnalysis.scenario_candidates
    : [];
  const counterAttackType = scenarioCandidates.find((candidate) => candidate?.type && candidate.type !== attackType)?.type || null;
  const survivalGuardApplied =
    !!attackHeadLane &&
    attackHeadLane !== 1 &&
    survivalResidualScore >= 38 &&
    nigeProbPct >= 18 &&
    attackScenarioScore >= 58 &&
    attackDominanceMargin <= 24 &&
    attackScenarioScore < 84;

  const headWeightsRaw = {};
  if (attackHeadLane) {
    headWeightsRaw[String(attackHeadLane)] = Math.max(0.08, attackScenarioScore / 100);
  }
  headWeightsRaw["1"] = Math.max(0.06, survivalResidualScore / 100);
  for (const lane of secondaryHeads.slice(0, 2)) {
    if (!Number.isInteger(lane) || lane === 1 || lane === attackHeadLane) continue;
    headWeightsRaw[String(lane)] = Math.max(headWeightsRaw[String(lane)] || 0, 0.08);
  }
  const totalWeight = Object.values(headWeightsRaw).reduce((sum, value) => sum + toNum(value, 0), 0) || 1;
  const headDistributionJson = Object.entries(headWeightsRaw)
    .map(([lane, weight]) => ({
      lane: Number(lane),
      role:
        Number(lane) === attackHeadLane
          ? "main"
          : Number(lane) === 1
            ? (survivalGuardApplied ? "survival" : "counter")
            : "counter",
      weight: Number((weight / totalWeight).toFixed(4))
    }))
    .sort((a, b) => b.weight - a.weight);

  const removedCandidateReasonTags = [];
  if (attackHeadLane && attackDominanceMargin > 24) removedCandidateReasonTags.push("ATTACK_DOMINANCE_HIGH");
  if (survivalResidualScore < 38) removedCandidateReasonTags.push("ONE_SURVIVAL_RESIDUAL_LOW");
  if (nigeProbPct < 18) removedCandidateReasonTags.push("NIGE_PROB_TOO_LOW");

  return {
    main_scenario_type: attackHeadLane
      ? attackType
      : toInt(headSelection?.main_head, null) === 1
        ? "one_head_survival"
        : null,
    counter_scenario_type: survivalGuardApplied
      ? "one_head_survival"
      : counterAttackType,
    survival_scenario_type: survivalResidualScore >= 32 ? "one_head_survival" : null,
    attack_head_lane: attackHeadLane,
    survival_head_lane: 1,
    survival_residual_score: Number(survivalResidualScore.toFixed(2)),
    attack_dominance_margin: Number(attackDominanceMargin.toFixed(2)),
    head_distribution_json: headDistributionJson,
    survival_guard_applied: survivalGuardApplied ? 1 : 0,
    removed_candidate_reason_tags: removedCandidateReasonTags
  };
}

function applyHeadScenarioBalanceToTickets(tickets, headScenarioBalanceAnalysis) {
  const rows = Array.isArray(tickets) ? tickets : [];
  if (!rows.length) return rows;

  const attackHeadLane = toInt(headScenarioBalanceAnalysis?.attack_head_lane, null);
  const survivalGuardApplied = toNum(headScenarioBalanceAnalysis?.survival_guard_applied, 0) === 1;
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const attackDominanceMargin = toNum(headScenarioBalanceAnalysis?.attack_dominance_margin, 99);
  const adjusted = rows.map((row) => {
    const combo = normalizeCombo(row?.combo);
    const lanes = combo
      ? combo.split("-").map((value) => toInt(value, null)).filter(Number.isInteger)
      : [];
    let bonus = 0;
    const balanceTags = Array.isArray(row?.scenario_balance_tags) ? [...row.scenario_balance_tags] : [];
    if (survivalGuardApplied && lanes[0] === 1) {
      bonus += 0.010 + Math.max(0, survivalResidualScore - 38) * 0.0003;
      balanceTags.push("SURVIVAL_GUARD");
      if (attackHeadLane && lanes.slice(1).includes(attackHeadLane)) {
        bonus += 0.006;
        balanceTags.push("ATTACK_SURVIVAL_BALANCE");
      }
    } else if (attackHeadLane && lanes[0] === attackHeadLane) {
      bonus += attackDominanceMargin >= 14 ? 0.0025 : 0;
    }
    const prob = Number.isFinite(Number(row?.prob)) ? Number(row.prob) : null;
    return {
      ...row,
      prob: Number.isFinite(prob) ? Number((prob + bonus).toFixed(4)) : prob,
      scenario_balance_bonus: Number(bonus.toFixed(4)),
      scenario_balance_tags: [...new Set(balanceTags)]
    };
  });

  adjusted.sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0));

  if (!survivalGuardApplied) return adjusted;

  const previewCount = Math.min(4, adjusted.length);
  const hasHeadOneCoverage = adjusted.slice(0, previewCount).some((row) => normalizeCombo(row?.combo).startsWith("1-"));
  if (hasHeadOneCoverage) return adjusted;

  const bestHeadOneIndex = adjusted.findIndex((row) => normalizeCombo(row?.combo).startsWith("1-"));
  if (bestHeadOneIndex < 0) {
    return adjusted;
  }

  const targetProb = toNum(adjusted[Math.max(0, previewCount - 1)]?.prob, 0);
  adjusted[bestHeadOneIndex] = {
    ...adjusted[bestHeadOneIndex],
    prob: Number((targetProb + 0.0002).toFixed(4)),
    scenario_balance_bonus: Number((toNum(adjusted[bestHeadOneIndex]?.scenario_balance_bonus, 0) + 0.0002).toFixed(4)),
    scenario_balance_tags: [
      ...new Set([...(Array.isArray(adjusted[bestHeadOneIndex]?.scenario_balance_tags) ? adjusted[bestHeadOneIndex].scenario_balance_tags : []), "SURVIVAL_GUARD_PROMOTED"])
    ]
  };

  return adjusted.sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0));
}

function buildBoat1HeadBetsSnapshot({
  recommendedBets,
  optimizedTickets,
  headScenarioBalanceAnalysis,
  escapePatternAnalysis,
  learningWeights,
  race
}) {
  const merged = normalizeSavedBetSnapshotItems([
    ...(Array.isArray(optimizedTickets) ? optimizedTickets : []),
    ...(Array.isArray(recommendedBets) ? recommendedBets : [])
  ]);
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const attackHeadLane = toInt(headScenarioBalanceAnalysis?.attack_head_lane, null);
  const shown = survivalResidualScore >= 32;
  const learnedPatternAdj = getSegmentCorrectionValue(
    learningWeights,
    "formation_pattern",
    escapePatternAnalysis?.formation_pattern || null,
    "second_place_bias_correction"
  );
  const learnedVenueAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "second_place_bias_correction"
  );

  const bucket = new Map();
  for (const row of merged) {
    const combo = normalizeCombo(row?.combo);
    if (!combo.startsWith("1-")) continue;
    const lanes = combo.split("-").map((value) => toInt(value, null)).filter(Number.isInteger);
    const secondLane = lanes[1];
    const escapeBias = escapePatternAnalysis?.escape_pattern_applied
      ? getEscapeSecondPlaceBiasScore(escapePatternAnalysis?.escape_second_place_bias_json || {}, secondLane) * 0.0024
      : 0;
    const balanceBonus = toNum(row?.scenario_balance_bonus, 0);
    const attackPartnerBonus = attackHeadLane && lanes.includes(attackHeadLane) ? 0.005 : 0;
    const learnedBonus = (learnedPatternAdj + learnedVenueAdj) * 0.0008;
    const prob = toNum(row?.prob, 0);
    const compositeScore = prob + balanceBonus + escapeBias + attackPartnerBonus + learnedBonus;
    const reasonTags = [
      "BOAT1_HEAD",
      survivalResidualScore >= 38 ? "SURVIVAL_RESIDUAL_ACTIVE" : null,
      escapePatternAnalysis?.escape_pattern_applied ? "ESCAPE_PATTERN_CONTEXT" : null,
      attackPartnerBonus > 0 ? "ATTACK_COUNTER_PARTNER" : null
    ].filter(Boolean);
    const nextRow = {
      ...row,
      combo,
      boat1_head_score: Number((compositeScore * 100).toFixed(2)),
      boat1_head_reason_tags: [...new Set(reasonTags)]
    };
    const existing = bucket.get(combo);
    if (!existing || toNum(existing?.boat1_head_score, 0) < nextRow.boat1_head_score) {
      bucket.set(combo, nextRow);
    }
  }

  const items = [...bucket.values()]
    .sort((a, b) => toNum(b?.boat1_head_score, 0) - toNum(a?.boat1_head_score, 0))
    .slice(0, 5)
    .map((row) => ({
      ...row,
      boat1_head_score: Number(toNum(row?.boat1_head_score, 0).toFixed(2))
    }));

  return {
    shown: shown && items.length > 0,
    boat1_head_score: items.length > 0 ? Number(toNum(items[0]?.boat1_head_score, survivalResidualScore).toFixed(2)) : Number(survivalResidualScore.toFixed(2)),
    boat1_survival_residual_score: Number(survivalResidualScore.toFixed(2)),
    boat1_head_reason_tags: [...new Set(items.flatMap((row) => safeArray(row?.boat1_head_reason_tags)).slice(0, 6))],
    items
  };
}

const CONFIDENCE_VERSION = "v1.1";
const PARTICIPATION_VERSION = "v1.2";
const PARTICIPATION_CONFIDENCE_THRESHOLDS = {
  participate: {
    headFixedMin: 67,
    betMin: 59
  },
  watch: {
    headMin: 53,
    betMin: 48
  }
};

const PARTICIPATION_TUNING = {
  skip: {
    chaosRiskHardMin: 93,
    contradictionHardMin: 3,
    lowHeadHardMax: 51,
    lowBetHardMax: 46,
    skipModeConfidenceHardMax: 48
  },
  caution: {
    headStrongCautionMin: 58,
    betStrongCautionMin: 52
  },
  positiveBoost: {
    formationClarityMin: 58,
    formationClarityBaseBoost: 5,
    formationConfidenceBoostScale: 0.15,
    escapePatternFocusBoost: 3,
    slitAlertPerLaneBoost: 3.5,
    attackScenarioExtraBoost: 2.5
  },
  cautionPenalty: {
    fHoldWeight: 0.15,
    contradictionPenalty: 4.5
  }
};

function buildParticipationDecision({
  raceDecision,
  raceRisk,
  raceStructure,
  entryMeta,
  confidenceScores,
  scenarioSuggestions,
  raceFlow,
  escapePatternAnalysis,
  attackScenarioAnalysis
}) {
  const mode = normalizeModeValue(raceDecision?.mode || raceRisk?.recommendation || "UNKNOWN");
  const confidence = toNum(raceDecision?.confidence, 0);
  const headStability = toNum(raceStructure?.head_stability_score, 50);
  const chaosRisk = toNum(raceStructure?.chaos_risk_score, 50);
  const entrySeverity = String(entryMeta?.severity || "none");
  const headFixed = toNum(confidenceScores?.head_fixed_confidence_pct, 50);
  const betConf = toNum(confidenceScores?.recommended_bet_confidence_pct, 50);
  const cautionFlags = Array.isArray(confidenceScores?.confidence_reason_tags)
    ? confidenceScores.confidence_reason_tags
    : [];
  const formationPatternClarity = Math.max(
    toNum(raceDecision?.factors?.formation_pattern_clarity_score, 0),
    toNum(raceStructure?.formation_pattern_clarity_score, 0),
    toNum(scenarioSuggestions?.scenario_confidence, 0)
  );
  const slitAlertLanes = Array.isArray(raceFlow?.slit_alert_lanes) ? raceFlow.slit_alert_lanes : [];
  const slitAlertCount = slitAlertLanes.length;
  const fHoldCautionScore = toNum(confidenceScores?.f_hold_caution_score, 0);
  const segmentParticipationCorrection = toNum(confidenceScores?.segment_participation_correction, 0);
  const escapePatternApplied = !!escapePatternAnalysis?.escape_pattern_applied;
  const flowMode = String(raceFlow?.race_flow_mode || "").toLowerCase();
  const isAttackScenario = flowMode === "sashi" || flowMode === "makuri" || flowMode === "makurizashi";
  const attackScenarioScore = toNum(attackScenarioAnalysis?.attack_scenario_score, 0);
  const attackScenarioApplied = toNum(attackScenarioAnalysis?.attack_scenario_applied, 0) === 1;
  const contradictionCount =
    (mode === "SKIP" ? 1 : 0) +
    (chaosRisk >= 80 ? 1 : 0) +
    (escapePatternApplied && flowMode === "chaos" ? 1 : 0) +
    (escapePatternApplied && slitAlertCount >= 2 && flowMode === "nige" ? 1 : 0) +
    (attackScenarioApplied && flowMode === "nige" && attackScenarioScore >= 64 ? 1 : 0);
  const formationBoost =
    formationPatternClarity >= PARTICIPATION_TUNING.positiveBoost.formationClarityMin
      ? PARTICIPATION_TUNING.positiveBoost.formationClarityBaseBoost +
        Math.max(0, formationPatternClarity - PARTICIPATION_TUNING.positiveBoost.formationClarityMin) *
          PARTICIPATION_TUNING.positiveBoost.formationConfidenceBoostScale
      : 0;
  const escapeFocusBoost =
    escapePatternApplied && formationPatternClarity >= 60 && chaosRisk < 74
      ? PARTICIPATION_TUNING.positiveBoost.escapePatternFocusBoost
      : 0;
  const slitBoost =
    slitAlertCount > 0
      ? slitAlertCount * PARTICIPATION_TUNING.positiveBoost.slitAlertPerLaneBoost +
        (isAttackScenario ? PARTICIPATION_TUNING.positiveBoost.attackScenarioExtraBoost : 0)
      : 0;
  const attackScenarioBoost =
    attackScenarioApplied
      ? clamp(0, 7, (attackScenarioScore - 54) * 0.18 + (isAttackScenario ? 1.6 : 0))
      : 0;
  const fHoldPenalty = Math.min(11, fHoldCautionScore * PARTICIPATION_TUNING.cautionPenalty.fHoldWeight);
  const contradictionPenalty = contradictionCount * PARTICIPATION_TUNING.cautionPenalty.contradictionPenalty;
  const adjustedHeadFixed = Math.min(
    100,
    Math.max(
      0,
      headFixed +
        formationBoost * 0.45 +
        escapeFocusBoost +
        slitBoost * 0.35 +
        attackScenarioBoost * 0.45 +
        segmentParticipationCorrection * 0.6 -
        fHoldPenalty * 0.65 -
        contradictionPenalty * 0.55
    )
  );
  const adjustedBetConf = Math.min(
    100,
    Math.max(
      0,
      betConf +
        formationBoost * 0.45 +
        escapeFocusBoost * 0.9 +
        slitBoost * 0.55 +
        attackScenarioBoost * 0.7 +
        segmentParticipationCorrection -
        fHoldPenalty * 0.8 -
        contradictionPenalty * 0.75
    )
  );
  const hasStrongCaution =
    cautionFlags.includes("ENTRY_CHANGE_PENALTY") ||
    cautionFlags.includes("ST_CHAOS") ||
    cautionFlags.includes("INSUFFICIENT_EXHIBITION_DATA");

  const reasonTags = [];
  if (mode === "SKIP") reasonTags.push("DECISION_SKIP");
  if (headFixed < 60) reasonTags.push("HEAD_CONFIDENCE_LOW");
  if (entrySeverity === "high") reasonTags.push("ENTRY_CHANGE_PENALTY");
  if (chaosRisk >= 70) reasonTags.push("ST_CHAOS");
  if (cautionFlags.includes("INSUFFICIENT_EXHIBITION_DATA")) reasonTags.push("INSUFFICIENT_EXHIBITION_DATA");
  if (cautionFlags.includes("WEAK_MOTOR_EXHIBITION_OVERLAP")) reasonTags.push("WEAK_MOTOR_EXHIBITION_OVERLAP");
  if (cautionFlags.includes("LEARNED_CAUTION_PENALTY")) reasonTags.push("LEARNED_CAUTION_PENALTY");
  if (cautionFlags.includes("START_SIGNAL_UNSTABLE")) reasonTags.push("START_SIGNAL_UNSTABLE");
  if (formationBoost > 0) reasonTags.push("FORMATION_PATTERN_CLEAR");
  if (escapePatternApplied) reasonTags.push("ESCAPE_PATTERN_APPLIED");
  if (escapeFocusBoost > 0) reasonTags.push("ESCAPE_PATTERN_FOCUSED");
  if (slitBoost > 0) reasonTags.push("SLIT_ALERT_POSITIVE");
  if (attackScenarioApplied) reasonTags.push("ATTACK_SCENARIO_POSITIVE");
  if (fHoldPenalty > 0) reasonTags.push("F_HOLD_CAUTION");
  if (contradictionCount > 0) reasonTags.push("SIGNAL_CONTRADICTION");
  if (headStability >= 62) reasonTags.push("HEAD_STABILITY_GOOD");
  if (adjustedHeadFixed >= 72) reasonTags.push("HEAD_CONFIDENCE_GOOD");
  if (adjustedBetConf >= 64) reasonTags.push("BET_CONFIDENCE_GOOD");

  let decision = "watch";
  if (
    mode !== "SKIP" &&
    adjustedHeadFixed >= PARTICIPATION_CONFIDENCE_THRESHOLDS.participate.headFixedMin &&
    adjustedBetConf >= PARTICIPATION_CONFIDENCE_THRESHOLDS.participate.betMin &&
    (
      !hasStrongCaution ||
      (
        adjustedHeadFixed >= PARTICIPATION_TUNING.caution.headStrongCautionMin &&
        adjustedBetConf >= PARTICIPATION_TUNING.caution.betStrongCautionMin
      )
    )
  ) {
    decision = "recommended";
  } else if (
    adjustedHeadFixed <= PARTICIPATION_TUNING.skip.lowHeadHardMax ||
    adjustedBetConf <= PARTICIPATION_TUNING.skip.lowBetHardMax ||
    (mode === "SKIP" && hasStrongCaution && confidence < PARTICIPATION_TUNING.skip.skipModeConfidenceHardMax) ||
    chaosRisk >= PARTICIPATION_TUNING.skip.chaosRiskHardMin ||
    contradictionCount >= PARTICIPATION_TUNING.skip.contradictionHardMin
  ) {
    decision = "not_recommended";
  } else if (
    mode !== "SKIP" &&
    adjustedHeadFixed >= PARTICIPATION_CONFIDENCE_THRESHOLDS.watch.headMin &&
    adjustedBetConf >= PARTICIPATION_CONFIDENCE_THRESHOLDS.watch.betMin
  ) {
    decision = "watch";
  }

  const summary =
    decision === "recommended"
      ? "参加推奨"
      : decision === "watch"
        ? "様子見（境界）"
        : "見送り";

  return {
    decision,
    is_recommended: decision !== "not_recommended",
    summary,
    reason_tags: [...new Set(reasonTags)],
    metrics: {
      head_fixed_confidence_pct: Number(headFixed.toFixed(2)),
      recommended_bet_confidence_pct: Number(betConf.toFixed(2)),
      adjusted_head_fixed_confidence_pct: Number(adjustedHeadFixed.toFixed(2)),
      adjusted_recommended_bet_confidence_pct: Number(adjustedBetConf.toFixed(2)),
      formation_pattern_clarity_score: Number(formationPatternClarity.toFixed(2)),
      escape_pattern_focus_boost: Number(escapeFocusBoost.toFixed(2)),
      slit_alert_count: slitAlertCount,
      attack_scenario_score: Number(attackScenarioScore.toFixed(2)),
      attack_scenario_boost: Number(attackScenarioBoost.toFixed(2)),
      f_hold_caution_score: Number(fHoldCautionScore.toFixed(2)),
      contradiction_count: contradictionCount,
      segment_participation_correction: Number(segmentParticipationCorrection.toFixed(2)),
      formation_boost: Number(formationBoost.toFixed(2)),
      slit_boost: Number(slitBoost.toFixed(2)),
      segment_participation_correction: Number(segmentParticipationCorrection.toFixed(2)),
      f_hold_penalty: Number(fHoldPenalty.toFixed(2)),
      contradiction_penalty: Number(contradictionPenalty.toFixed(2)),
      confidence_version: CONFIDENCE_VERSION,
      participation_version: PARTICIPATION_VERSION
    },
    participation_score_components: {
      base_head_fixed_confidence_pct: Number(headFixed.toFixed(2)),
      base_recommended_bet_confidence_pct: Number(betConf.toFixed(2)),
      formation_boost: Number(formationBoost.toFixed(2)),
      escape_pattern_focus_boost: Number(escapeFocusBoost.toFixed(2)),
      slit_boost: Number(slitBoost.toFixed(2)),
      attack_scenario_boost: Number(attackScenarioBoost.toFixed(2)),
      f_hold_penalty: Number(fHoldPenalty.toFixed(2)),
      contradiction_penalty: Number(contradictionPenalty.toFixed(2)),
      contradiction_count: contradictionCount
    },
    confidence_version: CONFIDENCE_VERSION,
    participation_version: PARTICIPATION_VERSION
  };
}

function buildConfidenceScores({
  raceDecision,
  headConfidence,
  ticketOptimization,
  raceStructure,
  startSignals,
  entryMeta,
  learningWeights,
  ranking,
  preRaceAnalysis,
  contenderSignals,
  scenarioSuggestions,
  escapePatternAnalysis,
  race
}) {
  const clampPct = (v) => Number(clamp(0, 100, toNum(v, 0)).toFixed(2));
  const headBase = clampPct(toNum(headConfidence?.head_confidence, 0.5) * 100);
  const headStability = clampPct(raceStructure?.head_stability_score);
  const chaosRisk = clampPct(raceStructure?.chaos_risk_score);
  const startStability = clampPct(startSignals?.stability_score);
  const raceConf = clampPct(raceDecision?.confidence);
  const ticketConf = clampPct(ticketOptimization?.ticket_confidence_score);
  const top3Concentration = clampPct(raceStructure?.top3_concentration_score);
  const scenarioConfidence = clampPct(toNum(scenarioSuggestions?.scenario_confidence, 0.5) * 100);
  const contenderOverlap = clampPct(toNum((contenderSignals?.overlap_lanes || []).length, 0) * 20);
  const formScore = clampPct(preRaceAnalysis?.pre_race_form_score);
  const topRows = (Array.isArray(ranking) ? ranking : []).slice(0, 3);
  const playerSignal = clampPct(
    topRows.length
      ? topRows.reduce((acc, row) => acc + toNum(row?.score, 0), 0) / topRows.length / 4
      : 50
  );
  const motorSignal = clampPct(
    topRows.length
      ? topRows.reduce((acc, row) => acc + toNum(row?.features?.motor_total_score, 0), 0) / topRows.length * 5
      : 50
  );
  const exhibitionSignal = clampPct(
    topRows.length
      ? 100 -
          (topRows.reduce((acc, row) => acc + toNum(row?.features?.exhibition_rank, 6), 0) / topRows.length - 1) * 20
      : 50
  );
  const entrySeverity = String(entryMeta?.severity || "none");
  const entryPenalty = entrySeverity === "high" ? 10 : entrySeverity === "medium" ? 6 : entrySeverity === "low" ? 3 : 0;
  const learnedCaution = Math.max(0, toNum(learningWeights?.recommendation_threshold, 52) - 54) * 0.7;
  const missingDataPenalty = (() => {
    const missPlayer = playerSignal <= 0 ? 1 : 0;
    const missMotor = motorSignal <= 0 ? 1 : 0;
    const missEx = exhibitionSignal <= 0 ? 1 : 0;
    return Math.min(12, (missPlayer + missMotor + missEx) * 4);
  })();
  const topFHoldPenalty = topRows.reduce((acc, row) => acc + toNum(row?.features?.f_hold_caution_penalty, 0), 0) / Math.max(1, topRows.length);
  const mainHeadFHoldPenalty = toNum(topRows[0]?.features?.f_hold_caution_penalty, 0);
  const fHoldCautionScore = clampPct(mainHeadFHoldPenalty * 7 + topFHoldPenalty * 4);
  const formationPatternClarity = clampPct(toNum(escapePatternAnalysis?.formation_pattern_clarity_score, 0));
  const overlapBucket = overlapBucketLabel(contenderSignals?.overlap_lanes);
  const predictedEntryPattern = Array.isArray(entryMeta?.predicted_entry_order) ? entryMeta.predicted_entry_order.join("-") : null;
  const actualEntryPattern = Array.isArray(entryMeta?.actual_entry_order) ? entryMeta.actual_entry_order.join("-") : null;
  const participationStateSeed = normalizeParticipationState(raceDecision?.mode);
  const calibrationThresholds = learningWeights?.confidence_calibration || {};
  const segmentHeadCorrection =
    getSegmentCorrectionValue(learningWeights, "venue", toInt(race?.venueId, null), "head_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "formation_pattern", escapePatternAnalysis?.formation_pattern || null, "head_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "scenario_type", scenarioSuggestions?.scenario_type || null, "head_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "predicted_entry_pattern", predictedEntryPattern, "head_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "actual_entry_pattern", actualEntryPattern, "head_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "head_confidence_band", confidenceBandLabel(headBase, calibrationThresholds), "head_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "participation_decision_state", participationStateSeed, "head_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "motor_exhibition_overlap_bucket", overlapBucket, "head_confidence_correction");
  const segmentBetCorrection =
    getSegmentCorrectionValue(learningWeights, "venue", toInt(race?.venueId, null), "bet_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "formation_pattern", escapePatternAnalysis?.formation_pattern || null, "bet_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "scenario_type", scenarioSuggestions?.scenario_type || null, "bet_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "scenario_match_bucket", scenarioMatchBucketLabel(scenarioSuggestions?.scenario_confidence), "bet_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "bet_confidence_band", confidenceBandLabel(ticketConf, calibrationThresholds), "bet_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "participation_decision_state", participationStateSeed, "bet_confidence_correction") +
    getSegmentCorrectionValue(learningWeights, "motor_exhibition_overlap_bucket", overlapBucket, "bet_confidence_correction");
  const segmentCautionAdj =
    getSegmentCorrectionValue(learningWeights, "entry_change_present", entryMeta?.entry_changed ? "changed" : "unchanged", "caution_penalty_correction") +
    getSegmentCorrectionValue(learningWeights, "entry_type", entryMeta?.entry_change_type || null, "caution_penalty_correction") +
    getSegmentCorrectionValue(learningWeights, "has_f_hold", rowsHaveFHold(ranking) ? "yes" : "no", "f_hold_penalty_adjustment") +
    getSegmentCorrectionValue(learningWeights, "f_hold_zone", fHoldZoneLabel(ranking), "f_hold_penalty_adjustment") +
    getSegmentCorrectionValue(learningWeights, "participation_decision_state", participationStateSeed, "caution_penalty_correction");
  const segmentParticipationCorrection =
    getSegmentCorrectionValue(learningWeights, "venue", toInt(race?.venueId, null), "participate_watch_skip_correction") +
    getSegmentCorrectionValue(learningWeights, "formation_pattern", escapePatternAnalysis?.formation_pattern || null, "participate_watch_skip_correction") +
    getSegmentCorrectionValue(learningWeights, "scenario_type", scenarioSuggestions?.scenario_type || null, "participate_watch_skip_correction") +
    getSegmentCorrectionValue(learningWeights, "entry_change_present", entryMeta?.entry_changed ? "changed" : "unchanged", "participate_watch_skip_correction") +
    getSegmentCorrectionValue(learningWeights, "has_f_hold", rowsHaveFHold(ranking) ? "yes" : "no", "participate_watch_skip_correction") +
    getSegmentCorrectionValue(learningWeights, "participation_decision_state", participationStateSeed, "participate_watch_skip_correction");

  const headFixedRaw = clampPct(
    headBase * 0.38 +
      headStability * 0.16 +
      startStability * 0.1 +
      top3Concentration * 0.08 +
      playerSignal * 0.1 +
      motorSignal * 0.08 +
      exhibitionSignal * 0.06 +
      formScore * 0.04 -
      entryPenalty -
      Math.max(0, fHoldCautionScore + segmentCautionAdj) * 0.18 -
      learnedCaution -
      missingDataPenalty
  );
  const headFixed = clampPct(headFixedRaw + segmentHeadCorrection);
  const betConfidenceRaw = clampPct(
    ticketConf * 0.34 +
      raceConf * 0.16 +
      (100 - chaosRisk) * 0.14 +
      headFixedRaw * 0.14 +
      scenarioConfidence * 0.08 +
      contenderOverlap * 0.08 +
      top3Concentration * 0.06 -
      Math.max(0, fHoldCautionScore + segmentCautionAdj) * 0.12 +
      learnedCaution * 0.6 -
      Math.max(0, missingDataPenalty - 4)
  );
  const betConfidence = clampPct(betConfidenceRaw + segmentBetCorrection);
  const raceConfidence = clampPct((raceConf * 0.6 + headFixed * 0.2 + betConfidence * 0.2));
  const headBandRaw = confidenceBandLabel(headFixedRaw, calibrationThresholds);
  const betBandRaw = confidenceBandLabel(betConfidenceRaw, calibrationThresholds);
  const headBandCalibrated = confidenceBandLabel(headFixed, calibrationThresholds);
  const betBandCalibrated = confidenceBandLabel(betConfidence, calibrationThresholds);
  const calibrationSegmentsUsed = [
    segmentHeadCorrection !== 0 ? "head" : null,
    segmentBetCorrection !== 0 ? "bet" : null,
    segmentCautionAdj !== 0 ? "caution" : null
  ].filter(Boolean);

  const toBand = (pct) => (pct >= 75 ? "high" : pct >= 55 ? "medium" : "caution");
  const confidenceReasonTags = [];
  if (headFixed < 60) confidenceReasonTags.push("HEAD_CONFIDENCE_LOW");
  if (betConfidence < 55) confidenceReasonTags.push("BET_CONFIDENCE_LOW");
  if (entrySeverity !== "none") confidenceReasonTags.push("ENTRY_CHANGE_PENALTY");
  if (chaosRisk >= 70) confidenceReasonTags.push("ST_CHAOS");
  if (missingDataPenalty >= 8) confidenceReasonTags.push("INSUFFICIENT_EXHIBITION_DATA");
  if (contenderOverlap < 40) confidenceReasonTags.push("WEAK_MOTOR_EXHIBITION_OVERLAP");
  if (learnedCaution >= 3.5) confidenceReasonTags.push("LEARNED_CAUTION_PENALTY");
  if (startStability < 44) confidenceReasonTags.push("START_SIGNAL_UNSTABLE");
  if (fHoldCautionScore >= 8) confidenceReasonTags.push("F_HOLD_CAUTION");
  if (formationPatternClarity >= 62) confidenceReasonTags.push("FORMATION_PATTERN_CLEAR");
  return {
    head_fixed_confidence_pct: headFixed,
    recommended_bet_confidence_pct: betConfidence,
    race_confidence_pct: raceConfidence,
    head_fixed_band: toBand(headFixed),
    recommended_bet_band: toBand(betConfidence),
    race_band: toBand(raceConfidence),
    head_confidence: headFixed,
    bet_confidence: betConfidence,
    head_confidence_raw: headFixedRaw,
    head_confidence_calibrated: headFixed,
    bet_confidence_raw: betConfidenceRaw,
    bet_confidence_calibrated: betConfidence,
    head_confidence_bucket: headBandCalibrated,
    bet_confidence_bucket: betBandCalibrated,
    head_confidence_bucket_raw: headBandRaw,
    bet_confidence_bucket_raw: betBandRaw,
    confidence_bucket: betBandCalibrated,
    confidence_calibration_applied: calibrationSegmentsUsed.length > 0 ? 1 : 0,
    confidence_calibration_segments: calibrationSegmentsUsed,
    confidence_calibration_source: calibrationSegmentsUsed.length > 0 ? "segmented_learning" : "raw_default",
    confidence_calibration_thresholds: {
      high_min: toNum(calibrationThresholds?.high_min, 80),
      medium_min: toNum(calibrationThresholds?.medium_min, 60)
    },
    participation_state_seed: participationStateSeed,
    f_hold_caution_score: Number(fHoldCautionScore.toFixed(2)),
    formation_pattern_clarity_score: Number(formationPatternClarity.toFixed(2)),
    segment_head_confidence_correction: Number(segmentHeadCorrection.toFixed(2)),
    segment_bet_confidence_correction: Number(segmentBetCorrection.toFixed(2)),
    segment_participation_correction: Number(segmentParticipationCorrection.toFixed(2)),
    segment_caution_adjustment: Number(segmentCautionAdj.toFixed(2)),
    confidence_reason_tags: [...new Set(confidenceReasonTags)],
    confidence_version: CONFIDENCE_VERSION
  };
}

function classifyWeaknessCodes({
  predictedHead,
  actualHead,
  hasHit,
  riskScore,
  placedCount,
  top3Concentration,
  avgBoughtOdds
}) {
  const codes = [];
  if (predictedHead && actualHead && predictedHead !== actualHead) codes.push("HEAD_MISS");
  if (predictedHead && actualHead && predictedHead === actualHead && !hasHit) codes.push("PARTNER_MISS");
  if (!hasHit && riskScore <= 55) codes.push("CHAOS_UNDERESTIMATED");
  if (!hasHit && placedCount > 0 && avgBoughtOdds > 0 && avgBoughtOdds <= 12) codes.push("ODDS_TRAP");
  if (placedCount >= 8) codes.push("OVERSPREAD");
  if (!hasHit && placedCount <= 2 && top3Concentration < 0.52) codes.push("UNDERSPREAD");
  return [...new Set(codes)];
}

function toInt(value, fallback = null) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

function normalizeTicketsForRecommendation({
  ticketGenerationV2,
  betPlan,
  ticketOptimization,
  limit = 4
}) {
  const pick = [];
  const primary = Array.isArray(ticketGenerationV2?.primary_tickets)
    ? ticketGenerationV2.primary_tickets
    : [];
  const secondary = Array.isArray(ticketGenerationV2?.secondary_tickets)
    ? ticketGenerationV2.secondary_tickets
    : [];
  const optimized = Array.isArray(ticketOptimization?.optimized_tickets)
    ? ticketOptimization.optimized_tickets
    : [];
  const planRows = Array.isArray(betPlan?.recommended_bets) ? betPlan.recommended_bets : [];

  for (const combo of [...primary, ...secondary]) {
    if (!combo || pick.some((p) => p.combo === combo)) continue;
    const opt = optimized.find((r) => String(r?.combo) === String(combo));
    const plan = planRows.find((r) => String(r?.combo) === String(combo));
    pick.push({
      combo: String(combo),
      prob: Number.isFinite(Number(opt?.probability ?? plan?.prob))
        ? Number(Number(opt?.probability ?? plan?.prob).toFixed(4))
        : null,
      odds: Number.isFinite(Number(opt?.odds ?? plan?.odds))
        ? Number(Number(opt?.odds ?? plan?.odds).toFixed(1))
        : null,
      ev: Number.isFinite(Number(opt?.ev ?? plan?.ev)) ? Number(Number(opt?.ev ?? plan?.ev).toFixed(4)) : null,
      bet: Number.isFinite(Number(opt?.recommended_bet ?? plan?.bet))
        ? Number(opt?.recommended_bet ?? plan?.bet)
        : null
    });
    if (pick.length >= limit) break;
  }

  if (pick.length < limit) {
    for (const row of planRows) {
      const combo = String(row?.combo || "");
      if (!combo || pick.some((p) => p.combo === combo)) continue;
      pick.push({
        combo,
        prob: Number.isFinite(Number(row?.prob)) ? Number(Number(row.prob).toFixed(4)) : null,
        odds: Number.isFinite(Number(row?.odds)) ? Number(Number(row.odds).toFixed(1)) : null,
        ev: Number.isFinite(Number(row?.ev)) ? Number(Number(row.ev).toFixed(4)) : null,
        bet: Number.isFinite(Number(row?.bet)) ? Number(row.bet) : null
      });
      if (pick.length >= limit) break;
    }
  }

  return pick;
}

function enrichRecommendedTickets({ recommendedBets, probabilities, oddsData, evAnalysis }) {
  const probMap = new Map();
  (Array.isArray(probabilities) ? probabilities : []).forEach((p) => {
    const prob = Number(p?.p ?? p?.prob);
    if (p?.combo && Number.isFinite(prob)) probMap.set(String(p.combo), prob);
  });

  const oddsMap = new Map();
  (Array.isArray(oddsData?.trifecta) ? oddsData.trifecta : []).forEach((o) => {
    const odds = Number(o?.odds);
    if (o?.combo && Number.isFinite(odds)) oddsMap.set(String(o.combo), odds);
  });

  const evMap = new Map();
  (Array.isArray(evAnalysis?.best_ev_bets) ? evAnalysis.best_ev_bets : []).forEach((e) => {
    const ev = Number(e?.ev);
    if (e?.combo && Number.isFinite(ev)) evMap.set(String(e.combo), ev);
  });

  return (Array.isArray(recommendedBets) ? recommendedBets : []).map((row) => {
    const combo = String(row?.combo || "");
    const prob = probMap.get(combo);
    const odds = oddsMap.get(combo);
    const ev = evMap.has(combo)
      ? evMap.get(combo)
      : Number.isFinite(prob) && Number.isFinite(odds)
        ? prob * odds
        : null;

    return {
      ...row,
      prob: Number.isFinite(prob) ? Number(prob.toFixed(4)) : null,
      odds: Number.isFinite(odds) ? Number(odds.toFixed(1)) : null,
      ev: Number.isFinite(ev) ? Number(ev.toFixed(4)) : null
    };
  });
}

function calcHitRateRankingScore({
  raceDecision,
  raceStructure,
  headPrecision,
  valueDetection,
  marketTrap,
  ticketOptimization,
  startSignals,
  recommendationScore,
  learningWeights
}) {
  const lw = learningWeights || {};
  const mode = String(raceDecision?.mode || "SMALL_BET").toUpperCase();
  const modeBase = mode === "FULL_BET" ? 18 : mode === "SMALL_BET" ? 11 : mode === "MICRO_BET" ? 7 : 0;
  const confidence = toNum(raceDecision?.confidence, 0);
  const headStability = toNum(raceStructure?.head_stability_score, 50);
  const headGap = toNum(headPrecision?.head_gap_score, 50);
  const chaosRisk = toNum(raceStructure?.chaos_risk_score, 50);
  const valueBalance = toNum(valueDetection?.value_balance_score, 50);
  const trapScore = toNum(marketTrap?.trap_score, 35);
  const ticketQuality = toNum(ticketOptimization?.ticket_confidence_score, 50);
  const startStability = toNum(startSignals?.stability_score, 50);
  const recScore = toNum(recommendationScore, 50);
  const rankingWeight = clamp(0.88, 1.18, toNum(lw?.ranking_weight, 1));
  const startWeight = clamp(0.8, 1.2, toNum(lw?.start_signal_weight, 1));
  const confidenceSeparationBoost = Math.max(0, confidence - 65) * 0.12;
  const confidenceLowPenalty = Math.max(0, 55 - confidence) * 0.16;

  const score = clamp(
    0,
    100,
    modeBase * rankingWeight +
      recScore * 0.16 +
      confidence * 0.22 +
      confidenceSeparationBoost -
      confidenceLowPenalty +
      headStability * 0.2 +
      headGap * 0.14 +
      startStability * 0.16 * startWeight +
      (100 - chaosRisk) * 0.16 +
      valueBalance * 0.12 +
      ticketQuality * 0.12 -
      trapScore * 0.2
  );
  return Number(score.toFixed(2));
}

raceRouter.get("/race", async (req, res, next) => {
  try {
    const { date, venueId, raceNo, participationMode } = req.query;
    const forceRefresh = parseBooleanFlag(req.query?.forceRefresh, false);

    if (!date || !venueId || !raceNo) {
      return res.status(400).json({
        error: "bad_request",
        message: "date, venueId, and raceNo are required query params"
      });
    }

    let data;
    try {
      data = await getRaceData({ date, venueId, raceNo, forceRefresh });
    } catch (fetchErr) {
      const fallback = loadRaceSnapshotFromDb({ date, venueId, raceNo });
      if (!fallback) throw fetchErr;
      data = fallback;
    }
    const learningWeights = getActiveLearningWeights();
    const learningState = getLatestLearningRun();
    const raceId = saveRace(data);
    const manualLapEvaluation = getManualLapEvaluation(raceId);
    const entryMeta = buildEntryOrderMeta(data.racers);
    const baseFeatures = applyMotorPerformanceFeatures(
      applyCoursePerformanceFeatures(buildRaceFeatures(data.racers, data.race))
    );
    const venueAdjustedBase = applyVenueAdjustments(baseFeatures, data.race);
    const preRanking = rankRace(venueAdjustedBase.racersWithFeatures);
    const prePattern = analyzeRacePattern(preRanking);
    const trendFeatures = applyMotorTrendFeatures(venueAdjustedBase.racersWithFeatures, {
      racePattern: prePattern.race_pattern
    });

    const entryAdjusted = applyEntryDynamicsFeatures(trendFeatures, {
      racePattern: prePattern.race_pattern,
      chaos_index: prePattern.indexes.chaos_index
    });

    const rankingBase = rankRace(entryAdjusted.racersWithFeatures);
    const contenderAdjusted = applyContenderSynergy(rankingBase);
    const rankingBeforePatternBias = contenderAdjusted.ranking;
    const patternBeforeBias = analyzeRacePattern(rankingBeforePatternBias);
    const escapePatternAnalysis = analyzeEscapeFormationLayer({
      ranking: rankingBeforePatternBias,
      racePattern: patternBeforeBias.race_pattern,
      indexes: patternBeforeBias.indexes
    });
    let ranking = applyEscapeFormationBiasToRanking(rankingBeforePatternBias, escapePatternAnalysis, learningWeights, data?.race || null);
    const manualLapImpact = {
      enabled: false,
      applied_lane_count: 0,
      average_adjustment: 0,
      note: "manual_lap_disabled"
    };
    const preRaceAnalysis = analyzePreRaceForm({
      ranking,
      race: data.race
    });
    const pattern = analyzeRacePattern(ranking);
    const adjustedChaos = Math.min(
      100,
      Number(
        (
          pattern.indexes.chaos_index +
          entryAdjusted.chaosBoost +
          (venueAdjustedBase.venue.chaosAdjustment || 0)
        ).toFixed(2)
      )
    );
    const indexes = {
      ...pattern.indexes,
      chaos_index: adjustedChaos
    };

    let racePattern = pattern.race_pattern;
    const maxNonChaos = Math.max(
      indexes.escape_index,
      indexes.sashi_index,
      indexes.makuri_index,
      indexes.makurizashi_index
    );
    if (indexes.chaos_index >= maxNonChaos) {
      racePattern = "chaos";
    }

    let buyType = pattern.buy_type;
    if (indexes.chaos_index >= 78) buyType = "skip";
    else if (indexes.chaos_index >= 62 && buyType === "solid") buyType = "standard";
    else if (indexes.chaos_index >= 62 && buyType === "standard") buyType = "aggressive";

    const monteCarlo = simulateTrifectaProbabilities(ranking, {
      topN: 10,
      simulations: 8000
    });
    const probabilities = monteCarlo.probabilities;
    const simulation = {
      method: "monte_carlo",
      simulations: monteCarlo.simulations,
      top_combinations: monteCarlo.top_combinations
    };
    let evData = {
      ev_analysis: { best_ev_bets: [] },
      oddsData: {
        trifecta: [],
        exacta: [],
        fetched_at: new Date().toISOString(),
        fetch_status: { trifecta: "failed", exacta: "failed" },
        errors: [{ type: "odds", message: "odds_fetch_failed" }]
      }
    };
    try {
      evData = await analyzeExpectedValue({
        date: data.race.date,
        venueId: data.race.venueId,
        raceNo: data.race.raceNo,
        simulation
      });
    } catch (oddsErr) {
      console.warn("[ODDS] fetch failed:", oddsErr?.message || oddsErr);
    }
    const rawBetPlan = buildBetPlan(evData.ev_analysis, 10000);
    const bet_plan = {
      ...rawBetPlan,
      recommended_bets: enrichRecommendedTickets({
        recommendedBets: rawBetPlan.recommended_bets,
        probabilities,
        oddsData: evData.oddsData,
        evAnalysis: evData.ev_analysis
      })
    };
    const motorAnalysis = ranking.map((r) => ({
      rank: r.rank,
      lane: r.racer.lane,
      motor_base_score: r.features.motor_base_score,
      motor_exhibition_score: r.features.motor_exhibition_score,
      motor_momentum_score: r.features.motor_momentum_score,
      motor_total_score: r.features.motor_total_score
    }));
    const motorTrendAnalysis = ranking.map((r) => ({
      rank: r.rank,
      lane: r.racer.lane,
      motor_rate_bias: r.features.motor_rate_bias,
      exhibition_bias: r.features.exhibition_bias,
      trend_up_score: r.features.trend_up_score,
      trend_down_score: r.features.trend_down_score,
      motor_trend_score: r.features.motor_trend_score
    }));
    const entryAnalysis = ranking.map((r) => ({
      rank: r.rank,
      lane: r.racer.lane,
      course_change_score: r.features.course_change_score,
      kado_bonus: r.features.kado_bonus,
      deep_in_penalty: r.features.deep_in_penalty,
      entry_chaos_bonus: r.features.entry_chaos_bonus,
      entry_advantage_score: r.features.entry_advantage_score
    }));

    let prediction = {
      ranking,
      top3: ranking.slice(0, 3).map((r) => r.racer.lane)
    };
    const prediction_before_entry_change = {
      ranking: preRanking,
      top3: preRanking.slice(0, 3).map((r) => r.racer.lane)
    };
    let prediction_after_entry_change = {
      ranking,
      top3: prediction.top3
    };
    let playerStartProfile = analyzePlayerStartProfiles({ ranking });
    const exhibitionAI = analyzeExhibitionAI({ ranking });
    const raceIndexes = analyzeRaceIndexes({
      ranking,
      top3: prediction.top3,
      racePattern,
      indexes,
      raceRisk: null
    });
    const baseRaceRisk = evaluateRaceRisk({
      indexes,
      racePattern,
      ranking,
      are_index: raceIndexes.are_index,
      probabilities,
      participation_mode: participationMode || "active"
    });
    const raceOutcomeProbabilities = estimateRaceOutcomeProbabilities({
      raceIndexes,
      raceRisk: baseRaceRisk,
      racePattern,
      ranking
    });
    let raceFlow = analyzeRaceFlow({
      ranking,
      raceIndexes,
      racePattern,
      raceRisk: baseRaceRisk,
      playerStartProfiles: playerStartProfile
    });
    let wallEvaluation = evaluateLane2Wall({
      ranking,
      raceIndexes,
      racePattern
    });
    const attackScenarioAnalysis = analyzeAttackScenarioLayer({
      ranking,
      raceIndexes,
      raceFlow,
      wallEvaluation,
      entryMeta,
      escapePatternAnalysis,
      playerStartProfile
    });
    ranking = applyAttackScenarioBiasToRanking(ranking, attackScenarioAnalysis);
    prediction = {
      ranking,
      top3: ranking.slice(0, 3).map((r) => r.racer.lane)
    };
    prediction_after_entry_change = {
      ranking,
      top3: prediction.top3
    };
    playerStartProfile = analyzePlayerStartProfiles({ ranking });
    raceFlow = analyzeRaceFlow({
      ranking,
      raceIndexes,
      racePattern,
      raceRisk: baseRaceRisk,
      playerStartProfiles: playerStartProfile
    });
    wallEvaluation = evaluateLane2Wall({
      ranking,
      raceIndexes,
      racePattern
    });
    const { headSelection, partnerSelection } = analyzeHeadAndPartners({
      ranking,
      raceIndexes,
      raceOutcomeProbabilities,
      raceRisk: baseRaceRisk
    });
    const venueBias = analyzeVenueBias({
      race: data.race,
      raceIndexes,
      ranking
    });
    const headPrecision = evaluateHeadPrecision({
      ranking,
      headSelection,
      probabilities,
      raceIndexes,
      raceOutcomeProbabilities,
      exhibitionAI,
      venueBias,
      raceFlow,
      playerStartProfiles: playerStartProfile
    });
    const headSelectionRefined = {
      ...headSelection,
      main_head: headPrecision.main_head ?? headSelection?.main_head ?? null,
      secondary_heads:
        Array.isArray(headPrecision.backup_heads) && headPrecision.backup_heads.length
          ? headPrecision.backup_heads
          : headSelection?.secondary_heads || []
    };
    const partnerPrecision = evaluatePartnerPrecision({
      ranking,
      headSelection: headSelectionRefined,
      raceFlow,
      playerStartProfile
    });
    const headConfidence = evaluateHeadConfidence({
      headSelection: headSelectionRefined,
      raceRisk: baseRaceRisk,
      raceIndexes,
      raceOutcomeProbabilities,
      probabilities,
      wallEvaluation,
      ranking
    });
    const roleCandidates = analyzeRoleCandidates({
      ranking,
      headSelection: headSelectionRefined,
      partnerSelection,
      exhibitionAI,
      raceFlow,
      playerStartProfiles: playerStartProfile,
      partnerPrecision
    });
    const baseRaceStructure = analyzeRaceStructure({
      ranking,
      probabilities,
      headConfidence,
      raceIndexes,
      preRaceAnalysis,
      roleCandidates,
      exhibitionAI
    });
    const raceStructure = applyVenueBiasToStructure({
      raceStructure: {
        ...baseRaceStructure,
        formation_pattern_clarity_score: escapePatternAnalysis.formation_pattern_clarity_score,
        formation_pattern: escapePatternAnalysis.formation_pattern,
        escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
        escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence,
        f_hold_caution_score: Number(
          (
            ranking.slice(0, 3).reduce((acc, row) => acc + toNum(row?.features?.f_hold_caution_penalty, 0), 0) /
            Math.max(1, ranking.slice(0, 3).length)
          ).toFixed(2)
        )
      },
      venueBias
    });
    const refinedRaceRisk = refineRaceRiskWithStructure({
      raceRisk: baseRaceRisk,
      headConfidence,
      preRaceAnalysis,
      roleCandidates,
      probabilities,
      ranking
    });
    const raceRisk = applyVenueBiasToRisk({
      raceRisk: refinedRaceRisk,
      venueBias
    });
    const ticketStrategy = buildTicketStrategy({
      raceOutcomeProbabilities,
      raceIndexes,
      raceRisk
    });
    const aiEnhancement = analyzeHitQuality({
      ranking,
      raceRisk,
      headConfidence,
      partnerSelection,
      oddsData: evData.oddsData,
      probabilities
    });
    const ticketOptimization = optimizeTickets({
      recommendedBets: bet_plan.recommended_bets,
      probabilities,
      oddsData: evData.oddsData,
      recommendation: raceRisk.recommendation,
      raceStructure,
      aiEnhancement
    });
    const marketTrap = detectMarketTraps({
      raceRisk,
      raceStructure,
      raceIndexes,
      recommendedBets: bet_plan.recommended_bets,
      ticketOptimization,
      probabilities
    });
    const rawRaceDecision = decideRaceSelection({
      raceStructure,
      preRaceAnalysis,
      roleCandidates,
      partnerPrecision,
      ticketOptimization,
      headPrecision,
      exhibitionAI,
      venueBias,
      marketTrap,
      raceFlow,
      playerStartProfiles: playerStartProfile
    });
    const signatureTrendContext = loadStartSignatureTrendContext();
    const startSignals = analyzeStartSignals(data.racers, entryMeta, signatureTrendContext);
    const entryAdjustedDecision = applyEntryChangeToDecision(rawRaceDecision, entryMeta);
    const raceDecisionBase = applyStartSignalToDecision(entryAdjustedDecision, startSignals, entryMeta);
    const raceDecision = {
      ...raceDecisionBase,
      factors: {
        ...(raceDecisionBase?.factors || {}),
        formation_pattern_clarity_score: escapePatternAnalysis.formation_pattern_clarity_score,
        formation_pattern: escapePatternAnalysis.formation_pattern,
        escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
        escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence,
        f_hold_caution_score: toNum(raceStructure?.f_hold_caution_score, 0),
        attack_scenario_type: attackScenarioAnalysis.attack_scenario_type,
        attack_scenario_score: attackScenarioAnalysis.attack_scenario_score,
        attack_scenario_applied: attackScenarioAnalysis.attack_scenario_applied
      }
    };
    const recommendation_score = computeRecommendationScore({
      raceDecision,
      raceStructure,
      startSignals,
      entryMeta,
      race: data.race,
      learningWeights,
      contenderSignals: contenderAdjusted.contenderSignals,
      escapePatternAnalysis,
      scenarioSuggestions: null,
      ranking,
      attackScenarioAnalysis
    });
    const ticketGenerationV2 = generateTicketsV2({
      headSelection: headSelectionRefined,
      partnerSelection,
      partnerPrecision,
      headConfidence,
      headPrecision,
      exhibitionAI,
      raceRisk,
      raceIndexes,
      wallEvaluation,
      venueBias,
      marketTrap,
      raceFlow,
      playerStartProfiles: playerStartProfile
    });
    const valueDetection = detectValue({
      recommendedBets: bet_plan.recommended_bets,
      ticketOptimization,
      raceDecision,
      venueBias,
      marketTrap
    });
    const valueByCombo = new Map(
      (Array.isArray(valueDetection?.tickets) ? valueDetection.tickets : []).map((t) => [String(t.combo), t])
    );
    const trapByCombo = new Map(
      (Array.isArray(marketTrap?.ticket_traps) ? marketTrap.ticket_traps : []).map((t) => [String(t.combo), t])
    );
    const stakeAllocation = buildStakeAllocationPlan({
      raceDecision,
      ticketOptimization,
      betPlan: bet_plan,
      ticketGenerationV2,
      valueDetection,
      marketTrap
    });
    const stakeByCombo = new Map(stakeAllocation.tickets.map((t) => [String(t.combo), t]));
    const bet_plan_with_stake = {
      ...bet_plan,
      recommended_bets: (Array.isArray(bet_plan.recommended_bets) ? bet_plan.recommended_bets : []).map((row) => {
        const stake = stakeByCombo.get(String(row?.combo || ""));
        const value = valueByCombo.get(String(row?.combo || ""));
        const trap = trapByCombo.get(String(row?.combo || ""));
        return {
          ...row,
          ticket_type: stake?.ticket_type || "backup",
          value_score: Number.isFinite(Number(value?.value_score)) ? Number(value.value_score) : null,
          overpriced_flag: !!value?.overpriced_flag,
          underpriced_flag: !!value?.underpriced_flag,
          bet_value_tier: value?.bet_value_tier || null,
          trap_flags: Array.isArray(trap?.trap_flags) ? trap.trap_flags : [],
          avoid_level: Number.isFinite(Number(trap?.avoid_level)) ? Number(trap.avoid_level) : 0,
          recommended_bet: Number.isFinite(Number(stake?.recommended_bet))
            ? Number(stake.recommended_bet)
            : Number(row?.bet ?? 100)
        };
      })
    };
    const ticketOptimizationWithStake = {
      ...ticketOptimization,
      optimized_tickets: (Array.isArray(ticketOptimization.optimized_tickets)
        ? ticketOptimization.optimized_tickets
        : []
      ).map((row) => {
        const stake = stakeByCombo.get(String(row?.combo || ""));
        const value = valueByCombo.get(String(row?.combo || ""));
        const trap = trapByCombo.get(String(row?.combo || ""));
        return {
          ...row,
          ticket_type: stake?.ticket_type || row?.ticket_type || "backup",
          value_score: Number.isFinite(Number(value?.value_score)) ? Number(value.value_score) : null,
          overpriced_flag: !!value?.overpriced_flag,
          underpriced_flag: !!value?.underpriced_flag,
          bet_value_tier: value?.bet_value_tier || null,
          trap_flags: Array.isArray(trap?.trap_flags) ? trap.trap_flags : [],
          avoid_level: Number.isFinite(Number(trap?.avoid_level)) ? Number(trap.avoid_level) : 0,
          recommended_bet: Number.isFinite(Number(stake?.recommended_bet))
            ? Number(stake.recommended_bet)
            : Number(row?.recommended_bet ?? 100)
        };
      }),
      bankrollPlan: stakeAllocation.bankrollPlan
    };
    const scenarioSuggestions = buildScenarioSuggestions({
      ranking,
      raceFlow,
      raceIndexes,
      raceDecision,
      entryMeta,
      startSignals,
      ticketOptimization: ticketOptimizationWithStake,
      betPlan: bet_plan_with_stake,
      ticketGenerationV2
    });
    const raceExplainability = buildRaceExplainability({
      raceDecision,
      raceRisk,
      raceFlow,
      raceIndexes,
      entryMeta,
      startSignals,
      manualLapImpact,
      headSelection: headSelectionRefined,
      scenarioSuggestions
    });
    const ticketExplainability = buildBetExplainability({
      tickets: [
        ...(Array.isArray(bet_plan_with_stake?.recommended_bets) ? bet_plan_with_stake.recommended_bets : []),
        ...(Array.isArray(ticketOptimizationWithStake?.optimized_tickets) ? ticketOptimizationWithStake.optimized_tickets : [])
      ],
      bucketByCombo: scenarioSuggestions?.bucket_by_combo || {},
      headSelection: headSelectionRefined,
      entryMeta,
      startSignals,
      scenarioSuggestions
    });
    bet_plan_with_stake.recommended_bets = (Array.isArray(bet_plan_with_stake.recommended_bets)
      ? bet_plan_with_stake.recommended_bets
      : []
    ).map((row) => {
      const combo = normalizeCombo(row?.combo);
      const exp = combo ? ticketExplainability[combo] : null;
      const comboLanes = combo ? combo.split("-").map((n) => toInt(n, null)).filter(Number.isInteger) : [];
      const attackTag =
        attackScenarioAnalysis?.attack_scenario_applied &&
        comboLanes.includes(comboLanes[0]) &&
        comboLanes.includes(
          attackScenarioAnalysis.attack_scenario_type === "two_sashi"
            ? 2
            : attackScenarioAnalysis.attack_scenario_type === "three_makuri" || attackScenarioAnalysis.attack_scenario_type === "three_makuri_sashi"
              ? 3
              : 4
        )
          ? attackScenarioAnalysis.attack_scenario_label
          : null;
      return {
        ...row,
        explanation_tags: [...new Set([...(exp?.explanation_tags || []), ...(attackTag ? [attackTag] : [])])],
        explanation_summary: exp?.explanation_summary || null
      };
    });
    ticketOptimizationWithStake.optimized_tickets = (Array.isArray(ticketOptimizationWithStake.optimized_tickets)
      ? ticketOptimizationWithStake.optimized_tickets
      : []
    ).map((row) => {
      const combo = normalizeCombo(row?.combo);
      const exp = combo ? ticketExplainability[combo] : null;
      const comboLanes = combo ? combo.split("-").map((n) => toInt(n, null)).filter(Number.isInteger) : [];
      const attackTag =
        attackScenarioAnalysis?.attack_scenario_applied &&
        comboLanes.includes(
          attackScenarioAnalysis.attack_scenario_type === "two_sashi"
            ? 2
            : attackScenarioAnalysis.attack_scenario_type === "three_makuri" || attackScenarioAnalysis.attack_scenario_type === "three_makuri_sashi"
              ? 3
              : 4
        )
          ? attackScenarioAnalysis.attack_scenario_label
          : null;
      return {
        ...row,
        explanation_tags: [...new Set([...(exp?.explanation_tags || []), ...(attackTag ? [attackTag] : [])])],
        explanation_summary: exp?.explanation_summary || null
      };
    });
    bet_plan_with_stake.recommended_bets = applyAttackScenarioBiasToTickets(
      bet_plan_with_stake.recommended_bets,
      attackScenarioAnalysis
    );
    ticketOptimizationWithStake.optimized_tickets = applyAttackScenarioBiasToTickets(
      ticketOptimizationWithStake.optimized_tickets,
      attackScenarioAnalysis
    );
    const headScenarioBalanceAnalysis = buildHeadScenarioBalanceAnalysis({
      ranking,
      raceFlow,
      headSelection: headSelectionRefined,
      attackScenarioAnalysis,
      escapePatternAnalysis
    });
    bet_plan_with_stake.recommended_bets = applyHeadScenarioBalanceToTickets(
      bet_plan_with_stake.recommended_bets,
      headScenarioBalanceAnalysis
    );
    ticketOptimizationWithStake.optimized_tickets = applyHeadScenarioBalanceToTickets(
      ticketOptimizationWithStake.optimized_tickets,
      headScenarioBalanceAnalysis
    );
    if (toNum(headScenarioBalanceAnalysis?.survival_guard_applied, 0) === 1) {
      const optimizedRows = Array.isArray(ticketOptimizationWithStake.optimized_tickets)
        ? ticketOptimizationWithStake.optimized_tickets
        : [];
      const optimizedHasHeadOne = optimizedRows.some((row) => normalizeCombo(row?.combo).startsWith("1-"));
      if (!optimizedHasHeadOne) {
        const bestHeadOneFallback = (Array.isArray(bet_plan_with_stake.recommended_bets)
          ? bet_plan_with_stake.recommended_bets
          : []
        ).find((row) => normalizeCombo(row?.combo).startsWith("1-"));
        if (bestHeadOneFallback) {
          ticketOptimizationWithStake.optimized_tickets = [...optimizedRows, {
            ...bestHeadOneFallback,
            ticket_type: bestHeadOneFallback?.ticket_type || "backup",
            survival_guard_injected: 1,
            scenario_balance_tags: [
              ...new Set([...(Array.isArray(bestHeadOneFallback?.scenario_balance_tags) ? bestHeadOneFallback.scenario_balance_tags : []), "SURVIVAL_GUARD_INJECTED"])
            ]
          }].sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0));
        }
      }
    }

    saveFeatureSnapshots(raceId, ranking);

    const confidenceScores = buildConfidenceScores({
      raceDecision,
      headConfidence,
      ticketOptimization: ticketOptimizationWithStake,
      raceStructure,
      startSignals,
      entryMeta,
      learningWeights,
      ranking,
      preRaceAnalysis,
      contenderSignals: contenderAdjusted.contenderSignals,
      scenarioSuggestions,
      escapePatternAnalysis,
      race: data?.race || null
    });
    const participationDecision = buildParticipationDecision({
      raceDecision,
      raceRisk,
      raceStructure,
      entryMeta,
      confidenceScores,
      scenarioSuggestions,
      raceFlow,
      escapePatternAnalysis,
      attackScenarioAnalysis
    });
    const segmentCorrectionUsage = buildSegmentCorrectionUsageSummary({
      learningWeights,
      race: data?.race || null,
      entryMeta,
      escapePatternAnalysis,
      scenarioSuggestions,
      contenderSignals: contenderAdjusted.contenderSignals,
      ranking,
      confidenceScores
    });
    const snapshotCreatedAt = new Date().toISOString();
    const modelVersion = "prediction_snapshot_v2";
    const finalRecommendedSnapshot = buildFinalRecommendedBetsSnapshot({
      recommendedBets: bet_plan_with_stake?.recommended_bets,
      optimizedTickets: ticketOptimizationWithStake?.optimized_tickets
    });
    const boat1HeadSnapshot = buildBoat1HeadBetsSnapshot({
      recommendedBets: bet_plan_with_stake?.recommended_bets,
      optimizedTickets: ticketOptimizationWithStake?.optimized_tickets,
      headScenarioBalanceAnalysis,
      escapePatternAnalysis,
      learningWeights,
      race: data?.race || null
    });
    const startDisplay = saveRaceStartDisplaySnapshot({
      raceId,
      racers: data.racers,
      sourceMeta: data.source || {},
      predictionSnapshot: {
        raceDecision,
        top3: prediction?.top3 || [],
        recommendation: raceRisk?.recommendation || null,
        mode: raceDecision?.mode || null,
        predicted_entry_order: entryMeta.predicted_entry_order,
        actual_entry_order: entryMeta.actual_entry_order,
        entry_changed: entryMeta.entry_changed,
        entry_change_type: entryMeta.entry_change_type
      }
    });
    const rankingFeatureByLane = new Map(
      ranking.map((row) => [toInt(row?.racer?.lane, null), row?.features || {}]).filter((row) => Number.isInteger(row[0]))
    );
    const snapshotPlayers = safeArray(data?.racers).map((racer) => {
      const lane = toInt(racer?.lane, null);
      const laneFeatures = rankingFeatureByLane.get(lane) || {};
      const contributionComponents = buildFeatureContributionComponents({ features: laneFeatures });
      return {
        ...(laneFeatures
          ? {
              f_hold_count: toInt(laneFeatures?.f_hold_count, 0),
              f_hold_bias_applied: toInt(laneFeatures?.f_hold_bias_applied, 0),
              left_neighbor_exists: toInt(laneFeatures?.left_neighbor_exists, 0),
              expected_actual_st_adjustment: toNullableNum(laneFeatures?.expected_actual_st_adjustment),
              expected_actual_st: toNullableNum(laneFeatures?.expected_actual_st),
              display_time_delta_vs_left: toNullableNum(laneFeatures?.display_time_delta_vs_left),
              avg_st_rank_delta_vs_left: toNullableNum(laneFeatures?.avg_st_rank_delta_vs_left),
              slit_alert_flag: toInt(laneFeatures?.slit_alert_flag, 0),
              feature_snapshot: laneFeatures,
              contribution_components: contributionComponents
            }
          : {}),
        lane,
        registration_no: toInt(racer?.registrationNo, null),
        name: racer?.name || null,
        class: racer?.class || null,
        branch: racer?.branch || null,
        age: toInt(racer?.age, null),
        weight: toNullableNum(racer?.weight),
        avg_st: toNullableNum(racer?.avgSt),
        nationwide_win_rate: toNullableNum(racer?.nationwideWinRate),
        local_win_rate: toNullableNum(racer?.localWinRate),
        motor_no: toInt(racer?.motorNo, null),
        motor_2rate: toNullableNum(racer?.motor2Rate),
        boat_no: toInt(racer?.boatNo, null),
        boat_2rate: toNullableNum(racer?.boat2Rate),
        exhibition_time: toNullableNum(racer?.exhibitionTime),
        exhibition_st: toNullableNum(racer?.exhibitionSt),
        exhibition_st_raw: racer?.exhibitionStRaw || null,
        entry_course: toInt(racer?.entryCourse, null),
        tilt: toNullableNum(racer?.tilt)
      };
    });
    const snapshotContext = {
      race_key: raceId,
      race_date: data?.race?.date || null,
      venue_code: Number.isFinite(Number(data?.race?.venueId)) ? Number(data.race.venueId) : null,
      venue_name: data?.race?.venueName || null,
      race_no: Number.isFinite(Number(data?.race?.raceNo)) ? Number(data.race.raceNo) : null,
      race_grade: data?.race?.grade || data?.race?.raceGrade || null,
      race_name: data?.race?.raceName || null,
      weather: data?.race?.weather || null,
      wind_speed: toNullableNum(data?.race?.windSpeed),
      wind_direction: data?.race?.windDirection || null,
      wave_height: toNullableNum(data?.race?.waveHeight),
      players: snapshotPlayers,
      player_summary: snapshotPlayers,
      entry: {
        predicted_entry_order: entryMeta.predicted_entry_order,
        actual_entry_order: entryMeta.actual_entry_order,
        start_exhibition_st: startDisplay?.start_display_st || {},
        start_display_order: startDisplay?.start_display_order || [],
        start_display_timing: startDisplay?.start_display_timing || {},
        entry_changed: !!entryMeta.entry_changed,
        entry_change_type: entryMeta.entry_change_type || "none",
        entry_change_summary: {
          changed: !!entryMeta.entry_changed,
          type: entryMeta.entry_change_type || "none",
          predicted: entryMeta.predicted_entry_order || [],
          actual: entryMeta.actual_entry_order || []
        }
      },
      start_display: startDisplay || null,
      manual_lap_evaluation: manualLapEvaluation || null,
      manual_lap_impact: manualLapImpact || null,
      scenario_labels: [
        racePattern?.pattern || racePattern?.label || null,
        scenarioSuggestions?.main_reason || null,
        attackScenarioAnalysis?.attack_scenario_label || null
      ].filter(Boolean),
      scenario_type: scenarioSuggestions?.scenario_type || null,
      scenario_match_score: toNullableNum(scenarioSuggestions?.scenario_confidence),
      attack_scenario_type: attackScenarioAnalysis?.attack_scenario_type || null,
      attack_scenario_label: attackScenarioAnalysis?.attack_scenario_label || null,
      attack_scenario_score: toNullableNum(attackScenarioAnalysis?.attack_scenario_score),
      attack_scenario_reason_tags: Array.isArray(attackScenarioAnalysis?.attack_scenario_reason_tags)
        ? attackScenarioAnalysis.attack_scenario_reason_tags
        : [],
      attack_scenario_applied: toInt(attackScenarioAnalysis?.attack_scenario_applied, 0),
      two_sashi_score: toNullableNum(attackScenarioAnalysis?.two_sashi_score),
      three_makuri_score: toNullableNum(attackScenarioAnalysis?.three_makuri_score),
      three_makuri_sashi_score: toNullableNum(attackScenarioAnalysis?.three_makuri_sashi_score),
      four_cado_makuri_score: toNullableNum(attackScenarioAnalysis?.four_cado_makuri_score),
      four_cado_makuri_sashi_score: toNullableNum(attackScenarioAnalysis?.four_cado_makuri_sashi_score),
      main_scenario_type: headScenarioBalanceAnalysis.main_scenario_type,
      counter_scenario_type: headScenarioBalanceAnalysis.counter_scenario_type,
      survival_scenario_type: headScenarioBalanceAnalysis.survival_scenario_type,
      head_distribution_json: headScenarioBalanceAnalysis.head_distribution_json,
      survival_guard_applied: toInt(headScenarioBalanceAnalysis.survival_guard_applied, 0),
      removed_candidate_reason_tags: Array.isArray(headScenarioBalanceAnalysis.removed_candidate_reason_tags)
        ? headScenarioBalanceAnalysis.removed_candidate_reason_tags
        : [],
      boat1_head_bets_snapshot: boat1HeadSnapshot.items,
      boat1_head_score: boat1HeadSnapshot.boat1_head_score,
      boat1_survival_residual_score: boat1HeadSnapshot.boat1_survival_residual_score,
      boat1_head_section_shown: boat1HeadSnapshot.shown ? 1 : 0,
      boat1_head_reason_tags: boat1HeadSnapshot.boat1_head_reason_tags,
      formation_pattern: escapePatternAnalysis.formation_pattern,
      escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
      escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence,
      escape_second_place_bias_json: escapePatternAnalysis.escape_second_place_bias_json,
      f_hold_bias_applied: ranking.some((row) => toNum(row?.features?.f_hold_bias_applied, 0) > 0) ? 1 : 0,
      contender_signals: contenderAdjusted.contenderSignals,
      feature_contribution_families: {
        head_fixed_confidence_raw: confidenceScores.head_confidence_raw,
        head_fixed_confidence: confidenceScores.head_confidence_calibrated,
        recommended_bet_confidence_raw: confidenceScores.bet_confidence_raw,
        recommended_bet_confidence: confidenceScores.bet_confidence_calibrated,
        participation_score_components: participationDecision.participation_score_components
      },
      confidence_calibration: {
        head_confidence_raw: confidenceScores.head_confidence_raw,
        head_confidence_calibrated: confidenceScores.head_confidence_calibrated,
        bet_confidence_raw: confidenceScores.bet_confidence_raw,
        bet_confidence_calibrated: confidenceScores.bet_confidence_calibrated,
        head_confidence_bucket: confidenceScores.head_confidence_bucket,
        bet_confidence_bucket: confidenceScores.bet_confidence_bucket,
        calibration_applied: confidenceScores.confidence_calibration_applied ? 1 : 0,
        calibration_source: confidenceScores.confidence_calibration_source,
        calibration_segments: confidenceScores.confidence_calibration_segments || []
      },
      segment_corrections_used: segmentCorrectionUsage
    };
    const learningContext = {
      venue_id: snapshotContext.venue_code,
      venue_name: snapshotContext.venue_name,
      race_grade: snapshotContext.race_grade,
      weather: snapshotContext.weather,
      wind_speed: snapshotContext.wind_speed,
      wave_height: snapshotContext.wave_height,
      motor_rate_avg: toNullableNum(
        snapshotPlayers.length
          ? snapshotPlayers.reduce((sum, row) => sum + Number(row.motor_2rate || 0), 0) / snapshotPlayers.length
          : null
      ),
      boat_rate_avg: toNullableNum(
        snapshotPlayers.length
          ? snapshotPlayers.reduce((sum, row) => sum + Number(row.boat_2rate || 0), 0) / snapshotPlayers.length
          : null
      ),
      avg_st_avg: toNullableNum(
        snapshotPlayers.length
          ? snapshotPlayers.reduce((sum, row) => sum + Number(row.avg_st || 0), 0) / snapshotPlayers.length
          : null
      ),
      exhibition_time_avg: toNullableNum(
        snapshotPlayers.length
          ? snapshotPlayers.reduce((sum, row) => sum + Number(row.exhibition_time || 0), 0) / snapshotPlayers.length
          : null
      ),
      start_display_signature: startDisplay?.start_display_signature || null,
      entry_changed: !!entryMeta.entry_changed,
      entry_change_type: entryMeta.entry_change_type || "none",
      recommendation_score,
      confidence: raceDecision?.confidence ?? null,
      head_confidence: confidenceScores.head_confidence_calibrated,
      bet_confidence: confidenceScores.bet_confidence_calibrated,
      head_confidence_raw: confidenceScores.head_confidence_raw,
      head_confidence_calibrated: confidenceScores.head_confidence_calibrated,
      bet_confidence_raw: confidenceScores.bet_confidence_raw,
      bet_confidence_calibrated: confidenceScores.bet_confidence_calibrated,
      head_confidence_bucket: confidenceScores.head_confidence_bucket,
      bet_confidence_bucket: confidenceScores.bet_confidence_bucket,
      confidence_bucket: confidenceScores.confidence_bucket,
      confidence_calibration_applied: confidenceScores.confidence_calibration_applied ? 1 : 0,
      confidence_calibration_source: confidenceScores.confidence_calibration_source,
      confidence_calibration_segments: confidenceScores.confidence_calibration_segments || [],
      confidence_calibration_thresholds: confidenceScores.confidence_calibration_thresholds || {},
      scenario_labels: snapshotContext.scenario_labels,
      scenario_type: snapshotContext.scenario_type,
      scenario_match_score: snapshotContext.scenario_match_score,
      attack_scenario_type: snapshotContext.attack_scenario_type,
      attack_scenario_label: snapshotContext.attack_scenario_label,
      attack_scenario_score: snapshotContext.attack_scenario_score,
      attack_scenario_reason_tags: snapshotContext.attack_scenario_reason_tags,
      attack_scenario_applied: snapshotContext.attack_scenario_applied,
      two_sashi_score: snapshotContext.two_sashi_score,
      three_makuri_score: snapshotContext.three_makuri_score,
      three_makuri_sashi_score: snapshotContext.three_makuri_sashi_score,
      four_cado_makuri_score: snapshotContext.four_cado_makuri_score,
      four_cado_makuri_sashi_score: snapshotContext.four_cado_makuri_sashi_score,
      main_scenario_type: snapshotContext.main_scenario_type,
      counter_scenario_type: snapshotContext.counter_scenario_type,
      survival_scenario_type: snapshotContext.survival_scenario_type,
      head_distribution_json: snapshotContext.head_distribution_json,
      survival_guard_applied: snapshotContext.survival_guard_applied,
      removed_candidate_reason_tags: snapshotContext.removed_candidate_reason_tags,
      boat1_head_bets_snapshot: snapshotContext.boat1_head_bets_snapshot,
      boat1_head_score: snapshotContext.boat1_head_score,
      boat1_survival_residual_score: snapshotContext.boat1_survival_residual_score,
      boat1_head_section_shown: snapshotContext.boat1_head_section_shown,
      boat1_head_reason_tags: snapshotContext.boat1_head_reason_tags,
      formation_pattern: escapePatternAnalysis.formation_pattern,
      escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
      escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence,
      escape_second_place_bias_json: escapePatternAnalysis.escape_second_place_bias_json,
      f_hold_bias_applied: ranking.some((row) => toNum(row?.features?.f_hold_bias_applied, 0) > 0) ? 1 : 0,
      participation_decision: participationDecision.decision,
      participation_decision_reason: participationDecision.summary,
      participation_score_components: participationDecision.participation_score_components,
      participation_version: participationDecision.participation_version,
      contender_signals: contenderAdjusted.contenderSignals,
      segment_corrections_used: segmentCorrectionUsage,
      feature_contribution_summary: {
        player_score_component: Number(ranking.slice(0, 3).reduce((acc, row) => acc + buildFeatureContributionComponents(row).player_score_component, 0).toFixed(3)),
        motor_score_component: Number(ranking.slice(0, 3).reduce((acc, row) => acc + buildFeatureContributionComponents(row).motor_score_component, 0).toFixed(3)),
        exhibition_score_component: Number(ranking.slice(0, 3).reduce((acc, row) => acc + buildFeatureContributionComponents(row).exhibition_score_component, 0).toFixed(3)),
        start_st_score_component: Number(ranking.slice(0, 3).reduce((acc, row) => acc + buildFeatureContributionComponents(row).start_st_score_component, 0).toFixed(3)),
        formation_pattern_bias_component: Number(ranking.slice(0, 3).reduce((acc, row) => acc + buildFeatureContributionComponents(row).formation_pattern_bias_component, 0).toFixed(3)),
        left_neighbor_bias_component: Number(ranking.slice(0, 3).reduce((acc, row) => acc + buildFeatureContributionComponents(row).left_neighbor_bias_component, 0).toFixed(3)),
        f_hold_bias_component: Number(ranking.slice(0, 3).reduce((acc, row) => acc + buildFeatureContributionComponents(row).f_hold_bias_component, 0).toFixed(3)),
        scenario_bias_component: Number(ranking.slice(0, 3).reduce((acc, row) => acc + buildFeatureContributionComponents(row).scenario_bias_component, 0).toFixed(3))
      }
    };
    const predictionWithEntry = {
      ...prediction,
      predicted_entry_order: entryMeta.predicted_entry_order,
      actual_entry_order: entryMeta.actual_entry_order,
      entry_changed: entryMeta.entry_changed,
      entry_change_type: entryMeta.entry_change_type,
      confidence_scores: confidenceScores,
      head_confidence: confidenceScores.head_confidence_calibrated,
      bet_confidence: confidenceScores.bet_confidence_calibrated,
      head_confidence_raw: confidenceScores.head_confidence_raw,
      head_confidence_calibrated: confidenceScores.head_confidence_calibrated,
      bet_confidence_raw: confidenceScores.bet_confidence_raw,
      bet_confidence_calibrated: confidenceScores.bet_confidence_calibrated,
      participation_decision: participationDecision.decision,
      confidence_reason_tags: confidenceScores.confidence_reason_tags,
      confidence_version: confidenceScores.confidence_version,
      snapshot_created_at: snapshotCreatedAt,
      race_key: raceId,
      model_version: modelVersion,
      formation_pattern: escapePatternAnalysis.formation_pattern,
      attack_scenario_type: attackScenarioAnalysis?.attack_scenario_type || null,
      attack_scenario_label: attackScenarioAnalysis?.attack_scenario_label || null,
      attack_scenario_score: toNullableNum(attackScenarioAnalysis?.attack_scenario_score),
      attack_scenario_reason_tags: Array.isArray(attackScenarioAnalysis?.attack_scenario_reason_tags)
        ? attackScenarioAnalysis.attack_scenario_reason_tags
        : [],
      attack_scenario_applied: toInt(attackScenarioAnalysis?.attack_scenario_applied, 0),
      two_sashi_score: toNullableNum(attackScenarioAnalysis?.two_sashi_score),
      three_makuri_score: toNullableNum(attackScenarioAnalysis?.three_makuri_score),
      three_makuri_sashi_score: toNullableNum(attackScenarioAnalysis?.three_makuri_sashi_score),
      four_cado_makuri_score: toNullableNum(attackScenarioAnalysis?.four_cado_makuri_score),
      four_cado_makuri_sashi_score: toNullableNum(attackScenarioAnalysis?.four_cado_makuri_sashi_score),
      main_scenario_type: snapshotContext.main_scenario_type,
      counter_scenario_type: snapshotContext.counter_scenario_type,
      survival_scenario_type: snapshotContext.survival_scenario_type,
      head_distribution_json: snapshotContext.head_distribution_json,
      survival_guard_applied: snapshotContext.survival_guard_applied,
      removed_candidate_reason_tags: snapshotContext.removed_candidate_reason_tags,
      boat1_head_bets_snapshot: snapshotContext.boat1_head_bets_snapshot,
      boat1_head_score: snapshotContext.boat1_head_score,
      boat1_survival_residual_score: snapshotContext.boat1_survival_residual_score,
      boat1_head_section_shown: snapshotContext.boat1_head_section_shown,
      boat1_head_reason_tags: snapshotContext.boat1_head_reason_tags,
      escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
      escape_second_place_bias_json: escapePatternAnalysis.escape_second_place_bias_json,
      escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence,
      f_hold_bias_applied: ranking.some((row) => toNum(row?.features?.f_hold_bias_applied, 0) > 0) ? 1 : 0,
      participation_decision_reason: participationDecision.summary,
      participation_score_components: participationDecision.participation_score_components,
      participation_version: participationDecision.participation_version,
      boat1_head_bets_snapshot: boat1HeadSnapshot.items,
      boat1_head_score: boat1HeadSnapshot.boat1_head_score,
      boat1_survival_residual_score: boat1HeadSnapshot.boat1_survival_residual_score,
      boat1_head_section_shown: boat1HeadSnapshot.shown ? 1 : 0,
      boat1_head_reason_tags: boat1HeadSnapshot.boat1_head_reason_tags,
      final_recommended_bets_snapshot: finalRecommendedSnapshot.items,
      final_recommended_bets_count: finalRecommendedSnapshot.items.length,
      final_recommended_bets_snapshot_source: finalRecommendedSnapshot.snapshot_source,
      snapshot_context: snapshotContext,
      learning_context: learningContext,
      ai_bets_full_snapshot: {
        recommended_bets: Array.isArray(bet_plan_with_stake?.recommended_bets)
          ? bet_plan_with_stake.recommended_bets
          : [],
        optimized_tickets: Array.isArray(ticketOptimizationWithStake?.optimized_tickets)
          ? ticketOptimizationWithStake.optimized_tickets
          : [],
        ticket_generation_v2: {
          primary_tickets: Array.isArray(ticketGenerationV2?.primary_tickets)
            ? ticketGenerationV2.primary_tickets
            : [],
          secondary_tickets: Array.isArray(ticketGenerationV2?.secondary_tickets)
            ? ticketGenerationV2.secondary_tickets
            : []
        },
        scenario_suggestions: {
          main_picks: Array.isArray(scenarioSuggestions?.main_picks) ? scenarioSuggestions.main_picks : [],
          backup_picks: Array.isArray(scenarioSuggestions?.backup_picks) ? scenarioSuggestions.backup_picks : [],
          longshot_picks: Array.isArray(scenarioSuggestions?.longshot_picks) ? scenarioSuggestions.longshot_picks : []
        },
        boat1_head_bets: boat1HeadSnapshot.items
      },
      ai_bets_display_snapshot: finalRecommendedSnapshot.items,
      prediction_before_entry_change,
      prediction_after_entry_change
    };

    savePredictionLog({
      raceId,
      race: data?.race || null,
      racePattern,
      buyType,
      raceRisk,
      prediction: predictionWithEntry,
      raceDecision,
      probabilities,
      ev_analysis: evData.ev_analysis,
      bet_plan: bet_plan_with_stake
    });
    const includeStartDebug = parseBooleanFlag(req.query?.debugStart, false);
    const startExhibitionDebug = includeStartDebug
      ? {
          race_key: {
            race_id: raceId,
            date: data?.race?.date || null,
            venue_id: data?.race?.venueId ?? null,
            race_no: data?.race?.raceNo ?? null
          },
          source_timestamp: data?.source?.fetched_at || null,
          layer1_raw_fetch: (Array.isArray(data?.racers) ? data.racers : [])
            .map((r) => ({
              boat_no: toInt(r?.lane, null),
              raw_entry_order: toInt(r?.startRaw?.fallbackEntryCourse ?? r?.entryCourse, null),
              raw_st_string: r?.startRaw?.fallbackRawSt ?? r?.startRaw?.rawSt ?? r?.exhibitionStRaw ?? null,
              source_block:
                r?.startRaw?.fallbackRawSt != null
                  ? "start_exhibition_block(.table1_boatImage1)"
                  : "beforeinfo_table(table.is-w748)",
              raw_beforeinfo_st: r?.startRaw?.rawSt ?? null,
              raw_start_exhibition_st: r?.startRaw?.fallbackRawSt ?? null
            }))
            .sort((a, b) => toInt(a.boat_no, 0) - toInt(b.boat_no, 0)),
          layer2_normalized_by_boat: (Array.isArray(data?.racers) ? data.racers : [])
            .map((r) => ({
              boat_no: toInt(r?.lane, null),
              normalized_entry_order: toInt(r?.entryCourse, null),
              normalized_st_raw: r?.exhibitionStRaw ?? null,
              normalized_st_type: r?.exhibitionStType ?? "missing",
              normalized_st_numeric: nullableNum(r?.exhibitionStNumeric)
            }))
            .sort((a, b) => toInt(a.boat_no, 0) - toInt(b.boat_no, 0)),
          layer3_render_input: (Array.isArray(startDisplay?.start_display_debug) ? startDisplay.start_display_debug : [])
            .map((row) => ({
              displayed_boat_no: toInt(row?.lane, null),
              displayed_entry_label: toInt(row?.entry_order, null),
              displayed_st_string:
                startDisplay?.start_display_timing?.[String(toInt(row?.lane, 0))]?.display ?? "--",
              computed_visual_unit: nullableNum(row?.visual_unit),
              computed_percent_position: nullableNum(row?.visual_percent)
            }))
            .sort((a, b) => toInt(a.displayed_boat_no, 0) - toInt(b.displayed_boat_no, 0))
        }
      : null;
    savePredictionFeatureLog({
      raceId,
      race: data.race,
      racers: data.racers,
      startDisplay,
      entryMeta,
      raceDecision,
      predictionSnapshot: predictionWithEntry,
      predictionBeforeEntryChange: prediction_before_entry_change,
      predictionAfterEntryChange: prediction_after_entry_change
    });
    return res.json({
      source: data.source || {},
      race: data.race,
      racers: data.racers,
      raceId,
      manualLapEvaluation,
      manualLapImpact,
      is_recommended: !!participationDecision?.is_recommended,
      recommendation_label:
        participationDecision?.decision === "recommended"
          ? "Recommended"
          : participationDecision?.decision === "watch"
            ? "Watch"
            : "Not Recommended",
      participationDecision,
      confidenceScores,
      attackScenario: attackScenarioAnalysis,
      headScenarioBalance: headScenarioBalanceAnalysis,
      boat1HeadSection: {
        boat1_head_bets_snapshot: boat1HeadSnapshot.items,
        boat1_head_score: boat1HeadSnapshot.boat1_head_score,
        boat1_survival_residual_score: boat1HeadSnapshot.boat1_survival_residual_score,
        boat1_head_section_shown: boat1HeadSnapshot.shown ? 1 : 0,
        boat1_head_reason_tags: boat1HeadSnapshot.boat1_head_reason_tags
      },
      prediction: predictionWithEntry,
      predicted_entry_order: entryMeta.predicted_entry_order,
      actual_entry_order: entryMeta.actual_entry_order,
      entry_changed: entryMeta.entry_changed,
      entry_change_type: entryMeta.entry_change_type,
      startSignalAnalysis: startSignals,
      recommendation_score,
      scenarioSuggestions,
      contenderSignals: contenderAdjusted.contenderSignals,
      explainability: raceExplainability,
      learningWeights,
      learningContext: {
        active_last_run_id: learningState?.active_last_run_id || null,
        active_updated_at: learningState?.active_updated_at || null
      },
      prediction_before_entry_change,
      prediction_after_entry_change,
      racePattern,
      buyType,
      indexes,
      probabilities,
      motorAnalysis,
      motorTrendAnalysis,
      entryAnalysis,
      preRaceAnalysis,
      exhibitionAI,
      simulation,
      oddsData: evData.oddsData,
      ev_analysis: evData.ev_analysis,
      bet_plan: bet_plan_with_stake,
      bankrollPlan: stakeAllocation.bankrollPlan,
      raceRisk,
      raceIndexes,
      raceOutcomeProbabilities,
      ticketStrategy,
      wallEvaluation,
      headSelection: headSelectionRefined,
      headPrecision,
      partnerSelection,
      partnerPrecision,
      roleCandidates,
      headConfidence,
      venueBias,
      raceStructure,
      playerStartProfile,
      ticketGenerationV2,
      aiEnhancement,
      ticketOptimization: ticketOptimizationWithStake,
      scenarioSuggestions,
      raceDecision,
      valueDetection,
      marketTrap,
      raceFlow,
      startDisplay: startDisplay || null,
      startDisplayDebug: Array.isArray(startDisplay?.start_display_debug)
        ? startDisplay.start_display_debug
        : [],
      startExhibitionDebug
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/manual-lap-evaluation", async (req, res, next) => {
  try {
    const raceIdFromQuery = String(req.query?.raceId || "").trim();
    const date = String(req.query?.date || "").trim();
    const venueId = toInt(req.query?.venueId, null);
    const raceNo = toInt(req.query?.raceNo, null);
    const raceId =
      raceIdFromQuery ||
      buildRaceIdFromParts({
        date,
        venueId,
        raceNo
      });
    if (!raceId) {
      return res.status(400).json({
        error: "bad_request",
        message: "raceId or (date, venueId, raceNo) is required"
      });
    }
    const row = getManualLapEvaluation(raceId);
    return res.json({
      raceId,
      manualLapEvaluation: row || null
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/manual-lap-evaluation", async (req, res, next) => {
  try {
    const raceIdFromBody = String(req.body?.raceId || "").trim();
    const date = String(req.body?.date || "").trim();
    const venueId = toInt(req.body?.venueId, null);
    const raceNo = toInt(req.body?.raceNo, null);
    const raceId =
      raceIdFromBody ||
      buildRaceIdFromParts({
        date,
        venueId,
        raceNo
      });
    if (!raceId) {
      return res.status(400).json({
        error: "bad_request",
        message: "raceId or (date, venueId, raceNo) is required"
      });
    }
    const scoresByLane = req.body?.scores_by_lane || req.body?.scoresByLane || {};
    const raceMemo = req.body?.race_memo ?? req.body?.raceMemo ?? null;
    const saved = saveManualLapEvaluation({
      raceId,
      scoresByLane,
      raceMemo
    });
    return res.json({
      ok: true,
      raceId,
      manualLapEvaluation: saved
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/recommendations", async (req, res, next) => {
  try {
    const { date, participationMode } = req.query;
    if (!date) {
      return res.status(400).json({
        error: "bad_request",
        message: "date is required query param"
      });
    }

    const limit = Math.max(1, Math.min(30, toInt(req.query?.limit, 12)));
    const dateMode = getDateMode(date);
    const confidenceMinBase = Math.max(0, Math.min(100, toInt(req.query?.confidenceMin, 68)));
    const maxChaosBase = Math.max(0, Math.min(100, toInt(req.query?.maxChaos, 65)));
    const headStabilityMinBase = Math.max(0, Math.min(100, toInt(req.query?.headStabilityMin, 50)));
    const confidenceMin = dateMode.isFuture ? Math.min(confidenceMinBase, 52) : confidenceMinBase;
    const maxChaos = dateMode.isFuture ? Math.max(maxChaosBase, 82) : maxChaosBase;
    const headStabilityMin = dateMode.isFuture ? Math.min(headStabilityMinBase, 38) : headStabilityMinBase;
    const learningWeights = getActiveLearningWeights();
    const raceNoList = String(req.query?.raceNos || "")
      .split(",")
      .map((v) => toInt(v))
      .filter((v) => Number.isInteger(v) && v >= 1 && v <= 12);
    const venueList = String(req.query?.venues || "")
      .split(",")
      .map((v) => toInt(v))
      .filter((v) => Number.isInteger(v) && v >= 1 && v <= 24);

    const scanRaceNos = raceNoList.length ? raceNoList : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    const scanVenues = venueList.length
      ? venueList
      : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24];
    const scanAll = scanVenues.length * scanRaceNos.length;
    const defaultMaxScan = dateMode.isFuture ? scanAll : 144;
    const maxScan = Math.max(1, Math.min(scanAll, toInt(req.query?.maxScan, defaultMaxScan)));
    const allowRefresh = parseBooleanFlag(req.query?.refresh, false);
    const refreshTimeoutMs = Math.max(800, Math.min(5000, toInt(req.query?.refreshTimeoutMs, 1800)));
    const refreshBudgetMs = Math.max(1500, Math.min(12000, toInt(req.query?.refreshBudgetMs, 5000)));

    const recs = [];
    const candidatePool = [];
    const errors = [];
    const refreshWarnings = [];
    const signatureTrendContext = loadStartSignatureTrendContext();
    const startedAt = Date.now();
    let usedCachedCount = 0;
    let partialData = false;
    let scanned = 0;

    for (const venueId of scanVenues) {
      for (const raceNo of scanRaceNos) {
        if (scanned >= maxScan) break;
        scanned += 1;
        try {
          const canRefresh = allowRefresh && Date.now() - startedAt <= refreshBudgetMs;
          const resolved = await resolveRaceDataForList({
            date,
            venueId,
            raceNo,
            allowRefresh: canRefresh,
            refreshTimeoutMs
          });
          if (resolved.warning) {
            partialData = true;
            refreshWarnings.push({
              venueId,
              raceNo,
              warning: resolved.warning
            });
          }
          if (!resolved.data) continue;
          if (resolved.usedCachedData) usedCachedCount += 1;
          const data = resolved.data;
          const entryMeta = buildEntryOrderMeta(data.racers);
          const baseFeatures = applyMotorPerformanceFeatures(
            applyCoursePerformanceFeatures(buildRaceFeatures(data.racers, data.race))
          );
          const venueAdjustedBase = applyVenueAdjustments(baseFeatures, data.race);
          const preRanking = rankRace(venueAdjustedBase.racersWithFeatures);
          const prePattern = analyzeRacePattern(preRanking);
          const trendFeatures = applyMotorTrendFeatures(venueAdjustedBase.racersWithFeatures, {
            racePattern: prePattern.race_pattern
          });
          const entryAdjusted = applyEntryDynamicsFeatures(trendFeatures, {
            racePattern: prePattern.race_pattern,
            chaos_index: prePattern.indexes.chaos_index
          });
          const rankingBase = rankRace(entryAdjusted.racersWithFeatures);
          const raceId = buildRaceIdFromParts({
            date: data.race?.date,
            venueId: data.race?.venueId,
            raceNo: data.race?.raceNo
          });
          const manualLapEvaluation = raceId ? getManualLapEvaluation(raceId) : null;
          const contenderAdjusted = applyContenderSynergy(rankingBase);
          const rankingBeforePatternBias = contenderAdjusted.ranking;
          const patternBeforeBias = analyzeRacePattern(rankingBeforePatternBias);
          const escapePatternAnalysis = analyzeEscapeFormationLayer({
            ranking: rankingBeforePatternBias,
            racePattern: patternBeforeBias.race_pattern,
            indexes: patternBeforeBias.indexes
          });
          const ranking = applyEscapeFormationBiasToRanking(rankingBeforePatternBias, escapePatternAnalysis, learningWeights, data?.race || null);
          const manualLapImpact = {
            enabled: false,
            applied_lane_count: 0,
            average_adjustment: 0,
            note: "manual_lap_disabled"
          };
          const preRaceAnalysis = analyzePreRaceForm({ ranking, race: data.race });
          const pattern = analyzeRacePattern(ranking);
          const adjustedChaos = Math.min(
            100,
            Number(
              (
                pattern.indexes.chaos_index +
                entryAdjusted.chaosBoost +
                (venueAdjustedBase.venue.chaosAdjustment || 0)
              ).toFixed(2)
            )
          );
          const indexes = { ...pattern.indexes, chaos_index: adjustedChaos };
          const monteCarlo = simulateTrifectaProbabilities(ranking, { topN: 10, simulations: 4000 });
          const probabilities = monteCarlo.probabilities;

          let evData = {
            ev_analysis: { best_ev_bets: [] },
            oddsData: {
              trifecta: [],
              exacta: [],
              fetched_at: new Date().toISOString(),
              fetch_status: { trifecta: "failed", exacta: "failed" },
              errors: [{ type: "odds", message: "odds_fetch_failed" }]
            }
          };
          try {
            evData = await analyzeExpectedValue({
              date: data.race.date,
              venueId: data.race.venueId,
              raceNo: data.race.raceNo,
              simulation: {
                method: "monte_carlo",
                simulations: monteCarlo.simulations,
                top_combinations: monteCarlo.top_combinations
              }
            });
          } catch {
            // keep graceful odds fallback
          }

          const rawBetPlan = buildBetPlan(evData.ev_analysis, 10000);
          const bet_plan = {
            ...rawBetPlan,
            recommended_bets: enrichRecommendedTickets({
              recommendedBets: rawBetPlan.recommended_bets,
              probabilities,
              oddsData: evData.oddsData,
              evAnalysis: evData.ev_analysis
            })
          };

          const raceIndexes = analyzeRaceIndexes({
            ranking,
            top3: ranking.slice(0, 3).map((r) => r.racer.lane),
            racePattern: pattern.race_pattern,
            indexes,
            raceRisk: null
          });
          const exhibitionAI = analyzeExhibitionAI({ ranking });
          const playerStartProfile = analyzePlayerStartProfiles({ ranking });
          const baseRaceRisk = evaluateRaceRisk({
            indexes,
            racePattern: pattern.race_pattern,
            ranking,
            are_index: raceIndexes.are_index,
            probabilities,
            participation_mode: participationMode || "active"
          });
          const raceOutcomeProbabilities = estimateRaceOutcomeProbabilities({
            raceIndexes,
            raceRisk: baseRaceRisk,
            racePattern: pattern.race_pattern,
            ranking
          });
          const raceFlow = analyzeRaceFlow({
            ranking,
            raceIndexes,
            racePattern: pattern.race_pattern,
            raceRisk: baseRaceRisk,
            playerStartProfiles: playerStartProfile
          });
          const wallEvaluation = evaluateLane2Wall({
            ranking,
            raceIndexes,
            racePattern: pattern.race_pattern
          });
          const { headSelection, partnerSelection } = analyzeHeadAndPartners({
            ranking,
            raceIndexes,
            raceOutcomeProbabilities,
            raceRisk: baseRaceRisk
          });
          const venueBias = analyzeVenueBias({
            race: data.race,
            raceIndexes,
            ranking
          });
          const headPrecision = evaluateHeadPrecision({
            ranking,
            headSelection,
            probabilities,
            raceIndexes,
            raceOutcomeProbabilities,
            exhibitionAI,
            venueBias,
            raceFlow,
            playerStartProfiles: playerStartProfile
          });
          const headSelectionRefined = {
            ...headSelection,
            main_head: headPrecision.main_head ?? headSelection?.main_head ?? null,
            secondary_heads:
              Array.isArray(headPrecision.backup_heads) && headPrecision.backup_heads.length
                ? headPrecision.backup_heads
                : headSelection?.secondary_heads || []
          };
          const headConfidence = evaluateHeadConfidence({
            headSelection: headSelectionRefined,
            raceRisk: baseRaceRisk,
            raceIndexes,
            raceOutcomeProbabilities,
            probabilities,
            wallEvaluation,
            ranking
          });
          const partnerPrecision = evaluatePartnerPrecision({
            ranking,
            headSelection: headSelectionRefined,
            raceFlow,
            playerStartProfile
          });
          const roleCandidates = analyzeRoleCandidates({
            ranking,
            headSelection: headSelectionRefined,
            partnerSelection,
            exhibitionAI,
            raceFlow,
            playerStartProfiles: playerStartProfile,
            partnerPrecision
          });
          const baseRaceStructure = analyzeRaceStructure({
            ranking,
            probabilities,
            headConfidence,
            raceIndexes,
            preRaceAnalysis,
            roleCandidates,
            exhibitionAI
          });
          const raceStructure = applyVenueBiasToStructure({
            raceStructure: {
              ...baseRaceStructure,
              formation_pattern_clarity_score: escapePatternAnalysis.formation_pattern_clarity_score,
              formation_pattern: escapePatternAnalysis.formation_pattern,
              escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
              escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence
            },
            venueBias
          });
          const refinedRaceRisk = refineRaceRiskWithStructure({
            raceRisk: baseRaceRisk,
            headConfidence,
            preRaceAnalysis,
            roleCandidates,
            probabilities,
            ranking
          });
          const raceRisk = applyVenueBiasToRisk({
            raceRisk: refinedRaceRisk,
            venueBias
          });
          const aiEnhancement = analyzeHitQuality({
            ranking,
            raceRisk,
            headConfidence,
            partnerSelection,
            oddsData: evData.oddsData,
            probabilities
          });
          const ticketOptimization = optimizeTickets({
            recommendedBets: bet_plan.recommended_bets,
            probabilities,
            oddsData: evData.oddsData,
            recommendation: raceRisk.recommendation,
            raceStructure,
            aiEnhancement
          });
          const marketTrap = detectMarketTraps({
            raceRisk,
            raceStructure,
            raceIndexes,
            recommendedBets: bet_plan.recommended_bets,
            ticketOptimization,
            probabilities
          });
          const rawRaceDecision = decideRaceSelection({
            raceStructure,
            preRaceAnalysis,
            roleCandidates,
            partnerPrecision,
            ticketOptimization,
            headPrecision,
            exhibitionAI,
            venueBias,
            marketTrap,
            raceFlow
          });
          const startSignals = analyzeStartSignals(data.racers, entryMeta, signatureTrendContext);
          const entryAdjustedDecision = applyEntryChangeToDecision(rawRaceDecision, entryMeta);
          const raceDecisionBase = applyStartSignalToDecision(entryAdjustedDecision, startSignals, entryMeta);
          const raceDecision = {
            ...raceDecisionBase,
            factors: {
              ...(raceDecisionBase?.factors || {}),
              formation_pattern_clarity_score: escapePatternAnalysis.formation_pattern_clarity_score,
              formation_pattern: escapePatternAnalysis.formation_pattern,
              escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
              escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence
            }
          };
          const ticketGenerationV2 = generateTicketsV2({
            headSelection: headSelectionRefined,
            partnerSelection,
            headConfidence,
            headPrecision,
            exhibitionAI,
            raceRisk,
            raceIndexes,
            wallEvaluation,
            venueBias,
            marketTrap,
            raceFlow,
            playerStartProfiles: playerStartProfile,
            partnerPrecision
          });

          const mode = String(raceDecision?.mode || raceRisk?.recommendation || "").toUpperCase();
          const confidence = Number(raceDecision?.confidence ?? 0);
          const headStability = Number(raceStructure?.head_stability_score ?? 0);
          const chaosRisk = Number(raceStructure?.chaos_risk_score ?? raceIndexes?.are_index ?? 0);
          const headFixedOk = !!headConfidence?.head_fixed_ok;
          const headFixedConfidence = Number(headConfidence?.head_confidence ?? 0);
          const headGapScore = Number(headPrecision?.head_gap_score ?? 0);
          const top3Concentration = Number(raceStructure?.top3_concentration_score ?? 0);
          const unstableEntryPattern = !!entryMeta?.entry_changed && chaosRisk >= 62;

          const stakeAllocation = buildStakeAllocationPlan({
            raceDecision,
            ticketOptimization,
            betPlan: bet_plan,
            ticketGenerationV2,
            marketTrap,
            valueDetection: detectValue({
              recommendedBets: bet_plan.recommended_bets,
              ticketOptimization,
              raceDecision,
              venueBias,
              marketTrap
            })
          });
          const provisional = dateMode.isFuture || mode !== "FULL_BET";
          const provisionalLabel = provisional ? "暫定" : null;
          const recommendation_score = computeRecommendationScore({
            raceDecision,
            raceStructure,
            startSignals,
            entryMeta,
            race: data.race,
            learningWeights,
            contenderSignals: contenderAdjusted.contenderSignals,
            escapePatternAnalysis,
            scenarioSuggestions,
            ranking
          });
          const recItem = {
            raceId: raceId || `${String(date).replace(/-/g, "")}_${venueId}_${raceNo}`,
            date,
            venueId: data.race.venueId,
            venueName: data.race.venueName || null,
            raceNo: data.race.raceNo,
            mode,
            is_recommended: mode !== "SKIP",
            confidence: Number(confidence.toFixed(2)),
            main_head: Number(headSelectionRefined?.main_head) || null,
            backup_heads: headSelectionRefined?.secondary_heads || [],
            head_win_score: headPrecision?.head_win_score ?? null,
            head_gap_score: headPrecision?.head_gap_score ?? null,
            head_fixed_ok: headFixedOk,
            head_confidence: Number(headFixedConfidence.toFixed(4)),
            top3_concentration_score: Number(top3Concentration.toFixed(2)),
            exhibition_ai_score: exhibitionAI?.exhibition_ai_score ?? null,
            head_stability_score: Number(headStability.toFixed(2)),
            chaos_risk_score: Number(chaosRisk.toFixed(2)),
            provisional,
            provisional_label: provisionalLabel,
            venueBias,
            marketTrap,
            raceFlow,
            predicted_entry_order: entryMeta.predicted_entry_order,
            actual_entry_order: entryMeta.actual_entry_order,
            entry_changed: entryMeta.entry_changed,
            entry_change_type: entryMeta.entry_change_type,
            recommendation_score,
            manualLapImpact,
            manual_lap_applied: false,
            contenderSignals: contenderAdjusted.contenderSignals,
            startSignalAnalysis: startSignals,
            tickets: stakeAllocation.tickets.slice(0, 4).map((t) => ({
              combo: t.combo,
              prob: Number.isFinite(Number(t.prob)) ? Number(t.prob) : null,
              odds: Number.isFinite(Number(t.odds)) ? Number(t.odds) : null,
              ev: Number.isFinite(Number(t.ev)) ? Number(t.ev) : null,
              bet: Number.isFinite(Number(t.recommended_bet)) ? Number(t.recommended_bet) : null,
              ticket_type: t.ticket_type || "backup",
              trap_flags: Array.isArray(t.trap_flags) ? t.trap_flags : [],
              avoid_level: Number.isFinite(Number(t.avoid_level)) ? Number(t.avoid_level) : 0
            })),
            bankrollPlan: stakeAllocation.bankrollPlan,
            raceFlow,
            summary:
              raceDecision?.summary ||
              ticketGenerationV2?.summary ||
              raceRisk?.skip_summary ||
              "本線向き",
            odds: {
              fetched_at: evData?.oddsData?.fetched_at || null,
              fetch_status: evData?.oddsData?.fetch_status || null
            }
          };
          const scenarioSuggestions = buildScenarioSuggestions({
            ranking,
            raceFlow,
            raceIndexes,
            raceDecision,
            entryMeta,
            startSignals,
            ticketOptimization,
            betPlan: bet_plan,
            ticketGenerationV2
          });
          const confidenceScores = buildConfidenceScores({
            raceDecision,
            headConfidence,
            ticketOptimization,
            raceStructure,
            startSignals,
            entryMeta,
            learningWeights,
            ranking,
            preRaceAnalysis,
            contenderSignals: contenderAdjusted.contenderSignals,
            scenarioSuggestions,
            escapePatternAnalysis,
            race: data?.race || null
          });
          const participationDecisionFinal = buildParticipationDecision({
            raceDecision,
            raceRisk,
            raceStructure,
            entryMeta,
            confidenceScores,
            scenarioSuggestions,
            raceFlow,
            escapePatternAnalysis
          });
          const raceExplainability = buildRaceExplainability({
            raceDecision,
            raceRisk,
            raceFlow,
            raceIndexes,
            entryMeta,
            startSignals,
            manualLapImpact,
            headSelection: headSelectionRefined,
            scenarioSuggestions
          });
          recItem.scenario_type = scenarioSuggestions.scenario_type;
          recItem.scenario_confidence = scenarioSuggestions.scenario_confidence;
          recItem.scenarioSuggestions = scenarioSuggestions;
          recItem.explainability = raceExplainability;
          recItem.participationDecision = participationDecisionFinal;
          recItem.participation_reason_tags = participationDecisionFinal.reason_tags || [];
          recItem.confidenceScores = confidenceScores;
          recItem.formation_pattern = escapePatternAnalysis.formation_pattern;
          recItem.escape_pattern_applied = escapePatternAnalysis.escape_pattern_applied;
          recItem.escape_pattern_confidence = escapePatternAnalysis.escape_pattern_confidence;
          recItem.escape_second_place_bias_json = escapePatternAnalysis.escape_second_place_bias_json;
          recItem.main_picks = scenarioSuggestions.main_picks;
          recItem.backup_picks = scenarioSuggestions.backup_picks;
          recItem.longshot_picks = scenarioSuggestions.longshot_picks;
          const ticketExplainability = buildBetExplainability({
            tickets: recItem.tickets,
            bucketByCombo: scenarioSuggestions.bucket_by_combo || {},
            headSelection: headSelectionRefined,
            entryMeta,
            startSignals,
            scenarioSuggestions
          });
          recItem.tickets = recItem.tickets.map((t) => ({
            ...t,
            suggestion_bucket: scenarioSuggestions.bucket_by_combo?.[String(t.combo)] || t.ticket_type || "backup",
            explanation_tags: ticketExplainability[String(t.combo)]?.explanation_tags || [],
            explanation_summary: ticketExplainability[String(t.combo)]?.explanation_summary || null
          }));
          const allowModes = dateMode.isFuture
            ? new Set(["FULL_BET", "SMALL_BET", "MICRO BET"])
            : new Set(["FULL_BET", "SMALL_BET"]);
          const baseRecommendationThreshold = clamp(46, 58, toNum(learningWeights?.recommendation_threshold, 52));
          const recommendationScoreMin = dateMode.isFuture
            ? Math.max(40, baseRecommendationThreshold - 10)
            : Math.max(44, baseRecommendationThreshold - 4);
          const worthBetting =
            allowModes.has(mode) &&
            confidence >= Math.max(44, confidenceMin - 6) &&
            headStability >= Math.max(40, headStabilityMin - 6) &&
            chaosRisk <= Math.max(72, maxChaos + 7) &&
            recommendation_score >= recommendationScoreMin;
          const strongHeadFixed =
            headFixedOk &&
            headFixedConfidence >= (dateMode.isFuture ? 0.56 : 0.62) &&
            headGapScore >= (dateMode.isFuture ? 6 : 9) &&
            top3Concentration >= (dateMode.isFuture ? 45 : 52) &&
            !unstableEntryPattern;

          if (mode === "FULL_BET" || mode === "SMALL_BET" || mode === "MICRO BET") {
            candidatePool.push(recItem);
          }
          const includePrimary =
            participationDecisionFinal.decision === "recommended" &&
            (strongHeadFixed || confidence >= (dateMode.isFuture ? 56 : 62));
          const includeWatch =
            participationDecisionFinal.decision === "watch" &&
            worthBetting &&
            confidence >= (dateMode.isFuture ? 48 : 54);
          if (includePrimary || includeWatch) recs.push(recItem);
        } catch (err) {
          errors.push({
            venueId,
            raceNo,
            message: err?.message || "failed_to_evaluate"
          });
        }
      }
      if (scanned >= maxScan) break;
    }

    const sortByPriority = (a, b) => {
      const modeRank = (x) => (x === "FULL_BET" ? 2 : x === "SMALL_BET" ? 1 : 0);
      const modeDiff = modeRank(b.mode) - modeRank(a.mode);
      if (modeDiff !== 0) return modeDiff;
      const rs = Number(b.recommendation_score || 0) - Number(a.recommendation_score || 0);
      if (rs !== 0) return rs;
      const c = Number(b.confidence || 0) - Number(a.confidence || 0);
      if (c !== 0) return c;
      return Number(b.head_stability_score || 0) - Number(a.head_stability_score || 0);
    };
    recs.sort(sortByPriority);
    candidatePool.sort(sortByPriority);

    const fallbackUsed = recs.length === 0;
    const fallbackRows = fallbackUsed
      ? candidatePool
          .filter(
            (row) =>
              (row.mode === "FULL_BET" || row.mode === "SMALL_BET") &&
              (row.head_fixed_ok || row.head_confidence >= 0.5) &&
              (row.participationDecision?.decision === "recommended" || row.participationDecision?.decision === "watch")
          )
          .slice(0, Math.min(limit, 6))
          .map((row) => ({
            ...row,
            provisional: true,
            provisional_label: "暫定",
            summary: row.summary || "暫定候補"
          }))
      : [];
    const output = (fallbackUsed ? fallbackRows : recs).slice(0, limit);

    return res.json({
      date,
      date_mode: dateMode.mode,
      participation_mode: participationMode || "active",
      scanned,
      returned: output.length,
      recommendations: output,
      used_cached_data: usedCachedCount > 0,
      partial_data: partialData,
      refresh_warnings: refreshWarnings.slice(0, 50),
      refresh_mode: allowRefresh ? "best_effort" : "snapshot_first",
      fallback_used: fallbackUsed,
      fallback_reason: fallbackUsed ? "no_full_bet_candidates" : null,
      skipped_count: Math.max(0, scanned - output.length),
      errors: errors.slice(0, 30)
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/rankings", async (req, res, next) => {
  try {
    const { date, participationMode } = req.query;
    const mode = String(req.query?.mode || "hit_rate").toLowerCase();
    if (!date) {
      return res.status(400).json({
        error: "bad_request",
        message: "date is required query param"
      });
    }

    const limit = Math.max(1, Math.min(50, toInt(req.query?.limit, 20)));
    const dateMode = getDateMode(date);
    const raceNoList = String(req.query?.raceNos || "")
      .split(",")
      .map((v) => toInt(v))
      .filter((v) => Number.isInteger(v) && v >= 1 && v <= 12);
    const venueList = String(req.query?.venues || "")
      .split(",")
      .map((v) => toInt(v))
      .filter((v) => Number.isInteger(v) && v >= 1 && v <= 24);

    const scanRaceNos = raceNoList.length ? raceNoList : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    const scanVenues = venueList.length
      ? venueList
      : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24];
    const scanAll = scanVenues.length * scanRaceNos.length;
    const defaultMaxScan = dateMode.isFuture ? scanAll : 168;
    const maxScan = Math.max(1, Math.min(scanAll, toInt(req.query?.maxScan, defaultMaxScan)));
    const allowRefresh = parseBooleanFlag(req.query?.refresh, false);
    const refreshTimeoutMs = Math.max(800, Math.min(5000, toInt(req.query?.refreshTimeoutMs, 1800)));
    const refreshBudgetMs = Math.max(1500, Math.min(12000, toInt(req.query?.refreshBudgetMs, 5000)));

    const items = [];
    const errors = [];
    const refreshWarnings = [];
    const signatureTrendContext = loadStartSignatureTrendContext();
    const startedAt = Date.now();
    let usedCachedCount = 0;
    let partialData = false;
    let scanned = 0;

    for (const venueId of scanVenues) {
      for (const raceNo of scanRaceNos) {
        if (scanned >= maxScan) break;
        scanned += 1;
        try {
          const canRefresh = allowRefresh && Date.now() - startedAt <= refreshBudgetMs;
          const resolved = await resolveRaceDataForList({
            date,
            venueId,
            raceNo,
            allowRefresh: canRefresh,
            refreshTimeoutMs
          });
          if (resolved.warning) {
            partialData = true;
            refreshWarnings.push({
              venueId,
              raceNo,
              warning: resolved.warning
            });
          }
          if (!resolved.data) continue;
          if (resolved.usedCachedData) usedCachedCount += 1;
          const data = resolved.data;
          const entryMeta = buildEntryOrderMeta(data.racers);
          const baseFeatures = applyMotorPerformanceFeatures(
            applyCoursePerformanceFeatures(buildRaceFeatures(data.racers, data.race))
          );
          const venueAdjustedBase = applyVenueAdjustments(baseFeatures, data.race);
          const preRanking = rankRace(venueAdjustedBase.racersWithFeatures);
          const prePattern = analyzeRacePattern(preRanking);
          const trendFeatures = applyMotorTrendFeatures(venueAdjustedBase.racersWithFeatures, {
            racePattern: prePattern.race_pattern
          });
          const entryAdjusted = applyEntryDynamicsFeatures(trendFeatures, {
            racePattern: prePattern.race_pattern,
            chaos_index: prePattern.indexes.chaos_index
          });
          const rankingBase = rankRace(entryAdjusted.racersWithFeatures);
          const raceId = buildRaceIdFromParts({
            date: data.race?.date,
            venueId: data.race?.venueId,
            raceNo: data.race?.raceNo
          });
          const manualLapEvaluation = raceId ? getManualLapEvaluation(raceId) : null;
          const contenderAdjusted = applyContenderSynergy(rankingBase);
          const rankingBeforePatternBias = contenderAdjusted.ranking;
          const patternBeforeBias = analyzeRacePattern(rankingBeforePatternBias);
          const escapePatternAnalysis = analyzeEscapeFormationLayer({
            ranking: rankingBeforePatternBias,
            racePattern: patternBeforeBias.race_pattern,
            indexes: patternBeforeBias.indexes
          });
          const ranking = applyEscapeFormationBiasToRanking(rankingBeforePatternBias, escapePatternAnalysis, learningWeights, data?.race || null);
          const manualLapImpact = {
            enabled: false,
            applied_lane_count: 0,
            average_adjustment: 0,
            note: "manual_lap_disabled"
          };
          const preRaceAnalysis = analyzePreRaceForm({
            ranking,
            race: data.race
          });
          const pattern = analyzeRacePattern(ranking);
          const adjustedChaos = Math.min(
            100,
            Number(
              (
                pattern.indexes.chaos_index +
                entryAdjusted.chaosBoost +
                (venueAdjustedBase.venue.chaosAdjustment || 0)
              ).toFixed(2)
            )
          );
          const indexes = { ...pattern.indexes, chaos_index: adjustedChaos };
          const raceIndexes = analyzeRaceIndexes({
            ranking,
            top3: ranking.slice(0, 3).map((r) => r.racer.lane),
            racePattern: pattern.race_pattern,
            indexes,
            raceRisk: null
          });
          const exhibitionAI = analyzeExhibitionAI({ ranking });
          const playerStartProfile = analyzePlayerStartProfiles({ ranking });
          const baseRaceRisk = evaluateRaceRisk({
            indexes,
            racePattern: pattern.race_pattern,
            ranking,
            are_index: raceIndexes.are_index,
            probabilities: [],
            participation_mode: participationMode || "active"
          });
          const raceOutcomeProbabilities = estimateRaceOutcomeProbabilities({
            raceIndexes,
            raceRisk: baseRaceRisk,
            racePattern: pattern.race_pattern,
            ranking
          });
          const raceFlow = analyzeRaceFlow({
            ranking,
            raceIndexes,
            racePattern: pattern.race_pattern,
            raceRisk: baseRaceRisk,
            playerStartProfiles: playerStartProfile
          });
          const wallEvaluation = evaluateLane2Wall({
            ranking,
            raceIndexes,
            racePattern: pattern.race_pattern
          });
          const { headSelection, partnerSelection } = analyzeHeadAndPartners({
            ranking,
            raceIndexes,
            raceOutcomeProbabilities,
            raceRisk: baseRaceRisk
          });
          const venueBias = analyzeVenueBias({
            race: data.race,
            raceIndexes,
            ranking
          });
          const headPrecision = evaluateHeadPrecision({
            ranking,
            headSelection,
            probabilities: [],
            raceIndexes,
            raceOutcomeProbabilities,
            exhibitionAI,
            venueBias,
            raceFlow,
            playerStartProfiles: playerStartProfile
          });
          const headSelectionRefined = {
            ...headSelection,
            main_head: headPrecision.main_head ?? headSelection?.main_head ?? null,
            secondary_heads:
              Array.isArray(headPrecision.backup_heads) && headPrecision.backup_heads.length
                ? headPrecision.backup_heads
                : headSelection?.secondary_heads || []
          };
          const headConfidence = evaluateHeadConfidence({
            headSelection: headSelectionRefined,
            raceRisk: baseRaceRisk,
            raceIndexes,
            raceOutcomeProbabilities,
            probabilities: [],
            wallEvaluation,
            ranking
          });
          const partnerPrecision = evaluatePartnerPrecision({
            ranking,
            headSelection: headSelectionRefined,
            raceFlow,
            playerStartProfile
          });
          const roleCandidates = analyzeRoleCandidates({
            ranking,
            headSelection: headSelectionRefined,
            partnerSelection,
            exhibitionAI,
            raceFlow,
            playerStartProfiles: playerStartProfile,
            partnerPrecision
          });
          const baseRaceStructure = analyzeRaceStructure({
            ranking,
            probabilities: [],
            headConfidence,
            raceIndexes,
            preRaceAnalysis,
            roleCandidates,
            exhibitionAI
          });
          const raceStructure = applyVenueBiasToStructure({
            raceStructure: {
              ...baseRaceStructure,
              formation_pattern_clarity_score: escapePatternAnalysis.formation_pattern_clarity_score,
              formation_pattern: escapePatternAnalysis.formation_pattern,
              escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
              escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence
            },
            venueBias
          });
          const refinedRaceRisk = refineRaceRiskWithStructure({
            raceRisk: baseRaceRisk,
            headConfidence,
            preRaceAnalysis,
            roleCandidates,
            probabilities: [],
            ranking
          });
          const raceRisk = applyVenueBiasToRisk({
            raceRisk: refinedRaceRisk,
            venueBias
          });

          let evData = {
            ev_analysis: { best_ev_bets: [] },
            oddsData: { trifecta: [], exacta: [], fetched_at: null, fetch_status: {} }
          };
          try {
            evData = await analyzeExpectedValue({
              date: data.race.date,
              venueId: data.race.venueId,
              raceNo: data.race.raceNo,
              simulation: { top_combinations: [] }
            });
          } catch {
            // keep graceful fallback
          }
          const rawBetPlan = buildBetPlan(evData.ev_analysis, 10000);
          const bet_plan = {
            ...rawBetPlan,
            recommended_bets: enrichRecommendedTickets({
              recommendedBets: rawBetPlan.recommended_bets,
              probabilities: [],
              oddsData: evData.oddsData,
              evAnalysis: evData.ev_analysis
            })
          };

          const aiEnhancement = analyzeHitQuality({
            ranking,
            raceRisk,
            headConfidence,
            partnerSelection,
            oddsData: evData.oddsData,
            probabilities: []
          });
          const ticketOptimization = optimizeTickets({
            recommendedBets: bet_plan.recommended_bets,
            probabilities: [],
            oddsData: evData.oddsData,
            recommendation: raceRisk.recommendation,
            raceStructure,
            aiEnhancement
          });
          const marketTrap = detectMarketTraps({
            raceRisk,
            raceStructure,
            raceIndexes,
            recommendedBets: bet_plan.recommended_bets,
            ticketOptimization,
            probabilities: []
          });
          const rawRaceDecision = decideRaceSelection({
            raceStructure,
            preRaceAnalysis,
            roleCandidates,
            partnerPrecision,
            ticketOptimization,
            headPrecision,
            exhibitionAI,
            venueBias,
            marketTrap,
            raceFlow
          });
          const startSignals = analyzeStartSignals(data.racers, entryMeta, signatureTrendContext);
          const entryAdjustedDecision = applyEntryChangeToDecision(rawRaceDecision, entryMeta);
          const raceDecisionBase = applyStartSignalToDecision(entryAdjustedDecision, startSignals, entryMeta);
          const raceDecision = {
            ...raceDecisionBase,
            factors: {
              ...(raceDecisionBase?.factors || {}),
              formation_pattern_clarity_score: escapePatternAnalysis.formation_pattern_clarity_score,
              formation_pattern: escapePatternAnalysis.formation_pattern,
              escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
              escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence
            }
          };
          const ticketGenerationV2 = generateTicketsV2({
            headSelection: headSelectionRefined,
            partnerSelection,
            headConfidence,
            headPrecision,
            exhibitionAI,
            raceRisk,
            raceIndexes,
            wallEvaluation,
            venueBias,
            marketTrap,
            raceFlow,
            playerStartProfiles: playerStartProfile,
            partnerPrecision
          });
          const valueDetection = detectValue({
            recommendedBets: bet_plan.recommended_bets,
            ticketOptimization,
            raceDecision,
            venueBias,
            marketTrap
          });
          const stakeAllocation = buildStakeAllocationPlan({
            raceDecision,
            ticketOptimization,
            betPlan: bet_plan,
            ticketGenerationV2,
            valueDetection,
            marketTrap
          });

          const recommendation_score = computeRecommendationScore({
            raceDecision,
            raceStructure,
            startSignals,
            entryMeta,
            race: data.race,
            learningWeights,
            contenderSignals: contenderAdjusted.contenderSignals,
            escapePatternAnalysis,
            scenarioSuggestions,
            ranking
          });

          const ranking_score = mode === "hit_rate"
            ? calcHitRateRankingScore({
              raceDecision,
              raceStructure,
              headPrecision,
              valueDetection,
              marketTrap,
              ticketOptimization,
              startSignals,
              recommendationScore: recommendation_score,
              learningWeights
            })
            : calcHitRateRankingScore({
              raceDecision,
              raceStructure,
              headPrecision,
              valueDetection,
              marketTrap,
              ticketOptimization,
              startSignals,
              recommendationScore: recommendation_score,
              learningWeights
            });

          items.push({
            venueId: data.race.venueId,
            venueName: data.race.venueName || null,
            raceNo: data.race.raceNo,
            ranking_score,
            decision_mode: raceDecision?.mode || raceRisk?.recommendation || "UNKNOWN",
            is_recommended:
              String(raceDecision?.mode || raceRisk?.recommendation || "UNKNOWN").toUpperCase() !== "SKIP",
            confidence: Number(toNum(raceDecision?.confidence, 0).toFixed(2)),
            main_head: Number(headSelectionRefined?.main_head) || null,
            provisional:
              dateMode.isFuture || String(raceDecision?.mode || raceRisk?.recommendation || "") !== "FULL_BET",
            provisional_label:
              dateMode.isFuture || String(raceDecision?.mode || raceRisk?.recommendation || "") !== "FULL_BET"
                ? "暫定"
                : null,
            summary: raceDecision?.summary || ticketGenerationV2?.summary || "評価中",
            raceId: raceId || `${String(date).replace(/-/g, "")}_${data.race.venueId}_${data.race.raceNo}`,
            ticket_quality: Number(toNum(ticketOptimization?.ticket_confidence_score, 0).toFixed(2)),
            trap_score: Number(toNum(marketTrap?.trap_score, 0).toFixed(2)),
            value_balance_score: Number(toNum(valueDetection?.value_balance_score, 0).toFixed(2)),
            race_budget: Number(toNum(stakeAllocation?.bankrollPlan?.race_budget, 0)),
            playerStartProfile,
            raceFlow,
            recommendation_score,
            startSignalAnalysis: startSignals,
            predicted_entry_order: entryMeta.predicted_entry_order,
            actual_entry_order: entryMeta.actual_entry_order,
            entry_changed: entryMeta.entry_changed,
            entry_change_type: entryMeta.entry_change_type,
            manualLapImpact,
            manual_lap_applied: false,
            contenderSignals: contenderAdjusted.contenderSignals
          });
        } catch (err) {
          errors.push({
            venueId,
            raceNo,
            message: err?.message || "failed_to_rank"
          });
        }
      }
      if (scanned >= maxScan) break;
    }

    items.sort((a, b) => b.ranking_score - a.ranking_score);
    const rankings = items.slice(0, limit).map((row, idx) => ({
      rank: idx + 1,
      venueId: row.venueId,
      venueName: row.venueName,
      raceNo: row.raceNo,
      ranking_score: row.ranking_score,
      decision_mode: row.decision_mode,
      is_recommended: !!row.is_recommended,
      confidence: row.confidence,
      main_head: row.main_head,
      provisional: !!row.provisional,
      provisional_label: row.provisional_label || null,
      summary: row.summary,
      raceId: row.raceId,
      ticket_quality: row.ticket_quality,
      trap_score: row.trap_score,
      value_balance_score: row.value_balance_score,
      race_budget: row.race_budget,
      recommendation_score: row.recommendation_score,
      predicted_entry_order: row.predicted_entry_order || [],
      actual_entry_order: row.actual_entry_order || [],
      entry_changed: !!row.entry_changed,
      entry_change_type: row.entry_change_type || "none",
      manual_lap_applied: !!row.manual_lap_applied,
      manualLapImpact: row.manualLapImpact || null,
      contenderSignals: row.contenderSignals || null
    }));

    return res.json({
      date,
      mode,
      date_mode: dateMode.mode,
      scanned,
      returned: rankings.length,
      rankings,
      used_cached_data: usedCachedCount > 0,
      partial_data: partialData,
      refresh_warnings: refreshWarnings.slice(0, 50),
      refresh_mode: allowRefresh ? "best_effort" : "snapshot_first",
      errors: errors.slice(0, 40)
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/race/result", async (req, res, next) => {
  try {
    const {
      raceId: bodyRaceId,
      date,
      venueId,
      raceNo,
      finishOrder,
      decisionType,
      payout2t,
      payout3t,
      predictedBets,
      payoutByCombo
    } = req.body || {};

    const top3 = normalizeFinishOrder(finishOrder);
    if (!top3) {
      return res.status(400).json({
        error: "invalid_finish_order",
        message: "finishOrder must be 3 unique lane numbers between 1 and 6"
      });
    }

    const raceId = bodyRaceId || buildRaceIdFromParts({ date, venueId, raceNo });
    if (!raceId) {
      return res.status(400).json({
        error: "invalid_race_id",
        message: "Provide raceId, or date+venueId+raceNo"
      });
    }

    saveRaceResult({
      raceId,
      finishOrder: top3,
      payout2t,
      payout3t,
      decisionType
    });
    saveRaceStartDisplayResult({
      raceId,
      settledResult: top3.join("-")
    });
    const comparison = compareActualTop3VsPredictedBets(top3, predictedBets, {
      payoutByCombo
    });
    attachPredictionFeatureLogSettlement({
      raceId,
      actualResult: top3.join("-"),
      settledBetHitCount: comparison?.summary?.hitCount ?? null,
      settledBetCount: comparison?.summary?.totalBets ?? null
    });

    const settlementUpdate = markSettlementHits({
      raceId,
      actualTop3: top3,
      payoutByCombo
    });

    return res.json({
      ok: true,
      raceId,
      result: {
        finishOrder: top3,
        decisionType: decisionType ?? null,
        payout2t: payout2t ?? null,
        payout3t: payout3t ?? null
      },
      comparison,
      settlementUpdate
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/results/edit", async (req, res, next) => {
  try {
    ensureVerificationLogColumns();
    const raceId = String(req.body?.race_id || req.body?.raceId || "").trim();
    const predictionSnapshotId = Number.isFinite(Number(req.body?.prediction_snapshot_id))
      ? Number(req.body.prediction_snapshot_id)
      : null;
    const confirmedResultRaw = String(req.body?.confirmed_result || "").trim();
    const verificationReason = String(req.body?.verification_reason || req.body?.note || "").trim() || null;
    const invalidReasonInput = String(req.body?.invalid_reason || "").trim() || null;

    if (!raceId) {
      return res.status(400).json({
        ok: false,
        error: "race_id_required",
        message: "race_id is required."
      });
    }

    const confirmedResult = normalizeCombo(confirmedResultRaw);
    const top3 = normalizeFinishOrder(confirmedResult.split("-").map((value) => Number(value)));
    if (!top3 || confirmedResult !== top3.join("-")) {
      return res.status(400).json({
        ok: false,
        error: "invalid_confirmed_result",
        message: "confirmed_result must be in 1-2-3 format using 3 unique lane numbers between 1 and 6."
      });
    }

    const existingResultRow = db
      .prepare(
        `
        SELECT race_id, finish_1, finish_2, finish_3, payout_2t, payout_3t, decision_type
        FROM results
        WHERE race_id = ?
        LIMIT 1
      `
      )
      .get(raceId);

    const previousConfirmedResult = normalizeCombo([
      existingResultRow?.finish_1,
      existingResultRow?.finish_2,
      existingResultRow?.finish_3
    ].join("-")) || null;
    const confirmedResultChanged = previousConfirmedResult !== confirmedResult;

    saveRaceResult({
      raceId,
      finishOrder: top3,
      payout2t: existingResultRow?.payout_2t ?? null,
      payout3t: existingResultRow?.payout_3t ?? null,
      decisionType: existingResultRow?.decision_type ?? null
    });
    saveRaceStartDisplayResult({
      raceId,
      settledResult: confirmedResult
    });

    const payoutByCombo =
      !confirmedResultChanged &&
      Number.isFinite(Number(existingResultRow?.payout_3t)) &&
      Number(existingResultRow?.payout_3t) > 0
        ? { [confirmedResult]: Number(existingResultRow.payout_3t) }
        : {};
    const settlementUpdate = markSettlementHits({
      raceId,
      actualTop3: top3,
      payoutByCombo
    });
    attachPredictionFeatureLogSettlement({
      raceId,
      actualResult: confirmedResult,
      settledBetHitCount: null,
      settledBetCount: null
    });

    let invalidatedVerificationCount = 0;
    let notedVerificationCount = 0;
    if (confirmedResultChanged) {
      const activeVerificationRows = db
        .prepare(
          `
          SELECT id, verification_summary_json
          FROM race_verification_logs
          WHERE race_id = ?
            AND COALESCE(is_invalid_verification, 0) = 0
          ORDER BY id DESC
        `
        )
        .all(raceId);
      const invalidatedAt = new Date().toISOString();
      const invalidReason =
        invalidReasonInput || "Confirmed result edited manually; re-verification required.";
      const invalidateTx = db.transaction((rows) => {
        for (const row of rows) {
          const existingSummary = safeJsonParse(row?.verification_summary_json, {});
          const nextSummary = {
            ...existingSummary,
            verification_reason: verificationReason || existingSummary?.verification_reason || null,
            status_note: verificationReason || existingSummary?.status_note || null,
            invalid_reason: invalidReason,
            invalidated_at: invalidatedAt,
            is_hidden_from_results: false,
            is_invalid_verification: true,
            exclude_from_learning: true,
            learning_ready: false,
            invalidation_source: "result_edit_reverify_required",
            reverify_required: true
          };
          db.prepare(
            `
            UPDATE race_verification_logs
            SET
              is_hidden_from_results = 0,
              is_invalid_verification = 1,
              exclude_from_learning = 1,
              invalid_reason = ?,
              invalidated_at = ?,
              learning_ready = 0,
              verification_reason = ?,
              verification_summary_json = ?
            WHERE id = ?
          `
          ).run(
            invalidReason,
            invalidatedAt,
            verificationReason,
            JSON.stringify(nextSummary),
            Number(row.id)
          );
        }
      });
      invalidateTx(activeVerificationRows);
      invalidatedVerificationCount = activeVerificationRows.length;
    } else if (verificationReason) {
      const latestVerificationRows = db
        .prepare(
          `
          SELECT id, verification_summary_json
          FROM race_verification_logs
          WHERE race_id = ?
            AND COALESCE(is_invalid_verification, 0) = 0
          ORDER BY id DESC
        `
        )
        .all(raceId);
      const noteTx = db.transaction((rows) => {
        for (const row of rows) {
          const existingSummary = safeJsonParse(row?.verification_summary_json, {});
          const nextSummary = {
            ...existingSummary,
            verification_reason: verificationReason,
            status_note: verificationReason
          };
          db.prepare(
            `
            UPDATE race_verification_logs
            SET
              verification_reason = ?,
              verification_summary_json = ?
            WHERE id = ?
          `
          ).run(verificationReason, JSON.stringify(nextSummary), Number(row.id));
        }
      });
      noteTx(latestVerificationRows);
      notedVerificationCount = latestVerificationRows.length;
    }

    return res.json({
      ok: true,
      race_id: raceId,
      prediction_snapshot_id: predictionSnapshotId,
      confirmed_result: confirmedResult,
      previous_confirmed_result: previousConfirmedResult,
      result_changed: confirmedResultChanged,
      reverification_required: confirmedResultChanged,
      invalidated_verification_count: invalidatedVerificationCount,
      noted_verification_count: notedVerificationCount,
      settlement_update: settlementUpdate,
      message: confirmedResultChanged
        ? "Confirmed result updated. Existing verification records were invalidated and re-verification is required."
        : "Confirmed result note updated."
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/placed-bets", async (_req, res, next) => {
  try {
    return res.json({
      items: listPlacedBets()
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/bet-policy", async (req, res, next) => {
  try {
    const envEnabled = parseBooleanFlag(process.env.RECOMMENDATION_ONLY, false);
    const effective = resolveRecommendationOnlyMode(req);
    return res.json({
      recommendation_only_env: envEnabled,
      recommendation_only_effective: effective
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/placed-bets", async (req, res, next) => {
  try {
    const recommendationOnly = resolveRecommendationOnlyMode(req);
    if (Array.isArray(req.body?.items) && req.body.items.length > 0) {
      enforceRecommendationOnlyForBets(req.body.items, recommendationOnly);
      const ids = createPlacedBets(req.body.items);
      return res.status(201).json({
        ok: true,
        ids,
        recommendation_only: recommendationOnly
      });
    }

    enforceRecommendationOnlyForBets(req.body || {}, recommendationOnly);
    const id = createPlacedBet(req.body || {});
    return res.status(201).json({
      ok: true,
      id,
      recommendation_only: recommendationOnly
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.put("/placed-bets/:id", async (req, res, next) => {
  try {
    const changes = updatePlacedBet(req.params.id, req.body || {});
    return res.json({
      ok: true,
      updated: changes
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.delete("/placed-bets/:id", async (req, res, next) => {
  try {
    const changes = deletePlacedBet(req.params.id);
    return res.json({
      ok: true,
      deleted: changes
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/placed-bets/settle", async (req, res, next) => {
  try {
    const result = await settlePlacedBetsForRace(req.body || {});
    const debug = result?.settlement_debug || {
      race_id: result?.race_id || req.body?.race_id || null,
      parsed_race_key: null,
      fetched_result: result?.winning_combo || null,
      placed_bets_found: Number(result?.settled_count || 0),
      matched_bets: Number(result?.hit_count || 0),
      updated_rows: Number(result?.updated_rows || 0),
      settlement_attempted: true
    };
    console.info("[SETTLEMENT][API] success", debug);
    return res.json({
      ok: true,
      ...result,
      settlement_debug: debug
    });
  } catch (err) {
    console.error("[SETTLEMENT][API] failed", {
      message: err?.message,
      code: err?.code,
      debug: err?.debug || null,
      input: req.body || {}
    });
    return next(err);
  }
});

raceRouter.get("/placed-bets/summaries", async (req, res, next) => {
  try {
    const baseDate = req.query?.baseDate;
    return res.json(getPlacedBetSummaries(baseDate));
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/stats", async (_req, res, next) => {
  try {
    const settlementRows = db
      .prepare(
        `
        SELECT race_id, combo, bet_amount, hit_flag, payout, profit_loss
        FROM settlement_logs
      `
      )
      .all();

    const latestPredictionRows = db
      .prepare(
        `
        SELECT pl.race_id, pl.recommendation, pl.ev_analysis_json
        FROM prediction_logs pl
        INNER JOIN (
          SELECT race_id, MAX(id) AS max_id
          FROM prediction_logs
          GROUP BY race_id
        ) latest
          ON latest.max_id = pl.id
      `
      )
      .all();

    const recommendationByRace = new Map();
    const evByRaceCombo = new Map();

    for (const row of latestPredictionRows) {
      const raceId = row.race_id;
      recommendationByRace.set(raceId, normalizeRecommendation(row.recommendation));

      const evAnalysis = safeJsonParse(row.ev_analysis_json, {});
      const bestEvBets = Array.isArray(evAnalysis?.best_ev_bets) ? evAnalysis.best_ev_bets : [];
      const comboToEv = new Map();
      for (const evRow of bestEvBets) {
        if (!evRow?.combo) continue;
        const ev = Number(evRow?.ev);
        if (Number.isFinite(ev)) comboToEv.set(String(evRow.combo), ev);
      }
      evByRaceCombo.set(raceId, comboToEv);
    }

    const overall = initBucket();
    const byRecommendationBuckets = {
      "FULL BET": initBucket(),
      "SMALL BET": initBucket(),
      "MICRO BET": initBucket(),
      SKIP: initBucket()
    };

    for (const row of settlementRows) {
      const raceId = row.race_id;
      const recType = recommendationByRace.get(raceId) || "UNKNOWN";
      const recBucket = byRecommendationBuckets[recType] || null;

      const betAmount = toNum(row.bet_amount);
      const payout = toNum(row.payout);
      const profitLoss = toNum(row.profit_loss);
      const hitFlag = toNum(row.hit_flag) ? 1 : 0;

      overall.raceIds.add(raceId);
      overall.totalBetAmount += betAmount;
      overall.totalBetCount += 1;
      overall.totalPayout += payout;
      overall.totalProfitLoss += profitLoss;
      overall.hitCount += hitFlag;

      if (recBucket) {
        recBucket.raceIds.add(raceId);
        recBucket.totalBetAmount += betAmount;
        recBucket.totalBetCount += 1;
        recBucket.totalPayout += payout;
        recBucket.totalProfitLoss += profitLoss;
        recBucket.hitCount += hitFlag;
      }

      const raceEvMap = evByRaceCombo.get(raceId);
      if (raceEvMap && raceEvMap.has(String(row.combo))) {
        const ev = raceEvMap.get(String(row.combo));
        overall.evSum += ev;
        overall.evCount += 1;
        if (recBucket) {
          recBucket.evSum += ev;
          recBucket.evCount += 1;
        }
      }
    }

    return res.json({
      total_races: overall.raceIds.size,
      total_bets: overall.totalBetAmount,
      hit_rate: pct(overall.hitCount, overall.totalBetCount),
      recovery_rate: pct(overall.totalPayout, overall.totalBetAmount),
      total_profit_loss: overall.totalProfitLoss,
      average_ev_of_placed_bets: overall.evCount
        ? Number((overall.evSum / overall.evCount).toFixed(4))
        : 0,
      by_recommendation_type: {
        "FULL BET": finalizeBucket(byRecommendationBuckets["FULL BET"]),
        "SMALL BET": finalizeBucket(byRecommendationBuckets["SMALL BET"]),
        "MICRO BET": finalizeBucket(byRecommendationBuckets["MICRO BET"]),
        SKIP: finalizeBucket(byRecommendationBuckets.SKIP)
      }
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/analytics", async (req, res, next) => {
  try {
    const baseDate = String(req.query?.date || new Date().toISOString().slice(0, 10));
    const periodSummaries = getPlacedBetSummaries(baseDate);

    const placedRows = db
      .prepare(
        `
        SELECT id, race_id, race_date, venue_id, race_no, source, copied_from_ai, combo, bet_amount, bought_odds, recommended_prob, recommended_ev, recommended_bet, hit_flag, payout, profit_loss
        FROM placed_bets
      `
      )
      .all();

    const raceRows = db
      .prepare(
        `
        SELECT race_id, race_date, venue_id, venue_name, race_no
        FROM races
      `
      )
      .all();
    const raceMap = new Map(raceRows.map((r) => [String(r.race_id), r]));

    const resultsRows = db
      .prepare(
        `
        SELECT race_id, finish_1, finish_2, finish_3, payout_3t
        FROM results
      `
      )
      .all();
    const resultMap = new Map(resultsRows.map((r) => [String(r.race_id), r]));

    const latestPredictionRows = db
      .prepare(
        `
        SELECT pl.race_id, pl.recommendation, pl.top3_json, pl.bet_plan_json, pl.race_decision_json, pl.probabilities_json, pl.created_at
        FROM prediction_logs pl
        INNER JOIN (
          SELECT race_id, MAX(id) AS max_id
          FROM prediction_logs
          GROUP BY race_id
        ) latest
          ON latest.max_id = pl.id
      `
      )
      .all();

    const predictionByRace = new Map();
    for (const row of latestPredictionRows) {
      predictionByRace.set(String(row.race_id), {
        recommendation: normalizeRecommendation(row.recommendation),
        top3: safeJsonParse(row.top3_json, []),
        betPlan: safeJsonParse(row.bet_plan_json, {}),
        raceDecision: safeJsonParse(row.race_decision_json, {}),
        probabilities: safeJsonParse(row.probabilities_json, []),
        created_at: row.created_at
      });
    }

    const settledRows = placedRows.filter((r) => r.hit_flag === 0 || r.hit_flag === 1);
    const totalBet = sumRows(placedRows, "bet_amount");
    const totalPayout = sumRows(placedRows, "payout");
    const totalPL = sumRows(placedRows, "profit_loss");
    const totalHit = settledRows.filter((r) => toNum(r.hit_flag) === 1).length;
    const totalSettled = settledRows.length;

    const total = {
      total_bets: totalBet,
      total_payout: totalPayout,
      total_profit_loss: totalPL,
      settled_count: totalSettled,
      hit_count: totalHit,
      ...calcRates({ bet: totalBet, payout: totalPayout, hit: totalHit, total: totalSettled })
    };

    const calcSourceBucket = (rows) => {
      const bucketRows = Array.isArray(rows) ? rows : [];
      const settled = bucketRows.filter((r) => r.hit_flag === 0 || r.hit_flag === 1);
      const total_stake = sumRows(bucketRows, "bet_amount");
      const total_payout = sumRows(bucketRows, "payout");
      const total_profit_loss = sumRows(bucketRows, "profit_loss");
      const hit_count = settled.filter((r) => toNum(r.hit_flag) === 1).length;
      const number_of_bets = bucketRows.length;
      return {
        number_of_bets,
        total_stake,
        total_payout,
        total_profit_loss,
        hit_rate: pct(hit_count, settled.length),
        roi: pct(total_payout, total_stake)
      };
    };

    const aiRows = placedRows.filter((r) => String(r.source || "ai").toLowerCase() !== "manual");
    const manualRows = placedRows.filter((r) => String(r.source || "ai").toLowerCase() === "manual");
    const copiedManualRows = manualRows.filter((r) => toNum(r.copied_from_ai, 0) === 1);
    const pureManualRows = manualRows.filter((r) => toNum(r.copied_from_ai, 0) !== 1);

    const bet_source_comparison = {
      ai_bets: calcSourceBucket(aiRows),
      manual_bets: calcSourceBucket(manualRows),
      copied_manual_bets: calcSourceBucket(copiedManualRows),
      pure_manual_bets: calcSourceBucket(pureManualRows)
    };

    const byVenueMap = new Map();
    for (const row of placedRows) {
      const race = raceMap.get(String(row.race_id));
      const key = `${row.venue_id}`;
      if (!byVenueMap.has(key)) {
        byVenueMap.set(key, {
          venue_id: toNum(row.venue_id),
          venue_name: race?.venue_name || null,
          total_bets: 0,
          total_payout: 0,
          total_profit_loss: 0,
          hit_count: 0,
          settled_count: 0
        });
      }
      const agg = byVenueMap.get(key);
      agg.total_bets += toNum(row.bet_amount);
      agg.total_payout += toNum(row.payout);
      agg.total_profit_loss += toNum(row.profit_loss);
      if (row.hit_flag === 0 || row.hit_flag === 1) {
        agg.settled_count += 1;
        if (toNum(row.hit_flag) === 1) agg.hit_count += 1;
      }
    }

    const venue_performance = Array.from(byVenueMap.values())
      .map((v) => ({
        ...v,
        ...calcRates({
          bet: v.total_bets,
          payout: v.total_payout,
          hit: v.hit_count,
          total: v.settled_count
        })
      }))
      .sort((a, b) => b.total_profit_loss - a.total_profit_loss);

    const byModeMap = new Map();
    for (const row of placedRows) {
      const pred = predictionByRace.get(String(row.race_id));
      const mode = pred?.raceDecision?.mode || pred?.recommendation || "UNKNOWN";
      if (!byModeMap.has(mode)) {
        byModeMap.set(mode, {
          mode,
          total_bets: 0,
          total_payout: 0,
          total_profit_loss: 0,
          hit_count: 0,
          settled_count: 0
        });
      }
      const agg = byModeMap.get(mode);
      agg.total_bets += toNum(row.bet_amount);
      agg.total_payout += toNum(row.payout);
      agg.total_profit_loss += toNum(row.profit_loss);
      if (row.hit_flag === 0 || row.hit_flag === 1) {
        agg.settled_count += 1;
        if (toNum(row.hit_flag) === 1) agg.hit_count += 1;
      }
    }

    const mode_performance = Array.from(byModeMap.values())
      .map((m) => ({
        ...m,
        ...calcRates({
          bet: m.total_bets,
          payout: m.total_payout,
          hit: m.hit_count,
          total: m.settled_count
        })
      }))
      .sort((a, b) => b.total_profit_loss - a.total_profit_loss);

    let headRaceCount = 0;
    let headHitCount = 0;
    for (const [raceId, pred] of predictionByRace.entries()) {
      const result = resultMap.get(raceId);
      const head = Array.isArray(pred?.top3) ? toNum(pred.top3[0]) : null;
      const actualHead = result ? toNum(result.finish_1) : null;
      if (!Number.isFinite(head) || !Number.isFinite(actualHead) || !actualHead) continue;
      headRaceCount += 1;
      if (head === actualHead) headHitCount += 1;
    }
    const head_prediction = {
      race_count: headRaceCount,
      hit_count: headHitCount,
      success_rate: pct(headHitCount, headRaceCount)
    };

    const recommendation_only = {
      race_count: 0,
      ticket_count: 0,
      total_bets: 0,
      total_payout: 0,
      total_profit_loss: 0,
      hit_count: 0
    };
    const stake_allocation = {
      race_count: 0,
      ticket_count: 0,
      total_bets: 0,
      total_payout: 0,
      total_profit_loss: 0,
      hit_count: 0
    };

    for (const [raceId, pred] of predictionByRace.entries()) {
      const result = resultMap.get(raceId);
      if (!result) continue;
      const actualCombo = `${result.finish_1}-${result.finish_2}-${result.finish_3}`;
      const payout3t = toNum(result.payout_3t, 0);
      const planRows = Array.isArray(pred?.betPlan?.recommended_bets) ? pred.betPlan.recommended_bets : [];
      if (!planRows.length) continue;
      recommendation_only.race_count += 1;
      stake_allocation.race_count += 1;

      for (const row of planRows) {
        const combo = String(row?.combo || "");
        if (!combo) continue;

        recommendation_only.ticket_count += 1;
        recommendation_only.total_bets += 100;
        if (combo === actualCombo && payout3t > 0) {
          recommendation_only.hit_count += 1;
          recommendation_only.total_payout += payout3t;
          recommendation_only.total_profit_loss += payout3t - 100;
        } else {
          recommendation_only.total_profit_loss -= 100;
        }

        const stakeBet = Math.max(100, toNum(row?.recommended_bet ?? row?.bet, 100));
        const units = Math.max(1, Math.floor(stakeBet / 100));
        const payout = combo === actualCombo && payout3t > 0 ? payout3t * units : 0;
        const profitLoss = payout - stakeBet;

        stake_allocation.ticket_count += 1;
        stake_allocation.total_bets += stakeBet;
        stake_allocation.total_payout += payout;
        stake_allocation.total_profit_loss += profitLoss;
        if (combo === actualCombo && payout3t > 0) stake_allocation.hit_count += 1;
      }
    }

    const recommendation_only_performance = {
      ...recommendation_only,
      ...calcRates({
        bet: recommendation_only.total_bets,
        payout: recommendation_only.total_payout,
        hit: recommendation_only.hit_count,
        total: recommendation_only.ticket_count
      })
    };
    const stake_allocation_performance = {
      ...stake_allocation,
      ...calcRates({
        bet: stake_allocation.total_bets,
        payout: stake_allocation.total_payout,
        hit: stake_allocation.hit_count,
        total: stake_allocation.ticket_count
      })
    };

    const weaknessCount = new Map();
    const venueWeakMap = new Map();
    let analyzedRaces = 0;
    let racesWithFailures = 0;
    let headTotal = 0;
    let headHit = 0;
    let partnerTotal = 0;
    let partnerHit = 0;
    let hpHighTotal = 0;
    let hpHighHit = 0;
    let hpMidTotal = 0;
    let hpMidHit = 0;
    let hpLowTotal = 0;
    let hpLowHit = 0;
    let recommendationUsedRaces = 0;
    let stakeUsedTickets = 0;
    let stakeTotalTickets = 0;

    for (const [raceId, pred] of predictionByRace.entries()) {
      const result = resultMap.get(raceId);
      if (!result) continue;
      analyzedRaces += 1;

      const placed = placedRows.filter((r) => String(r.race_id) === raceId);
      const settled = placed.filter((r) => r.hit_flag === 0 || r.hit_flag === 1);
      const hasHit = settled.some((r) => toNum(r.hit_flag) === 1);
      const placedCount = settled.length;
      const avgBoughtOdds =
        placedCount > 0
          ? settled.reduce((a, b) => a + toNum(b.bought_odds), 0) / placedCount
          : 0;

      const predictedHead = toNum(Array.isArray(pred?.top3) ? pred.top3[0] : null, 0);
      const actualHead = toNum(result.finish_1, 0);
      if (predictedHead && actualHead) {
        headTotal += 1;
        if (predictedHead === actualHead) headHit += 1;
      }

      const actualCombo = `${result.finish_1}-${result.finish_2}-${result.finish_3}`;
      const recCombos = new Set(
        (Array.isArray(pred?.betPlan?.recommended_bets) ? pred.betPlan.recommended_bets : [])
          .map((r) => normalizeCombo(r?.combo))
          .filter(Boolean)
      );
      if (recCombos.size > 0 && recCombos.has(actualCombo)) recommendationUsedRaces += 1;

      const hp = toNum(pred?.raceDecision?.factors?.head_precision_score, 50);
      if (hp >= 66) {
        hpHighTotal += 1;
        if (predictedHead && actualHead && predictedHead === actualHead) hpHighHit += 1;
      } else if (hp >= 50) {
        hpMidTotal += 1;
        if (predictedHead && actualHead && predictedHead === actualHead) hpMidHit += 1;
      } else {
        hpLowTotal += 1;
        if (predictedHead && actualHead && predictedHead === actualHead) hpLowHit += 1;
      }

      if (predictedHead && actualHead && predictedHead === actualHead) {
        partnerTotal += 1;
        if (recCombos.has(actualCombo)) partnerHit += 1;
      }

      for (const row of placed) {
        stakeTotalTickets += 1;
        if (toNum(row.recommended_bet) > 0) stakeUsedTickets += 1;
      }

      const probs = (Array.isArray(pred?.probabilities) ? pred.probabilities : [])
        .map((x) => toNum(x?.p ?? x?.prob, 0))
        .filter((x) => x > 0)
        .sort((a, b) => b - a);
      const top3Concentration = (probs[0] || 0) + (probs[1] || 0) + (probs[2] || 0);
      const riskScore = toNum(pred?.raceDecision?.factors?.chaos_risk_score, 50);

      const codes = classifyWeaknessCodes({
        predictedHead,
        actualHead,
        hasHit,
        riskScore,
        placedCount,
        top3Concentration,
        avgBoughtOdds
      });
      if (codes.length) racesWithFailures += 1;
      for (const code of codes) {
        weaknessCount.set(code, toNum(weaknessCount.get(code), 0) + 1);
      }

      const race = raceMap.get(raceId) || {};
      const vk = String(race?.venue_id ?? "unknown");
      if (!venueWeakMap.has(vk)) {
        venueWeakMap.set(vk, {
          venue_id: toNum(race?.venue_id),
          venue_name: race?.venue_name || null,
          races: 0,
          failures: 0,
          weakness_counts: {}
        });
      }
      const va = venueWeakMap.get(vk);
      va.races += 1;
      if (codes.length) va.failures += 1;
      for (const c of codes) {
        va.weakness_counts[c] = toNum(va.weakness_counts[c], 0) + 1;
      }
    }

    const top_failure_modes = Array.from(weaknessCount.entries())
      .map(([code, count]) => ({
        code,
        count,
        rate: analyzedRaces ? Number(((count / analyzedRaces) * 100).toFixed(2)) : 0
      }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 8);

    const venue_weakness_stats = Array.from(venueWeakMap.values())
      .map((v) => {
        const topMode = Object.entries(v.weakness_counts || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || null;
        return {
          ...v,
          failure_rate: v.races ? Number(((v.failures / v.races) * 100).toFixed(2)) : 0,
          top_failure_mode: topMode
        };
      })
      .sort((a, b) => b.failure_rate - a.failure_rate);

    const weakness_analysis = {
      weakness_summary: {
        analyzed_races: analyzedRaces,
        races_with_failures: racesWithFailures,
        failure_rate: analyzedRaces ? Number(((racesWithFailures / analyzedRaces) * 100).toFixed(2)) : 0
      },
      top_failure_modes,
      head_accuracy_stats: {
        total: headTotal,
        hit: headHit,
        rate: pct(headHit, headTotal),
        by_head_precision: {
          high: { total: hpHighTotal, hit: hpHighHit, rate: pct(hpHighHit, hpHighTotal) },
          medium: { total: hpMidTotal, hit: hpMidHit, rate: pct(hpMidHit, hpMidTotal) },
          low: { total: hpLowTotal, hit: hpLowHit, rate: pct(hpLowHit, hpLowTotal) }
        }
      },
      partner_accuracy_stats: {
        total_head_hit_races: partnerTotal,
        partner_hit_races: partnerHit,
        rate: pct(partnerHit, partnerTotal)
      },
      venue_weakness_stats,
      recommendation_usage: {
        analyzed_races: analyzedRaces,
        used_races: recommendationUsedRaces,
        usage_rate: pct(recommendationUsedRaces, analyzedRaces)
      },
      stake_allocation_usage: {
        total_tickets: stakeTotalTickets,
        used_tickets: stakeUsedTickets,
        usage_rate: pct(stakeUsedTickets, stakeTotalTickets)
      }
    };

    return res.json({
      total,
      periods: periodSummaries,
      bet_source_comparison,
      venue_performance,
      mode_performance,
      head_prediction,
      recommendation_only_performance,
      stake_allocation_performance,
      weakness_analysis
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/start-entry-analysis", async (req, res, next) => {
  try {
    const startRows = db
      .prepare(
        `
        SELECT race_id, start_display_signature, start_display_st_json
        FROM race_start_displays
      `
      )
      .all();

    const resultsRows = db
      .prepare(
        `
        SELECT race_id, finish_1, finish_2, finish_3
        FROM results
      `
      )
      .all();
    const resultMap = new Map(resultsRows.map((r) => [String(r.race_id), r]));

    const latestPredictionRows = db
      .prepare(
        `
        SELECT pl.race_id, pl.top3_json, pl.prediction_json
        FROM prediction_logs pl
        INNER JOIN (
          SELECT race_id, MAX(id) AS max_id
          FROM prediction_logs
          GROUP BY race_id
        ) latest
          ON latest.max_id = pl.id
      `
      )
      .all();
    const predictionMap = new Map(
      latestPredictionRows.map((row) => [
        String(row.race_id),
        {
          top3: safeJsonParse(row.top3_json, []),
          prediction: safeJsonParse(row.prediction_json, {})
        }
      ])
    );

    const bySignature = new Map();
    let fastestStRaces = 0;
    let fastestStWins = 0;
    let entryChangedCount = 0;
    const changedFinishCounts = new Map();
    const hitByEntryChanged = {
      true: { races: 0, hits: 0 },
      false: { races: 0, hits: 0 }
    };

    for (const row of startRows) {
      const raceId = String(row.race_id);
      const signature = String(row.start_display_signature || "unknown");
      const result = resultMap.get(raceId);
      const pred = predictionMap.get(raceId);
      const actualCombo = result
        ? `${toInt(result.finish_1, 0)}-${toInt(result.finish_2, 0)}-${toInt(result.finish_3, 0)}`
        : null;
      const predictedTop3 = Array.isArray(pred?.top3) ? pred.top3.map((v) => toInt(v, 0)).filter(Boolean) : [];
      const predictedCombo = predictedTop3.length === 3 ? predictedTop3.join("-") : null;
      const aiHit = !!(actualCombo && predictedCombo && actualCombo === predictedCombo);

      if (!bySignature.has(signature)) {
        bySignature.set(signature, {
          start_display_signature: signature,
          race_count: 0,
          finish_counts: new Map(),
          ai_hits: 0,
          ai_total: 0
        });
      }
      const sig = bySignature.get(signature);
      sig.race_count += 1;
      if (actualCombo) {
        sig.finish_counts.set(actualCombo, toNum(sig.finish_counts.get(actualCombo), 0) + 1);
      }
      if (actualCombo && predictedCombo) {
        sig.ai_total += 1;
        if (aiHit) sig.ai_hits += 1;
      }

      const stMap = safeJsonParse(row.start_display_st_json, {});
      const stEntries = Object.entries(stMap || {})
        .map(([lane, st]) => ({
          lane: toInt(lane, null),
          st: Number(st)
        }))
        .filter((x) => Number.isInteger(x.lane) && Number.isFinite(x.st) && x.st >= 0)
        .sort((a, b) => a.st - b.st);
      const fastest = stEntries[0];
      if (fastest && result) {
        fastestStRaces += 1;
        if (toInt(result.finish_1, 0) === fastest.lane) fastestStWins += 1;
      }

      const predictedEntryOrder = Array.isArray(pred?.prediction?.predicted_entry_order)
        ? pred.prediction.predicted_entry_order
        : [];
      const actualEntryOrder = Array.isArray(pred?.prediction?.actual_entry_order)
        ? pred.prediction.actual_entry_order
        : String(signature || "")
            .split("-")
            .map((v) => toInt(v, null))
            .filter((v) => Number.isInteger(v));
      const inferredEntryChanged =
        predictedEntryOrder.length > 0 &&
        actualEntryOrder.length > 0 &&
        predictedEntryOrder.join("-") !== actualEntryOrder.join("-");
      const entryChanged =
        typeof pred?.prediction?.entry_changed === "boolean" ? pred.prediction.entry_changed : inferredEntryChanged;

      const key = entryChanged ? "true" : "false";
      if (entryChanged) entryChangedCount += 1;
      if (actualCombo && predictedCombo) {
        hitByEntryChanged[key].races += 1;
        if (aiHit) hitByEntryChanged[key].hits += 1;
      }
      if (entryChanged && actualCombo) {
        changedFinishCounts.set(actualCombo, toNum(changedFinishCounts.get(actualCombo), 0) + 1);
      }
    }

    const by_signature = Array.from(bySignature.values())
      .map((s) => {
        const topFinish = Array.from(s.finish_counts.entries()).sort((a, b) => b[1] - a[1])[0] || [null, 0];
        return {
          start_display_signature: s.start_display_signature,
          race_count: s.race_count,
          most_common_finishing_order: topFinish[0],
          most_common_finishing_order_count: topFinish[1],
          ai_hit_rate: s.ai_total ? Number(((s.ai_hits / s.ai_total) * 100).toFixed(2)) : 0,
          ai_hit_sample: s.ai_total
        };
      })
      .sort((a, b) => b.race_count - a.race_count)
      .slice(0, 20);

    const changedTop = Array.from(changedFinishCounts.entries()).sort((a, b) => b[1] - a[1])[0] || [null, 0];
    const hitRateComparison = {
      entry_changed_true: {
        races: hitByEntryChanged.true.races,
        hits: hitByEntryChanged.true.hits,
        hit_rate: pct(hitByEntryChanged.true.hits, hitByEntryChanged.true.races)
      },
      entry_changed_false: {
        races: hitByEntryChanged.false.races,
        hits: hitByEntryChanged.false.hits,
        hit_rate: pct(hitByEntryChanged.false.hits, hitByEntryChanged.false.races)
      }
    };

    return res.json({
      totals: {
        analyzed_races: startRows.length,
        entry_changed_count: entryChangedCount
      },
      by_signature,
      fastest_st_boat_win_rate: {
        races: fastestStRaces,
        wins: fastestStWins,
        win_rate: pct(fastestStWins, fastestStRaces)
      },
      entry_changed_summary: {
        race_count: entryChangedCount,
        most_common_finishing_order: changedTop[0],
        most_common_finishing_order_count: changedTop[1]
      },
      ai_hit_rate_comparison: hitRateComparison
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/learning/latest", async (_req, res, next) => {
  try {
    const auto = parseBooleanFlag(_req.query?.auto, false);
    const autoResult = auto
      ? runContinuousLearningIfNeeded()
      : null;
    return res.json({
      ...getLatestLearningRun(),
      auto_trigger: autoResult
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/learning/batch", async (req, res, next) => {
  try {
    const apply = String(req.body?.apply ?? "0") === "1" || req.body?.apply === true;
    const dryRun = String(req.body?.dryRun ?? "1") !== "0" && req.body?.dryRun !== false;
    const result = applyLearningBatchManually({
      apply,
      dryRun
    });
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/learning/rollback", async (req, res, next) => {
  try {
    const runId = toInt(req.body?.runId, null);
    const result = rollbackLearningWeights({
      runId
    });
    if (!result?.ok) {
      return res.status(400).json({
        error: "rollback_failed",
        message: result?.message || "rollback failed"
      });
    }
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/self-learning", async (req, res, next) => {
  try {
    const mode = String(req.query?.mode || "proposal_only");
    const saveSnapshot = String(req.query?.save || "1") !== "0";
    const snapshotDate = String(req.query?.date || new Date().toISOString().slice(0, 10));

    const verifiedLearningRows = buildVerifiedLearningRows();
    const latestPredictionRows = verifiedLearningRows.map((row) => ({
      race_id: row.race_id,
      recommendation: row.recommendation_mode,
      prediction: row.snapshot?.prediction || {},
      raceDecision: row.snapshot?.raceDecision || {},
      betPlan: row.snapshot?.betPlan || {}
    }));

    const resultRows = verifiedLearningRows
      .map((row) => {
        const combo = String(row?.confirmed_result || "");
        const parts = combo.split("-").map((v) => Number(v)).filter((v) => Number.isInteger(v));
        if (parts.length !== 3) return null;
        return {
          race_id: row.race_id,
          finish_1: parts[0],
          finish_2: parts[1],
          finish_3: parts[2],
          payout_3t: null
        };
      })
      .filter(Boolean);

    const placedRows = db
      .prepare(
        `
        SELECT race_id, venue_id, bet_amount, hit_flag, payout, profit_loss
        FROM placed_bets
      `
      )
      .all();

    const raceRows = db
      .prepare(
        `
        SELECT race_id, venue_id, venue_name
        FROM races
      `
      )
      .all();

    const selfLearning = runSelfLearning({
      predictionRows: latestPredictionRows,
      resultRows,
      placedRows,
      raceRows,
      mode
    });

    db.exec(`
      CREATE TABLE IF NOT EXISTS self_learning_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        snapshot_date TEXT NOT NULL,
        sample_size INTEGER NOT NULL,
        mode TEXT NOT NULL DEFAULT 'proposal_only',
        current_weights_json TEXT NOT NULL,
        suggested_weights_json TEXT NOT NULL,
        applied_weights_json TEXT,
        summary TEXT
      )
    `);

    if (saveSnapshot) {
      db.prepare(
        `
        INSERT INTO self_learning_snapshots (
          snapshot_date,
          sample_size,
          mode,
          current_weights_json,
          suggested_weights_json,
          applied_weights_json,
          summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `
      ).run(
        snapshotDate,
        Number(selfLearning.sample_size || 0),
        String(selfLearning.mode || "proposal_only"),
        JSON.stringify(selfLearning.current_weights || {}),
        JSON.stringify(selfLearning.suggested_weights || {}),
        JSON.stringify({}),
        String(selfLearning.summary || "")
      );
    }

    const snapshots = db
      .prepare(
        `
        SELECT id, created_at, snapshot_date, sample_size, mode, current_weights_json, suggested_weights_json, applied_weights_json, summary
        FROM self_learning_snapshots
        ORDER BY id DESC
        LIMIT 20
      `
      )
      .all()
      .map((row) => ({
        id: row.id,
        created_at: row.created_at,
        date: row.snapshot_date,
        sample_size: row.sample_size,
        mode: row.mode,
        current_weights: safeJsonParse(row.current_weights_json, {}),
        suggested_weights: safeJsonParse(row.suggested_weights_json, {}),
        applied_weights: safeJsonParse(row.applied_weights_json, {}),
        summary: row.summary
      }));

    return res.json({
      selfLearning,
      snapshots
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/logs", async (req, res, next) => {
  try {
    const limitRaw = Number(req.query?.limit);
    const limit = Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 300) : 100;

    const latestPredictionRows = db
      .prepare(
        `
        SELECT pl.race_id, pl.created_at, pl.recommendation, pl.risk_score, pl.race_decision_json, pl.bet_plan_json
        FROM prediction_logs pl
        INNER JOIN (
          SELECT race_id, MAX(id) AS max_id
          FROM prediction_logs
          GROUP BY race_id
        ) latest
          ON latest.max_id = pl.id
        ORDER BY pl.id DESC
        LIMIT ?
      `
      )
      .all(limit);

    const raceRows = db
      .prepare(
        `
        SELECT race_id, race_date, venue_id, venue_name, race_no
        FROM races
      `
      )
      .all();
    const raceMap = new Map(raceRows.map((r) => [String(r.race_id), r]));

    const resultRows = db
      .prepare(
        `
        SELECT race_id, finish_1, finish_2, finish_3
        FROM results
      `
      )
      .all();
    const resultMap = new Map(resultRows.map((r) => [String(r.race_id), r]));

    const placedRows = db
      .prepare(
        `
        SELECT race_id, combo, bet_amount, bought_odds, recommended_prob, recommended_ev, recommended_bet, hit_flag, payout, profit_loss
        FROM placed_bets
      `
      )
      .all();
    const placedByRace = new Map();
    for (const row of placedRows) {
      const key = String(row.race_id);
      if (!placedByRace.has(key)) placedByRace.set(key, []);
      placedByRace.get(key).push(row);
    }

    const items = latestPredictionRows.map((row) => {
      const raceId = String(row.race_id);
      const race = raceMap.get(raceId) || {};
      const result = resultMap.get(raceId) || {};
      const raceDecision = safeJsonParse(row.race_decision_json, {});
      const betPlan = safeJsonParse(row.bet_plan_json, {});
      const recommendedTickets = Array.isArray(betPlan?.recommended_bets) ? betPlan.recommended_bets : [];
      const actualPlacedTickets = placedByRace.get(raceId) || [];

      const totalActualBet = actualPlacedTickets.reduce((a, b) => a + toNum(b.bet_amount), 0);
      const totalActualPayout = actualPlacedTickets.reduce((a, b) => a + toNum(b.payout), 0);
      const totalActualPL = actualPlacedTickets.reduce((a, b) => a + toNum(b.profit_loss), 0);
      const hitCount = actualPlacedTickets.filter((b) => toNum(b.hit_flag) === 1).length;
      const missCount = actualPlacedTickets.filter((b) => toNum(b.hit_flag) === 0).length;

      return {
        race_id: raceId,
        date: race.race_date ?? null,
        venueId: race.venue_id ?? null,
        venueName: race.venue_name ?? null,
        raceNo: race.race_no ?? null,
        raceDecision: {
          mode: raceDecision?.mode || row.recommendation || null,
          confidence: raceDecision?.confidence ?? null,
          risk_score: row.risk_score ?? null
        },
        recommended_tickets: recommendedTickets.map((t) => ({
          combo: t.combo,
          recommended_bet: t.recommended_bet ?? t.bet ?? null,
          odds: t.odds ?? null,
          ev: t.ev ?? null,
          probability: t.prob ?? null,
          ticket_type: t.ticket_type ?? null
        })),
        actual_placed_tickets: actualPlacedTickets.map((t) => ({
          combo: t.combo,
          actual_bet_amount: t.bet_amount,
          recommended_bet_amount: t.recommended_bet,
          odds: t.bought_odds,
          recommended_ev: t.recommended_ev,
          recommended_prob: t.recommended_prob,
          hit_flag: t.hit_flag,
          payout: t.payout,
          profit_loss: t.profit_loss
        })),
        actual_result: [
          result?.finish_1 ?? null,
          result?.finish_2 ?? null,
          result?.finish_3 ?? null
        ],
        hit_count: hitCount,
        miss_count: missCount,
        total_actual_bet: totalActualBet,
        total_payout: totalActualPayout,
        total_profit_loss: totalActualPL,
        logged_at: row.created_at
      };
    });

    return res.json({
      items
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/results-history", async (req, res, next) => {
  try {
    const limitRaw = Number(req.query?.limit);
    const limit = Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 1000) : 300;
    const statusFilter = String(req.query?.status || "all").toLowerCase();
    const includeInvalidated = parseBooleanFlag(req.query?.include_invalidated, false);

    const predictionSnapshots = listPredictionSnapshots({ limit }).map(mapPredictionSnapshotRow);

    const resultsRows = db
      .prepare(
        `
        SELECT race_id, finish_1, finish_2, finish_3
        FROM results
      `
      )
      .all();

    const settlementRows = db
      .prepare(
        `
        SELECT race_id, combo, bet_amount, hit_flag, payout, profit_loss
        FROM settlement_logs
      `
      )
      .all();

    const raceRows = db
      .prepare(
        `
        SELECT race_id, race_date, venue_id, race_no, venue_name
        FROM races
      `
      )
      .all();
    const startDisplayRows = db
      .prepare(
        `
        SELECT
          race_id,
          start_display_order_json,
          start_display_st_json,
          start_display_positions_json,
          start_display_signature,
          start_display_timing_json,
          start_display_raw_json,
          start_display_layout_mode,
          start_display_source,
          source_fetched_at,
          prediction_snapshot_json,
          fetched_result,
          settled_result
        FROM race_start_displays
      `
      )
      .all();
    const featureEventRows = db
      .prepare(
        `
        SELECT e.*
        FROM prediction_feature_log_events e
        INNER JOIN (
          SELECT race_id, MAX(id) AS max_id
          FROM prediction_feature_log_events
          GROUP BY race_id
        ) latest
          ON latest.max_id = e.id
      `
      )
      .all();
    const featureSnapshotRows = db
      .prepare(
        `
        SELECT race_id, COUNT(*) AS feature_snapshot_count, MAX(id) AS latest_feature_snapshot_id
        FROM feature_snapshots
        GROUP BY race_id
      `
      )
      .all();
    const verificationRows = listLatestVerificationRecords({ includeInvalidated: includeInvalidated });

    const resultMap = new Map(resultsRows.map((r) => [r.race_id, r]));
    const raceMap = new Map(raceRows.map((r) => [r.race_id, r]));
    const startDisplayMap = new Map(
      startDisplayRows.map((row) => [
        row.race_id,
        {
          start_display_order: safeJsonParse(row.start_display_order_json, []),
          start_display_st: safeJsonParse(row.start_display_st_json, {}),
          start_display_positions: safeJsonParse(row.start_display_positions_json, []),
          start_display_signature: row.start_display_signature || null,
          start_display_timing: safeJsonParse(row.start_display_timing_json, {}),
          start_display_raw: safeJsonParse(row.start_display_raw_json, {}),
          start_display_layout_mode: row.start_display_layout_mode || null,
          start_display_source: row.start_display_source || null,
          source_fetched_at: row.source_fetched_at || null,
          prediction_snapshot: safeJsonParse(row.prediction_snapshot_json, {}),
          fetched_result: row.fetched_result || null,
          settled_result: row.settled_result || null
        }
      ])
    );
    const featureEventMap = new Map(
      featureEventRows.map((row) => [
        String(row.race_id),
        {
          start_display_order: safeJsonParse(row.start_display_order_json, []),
          start_display_st: safeJsonParse(row.start_display_st_json, {}),
          start_display_timing: safeJsonParse(row.start_display_timing_json, {}),
          start_display_raw: safeJsonParse(row.start_display_raw_json, {}),
          start_display_signature: row.start_display_signature || null,
          source_fetched_at: row.source_timestamp || null,
          start_display_source: "prediction_snapshot_event",
          prediction_snapshot: safeJsonParse(row.prediction_snapshot_json, {}),
          predicted_entry_order: safeJsonParse(row.predicted_entry_order_json, []),
          actual_entry_order: safeJsonParse(row.actual_entry_order_json, [])
        }
      ])
    );
    const featureSnapshotMetaMap = new Map(
      featureSnapshotRows.map((row) => [
        String(row.race_id),
        {
          feature_snapshot_count: toNum(row.feature_snapshot_count),
          latest_feature_snapshot_id: toNum(row.latest_feature_snapshot_id, null)
        }
      ])
    );
    const settlementByRace = new Map();
    for (const row of settlementRows) {
      const list = settlementByRace.get(row.race_id) || [];
      list.push(row);
      settlementByRace.set(row.race_id, list);
    }
    const latestSnapshotIdByRace = new Map();
    for (const snapshotRow of predictionSnapshots) {
      if (!latestSnapshotIdByRace.has(snapshotRow.race_id) && Number.isFinite(Number(snapshotRow.id))) {
        latestSnapshotIdByRace.set(snapshotRow.race_id, Number(snapshotRow.id));
      }
    }
    const verificationBySnapshotId = new Map();
    const verificationByRaceFallback = new Map();
    for (const row of verificationRows) {
      const summary = row.summary && typeof row.summary === "object"
        ? row.summary
        : safeJsonParse(row.verification_summary_json, {});
      const snapshotId = Number.isFinite(Number(row?.verified_against_snapshot_id))
        ? Number(row.verified_against_snapshot_id)
        : Number.isFinite(Number(row?.prediction_snapshot_id))
          ? Number(row.prediction_snapshot_id)
          : Number.isFinite(Number(summary?.verified_against_snapshot_id))
            ? Number(summary.verified_against_snapshot_id)
            : Number.isFinite(Number(summary?.prediction_snapshot_id))
              ? Number(summary.prediction_snapshot_id)
              : null;
      const normalizedVerification = {
        verified_at: row.verified_at || null,
        hit_miss: row.hit_miss || null,
        mismatch_categories: safeJsonParse(row.mismatch_categories_json, []),
        prediction_snapshot_id: Number.isFinite(Number(row?.prediction_snapshot_id))
          ? Number(row.prediction_snapshot_id)
          : Number.isFinite(Number(summary?.prediction_snapshot_id))
            ? Number(summary.prediction_snapshot_id)
            : null,
        verified_against_snapshot_id: Number.isFinite(Number(row?.verified_against_snapshot_id))
          ? Number(row.verified_against_snapshot_id)
          : Number.isFinite(Number(summary?.verified_against_snapshot_id))
            ? Number(summary.verified_against_snapshot_id)
            : null,
        verification_status: row?.verification_status || summary?.verification_status || null,
        verification_reason: row?.verification_reason || summary?.verification_reason || null,
        summary
      };
      if (Number.isFinite(snapshotId) && !verificationBySnapshotId.has(snapshotId)) {
        verificationBySnapshotId.set(snapshotId, normalizedVerification);
        continue;
      }
      const raceVerificationKey = String(row?.race_id || "");
      if (raceVerificationKey && !verificationByRaceFallback.has(raceVerificationKey)) {
        verificationByRaceFallback.set(raceVerificationKey, normalizedVerification);
      }
    }

    const items = predictionSnapshots.map((snapshotRow) => {
      const logRow = snapshotRow.row;
      const raceId = snapshotRow.race_id;
      const prediction = snapshotRow.prediction;
      const betPlan = snapshotRow.betPlan;
      const race = raceMap.get(raceId) || {};
      const result = resultMap.get(raceId) || null;
      const settlements = settlementByRace.get(raceId) || [];
      const mutableStartDisplay = startDisplayMap.get(raceId) || null;
      const eventStartDisplay = featureEventMap.get(raceId) || null;
      const featureSnapshotMeta = featureSnapshotMetaMap.get(raceId) || null;
      const snapshotStartDisplay =
        prediction?.snapshot_context?.start_display && typeof prediction.snapshot_context.start_display === "object"
          ? prediction.snapshot_context.start_display
          : null;
      const hasStartDisplaySource = !!mutableStartDisplay || !!eventStartDisplay;
      const startDisplay = snapshotStartDisplay || (hasStartDisplaySource
        ? {
            ...(mutableStartDisplay || {}),
            ...(eventStartDisplay || {}),
            start_display_order:
              (Array.isArray(eventStartDisplay?.start_display_order) && eventStartDisplay.start_display_order.length
                ? eventStartDisplay.start_display_order
                : Array.isArray(mutableStartDisplay?.start_display_order)
                  ? mutableStartDisplay.start_display_order
                  : []),
            start_display_st:
              (eventStartDisplay?.start_display_st && Object.keys(eventStartDisplay.start_display_st).length
                ? eventStartDisplay.start_display_st
                : mutableStartDisplay?.start_display_st || {}),
            start_display_timing:
              (eventStartDisplay?.start_display_timing && Object.keys(eventStartDisplay.start_display_timing).length
                ? eventStartDisplay.start_display_timing
                : mutableStartDisplay?.start_display_timing || {}),
            source_fetched_at:
              eventStartDisplay?.source_fetched_at || mutableStartDisplay?.source_fetched_at || null,
            prediction_snapshot:
              eventStartDisplay?.prediction_snapshot || mutableStartDisplay?.prediction_snapshot || {}
          }
        : null);
      const currentPredictionSnapshotId = Number.isFinite(Number(snapshotRow.id))
        ? Number(snapshotRow.id)
        : null;
      const verification =
        (Number.isFinite(currentPredictionSnapshotId)
          ? verificationBySnapshotId.get(currentPredictionSnapshotId)
          : null) ||
        (latestSnapshotIdByRace.get(raceId) === currentPredictionSnapshotId
          ? verificationByRaceFallback.get(raceId) || null
          : null);
      const verificationSummary = verification?.summary && typeof verification.summary === "object"
        ? verification.summary
        : {};
      const predictedEntryOrder = Array.isArray(prediction?.predicted_entry_order)
        ? prediction.predicted_entry_order
        : [];
      const actualEntryOrder = Array.isArray(prediction?.actual_entry_order)
        ? prediction.actual_entry_order
        : Array.isArray(startDisplay?.start_display_order)
          ? startDisplay.start_display_order
          : [];
      const entryChanged = typeof prediction?.entry_changed === "boolean"
        ? prediction.entry_changed
        : predictedEntryOrder.length > 0 &&
          actualEntryOrder.length > 0 &&
          predictedEntryOrder.join("-") !== actualEntryOrder.join("-");
      const entryChangeType = prediction?.entry_change_type || (entryChanged ? "minor_shift" : "none");

      const predictedTop3 = Array.isArray(prediction?.top3) ? prediction.top3.slice(0, 3) : [];
      const actualTop3 = result
        ? [result.finish_1, result.finish_2, result.finish_3].filter((v) => Number.isFinite(Number(v)))
        : [];

      const latestLogDisplayBets = Array.isArray(betPlan?.recommended_bets) ? betPlan.recommended_bets : [];
      const finalRecommendedFromPrediction = normalizeSavedBetSnapshotItems(prediction?.final_recommended_bets_snapshot);
      const legacyDisplaySnapshotFromPrediction = normalizeSavedBetSnapshotItems(prediction?.ai_bets_display_snapshot);
      const savedFinalRecommendedBetsSnapshot = finalRecommendedFromPrediction.length > 0
        ? finalRecommendedFromPrediction
        : legacyDisplaySnapshotFromPrediction;
      const snapshotFullFromPrediction =
        prediction?.ai_bets_full_snapshot && typeof prediction.ai_bets_full_snapshot === "object"
          ? prediction.ai_bets_full_snapshot
          : {
              recommended_bets: latestLogDisplayBets,
              optimized_tickets: [],
              ticket_generation_v2: { primary_tickets: [], secondary_tickets: [] },
              scenario_suggestions: { main_picks: [], backup_picks: [], longshot_picks: [] }
            };
      const finalRecommendedFromVerification = normalizeSavedBetSnapshotItems(verificationSummary?.final_recommended_bets_snapshot);
      const legacyDisplaySnapshotFromVerification = normalizeSavedBetSnapshotItems(verificationSummary?.ai_bets_display_snapshot);
      const aiBetsDisplaySnapshot = savedFinalRecommendedBetsSnapshot;
      const verificationDisplaySnapshot = finalRecommendedFromVerification.length > 0
        ? finalRecommendedFromVerification
        : legacyDisplaySnapshotFromVerification;
      const aiBetsFullSnapshot =
        verificationSummary?.ai_bets_full_snapshot && typeof verificationSummary.ai_bets_full_snapshot === "object"
          ? verificationSummary.ai_bets_full_snapshot
          : snapshotFullFromPrediction;
      const snapshotCreatedAt =
        verificationSummary?.snapshot_created_at ||
        prediction?.snapshot_created_at ||
        snapshotRow.prediction_timestamp ||
        logRow.created_at ||
        null;
      const predictionSnapshotId = currentPredictionSnapshotId ??
        (Number.isFinite(Number(verificationSummary?.prediction_snapshot_id))
          ? Number(verificationSummary.prediction_snapshot_id)
          : null);
      const aiBetsSnapshotSource = finalRecommendedFromPrediction.length > 0
        ? (prediction?.final_recommended_bets_snapshot_source || "prediction_final_recommended_bets_snapshot")
        : legacyDisplaySnapshotFromPrediction.length > 0
          ? "prediction_snapshot_legacy_display"
          : "missing_final_recommended_bets_snapshot";
      const predictedCombo = predictedTop3.length === 3 ? predictedTop3.join("-") : null;
      const actualCombo = actualTop3.length === 3 ? actualTop3.join("-") : null;
      const verificationAgainstSnapshotId = Number.isFinite(Number(verification?.verified_against_snapshot_id))
        ? Number(verification.verified_against_snapshot_id)
        : Number.isFinite(Number(verificationSummary?.verified_against_snapshot_id))
          ? Number(verificationSummary.verified_against_snapshot_id)
          : Number.isFinite(Number(verificationSummary?.prediction_snapshot_id))
            ? Number(verificationSummary.prediction_snapshot_id)
            : null;
      const invalidatedVerification =
        Number(verification?.is_invalid_verification) === 1 ||
        Number(verification?.is_hidden_from_results) === 1 ||
        Number(verification?.exclude_from_learning) === 1;
      const displaySnapshotCombos = (Array.isArray(aiBetsDisplaySnapshot) ? aiBetsDisplaySnapshot : [])
        .map((row) => normalizeCombo(row?.combo ?? row))
        .filter((c) => c && c.split("-").length === 3);
      const hasValidBetSnapshot = displaySnapshotCombos.length > 0;
      const verificationBetSource = String(verificationSummary?.verification_bet_source || "").toLowerCase();
      const verificationUsedSavedBetSnapshot =
        verificationBetSource === "prediction_snapshot" ||
        verificationBetSource === "prediction_snapshot_legacy_display";
      const verificationSnapshotOutdated =
        hasValidBetSnapshot &&
        !!verification &&
        Number.isFinite(currentPredictionSnapshotId) &&
        Number.isFinite(verificationAgainstSnapshotId) &&
        currentPredictionSnapshotId !== verificationAgainstSnapshotId;
      const computedHitMiss = !hasValidBetSnapshot
        ? "NOT_VERIFIABLE"
        : actualCombo
          ? (displaySnapshotCombos.includes(actualCombo) ? "HIT" : "MISS")
          : "PENDING";
      const persistedHitMiss = String(verification?.hit_miss || "").toUpperCase();
      const recoveredSnapshotNeedsReverify =
        hasValidBetSnapshot &&
        !!verification &&
        (
          !verificationUsedSavedBetSnapshot ||
          verificationSnapshotOutdated ||
          String(verificationSummary?.verification_status || "").toUpperCase() === "NO_BET_SNAPSHOT"
        );
      const hitMiss = invalidatedVerification
        ? "INVALIDATED"
        : recoveredSnapshotNeedsReverify
          ? computedHitMiss
          : (persistedHitMiss || computedHitMiss);
      const confirmedResult =
        actualCombo ||
        (startDisplay?.settled_result ? normalizeCombo(startDisplay.settled_result) : null) ||
        (startDisplay?.fetched_result ? normalizeCombo(startDisplay.fetched_result) : null) ||
        null;
      const summaryStatus = String(verification?.summary?.verification_status || "").toUpperCase();
      const legacyFallbackVerified =
        !!verification?.summary?.warning &&
        String(verification.summary.warning).includes("fallback verification used predicted top3");
      const verificationStatus = invalidatedVerification
        ? "INVALIDATED"
        : summaryStatus || (!hasValidBetSnapshot
        ? "NO_BET_SNAPSHOT"
        : recoveredSnapshotNeedsReverify
          ? (confirmedResult ? "UNVERIFIED" : "NO_CONFIRMED_RESULT")
        : verification?.verified_at && !legacyFallbackVerified
          ? (hitMiss === "HIT" ? "VERIFIED_HIT" : hitMiss === "MISS" ? "VERIFIED_MISS" : "VERIFIED")
          : confirmedResult
            ? "UNVERIFIED"
            : "NO_CONFIRMED_RESULT");
      const verificationReason = !hasValidBetSnapshot
        ? "No final recommended bet snapshot saved."
        : recoveredSnapshotNeedsReverify
          ? verificationSnapshotOutdated
            ? "A newer or better prediction snapshot is available. Re-run verification to persist updated HIT/MISS."
            : "Recovered final recommended bet snapshot from historical prediction storage. Re-run verification to persist updated HIT/MISS."
        : legacyFallbackVerified
          ? "Legacy verification used top3 fallback; re-verify after snapshot is available."
          : null;

      const totals = settlements.reduce(
        (acc, s) => {
          acc.bet_amount += toNum(s.bet_amount);
          acc.payout += toNum(s.payout);
          acc.profit_loss += toNum(s.profit_loss);
          if (toNum(s.hit_flag)) acc.hit_count += 1;
          acc.bet_count += 1;
          return acc;
        },
        { bet_amount: 0, payout: 0, profit_loss: 0, hit_count: 0, bet_count: 0 }
      );

      return {
        race_id: raceId,
        history_id: predictionSnapshotId || `${raceId}-${snapshotCreatedAt || logRow.created_at || ""}`,
        race_date: snapshotRow.race_date ?? race.race_date ?? verification?.summary?.race_date ?? null,
        venue_id: snapshotRow.venue_code ?? race.venue_id ?? verification?.summary?.venue_code ?? null,
        venue_name: snapshotRow.venue_name ?? race.venue_name ?? verification?.summary?.venue_name ?? null,
        race_no: snapshotRow.race_no ?? race.race_no ?? verification?.summary?.race_no ?? null,
        recommendation: normalizeRecommendation(logRow.recommendation || snapshotRow.raceDecision?.mode),
        predicted_entry_order: predictedEntryOrder,
        actual_entry_order: actualEntryOrder,
        entry_changed: entryChanged,
        entry_change_type: entryChangeType,
        prediction_before_entry_change: prediction?.prediction_before_entry_change || null,
        prediction_after_entry_change: prediction?.prediction_after_entry_change || null,
        predicted_top3: predictedTop3,
        actual_top3: actualTop3,
        confirmed_result: confirmedResult,
        hit_miss: hitMiss,
        verification_status: verificationStatus,
        verification_reason: verificationReason,
        totals,
        bets: settlements.map((s) => ({
          combo: s.combo,
          bet_amount: toNum(s.bet_amount),
          hit_flag: toNum(s.hit_flag) ? 1 : 0,
          payout: toNum(s.payout),
          profit_loss: toNum(s.profit_loss)
        })),
        startDisplay,
        verification,
        invalidation: invalidatedVerification
          ? {
              is_hidden_from_results: Number(verification?.is_hidden_from_results) === 1,
              is_invalid_verification: Number(verification?.is_invalid_verification) === 1,
              exclude_from_learning: Number(verification?.exclude_from_learning) === 1,
              invalid_reason: verification?.invalid_reason || verificationSummary?.invalid_reason || null,
              invalidated_at: verification?.invalidated_at || verificationSummary?.invalidated_at || null
            }
          : null,
        prediction_snapshot_id: predictionSnapshotId,
        snapshot_created_at: snapshotCreatedAt,
        ai_bets_snapshot_source: aiBetsSnapshotSource,
        ai_bets_full_snapshot: aiBetsFullSnapshot,
        ai_bets_display_snapshot: aiBetsDisplaySnapshot,
        final_recommended_bets_snapshot: aiBetsDisplaySnapshot,
        final_recommended_bets_count: Array.isArray(aiBetsDisplaySnapshot) ? aiBetsDisplaySnapshot.length : 0,
        ai_bets_latest_log: latestLogDisplayBets,
        debug_bet_compare: {
          confirmed_result: confirmedResult,
          saved_display_snapshot: savedFinalRecommendedBetsSnapshot,
          displayed_in_results: aiBetsDisplaySnapshot,
          verification_display_snapshot: verificationDisplaySnapshot,
          verification_input_bet_list: displaySnapshotCombos,
          latest_log_bets: latestLogDisplayBets,
          final_hit_miss_result: hitMiss
        },
        feature_snapshot_debug: {
          feature_snapshot_exists: !!featureSnapshotMeta?.feature_snapshot_count,
          feature_snapshot_count: toNum(featureSnapshotMeta?.feature_snapshot_count, 0),
          latest_feature_snapshot_id: toNum(featureSnapshotMeta?.latest_feature_snapshot_id, null),
          saved_feature_families: Object.keys(
            prediction?.learning_context?.feature_contribution_summary && typeof prediction.learning_context.feature_contribution_summary === "object"
              ? prediction.learning_context.feature_contribution_summary
              : {}
          ),
          contribution_data_exists: !!(
            prediction?.learning_context?.feature_contribution_summary &&
            Object.keys(prediction.learning_context.feature_contribution_summary).length
          ),
          segment_corrections_used_count: toNum(prediction?.learning_context?.segment_corrections_used?.segment_count, 0),
          confidence_calibration_applied: toNum(prediction?.learning_context?.confidence_calibration_applied, 0) === 1,
          confidence_calibration_source: prediction?.learning_context?.confidence_calibration_source || null,
          head_confidence_raw: toNum(prediction?.learning_context?.head_confidence_raw, null),
          head_confidence_calibrated: toNum(prediction?.learning_context?.head_confidence_calibrated, null),
          bet_confidence_raw: toNum(prediction?.learning_context?.bet_confidence_raw, null),
          bet_confidence_calibrated: toNum(prediction?.learning_context?.bet_confidence_calibrated, null)
        },
        recommended_bets: aiBetsDisplaySnapshot,
        logged_at: snapshotRow.prediction_timestamp || logRow.created_at
      };
    });

    const filtered = items.filter((row) => {
      const status = String(row?.verification_status || "").toLowerCase();
      if (statusFilter === "all") return true;
      if (statusFilter === "verified") return status.startsWith("verified");
      if (statusFilter === "unverified") return status === "unverified";
      if (statusFilter === "failed") return status === "verify_failed";
      if (statusFilter === "missing") return status === "no_bet_snapshot" || status === "no_confirmed_result";
      return true;
    });

    return res.json({ items: filtered });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/results/verify", async (req, res, next) => {
  try {
    const raceId = String(req.body?.race_id || req.body?.raceId || "").trim();
    const requestedSnapshotId = Number(req.body?.prediction_snapshot_id ?? req.body?.snapshot_id);
    if (!raceId) {
      return res.status(400).json({
        error: "bad_request",
        message: "race_id is required"
      });
    }

    let settlement = null;
    try {
      settlement = await settlePlacedBetsForRace({ race_id: raceId });
    } catch (settleErr) {
      if (settleErr?.code === "result_not_found") {
        return res.status(409).json({
          ok: false,
          status: "NO_CONFIRMED_RESULT",
          verification_performed: false,
          message: "Verification cannot run yet because the confirmed race result is not available.",
          debug: settleErr?.debug || null
        });
      }
      if (settleErr?.code === "official_result_race_mismatch") {
        return res.status(409).json({
          ok: false,
          status: "VERIFY_FAILED",
          verification_performed: false,
          message: "Verification failed due to official race mismatch.",
          debug: settleErr?.debug || null
        });
      }
      throw settleErr;
    }

    const latestPrediction = getPredictionSnapshot({
      raceId,
      snapshotId: Number.isFinite(requestedSnapshotId) ? requestedSnapshotId : null
    });
    const latestRaceVerificationRow = db
      .prepare(
        `
        SELECT *
        FROM race_verification_logs
        WHERE race_id = ?
        ORDER BY id DESC
        LIMIT 1
      `
      )
      .get(raceId);
    const latestVerificationRowForRequestedSnapshot = Number.isFinite(requestedSnapshotId)
      ? db
          .prepare(
            `
            SELECT *
            FROM race_verification_logs
            WHERE verified_against_snapshot_id = ?
               OR prediction_snapshot_id = ?
            ORDER BY id DESC
            LIMIT 1
          `
          )
          .get(Number(requestedSnapshotId), Number(requestedSnapshotId))
      : latestRaceVerificationRow;
    const latestVerificationRow = latestRaceVerificationRow;
    const latestVerificationSummary = safeJsonParse(latestVerificationRow?.verification_summary_json, {});
    const latestRequestedVerificationSummary = safeJsonParse(
      latestVerificationRowForRequestedSnapshot?.verification_summary_json,
      {}
    );
    if (!latestPrediction) {
      return res.status(409).json({
        ok: false,
        status: "VERIFY_FAILED",
        verification_performed: false,
        message: "Verification cannot run because prediction snapshot is missing."
      });
    }

    const resultRow = db
      .prepare(
        `
        SELECT finish_1, finish_2, finish_3, payout_3t
        FROM results
        WHERE race_id = ?
        LIMIT 1
      `
      )
      .get(raceId);
    const startDisplayResultRow = db
      .prepare(
        `
        SELECT fetched_result, settled_result
        FROM race_start_displays
        WHERE race_id = ?
        LIMIT 1
      `
      )
      .get(raceId);

    const featureLog = db
      .prepare(
        `
        SELECT entry_changed, recommendation_mode, confidence, recommendation_score
        FROM prediction_feature_logs
        WHERE race_id = ?
        LIMIT 1
      `
      )
      .get(raceId);
    const raceMeta = db
      .prepare(
        `
        SELECT race_date, venue_id, venue_name, race_no
        FROM races
        WHERE race_id = ?
        LIMIT 1
      `
      )
      .get(raceId);
    const predictionJson = safeJsonParse(latestPrediction?.prediction_json, {});
    const betPlanJson = safeJsonParse(latestPrediction?.bet_plan_json, {});
    const predictionSnapshotContext =
      predictionJson?.snapshot_context && typeof predictionJson.snapshot_context === "object"
        ? predictionJson.snapshot_context
        : {};
    const immutableRaceMeta = {
      race_date: predictionSnapshotContext?.race_date || raceMeta?.race_date || null,
      venue_id: Number.isFinite(Number(predictionSnapshotContext?.venue_code))
        ? Number(predictionSnapshotContext.venue_code)
        : Number.isFinite(Number(raceMeta?.venue_id))
          ? Number(raceMeta.venue_id)
          : null,
      venue_name: predictionSnapshotContext?.venue_name || raceMeta?.venue_name || null,
      race_no: Number.isFinite(Number(predictionSnapshotContext?.race_no))
        ? Number(predictionSnapshotContext.race_no)
        : Number.isFinite(Number(raceMeta?.race_no))
          ? Number(raceMeta.race_no)
          : null
    };
    const snapshotDisplayBetsFromPrediction = normalizeSavedBetSnapshotItems(predictionJson?.final_recommended_bets_snapshot);
    const legacySnapshotDisplayBetsFromPrediction = normalizeSavedBetSnapshotItems(predictionJson?.ai_bets_display_snapshot);
    const snapshotDisplayBets =
      snapshotDisplayBetsFromPrediction.length > 0
        ? snapshotDisplayBetsFromPrediction
        : legacySnapshotDisplayBetsFromPrediction.length > 0
          ? legacySnapshotDisplayBetsFromPrediction
          : [];
    const snapshotFullBets =
      predictionJson?.ai_bets_full_snapshot && typeof predictionJson.ai_bets_full_snapshot === "object"
        ? predictionJson.ai_bets_full_snapshot
        : {
            recommended_bets: safeArray(betPlanJson?.recommended_bets),
            optimized_tickets: [],
            ticket_generation_v2: { primary_tickets: [], secondary_tickets: [] },
            scenario_suggestions: { main_picks: [], backup_picks: [], longshot_picks: [] }
          };
    const predictionConfidenceScores =
      predictionJson?.confidence_scores && typeof predictionJson.confidence_scores === "object"
        ? predictionJson.confidence_scores
        : {};
    const predictedTop3 = safeArray(predictionJson?.top3)
      .map((x) => toInt(x, null))
      .filter((x) => Number.isInteger(x))
      .slice(0, 3);
    let actualTop3 = [toInt(resultRow?.finish_1, null), toInt(resultRow?.finish_2, null), toInt(resultRow?.finish_3, null)]
      .filter((x) => Number.isInteger(x))
      .slice(0, 3);
    if (actualTop3.length < 3) {
      const comboFallback = String(startDisplayResultRow?.settled_result || startDisplayResultRow?.fetched_result || "").trim();
      actualTop3 = parseTop3FromCombo(comboFallback);
    }
    if (actualTop3.length < 3) {
      return res.status(409).json({
        ok: false,
        status: "NO_CONFIRMED_RESULT",
        verification_performed: false,
        error: "result_not_parseable",
        message: "Verification cannot run yet because the confirmed race result is not available."
      });
    }
    const predictedBets = snapshotDisplayBets;
    const verificationBetSource =
      snapshotDisplayBetsFromPrediction.length > 0
        ? "prediction_snapshot"
        : legacySnapshotDisplayBetsFromPrediction.length > 0
          ? "prediction_snapshot_legacy_display"
          : "none";
    const prevVerificationVersion = Number.isFinite(Number(latestVerificationSummary?.verification_version))
      ? Number(latestVerificationSummary.verification_version)
      : 0;
    const nextVerificationVersion = prevVerificationVersion + 1;
    const verifiedAgainstSnapshotId =
      verificationBetSource === "prediction_snapshot" || verificationBetSource === "prediction_snapshot_legacy_display"
        ? (Number.isFinite(Number(latestPrediction?.id)) ? Number(latestPrediction.id) : null)
        : (Number.isFinite(Number(latestVerificationSummary?.prediction_snapshot_id))
            ? Number(latestVerificationSummary.prediction_snapshot_id)
            : null);
    const canonicalSnapshotBets = safeArray(predictedBets)
      .map((row) => normalizeCombo(row?.combo ?? row))
      .filter((combo) => combo && combo.split("-").length === 3);
    if (canonicalSnapshotBets.length === 0) {
      const summary = {
        race_id: raceId,
        race_date: immutableRaceMeta.race_date,
        venue_code: immutableRaceMeta.venue_id,
        venue_name: immutableRaceMeta.venue_name,
        race_no: immutableRaceMeta.race_no,
        predicted_top3: safeArray(predictionJson?.top3).slice(0, 3),
        actual_top3: actualTop3,
        head_correct: null,
        second_third_correct: null,
        hit_miss: "NOT_VERIFIABLE",
        mismatch_categories: ["NO_BET_SNAPSHOT"],
        recommendation_mode: latestPrediction?.recommendation || featureLog?.recommendation_mode || null,
        confidence: Number.isFinite(Number(featureLog?.confidence)) ? Number(featureLog.confidence) : null,
        recommendation_score: Number.isFinite(Number(featureLog?.recommendation_score))
          ? Number(featureLog.recommendation_score)
          : null,
        verification_status: "NO_BET_SNAPSHOT",
        verification_reason: "No final recommended bet snapshot saved; verification was skipped.",
        verification_version: nextVerificationVersion,
        prediction_snapshot_id: Number.isFinite(Number(latestPrediction?.id)) ? Number(latestPrediction.id) : null,
        verified_against_snapshot_id: verifiedAgainstSnapshotId,
        snapshot_created_at: predictionJson?.snapshot_created_at || latestPrediction?.created_at || null,
        race_key: predictionJson?.race_key || raceId,
        final_recommended_bets_snapshot: [],
        final_recommended_bets_count: 0,
        snapshot_source: "missing_final_recommended_bets_snapshot",
        ai_bets_display_snapshot: [],
        ai_bets_full_snapshot: predictionJson?.ai_bets_full_snapshot || null,
        verification_bet_source: verificationBetSource,
        verified_against_bets: [],
        confirmed_result_canonical: actualTop3.length === 3 ? actualTop3.join("-") : null,
        hit_match_found: false,
        learning_ready: false,
        warning: null
      };
      const insertInfo = insertVerificationRecord({
        raceId,
        raceMeta: immutableRaceMeta,
        predictedTop3Text: safeArray(predictionJson?.top3).slice(0, 3).join("-") || null,
        actualTop3Text: actualTop3.length === 3 ? actualTop3.join("-") : null,
        hitMiss: "NOT_VERIFIABLE",
        mismatchCategories: ["NO_BET_SNAPSHOT"],
        summary
      });
      return res.json({
        ok: true,
        status: "NO_BET_SNAPSHOT",
        verification_performed: false,
        message: "Verification skipped because no final recommended bet snapshot was saved.",
        reason_code: "NO_BET_SNAPSHOT",
        persisted: !!insertInfo?.lastInsertRowid,
        verification_version: nextVerificationVersion,
        verified_against_snapshot_id: verifiedAgainstSnapshotId,
        verification_bet_source: verificationBetSource,
        verified_against_bets: [],
        confirmed_result_canonical: actualTop3.length === 3 ? actualTop3.join("-") : null,
        hit_match_found: false
      });
    }
    let verifyWarning = null;
    const raceRisk = {
      recommendation: latestPrediction?.recommendation || featureLog?.recommendation_mode || null
    };

    const analysis = buildMismatchAnalysis({
      predictedTop3,
      actualTop3,
      predictedBets,
      predictionJson,
      raceRisk,
      featureLog
    });
    const summary = {
      race_id: raceId,
      race_date: immutableRaceMeta.race_date,
      venue_code: immutableRaceMeta.venue_id,
      venue_name: immutableRaceMeta.venue_name,
      race_no: immutableRaceMeta.race_no,
      predicted_top3: predictedTop3,
      actual_top3: actualTop3,
      head_correct: analysis.head_correct,
      second_third_correct: analysis.second_third_correct,
      hit_miss: analysis.hit_miss,
      mismatch_categories: analysis.categories,
      recommendation_mode: latestPrediction?.recommendation || featureLog?.recommendation_mode || null,
      confidence: Number.isFinite(Number(featureLog?.confidence)) ? Number(featureLog.confidence) : null,
      recommendation_score: Number.isFinite(Number(featureLog?.recommendation_score))
        ? Number(featureLog.recommendation_score)
        : null,
      head_confidence: Number.isFinite(Number(predictionJson?.head_confidence))
        ? Number(predictionJson.head_confidence)
        : Number.isFinite(Number(predictionConfidenceScores?.head_confidence))
          ? Number(predictionConfidenceScores.head_confidence)
          : null,
      bet_confidence: Number.isFinite(Number(predictionJson?.bet_confidence))
        ? Number(predictionJson.bet_confidence)
        : Number.isFinite(Number(predictionConfidenceScores?.bet_confidence))
          ? Number(predictionConfidenceScores.bet_confidence)
          : null,
      participation_decision: predictionJson?.participation_decision || null,
      confidence_reason_tags: Array.isArray(predictionJson?.confidence_reason_tags)
        ? predictionJson.confidence_reason_tags
        : Array.isArray(predictionConfidenceScores?.confidence_reason_tags)
          ? predictionConfidenceScores.confidence_reason_tags
          : [],
      confidence_version:
        predictionJson?.confidence_version ||
        predictionConfidenceScores?.confidence_version ||
        null,
      verification_status: analysis.hit_miss === "HIT" ? "VERIFIED_HIT" : analysis.hit_miss === "MISS" ? "VERIFIED_MISS" : "VERIFIED",
      verification_reason: verifyWarning || null,
      verification_version: nextVerificationVersion,
      prediction_snapshot_id: Number.isFinite(Number(latestPrediction?.id)) ? Number(latestPrediction.id) : null,
      verified_against_snapshot_id: verifiedAgainstSnapshotId,
      snapshot_created_at: predictionJson?.snapshot_created_at || latestPrediction?.created_at || null,
      race_key: predictionJson?.race_key || raceId,
      final_recommended_bets_snapshot: snapshotDisplayBets,
      final_recommended_bets_count: snapshotDisplayBets.length,
      snapshot_source:
        predictionJson?.final_recommended_bets_snapshot_source ||
        (snapshotDisplayBetsFromPrediction.length > 0
          ? "prediction_final_recommended_bets_snapshot"
          : legacySnapshotDisplayBetsFromPrediction.length > 0
            ? "prediction_snapshot_legacy_display"
            : "missing_final_recommended_bets_snapshot"),
      ai_bets_display_snapshot: snapshotDisplayBets,
      ai_bets_full_snapshot: snapshotFullBets,
      verification_bet_source: verificationBetSource,
      verified_against_bets: analysis.verified_against_bets,
      confirmed_result_canonical: analysis.confirmed_result_canonical,
      hit_match_found: analysis.hit_match_found,
      learning_ready: analysis.categories.length > 0,
      warning: verifyWarning
    };

    let insertInfo = null;
    try {
      insertInfo = insertVerificationRecord({
        raceId,
        raceMeta: immutableRaceMeta,
        predictedTop3Text: analysis.predicted_combo,
        actualTop3Text: analysis.actual_combo,
        hitMiss: analysis.hit_miss,
        mismatchCategories: analysis.categories,
        summary
      });
    } catch (persistErr) {
      return res.status(500).json({
        ok: false,
        status: "VERIFY_FAILED",
        verification_performed: false,
        reason_code: "PERSISTENCE_ERROR",
        message: "Verification failed because persistence failed.",
        error_detail: persistErr?.message || String(persistErr)
      });
    }

    const continuousLearning = runContinuousLearningIfNeeded();

    console.info("[VERIFY]", {
      race_id: raceId,
      predicted_top3: predictedTop3,
      actual_top3: actualTop3,
      hit_miss: analysis.hit_miss,
      mismatch_categories: analysis.categories
    });

    const settledAgg = db
      .prepare(
        `
        SELECT
          COALESCE(SUM(bet_amount), 0) AS total_bet_amount,
          COALESCE(SUM(COALESCE(payout, 0)), 0) AS total_payout,
          COALESCE(SUM(COALESCE(profit_loss, 0)), 0) AS total_profit_loss,
          COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hit_count,
          COALESCE(SUM(CASE WHEN hit_flag = 0 THEN 1 ELSE 0 END), 0) AS miss_count
        FROM placed_bets
        WHERE race_id = ?
      `
      )
      .get(raceId);

    return res.json({
      ok: true,
      status: summary.verification_status,
      verification_performed: true,
      message: "Verification completed.",
      verification: summary,
      warning: verifyWarning,
      persisted: !!insertInfo?.lastInsertRowid,
      summary_updated: true,
      learning_ready: analysis.categories.length > 0,
      verification_version: nextVerificationVersion,
      verified_against_snapshot_id: verifiedAgainstSnapshotId,
      verified_against_bets: analysis.verified_against_bets,
      confirmed_result_canonical: analysis.confirmed_result_canonical,
      hit_match_found: analysis.hit_match_found,
      verification_bet_source: verificationBetSource,
      continuous_learning: continuousLearning,
      confirmed_result: actualTop3.join("-"),
      settlement: settlement || null,
      updated_result_fields: {
        payout_3t: Number.isFinite(Number(resultRow?.payout_3t)) ? Number(resultRow.payout_3t) : null,
        total_bet_amount: Number(settledAgg?.total_bet_amount || 0),
        total_payout: Number(settledAgg?.total_payout || 0),
        total_profit_loss: Number(settledAgg?.total_profit_loss || 0),
        hit_count: Number(settledAgg?.hit_count || 0),
        miss_count: Number(settledAgg?.miss_count || 0)
      }
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/results/invalidate", async (req, res, next) => {
  try {
    ensureVerificationLogColumns();
    const verificationLogId = Number.isFinite(Number(req.body?.verification_log_id))
      ? Number(req.body.verification_log_id)
      : null;
    const predictionSnapshotId = Number.isFinite(Number(req.body?.prediction_snapshot_id))
      ? Number(req.body.prediction_snapshot_id)
      : null;
    const raceId = String(req.body?.race_id || "").trim() || null;
    const invalidReason = String(req.body?.invalid_reason || "").trim() || null;
    const hideFromResults = parseBooleanFlag(req.body?.is_hidden_from_results, true);
    const invalidVerification = parseBooleanFlag(req.body?.is_invalid_verification, true);
    const excludeFromLearning = parseBooleanFlag(req.body?.exclude_from_learning, true);

    let targetRow = null;
    if (verificationLogId) {
      targetRow = db.prepare("SELECT * FROM race_verification_logs WHERE id = ? LIMIT 1").get(verificationLogId);
    } else if (predictionSnapshotId) {
      targetRow = db.prepare(
        `
        SELECT *
        FROM race_verification_logs
        WHERE COALESCE(verified_against_snapshot_id, prediction_snapshot_id) = ?
        ORDER BY id DESC
        LIMIT 1
      `
      ).get(predictionSnapshotId);
    } else if (raceId) {
      targetRow = db.prepare(
        `
        SELECT *
        FROM race_verification_logs
        WHERE race_id = ?
        ORDER BY id DESC
        LIMIT 1
      `
      ).get(raceId);
    }

    if (!targetRow) {
      return res.status(404).json({
        ok: false,
        error: "verification_not_found",
        message: "No verification record was found to invalidate."
      });
    }

    const invalidatedAt = new Date().toISOString();
    const existingSummary = safeJsonParse(targetRow?.verification_summary_json, {});
    const nextSummary = {
      ...existingSummary,
      invalid_reason: invalidReason,
      invalidated_at: invalidatedAt,
      is_hidden_from_results: hideFromResults,
      is_invalid_verification: invalidVerification,
      exclude_from_learning: excludeFromLearning,
      learning_ready: false,
      invalidation_source: "manual_soft_invalidate"
    };

    db.prepare(
      `
      UPDATE race_verification_logs
      SET
        is_hidden_from_results = ?,
        is_invalid_verification = ?,
        exclude_from_learning = ?,
        invalid_reason = ?,
        invalidated_at = ?,
        learning_ready = 0,
        verification_summary_json = ?
      WHERE id = ?
    `
    ).run(
      hideFromResults ? 1 : 0,
      invalidVerification ? 1 : 0,
      excludeFromLearning ? 1 : 0,
      invalidReason,
      invalidatedAt,
      JSON.stringify(nextSummary),
      Number(targetRow.id)
    );

    return res.json({
      ok: true,
      verification_log_id: Number(targetRow.id),
      race_id: targetRow.race_id,
      prediction_snapshot_id:
        Number.isFinite(Number(targetRow?.prediction_snapshot_id)) ? Number(targetRow.prediction_snapshot_id) : null,
      verified_against_snapshot_id:
        Number.isFinite(Number(targetRow?.verified_against_snapshot_id)) ? Number(targetRow.verified_against_snapshot_id) : null,
      is_hidden_from_results: hideFromResults,
      is_invalid_verification: invalidVerification,
      exclude_from_learning: excludeFromLearning,
      invalid_reason: invalidReason,
      invalidated_at: invalidatedAt
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/results/restore", async (req, res, next) => {
  try {
    ensureVerificationLogColumns();
    const verificationLogId = Number.isFinite(Number(req.body?.verification_log_id))
      ? Number(req.body.verification_log_id)
      : null;
    if (!verificationLogId) {
      return res.status(400).json({
        ok: false,
        error: "verification_log_id_required",
        message: "verification_log_id is required to restore a record."
      });
    }
    const targetRow = db.prepare("SELECT * FROM race_verification_logs WHERE id = ? LIMIT 1").get(verificationLogId);
    if (!targetRow) {
      return res.status(404).json({
        ok: false,
        error: "verification_not_found",
        message: "No verification record was found to restore."
      });
    }
    const existingSummary = safeJsonParse(targetRow?.verification_summary_json, {});
    const nextSummary = {
      ...existingSummary,
      is_hidden_from_results: false,
      is_invalid_verification: false,
      exclude_from_learning: false,
      invalid_reason: null,
      invalidated_at: null,
      learning_ready: String(targetRow?.verification_status || existingSummary?.verification_status || "").toUpperCase().startsWith("VERIFIED")
        ? ((Array.isArray(existingSummary?.mismatch_categories) && existingSummary.mismatch_categories.length > 0) ? true : Number(targetRow?.learning_ready) === 1)
        : false,
      invalidation_source: "manual_restore"
    };

    db.prepare(
      `
      UPDATE race_verification_logs
      SET
        is_hidden_from_results = 0,
        is_invalid_verification = 0,
        exclude_from_learning = 0,
        invalid_reason = NULL,
        invalidated_at = NULL,
        learning_ready = ?,
        verification_summary_json = ?
      WHERE id = ?
    `
    ).run(
      nextSummary.learning_ready ? 1 : 0,
      JSON.stringify(nextSummary),
      verificationLogId
    );

    return res.json({
      ok: true,
      verification_log_id: verificationLogId,
      restored: true
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/results/verification-note", async (req, res, next) => {
  try {
    ensureVerificationLogColumns();
    const verificationLogId = Number.isFinite(Number(req.body?.verification_log_id))
      ? Number(req.body.verification_log_id)
      : null;
    const note = String(req.body?.verification_reason || req.body?.note || "").trim() || null;
    if (!verificationLogId) {
      return res.status(400).json({
        ok: false,
        error: "verification_log_id_required",
        message: "verification_log_id is required to update a verification note."
      });
    }
    const targetRow = db.prepare("SELECT * FROM race_verification_logs WHERE id = ? LIMIT 1").get(verificationLogId);
    if (!targetRow) {
      return res.status(404).json({
        ok: false,
        error: "verification_not_found",
        message: "No verification record was found to update."
      });
    }
    const existingSummary = safeJsonParse(targetRow?.verification_summary_json, {});
    const nextSummary = {
      ...existingSummary,
      verification_reason: note,
      status_note: note
    };
    db.prepare(
      `
      UPDATE race_verification_logs
      SET
        verification_reason = ?,
        verification_summary_json = ?
      WHERE id = ?
    `
    ).run(note, JSON.stringify(nextSummary), verificationLogId);
    return res.json({
      ok: true,
      verification_log_id: verificationLogId,
      verification_reason: note
    });
  } catch (err) {
    return next(err);
  }
});

raceRouter.get("/prediction-feature-logs", async (req, res, next) => {
  try {
    const limitRaw = Number(req.query?.limit);
    const limit = Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 300) : 100;
    const rows = db
      .prepare(
        `
        SELECT *
        FROM prediction_feature_logs
        ORDER BY updated_at DESC, id DESC
        LIMIT ?
      `
      )
      .all(limit)
      .map((row) => ({
        ...row,
        start_display_st: safeJsonParse(row.start_display_st_json, {}),
        predicted_entry_order: safeJsonParse(row.predicted_entry_order_json, []),
        actual_entry_order: safeJsonParse(row.actual_entry_order_json, []),
        prediction_snapshot: safeJsonParse(row.prediction_snapshot_json, {}),
        prediction_before_entry_change: safeJsonParse(row.prediction_before_entry_change_json, {}),
        prediction_after_entry_change: safeJsonParse(row.prediction_after_entry_change_json, {})
      }));

    return res.json({ items: rows });
  } catch (err) {
    return next(err);
  }
});

