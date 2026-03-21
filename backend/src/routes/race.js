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
import {
  applyHitRateEnhancementToProbabilities,
  buildEnhancedShapeBasedTrifectaTickets,
  buildEnhancedTrifectaShapeRecommendation,
  buildHitRateEnhancementContext,
  buildScenarioTreeOrderCandidates
} from "../services/hit-rate-enhancement.js";
import { buildHardRace1234Response } from "../services/hard-race-1234-v2.js";

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

db.exec(`
  CREATE TABLE IF NOT EXISTS evaluation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    date_range_start TEXT,
    date_range_end TEXT,
    latest_verified_at TEXT,
    verified_race_count INTEGER NOT NULL DEFAULT 0,
    trifecta_hit_rate REAL NOT NULL DEFAULT 0,
    exacta_hit_rate REAL NOT NULL DEFAULT 0,
    head_hit_rate REAL NOT NULL DEFAULT 0,
    second_place_hit_rate REAL NOT NULL DEFAULT 0,
    third_place_hit_rate REAL NOT NULL DEFAULT 0,
    model_version TEXT,
    learning_run_id INTEGER,
    summary_json TEXT NOT NULL DEFAULT '{}'
  );
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS evaluation_segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evaluation_run_id INTEGER NOT NULL,
    segment_type TEXT NOT NULL,
    segment_key TEXT NOT NULL,
    model_version TEXT,
    learning_run_id INTEGER,
    verified_race_count INTEGER NOT NULL DEFAULT 0,
    trifecta_hit_rate REAL NOT NULL DEFAULT 0,
    exacta_hit_rate REAL NOT NULL DEFAULT 0,
    head_hit_rate REAL NOT NULL DEFAULT 0,
    second_place_hit_rate REAL NOT NULL DEFAULT 0,
    third_place_hit_rate REAL NOT NULL DEFAULT 0,
    evaluation_created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    metrics_json TEXT NOT NULL DEFAULT '{}'
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
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function buildFailOpenFeatureSet(racer, raceContext = {}) {
  const classScoreMap = { A1: 4, A2: 3, B1: 2, B2: 1 };
  const lane = Number.isFinite(Number(racer?.lane)) ? Number(racer.lane) : 0;
  const entryCourse = toNullableNum(racer?.entryCourse);
  const actualLane = Number.isFinite(entryCourse) ? entryCourse : lane || null;
  const nationwideWinRate = toNum(racer?.nationwideWinRate, 0);
  const localWinRate = toNum(racer?.localWinRate, 0);
  const avgSt = toNullableNum(racer?.avgSt);
  const exhibitionSt = toNullableNum(racer?.exhibitionSt ?? racer?.exhibitionStNumeric);
  const boat2Rate = toNum(racer?.boat2Rate, 0);
  const boat3Rate = toNullableNum(racer?.boat3Rate);
  const actualStInv =
    Number.isFinite(avgSt) && avgSt > 0
      ? Number((1 / avgSt).toFixed(6))
      : 0;

  return {
    lane,
    original_lane: lane || null,
    boat_number: lane || null,
    actual_lane: actualLane,
    class_score: classScoreMap[String(racer?.class || "").toUpperCase()] ?? 0,
    nationwide_win_rate: nationwideWinRate,
    local_win_rate: localWinRate,
    motor2_rate: toNullableNum(racer?.motor2ren ?? racer?.motor2Rate),
    motor3_rate: toNullableNum(racer?.motor3ren ?? racer?.motor3Rate),
    boat2_rate: boat2Rate,
    boat3_rate: boat3Rate,
    weight: toNum(racer?.weight, 0),
    avg_st: avgSt,
    exhibition_st: exhibitionSt,
    exhibition_time: toNullableNum(racer?.exhibitionTime),
    lap_time: toNullableNum(racer?.kyoteiBiyoriLapTime ?? racer?.lapTime),
    lap_exhibition_score: toNullableNum(racer?.kyoteiBiyoriLapExStretch ?? racer?.lapExStretch),
    local_minus_nation: localWinRate - nationwideWinRate,
    motor_boat_avg: ((toNum(racer?.motor2ren ?? racer?.motor2Rate, 0)) + boat2Rate) / 2,
    st_inv: actualStInv,
    expected_actual_st_inv: actualStInv,
    is_inner: actualLane >= 1 && actualLane <= 3 ? 1 : 0,
    is_outer: actualLane >= 5 && actualLane <= 6 ? 1 : 0,
    entry_course: entryCourse,
    wind_speed: toNum(raceContext?.windSpeed ?? racer?.windSpeed, 0),
    tilt_bonus: toNullableNum(racer?.tilt) === 0.5 ? 2 : 0,
    course_change: Number.isFinite(entryCourse) && lane ? (entryCourse !== lane ? 1 : 0) : 0,
    prediction_field_meta:
      racer?.predictionFieldMeta && typeof racer.predictionFieldMeta === "object"
        ? racer.predictionFieldMeta
        : {},
    course_fit_score: 0,
    motor_total_score: 0,
    entry_advantage_score: 0,
    motor_trend_score: 0,
    venue_lane_adjustment: 0,
    f_hold_caution_penalty: 0,
    exhibition_rank: null,
    st_rank: null,
    avg_st_rank: null,
    lane_st_rank: null,
    expected_actual_st_rank: null,
    display_time_delta_vs_left: null,
    avg_st_rank_delta_vs_left: null,
    slit_alert_flag: 0,
    lap_time_delta_vs_front: null,
    lap_time_rank: null,
    lap_attack_strength: 0,
    hidden_f_flag: 0,
    unresolved_f_count: null,
    start_caution_penalty: 0,
    f_hold_count: null,
    f_hold_bias_applied: 0,
    expected_actual_st_adjustment: 0,
    expected_actual_st: avgSt,
    start_stability_score: 50,
    motor_true: 0,
    lane_fit_1st: toNullableNum(racer?.lane1stScore ?? racer?.lane1stAvg ?? racer?.laneFirstRate),
    lane_fit_2ren: toNullableNum(racer?.lane2renScore ?? racer?.lane2renAvg ?? racer?.lane2RenRate),
    lane_fit_3ren: toNullableNum(racer?.lane3renScore ?? racer?.lane3renAvg ?? racer?.lane3RenRate)
  };
}

function buildFeaturePipelineFallback(data, featurePipelineDebug, learningWeights) {
  const racersWithFeatures = (Array.isArray(data?.racers) ? data.racers : []).map((racer) => ({
    racer,
    features: buildFailOpenFeatureSet(racer, data?.race || {})
  }));
  const venueAdjustedBase = {
    racersWithFeatures,
    venue: { chaosAdjustment: 0, fallbackApplied: true }
  };
  const rankingBase = rankRace(racersWithFeatures);
  const fallbackPattern = analyzeRacePattern(rankingBase);
  let escapePatternAnalysis = {
    formation_pattern: fallbackPattern?.race_pattern || "mixed",
    formation_pattern_clarity_score: 0,
    escape_pattern_applied: false,
    escape_pattern_confidence: 0,
    escape_second_place_bias_json: {}
  };

  try {
    escapePatternAnalysis = analyzeEscapeFormationLayer({
      ranking: rankingBase,
      racePattern: fallbackPattern?.race_pattern || "mixed",
      indexes: fallbackPattern?.indexes || {}
    });
  } catch (fallbackErr) {
    featurePipelineDebug.skipped_optional_features.push("escape_pattern_analysis");
    featurePipelineDebug.errors.push(`escape_pattern_analysis:${fallbackErr?.message || fallbackErr}`);
  }

  return {
    baseFeatures: venueAdjustedBase,
    venueAdjustedBase,
    preRanking: rankingBase,
    prePattern: fallbackPattern,
    trendFeatures: venueAdjustedBase,
    entryAdjusted: { racersWithFeatures, chaosBoost: 0 },
    rankingBase,
    contenderAdjusted: { ranking: rankingBase, contenderSignals: [] },
    rankingBeforePatternBias: rankingBase,
    patternBeforeBias: fallbackPattern,
    escapePatternAnalysis,
    ranking: rankingBase
  };
}

function buildTemporaryFeaturePipelineDebug(ranking, featurePipelineDebug) {
  const missingLegacyFields = [...(featurePipelineDebug?.legacy_fields_missing || [])];
  const skippedOptionalFeatures = [...(featurePipelineDebug?.skipped_optional_features || [])];

  for (const row of Array.isArray(ranking) ? ranking : []) {
    const lane = toInt(row?.racer?.lane ?? row?.features?.lane, null);
    const laneLabel = Number.isInteger(lane) ? `lane_${lane}` : "lane_unknown";
    const features = row?.features || {};
    const hasAvgStRank = Number.isFinite(features?.avg_st_rank) || Number.isFinite(features?.lane_st_rank);
    if (!hasAvgStRank) {
      missingLegacyFields.push(`${laneLabel}: laneStRank unavailable`);
      missingLegacyFields.push(`${laneLabel}: lane_st_rank unavailable`);
      skippedOptionalFeatures.push(`${laneLabel}: start_stability_score lane-rank contribution skipped`);
      skippedOptionalFeatures.push(`${laneLabel}: hit_rate_start_edge avg_st_rank contribution skipped`);
    }
  }

  return {
    feature_pipeline_fail_open_applied: !!featurePipelineDebug?.fail_open_applied,
    missing_legacy_fields: [...new Set(missingLegacyFields)],
    skipped_optional_features: [...new Set(skippedOptionalFeatures)],
    skipped_pipeline_steps: [...new Set(featurePipelineDebug?.skipped_pipeline_steps || [])],
    errors: [...new Set(featurePipelineDebug?.errors || [])]
  };
}

function createTimeoutError({ code, where, route, message, statusCode = 504 }) {
  const error = new Error(message);
  error.code = code;
  error.where = where;
  error.route = route;
  error.statusCode = statusCode;
  return error;
}

async function withTimeout(promiseFactory, { timeoutMs, code, where, route, message, fallbackValue, swallow = false }) {
  let timeoutHandle = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(createTimeoutError({ code, where, route, message, statusCode: 504 }));
    }, Math.max(100, Number(timeoutMs) || 1000));
  });
  try {
    return await Promise.race([Promise.resolve().then(promiseFactory), timeoutPromise]);
  } catch (error) {
    if (swallow) return fallbackValue;
    throw error;
  } finally {
    if (timeoutHandle) clearTimeout(timeoutHandle);
  }
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

function normalizeParticipationDecision(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (["participate", "buy", "enter"].includes(raw)) return "participate";
  if (["watch", "hold"].includes(raw)) return "watch";
  if (["skip", "pass"].includes(raw)) return "skip";
  return "unknown";
}

function hasTag(tags, expected) {
  const needle = String(expected || "").trim().toLowerCase();
  if (!needle) return false;
  return safeArray(tags).some((tag) => String(tag || "").trim().toLowerCase() === needle);
}

function asDistributionRows(distribution) {
  return safeArray(distribution)
    .map((row) => ({
      lane: toInt(row?.lane ?? row?.boat ?? row?.course, null),
      weight: toNum(row?.weight ?? row?.score ?? row?.probability, 0)
    }))
    .filter((row) => Number.isInteger(row.lane) && row.lane >= 1 && row.lane <= 6 && row.weight > 0)
    .sort((a, b) => b.weight - a.weight);
}

function topDistributionLane(distribution) {
  const rows = asDistributionRows(distribution);
  return rows[0]?.lane ?? null;
}

function distributionConcentrationForEvaluation(distribution, topN = 2) {
  const rows = asDistributionRows(distribution);
  if (!rows.length) return 0;
  const total = rows.reduce((acc, row) => acc + row.weight, 0);
  if (!total) return 0;
  const top = rows.slice(0, topN).reduce((acc, row) => acc + row.weight, 0);
  return Number(((top / total) * 100).toFixed(2));
}

function deriveBoat1HeadEvaluation(row) {
  if (toNum(row?.boat1_partner_model_applied, 0) === 1) return true;
  if (toNum(row?.boat1_priority_mode_applied, 0) === 1) return true;
  const firstLane =
    topDistributionLane(row?.first_place_distribution_json) ??
    topDistributionLane(row?.head_distribution_json);
  if (firstLane === 1) return true;
  return false;
}

function derivePredictionModeKey(row) {
  const recommendationMode = normalizeModeValue(row?.recommendation_mode);
  const focus = toNum(row?.hit_rate_focus_applied, 0) === 1 ? "hit_rate" : "standard";
  return `${focus}:${recommendationMode}`;
}

function deriveModelVersion(row) {
  const rebalance = row?.rebalance_version || "legacy_rebalance";
  const confidence = row?.confidence_version || "legacy_confidence";
  return `${rebalance} | ${confidence}`;
}

function normalizeConfidenceValue(value) {
  const n = toNum(value, null);
  if (!Number.isFinite(n)) return null;
  if (n > 1.0001) return Number((n / 100).toFixed(4));
  if (n < 0) return 0;
  return Number(n.toFixed(4));
}

function confidenceBucketKey(value) {
  const normalized = normalizeConfidenceValue(value);
  if (!Number.isFinite(normalized)) return null;
  if (normalized >= 0.9) return "0.9-1.0";
  if (normalized >= 0.8) return "0.8-0.9";
  if (normalized >= 0.7) return "0.7-0.8";
  if (normalized >= 0.6) return "0.6-0.7";
  if (normalized >= 0.5) return "0.5-0.6";
  return "<0.5";
}

function deriveRecommendationLevel(row) {
  const decision = normalizeParticipationDecision(row?.participation_decision);
  if (decision === "participate") return "recommended";
  if (decision === "watch") return "caution";
  if (decision === "skip") return "not_recommended";
  const mode = normalizeRecommendation(row?.recommendation_mode);
  if (mode === "FULL BET") return "recommended";
  if (mode === "SMALL BET" || mode === "MICRO BET") return "caution";
  if (mode === "SKIP") return "not_recommended";
  return "unknown";
}

function hasOutsideLeadFlag(row) {
  return String(row?.formation_pattern || "").trim().toLowerCase() === "outside_lead";
}

function parseRecommendedSnapshotCombos(row) {
  return normalizeSavedBetSnapshotItems(
    row?.role_based_main_trifecta_tickets_snapshot?.length
      ? row.role_based_main_trifecta_tickets_snapshot
      : row?.final_recommended_bets_snapshot
  );
}

function hasOutsideHeadRecommendation(row) {
  const mainHeadLane =
    topDistributionLane(row?.confirmed_first_place_probability_json) ??
    topDistributionLane(row?.first_place_probability_json) ??
    topDistributionLane(row?.first_place_distribution_json) ??
    topDistributionLane(row?.head_distribution_json);
  if (mainHeadLane === 5 || mainHeadLane === 6) return true;
  return parseRecommendedSnapshotCombos(row).some((ticket) => {
    const firstLane = toInt(String(ticket?.combo || "").split("-")[0], null);
    return firstLane === 5 || firstLane === 6;
  });
}

function getMainPredictedHeadLane(row) {
  return (
    topDistributionLane(row?.confirmed_first_place_probability_json) ??
    topDistributionLane(row?.first_place_probability_json) ??
    topDistributionLane(row?.first_place_distribution_json) ??
    topDistributionLane(row?.head_distribution_json)
  );
}

function getActualTop3Lanes(row) {
  return parseTop3FromCombo(row?.confirmed_result);
}

function hasOuterLaneInActualPlace(row, targetPlaces = [2, 3]) {
  const actual = getActualTop3Lanes(row);
  return targetPlaces.some((place) => {
    const lane = actual[place - 1];
    return lane === 5 || lane === 6;
  });
}

function isBoat1EscapePredicted(row) {
  if (deriveBoat1HeadEvaluation(row)) return true;
  return getMainPredictedHeadLane(row) === 1 || normalizeConfidenceValue(row?.boat1_escape_probability) >= 0.55;
}

function buildMissCategoryRows(rows) {
  const items = safeArray(rows);
  const definitions = [
    ["miss_head", (row) => toNum(row?.miss_head, 0) === 1 || toNum(row?.head_hit, 0) === 0],
    ["miss_second", (row) => toNum(row?.miss_second, 0) === 1 || toNum(row?.second_place_miss, 0) === 1],
    ["miss_third", (row) => toNum(row?.miss_third, 0) === 1 || toNum(row?.third_place_miss, 0) === 1],
    ["boat1_escape_correct_but_opponent_wrong", (row) => toNum(row?.boat1_escape_correct_but_opponent_wrong, 0) === 1],
    ["attack_read_correct_but_finish_wrong", (row) => toNum(row?.attack_read_correct_but_finish_wrong, 0) === 1],
    ["outside_head_overpromotion", (row) => hasTag(row?.miss_pattern_tags, "outer_head_overpromotion")],
    [
      "low_confidence_but_bet",
      (row) => deriveRecommendationLevel(row) === "recommended" && normalizeConfidenceValue(row?.bet_confidence) < 0.6
    ],
    [
      "recommendation_quality_mismatch",
      (row) =>
        deriveRecommendationLevel(row) === "recommended" &&
        (toNum(row?.quality_gate_applied, 0) === 1 ||
          toNum(row?.data_quality_score, 100) < 55 ||
          toNum(row?.race_stability_score, 100) < 55)
    ]
  ];
  return definitions.map(([key, predicate]) => {
    const count = items.filter((row) => predicate(row)).length;
    return {
      category: key,
      count,
      rate: pct(count, items.length)
    };
  });
}

function buildConfidenceCalibration(rows, field, hitField = "hit_flag") {
  const buckets = ["0.9-1.0", "0.8-0.9", "0.7-0.8", "0.6-0.7", "0.5-0.6", "<0.5"];
  const grouped = new Map(buckets.map((key) => [key, []]));
  for (const row of safeArray(rows)) {
    const bucket = confidenceBucketKey(row?.[field]);
    if (!bucket) continue;
    grouped.get(bucket).push(row);
  }
  return buckets.map((bucket) => {
    const bucketRows = grouped.get(bucket) || [];
    const hitCount = bucketRows.filter((row) => toNum(row?.[hitField], 0) === 1).length;
    const avgConfidence = bucketRows.length
      ? Number(
          (
            bucketRows.reduce((acc, row) => acc + toNum(normalizeConfidenceValue(row?.[field]), 0), 0) /
            bucketRows.length
          ).toFixed(4)
        )
      : null;
    return {
      bucket,
      race_count: bucketRows.length,
      hit_count: hitCount,
      hit_rate: pct(hitCount, bucketRows.length),
      average_confidence: avgConfidence
    };
  });
}

function buildOutsideHeadMonitoring(rows) {
  const items = safeArray(rows);
  const boat5Rows = items.filter((row) => getMainPredictedHeadLane(row) === 5);
  const boat6Rows = items.filter((row) => getMainPredictedHeadLane(row) === 6);
  const outsideRecommendedRows = items.filter((row) => hasOutsideHeadRecommendation(row));
  const aggressiveChaosRows = items.filter((row) => {
    const recommendationLevel = deriveRecommendationLevel(row);
    const lowHeadConfidence = normalizeConfidenceValue(row?.head_confidence) < 0.6;
    return hasOutsideHeadRecommendation(row) && (recommendationLevel !== "recommended" || lowHeadConfidence);
  });
  const outsideLeadOverpromotionCount = items.filter((row) => (
    hasOutsideLeadFlag(row) && hasTag(row?.miss_pattern_tags, "outer_head_overpromotion")
  )).length;
  return {
    boat5_main_head_count: boat5Rows.length,
    boat5_main_head_first_hit_rate: pct(boat5Rows.filter((row) => toNum(row?.head_hit, 0) === 1).length, boat5Rows.length),
    boat6_main_head_count: boat6Rows.length,
    boat6_main_head_first_hit_rate: pct(boat6Rows.filter((row) => toNum(row?.head_hit, 0) === 1).length, boat6Rows.length),
    outside_head_recommendation_count: outsideRecommendedRows.length,
    outside_second_third_survival_rate: pct(
      outsideRecommendedRows.filter((row) => hasOuterLaneInActualPlace(row, [2, 3])).length,
      outsideRecommendedRows.length
    ),
    outside_lead_overpromotion_count: outsideLeadOverpromotionCount,
    chaos_or_not_recommended_outside_head_count: aggressiveChaosRows.length
  };
}

function buildBoat1EscapeDiagnostics(rows) {
  const boat1Rows = safeArray(rows).filter((row) => isBoat1EscapePredicted(row));
  const boat1HeadHitRows = boat1Rows.filter((row) => toNum(row?.head_hit, 0) === 1);
  const familyRows = [
    { key: "1-2-x", predicate: (row) => parseRecommendedSnapshotCombos(row).some((ticket) => String(ticket?.combo || "").startsWith("1-2-")) },
    { key: "1-3-x", predicate: (row) => parseRecommendedSnapshotCombos(row).some((ticket) => String(ticket?.combo || "").startsWith("1-3-")) },
    { key: "1-4-x", predicate: (row) => parseRecommendedSnapshotCombos(row).some((ticket) => String(ticket?.combo || "").startsWith("1-4-")) }
  ].map((family) => {
    const predictedRows = boat1Rows.filter((row) => family.predicate(row));
    const capturedRows = predictedRows.filter((row) => {
      const actual = getActualTop3Lanes(row);
      return actual.length === 3 && actual[0] === 1 && actual[1] === toInt(family.key.split("-")[1], null);
    });
    return {
      family: family.key,
      prediction_count: predictedRows.length,
      captured_count: capturedRows.length,
      capture_rate: pct(capturedRows.length, predictedRows.length)
    };
  });
  const exactOpponentHitCount = boat1Rows.filter((row) => (
    toNum(row?.head_hit, 0) === 1 &&
    toNum(row?.second_place_miss, 0) === 0 &&
    toNum(row?.third_place_miss, 0) === 0
  )).length;
  return {
    boat1_escape_prediction_count: boat1Rows.length,
    boat1_escape_hit_rate: pct(boat1HeadHitRows.length, boat1Rows.length),
    boat1_escape_opponent_hit_rate: pct(exactOpponentHitCount, boat1Rows.length),
    opponent_exact_hit_count: exactOpponentHitCount,
    attack_read_correct_but_finish_wrong_count: boat1Rows.filter((row) => toNum(row?.attack_read_correct_but_finish_wrong, 0) === 1).length,
    family_capture_rows: familyRows
  };
}

function computeEvaluationMetrics(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const verifiedRaceCount = items.length;
  const trifectaHitCount = items.filter((row) => toNum(row?.hit_flag, 0) === 1).length;
  const headHitCount = items.filter((row) => toNum(row?.head_hit, 0) === 1).length;
  const exactaRows = items.filter((row) => toNullableNum(row?.exacta_hit) !== null);
  const exactaHitCount = exactaRows.filter((row) => toNum(row?.exacta_hit, 0) === 1).length;
  const secondRows = items.filter((row) => toNullableNum(row?.second_place_miss) !== null);
  const secondPlaceHitCount = secondRows.filter((row) => toNum(row?.second_place_miss, 0) === 0).length;
  const thirdRows = items.filter((row) => toNullableNum(row?.third_place_miss) !== null);
  const thirdPlaceHitCount = thirdRows.filter((row) => toNum(row?.third_place_miss, 0) === 0).length;
  const secondThirdSwapCount = items.filter((row) => toNum(row?.second_third_swap, 0) === 1).length;
  const nearMissCount = items.filter((row) => (
    toNum(row?.structure_near_but_order_miss, 0) === 1 ||
    hasTag(row?.miss_pattern_tags, "structure_near_miss")
  )).length;
  const partnerSelectionMissCount = items.filter((row) => toNum(row?.partner_selection_miss, 0) === 1).length;
  const thirdPlaceNoiseCount = items.filter((row) => toNum(row?.third_place_noise, 0) === 1).length;
  const boat1SurvivalUnderestimatedCount = items.filter((row) => hasTag(row?.miss_pattern_tags, "boat1_survival_underestimated")).length;
  const outerHeadOverpromotionCount = items.filter((row) => hasTag(row?.miss_pattern_tags, "outer_head_overpromotion")).length;
  const qualityGateRows = items.filter((row) => toNum(row?.quality_gate_applied, 0) === 1);
  const participateRows = items.filter((row) => normalizeParticipationDecision(row?.participation_decision) === "participate");
  const watchRows = items.filter((row) => normalizeParticipationDecision(row?.participation_decision) === "watch");
  const skipRows = items.filter((row) => normalizeParticipationDecision(row?.participation_decision) === "skip");
  const participateHitCount = participateRows.filter((row) => toNum(row?.hit_flag, 0) === 1).length;
  const watchHitCount = watchRows.filter((row) => toNum(row?.hit_flag, 0) === 1).length;
  const skipCorrectCount = skipRows.filter((row) => toNum(row?.hit_flag, 0) !== 1).length;
  const recommendedRows = items.filter((row) => deriveRecommendationLevel(row) === "recommended");
  const cautionRows = items.filter((row) => deriveRecommendationLevel(row) === "caution");
  const notRecommendedRows = items.filter((row) => deriveRecommendationLevel(row) === "not_recommended");
  const boat1EscapeRows = items.filter((row) => isBoat1EscapePredicted(row));
  const boat1EscapeHitCount = boat1EscapeRows.filter((row) => toNum(row?.head_hit, 0) === 1).length;
  const boat1EscapeOpponentHitCount = boat1EscapeRows.filter((row) => (
    toNum(row?.head_hit, 0) === 1 &&
    toNum(row?.second_place_miss, 0) === 0 &&
    toNum(row?.third_place_miss, 0) === 0
  )).length;

  return {
    verified_race_count: verifiedRaceCount,
    total_races: verifiedRaceCount,
    trifecta_hit_count: trifectaHitCount,
    trifecta_hit_rate: pct(trifectaHitCount, verifiedRaceCount),
    exacta_hit_count: exactaHitCount,
    exacta_hit_rate: pct(exactaHitCount, exactaRows.length),
    head_hit_count: headHitCount,
    head_hit_rate: pct(headHitCount, verifiedRaceCount),
    first_place_hit_count: headHitCount,
    first_place_hit_rate: pct(headHitCount, verifiedRaceCount),
    second_place_hit_count: secondPlaceHitCount,
    second_place_hit_rate: pct(secondPlaceHitCount, secondRows.length),
    third_place_hit_count: thirdPlaceHitCount,
    third_place_hit_rate: pct(thirdPlaceHitCount, thirdRows.length),
    second_third_swap_count: secondThirdSwapCount,
    near_miss_count: nearMissCount,
    partner_selection_miss_count: partnerSelectionMissCount,
    third_place_noise_count: thirdPlaceNoiseCount,
    boat1_survival_underestimated_count: boat1SurvivalUnderestimatedCount,
    outer_head_overpromotion_count: outerHeadOverpromotionCount,
    participate_race_count: participateRows.length,
    participated_races: participateRows.length,
    participate_only_hit_rate: pct(participateHitCount, participateRows.length),
    participation_hit_rate: pct(participateHitCount, participateRows.length),
    watch_race_count: watchRows.length,
    caution_race_count: watchRows.length,
    watch_hit_rate: pct(watchHitCount, watchRows.length),
    caution_only_hit_rate: pct(watchHitCount, watchRows.length),
    skip_race_count: skipRows.length,
    skipped_races: skipRows.length,
    skip_correct_count: skipCorrectCount,
    skip_correctness_rate: pct(skipCorrectCount, skipRows.length),
    recommended_race_count: recommendedRows.length,
    recommended_only_hit_rate: pct(
      recommendedRows.filter((row) => toNum(row?.hit_flag, 0) === 1).length,
      recommendedRows.length
    ),
    not_recommended_race_count: notRecommendedRows.length,
    not_recommended_participation_hit_rate: pct(
      notRecommendedRows.filter((row) => toNum(row?.hit_flag, 0) === 1).length,
      notRecommendedRows.length
    ),
    boat1_escape_prediction_count: boat1EscapeRows.length,
    boat1_escape_hit_count: boat1EscapeHitCount,
    boat1_escape_hit_rate: pct(boat1EscapeHitCount, boat1EscapeRows.length),
    boat1_escape_opponent_hit_count: boat1EscapeOpponentHitCount,
    boat1_escape_opponent_hit_rate: pct(boat1EscapeOpponentHitCount, boat1EscapeRows.length),
    quality_gate_applied_count: qualityGateRows.length,
    quality_gate_hit_rate: pct(
      qualityGateRows.filter((row) => toNum(row?.hit_flag, 0) === 1).length,
      qualityGateRows.length
    )
  };
}

function buildEvaluationTrend(rows, windowSize = 30) {
  const ordered = [...safeArray(rows)].sort((a, b) =>
    String(b?.verified_at || b?.prediction_timestamp || "").localeCompare(
      String(a?.verified_at || a?.prediction_timestamp || "")
    )
  );
  const recent = ordered.slice(0, windowSize);
  const previous = ordered.slice(windowSize, windowSize * 2);
  const recentMetrics = computeEvaluationMetrics(recent);
  const previousMetrics = computeEvaluationMetrics(previous);
  return {
    recent_window_size: recent.length,
    previous_window_size: previous.length,
    recent: recentMetrics,
    previous: previousMetrics,
    trifecta_hit_rate_delta: Number((recentMetrics.trifecta_hit_rate - previousMetrics.trifecta_hit_rate).toFixed(2)),
    exacta_hit_rate_delta: Number((recentMetrics.exacta_hit_rate - previousMetrics.exacta_hit_rate).toFixed(2)),
    head_hit_rate_delta: Number((recentMetrics.head_hit_rate - previousMetrics.head_hit_rate).toFixed(2))
  };
}

function buildEvaluationSegments(rows, learningRunId = null) {
  const segmentDefs = [
    { type: "venue", getKey: (row) => row?.venue_name || (row?.venue_id ? String(row.venue_id) : null) },
    { type: "formation_pattern", getKey: (row) => row?.formation_pattern || null },
    { type: "scenario_type", getKey: (row) => row?.scenario_type || null },
    { type: "attack_scenario", getKey: (row) => row?.attack_scenario_type || row?.attack_scenario_label || "none" },
    { type: "recommendation_level", getKey: (row) => deriveRecommendationLevel(row) },
    { type: "boat1_escape_confidence_bucket", getKey: (row) => confidenceBucketKey(row?.boat1_escape_probability) },
    { type: "outside_lead_flag", getKey: (row) => (hasOutsideLeadFlag(row) ? "outside_lead" : "non_outside_lead") },
    {
      type: "outside_head_recommendation_presence",
      getKey: (row) => (hasOutsideHeadRecommendation(row) ? "present" : "absent")
    },
    {
      type: "outside_head_promoted_5_6",
      getKey: (row) => {
        const lane = getMainPredictedHeadLane(row);
        if (lane === 5 || lane === 6) return `lane_${lane}`;
        return "not_promoted";
      }
    },
    { type: "boat1_head_mode", getKey: (row) => (deriveBoat1HeadEvaluation(row) ? "boat1_head" : "non_boat1_head") },
    { type: "f_hold_present", getKey: (row) => (toNum(row?.f_hold_lane_count, 0) > 0 ? "present" : "absent") },
    { type: "entry_change_present", getKey: (row) => (toNum(row?.entry_changed, 0) === 1 ? "present" : "absent") },
    { type: "prediction_mode", getKey: (row) => derivePredictionModeKey(row) },
    { type: "rebalance_version", getKey: (row) => row?.rebalance_version || "legacy_rebalance" },
    { type: "confidence_version", getKey: (row) => row?.confidence_version || "legacy_confidence" }
  ];

  const segments = [];

  for (const def of segmentDefs) {
    const grouped = new Map();
    for (const row of safeArray(rows)) {
      const key = def.getKey(row);
      if (!key) continue;
      const bucket = grouped.get(key) || [];
      bucket.push(row);
      grouped.set(key, bucket);
    }
    for (const [segmentKey, bucketRows] of grouped.entries()) {
      const metrics = computeEvaluationMetrics(bucketRows);
      segments.push({
        segment_type: def.type,
        segment_key: segmentKey,
        verified_race_count: metrics.verified_race_count,
        participation_count: metrics.participated_races,
        trifecta_hit_rate: metrics.trifecta_hit_rate,
        exacta_hit_rate: metrics.exacta_hit_rate,
        head_hit_rate: metrics.head_hit_rate,
        second_place_hit_rate: metrics.second_place_hit_rate,
        third_place_hit_rate: metrics.third_place_hit_rate,
        participation_hit_rate: metrics.participation_hit_rate,
        model_version: deriveModelVersion(
          [...bucketRows].sort((a, b) =>
            String(b?.verified_at || b?.prediction_timestamp || "").localeCompare(
              String(a?.verified_at || a?.prediction_timestamp || "")
            )
          )[0] || {}
        ),
        learning_run_id: learningRunId,
        metrics
      });
    }
  }

  return segments;
}

function pickSegmentHighlights(segments, segmentType, direction = "best", limit = 5, minSample = 5) {
  const filtered = safeArray(segments).filter(
    (segment) => segment?.segment_type === segmentType && toNum(segment?.verified_race_count, 0) >= minSample
  );
  const sorted = [...filtered].sort((a, b) => {
    const rateDiff = toNum(b?.trifecta_hit_rate, 0) - toNum(a?.trifecta_hit_rate, 0);
    if (rateDiff !== 0) return rateDiff;
    return toNum(b?.verified_race_count, 0) - toNum(a?.verified_race_count, 0);
  });
  return (direction === "best" ? sorted : sorted.reverse()).slice(0, limit);
}

function buildEvaluationComparisons(segments) {
  const types = ["rebalance_version", "confidence_version", "prediction_mode", "boat1_head_mode"];
  return types.reduce((acc, type) => {
    acc[type] = safeArray(segments)
      .filter((segment) => segment?.segment_type === type)
      .sort((a, b) => toNum(b?.verified_race_count, 0) - toNum(a?.verified_race_count, 0));
    return acc;
  }, {});
}

function buildEvaluationFilterOptions(rows) {
  return {
    venues: Array.from(new Set(safeArray(rows).map((row) => row?.venue_name).filter(Boolean))).sort((a, b) => a.localeCompare(b)),
    formation_patterns: Array.from(new Set(safeArray(rows).map((row) => row?.formation_pattern).filter(Boolean))).sort((a, b) => a.localeCompare(b)),
    recommendation_levels: ["recommended", "caution", "not_recommended"],
    attack_scenarios: Array.from(
      new Set(safeArray(rows).map((row) => row?.attack_scenario_type || row?.attack_scenario_label).filter(Boolean))
    ).sort((a, b) => a.localeCompare(b))
  };
}

function applyEvaluationFilters(rows, filters = {}) {
  return safeArray(rows).filter((row) => {
    const predictionDate = String(row?.prediction_timestamp || row?.verified_at || "").slice(0, 10);
    if (filters?.venue && filters.venue !== "all" && String(row?.venue_name || row?.venue_id || "") !== String(filters.venue)) {
      return false;
    }
    if (filters?.date_from && predictionDate && predictionDate < filters.date_from) return false;
    if (filters?.date_to && predictionDate && predictionDate > filters.date_to) return false;
    if (
      filters?.recommendation_level &&
      filters.recommendation_level !== "all" &&
      deriveRecommendationLevel(row) !== filters.recommendation_level
    ) {
      return false;
    }
    if (
      filters?.formation_pattern &&
      filters.formation_pattern !== "all" &&
      String(row?.formation_pattern || "") !== String(filters.formation_pattern)
    ) {
      return false;
    }
    if (toNum(filters?.only_participated, 0) === 1 && normalizeParticipationDecision(row?.participation_decision) !== "participate") {
      return false;
    }
    if (toNum(filters?.only_recommended, 0) === 1 && deriveRecommendationLevel(row) !== "recommended") {
      return false;
    }
    if (toNum(filters?.only_boat1_escape_predicted, 0) === 1 && !isBoat1EscapePredicted(row)) {
      return false;
    }
    if (toNum(filters?.only_outside_head_cases, 0) === 1 && !hasOutsideHeadRecommendation(row)) {
      return false;
    }
    return true;
  });
}

function persistEvaluationSnapshot(evaluation, segments) {
  const overall = evaluation?.overall || {};
  const latestRun = db
    .prepare(
      `
      SELECT id, latest_verified_at, verified_race_count, model_version, learning_run_id
      FROM evaluation_runs
      ORDER BY id DESC
      LIMIT 1
    `
    )
    .get();

  if (
    latestRun &&
    String(latestRun.latest_verified_at || "") === String(evaluation?.date_range?.latest_verified_at || "") &&
    toNum(latestRun.verified_race_count, 0) === toNum(overall.verified_race_count, 0) &&
    String(latestRun.model_version || "") === String(evaluation?.model_version || "") &&
    toNullableNum(latestRun.learning_run_id) === toNullableNum(evaluation?.learning_run_id)
  ) {
    return latestRun.id;
  }

  const insertRun = db.prepare(
    `
    INSERT INTO evaluation_runs (
      date_range_start,
      date_range_end,
      latest_verified_at,
      verified_race_count,
      trifecta_hit_rate,
      exacta_hit_rate,
      head_hit_rate,
      second_place_hit_rate,
      third_place_hit_rate,
      model_version,
      learning_run_id,
      summary_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `
  );

  const runResult = insertRun.run(
    evaluation?.date_range?.start || null,
    evaluation?.date_range?.end || null,
    evaluation?.date_range?.latest_verified_at || null,
    toNum(overall.verified_race_count, 0),
    toNum(overall.trifecta_hit_rate, 0),
    toNum(overall.exacta_hit_rate, 0),
    toNum(overall.head_hit_rate, 0),
    toNum(overall.second_place_hit_rate, 0),
    toNum(overall.third_place_hit_rate, 0),
    evaluation?.model_version || null,
    toNullableNum(evaluation?.learning_run_id),
    JSON.stringify(evaluation || {})
  );
  const evaluationRunId = Number(runResult.lastInsertRowid);

  const insertSegment = db.prepare(
    `
    INSERT INTO evaluation_segments (
      evaluation_run_id,
      segment_type,
      segment_key,
      model_version,
      learning_run_id,
      verified_race_count,
      trifecta_hit_rate,
      exacta_hit_rate,
      head_hit_rate,
      second_place_hit_rate,
      third_place_hit_rate,
      metrics_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `
  );

  const insertMany = db.transaction((segmentRows) => {
    for (const segment of segmentRows) {
      insertSegment.run(
        evaluationRunId,
        segment.segment_type,
        segment.segment_key,
        segment.model_version || null,
        toNullableNum(segment.learning_run_id),
        toNum(segment.verified_race_count, 0),
        toNum(segment.trifecta_hit_rate, 0),
        toNum(segment.exacta_hit_rate, 0),
        toNum(segment.head_hit_rate, 0),
        toNum(segment.second_place_hit_rate, 0),
        toNum(segment.third_place_hit_rate, 0),
        JSON.stringify(segment.metrics || {})
      );
    }
  });
  insertMany(safeArray(segments));

  return evaluationRunId;
}

function buildEvaluationSummary(verifiedRows, options = {}) {
  const allRows = safeArray(verifiedRows);
  const filters = options?.filters || {};
  const rows = applyEvaluationFilters(allRows, filters);
  const latestLearningRun = getLatestLearningRun();
  const learningRunId = toNullableNum(latestLearningRun?.run_id);
  const overall = computeEvaluationMetrics(rows);
  const orderedRows = [...rows].sort((a, b) =>
    String(a?.verified_at || a?.prediction_timestamp || "").localeCompare(
      String(b?.verified_at || b?.prediction_timestamp || "")
    )
  );
  const dateRange = {
    start: orderedRows[0]?.prediction_timestamp?.slice?.(0, 10) || orderedRows[0]?.verified_at?.slice?.(0, 10) || null,
    end:
      orderedRows[orderedRows.length - 1]?.prediction_timestamp?.slice?.(0, 10) ||
      orderedRows[orderedRows.length - 1]?.verified_at?.slice?.(0, 10) ||
      null,
    latest_verified_at: orderedRows[orderedRows.length - 1]?.verified_at || null
  };
  const latestRow = [...rows].sort((a, b) =>
    String(b?.verified_at || b?.prediction_timestamp || "").localeCompare(
      String(a?.verified_at || a?.prediction_timestamp || "")
    )
  )[0] || {};
  const modelVersion = deriveModelVersion(latestRow);
  const segments = buildEvaluationSegments(rows, learningRunId);
  const evaluation = {
    evaluation_run_id: null,
    date_range: dateRange,
    filters,
    filtered_race_count: rows.length,
    total_available_race_count: allRows.length,
    filter_options: buildEvaluationFilterOptions(allRows),
    overall,
    recent_trend: buildEvaluationTrend(rows),
    highlights: {
      strongest_venues: pickSegmentHighlights(segments, "venue", "best"),
      weakest_venues: pickSegmentHighlights(segments, "venue", "worst"),
      strongest_formations: pickSegmentHighlights(segments, "formation_pattern", "best"),
      weakest_formations: pickSegmentHighlights(segments, "formation_pattern", "worst")
    },
    comparisons: buildEvaluationComparisons(segments),
    confidence_calibration: {
      bet_confidence_bins: buildConfidenceCalibration(rows, "bet_confidence", "hit_flag"),
      head_confidence_bins: buildConfidenceCalibration(rows, "head_confidence", "head_hit"),
      boat1_escape_confidence_bins: buildConfidenceCalibration(
        rows.filter((row) => isBoat1EscapePredicted(row)),
        "boat1_escape_probability",
        "head_hit"
      )
    },
    miss_categories: buildMissCategoryRows(rows),
    outside_head_monitoring: buildOutsideHeadMonitoring(rows),
    boat1_escape_diagnostics: buildBoat1EscapeDiagnostics(rows),
    segmented_tables: {
      venue: safeArray(segments).filter((segment) => segment.segment_type === "venue"),
      formation_pattern: safeArray(segments).filter((segment) => segment.segment_type === "formation_pattern"),
      attack_scenario: safeArray(segments).filter((segment) => segment.segment_type === "attack_scenario"),
      recommendation_level: safeArray(segments).filter((segment) => segment.segment_type === "recommendation_level"),
      boat1_escape_confidence_bucket: safeArray(segments).filter(
        (segment) => segment.segment_type === "boat1_escape_confidence_bucket"
      ),
      outside_lead_flag: safeArray(segments).filter((segment) => segment.segment_type === "outside_lead_flag"),
      outside_head_recommendation_presence: safeArray(segments).filter(
        (segment) => segment.segment_type === "outside_head_recommendation_presence"
      ),
      outside_head_promoted_5_6: safeArray(segments).filter(
        (segment) => segment.segment_type === "outside_head_promoted_5_6"
      )
    },
    segment_counts: segments.reduce((acc, segment) => {
      acc[segment.segment_type] = (acc[segment.segment_type] || 0) + 1;
      return acc;
    }, {}),
    model_version: modelVersion,
    learning_run_id: learningRunId,
    evaluation_created_at: new Date().toISOString()
  };
  const shouldPersist = !Object.values(filters).some((value) => {
    if (value === null || value === undefined || value === "" || value === "all") return false;
    return !(Number(value) === 0);
  });
  const evaluationRunId = shouldPersist ? persistEvaluationSnapshot(evaluation, segments) : null;
  return {
    ...evaluation,
    evaluation_run_id: evaluationRunId,
    segments
  };
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function normalizeExactaCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  const lanes = digits.slice(0, 2);
  if (lanes.length !== 2) return "";
  if (lanes[0] === lanes[1]) return "";
  return lanes.join("-");
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

function normalizeSavedExactaSnapshotItems(items) {
  return safeArray(items)
    .map((row) => {
      const combo = normalizeExactaCombo(row?.combo ?? row);
      if (!combo || combo.split("-").length !== 2) return null;
      return {
        ...(row && typeof row === "object" ? row : {}),
        combo,
        prob: Number.isFinite(Number(row?.prob)) ? Number(row.prob) : null,
        odds: Number.isFinite(Number(row?.odds)) ? Number(row.odds) : null,
        ev: Number.isFinite(Number(row?.ev)) ? Number(row.ev) : null,
        recommended_bet: Number.isFinite(Number(row?.recommended_bet))
          ? Number(row.recommended_bet)
          : Number.isFinite(Number(row?.bet))
            ? Number(row.bet)
            : 100,
        exacta_head_score: Number.isFinite(Number(row?.exacta_head_score)) ? Number(row.exacta_head_score) : null,
        exacta_partner_score: Number.isFinite(Number(row?.exacta_partner_score)) ? Number(row.exacta_partner_score) : null,
        exacta_reason_tags: Array.isArray(row?.exacta_reason_tags) ? [...row.exacta_reason_tags] : [],
        explanation_tags: Array.isArray(row?.explanation_tags) ? [...row.explanation_tags] : []
      };
    })
    .filter(Boolean);
}

function estimateUnifiedTicketHitRate(ticket, ticketType, tier) {
  const directProb = toNum(ticket?.prob ?? ticket?.estimated_hit_rate, 0);
  if (directProb > 0) return Number(clamp(0, 1, directProb).toFixed(4));
  if (ticketType === "exacta") {
    const head = toNum(ticket?.exacta_head_score, 0);
    const partner = toNum(ticket?.exacta_partner_score, 0);
    return Number(clamp(0, 1, (head * 0.58 + partner * 0.42) / 100).toFixed(4));
  }
  const boat1Head = toNum(ticket?.boat1_head_score, 0);
  const tierBoost = tier === "main" ? 0.02 : tier === "cover" ? 0.01 : 0;
  return Number(clamp(0, 1, boat1Head / 100 + tierBoost).toFixed(4));
}

function collectAllTicketCandidates({
  finalRecommendedBets,
  exactaBets,
  backupUrasujiBets
}) {
  const tierRank = { main: 3, cover: 2, backup: 1 };
  const makeRows = (items, ticketType, tier) =>
    safeArray(items)
      .map((ticket) => {
        const combo = ticketType === "exacta"
          ? normalizeExactaCombo(ticket?.combo ?? ticket)
          : normalizeCombo(ticket?.combo ?? ticket);
        if (!combo) return null;
        return {
          ticket_type: ticketType,
          ticket: combo,
          estimated_hit_rate: estimateUnifiedTicketHitRate(ticket, ticketType, tier),
          recommendation_tier: tier,
          recommendation_tier_rank: tierRank[tier] || 0,
          reason_tags: Array.isArray(ticket?.explanation_tags)
            ? ticket.explanation_tags
            : Array.isArray(ticket?.exacta_reason_tags)
              ? ticket.exacta_reason_tags
              : Array.isArray(ticket?.trap_flags)
                ? ticket.trap_flags
                : [],
          source_row: ticket
        };
      })
      .filter(Boolean);
  return [
    ...makeRows(finalRecommendedBets, "trifecta", "main"),
    ...makeRows(exactaBets, "exacta", "cover"),
    ...makeRows(backupUrasujiBets, "trifecta", "backup")
  ];
}

function rankTicketCandidatesByHitRate(rows = []) {
  return [...safeArray(rows)].sort((a, b) => {
    const hitDiff = toNum(b?.estimated_hit_rate, 0) - toNum(a?.estimated_hit_rate, 0);
    if (Math.abs(hitDiff) > 0.0005) return hitDiff;
    const tierDiff = toNum(b?.recommendation_tier_rank, 0) - toNum(a?.recommendation_tier_rank, 0);
    if (tierDiff !== 0) return tierDiff;
    if (String(a?.ticket_type || "") !== String(b?.ticket_type || "")) {
      return String(a?.ticket_type || "") === "exacta" ? -1 : 1;
    }
    return String(a?.ticket || "").localeCompare(String(b?.ticket || ""));
  });
}

function buildTopRecommendedTickets({
  finalRecommendedBets,
  exactaBets,
  backupUrasujiBets,
  maxItems = 10
}) {
  const deduped = new Map();
  for (const row of collectAllTicketCandidates({ finalRecommendedBets, exactaBets, backupUrasujiBets })) {
    if (String(row?.ticket_type || "") !== "trifecta") continue;
    const key = `${row.ticket_type}:${row.ticket}`;
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, row);
      continue;
    }
    deduped.set(key, rankTicketCandidatesByHitRate([existing, row])[0]);
  }
  return rankTicketCandidatesByHitRate([...deduped.values()])
    .slice(0, Math.max(1, Math.min(10, toInt(maxItems, 10))))
    .map((row, idx) => ({
      rank: idx + 1,
      ticket_type: row.ticket_type,
      ticket: row.ticket,
      estimated_hit_rate: row.estimated_hit_rate,
      recommendation_tier: row.recommendation_tier,
      reason_tags: row.reason_tags
    }));
}

function computeUpsetRiskScore({
  confidenceScores,
  participationDecision,
  roleProbabilityLayers,
  attackScenarioAnalysis,
  headScenarioBalanceAnalysis,
  outsideHeadPromotionGate
}) {
  const scoreSource = participationDecision?.participation_score_components || {};
  const headConfidence = toNum(
    confidenceScores?.head_fixed_confidence_pct ?? confidenceScores?.head_confidence_calibrated,
    0
  );
  const recommendationConfidence = toNum(
    confidenceScores?.recommended_bet_confidence_pct ?? confidenceScores?.bet_confidence_calibrated,
    0
  );
  const firstRows = safeArray(roleProbabilityLayers?.first_place_probability_json);
  const secondRows = safeArray(roleProbabilityLayers?.second_place_probability_json);
  const thirdRows = safeArray(roleProbabilityLayers?.third_place_probability_json);
  const spread = (rows, take) => {
    const top = toNum(rows[0]?.weight, 0);
    const others = safeArray(rows).slice(1, take).reduce((sum, row) => sum + toNum(row?.weight, 0), 0);
    return Math.max(0, top - others) * 100;
  };
  const chaosRisk = Math.max(
    0,
    100 - toNum(scoreSource?.race_stability_score, 0),
    100 - toNum(scoreSource?.prediction_readability_score, 0)
  );
  const partnerNoise = Math.max(0, 100 - toNum(scoreSource?.partner_clarity_score, 0));
  const attackPressure = toNum(attackScenarioAnalysis?.attack_scenario_score, 0) * 0.18;
  const boat1EscapeProbability = toNum(roleProbabilityLayers?.boat1_escape_probability, 0);
  const innerCollapseScore = toNum(outsideHeadPromotionGate?.inner_collapse_score, 0);
  const venueLaunchCalibration =
    headScenarioBalanceAnalysis?.launch_venue_calibration_json ||
    getVenueLaunchMicroCalibration({
      race: null,
      venueSummary: headScenarioBalanceAnalysis?.venue_correction_summary
    });
  const venueUpsetBias = toNum(venueLaunchCalibration?.values?.upset_risk_bias, 0);
  const outerEvidence = [5, 6].reduce((acc, lane) => {
    const row = outsideHeadPromotionGate?.by_lane?.[String(lane)] || outsideHeadPromotionGate?.by_lane?.[lane] || {};
    return Math.max(acc, safeArray(row?.matched_evidence_categories).length);
  }, 0);
  return Number(
    clamp(
      0,
      100,
      chaosRisk * 0.28 +
        partnerNoise * 0.22 +
        Math.max(0, 70 - headConfidence) * 0.18 +
        Math.max(0, 62 - recommendationConfidence) * 0.1 +
        Math.max(0, 0.62 - boat1EscapeProbability) * 100 * 0.16 +
        Math.max(0, 14 - spread(firstRows, 2)) * 0.8 +
        Math.max(0, 12 - spread(secondRows, 2)) * 0.55 +
        Math.max(0, 10 - spread(thirdRows, 3)) * 0.35 +
        attackPressure +
        outerEvidence * 2.6 +
        innerCollapseScore * 0.22 +
        venueUpsetBias +
        (toInt(scoreSource?.quality_gate_applied, 0) === 1 ? 12 : 0)
    ).toFixed(2)
  );
}

function shouldShowUpsetAlert({
  upsetRiskScore,
  confidenceScores,
  boat1EscapeProbability,
  participationDecision
}) {
  const headConfidence = toNum(
    confidenceScores?.head_fixed_confidence_pct ?? confidenceScores?.head_confidence_calibrated,
    0
  );
  const recommendationState = String(participationDecision?.decision || "").toLowerCase();
  if (upsetRiskScore >= 78) return true;
  if (upsetRiskScore < 62) return false;
  if (headConfidence >= 84 && boat1EscapeProbability >= 0.72 && recommendationState === "recommended") return false;
  return true;
}

function buildUpsetAlert({
  upsetRiskScore,
  showUpsetAlert,
  attackScenarioAnalysis,
  escapePatternAnalysis,
  roleProbabilityLayers,
  outsideHeadPromotionGate,
  isRecommendedRace,
  backupUrasujiBets,
  finalRecommendedBets
}) {
  if (!showUpsetAlert) {
    return {
      shown: false,
      level: null,
      reasons: [],
      warning_boats: [],
      likely_scenario: null,
      reference_tickets: [],
      reference_only: false,
      score: upsetRiskScore
    };
  }
  const firstRows = safeArray(roleProbabilityLayers?.first_place_probability_json);
  const secondRows = safeArray(roleProbabilityLayers?.second_place_probability_json);
  const gateByLane = outsideHeadPromotionGate?.by_lane || {};
  const warningBoats = Array.from(new Set([
    ...firstRows.filter((row) => toInt(row?.lane, null) >= 4 && toNum(row?.weight, 0) >= 0.1).map((row) => toInt(row?.lane, null)),
    ...secondRows.filter((row) => toInt(row?.lane, null) >= 4 && toNum(row?.weight, 0) >= 0.12).map((row) => toInt(row?.lane, null))
  ])).slice(0, 4);
  const reasons = [];
  if (String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead") reasons.push("outside-lead shape is active");
  if (attackScenarioAnalysis?.attack_scenario_label) reasons.push(`attack scenario pressure: ${attackScenarioAnalysis.attack_scenario_label}`);
  if (toNum(outsideHeadPromotionGate?.inner_collapse_score, 0) >= 58) reasons.push("inner collapse evidence is elevated");
  if (warningBoats.some((lane) => safeArray(gateByLane?.[String(lane)]?.matched_evidence_categories).length >= 3)) {
    reasons.push("multiple outside evidence groups are aligned");
  }
  const reference_tickets = normalizeSavedBetSnapshotItems(
    safeArray(backupUrasujiBets).length > 0
      ? backupUrasujiBets
      : safeArray(finalRecommendedBets).filter((row) => {
          const firstLane = toInt(String(row?.combo || "").split("-")[0], null);
          return firstLane >= 4;
        })
  ).slice(0, isRecommendedRace ? 3 : 2);
  return {
    shown: true,
    level: upsetRiskScore >= 78 ? "大穴警戒" : "穴注意",
    reasons: reasons.slice(0, 3),
    warning_boats: warningBoats,
    likely_scenario: attackScenarioAnalysis?.attack_scenario_label || escapePatternAnalysis?.formation_pattern || "mixed upset pressure",
    reference_tickets,
    reference_only: !isRecommendedRace || upsetRiskScore >= 78,
    score: upsetRiskScore
  };
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
      Math.max(0, toNum(f.lap_attack_strength, 0)) * 0.4 +
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
      toNum(f.slit_alert_flag, 0) * 6 +
      Math.max(0, toNum(f.lap_time_delta_vs_front, 0)) * 12 +
      toNum(f.lap_attack_flag, 0) * 4
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
  const actualHead = Number(actualTop3[0]);
  const actualSecond = Number(actualTop3[1]);
  const actualThird = Number(actualTop3[2]);

  const headCorrect =
    predictedTop3.length > 0 && actualTop3.length > 0 && Number(predictedTop3[0]) === Number(actualTop3[0]);
  if (headCorrect) categories.push("HEAD_HIT");
  else categories.push("HEAD_MISS");

  const secondCorrect =
    predictedTop3.length >= 2 &&
    actualTop3.length >= 2 &&
    Number(predictedTop3[1]) === actualSecond;
  const thirdCorrect =
    predictedTop3.length >= 3 &&
    actualTop3.length >= 3 &&
    Number(predictedTop3[2]) === actualThird;
  const secondThirdCorrect =
    secondCorrect && thirdCorrect;
  if (!secondThirdCorrect) categories.push("PARTNER_MISS");

  const canonicalCombos = recBets
    .map((combo) => combo.split("-").map((value) => toInt(value, null)))
    .filter((lanes) => lanes.length === 3 && lanes.every((lane) => Number.isInteger(lane)));
  const combosWithCorrectHead = canonicalCombos.filter((lanes) => lanes[0] === actualHead);
  const hasCorrectSecondWithHead = combosWithCorrectHead.some((lanes) => lanes[1] === actualSecond);
  const hasCorrectThirdWithHead = combosWithCorrectHead.some((lanes) => lanes[2] === actualThird);
  const hasSecondThirdSwap = combosWithCorrectHead.some(
    (lanes) => lanes[1] === actualThird && lanes[2] === actualSecond
  );
  const hasNearStructureWithHead = combosWithCorrectHead.some((lanes) => {
    const actualTail = [actualSecond, actualThird].sort((a, b) => a - b).join("-");
    const predictedTail = [lanes[1], lanes[2]].sort((a, b) => a - b).join("-");
    return actualTail === predictedTail;
  });
  const actualSecondSeenAnywhere = canonicalCombos.some((lanes) => lanes[1] === actualSecond || lanes[2] === actualSecond);
  const actualThirdSeenAnywhere = canonicalCombos.some((lanes) => lanes[2] === actualThird || lanes[1] === actualThird);

  const secondPlaceMiss =
    actualTop3.length >= 2 &&
    (!secondCorrect || (headCorrect && combosWithCorrectHead.length > 0 && !hasCorrectSecondWithHead));
  const thirdPlaceMiss =
    actualTop3.length >= 3 &&
    (!thirdCorrect || (headCorrect && combosWithCorrectHead.length > 0 && !hasCorrectThirdWithHead));
  const secondThirdSwap =
    actualTop3.length >= 3 &&
    !secondThirdCorrect &&
    hasSecondThirdSwap;
  const structureNearButOrderMiss =
    actualTop3.length >= 3 &&
    !secondThirdCorrect &&
    hasNearStructureWithHead;
  const partnerSelectionMiss =
    actualTop3.length >= 2 &&
    actualHead === 1 &&
    headCorrect &&
    secondPlaceMiss &&
    !hasCorrectSecondWithHead &&
    !actualSecondSeenAnywhere;
  const thirdPlaceNoise =
    actualTop3.length >= 3 &&
    thirdPlaceMiss &&
    !hasCorrectThirdWithHead &&
    !actualThirdSeenAnywhere;
  const predictedHead = Number(predictedTop3[0]);
  const predictedSecond = Number(predictedTop3[1]);
  const predictedThird = Number(predictedTop3[2]);
  const boat1InsidePartnerUnderweighted =
    actualHead === 1 &&
    secondPlaceMiss &&
    [2, 3, 4].includes(actualSecond) &&
    !hasCorrectSecondWithHead;
  const boat1InsidePartnerOverweighted =
    predictedHead === 1 &&
    predictedSecond >= 2 &&
    predictedSecond <= 4 &&
    actualHead === 1 &&
    actualSecond >= 5;

  if (secondPlaceMiss) categories.push("second_place_miss");
  if (thirdPlaceMiss) categories.push("third_place_miss");
  if (partnerSelectionMiss) categories.push("partner_selection_miss");
  if (thirdPlaceNoise) categories.push("third_place_noise");
  if (secondThirdSwap) categories.push("second_third_swap");
  if (structureNearButOrderMiss) categories.push("structure_near_but_order_miss");
  if (boat1InsidePartnerUnderweighted) categories.push("boat1_inside_partner_underweighted");
  if (boat1InsidePartnerOverweighted) categories.push("boat1_inside_partner_overweighted");
  const attackScenarioType = String(predictionJson?.attack_scenario_type || "").trim();
  const attackScenarioApplied = Number(predictionJson?.attack_scenario_applied) === 1;
  const survivalResidualScore = toNum(
    predictionJson?.boat1_survival_residual_score ?? predictionJson?.survival_residual_score,
    0
  );
  const outerAttackPredicted = predictedHead === 5 || predictedHead === 6;
  const actualOuterHead = actualHead === 5 || actualHead === 6;
  const boat1SurvivalUnderestimated =
    actualHead === 1 &&
    !headCorrect &&
    survivalResidualScore >= 30 &&
    predictedHead !== 1;
  const outerHeadOverpromotion =
    hitMiss === "MISS" &&
    outerAttackPredicted &&
    !actualOuterHead &&
    survivalResidualScore >= 24;
  const attackScenarioOverweight =
    hitMiss === "MISS" &&
    attackScenarioApplied &&
    !!attackScenarioType &&
    predictedHead !== actualHead;
  const attackScenarioUnderweight =
    hitMiss === "MISS" &&
    !!attackScenarioType &&
    actualHead !== 1 &&
    predictedHead === 1;
  const attackReadCorrectButFinishWrong =
    hitMiss === "MISS" &&
    attackScenarioApplied &&
    !!attackScenarioType &&
    ((attackScenarioType.includes("three") && actualTop3.includes(3)) ||
      (attackScenarioType.includes("four") && actualTop3.includes(4)) ||
      (attackScenarioType.includes("two") && actualTop3.includes(2))) &&
    predictedCombo !== actualCombo;
  const boat1EscapeCorrectButOpponentWrong =
    actualHead === 1 &&
    predictedHead === 1 &&
    hitMiss === "MISS" &&
    (secondPlaceMiss || thirdPlaceMiss);

  if (boat1SurvivalUnderestimated) categories.push("boat1_survival_underestimated");
  if (outerHeadOverpromotion) categories.push("outer_head_overpromotion");
  if (attackScenarioOverweight) categories.push("attack_scenario_overweight");
  if (attackScenarioUnderweight) categories.push("attack_scenario_underweight");
  if (attackReadCorrectButFinishWrong) categories.push("attack_read_correct_but_finish_wrong");
  if (boat1EscapeCorrectButOpponentWrong) categories.push("boat1_escape_correct_but_opponent_wrong");

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

  const learningAdjustmentReasonTags = [];
  if (partnerSelectionMiss) learningAdjustmentReasonTags.push("REFINE_SECOND_PLACE_PARTNER");
  if (thirdPlaceMiss) learningAdjustmentReasonTags.push("REFINE_THIRD_PLACE_RESIDUAL");
  if (thirdPlaceNoise) learningAdjustmentReasonTags.push("SUPPRESS_THIRD_PLACE_NOISE");
  if (secondThirdSwap || structureNearButOrderMiss) learningAdjustmentReasonTags.push("REFINE_SECOND_THIRD_ORDER");
  if (actualHead === 1 && (partnerSelectionMiss || thirdPlaceMiss)) {
    learningAdjustmentReasonTags.push("REFINE_BOAT1_ESCAPE_PARTNER_SEARCH");
  }
  if (boat1InsidePartnerUnderweighted) learningAdjustmentReasonTags.push("STRENGTHEN_1_2_3_4_PARTNER_FAMILY");
  if (boat1InsidePartnerOverweighted) learningAdjustmentReasonTags.push("RELAX_INNER_PARTNER_FAMILY");
  if (boat1SurvivalUnderestimated) learningAdjustmentReasonTags.push("STRENGTHEN_BOAT1_SURVIVAL_GUARD");
  if (outerHeadOverpromotion) learningAdjustmentReasonTags.push("SUPPRESS_OUTER_HEAD_PROMOTION");
  if (attackScenarioOverweight) learningAdjustmentReasonTags.push("REDUCE_ATTACK_SCENARIO_WEIGHT");
  if (attackScenarioUnderweight) learningAdjustmentReasonTags.push("RESTORE_ATTACK_COUNTER_COVERAGE");
  if (attackReadCorrectButFinishWrong) learningAdjustmentReasonTags.push("SEPARATE_ATTACK_FROM_FINISH_ORDER");
  if (boat1EscapeCorrectButOpponentWrong) learningAdjustmentReasonTags.push("REFINE_BOAT1_ESCAPE_OPPONENT_MODEL");

  const missPatternTags = [];
  missPatternTags.push(headCorrect ? "head_hit" : "head_miss");
  if (actualTop3.length >= 2) missPatternTags.push(secondCorrect ? "second_place_hit" : "second_place_miss");
  if (actualTop3.length >= 3) missPatternTags.push(thirdCorrect ? "third_place_hit" : "third_place_miss");
  if (secondThirdSwap) missPatternTags.push("second_third_swap");
  if (partnerSelectionMiss) missPatternTags.push("partner_selection_miss");
  if (thirdPlaceNoise) missPatternTags.push("third_place_noise");
  if (structureNearButOrderMiss) missPatternTags.push("structure_near_miss");
  if (boat1InsidePartnerUnderweighted) missPatternTags.push("boat1_inside_partner_underweighted");
  if (boat1InsidePartnerOverweighted) missPatternTags.push("boat1_inside_partner_overweighted");
  if (actualCombo && !hitMatchFound && hasNearStructureWithHead) missPatternTags.push("structure_near_but_order_miss");
  const exactaHit =
    predictedTop3.length >= 2 &&
    actualTop3.length >= 2 &&
    predictedHead === actualHead &&
    predictedSecond === actualSecond;
  if (exactaHit) missPatternTags.push("exacta_hit");
  else if (actualTop3.length >= 2) missPatternTags.push("exacta_miss");
  if (boat1SurvivalUnderestimated) missPatternTags.push("boat1_survival_underestimated");
  if (outerHeadOverpromotion) missPatternTags.push("outer_head_overpromotion");
  if (attackScenarioOverweight) missPatternTags.push("attack_scenario_overweight");
  if (attackScenarioUnderweight) missPatternTags.push("attack_scenario_underweight");
  if (attackReadCorrectButFinishWrong) missPatternTags.push("attack_read_correct_but_finish_wrong");
  if (boat1EscapeCorrectButOpponentWrong) missPatternTags.push("boat1_escape_correct_but_opponent_wrong");

  return {
    hit_miss: hitMiss,
    miss_head: !headCorrect,
    miss_second: secondPlaceMiss,
    miss_third: thirdPlaceMiss,
    head_correct: headCorrect,
    second_place_correct: secondCorrect,
    third_place_correct: thirdCorrect,
    second_third_correct: secondThirdCorrect,
    second_place_miss: secondPlaceMiss,
    third_place_miss: thirdPlaceMiss,
    partner_selection_miss: partnerSelectionMiss,
    third_place_noise: thirdPlaceNoise,
    second_third_swap: secondThirdSwap,
    structure_near_but_order_miss: structureNearButOrderMiss,
    attack_read_correct_but_finish_wrong: attackReadCorrectButFinishWrong,
    boat1_escape_correct_but_opponent_wrong: boat1EscapeCorrectButOpponentWrong,
    categories: [...new Set(categories)],
    miss_pattern_tags: [...new Set(missPatternTags)],
    learning_adjustment_reason_tags: [...new Set(learningAdjustmentReasonTags)],
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

function isValidCanonicalEntryOrder(order) {
  return Array.isArray(order) &&
    order.length === 6 &&
    order.every((lane) => Number.isInteger(lane) && lane >= 1 && lane <= 6) &&
    new Set(order).size === 6;
}

function buildCanonicalEntryOrderMeta(racers, actualEntrySource = null) {
  const rows = Array.isArray(racers) ? racers : [];
  const predicted_entry_order = rows
    .map((r) => toInt(r?.lane))
    .filter((v) => Number.isInteger(v) && v >= 1 && v <= 6)
    .sort((a, b) => a - b);
  const sourceOrder = Array.isArray(actualEntrySource?.parsed_actual_entry_order)
    ? actualEntrySource.parsed_actual_entry_order.map((lane) => toInt(lane, null)).filter(Number.isInteger)
    : [];
  const validationOk = actualEntrySource?.validation_ok === true && isValidCanonicalEntryOrder(sourceOrder);
  const actual_entry_order = validationOk ? sourceOrder : predicted_entry_order;
  const laneToEntry = new Map(actual_entry_order.map((lane, idx) => [lane, idx + 1]));
  const actual_lane_map = Object.fromEntries(actual_entry_order.map((lane, idx) => [String(lane), idx + 1]));
  const fallback_used = !validationOk;
  const fallback_reason = validationOk ? null : actualEntrySource?.fallback_reason || actualEntrySource?.validation_error || "predicted_order_fallback";
  const raw_actual_entry_source_text = actualEntrySource?.raw_actual_entry_source_text || null;
  const raw_text_by_lane =
    actualEntrySource?.raw_text_by_lane && typeof actualEntrySource.raw_text_by_lane === "object"
      ? actualEntrySource.raw_text_by_lane
      : {};
  const validation = {
    validation_ok: validationOk,
    every_boat_once: new Set(actual_entry_order).size === actual_entry_order.length,
    lanes_1_to_6_once: isValidCanonicalEntryOrder(actual_entry_order),
    ui_order_matches_actual_entry: isValidCanonicalEntryOrder(actual_entry_order),
    validation_error: validationOk ? null : fallback_reason
  };
  const per_boat_lane_map = Object.fromEntries(
    predicted_entry_order.map((lane) => {
      const actualLane = Number(laneToEntry.get(lane) || lane);
      return [String(lane), {
        boat: lane,
        original_lane: lane,
        actual_lane: actualLane,
        course_change_occurred: actualLane !== lane
      }];
    })
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
    actual_lane_map,
    entry_changed,
    entry_change_type,
    changed_count: changedCount,
    max_shift: maxShift,
    severity,
    authoritative_source: actualEntrySource?.authoritative_source || "official_beforeinfo_entry_course",
    raw_actual_entry_source_text,
    raw_text_by_lane,
    fallback_used,
    fallback_reason,
    validation,
    per_boat_lane_map
  };
}

function isFiniteNumber(value) {
  return Number.isFinite(Number(value));
}

function inferSourceUrlForAudit(source = {}, sourceType = null) {
  if (!source || typeof source !== "object") return null;
  if (sourceType === "kyoteibiyori") {
    const tried = source?.kyotei_biyori?.tried_urls;
    return Array.isArray(tried) && tried.length > 0 ? tried[0] : null;
  }
  if (sourceType === "official_pre_race") {
    const candidates = [
      source?.official_fetch_status?.beforeinfo_url,
      source?.official_fetch_status?.racelist_url
    ];
    return candidates.find((value) => value) || null;
  }
  return null;
}

function buildAuditField({
  label,
  sourceType = null,
  sourceUrl = null,
  sourceSection = null,
  sourceRowLabel = null,
  sourceBoatColumn = null,
  rawCellText = null,
  parsedRawText = null,
  normalizedValue = null,
  backendField = null,
  frontendField = null,
  predictionUsableField = null,
  validationPassed = false,
  predictionUsable = false,
  reason = null
}) {
  return {
    label,
    source_type: sourceType,
    source_url: sourceUrl,
    matched_section_label: sourceSection,
    matched_row_label: sourceRowLabel,
    matched_boat_column: sourceBoatColumn,
    raw_cell_text: rawCellText,
    parsed_raw_text: parsedRawText,
    normalized_value: normalizedValue,
    backend_field: backendField,
    frontend_display_field: frontendField,
    prediction_usable_field: predictionUsableField,
    validation_passed: !!validationPassed,
    prediction_usable: !!predictionUsable,
    reason: reason || (validationPassed ? "verified" : "unverified")
  };
}

function buildMetaAuditField({
  label,
  meta,
  fallbackValue,
  backendField,
  frontendField,
  predictionUsableField,
  source,
  fallbackSourceType = "kyoteibiyori"
}) {
  const value = toNullableNum(meta?.value ?? fallbackValue);
  const sourceType = meta?.source
    ? String(meta.source).includes("boatrace") ? "official_pre_race" : fallbackSourceType
    : null;
  const validationPassed = meta?.is_usable === true;
  return buildAuditField({
    label,
    sourceType,
    sourceUrl: inferSourceUrlForAudit(source, sourceType),
    sourceSection: meta?.source_section || null,
    sourceRowLabel: meta?.source_row_label || null,
    sourceBoatColumn: meta?.source_boat_column || null,
    rawCellText: meta?.raw_cell_text ?? null,
    parsedRawText: meta?.raw_cell_text ?? null,
    normalizedValue: value,
    backendField,
    frontendField,
    predictionUsableField,
    validationPassed,
    predictionUsable: validationPassed,
    reason: meta?.reason || (validationPassed ? "verified" : "unverified")
  });
}

function buildRangeAuditField({
  label,
  value,
  backendField,
  frontendField,
  predictionUsableField,
  lane,
  sourceType = "official_racelist",
  sourceSection = "racelist_row",
  sourceRowLabel = null,
  min = null,
  max = null
}) {
  const numeric = toNullableNum(value);
  const validationPassed =
    numeric !== null &&
    (min === null || numeric >= min) &&
    (max === null || numeric <= max);
  return buildAuditField({
    label,
    sourceType,
    sourceSection,
    sourceRowLabel,
    sourceBoatColumn: lane,
    rawCellText: value ?? null,
    parsedRawText: value ?? null,
    normalizedValue: numeric,
    backendField,
    frontendField,
    predictionUsableField,
    validationPassed,
    predictionUsable: validationPassed,
    reason: validationPassed ? "range_validated" : "missing_or_out_of_range"
  });
}

function buildStrictDataAudit({ data, entryMeta, hitRateEnhancement }) {
  const source = data?.source || {};
  const racers = Array.isArray(data?.racers) ? data.racers : [];
  const styleProfiles = hitRateEnhancement?.stage1_static?.style_profile_by_lane || {};
  const startStates = hitRateEnhancement?.stage2_dynamic?.start_development_states || {};

  const perBoat = racers.map((racer) => {
    const lane = toInt(racer?.lane, null);
    const meta = racer?.predictionFieldMeta && typeof racer.predictionFieldMeta === "object"
      ? racer.predictionFieldMeta
      : {};
    const state = startStates?.[String(lane)] || {};
    const styleProfile = styleProfiles?.[String(lane)] || {};
    return {
      boat: lane,
      original_lane: lane,
      actual_lane: toInt(entryMeta?.actual_lane_map?.[String(lane)], lane),
      course_change_occurred: !!entryMeta?.per_boat_lane_map?.[String(lane)]?.course_change_occurred,
      fields: {
        actual_entry: buildAuditField({
          label: "actual entry lane",
          sourceType: entryMeta?.fallback_used ? null : "official_pre_race",
          sourceUrl: inferSourceUrlForAudit(source, "official_pre_race"),
          sourceSection: "beforeinfo_entry_course",
          sourceRowLabel: "entry course",
          sourceBoatColumn: lane,
          rawCellText: entryMeta?.raw_text_by_lane?.[String(lane)] ?? null,
          parsedRawText: entryMeta?.raw_text_by_lane?.[String(lane)] ?? null,
          normalizedValue: toInt(entryMeta?.actual_lane_map?.[String(lane)], lane),
          backendField: "entryMeta.actual_lane_map",
          frontendField: "entryCourse / actualLane",
          predictionUsableField: "entryMeta.actual_lane_map",
          validationPassed: entryMeta?.validation?.validation_ok === true,
          predictionUsable: entryMeta?.validation?.validation_ok === true,
          reason: entryMeta?.validation?.validation_ok === true ? "verified" : entryMeta?.fallback_reason || "fallback_used"
        }),
        lapTime: buildMetaAuditField({
          label: "lap time",
          meta: meta?.lapTime,
          fallbackValue: racer?.lapTime,
          backendField: "racers[].lapTime",
          frontendField: "playerComparisonRows.lapTime",
          predictionUsableField: "predictionFieldMeta.lapTime",
          source
        }),
        exhibitionST: buildMetaAuditField({
          label: "exhibition ST",
          meta: meta?.exhibitionST,
          fallbackValue: racer?.exhibitionSt ?? racer?.exhibitionST,
          backendField: "racers[].exhibitionSt",
          frontendField: "playerComparisonRows.exhibitionSt",
          predictionUsableField: "predictionFieldMeta.exhibitionST",
          source
        }),
        exhibitionTime: buildMetaAuditField({
          label: "exhibition time",
          meta: meta?.exhibitionTime,
          fallbackValue: racer?.exhibitionTime,
          backendField: "racers[].exhibitionTime",
          frontendField: "playerComparisonRows.exhibitionTime",
          predictionUsableField: "predictionFieldMeta.exhibitionTime",
          source
        }),
        motor2ren: buildMetaAuditField({
          label: "motor 2-ren",
          meta: meta?.motor2ren,
          fallbackValue: racer?.motor2ren ?? racer?.motor2Rate,
          backendField: "racers[].motor2ren",
          frontendField: "playerComparisonRows.motor2ren",
          predictionUsableField: "predictionFieldMeta.motor2ren",
          source
        }),
        motor3ren: buildMetaAuditField({
          label: "motor 3-ren",
          meta: meta?.motor3ren,
          fallbackValue: racer?.motor3ren ?? racer?.motor3Rate,
          backendField: "racers[].motor3ren",
          frontendField: "playerComparisonRows.motor3ren",
          predictionUsableField: "predictionFieldMeta.motor3ren",
          source
        }),
        nationwideWinRate: buildRangeAuditField({
          label: "national win rate",
          value: racer?.nationwideWinRate,
          backendField: "racers[].nationwideWinRate",
          frontendField: "playerComparisonRows.nationwideWinRate",
          predictionUsableField: "features.nationwide_win_rate",
          lane,
          min: 0,
          max: 100
        }),
        localWinRate: buildRangeAuditField({
          label: "local win rate",
          value: racer?.localWinRate,
          backendField: "racers[].localWinRate",
          frontendField: "playerComparisonRows.localWinRate",
          predictionUsableField: "features.local_win_rate",
          lane,
          min: 0,
          max: 100
        }),
        avgSt: buildRangeAuditField({
          label: "average ST",
          value: racer?.avgSt,
          backendField: "racers[].avgSt",
          frontendField: "playerComparisonRows.avgSt",
          predictionUsableField: "features.avg_st",
          lane,
          min: 0,
          max: 1
        }),
        fHoldCount: buildRangeAuditField({
          label: "F count",
          value: racer?.fHoldCount,
          backendField: "racers[].fHoldCount",
          frontendField: "playerComparisonRows.fHoldCount",
          predictionUsableField: "features.f_hold_count",
          lane,
          min: 0,
          max: 9
        }),
        ippansenLane3ren: buildMetaAuditField({
          label: "ippansen lane 3-ren",
          meta: meta?.lane3renScore || meta?.lane3renAvg,
          fallbackValue: racer?.lane3renScore ?? racer?.lane3renAvg ?? racer?.lane3RenRate,
          backendField: "racers[].lane3renScore",
          frontendField: "playerComparisonRows.lane3renScore",
          predictionUsableField: "predictionFieldMeta.lane3renScore",
          source,
          fallbackSourceType: "lane_stat_source"
        }),
        stabilityRate: buildRangeAuditField({
          label: "stability rate",
          value: state?.stability_rate ?? racer?.stability_rate,
          backendField: "hitRateEnhancement.stage2_dynamic.start_development_states[].stability_rate",
          frontendField: "dataAudit only",
          predictionUsableField: "enhancement.stability_rate",
          lane,
          sourceType: "derived_profile",
          sourceSection: "start_profile",
          min: 0,
          max: 100
        }),
        breakoutRate: buildRangeAuditField({
          label: "breakout rate",
          value: state?.breakout_rate ?? racer?.breakout_rate,
          backendField: "hitRateEnhancement.stage2_dynamic.start_development_states[].breakout_rate",
          frontendField: "dataAudit only",
          predictionUsableField: "enhancement.breakout_rate",
          lane,
          sourceType: "derived_profile",
          sourceSection: "start_profile",
          min: 0,
          max: 100
        }),
        delayRate: buildRangeAuditField({
          label: "delay rate",
          value: state?.delay_rate ?? racer?.delay_rate,
          backendField: "hitRateEnhancement.stage2_dynamic.start_development_states[].delay_rate",
          frontendField: "dataAudit only",
          predictionUsableField: "enhancement.delay_rate",
          lane,
          sourceType: "derived_profile",
          sourceSection: "start_profile",
          min: 0,
          max: 100
        }),
        styleProfile: buildAuditField({
          label: "style profile",
          sourceType: "derived_profile",
          sourceSection: "player_start_profile",
          sourceBoatColumn: lane,
          rawCellText: styleProfile,
          parsedRawText: styleProfile,
          normalizedValue: styleProfile,
          backendField: "hitRateEnhancement.stage1_static.style_profile_by_lane",
          frontendField: "dataAudit only",
          predictionUsableField: "enhancement.style_profile",
          validationPassed: styleProfile && typeof styleProfile === "object" && Object.keys(styleProfile).length > 0,
          predictionUsable: styleProfile && typeof styleProfile === "object" && Object.keys(styleProfile).length > 0,
          reason: styleProfile && typeof styleProfile === "object" && Object.keys(styleProfile).length > 0 ? "derived_profile_available" : "missing"
        })
      }
    };
  });

  const summary = {
    authoritative_sources: {
      actual_entry_order: entryMeta?.authoritative_source || "official_beforeinfo_entry_course",
      lap_time: "kyoteibiyori pre-race row 周回 only",
      exhibition_st: "kyoteibiyori ST row only",
      exhibition_time: "kyoteibiyori 展示 row only",
      motor_rates: "official race-card source",
      national_local_rates: "official race-card source",
      avg_st: "official race-card source",
      lane_stats: "validated lane-stat source only"
    },
    actual_entry: {
      raw_source_text: entryMeta?.raw_actual_entry_source_text || null,
      parsed_actual_entry_order: entryMeta?.actual_entry_order || [],
      actual_lane_map: entryMeta?.actual_lane_map || {},
      validation_passed: entryMeta?.validation?.validation_ok === true,
      fallback_used: !!entryMeta?.fallback_used,
      fallback_reason: entryMeta?.fallback_reason || null,
      validation: entryMeta?.validation || {}
    },
    usable_fields: [],
    unusable_fields: []
  };

  perBoat.forEach((row) => {
    Object.values(row.fields).forEach((field) => {
      const tag = `boat${row.boat}:${field.label}`;
      if (field?.prediction_usable) summary.usable_fields.push(tag);
      else summary.unusable_fields.push(tag);
    });
  });

  return {
    enabled: true,
    policy: {
      correctness_first: true,
      canonical_mapping_only: true,
      failed_parse_to_null: true,
      frontend_must_not_guess: true
    },
    summary,
    per_boat: perBoat
  };
}

function summarizeSupplementalFieldUsage(snapshotPlayers = []) {
  const rows = Array.isArray(snapshotPlayers) ? snapshotPlayers : [];
  const fieldChecks = {
    lap_time: (row) => toNullableNum(row?.kyoteibiyori_lap_time ?? row?.lap_time) !== null,
    exhibition_st: (row) => toNullableNum(row?.exhibition_st) !== null,
    exhibition_time: (row) => toNullableNum(row?.exhibition_time) !== null,
    motor2ren: (row) => toNullableNum(row?.motor_2rate) !== null,
    motor3ren: (row) => toNullableNum(row?.motor_3rate) !== null,
    lane_fit_1st: (row) => toNullableNum(row?.lane1st_score_after_reassignment) !== null,
    lane_fit_2ren: (row) => toNullableNum(row?.lane2ren_score_after_reassignment) !== null,
    lane_fit_3ren: (row) => toNullableNum(row?.lane3ren_score_after_reassignment) !== null
  };

  const usable = [];
  const skipped = [];
  for (const [field, check] of Object.entries(fieldChecks)) {
    if (rows.some((row) => check(row))) usable.push(field);
    else skipped.push(field);
  }

  return {
    usable,
    skipped
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
      st: r?.predictionFieldMeta?.exhibitionST?.is_usable ? Number(r?.exhibitionST) : NaN
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

const NON_ESCAPE_FIRST_PLACE_PRIOR_TABLE = {
  inside_lead: { main: { lane: 1, weight: 0.63 }, counter: { lane: 2, weight: 0.23 }, longshot: { lane: 3, weight: 0.08 } },
  one_two_lead: { main: { lane: 1, weight: 0.6 }, counter: { lane: 2, weight: 0.26 }, longshot: { lane: 3, weight: 0.09 } },
  slow_line_lead: { main: { lane: 1, weight: 0.56 }, counter: { lane: 2, weight: 0.22 }, longshot: { lane: 3, weight: 0.12 } },
  one_delayed: { main: { lane: 2, weight: 0.37 }, counter: { lane: 3, weight: 0.3 }, longshot: { lane: 4, weight: 0.16 } },
  three_attacks_first: { main: { lane: 3, weight: 0.44 }, counter: { lane: 4, weight: 0.28 }, longshot: { lane: 1, weight: 0.14 } },
  middle_bulge: { main: { lane: 3, weight: 0.35 }, counter: { lane: 4, weight: 0.31 }, longshot: { lane: 5, weight: 0.17 } },
  no_wall: { main: { lane: 4, weight: 0.39 }, counter: { lane: 3, weight: 0.35 }, longshot: { lane: 5, weight: 0.12 } },
  two_three_delayed: { main: { lane: 4, weight: 0.42 }, counter: { lane: 5, weight: 0.27 }, longshot: { lane: 1, weight: 0.14 } },
  dash_lead: { main: { lane: 4, weight: 0.41 }, counter: { lane: 5, weight: 0.25 }, longshot: { lane: 1, weight: 0.14 } },
  middle_dent: { main: { lane: 4, weight: 0.38 }, counter: { lane: 5, weight: 0.26 }, longshot: { lane: 2, weight: 0.15 } },
  outside_lead: { main: { lane: 4, weight: 0.34 }, counter: { lane: 6, weight: 0.27 }, longshot: { lane: 5, weight: 0.22 } }
};

function normalizeDistributionRows(rows) {
  const items = Array.isArray(rows) ? rows.filter((row) => Number.isInteger(toInt(row?.lane, null))) : [];
  const total = items.reduce((sum, row) => sum + Math.max(0, toNum(row?.weight, 0)), 0) || 1;
  return items
    .map((row) => ({
      lane: toInt(row?.lane, null),
      role: row?.role || null,
      weight: Number((Math.max(0, toNum(row?.weight, 0)) / total).toFixed(4))
    }))
    .sort((a, b) => b.weight - a.weight);
}

function buildFormationFirstPlacePrior(escapePatternAnalysis) {
  if (escapePatternAnalysis?.escape_pattern_applied) return [];
  const pattern = String(escapePatternAnalysis?.formation_pattern || "");
  const prior = NON_ESCAPE_FIRST_PLACE_PRIOR_TABLE[pattern];
  if (!prior) return [];
  return normalizeDistributionRows([
    { lane: prior.main.lane, role: "main", weight: prior.main.weight },
    { lane: prior.counter.lane, role: "counter", weight: prior.counter.weight },
    { lane: prior.longshot.lane, role: "longshot", weight: prior.longshot.weight }
  ]);
}

function buildBoat1EscapeOpponentModel({
  rows,
  escapePatternAnalysis,
  attackScenarioAnalysis
}) {
  const laneFeatureMap = new Map(
    safeArray(rows)
      .map((row) => [toInt(row?.racer?.lane, null), row?.features || {}])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const lane2 = laneFeatureMap.get(2) || {};
  const lane3 = laneFeatureMap.get(3) || {};
  const lane4 = laneFeatureMap.get(4) || {};
  const lane5 = laneFeatureMap.get(5) || {};
  const formationPattern = String(escapePatternAnalysis?.formation_pattern || "");
  const attackType = String(attackScenarioAnalysis?.attack_scenario_type || "");
  const stableInsideFormation =
    !!escapePatternAnalysis?.escape_pattern_applied ||
    ["inside_lead", "one_two_lead", "slow_line_lead"].includes(formationPattern);
  const lane2WeakOrLate =
    toNum(lane2?.expected_actual_st_rank ?? lane2?.st_rank, 6) >= 4 ||
    toNum(lane2?.f_hold_caution_penalty, 0) >= 5.5 ||
    toNum(lane2?.display_time_delta_vs_left, 0) < -0.015;
  const lane3StrongAttack =
    attackType === "three_makuri" ||
    attackType === "three_makuri_sashi" ||
    (toNum(lane3?.lap_attack_strength, 0) >= 8 && toNum(lane3?.slit_alert_flag, 0) === 1);
  const lane4CornerPressure =
    attackType === "four_cado_makuri" ||
    attackType === "four_cado_makuri_sashi" ||
    (toNum(lane4?.entry_advantage_score, 0) >= 9 && toNum(lane4?.lap_attack_strength, 0) >= 7);
  const lane5FollowPressure =
    lane4CornerPressure &&
    toNum(lane5?.lap_attack_strength, 0) >= 6 &&
    toNum(lane5?.motor_total_score, 0) >= 9;

  const secondAdjustments = new Map([
    [2, stableInsideFormation ? 16 : 12],
    [3, stableInsideFormation ? 12 : 10],
    [4, stableInsideFormation ? 6 : 5],
    [5, -4],
    [6, -6]
  ]);
  const thirdAdjustments = new Map([
    [2, 8.5],
    [3, 7.5],
    [4, 6.2],
    [5, -1.6],
    [6, -2.8]
  ]);
  const boat1StayedHeadReasonTags = ["BOAT1_DEFAULT_STRONGEST", stableInsideFormation ? "INSIDE_FORMATION_STABLE" : "INSIDE_BASELINE_ACTIVE"];
  const reasonTags = ["BOAT1_ESCAPE_OPPONENT_MODEL", "SECOND_ROLE_SEPARATED", "THIRD_ROLE_SEPARATED"];
  let sujiUsed = false;
  let urasujiUsed = false;
  let attackMovedSecondOnlyLane = null;
  let attackMovedThirdOnlyLane = null;

  secondAdjustments.set(2, toNum(secondAdjustments.get(2), 0) + 4.6);
  secondAdjustments.set(3, toNum(secondAdjustments.get(3), 0) + 3.4);
  thirdAdjustments.set(2, toNum(thirdAdjustments.get(2), 0) + 2.2);
  thirdAdjustments.set(3, toNum(thirdAdjustments.get(3), 0) + 1.6);
  sujiUsed = true;
  reasonTags.push("SUJI_SECOND_PRIOR");

  if (lane2WeakOrLate) {
    secondAdjustments.set(2, toNum(secondAdjustments.get(2), 0) - 8.5);
    secondAdjustments.set(3, toNum(secondAdjustments.get(3), 0) + 7.5);
    thirdAdjustments.set(3, toNum(thirdAdjustments.get(3), 0) + 2.5);
    reasonTags.push("LANE2_WEAK_LATE");
  } else {
    boat1StayedHeadReasonTags.push("LANE2_REMAIN_LIVE");
  }

  if (lane3StrongAttack) {
    secondAdjustments.set(3, toNum(secondAdjustments.get(3), 0) + 8.8);
    thirdAdjustments.set(4, toNum(thirdAdjustments.get(4), 0) + 3.4);
    attackMovedSecondOnlyLane = 3;
    attackMovedThirdOnlyLane = 4;
    reasonTags.push("ATTACK_SHAPE_SECOND_ONLY_L3");
  }

  if (lane4CornerPressure) {
    secondAdjustments.set(4, toNum(secondAdjustments.get(4), 0) + 7.8);
    thirdAdjustments.set(4, toNum(thirdAdjustments.get(4), 0) + 2.4);
    attackMovedSecondOnlyLane = 4;
    if (!lane3StrongAttack) attackMovedThirdOnlyLane = 3;
    reasonTags.push("ATTACK_SHAPE_SECOND_ONLY_L4");
  }

  if (lane3StrongAttack) {
    secondAdjustments.set(4, toNum(secondAdjustments.get(4), 0) + 1.6);
    thirdAdjustments.set(4, toNum(thirdAdjustments.get(4), 0) + 2.2);
    urasujiUsed = true;
  }
  if (lane5FollowPressure) {
    thirdAdjustments.set(5, toNum(thirdAdjustments.get(5), 0) + 1.8);
    urasujiUsed = true;
  }
  if (urasujiUsed) reasonTags.push("URASUJI_LIMITED");

  if (!lane3StrongAttack && !lane4CornerPressure) {
    boat1StayedHeadReasonTags.push("ATTACK_NOT_HEAD_OVERRIDE");
  }

  return {
    second_adjustments: secondAdjustments,
    third_adjustments: thirdAdjustments,
    stable_inside_formation: stableInsideFormation ? 1 : 0,
    lane2_weak_or_late: lane2WeakOrLate ? 1 : 0,
    lane3_strong_attack: lane3StrongAttack ? 1 : 0,
    lane4_corner_pressure: lane4CornerPressure ? 1 : 0,
    suji_used: sujiUsed ? 1 : 0,
    urasuji_used: urasujiUsed ? 1 : 0,
    attack_moved_second_only_lane: attackMovedSecondOnlyLane,
    attack_moved_third_only_lane: attackMovedThirdOnlyLane,
    boat1_stayed_head_reason_tags: [...new Set(boat1StayedHeadReasonTags)],
    reason_tags: [...new Set(reasonTags)]
  };
}

const PLAYER_STAT_WINDOW_POLICY = {
  recent_3_months_weight: 0.65,
  current_season_weight: 0.35,
  recent_3_months_small_sample_threshold: 12,
  recent_3_months_tiny_sample_threshold: 6
};

function parseDateOnlyUtc(value) {
  const text = String(value || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const parsed = new Date(`${text}T00:00:00.000Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatDateOnlyUtc(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function shiftDateByMonthsUtc(dateText, deltaMonths = 0) {
  const base = parseDateOnlyUtc(dateText);
  if (!base) return null;
  const shifted = new Date(base.getTime());
  shifted.setUTCMonth(shifted.getUTCMonth() + deltaMonths);
  return formatDateOnlyUtc(shifted);
}

function buildWindowPerformanceStats(rows) {
  const items = safeArray(rows);
  const laneBuckets = new Map();
  const stats = {
    starts: items.length,
    win_count: 0,
    top2_count: 0,
    top3_count: 0
  };
  for (const row of items) {
    const startLane = toInt(row?.start_lane, null);
    const finish1 = toInt(row?.finish_1, null);
    const finish2 = toInt(row?.finish_2, null);
    const finish3 = toInt(row?.finish_3, null);
    if (finish1 === startLane) stats.win_count += 1;
    if (finish1 === startLane || finish2 === startLane) stats.top2_count += 1;
    if (finish1 === startLane || finish2 === startLane || finish3 === startLane) stats.top3_count += 1;
    if (!Number.isInteger(startLane)) continue;
    if (!laneBuckets.has(startLane)) {
      laneBuckets.set(startLane, { starts: 0, win_count: 0, top2_count: 0, top3_count: 0 });
    }
    const laneStats = laneBuckets.get(startLane);
    laneStats.starts += 1;
    if (finish1 === startLane) laneStats.win_count += 1;
    if (finish1 === startLane || finish2 === startLane) laneStats.top2_count += 1;
    if (finish1 === startLane || finish2 === startLane || finish3 === startLane) laneStats.top3_count += 1;
  }

  const toRate = (count, starts) => Number((starts > 0 ? (count / starts) * 100 : 0).toFixed(2));
  const lane_stats = Object.fromEntries(
    [...laneBuckets.entries()].map(([lane, laneStats]) => [
      String(lane),
      {
        sample_size: laneStats.starts,
        first_rate: toRate(laneStats.win_count, laneStats.starts),
        top2_rate: toRate(laneStats.top2_count, laneStats.starts),
        top3_rate: toRate(laneStats.top3_count, laneStats.starts)
      }
    ])
  );

  return {
    sample_size: stats.starts,
    first_rate: toRate(stats.win_count, stats.starts),
    top2_rate: toRate(stats.top2_count, stats.starts),
    top3_rate: toRate(stats.top3_count, stats.starts),
    lane_stats
  };
}

function derivePlayerStrengthFromRates(windowStats) {
  if (!windowStats || toNum(windowStats?.sample_size, 0) <= 0) return null;
  return Number(
    clamp(
      0,
      10,
      toNum(windowStats?.first_rate, 0) * 0.12 +
        toNum(windowStats?.top2_rate, 0) * 0.05 +
        toNum(windowStats?.top3_rate, 0) * 0.02
    ).toFixed(3)
  );
}

function computePlayerWindowBlendWeights(recentSampleSize, seasonSampleSize) {
  if (recentSampleSize <= 0 && seasonSampleSize <= 0) {
    return {
      recent_weight: 0,
      current_season_weight: 0
    };
  }
  if (recentSampleSize <= 0) {
    return {
      recent_weight: 0,
      current_season_weight: 1
    };
  }
  if (seasonSampleSize <= 0) {
    return {
      recent_weight: 1,
      current_season_weight: 0
    };
  }
  if (recentSampleSize < PLAYER_STAT_WINDOW_POLICY.recent_3_months_tiny_sample_threshold) {
    return {
      recent_weight: 0.35,
      current_season_weight: 0.65
    };
  }
  if (recentSampleSize < PLAYER_STAT_WINDOW_POLICY.recent_3_months_small_sample_threshold) {
    return {
      recent_weight: 0.5,
      current_season_weight: 0.5
    };
  }
  return {
    recent_weight: PLAYER_STAT_WINDOW_POLICY.recent_3_months_weight,
    current_season_weight: PLAYER_STAT_WINDOW_POLICY.current_season_weight
  };
}

function blendWeightedValues(recentValue, seasonValue, recentWeight, seasonWeight) {
  const recent = Number.isFinite(Number(recentValue)) ? Number(recentValue) : null;
  const season = Number.isFinite(Number(seasonValue)) ? Number(seasonValue) : null;
  if (recent === null && season === null) return null;
  if (recent !== null && season === null) return recent;
  if (recent === null && season !== null) return season;
  const totalWeight = recentWeight + seasonWeight || 1;
  return Number((((recent * recentWeight) + (season * seasonWeight)) / totalWeight).toFixed(3));
}

function buildPlayerStatProfileFromHistory({ historyRows, raceDate }) {
  const raceDateText = String(raceDate || "").trim();
  const currentSeasonStart = /^\d{4}-/.test(raceDateText) ? `${raceDateText.slice(0, 4)}-01-01` : null;
  const recentStart = shiftDateByMonthsUtc(raceDateText, -3);
  const priorRows = safeArray(historyRows)
    .filter((row) => String(row?.race_date || "") < raceDateText)
    .sort((a, b) => String(b?.race_date || "").localeCompare(String(a?.race_date || "")));
  const recentRows = recentStart ? priorRows.filter((row) => String(row?.race_date || "") >= recentStart) : [];
  const seasonRows = currentSeasonStart ? priorRows.filter((row) => String(row?.race_date || "") >= currentSeasonStart) : [];
  const recentStats = buildWindowPerformanceStats(recentRows);
  const seasonStats = buildWindowPerformanceStats(seasonRows);
  const weights = computePlayerWindowBlendWeights(
    toNum(recentStats?.sample_size, 0),
    toNum(seasonStats?.sample_size, 0)
  );
  const recentStrength = derivePlayerStrengthFromRates(recentStats);
  const seasonStrength = derivePlayerStrengthFromRates(seasonStats);
  const blendedStrength = blendWeightedValues(
    recentStrength,
    seasonStrength,
    toNum(weights?.recent_weight, 0),
    toNum(weights?.current_season_weight, 0)
  );
  const laneStatsByLane = {};
  for (let lane = 1; lane <= 6; lane += 1) {
    const recentLane = recentStats?.lane_stats?.[String(lane)] || null;
    const seasonLane = seasonStats?.lane_stats?.[String(lane)] || null;
    const sampleSize = Math.max(
      toNum(recentLane?.sample_size, 0),
      toNum(seasonLane?.sample_size, 0)
    );
    laneStatsByLane[String(lane)] = {
      sample_size: sampleSize,
      first_rate: blendWeightedValues(
        recentLane?.first_rate,
        seasonLane?.first_rate,
        toNum(weights?.recent_weight, 0),
        toNum(weights?.current_season_weight, 0)
      ),
      top2_rate: blendWeightedValues(
        recentLane?.top2_rate,
        seasonLane?.top2_rate,
        toNum(weights?.recent_weight, 0),
        toNum(weights?.current_season_weight, 0)
      ),
      top3_rate: blendWeightedValues(
        recentLane?.top3_rate,
        seasonLane?.top3_rate,
        toNum(weights?.recent_weight, 0),
        toNum(weights?.current_season_weight, 0)
      )
    };
  }

  return {
    recent_3_months_start: recentStart,
    current_season_start: currentSeasonStart,
    recent_3_months_sample_size: toNum(recentStats?.sample_size, 0),
    current_season_sample_size: toNum(seasonStats?.sample_size, 0),
    recent_3_months_strength: recentStrength,
    current_season_strength: seasonStrength,
    blended_strength: blendedStrength,
    blend_weights: weights,
    player_stat_confidence: Number(
      clamp(
        0.22,
        1,
        0.32 +
          Math.min(0.48, toNum(recentStats?.sample_size, 0) * 0.035) +
          Math.min(0.2, toNum(seasonStats?.sample_size, 0) * 0.012)
      ).toFixed(3)
    ),
    lane_stats_by_lane: laneStatsByLane
  };
}

function loadRecentPlayerStatProfiles({ raceDate, racers }) {
  const registrationNos = [...new Set(
    safeArray(racers)
      .map((row) => toInt(row?.registrationNo ?? row?.registration_no, null))
      .filter((value) => Number.isInteger(value) && value > 0)
  )];
  if (registrationNos.length === 0 || !raceDate) return new Map();
  const placeholders = registrationNos.map(() => "?").join(",");
  const rows = db.prepare(
    `
      SELECT
        e.registration_no,
        e.lane AS start_lane,
        r.race_date,
        res.finish_1,
        res.finish_2,
        res.finish_3
      FROM entries e
      INNER JOIN races r ON r.race_id = e.race_id
      INNER JOIN results res ON res.race_id = e.race_id
      WHERE e.registration_no IN (${placeholders})
        AND r.race_date < ?
        AND res.finish_1 IS NOT NULL
        AND res.finish_2 IS NOT NULL
        AND res.finish_3 IS NOT NULL
      ORDER BY r.race_date DESC
    `
  ).all(...registrationNos, raceDate);
  const rowsByRegistration = new Map();
  for (const row of safeArray(rows)) {
    const registrationNo = toInt(row?.registration_no, null);
    if (!Number.isInteger(registrationNo)) continue;
    if (!rowsByRegistration.has(registrationNo)) rowsByRegistration.set(registrationNo, []);
    rowsByRegistration.get(registrationNo).push(row);
  }
  return new Map(
    registrationNos.map((registrationNo) => [
      registrationNo,
      buildPlayerStatProfileFromHistory({
        historyRows: rowsByRegistration.get(registrationNo) || [],
        raceDate
      })
    ])
  );
}

function applyRecentPlayerStatProfilesToRacers({ racers, raceDate }) {
  const playerProfiles = loadRecentPlayerStatProfiles({ raceDate, racers });
  return safeArray(racers).map((racer) => {
    const registrationNo = toInt(racer?.registrationNo ?? racer?.registration_no, null);
    const lane = toInt(racer?.lane, null);
    const profile = Number.isInteger(registrationNo) ? playerProfiles.get(registrationNo) || null : null;
    const officialNationwide = toNullableNum(racer?.nationwideWinRate);
    const officialLocal = toNullableNum(racer?.localWinRate);
    const fallbackStrength = Number(
      clamp(
        0,
        10,
        ((toNum(officialNationwide, 0) + toNum(officialLocal, toNum(officialNationwide, 0))) * 0.5) * 0.72
      ).toFixed(3)
    );
    const currentSeasonStrength = toNum(profile?.current_season_strength, null);
    const blendedStrength = toNum(profile?.blended_strength, null);
    const effectiveNationwide = Number.isFinite(currentSeasonStrength) ? currentSeasonStrength : fallbackStrength;
    const effectiveLocal = Number.isFinite(blendedStrength) ? blendedStrength : effectiveNationwide;
    const laneStats = profile?.lane_stats_by_lane?.[String(lane)] || {};
    return {
      ...racer,
      officialNationwideWinRate: officialNationwide,
      officialLocalWinRate: officialLocal,
      nationwideWinRate: effectiveNationwide,
      localWinRate: effectiveLocal,
      playerRecent3MonthsStrength: toNullableNum(profile?.recent_3_months_strength),
      playerCurrentSeasonStrength: toNullableNum(profile?.current_season_strength),
      playerStrengthBlended: toNullableNum(profile?.blended_strength),
      playerStatConfidence: toNullableNum(profile?.player_stat_confidence),
      recent3MonthsSampleSize: toInt(profile?.recent_3_months_sample_size, 0),
      currentSeasonSampleSize: toInt(profile?.current_season_sample_size, 0),
      playerStatFallbackUsed: Number.isFinite(blendedStrength) || Number.isFinite(currentSeasonStrength) ? 0 : 1,
      playerStatWindowsUsed: {
        recent_3_months: {
          start_date: profile?.recent_3_months_start || null,
          end_date: raceDate || null,
          sample_size: toInt(profile?.recent_3_months_sample_size, 0)
        },
        current_season: {
          start_date: profile?.current_season_start || null,
          end_date: raceDate || null,
          sample_size: toInt(profile?.current_season_sample_size, 0)
        },
        blend_weights: profile?.blend_weights || computePlayerWindowBlendWeights(0, 0)
      },
      course1WinRate: lane === 1 ? toNullableNum(laneStats?.first_rate) : null,
      course1_2rate: lane === 1 ? toNullableNum(laneStats?.top2_rate) : null,
      course2_2rate: lane === 2 ? toNullableNum(laneStats?.top2_rate) : null,
      course3_3rate: lane === 3 ? toNullableNum(laneStats?.top3_rate) : null,
      course4_3rate: lane === 4 ? toNullableNum(laneStats?.top3_rate) : null,
      laneFirstRate: toNullableNum(laneStats?.first_rate),
      lane2RenRate: toNullableNum(laneStats?.top2_rate),
      lane3RenRate: toNullableNum(laneStats?.top3_rate),
      laneRecentSampleSize: toInt(laneStats?.sample_size, 0)
    };
  });
}

function buildRoleProbabilityLayers({
  rows,
  candidateDistributions,
  headScenarioBalanceAnalysis,
  escapePatternAnalysis,
  attackScenarioAnalysis
}) {
  const firstPlaceProbability = normalizeDistributionRows(
    safeArray(candidateDistributions?.first_place_distribution_json).length > 0
      ? candidateDistributions.first_place_distribution_json
      : safeArray(headScenarioBalanceAnalysis?.head_distribution_json)
  );
  const secondPlaceProbability = normalizeDistributionRows(
    safeArray(candidateDistributions?.second_place_distribution_json)
  );
  const thirdPlaceProbability = normalizeDistributionRows(
    safeArray(candidateDistributions?.third_place_distribution_json)
  );
  const boat1SecondPlaceProbability = normalizeDistributionRows(
    safeArray(candidateDistributions?.boat1_second_place_distribution_json)
  );
  const boat1ThirdPlaceProbability = normalizeDistributionRows(
    safeArray(candidateDistributions?.boat1_third_place_distribution_json)
  );
  const firstMap = new Map(firstPlaceProbability.map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const secondMap = new Map(secondPlaceProbability.map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const thirdMap = new Map(thirdPlaceProbability.map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const boat1SecondMap = new Map(boat1SecondPlaceProbability.map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const boat1ThirdMap = new Map(boat1ThirdPlaceProbability.map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const escapePatternApplied = !!escapePatternAnalysis?.escape_pattern_applied;
  const mainHeadLane = toInt(headScenarioBalanceAnalysis?.main_head_lane, null) ?? topDistributionLane(firstPlaceProbability);
  const attackHeadLane = getAttackScenarioHeadLane(attackScenarioAnalysis?.attack_scenario_type || null);
  const attackScenarioEntries = [
    { scenario: "boat2_sashi", raw: toNum(attackScenarioAnalysis?.two_sashi_score, 0) },
    { scenario: "boat3_makuri", raw: toNum(attackScenarioAnalysis?.three_makuri_score, 0) },
    { scenario: "boat3_makuri_sashi", raw: toNum(attackScenarioAnalysis?.three_makuri_sashi_score, 0) },
    { scenario: "boat4_cado_attack", raw: Math.max(toNum(attackScenarioAnalysis?.four_cado_makuri_score, 0), toNum(attackScenarioAnalysis?.four_cado_makuri_sashi_score, 0)) },
    { scenario: "boat4_cado_makuri", raw: toNum(attackScenarioAnalysis?.four_cado_makuri_score, 0) },
    { scenario: "boat4_cado_makuri_sashi", raw: toNum(attackScenarioAnalysis?.four_cado_makuri_sashi_score, 0) },
    {
      scenario: "outside_lead",
      raw: String(attackScenarioAnalysis?.attack_scenario_type || "") === "outside_lead"
        ? Math.max(1, toNum(attackScenarioAnalysis?.attack_scenario_score, 0))
        : 0
    }
  ];
  if (attackScenarioEntries.every((row) => row.raw <= 0) && toInt(attackScenarioAnalysis?.attack_scenario_applied, 0) === 1) {
    const fallbackScenarioMap = {
      two_sashi: "boat2_sashi",
      three_makuri: "boat3_makuri",
      three_makuri_sashi: "boat3_makuri_sashi",
      four_cado_makuri: "boat4_cado_makuri",
      four_cado_makuri_sashi: "boat4_cado_makuri_sashi"
    };
    const fallbackScenario = fallbackScenarioMap[String(attackScenarioAnalysis?.attack_scenario_type || "")] || null;
    if (fallbackScenario) {
      const fallbackIndex = attackScenarioEntries.findIndex((row) => row.scenario === fallbackScenario);
      if (fallbackIndex >= 0) {
        attackScenarioEntries[fallbackIndex] = {
          ...attackScenarioEntries[fallbackIndex],
          raw: Math.max(1, toNum(attackScenarioAnalysis?.attack_scenario_score, 0))
        };
      }
    }
  }
  const attackTotal = attackScenarioEntries.reduce((sum, row) => sum + Math.max(0, row.raw), 0) || 1;
  const attackScenarioProbability = attackScenarioEntries
    .filter((row) => row.raw > 0)
    .map((row) => ({
      scenario: row.scenario,
      probability: Number((Math.max(0, row.raw) / attackTotal).toFixed(4))
    }))
    .sort((a, b) => b.probability - a.probability);
  const topAttackProbability = toNum(attackScenarioProbability[0]?.probability, 0);
  const boat1EscapeProbability = Number(clamp(
    0,
    1,
    toNum(firstMap.get(1), 0) * 0.72 +
      (escapePatternApplied ? 0.14 : 0) +
      Math.min(0.18, survivalResidualScore * 0.0046) -
      ((attackHeadLane && attackHeadLane !== 1) ? topAttackProbability * 0.09 : 0)
  ).toFixed(4));

  const survivalProbability = normalizeDistributionRows(
    safeArray(rows).map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      if (!Number.isInteger(lane)) return null;
      const secondProb = toNum((lane === 1 ? null : boat1SecondMap.get(lane)) ?? secondMap.get(lane), 0);
      const thirdProb = toNum((lane === 1 ? null : boat1ThirdMap.get(lane)) ?? thirdMap.get(lane), 0);
      const insideRemainBias = lane === 1 ? survivalResidualScore * 0.18 : lane === 2 ? 0.12 : lane === 3 ? 0.1 : lane === 4 ? 0.08 : lane === 5 ? 0.01 : 0;
      return {
        lane,
        role: lane === 1 ? "boat1_survival" : lane <= 4 ? "remain" : "outer_remain",
        weight: secondProb * 0.34 + thirdProb * 0.54 + insideRemainBias
      };
    }).filter(Boolean)
  );

  return {
    first_place_probability_json: firstPlaceProbability,
    second_place_probability_json: secondPlaceProbability,
    third_place_probability_json: thirdPlaceProbability,
    boat1_second_place_probability_json: boat1SecondPlaceProbability,
    boat1_third_place_probability_json: boat1ThirdPlaceProbability,
    survival_probability_json: survivalProbability,
    boat1_escape_probability: boat1EscapeProbability,
    attack_scenario_probability_json: attackScenarioProbability,
    role_probability_summary_json: {
      main_head_lane: mainHeadLane,
      boat1_escape_probability: boat1EscapeProbability,
      first_place_concentration: Number(
        firstPlaceProbability.slice(0, 2).reduce((sum, row) => sum + toNum(row?.weight, 0), 0).toFixed(4)
      ),
      second_place_concentration: Number(
        secondPlaceProbability.slice(0, 2).reduce((sum, row) => sum + toNum(row?.weight, 0), 0).toFixed(4)
      ),
      third_place_concentration: Number(
        thirdPlaceProbability.slice(0, 3).reduce((sum, row) => sum + toNum(row?.weight, 0), 0).toFixed(4)
      ),
      attack_affects_first_only: attackHeadLane && attackHeadLane !== 1 && toNum(firstMap.get(attackHeadLane), 0) > 0.24 ? 1 : 0,
      attack_affects_second_only: toInt(candidateDistributions?.partner_search_bias_json?.attack_moved_second_only_lane, 0) > 0 ? 1 : 0,
      attack_affects_third_only: toInt(candidateDistributions?.partner_search_bias_json?.attack_moved_third_only_lane, 0) > 0 ? 1 : 0
    },
    role_probability_version: "role_probability_v1"
  };
}

function buildBoat3WeakStHeadSuppressionContext({
  rows,
  headScenarioBalanceAnalysis,
  attackScenarioAnalysis,
  outsideHeadPromotionContext
}) {
  const laneRows = new Map(
    safeArray(rows)
      .map((row) => [toInt(row?.racer?.lane, null), row])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const lane1 = laneRows.get(1) || null;
  const lane2 = laneRows.get(2) || null;
  const lane3 = laneRows.get(3) || null;
  const lane1Features = lane1?.features || {};
  const lane2Features = lane2?.features || {};
  const lane3Features = lane3?.features || {};
  const st1 = toNum(lane1Features?.expected_actual_st ?? lane1?.racer?.exhibitionSt ?? lane1Features?.avg_st, null);
  const st2 = toNum(lane2Features?.expected_actual_st ?? lane2?.racer?.exhibitionSt ?? lane2Features?.avg_st, null);
  const st3 = toNum(lane3Features?.expected_actual_st ?? lane3?.racer?.exhibitionSt ?? lane3Features?.avg_st, null);
  const stGapVs1 = Number.isFinite(st3) && Number.isFinite(st1) ? Number((st3 - st1).toFixed(3)) : null;
  const stGapVs2 = Number.isFinite(st3) && Number.isFinite(st2) ? Number((st3 - st2).toFixed(3)) : null;
  const boat1HeadWeight = toNum(
    safeArray(headScenarioBalanceAnalysis?.head_distribution_json).find((row) => toInt(row?.lane, null) === 1)?.weight,
    0
  );
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const insideStable =
    boat1HeadWeight >= 0.45 &&
    survivalResidualScore >= 36 &&
    toNum(outsideHeadPromotionContext?.inner_collapse_score, 0) < 55 &&
    toNum(lane1Features?.expected_actual_st_rank ?? lane1Features?.st_rank, 6) <= 3 &&
    toNum(lane2Features?.expected_actual_st_rank ?? lane2Features?.st_rank, 6) <= 3;
  const weakStVsInside =
    Number.isFinite(stGapVs1) &&
    Number.isFinite(stGapVs2) &&
    stGapVs1 >= 0.025 &&
    stGapVs2 >= 0.02 &&
    toNum(lane3Features?.expected_actual_st_rank ?? lane3Features?.st_rank, 6) >=
      Math.max(
        toNum(lane1Features?.expected_actual_st_rank ?? lane1Features?.st_rank, 6),
        toNum(lane2Features?.expected_actual_st_rank ?? lane2Features?.st_rank, 6)
      ) + 2;
  const matchedSignals = [];
  if (toNum(lane3Features?.motor_total_score, 0) >= 10) matchedSignals.push("strong_motor");
  if (toNum(lane3Features?.exhibition_rank, 6) <= 2) matchedSignals.push("strong_exhibition");
  if (toNum(lane3Features?.lap_attack_strength, 0) >= 8 || toNum(lane3Features?.lap_time_delta_vs_front, 0) >= 0.055) {
    matchedSignals.push("lap_attack");
  }
  if (toNum(lane3Features?.slit_alert_flag, 0) === 1 || toNum(lane3Features?.display_time_delta_vs_left, 0) >= 0.055) {
    matchedSignals.push("slit_or_left_advantage");
  }
  if (toNum(lane3Features?.entry_advantage_score, 0) >= 8) matchedSignals.push("entry_shape");
  if (
    ["three_makuri", "three_makuri_sashi"].includes(String(attackScenarioAnalysis?.attack_scenario_type || "")) &&
    toNum(attackScenarioAnalysis?.attack_scenario_score, 0) >= 65
  ) {
    matchedSignals.push("scenario_attack");
  }
  const applied = insideStable && weakStVsInside && matchedSignals.length < 3;
  const penaltyScore = applied
    ? Number(clamp(0, 24, 11 + toNum(stGapVs1, 0) * 120 + toNum(stGapVs2, 0) * 95 + (3 - matchedSignals.length) * 3.2).toFixed(2))
    : 0;
  const firstPlaceCapWeight = applied
    ? matchedSignals.length >= 2
      ? 0.17
      : 0.13
    : 1;
  return {
    applied: applied ? 1 : 0,
    inside_stable: insideStable ? 1 : 0,
    weak_st_vs_inside: weakStVsInside ? 1 : 0,
    st_gap_vs_lane1: stGapVs1,
    st_gap_vs_lane2: stGapVs2,
    matched_signal_count: matchedSignals.length,
    matched_signal_tags: matchedSignals,
    penalty_score: penaltyScore,
    first_place_cap_weight: firstPlaceCapWeight,
    reason_tags: applied
      ? ["BOAT3_WEAK_ST_HEAD_SUPPRESSED", "INSIDE_STABLE", "ATTACK_NOT_FINISH_OVERRIDE", ...matchedSignals]
      : matchedSignals.length >= 3
        ? ["BOAT3_RECOVERY_EVIDENCE_SUFFICIENT", ...matchedSignals]
        : []
  };
}

function normalizeLaunchStateLabel(score) {
  const value = toNum(score, 0);
  if (value >= 28) return "strong_out";
  if (value >= 10) return "out";
  if (value <= -28) return "strong_hollow";
  if (value <= -10) return "hollow";
  return "neutral";
}

const LAUNCH_STATE_CONFIG = {
  st_margins: {
    ST_NEIGHBOR_STRONG_OUT_MARGIN: 0.03,
    ST_NEIGHBOR_OUT_MARGIN: 0.015,
    ST_NEIGHBOR_HOLLOW_MARGIN: -0.015,
    ST_NEIGHBOR_STRONG_HOLLOW_MARGIN: -0.03,
    ST_INSIDE_STRONG_OUT_MARGIN: 0.025,
    ST_INSIDE_OUT_MARGIN: 0.012,
    ST_INSIDE_HOLLOW_MARGIN: -0.012,
    ST_INSIDE_STRONG_HOLLOW_MARGIN: -0.025
  },
  score_thresholds: {
    LAUNCH_SCORE_STRONG_OUT: 28,
    LAUNCH_SCORE_OUT: 10,
    LAUNCH_SCORE_NEUTRAL_LOW: -9.99,
    LAUNCH_SCORE_NEUTRAL_HIGH: 9.99,
    LAUNCH_SCORE_HOLLOW: -10,
    LAUNCH_SCORE_STRONG_HOLLOW: -28
  },
  score_weights: {
    st_rank_weight: 8,
    st_rank_late_penalty: 7,
    neighbor_margin_weight: 240,
    inside_margin_weight: 160,
    formation_fit_weight: 0.55,
    lap_support_weight: 0.45,
    environment_weight: 10,
    display_time_positive_weight: 55,
    display_time_negative_weight: 30,
    slit_alert_weight: 12,
    f_hold_penalty_weight: 1.25,
    lane1_baseline_bonus: 8
  },
  event_thresholds: {
    STATE_OUT_MIN: 10,
    STATE_STRONG_OUT_MIN: 28,
    STATE_HOLLOW_MAX: -10,
    STATE_STRONG_HOLLOW_MAX: -28,
    INNER_STABLE_MIN: 48,
    INNER_COLLAPSE_MIN: 52,
    WEAK_WALL_ON_2_MIN: 34,
    WEAK_WALL_ON_3_MIN: 34,
    BOAT3_ATTACK_READY_MIN: 44,
    BOAT4_CADO_READY_MIN: 44,
    BOAT5_OUTER_PUSH_MIN: 40,
    OUTER_MIX_READY_MIN: 42
  }
};

const VENUE_LAUNCH_MICRO_CALIBRATION = {
  default: {
    inner_stable_bias: 0,
    boat1_escape_bias: 0,
    boat3_attack_bias: 0,
    boat4_cado_bias: 0,
    outer_mix_bias: 0,
    upset_risk_bias: 0,
    launch_threshold_bias: 0
  },
  5: {
    inner_stable_bias: 3,
    boat1_escape_bias: 0.035,
    boat3_attack_bias: -0.018,
    boat4_cado_bias: 0.012,
    outer_mix_bias: -0.015,
    upset_risk_bias: -2.5,
    launch_threshold_bias: 0
  },
  23: {
    inner_stable_bias: 1.5,
    boat1_escape_bias: 0.015,
    boat3_attack_bias: 0.008,
    boat4_cado_bias: 0.015,
    outer_mix_bias: -0.01,
    upset_risk_bias: -1,
    launch_threshold_bias: 0
  },
  10: {
    inner_stable_bias: -1,
    boat1_escape_bias: -0.015,
    boat3_attack_bias: 0.018,
    boat4_cado_bias: 0.022,
    outer_mix_bias: 0.012,
    upset_risk_bias: 1.5,
    launch_threshold_bias: 0
  }
};

function getVenueLaunchMicroCalibration({ race, venueSummary } = {}) {
  const venueKey =
    toInt(race?.venueId, null) ??
    toInt(venueSummary?.venue_segment_key, null) ??
    null;
  const base = VENUE_LAUNCH_MICRO_CALIBRATION.default;
  const venue = venueKey !== null
    ? (VENUE_LAUNCH_MICRO_CALIBRATION[venueKey] || base)
    : base;
  const calibration = {
    inner_stable_bias: clamp(-4, 4, toNum(venue?.inner_stable_bias, 0)),
    boat1_escape_bias: clamp(-0.05, 0.05, toNum(venue?.boat1_escape_bias, 0)),
    boat3_attack_bias: clamp(-0.04, 0.04, toNum(venue?.boat3_attack_bias, 0)),
    boat4_cado_bias: clamp(-0.04, 0.04, toNum(venue?.boat4_cado_bias, 0)),
    outer_mix_bias: clamp(-0.04, 0.04, toNum(venue?.outer_mix_bias, 0)),
    upset_risk_bias: clamp(-4, 4, toNum(venue?.upset_risk_bias, 0)),
    launch_threshold_bias: clamp(-2, 2, toNum(venue?.launch_threshold_bias, 0))
  };
  return {
    venue_key: venueKey,
    values: calibration
  };
}

function getLaunchStateConfig() {
  return {
    st_margins: { ...LAUNCH_STATE_CONFIG.st_margins },
    score_thresholds: { ...LAUNCH_STATE_CONFIG.score_thresholds },
    score_weights: { ...LAUNCH_STATE_CONFIG.score_weights },
    event_thresholds: { ...LAUNCH_STATE_CONFIG.event_thresholds }
  };
}

function getLaunchStateConfigWithVenueCalibration(venueCalibration = null) {
  const config = getLaunchStateConfig();
  const launchBias = clamp(-2, 2, toNum(venueCalibration?.values?.launch_threshold_bias, 0));
  if (launchBias !== 0) {
    config.score_thresholds.LAUNCH_SCORE_STRONG_OUT = Number((config.score_thresholds.LAUNCH_SCORE_STRONG_OUT + launchBias).toFixed(2));
    config.score_thresholds.LAUNCH_SCORE_OUT = Number((config.score_thresholds.LAUNCH_SCORE_OUT + launchBias * 0.7).toFixed(2));
    config.score_thresholds.LAUNCH_SCORE_HOLLOW = Number((config.score_thresholds.LAUNCH_SCORE_HOLLOW - launchBias * 0.7).toFixed(2));
    config.score_thresholds.LAUNCH_SCORE_STRONG_HOLLOW = Number((config.score_thresholds.LAUNCH_SCORE_STRONG_HOLLOW - launchBias).toFixed(2));
  }
  return config;
}

function normalizeLaunchStateLabelWithThresholds(score, thresholds = LAUNCH_STATE_CONFIG.score_thresholds) {
  const value = toNum(score, 0);
  if (value >= toNum(thresholds?.LAUNCH_SCORE_STRONG_OUT, 28)) return "strong_out";
  if (value >= toNum(thresholds?.LAUNCH_SCORE_OUT, 10)) return "out";
  if (value <= toNum(thresholds?.LAUNCH_SCORE_STRONG_HOLLOW, -28)) return "strong_hollow";
  if (value <= toNum(thresholds?.LAUNCH_SCORE_HOLLOW, -10)) return "hollow";
  return "neutral";
}

function launchStateLevel(label) {
  switch (String(label || "")) {
    case "strong_out":
      return 2;
    case "out":
      return 1;
    case "neutral":
      return 0;
    case "hollow":
      return -1;
    case "strong_hollow":
      return -2;
    default:
      return 0;
  }
}

function launchEventTriggered(value, threshold) {
  return toNum(value, 0) >= toNum(threshold, 0) ? 1 : 0;
}

function hasFHolderSignal(features = {}) {
  return toNum(features?.f_hold_count, 0) > 0 || toNum(features?.f_hold_bias_applied, 0) === 1 || toNum(features?.f_hold_caution_penalty, 0) > 0;
}

function getLaneBaseAdvantage(lane) {
  switch (toInt(lane, 0)) {
    case 1:
      return 16;
    case 2:
      return 11;
    case 3:
      return 7;
    case 4:
      return 3;
    case 5:
      return -2;
    case 6:
      return -6;
    default:
      return 0;
  }
}

function getFHolderPenaltyByRole(features = {}, lane = null) {
  const hasF = hasFHolderSignal(features);
  const caution = toNum(features?.f_hold_caution_penalty, 0);
  if (!hasF && caution <= 0) {
    return {
      has_f_holder: 0,
      first_penalty: 0,
      second_penalty: 0,
      third_penalty: 0
    };
  }
  const mildFirst = 1.8 + caution * 1.15;
  const strongFirst = 5.2 + caution * 2.15;
  return {
    has_f_holder: 1,
    first_penalty: Number((toInt(lane, 0) === 1 ? mildFirst : strongFirst).toFixed(2)),
    second_penalty: Number((0.25 + caution * (toInt(lane, 0) === 1 ? 0.22 : 0.34)).toFixed(2)),
    third_penalty: Number((0.08 + caution * 0.12).toFixed(2))
  };
}

function computeStartAdvantageScore({ row, rows }) {
  const lane = toInt(row?.racer?.lane, null);
  const f = row?.features || {};
  const laneBaseAdvantage = getLaneBaseAdvantage(lane);
  const rawExhibitionSt = toNum(f?.expected_actual_st ?? f?.exhibition_st ?? row?.racer?.exhibitionSt ?? f?.avg_st, null);
  const rawExhibitionStContribution = Number(
    (
      (Number.isFinite(rawExhibitionSt) ? Math.max(-10, Math.min(18, (0.19 - rawExhibitionSt) * 150)) : 0) +
      Math.max(0, 7 - toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6)) * 2.6
    ).toFixed(2)
  );
  const correctedStInterpretation = Number((rawExhibitionStContribution + laneBaseAdvantage * 0.78).toFixed(2));
  const entryContextBonus = Number(
    (
      toNum(f?.entry_advantage_score, 0) * 0.72 +
      Math.max(0, toNum(f?.avg_st_rank_delta_vs_left, 0)) * 1.8
    ).toFixed(2)
  );
  const wallContextBonus = Number(
    (
      Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * 18 +
      toNum(f?.slit_alert_flag, 0) * 6 +
      (lane <= 2 ? 2 : 0)
    ).toFixed(2)
  );
  const fRolePenalty = getFHolderPenaltyByRole(f, lane);
  const fPenaltyComponent = Number((-toNum(fRolePenalty?.first_penalty, 0)).toFixed(2));
  const finalStartAdvantageScore = Number(
    clamp(
      -100,
      100,
      laneBaseAdvantage +
        correctedStInterpretation +
        entryContextBonus +
        wallContextBonus +
        fPenaltyComponent
    ).toFixed(2)
  );
  return {
    lane_base_advantage: laneBaseAdvantage,
    raw_exhibition_st_contribution: rawExhibitionStContribution,
    corrected_st_interpretation: correctedStInterpretation,
    entry_context_bonus: entryContextBonus,
    wall_context_bonus: wallContextBonus,
    f_penalty_component: fPenaltyComponent,
    start_advantage_score: finalStartAdvantageScore,
    f_holder_penalty_by_role: fRolePenalty
  };
}

function computeLaunchStateScores(rows, venueCalibration = null) {
  const laneRows = safeArray(rows);
  const config = getLaunchStateConfigWithVenueCalibration(venueCalibration);
  const weights = config.score_weights;
  const stByLane = new Map(
    laneRows
      .map((row) => [
        toInt(row?.racer?.lane, null),
        toNum(row?.features?.expected_actual_st ?? row?.racer?.exhibitionSt ?? row?.features?.avg_st, null)
      ])
      .filter(([lane, value]) => Number.isInteger(lane) && Number.isFinite(value))
  );
  return laneRows
    .map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      if (!Number.isInteger(lane)) return null;
      const f = row?.features || {};
      const startAdvantage = computeStartAdvantageScore({ row, rows: laneRows });
      const selfSt = toNum(stByLane.get(lane), null);
      const leftSt = toNum(stByLane.get(lane - 1), null);
      const insideStValues = [...stByLane.entries()]
        .filter(([otherLane]) => otherLane < lane)
        .map(([, value]) => value)
        .filter(Number.isFinite);
      const insideAvgSt = insideStValues.length
        ? insideStValues.reduce((sum, value) => sum + value, 0) / insideStValues.length
        : null;
      const stMarginVsLeft = Number.isFinite(selfSt) && Number.isFinite(leftSt) ? Number((leftSt - selfSt).toFixed(3)) : null;
      const stMarginVsInside = Number.isFinite(selfSt) && Number.isFinite(insideAvgSt)
        ? Number((insideAvgSt - selfSt).toFixed(3))
        : null;
      const stRankComponent =
        Math.max(0, 4 - toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6)) * toNum(weights?.st_rank_weight, 8) -
        Math.max(0, toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6) - 3) * toNum(weights?.st_rank_late_penalty, 7);
      const neighborMarginComponent = Number.isFinite(stMarginVsLeft)
        ? stMarginVsLeft * toNum(weights?.neighbor_margin_weight, 240)
        : 0;
      const insideMarginComponent = Number.isFinite(stMarginVsInside)
        ? stMarginVsInside * toNum(weights?.inside_margin_weight, 160)
        : 0;
      const formationFitComponent = toNum(f?.entry_advantage_score, 0) * toNum(weights?.formation_fit_weight, 0.55);
      const lapSupportComponent = toNum(f?.lap_attack_strength, 0) * toNum(weights?.lap_support_weight, 0.45);
      const environmentComponent =
        Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * toNum(weights?.display_time_positive_weight, 55) -
        Math.max(0, -toNum(f?.display_time_delta_vs_left, 0)) * toNum(weights?.display_time_negative_weight, 30) +
        toNum(f?.slit_alert_flag, 0) * toNum(weights?.slit_alert_weight, 12) -
        toNum(startAdvantage?.f_holder_penalty_by_role?.first_penalty, 0) * toNum(weights?.f_hold_penalty_weight, 1.25) +
        (lane === 1 ? toNum(weights?.lane1_baseline_bonus, 8) : 0) +
        toNum(startAdvantage?.lane_base_advantage, 0) * 0.32;
      const score = Number(
        clamp(
          -100,
          100,
          stRankComponent +
            neighborMarginComponent +
            insideMarginComponent +
            formationFitComponent +
            lapSupportComponent +
            environmentComponent +
            toNum(startAdvantage?.start_advantage_score, 0) * 0.36
        ).toFixed(2)
      );
      const label = normalizeLaunchStateLabelWithThresholds(score, config.score_thresholds);
      return {
        lane,
        score,
        label,
        st_margin_vs_left: stMarginVsLeft,
        st_margin_vs_inside: stMarginVsInside,
        st_rank_component: Number(stRankComponent.toFixed(2)),
        neighbor_margin_component: Number(neighborMarginComponent.toFixed(2)),
        inside_margin_component: Number(insideMarginComponent.toFixed(2)),
        formation_fit_component: Number(formationFitComponent.toFixed(2)),
        lap_support_component: Number(lapSupportComponent.toFixed(2)),
        environment_component: Number(environmentComponent.toFixed(2)),
        lane_base_advantage: toNum(startAdvantage?.lane_base_advantage, 0),
        raw_exhibition_st_contribution: toNum(startAdvantage?.raw_exhibition_st_contribution, 0),
        corrected_st_interpretation: toNum(startAdvantage?.corrected_st_interpretation, 0),
        entry_context_bonus: toNum(startAdvantage?.entry_context_bonus, 0),
        wall_context_bonus: toNum(startAdvantage?.wall_context_bonus, 0),
        f_penalty_component: toNum(startAdvantage?.f_penalty_component, 0),
        f_holder_penalty_by_role: startAdvantage?.f_holder_penalty_by_role || {},
        final_start_advantage_score: toNum(startAdvantage?.start_advantage_score, 0),
        final_launch_state_score: score,
        thresholds_used: config
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.lane - b.lane);
}

function classifyLaunchStates(launchStateScores) {
  return safeArray(launchStateScores).map((row) => ({
    lane: toInt(row?.lane, null),
    label: row?.label || normalizeLaunchStateLabelWithThresholds(row?.score),
    score: toNum(row?.score, 0),
    thresholds_used: row?.thresholds_used || getLaunchStateConfig()
  }));
}

function buildIntermediateDevelopmentEvents({
  launchStateScores,
  rows,
  race,
  headScenarioBalanceAnalysis,
  escapePatternAnalysis,
  venueCalibration
}) {
  const config = getLaunchStateConfig();
  const eventThresholds = config.event_thresholds;
  const effectiveVenueCalibration =
    venueCalibration || getVenueLaunchMicroCalibration({ race, venueSummary: headScenarioBalanceAnalysis?.venue_correction_summary });
  const venueBias = effectiveVenueCalibration?.values || {};
  const launchMap = new Map(
    safeArray(launchStateScores).map((row) => [toInt(row?.lane, null), { score: toNum(row?.score, 0), label: row?.label || "neutral" }])
  );
  const laneRows = new Map(
    safeArray(rows).map((row) => [toInt(row?.racer?.lane, null), row]).filter(([lane]) => Number.isInteger(lane))
  );
  const laneFeature = (lane) => laneRows.get(lane)?.features || {};
  const boat1EscapeBase = toNum(
    safeArray(headScenarioBalanceAnalysis?.head_distribution_json).find((row) => toInt(row?.lane, null) === 1)?.weight,
    0
  );
  const event = (value) => Number(clamp(0, 100, value).toFixed(2));
  const laneState = (lane) => launchStateLevel(launchMap.get(lane)?.label);
  const lane1Out = Math.max(0, toNum(launchMap.get(1)?.score, 0));
  const lane1Hollow = Math.max(0, -toNum(launchMap.get(1)?.score, 0));
  const lane2Out = Math.max(0, toNum(launchMap.get(2)?.score, 0));
  const lane2Hollow = Math.max(0, -toNum(launchMap.get(2)?.score, 0));
  const lane3Out = Math.max(0, toNum(launchMap.get(3)?.score, 0));
  const lane3Hollow = Math.max(0, -toNum(launchMap.get(3)?.score, 0));
  const lane4Out = Math.max(0, toNum(launchMap.get(4)?.score, 0));
  const lane4Hollow = Math.max(0, -toNum(launchMap.get(4)?.score, 0));
  const lane5Out = Math.max(0, toNum(launchMap.get(5)?.score, 0));
  const lane6Out = Math.max(0, toNum(launchMap.get(6)?.score, 0));
  const weakWallOn2 = event(
    lane2Hollow * 0.72 +
      toNum(laneFeature(2)?.f_hold_caution_penalty, 0) * 5 +
      Math.max(0, toNum(laneFeature(2)?.expected_actual_st_rank ?? laneFeature(2)?.st_rank, 6) - 3) * 8
  );
  const weakWallOn3 = event(
    lane3Hollow * 0.7 +
      toNum(laneFeature(3)?.f_hold_caution_penalty, 0) * 4 +
      Math.max(0, toNum(laneFeature(3)?.expected_actual_st_rank ?? laneFeature(3)?.st_rank, 6) - 3) * 7
  );
  const boat3AttackBase = event(
    lane3Out * 0.75 +
      toNum(laneFeature(3)?.lap_attack_strength, 0) * 2.6 +
      toNum(laneFeature(3)?.slit_alert_flag, 0) * 18 +
      Math.max(0, toNum(laneFeature(3)?.display_time_delta_vs_left, 0)) * 85
  );
  const boat3AttackReady =
    laneState(3) >= 1 && (laneState(2) <= -1 || weakWallOn2 >= toNum(eventThresholds?.WEAK_WALL_ON_2_MIN, 34))
      ? boat3AttackBase
      : Number((boat3AttackBase * 0.38).toFixed(2));
  const boat4CadoBase = event(
    lane4Out * 0.78 +
      toNum(laneFeature(4)?.lap_attack_strength, 0) * 2.4 +
      toNum(laneFeature(4)?.entry_advantage_score, 0) * 2.2 +
      toNum(laneFeature(4)?.kado_bonus, 0) * 12 +
      toNum(laneFeature(4)?.slit_alert_flag, 0) * 16
  );
  const boat4CadoReady =
    laneState(4) >= 1 && (laneState(3) <= -1 || weakWallOn3 >= toNum(eventThresholds?.WEAK_WALL_ON_3_MIN, 34))
      ? boat4CadoBase
      : Number((boat4CadoBase * 0.36).toFixed(2));
  const boat5OuterPush = event(
    lane5Out * 0.78 +
      toNum(laneFeature(5)?.lap_attack_strength, 0) * 2.2 +
      toNum(laneFeature(5)?.motor_total_score, 0) * 1.6
  );
  const outerOutCount = [4, 5, 6].filter((lane) => laneState(lane) >= 1).length;
  const outerMixBase = event(
    boat5OuterPush * 0.65 +
      lane6Out * 0.62 +
      (String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead" ? 12 : 0)
  );
  const outerMixReady = outerOutCount >= 2 ? outerMixBase : Number((outerMixBase * 0.45).toFixed(2));
  const innerCollapseBase = event(
    lane1Hollow * 0.55 +
      weakWallOn2 * 0.28 +
      weakWallOn3 * 0.18 +
      outerMixReady * 0.22
  );
  const innerCollapse =
    launchEventTriggered(innerCollapseBase, toNum(eventThresholds?.INNER_COLLAPSE_MIN, 52)) ||
    (laneState(1) <= -1 && outerOutCount >= 2)
      ? innerCollapseBase
      : Number((innerCollapseBase * 0.72).toFixed(2));
  const innerStableBase = event(
    lane1Out * 0.48 +
      boat1EscapeBase * 55 +
      Math.max(0, 55 - innerCollapse) * 0.25 +
      Math.max(0, 20 - lane2Out) * 0.4
  );
  const innerStable =
    laneState(1) >= 0 && laneState(2) >= 0 && boat1EscapeBase >= 0.42
      ? event(innerStableBase + toNum(venueBias?.inner_stable_bias, 0))
      : Number((innerStableBase * 0.68).toFixed(2));
  const venueAdjustedBoat3AttackReady = event(
    boat3AttackReady * (1 + toNum(venueBias?.boat3_attack_bias, 0))
  );
  const venueAdjustedBoat4CadoReady = event(
    boat4CadoReady * (1 + toNum(venueBias?.boat4_cado_bias, 0))
  );
  const venueAdjustedOuterMixReady = event(
    outerMixReady * (1 + toNum(venueBias?.outer_mix_bias, 0))
  );

  return {
    inner_stable: innerStable,
    boat1_out: event(lane1Out),
    boat1_hollow: event(lane1Hollow),
    boat2_out: event(lane2Out),
    boat2_hollow: event(lane2Hollow),
    boat3_attack_ready: venueAdjustedBoat3AttackReady,
    boat3_hollow: event(lane3Hollow),
    boat4_cado_ready: venueAdjustedBoat4CadoReady,
    boat4_hollow: event(lane4Hollow),
    boat5_outer_push: boat5OuterPush,
    outer_mix_ready: venueAdjustedOuterMixReady,
    inner_collapse: innerCollapse,
    weak_wall_on_2: weakWallOn2,
    weak_wall_on_3: weakWallOn3,
    triggered_flags: {
      boat1_out: launchEventTriggered(event(lane1Out), toNum(eventThresholds?.STATE_OUT_MIN, 10)),
      boat1_hollow: launchEventTriggered(event(lane1Hollow), Math.abs(toNum(eventThresholds?.STATE_HOLLOW_MAX, -10))),
      boat2_out: launchEventTriggered(event(lane2Out), toNum(eventThresholds?.STATE_OUT_MIN, 10)),
      boat2_hollow: laneState(2) <= -1 || weakWallOn2 >= toNum(eventThresholds?.WEAK_WALL_ON_2_MIN, 34) ? 1 : 0,
      boat3_attack_ready:
        laneState(3) >= 1 && (laneState(2) <= -1 || weakWallOn2 >= toNum(eventThresholds?.WEAK_WALL_ON_2_MIN, 34)) ? 1 : 0,
      boat3_hollow: laneState(3) <= -1 ? 1 : 0,
      boat4_cado_ready:
        laneState(4) >= 1 && (laneState(3) <= -1 || weakWallOn3 >= toNum(eventThresholds?.WEAK_WALL_ON_3_MIN, 34)) ? 1 : 0,
      boat4_hollow: laneState(4) <= -1 ? 1 : 0,
      boat5_outer_push: boat5OuterPush >= toNum(eventThresholds?.BOAT5_OUTER_PUSH_MIN, 40) ? 1 : 0,
      outer_mix_ready: outerOutCount >= 2 && venueAdjustedOuterMixReady >= toNum(eventThresholds?.OUTER_MIX_READY_MIN, 42) ? 1 : 0,
      inner_stable:
        laneState(1) >= 0 &&
        laneState(2) >= 0 &&
        innerStable >= toNum(eventThresholds?.INNER_STABLE_MIN, 48) ? 1 : 0,
      inner_collapse:
        (laneState(1) <= -1 && outerOutCount >= 2) ||
        innerCollapse >= toNum(eventThresholds?.INNER_COLLAPSE_MIN, 52) ? 1 : 0,
      weak_wall_on_2: weakWallOn2 >= toNum(eventThresholds?.WEAK_WALL_ON_2_MIN, 34) ? 1 : 0,
      weak_wall_on_3: weakWallOn3 >= toNum(eventThresholds?.WEAK_WALL_ON_3_MIN, 34) ? 1 : 0
    },
    thresholds_used: config,
    venue_calibration_used: effectiveVenueCalibration,
    venue_adjusted_events: {
      inner_stable: {
        before: innerStableBase,
        after: innerStable,
        delta: Number((innerStable - innerStableBase).toFixed(2))
      },
      boat3_attack_ready: {
        before: boat3AttackReady,
        after: venueAdjustedBoat3AttackReady,
        delta: Number((venueAdjustedBoat3AttackReady - boat3AttackReady).toFixed(2))
      },
      boat4_cado_ready: {
        before: boat4CadoReady,
        after: venueAdjustedBoat4CadoReady,
        delta: Number((venueAdjustedBoat4CadoReady - boat4CadoReady).toFixed(2))
      },
      outer_mix_ready: {
        before: outerMixReady,
        after: venueAdjustedOuterMixReady,
        delta: Number((venueAdjustedOuterMixReady - outerMixReady).toFixed(2))
      }
    }
  };
}

function computeRaceScenarioProbabilities({
  intermediateEvents,
  rows,
  race,
  attackScenarioAnalysis,
  escapePatternAnalysis,
  outsideHeadPromotionContext,
  headScenarioBalanceAnalysis,
  venueCalibration
}) {
  const effectiveVenueCalibration =
    venueCalibration || getVenueLaunchMicroCalibration({ race, venueSummary: headScenarioBalanceAnalysis?.venue_correction_summary });
  const venueBias = effectiveVenueCalibration?.values || {};
  const laneRows = new Map(
    safeArray(rows).map((row) => [toInt(row?.racer?.lane, null), row]).filter(([lane]) => Number.isInteger(lane))
  );
  const laneFeature = (lane) => laneRows.get(lane)?.features || {};
  const boat1EscapeBase = toNum(
    safeArray(headScenarioBalanceAnalysis?.head_distribution_json).find((row) => toInt(row?.lane, null) === 1)?.weight,
    0
  );
  const survivalResidual = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const playerAdj = (lane) =>
    toNum(laneFeature(lane)?.player_strength_blended, 0) * 2.4 +
    toNum(laneFeature(lane)?.player_recent_3_months_strength, 0) * 1.2;
  const preAdjustedRawScores = {
    boat1_escape:
      28 +
      toNum(intermediateEvents?.inner_stable, 0) * 0.78 +
      toNum(intermediateEvents?.boat1_out, 0) * 0.52 +
      boat1EscapeBase * 60 +
      Math.min(18, survivalResidual * 0.28) -
      toNum(intermediateEvents?.boat1_hollow, 0) * 0.88 -
      toNum(intermediateEvents?.inner_collapse, 0) * 0.34,
    boat2_sashi:
      toNum(intermediateEvents?.boat1_hollow, 0) * 0.46 +
      toNum(intermediateEvents?.boat2_out, 0) * 0.68 +
      toNum(intermediateEvents?.weak_wall_on_2, 0) * 0.34 +
      toNum(attackScenarioAnalysis?.two_sashi_score, 0) * 0.56 +
      playerAdj(2),
    boat3_makuri:
      toNum(intermediateEvents?.boat2_hollow, 0) * 0.42 +
      toNum(intermediateEvents?.boat3_attack_ready, 0) * 0.72 +
      toNum(attackScenarioAnalysis?.three_makuri_score, 0) * 0.62 +
      playerAdj(3) +
      toNum(laneFeature(3)?.motor_total_score, 0) * 0.8 -
      boat1EscapeBase * 14,
    boat3_makuri_sashi:
      toNum(intermediateEvents?.boat1_hollow, 0) * 0.26 +
      toNum(intermediateEvents?.boat2_hollow, 0) * 0.34 +
      toNum(intermediateEvents?.boat3_attack_ready, 0) * 0.62 +
      toNum(attackScenarioAnalysis?.three_makuri_sashi_score, 0) * 0.66 +
      playerAdj(3),
    boat4_cado_attack:
      toNum(intermediateEvents?.boat3_hollow, 0) * 0.42 +
      toNum(intermediateEvents?.boat4_cado_ready, 0) * 0.78 +
      Math.max(
        toNum(attackScenarioAnalysis?.four_cado_makuri_score, 0),
        toNum(attackScenarioAnalysis?.four_cado_makuri_sashi_score, 0)
      ) * 0.6 +
      playerAdj(4) +
      toNum(intermediateEvents?.weak_wall_on_3, 0) * 0.24,
    chaos_outer_mix:
      toNum(intermediateEvents?.outer_mix_ready, 0) * 0.68 +
      toNum(intermediateEvents?.inner_collapse, 0) * 0.58 +
      toNum(outsideHeadPromotionContext?.inner_collapse_score, 0) * 0.22 +
      (String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead" ? 18 : 0)
  };
  const rawScores = {
    ...preAdjustedRawScores,
    boat1_escape: preAdjustedRawScores.boat1_escape * (1 + toNum(venueBias?.boat1_escape_bias, 0)),
    boat3_makuri: preAdjustedRawScores.boat3_makuri * (1 + toNum(venueBias?.boat3_attack_bias, 0)),
    boat3_makuri_sashi: preAdjustedRawScores.boat3_makuri_sashi * (1 + toNum(venueBias?.boat3_attack_bias, 0) * 0.9),
    boat4_cado_attack: preAdjustedRawScores.boat4_cado_attack * (1 + toNum(venueBias?.boat4_cado_bias, 0)),
    chaos_outer_mix: preAdjustedRawScores.chaos_outer_mix * (1 + toNum(venueBias?.outer_mix_bias, 0))
  };
  const preNormalized = normalizeDistributionRows(
    Object.entries(preAdjustedRawScores).map(([scenario, weight]) => ({
      lane: ({ boat1_escape: 1, boat2_sashi: 2, boat3_makuri: 3, boat3_makuri_sashi: 3, boat4_cado_attack: 4, chaos_outer_mix: 5 })[scenario],
      role: scenario,
      weight
    }))
  );
  const preMap = new Map(preNormalized.map((row) => [row.role, row.weight]));
  const normalized = normalizeDistributionRows(
    Object.entries(rawScores).map(([scenario, weight]) => ({
      lane: ({ boat1_escape: 1, boat2_sashi: 2, boat3_makuri: 3, boat3_makuri_sashi: 3, boat4_cado_attack: 4, chaos_outer_mix: 5 })[scenario],
      role: scenario,
      weight
    }))
  ).map((row) => ({
    scenario: row.role,
    probability: row.weight,
    pre_adjustment_probability: Number(toNum(preMap.get(row.role), 0).toFixed(4)),
    venue_adjustment_delta: Number((toNum(row.weight, 0) - toNum(preMap.get(row.role), 0)).toFixed(4)),
    venue_calibration_used: effectiveVenueCalibration
  }));
  return normalized.sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0));
}

const PDF_SCENARIO_PRIOR_DICTIONARY = [
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "逃げ（鉄板）",
    priority_rank: "A",
    entry_shape: "stable_inside",
    representative_tickets: ["1-2-34", "1-3-24"],
    backup_tickets: ["1-2-5", "1-3-5"],
    key_exhibition_signals: ["inner_stable", "boat1_out", "boat1_escape_strong"],
    success_conditions: ["boat1_escape_strong", "inner_stable"],
    rejection_conditions: ["inner_collapse_strong", "chaos_high"],
    wall_or_cado_notes: "1と2の内壁が維持される形を優先",
    venue_notes: "イン有利水面で少し強化",
    memo: "安定イン逃げの基準シナリオ"
  },
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "逃げ（軽技）",
    priority_rank: "B",
    entry_shape: "inside_light_attack",
    representative_tickets: ["1-2-34", "1-3-24"],
    backup_tickets: ["1-4-23"],
    key_exhibition_signals: ["boat1_escape_mid", "inner_stable"],
    success_conditions: ["boat1_escape_mid"],
    rejection_conditions: ["inner_collapse_strong", "boat4_cado_ready_high"],
    wall_or_cado_notes: "軽い攻めを受けても1残り優先",
    venue_notes: "",
    memo: "逃げ主体だが相手筆頭が揺れる時の基本"
  },
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "逃げ（3攻め残り）",
    priority_rank: "A",
    entry_shape: "3_attack_residual",
    representative_tickets: ["1-3-24", "1-3-45"],
    backup_tickets: ["1-2-3", "1-4-3"],
    key_exhibition_signals: ["boat3_attack_ready", "boat1_escape_mid"],
    success_conditions: ["boat3_attack_ready", "boat1_escape_mid"],
    rejection_conditions: ["boat3_head_clear", "inner_collapse_strong"],
    wall_or_cado_notes: "3の攻めを受けても1残りを前提",
    venue_notes: "",
    memo: "3攻めを頭固定ではなく1残りに読む"
  },
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "逃げ（4カド警戒）",
    priority_rank: "A",
    entry_shape: "4_cado_warning",
    representative_tickets: ["1-4-23", "1-3-4"],
    backup_tickets: ["1-2-4"],
    key_exhibition_signals: ["boat4_cado_ready", "boat1_escape_mid"],
    success_conditions: ["boat4_cado_ready", "boat1_escape_mid"],
    rejection_conditions: ["boat4_head_clear", "inner_collapse_strong"],
    wall_or_cado_notes: "4カドを受けて相手4を上げる",
    venue_notes: "",
    memo: "4の圧は相手強化として扱う"
  },
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "逃げ（カド連動）",
    priority_rank: "B",
    entry_shape: "cado_linked",
    representative_tickets: ["1-4-23", "1-2-4"],
    backup_tickets: ["1-3-4"],
    key_exhibition_signals: ["boat4_cado_ready", "weak_wall_on_3"],
    success_conditions: ["boat4_cado_ready"],
    rejection_conditions: ["chaos_high", "outer_mix_ready_high"],
    wall_or_cado_notes: "3の壁弱化を伴う4連動",
    venue_notes: "",
    memo: "4を2,3着寄りに使う"
  },
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "逃げ（奇数スジ）",
    priority_rank: "B",
    entry_shape: "suji_inside",
    representative_tickets: ["1-3-25", "1-2-35"],
    backup_tickets: ["1-3-4"],
    key_exhibition_signals: ["boat1_escape_mid", "suji_preferred"],
    success_conditions: ["boat1_escape_mid", "suji_preferred"],
    rejection_conditions: ["inner_collapse_strong"],
    wall_or_cado_notes: "1頭時のスジ相手を優先",
    venue_notes: "",
    memo: "スジは補助的に使用"
  },
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "逃げ（外枠3着拾い）",
    priority_rank: "C",
    entry_shape: "outer_third_pickup",
    representative_tickets: ["1-2-5", "1-3-5", "1-2-6"],
    backup_tickets: ["1-4-5"],
    key_exhibition_signals: ["outer_mix_ready", "boat1_escape_mid"],
    success_conditions: ["boat1_escape_mid", "outer_mix_ready"],
    rejection_conditions: ["boat5_head_clear", "boat6_head_clear"],
    wall_or_cado_notes: "外の圧は3着拾い中心",
    venue_notes: "",
    memo: "外枠は頭ではなく3着優先"
  },
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "展開拾い",
    priority_rank: "C",
    entry_shape: "development_pickup",
    representative_tickets: ["1-3-45", "1-4-35"],
    backup_tickets: ["1-2-45"],
    key_exhibition_signals: ["boat3_attack_ready", "boat4_cado_ready", "outer_mix_ready"],
    success_conditions: ["boat1_escape_mid"],
    rejection_conditions: ["boat1_escape_strong"],
    wall_or_cado_notes: "展開差し残りの拾い",
    venue_notes: "",
    memo: "安定レースでは主役化しない"
  },
  {
    winning_boat: 1,
    development_category: "boat1_escape",
    scenario_name: "超・展開拾い",
    priority_rank: "D",
    entry_shape: "super_development_pickup",
    representative_tickets: ["1-5-46", "1-6-45"],
    backup_tickets: ["1-4-56"],
    key_exhibition_signals: ["outer_mix_ready", "chaos_high"],
    success_conditions: ["outer_mix_ready", "chaos_high", "upset_high"],
    rejection_conditions: ["boat1_escape_strong", "inner_stable"],
    wall_or_cado_notes: "大きく崩れた時だけ外を拾う",
    venue_notes: "",
    memo: "大穴専用"
  },
  {
    winning_boat: 2,
    development_category: "boat2",
    scenario_name: "差し",
    priority_rank: "A",
    entry_shape: "2_sashi",
    representative_tickets: ["2-1-34", "2-3-14"],
    backup_tickets: ["2-1-5"],
    key_exhibition_signals: ["boat2_out", "boat1_hollow"],
    success_conditions: ["boat2_sashi_high"],
    rejection_conditions: ["inner_stable", "boat1_escape_strong"],
    wall_or_cado_notes: "2差しが決まる形",
    venue_notes: "",
    memo: "2頭の本線"
  },
  {
    winning_boat: 2,
    development_category: "boat2",
    scenario_name: "捲り",
    priority_rank: "C",
    entry_shape: "2_makuri",
    representative_tickets: ["2-3-14", "2-1-34"],
    backup_tickets: ["2-4-13"],
    key_exhibition_signals: ["boat2_out", "weak_wall_on_2"],
    success_conditions: ["boat2_sashi_high", "boat1_hollow"],
    rejection_conditions: ["inner_stable"],
    wall_or_cado_notes: "2が壁を消してまくる形",
    venue_notes: "",
    memo: "差しより条件厳しめ"
  },
  {
    winning_boat: 2,
    development_category: "boat2",
    scenario_name: "ジカ捲り穴",
    priority_rank: "D",
    entry_shape: "2_direct_makuri_hole",
    representative_tickets: ["2-4-13", "2-5-13"],
    backup_tickets: ["2-3-45"],
    key_exhibition_signals: ["boat2_out", "chaos_high"],
    success_conditions: ["boat1_hollow", "chaos_high", "upset_high"],
    rejection_conditions: ["boat1_escape_strong"],
    wall_or_cado_notes: "大穴限定",
    venue_notes: "",
    memo: "通常は出さない"
  },
  {
    winning_boat: 2,
    development_category: "boat2",
    scenario_name: "中凹み連動",
    priority_rank: "B",
    entry_shape: "middle_collapse",
    representative_tickets: ["2-1-34", "2-3-14"],
    backup_tickets: ["2-4-13"],
    key_exhibition_signals: ["weak_wall_on_2", "inner_collapse"],
    success_conditions: ["weak_wall_on_2"],
    rejection_conditions: ["inner_stable"],
    wall_or_cado_notes: "内壁緩み連動",
    venue_notes: "",
    memo: "2差し系の補助"
  },
  {
    winning_boat: 2,
    development_category: "boat2",
    scenario_name: "内枠崩壊",
    priority_rank: "C",
    entry_shape: "inner_breakdown",
    representative_tickets: ["2-4-13", "2-5-13"],
    backup_tickets: ["2-3-45"],
    key_exhibition_signals: ["inner_collapse", "chaos_high"],
    success_conditions: ["inner_collapse", "boat1_hollow"],
    rejection_conditions: ["boat1_escape_strong"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "崩壊時だけ"
  },
  {
    winning_boat: 3,
    development_category: "boat3",
    scenario_name: "捲り",
    priority_rank: "B",
    entry_shape: "3_makuri",
    representative_tickets: ["3-1-24", "3-2-14"],
    backup_tickets: ["1-3-24"],
    key_exhibition_signals: ["boat3_attack_ready", "weak_wall_on_2"],
    success_conditions: ["boat3_attack_high"],
    rejection_conditions: ["boat3_weak_st_suppressed", "inner_stable"],
    wall_or_cado_notes: "3攻め本体",
    venue_notes: "",
    memo: "3攻めでも1残りと分離して扱う"
  },
  {
    winning_boat: 3,
    development_category: "boat3",
    scenario_name: "捲り差し",
    priority_rank: "B",
    entry_shape: "3_makuri_sashi",
    representative_tickets: ["3-1-24", "1-3-24"],
    backup_tickets: ["3-2-14"],
    key_exhibition_signals: ["boat3_attack_ready", "boat1_hollow"],
    success_conditions: ["boat3_makuri_sashi_high"],
    rejection_conditions: ["boat3_weak_st_suppressed"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "差し寄りでも1残り余地を残す"
  },
  {
    winning_boat: 3,
    development_category: "boat3",
    scenario_name: "捲り連動",
    priority_rank: "C",
    entry_shape: "3_linked_attack",
    representative_tickets: ["1-3-24", "3-1-24"],
    backup_tickets: ["1-4-3"],
    key_exhibition_signals: ["boat3_attack_ready"],
    success_conditions: ["boat3_attack_ready"],
    rejection_conditions: ["inner_stable", "boat3_weak_st_suppressed"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "3攻め残り用"
  },
  {
    winning_boat: 3,
    development_category: "boat3",
    scenario_name: "4飛ばし",
    priority_rank: "C",
    entry_shape: "3_beats_4",
    representative_tickets: ["1-3-2", "3-1-2"],
    backup_tickets: ["1-3-4"],
    key_exhibition_signals: ["boat3_attack_ready", "boat4_hollow"],
    success_conditions: ["boat3_attack_ready"],
    rejection_conditions: ["boat4_cado_ready_high"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "4を消して3残し"
  },
  {
    winning_boat: 3,
    development_category: "boat3",
    scenario_name: "周回りマーク差し",
    priority_rank: "D",
    entry_shape: "3_lap_mark",
    representative_tickets: ["3-4-15", "1-3-45"],
    backup_tickets: ["3-1-45"],
    key_exhibition_signals: ["lap_override_high", "chaos_high"],
    success_conditions: ["boat3_attack_ready", "lap_override_high", "upset_high"],
    rejection_conditions: ["boat3_weak_st_suppressed"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "周回り型の穴"
  },
  {
    winning_boat: 4,
    development_category: "boat4",
    scenario_name: "捲り（カド）",
    priority_rank: "B",
    entry_shape: "4_cado",
    representative_tickets: ["4-1-23", "4-2-13"],
    backup_tickets: ["1-4-23"],
    key_exhibition_signals: ["boat4_cado_ready", "weak_wall_on_3"],
    success_conditions: ["boat4_cado_high"],
    rejection_conditions: ["inner_stable"],
    wall_or_cado_notes: "4カド本体",
    venue_notes: "",
    memo: "4頭は条件成立時のみ"
  },
  {
    winning_boat: 4,
    development_category: "boat4",
    scenario_name: "捲り差し",
    priority_rank: "B",
    entry_shape: "4_cado_sashi",
    representative_tickets: ["4-1-23", "1-4-23"],
    backup_tickets: ["4-2-13"],
    key_exhibition_signals: ["boat4_cado_ready", "boat3_hollow"],
    success_conditions: ["boat4_cado_high"],
    rejection_conditions: ["inner_stable"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "4の差し残り/頭両睨み"
  },
  {
    winning_boat: 4,
    development_category: "boat4",
    scenario_name: "捲り差し穴",
    priority_rank: "D",
    entry_shape: "4_cado_hole",
    representative_tickets: ["4-5-12", "4-1-56"],
    backup_tickets: ["1-4-56"],
    key_exhibition_signals: ["boat4_cado_ready", "chaos_high"],
    success_conditions: ["boat4_cado_high", "upset_high", "inner_collapse_strong"],
    rejection_conditions: ["boat1_escape_strong"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "大穴限定"
  },
  {
    winning_boat: 4,
    development_category: "boat4",
    scenario_name: "間割り穴",
    priority_rank: "D",
    entry_shape: "4_gap_hole",
    representative_tickets: ["4-2-13", "4-1-23"],
    backup_tickets: ["1-4-23"],
    key_exhibition_signals: ["boat4_cado_ready", "weak_wall_on_3", "chaos_high"],
    success_conditions: ["boat4_cado_high", "chaos_high"],
    rejection_conditions: ["inner_stable"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "間が開く時だけ"
  },
  {
    winning_boat: 4,
    development_category: "boat4",
    scenario_name: "イン残りカド捲り",
    priority_rank: "A",
    entry_shape: "1_residual_4_attack",
    representative_tickets: ["1-4-23", "1-3-4"],
    backup_tickets: ["4-1-23"],
    key_exhibition_signals: ["boat4_cado_ready", "boat1_escape_mid"],
    success_conditions: ["boat4_cado_ready", "boat1_escape_mid"],
    rejection_conditions: ["inner_collapse_strong"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "4攻めでも1残り優先"
  },
  {
    winning_boat: 5,
    development_category: "boat5",
    scenario_name: "展開／差し",
    priority_rank: "C",
    entry_shape: "5_development_sashi",
    representative_tickets: ["5-1-23", "1-5-23"],
    backup_tickets: ["1-3-5"],
    key_exhibition_signals: ["outer_mix_ready", "inner_collapse"],
    success_conditions: ["outer_mix_ready", "upset_high"],
    rejection_conditions: ["boat5_head_blocked", "boat1_escape_strong"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "5は展開依存"
  },
  {
    winning_boat: 5,
    development_category: "boat5",
    scenario_name: "マーク差し",
    priority_rank: "D",
    entry_shape: "5_mark_sashi",
    representative_tickets: ["5-4-12", "1-5-46"],
    backup_tickets: ["5-1-46"],
    key_exhibition_signals: ["outer_mix_ready", "chaos_high"],
    success_conditions: ["outer_mix_ready", "upset_high", "inner_collapse_strong"],
    rejection_conditions: ["boat5_head_blocked"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "大穴専用"
  },
  {
    winning_boat: 5,
    development_category: "boat5",
    scenario_name: "カド展開逆転",
    priority_rank: "D",
    entry_shape: "5_reverse_outer",
    representative_tickets: ["5-1-46", "5-4-16"],
    backup_tickets: ["1-5-46"],
    key_exhibition_signals: ["outer_mix_ready", "boat4_cado_ready"],
    success_conditions: ["outer_mix_ready", "chaos_high", "upset_high"],
    rejection_conditions: ["boat5_head_blocked", "boat1_escape_strong"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "通常は出さない"
  },
  {
    winning_boat: 6,
    development_category: "boat6",
    scenario_name: "大外一閃",
    priority_rank: "D",
    entry_shape: "6_outer_flash",
    representative_tickets: ["6-1-45", "6-4-15"],
    backup_tickets: ["1-6-45"],
    key_exhibition_signals: ["outer_mix_ready", "chaos_high"],
    success_conditions: ["outer_mix_ready", "upset_high", "inner_collapse_strong"],
    rejection_conditions: ["boat6_head_blocked", "boat1_escape_strong"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "大穴専用"
  },
  {
    winning_boat: 6,
    development_category: "boat6",
    scenario_name: "3着固定",
    priority_rank: "C",
    entry_shape: "6_third_anchor",
    representative_tickets: ["1-2-6", "1-3-6", "4-1-6"],
    backup_tickets: ["2-1-6"],
    key_exhibition_signals: ["outer_mix_ready"],
    success_conditions: ["outer_mix_ready"],
    rejection_conditions: ["boat6_head_blocked"],
    wall_or_cado_notes: "",
    venue_notes: "",
    memo: "6は3着固定寄りで扱う"
  }
];

function getScenarioPriorityBaseWeight(rank) {
  switch (String(rank || "").toUpperCase()) {
    case "A": return 1;
    case "B": return 0.72;
    case "C": return 0.42;
    case "D": return 0.18;
    default: return 0.3;
  }
}

function expandDictionaryTicketPattern(pattern) {
  const value = String(pattern || "").trim();
  if (!/^\d-\d-\d+$/.test(value.replace(/[^\d-]/g, "")) && value.split("-").length !== 3) return [];
  const parts = value.split("-");
  if (parts.length !== 3) return [];
  const heads = parts[0].split("").map((v) => toInt(v, null)).filter(Number.isInteger);
  const seconds = parts[1].split("").map((v) => toInt(v, null)).filter(Number.isInteger);
  const thirds = parts[2].split("").map((v) => toInt(v, null)).filter(Number.isInteger);
  const combos = [];
  for (const head of heads) {
    for (const second of seconds) {
      for (const third of thirds) {
        if (new Set([head, second, third]).size !== 3) continue;
        combos.push(`${head}-${second}-${third}`);
      }
    }
  }
  return [...new Set(combos)];
}

function buildScenarioDictionaryContext({
  rows,
  race,
  escapePatternAnalysis,
  attackScenarioAnalysis,
  headScenarioBalanceAnalysis,
  candidateDistributions
}) {
  const firstMap = new Map(
    safeArray(candidateDistributions?.first_place_probability_json || candidateDistributions?.first_place_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
  );
  const finishOverrideMap = new Map(
    Object.entries(candidateDistributions?.finish_override_strength_by_lane_json || {})
      .map(([lane, value]) => [toInt(lane, null), value])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const eventFlags = candidateDistributions?.intermediate_development_events_json?.triggered_flags || {};
  const scenarioProbabilities = safeArray(candidateDistributions?.race_scenario_probabilities_json);
  const scenarioMap = new Map(scenarioProbabilities.map((row) => [String(row?.scenario || ""), toNum(row?.probability, 0)]));
  const attackType = String(attackScenarioAnalysis?.attack_scenario_type || "");
  const chaosRisk = toNum(headScenarioBalanceAnalysis?.chaos_risk_score, 0);
  const upsetRiskProxy =
    toNum(candidateDistributions?.outside_head_promotion_gate_json?.inner_collapse_score, 0) * 0.42 +
    toNum(candidateDistributions?.intermediate_development_events_json?.outer_mix_ready, 0) * 0.26 +
    Math.max(0, 0.42 - toNum(candidateDistributions?.boat1_escape_probability, 0)) * 100 * 0.18 +
    (attackType === "outside_lead" ? 8 : 0);
  const signalTags = new Set();
  const formationPattern = String(escapePatternAnalysis?.formation_pattern || "");
  if (formationPattern) signalTags.add(`formation:${formationPattern}`);
  if (formationPattern === "outside_lead") signalTags.add("outside_lead");
  if (mainHeadLaneFromMap(firstMap) === 1) signalTags.add("boat1_head");
  if (toNum(candidateDistributions?.boat1_escape_probability, 0) >= 0.42) signalTags.add("boat1_escape_strong");
  if (toNum(candidateDistributions?.boat1_escape_probability, 0) >= 0.3) signalTags.add("boat1_escape_mid");
  if (toInt(eventFlags?.inner_stable, 0) === 1) signalTags.add("inner_stable");
  if (toInt(eventFlags?.inner_collapse, 0) === 1 || toNum(candidateDistributions?.outside_head_promotion_gate_json?.inner_collapse_score, 0) >= 58) {
    signalTags.add("inner_collapse_strong");
  }
  if (toInt(eventFlags?.boat3_attack_ready, 0) === 1) signalTags.add("boat3_attack_ready");
  if (toInt(eventFlags?.boat4_cado_ready, 0) === 1) signalTags.add("boat4_cado_ready");
  if (toInt(eventFlags?.outer_mix_ready, 0) === 1) signalTags.add("outer_mix_ready");
  if (attackType.includes("three") && toNum(attackScenarioAnalysis?.attack_scenario_score, 0) >= 60) signalTags.add("boat3_attack_ready");
  if (attackType.includes("four_cado") && toNum(attackScenarioAnalysis?.attack_scenario_score, 0) >= 60) signalTags.add("boat4_cado_ready");
  if (toNum(candidateDistributions?.intermediate_development_events_json?.boat4_cado_ready, 0) >= 56) signalTags.add("boat4_cado_ready_high");
  if (toNum(candidateDistributions?.intermediate_development_events_json?.outer_mix_ready, 0) >= 58) signalTags.add("outer_mix_ready_high");
  if (toNum(scenarioMap.get("boat2_sashi"), 0) >= 0.2) signalTags.add("boat2_sashi_high");
  if (toNum(scenarioMap.get("boat3_makuri"), 0) >= 0.2) signalTags.add("boat3_attack_high");
  if (toNum(scenarioMap.get("boat3_makuri_sashi"), 0) >= 0.18) signalTags.add("boat3_makuri_sashi_high");
  if (toNum(scenarioMap.get("boat4_cado_attack"), 0) >= 0.2) signalTags.add("boat4_cado_high");
  if (chaosRisk >= 70 || upsetRiskProxy >= 64) signalTags.add("chaos_high");
  if (upsetRiskProxy >= 72) signalTags.add("upset_high");
  if (toInt(candidateDistributions?.boat3_weak_st_head_suppressed, 0) === 1) signalTags.add("boat3_weak_st_suppressed");
  if (toInt(candidateDistributions?.partner_search_bias_json?.suji_used, 0) === 1) signalTags.add("suji_preferred");
  if (toNum(finishOverrideMap.get(3)?.finish_override_strength, 0) >= 68) signalTags.add("lap_override_high");
  const outsideGateByLane = candidateDistributions?.outside_head_promotion_gate_json?.by_lane || {};
  if (toInt(outsideGateByLane?.["5"]?.blocked_by_gate, 0) === 1) signalTags.add("boat5_head_blocked");
  if (toInt(outsideGateByLane?.["6"]?.blocked_by_gate, 0) === 1) signalTags.add("boat6_head_blocked");
  if (toNum(firstMap.get(3), 0) >= 0.3) signalTags.add("boat3_head_clear");
  if (toNum(firstMap.get(4), 0) >= 0.27) signalTags.add("boat4_head_clear");
  if (toNum(firstMap.get(5), 0) >= 0.16) signalTags.add("boat5_head_clear");
  if (toNum(firstMap.get(6), 0) >= 0.14) signalTags.add("boat6_head_clear");
  return {
    race,
    rows,
    formation_pattern: formationPattern,
    attack_scenario_type: attackType,
    boat1_escape_probability: toNum(candidateDistributions?.boat1_escape_probability, 0),
    scenario_probabilities: scenarioMap,
    first_place_map: firstMap,
    signal_tags: signalTags,
    upset_risk_proxy: Number(upsetRiskProxy.toFixed(2)),
    chaos_risk_score: chaosRisk
  };
}

function mainHeadLaneFromMap(firstMap) {
  let bestLane = null;
  let bestWeight = -Infinity;
  for (const [lane, weight] of firstMap.entries()) {
    if (toNum(weight, 0) > bestWeight) {
      bestLane = lane;
      bestWeight = toNum(weight, 0);
    }
  }
  return bestLane;
}

function computeScenarioDictionaryMatchScore(entry, context) {
  const signalTags = context?.signal_tags || new Set();
  const successConditions = safeArray(entry?.success_conditions);
  const rejectionConditions = safeArray(entry?.rejection_conditions);
  const satisfied = successConditions.filter((condition) => signalTags.has(condition));
  const rejected = rejectionConditions.filter((condition) => signalTags.has(condition));
  const baseWeight = getScenarioPriorityBaseWeight(entry?.priority_rank);
  let score = 22 * baseWeight;
  score += satisfied.length * 16;
  score -= rejected.length * 20;
  if (toInt(entry?.winning_boat, null) === 1 && signalTags.has("boat1_head")) score += 9;
  if (toInt(entry?.winning_boat, null) === 3 && signalTags.has("boat3_attack_ready")) score += 7;
  if (toInt(entry?.winning_boat, null) === 4 && signalTags.has("boat4_cado_ready")) score += 7;
  if ([5, 6].includes(toInt(entry?.winning_boat, null)) && !signalTags.has("upset_high")) score -= 18;
  if (String(entry?.development_category || "") === "boat1_escape" && signalTags.has("boat1_escape_strong")) score += 11;
  if (safeArray(entry?.key_exhibition_signals).some((condition) => signalTags.has(condition))) score += 6;
  if (String(context?.formation_pattern || "") && String(entry?.entry_shape || "").includes(context.formation_pattern)) score += 5;
  if (String(context?.attack_scenario_type || "").includes("four_cado") && String(entry?.scenario_name || "").includes("4")) score += 4;
  if (String(context?.attack_scenario_type || "").includes("three") && String(entry?.scenario_name || "").includes("3")) score += 4;
  const activated = rejected.length === 0 && (satisfied.length > 0 || baseWeight >= 0.72);
  const conditionalOnly =
    String(entry?.priority_rank || "").toUpperCase() === "C" ||
    String(entry?.priority_rank || "").toUpperCase() === "D";
  return {
    ...entry,
    success_conditions_satisfied: satisfied,
    rejection_conditions_triggered: rejected,
    match_score: Number(clamp(0, 100, score).toFixed(2)),
    activated: activated ? 1 : 0,
    conditional_only: conditionalOnly ? 1 : 0
  };
}

function matchScenarioDictionaryEntries(args) {
  return PDF_SCENARIO_PRIOR_DICTIONARY
    .map((entry) => computeScenarioDictionaryMatchScore(entry, args))
    .sort((a, b) => toNum(b?.match_score, 0) - toNum(a?.match_score, 0));
}

function getDictionaryScenarioKey(entry) {
  const boat = toInt(entry?.winning_boat, null);
  const name = String(entry?.scenario_name || "");
  if (boat === 1) return "boat1_escape";
  if (boat === 2) return "boat2_sashi";
  if (boat === 3) return name.includes("差し") ? "boat3_makuri_sashi" : "boat3_makuri";
  if (boat === 4) return "boat4_cado_attack";
  if (boat === 5 || boat === 6) return "chaos_outer_mix";
  return null;
}

function applyScenarioDictionaryPriors({ scenarioProbabilities, matchedDictionaryScenarios }) {
  const rows = safeArray(scenarioProbabilities);
  const deltas = new Map();
  for (const entry of safeArray(matchedDictionaryScenarios)) {
    if (toInt(entry?.activated, 0) !== 1) continue;
    if (toInt(entry?.conditional_only, 0) === 1 && toNum(entry?.match_score, 0) < 62) continue;
    const scenarioKey = getDictionaryScenarioKey(entry);
    if (!scenarioKey) continue;
    const rank = String(entry?.priority_rank || "").toUpperCase();
    const cap = rank === "A" ? 0.045 : rank === "B" ? 0.03 : rank === "C" ? 0.018 : 0.012;
    const addition = Math.min(cap, toNum(entry?.match_score, 0) / 2000);
    deltas.set(scenarioKey, toNum(deltas.get(scenarioKey), 0) + addition);
  }
  const adjusted = normalizeDistributionRows(
    rows.map((row) => ({
      lane: ({ boat1_escape: 1, boat2_sashi: 2, boat3_makuri: 3, boat3_makuri_sashi: 3, boat4_cado_attack: 4, chaos_outer_mix: 5 })[
        String(row?.scenario || "")
      ],
      role: row?.scenario || null,
      weight: toNum(row?.probability, 0) + toNum(deltas.get(String(row?.scenario || "")), 0)
    }))
  ).map((row) => {
    const original = rows.find((candidate) => String(candidate?.scenario || "") === String(row?.role || ""));
    return {
      scenario: row.role,
      probability: row.weight,
      pre_adjustment_probability: toNum(original?.probability, 0),
      dictionary_adjustment_delta: Number((toNum(row.weight, 0) - toNum(original?.probability, 0)).toFixed(4)),
      dictionary_adjustment_sources: safeArray(matchedDictionaryScenarios)
        .filter((entry) => getDictionaryScenarioKey(entry) === row.role && toInt(entry?.activated, 0) === 1)
        .map((entry) => ({
          scenario_name: entry.scenario_name,
          priority_rank: entry.priority_rank,
          match_score: entry.match_score
        })),
      venue_adjustment_delta: toNum(original?.venue_adjustment_delta, 0),
      venue_calibration_used: original?.venue_calibration_used || null
    };
  });
  return adjusted.sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0));
}

function applyDictionaryPriorsToOrderCandidates({ orderCandidates, matchedDictionaryScenarios, maxSeedProbability = 0.11 }) {
  const bucket = new Map(
    safeArray(orderCandidates).map((row) => [normalizeCombo(row?.combo), { ...row, combo: normalizeCombo(row?.combo) }]).filter(([combo]) => combo)
  );
  const activatedNames = [];
  for (const entry of safeArray(matchedDictionaryScenarios).slice(0, 8)) {
    if (toInt(entry?.activated, 0) !== 1) continue;
    if (toInt(entry?.conditional_only, 0) === 1 && toNum(entry?.match_score, 0) < 62) continue;
    activatedNames.push(entry.scenario_name);
    const repBoost = Math.min(0.075, toNum(entry?.match_score, 0) / 1400);
    const backupBoost = Math.min(0.04, toNum(entry?.match_score, 0) / 2200);
    for (const combo of safeArray(entry?.representative_tickets).flatMap(expandDictionaryTicketPattern)) {
      const existing = bucket.get(combo);
      const probability = existing
        ? Number((toNum(existing?.probability, 0) + repBoost).toFixed(4))
        : Number(Math.min(maxSeedProbability, repBoost).toFixed(4));
      bucket.set(combo, {
        combo,
        probability,
        reason_tags: [...new Set([...
          safeArray(existing?.reason_tags),
          "SCENARIO_DICTIONARY_PRIOR",
          `DICT:${entry.scenario_name}`
        ])]
      });
    }
    for (const combo of safeArray(entry?.backup_tickets).flatMap(expandDictionaryTicketPattern)) {
      const existing = bucket.get(combo);
      const probability = existing
        ? Number((toNum(existing?.probability, 0) + backupBoost).toFixed(4))
        : Number(Math.min(maxSeedProbability * 0.7, backupBoost).toFixed(4));
      bucket.set(combo, {
        combo,
        probability,
        reason_tags: [...new Set([...
          safeArray(existing?.reason_tags),
          "SCENARIO_DICTIONARY_BACKUP",
          `DICT:${entry.scenario_name}`
        ])]
      });
    }
  }
  return {
    order_candidates: [...bucket.values()]
      .sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0))
      .slice(0, 18),
    activated_scenario_names: activatedNames
  };
}

function boostScenarioDistribution(baseRows, boosts) {
  return normalizeDistributionRows(
    safeArray(baseRows).map((row) => ({
      lane: toInt(row?.lane, null),
      role: row?.role || null,
      weight: toNum(row?.weight, 0) + toNum(boosts?.[String(toInt(row?.lane, null))] ?? boosts?.[toInt(row?.lane, null)], 0)
    }))
  );
}

function computeMotor2renStrength(features) {
  const motor2Meta = features?.prediction_field_meta?.motor2ren || {};
  const motor2Value = motor2Meta?.is_usable
    ? toNum(motor2Meta.value, 0)
    : null;
  return Number(
    clamp(
      0,
      100,
      (Number.isFinite(motor2Value) ? motor2Value : 0) * 1.15 +
        toNum(features?.boat2_rate, 0) * 0.22
    ).toFixed(2)
  );
}

function computeLapExhibitionStrength(features) {
  const lapTimeMeta = features?.prediction_field_meta?.lapTime || {};
  const lapStretchMeta = features?.prediction_field_meta?.lapExStretch || {};
  const exhibitionStMeta = features?.prediction_field_meta?.exhibitionST || {};
  return Number(
    clamp(
      0,
      100,
      Math.max(0, 7 - toNum(features?.lap_time_rank, 6)) * 11 +
        Math.max(0, 6.9 - (lapTimeMeta?.is_usable ? toNum(features?.lap_time, 6.9) : 6.9)) * 150 +
        Math.max(0, lapStretchMeta?.is_usable ? toNum(features?.lap_exhibition_score, 0) : 0) * 7 +
        Math.max(0, 7 - toNum(features?.exhibition_rank, 6)) * 9 +
        Math.max(0, toNum(features?.lap_time_delta_vs_front, 0)) * 180 +
        Math.max(0, toNum(features?.lap_attack_strength, 0)) * 4.2 +
        Math.max(0, 6.86 - toNum(features?.exhibition_time, 6.86)) * 120 +
        (exhibitionStMeta?.is_usable ? 2 : 0)
    ).toFixed(2)
  );
}

function computeFinishOverrideStrength(features) {
  const lapTimeMeta = features?.prediction_field_meta?.lapTime || {};
  const lapStretchMeta = features?.prediction_field_meta?.lapExStretch || {};
  const motor2Meta = features?.prediction_field_meta?.motor2ren || {};
  const motor3Meta = features?.prediction_field_meta?.motor3ren || {};
  const exhibitionStMeta = features?.prediction_field_meta?.exhibitionST || {};
  const lapTimeContribution = Number(
    clamp(
      0,
      100,
      Math.max(0, 7 - toNum(features?.lap_time_rank, 6)) * 12 +
        Math.max(0, 6.9 - (lapTimeMeta?.is_usable ? toNum(features?.lap_time, 6.9) : 6.9)) * 195 +
        Math.max(0, toNum(features?.lap_time_delta_vs_front, 0)) * 255 +
        Math.max(0, toNum(features?.lap_attack_strength, 0)) * 2.8
    ).toFixed(2)
  );
  const lapExhibitionContribution = computeLapExhibitionStrength(features);
  const motor2renContribution = computeMotor2renStrength(features);
  const motor3renContribution = Number(
    clamp(0, 100, (motor3Meta?.is_usable ? toNum(features?.motor3_rate ?? features?.motor_3rate, 0) : 0) * 0.9).toFixed(2)
  );
  const recentPlayerContribution = Number(
    clamp(
      0,
      100,
      toNum(features?.player_recent_3_months_strength, 0) * 7.5 +
        toNum(features?.player_current_season_strength, 0) * 4.5 +
        toNum(features?.player_strength_blended, 0) * 4
    ).toFixed(2)
  );
  const exhibitionTimeContribution = Number(
    clamp(0, 100, Math.max(0, 6.86 - toNum(features?.exhibition_time, 6.86)) * 105).toFixed(2)
  );
  const exhibitionStContribution = Number(
    clamp(
      0,
      100,
      Math.max(0, 7 - toNum(features?.st_rank, 6)) * 10 +
        Math.max(0, 7 - toNum(features?.expected_actual_st_rank, 6)) * 11 +
        Math.max(0, 0.18 - (exhibitionStMeta?.is_usable ? toNum(features?.exhibition_st, 0.18) : 0.18)) * 260
    ).toFixed(2)
  );
  const venueFitContribution = Number(
    clamp(0, 100, toNum(features?.course_fit_score, 0) * 10 + toNum(features?.venue_lane_adjustment, 0) * 8).toFixed(2)
  );
  const firstPlaceStrength = Number(
    clamp(
      0,
      100,
      lapTimeContribution * 0.31 +
        lapExhibitionContribution * 0.18 +
        motor2renContribution * 0.23 +
        motor3renContribution * 0.08 +
        recentPlayerContribution * 0.1 +
        exhibitionTimeContribution * 0.03 +
        exhibitionStContribution * 0.11 +
        venueFitContribution * 0.06
    ).toFixed(2)
  );
  const secondPlaceStrength = Number(
    clamp(
      0,
      100,
      lapTimeContribution * 0.18 +
        lapExhibitionContribution * 0.14 +
        motor2renContribution * 0.33 +
        motor3renContribution * 0.15 +
        recentPlayerContribution * 0.1 +
        exhibitionTimeContribution * 0.03 +
        exhibitionStContribution * 0.05 +
        venueFitContribution * 0.02
    ).toFixed(2)
  );
  const thirdPlaceStrength = Number(
    clamp(
      0,
      100,
      lapTimeContribution * 0.12 +
        lapExhibitionContribution * 0.12 +
        motor2renContribution * 0.24 +
        motor3renContribution * 0.24 +
        recentPlayerContribution * 0.1 +
        exhibitionTimeContribution * 0.04 +
        exhibitionStContribution * 0.04 +
        venueFitContribution * 0.1
    ).toFixed(2)
  );
  return {
    lap_time_contribution: lapTimeContribution,
    lap_exhibition_contribution: lapExhibitionContribution,
    motor_2ren_contribution: motor2renContribution,
    motor_3ren_contribution: motor3renContribution,
    prediction_data_usage: {
      lapTime: { used: !!lapTimeMeta?.is_usable, source: lapTimeMeta?.source || null, confidence: toNum(lapTimeMeta?.confidence, 0), reason: lapTimeMeta?.reason || null },
      exhibitionST: { used: !!exhibitionStMeta?.is_usable, source: exhibitionStMeta?.source || null, confidence: toNum(exhibitionStMeta?.confidence, 0), reason: exhibitionStMeta?.reason || null },
      exhibitionTime: { used: !!features?.prediction_field_meta?.exhibitionTime?.is_usable, source: features?.prediction_field_meta?.exhibitionTime?.source || null, confidence: toNum(features?.prediction_field_meta?.exhibitionTime?.confidence, 0), reason: features?.prediction_field_meta?.exhibitionTime?.reason || null },
      lapExStretch: { used: !!lapStretchMeta?.is_usable, source: lapStretchMeta?.source || null, confidence: toNum(lapStretchMeta?.confidence, 0), reason: lapStretchMeta?.reason || null },
      motor2ren: { used: !!motor2Meta?.is_usable, source: motor2Meta?.source || null, confidence: toNum(motor2Meta?.confidence, 0), reason: motor2Meta?.reason || null },
      motor3ren: { used: !!motor3Meta?.is_usable, source: motor3Meta?.source || null, confidence: toNum(motor3Meta?.confidence, 0), reason: motor3Meta?.reason || null }
    },
    recent_player_form_contribution: recentPlayerContribution,
    exhibition_time_contribution: exhibitionTimeContribution,
    exhibition_st_contribution: exhibitionStContribution,
    venue_fit_contribution: venueFitContribution,
    finish_override_strength: firstPlaceStrength,
    first_place_finish_strength: firstPlaceStrength,
    second_place_finish_strength: secondPlaceStrength,
    third_place_finish_strength: thirdPlaceStrength
  };
}

function applyFinishOverrideStrength(baseFinishProbs, finishOverrideStrengthByLane, lanePriors = {}) {
  const boat1EscapeProbability = toNum(lanePriors?.boat1_escape_probability, 0);
  const boat1LaneFirstPrior = toNum(lanePriors?.boat1_lane_first_prior, 0.18);
  const normalizeWithDetails = (rows) => {
    const detailMap = new Map(
      safeArray(rows).map((row) => [toInt(row?.lane, null), row?.finish_override_detail || {}])
    );
    return normalizeDistributionRows(rows).map((row) => ({
      ...row,
      finish_override_detail: detailMap.get(toInt(row?.lane, null)) || {}
    }));
  };
  const getTopLane = (rows) => normalizeDistributionRows(rows)[0]?.lane ?? null;
  const baseFirstLeader = getTopLane(baseFinishProbs?.first);
  const baseSecondLeader = getTopLane(baseFinishProbs?.second);
  const firstRows = normalizeWithDetails(
    safeArray(baseFinishProbs?.first).map((row) => {
      const lane = toInt(row?.lane, null);
      const override = finishOverrideStrengthByLane.get(lane) || {};
      const overrideStrength = toNum(override?.first_place_finish_strength ?? override?.finish_override_strength, 0);
      const baseWeight = toNum(row?.weight, 0);
      const laneOneBlock =
        lane !== 1 &&
        lane >= 4 &&
        boat1EscapeProbability >= 0.48 &&
        boat1LaneFirstPrior >= 0.18 &&
        overrideStrength < 74;
      const adjusted = laneOneBlock
        ? baseWeight + Math.min(0.012, overrideStrength * 0.00018)
        : baseWeight + Math.min(0.055, overrideStrength * 0.0007);
      return {
        ...row,
        weight: adjusted,
        finish_override_detail: {
          ...(override || {}),
          first_place_override_applied: Number((adjusted - baseWeight).toFixed(4)),
          boat1_prior_blocked_outside_head_promotion: laneOneBlock ? 1 : 0
        }
      };
    })
  );
  const secondRows = normalizeWithDetails(
    safeArray(baseFinishProbs?.second).map((row) => {
      const lane = toInt(row?.lane, null);
      const override = finishOverrideStrengthByLane.get(lane) || {};
      const overrideStrength = toNum(override?.second_place_finish_strength ?? override?.finish_override_strength, 0);
      const baseWeight = toNum(row?.weight, 0);
      const adjusted = baseWeight + Math.min(0.065, overrideStrength * 0.00082);
      return {
        ...row,
        weight: adjusted,
        finish_override_detail: {
          ...(override || {}),
          second_place_override_applied: Number((adjusted - baseWeight).toFixed(4))
        }
      };
    })
  );
  const thirdRows = normalizeWithDetails(
    safeArray(baseFinishProbs?.third).map((row) => {
      const lane = toInt(row?.lane, null);
      const override = finishOverrideStrengthByLane.get(lane) || {};
      const overrideStrength = toNum(override?.third_place_finish_strength ?? override?.finish_override_strength, 0);
      const baseWeight = toNum(row?.weight, 0);
      const adjusted = baseWeight + Math.min(0.04, overrideStrength * 0.00046);
      return {
        ...row,
        weight: adjusted,
        finish_override_detail: {
          ...(override || {}),
          third_place_override_applied: Number((adjusted - baseWeight).toFixed(4))
        }
      };
    })
  );
  return {
    first: firstRows,
    second: secondRows,
    third: thirdRows,
    diagnostics: {
      first_place_rank_changed: baseFirstLeader !== getTopLane(firstRows),
      second_place_rank_changed: baseSecondLeader !== getTopLane(secondRows),
      boat1_prior_blocked_outside_head_promotion: firstRows.some(
        (entry) => toNum(entry?.finish_override_detail?.boat1_prior_blocked_outside_head_promotion, 0) === 1
      )
    }
  };
}

function computeFinishProbsByScenario({
  scenarioProbabilities,
  firstPlaceProbability,
  secondPlaceProbability,
  thirdPlaceProbability,
  boat1EscapeProbability,
  rows
}) {
  const finishOverrideStrengthByLane = new Map(
    safeArray(rows).map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      return [lane, computeFinishOverrideStrength(row?.features || {})];
    }).filter(([lane]) => Number.isInteger(lane))
  );
  const scenarioBoosts = {
    boat1_escape: {
      first: { 1: 0.22, 2: 0.04, 3: 0.03, 4: 0.02 },
      second: { 2: 0.12, 3: 0.09, 4: 0.06, 1: 0.015 },
      third: { 2: 0.06, 3: 0.07, 4: 0.06, 5: 0.02 }
    },
    boat2_sashi: {
      first: { 2: 0.16, 1: 0.09, 3: 0.05 },
      second: { 1: 0.1, 3: 0.09, 4: 0.05 },
      third: { 3: 0.07, 4: 0.06, 1: 0.03 }
    },
    boat3_makuri: {
      first: { 3: 0.12, 1: boat1EscapeProbability >= 0.45 ? 0.1 : 0.05, 4: 0.07 },
      second: { 1: 0.11, 4: 0.09, 2: 0.06, 3: 0.04 },
      third: { 4: 0.08, 2: 0.07, 1: 0.05, 5: 0.03 }
    },
    boat3_makuri_sashi: {
      first: { 3: 0.1, 1: boat1EscapeProbability >= 0.45 ? 0.11 : 0.06, 2: 0.06, 4: 0.05 },
      second: { 1: 0.1, 2: 0.09, 4: 0.08, 3: 0.03 },
      third: { 2: 0.08, 4: 0.07, 1: 0.05, 5: 0.02 }
    },
    boat4_cado_attack: {
      first: { 4: 0.12, 1: boat1EscapeProbability >= 0.45 ? 0.09 : 0.05, 3: 0.06, 5: 0.04 },
      second: { 1: 0.11, 3: 0.08, 5: 0.07, 2: 0.05 },
      third: { 3: 0.07, 5: 0.06, 2: 0.06, 1: 0.04 }
    },
    chaos_outer_mix: {
      first: { 4: 0.08, 5: 0.08, 6: 0.06, 1: 0.05 },
      second: { 5: 0.09, 4: 0.08, 6: 0.07, 1: 0.04, 3: 0.04 },
      third: { 5: 0.09, 6: 0.08, 4: 0.07, 2: 0.05, 3: 0.05 }
    }
  };
  return safeArray(scenarioProbabilities).map((row) => {
    const scenario = String(row?.scenario || "");
    const boosts = scenarioBoosts[scenario] || { first: {}, second: {}, third: {} };
    const baseFinishProbs = {
      first: boostScenarioDistribution(firstPlaceProbability, boosts.first),
      second: boostScenarioDistribution(secondPlaceProbability, boosts.second),
      third: boostScenarioDistribution(thirdPlaceProbability, boosts.third)
    };
    const adjustedFinishProbs = applyFinishOverrideStrength(
      baseFinishProbs,
      finishOverrideStrengthByLane,
      {
        boat1_escape_probability: boat1EscapeProbability,
        boat1_lane_first_prior: toNum(
          safeArray(firstPlaceProbability).find((entry) => toInt(entry?.lane, null) === 1)?.weight,
          0
        )
      }
    );
    return {
      scenario,
      probability: toNum(row?.probability, 0),
      first: adjustedFinishProbs.first,
      second: adjustedFinishProbs.second,
      third: adjustedFinishProbs.third,
      finish_override_diagnostics: adjustedFinishProbs.diagnostics || {},
      finish_override_strength_json: Object.fromEntries(
        [...finishOverrideStrengthByLane.entries()].map(([lane, value]) => [String(lane), value])
      )
    };
  });
}

function combineScenarioAndFinishProbs({
  scenarioProbabilities,
  conditionalFinishProbs
}) {
  const scenarioMap = new Map(
    safeArray(scenarioProbabilities).map((row) => [String(row?.scenario || ""), toNum(row?.probability, 0)])
  );
  const bucket = new Map();
  for (const scenarioRow of safeArray(conditionalFinishProbs)) {
    const scenario = String(scenarioRow?.scenario || "");
    const scenarioProbability = toNum(scenarioMap.get(scenario), 0);
    const topFirst = safeArray(scenarioRow?.first).slice(0, 3);
    const topSecond = safeArray(scenarioRow?.second).slice(0, 4);
    const topThird = safeArray(scenarioRow?.third).slice(0, 5);
    for (const first of topFirst) {
      for (const second of topSecond) {
        for (const third of topThird) {
          const lanes = [toInt(first?.lane, null), toInt(second?.lane, null), toInt(third?.lane, null)];
          if (lanes.some((lane) => !Number.isInteger(lane))) continue;
          if (new Set(lanes).size !== 3) continue;
          const combo = lanes.join("-");
          const conditionalProbability = clamp(
            0,
            1,
            toNum(first?.weight, 0) * toNum(second?.weight, 0) * toNum(third?.weight, 0) * 9.5
          );
          const probability = Number((scenarioProbability * conditionalProbability).toFixed(4));
          const existing = bucket.get(combo);
          const candidate = {
            combo,
            probability,
            reason_tags: [
              `SCENARIO_${scenario.toUpperCase()}`,
              lanes[0] === 1 && scenario !== "boat1_escape" ? "ATTACK_WITH_BOAT1_SURVIVAL" : null
            ].filter(Boolean)
          };
          if (!existing || toNum(existing?.probability, 0) < probability) {
            bucket.set(combo, candidate);
          }
        }
      }
    }
  }
  return [...bucket.values()].sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0)).slice(0, 18);
}

function buildPredictionFeatureBundle({
  ranking,
  race,
  entryMeta,
  learningWeights,
  escapePatternAnalysis,
  attackScenarioAnalysis,
  headScenarioBalanceAnalysis,
  candidateDistributions
}) {
  return {
    rows: safeArray(ranking),
    race: race || null,
    entry_context: entryMeta || {},
    learning_weights: learningWeights || {},
    formation_context: {
      formation_pattern: escapePatternAnalysis?.formation_pattern || null,
      escape_pattern_applied: escapePatternAnalysis?.escape_pattern_applied ? 1 : 0,
      escape_pattern_confidence: toNum(escapePatternAnalysis?.escape_pattern_confidence, 0),
      escape_second_place_bias_json: escapePatternAnalysis?.escape_second_place_bias_json || {}
    },
    attack_context: attackScenarioAnalysis || {},
    head_context: headScenarioBalanceAnalysis || {},
    role_layers: candidateDistributions || {},
    launch_context: {
      launch_state_thresholds_used_json: candidateDistributions?.launch_state_thresholds_used_json || getLaunchStateConfig(),
      launch_venue_calibration_json: candidateDistributions?.launch_venue_calibration_json || getVenueLaunchMicroCalibration(),
      launch_state_scores_json: candidateDistributions?.launch_state_scores_json || [],
      launch_state_labels_json: candidateDistributions?.launch_state_labels_json || [],
      intermediate_development_events_json: candidateDistributions?.intermediate_development_events_json || {},
      race_scenario_probabilities_json: candidateDistributions?.race_scenario_probabilities_json || [],
      finish_probabilities_by_scenario_json: candidateDistributions?.finish_probabilities_by_scenario_json || [],
      finish_override_strength_by_lane_json: candidateDistributions?.finish_override_strength_by_lane_json || {},
      scenario_based_order_candidates_json: candidateDistributions?.scenario_based_order_candidates_json || [],
      matched_dictionary_scenarios_json: candidateDistributions?.matched_dictionary_scenarios_json || [],
      dictionary_scenario_match_scores_json: candidateDistributions?.dictionary_scenario_match_scores_json || [],
      dictionary_prior_adjustment_json: candidateDistributions?.dictionary_prior_adjustment_json || {},
      dictionary_condition_flags_json: candidateDistributions?.dictionary_condition_flags_json || [],
      dictionary_representative_ticket_priors_json: candidateDistributions?.dictionary_representative_ticket_priors_json || [],
      dictionary_cd_scenarios_activated: toInt(candidateDistributions?.dictionary_cd_scenarios_activated, 0)
    },
    lane_bundle: safeArray(ranking).map((row) => ({
      lane: toInt(row?.racer?.lane, null),
      score: toNum(row?.score, 0),
      features: row?.features || {},
      racer: row?.racer || {}
    }))
  };
}

function computeBoat1EscapeProbability(featureBundle) {
  return toNum(featureBundle?.role_layers?.boat1_escape_probability, 0);
}

function computeAttackScenarioProbabilities(featureBundle) {
  return safeArray(featureBundle?.role_layers?.attack_scenario_probability_json);
}

function computeFirstPlaceProbabilities(featureBundle) {
  return normalizeDistributionRows(
    safeArray(featureBundle?.role_layers?.first_place_probability_json || featureBundle?.head_context?.first_place_distribution_json)
  );
}

function computeSecondPlaceProbabilities(featureBundle, boat1EscapeProb = 0) {
  const base = safeArray(
    boat1EscapeProb >= 0.34 && safeArray(featureBundle?.role_layers?.boat1_second_place_probability_json).length > 0
      ? featureBundle?.role_layers?.boat1_second_place_probability_json
      : featureBundle?.role_layers?.second_place_probability_json || featureBundle?.head_context?.second_place_distribution_json
  );
  return normalizeDistributionRows(base);
}

function computeSurvivalProbabilities(featureBundle) {
  return normalizeDistributionRows(
    safeArray(featureBundle?.role_layers?.survival_probability_json)
  );
}

function computeThirdPlaceProbabilities(featureBundle, firstProbs, secondProbs, attackProbs, survivalProbs) {
  const baseRows = safeArray(
    computeBoat1EscapeProbability(featureBundle) >= 0.34 && safeArray(featureBundle?.role_layers?.boat1_third_place_probability_json).length > 0
      ? featureBundle?.role_layers?.boat1_third_place_probability_json
      : featureBundle?.role_layers?.third_place_probability_json || featureBundle?.head_context?.third_place_distribution_json
  );
  const survivalMap = new Map(safeArray(survivalProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const attackPartnerLaneSet = new Set(
    safeArray(attackProbs).flatMap((row) => {
      const scenario = String(row?.scenario || "");
      if (scenario.includes("boat2")) return [2];
      if (scenario.includes("boat3")) return [3];
      if (scenario.includes("boat4")) return [4];
      return [];
    })
  );
  return normalizeDistributionRows(baseRows.map((row) => {
    const lane = toInt(row?.lane, null);
    return {
      ...row,
      weight: toNum(row?.weight, 0) +
        toNum(survivalMap.get(lane), 0) * 0.18 +
        (attackPartnerLaneSet.has(lane) ? 0.02 : 0)
    };
  }));
}

function composeFinishOrderCandidates({
  featureBundle,
  firstProbs,
  secondProbs,
  thirdProbs,
  attackProbs,
  survivalProbs
}) {
  const partnerBias = featureBundle?.role_layers?.partner_search_bias_json || {};
  const boat1Bias = featureBundle?.role_layers?.boat1_partner_bias_json || {};
  const outsideHeadGate = featureBundle?.role_layers?.outside_head_promotion_gate_json || {};
  const outsideHeadGateByLane = outsideHeadGate?.by_lane || {};
  const mainHeadLane = toInt(featureBundle?.role_layers?.role_probability_summary_json?.main_head_lane, null)
    ?? topDistributionLane(firstProbs);
  const attackWeightMap = new Map();
  for (const row of safeArray(attackProbs)) {
    const scenario = String(row?.scenario || "");
    const probability = toNum(row?.probability, 0);
    if (scenario.includes("boat2")) attackWeightMap.set(2, Math.max(toNum(attackWeightMap.get(2), 0), probability));
    if (scenario.includes("boat3")) attackWeightMap.set(3, Math.max(toNum(attackWeightMap.get(3), 0), probability));
    if (scenario.includes("boat4")) attackWeightMap.set(4, Math.max(toNum(attackWeightMap.get(4), 0), probability));
  }
  const survivalMap = new Map(safeArray(survivalProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const scenarioBasedCandidates = safeArray(
    featureBundle?.launch_context?.scenario_based_order_candidates_json ||
    featureBundle?.role_layers?.scenario_based_order_candidates_json
  );
  const topFirst = safeArray(firstProbs).slice(0, 3);
  const topSecond = safeArray(secondProbs).slice(0, mainHeadLane === 1 ? 4 : 5);
  const topThird = safeArray(thirdProbs).slice(0, mainHeadLane === 1 ? 5 : 6);
  const bucket = new Map();
  for (const first of topFirst) {
    for (const second of topSecond) {
      for (const third of topThird) {
        const lanes = [toInt(first?.lane, null), toInt(second?.lane, null), toInt(third?.lane, null)];
        if (lanes.some((lane) => !Number.isInteger(lane))) continue;
        if (new Set(lanes).size !== 3) continue;
        const [headLane, secondLane, thirdLane] = lanes;
        const outsideGate = outsideHeadGateByLane[String(headLane)] || null;
        const chaosRisk = toNum(featureBundle?.head_context?.chaos_risk_score, 0);
        const lowHeadConfidence = toNum(featureBundle?.head_context?.head_confidence, 0) > 0
          ? toNum(featureBundle?.head_context?.head_confidence, 0) < 60
          : toNum(featureBundle?.head_context?.head_distribution_json?.[0]?.weight, 0) < 0.38;
        if ((headLane === 5 || headLane === 6) && outsideGate?.blocked_by_gate) continue;
        if ((headLane === 5 || headLane === 6) && outsideGate?.allowed_as_counter_only && !safeArray(firstProbs).slice(0, 2).some((row) => toInt(row?.lane, null) === headLane)) {
          continue;
        }
        if ((headLane === 5 || headLane === 6) && (chaosRisk >= 76 || lowHeadConfidence)) {
          continue;
        }
        const sujiBias =
          headLane === 1 && toInt(partnerBias?.suji_used, 0) === 1 && (secondLane === 2 || secondLane === 3)
            ? (secondLane === 2 ? 0.03 : 0.024)
            : 0;
        const urasujiBias =
          headLane === 1 && toInt(boat1Bias?.urasuji_used, 0) === 1 && (thirdLane === secondLane + 1 || secondLane === thirdLane + 1)
            ? 0.01
            : 0;
        const outerHeadPenalty =
          headLane === 5 ? 0.045 : headLane === 6 ? 0.06 : 0;
        const outsideGatePenalty =
          headLane === 5 || headLane === 6
            ? outsideGate?.allowed_as_main_head
              ? 0
              : outsideGate?.allowed_as_counter_only
                ? 0.02
                : 0.08
            : 0;
        const boat1PriorityBonus =
          headLane === 1 && computeBoat1EscapeProbability(featureBundle) >= 0.34
            ? 0.04
            : 0;
        const attackSecondOnlyBias = toInt(partnerBias?.attack_moved_second_only_lane, null) === secondLane ? 0.018 : 0;
        const attackThirdOnlyBias = toInt(partnerBias?.attack_moved_third_only_lane, null) === thirdLane ? 0.012 : 0;
        const survivalBias = (toNum(survivalMap.get(secondLane), 0) + toNum(survivalMap.get(thirdLane), 0)) * 0.08;
        const composite =
          toNum(first?.weight, 0) * 0.52 +
          toNum(second?.weight, 0) * 0.3 +
          toNum(third?.weight, 0) * 0.18 +
          toNum(attackWeightMap.get(secondLane), 0) * 0.06 +
          survivalBias +
          sujiBias +
          urasujiBias +
          boat1PriorityBonus +
          attackSecondOnlyBias +
          attackThirdOnlyBias -
          outsideGatePenalty -
          outerHeadPenalty;
        const combo = `${headLane}-${secondLane}-${thirdLane}`;
        const existing = bucket.get(combo);
        if (!existing || toNum(existing?.probability, 0) < composite) {
          bucket.set(combo, {
            combo,
            probability: Number(clamp(0, 1, composite).toFixed(4)),
            reason_tags: [
              headLane === 1 ? "BOAT1_HEAD_PRIORITY" : null,
              sujiBias > 0 ? "SUJI_PRIOR_USED" : null,
              urasujiBias > 0 ? "URASUJI_BACKUP_USED" : null,
              attackSecondOnlyBias > 0 ? "ATTACK_CHANGED_SECOND_ONLY" : null,
              attackThirdOnlyBias > 0 ? "ATTACK_CHANGED_THIRD_ONLY" : null
              ,
              outsideGate?.blocked_by_gate ? "OUTSIDE_HEAD_BLOCKED" : null,
              outsideGate?.allowed_as_counter_only ? "OUTSIDE_HEAD_COUNTER_ONLY" : null
            ].filter(Boolean)
          });
        }
      }
    }
  }
  const merged = [...bucket.values()]
    .concat(
      scenarioBasedCandidates.map((row) => ({
        combo: normalizeCombo(row?.combo),
        probability: Number((toNum(row?.probability, 0) * 0.92).toFixed(4)),
        reason_tags: safeArray(row?.reason_tags)
      }))
    )
    .reduce((acc, row) => {
      const combo = normalizeCombo(row?.combo);
      if (!combo) return acc;
      const existing = acc.get(combo);
      if (!existing) {
        acc.set(combo, row);
        return acc;
      }
      acc.set(combo, {
        combo,
        probability: Number((toNum(existing?.probability, 0) + toNum(row?.probability, 0) * 0.65).toFixed(4)),
        reason_tags: [...new Set([...
          safeArray(existing?.reason_tags),
          ...safeArray(row?.reason_tags)
        ])]
      });
      return acc;
    }, new Map());
  return [...merged.values()]
    .sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0))
    .slice(0, 18);
}

function generateMainTrifectaTickets(orderCandidates, confidence) {
  const confidenceFactor = clamp(0.6, 1.12, toNum(confidence, 50) / 70);
  return safeArray(orderCandidates)
    .slice(0, confidenceFactor >= 1 ? 6 : 5)
    .map((row, index) => ({
      combo: normalizeCombo(row?.combo),
      prob: Number((toNum(row?.probability, 0) * confidenceFactor).toFixed(4)),
      recommended_bet: Math.max(100, (6 - index) * 100),
      ticket_type: index < 3 ? "main" : "counter",
      explanation_tags: safeArray(row?.reason_tags)
    }));
}

function formatShapeGroup(lanes = []) {
  return [...new Set(safeArray(lanes).filter(Number.isInteger))].sort((a, b) => a - b).join("");
}

function expandTrifectaShape({ first = [], second = [], third = [] }) {
  const firstSet = [...new Set(safeArray(first).filter(Number.isInteger))];
  const secondSet = [...new Set(safeArray(second).filter(Number.isInteger))];
  const thirdSet = [...new Set(safeArray(third).filter(Number.isInteger))];
  const combos = [];
  for (const a of firstSet) {
    for (const b of secondSet) {
      for (const c of thirdSet) {
        if (new Set([a, b, c]).size !== 3) continue;
        combos.push(`${a}-${b}-${c}`);
      }
    }
  }
  return [...new Set(combos)].sort();
}

function buildHitRateShapeRecommendation({
  firstProbs,
  secondProbs,
  thirdProbs,
  boat1EscapeProbability,
  confidence
}) {
  const firstRows = safeArray(firstProbs).map((row) => ({
    lane: toInt(row?.lane, null),
    weight: toNum(row?.weight, 0)
  })).filter((row) => Number.isInteger(row.lane));
  const secondRows = safeArray(secondProbs).map((row) => ({
    lane: toInt(row?.lane, null),
    weight: toNum(row?.weight, 0)
  })).filter((row) => Number.isInteger(row.lane));
  const thirdRows = safeArray(thirdProbs).map((row) => ({
    lane: toInt(row?.lane, null),
    weight: toNum(row?.weight, 0)
  })).filter((row) => Number.isInteger(row.lane));
  const topFirst = firstRows[0] || { lane: null, weight: 0 };
  const secondFirst = firstRows[1] || { lane: null, weight: 0 };
  const firstDominance = topFirst.weight - secondFirst.weight;
  const secondConcentration = secondRows.slice(0, 2).reduce((sum, row) => sum + row.weight, 0);
  const thirdConcentration = thirdRows.slice(0, 3).reduce((sum, row) => sum + row.weight, 0);
  const dominantBoat1 =
    topFirst.lane === 1 &&
    topFirst.weight >= 0.33 &&
    (firstDominance >= 0.08 || toNum(boat1EscapeProbability, 0) >= 0.48) &&
    toNum(confidence, 0) >= 50;
  if (!dominantBoat1) {
    return {
      selected_shape: null,
      expanded_tickets: [],
      reason_tags: [],
      concentration_metrics: {
        first_place_dominance: Number(firstDominance.toFixed(4)),
        second_place_concentration: Number(secondConcentration.toFixed(4)),
        third_place_concentration: Number(thirdConcentration.toFixed(4))
      }
    };
  }

  const secondCandidates = secondRows
    .filter((row) => row.lane !== 1)
    .slice(0, secondConcentration >= 0.58 ? 2 : 1)
    .map((row) => row.lane);
  const thirdCandidates = thirdRows
    .filter((row) => row.lane !== 1)
    .slice(0, thirdConcentration >= 0.7 ? 3 : secondConcentration >= 0.54 ? 2 : 1)
    .map((row) => row.lane);
  if (secondCandidates.length === 0 || thirdCandidates.length === 0) {
    return {
      selected_shape: null,
      expanded_tickets: [],
      reason_tags: [],
      concentration_metrics: {
        first_place_dominance: Number(firstDominance.toFixed(4)),
        second_place_concentration: Number(secondConcentration.toFixed(4)),
        third_place_concentration: Number(thirdConcentration.toFixed(4))
      }
    };
  }
  const shape = {
    first: [1],
    second: secondCandidates,
    third: [...new Set([...secondCandidates, ...thirdCandidates])].sort((a, b) => a - b)
  };
  const selectedShape = `${formatShapeGroup(shape.first)}-${formatShapeGroup(shape.second)}-${formatShapeGroup(shape.third)}`;
  const expandedTickets = expandTrifectaShape(shape);
  return {
    selected_shape: selectedShape,
    expanded_tickets: expandedTickets,
    first: shape.first,
    second: shape.second,
    third: shape.third,
    reason_tags: [
      "HIT_RATE_SHAPE",
      "BOAT1_FIRST_DOMINANT",
      secondCandidates.length >= 2 ? "SECOND_PLACE_CONCENTRATED" : "SECOND_PLACE_SINGLE_FOCUS",
      thirdCandidates.length >= 3 ? "THIRD_PLACE_SURVIVOR_CLUSTER" : "THIRD_PLACE_TIGHT_CLUSTER"
    ],
    concentration_metrics: {
      first_place_dominance: Number(firstDominance.toFixed(4)),
      second_place_concentration: Number(secondConcentration.toFixed(4)),
      third_place_concentration: Number(thirdConcentration.toFixed(4))
    }
  };
}

function buildShapeBasedTrifectaTickets({
  shapeRecommendation,
  firstProbs,
  secondProbs,
  thirdProbs,
  confidence
}) {
  if (!shapeRecommendation?.selected_shape || !safeArray(shapeRecommendation?.expanded_tickets).length) return [];
  const firstMap = new Map(safeArray(firstProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const secondMap = new Map(safeArray(secondProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const thirdMap = new Map(safeArray(thirdProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const confFactor = clamp(0.9, 1.08, toNum(confidence, 50) / 62);
  return safeArray(shapeRecommendation.expanded_tickets)
    .map((combo, index) => {
      const [a, b, c] = combo.split("-").map((v) => toInt(v, null));
      if (![a, b, c].every(Number.isInteger)) return null;
      const prob = clamp(
        0,
        1,
        toNum(firstMap.get(a), 0) * toNum(secondMap.get(b), 0) * toNum(thirdMap.get(c), 0) * 7.8 * confFactor
      );
      return {
        combo,
        prob: Number(prob.toFixed(4)),
        recommended_bet: Math.max(100, Math.round((280 - index * 30) / 100) * 100),
        ticket_type: "shape_main",
        explanation_tags: [...new Set([...(shapeRecommendation.reason_tags || []), `SHAPE_${shapeRecommendation.selected_shape}`])],
        explanation_summary: `Recommended Shape: ${shapeRecommendation.selected_shape}`,
        shape_label: shapeRecommendation.selected_shape,
        shape_rank_bonus: Number((0.0012 - index * 0.00008).toFixed(4))
      };
    })
    .filter(Boolean);
}

function mergeShapeBasedTickets(baseTickets, shapeTickets) {
  const merged = new Map();
  for (const row of normalizeSavedBetSnapshotItems(baseTickets)) {
    merged.set(normalizeCombo(row?.combo), row);
  }
  for (const row of normalizeSavedBetSnapshotItems(shapeTickets)) {
    const combo = normalizeCombo(row?.combo);
    const existing = merged.get(combo);
    if (!existing) {
      merged.set(combo, row);
      continue;
    }
    merged.set(combo, {
      ...existing,
      prob: Number(Math.max(toNum(existing?.prob, 0), toNum(row?.prob, 0) + toNum(row?.shape_rank_bonus, 0)).toFixed(4)),
      recommended_bet: Math.max(toNum(existing?.recommended_bet, 100), toNum(row?.recommended_bet, 100)),
      explanation_tags: [...new Set([...
        safeArray(existing?.explanation_tags),
        ...safeArray(row?.explanation_tags)
      ])],
      explanation_summary: existing?.explanation_summary || row?.explanation_summary || null,
      shape_label: existing?.shape_label || row?.shape_label || null
    });
  }
  return [...merged.values()].sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0));
}

function generateExactaCoverTickets(firstProbs, secondProbs, confidence) {
  const mainHeadLane = topDistributionLane(firstProbs);
  if (mainHeadLane !== 1) return [];
  const secondTop = safeArray(secondProbs).slice(0, 3);
  const concentration = secondTop.slice(0, 2).reduce((sum, row) => sum + toNum(row?.weight, 0), 0);
  if (concentration < 0.42 || toNum(confidence, 0) < 48) return [];
  return secondTop
    .slice(0, concentration >= 0.62 ? 3 : 2)
    .filter((row) => [2, 3, 4].includes(toInt(row?.lane, null)))
    .map((row, index) => ({
      combo: `1-${toInt(row?.lane, null)}`,
      prob: Number((toNum(row?.weight, 0) * clamp(0.88, 1.05, toNum(confidence, 50) / 65)).toFixed(4)),
      recommended_bet: Math.max(100, (3 - index) * 100),
      exacta_reason_tags: ["ROLE_BASED_EXACTA", "BOAT1_ESCAPE_COVER"]
    }));
}

function generateBackupUrasujiTickets(orderCandidates, attackProbs, confidence) {
  if (toNum(confidence, 0) < 52) return [];
  const attackScenario = safeArray(attackProbs).sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0))[0];
  if (!attackScenario || toNum(attackScenario?.probability, 0) < 0.28) return [];
  return safeArray(orderCandidates)
    .filter((row) => safeArray(row?.reason_tags).includes("URASUJI_BACKUP_USED"))
    .slice(0, 2)
    .map((row) => ({
      combo: normalizeCombo(row?.combo),
      prob: Number(toNum(row?.probability, 0).toFixed(4)),
      recommended_bet: 100,
      backup_reason_tags: ["ROLE_BASED_URASUJI", "CONDITIONAL_URASUJI"]
    }));
}

function buildEvidenceBiasTable({
  featureBundle,
  firstProbs,
  secondProbs,
  thirdProbs,
  attackProbs,
  survivalProbs
}) {
  const laneRows = safeArray(featureBundle?.lane_bundle);
  const firstMap = new Map(safeArray(firstProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const secondMap = new Map(safeArray(secondProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const thirdMap = new Map(safeArray(thirdProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const survivalMap = new Map(safeArray(survivalProbs).map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]));
  const attackMap = new Map();
  for (const row of safeArray(attackProbs)) {
    const probability = toNum(row?.probability, 0);
    const scenario = String(row?.scenario || "");
    if (scenario.includes("boat2")) attackMap.set(2, Math.max(toNum(attackMap.get(2), 0), probability));
    if (scenario.includes("boat3")) attackMap.set(3, Math.max(toNum(attackMap.get(3), 0), probability));
    if (scenario.includes("boat4")) attackMap.set(4, Math.max(toNum(attackMap.get(4), 0), probability));
    if (scenario.includes("outside")) {
      attackMap.set(4, Math.max(toNum(attackMap.get(4), 0), probability));
      attackMap.set(5, Math.max(toNum(attackMap.get(5), 0), probability * 0.9));
      attackMap.set(6, Math.max(toNum(attackMap.get(6), 0), probability * 0.85));
    }
  }
  const evidenceGate = featureBundle?.role_layers?.outside_head_promotion_gate_json || {};
  const venueSummary = featureBundle?.role_layers?.venue_correction_summary || {};
  const partnerBias = featureBundle?.role_layers?.boat1_partner_bias_json || {};
  const groupScoresByLane = new Map();

  const groupConfigs = {
    motor: (lane, f) => ({
      head: toNum(f?.motor_total_score, 0) * 0.018,
      second: toNum(f?.motor_total_score, 0) * 0.024,
      third: toNum(f?.motor_total_score, 0) * 0.019,
      risk: 0
    }),
    lane_course_fit: (lane, f) => ({
      head: (lane === 1 ? 0.09 : lane === 2 ? 0.055 : lane === 3 ? 0.04 : lane === 4 ? 0.018 : 0) +
        toNum(f?.course_fit_score, 0) * 0.012 +
        toNum(f?.venue_lane_adjustment, 0) * 0.01,
      second: (lane >= 2 && lane <= 4 ? 0.03 : 0) + toNum(f?.course_fit_score, 0) * 0.01,
      third: (lane >= 2 && lane <= 5 ? 0.024 : 0) + toNum(f?.course_fit_score, 0) * 0.007,
      risk: lane >= 5 ? 0.01 : 0
    }),
    exhibition: (lane, f) => ({
      head:
        Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 0.012 +
        Math.max(0, toNum(f?.lap_attack_strength, 0)) * 0.002,
      second:
        Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 0.016 +
        Math.max(0, toNum(f?.lap_attack_strength, 0)) * 0.003 +
        Math.max(0, toNum(f?.lap_time_delta_vs_front, 0)) * 0.18,
      third:
        Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 0.01 +
        Math.max(0, toNum(f?.lap_attack_strength, 0)) * 0.0024,
      risk: 0
    }),
    risk: (lane, f) => ({
      head: 0,
      second: 0,
      third: 0,
      risk:
        toNum(f?.f_hold_caution_penalty, 0) * (lane === 1 ? 0.06 : 0.042) +
        (lane >= 5 && toInt(evidenceGate?.by_lane?.[String(lane)]?.blocked_by_gate, 0) === 1 ? 0.08 : 0)
    }),
    formation_scenario: (lane) => ({
      head: toNum(attackMap.get(lane), 0) * (lane >= 5 ? 0.03 : 0.06),
      second: toNum(attackMap.get(lane), 0) * 0.1 + (lane >= 2 && lane <= 4 ? 0.01 : 0),
      third: toNum(attackMap.get(lane), 0) * 0.075 + toNum(survivalMap.get(lane), 0) * 0.04,
      risk: lane >= 5 && toNum(attackMap.get(lane), 0) > 0 ? 0.01 : 0
    }),
    learning: (lane) => ({
      head:
        (lane === 1 ? 0.02 : lane >= 2 && lane <= 4 ? 0.01 : 0) +
        toNum(venueSummary?.recommendation_score_adjustment, 0) * 0.01,
      second:
        toNum(venueSummary?.second_place_partner_adjustment, 0) * (lane >= 2 && lane <= 4 ? 0.012 : 0.004),
      third:
        toNum(venueSummary?.third_place_residual_adjustment, 0) * (lane >= 2 && lane <= 4 ? 0.012 : 0.004),
      risk: lane >= 5 && toInt(evidenceGate?.by_lane?.[String(lane)]?.allowed_as_main_head, 0) !== 1 ? 0.008 : 0
    })
  };

  for (const row of laneRows) {
    const lane = toInt(row?.lane, null);
    if (!Number.isInteger(lane)) continue;
    const f = row?.features || {};
    const perGroup = {};
    for (const [groupKey, scorer] of Object.entries(groupConfigs)) {
      perGroup[groupKey] = scorer(lane, f);
    }
    groupScoresByLane.set(lane, perGroup);
  }

  const perGroupRankings = {};
  for (const groupKey of Object.keys(groupConfigs)) {
    perGroupRankings[groupKey] = [...groupScoresByLane.entries()]
      .map(([lane, groups]) => ({
        lane,
        head: Number(toNum(groups?.[groupKey]?.head, 0).toFixed(4)),
        second: Number(toNum(groups?.[groupKey]?.second, 0).toFixed(4)),
        third: Number(toNum(groups?.[groupKey]?.third, 0).toFixed(4)),
        risk: Number(toNum(groups?.[groupKey]?.risk, 0).toFixed(4)),
        total: Number((toNum(groups?.[groupKey]?.head, 0) + toNum(groups?.[groupKey]?.second, 0) + toNum(groups?.[groupKey]?.third, 0) - toNum(groups?.[groupKey]?.risk, 0)).toFixed(4))
      }))
      .sort((a, b) => toNum(b?.total, 0) - toNum(a?.total, 0))
      .slice(0, 3);
  }

  const boatSummary = {};
  for (const [lane, groups] of groupScoresByLane.entries()) {
    const strongestGroups = Object.entries(groups)
      .map(([groupKey, values]) => ({
        group: groupKey,
        total: toNum(values?.head, 0) + toNum(values?.second, 0) + toNum(values?.third, 0) - toNum(values?.risk, 0)
      }))
      .sort((a, b) => b.total - a.total)
      .slice(0, 3)
      .map((row) => row.group);
    const headSupportScore = Number((
      Object.values(groups).reduce((sum, values) => sum + toNum(values?.head, 0), 0) +
      toNum(firstMap.get(lane), 0) * 0.42 +
      (lane === 1 ? 0.06 : lane === 2 ? 0.028 : lane === 3 ? 0.015 : 0)
    ).toFixed(4));
    const secondSupportScore = Number((Object.values(groups).reduce((sum, values) => sum + toNum(values?.second, 0), 0) + toNum(secondMap.get(lane), 0) * 0.28).toFixed(4));
    const thirdSupportScore = Number((Object.values(groups).reduce((sum, values) => sum + toNum(values?.third, 0), 0) + toNum(thirdMap.get(lane), 0) * 0.24).toFixed(4));
    const riskPenalty = Number((Object.values(groups).reduce((sum, values) => sum + toNum(values?.risk, 0), 0)).toFixed(4));
    const independentEvidenceCount = Object.values(groups)
      .filter((values) => Math.max(toNum(values?.head, 0), toNum(values?.second, 0), toNum(values?.third, 0)) - toNum(values?.risk, 0) >= 0.028)
      .length;
    const warnings = [];
    if (
      lane >= 5 &&
      (
        independentEvidenceCount < 3 ||
        toInt(evidenceGate?.by_lane?.[String(lane)]?.blocked_by_gate, 0) === 1 ||
        toInt(evidenceGate?.by_lane?.[String(lane)]?.allowed_as_counter_only, 0) === 1
      )
    ) warnings.push("OUTER_SUPPORT_NARROW");
    if (lane >= 5 && strongestGroups.every((group) => ["formation_scenario", "exhibition"].includes(group))) {
      warnings.push("RELATED_OUTER_SIGNALS_DUPLICATED");
    }
    if (riskPenalty >= 0.08) warnings.push("RISK_HEAVY");
    boatSummary[String(lane)] = {
      head_support_score: headSupportScore,
      second_support_score: secondSupportScore,
      third_support_score: thirdSupportScore,
      risk_penalty: riskPenalty,
      independent_evidence_count: independentEvidenceCount,
      strongest_groups: strongestGroups,
      warnings
    };
  }

  const sortedByHead = Object.entries(boatSummary).sort((a, b) => toNum(b[1]?.head_support_score, 0) - toNum(a[1]?.head_support_score, 0));
  const sortedBySecond = Object.entries(boatSummary).sort((a, b) => toNum(b[1]?.second_support_score, 0) - toNum(a[1]?.second_support_score, 0));
  const sortedByThird = Object.entries(boatSummary).sort((a, b) => toNum(b[1]?.third_support_score, 0) - toNum(a[1]?.third_support_score, 0));

  const interpretation = [
    sortedByHead[0]
      ? `Boat ${sortedByHead[0][0]} is the main head candidate with support across ${safeArray(sortedByHead[0][1]?.strongest_groups).slice(0, 2).join(" + ")}`
      : null,
    sortedBySecond[0]
      ? `Boat ${sortedBySecond[0][0]} is the main second-place candidate from broad grouped support`
      : null,
    sortedByThird[0]
      ? `Boat ${sortedByThird[0][0]} remains a practical third-place survivor`
      : null,
    boatSummary["1"] && toNum(boatSummary["1"]?.head_support_score, 0) >= toNum(boatSummary["1"]?.second_support_score, 0)
      ? "Boat 1 remains the most stable head due to inside and lane-fit strength"
      : null
  ].filter(Boolean);

  return {
    per_group_rankings: perGroupRankings,
    boat_summary: boatSummary,
    interpretation,
    confirmation_flags: {
      main_head_candidate: sortedByHead[0] ? Number(sortedByHead[0][0]) : null,
      main_second_candidate: sortedBySecond[0] ? Number(sortedBySecond[0][0]) : null,
      counter_second_candidate: sortedBySecond[1] ? Number(sortedBySecond[1][0]) : null,
      third_place_survivors: sortedByThird.slice(0, 3).map(([lane]) => Number(lane))
    }
  };
}

function applyEvidenceBiasConfirmationToRoleProbabilities({
  featureBundle,
  firstProbs,
  secondProbs,
  thirdProbs,
  evidenceBiasTable
}) {
  const boatSummary = evidenceBiasTable?.boat_summary || {};
  const confirmRows = (rows, roleKey) => normalizeDistributionRows(safeArray(rows).map((row) => {
    const lane = toInt(row?.lane, null);
    const summary = boatSummary[String(lane)] || {};
    const supportScore = toNum(summary?.[roleKey], 0);
    const independentCount = toInt(summary?.independent_evidence_count, 0);
    const riskPenalty = toNum(summary?.risk_penalty, 0);
    const warnings = new Set(safeArray(summary?.warnings));
    let bonus = 0;
    if (independentCount >= 3) bonus += roleKey === "head_support_score" ? 0.035 : 0.028;
    else if (independentCount === 2) bonus += roleKey === "head_support_score" ? 0.008 : 0.018;
    bonus += Math.min(0.03, supportScore * (roleKey === "head_support_score" ? 0.05 : 0.065));
    if (warnings.has("OUTER_SUPPORT_NARROW") && lane >= 5 && roleKey === "head_support_score") bonus -= 0.04;
    if (warnings.has("RELATED_OUTER_SIGNALS_DUPLICATED") && lane >= 5 && roleKey === "head_support_score") bonus -= 0.035;
    bonus -= Math.min(0.05, riskPenalty * (roleKey === "head_support_score" ? 0.7 : 0.38));
    return {
      ...row,
      weight: toNum(row?.weight, 0) + bonus
    };
  }));
  return {
    confirmed_first_place_probability_json: confirmRows(firstProbs, "head_support_score"),
    confirmed_second_place_probability_json: confirmRows(secondProbs, "second_support_score"),
    confirmed_third_place_probability_json: confirmRows(thirdProbs, "third_support_score")
  };
}

function buildBackupUrasujiRecommendationsSnapshot({
  recommendedBets,
  optimizedTickets,
  candidateDistributions
}) {
  const sourceRows = [
    ...safeArray(optimizedTickets),
    ...safeArray(recommendedBets)
  ];
  if (!sourceRows.length) {
    return {
      shown: false,
      items: [],
      backup_reason_tags: []
    };
  }
  const roleSummary = candidateDistributions?.role_probability_summary_json || {};
  const partnerBias = candidateDistributions?.boat1_partner_bias_json || {};
  const useUrasuji = toInt(partnerBias?.urasuji_used, 0) === 1 || toInt(candidateDistributions?.partner_search_bias_json?.urasuji_used, 0) === 1;
  const attackThirdOnlyLane = toInt(partnerBias?.attack_shape_third_only_lane, null);
  const attackSecondOnlyLane = toInt(partnerBias?.attack_shape_second_only_lane, null);
  const mainHeadLane = toInt(roleSummary?.main_head_lane, null);
  if (!useUrasuji || mainHeadLane !== 1) {
    return {
      shown: false,
      items: [],
      backup_reason_tags: []
    };
  }

  const backupItems = sourceRows
    .map((row) => {
      const combo = normalizeCombo(row?.combo);
      const lanes = combo ? combo.split("-").map((value) => toInt(value, null)).filter(Number.isInteger) : [];
      const [headLane, secondLane, thirdLane] = lanes;
      if (headLane !== 1 || lanes.length !== 3) return null;
      const followsAttackLane =
        (Number.isInteger(attackSecondOnlyLane) && (secondLane === attackSecondOnlyLane + 1 || thirdLane === attackSecondOnlyLane + 1)) ||
        (Number.isInteger(attackThirdOnlyLane) && (secondLane === attackThirdOnlyLane + 1 || thirdLane === attackThirdOnlyLane + 1));
      if (!followsAttackLane) return null;
      return {
        combo,
        prob: Number(toNum(row?.prob, 0).toFixed(4)),
        recommended_bet: toNum(row?.recommended_bet ?? row?.bet, 100),
        ticket_type: row?.ticket_type || "backup",
        backup_reason_tags: ["URASUJI_BACKUP", "BOAT1_ESCAPE_BACKUP"]
      };
    })
    .filter(Boolean)
    .sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0))
    .slice(0, 2);

  return {
    shown: backupItems.length > 0,
    items: backupItems,
    backup_reason_tags: backupItems.length > 0 ? ["URASUJI_BACKUP", "BOAT1_ESCAPE_BACKUP"] : []
  };
}

function buildSeparatedCandidateDistributions({
  ranking,
  tickets,
  headScenarioBalanceAnalysis,
  escapePatternAnalysis,
  attackScenarioAnalysis,
  learningWeights,
  race
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const formationFirstPlacePrior = buildFormationFirstPlacePrior(escapePatternAnalysis);
  const priorMap = new Map(formationFirstPlacePrior.map((row) => [row.lane, toNum(row.weight, 0)]));
  const headMap = new Map(
    safeArray(headScenarioBalanceAnalysis?.head_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const secondMapFromExisting = new Map(
    safeArray(headScenarioBalanceAnalysis?.second_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const mainHeadLane = toInt(headScenarioBalanceAnalysis?.main_head_lane, 1);
  const attackHeadLane = toInt(headScenarioBalanceAnalysis?.attack_head_lane, null);
  const formationLearnedAdj = getSegmentCorrectionValue(
    learningWeights,
    "formation_pattern",
    escapePatternAnalysis?.formation_pattern || null,
    "pattern_strength_adjustment"
  );
  const venueLearnedAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "recommendation_score_adjustment"
  );
  const venuePartnerAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "second_place_partner_adjustment"
  );
  const venueThirdResidualAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "third_place_residual_adjustment"
  );
  const venueLapWeightAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "lap_weight_adjustment"
  );
  const venueFHoldAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "f_hold_caution_adjustment"
  );
  const venueOuterSuppressionAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "outer_head_suppression_adjustment"
  );
  const boat1EscapeOpponentModel = mainHeadLane === 1
    ? buildBoat1EscapeOpponentModel({
        rows,
        escapePatternAnalysis,
        attackScenarioAnalysis
      })
    : null;
  const outsideHeadPromotionContext = buildOutsideHeadPromotionContext({
    rows,
    race,
    learningWeights,
    escapePatternAnalysis,
    attackScenarioAnalysis,
    headScenarioBalanceAnalysis
  });
  const boat3WeakStHeadSuppression = buildBoat3WeakStHeadSuppressionContext({
    rows,
    headScenarioBalanceAnalysis,
    attackScenarioAnalysis,
    outsideHeadPromotionContext
  });
  const boat1EscapePartnerVersion = "boat1_escape_partner_v2";
  const launchVenueCalibration = getVenueLaunchMicroCalibration({
    race,
    venueSummary: { ...headScenarioBalanceAnalysis?.venue_correction_summary, venue_segment_key: race?.venueId ?? headScenarioBalanceAnalysis?.venue_correction_summary?.venue_segment_key }
  });
  const launchStateScores = computeLaunchStateScores(rows, launchVenueCalibration);
  const launchStateLabels = classifyLaunchStates(launchStateScores);
  const launchStateThresholdsUsed = getLaunchStateConfig();
  const fHolderRolePenaltySummary = Object.fromEntries(
    rows
      .map((row) => {
        const lane = toInt(row?.racer?.lane, null);
        if (!Number.isInteger(lane)) return null;
        return [String(lane), getFHolderPenaltyByRole(row?.features || {}, lane)];
      })
      .filter(Boolean)
  );
  const intermediateDevelopmentEvents = buildIntermediateDevelopmentEvents({
    launchStateScores,
    rows,
    race,
    headScenarioBalanceAnalysis,
    escapePatternAnalysis,
    venueCalibration: launchVenueCalibration
  });

  let firstPlaceDistribution = normalizeDistributionRows(rows.map((row) => {
    const lane = toInt(row?.racer?.lane, null);
    const f = row?.features || {};
    const fRolePenalty = getFHolderPenaltyByRole(f, lane);
    const outsideLeadRoleBoost =
      String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead"
        ? lane === 4 ? 2.1 : lane === 5 ? 1.2 : lane === 6 ? 1.0 : 0
        : 0;
    const outsideGate = outsideHeadPromotionContext.by_lane.get(lane) || null;
    const outerHeadPenalty = lane === 5
      ? 12 + venueOuterSuppressionAdj * 1.7 + (mainHeadLane === 1 ? 4.5 : 0)
      : lane === 6
        ? 20 + venueOuterSuppressionAdj * 1.9 + (mainHeadLane === 1 ? 6.5 : 0)
        : 0;
    const outsideGatePenalty =
      lane === 5 || lane === 6
        ? outsideGate?.blocked_by_gate
          ? 22
          : outsideGate?.allowed_as_counter_only
            ? 11
            : 0
        : 0;
    const boat1StrengthPenalty =
      (lane === 5 || lane === 6) && outsideGate?.blocked_by_boat1_escape
        ? 10
        : 0;
    const firstPlaceScore =
      toNum(row?.score, 0) * 0.56 +
        toNum(priorMap.get(lane), 0) * 42 +
        toNum(headMap.get(lane), 0) * 34 +
        Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 3.6 +
        Math.max(0, toNum(f?.lap_time_delta_vs_front, 0)) * (18 + venueLapWeightAdj * 20) +
        toNum(f?.lap_attack_flag, 0) * 4 +
        toNum(f?.lap_attack_strength, 0) * (0.34 + venueLapWeightAdj * 0.2) +
        toNum(f?.motor_total_score, 0) * 1.15 +
        Math.max(0, 7 - toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6)) * 2.8 -
      outsideGatePenalty -
      boat1StrengthPenalty -
      outerHeadPenalty -
      toNum(fRolePenalty?.first_penalty, 0) * (1.12 + venueFHoldAdj * 0.08) +
        (lane === 1 ? toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0) * 0.22 : 0) +
        (attackHeadLane && lane === attackHeadLane ? toNum(attackScenarioAnalysis?.attack_scenario_score, 0) * 0.08 : 0) +
      outsideLeadRoleBoost +
        formationLearnedAdj * 0.5 +
        venueLearnedAdj * 0.4 -
        (lane === 3 ? toNum(boat3WeakStHeadSuppression?.penalty_score, 0) : 0);
    return { lane, role: lane === mainHeadLane ? "main" : lane === 1 ? "survival" : "counter", weight: firstPlaceScore };
  }));
  firstPlaceDistribution = normalizeDistributionRows(firstPlaceDistribution.map((row) => {
    const lane = toInt(row?.lane, null);
    const outsideGate = outsideHeadPromotionContext.by_lane.get(lane) || null;
    if (lane === 3 && toInt(boat3WeakStHeadSuppression?.applied, 0) === 1) {
      return {
        ...row,
        weight: Math.min(
          toNum(row?.weight, 0),
          toNum(boat3WeakStHeadSuppression?.first_place_cap_weight, 1)
        )
      };
    }
    if (!outsideGate) return row;
    return {
      ...row,
      weight: Math.min(toNum(row?.weight, 0), toNum(outsideGate?.first_place_cap_weight, 1))
    };
  }));

  const secondPlaceDistribution = normalizeDistributionRows(rows.map((row) => {
    const lane = toInt(row?.racer?.lane, null);
    const f = row?.features || {};
    const fRolePenalty = getFHolderPenaltyByRole(f, lane);
    const outsideLeadRoleBoost =
      String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead"
        ? lane === 4 ? 5.8 : lane === 5 ? 4.4 : lane === 6 ? 3.8 : 0
        : 0;
    const outsideGate = outsideHeadPromotionContext.by_lane.get(lane) || null;
    const escapePartnerBias =
      mainHeadLane === 1 && escapePatternAnalysis?.escape_pattern_applied
        ? getEscapeSecondPlaceBiasScore(escapePatternAnalysis?.escape_second_place_bias_json || {}, lane) * 2.5
        : 0;
    const insideRemainBias =
      mainHeadLane === 1
        ? lane === 2 ? 12 : lane === 3 ? 10 : lane === 4 ? 6.4 : lane === 1 ? 1.4 : -2
        : lane === 2 ? 8 : lane === 3 ? 7 : lane === 4 ? 4.2 : lane === 1 ? 1.8 : 0;
    const outerPartnerPenalty = lane === 5 ? 5.5 + venueOuterSuppressionAdj : lane === 6 ? 8.5 + venueOuterSuppressionAdj : 0;
    const partnerWeight =
      toNum(secondMapFromExisting.get(lane), 0) * 42 +
      toNum(row?.score, 0) * 0.34 +
      escapePartnerBias +
      insideRemainBias +
      venuePartnerAdj * (lane >= 2 && lane <= 4 ? 1.4 : lane === 1 ? 0.2 : -0.8) +
      Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 2.4 +
      Math.max(0, toNum(f?.lap_time_delta_vs_front, 0)) * (16 + venueLapWeightAdj * 18) +
      toNum(f?.lap_attack_flag, 0) * 3.5 +
      toNum(f?.lap_attack_strength, 0) * (0.28 + venueLapWeightAdj * 0.16) +
      toNum(f?.motor_total_score, 0) * 0.95 +
      Math.max(0, 7 - toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6)) * 2.1 +
      Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * 16 +
      toNum(f?.slit_alert_flag, 0) * 4 -
      (outsideGate?.blocked_by_gate ? 1.8 : 0) +
      outsideLeadRoleBoost -
      outerPartnerPenalty -
      toNum(fRolePenalty?.second_penalty, 0) * (1 + venueFHoldAdj * 0.04);
    return { lane, role: lane === 2 || lane === 3 ? "primary_partner" : "partner", weight: partnerWeight };
  }));

  const ticketRows = Array.isArray(tickets) ? tickets : [];
  const thirdCounts = new Map();
  for (const row of ticketRows) {
    const combo = normalizeCombo(row?.combo);
    const thirdLane = toInt(String(combo || "").split("-")[2], null);
    if (!Number.isInteger(thirdLane)) continue;
    thirdCounts.set(thirdLane, toNum(thirdCounts.get(thirdLane), 0) + Math.max(0.001, toNum(row?.prob, 0)));
  }
  const thirdPlaceDistribution = normalizeDistributionRows(rows.map((row) => {
    const lane = toInt(row?.racer?.lane, null);
    const f = row?.features || {};
    const fRolePenalty = getFHolderPenaltyByRole(f, lane);
    const outsideLeadRoleBoost =
      String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead"
        ? lane === 4 ? 3.8 : lane === 5 ? 3.2 : lane === 6 ? 2.6 : 0
        : 0;
    const outsideGate = outsideHeadPromotionContext.by_lane.get(lane) || null;
    const insideResidualBias = lane === 2 ? 5.4 : lane === 3 ? 4.8 : lane === 4 ? 3.2 : lane === 1 ? 2.4 : 0;
    const outerThirdPenalty = lane === 5 ? 2.4 + venueOuterSuppressionAdj * 0.75 : lane === 6 ? 3.8 + venueOuterSuppressionAdj * 0.75 : 0;
    const thirdWeight =
      toNum(thirdCounts.get(lane), 0) * 48 +
      toNum(row?.score, 0) * 0.18 +
      insideResidualBias +
      venueThirdResidualAdj * (lane >= 2 && lane <= 4 ? 1.2 : lane === 1 ? 0.4 : -0.6) +
      Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 1.7 +
      Math.max(0, toNum(f?.lap_time_delta_vs_front, 0)) * (10 + venueLapWeightAdj * 10) +
      toNum(f?.lap_attack_strength, 0) * (0.16 + venueLapWeightAdj * 0.08) +
      toNum(f?.motor_total_score, 0) * 0.72 +
      Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * 10 -
      (outsideGate?.blocked_by_gate ? 1.1 : 0) +
      outsideLeadRoleBoost -
      outerThirdPenalty -
      toNum(fRolePenalty?.third_penalty, 0) * (1 + venueFHoldAdj * 0.02);
    return { lane, role: "third", weight: thirdWeight };
  }));

  const boat1SecondPlaceDistribution =
    mainHeadLane === 1
      ? normalizeDistributionRows(
          rows
            .map((row) => {
              const lane = toInt(row?.racer?.lane, null);
              if (!Number.isInteger(lane) || lane === 1) return null;
              const f = row?.features || {};
              const fRolePenalty = getFHolderPenaltyByRole(f, lane);
              const opponentSecondBias = toNum(boat1EscapeOpponentModel?.second_adjustments?.get(lane), 0);
              const escapeBias = escapePatternAnalysis?.escape_pattern_applied
                ? getEscapeSecondPlaceBiasScore(escapePatternAnalysis?.escape_second_place_bias_json || {}, lane) * 3.1
                : 0;
              const insidePriority = lane === 2 ? 18 : lane === 3 ? 15 : lane === 4 ? 10.5 : lane === 5 ? -4.5 : -7;
              const stabilityBias =
                toNum(f?.class_score, 0) * 1.9 +
                toNum(f?.nationwide_win_rate, 0) * 0.42 +
                toNum(f?.local_win_rate, 0) * 0.5;
              const secondPlaceScore =
                toNum(secondMapFromExisting.get(lane), 0) * 18 +
                escapeBias +
                insidePriority +
                opponentSecondBias +
                venuePartnerAdj * (lane >= 2 && lane <= 4 ? 1.7 : -1) +
                toNum(f?.course_fit_score, 0) * 0.85 +
                toNum(f?.venue_lane_adjustment, 0) * 0.9 +
                Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 3.2 +
                Math.max(0, toNum(f?.lap_time_delta_vs_front, 0)) * (18 + venueLapWeightAdj * 18) +
                toNum(f?.lap_attack_flag, 0) * 4.2 +
                toNum(f?.lap_attack_strength, 0) * (0.26 + venueLapWeightAdj * 0.14) +
                toNum(f?.motor_total_score, 0) * 1.15 +
                Math.max(0, 7 - toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6)) * 2.8 +
                toNum(f?.entry_advantage_score, 0) * 0.8 +
                Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * 18 +
                Math.max(0, toNum(f?.avg_st_rank_delta_vs_left, 0)) * 2.4 +
                toNum(f?.slit_alert_flag, 0) * 4.5 +
                (String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead" && lane >= 4 ? (lane === 4 ? 4.5 : lane === 5 ? 3.4 : 2.8) : 0) +
                stabilityBias -
                (lane === 5 ? 8.5 + venueOuterSuppressionAdj * 1.1 : lane === 6 ? 12.5 + venueOuterSuppressionAdj * 1.3 : 0) -
                Math.min(2.2, toNum(fRolePenalty?.second_penalty, 0) * (0.95 + venueFHoldAdj * 0.04));
              return { lane, role: lane <= 4 ? "boat1_partner_primary" : "boat1_partner_secondary", weight: secondPlaceScore };
            })
            .filter(Boolean)
        )
      : [];

  const boat1ThirdPlaceDistribution =
    mainHeadLane === 1
      ? normalizeDistributionRows(
          rows
            .map((row) => {
              const lane = toInt(row?.racer?.lane, null);
              if (!Number.isInteger(lane) || lane === 1) return null;
              const f = row?.features || {};
              const fRolePenalty = getFHolderPenaltyByRole(f, lane);
              const insideRemainBias = lane === 2 ? 9 : lane === 3 ? 8.4 : lane === 4 ? 6.6 : lane === 5 ? -2.1 : -3.4;
              const opponentThirdBias = toNum(boat1EscapeOpponentModel?.third_adjustments?.get(lane), 0);
              const thirdPlaceScore =
                toNum(thirdCounts.get(lane), 0) * 42 +
                insideRemainBias +
                opponentThirdBias +
                venueThirdResidualAdj * (lane >= 2 && lane <= 4 ? 1.5 : -0.8) +
                Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 1.9 +
                Math.max(0, toNum(f?.lap_time_delta_vs_front, 0)) * (9 + venueLapWeightAdj * 9) +
                toNum(f?.lap_attack_flag, 0) * 1.6 +
                toNum(f?.motor_total_score, 0) * 0.88 +
                Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * 10 +
                Math.max(0, toNum(f?.avg_st_rank_delta_vs_left, 0)) * 1.2 +
                toNum(f?.entry_advantage_score, 0) * 0.35 -
                (String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead" && lane >= 4 ? (lane === 4 ? 2.8 : lane === 5 ? 2.2 : 1.8) : 0) -
                (lane === 5 ? 3.5 + venueOuterSuppressionAdj * 0.7 : lane === 6 ? 5.4 + venueOuterSuppressionAdj * 0.9 : 0) -
                Math.min(1.2, toNum(fRolePenalty?.third_penalty, 0) * (0.9 + venueFHoldAdj * 0.02));
              return { lane, role: lane <= 4 ? "boat1_third_primary" : "boat1_third_secondary", weight: thirdPlaceScore };
            })
            .filter(Boolean)
        )
      : [];

  const effectiveSecondPlaceDistribution =
    mainHeadLane === 1 && boat1SecondPlaceDistribution.length > 0
      ? normalizeDistributionRows(
          rows
            .map((row) => {
              const lane = toInt(row?.racer?.lane, null);
              if (!Number.isInteger(lane)) return null;
              const generic = toNum(secondPlaceDistribution.find((item) => toInt(item?.lane, null) === lane)?.weight, 0);
              const boat1Specific = toNum(boat1SecondPlaceDistribution.find((item) => toInt(item?.lane, null) === lane)?.weight, 0);
              return {
                lane,
                role: boat1Specific > generic ? "boat1_partner_primary" : "partner",
                weight: generic * 0.42 + boat1Specific * 0.58
              };
            })
            .filter(Boolean)
        )
      : secondPlaceDistribution;

  const effectiveThirdPlaceDistribution =
    mainHeadLane === 1 && boat1ThirdPlaceDistribution.length > 0
      ? normalizeDistributionRows(
          rows
            .map((row) => {
              const lane = toInt(row?.racer?.lane, null);
              if (!Number.isInteger(lane)) return null;
              const generic = toNum(thirdPlaceDistribution.find((item) => toInt(item?.lane, null) === lane)?.weight, 0);
              const boat1Specific = toNum(boat1ThirdPlaceDistribution.find((item) => toInt(item?.lane, null) === lane)?.weight, 0);
              return {
                lane,
                role: boat1Specific > generic ? "boat1_third_primary" : "third",
                weight: generic * 0.48 + boat1Specific * 0.52
              };
            })
            .filter(Boolean)
        )
      : thirdPlaceDistribution;

  const boat1PartnerReasonTags = [];
  if (mainHeadLane === 1) {
    boat1PartnerReasonTags.push("BOAT1_ESCAPE_PARTNER_MODEL");
    if (escapePatternAnalysis?.escape_pattern_applied) boat1PartnerReasonTags.push("ESCAPE_DICTIONARY");
    if (venuePartnerAdj !== 0) boat1PartnerReasonTags.push("VENUE_PARTNER_CORRECTION");
    if (venueThirdResidualAdj !== 0) boat1PartnerReasonTags.push("VENUE_THIRD_CORRECTION");
    boat1PartnerReasonTags.push("INSIDE_234_PRIORITY");
    boat1PartnerReasonTags.push(...safeArray(boat1EscapeOpponentModel?.reason_tags));
    boat1PartnerReasonTags.push(...safeArray(boat1EscapeOpponentModel?.boat1_stayed_head_reason_tags));
  }
  const roleProbabilityLayers = buildRoleProbabilityLayers({
    rows,
    candidateDistributions: {
      first_place_distribution_json: firstPlaceDistribution,
      second_place_distribution_json: effectiveSecondPlaceDistribution,
      third_place_distribution_json: effectiveThirdPlaceDistribution,
      boat1_second_place_distribution_json: boat1SecondPlaceDistribution,
      boat1_third_place_distribution_json: boat1ThirdPlaceDistribution,
      partner_search_bias_json: {
        attack_moved_second_only_lane: boat1EscapeOpponentModel?.attack_moved_second_only_lane ?? null,
        attack_moved_third_only_lane: boat1EscapeOpponentModel?.attack_moved_third_only_lane ?? null
      }
    },
    headScenarioBalanceAnalysis,
    escapePatternAnalysis,
    attackScenarioAnalysis
  });
  const raceScenarioProbabilities = computeRaceScenarioProbabilities({
    intermediateEvents: intermediateDevelopmentEvents,
    rows,
    race,
    attackScenarioAnalysis,
    escapePatternAnalysis,
    outsideHeadPromotionContext,
    headScenarioBalanceAnalysis,
    venueCalibration: launchVenueCalibration
  });
  const scenarioDictionaryContext = buildScenarioDictionaryContext({
    rows,
    race,
    escapePatternAnalysis,
    attackScenarioAnalysis,
    headScenarioBalanceAnalysis,
    candidateDistributions: {
      ...roleProbabilityLayers,
      launch_state_scores_json: launchStateScores,
      launch_state_labels_json: launchStateLabels,
      intermediate_development_events_json: intermediateDevelopmentEvents,
      race_scenario_probabilities_json: raceScenarioProbabilities,
      outside_head_promotion_gate_json: {
        inner_collapse_score: outsideHeadPromotionContext.inner_collapse_score,
        boat1_escape_probability_proxy: outsideHeadPromotionContext.boat1_escape_probability_proxy,
        by_lane: Object.fromEntries(
          [...outsideHeadPromotionContext.by_lane.entries()].map(([lane, value]) => [String(lane), value])
        )
      },
      boat3_weak_st_head_suppressed: toInt(boat3WeakStHeadSuppression?.applied, 0),
      partner_search_bias_json: {
        suji_used: toInt(boat1EscapeOpponentModel?.suji_used, 0)
      }
    }
  });
  const matchedDictionaryScenarios = matchScenarioDictionaryEntries(scenarioDictionaryContext);
  const dictionaryAdjustedScenarioProbabilities = applyScenarioDictionaryPriors({
    scenarioProbabilities: raceScenarioProbabilities,
    matchedDictionaryScenarios
  });
  const finishProbabilitiesByScenario = computeFinishProbsByScenario({
    scenarioProbabilities: dictionaryAdjustedScenarioProbabilities,
    firstPlaceProbability: roleProbabilityLayers.first_place_probability_json,
    secondPlaceProbability: roleProbabilityLayers.second_place_probability_json,
    thirdPlaceProbability: roleProbabilityLayers.third_place_probability_json,
    boat1EscapeProbability: roleProbabilityLayers.boat1_escape_probability,
    rows
  });
  const rawScenarioBasedOrderCandidates = combineScenarioAndFinishProbs({
    scenarioProbabilities: dictionaryAdjustedScenarioProbabilities,
    conditionalFinishProbs: finishProbabilitiesByScenario
  });
  const dictionaryAdjustedOrderCandidates = applyDictionaryPriorsToOrderCandidates({
    orderCandidates: rawScenarioBasedOrderCandidates,
    matchedDictionaryScenarios
  });
  const scenarioBasedOrderCandidates = dictionaryAdjustedOrderCandidates.order_candidates;
  const finishOverrideStrengthByLane = finishProbabilitiesByScenario[0]?.finish_override_strength_json || {};

  return {
    formation_first_place_prior_json: formationFirstPlacePrior,
    first_place_distribution_json: firstPlaceDistribution,
    second_place_distribution_json: effectiveSecondPlaceDistribution,
    third_place_distribution_json: effectiveThirdPlaceDistribution,
    boat1_second_place_distribution_json: boat1SecondPlaceDistribution,
    boat1_third_place_distribution_json: boat1ThirdPlaceDistribution,
    partner_search_bias_json: {
      boat1_main_head: mainHeadLane === 1 ? 1 : 0,
      escape_pattern_applied: escapePatternAnalysis?.escape_pattern_applied ? 1 : 0,
      boat1_partner_priority_lanes: mainHeadLane === 1 ? [2, 3, 4] : [],
      suji_used: toInt(boat1EscapeOpponentModel?.suji_used, 0),
      urasuji_used: toInt(boat1EscapeOpponentModel?.urasuji_used, 0),
      attack_moved_second_only_lane: boat1EscapeOpponentModel?.attack_moved_second_only_lane ?? null,
      attack_moved_third_only_lane: boat1EscapeOpponentModel?.attack_moved_third_only_lane ?? null,
      boat1_stayed_head_reason_tags: safeArray(boat1EscapeOpponentModel?.boat1_stayed_head_reason_tags),
      strongest_second_lanes: effectiveSecondPlaceDistribution.slice(0, 3),
      strongest_third_lanes: effectiveThirdPlaceDistribution.slice(0, 4)
    },
    boat1_partner_bias_json: {
      boat1_main_head: mainHeadLane === 1 ? 1 : 0,
      second_place_top: boat1SecondPlaceDistribution.slice(0, 4),
      third_place_top: boat1ThirdPlaceDistribution.slice(0, 4),
      inside_family_priority: ["1-2-x", "1-3-x", "1-4-x"],
      suji_used: toInt(boat1EscapeOpponentModel?.suji_used, 0),
      urasuji_used: toInt(boat1EscapeOpponentModel?.urasuji_used, 0),
      attack_shape_second_only_lane: boat1EscapeOpponentModel?.attack_moved_second_only_lane ?? null,
      attack_shape_third_only_lane: boat1EscapeOpponentModel?.attack_moved_third_only_lane ?? null,
      boat1_stayed_head_reason_tags: safeArray(boat1EscapeOpponentModel?.boat1_stayed_head_reason_tags),
      venue_partner_adjustment: venuePartnerAdj,
      venue_third_adjustment: venueThirdResidualAdj,
      outer_head_suppression_adjustment: venueOuterSuppressionAdj,
      outside_head_promotion_by_lane: Object.fromEntries(
        [...outsideHeadPromotionContext.by_lane.entries()].map(([lane, value]) => [String(lane), value])
      )
    },
    boat1_partner_reason_tags: [...new Set(boat1PartnerReasonTags)],
    partner_search_lap_bias_json: {
      top_lap_attack_second_lanes: effectiveSecondPlaceDistribution
        .filter((row) => toInt(row?.lane, null) >= 2)
        .slice(0, 3),
      top_lap_attack_third_lanes: effectiveThirdPlaceDistribution
        .filter((row) => toInt(row?.lane, null) >= 2)
        .slice(0, 4)
    },
    venue_correction_summary: {
      venue_segment_key: toInt(race?.venueId, null) ?? race?.venueName ?? null,
      recommendation_score_adjustment: venueLearnedAdj,
      second_place_partner_adjustment: venuePartnerAdj,
      third_place_residual_adjustment: venueThirdResidualAdj,
      lap_weight_adjustment: venueLapWeightAdj,
      f_hold_caution_adjustment: venueFHoldAdj,
      outer_head_suppression_adjustment: venueOuterSuppressionAdj
    },
    third_place_residual_bias_json: {
      strongest_third_lanes: thirdPlaceDistribution.slice(0, 4),
      inside_residual_priority: [2, 3, 4],
      noisy_outer_lanes: thirdPlaceDistribution
        .filter((row) => {
          const lane = toInt(row?.lane, null);
          return lane === 5 || lane === 6;
        })
        .slice(0, 2)
    },
    boat1_partner_search_applied: mainHeadLane === 1 ? 1 : 0,
    stronger_lap_bias_applied: 1,
    inside_baseline_priority_applied: 1,
    candidate_balance_adjustment_json: {
      first_place_distribution_top: firstPlaceDistribution.slice(0, 4),
      second_place_distribution_top: effectiveSecondPlaceDistribution.slice(0, 4),
      third_place_distribution_top: effectiveThirdPlaceDistribution.slice(0, 4),
      boat1_second_place_distribution_top: boat1SecondPlaceDistribution.slice(0, 4),
      boat1_third_place_distribution_top: boat1ThirdPlaceDistribution.slice(0, 4)
      ,
      outside_head_promotion_context: {
        inner_collapse_score: outsideHeadPromotionContext.inner_collapse_score,
        boat1_escape_probability_proxy: outsideHeadPromotionContext.boat1_escape_probability_proxy
      },
      boat3_head_suppression: boat3WeakStHeadSuppression
    },
    f_holder_role_penalty_summary_json: fHolderRolePenaltySummary,
    boat1_partner_model_applied: mainHeadLane === 1 ? 1 : 0,
    boat1_escape_partner_version: boat1EscapePartnerVersion,
    hit_rate_focus_applied: 1,
    launch_state_thresholds_used_json: launchStateThresholdsUsed,
    launch_venue_calibration_json: launchVenueCalibration,
    launch_state_scores_json: launchStateScores,
    launch_state_labels_json: launchStateLabels,
    intermediate_development_events_json: intermediateDevelopmentEvents,
    race_scenario_probabilities_json: dictionaryAdjustedScenarioProbabilities,
    finish_probabilities_by_scenario_json: finishProbabilitiesByScenario,
    finish_override_strength_by_lane_json: finishOverrideStrengthByLane,
    scenario_based_order_candidates_json: scenarioBasedOrderCandidates,
    matched_dictionary_scenarios_json: matchedDictionaryScenarios.slice(0, 8),
    dictionary_scenario_match_scores_json: matchedDictionaryScenarios.map((entry) => ({
      scenario_name: entry.scenario_name,
      development_category: entry.development_category,
      priority_rank: entry.priority_rank,
      match_score: entry.match_score
    })),
    dictionary_prior_adjustment_json: {
      activated_scenario_names: dictionaryAdjustedOrderCandidates.activated_scenario_names,
      activated_priority_ranks: [...new Set(
        matchedDictionaryScenarios
          .filter((entry) => toInt(entry?.activated, 0) === 1)
          .map((entry) => entry.priority_rank)
      )],
      representative_ticket_seed_count: matchedDictionaryScenarios
        .slice(0, 8)
        .reduce((sum, entry) => sum + safeArray(entry?.representative_tickets).flatMap(expandDictionaryTicketPattern).length, 0),
      backup_ticket_seed_count: matchedDictionaryScenarios
        .slice(0, 8)
        .reduce((sum, entry) => sum + safeArray(entry?.backup_tickets).flatMap(expandDictionaryTicketPattern).length, 0)
    },
    dictionary_condition_flags_json: matchedDictionaryScenarios.slice(0, 8).map((entry) => ({
      scenario_name: entry.scenario_name,
      success_conditions_satisfied: entry.success_conditions_satisfied,
      rejection_conditions_triggered: entry.rejection_conditions_triggered,
      activated: entry.activated
    })),
    dictionary_representative_ticket_priors_json: matchedDictionaryScenarios.slice(0, 5).map((entry) => ({
      scenario_name: entry.scenario_name,
      priority_rank: entry.priority_rank,
      representative_tickets: safeArray(entry?.representative_tickets).flatMap(expandDictionaryTicketPattern).slice(0, 8),
      backup_tickets: safeArray(entry?.backup_tickets).flatMap(expandDictionaryTicketPattern).slice(0, 8)
    })),
    dictionary_cd_scenarios_activated: matchedDictionaryScenarios.some(
      (entry) => ["C", "D"].includes(String(entry?.priority_rank || "").toUpperCase()) && toInt(entry?.activated, 0) === 1
    ) ? 1 : 0,
    outside_head_promotion_gate_json: {
      inner_collapse_score: outsideHeadPromotionContext.inner_collapse_score,
      boat1_escape_probability_proxy: outsideHeadPromotionContext.boat1_escape_probability_proxy,
      role_contribution: outsideHeadPromotionContext.role_contribution,
      by_lane: Object.fromEntries(
        [...outsideHeadPromotionContext.by_lane.entries()].map(([lane, value]) => [String(lane), value])
      )
    },
    boat3_weak_st_head_suppression_json: boat3WeakStHeadSuppression,
    boat3_weak_st_head_suppressed: toInt(boat3WeakStHeadSuppression?.applied, 0),
    ...roleProbabilityLayers,
    scoring_family_components_json: {
      first_place: {
        formation_prior_weight: 42,
        ranking_weight: 0.56,
        head_distribution_weight: 34,
        exhibition_weight: 3.6,
        lap_relative_weight: 18,
        motor_weight: 1.15,
        st_weight: 2.8
      },
      second_place: {
        existing_second_distribution_weight: 42,
        ranking_weight: 0.34,
        escape_partner_bias_weight: 1.9,
        lap_relative_weight: 16,
        inside_partner_bias: { 2: 8, 3: 7, 4: 4.2 }
      },
      third_place: {
        residual_ticket_weight: 48,
        ranking_weight: 0.18,
        lap_relative_weight: 10,
        inside_residual_bias: { 2: 4.5, 3: 4.1, 4: 2.8 }
      }
    },
    rebalance_version: "hit_rate_rebalance_v3"
  };
}

function applySeparatedDistributionBiasToTickets(tickets, candidateDistributions) {
  const rows = Array.isArray(tickets) ? tickets : [];
  if (!rows.length) return rows;
  const firstMap = new Map(
    safeArray(candidateDistributions?.first_place_probability_json || candidateDistributions?.first_place_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
  );
  const secondMap = new Map(
    safeArray(candidateDistributions?.second_place_probability_json || candidateDistributions?.second_place_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
  );
  const thirdMap = new Map(
    safeArray(candidateDistributions?.third_place_probability_json || candidateDistributions?.third_place_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
  );
  const boat1SecondMap = new Map(
    safeArray(candidateDistributions?.boat1_second_place_probability_json || candidateDistributions?.boat1_second_place_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
  );
  const boat1ThirdMap = new Map(
    safeArray(candidateDistributions?.boat1_third_place_probability_json || candidateDistributions?.boat1_third_place_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
  );
  const survivalMap = new Map(
    safeArray(candidateDistributions?.survival_probability_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
  );
  const outsideHeadGateByLane = candidateDistributions?.outside_head_promotion_gate_json?.by_lane || {};
  const boat1PartnerApplied = toInt(candidateDistributions?.boat1_partner_search_applied, 0) === 1;
  const boat1EscapeProbability = toNum(candidateDistributions?.boat1_escape_probability, 0);
  return [...rows]
    .map((row) => {
      const combo = normalizeCombo(row?.combo);
      const lanes = combo ? combo.split("-").map((value) => toInt(value, null)).filter(Number.isInteger) : [];
      const [headLane, secondLane, thirdLane] = lanes;
      const firstBias = toNum(firstMap.get(headLane), 0) * 0.028;
      const secondSourceMap = boat1PartnerApplied && headLane === 1 && boat1SecondMap.size > 0 ? boat1SecondMap : secondMap;
      const thirdSourceMap = boat1PartnerApplied && headLane === 1 && boat1ThirdMap.size > 0 ? boat1ThirdMap : thirdMap;
      const secondBias = toNum(secondSourceMap.get(secondLane), 0) * 0.026;
      const thirdBias = toNum(thirdSourceMap.get(thirdLane), 0) * 0.015;
      const survivalBias = (toNum(survivalMap.get(secondLane), 0) + toNum(survivalMap.get(thirdLane), 0)) * 0.008;
      const outsideHeadGate = outsideHeadGateByLane[String(headLane)] || null;
      const boat1PartnerBonus =
        boat1PartnerApplied && headLane === 1 && (secondLane === 2 || secondLane === 3 || secondLane === 4)
          ? (secondLane === 2 ? 0.012 : secondLane === 3 ? 0.0105 : 0.0088)
          : 0;
      const boat1EscapeBonus =
        headLane === 1 && boat1EscapeProbability > 0
          ? clamp(0, 0.02, boat1EscapeProbability * 0.018)
          : 0;
      const outerHeadPenalty = headLane === 5 ? 0.005 : headLane === 6 ? 0.009 : 0;
      const outsideGatePenalty =
        headLane === 5 || headLane === 6
          ? outsideHeadGate?.blocked_by_gate
            ? 0.03
            : outsideHeadGate?.allowed_as_counter_only
              ? 0.012
              : 0
          : 0;
      const totalBias = Number((firstBias + secondBias + thirdBias + survivalBias + boat1PartnerBonus + boat1EscapeBonus - outerHeadPenalty - outsideGatePenalty).toFixed(4));
      return {
        ...row,
        prob: Number((toNum(row?.prob, 0) + totalBias).toFixed(4)),
        separated_distribution_bias_score: totalBias
      };
    })
    .sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0));
}

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

const HIT_RATE_FOCUS_TUNING = {
  inner_lane_base_bias: {
    1: 5.3,
    2: 3.9,
    3: 2.8,
    4: 0.25,
    5: -1.4,
    6: -2.4
  },
  inner_lane_quality_scale: 0.28,
  expected_st_scale: 2.45,
  escape_survival_bonus: 3.4,
  boat2_counter_bonus: 2.4,
  boat3_counter_bonus: 1.85,
  outer_head_soft_threshold: {
    5: 78,
    6: 84
  },
  outer_head_soft_penalty_scale: 0.13
};

function computePreAttackOuterEvidenceScore(row, escapePatternAnalysis) {
  const lane = toInt(row?.racer?.lane, null);
  if (lane !== 5 && lane !== 6) return 100;
  const f = row?.features || {};
  const exhibitionSupport =
    Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 8 +
    Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * 100 +
    toNum(f?.slit_alert_flag, 0) * 14;
  const motorSupport =
    toNum(f?.motor_total_score, 0) * 2.6 +
    toNum(f?.motor_trend_score, 0) * 0.9;
  const startSupport =
    toNum(f?.expected_actual_st_inv, 0) * 22 +
    Math.max(0, 7 - toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6)) * 5;
  const formationSupport =
    ["dash_lead", "outside_lead", "two_three_delayed"].includes(String(escapePatternAnalysis?.formation_pattern || ""))
      ? 8
      : 0;
  const fHoldPenalty = toNum(f?.f_hold_caution_penalty, 0) * 10;
  return clamp(0, 100, exhibitionSupport + motorSupport + startSupport + formationSupport - fHoldPenalty);
}

function buildInnerCollapseScore({
  rows,
  headScenarioBalanceAnalysis,
  escapePatternAnalysis,
  attackScenarioAnalysis
}) {
  const lane1 = safeArray(rows).find((row) => toInt(row?.racer?.lane, null) === 1) || null;
  const lane2 = safeArray(rows).find((row) => toInt(row?.racer?.lane, null) === 2) || null;
  const lane3 = safeArray(rows).find((row) => toInt(row?.racer?.lane, null) === 3) || null;
  const lane4 = safeArray(rows).find((row) => toInt(row?.racer?.lane, null) === 4) || null;
  const boat1HeadWeight = toNum(
    safeArray(headScenarioBalanceAnalysis?.head_distribution_json).find((row) => toInt(row?.lane, null) === 1)?.weight,
    0
  );
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const lane1Feature = lane1?.features || {};
  const lane2Feature = lane2?.features || {};
  const lane3Feature = lane3?.features || {};
  const lane4Feature = lane4?.features || {};
  const lowBoat1EscapeSignal =
    Math.max(0, 0.46 - boat1HeadWeight) * 110 +
    Math.max(0, 34 - survivalResidualScore) * 0.9 +
    Math.max(0, toNum(lane1Feature?.expected_actual_st_rank ?? lane1Feature?.st_rank, 6) - 2) * 8 +
    Math.max(0, 9.2 - toNum(lane1Feature?.motor_total_score, 0)) * 2.2;
  const weakBoat2WallSignal =
    Math.max(0, toNum(lane2Feature?.expected_actual_st_rank ?? lane2Feature?.st_rank, 6) - 3) * 8 +
    Math.max(0, 8.8 - toNum(lane2Feature?.motor_total_score, 0)) * 2.1 +
    toNum(lane2Feature?.f_hold_caution_penalty, 0) * 6;
  const innerPoorExhibitionSignal =
    Math.max(0, toNum(lane1Feature?.exhibition_rank, 6) - 2) * 6 +
    Math.max(0, toNum(lane2Feature?.exhibition_rank, 6) - 3) * 4 +
    Math.max(0, toNum(lane3Feature?.exhibition_rank, 6) - 4) * 3;
  const attackPressureSignal =
    Math.max(
      toNum(lane3Feature?.lap_attack_strength, 0),
      toNum(lane4Feature?.lap_attack_strength, 0),
      toNum(attackScenarioAnalysis?.three_makuri_score, 0),
      toNum(attackScenarioAnalysis?.three_makuri_sashi_score, 0),
      toNum(attackScenarioAnalysis?.four_cado_makuri_score, 0),
      toNum(attackScenarioAnalysis?.four_cado_makuri_sashi_score, 0)
    ) * 0.9;
  const outsideLeadShapeBoost = String(escapePatternAnalysis?.formation_pattern || "") === "outside_lead" ? 10 : 0;
  return Number(
    clamp(
      0,
      100,
      lowBoat1EscapeSignal +
        weakBoat2WallSignal +
        innerPoorExhibitionSignal +
        attackPressureSignal +
        outsideLeadShapeBoost
    ).toFixed(2)
  );
}

function buildOutsideHeadPromotionContext({
  rows,
  race,
  learningWeights,
  escapePatternAnalysis,
  attackScenarioAnalysis,
  headScenarioBalanceAnalysis
}) {
  const innerCollapseScore = buildInnerCollapseScore({
    rows,
    headScenarioBalanceAnalysis,
    escapePatternAnalysis,
    attackScenarioAnalysis
  });
  const boat1EscapeProbabilityProxy = clamp(
    0,
    1,
    toNum(
      safeArray(headScenarioBalanceAnalysis?.head_distribution_json).find((row) => toInt(row?.lane, null) === 1)?.weight,
      0
    ) * 0.72 + Math.min(0.2, toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0) * 0.0044)
  );
  const scenarioLearningAdj = getSegmentCorrectionValue(
    learningWeights,
    "scenario_type",
    String(escapePatternAnalysis?.formation_pattern || ""),
    "recommendation_score_adjustment"
  );
  const venueOuterSuppressionAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "outer_head_suppression_adjustment"
  );
  const byLane = new Map();
  for (const row of safeArray(rows)) {
    const lane = toInt(row?.racer?.lane, null);
    if (lane !== 5 && lane !== 6) continue;
    const f = row?.features || {};
    const matchedCategories = [];
    if (
      ["outside_lead", "dash_lead", "two_three_delayed"].includes(String(escapePatternAnalysis?.formation_pattern || "")) &&
      (toNum(f?.entry_advantage_score, 0) >= 4 || toNum(f?.course_fit_score, 0) >= 1.2)
    ) {
      matchedCategories.push("entry_shape_advantage");
    }
    if (
      toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6) <= 2 &&
      (toNum(f?.avg_st_rank_delta_vs_left, 0) >= 1 || toNum(f?.slit_alert_flag, 0) === 1)
    ) {
      matchedCategories.push("clear_exhibition_st_advantage");
    }
    if (
      toNum(f?.lap_attack_strength, 0) >= 8 ||
      (toNum(f?.lap_time_delta_vs_front, 0) >= 0.055 && toNum(f?.exhibition_rank, 6) <= 2)
    ) {
      matchedCategories.push("lap_exhibition_advantage");
    }
    if (toNum(f?.motor_total_score, 0) >= 10 || toNum(f?.motor_trend_score, 0) >= 2.8) {
      matchedCategories.push("strong_motor");
    }
    if (venueOuterSuppressionAdj <= -0.5 || scenarioLearningAdj >= 1.2) {
      matchedCategories.push("venue_tendency_match");
    }
    if (innerCollapseScore >= 58) {
      matchedCategories.push("inner_collapse_evidence");
    }
    if (scenarioLearningAdj >= 1.6 || toNum(f?.outer_head_support_score, 0) >= 78) {
      matchedCategories.push("learning_correction_match");
    }

    const evidenceCount = matchedCategories.length;
    const strongInnerCollapse = innerCollapseScore >= 62;
    const blockedByBoat1Strength = boat1EscapeProbabilityProxy >= 0.52 && (!strongInnerCollapse || evidenceCount < 4);
    const allowedAsMainHead = evidenceCount >= 4 && strongInnerCollapse && !blockedByBoat1Strength;
    const counterOnly = !allowedAsMainHead && evidenceCount >= 3;
    const blocked = !allowedAsMainHead && !counterOnly;
    const firstPlaceCapWeight = allowedAsMainHead ? 0.26 : counterOnly ? (lane === 5 ? 0.13 : 0.11) : (lane === 5 ? 0.075 : 0.06);
    byLane.set(lane, {
      lane,
      matched_evidence_categories: matchedCategories,
      evidence_count: evidenceCount,
      inner_collapse_score: innerCollapseScore,
      strong_inner_collapse: strongInnerCollapse ? 1 : 0,
      blocked_by_gate: blocked ? 1 : 0,
      allowed_as_counter_only: counterOnly ? 1 : 0,
      allowed_as_main_head: allowedAsMainHead ? 1 : 0,
      blocked_by_boat1_escape: blockedByBoat1Strength ? 1 : 0,
      first_place_cap_weight: firstPlaceCapWeight
    });
  }
  return {
    inner_collapse_score: innerCollapseScore,
    boat1_escape_probability_proxy: Number(boat1EscapeProbabilityProxy.toFixed(4)),
    role_contribution: {
      attack: 1,
      first: 0.24,
      second: 0.62,
      third: 0.44
    },
    by_lane: byLane
  };
}

function applyHitRateFocusToRanking(ranking, escapePatternAnalysis) {
  const rows = Array.isArray(ranking) ? ranking : [];
  return [...rows]
    .map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      const f = row?.features || {};
      const baseBias = Number.isFinite(Number(HIT_RATE_FOCUS_TUNING.inner_lane_base_bias?.[lane]))
        ? Number(HIT_RATE_FOCUS_TUNING.inner_lane_base_bias[lane])
        : 0;
      const qualityBias =
        Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * HIT_RATE_FOCUS_TUNING.inner_lane_quality_scale +
        toNum(f?.motor_total_score, 0) * 0.12 +
        toNum(f?.expected_actual_st_inv, 0) * HIT_RATE_FOCUS_TUNING.expected_st_scale;
      const escapeBonus =
        lane === 1 && escapePatternAnalysis?.escape_pattern_applied
          ? HIT_RATE_FOCUS_TUNING.escape_survival_bonus
          : 0;
      const counterBonus =
        lane === 2
          ? HIT_RATE_FOCUS_TUNING.boat2_counter_bonus
          : lane === 3
            ? HIT_RATE_FOCUS_TUNING.boat3_counter_bonus
            : 0;
      const outerEvidenceScore = computePreAttackOuterEvidenceScore(row, escapePatternAnalysis);
      const outerThreshold = Number.isFinite(Number(HIT_RATE_FOCUS_TUNING.outer_head_soft_threshold?.[lane]))
        ? Number(HIT_RATE_FOCUS_TUNING.outer_head_soft_threshold[lane])
        : 0;
      const outerPenalty =
        lane === 5 || lane === 6
          ? Math.max(0, outerThreshold - outerEvidenceScore) * HIT_RATE_FOCUS_TUNING.outer_head_soft_penalty_scale
          : 0;
      const appliedBias = Number((baseBias + qualityBias + escapeBonus + counterBonus - outerPenalty).toFixed(4));
      return {
        ...row,
        score: Number((toNum(row?.score, 0) + appliedBias).toFixed(4)),
        features: {
          ...(row?.features || {}),
          inner_course_bias_applied: 1,
          hit_rate_focus_applied: 1,
          inner_course_bias_score: appliedBias,
          outer_head_soft_penalty: Number(outerPenalty.toFixed(4)),
          pre_attack_outer_evidence_score: Number(outerEvidenceScore.toFixed(2))
        }
      };
    })
    .sort((a, b) => toNum(b?.score, 0) - toNum(a?.score, 0))
    .map((row, idx) => ({
      ...row,
      rank: idx + 1
    }));
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
        Math.max(0, toNum(lane3?.features?.lap_attack_strength, 0)) * 0.2 +
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
        Math.max(0, toNum(lane3?.features?.lap_attack_strength, 0)) * 0.16 +
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
        Math.max(0, toNum(lane4?.features?.lap_attack_strength, 0)) * 0.22 +
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
        Math.max(0, toNum(lane4?.features?.lap_attack_strength, 0)) * 0.18 +
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
  const multiplier = clamp(0.22, 0.68, toNum(attackScenarioAnalysis?.attack_scenario_score, 0) / 96);
  return [...rows]
    .map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      const baseLaneBias = Number.isFinite(Number(config.lane_bias?.[lane])) ? Number(config.lane_bias[lane]) : 0;
      const partnerBias = Number.isFinite(Number(config.partner_bias?.[lane])) ? Number(config.partner_bias[lane]) : 0;
      const outsideHeadCap = lane === 5 || lane === 6 ? 0.42 : lane === 4 ? 0.72 : 1;
      const appliedBias = Number(((baseLaneBias * multiplier + partnerBias * multiplier * 0.45) * outsideHeadCap).toFixed(2));
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
  const scoreFactor = clamp(0.12, 0.62, toNum(attackScenarioAnalysis?.attack_scenario_score, 0) / 96);
  return [...rows]
    .map((row) => {
      const combo = normalizeCombo(row?.combo);
      const lanes = combo ? combo.split("-").map((n) => toInt(n, null)).filter(Number.isInteger) : [];
      const headLane = lanes[0];
      const outsideHeadCap = headLane === 5 || headLane === 6 ? 0.26 : headLane === 4 ? 0.58 : 1;
      const headBonus = targets.head.includes(headLane) ? 0.011 * scoreFactor * outsideHeadCap : 0;
      const partnerBonus = targets.partner.includes(lanes[1]) ? 0.007 * scoreFactor : 0;
      const thirdBonus = targets.partner.includes(lanes[2]) ? 0.004 * scoreFactor : 0;
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

function buildHeadDistributionFromRanking(ranking, defaultRole = "counter") {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!rows.length) return [];
  const weights = rows
    .slice(0, 4)
    .map((row, idx) => ({
      lane: toInt(row?.racer?.lane, null),
      role: idx === 0 ? "main" : defaultRole,
      weight: Math.max(0.05, toNum(row?.score, 0))
    }))
    .filter((row) => Number.isInteger(row.lane));
  const total = weights.reduce((sum, row) => sum + toNum(row.weight, 0), 0) || 1;
  return weights
    .map((row) => ({
      lane: row.lane,
      role: row.role,
      weight: Number((row.weight / total).toFixed(4))
    }))
    .sort((a, b) => b.weight - a.weight);
}

function buildSecondDistributionFromTickets(tickets) {
  const rows = Array.isArray(tickets) ? tickets : [];
  const weights = new Map();
  for (const row of rows) {
    const combo = normalizeCombo(row?.combo);
    const lanes = combo ? combo.split("-").map((value) => toInt(value, null)).filter(Number.isInteger) : [];
    const secondLane = lanes[1];
    if (!Number.isInteger(secondLane)) continue;
    weights.set(secondLane, toNum(weights.get(secondLane), 0) + Math.max(0.001, toNum(row?.prob, 0)));
  }
  const total = [...weights.values()].reduce((sum, value) => sum + toNum(value, 0), 0) || 1;
  return [...weights.entries()]
    .map(([lane, weight]) => ({
      lane,
      weight: Number((weight / total).toFixed(4))
    }))
    .sort((a, b) => b.weight - a.weight);
}

function buildFinalBalanceAdjustmentSummary({ ranking, recommendedBets, headScenarioBalanceAnalysis }) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const tickets = Array.isArray(recommendedBets) ? recommendedBets : [];
  const topHeads = tickets
    .slice(0, 6)
    .map((row) => normalizeCombo(row?.combo))
    .map((combo) => toInt(String(combo || "").split("-")[0], null))
    .filter(Number.isInteger);
  const uniqueTopHeads = [...new Set(topHeads)];
  return {
    hit_rate_focus_applied: 1,
    stronger_inner_bias_applied: 1,
    exacta_inside_bias_applied: 1,
    inner_course_bias_applied: rows.some((row) => toInt(row?.features?.inner_course_bias_applied, 0) === 1) ? 1 : 0,
    outer_head_guard_applied: toInt(headScenarioBalanceAnalysis?.outer_head_guard_applied, 0),
    survival_guard_applied: toInt(headScenarioBalanceAnalysis?.survival_guard_applied, 0),
    formation_first_place_prior_json: safeArray(headScenarioBalanceAnalysis?.formation_first_place_prior_json),
    first_place_distribution_json: safeArray(headScenarioBalanceAnalysis?.first_place_distribution_json),
    second_place_distribution_json: safeArray(headScenarioBalanceAnalysis?.second_place_distribution_json),
    third_place_distribution_json: safeArray(headScenarioBalanceAnalysis?.third_place_distribution_json),
    boat1_survival_guard_strength: toNum(headScenarioBalanceAnalysis?.boat1_survival_guard_strength, null),
    outer_head_promotion_threshold: toNum(headScenarioBalanceAnalysis?.outer_head_promotion_threshold, null),
    top_head_count: uniqueTopHeads.length,
    top_heads: uniqueTopHeads,
    main_head_lane: toInt(headScenarioBalanceAnalysis?.main_head_lane, null),
    counter_head_lane: toInt(headScenarioBalanceAnalysis?.second_head_lane, null),
    survival_head_lane: toInt(headScenarioBalanceAnalysis?.survival_head_lane, null)
  };
}

function computeOuterHeadSupportScore(row, attackScenarioAnalysis, escapePatternAnalysis) {
  const lane = toInt(row?.racer?.lane, null);
  if (lane !== 5 && lane !== 6) return 100;
  const f = row?.features || {};
  const exhibitionSupport =
    Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 9 +
    Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * 120 +
    toNum(f?.slit_alert_flag, 0) * 16;
  const motorSupport =
    toNum(f?.motor_total_score, 0) * 3 +
    toNum(f?.motor_trend_score, 0) * 1.2;
  const startSupport =
    toNum(f?.expected_actual_st_inv, 0) * 28 +
    Math.max(0, 7 - toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6)) * 6;
  const wallSupport = toNum(attackScenarioAnalysis?.evidence?.wall_weakness, 0) * 0.26;
  const collapseSupport = toNum(attackScenarioAnalysis?.evidence?.partial_collapse, 0) * 0.18;
  const formationSupport =
    ["dash_lead", "outside_lead", "two_three_delayed"].includes(String(escapePatternAnalysis?.formation_pattern || ""))
      ? 10
      : 0;
  const fHoldPenalty = toNum(f?.f_hold_caution_penalty, 0) * 12;
  return clamp(
    0,
    100,
    exhibitionSupport + motorSupport + startSupport + wallSupport + collapseSupport + formationSupport - fHoldPenalty
  );
}

function applyHeadDistributionGuardToRanking({
  ranking,
  baselineRanking,
  attackScenarioAnalysis,
  escapePatternAnalysis
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const baselineRows = Array.isArray(baselineRanking) ? baselineRanking : rows;
  const baselineScoreByLane = new Map(
    baselineRows
      .map((row) => [toInt(row?.racer?.lane, null), toNum(row?.score, 0)])
      .filter(([lane]) => Number.isInteger(lane))
  );

  return [...rows]
    .map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      const baselineScore = Number(
        (
          baselineScoreByLane.get(lane) ??
          (toNum(row?.score, 0) - toNum(row?.features?.attack_scenario_bias_score, 0))
        ).toFixed(4)
      );
      const rawAggressiveAdjustment = Number((toNum(row?.score, 0) - baselineScore).toFixed(4));
      const outerHeadSupportScore = computeOuterHeadSupportScore(row, attackScenarioAnalysis, escapePatternAnalysis);
      const isOuterHead = lane === 5 || lane === 6;
      const allowedPositiveAdjustment = isOuterHead
        ? outerHeadSupportScore >= 78
          ? 2.8
          : outerHeadSupportScore >= 70
            ? 1.8
            : outerHeadSupportScore >= 64
              ? 0.9
              : 0.25
        : 3.6;
      const boundedAggressiveAdjustment = clamp(-2.2, allowedPositiveAdjustment, rawAggressiveAdjustment);
      const outerHeadGuardApplied = isOuterHead && rawAggressiveAdjustment > allowedPositiveAdjustment + 0.05 ? 1 : 0;
      const softPenalty = isOuterHead && outerHeadSupportScore < 64
        ? Math.min(2.2, (64 - outerHeadSupportScore) * 0.06)
        : 0;
      const guardedScore = Number((baselineScore + boundedAggressiveAdjustment - softPenalty).toFixed(4));
      return {
        ...row,
        score: guardedScore,
        features: {
          ...(row?.features || {}),
          baseline_head_score: baselineScore,
          aggressive_head_adjustment: Number(boundedAggressiveAdjustment.toFixed(4)),
          raw_aggressive_head_adjustment: rawAggressiveAdjustment,
          outer_head_support_score: Number(outerHeadSupportScore.toFixed(2)),
          outer_head_guard_applied: outerHeadGuardApplied
        }
      };
    })
    .sort((a, b) => toNum(b?.score, 0) - toNum(a?.score, 0))
    .map((row, idx) => ({
      ...row,
      rank: idx + 1
    }));
}

function buildHeadScenarioBalanceAnalysis({
  ranking,
  baselineRanking,
  raceFlow,
  headSelection,
  attackScenarioAnalysis,
  escapePatternAnalysis,
  learningWeights,
  race
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const baselineRows = Array.isArray(baselineRanking) ? baselineRanking : rows;
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
  const strongerInnerBiasApplied = 1;
  const venueBoat1SurvivalAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "boat1_survival_guard_adjustment"
  );
  const venueOuterSuppressionAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "outer_head_suppression_adjustment"
  );
  const outerHeadPromotionThreshold = Math.max(
    toNum(HIT_RATE_FOCUS_TUNING.outer_head_soft_threshold?.[5], 0),
    toNum(HIT_RATE_FOCUS_TUNING.outer_head_soft_threshold?.[6], 0)
  ) + venueOuterSuppressionAdj * 1.8;
  const survivalResidualScore = clamp(
    0,
    100,
    nigeProbPct * 0.76 +
      escapeConfidence * 0.34 +
      lane1Support -
      boat1Weakness * 0.13 +
      venueBoat1SurvivalAdj * 32
  );
  const attackDominanceMargin = attackScenarioScore - survivalResidualScore;
  const scenarioCandidates = Array.isArray(attackScenarioAnalysis?.scenario_candidates)
    ? attackScenarioAnalysis.scenario_candidates
    : [];
  const counterAttackType = scenarioCandidates.find((candidate) => candidate?.type && candidate.type !== attackType)?.type || null;
  const baselineHeadDistributionJson = buildHeadDistributionFromRanking(baselineRows);
  const secondDistributionJson = buildSecondDistributionFromTickets(
    rows.flatMap((row) => {
      const lane = toInt(row?.racer?.lane, null);
      if (!Number.isInteger(lane)) return [];
      return [
        { combo: `1-${lane}-2`, prob: Math.max(0.001, toNum(row?.score, 0) / 1000) },
        { combo: `2-${lane}-1`, prob: Math.max(0.001, toNum(row?.score, 0) / 1200) }
      ];
    })
  );
  const aggressiveAdjustmentJson = rows
    .map((row) => ({
      lane: toInt(row?.racer?.lane, null),
      baseline_score: toNum(row?.features?.baseline_head_score, null),
      aggressive_adjustment: toNum(row?.features?.aggressive_head_adjustment, 0),
      inner_course_bias_score: toNum(row?.features?.inner_course_bias_score, 0),
      outer_head_support_score: toNum(row?.features?.outer_head_support_score, null),
      outer_head_guard_applied: toInt(row?.features?.outer_head_guard_applied, 0)
    }))
    .filter((row) => Number.isInteger(row.lane))
    .sort((a, b) => Math.abs(toNum(b.aggressive_adjustment, 0)) - Math.abs(toNum(a.aggressive_adjustment, 0)));
  const outerHeadGuardApplied = rows.some((row) => toInt(row?.features?.outer_head_guard_applied, 0) === 1);
  const survivalGuardApplied =
    !!attackHeadLane &&
    attackHeadLane !== 1 &&
    survivalResidualScore >= 32 &&
    nigeProbPct >= 16 &&
    attackScenarioScore >= 56 &&
    attackDominanceMargin <= 30 &&
    attackScenarioScore < 84;
  const boat1SurvivalGuardStrength = survivalGuardApplied
    ? Number((1.15 + Math.max(0, survivalResidualScore - 34) * 0.02 + venueBoat1SurvivalAdj * 0.4).toFixed(2))
    : Number((Math.max(0.85, survivalResidualScore / 50 + venueBoat1SurvivalAdj * 0.24)).toFixed(2));

  const headWeightsRaw = {};
  const baselineTop = baselineHeadDistributionJson[0] || null;
  if (baselineTop?.lane) {
    headWeightsRaw[String(baselineTop.lane)] = Math.max(0.12, toNum(baselineTop.weight, 0));
  }
  if (attackHeadLane) {
    headWeightsRaw[String(attackHeadLane)] = Math.max(
      headWeightsRaw[String(attackHeadLane)] || 0,
      Math.max(0.08, Math.min(0.38, attackScenarioScore / 160))
    );
  }
  headWeightsRaw["1"] = Math.max(0.06, survivalResidualScore / 100);
  const guardedTopRows = rows.slice(0, 3);
  for (const row of guardedTopRows) {
    const lane = toInt(row?.racer?.lane, null);
    if (!Number.isInteger(lane)) continue;
    headWeightsRaw[String(lane)] = Math.max(headWeightsRaw[String(lane)] || 0, Math.max(0.08, toNum(row?.score, 0) / 100));
  }
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
  if (outerHeadGuardApplied) removedCandidateReasonTags.push("OUTER_HEAD_GUARD");

  return {
    main_head_lane: toInt(headDistributionJson[0]?.lane, toInt(headSelection?.main_head, null)),
    second_head_lane: toInt(headDistributionJson[1]?.lane, secondaryHeads[0] ?? null),
    third_head_lane: toInt(headDistributionJson[2]?.lane, secondaryHeads[1] ?? null),
    main_head_candidate_score: Number(toNum(headDistributionJson[0]?.weight, 0).toFixed(4)),
    second_head_candidate_score: Number(toNum(headDistributionJson[1]?.weight, 0).toFixed(4)),
    third_head_candidate_score: Number(toNum(headDistributionJson[2]?.weight, 0).toFixed(4)),
    main_scenario_type: attackHeadLane
      ? attackType
      : toInt(headSelection?.main_head, null) === 1
        ? "one_head_survival"
        : null,
    counter_scenario_type: survivalGuardApplied
      ? "one_head_survival"
      : counterAttackType,
    survival_scenario_type: survivalResidualScore >= 28 ? "one_head_survival" : null,
    attack_head_lane: attackHeadLane,
    survival_head_lane: 1,
    survival_residual_score: Number(survivalResidualScore.toFixed(2)),
    attack_dominance_margin: Number(attackDominanceMargin.toFixed(2)),
    baseline_head_distribution_json: baselineHeadDistributionJson,
    second_distribution_json: secondDistributionJson,
    aggressive_adjustment_json: aggressiveAdjustmentJson,
    head_distribution_json: headDistributionJson,
    stronger_inner_bias_applied: strongerInnerBiasApplied,
    boat1_survival_guard_strength: boat1SurvivalGuardStrength,
    outer_head_promotion_threshold: outerHeadPromotionThreshold,
    outer_head_guard_applied: outerHeadGuardApplied ? 1 : 0,
    survival_guard_applied: survivalGuardApplied ? 1 : 0,
    removed_candidate_reason_tags: removedCandidateReasonTags
  };
}

function applyHeadScenarioBalanceToTickets(tickets, headScenarioBalanceAnalysis) {
  const rows = Array.isArray(tickets) ? tickets : [];
  if (!rows.length) return rows;

  const attackHeadLane = toInt(headScenarioBalanceAnalysis?.attack_head_lane, null);
  const mainHeadLane = toInt(headScenarioBalanceAnalysis?.main_head_lane, null);
  const counterHeadLane = toInt(headScenarioBalanceAnalysis?.second_head_lane, null);
  const thirdHeadLane = toInt(headScenarioBalanceAnalysis?.third_head_lane, null);
  const survivalGuardApplied = toNum(headScenarioBalanceAnalysis?.survival_guard_applied, 0) === 1;
  const outerHeadGuardApplied = toNum(headScenarioBalanceAnalysis?.outer_head_guard_applied, 0) === 1;
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const attackDominanceMargin = toNum(headScenarioBalanceAnalysis?.attack_dominance_margin, 99);
  const distributionMap = new Map(
    safeArray(headScenarioBalanceAnalysis?.head_distribution_json)
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const aggressiveAdjustmentMap = new Map(
    safeArray(headScenarioBalanceAnalysis?.aggressive_adjustment_json)
      .map((row) => [toInt(row?.lane, null), row])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const adjusted = rows.map((row) => {
    const combo = normalizeCombo(row?.combo);
    const lanes = combo
      ? combo.split("-").map((value) => toInt(value, null)).filter(Number.isInteger)
      : [];
    let bonus = 0;
    const balanceTags = Array.isArray(row?.scenario_balance_tags) ? [...row.scenario_balance_tags] : [];
    const headLane = lanes[0];
    const headWeight = toNum(distributionMap.get(headLane), 0);
    const aggressiveAdjustment = toNum(aggressiveAdjustmentMap.get(headLane)?.aggressive_adjustment, 0);
    const outerSupportScore = toNum(aggressiveAdjustmentMap.get(headLane)?.outer_head_support_score, 100);
    bonus += headWeight * 0.018;
    if (mainHeadLane && headLane === mainHeadLane) {
      bonus += 0.002;
      balanceTags.push("MAIN_HEAD_SCENARIO");
    }
    if (counterHeadLane && headLane === counterHeadLane) {
      bonus += 0.0045;
      balanceTags.push("COUNTER_HEAD_SCENARIO");
    }
    if (thirdHeadLane && headLane === thirdHeadLane) {
      bonus += 0.0025;
      balanceTags.push("THIRD_HEAD_SCENARIO");
    }
    if (survivalGuardApplied && lanes[0] === 1) {
      const survivalGuardStrength = toNum(headScenarioBalanceAnalysis?.boat1_survival_guard_strength, 1);
      bonus += (0.016 + Math.max(0, survivalResidualScore - 30) * 0.00042) * survivalGuardStrength;
      balanceTags.push("SURVIVAL_GUARD");
      if (attackHeadLane && lanes.slice(1).includes(attackHeadLane)) {
        bonus += 0.009;
        balanceTags.push("ATTACK_SURVIVAL_BALANCE");
      }
    } else if (attackHeadLane && lanes[0] === attackHeadLane) {
      bonus += attackDominanceMargin >= 14 ? 0.0025 : 0;
    }
    if (outerHeadGuardApplied && (headLane === 5 || headLane === 6)) {
      if (aggressiveAdjustment <= 1.7 || outerSupportScore < 80) {
        bonus -= 0.013;
        balanceTags.push("OUTER_HEAD_GUARD");
      }
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
  ranking,
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
  const rows = Array.isArray(ranking) ? ranking : [];
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const attackHeadLane = toInt(headScenarioBalanceAnalysis?.attack_head_lane, null);
  const shown = survivalResidualScore >= 18;
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
  const venuePartnerAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "second_place_partner_adjustment"
  );
  const venueLapAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "lap_weight_adjustment"
  );
  const venueFHoldAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "f_hold_caution_adjustment"
  );
  const boat1SecondDistribution = safeArray(headScenarioBalanceAnalysis?.boat1_second_place_distribution_json);
  const boat1ThirdDistribution = safeArray(headScenarioBalanceAnalysis?.boat1_third_place_distribution_json);
  const boat1SecondMap = new Map(
    boat1SecondDistribution.map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]).filter(([lane]) => Number.isInteger(lane))
  );
  const boat1ThirdMap = new Map(
    boat1ThirdDistribution.map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)]).filter(([lane]) => Number.isInteger(lane))
  );
  const partnerRows = rows
    .filter((row) => toInt(row?.racer?.lane, null) !== 1)
    .map((row) => {
      const lane = toInt(row?.racer?.lane, null);
      const f = row?.features || {};
    const partnerScore =
      toNum(row?.score, 0) * 0.62 +
      (escapePatternAnalysis?.escape_pattern_applied
          ? getEscapeSecondPlaceBiasScore(escapePatternAnalysis?.escape_second_place_bias_json || {}, lane) * 2.1
          : 0) +
        toNum(boat1SecondMap.get(lane), 0) * 62 +
        venuePartnerAdj * (lane >= 2 && lane <= 4 ? 1.2 : -0.5) +
        Math.max(0, 7 - toNum(f?.exhibition_rank, 6)) * 4.2 +
        toNum(f?.motor_total_score, 0) * 1.3 +
        Math.max(0, 7 - toNum(f?.expected_actual_st_rank ?? f?.st_rank, 6)) * 2.6 +
        Math.max(0, toNum(f?.display_time_delta_vs_left, 0)) * 22 +
        Math.max(0, toNum(f?.lap_time_delta_vs_front, 0)) * (16 + venueLapAdj * 16) +
        toNum(f?.slit_alert_flag, 0) * 6 -
        (lane === 5 ? 4.5 : lane === 6 ? 7.5 : 0) -
        toNum(f?.f_hold_caution_penalty, 0) * (7 + venueFHoldAdj * 0.6);
      return { lane, partnerScore: Number(partnerScore.toFixed(2)) };
    })
    .filter((row) => Number.isInteger(row.lane))
    .sort((a, b) => b.partnerScore - a.partnerScore);

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
      attackPartnerBonus > 0 ? "ATTACK_COUNTER_PARTNER" : null,
      ...safeArray(headScenarioBalanceAnalysis?.boat1_partner_reason_tags).slice(0, 4)
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

  const fallbackPartners = partnerRows.slice(0, 5);
  for (const second of fallbackPartners) {
    for (const third of fallbackPartners) {
      if (second.lane === third.lane) continue;
      const combo = `1-${second.lane}-${third.lane}`;
      const escapeBias = escapePatternAnalysis?.escape_pattern_applied
        ? getEscapeSecondPlaceBiasScore(escapePatternAnalysis?.escape_second_place_bias_json || {}, second.lane) * 0.32
        : 0;
      const attackPartnerBonus = attackHeadLane && third.lane === attackHeadLane ? 0.55 : 0;
      const compositeScore =
        survivalResidualScore +
        second.partnerScore * 0.68 +
        (third.partnerScore + toNum(boat1ThirdMap.get(third.lane), 0) * 38) * 0.34 +
        escapeBias +
        attackPartnerBonus +
        (learnedPatternAdj + learnedVenueAdj) * 0.35 +
        venuePartnerAdj * 0.6;
      const nextRow = {
        combo,
        prob: Number((compositeScore / 1000).toFixed(4)),
        recommended_bet: 100,
        boat1_head_score: Number(compositeScore.toFixed(2)),
        boat1_head_reason_tags: [
          "BOAT1_HEAD",
          "BOAT1_FALLBACK_GENERATED",
          survivalResidualScore >= 28 ? "SURVIVAL_RESIDUAL_ACTIVE" : null,
          escapePatternAnalysis?.escape_pattern_applied ? "ESCAPE_PATTERN_CONTEXT" : null,
          ...safeArray(headScenarioBalanceAnalysis?.boat1_partner_reason_tags).slice(0, 3)
        ].filter(Boolean)
      };
      const existing = bucket.get(combo);
      if (!existing || toNum(existing?.boat1_head_score, 0) < nextRow.boat1_head_score) {
        bucket.set(combo, nextRow);
      }
    }
  }

  const items = [...bucket.values()]
    .sort((a, b) => toNum(b?.boat1_head_score, 0) - toNum(a?.boat1_head_score, 0))
    .slice(0, 8)
    .map((row) => ({
      ...row,
      boat1_head_score: Number(toNum(row?.boat1_head_score, 0).toFixed(2))
    }));

  return {
    shown: (shown || toNum(items[0]?.boat1_head_score, 0) >= 24) && items.length > 0,
    boat1_head_score: items.length > 0 ? Number(toNum(items[0]?.boat1_head_score, survivalResidualScore).toFixed(2)) : Number(survivalResidualScore.toFixed(2)),
    boat1_survival_residual_score: Number(survivalResidualScore.toFixed(2)),
    boat1_head_reason_tags: [...new Set(items.flatMap((row) => safeArray(row?.boat1_head_reason_tags)).slice(0, 6))],
    boat1_head_top8_generated: items.length > 0 ? 1 : 0,
    items
  };
}

function ensureExplanationTagList(row, tag) {
  return [...new Set([...(Array.isArray(row?.explanation_tags) ? row.explanation_tags : []), tag])];
}

function applyBoat1PriorityModeToTickets({
  recommendedBets,
  optimizedTickets,
  boat1HeadSnapshot,
  headScenarioBalanceAnalysis
}) {
  const optimizeRows = normalizeSavedBetSnapshotItems(optimizedTickets);
  const recommendRows = normalizeSavedBetSnapshotItems(recommendedBets);
  const sourceRows = optimizeRows.length > 0 ? optimizeRows : recommendRows;
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const lane1Weight = toNum(
    safeArray(headScenarioBalanceAnalysis?.head_distribution_json).find((row) => toInt(row?.lane, null) === 1)?.weight,
    0
  );
  const attackDominanceMargin = toNum(headScenarioBalanceAnalysis?.attack_dominance_margin, 99);
  const shouldApply =
    sourceRows.length >= 3 &&
    safeArray(boat1HeadSnapshot?.items).length > 0 &&
    survivalResidualScore >= 28 &&
    lane1Weight >= 0.18 &&
    attackDominanceMargin <= 32;

  const rebalanceRows = (rows) => {
    const list = normalizeSavedBetSnapshotItems(rows);
    if (!shouldApply || !list.length) return list;
    const windowSize = Math.min(6, list.length);
    const requiredBoat1Count = Math.min(windowSize - 1, Math.max(2, Math.ceil(windowSize * 0.67)));
    const currentBoat1Count = list.slice(0, windowSize).filter((row) => normalizeCombo(row?.combo).startsWith("1-")).length;
      if (currentBoat1Count >= requiredBoat1Count) {
        return list.map((row) => ({
          ...row,
          explanation_tags: ensureExplanationTagList(row, "BOAT1_PRIORITY_MODE")
      }));
    }

    const promoted = [...list];
    const thresholdProb = toNum(promoted[Math.max(0, windowSize - 1)]?.prob, 0);
    const headCandidates = normalizeSavedBetSnapshotItems(boat1HeadSnapshot?.items)
      .filter((row) => normalizeCombo(row?.combo).startsWith("1-"))
      .sort((a, b) => toNum(b?.boat1_head_score ?? b?.prob, 0) - toNum(a?.boat1_head_score ?? a?.prob, 0));

    let nextProb = thresholdProb + 0.0006;
    for (const candidate of headCandidates) {
      const topBoat1Count = promoted.slice(0, windowSize).filter((row) => normalizeCombo(row?.combo).startsWith("1-")).length;
      if (topBoat1Count >= requiredBoat1Count) break;
      const combo = normalizeCombo(candidate?.combo);
      const idx = promoted.findIndex((row) => normalizeCombo(row?.combo) === combo);
      if (idx >= 0) {
        promoted[idx] = {
          ...promoted[idx],
          prob: Number((Math.max(toNum(promoted[idx]?.prob, 0), nextProb)).toFixed(4)),
          explanation_tags: ensureExplanationTagList(promoted[idx], "BOAT1_PRIORITY_MODE")
        };
      } else {
        promoted.push({
          ...candidate,
          combo,
          prob: Number(nextProb.toFixed(4)),
          explanation_tags: ensureExplanationTagList(candidate, "BOAT1_PRIORITY_MODE")
        });
      }
      nextProb += 0.0004;
      promoted.sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0));
    }

    return promoted.sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0));
  };

  const nextOptimized = optimizeRows.length > 0 ? rebalanceRows(optimizeRows) : [];
  const nextRecommended = rebalanceRows(recommendRows);
  const finalRows = nextOptimized.length > 0 ? nextOptimized : nextRecommended;
  const finalWindow = Math.min(6, finalRows.length);
  const boat1Count = finalRows.slice(0, finalWindow).filter((row) => normalizeCombo(row?.combo).startsWith("1-")).length;
  const ratio = finalWindow > 0 ? boat1Count / finalWindow : 0;
  return {
    recommendedBets: nextRecommended,
    optimizedTickets: nextOptimized,
    boat1_priority_mode_applied: shouldApply && ratio >= 0.65 ? 1 : 0,
    boat1_head_ratio_in_final_bets: Number(ratio.toFixed(4)),
    boat1_priority_reason_tags: shouldApply
      ? ["BOAT1_SURVIVAL_MEANINGFUL", "BOAT1_PRIORITY_MODE"]
      : []
  };
}

function buildExactaCoverageSnapshot({
  ranking,
  recommendedBets,
  optimizedTickets,
  finalRecommendedSnapshot,
  boat1HeadSnapshot,
  headScenarioBalanceAnalysis,
  roleProbabilityLayers,
  escapePatternAnalysis,
  attackScenarioAnalysis,
  learningWeights,
  race
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const attackScenarioType = attackScenarioAnalysis?.attack_scenario_type || null;
  const attackScenarioApplied = toNum(attackScenarioAnalysis?.attack_scenario_applied, 0) === 1;
  const attackScenarioScore = toNum(attackScenarioAnalysis?.attack_scenario_score, 0);
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const boat1PriorityModeApplied = toNum(headScenarioBalanceAnalysis?.boat1_priority_mode_applied, 0) === 1;
  const secondDistribution = Array.isArray(roleProbabilityLayers?.second_place_probability_json) &&
    roleProbabilityLayers.second_place_probability_json.length > 0
    ? roleProbabilityLayers.second_place_probability_json
    : Array.isArray(headScenarioBalanceAnalysis?.second_place_distribution_json) &&
      headScenarioBalanceAnalysis.second_place_distribution_json.length > 0
      ? headScenarioBalanceAnalysis.second_place_distribution_json
    : Array.isArray(headScenarioBalanceAnalysis?.second_distribution_json)
      ? headScenarioBalanceAnalysis.second_distribution_json
      : [];
  const firstDistribution = Array.isArray(roleProbabilityLayers?.first_place_probability_json) &&
    roleProbabilityLayers.first_place_probability_json.length > 0
    ? roleProbabilityLayers.first_place_probability_json
    : Array.isArray(headScenarioBalanceAnalysis?.first_place_distribution_json) &&
      headScenarioBalanceAnalysis.first_place_distribution_json.length > 0
      ? headScenarioBalanceAnalysis.first_place_distribution_json
    : [];
  const boat1SecondDistribution = Array.isArray(roleProbabilityLayers?.boat1_second_place_probability_json) &&
    roleProbabilityLayers.boat1_second_place_probability_json.length > 0
    ? roleProbabilityLayers.boat1_second_place_probability_json
    : Array.isArray(headScenarioBalanceAnalysis?.boat1_second_place_distribution_json) &&
      headScenarioBalanceAnalysis.boat1_second_place_distribution_json.length > 0
      ? headScenarioBalanceAnalysis.boat1_second_place_distribution_json
    : [];
  const secondDistributionMap = new Map(
    secondDistribution
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const boat1SecondDistributionMap = new Map(
    boat1SecondDistribution
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const headDistribution = firstDistribution.length > 0
    ? firstDistribution
    : Array.isArray(headScenarioBalanceAnalysis?.head_distribution_json)
      ? headScenarioBalanceAnalysis.head_distribution_json
      : [];
  const headDistributionMap = new Map(
    headDistribution
      .map((row) => [toInt(row?.lane, null), toNum(row?.weight, 0)])
      .filter(([lane]) => Number.isInteger(lane))
  );
  const mainHeadLane = toInt(roleProbabilityLayers?.role_probability_summary_json?.main_head_lane, null)
    ?? toInt(headScenarioBalanceAnalysis?.main_head_lane, null)
    ?? topDistributionLane(headDistribution);
  const boat1HeadLikely = mainHeadLane === 1;
  const boat1SecondConcentration = boat1SecondDistribution
    .slice(0, 2)
    .reduce((sum, row) => sum + toNum(row?.weight, 0), 0);
  const focusedBoat1PartnerLanes = boat1HeadLikely
    ? boat1SecondDistribution
        .slice(0, boat1SecondConcentration >= 0.66 ? 3 : 2)
        .map((row) => toInt(row?.lane, null))
        .filter(Number.isInteger)
    : [];
  const exactaEvidence = normalizeSavedBetSnapshotItems([
    ...safeArray(optimizedTickets),
    ...safeArray(recommendedBets),
    ...safeArray(finalRecommendedSnapshot?.items),
    ...safeArray(boat1HeadSnapshot?.items)
  ]);
  const exactaEvidenceMap = new Map();
  for (const row of exactaEvidence) {
    const combo = normalizeCombo(row?.combo);
    if (!combo) continue;
    const pair = normalizeExactaCombo(combo);
    if (!pair) continue;
    const current = exactaEvidenceMap.get(pair) || {
      prob: 0,
      recommended_bet: 0,
      explanation_tags: []
    };
    current.prob = Math.max(current.prob, toNum(row?.prob, 0));
    current.recommended_bet = Math.max(current.recommended_bet, toNum(row?.recommended_bet ?? row?.bet, 0));
    current.explanation_tags = [...new Set([...current.explanation_tags, ...safeArray(row?.explanation_tags)])];
    exactaEvidenceMap.set(pair, current);
  }

  const learnedVenueHeadAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "head_confidence_correction"
  );
  const learnedFormationHeadAdj = getSegmentCorrectionValue(
    learningWeights,
    "formation_pattern",
    escapePatternAnalysis?.formation_pattern || null,
    "pattern_strength_adjustment"
  );
  const learnedScenarioHeadAdj = getSegmentCorrectionValue(
    learningWeights,
    "scenario_type",
    attackScenarioType || null,
    "recommendation_score_adjustment"
  );
  const learnedFHoldAdj = getSegmentCorrectionValue(
    learningWeights,
    "has_f_hold",
    rowsHaveFHold(rows) ? "yes" : "no",
    "f_hold_penalty_adjustment"
  );
  const venuePartnerAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "second_place_partner_adjustment"
  );
  const venueThirdAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "third_place_residual_adjustment"
  );
  const venueOuterSuppressionAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "outer_head_suppression_adjustment"
  );
  const venueLapAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "lap_weight_adjustment"
  );
  const venueExactaAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "exacta_balance_adjustment"
  );
  const venueFHoldAdj = getSegmentCorrectionValue(
    learningWeights,
    "venue",
    toInt(race?.venueId, null),
    "f_hold_caution_adjustment"
  );

  const headScores = rows.map((row) => {
    const lane = toInt(row?.racer?.lane, null);
    const features = row?.features || {};
    const exhibitionRank = toNum(features?.exhibition_rank, 6);
    const exhibitionStrength = Math.max(0, 7 - exhibitionRank) * 9.5 + Math.max(0, 6.9 - toNum(row?.racer?.exhibitionTime, 6.9)) * 20;
    const lapFrontBias =
      Math.max(0, toNum(features?.lap_time_delta_vs_front, 0)) * (26 + venueLapAdj * 24) +
      toNum(features?.lap_attack_flag, 0) * 7 +
      toNum(features?.lap_attack_strength, 0) * (0.36 + venueLapAdj * 0.22);
    const motorStrength = toNum(features?.motor_total_score, 0) * 2.2 + toNum(features?.motor_trend_score, 0) * 0.7;
    const playerStrength =
      toNum(features?.class_score, 0) * 5.5 +
      toNum(features?.nationwide_win_rate, 0) * 1.6 +
      toNum(features?.local_win_rate, 0) * 1.9;
    const startStrength =
      toNum(features?.expected_actual_st_inv, 0) * 42 +
      Math.max(0, 7 - toNum(features?.expected_actual_st_rank ?? features?.st_rank, 6)) * 4.5;
    const leftNeighborBias =
      Math.max(0, toNum(features?.display_time_delta_vs_left, 0)) * 35 +
      Math.max(0, toNum(features?.avg_st_rank_delta_vs_left, 0)) * 5 +
      toNum(features?.slit_alert_flag, 0) * 10;
    const headDistributionBias = toNum(headDistributionMap.get(lane), 0) * 42;
    const stableInsideBias = lane === 1 ? 13.5 : lane === 2 ? 9.2 : lane === 3 ? 7.2 : 0;
    const boat1PriorityHeadBias = boat1PriorityModeApplied && lane === 1 ? 22 : 0;
    const laneAttackBias = attackScenarioApplied && getAttackScenarioHeadLane(attackScenarioType) === lane
      ? attackScenarioScore * 0.24
      : 0;
      const survivalBias = lane === 1
        ? Math.max(survivalResidualScore * 0.54, toNum(roleProbabilityLayers?.boat1_escape_probability, 0) * 22)
        : lane === 2 ? survivalResidualScore * 0.12 : lane === 3 ? survivalResidualScore * 0.08 : 0;
    const fHoldPenalty = toNum(features?.f_hold_caution_penalty, 0) * (16 + venueFHoldAdj * 0.7);
    const outerHeadPenalty = lane === 5 ? 11 + venueOuterSuppressionAdj * 1.5 : lane === 6 ? 18 + venueOuterSuppressionAdj * 1.7 : 0;
    const score =
      toNum(row?.score, 0) * 0.68 +
      exhibitionStrength +
      motorStrength +
      playerStrength +
      startStrength +
      leftNeighborBias +
      lapFrontBias +
      headDistributionBias +
      stableInsideBias +
      boat1PriorityHeadBias +
      laneAttackBias +
      survivalBias +
      learnedVenueHeadAdj * 0.8 +
      learnedFormationHeadAdj * 0.8 +
      learnedScenarioHeadAdj * 0.6 +
      venueExactaAdj * 0.8 -
      outerHeadPenalty -
      fHoldPenalty -
      learnedFHoldAdj * 3;
    return {
      lane,
      score: Number(score.toFixed(2)),
      exhibition_strength: Number(exhibitionStrength.toFixed(2))
    };
  });
  const headScoreMap = new Map(headScores.map((row) => [row.lane, row]));

  const partnerScores = rows.map((row) => {
    const lane = toInt(row?.racer?.lane, null);
    const features = row?.features || {};
    const escapeBias = escapePatternAnalysis?.escape_pattern_applied
      ? getEscapeSecondPlaceBiasScore(escapePatternAnalysis?.escape_second_place_bias_json || {}, lane) * 1.4
      : 0;
    const attackPartnerBias = attackScenarioApplied && getAttackScenarioTargetLanes(attackScenarioType).partner.includes(lane)
      ? attackScenarioScore * 0.1
      : 0;
    const secondDistributionBias = toNum(
      (boat1HeadLikely ? boat1SecondDistributionMap.get(lane) : null) ?? secondDistributionMap.get(lane),
      0
    ) * (boat1HeadLikely ? 44 : 36);
    const insidePartnerBias = boat1HeadLikely
      ? lane === 2 ? 13 : lane === 3 ? 11 : lane === 4 ? 7 : 0
      : lane === 2 ? 9 : lane === 3 ? 7.5 : lane === 4 ? 4.6 : 0;
    const boat1PriorityPartnerBias = boat1PriorityModeApplied && lane >= 2 && lane <= 4 ? 10 : 0;
    const exhibitionPartnerBias = Math.max(0, 7 - toNum(features?.exhibition_rank, 6)) * 5;
    const motorPartnerBias = toNum(features?.motor_total_score, 0) * 1.5;
    const startPartnerBias = Math.max(0, 7 - toNum(features?.expected_actual_st_rank ?? features?.st_rank, 6)) * 3.4;
    const leftNeighborBias =
      Math.max(0, toNum(features?.display_time_delta_vs_left, 0)) * 22 +
      Math.max(0, toNum(features?.avg_st_rank_delta_vs_left, 0)) * 4 +
      toNum(features?.slit_alert_flag, 0) * 8;
    const lapFrontAttackSupport =
      Math.max(0, toNum(features?.lap_time_delta_vs_front, 0)) * (18 + venueLapAdj * 18) +
      toNum(features?.lap_attack_flag, 0) * 4 +
      toNum(features?.lap_attack_strength, 0) * (0.22 + venueLapAdj * 0.14);
    const fHoldPenalty = toNum(features?.f_hold_caution_penalty, 0) * (10 + venueFHoldAdj * 0.5);
    const score =
      toNum(row?.score, 0) * 0.44 +
      escapeBias +
      attackPartnerBias +
      secondDistributionBias +
      insidePartnerBias +
      venuePartnerAdj * (lane >= 2 && lane <= 4 ? 1.3 : -0.5) +
      venueThirdAdj * (lane >= 2 && lane <= 4 ? 0.35 : lane === 1 ? 0.1 : -0.2) +
      boat1PriorityPartnerBias +
      exhibitionPartnerBias +
      motorPartnerBias +
      startPartnerBias +
      leftNeighborBias -
      (boat1HeadLikely ? 0 : lapFrontAttackSupport * 0.35) +
      (boat1HeadLikely ? lapFrontAttackSupport * 0.55 : 0) -
      (lane === 5 ? 5.5 + venueOuterSuppressionAdj : lane === 6 ? 8.5 + venueOuterSuppressionAdj : 0) -
      fHoldPenalty;
    return {
      lane,
      score: Number(score.toFixed(2))
    };
  });
  const partnerScoreMap = new Map(partnerScores.map((row) => [row.lane, row]));

  const bucket = new Map();
  for (const head of headScores) {
    for (const partner of partnerScores) {
      if (!Number.isInteger(head.lane) || !Number.isInteger(partner.lane) || head.lane === partner.lane) continue;
      if (boat1HeadLikely && head.lane !== 1) continue;
      if (boat1HeadLikely && head.lane === 1 && focusedBoat1PartnerLanes.length > 0 && !focusedBoat1PartnerLanes.includes(partner.lane)) {
        continue;
      }
      const combo = `${head.lane}-${partner.lane}`;
      const evidence = exactaEvidenceMap.get(combo) || { prob: 0, recommended_bet: 0, explanation_tags: [] };
      const balanceTag = head.lane === 1 && survivalResidualScore >= 32 ? "BOAT1_SURVIVAL" : null;
      const boat1InsideExactaBonus =
        head.lane === 1 && (partner.lane === 2 || partner.lane === 3 || partner.lane === 4)
          ? (partner.lane === 2 ? 18 : partner.lane === 3 ? 14 : 9)
          : 0;
      const outerHeadExactaPenalty = head.lane === 5 ? 16 : head.lane === 6 ? 24 : 0;
      const reasonTags = [
        head.exhibition_strength >= 40 ? "ONE_LAP_EXHIBITION_HEAD" : null,
        escapePatternAnalysis?.escape_pattern_applied && head.lane === 1 ? "ESCAPE_CONTEXT" : null,
        attackScenarioApplied && getAttackScenarioHeadLane(attackScenarioType) === head.lane ? "ATTACK_HEAD" : null,
        attackScenarioApplied && getAttackScenarioTargetLanes(attackScenarioType).partner.includes(partner.lane) ? "ATTACK_PARTNER" : null,
        boat1InsideExactaBonus > 0 ? "BOAT1_INSIDE_EXACTA" : null,
        boat1HeadLikely && focusedBoat1PartnerLanes.includes(partner.lane) ? "BOAT1_SECOND_FOCUS" : null,
        boat1HeadLikely && !focusedBoat1PartnerLanes.includes(partner.lane) ? "DIFFUSE_PARTNER_SKIP" : null,
        toNum(partnerScoreMap.get(partner.lane)?.score, 0) >= 70 ? "STRONG_PARTNER" : null,
        balanceTag
      ].filter(Boolean);
      const compositeScore =
        head.score * 0.61 +
        partner.score * 0.39 +
        toNum(evidence?.prob, 0) * 220 +
        Math.min(24, toNum(evidence?.recommended_bet, 0) / 100) +
        boat1InsideExactaBonus +
        venueExactaAdj * (head.lane === 1 ? 0.9 : 0.35) -
        outerHeadExactaPenalty;
      const existing = bucket.get(combo);
      const nextRow = {
        combo,
        prob: Number((compositeScore / 1000).toFixed(4)),
        recommended_bet: Math.max(100, Math.round(Math.max(120, compositeScore * 1.3) / 100) * 100),
        exacta_head_score: head.score,
        exacta_partner_score: partner.score,
        exacta_reason_tags: [...new Set(reasonTags)],
        explanation_tags: [...new Set(safeArray(evidence?.explanation_tags))],
        source_prob: Number(toNum(evidence?.prob, 0).toFixed(4))
      };
      if (!existing || toNum(existing?.exacta_head_score, 0) + toNum(existing?.exacta_partner_score, 0) < head.score + partner.score) {
        bucket.set(combo, nextRow);
      }
    }
  }

  const items = [...bucket.values()]
    .sort((a, b) => {
      const scoreA = toNum(a?.prob, 0) * 1000 + toNum(a?.exacta_head_score, 0) + toNum(a?.exacta_partner_score, 0);
      const scoreB = toNum(b?.prob, 0) * 1000 + toNum(b?.exacta_head_score, 0) + toNum(b?.exacta_partner_score, 0);
      return scoreB - scoreA;
    })
    .slice(0, boat1HeadLikely ? (boat1SecondConcentration >= 0.66 ? 3 : 2) : 4)
    .map((row) => ({
      ...row,
      exacta_head_score: Number(toNum(row?.exacta_head_score, 0).toFixed(2)),
      exacta_partner_score: Number(toNum(row?.exacta_partner_score, 0).toFixed(2))
    }));

  return {
    shown: items.length > 0 && (!boat1HeadLikely || boat1SecondConcentration >= 0.48),
    exacta_head_score: items.length > 0 ? Number(toNum(items[0]?.exacta_head_score, 0).toFixed(2)) : 0,
    exacta_partner_score: items.length > 0 ? Number(toNum(items[0]?.exacta_partner_score, 0).toFixed(2)) : 0,
    exacta_reason_tags: [...new Set(items.flatMap((row) => safeArray(row?.exacta_reason_tags)).slice(0, 8))],
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
    skipModeConfidenceHardMax: 48,
    dataQualityHardMax: 46,
    raceStabilityHardMax: 42,
    partnerClarityHardMax: 38
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
  },
  qualityGate: {
    participateDataQualityMin: 66,
    participateRaceStabilityMin: 63,
    participatePartnerClarityMin: 60,
    watchDataQualityMin: 48,
    watchRaceStabilityMin: 44,
    watchPartnerClarityMin: 40
  }
};

function distributionConcentration(distribution, topCount = 2) {
  return safeArray(distribution)
    .slice(0, topCount)
    .reduce((sum, row) => sum + toNum(row?.weight, 0), 0) * 100;
}

function buildQualityGateScores({
  ranking,
  racers,
  entryMeta,
  confidenceScores,
  raceStructure,
  escapePatternAnalysis,
  headScenarioBalanceAnalysis,
  roleProbabilityLayers,
  exactaSnapshot,
  learningWeights,
  race
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const racerRows = Array.isArray(racers) ? racers : [];
  const features = rows.map((row) => row?.features || {});
  const featureCount = Math.max(1, Math.max(rows.length, racerRows.length, 6));
  const exhibitionStReady = features.filter((f) => Number.isFinite(Number(f?.exhibition_st))).length / featureCount;
  const exhibitionTimeReady = features.filter((f) => Number.isFinite(Number(f?.exhibition_time))).length / featureCount;
  const lapReady = features.filter((f) => Number.isFinite(Number(f?.lap_attack_strength)) || Number.isFinite(Number(f?.lap_time_delta_vs_front))).length / featureCount;
  const playerMotorReady = features.filter((f) =>
    Number.isFinite(Number(f?.motor_total_score)) && Number.isFinite(Number(f?.class_score))
  ).length / featureCount;
  const entryClarity = entryMeta?.entry_changed
    ? entryMeta?.severity === "high"
      ? 18
      : entryMeta?.severity === "medium"
        ? 40
        : 58
    : 88;
  const formationConfidence = toNum(escapePatternAnalysis?.escape_pattern_confidence, 0);
  const cautionFlags = Array.isArray(confidenceScores?.confidence_reason_tags)
    ? confidenceScores.confidence_reason_tags
    : [];
  const missingPenalty =
    (cautionFlags.includes("INSUFFICIENT_EXHIBITION_DATA") ? 18 : 0) +
    (cautionFlags.includes("START_SIGNAL_UNSTABLE") ? 8 : 0) +
    (cautionFlags.includes("ENTRY_CHANGE_PENALTY") ? 6 : 0);
  const dataQualityScore = clamp(
    0,
    100,
    exhibitionStReady * 24 +
      exhibitionTimeReady * 22 +
      lapReady * 16 +
      playerMotorReady * 18 +
      entryClarity * 0.12 +
      formationConfidence * 0.16 -
      missingPenalty
  );

  const headConcentration = distributionConcentration(
    roleProbabilityLayers?.first_place_probability_json || headScenarioBalanceAnalysis?.head_distribution_json,
    2
  );
  const secondConcentration = distributionConcentration(
    roleProbabilityLayers?.second_place_probability_json || headScenarioBalanceAnalysis?.second_place_distribution_json,
    2
  );
  const thirdConcentration = distributionConcentration(
    roleProbabilityLayers?.third_place_probability_json || headScenarioBalanceAnalysis?.third_place_distribution_json,
    2
  );
  const survivalResidual = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const attackMargin = Math.abs(toNum(headScenarioBalanceAnalysis?.attack_dominance_margin, 0));
  const outerGuardPenalty = toNum(headScenarioBalanceAnalysis?.outer_head_guard_applied, 0) === 1 ? 8 : 0;
  const contradictionPenalty = toNum(raceStructure?.chaos_risk_score, 0) >= 76 ? 10 : 0;
  const fHoldUncertainty = features.reduce((sum, f) => sum + toNum(f?.f_hold_caution_penalty, 0), 0) / featureCount;
  const entryUncertainty = entryMeta?.entry_changed
    ? entryMeta?.severity === "high"
      ? 14
      : entryMeta?.severity === "medium"
        ? 8
        : 4
    : 0;
  const venueUncertainty = Math.abs(
    getSegmentCorrectionValue(learningWeights, "venue", toInt(race?.venueId, null), "recommendation_score_adjustment")
  ) * 1.5;
  const raceStabilityScore = clamp(
    0,
    100,
    headConcentration * 0.34 +
      secondConcentration * 0.22 +
      thirdConcentration * 0.12 +
      Math.min(18, survivalResidual * 0.22) +
      Math.max(0, 12 - attackMargin * 0.12) -
      outerGuardPenalty -
      contradictionPenalty -
      fHoldUncertainty * 6 -
      entryUncertainty -
      venueUncertainty
  );

  const boat1SecondDistribution = safeArray(
    roleProbabilityLayers?.boat1_second_place_probability_json || headScenarioBalanceAnalysis?.boat1_second_place_distribution_json
  );
  const boat1ThirdDistribution = safeArray(
    roleProbabilityLayers?.boat1_third_place_probability_json || headScenarioBalanceAnalysis?.boat1_third_place_distribution_json
  );
  const boat1SecondFocus = distributionConcentration(
    boat1SecondDistribution.length ? boat1SecondDistribution : headScenarioBalanceAnalysis?.second_place_distribution_json,
    3
  );
  const boat1ThirdFocus = distributionConcentration(
    boat1ThirdDistribution.length ? boat1ThirdDistribution : headScenarioBalanceAnalysis?.third_place_distribution_json,
    4
  );
  const boat1FamilySupport = boat1SecondDistribution
    .filter((row) => [2, 3, 4].includes(toInt(row?.lane, null)))
    .reduce((sum, row) => sum + toNum(row?.weight, 0), 0) * 100;
  const exactaStabilityScore = Array.isArray(exactaSnapshot?.items) && exactaSnapshot.items.length > 0
    ? clamp(
        0,
        100,
        distributionConcentration(
          exactaSnapshot.items.map((item) => ({ weight: toNum(item?.prob, 0) })),
          2
        ) * 0.9
      )
    : clamp(0, 100, headConcentration * 0.58 + secondConcentration * 0.42);
  const partnerClarityScore = clamp(
    0,
    100,
    boat1SecondFocus * 0.34 +
      boat1ThirdFocus * 0.16 +
      boat1FamilySupport * 0.32 +
      exactaStabilityScore * 0.12 +
      Math.min(12, survivalResidual * 0.16) -
      Math.max(0, 26 - thirdConcentration) * 0.2
  );
  const predictionReadabilityScore = clamp(
    0,
    100,
    dataQualityScore * 0.34 +
      raceStabilityScore * 0.38 +
      ((toInt(headScenarioBalanceAnalysis?.main_head_lane, null) === 1 ? partnerClarityScore : exactaStabilityScore) * 0.28)
  );

  return {
    data_quality_score: Number(dataQualityScore.toFixed(2)),
    race_stability_score: Number(raceStabilityScore.toFixed(2)),
    partner_clarity_score: Number(partnerClarityScore.toFixed(2)),
    prediction_readability_score: Number(predictionReadabilityScore.toFixed(2)),
    exacta_stability_score: Number(exactaStabilityScore.toFixed(2))
  };
}

function buildParticipationDecision({
  raceDecision,
  raceRisk,
  raceStructure,
  entryMeta,
  confidenceScores,
  scenarioSuggestions,
  raceFlow,
  escapePatternAnalysis,
  attackScenarioAnalysis,
  headScenarioBalanceAnalysis = null,
  roleProbabilityLayers = null,
  ranking = [],
  racers = [],
  exactaSnapshot = null,
  learningWeights = null,
  race = null
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
  const outerHeadGuardApplied = toNum(headScenarioBalanceAnalysis?.outer_head_guard_applied, 0) === 1;
  const mainHeadLane = toInt(headScenarioBalanceAnalysis?.main_head_lane, null);
  const counterHeadLane = toInt(headScenarioBalanceAnalysis?.second_head_lane, null);
  const survivalResidualScore = toNum(headScenarioBalanceAnalysis?.survival_residual_score, 0);
  const attackDominanceMargin = toNum(headScenarioBalanceAnalysis?.attack_dominance_margin, 0);
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
  const insideStabilityBonus =
    (mainHeadLane === 1 ? 5.6 : mainHeadLane >= 2 && mainHeadLane <= 3 ? 4.6 : 0) +
    (counterHeadLane >= 2 && counterHeadLane <= 3 ? 2.2 : 0) +
    (survivalResidualScore >= 30 ? 2.1 : 0);
  const outerHeadWatchPenalty =
    outerHeadGuardApplied && (mainHeadLane === 5 || mainHeadLane === 6) && attackDominanceMargin < 30
      ? 5.8
      : 0;
  const qualityScores = buildQualityGateScores({
    ranking,
    racers,
    entryMeta,
    confidenceScores,
    raceStructure,
    escapePatternAnalysis,
    headScenarioBalanceAnalysis,
    roleProbabilityLayers,
    exactaSnapshot,
    learningWeights,
    race
  });
  const dataQualityScore = toNum(qualityScores?.data_quality_score, 50);
  const raceStabilityScore = toNum(qualityScores?.race_stability_score, 50);
  const partnerClarityScore = toNum(qualityScores?.partner_clarity_score, 50);
  const exactaStabilityScore = toNum(qualityScores?.exacta_stability_score, 50);
  const predictionReadabilityScore = toNum(qualityScores?.prediction_readability_score, 50);
  const qualityGatePenalty =
    Math.max(0, PARTICIPATION_TUNING.qualityGate.participateDataQualityMin - dataQualityScore) * 0.16 +
    Math.max(0, PARTICIPATION_TUNING.qualityGate.participateRaceStabilityMin - raceStabilityScore) * 0.14 +
    (mainHeadLane === 1
      ? Math.max(0, PARTICIPATION_TUNING.qualityGate.participatePartnerClarityMin - partnerClarityScore) * 0.15
      : Math.max(0, 52 - exactaStabilityScore) * 0.08);
  const qualityGateApplied =
    dataQualityScore < PARTICIPATION_TUNING.qualityGate.participateDataQualityMin ||
    raceStabilityScore < PARTICIPATION_TUNING.qualityGate.participateRaceStabilityMin ||
    (mainHeadLane === 1 && partnerClarityScore < PARTICIPATION_TUNING.qualityGate.participatePartnerClarityMin);
  const adjustedHeadFixed = Math.min(
    100,
    Math.max(
      0,
      headFixed +
        insideStabilityBonus * 0.8 +
        formationBoost * 0.45 +
        escapeFocusBoost +
        slitBoost * 0.35 +
        attackScenarioBoost * 0.45 +
        segmentParticipationCorrection * 0.6 -
        fHoldPenalty * 0.65 -
        outerHeadWatchPenalty * 0.8 -
        qualityGatePenalty * 0.7 -
        contradictionPenalty * 0.55
    )
  );
  const adjustedBetConf = Math.min(
    100,
    Math.max(
      0,
      betConf +
        insideStabilityBonus * 0.7 +
        formationBoost * 0.45 +
        escapeFocusBoost * 0.9 +
        slitBoost * 0.55 +
        attackScenarioBoost * 0.7 +
        segmentParticipationCorrection -
        fHoldPenalty * 0.8 -
        outerHeadWatchPenalty -
        qualityGatePenalty -
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
  if (outerHeadWatchPenalty > 0) reasonTags.push("OUTER_HEAD_WATCH_GUARD");
  if (insideStabilityBonus > 0) reasonTags.push("INSIDE_STABLE_STRUCTURE");
  if (headStability >= 62) reasonTags.push("HEAD_STABILITY_GOOD");
  if (adjustedHeadFixed >= 72) reasonTags.push("HEAD_CONFIDENCE_GOOD");
  if (adjustedBetConf >= 64) reasonTags.push("BET_CONFIDENCE_GOOD");
  if (dataQualityScore >= 72) reasonTags.push("HIGH_QUALITY");
  else if (dataQualityScore < PARTICIPATION_TUNING.qualityGate.watchDataQualityMin) reasonTags.push("LOW_DATA_QUALITY");
  if (raceStabilityScore >= 68) reasonTags.push("STABLE");
  else if (raceStabilityScore < PARTICIPATION_TUNING.qualityGate.watchRaceStabilityMin) reasonTags.push("LOW_STABILITY");
  if (mainHeadLane === 1 && partnerClarityScore >= 64) reasonTags.push("PARTNER_CLEAR");
  else if (mainHeadLane === 1 && partnerClarityScore < PARTICIPATION_TUNING.qualityGate.watchPartnerClarityMin) {
    reasonTags.push("PARTNER_UNCLEAR");
  }
  if (exactaStabilityScore >= 66) reasonTags.push("EXACTA_STABLE");
  if (qualityGateApplied) reasonTags.push("QUALITY_GATE_APPLIED");

  let decision = "watch";
  if (
    mode !== "SKIP" &&
    dataQualityScore >= PARTICIPATION_TUNING.qualityGate.participateDataQualityMin &&
    raceStabilityScore >= PARTICIPATION_TUNING.qualityGate.participateRaceStabilityMin &&
    (mainHeadLane !== 1 || partnerClarityScore >= PARTICIPATION_TUNING.qualityGate.participatePartnerClarityMin) &&
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
    dataQualityScore <= PARTICIPATION_TUNING.skip.dataQualityHardMax ||
    raceStabilityScore <= PARTICIPATION_TUNING.skip.raceStabilityHardMax ||
    (mainHeadLane === 1 && partnerClarityScore <= PARTICIPATION_TUNING.skip.partnerClarityHardMax) ||
    adjustedHeadFixed <= PARTICIPATION_TUNING.skip.lowHeadHardMax ||
    adjustedBetConf <= PARTICIPATION_TUNING.skip.lowBetHardMax ||
    (mode === "SKIP" && hasStrongCaution && confidence < PARTICIPATION_TUNING.skip.skipModeConfidenceHardMax) ||
    chaosRisk >= PARTICIPATION_TUNING.skip.chaosRiskHardMin ||
    contradictionCount >= PARTICIPATION_TUNING.skip.contradictionHardMin
  ) {
    decision = "not_recommended";
  } else if (
    mode !== "SKIP" &&
    dataQualityScore >= PARTICIPATION_TUNING.qualityGate.watchDataQualityMin &&
    raceStabilityScore >= PARTICIPATION_TUNING.qualityGate.watchRaceStabilityMin &&
    (mainHeadLane !== 1 || partnerClarityScore >= PARTICIPATION_TUNING.qualityGate.watchPartnerClarityMin) &&
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
      data_quality_score: Number(dataQualityScore.toFixed(2)),
      race_stability_score: Number(raceStabilityScore.toFixed(2)),
      partner_clarity_score: Number(partnerClarityScore.toFixed(2)),
      prediction_readability_score: Number(predictionReadabilityScore.toFixed(2)),
      exacta_stability_score: Number(exactaStabilityScore.toFixed(2)),
      quality_gate_applied: qualityGateApplied ? 1 : 0,
      inside_stability_bonus: Number(insideStabilityBonus.toFixed(2)),
      outer_head_watch_penalty: Number(outerHeadWatchPenalty.toFixed(2)),
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
      data_quality_score: Number(dataQualityScore.toFixed(2)),
      race_stability_score: Number(raceStabilityScore.toFixed(2)),
      partner_clarity_score: Number(partnerClarityScore.toFixed(2)),
      prediction_readability_score: Number(predictionReadabilityScore.toFixed(2)),
      exacta_stability_score: Number(exactaStabilityScore.toFixed(2)),
      quality_gate_applied: qualityGateApplied ? 1 : 0,
      gating_adjustment_json: {
        data_quality_penalty: Number(Math.max(0, PARTICIPATION_TUNING.qualityGate.participateDataQualityMin - dataQualityScore).toFixed(2)),
        race_stability_penalty: Number(Math.max(0, PARTICIPATION_TUNING.qualityGate.participateRaceStabilityMin - raceStabilityScore).toFixed(2)),
        partner_clarity_penalty: Number(
          (mainHeadLane === 1
            ? Math.max(0, PARTICIPATION_TUNING.qualityGate.participatePartnerClarityMin - partnerClarityScore)
            : 0
          ).toFixed(2)
        ),
        quality_gate_penalty: Number(qualityGatePenalty.toFixed(2))
      },
      inside_stability_bonus: Number(insideStabilityBonus.toFixed(2)),
      f_hold_penalty: Number(fHoldPenalty.toFixed(2)),
      outer_head_watch_penalty: Number(outerHeadWatchPenalty.toFixed(2)),
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
  race,
  predictionDataUsage
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
  const predictionDataPenalty = Math.min(
    22,
    toNum(predictionDataUsage?.penalties?.head_confidence_penalty, 0) +
      toNum(predictionDataUsage?.penalties?.top2_confidence_penalty, 0) * 0.75 +
      toNum(predictionDataUsage?.penalties?.lane_fit_confidence_penalty, 0) * 0.55
  );
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
      missingDataPenalty -
      predictionDataPenalty
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
      Math.max(0, missingDataPenalty - 4) -
      predictionDataPenalty * 0.72
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
  if (predictionDataPenalty >= 6) confidenceReasonTags.push("PREDICTION_FIELD_RELIABILITY_LOW");
  if (toNum(predictionDataUsage?.penalties?.head_confidence_penalty, 0) >= 5) confidenceReasonTags.push("MISSING_LAPTIME_OR_ST");
  if (toNum(predictionDataUsage?.penalties?.top2_confidence_penalty, 0) >= 4) confidenceReasonTags.push("MISSING_MOTOR2REN");
  if (toNum(predictionDataUsage?.penalties?.lane_fit_confidence_penalty, 0) >= 4) confidenceReasonTags.push("MISSING_LANE_FIT");
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
    prediction_data_penalty: Number(predictionDataPenalty.toFixed(2)),
    prediction_data_usage: predictionDataUsage || {},
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

function buildPredictionDataUsageSummary(ranking) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const fieldConfig = {
    lapTime: { required: true, penalty: 6 },
    exhibitionST: { required: true, penalty: 5 },
    exhibitionTime: { required: true, penalty: 4 },
    lapExStretch: { required: true, penalty: 5 },
    motor2ren: { required: true, penalty: 5 },
    lane1stScore: { required: true, penalty: 4 },
    lane2renScore: { required: true, penalty: 4 },
    lane3renScore: { required: true, penalty: 4 },
    lane1stAvg: { required: true, penalty: 4 },
    lane2renAvg: { required: true, penalty: 4 },
    lane3renAvg: { required: true, penalty: 4 },
    motor3ren: { required: false, penalty: 2 },
    fCount: { required: false, penalty: 1.5 }
  };
  const byLane = {};
  const fieldSummary = {};
  let penaltyHead = 0;
  let penaltyTop2 = 0;
  let penaltyLaneFit = 0;

  for (const row of rows) {
    const lane = toInt(row?.racer?.lane, null);
    const metaMap = row?.features?.prediction_field_meta || row?.racer?.predictionFieldMeta || {};
    if (!Number.isInteger(lane)) continue;
    byLane[String(lane)] = {};
    for (const [field, config] of Object.entries(fieldConfig)) {
      const meta = metaMap?.[field] || {};
      const entry = {
        used: !!meta?.is_usable && !!meta?.source,
        source: meta?.source || null,
        confidence: toNum(meta?.confidence, 0),
        is_usable: !!meta?.is_usable,
        reason: meta?.reason || (!meta?.source ? "missing" : "skipped")
      };
      byLane[String(lane)][field] = entry;
      if (!fieldSummary[field]) {
        fieldSummary[field] = {
          required: !!config.required,
          used_count: 0,
          missing_count: 0,
          lanes_used: [],
          lanes_missing: []
        };
      }
      if (entry.used) {
        fieldSummary[field].used_count += 1;
        fieldSummary[field].lanes_used.push(lane);
      } else {
        fieldSummary[field].missing_count += 1;
        fieldSummary[field].lanes_missing.push(lane);
      }
    }
  }

  const topRows = rows.slice(0, 3);
  for (const row of topRows) {
    const metaMap = row?.features?.prediction_field_meta || {};
    if (!(metaMap?.lapTime?.is_usable)) penaltyHead += fieldConfig.lapTime.penalty;
    if (!(metaMap?.exhibitionST?.is_usable)) penaltyHead += fieldConfig.exhibitionST.penalty * 0.8;
    if (!(metaMap?.exhibitionTime?.is_usable)) penaltyHead += fieldConfig.exhibitionTime.penalty * 0.65;
    if (!(metaMap?.lapExStretch?.is_usable)) penaltyHead += fieldConfig.lapExStretch.penalty * 0.75;
    if (!(metaMap?.motor2ren?.is_usable)) penaltyTop2 += fieldConfig.motor2ren.penalty;
    if (!(metaMap?.lane1stScore?.is_usable || metaMap?.lane1stAvg?.is_usable)) penaltyLaneFit += fieldConfig.lane1stScore.penalty * 0.8;
    if (!(metaMap?.lane2renScore?.is_usable || metaMap?.lane2renAvg?.is_usable)) penaltyLaneFit += fieldConfig.lane2renScore.penalty * 0.8;
    if (!(metaMap?.lane3renScore?.is_usable || metaMap?.lane3renAvg?.is_usable)) penaltyLaneFit += fieldConfig.lane3renScore.penalty * 0.8;
  }

  return {
    by_lane: byLane,
    field_summary: fieldSummary,
    required_fields: Object.keys(fieldConfig).filter((field) => fieldConfig[field].required),
    secondary_fields: Object.keys(fieldConfig).filter((field) => !fieldConfig[field].required),
    penalties: {
      head_confidence_penalty: Number(penaltyHead.toFixed(2)),
      top2_confidence_penalty: Number(penaltyTop2.toFixed(2)),
      lane_fit_confidence_penalty: Number(penaltyLaneFit.toFixed(2))
    }
  };
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
  let failureWhere = "race.route:validate_query";
  let temporaryFeaturePipelineDebug = null;
  try {
    const routeStartedAt = Date.now();
    let artifactCollector = null;
    const routeTimings = {
      official_base_fetch_ms: null,
      kyoteibiyori_fetch_ms: null,
      parsing_ms: null,
      score_calculation_ms: null,
      actual_entry_reassignment_ms: null,
      odds_fetch_ms: null,
      prediction_build_ms: null,
      total_response_ms: null
    };
    const { date, venueId, raceNo, participationMode } = req.query;
    const forceRefresh = parseBooleanFlag(req.query?.forceRefresh, false);
    const screeningMode = String(req.query?.screening || "").toLowerCase();
    const isHardRaceScreening = screeningMode === "hard_race";

    if (!date || !venueId || !raceNo) {
      return res.status(400).json({
        error: "bad_request",
        code: "missing_required_query_params",
        where: failureWhere,
        route: "/api/race",
        message: "date, venueId, and raceNo are required query params"
      });
    }

    let data;
    try {
      failureWhere = "race.route:getRaceData";
      const raceDataTimeoutMs = Math.max(
        3000,
        Math.min(
          toInt(req.query?.getRaceDataTimeoutMs, Number(process.env.RACE_API_GETRACEDATA_TIMEOUT_MS || 9000)),
          isHardRaceScreening ? 14000 : 9000
        )
      );
      const dataFetchTimeoutMs = Math.max(
        2500,
        Math.min(
          toInt(req.query?.dataFetchTimeoutMs, isHardRaceScreening ? 7500 : 5000),
          isHardRaceScreening ? 8500 : 5000
        )
      );
      artifactCollector = isHardRaceScreening ? {} : null;
      console.info("[RACE_ROUTE][fetch_start]", JSON.stringify({
        route: "/api/race",
        date,
        venueId: toInt(venueId, null),
        raceNo: toInt(raceNo, null),
        screening: screeningMode || null,
        raceDataTimeoutMs,
        dataFetchTimeoutMs
      }));
      data = await withTimeout(
        () => getRaceData({
          date,
          venueId,
          raceNo,
          forceRefresh,
          timeoutMs: dataFetchTimeoutMs,
          screeningProfile: isHardRaceScreening,
          includeKyoteiBiyori: true,
          artifactCollector
        }),
        {
          timeoutMs: raceDataTimeoutMs,
          code: "get_race_data_timeout",
          where: failureWhere,
          route: "/api/race",
          message: "base race data fetch timed out"
        }
      );
      routeTimings.official_base_fetch_ms = toNullableNum(data?.source?.timings?.official_base_fetch_ms);
      routeTimings.kyoteibiyori_fetch_ms = toNullableNum(data?.source?.timings?.kyoteibiyori_fetch_ms);
      routeTimings.parsing_ms = toNullableNum(data?.source?.timings?.parsing_ms);
      console.info("[RACE_ROUTE][fetch_success]", JSON.stringify({
        route: "/api/race",
        date,
        venueId: toInt(venueId, null),
        raceNo: toInt(raceNo, null),
        official_fetch_status: data?.source?.official_fetch_status || {},
        kyotei_ok: !!data?.source?.kyotei_biyori?.ok
      }));
      if (isHardRaceScreening) {
        const predictionStartedAt = Date.now();
        let hardRace1234;
        try {
          hardRace1234 = await buildHardRace1234Response({
            data,
            date,
            venueId,
            raceNo,
            artifactCollector
          });
        } catch (hardRaceErr) {
          hardRace1234 = {
            race_no: toInt(raceNo, null),
            status: "DATA_ERROR",
            data_status: data?.source?.official_fetch_status?.racelist === "success" ? "PARTIAL" : "DATA_ERROR",
            boat1_escape_trust: null,
            opponent_234_fit: null,
            pair23_fit: null,
            pair24_fit: null,
            pair34_fit: null,
            kill_escape_risk: null,
            shape_shuffle_risk: null,
            makuri_risk: null,
            outside_break_risk: null,
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
            p_143: null,
            fixed1234_matrix: {},
            fixed1234_top4: [],
            suggested_shape: null,
            decision: data?.source?.official_fetch_status?.racelist === "success" ? "PARTIAL" : "DATA_ERROR",
            decision_reason: String(hardRaceErr?.message || hardRaceErr || "hard race response build failed"),
            missing_fields: ["hard_race_response"],
            missing_field_details: {
              hard_race_response: {
                reason: "not calculated",
                source: "route",
                lane: null,
                field: "hard_race_response"
              }
            },
            metric_status: {},
            source_summary: {
              primary: {
                source: "boatrace",
                racelist: data?.source?.official_fetch_status?.racelist || "unknown",
                beforeinfo: data?.source?.official_fetch_status?.beforeinfo || "unknown"
              },
              supplement: {
                source: "kyoteibiyori",
                fetch_ok: !!data?.source?.kyotei_biyori?.ok
              },
              source_priority: [
                "boatrace > kyoteibiyori",
                "official overlap wins",
                "kyoteibiyori only supplements missing traits"
              ]
            },
            fetched_urls: artifactCollector?.fetched_urls || {},
            fetch_timings: data?.source?.fetch_timings || data?.source?.timings || {},
            raw_saved_paths: {},
            parsed_saved_paths: {},
            normalized_data: null,
            features: {},
            scores: {},
            screeningDebug: {
              fetch_success: data?.source?.official_fetch_status?.racelist === "success",
              parse_success: false,
              score_success: false,
              decision_reason: String(hardRaceErr?.message || hardRaceErr || "hard race response build failed"),
              missing_required_scores: ["hard_race_response"]
            }
          };
          console.error("[RACE_ROUTE][hard_race_fail_open]", JSON.stringify({
            route: "/api/race",
            date,
            venueId: toInt(venueId, null),
            raceNo: toInt(raceNo, null),
            message: hardRace1234.decision_reason
          }));
        }
        routeTimings.prediction_build_ms = Date.now() - predictionStartedAt;
        routeTimings.total_response_ms = Date.now() - routeStartedAt;
        console.info("[RACE_ROUTE][response_end]", JSON.stringify({
          route: "/api/race",
          date,
          venueId: toInt(venueId, null),
          raceNo: toInt(raceNo, null),
          data_status: hardRace1234?.data_status || null,
          decision: hardRace1234?.decision || null,
          total_response_ms: routeTimings.total_response_ms
        }));
        return res.json({
          source: data.source || {},
          race: data.race,
          racers: data.racers,
          hardRace1234,
          ...hardRace1234,
          routeTiming: routeTimings
        });
      }
    } catch (fetchErr) {
      console.error("[RACE_ROUTE][fetch_failure]", JSON.stringify({
        route: "/api/race",
        date,
        venueId: toInt(venueId, null),
        raceNo: toInt(raceNo, null),
        where: failureWhere,
        message: String(fetchErr?.message || fetchErr)
      }));
      failureWhere = "race.route:loadRaceSnapshotFallback";
      const fallback = loadRaceSnapshotFromDb({ date, venueId, raceNo });
      if (!fallback) throw fetchErr;
      data = fallback;
    }
    const predictionStartedAt = Date.now();
    failureWhere = "race.route:feature_pipeline";
    const learningWeights = getActiveLearningWeights();
    const learningState = getLatestLearningRun();
    const featurePipelineDebug = {
      fail_open_applied: false,
      legacy_fields_missing: [],
      skipped_optional_features: [],
      skipped_pipeline_steps: [],
      errors: []
    };
    const raceId = saveRace(data);
    const manualLapEvaluation = getManualLapEvaluation(raceId);
    const entryMeta = buildCanonicalEntryOrderMeta(data.racers, data?.source?.actual_entry || null);
    if (entryMeta.fallback_used) {
      console.warn("[RACE_API][actual_entry_fallback]", {
        route: "/api/race",
        date,
        venueId,
        raceNo,
        reason: entryMeta.fallback_reason,
        raw_actual_entry_source_text: entryMeta.raw_actual_entry_source_text
      });
    }
    const racersWithRecentPlayerStats = applyRecentPlayerStatProfilesToRacers({
      racers: data.racers,
      raceDate: data?.race?.date || null
    });
    data = {
      ...data,
      racers: racersWithRecentPlayerStats
    };
    const reassignmentStartedAt = Date.now();
    let baseFeatures;
    let venueAdjustedBase;
    let preRanking;
    let prePattern;
    let trendFeatures;
    let entryAdjusted;
    let rankingBase;
    let contenderAdjusted;
    let rankingBeforePatternBias;
    let patternBeforeBias;
    let escapePatternAnalysis;
    let ranking;
    try {
      featurePipelineDebug.legacy_fields_missing = (Array.isArray(data?.racers) ? data.racers : [])
        .flatMap((racer) => {
          const lane = toInt(racer?.lane, null);
          const laneLabel = Number.isInteger(lane) ? `lane_${lane}` : "lane_unknown";
          const legacyEntries = [];
          if (!Number.isFinite(toNullableNum(racer?.avgStRank))) legacyEntries.push(`${laneLabel}: avgStRank missing`);
          if (!Number.isFinite(toNullableNum(racer?.["laneStRank"]))) legacyEntries.push(`${laneLabel}: laneStRank missing`);
          if (!Number.isFinite(toNullableNum(racer?.lane_st_rank))) legacyEntries.push(`${laneLabel}: lane_st_rank missing`);
          return legacyEntries;
        });
      baseFeatures = applyMotorPerformanceFeatures(
        applyCoursePerformanceFeatures(buildRaceFeatures(data.racers, data.race))
      );
      routeTimings.actual_entry_reassignment_ms = Date.now() - reassignmentStartedAt;
      venueAdjustedBase = applyVenueAdjustments(baseFeatures, data.race);
      preRanking = rankRace(venueAdjustedBase.racersWithFeatures);
      prePattern = analyzeRacePattern(preRanking);
      trendFeatures = applyMotorTrendFeatures(venueAdjustedBase.racersWithFeatures, {
        racePattern: prePattern.race_pattern
      });

      entryAdjusted = applyEntryDynamicsFeatures(trendFeatures, {
        racePattern: prePattern.race_pattern,
        chaos_index: prePattern.indexes.chaos_index
      });

      rankingBase = rankRace(entryAdjusted.racersWithFeatures);
      contenderAdjusted = applyContenderSynergy(rankingBase);
      rankingBeforePatternBias = contenderAdjusted.ranking;
      patternBeforeBias = analyzeRacePattern(rankingBeforePatternBias);
      escapePatternAnalysis = analyzeEscapeFormationLayer({
        ranking: rankingBeforePatternBias,
        racePattern: patternBeforeBias.race_pattern,
        indexes: patternBeforeBias.indexes
      });
      ranking = applyEscapeFormationBiasToRanking(rankingBeforePatternBias, escapePatternAnalysis, learningWeights, data?.race || null);
      ranking = applyHitRateFocusToRanking(ranking, escapePatternAnalysis);
    } catch (featurePipelineErr) {
      featurePipelineDebug.fail_open_applied = true;
      featurePipelineDebug.skipped_optional_features.push("feature_pipeline_primary_path");
      featurePipelineDebug.skipped_pipeline_steps.push(
        "applyCoursePerformanceFeatures",
        "applyMotorPerformanceFeatures",
        "applyVenueAdjustments",
        "applyMotorTrendFeatures",
        "applyEntryDynamicsFeatures",
        "applyContenderSynergy",
        "applyEscapeFormationBiasToRanking",
        "applyHitRateFocusToRanking"
      );
      featurePipelineDebug.errors.push(String(featurePipelineErr?.message || featurePipelineErr || "feature_pipeline_failed"));
      console.warn("[RACE_API][feature_pipeline_fail_open]", {
        route: "/api/race",
        date,
        venueId,
        raceNo,
        message: featurePipelineErr?.message || String(featurePipelineErr)
      });
      const fallbackPipeline = buildFeaturePipelineFallback(data, featurePipelineDebug, learningWeights);
      baseFeatures = fallbackPipeline.baseFeatures;
      venueAdjustedBase = fallbackPipeline.venueAdjustedBase;
      preRanking = fallbackPipeline.preRanking;
      prePattern = fallbackPipeline.prePattern;
      trendFeatures = fallbackPipeline.trendFeatures;
      entryAdjusted = fallbackPipeline.entryAdjusted;
      rankingBase = fallbackPipeline.rankingBase;
      contenderAdjusted = fallbackPipeline.contenderAdjusted;
      rankingBeforePatternBias = fallbackPipeline.rankingBeforePatternBias;
      patternBeforeBias = fallbackPipeline.patternBeforeBias;
      escapePatternAnalysis = fallbackPipeline.escapePatternAnalysis;
      ranking = fallbackPipeline.ranking;
      routeTimings.actual_entry_reassignment_ms = Date.now() - reassignmentStartedAt;
    }
    temporaryFeaturePipelineDebug = buildTemporaryFeaturePipelineDebug(ranking, featurePipelineDebug);
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

    const scoreCalculationStartedAt = Date.now();
    const monteCarlo = simulateTrifectaProbabilities(ranking, {
      topN: 10,
      simulations: 8000
    });
    routeTimings.score_calculation_ms = Date.now() - scoreCalculationStartedAt;
    const probabilities = monteCarlo.probabilities;
    const simulation = {
      method: "monte_carlo",
      simulations: monteCarlo.simulations,
      top_combinations: monteCarlo.top_combinations
    };
    failureWhere = "race.route:odds_pipeline";
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
      const oddsStartedAt = Date.now();
      evData = await withTimeout(
        () => analyzeExpectedValue({
          date: data.race.date,
          venueId: data.race.venueId,
          raceNo: data.race.raceNo,
          simulation
        }),
        {
          timeoutMs: Math.min(Number(process.env.RACE_API_ODDS_TIMEOUT_MS || 2800), 2800),
          code: "odds_pipeline_timeout",
          where: failureWhere,
          route: "/api/race",
          message: "odds pipeline timed out",
          fallbackValue: evData,
          swallow: true
        }
      );
      routeTimings.odds_fetch_ms = Date.now() - oddsStartedAt;
    } catch (oddsErr) {
      console.warn("[ODDS] fetch failed:", oddsErr?.message || oddsErr);
    }
    failureWhere = "race.route:recommendation_pipeline";
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
    const baselineRankingBeforeAttack = Array.isArray(ranking)
      ? ranking.map((row) => ({
          ...row,
          features: { ...(row?.features || {}) }
        }))
      : [];
    ranking = applyAttackScenarioBiasToRanking(ranking, attackScenarioAnalysis);
    ranking = applyHeadDistributionGuardToRanking({
      ranking,
      baselineRanking: baselineRankingBeforeAttack,
      attackScenarioAnalysis,
      escapePatternAnalysis
    });
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
      baselineRanking: baselineRankingBeforeAttack,
      raceFlow,
      headSelection: headSelectionRefined,
      attackScenarioAnalysis,
      escapePatternAnalysis,
      learningWeights,
      race: data?.race || null
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
    headScenarioBalanceAnalysis.second_distribution_json = buildSecondDistributionFromTickets(
      ticketOptimizationWithStake.optimized_tickets.length > 0
        ? ticketOptimizationWithStake.optimized_tickets
        : bet_plan_with_stake.recommended_bets
    );
    const candidateDistributions = buildSeparatedCandidateDistributions({
      ranking,
      tickets: ticketOptimizationWithStake.optimized_tickets.length > 0
        ? ticketOptimizationWithStake.optimized_tickets
        : bet_plan_with_stake.recommended_bets,
      headScenarioBalanceAnalysis,
      escapePatternAnalysis,
      attackScenarioAnalysis,
      learningWeights,
      race: data?.race || null
    });
    headScenarioBalanceAnalysis.formation_first_place_prior_json = candidateDistributions.formation_first_place_prior_json;
    headScenarioBalanceAnalysis.first_place_distribution_json = candidateDistributions.first_place_distribution_json;
    headScenarioBalanceAnalysis.second_place_distribution_json = candidateDistributions.second_place_distribution_json;
    headScenarioBalanceAnalysis.third_place_distribution_json = candidateDistributions.third_place_distribution_json;
    headScenarioBalanceAnalysis.boat1_second_place_distribution_json = candidateDistributions.boat1_second_place_distribution_json;
    headScenarioBalanceAnalysis.boat1_third_place_distribution_json = candidateDistributions.boat1_third_place_distribution_json;
    headScenarioBalanceAnalysis.partner_search_bias_json = candidateDistributions.partner_search_bias_json;
    headScenarioBalanceAnalysis.boat1_partner_bias_json = candidateDistributions.boat1_partner_bias_json;
    headScenarioBalanceAnalysis.boat1_partner_reason_tags = candidateDistributions.boat1_partner_reason_tags;
    headScenarioBalanceAnalysis.partner_search_lap_bias_json = candidateDistributions.partner_search_lap_bias_json;
    headScenarioBalanceAnalysis.venue_correction_summary = candidateDistributions.venue_correction_summary;
    headScenarioBalanceAnalysis.launch_venue_calibration_json = candidateDistributions.launch_venue_calibration_json;
    headScenarioBalanceAnalysis.third_place_residual_bias_json = candidateDistributions.third_place_residual_bias_json;
    headScenarioBalanceAnalysis.boat1_partner_search_applied = candidateDistributions.boat1_partner_search_applied;
    headScenarioBalanceAnalysis.boat1_partner_model_applied = candidateDistributions.boat1_partner_model_applied;
    headScenarioBalanceAnalysis.boat1_escape_partner_version = candidateDistributions.boat1_escape_partner_version;
    headScenarioBalanceAnalysis.stronger_lap_bias_applied = candidateDistributions.stronger_lap_bias_applied;
    headScenarioBalanceAnalysis.inside_baseline_priority_applied = candidateDistributions.inside_baseline_priority_applied;
    headScenarioBalanceAnalysis.candidate_balance_adjustment_json = candidateDistributions.candidate_balance_adjustment_json;
    headScenarioBalanceAnalysis.hit_rate_focus_applied = candidateDistributions.hit_rate_focus_applied;
    headScenarioBalanceAnalysis.first_place_probability_json = candidateDistributions.first_place_probability_json;
    headScenarioBalanceAnalysis.second_place_probability_json = candidateDistributions.second_place_probability_json;
    headScenarioBalanceAnalysis.third_place_probability_json = candidateDistributions.third_place_probability_json;
    headScenarioBalanceAnalysis.boat1_second_place_probability_json = candidateDistributions.boat1_second_place_probability_json;
    headScenarioBalanceAnalysis.boat1_third_place_probability_json = candidateDistributions.boat1_third_place_probability_json;
    headScenarioBalanceAnalysis.survival_probability_json = candidateDistributions.survival_probability_json;
    headScenarioBalanceAnalysis.boat1_escape_probability = candidateDistributions.boat1_escape_probability;
    headScenarioBalanceAnalysis.attack_scenario_probability_json = candidateDistributions.attack_scenario_probability_json;
    headScenarioBalanceAnalysis.role_probability_summary_json = candidateDistributions.role_probability_summary_json;
    headScenarioBalanceAnalysis.role_probability_version = candidateDistributions.role_probability_version;
    headScenarioBalanceAnalysis.scoring_family_components_json = candidateDistributions.scoring_family_components_json;
    headScenarioBalanceAnalysis.rebalance_version = candidateDistributions.rebalance_version;
    headScenarioBalanceAnalysis.boat3_weak_st_head_suppression_json = candidateDistributions.boat3_weak_st_head_suppression_json;
    headScenarioBalanceAnalysis.boat3_weak_st_head_suppressed = candidateDistributions.boat3_weak_st_head_suppressed;
    headScenarioBalanceAnalysis.launch_state_scores_json = candidateDistributions.launch_state_scores_json;
    headScenarioBalanceAnalysis.launch_state_thresholds_used_json = candidateDistributions.launch_state_thresholds_used_json;
    headScenarioBalanceAnalysis.launch_state_labels_json = candidateDistributions.launch_state_labels_json;
    headScenarioBalanceAnalysis.intermediate_development_events_json = candidateDistributions.intermediate_development_events_json;
    headScenarioBalanceAnalysis.race_scenario_probabilities_json = candidateDistributions.race_scenario_probabilities_json;
    headScenarioBalanceAnalysis.finish_probabilities_by_scenario_json = candidateDistributions.finish_probabilities_by_scenario_json;
    headScenarioBalanceAnalysis.finish_override_strength_by_lane_json = candidateDistributions.finish_override_strength_by_lane_json;
    headScenarioBalanceAnalysis.scenario_based_order_candidates_json = candidateDistributions.scenario_based_order_candidates_json;
    headScenarioBalanceAnalysis.matched_dictionary_scenarios_json = candidateDistributions.matched_dictionary_scenarios_json;
    headScenarioBalanceAnalysis.dictionary_scenario_match_scores_json = candidateDistributions.dictionary_scenario_match_scores_json;
    headScenarioBalanceAnalysis.dictionary_prior_adjustment_json = candidateDistributions.dictionary_prior_adjustment_json;
    headScenarioBalanceAnalysis.dictionary_condition_flags_json = candidateDistributions.dictionary_condition_flags_json;
    headScenarioBalanceAnalysis.dictionary_representative_ticket_priors_json = candidateDistributions.dictionary_representative_ticket_priors_json;
    headScenarioBalanceAnalysis.dictionary_cd_scenarios_activated = candidateDistributions.dictionary_cd_scenarios_activated;
    bet_plan_with_stake.recommended_bets = applySeparatedDistributionBiasToTickets(
      bet_plan_with_stake.recommended_bets,
      candidateDistributions
    );
    ticketOptimizationWithStake.optimized_tickets = applySeparatedDistributionBiasToTickets(
      ticketOptimizationWithStake.optimized_tickets,
      candidateDistributions
    );
    const predictionFeatureBundle = buildPredictionFeatureBundle({
      ranking,
      race: data?.race || null,
      entryMeta,
      learningWeights,
      escapePatternAnalysis,
      attackScenarioAnalysis,
      headScenarioBalanceAnalysis,
      candidateDistributions
    });
    const explicitBoat1EscapeProbability = computeBoat1EscapeProbability(predictionFeatureBundle);
    const explicitAttackScenarioProbabilities = computeAttackScenarioProbabilities(predictionFeatureBundle);
    const explicitFirstPlaceProbabilities = computeFirstPlaceProbabilities(
      predictionFeatureBundle,
      explicitBoat1EscapeProbability,
      explicitAttackScenarioProbabilities
    );
    const explicitSecondPlaceProbabilities = computeSecondPlaceProbabilities(
      predictionFeatureBundle,
      explicitBoat1EscapeProbability,
      explicitAttackScenarioProbabilities,
      explicitFirstPlaceProbabilities
    );
    const explicitSurvivalProbabilities = computeSurvivalProbabilities(
      predictionFeatureBundle,
      explicitAttackScenarioProbabilities
    );
    const explicitThirdPlaceProbabilities = computeThirdPlaceProbabilities(
      predictionFeatureBundle,
      explicitFirstPlaceProbabilities,
      explicitSecondPlaceProbabilities,
      explicitAttackScenarioProbabilities,
      explicitSurvivalProbabilities
    );
    const evidenceBiasTable = buildEvidenceBiasTable({
      featureBundle: predictionFeatureBundle,
      firstProbs: explicitFirstPlaceProbabilities,
      secondProbs: explicitSecondPlaceProbabilities,
      thirdProbs: explicitThirdPlaceProbabilities,
      attackProbs: explicitAttackScenarioProbabilities,
      survivalProbs: explicitSurvivalProbabilities
    });
    const confirmedRoleProbabilities = applyEvidenceBiasConfirmationToRoleProbabilities({
      featureBundle: predictionFeatureBundle,
      firstProbs: explicitFirstPlaceProbabilities,
      secondProbs: explicitSecondPlaceProbabilities,
      thirdProbs: explicitThirdPlaceProbabilities,
      evidenceBiasTable
    });
    const hitRateEnhancement = buildHitRateEnhancementContext({
      ranking,
      race: data?.race || null,
      raceFlow,
      playerStartProfile,
      roleProbabilityLayers: candidateDistributions,
      confidence: null
    });
    const enhancedRoleProbabilities = applyHitRateEnhancementToProbabilities({
      firstProbs: confirmedRoleProbabilities.confirmed_first_place_probability_json,
      secondProbs: confirmedRoleProbabilities.confirmed_second_place_probability_json,
      thirdProbs: confirmedRoleProbabilities.confirmed_third_place_probability_json,
      enhancement: hitRateEnhancement
    });
    const scenarioTreeOrderCandidates = buildScenarioTreeOrderCandidates(
      hitRateEnhancement,
      null
    );
    const roleBasedOrderCandidatesBase = composeFinishOrderCandidates({
      featureBundle: predictionFeatureBundle,
      firstProbs: enhancedRoleProbabilities.first,
      secondProbs: enhancedRoleProbabilities.second,
      thirdProbs: enhancedRoleProbabilities.third,
      attackProbs: explicitAttackScenarioProbabilities,
      survivalProbs: explicitSurvivalProbabilities
    });
    const roleBasedOrderCandidates = [...new Map([
      ...scenarioTreeOrderCandidates.map((row) => [row.combo, {
        combo: row.combo,
        probability: toNum(row?.probability, 0) + toNum(row?.rank_bonus, 0),
        reason_tags: row.reason_tags || []
      }]),
      ...roleBasedOrderCandidatesBase.map((row) => [row.combo, row])
    ]).values()]
      .sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0))
      .slice(0, 18);
    const finalBalanceAdjustmentJson = buildFinalBalanceAdjustmentSummary({
      ranking,
      recommendedBets: bet_plan_with_stake.recommended_bets,
      headScenarioBalanceAnalysis
    });
    const predictionDataUsage = buildPredictionDataUsageSummary(ranking);

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
      race: data?.race || null,
      predictionDataUsage
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
    const preliminaryBoat1HeadSnapshot = buildBoat1HeadBetsSnapshot({
      ranking,
      recommendedBets: bet_plan_with_stake?.recommended_bets,
      optimizedTickets: ticketOptimizationWithStake?.optimized_tickets,
      headScenarioBalanceAnalysis,
      escapePatternAnalysis,
      learningWeights,
      race: data?.race || null
    });
    const boat1PriorityAdjustment = applyBoat1PriorityModeToTickets({
      recommendedBets: bet_plan_with_stake?.recommended_bets,
      optimizedTickets: ticketOptimizationWithStake?.optimized_tickets,
      boat1HeadSnapshot: preliminaryBoat1HeadSnapshot,
      headScenarioBalanceAnalysis
    });
    bet_plan_with_stake.recommended_bets = boat1PriorityAdjustment.recommendedBets;
    ticketOptimizationWithStake.optimized_tickets = boat1PriorityAdjustment.optimizedTickets;
    headScenarioBalanceAnalysis.boat1_priority_mode_applied = boat1PriorityAdjustment.boat1_priority_mode_applied;
    headScenarioBalanceAnalysis.boat1_head_ratio_in_final_bets = boat1PriorityAdjustment.boat1_head_ratio_in_final_bets;
    headScenarioBalanceAnalysis.boat1_priority_reason_tags = boat1PriorityAdjustment.boat1_priority_reason_tags;
    let shapeRecommendation = null;
    let shapeBasedTrifectaTickets = [];
    let shapeGenerationError = null;
    try {
      hitRateEnhancement.confidence = confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct ?? null;
      shapeRecommendation = buildEnhancedTrifectaShapeRecommendation({
        firstProbs: enhancedRoleProbabilities.first,
        secondProbs: enhancedRoleProbabilities.second,
        thirdProbs: enhancedRoleProbabilities.third,
        enhancement: hitRateEnhancement,
        confidence: confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct
      });
      if (!shapeRecommendation?.selected_shape) {
        shapeRecommendation = buildHitRateShapeRecommendation({
          firstProbs: enhancedRoleProbabilities.first,
          secondProbs: enhancedRoleProbabilities.second,
          thirdProbs: enhancedRoleProbabilities.third,
          boat1EscapeProbability: explicitBoat1EscapeProbability,
          confidence: confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct
        });
      }
      shapeBasedTrifectaTickets = buildEnhancedShapeBasedTrifectaTickets({
        shapeRecommendation,
        firstProbs: enhancedRoleProbabilities.first,
        secondProbs: enhancedRoleProbabilities.second,
        thirdProbs: enhancedRoleProbabilities.third,
        enhancement: hitRateEnhancement,
        confidence: confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct
      });
      if (shapeBasedTrifectaTickets.length === 0) {
        shapeBasedTrifectaTickets = buildShapeBasedTrifectaTickets({
          shapeRecommendation,
          firstProbs: enhancedRoleProbabilities.first,
          secondProbs: enhancedRoleProbabilities.second,
          thirdProbs: enhancedRoleProbabilities.third,
          confidence: confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct
        });
      }
    } catch (error) {
      shapeRecommendation = null;
      shapeBasedTrifectaTickets = [];
      shapeGenerationError = error instanceof Error ? error.message : String(error || "shape_generation_failed");
    }
    bet_plan_with_stake.recommended_bets = mergeShapeBasedTickets(
      bet_plan_with_stake.recommended_bets,
      shapeBasedTrifectaTickets
    );
    ticketOptimizationWithStake.optimized_tickets = mergeShapeBasedTickets(
      ticketOptimizationWithStake.optimized_tickets,
      shapeBasedTrifectaTickets
    );
    const finalRecommendedSnapshot = buildFinalRecommendedBetsSnapshot({
      recommendedBets: bet_plan_with_stake?.recommended_bets,
      optimizedTickets: ticketOptimizationWithStake?.optimized_tickets
    });
    const boat1HeadSnapshot = buildBoat1HeadBetsSnapshot({
      ranking,
      recommendedBets: bet_plan_with_stake?.recommended_bets,
      optimizedTickets: ticketOptimizationWithStake?.optimized_tickets,
      headScenarioBalanceAnalysis,
      escapePatternAnalysis,
      learningWeights,
      race: data?.race || null
    });
    const exactaSnapshot = buildExactaCoverageSnapshot({
      ranking,
      recommendedBets: bet_plan_with_stake?.recommended_bets,
      optimizedTickets: ticketOptimizationWithStake?.optimized_tickets,
      finalRecommendedSnapshot,
      boat1HeadSnapshot,
      headScenarioBalanceAnalysis,
      roleProbabilityLayers: candidateDistributions,
      escapePatternAnalysis,
      attackScenarioAnalysis,
      learningWeights,
      race: data?.race || null
    });
    const roleBasedMainTrifectaTickets = generateMainTrifectaTickets(
      roleBasedOrderCandidates,
      confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct
    );
    const roleBasedMainTrifectaWithShape = mergeShapeBasedTickets(
      roleBasedMainTrifectaTickets,
      shapeBasedTrifectaTickets
    ).slice(0, 8);
    const roleBasedExactaCoverTickets = generateExactaCoverTickets(
      enhancedRoleProbabilities.first,
      enhancedRoleProbabilities.second,
      confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct
    );
    const backupUrasujiSnapshot = buildBackupUrasujiRecommendationsSnapshot({
      recommendedBets: bet_plan_with_stake?.recommended_bets,
      optimizedTickets: ticketOptimizationWithStake?.optimized_tickets,
      candidateDistributions
    });
    const roleBasedBackupUrasujiTickets = generateBackupUrasujiTickets(
      roleBasedOrderCandidates,
      explicitAttackScenarioProbabilities,
      confidenceScores?.bet_confidence_calibrated ?? confidenceScores?.recommended_bet_confidence_pct
    );
    const participationDecision = buildParticipationDecision({
      raceDecision,
      raceRisk,
      raceStructure,
      entryMeta,
      confidenceScores,
      scenarioSuggestions,
      raceFlow,
      escapePatternAnalysis,
      attackScenarioAnalysis,
      headScenarioBalanceAnalysis,
      roleProbabilityLayers: candidateDistributions,
      ranking,
      racers: data?.racers || [],
      exactaSnapshot,
      learningWeights,
      race: data?.race || null
    });
    const topRecommendedTicketsSnapshot = buildTopRecommendedTickets({
      finalRecommendedBets: finalRecommendedSnapshot.items,
      exactaBets: exactaSnapshot.items,
      backupUrasujiBets: backupUrasujiSnapshot.items,
      maxItems: 10
    });
    const upsetRiskScore = computeUpsetRiskScore({
      confidenceScores,
      participationDecision,
      roleProbabilityLayers: candidateDistributions,
      attackScenarioAnalysis,
      headScenarioBalanceAnalysis,
      outsideHeadPromotionGate: candidateDistributions.outside_head_promotion_gate_json
    });
    const upsetAlertSnapshot = buildUpsetAlert({
      upsetRiskScore,
      showUpsetAlert: shouldShowUpsetAlert({
        upsetRiskScore,
        confidenceScores,
        boat1EscapeProbability: toNum(candidateDistributions?.boat1_escape_probability, 0),
        participationDecision
      }),
      attackScenarioAnalysis,
      escapePatternAnalysis,
      roleProbabilityLayers: candidateDistributions,
      outsideHeadPromotionGate: candidateDistributions.outside_head_promotion_gate_json,
      isRecommendedRace: String(participationDecision?.decision || "").toLowerCase() === "recommended",
      backupUrasujiBets: backupUrasujiSnapshot.items,
      finalRecommendedBets: finalRecommendedSnapshot.items
    });
    const startDisplay = saveRaceStartDisplaySnapshot({
      raceId,
      racers: data.racers,
      entryMeta,
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
    const canonicalPerBoatLaneMap =
      entryMeta?.per_boat_lane_map && typeof entryMeta.per_boat_lane_map === "object"
        ? entryMeta.per_boat_lane_map
        : {};
    const snapshotPlayers = safeArray(data?.racers).map((racer) => {
      const lane = toInt(racer?.lane, null);
      const canonicalLaneMeta = canonicalPerBoatLaneMap[String(lane)] || null;
      const canonicalActualLane = toInt(canonicalLaneMeta?.actual_lane, lane);
      const laneFeatures = rankingFeatureByLane.get(lane) || {};
      const predictionFieldMeta =
        racer?.predictionFieldMeta && typeof racer.predictionFieldMeta === "object"
          ? racer.predictionFieldMeta
          : {};
      const lane1stVerified = !!(predictionFieldMeta?.lane1stScore?.is_usable || predictionFieldMeta?.lane1stAvg?.is_usable);
      const lane2renVerified = !!(predictionFieldMeta?.lane2renScore?.is_usable || predictionFieldMeta?.lane2renAvg?.is_usable);
      const lane3renVerified = !!(predictionFieldMeta?.lane3renScore?.is_usable || predictionFieldMeta?.lane3renAvg?.is_usable);
      const rawLane1st = toNullableNum(racer?.lane1stScoreRawParsed ?? racer?.lane1stScore ?? racer?.lane1stAvg ?? racer?.laneFirstRate);
      const rawLane2ren = toNullableNum(racer?.lane2renScoreRawParsed ?? racer?.lane2renScore ?? racer?.lane2renAvg ?? racer?.lane2RenRate);
      const rawLane3ren = toNullableNum(racer?.lane3renScoreRawParsed ?? racer?.lane3renScore ?? racer?.lane3renAvg ?? racer?.lane3RenRate);
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
              lap_time_delta_vs_front: toNullableNum(laneFeatures?.lap_time_delta_vs_front),
              lap_time_rank: toInt(laneFeatures?.lap_time_rank, null),
              lap_time: toNullableNum(laneFeatures?.lap_time),
              lap_attack_flag: toInt(laneFeatures?.lap_attack_flag, 0),
              lap_attack_strength: toNullableNum(laneFeatures?.lap_attack_strength),
              lap_exhibition_score: toNullableNum(laneFeatures?.lap_exhibition_score),
              feature_snapshot: laneFeatures,
              contribution_components: contributionComponents
            }
          : {}),
        lane,
        original_lane: lane,
        actual_lane: canonicalActualLane,
        course_change_occurred: canonicalActualLane !== lane ? 1 : 0,
        registration_no: toInt(racer?.registrationNo, null),
        name: racer?.name || null,
        class: racer?.class || null,
        branch: racer?.branch || null,
        age: toInt(racer?.age, null),
        weight: toNullableNum(racer?.weight),
        avg_st: toNullableNum(racer?.avgSt),
        nationwide_win_rate: toNullableNum(racer?.nationwideWinRate),
        local_win_rate: toNullableNum(racer?.localWinRate),
        official_nationwide_win_rate: toNullableNum(racer?.officialNationwideWinRate),
        official_local_win_rate: toNullableNum(racer?.officialLocalWinRate),
        player_recent_3_months_strength: toNullableNum(racer?.playerRecent3MonthsStrength),
        player_current_season_strength: toNullableNum(racer?.playerCurrentSeasonStrength),
        player_strength_blended: toNullableNum(racer?.playerStrengthBlended),
        player_stat_confidence: toNullableNum(racer?.playerStatConfidence),
        recent_3_months_sample_size: toInt(racer?.recent3MonthsSampleSize, 0),
        current_season_sample_size: toInt(racer?.currentSeasonSampleSize, 0),
        player_stat_fallback_used: toInt(racer?.playerStatFallbackUsed, 0),
        player_stat_windows_used: racer?.playerStatWindowsUsed || null,
        lane_recent_sample_size: toInt(racer?.laneRecentSampleSize, 0),
        motor_no: toInt(racer?.motorNo, null),
        motor_2rate: toNullableNum(racer?.motor2ren ?? racer?.motor2Rate),
        motor_3rate: toNullableNum(racer?.motor3ren ?? racer?.motor3Rate),
        boat_no: toInt(racer?.boatNo, null),
        boat_2rate: toNullableNum(racer?.boat2Rate),
        exhibition_time: toNullableNum(racer?.exhibitionTime),
        exhibition_st: toNullableNum(racer?.exhibitionSt),
        exhibition_st_raw: racer?.exhibitionStRaw || null,
        kyoteibiyori_fetched: toInt(racer?.kyoteiBiyoriFetched, 0),
        kyoteibiyori_lap_time: toNullableNum(racer?.kyoteiBiyoriLapTime),
        kyoteibiyori_lap_time_raw: toNullableNum(racer?.kyoteiBiyoriLapTimeRaw ?? racer?.lapTimeRaw),
        kyoteibiyori_lap_ex_stretch: toNullableNum(racer?.kyoteiBiyoriLapExStretch ?? racer?.lapExStretch),
        kyoteibiyori_lap_exhibition_score: toNullableNum(racer?.kyoteiBiyoriLapExStretch ?? racer?.kyoteiBiyoriLapExhibitionScore),
        kyoteibiyori_stretch_foot_label: racer?.kyoteiBiyoriStretchFootLabel || null,
        kyoteibiyori_exhibition_st: toNullableNum(racer?.kyoteiBiyoriExhibitionSt),
        entry_course: canonicalActualLane,
        tilt: toNullableNum(racer?.tilt),
        lane1st_score: lane1stVerified ? rawLane1st : null,
        lane2ren_score: lane2renVerified ? rawLane2ren : null,
        lane3ren_score: lane3renVerified ? rawLane3ren : null,
        lane1st_score_raw: rawLane1st,
        lane2ren_score_raw: rawLane2ren,
        lane3ren_score_raw: rawLane3ren,
        lane1st_score_before_reassignment: lane1stVerified ? rawLane1st : null,
        lane2ren_score_before_reassignment: lane2renVerified ? rawLane2ren : null,
        lane3ren_score_before_reassignment: lane3renVerified ? rawLane3ren : null,
        lane1st_score_after_reassignment: toNullableNum(laneFeatures?.lane_fit_1st ?? laneFeatures?.laneFirstRate),
        lane2ren_score_after_reassignment: toNullableNum(laneFeatures?.lane_fit_2ren ?? laneFeatures?.lane2RenRate),
        lane3ren_score_after_reassignment: toNullableNum(laneFeatures?.lane_fit_3ren ?? laneFeatures?.lane3RenRate),
        lane_first_rate: lane1stVerified ? rawLane1st : null,
        lane_2ren_rate: lane2renVerified ? rawLane2ren : null,
        lane_3ren_rate: lane3renVerified ? rawLane3ren : null,
        lane_assignment_debug: laneFeatures?.lane_assignment_debug || null,
        prediction_field_meta: predictionFieldMeta || null
      };
    });
    const fetchedSignalDiagnostics = snapshotPlayers.map((row) => ({
      lane: toInt(row?.lane, null),
      lap_time_contribution: toNullableNum(row?.feature_snapshot?.fetched_signal_score_breakdown?.lap_time_contribution),
      exhibition_st_contribution: toNullableNum(row?.feature_snapshot?.fetched_signal_score_breakdown?.exhibition_st_contribution),
      motor_2ren_contribution: toNullableNum(row?.feature_snapshot?.fetched_signal_score_breakdown?.motor_2ren_contribution),
      motor_3ren_contribution: toNullableNum(row?.feature_snapshot?.fetched_signal_score_breakdown?.motor_3ren_contribution),
      signal_only_rank: toInt(row?.feature_snapshot?.fetched_signal_score_breakdown?.signal_only_rank, null),
      final_rank: toInt(row?.feature_snapshot?.fetched_signal_score_breakdown?.final_rank, null),
      changed_first_place_ranking:
        toInt(row?.feature_snapshot?.fetched_signal_score_breakdown?.signal_only_rank, null) !==
        toInt(row?.feature_snapshot?.fetched_signal_score_breakdown?.final_rank, null),
      changed_second_place_ranking: false
    }));
    const supplementalFieldUsage = summarizeSupplementalFieldUsage(snapshotPlayers);
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
      fetched_signal_diagnostics: fetchedSignalDiagnostics,
      prediction_data_usage: predictionDataUsage,
      recommended_shape: shapeRecommendation?.selected_shape || null,
      recommended_shape_debug: shapeRecommendation
        ? {
            ...shapeRecommendation,
            hit_rate_enhancement: hitRateEnhancement,
            shape_generation_error: shapeGenerationError
          }
        : shapeGenerationError
          ? { shape_generation_error: shapeGenerationError }
          : null,
      kyoteibiyori_fetch_status_json: data?.source?.kyotei_biyori || {},
      entry: {
        predicted_entry_order: entryMeta.predicted_entry_order,
        actual_entry_order: entryMeta.actual_entry_order,
        actual_lane_map: entryMeta.actual_lane_map,
        per_boat_lane_map: entryMeta.per_boat_lane_map,
        start_exhibition_st: startDisplay?.start_display_st || {},
        start_display_order: startDisplay?.start_display_order || [],
        start_display_timing: startDisplay?.start_display_timing || {},
        entry_changed: !!entryMeta.entry_changed,
        entry_change_type: entryMeta.entry_change_type || "none",
        validation: entryMeta.validation,
        fallback_used: !!entryMeta.fallback_used,
        fallback_reason: entryMeta.fallback_reason || null,
        raw_actual_entry_source_text: entryMeta.raw_actual_entry_source_text || null,
        entry_change_summary: {
          changed: !!entryMeta.entry_changed,
          type: entryMeta.entry_change_type || "none",
          predicted: entryMeta.predicted_entry_order || [],
          actual: entryMeta.actual_entry_order || []
        },
        supplemental_fields_usable: supplementalFieldUsage.usable,
        supplemental_fields_skipped: supplementalFieldUsage.skipped
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
      formation_first_place_prior_json: headScenarioBalanceAnalysis.formation_first_place_prior_json,
      first_place_distribution_json: headScenarioBalanceAnalysis.first_place_distribution_json,
      first_place_probability_json: headScenarioBalanceAnalysis.first_place_probability_json,
      head_distribution_json: headScenarioBalanceAnalysis.head_distribution_json,
      baseline_head_distribution_json: headScenarioBalanceAnalysis.baseline_head_distribution_json,
      second_distribution_json: headScenarioBalanceAnalysis.second_distribution_json,
      second_place_distribution_json: headScenarioBalanceAnalysis.second_place_distribution_json,
      second_place_probability_json: headScenarioBalanceAnalysis.second_place_probability_json,
      third_place_distribution_json: headScenarioBalanceAnalysis.third_place_distribution_json,
      third_place_probability_json: headScenarioBalanceAnalysis.third_place_probability_json,
      boat1_second_place_distribution_json: headScenarioBalanceAnalysis.boat1_second_place_distribution_json,
      boat1_second_place_probability_json: headScenarioBalanceAnalysis.boat1_second_place_probability_json,
      boat1_third_place_distribution_json: headScenarioBalanceAnalysis.boat1_third_place_distribution_json,
      boat1_third_place_probability_json: headScenarioBalanceAnalysis.boat1_third_place_probability_json,
      survival_probability_json: headScenarioBalanceAnalysis.survival_probability_json,
      boat1_escape_probability: toNullableNum(headScenarioBalanceAnalysis.boat1_escape_probability),
      attack_scenario_probability_json: headScenarioBalanceAnalysis.attack_scenario_probability_json || [],
      role_probability_summary_json: headScenarioBalanceAnalysis.role_probability_summary_json || {},
      role_probability_version: headScenarioBalanceAnalysis.role_probability_version || null,
      role_based_order_candidates_json: roleBasedOrderCandidates,
      evidence_bias_table_json: evidenceBiasTable,
      player_stat_window_policy_json: PLAYER_STAT_WINDOW_POLICY,
      player_stat_windows_used_json: safeArray(data?.racers).map((racer) => ({
        lane: toInt(racer?.lane, null),
        registration_no: toInt(racer?.registrationNo, null),
        recent_3_months_sample_size: toInt(racer?.recent3MonthsSampleSize, 0),
        current_season_sample_size: toInt(racer?.currentSeasonSampleSize, 0),
        player_stat_confidence: toNullableNum(racer?.playerStatConfidence),
        windows: racer?.playerStatWindowsUsed || null
      })),
      launch_state_thresholds_used_json: headScenarioBalanceAnalysis.launch_state_thresholds_used_json || {},
      launch_venue_calibration_json: headScenarioBalanceAnalysis.launch_venue_calibration_json || {},
      launch_state_scores_json: headScenarioBalanceAnalysis.launch_state_scores_json || [],
      launch_state_labels_json: headScenarioBalanceAnalysis.launch_state_labels_json || [],
      intermediate_development_events_json: headScenarioBalanceAnalysis.intermediate_development_events_json || {},
      race_scenario_probabilities_json: headScenarioBalanceAnalysis.race_scenario_probabilities_json || [],
      finish_probabilities_by_scenario_json: headScenarioBalanceAnalysis.finish_probabilities_by_scenario_json || [],
      finish_override_strength_by_lane_json: headScenarioBalanceAnalysis.finish_override_strength_by_lane_json || {},
      scenario_based_order_candidates_json: headScenarioBalanceAnalysis.scenario_based_order_candidates_json || [],
      matched_dictionary_scenarios_json: headScenarioBalanceAnalysis.matched_dictionary_scenarios_json || [],
      dictionary_scenario_match_scores_json: headScenarioBalanceAnalysis.dictionary_scenario_match_scores_json || [],
      dictionary_prior_adjustment_json: headScenarioBalanceAnalysis.dictionary_prior_adjustment_json || {},
      dictionary_condition_flags_json: headScenarioBalanceAnalysis.dictionary_condition_flags_json || [],
      dictionary_representative_ticket_priors_json: headScenarioBalanceAnalysis.dictionary_representative_ticket_priors_json || [],
      dictionary_cd_scenarios_activated: toInt(headScenarioBalanceAnalysis.dictionary_cd_scenarios_activated, 0),
      confirmed_first_place_probability_json: confirmedRoleProbabilities.confirmed_first_place_probability_json,
      confirmed_second_place_probability_json: confirmedRoleProbabilities.confirmed_second_place_probability_json,
      confirmed_third_place_probability_json: confirmedRoleProbabilities.confirmed_third_place_probability_json,
      enhanced_first_place_probability_json: enhancedRoleProbabilities.first,
      enhanced_second_place_probability_json: enhancedRoleProbabilities.second,
      enhanced_third_place_probability_json: enhancedRoleProbabilities.third,
      hit_rate_enhancement_json: hitRateEnhancement,
      scenarioProbabilities: hitRateEnhancement?.scenarioProbabilities || [],
      finishProbabilitiesByScenario: hitRateEnhancement?.finishProbabilitiesByScenario || [],
      aggregatedFinishProbabilities: hitRateEnhancement?.aggregatedFinishProbabilities || {},
      expandedShapeTickets: shapeBasedTrifectaTickets || [],
      style_profile_by_lane_json: hitRateEnhancement?.stage1_static?.style_profile_by_lane || {},
      escape_score: toNullableNum(hitRateEnhancement?.stage1_static?.escape_score),
      start_development_states_json: hitRateEnhancement?.startDevelopmentStates || {},
      intermediate_events_json: hitRateEnhancement?.intermediateEvents || {},
      start_edge_by_lane_json: hitRateEnhancement?.stage2_dynamic?.start_edge_by_lane || {},
      late_risk_by_lane_json: hitRateEnhancement?.stage2_dynamic?.late_risk_by_lane || {},
      hidden_f_by_lane_json: hitRateEnhancement?.stage1_static?.hidden_F_by_lane || {},
      motivation_by_lane_json: hitRateEnhancement?.stage1_static?.motivation_by_lane || {},
      motor_true_by_lane_json: hitRateEnhancement?.stage1_static?.motor_true_by_lane || {},
      lane_fit_by_lane_json: hitRateEnhancement?.stage2_dynamic?.lane_fit_by_lane || {},
      enhanced_scenario_probabilities_json: hitRateEnhancement?.stage3_scenarios?.selected_scenario_probabilities || [],
      enhanced_ticket_shape_json: shapeRecommendation || null,
      enhanced_ticket_shape_reason: shapeRecommendation?.why_shape_chosen || null,
      recommendedShape: shapeRecommendation || null,
      expandedShapeTickets: shapeBasedTrifectaTickets || [],
      scenario_tree_order_candidates_json: scenarioTreeOrderCandidates || [],
      dark_horse_alerts_json: hitRateEnhancement?.dark_horse_alerts || [],
      outside_head_promotion_gate_json: candidateDistributions.outside_head_promotion_gate_json || {},
      partner_search_bias_json: headScenarioBalanceAnalysis.partner_search_bias_json,
      boat1_partner_search_bias_json: headScenarioBalanceAnalysis.partner_search_bias_json,
      boat1_partner_bias_json: headScenarioBalanceAnalysis.boat1_partner_bias_json,
      boat1_partner_reason_tags: headScenarioBalanceAnalysis.boat1_partner_reason_tags,
      partner_search_lap_bias_json: headScenarioBalanceAnalysis.partner_search_lap_bias_json,
      venue_correction_summary: headScenarioBalanceAnalysis.venue_correction_summary,
      third_place_residual_bias_json: headScenarioBalanceAnalysis.third_place_residual_bias_json,
      boat1_partner_search_applied: toInt(headScenarioBalanceAnalysis.boat1_partner_search_applied, 0),
      boat1_partner_model_applied: toInt(headScenarioBalanceAnalysis.boat1_partner_model_applied, 0),
      boat1_escape_partner_version: headScenarioBalanceAnalysis.boat1_escape_partner_version,
      stronger_lap_bias_applied: toInt(headScenarioBalanceAnalysis.stronger_lap_bias_applied, 0),
      inside_baseline_priority_applied: toInt(headScenarioBalanceAnalysis.inside_baseline_priority_applied, 0),
      candidate_balance_adjustment_json: headScenarioBalanceAnalysis.candidate_balance_adjustment_json || {},
      aggressive_adjustment_json: headScenarioBalanceAnalysis.aggressive_adjustment_json,
      scoring_family_components_json: headScenarioBalanceAnalysis.scoring_family_components_json,
      rebalance_version: headScenarioBalanceAnalysis.rebalance_version,
      boat3_weak_st_head_suppression_json: headScenarioBalanceAnalysis.boat3_weak_st_head_suppression_json || {},
      boat3_weak_st_head_suppressed: toInt(headScenarioBalanceAnalysis.boat3_weak_st_head_suppressed, 0),
      inner_course_bias_applied: ranking.some((row) => toInt(row?.features?.inner_course_bias_applied, 0) === 1) ? 1 : 0,
      stronger_inner_bias_applied: toInt(headScenarioBalanceAnalysis.stronger_inner_bias_applied, 0),
      outer_head_guard_applied: toInt(headScenarioBalanceAnalysis.outer_head_guard_applied, 0),
      survival_guard_applied: toInt(headScenarioBalanceAnalysis.survival_guard_applied, 0),
      boat1_survival_guard_strength: toNullableNum(headScenarioBalanceAnalysis.boat1_survival_guard_strength),
      outer_head_promotion_threshold: toNullableNum(headScenarioBalanceAnalysis.outer_head_promotion_threshold),
      final_balance_adjustment_json: finalBalanceAdjustmentJson,
      exacta_inside_bias_applied: 1,
      hit_rate_focus_applied: 1,
      removed_candidate_reason_tags: Array.isArray(headScenarioBalanceAnalysis.removed_candidate_reason_tags)
        ? headScenarioBalanceAnalysis.removed_candidate_reason_tags
        : [],
      boat1_head_bets_snapshot: boat1HeadSnapshot.items,
      boat1_priority_mode_applied: toInt(headScenarioBalanceAnalysis.boat1_priority_mode_applied, 0),
      boat1_head_ratio_in_final_bets: toNullableNum(headScenarioBalanceAnalysis.boat1_head_ratio_in_final_bets),
      boat1_head_score: boat1HeadSnapshot.boat1_head_score,
      boat1_survival_residual_score: boat1HeadSnapshot.boat1_survival_residual_score,
      boat1_head_section_shown: boat1HeadSnapshot.shown ? 1 : 0,
      boat1_head_top8_generated: boat1HeadSnapshot.boat1_head_top8_generated,
      boat1_head_reason_tags: [...new Set([...
        boat1HeadSnapshot.boat1_head_reason_tags,
        ...safeArray(headScenarioBalanceAnalysis.boat1_priority_reason_tags)
      ])],
      exacta_recommended_bets_snapshot: exactaSnapshot.items,
      exacta_head_score: exactaSnapshot.exacta_head_score,
      exacta_partner_score: exactaSnapshot.exacta_partner_score,
      exacta_reason_tags: exactaSnapshot.exacta_reason_tags,
      exacta_section_shown: exactaSnapshot.shown ? 1 : 0,
      role_based_main_trifecta_tickets_snapshot: roleBasedMainTrifectaWithShape,
      role_based_exacta_cover_tickets_snapshot: roleBasedExactaCoverTickets,
      role_based_backup_urasuji_tickets_snapshot: roleBasedBackupUrasujiTickets,
      backup_urasuji_recommendations_snapshot: backupUrasujiSnapshot.items,
      backup_urasuji_section_shown: backupUrasujiSnapshot.shown ? 1 : 0,
      backup_urasuji_reason_tags: backupUrasujiSnapshot.backup_reason_tags,
      top_recommended_tickets_snapshot: topRecommendedTicketsSnapshot,
      upset_alert_snapshot: upsetAlertSnapshot,
      formation_pattern: escapePatternAnalysis.formation_pattern,
      escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
      escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence,
      escape_second_place_bias_json: escapePatternAnalysis.escape_second_place_bias_json,
      f_hold_bias_applied: ranking.some((row) => toNum(row?.features?.f_hold_bias_applied, 0) > 0) ? 1 : 0,
      data_quality_score: toNullableNum(participationDecision?.metrics?.data_quality_score),
      race_stability_score: toNullableNum(participationDecision?.metrics?.race_stability_score),
      partner_clarity_score: toNullableNum(participationDecision?.metrics?.partner_clarity_score),
      quality_gate_applied: toInt(participationDecision?.metrics?.quality_gate_applied, 0),
      gating_adjustment_json: participationDecision?.participation_score_components?.gating_adjustment_json || {},
      prediction_readability_score: toNullableNum(participationDecision?.metrics?.prediction_readability_score),
      exacta_stability_score: toNullableNum(participationDecision?.metrics?.exacta_stability_score),
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
      data_quality_score: snapshotContext.data_quality_score,
      race_stability_score: snapshotContext.race_stability_score,
      partner_clarity_score: snapshotContext.partner_clarity_score,
      quality_gate_applied: snapshotContext.quality_gate_applied,
      gating_adjustment_json: snapshotContext.gating_adjustment_json,
      prediction_readability_score: snapshotContext.prediction_readability_score,
      exacta_stability_score: snapshotContext.exacta_stability_score,
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
      prediction_data_usage: predictionDataUsage,
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
      formation_first_place_prior_json: snapshotContext.formation_first_place_prior_json,
      first_place_distribution_json: snapshotContext.first_place_distribution_json,
      first_place_probability_json: snapshotContext.first_place_probability_json,
      head_distribution_json: snapshotContext.head_distribution_json,
      baseline_head_distribution_json: snapshotContext.baseline_head_distribution_json,
      second_distribution_json: snapshotContext.second_distribution_json,
      second_place_distribution_json: snapshotContext.second_place_distribution_json,
      second_place_probability_json: snapshotContext.second_place_probability_json,
      third_place_distribution_json: snapshotContext.third_place_distribution_json,
      third_place_probability_json: snapshotContext.third_place_probability_json,
      boat1_second_place_distribution_json: snapshotContext.boat1_second_place_distribution_json,
      boat1_second_place_probability_json: snapshotContext.boat1_second_place_probability_json,
      boat1_third_place_distribution_json: snapshotContext.boat1_third_place_distribution_json,
      boat1_third_place_probability_json: snapshotContext.boat1_third_place_probability_json,
      survival_probability_json: snapshotContext.survival_probability_json,
      boat1_escape_probability: snapshotContext.boat1_escape_probability,
      attack_scenario_probability_json: snapshotContext.attack_scenario_probability_json,
      role_probability_summary_json: snapshotContext.role_probability_summary_json,
      role_probability_version: snapshotContext.role_probability_version,
      role_based_order_candidates_json: snapshotContext.role_based_order_candidates_json,
      evidence_bias_table_json: snapshotContext.evidence_bias_table_json,
      confirmed_first_place_probability_json: snapshotContext.confirmed_first_place_probability_json,
      confirmed_second_place_probability_json: snapshotContext.confirmed_second_place_probability_json,
      confirmed_third_place_probability_json: snapshotContext.confirmed_third_place_probability_json,
      enhanced_first_place_probability_json: snapshotContext.enhanced_first_place_probability_json,
      enhanced_second_place_probability_json: snapshotContext.enhanced_second_place_probability_json,
      enhanced_third_place_probability_json: snapshotContext.enhanced_third_place_probability_json,
      hit_rate_enhancement_json: snapshotContext.hit_rate_enhancement_json,
      scenarioProbabilities: snapshotContext.scenarioProbabilities,
      finishProbabilitiesByScenario: snapshotContext.finishProbabilitiesByScenario,
      aggregatedFinishProbabilities: snapshotContext.aggregatedFinishProbabilities,
      start_development_states_json: snapshotContext.start_development_states_json,
      intermediate_events_json: snapshotContext.intermediate_events_json,
      scenario_tree_order_candidates_json: snapshotContext.scenario_tree_order_candidates_json,
      style_profile_by_lane_json: snapshotContext.style_profile_by_lane_json,
      escape_score: snapshotContext.escape_score,
      start_edge_by_lane_json: snapshotContext.start_edge_by_lane_json,
      late_risk_by_lane_json: snapshotContext.late_risk_by_lane_json,
      hidden_f_by_lane_json: snapshotContext.hidden_f_by_lane_json,
      motivation_by_lane_json: snapshotContext.motivation_by_lane_json,
      motor_true_by_lane_json: snapshotContext.motor_true_by_lane_json,
      lane_fit_by_lane_json: snapshotContext.lane_fit_by_lane_json,
      enhanced_scenario_probabilities_json: snapshotContext.enhanced_scenario_probabilities_json,
      enhanced_ticket_shape_json: snapshotContext.enhanced_ticket_shape_json,
      enhanced_ticket_shape_reason: snapshotContext.enhanced_ticket_shape_reason,
      recommendedShape: snapshotContext.recommendedShape,
      expandedShapeTickets: snapshotContext.expandedShapeTickets,
      dark_horse_alerts_json: snapshotContext.dark_horse_alerts_json,
      outside_head_promotion_gate_json: snapshotContext.outside_head_promotion_gate_json,
      partner_search_bias_json: snapshotContext.partner_search_bias_json,
      boat1_partner_search_bias_json: snapshotContext.boat1_partner_search_bias_json,
      boat1_partner_bias_json: snapshotContext.boat1_partner_bias_json,
      boat1_partner_reason_tags: snapshotContext.boat1_partner_reason_tags,
      partner_search_lap_bias_json: snapshotContext.partner_search_lap_bias_json,
      venue_correction_summary: snapshotContext.venue_correction_summary,
      third_place_residual_bias_json: snapshotContext.third_place_residual_bias_json,
      boat1_partner_search_applied: snapshotContext.boat1_partner_search_applied,
      boat1_partner_model_applied: snapshotContext.boat1_partner_model_applied,
      boat1_escape_partner_version: snapshotContext.boat1_escape_partner_version,
      stronger_lap_bias_applied: snapshotContext.stronger_lap_bias_applied,
      inside_baseline_priority_applied: snapshotContext.inside_baseline_priority_applied,
      candidate_balance_adjustment_json: snapshotContext.candidate_balance_adjustment_json,
      aggressive_adjustment_json: snapshotContext.aggressive_adjustment_json,
      scoring_family_components_json: snapshotContext.scoring_family_components_json,
      rebalance_version: snapshotContext.rebalance_version,
      inner_course_bias_applied: snapshotContext.inner_course_bias_applied,
      stronger_inner_bias_applied: snapshotContext.stronger_inner_bias_applied,
      outer_head_guard_applied: snapshotContext.outer_head_guard_applied,
      survival_guard_applied: snapshotContext.survival_guard_applied,
      boat1_survival_guard_strength: snapshotContext.boat1_survival_guard_strength,
      outer_head_promotion_threshold: snapshotContext.outer_head_promotion_threshold,
      final_balance_adjustment_json: snapshotContext.final_balance_adjustment_json,
      exacta_inside_bias_applied: snapshotContext.exacta_inside_bias_applied,
      hit_rate_focus_applied: snapshotContext.hit_rate_focus_applied,
      removed_candidate_reason_tags: snapshotContext.removed_candidate_reason_tags,
      boat1_head_bets_snapshot: snapshotContext.boat1_head_bets_snapshot,
      boat1_priority_mode_applied: snapshotContext.boat1_priority_mode_applied,
      boat1_head_ratio_in_final_bets: snapshotContext.boat1_head_ratio_in_final_bets,
      boat1_head_score: snapshotContext.boat1_head_score,
      boat1_survival_residual_score: snapshotContext.boat1_survival_residual_score,
      boat1_head_section_shown: snapshotContext.boat1_head_section_shown,
      boat1_head_top8_generated: snapshotContext.boat1_head_top8_generated,
      boat1_head_reason_tags: snapshotContext.boat1_head_reason_tags,
      exacta_recommended_bets_snapshot: snapshotContext.exacta_recommended_bets_snapshot,
      exacta_head_score: snapshotContext.exacta_head_score,
      exacta_partner_score: snapshotContext.exacta_partner_score,
      exacta_reason_tags: snapshotContext.exacta_reason_tags,
      exacta_section_shown: snapshotContext.exacta_section_shown,
      role_based_main_trifecta_tickets_snapshot: snapshotContext.role_based_main_trifecta_tickets_snapshot,
      role_based_exacta_cover_tickets_snapshot: snapshotContext.role_based_exacta_cover_tickets_snapshot,
      role_based_backup_urasuji_tickets_snapshot: snapshotContext.role_based_backup_urasuji_tickets_snapshot,
      backup_urasuji_recommendations_snapshot: snapshotContext.backup_urasuji_recommendations_snapshot,
      backup_urasuji_section_shown: snapshotContext.backup_urasuji_section_shown,
      backup_urasuji_reason_tags: snapshotContext.backup_urasuji_reason_tags,
      formation_pattern: escapePatternAnalysis.formation_pattern,
      escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
      escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence,
      escape_second_place_bias_json: escapePatternAnalysis.escape_second_place_bias_json,
      f_hold_bias_applied: ranking.some((row) => toNum(row?.features?.f_hold_bias_applied, 0) > 0) ? 1 : 0,
      participation_decision: participationDecision.decision,
      participation_decision_reason: participationDecision.summary,
      participate_watch_skip_reason_tags: participationDecision.reason_tags,
      data_quality_score: snapshotContext.data_quality_score,
      race_stability_score: snapshotContext.race_stability_score,
      partner_clarity_score: snapshotContext.partner_clarity_score,
      quality_gate_applied: snapshotContext.quality_gate_applied,
      gating_adjustment_json: snapshotContext.gating_adjustment_json,
      prediction_readability_score: snapshotContext.prediction_readability_score,
      exacta_stability_score: snapshotContext.exacta_stability_score,
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
      prediction_data_usage: predictionDataUsage,
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
      formation_first_place_prior_json: snapshotContext.formation_first_place_prior_json,
      first_place_distribution_json: snapshotContext.first_place_distribution_json,
      first_place_probability_json: snapshotContext.first_place_probability_json,
      head_distribution_json: snapshotContext.head_distribution_json,
      baseline_head_distribution_json: snapshotContext.baseline_head_distribution_json,
      second_distribution_json: snapshotContext.second_distribution_json,
      second_place_distribution_json: snapshotContext.second_place_distribution_json,
      second_place_probability_json: snapshotContext.second_place_probability_json,
      third_place_distribution_json: snapshotContext.third_place_distribution_json,
      third_place_probability_json: snapshotContext.third_place_probability_json,
      boat1_second_place_distribution_json: snapshotContext.boat1_second_place_distribution_json,
      boat1_second_place_probability_json: snapshotContext.boat1_second_place_probability_json,
      boat1_third_place_distribution_json: snapshotContext.boat1_third_place_distribution_json,
      boat1_third_place_probability_json: snapshotContext.boat1_third_place_probability_json,
      survival_probability_json: snapshotContext.survival_probability_json,
      boat1_escape_probability: snapshotContext.boat1_escape_probability,
      attack_scenario_probability_json: snapshotContext.attack_scenario_probability_json,
      role_probability_summary_json: snapshotContext.role_probability_summary_json,
      role_probability_version: snapshotContext.role_probability_version,
      role_based_order_candidates_json: snapshotContext.role_based_order_candidates_json,
      evidence_bias_table_json: snapshotContext.evidence_bias_table_json,
      confirmed_first_place_probability_json: snapshotContext.confirmed_first_place_probability_json,
      confirmed_second_place_probability_json: snapshotContext.confirmed_second_place_probability_json,
      confirmed_third_place_probability_json: snapshotContext.confirmed_third_place_probability_json,
      outside_head_promotion_gate_json: snapshotContext.outside_head_promotion_gate_json,
      partner_search_bias_json: snapshotContext.partner_search_bias_json,
      boat1_partner_search_bias_json: snapshotContext.boat1_partner_search_bias_json,
      boat1_partner_bias_json: snapshotContext.boat1_partner_bias_json,
      boat1_partner_reason_tags: snapshotContext.boat1_partner_reason_tags,
      partner_search_lap_bias_json: snapshotContext.partner_search_lap_bias_json,
      venue_correction_summary: snapshotContext.venue_correction_summary,
      third_place_residual_bias_json: snapshotContext.third_place_residual_bias_json,
      boat1_partner_search_applied: snapshotContext.boat1_partner_search_applied,
      boat1_partner_model_applied: snapshotContext.boat1_partner_model_applied,
      boat1_escape_partner_version: snapshotContext.boat1_escape_partner_version,
      stronger_lap_bias_applied: snapshotContext.stronger_lap_bias_applied,
      inside_baseline_priority_applied: snapshotContext.inside_baseline_priority_applied,
      candidate_balance_adjustment_json: snapshotContext.candidate_balance_adjustment_json,
      aggressive_adjustment_json: snapshotContext.aggressive_adjustment_json,
      scoring_family_components_json: snapshotContext.scoring_family_components_json,
      rebalance_version: snapshotContext.rebalance_version,
      inner_course_bias_applied: snapshotContext.inner_course_bias_applied,
      stronger_inner_bias_applied: snapshotContext.stronger_inner_bias_applied,
      outer_head_guard_applied: snapshotContext.outer_head_guard_applied,
      survival_guard_applied: snapshotContext.survival_guard_applied,
      boat1_survival_guard_strength: snapshotContext.boat1_survival_guard_strength,
      outer_head_promotion_threshold: snapshotContext.outer_head_promotion_threshold,
      final_balance_adjustment_json: snapshotContext.final_balance_adjustment_json,
      exacta_inside_bias_applied: snapshotContext.exacta_inside_bias_applied,
      hit_rate_focus_applied: snapshotContext.hit_rate_focus_applied,
      removed_candidate_reason_tags: snapshotContext.removed_candidate_reason_tags,
      boat1_head_bets_snapshot: snapshotContext.boat1_head_bets_snapshot,
      boat1_priority_mode_applied: snapshotContext.boat1_priority_mode_applied,
      boat1_head_ratio_in_final_bets: snapshotContext.boat1_head_ratio_in_final_bets,
      boat1_head_score: snapshotContext.boat1_head_score,
      boat1_survival_residual_score: snapshotContext.boat1_survival_residual_score,
      boat1_head_section_shown: snapshotContext.boat1_head_section_shown,
      boat1_head_top8_generated: snapshotContext.boat1_head_top8_generated,
      boat1_head_reason_tags: snapshotContext.boat1_head_reason_tags,
      exacta_recommended_bets_snapshot: snapshotContext.exacta_recommended_bets_snapshot,
      exacta_head_score: snapshotContext.exacta_head_score,
      exacta_partner_score: snapshotContext.exacta_partner_score,
      exacta_reason_tags: snapshotContext.exacta_reason_tags,
      exacta_section_shown: snapshotContext.exacta_section_shown,
      role_based_main_trifecta_tickets_snapshot: snapshotContext.role_based_main_trifecta_tickets_snapshot,
      role_based_exacta_cover_tickets_snapshot: snapshotContext.role_based_exacta_cover_tickets_snapshot,
      role_based_backup_urasuji_tickets_snapshot: snapshotContext.role_based_backup_urasuji_tickets_snapshot,
      backup_urasuji_recommendations_snapshot: snapshotContext.backup_urasuji_recommendations_snapshot,
      backup_urasuji_section_shown: snapshotContext.backup_urasuji_section_shown,
      backup_urasuji_reason_tags: snapshotContext.backup_urasuji_reason_tags,
      escape_pattern_applied: escapePatternAnalysis.escape_pattern_applied ? 1 : 0,
      escape_second_place_bias_json: escapePatternAnalysis.escape_second_place_bias_json,
      escape_pattern_confidence: escapePatternAnalysis.escape_pattern_confidence,
      f_hold_bias_applied: ranking.some((row) => toNum(row?.features?.f_hold_bias_applied, 0) > 0) ? 1 : 0,
      participation_decision_reason: participationDecision.summary,
      participate_watch_skip_reason_tags: participationDecision.reason_tags,
      participation_score_components: participationDecision.participation_score_components,
      participation_version: participationDecision.participation_version,
      boat1_head_bets_snapshot: boat1HeadSnapshot.items,
      boat1_priority_mode_applied: toInt(headScenarioBalanceAnalysis.boat1_priority_mode_applied, 0),
      boat1_head_ratio_in_final_bets: toNullableNum(headScenarioBalanceAnalysis.boat1_head_ratio_in_final_bets),
      boat1_head_score: boat1HeadSnapshot.boat1_head_score,
      boat1_survival_residual_score: boat1HeadSnapshot.boat1_survival_residual_score,
      boat1_head_section_shown: boat1HeadSnapshot.shown ? 1 : 0,
      boat1_head_top8_generated: boat1HeadSnapshot.boat1_head_top8_generated,
      boat1_head_reason_tags: [...new Set([...
        boat1HeadSnapshot.boat1_head_reason_tags,
        ...safeArray(headScenarioBalanceAnalysis.boat1_priority_reason_tags)
      ])],
      recommended_shape: shapeRecommendation?.selected_shape || null,
      recommended_shape_debug: shapeRecommendation
        ? {
            ...shapeRecommendation,
            hit_rate_enhancement: hitRateEnhancement,
            shape_generation_error: shapeGenerationError
          }
        : shapeGenerationError
          ? { shape_generation_error: shapeGenerationError }
          : null,
      exacta_recommended_bets_snapshot: exactaSnapshot.items,
      exacta_head_score: exactaSnapshot.exacta_head_score,
      exacta_partner_score: exactaSnapshot.exacta_partner_score,
      exacta_reason_tags: exactaSnapshot.exacta_reason_tags,
      exacta_section_shown: exactaSnapshot.shown ? 1 : 0,
      final_recommended_bets_snapshot: finalRecommendedSnapshot.items,
      final_recommended_bets_count: finalRecommendedSnapshot.items.length,
      final_recommended_bets_snapshot_source: finalRecommendedSnapshot.snapshot_source,
      top_recommended_tickets_snapshot: topRecommendedTicketsSnapshot,
      upset_alert_snapshot: upsetAlertSnapshot,
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
        boat1_head_bets: boat1HeadSnapshot.items,
        boat1_priority_mode_applied: toInt(headScenarioBalanceAnalysis.boat1_priority_mode_applied, 0),
        exacta_recommended_bets: exactaSnapshot.items,
        backup_urasuji_recommendations: backupUrasujiSnapshot.items,
        role_based_main_trifecta_tickets: roleBasedMainTrifectaWithShape,
        role_based_exacta_cover_tickets: roleBasedExactaCoverTickets,
        role_based_backup_urasuji_tickets: roleBasedBackupUrasujiTickets
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
              raw_entry_order: toInt(r?.startRaw?.authoritativeRawEntryCourse, null),
              raw_st_string: r?.startRaw?.fallbackRawSt ?? r?.startRaw?.rawSt ?? r?.exhibitionStRaw ?? null,
              source_block:
                "official_beforeinfo_entry_course",
              raw_beforeinfo_st: r?.startRaw?.rawSt ?? null,
              raw_start_exhibition_st: r?.startRaw?.fallbackRawSt ?? null
            }))
            .sort((a, b) => toInt(a.boat_no, 0) - toInt(b.boat_no, 0)),
          layer2_normalized_by_boat: (Array.isArray(data?.racers) ? data.racers : [])
            .map((r) => ({
              boat_no: toInt(r?.lane, null),
              normalized_entry_order: toInt(entryMeta?.actual_lane_map?.[String(toInt(r?.lane, null))], toInt(r?.lane, null)),
              normalized_st_raw: r?.exhibitionStRaw ?? null,
              normalized_st_type: r?.exhibitionStType ?? "missing",
              normalized_st_numeric: toNullableNum(r?.exhibitionStNumeric)
            }))
            .sort((a, b) => toInt(a.boat_no, 0) - toInt(b.boat_no, 0)),
          layer3_render_input: (Array.isArray(startDisplay?.start_display_debug) ? startDisplay.start_display_debug : [])
            .map((row) => ({
              displayed_boat_no: toInt(row?.lane, null),
              displayed_entry_label: toInt(row?.entry_order, null),
              displayed_st_string:
                startDisplay?.start_display_timing?.[String(toInt(row?.lane, 0))]?.display ?? "--",
              computed_visual_unit: toNullableNum(row?.visual_unit),
              computed_percent_position: toNullableNum(row?.visual_percent)
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
    failureWhere = "race.route:response_build";
    const safeScenarioSuggestions =
      scenarioSuggestions && typeof scenarioSuggestions === "object"
        ? scenarioSuggestions
        : {};
    const compactEntryDebug = {
      authoritative_source: entryMeta.authoritative_source,
      raw_actual_entry_source_text: entryMeta.raw_actual_entry_source_text,
      parsed_entry_by_boat:
        data?.source?.actual_entry?.parsed_entry_by_boat && typeof data.source.actual_entry.parsed_entry_by_boat === "object"
          ? data.source.actual_entry.parsed_entry_by_boat
          : {},
      parsed_actual_entry_order: Array.isArray(data?.source?.actual_entry?.parsed_actual_entry_order)
        ? data.source.actual_entry.parsed_actual_entry_order
        : entryMeta.actual_entry_order,
      actual_lane_map:
        data?.source?.actual_entry?.actual_lane_map && typeof data.source.actual_entry.actual_lane_map === "object"
          ? data.source.actual_entry.actual_lane_map
          : entryMeta.actual_lane_map,
      validation_passed: entryMeta?.validation?.validation_ok === true,
      authoritative_entry_usable: data?.source?.actual_entry?.validation_ok === true,
      fallback_used: !!entryMeta.fallback_used,
      fallback_reason: entryMeta.fallback_reason || null,
      per_boat_lanes: entryMeta.per_boat_lane_map,
      supplemental_fields_usable: supplementalFieldUsage.usable,
      supplemental_fields_skipped: supplementalFieldUsage.skipped
    };
    const safeRecommendedShape = shapeRecommendation?.selected_shape
      ? {
          shape: shapeRecommendation.selected_shape,
          expanded_tickets: Array.isArray(shapeRecommendation.expanded_tickets)
            ? shapeRecommendation.expanded_tickets
            : [],
          reason_tags: Array.isArray(shapeRecommendation.reason_tags)
            ? shapeRecommendation.reason_tags
            : [],
          concentration_metrics: shapeRecommendation.concentration_metrics || null,
          shape_generation_error: shapeGenerationError
        }
      : shapeGenerationError
        ? {
            shape: null,
            expanded_tickets: [],
            reason_tags: [],
            concentration_metrics: null,
            shape_generation_error: shapeGenerationError
          }
        : null;
    const safeFinishProbabilitiesByScenario = Array.isArray(headScenarioBalanceAnalysis?.finish_probabilities_by_scenario_json)
      ? headScenarioBalanceAnalysis.finish_probabilities_by_scenario_json
      : Array.isArray(candidateDistributions?.finish_probabilities_by_scenario_json)
        ? candidateDistributions.finish_probabilities_by_scenario_json
        : [];
    const dataAudit = buildStrictDataAudit({
      data,
      entryMeta,
      hitRateEnhancement
    });
    const hardRaceResponseContract = isHardRaceScreening
      ? {
          race_no: toInt(data?.race?.raceNo, null),
          status: "FETCHED",
          data_status: "OK",
          hard_race_score: null,
          boat1_anchor_score: null,
          boat1_escape_trust: null,
          box_234_fit_score: null,
          opponent_234_fit: null,
          pair23_fit: null,
          pair24_fit: null,
          pair34_fit: null,
          kill_escape_risk: null,
          shape_shuffle_risk: null,
          makuri_risk: null,
          outside_break_risk: null,
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
          p_143: null,
          suggested_shape: null,
          recommendation: null,
          decision: null,
          decision_reason: null,
          errors: [],
          missing_fields: []
        }
      : null;
    routeTimings.prediction_build_ms = Date.now() - predictionStartedAt;
    routeTimings.total_response_ms = Date.now() - routeStartedAt;
    console.info(
      "[RACE_API_TIMING]",
      JSON.stringify({
        route: "/api/race",
        date,
        venueId: toInt(venueId, null),
        raceNo: toInt(raceNo, null),
        cache_hit: !!data?.source?.cache?.hit,
        ...routeTimings
      })
    );

    return res.json({
      source: data.source || {},
      race: data.race,
      racers: data.racers,
      kyoteibiyori_debug:
        data?.kyoteibiyori_debug ||
        data?.source?.kyotei_biyori?.kyoteibiyori_debug ||
        data?.source?.kyotei_biyori?.request_diagnostics ||
        {},
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
      roleCandidates: {
        first_place_candidates: confirmedRoleProbabilities.confirmed_first_place_probability_json,
        second_place_candidates: confirmedRoleProbabilities.confirmed_second_place_probability_json,
        third_place_candidates: confirmedRoleProbabilities.confirmed_third_place_probability_json,
        survival_candidates: explicitSurvivalProbabilities,
        boat1_escape_probability: explicitBoat1EscapeProbability,
        attack_scenario_probabilities: explicitAttackScenarioProbabilities,
        finish_order_candidates: roleBasedOrderCandidates,
        outside_head_promotion_gate: candidateDistributions.outside_head_promotion_gate_json || {}
      },
      evidenceBiasTable: evidenceBiasTable,
      boat1HeadSection: {
        boat1_head_bets_snapshot: boat1HeadSnapshot.items,
        boat1_priority_mode_applied: toInt(headScenarioBalanceAnalysis.boat1_priority_mode_applied, 0),
        boat1_head_ratio_in_final_bets: toNullableNum(headScenarioBalanceAnalysis.boat1_head_ratio_in_final_bets),
        boat1_head_score: boat1HeadSnapshot.boat1_head_score,
        boat1_survival_residual_score: boat1HeadSnapshot.boat1_survival_residual_score,
        boat1_head_section_shown: boat1HeadSnapshot.shown ? 1 : 0,
        boat1_head_top8_generated: boat1HeadSnapshot.boat1_head_top8_generated,
        boat1_head_reason_tags: [...new Set([...
          boat1HeadSnapshot.boat1_head_reason_tags,
          ...safeArray(headScenarioBalanceAnalysis.boat1_priority_reason_tags)
        ])]
      },
      exactaSection: {
        exacta_recommended_bets_snapshot: exactaSnapshot.items,
        exacta_head_score: exactaSnapshot.exacta_head_score,
        exacta_partner_score: exactaSnapshot.exacta_partner_score,
        exacta_reason_tags: exactaSnapshot.exacta_reason_tags,
        exacta_section_shown: exactaSnapshot.shown ? 1 : 0
      },
      backupUrasujiSection: {
        backup_urasuji_recommendations_snapshot: backupUrasujiSnapshot.items,
        backup_urasuji_section_shown: backupUrasujiSnapshot.shown ? 1 : 0,
        backup_urasuji_reason_tags: backupUrasujiSnapshot.backup_reason_tags
      },
      roleBasedTickets: {
        main_trifecta_tickets: roleBasedMainTrifectaWithShape,
        exacta_cover_tickets: roleBasedExactaCoverTickets,
        backup_urasuji_tickets: roleBasedBackupUrasujiTickets
      },
      recommendedShape: safeRecommendedShape,
      prediction: predictionWithEntry,
      predicted_entry_order: entryMeta.predicted_entry_order,
      actual_entry_order: entryMeta.actual_entry_order,
      actual_lane_map: entryMeta.actual_lane_map,
      entry_changed: entryMeta.entry_changed,
      entry_change_type: entryMeta.entry_change_type,
      entry_fallback_used: entryMeta.fallback_used,
      entry_fallback_reason: entryMeta.fallback_reason,
      entry_validation: entryMeta.validation,
      actual_entry_authoritative_source: entryMeta.authoritative_source,
      raw_actual_entry_source_text: entryMeta.raw_actual_entry_source_text,
      per_boat_lane_map: entryMeta.per_boat_lane_map,
      entry_debug: compactEntryDebug,
      dataAudit,
      hardRaceResponseContract,
      startSignalAnalysis: startSignals,
      recommendation_score,
      scenarioSuggestions: safeScenarioSuggestions,
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
      scenarioSuggestions: safeScenarioSuggestions,
      raceDecision,
      valueDetection,
      marketTrap,
      raceFlow,
      fetchedSignalDiagnostics,
      temporary_backend_debug: temporaryFeaturePipelineDebug,
      finishProbabilitiesByScenario: safeFinishProbabilitiesByScenario,
      startDisplay: startDisplay || null,
      startDisplayDebug: Array.isArray(startDisplay?.start_display_debug)
        ? startDisplay.start_display_debug
        : [],
      startExhibitionDebug,
      routeTiming: routeTimings
    });
  } catch (err) {
    const status = Number(err?.statusCode || err?.status || 500);
    const payload = {
      error: status >= 500 ? "race_api_failed" : "bad_request",
      code: err?.code || "race_route_error",
      where: failureWhere,
      route: "/api/race",
      message: String(err?.message || err || "unknown_error")
    };
    if (temporaryFeaturePipelineDebug) {
      payload.temporary_backend_debug = temporaryFeaturePipelineDebug;
    }
    console.error("[RACE_API_ERROR]", JSON.stringify(payload));
    return res.status(status).json(payload);
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
          const entryMeta = buildCanonicalEntryOrderMeta(data.racers, data?.source?.actual_entry || null);
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
          const entryMeta = buildCanonicalEntryOrderMeta(data.racers, data?.source?.actual_entry || null);
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

    const savedResultRow = db
      .prepare(
        `
        SELECT race_id, finish_1, finish_2, finish_3, payout_2t, payout_3t, decision_type
        FROM results
        WHERE race_id = ?
        LIMIT 1
      `
      )
      .get(raceId);

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
      saved_result_record: savedResultRow
        ? {
            race_id: savedResultRow.race_id,
            confirmed_result: normalizeCombo([
              savedResultRow.finish_1,
              savedResultRow.finish_2,
              savedResultRow.finish_3
            ].join("-")),
            payout_2t: savedResultRow.payout_2t ?? null,
            payout_3t: savedResultRow.payout_3t ?? null,
            decision_type: savedResultRow.decision_type ?? null
          }
        : null,
      message: confirmedResultChanged
        ? "Confirmed result updated. Existing verification records were invalidated and re-verification is required."
        : "Confirmed result note updated."
    });
  } catch (err) {
    if (err && typeof err === "object") {
      err.statusCode = err?.statusCode || 500;
      err.code = err?.code || "race_search_failed";
      err.where = err?.where || failureWhere;
      err.route = err?.route || "/api/race";
      err.details = {
        ...(err?.details && typeof err.details === "object" ? err.details : {}),
        query: {
          date: req.query?.date || null,
          venueId: req.query?.venueId || null,
          raceNo: req.query?.raceNo || null,
          participationMode: req.query?.participationMode || null,
          forceRefresh: parseBooleanFlag(req.query?.forceRefresh, false)
        }
      };
    }
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

raceRouter.get("/stats", async (req, res, next) => {
  try {
    const evaluationFilters = {
      venue: String(req.query?.venue || "").trim() || "all",
      date_from: dateKey(req.query?.date_from) || null,
      date_to: dateKey(req.query?.date_to) || null,
      recommendation_level: String(req.query?.recommendation_level || "").trim() || "all",
      formation_pattern: String(req.query?.formation_pattern || "").trim() || "all",
      only_participated: toNum(req.query?.only_participated, 0),
      only_recommended: toNum(req.query?.only_recommended, 0),
      only_boat1_escape_predicted: toNum(req.query?.only_boat1_escape_predicted, 0),
      only_outside_head_cases: toNum(req.query?.only_outside_head_cases, 0)
    };
    const evaluation = buildEvaluationSummary(buildVerifiedLearningRows(), { filters: evaluationFilters });
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
      },
      evaluation
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
        id: Number.isFinite(Number(row?.id)) ? Number(row.id) : null,
        race_id: row?.race_id || null,
        verified_at: row.verified_at || null,
        hit_miss: row.hit_miss || null,
        mismatch_categories: safeJsonParse(row.mismatch_categories_json, []),
        miss_pattern_tags: Array.isArray(summary?.miss_pattern_tags) ? summary.miss_pattern_tags : [],
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
        confirmed_result:
          row?.confirmed_result ||
          summary?.confirmed_result_canonical ||
          summary?.confirmed_result ||
          null,
        invalid_reason: row?.invalid_reason || summary?.invalid_reason || null,
        invalidated_at: row?.invalidated_at || summary?.invalidated_at || null,
        is_hidden_from_results: Number(row?.is_hidden_from_results) === 1 ? 1 : 0,
        is_invalid_verification: Number(row?.is_invalid_verification) === 1 ? 1 : 0,
        exclude_from_learning: Number(row?.exclude_from_learning) === 1 ? 1 : 0,
        second_place_miss: summary?.second_place_miss === true,
        third_place_miss: summary?.third_place_miss === true,
        second_third_swap: summary?.second_third_swap === true,
        structure_near_miss:
          summary?.structure_near_but_order_miss === true ||
          (Array.isArray(summary?.miss_pattern_tags) && summary.miss_pattern_tags.includes("structure_near_miss")),
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
      const boat1HeadBetsSnapshot = normalizeSavedBetSnapshotItems(prediction?.boat1_head_bets_snapshot);
      const exactaBetsFromPrediction = normalizeSavedExactaSnapshotItems(
        prediction?.exacta_recommended_bets_snapshot ||
        prediction?.ai_bets_full_snapshot?.exacta_recommended_bets
      );
      const exactaBetsFromVerification = normalizeSavedExactaSnapshotItems(
        verificationSummary?.exacta_recommended_bets_snapshot
      );
      const exactaBetsSnapshot = exactaBetsFromPrediction.length > 0
        ? exactaBetsFromPrediction
        : exactaBetsFromVerification;
      const savedFinalRecommendedBetsSnapshot = finalRecommendedFromPrediction.length > 0
        ? finalRecommendedFromPrediction
        : legacyDisplaySnapshotFromPrediction;
      const snapshotFullFromPrediction =
        prediction?.ai_bets_full_snapshot && typeof prediction.ai_bets_full_snapshot === "object"
          ? prediction.ai_bets_full_snapshot
          : {
              recommended_bets: latestLogDisplayBets,
              exacta_recommended_bets: [],
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
      const actualExactaCombo = actualTop3.length >= 2 ? actualTop3.slice(0, 2).join("-") : null;
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
      const exactaSnapshotCombos = exactaBetsSnapshot
        .map((row) => normalizeExactaCombo(row?.combo ?? row))
        .filter((c) => c && c.split("-").length === 2);
      const hasValidBetSnapshot = displaySnapshotCombos.length > 0;
      const hasValidExactaSnapshot = exactaSnapshotCombos.length > 0;
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
      const persistedExactaStatus = String(verificationSummary?.exacta_verification_status || "").toUpperCase();
      const computedExactaStatus = !hasValidExactaSnapshot
        ? "NO_BET_SNAPSHOT"
        : actualExactaCombo
          ? (exactaSnapshotCombos.includes(actualExactaCombo) ? "HIT" : "MISS")
          : "NO_CONFIRMED_RESULT";
      const exactaVerificationStatus = invalidatedVerification
        ? "INVALIDATED"
        : persistedExactaStatus || computedExactaStatus;
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
        normalizeCombo(
          verification?.confirmed_result ||
          verificationSummary?.confirmed_result_canonical ||
          verificationSummary?.confirmed_result ||
          ""
        ) ||
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
        participation_decision:
          prediction?.participation_decision ||
          prediction?.learning_context?.participation_decision ||
          null,
        participation_decision_reason:
          prediction?.participation_decision_reason ||
          prediction?.learning_context?.participation_decision_reason ||
          null,
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
        boat1_head_bets_snapshot: boat1HeadBetsSnapshot,
        boat1_priority_mode_applied: toNum(prediction?.boat1_priority_mode_applied, 0),
        boat1_head_ratio_in_final_bets: toNum(prediction?.boat1_head_ratio_in_final_bets, null),
        boat1_head_score: toNum(prediction?.boat1_head_score, null),
        boat1_survival_residual_score: toNum(prediction?.boat1_survival_residual_score, null),
        boat1_head_section_shown: toNum(prediction?.boat1_head_section_shown, 0),
        boat1_head_top8_generated: toNum(prediction?.boat1_head_top8_generated, 0),
        boat1_head_reason_tags: Array.isArray(prediction?.boat1_head_reason_tags) ? prediction.boat1_head_reason_tags : [],
        exacta_recommended_bets_snapshot: exactaBetsSnapshot,
        exacta_head_score: toNum(prediction?.exacta_head_score, null),
        exacta_partner_score: toNum(prediction?.exacta_partner_score, null),
        exacta_reason_tags: Array.isArray(prediction?.exacta_reason_tags) ? prediction.exacta_reason_tags : [],
        exacta_section_shown: toNum(prediction?.exacta_section_shown, 0),
        exacta_hit: exactaVerificationStatus === "HIT",
        exacta_miss: exactaVerificationStatus === "MISS",
        exacta_verification_status: exactaVerificationStatus,
        ai_bets_latest_log: latestLogDisplayBets,
        debug_bet_compare: {
          confirmed_result: confirmedResult,
          confirmed_exacta_result: actualExactaCombo,
          saved_display_snapshot: savedFinalRecommendedBetsSnapshot,
          saved_exacta_snapshot: exactaBetsSnapshot,
          displayed_in_results: aiBetsDisplaySnapshot,
          verification_display_snapshot: verificationDisplaySnapshot,
          verification_input_bet_list: displaySnapshotCombos,
          verification_input_exacta_bet_list: exactaSnapshotCombos,
          latest_log_bets: latestLogDisplayBets,
          final_hit_miss_result: hitMiss,
          final_exacta_result: exactaVerificationStatus
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
          venue_correction_applied: !!(
            prediction?.learning_context?.venue_correction_summary &&
            Object.keys(prediction.learning_context.venue_correction_summary).length
          ),
          venue_segment_key: prediction?.learning_context?.venue_correction_summary?.venue_segment_key || null,
          venue_sample_count: toNum(
            prediction?.learning_context?.segment_corrections_used?.segments?.find((row) => row?.type === "venue")?.sample_count,
            0
          ),
          venue_correction_summary: prediction?.learning_context?.venue_correction_summary || {},
          boat1_partner_model_applied: toNum(prediction?.learning_context?.boat1_partner_model_applied, 0) === 1,
          boat1_escape_partner_version: prediction?.learning_context?.boat1_escape_partner_version || null,
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
    const snapshotExactaBets = normalizeSavedExactaSnapshotItems(
      predictionJson?.exacta_recommended_bets_snapshot ||
      predictionJson?.ai_bets_full_snapshot?.exacta_recommended_bets
    );
    const snapshotFullBets =
      predictionJson?.ai_bets_full_snapshot && typeof predictionJson.ai_bets_full_snapshot === "object"
        ? predictionJson.ai_bets_full_snapshot
        : {
            recommended_bets: safeArray(betPlanJson?.recommended_bets),
            exacta_recommended_bets: [],
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
    const canonicalExactaSnapshotBets = safeArray(snapshotExactaBets)
      .map((row) => normalizeExactaCombo(row?.combo ?? row))
      .filter((combo) => combo && combo.split("-").length === 2);
    const actualExactaCombo = actualTop3.length >= 2 ? actualTop3.slice(0, 2).join("-") : null;
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
        exacta_recommended_bets_snapshot: snapshotExactaBets,
        exacta_verification_status: canonicalExactaSnapshotBets.length === 0
          ? "NO_BET_SNAPSHOT"
          : actualExactaCombo && canonicalExactaSnapshotBets.includes(actualExactaCombo)
            ? "HIT"
            : actualExactaCombo
              ? "MISS"
              : "NO_CONFIRMED_RESULT",
        exacta_hit: canonicalExactaSnapshotBets.length > 0 && !!actualExactaCombo && canonicalExactaSnapshotBets.includes(actualExactaCombo),
        exacta_miss: canonicalExactaSnapshotBets.length > 0 && !!actualExactaCombo && !canonicalExactaSnapshotBets.includes(actualExactaCombo),
        verified_against_exacta_bets: canonicalExactaSnapshotBets,
        confirmed_exacta_result_canonical: actualExactaCombo,
        hit_match_found: false,
        exacta_hit_match_found: canonicalExactaSnapshotBets.length > 0 && !!actualExactaCombo && canonicalExactaSnapshotBets.includes(actualExactaCombo),
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
      second_place_correct: analysis.second_place_correct,
      third_place_correct: analysis.third_place_correct,
      second_third_correct: analysis.second_third_correct,
      hit_miss: analysis.hit_miss,
      miss_head: analysis.miss_head,
      miss_second: analysis.miss_second,
      miss_third: analysis.miss_third,
      mismatch_categories: analysis.categories,
      second_place_miss: analysis.second_place_miss,
      third_place_miss: analysis.third_place_miss,
      partner_selection_miss: analysis.partner_selection_miss,
      third_place_noise: analysis.third_place_noise,
      second_third_swap: analysis.second_third_swap,
      structure_near_but_order_miss: analysis.structure_near_but_order_miss,
      attack_read_correct_but_finish_wrong: analysis.attack_read_correct_but_finish_wrong,
      boat1_escape_correct_but_opponent_wrong: analysis.boat1_escape_correct_but_opponent_wrong,
      miss_pattern_tags: analysis.miss_pattern_tags,
      learning_adjustment_reason_tags: analysis.learning_adjustment_reason_tags,
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
      exacta_recommended_bets_snapshot: snapshotExactaBets,
      exacta_verification_status: canonicalExactaSnapshotBets.length === 0
        ? "NO_BET_SNAPSHOT"
        : actualExactaCombo && canonicalExactaSnapshotBets.includes(actualExactaCombo)
          ? "HIT"
          : actualExactaCombo
            ? "MISS"
            : "NO_CONFIRMED_RESULT",
      exacta_hit: canonicalExactaSnapshotBets.length > 0 && !!actualExactaCombo && canonicalExactaSnapshotBets.includes(actualExactaCombo),
      exacta_miss: canonicalExactaSnapshotBets.length > 0 && !!actualExactaCombo && !canonicalExactaSnapshotBets.includes(actualExactaCombo),
      verified_against_exacta_bets: canonicalExactaSnapshotBets,
      confirmed_exacta_result_canonical: actualExactaCombo,
      exacta_hit_match_found: canonicalExactaSnapshotBets.length > 0 && !!actualExactaCombo && canonicalExactaSnapshotBets.includes(actualExactaCombo),
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
      verified_against_exacta_bets: canonicalExactaSnapshotBets,
      confirmed_result_canonical: analysis.confirmed_result_canonical,
      confirmed_exacta_result_canonical: actualExactaCombo,
      hit_match_found: analysis.hit_match_found,
      exacta_hit_match_found: canonicalExactaSnapshotBets.length > 0 && !!actualExactaCombo && canonicalExactaSnapshotBets.includes(actualExactaCombo),
      exacta_verification_status: summary.exacta_verification_status,
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

export const __testHooks = {
  buildSeparatedCandidateDistributions,
  buildPlayerStatProfileFromHistory,
  buildPredictionFeatureBundle,
  buildRoleProbabilityLayers,
  buildBoat3WeakStHeadSuppressionContext,
  getLaunchStateConfig,
  getVenueLaunchMicroCalibration,
  getFHolderPenaltyByRole,
  computeStartAdvantageScore,
  computeMotor2renStrength,
  computeLapExhibitionStrength,
  computeFinishOverrideStrength,
  applyFinishOverrideStrength,
  computeLaunchStateScores,
  classifyLaunchStates,
  buildIntermediateDevelopmentEvents,
  computeRaceScenarioProbabilities,
  computeFinishProbsByScenario,
  combineScenarioAndFinishProbs,
  buildTopRecommendedTickets,
  computeUpsetRiskScore,
  shouldShowUpsetAlert,
  buildUpsetAlert,
  computeBoat1EscapeProbability,
  computeAttackScenarioProbabilities,
  computeFirstPlaceProbabilities,
  computeSecondPlaceProbabilities,
  computeSurvivalProbabilities,
  computeThirdPlaceProbabilities,
  buildEvidenceBiasTable,
  applyEvidenceBiasConfirmationToRoleProbabilities,
  composeFinishOrderCandidates,
  generateMainTrifectaTickets,
  generateExactaCoverTickets,
  generateBackupUrasujiTickets,
  buildExactaCoverageSnapshot,
  buildParticipationDecision,
  buildBackupUrasujiRecommendationsSnapshot,
  normalizeDistributionRows,
  computeEvaluationMetrics,
  buildEvaluationSummary,
  buildConfidenceCalibration,
  buildOutsideHeadMonitoring,
  buildBoat1EscapeDiagnostics
};

