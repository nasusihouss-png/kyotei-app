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
  runContinuousLearningIfNeeded,
  runLearningBatch
} from "../../learning-weight-engine.js";

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
    verification_summary_json TEXT
  );
`);

function ensureVerificationLogColumns() {
  const cols = db.prepare("PRAGMA table_info(race_verification_logs)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("race_date")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN race_date TEXT");
  if (!names.has("venue_code")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN venue_code INTEGER");
  if (!names.has("venue_name")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN venue_name TEXT");
  if (!names.has("race_no")) db.exec("ALTER TABLE race_verification_logs ADD COLUMN race_no INTEGER");
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

function toNum(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
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
  const hitMiss = predictedCombo && actualCombo && predictedCombo === actualCombo ? "HIT" : "MISS";

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

  const recBets = safeArray(predictedBets)
    .map((b) => normalizeCombo(b?.combo))
    .filter((x) => x && x.split("-").length === 3);
  if (recBets.length > 0 && actualCombo) {
    const hasHitTicket = recBets.includes(actualCombo);
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
    actual_combo: actualCombo
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

function loadRaceSnapshotFromDb({ date, venueId, raceNo }) {
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
  contenderSignals
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
      venueAdj +
      gradeAdj +
      sigAdj
  );
  return Number(score.toFixed(2));
}

const CONFIDENCE_VERSION = "v1.1";
const PARTICIPATION_CONFIDENCE_THRESHOLDS = {
  participate: {
    headFixedMin: 75,
    betMin: 68
  },
  watch: {
    headMin: 60,
    betMin: 55
  }
};

function buildParticipationDecision({
  raceDecision,
  raceRisk,
  raceStructure,
  entryMeta,
  confidenceScores
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
  if (headStability >= 62) reasonTags.push("HEAD_STABILITY_GOOD");
  if (headFixed >= 75) reasonTags.push("HEAD_CONFIDENCE_GOOD");
  if (betConf >= 68) reasonTags.push("BET_CONFIDENCE_GOOD");

  let decision = "watch";
  if (
    mode !== "SKIP" &&
    headFixed >= PARTICIPATION_CONFIDENCE_THRESHOLDS.participate.headFixedMin &&
    betConf >= PARTICIPATION_CONFIDENCE_THRESHOLDS.participate.betMin &&
    !hasStrongCaution
  ) {
    decision = "recommended";
  } else if (
    headFixed <= 59 ||
    betConf <= 54 ||
    (mode === "SKIP" && hasStrongCaution && confidence < 55) ||
    chaosRisk >= 90
  ) {
    decision = "not_recommended";
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
      confidence_version: CONFIDENCE_VERSION
    },
    confidence_version: CONFIDENCE_VERSION
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
  scenarioSuggestions
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

  const headFixed = clampPct(
    headBase * 0.38 +
      headStability * 0.16 +
      startStability * 0.1 +
      top3Concentration * 0.08 +
      playerSignal * 0.1 +
      motorSignal * 0.08 +
      exhibitionSignal * 0.06 +
      formScore * 0.04 -
      entryPenalty -
      learnedCaution -
      missingDataPenalty
  );
  const betConfidence = clampPct(
    ticketConf * 0.34 +
      raceConf * 0.16 +
      (100 - chaosRisk) * 0.14 +
      headFixed * 0.14 +
      scenarioConfidence * 0.08 +
      contenderOverlap * 0.08 +
      top3Concentration * 0.06 -
      learnedCaution * 0.6 -
      Math.max(0, missingDataPenalty - 4)
  );
  const raceConfidence = clampPct((raceConf * 0.6 + headFixed * 0.2 + betConfidence * 0.2));

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
  return {
    head_fixed_confidence_pct: headFixed,
    recommended_bet_confidence_pct: betConfidence,
    race_confidence_pct: raceConfidence,
    head_fixed_band: toBand(headFixed),
    recommended_bet_band: toBand(betConfidence),
    race_band: toBand(raceConfidence),
    head_confidence: headFixed,
    bet_confidence: betConfidence,
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
    const ranking = contenderAdjusted.ranking;
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

    const prediction = {
      ranking,
      top3: ranking.slice(0, 3).map((r) => r.racer.lane)
    };
    const prediction_before_entry_change = {
      ranking: preRanking,
      top3: preRanking.slice(0, 3).map((r) => r.racer.lane)
    };
    const prediction_after_entry_change = {
      ranking,
      top3: prediction.top3
    };
    const playerStartProfile = analyzePlayerStartProfiles({ ranking });
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
    const raceFlow = analyzeRaceFlow({
      ranking,
      raceIndexes,
      racePattern,
      raceRisk: baseRaceRisk,
      playerStartProfiles: playerStartProfile
    });
    const wallEvaluation = evaluateLane2Wall({
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
      wallEvaluation
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
      raceStructure: baseRaceStructure,
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
    const raceDecision = applyStartSignalToDecision(entryAdjustedDecision, startSignals, entryMeta);
    const recommendation_score = computeRecommendationScore({
      raceDecision,
      raceStructure,
      startSignals,
      entryMeta,
      race: data.race,
      learningWeights,
      contenderSignals: contenderAdjusted.contenderSignals
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
      return {
        ...row,
        explanation_tags: exp?.explanation_tags || [],
        explanation_summary: exp?.explanation_summary || null
      };
    });
    ticketOptimizationWithStake.optimized_tickets = (Array.isArray(ticketOptimizationWithStake.optimized_tickets)
      ? ticketOptimizationWithStake.optimized_tickets
      : []
    ).map((row) => {
      const combo = normalizeCombo(row?.combo);
      const exp = combo ? ticketExplainability[combo] : null;
      return {
        ...row,
        explanation_tags: exp?.explanation_tags || [],
        explanation_summary: exp?.explanation_summary || null
      };
    });

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
      scenarioSuggestions
    });
    const participationDecision = buildParticipationDecision({
      raceDecision,
      raceRisk,
      raceStructure,
      entryMeta,
      confidenceScores
    });
    const predictionWithEntry = {
      ...prediction,
      predicted_entry_order: entryMeta.predicted_entry_order,
      actual_entry_order: entryMeta.actual_entry_order,
      entry_changed: entryMeta.entry_changed,
      entry_change_type: entryMeta.entry_change_type,
      confidence_scores: confidenceScores,
      head_confidence: confidenceScores.head_confidence,
      bet_confidence: confidenceScores.bet_confidence,
      participation_decision: participationDecision.decision,
      confidence_reason_tags: confidenceScores.confidence_reason_tags,
      confidence_version: confidenceScores.confidence_version,
      snapshot_created_at: new Date().toISOString(),
      race_key: raceId,
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
        }
      },
      ai_bets_display_snapshot: Array.isArray(bet_plan_with_stake?.recommended_bets)
        ? bet_plan_with_stake.recommended_bets
        : [],
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

    const startDisplay = saveRaceStartDisplaySnapshot({
      raceId,
      racers: data.racers,
      sourceMeta: data.source || {},
      predictionSnapshot: {
        raceDecision,
        top3: predictionWithEntry.top3,
        recommendation: raceRisk?.recommendation || null,
        mode: raceDecision?.mode || null,
        predicted_entry_order: entryMeta.predicted_entry_order,
        actual_entry_order: entryMeta.actual_entry_order,
        entry_changed: entryMeta.entry_changed,
        entry_change_type: entryMeta.entry_change_type
      }
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
          const ranking = contenderAdjusted.ranking;
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
            wallEvaluation
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
            raceStructure: baseRaceStructure,
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
          const raceDecision = applyStartSignalToDecision(entryAdjustedDecision, startSignals, entryMeta);
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
            contenderSignals: contenderAdjusted.contenderSignals
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
            scenarioSuggestions
          });
          const participationDecisionFinal = buildParticipationDecision({
            raceDecision,
            raceRisk,
            raceStructure,
            entryMeta,
            confidenceScores
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
          const ranking = contenderAdjusted.ranking;
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
            wallEvaluation
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
            raceStructure: baseRaceStructure,
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
          const raceDecision = applyStartSignalToDecision(entryAdjustedDecision, startSignals, entryMeta);
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
            contenderSignals: contenderAdjusted.contenderSignals
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
    attachPredictionFeatureLogSettlement({
      raceId,
      actualResult: top3.join("-"),
      settledBetHitCount: comparison?.hitCount ?? null,
      settledBetCount: comparison?.totalBets ?? null
    });

    const comparison = compareActualTop3VsPredictedBets(top3, predictedBets, {
      payoutByCombo
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
    const auto = parseBooleanFlag(_req.query?.auto, true);
    const autoResult = auto
      ? runContinuousLearningIfNeeded({
          minNewLearningReady: 3,
          minLearningReadyTotal: 10
        })
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
    const result = runLearningBatch({
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

    const latestPredictionRows = db
      .prepare(
        `
        SELECT pl.race_id, pl.recommendation, pl.prediction_json, pl.race_decision_json, pl.bet_plan_json
        FROM prediction_logs pl
        INNER JOIN (
          SELECT race_id, MAX(id) AS max_id
          FROM prediction_logs
          GROUP BY race_id
        ) latest
          ON latest.max_id = pl.id
      `
      )
      .all()
      .map((row) => ({
        race_id: row.race_id,
        recommendation: row.recommendation,
        prediction: safeJsonParse(row.prediction_json, {}),
        raceDecision: safeJsonParse(row.race_decision_json, {}),
        betPlan: safeJsonParse(row.bet_plan_json, {})
      }));

    const resultRows = db
      .prepare(
        `
        SELECT race_id, finish_1, finish_2, finish_3, payout_3t
        FROM results
      `
      )
      .all();

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
    const limit = Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 200) : 50;

    const latestPredictionRows = db
      .prepare(
        `
        SELECT pl.id, pl.race_id, pl.recommendation, pl.prediction_json, pl.bet_plan_json, pl.created_at
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
    const verificationRows = db
      .prepare(
        `
        SELECT v.race_id, v.verified_at, v.hit_miss, v.mismatch_categories_json, v.verification_summary_json
        FROM race_verification_logs v
        INNER JOIN (
          SELECT race_id, MAX(id) AS max_id
          FROM race_verification_logs
          GROUP BY race_id
        ) latest
          ON latest.max_id = v.id
      `
      )
      .all();

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
    const settlementByRace = new Map();
    for (const row of settlementRows) {
      const list = settlementByRace.get(row.race_id) || [];
      list.push(row);
      settlementByRace.set(row.race_id, list);
    }
    const verificationMap = new Map(
      verificationRows.map((row) => [
        row.race_id,
        {
          verified_at: row.verified_at || null,
          hit_miss: row.hit_miss || null,
          mismatch_categories: safeJsonParse(row.mismatch_categories_json, []),
          summary: safeJsonParse(row.verification_summary_json, {})
        }
      ])
    );

    const items = latestPredictionRows.map((logRow) => {
      const raceId = logRow.race_id;
      const prediction = safeJsonParse(logRow.prediction_json, {});
      const betPlan = safeJsonParse(logRow.bet_plan_json, {});
      const race = raceMap.get(raceId) || {};
      const result = resultMap.get(raceId) || null;
      const settlements = settlementByRace.get(raceId) || [];
      const mutableStartDisplay = startDisplayMap.get(raceId) || null;
      const eventStartDisplay = featureEventMap.get(raceId) || null;
      const hasStartDisplaySource = !!mutableStartDisplay || !!eventStartDisplay;
      const startDisplay = hasStartDisplaySource
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
        : null;
      const verification = verificationMap.get(raceId) || null;
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

      const predictedCombo = predictedTop3.length === 3 ? predictedTop3.join("-") : null;
      const actualCombo = actualTop3.length === 3 ? actualTop3.join("-") : null;
      const hitMiss =
        predictedCombo && actualCombo ? (predictedCombo === actualCombo ? "HIT" : "MISS") : "PENDING";
      const confirmedResult =
        actualCombo ||
        (startDisplay?.settled_result ? normalizeCombo(startDisplay.settled_result) : null) ||
        (startDisplay?.fetched_result ? normalizeCombo(startDisplay.fetched_result) : null) ||
        null;
      const verificationStatus = verification?.verified_at
        ? "VERIFIED"
        : confirmedResult
          ? "PENDING_RESULT"
          : "NO_CONFIRMED_RESULT";

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

      const latestLogDisplayBets = Array.isArray(betPlan?.recommended_bets) ? betPlan.recommended_bets : [];
      const snapshotDisplayFromPrediction = Array.isArray(prediction?.ai_bets_display_snapshot)
        ? prediction.ai_bets_display_snapshot
        : latestLogDisplayBets;
      const snapshotFullFromPrediction =
        prediction?.ai_bets_full_snapshot && typeof prediction.ai_bets_full_snapshot === "object"
          ? prediction.ai_bets_full_snapshot
          : {
              recommended_bets: latestLogDisplayBets,
              optimized_tickets: [],
              ticket_generation_v2: { primary_tickets: [], secondary_tickets: [] },
              scenario_suggestions: { main_picks: [], backup_picks: [], longshot_picks: [] }
            };
      const aiBetsDisplaySnapshot = Array.isArray(verificationSummary?.ai_bets_display_snapshot)
        ? verificationSummary.ai_bets_display_snapshot
        : snapshotDisplayFromPrediction;
      const aiBetsFullSnapshot =
        verificationSummary?.ai_bets_full_snapshot && typeof verificationSummary.ai_bets_full_snapshot === "object"
          ? verificationSummary.ai_bets_full_snapshot
          : snapshotFullFromPrediction;
      const snapshotCreatedAt =
        verificationSummary?.snapshot_created_at ||
        prediction?.snapshot_created_at ||
        logRow.created_at ||
        null;
      const predictionSnapshotId = Number.isFinite(Number(verificationSummary?.prediction_snapshot_id))
        ? Number(verificationSummary.prediction_snapshot_id)
        : Number.isFinite(Number(logRow.id))
          ? Number(logRow.id)
          : null;
      const aiBetsSnapshotSource = verificationSummary?.ai_bets_display_snapshot
        ? "verification_snapshot"
        : prediction?.ai_bets_display_snapshot
          ? "prediction_snapshot"
          : "legacy_bet_plan";

      return {
        race_id: raceId,
        race_date: race.race_date ?? verification?.summary?.race_date ?? null,
        venue_id: race.venue_id ?? verification?.summary?.venue_code ?? null,
        venue_name: race.venue_name ?? verification?.summary?.venue_name ?? null,
        race_no: race.race_no ?? verification?.summary?.race_no ?? null,
        recommendation: normalizeRecommendation(logRow.recommendation),
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
        prediction_snapshot_id: predictionSnapshotId,
        snapshot_created_at: snapshotCreatedAt,
        ai_bets_snapshot_source: aiBetsSnapshotSource,
        ai_bets_full_snapshot: aiBetsFullSnapshot,
        ai_bets_display_snapshot: aiBetsDisplaySnapshot,
        ai_bets_latest_log: latestLogDisplayBets,
        debug_bet_compare: {
          saved_display_snapshot: aiBetsDisplaySnapshot,
          displayed_in_results: aiBetsDisplaySnapshot,
          latest_log_bets: latestLogDisplayBets
        },
        recommended_bets: aiBetsDisplaySnapshot,
        logged_at: logRow.created_at
      };
    });

    return res.json({ items });
  } catch (err) {
    return next(err);
  }
});

raceRouter.post("/results/verify", async (req, res, next) => {
  try {
    const raceId = String(req.body?.race_id || req.body?.raceId || "").trim();
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

    const latestPrediction = db
      .prepare(
        `
        SELECT id, prediction_json, bet_plan_json, recommendation, created_at
        FROM prediction_logs
        WHERE race_id = ?
        ORDER BY id DESC
        LIMIT 1
      `
      )
      .get(raceId);
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
    const snapshotDisplayBets = Array.isArray(predictionJson?.ai_bets_display_snapshot)
      ? predictionJson.ai_bets_display_snapshot
      : safeArray(betPlanJson?.recommended_bets);
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
    let predictedBets = snapshotDisplayBets;
    let verifyWarning = null;
    if (predictedBets.length === 0 && predictedTop3.length === 3) {
      predictedBets = [{ combo: predictedTop3.join("-"), source: "top3_fallback" }];
      verifyWarning = "AI recommended bets were missing, fallback verification used predicted top3.";
    }
    if (predictedBets.length === 0) {
      return res.status(409).json({
        ok: false,
        status: "VERIFY_FAILED",
        verification_performed: false,
        reason_code: "MISSING_AI_RECOMMENDED_BETS",
        message: "Verification cannot run because AI recommended bets are missing."
      });
    }
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
      race_date: raceMeta?.race_date || null,
      venue_code: Number.isFinite(Number(raceMeta?.venue_id)) ? Number(raceMeta.venue_id) : null,
      venue_name: raceMeta?.venue_name || null,
      race_no: Number.isFinite(Number(raceMeta?.race_no)) ? Number(raceMeta.race_no) : null,
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
      prediction_snapshot_id: Number.isFinite(Number(latestPrediction?.id)) ? Number(latestPrediction.id) : null,
      snapshot_created_at: predictionJson?.snapshot_created_at || latestPrediction?.created_at || null,
      race_key: predictionJson?.race_key || raceId,
      ai_bets_display_snapshot: snapshotDisplayBets,
      ai_bets_full_snapshot: snapshotFullBets,
      learning_ready: analysis.categories.length > 0,
      warning: verifyWarning
    };

    let insertInfo = null;
    try {
      insertInfo = db.prepare(
      `
      INSERT INTO race_verification_logs (
        race_id,
        race_date,
        venue_code,
        venue_name,
        race_no,
        predicted_top3,
        actual_top3,
        hit_miss,
        mismatch_categories_json,
        verification_summary_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
      ).run(
      raceId,
      raceMeta?.race_date || null,
      Number.isFinite(Number(raceMeta?.venue_id)) ? Number(raceMeta.venue_id) : null,
      raceMeta?.venue_name || null,
      Number.isFinite(Number(raceMeta?.race_no)) ? Number(raceMeta.race_no) : null,
      analysis.predicted_combo,
      analysis.actual_combo,
      analysis.hit_miss,
      JSON.stringify(analysis.categories),
      JSON.stringify(summary)
      );
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

    const continuousLearning = runContinuousLearningIfNeeded({
      minNewLearningReady: 3,
      minLearningReadyTotal: 10
    });

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
      status: "VERIFIED",
      verification_performed: true,
      message: "Verification completed.",
      verification: summary,
      warning: verifyWarning,
      persisted: !!insertInfo?.lastInsertRowid,
      summary_updated: true,
      learning_ready: analysis.categories.length > 0,
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

