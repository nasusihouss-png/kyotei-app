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
  settlePlacedBetsForRace,
  getPlacedBetSummaries
} from "../../placed-bet-service.js";
import { runSelfLearning } from "../../self-learning-engine.js";

export const raceRouter = Router();

function safeJsonParse(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
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

raceRouter.get("/race", async (req, res, next) => {
  try {
    const { date, venueId, raceNo, participationMode } = req.query;

    if (!date || !venueId || !raceNo) {
      return res.status(400).json({
        error: "bad_request",
        message: "date, venueId, and raceNo are required query params"
      });
    }

    const data = await getRaceData({ date, venueId, raceNo });
    const raceId = saveRace(data);
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

    const ranking = rankRace(entryAdjusted.racersWithFeatures);
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
      venueBias
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
    const roleCandidates = analyzeRoleCandidates({
      ranking,
      headSelection: headSelectionRefined,
      partnerSelection,
      exhibitionAI
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
    const ticketGenerationV2 = generateTicketsV2({
      headSelection: headSelectionRefined,
      partnerSelection,
      headConfidence,
      headPrecision,
      exhibitionAI,
      raceRisk,
      raceIndexes,
      wallEvaluation,
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
    const raceDecision = decideRaceSelection({
      raceStructure,
      preRaceAnalysis,
      roleCandidates,
      ticketOptimization,
      headPrecision,
      exhibitionAI,
      venueBias
    });
    const valueDetection = detectValue({
      recommendedBets: bet_plan.recommended_bets,
      ticketOptimization,
      raceDecision,
      venueBias
    });
    const valueByCombo = new Map(
      (Array.isArray(valueDetection?.tickets) ? valueDetection.tickets : []).map((t) => [String(t.combo), t])
    );
    const stakeAllocation = buildStakeAllocationPlan({
      raceDecision,
      ticketOptimization,
      betPlan: bet_plan,
      ticketGenerationV2,
      valueDetection
    });
    const stakeByCombo = new Map(stakeAllocation.tickets.map((t) => [String(t.combo), t]));
    const bet_plan_with_stake = {
      ...bet_plan,
      recommended_bets: (Array.isArray(bet_plan.recommended_bets) ? bet_plan.recommended_bets : []).map((row) => {
        const stake = stakeByCombo.get(String(row?.combo || ""));
        const value = valueByCombo.get(String(row?.combo || ""));
        return {
          ...row,
          ticket_type: stake?.ticket_type || "backup",
          value_score: Number.isFinite(Number(value?.value_score)) ? Number(value.value_score) : null,
          overpriced_flag: !!value?.overpriced_flag,
          underpriced_flag: !!value?.underpriced_flag,
          bet_value_tier: value?.bet_value_tier || null,
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
        return {
          ...row,
          ticket_type: stake?.ticket_type || row?.ticket_type || "backup",
          value_score: Number.isFinite(Number(value?.value_score)) ? Number(value.value_score) : null,
          overpriced_flag: !!value?.overpriced_flag,
          underpriced_flag: !!value?.underpriced_flag,
          bet_value_tier: value?.bet_value_tier || null,
          recommended_bet: Number.isFinite(Number(stake?.recommended_bet))
            ? Number(stake.recommended_bet)
            : Number(row?.recommended_bet ?? 100)
        };
      }),
      bankrollPlan: stakeAllocation.bankrollPlan
    };

    saveFeatureSnapshots(raceId, ranking);

    savePredictionLog({
      raceId,
      racePattern,
      buyType,
      raceRisk,
      prediction,
      raceDecision,
      probabilities,
      ev_analysis: evData.ev_analysis,
      bet_plan: bet_plan_with_stake
    });

    return res.json({
      race: data.race,
      racers: data.racers,
      raceId,
      prediction,
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
      roleCandidates,
      headConfidence,
      venueBias,
      raceStructure,
      ticketGenerationV2,
      aiEnhancement,
      ticketOptimization: ticketOptimizationWithStake,
      raceDecision,
      valueDetection
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
    const confidenceMin = Math.max(0, Math.min(100, toInt(req.query?.confidenceMin, 68)));
    const maxChaos = Math.max(0, Math.min(100, toInt(req.query?.maxChaos, 65)));
    const headStabilityMin = Math.max(0, Math.min(100, toInt(req.query?.headStabilityMin, 50)));
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
    const maxScan = Math.max(1, Math.min(240, toInt(req.query?.maxScan, 72)));

    const recs = [];
    const errors = [];
    let scanned = 0;

    for (const venueId of scanVenues) {
      for (const raceNo of scanRaceNos) {
        if (scanned >= maxScan) break;
        scanned += 1;
        try {
          const data = await getRaceData({ date, venueId, raceNo });
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
          const ranking = rankRace(entryAdjusted.racersWithFeatures);
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
            venueBias
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
          const roleCandidates = analyzeRoleCandidates({
            ranking,
            headSelection: headSelectionRefined,
            partnerSelection,
            exhibitionAI
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
          const raceDecision = decideRaceSelection({
            raceStructure,
            preRaceAnalysis,
            roleCandidates,
            ticketOptimization,
            headPrecision,
            exhibitionAI,
            venueBias
          });
          const ticketGenerationV2 = generateTicketsV2({
            headSelection: headSelectionRefined,
            partnerSelection,
            headConfidence,
            headPrecision,
            exhibitionAI,
            raceRisk,
            raceIndexes,
            wallEvaluation,
            venueBias
          });

          const mode = String(raceDecision?.mode || raceRisk?.recommendation || "").toUpperCase();
          const confidence = Number(raceDecision?.confidence ?? 0);
          const headStability = Number(raceStructure?.head_stability_score ?? 0);
          const chaosRisk = Number(raceStructure?.chaos_risk_score ?? raceIndexes?.are_index ?? 0);

          const worthBetting =
            mode === "FULL_BET" &&
            confidence >= confidenceMin &&
            headStability >= headStabilityMin &&
            chaosRisk <= maxChaos;

          if (!worthBetting) continue;
          const stakeAllocation = buildStakeAllocationPlan({
            raceDecision,
            ticketOptimization,
            betPlan: bet_plan,
            ticketGenerationV2,
            valueDetection: detectValue({
              recommendedBets: bet_plan.recommended_bets,
              ticketOptimization,
              raceDecision,
              venueBias
            })
          });

          recs.push({
            raceId: `${date}_${venueId}_${raceNo}`,
            date,
            venueId: data.race.venueId,
            venueName: data.race.venueName || null,
            raceNo: data.race.raceNo,
            mode,
            confidence: Number(confidence.toFixed(2)),
            main_head: Number(headSelectionRefined?.main_head) || null,
            backup_heads: headSelectionRefined?.secondary_heads || [],
            head_win_score: headPrecision?.head_win_score ?? null,
            head_gap_score: headPrecision?.head_gap_score ?? null,
            exhibition_ai_score: exhibitionAI?.exhibition_ai_score ?? null,
            head_stability_score: Number(headStability.toFixed(2)),
            chaos_risk_score: Number(chaosRisk.toFixed(2)),
            venueBias,
            tickets: stakeAllocation.tickets.slice(0, 4).map((t) => ({
              combo: t.combo,
              prob: Number.isFinite(Number(t.prob)) ? Number(t.prob) : null,
              odds: Number.isFinite(Number(t.odds)) ? Number(t.odds) : null,
              ev: Number.isFinite(Number(t.ev)) ? Number(t.ev) : null,
              bet: Number.isFinite(Number(t.recommended_bet)) ? Number(t.recommended_bet) : null,
              ticket_type: t.ticket_type || "backup"
            })),
            bankrollPlan: stakeAllocation.bankrollPlan,
            summary:
              raceDecision?.summary ||
              ticketGenerationV2?.summary ||
              raceRisk?.skip_summary ||
              "本線向き",
            odds: {
              fetched_at: evData?.oddsData?.fetched_at || null,
              fetch_status: evData?.oddsData?.fetch_status || null
            }
          });
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

    recs.sort((a, b) => {
      const c = Number(b.confidence || 0) - Number(a.confidence || 0);
      if (c !== 0) return c;
      return Number(b.head_stability_score || 0) - Number(a.head_stability_score || 0);
    });

    return res.json({
      date,
      participation_mode: participationMode || "active",
      scanned,
      returned: Math.min(limit, recs.length),
      recommendations: recs.slice(0, limit),
      skipped_count: Math.max(0, scanned - recs.length),
      errors: errors.slice(0, 30)
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

raceRouter.post("/placed-bets", async (req, res, next) => {
  try {
    if (Array.isArray(req.body?.items) && req.body.items.length > 0) {
      const ids = createPlacedBets(req.body.items);
      return res.status(201).json({
        ok: true,
        ids
      });
    }

    const id = createPlacedBet(req.body || {});
    return res.status(201).json({
      ok: true,
      id
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
    return res.json({
      ok: true,
      ...result
    });
  } catch (err) {
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
        SELECT id, race_id, race_date, venue_id, race_no, combo, bet_amount, bought_odds, recommended_prob, recommended_ev, recommended_bet, hit_flag, payout, profit_loss
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
        SELECT pl.race_id, pl.recommendation, pl.prediction_json, pl.bet_plan_json, pl.created_at
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

    const resultMap = new Map(resultsRows.map((r) => [r.race_id, r]));
    const raceMap = new Map(raceRows.map((r) => [r.race_id, r]));
    const settlementByRace = new Map();
    for (const row of settlementRows) {
      const list = settlementByRace.get(row.race_id) || [];
      list.push(row);
      settlementByRace.set(row.race_id, list);
    }

    const items = latestPredictionRows.map((logRow) => {
      const raceId = logRow.race_id;
      const prediction = safeJsonParse(logRow.prediction_json, {});
      const betPlan = safeJsonParse(logRow.bet_plan_json, {});
      const race = raceMap.get(raceId) || {};
      const result = resultMap.get(raceId) || null;
      const settlements = settlementByRace.get(raceId) || [];

      const predictedTop3 = Array.isArray(prediction?.top3) ? prediction.top3.slice(0, 3) : [];
      const actualTop3 = result
        ? [result.finish_1, result.finish_2, result.finish_3].filter((v) => Number.isFinite(Number(v)))
        : [];

      const predictedCombo = predictedTop3.length === 3 ? predictedTop3.join("-") : null;
      const actualCombo = actualTop3.length === 3 ? actualTop3.join("-") : null;
      const hitMiss =
        predictedCombo && actualCombo ? (predictedCombo === actualCombo ? "HIT" : "MISS") : "PENDING";

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
        race_date: race.race_date ?? null,
        venue_id: race.venue_id ?? null,
        venue_name: race.venue_name ?? null,
        race_no: race.race_no ?? null,
        recommendation: normalizeRecommendation(logRow.recommendation),
        predicted_top3: predictedTop3,
        actual_top3: actualTop3,
        hit_miss: hitMiss,
        totals,
        bets: settlements.map((s) => ({
          combo: s.combo,
          bet_amount: toNum(s.bet_amount),
          hit_flag: toNum(s.hit_flag) ? 1 : 0,
          payout: toNum(s.payout),
          profit_loss: toNum(s.profit_loss)
        })),
        recommended_bets: Array.isArray(betPlan?.recommended_bets) ? betPlan.recommended_bets : [],
        logged_at: logRow.created_at
      };
    });

    return res.json({ items });
  } catch (err) {
    return next(err);
  }
});

