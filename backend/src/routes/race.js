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
import { generateTicketsV2 } from "../../ticket-generation-v2-engine.js";
import { analyzeHitQuality } from "../../hit-quality-engine.js";
import { analyzePreRaceForm } from "../../pre-race-form-engine.js";
import { analyzeRoleCandidates } from "../../candidate-role-engine.js";
import { refineRaceRiskWithStructure } from "../../risk-structure-engine.js";
import { analyzeRaceStructure } from "../../race-structure-engine.js";
import { optimizeTickets } from "../../ticket-optimization-engine.js";
import {
  createPlacedBet,
  createPlacedBets,
  updatePlacedBet,
  deletePlacedBet,
  listPlacedBets,
  settlePlacedBetsForRace,
  getPlacedBetSummaries
} from "../../placed-bet-service.js";

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
    const bet_plan = buildBetPlan(evData.ev_analysis, 10000);
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
    const headConfidence = evaluateHeadConfidence({
      headSelection,
      raceRisk: baseRaceRisk,
      raceIndexes,
      raceOutcomeProbabilities,
      probabilities,
      wallEvaluation
    });
    const roleCandidates = analyzeRoleCandidates({
      ranking,
      headSelection,
      partnerSelection
    });
    const raceStructure = analyzeRaceStructure({
      ranking,
      probabilities,
      headConfidence,
      raceIndexes,
      preRaceAnalysis,
      roleCandidates
    });
    const raceRisk = refineRaceRiskWithStructure({
      raceRisk: baseRaceRisk,
      headConfidence,
      preRaceAnalysis,
      roleCandidates,
      probabilities,
      ranking
    });
    const ticketStrategy = buildTicketStrategy({
      raceOutcomeProbabilities,
      raceIndexes,
      raceRisk
    });
    const ticketGenerationV2 = generateTicketsV2({
      headSelection,
      partnerSelection,
      headConfidence,
      raceRisk,
      raceIndexes,
      wallEvaluation
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

    saveFeatureSnapshots(raceId, ranking);

    savePredictionLog({
      raceId,
      racePattern,
      buyType,
      raceRisk,
      prediction,
      probabilities,
      ev_analysis: evData.ev_analysis,
      bet_plan
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
      simulation,
      oddsData: evData.oddsData,
      ev_analysis: evData.ev_analysis,
      bet_plan,
      raceRisk,
      raceIndexes,
      raceOutcomeProbabilities,
      ticketStrategy,
      wallEvaluation,
      headSelection,
      partnerSelection,
      roleCandidates,
      headConfidence,
      raceStructure,
      ticketGenerationV2,
      aiEnhancement,
      ticketOptimization
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
