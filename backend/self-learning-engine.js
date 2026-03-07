function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function pct(numerator, denominator) {
  if (!denominator) return 0;
  return Number(((numerator / denominator) * 100).toFixed(2));
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function boundedDelta(sampleSize, rawDelta) {
  const absCap = sampleSize >= 180 ? 0.08 : sampleSize >= 100 ? 0.06 : sampleSize >= 60 ? 0.04 : 0.025;
  return clamp(-absCap, absCap, rawDelta);
}

function roundWeight(value) {
  return Number(toNum(value, 0).toFixed(4));
}

export const DEFAULT_WEIGHT_PROFILE = {
  exhibition_weight: 0.18,
  motor_weight: 0.15,
  course_weight: 0.14,
  chaos_weight: 0.12,
  head_precision_weight: 0.2,
  venue_inner_weight: 0.08,
  outer_lane_penalty: 0.06,
  value_detection_weight: 0.07
};

function buildModeStats(predictionsByRace, placedRows) {
  const map = new Map();
  for (const row of placedRows) {
    const raceId = String(row.race_id || "");
    const pred = predictionsByRace.get(raceId) || {};
    const mode = String(pred?.raceDecision?.mode || pred?.recommendation || "UNKNOWN");
    if (!map.has(mode)) {
      map.set(mode, { mode, bet_count: 0, hit_count: 0, bet_total: 0, payout_total: 0, pl_total: 0 });
    }
    const item = map.get(mode);
    item.bet_count += 1;
    item.bet_total += toNum(row.bet_amount);
    item.payout_total += toNum(row.payout);
    item.pl_total += toNum(row.profit_loss);
    if (toNum(row.hit_flag) === 1) item.hit_count += 1;
  }
  return Array.from(map.values()).map((m) => ({
    ...m,
    hit_rate: pct(m.hit_count, m.bet_count),
    recovery_rate: pct(m.payout_total, m.bet_total)
  }));
}

function buildVenueStats(placedRows, raceMap) {
  const map = new Map();
  for (const row of placedRows) {
    const raceId = String(row.race_id || "");
    const race = raceMap.get(raceId) || {};
    const key = String(race.venue_id || row.venue_id || "unknown");
    if (!map.has(key)) {
      map.set(key, {
        venue_id: toNum(race.venue_id || row.venue_id),
        venue_name: race.venue_name || null,
        bet_count: 0,
        hit_count: 0,
        bet_total: 0,
        payout_total: 0,
        pl_total: 0
      });
    }
    const item = map.get(key);
    item.bet_count += 1;
    item.bet_total += toNum(row.bet_amount);
    item.payout_total += toNum(row.payout);
    item.pl_total += toNum(row.profit_loss);
    if (toNum(row.hit_flag) === 1) item.hit_count += 1;
  }
  return Array.from(map.values())
    .map((v) => ({
      ...v,
      hit_rate: pct(v.hit_count, v.bet_count),
      recovery_rate: pct(v.payout_total, v.bet_total)
    }))
    .sort((a, b) => b.recovery_rate - a.recovery_rate);
}

function buildFailureModes(predictionsByRace, resultsByRace) {
  const failure = new Map();
  let analyzed = 0;
  let headHitCount = 0;
  let headTotal = 0;
  let exHighTotal = 0;
  let exHighHeadHit = 0;

  for (const [raceId, pred] of predictionsByRace.entries()) {
    const result = resultsByRace.get(raceId);
    if (!result) continue;
    analyzed += 1;

    const actualHead = toNum(result.finish_1, 0);
    const top3 = Array.isArray(pred?.prediction?.top3) ? pred.prediction.top3 : [];
    const predHead = toNum(top3[0], 0);
    const headHit = predHead > 0 && actualHead > 0 && predHead === actualHead;
    if (predHead > 0 && actualHead > 0) {
      headTotal += 1;
      if (headHit) headHitCount += 1;
    }

    const factors = pred?.raceDecision?.factors || {};
    const chaos = toNum(factors.chaos_risk_score, 50);
    const headPrecision = toNum(factors.head_precision_score, 50);
    const exhibition = toNum(factors.exhibition_ai_score, 50);

    if (exhibition >= 65) {
      exHighTotal += 1;
      if (headHit) exHighHeadHit += 1;
    }

    if (!headHit) {
      failure.set("HEAD_MISS", toNum(failure.get("HEAD_MISS"), 0) + 1);
      if (headPrecision >= 62) failure.set("HEAD_OVERCONFIDENCE", toNum(failure.get("HEAD_OVERCONFIDENCE"), 0) + 1);
    }
    if (!headHit && chaos <= 55) {
      failure.set("CHAOS_UNDERESTIMATED", toNum(failure.get("CHAOS_UNDERESTIMATED"), 0) + 1);
    }
    if (!headHit && exhibition >= 68) {
      failure.set("EXHIBITION_SIGNAL_NOISE", toNum(failure.get("EXHIBITION_SIGNAL_NOISE"), 0) + 1);
    }
  }

  return {
    analyzed,
    top_failure_modes: Array.from(failure.entries())
      .map(([code, count]) => ({
        code,
        count,
        rate: analyzed ? Number(((count / analyzed) * 100).toFixed(2)) : 0
      }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 8),
    head_precision_performance: {
      sample: headTotal,
      hit_count: headHitCount,
      hit_rate: pct(headHitCount, headTotal)
    },
    exhibition_ai_performance: {
      high_signal_sample: exHighTotal,
      high_signal_head_hit: exHighHeadHit,
      high_signal_head_hit_rate: pct(exHighHeadHit, exHighTotal)
    }
  };
}

function buildRecommendations({
  sampleSize,
  modeStats,
  venueStats,
  failureModes,
  currentWeights
}) {
  const weights = { ...DEFAULT_WEIGHT_PROFILE, ...(currentWeights || {}) };
  const headRate = toNum(failureModes?.head_precision_performance?.hit_rate, 0);
  const exRate = toNum(failureModes?.exhibition_ai_performance?.high_signal_head_hit_rate, 0);
  const chaosFailRate = toNum(
    (failureModes?.top_failure_modes || []).find((x) => x.code === "CHAOS_UNDERESTIMATED")?.rate,
    0
  );
  const headOverConfRate = toNum(
    (failureModes?.top_failure_modes || []).find((x) => x.code === "HEAD_OVERCONFIDENCE")?.rate,
    0
  );
  const fullBet = (modeStats || []).find((m) => String(m.mode).toUpperCase() === "FULL_BET");
  const fullBetRecovery = toNum(fullBet?.recovery_rate, 0);
  const venueSpread = (() => {
    if (!Array.isArray(venueStats) || venueStats.length < 2) return 0;
    const top = toNum(venueStats[0]?.recovery_rate, 0);
    const low = toNum(venueStats[venueStats.length - 1]?.recovery_rate, 0);
    return top - low;
  })();

  const deltas = {
    exhibition_weight: boundedDelta(sampleSize, exRate < 33 ? 0.02 : exRate > 52 ? -0.01 : 0),
    motor_weight: boundedDelta(sampleSize, fullBetRecovery < 90 ? 0.01 : 0),
    course_weight: boundedDelta(sampleSize, venueSpread >= 45 ? 0.02 : 0),
    chaos_weight: boundedDelta(sampleSize, chaosFailRate >= 16 ? 0.03 : chaosFailRate < 8 ? -0.01 : 0),
    head_precision_weight: boundedDelta(sampleSize, headRate < 35 ? 0.03 : headRate > 52 ? -0.01 : 0),
    venue_inner_weight: boundedDelta(sampleSize, venueSpread >= 45 ? 0.025 : 0),
    outer_lane_penalty: boundedDelta(sampleSize, headOverConfRate >= 10 ? 0.015 : 0),
    value_detection_weight: boundedDelta(sampleSize, fullBetRecovery < 92 ? 0.012 : 0)
  };

  const suggested = {};
  const recommendations = {};
  for (const key of Object.keys(weights)) {
    const current = toNum(weights[key], 0);
    const delta = sampleSize < 24 ? 0 : toNum(deltas[key], 0);
    const next = clamp(0.01, 0.5, current + delta);
    suggested[key] = roundWeight(next);
    recommendations[key] = {
      current: roundWeight(current),
      suggested: roundWeight(next),
      delta: roundWeight(next - current),
      confidence:
        sampleSize >= 180 ? "high" : sampleSize >= 100 ? "medium" : sampleSize >= 40 ? "low" : "very_low"
    };
  }

  const summary =
    sampleSize < 24
      ? "サンプル不足のため提案は参考レベルです。まずはデータ蓄積を優先。"
      : `提案生成完了: 頭精度${headRate.toFixed(1)}%、展示高信号時頭的中${exRate.toFixed(1)}%、CHAOS過小評価${chaosFailRate.toFixed(1)}%`;

  return {
    current_weights: weights,
    suggested_weights: suggested,
    recommendations,
    summary
  };
}

export function runSelfLearning({
  predictionRows,
  resultRows,
  placedRows,
  raceRows,
  mode = "proposal_only"
}) {
  const predictionsByRace = new Map((predictionRows || []).map((r) => [String(r.race_id), r]));
  const resultsByRace = new Map((resultRows || []).map((r) => [String(r.race_id), r]));
  const raceMap = new Map((raceRows || []).map((r) => [String(r.race_id), r]));
  const settledPlaced = (placedRows || []).filter((r) => toNum(r.hit_flag, -1) >= 0);

  const sampleSize = Array.from(predictionsByRace.keys()).filter((raceId) => resultsByRace.has(raceId)).length;
  const modeStats = buildModeStats(predictionsByRace, settledPlaced);
  const venueStats = buildVenueStats(settledPlaced, raceMap);
  const failureModes = buildFailureModes(predictionsByRace, resultsByRace);
  const recommendationPack = buildRecommendations({
    sampleSize,
    modeStats,
    venueStats,
    failureModes,
    currentWeights: DEFAULT_WEIGHT_PROFILE
  });

  const status = sampleSize >= 24 ? "ready" : "insufficient_data";
  return {
    status,
    mode,
    sample_size: sampleSize,
    raceDecision_performance: modeStats,
    venue_performance: venueStats.slice(0, 20),
    failure_mode_frequency: failureModes.top_failure_modes,
    head_precision_performance: failureModes.head_precision_performance,
    exhibition_ai_performance: failureModes.exhibition_ai_performance,
    recommendations: recommendationPack.recommendations,
    current_weights: recommendationPack.current_weights,
    suggested_weights: recommendationPack.suggested_weights,
    summary: recommendationPack.summary
  };
}
