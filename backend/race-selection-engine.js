function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function calcPartnerClarityScore(roleCandidates) {
  const second = Array.isArray(roleCandidates?.second_candidates) ? roleCandidates.second_candidates : [];
  const third = Array.isArray(roleCandidates?.third_candidates) ? roleCandidates.third_candidates : [];
  const fade = Array.isArray(roleCandidates?.fade_lanes) ? roleCandidates.fade_lanes : [];

  const overlap = second.filter((x) => third.includes(x)).length;
  const coreSpreadPenalty = Math.max(0, second.length - 4) * 8 + Math.max(0, third.length - 4) * 6;
  const fadeBonus = Math.min(20, fade.length * 6);

  return clamp(0, 100, 58 + overlap * 8 + fadeBonus - coreSpreadPenalty);
}

function calcValueBalanceScore(ticketOptimization) {
  const warningPenalty = ticketOptimization?.value_warning ? 16 : 0;
  const oddsAdjusted = toNum(ticketOptimization?.odds_adjusted_ticket_score, 50);
  const ticketConfidence = toNum(ticketOptimization?.ticket_confidence_score, 50);
  return clamp(0, 100, oddsAdjusted * 0.45 + ticketConfidence * 0.55 - warningPenalty);
}

export function decideRaceSelection({
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
}) {
  const head_stability_score = toNum(raceStructure?.head_stability_score, 50);
  const chaos_risk_score = toNum(raceStructure?.chaos_risk_score, 50);
  const pre_race_form_score = toNum(preRaceAnalysis?.pre_race_form_score, 50);
  const partner_clarity_score = calcPartnerClarityScore(roleCandidates);
  const value_balance_score = calcValueBalanceScore(ticketOptimization);
  const exhibition_ai_score = toNum(exhibitionAI?.exhibition_ai_score, 50);
  const partner_precision_score = clamp(
    0,
    100,
    toNum(partnerPrecision?.second_place_fit_score, 50) * 0.55 +
      toNum(partnerPrecision?.third_place_fit_score, 50) * 0.45
  );
  const venue_bias_score = toNum(venueBias?.venue_bias_score, 50);
  const venue_inner_reliability = toNum(venueBias?.venue_inner_reliability, 50);
  const venue_chaos_factor = toNum(venueBias?.venue_chaos_factor, 50);
  const trap_score = toNum(marketTrap?.trap_score, 35);
  const flow_confidence = toNum(raceFlow?.flow_confidence, 0.45) * 100;
  const flow_mode = String(raceFlow?.race_flow_mode || "");
  const head_precision_score = clamp(
    0,
    100,
    toNum(headPrecision?.head_win_score, 50) * 0.7 + toNum(headPrecision?.head_gap_score, 50) * 0.3
  );

  const race_select_score = clamp(
    0,
    100,
    head_stability_score * 0.24 +
      head_precision_score * 0.12 +
      partner_clarity_score * 0.22 +
      pre_race_form_score * 0.2 +
      (100 - chaos_risk_score) * 0.14 +
      value_balance_score * 0.05 +
      partner_precision_score * 0.05 +
      exhibition_ai_score * 0.03 +
      venue_bias_score * 0.02 -
      trap_score * 0.05 +
      flow_confidence * 0.04
  );

  let mode = "SMALL_BET";
  const venueRiskPenalty = Math.max(0, venue_chaos_factor - 62) * 0.18;
  const venueHeadBoost = Math.max(0, venue_inner_reliability - 58) * 0.12;
  if (
    race_select_score + venueHeadBoost - venueRiskPenalty >= 66 &&
    chaos_risk_score <= 64 + (venue_inner_reliability >= 60 ? 2 : 0) &&
    head_stability_score >= 56
  ) {
    mode = "FULL_BET";
  } else if (race_select_score < 42 || chaos_risk_score >= 88 || head_stability_score < 34) {
    mode = "SKIP";
  }

  const reason_codes = [];
  if (head_stability_score >= 65) reason_codes.push("HEAD_STABLE");
  if (head_precision_score >= 64) reason_codes.push("HEAD_PRECISION_HIGH");
  if (partner_clarity_score >= 62) reason_codes.push("PARTNER_CLEAR");
  if (partner_precision_score >= 60) reason_codes.push("PARTNER_PRECISION_HIGH");
  if (pre_race_form_score >= 62) reason_codes.push("PRE_RACE_GOOD");
  if (chaos_risk_score <= 52) reason_codes.push("LOW_CHAOS");
  if (value_balance_score >= 58) reason_codes.push("VALUE_BALANCED");
  if (exhibition_ai_score >= 62) reason_codes.push("EXHIBITION_STRONG");
  if (venue_bias_score >= 58) reason_codes.push("VENUE_BIAS_FAVORABLE");
  if (venue_chaos_factor >= 65) reason_codes.push("VENUE_CHAOS_HIGH");
  if (trap_score >= 65) reason_codes.push("MARKET_TRAP_HIGH");
  else if (trap_score >= 45) reason_codes.push("MARKET_TRAP_MEDIUM");
  if (flow_confidence >= 55) reason_codes.push("FLOW_STABLE");
  if (flow_mode === "chaos") reason_codes.push("FLOW_CHAOTIC");
  if (chaos_risk_score >= 74) reason_codes.push("CHAOS_HIGH");
  if (head_stability_score < 42) reason_codes.push("HEAD_WEAK");
  if (head_precision_score < 42) reason_codes.push("HEAD_PRECISION_LOW");

  let summary = "総合的に判断";
  if (mode === "FULL_BET") summary = "頭安定・相手明確で本線向き";
  else if (mode === "SMALL_BET") summary = "本線はあるが不確定要素あり";
  else summary = "不安定要素が強く見送り推奨";

  const confidence = clamp(
    0,
    100,
    mode === "SKIP"
      ? 100 - race_select_score * 0.45 + chaos_risk_score * 0.35
      : race_select_score * 0.8 + head_stability_score * 0.2
  );

  return {
    mode,
    confidence: Number(confidence.toFixed(2)),
    race_select_score: Number(race_select_score.toFixed(2)),
    reason_codes,
    summary,
    factors: {
      head_stability_score: Number(head_stability_score.toFixed(2)),
      head_precision_score: Number(head_precision_score.toFixed(2)),
      partner_clarity_score: Number(partner_clarity_score.toFixed(2)),
      partner_precision_score: Number(partner_precision_score.toFixed(2)),
      pre_race_form_score: Number(pre_race_form_score.toFixed(2)),
      exhibition_ai_score: Number(exhibition_ai_score.toFixed(2)),
      chaos_risk_score: Number(chaos_risk_score.toFixed(2)),
      value_balance_score: Number(value_balance_score.toFixed(2)),
      venue_bias_score: Number(venue_bias_score.toFixed(2)),
      venue_inner_reliability: Number(venue_inner_reliability.toFixed(2)),
      venue_chaos_factor: Number(venue_chaos_factor.toFixed(2)),
      trap_score: Number(trap_score.toFixed(2)),
      flow_confidence: Number(flow_confidence.toFixed(2))
    }
  };
}
