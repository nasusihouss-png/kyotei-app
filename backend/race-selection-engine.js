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
  ticketOptimization
}) {
  const head_stability_score = toNum(raceStructure?.head_stability_score, 50);
  const chaos_risk_score = toNum(raceStructure?.chaos_risk_score, 50);
  const pre_race_form_score = toNum(preRaceAnalysis?.pre_race_form_score, 50);
  const partner_clarity_score = calcPartnerClarityScore(roleCandidates);
  const value_balance_score = calcValueBalanceScore(ticketOptimization);

  const race_select_score = clamp(
    0,
    100,
    head_stability_score * 0.3 +
      partner_clarity_score * 0.24 +
      pre_race_form_score * 0.2 +
      (100 - chaos_risk_score) * 0.16 +
      value_balance_score * 0.1
  );

  let mode = "SMALL_BET";
  if (race_select_score >= 72 && chaos_risk_score <= 56 && head_stability_score >= 64) mode = "FULL_BET";
  else if (race_select_score < 50 || chaos_risk_score >= 74 || head_stability_score < 42) mode = "SKIP";

  const reason_codes = [];
  if (head_stability_score >= 65) reason_codes.push("HEAD_STABLE");
  if (partner_clarity_score >= 62) reason_codes.push("PARTNER_CLEAR");
  if (pre_race_form_score >= 62) reason_codes.push("PRE_RACE_GOOD");
  if (chaos_risk_score <= 52) reason_codes.push("LOW_CHAOS");
  if (value_balance_score >= 58) reason_codes.push("VALUE_BALANCED");
  if (chaos_risk_score >= 74) reason_codes.push("CHAOS_HIGH");
  if (head_stability_score < 42) reason_codes.push("HEAD_WEAK");

  let summary = "様子見寄り";
  if (mode === "FULL_BET") summary = "頭安定、相手絞りやすく、本線向き";
  else if (mode === "SMALL_BET") summary = "一定の軸はあるが、慎重に小口運用";
  else summary = "頭の不安定要素または混戦度が高く、見送り推奨";

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
      partner_clarity_score: Number(partner_clarity_score.toFixed(2)),
      pre_race_form_score: Number(pre_race_form_score.toFixed(2)),
      chaos_risk_score: Number(chaos_risk_score.toFixed(2)),
      value_balance_score: Number(value_balance_score.toFixed(2))
    }
  };
}
