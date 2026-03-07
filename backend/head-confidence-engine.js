function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function summarize(ok, spread) {
  if (ok && !spread) return "頭固定向き";
  if (!ok && spread) return "頭分散推奨";
  if (ok && spread) return "準固定可、相手広め";
  return "頭慎重、分散寄り";
}

export function evaluateHeadConfidence({
  headSelection,
  raceRisk,
  raceIndexes,
  raceOutcomeProbabilities,
  probabilities,
  wallEvaluation
}) {
  const winMap = headSelection?.win_prob_by_lane || {};
  const rows = Object.entries(winMap)
    .map(([lane, p]) => ({ lane: Number(lane), p: toNum(p) }))
    .filter((x) => Number.isInteger(x.lane) && x.lane >= 1 && x.lane <= 6)
    .sort((a, b) => b.p - a.p);

  const mainHead = Number(headSelection?.main_head);
  const secondaryHeads = (Array.isArray(headSelection?.secondary_heads) ? headSelection.secondary_heads : [])
    .map((x) => Number(x))
    .filter((x) => Number.isInteger(x));

  const top = rows[0]?.p || 0;
  const second = rows[1]?.p || 0;
  const third = rows[2]?.p || 0;
  const dominanceGap = top - second;
  const headVsSecondaryGap =
    secondaryHeads.length > 0
      ? top - Math.max(...secondaryHeads.map((lane) => toNum(winMap?.[lane], 0)))
      : dominanceGap;

  const risk = toNum(raceRisk?.risk_score);
  const areIndex = toNum(raceIndexes?.are_index);
  const nigeIndex = toNum(raceIndexes?.nige_index);
  const sashiIndex = toNum(raceIndexes?.sashi_index);
  const makuriIndex = toNum(raceIndexes?.makuri_index);

  const escapeP = toNum(raceOutcomeProbabilities?.escape_success_prob);
  const sashiP = toNum(raceOutcomeProbabilities?.sashi_success_prob);
  const makuriP = toNum(raceOutcomeProbabilities?.makuri_success_prob);

  const probRows = Array.isArray(probabilities) ? probabilities : [];
  const topCombos = probRows
    .map((x) => toNum(x?.p ?? x?.prob))
    .filter((x) => Number.isFinite(x) && x > 0)
    .sort((a, b) => b - a);

  const topCombo = topCombos[0] || 0;
  const top3Sum = (topCombos[0] || 0) + (topCombos[1] || 0) + (topCombos[2] || 0);
  const concentration = clamp(0, 1, topCombo * 1.8 + top3Sum * 0.55);

  const wallStrength = toNum(wallEvaluation?.wall_strength, 50);
  const wallBreakRisk = toNum(wallEvaluation?.wall_break_risk, 50);

  const dominanceScore = clamp(0, 1, top * 1.75 + dominanceGap * 3.0 + headVsSecondaryGap * 1.3 - third * 0.4);

  const indexStability = clamp(
    0,
    1,
    nigeIndex * 0.006 +
      Math.max(0, 60 - areIndex) * 0.004 +
      Math.max(0, 70 - Math.max(sashiIndex, makuriIndex)) * 0.002
  );

  const outcomeShape = clamp(
    0,
    1,
    Math.max(escapeP, sashiP, makuriP) * 1.2 - Math.abs(sashiP - makuriP) * 0.2
  );

  const riskPenalty = clamp(0, 1, risk / 115 + areIndex / 210);
  const wallPenalty = clamp(0, 1, wallBreakRisk / 120 - wallStrength / 240);

  let confidence =
    dominanceScore * 0.36 +
    concentration * 0.2 +
    indexStability * 0.16 +
    outcomeShape * 0.14 -
    riskPenalty * 0.1 -
    wallPenalty * 0.08;

  confidence = clamp(0, 1, confidence);
  const head_confidence = Number(confidence.toFixed(4));

  const head_fixed_ok =
    head_confidence >= 0.62 &&
    dominanceGap >= 0.03 &&
    headVsSecondaryGap >= 0.02 &&
    risk <= 92 &&
    areIndex < 78;

  const head_spread_needed =
    head_confidence < 0.56 ||
    dominanceGap < 0.018 ||
    headVsSecondaryGap < 0.015 ||
    areIndex >= 72 ||
    wallBreakRisk >= 68;

  return {
    head_confidence,
    head_fixed_ok,
    head_spread_needed,
    summary: summarize(head_fixed_ok, head_spread_needed)
  };
}
