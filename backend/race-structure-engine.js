function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

export function analyzeRaceStructure({
  ranking,
  probabilities,
  headConfidence,
  raceIndexes,
  preRaceAnalysis,
  roleCandidates,
  exhibitionAI
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const probs = (Array.isArray(probabilities) ? probabilities : [])
    .map((x) => toNum(x?.p ?? x?.prob))
    .filter((x) => x > 0)
    .sort((a, b) => b - a);

  const top1 = probs[0] || 0;
  const top3 = (probs[0] || 0) + (probs[1] || 0) + (probs[2] || 0);
  const headConf = toNum(headConfidence?.head_confidence, 0.5);
  const chaosIndex = toNum(raceIndexes?.are_index, 50);
  const windRisk = toNum(preRaceAnalysis?.wind_risk_score, 50);
  const exhibitionScore = toNum(exhibitionAI?.exhibition_ai_score, 50);

  const outerTop = rows.slice(0, 3).filter((r) => toNum(r?.racer?.lane) >= 5).length;
  const fadeCount = Array.isArray(roleCandidates?.fade_lanes) ? roleCandidates.fade_lanes.length : 0;

  const head_stability_score = clamp(
    0,
    100,
    headConf * 100 * 0.72 + top1 * 100 * 0.18 + exhibitionScore * 0.1
  );
  const top3_concentration_score = clamp(0, 100, top3 * 100);
  const chaos_risk_score = clamp(
    0,
    100,
    chaosIndex * 0.52 + windRisk * 0.24 + outerTop * 7 + fadeCount * 2 + (70 - exhibitionScore) * 0.12
  );
  const race_structure_score = clamp(
    0,
    100,
    head_stability_score * 0.4 + top3_concentration_score * 0.35 + (100 - chaos_risk_score) * 0.25
  );

  let summary = "構造は標準";
  if (race_structure_score >= 68) summary = "構造は安定、的中重視で組みやすい";
  else if (race_structure_score < 48) summary = "構造が不安定、慎重運用または見送り寄り";

  return {
    head_stability_score: Number(head_stability_score.toFixed(2)),
    top3_concentration_score: Number(top3_concentration_score.toFixed(2)),
    chaos_risk_score: Number(chaos_risk_score.toFixed(2)),
    race_structure_score: Number(race_structure_score.toFixed(2)),
    summary
  };
}
