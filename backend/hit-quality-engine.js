function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

export function analyzeHitQuality({
  ranking,
  raceRisk,
  headConfidence,
  partnerSelection,
  oddsData,
  probabilities
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const top3 = rows.slice(0, 3);
  const risk = toNum(raceRisk?.risk_score, 50);
  const headConf = toNum(headConfidence?.head_confidence, 0.5);
  const mainPartners = Array.isArray(partnerSelection?.main_partners) ? partnerSelection.main_partners : [];

  const innerTopCount = top3.filter((r) => {
    const lane = toNum(r?.racer?.lane);
    return lane >= 1 && lane <= 3;
  }).length;

  const outsideWeakPenalty = top3.reduce((acc, row) => {
    const lane = toNum(row?.racer?.lane);
    if (lane < 5) return acc;
    const score = toNum(row?.score);
    return acc + (score < toNum(rows[0]?.score) * 0.9 ? 8 : 0);
  }, 0);

  const inner_reliability_score = clamp(
    0,
    100,
    innerTopCount * 28 + headConf * 28 + Math.max(0, 45 - risk) * 0.45 - outsideWeakPenalty
  );

  const mainPartnerFocus = clamp(0, 100, 100 - Math.abs(mainPartners.length - 3) * 15);
  const solid_ticket_score = clamp(
    0,
    100,
    headConf * 52 + inner_reliability_score * 0.26 + mainPartnerFocus * 0.22 - Math.max(0, risk - 70) * 0.35
  );

  const probRows = Array.isArray(probabilities) ? probabilities : [];
  const topProb = probRows
    .map((x) => toNum(x?.p ?? x?.prob))
    .filter((x) => x > 0)
    .sort((a, b) => b - a)
    .slice(0, 5);
  const probConcentration = topProb.reduce((a, b) => a + b, 0);

  const trifectaOdds = Array.isArray(oddsData?.trifecta) ? oddsData.trifecta : [];
  const averageTopOdds =
    trifectaOdds.slice(0, 10).reduce((a, b) => a + toNum(b?.odds), 0) /
    Math.max(1, trifectaOdds.slice(0, 10).length);
  const oddsBalance = clamp(0, 100, 78 - Math.max(0, averageTopOdds - 25) * 1.2);

  const odds_adjusted_ticket_score = clamp(
    0,
    100,
    solid_ticket_score * 0.72 + oddsBalance * 0.18 + probConcentration * 100 * 0.1
  );

  const hit_mode_score = clamp(
    0,
    100,
    inner_reliability_score * 0.38 +
      solid_ticket_score * 0.34 +
      odds_adjusted_ticket_score * 0.28
  );

  return {
    hit_mode_score: Number(hit_mode_score.toFixed(2)),
    solid_ticket_score: Number(solid_ticket_score.toFixed(2)),
    inner_reliability_score: Number(inner_reliability_score.toFixed(2)),
    odds_adjusted_ticket_score: Number(odds_adjusted_ticket_score.toFixed(2))
  };
}
