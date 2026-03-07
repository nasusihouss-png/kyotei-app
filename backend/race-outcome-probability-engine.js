function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function laneRow(ranking, lane) {
  return (
    (ranking || []).find((r) => toNum(r?.racer?.lane) === lane) || {
      score: 0,
      features: {},
      racer: { lane }
    }
  );
}

function rankQuality(rank, maxRank = 6) {
  const r = toNum(rank, maxRank);
  return clamp(0, 1, (maxRank + 1 - r) / maxRank);
}

function softmax(values) {
  if (!values.length) return [];
  const max = Math.max(...values);
  const exps = values.map((v) => Math.exp(v - max));
  const sum = exps.reduce((a, b) => a + b, 0) || 1;
  return exps.map((v) => v / sum);
}

function normalizeScore(score, mean, std) {
  return std > 0 ? (score - mean) / std : 0;
}

export function estimateRaceOutcomeProbabilities({
  raceIndexes,
  raceRisk,
  racePattern,
  ranking
}) {
  const rows = ranking || [];
  const l1 = laneRow(rows, 1);
  const l2 = laneRow(rows, 2);
  const l3 = laneRow(rows, 3);
  const l4 = laneRow(rows, 4);

  const scores = rows.map((r) => toNum(r?.score));
  const mean = scores.length ? scores.reduce((a, b) => a + b, 0) / scores.length : 0;
  const variance =
    scores.length > 0
      ? scores.reduce((a, b) => a + (b - mean) ** 2, 0) / scores.length
      : 0;
  const std = Math.sqrt(variance) || 1;

  const s1 = normalizeScore(toNum(l1.score), mean, std);
  const s2 = normalizeScore(toNum(l2.score), mean, std);
  const s3 = normalizeScore(toNum(l3.score), mean, std);
  const s4 = normalizeScore(toNum(l4.score), mean, std);

  const nigeIndex = toNum(raceIndexes?.nige_index);
  const sashiIndex = toNum(raceIndexes?.sashi_index);
  const makuriIndex = toNum(raceIndexes?.makuri_index);
  const areIndex = toNum(raceIndexes?.are_index);
  const riskScore = toNum(raceRisk?.risk_score);

  const ex1 = rankQuality(l1.features?.exhibition_rank);
  const ex2 = rankQuality(l2.features?.exhibition_rank);
  const ex3 = rankQuality(l3.features?.exhibition_rank);
  const st1 = rankQuality(l1.features?.st_rank);
  const st2 = rankQuality(l2.features?.st_rank);
  const st3 = rankQuality(l3.features?.st_rank);

  const entryAdv1 = toNum(l1.features?.entry_advantage_score);
  const entryAdv2 = toNum(l2.features?.entry_advantage_score);
  const entryAdv3 = toNum(l3.features?.entry_advantage_score);
  const motor3 = toNum(l3.features?.motor_total_score);
  const motor4 = toNum(l4.features?.motor_total_score);
  const trend3 = toNum(l3.features?.motor_trend_score);
  const trend4 = toNum(l4.features?.motor_trend_score);

  // Escape: lane1 quality/stability vs attack and risk.
  let escapeLogit =
    nigeIndex * 0.07 +
    s1 * 1.2 +
    ex1 * 0.9 +
    st1 * 1.0 +
    toNum(l1.features?.course_fit_score) * 0.03 -
    (s2 + s3) * 0.35 -
    Math.max(0, entryAdv1 * -0.2) -
    areIndex * 0.02 -
    riskScore * 0.015;

  // Sashi: lane2 attack + lane1 weakness + pattern support.
  let sashiLogit =
    sashiIndex * 0.07 +
    s2 * 1.1 +
    ex2 * 0.8 +
    st2 * 1.0 +
    entryAdv2 * 0.08 +
    Math.max(0, (s2 + s3) / 2 - s1) * 0.6 +
    areIndex * 0.005 -
    riskScore * 0.006;

  // Makuri: lane3/4 momentum + entry edge + pattern support.
  let makuriLogit =
    makuriIndex * 0.07 +
    Math.max(s3, s4) * 1.05 +
    ex3 * 0.7 +
    st3 * 0.9 +
    (motor3 + motor4) * 0.035 +
    (trend3 + trend4) * 0.06 +
    Math.max(entryAdv3, toNum(l4.features?.entry_advantage_score)) * 0.1 +
    areIndex * 0.01 -
    riskScore * 0.005;

  if (racePattern === "escape") escapeLogit += 0.45;
  if (racePattern === "sashi") sashiLogit += 0.45;
  if (racePattern === "makuri" || racePattern === "makurizashi") makuriLogit += 0.45;
  if (racePattern === "chaos") {
    escapeLogit -= 0.2;
    sashiLogit += 0.1;
    makuriLogit += 0.15;
  }

  const [escapeP, sashiP, makuriP] = softmax([
    escapeLogit,
    sashiLogit,
    makuriLogit
  ]);

  // Ensure numeric stability and exact sum = 1.
  let e = clamp(0, 1, escapeP);
  let s = clamp(0, 1, sashiP);
  let m = clamp(0, 1, makuriP);
  const sum = e + s + m || 1;
  e /= sum;
  s /= sum;
  m /= sum;

  e = Number(e.toFixed(4));
  s = Number(s.toFixed(4));
  m = Number((1 - e - s).toFixed(4));

  return {
    escape_success_prob: e,
    sashi_success_prob: s,
    makuri_success_prob: m
  };
}
