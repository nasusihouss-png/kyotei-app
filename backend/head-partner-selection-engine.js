function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function normalize(values) {
  const arr = values.map((v) => toNum(v, 0));
  const min = Math.min(...arr);
  const max = Math.max(...arr);
  if (!Number.isFinite(min) || !Number.isFinite(max) || max === min) {
    return arr.map(() => 0.5);
  }
  return arr.map((v) => (v - min) / (max - min));
}

function rankQuality(rank, maxRank = 6) {
  const r = toNum(rank, maxRank);
  return clamp(0, 1, (maxRank + 1 - r) / maxRank);
}

function softmax(rows, key) {
  if (!rows.length) return [];
  const vals = rows.map((r) => toNum(r[key]));
  const max = Math.max(...vals);
  const exps = vals.map((v) => Math.exp(v - max));
  const sum = exps.reduce((a, b) => a + b, 0) || 1;
  return exps.map((v) => v / sum);
}

export function analyzeHeadAndPartners({
  ranking,
  raceIndexes,
  raceOutcomeProbabilities,
  raceRisk
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!rows.length) {
    return {
      headSelection: {
        win_prob_by_lane: {},
        main_head: null,
        secondary_heads: []
      },
      partnerSelection: {
        main_partners: [],
        backup_partners: [],
        fade_lanes: []
      }
    };
  }

  const style = String(raceIndexes?.recommended_style || "mixed");
  const riskScore = toNum(raceRisk?.risk_score);
  const escapeP = toNum(raceOutcomeProbabilities?.escape_success_prob);
  const sashiP = toNum(raceOutcomeProbabilities?.sashi_success_prob);
  const makuriP = toNum(raceOutcomeProbabilities?.makuri_success_prob);

  const baseScores = rows.map((r) => toNum(r.score));
  const scoreNorm = normalize(baseScores);

  const adjusted = rows.map((row, idx) => {
    const lane = toNum(row?.racer?.lane, 0);
    const f = row?.features || {};

    const exQ = rankQuality(f.exhibition_rank);
    const stQ = rankQuality(f.st_rank);
    const motorT = toNum(f.motor_total_score);
    const trend = toNum(f.motor_trend_score);
    const entryAdv = toNum(f.entry_advantage_score);

    let styleBias = 0;
    if (style === "nige-main" && lane === 1) styleBias += 0.14;
    if (style === "sashi-main" && lane === 2) styleBias += 0.12;
    if (style === "makuri-main" && (lane === 3 || lane === 4)) styleBias += 0.12;
    if (style === "chaos" && lane >= 5) styleBias += 0.05;

    // outcome probability alignment by lane archetype
    let outcomeBias = 0;
    if (lane === 1) outcomeBias += escapeP * 0.22;
    if (lane === 2) outcomeBias += sashiP * 0.2;
    if (lane === 3 || lane === 4) outcomeBias += makuriP * 0.18;

    // higher risk slightly compresses edge, but keeps ranking order meaningful
    const riskCompression = clamp(0.82, 1, 1 - riskScore / 650);

    const headRaw =
      scoreNorm[idx] * 0.44 +
      exQ * 0.13 +
      stQ * 0.14 +
      motorT * 0.012 +
      trend * 0.016 +
      entryAdv * 0.02 +
      styleBias +
      outcomeBias;

    const partnerRaw =
      scoreNorm[idx] * 0.3 +
      exQ * 0.2 +
      stQ * 0.2 +
      motorT * 0.013 +
      trend * 0.018 +
      entryAdv * 0.04 +
      (lane === 1 ? -0.03 : 0); // avoid over-concentrating partner set on lane1

    return {
      lane,
      headRaw: headRaw * riskCompression,
      partnerRaw
    };
  });

  const probs = softmax(adjusted, "headRaw");
  const withProb = adjusted.map((r, idx) => ({
    ...r,
    winProb: probs[idx]
  }));

  const byHead = [...withProb].sort((a, b) => b.winProb - a.winProb);
  const mainHead = byHead[0]?.lane ?? null;
  const secondaryHeads = byHead.slice(1, 3).map((r) => r.lane);

  const partnerPool = withProb.filter((r) => r.lane !== mainHead);
  const byPartner = [...partnerPool].sort((a, b) => b.partnerRaw - a.partnerRaw);

  const bestPartnerRaw = byPartner[0]?.partnerRaw ?? 0;
  const filteredByLaneQuality = byPartner.filter((r) => {
    if (r.lane <= 4) return true;
    // lane 5/6 requires strong support to avoid weak spread
    return r.partnerRaw >= bestPartnerRaw - 0.12;
  });

  const mainPartners = filteredByLaneQuality.slice(0, 3).map((r) => r.lane);
  const backupPartners = filteredByLaneQuality.slice(3, 5).map((r) => r.lane);
  const selected = new Set([mainHead, ...mainPartners, ...backupPartners]);
  const fadeLanes = withProb
    .filter((r) => !selected.has(r.lane))
    .sort((a, b) => a.partnerRaw - b.partnerRaw)
    .map((r) => r.lane);

  const winProbByLane = {};
  for (const row of byHead) {
    winProbByLane[row.lane] = Number(row.winProb.toFixed(4));
  }

  return {
    headSelection: {
      win_prob_by_lane: winProbByLane,
      main_head: mainHead,
      secondary_heads: secondaryHeads
    },
    partnerSelection: {
      main_partners: mainPartners,
      backup_partners: backupPartners,
      fade_lanes: fadeLanes
    }
  };
}
