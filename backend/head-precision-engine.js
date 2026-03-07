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

export function evaluateHeadPrecision({
  ranking,
  headSelection,
  probabilities,
  raceIndexes,
  raceOutcomeProbabilities,
  exhibitionAI,
  venueBias
}) {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!rows.length) {
    return {
      head_win_score: 0,
      head_gap_score: 0,
      main_head: null,
      backup_heads: []
    };
  }

  const scoreNorm = normalize(rows.map((r) => toNum(r?.score, 0)));
  const winMap = headSelection?.win_prob_by_lane || {};
  const escapeP = toNum(raceOutcomeProbabilities?.escape_success_prob, 0);
  const sashiP = toNum(raceOutcomeProbabilities?.sashi_success_prob, 0);
  const makuriP = toNum(raceOutcomeProbabilities?.makuri_success_prob, 0);
  const nige = toNum(raceIndexes?.nige_index, 50);
  const are = toNum(raceIndexes?.are_index, 50);
  const venueInner = toNum(venueBias?.venue_inner_reliability, 50);
  const venueChaos = toNum(venueBias?.venue_chaos_factor, 50);
  const venueStyle = String(venueBias?.venue_style_bias || "balanced");

  const candidateRows = rows.map((row, idx) => {
    const lane = toNum(row?.racer?.lane, 0);
    const f = row?.features || {};
    const stQ = rankQuality(f.st_rank);
    const exQ = rankQuality(f.exhibition_rank);
    const motorQ = clamp(0, 1, toNum(f.motor_total_score, 0) / 20);
    const entryQ = clamp(0, 1, (toNum(f.entry_advantage_score, 0) + 12) / 24);
    const baseWinProb = toNum(winMap?.[lane], 0);

    let laneShape = 0;
    if (lane === 1) laneShape = escapeP * 0.9 + nige / 120;
    else if (lane === 2) laneShape = sashiP * 0.75;
    else if (lane === 3 || lane === 4) laneShape = makuriP * 0.65;
    else laneShape = (1 - are / 120) * 0.15;

    let headRaw =
      scoreNorm[idx] * 0.36 +
      stQ * 0.2 +
      exQ * 0.18 +
      motorQ * 0.13 +
      entryQ * 0.08 +
      baseWinProb * 0.22 +
      laneShape * 0.14;

    if (lane === 1) {
      headRaw += (venueInner - 50) * 0.0028;
      headRaw -= (venueChaos - 50) * 0.0018;
    } else if (lane >= 3) {
      headRaw += (venueChaos - 50) * 0.0015;
    }
    if (venueStyle === "inner" && lane <= 2) headRaw += 0.035;
    if (venueStyle === "chaos" && lane >= 3) headRaw += 0.03;

    if (lane === toNum(exhibitionAI?.top_exhibition_lane, 0)) headRaw += 0.11;
    if (lane === toNum(exhibitionAI?.stable_st_lane, 0)) headRaw += 0.08;
    if (lane === toNum(exhibitionAI?.breakout_lane, 0)) headRaw += 0.09;
    if (lane === toNum(exhibitionAI?.weak_lane, 0)) headRaw -= 0.08;

    return { lane, headRaw };
  });

  const probs = softmax(candidateRows, "headRaw");
  const byHead = candidateRows
    .map((r, idx) => ({ ...r, p: probs[idx] }))
    .sort((a, b) => b.p - a.p);

  const main = byHead[0];
  const second = byHead[1];
  const third = byHead[2];
  const topProb = toNum(main?.p, 0);
  const gap12 = topProb - toNum(second?.p, 0);
  const gap23 = toNum(second?.p, 0) - toNum(third?.p, 0);

  const topCombos = (Array.isArray(probabilities) ? probabilities : [])
    .map((x) => toNum(x?.p ?? x?.prob, 0))
    .filter((x) => x > 0)
    .sort((a, b) => b - a);
  const concentration = (topCombos[0] || 0) + (topCombos[1] || 0) + (topCombos[2] || 0);

  const head_win_score = clamp(
    0,
    100,
    topProb * 100 * 0.54 +
      concentration * 100 * 0.17 +
      Math.max(0, 70 - are) * 0.2 +
      Math.max(0, venueInner - 50) * 0.18 -
      Math.max(0, venueChaos - 55) * 0.08
  );
  const head_gap_score = clamp(
    0,
    100,
    gap12 * 100 * 2.8 + gap23 * 100 * 0.8 + (main?.lane === 1 ? nige * 0.25 : 0)
  );

  return {
    head_win_score: Number(head_win_score.toFixed(2)),
    head_gap_score: Number(head_gap_score.toFixed(2)),
    main_head: Number(main?.lane) || null,
    backup_heads: [second?.lane, third?.lane]
      .map((x) => Number(x))
      .filter((x) => Number.isInteger(x))
  };
}
