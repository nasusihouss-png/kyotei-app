function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function avg(list) {
  const arr = (Array.isArray(list) ? list : []).filter((v) => Number.isFinite(v));
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function normalizeDescending(values) {
  const arr = values.map((v) => toNum(v, 0));
  const min = Math.min(...arr);
  const max = Math.max(...arr);
  if (!Number.isFinite(min) || !Number.isFinite(max) || max === min) {
    return arr.map(() => 0.5);
  }
  return arr.map((v) => (v - min) / (max - min));
}

function normalizeAscending(values) {
  return normalizeDescending(values.map((v) => -toNum(v, 0)));
}

function rankScore(rank) {
  const r = toNum(rank, 6);
  return clamp(0, 1, (7 - r) / 6);
}

export function analyzeExhibitionAI({ ranking }) {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!rows.length) {
    return {
      exhibition_time_score: 0,
      exhibition_st_score: 0,
      exhibition_gap_score: 0,
      exhibition_rank_score: 0,
      exhibition_breakout_score: 0,
      exhibition_ai_score: 0,
      top_exhibition_lane: null,
      stable_st_lane: null,
      breakout_lane: null,
      weak_lane: null
    };
  }

  const lanes = rows.map((r) => toNum(r?.racer?.lane, 0));
  const exTimes = rows.map((r) => toNum(r?.racer?.exhibitionTime ?? r?.features?.exhibition_time, NaN));
  const exSts = rows.map((r) => toNum(r?.racer?.exhibitionST ?? r?.features?.exhibition_st, NaN));
  const exRanks = rows.map((r) => toNum(r?.features?.exhibition_rank, 6));
  const exGaps = rows.map((r) => toNum(r?.features?.exhibition_gap_from_best, NaN));

  const fieldAvgTime = avg(exTimes);
  const fieldAvgSt = avg(exSts);

  const timeNorm = normalizeAscending(exTimes.map((v) => (Number.isFinite(v) ? v : fieldAvgTime + 0.3)));
  const stNorm = normalizeAscending(exSts.map((v) => (Number.isFinite(v) ? v : fieldAvgSt + 0.08)));
  const gapNorm = normalizeAscending(exGaps.map((v) => (Number.isFinite(v) ? v : 0.2)));
  const rankNorm = exRanks.map((r) => rankScore(r));

  const perLane = rows.map((row, i) => {
    const lane = lanes[i];
    const relTime = Number.isFinite(exTimes[i]) ? fieldAvgTime - exTimes[i] : 0;
    const relSt = Number.isFinite(exSts[i]) ? fieldAvgSt - exSts[i] : 0;
    const breakout =
      timeNorm[i] * 0.38 + stNorm[i] * 0.26 + gapNorm[i] * 0.2 + rankNorm[i] * 0.16 + relTime * 2.2 + relSt * 3.1;
    const ai =
      timeNorm[i] * 0.32 + stNorm[i] * 0.24 + gapNorm[i] * 0.16 + rankNorm[i] * 0.16 + clamp(0, 1, relTime * 4 + 0.5) * 0.12;
    return {
      lane,
      breakout,
      ai
    };
  });

  const topEx = [...perLane].sort((a, b) => b.ai - a.ai)[0]?.lane ?? null;
  const stableSt = [...rows]
    .sort((a, b) => toNum(a?.features?.st_rank, 6) - toNum(b?.features?.st_rank, 6))[0]
    ?.racer?.lane ?? null;
  const breakoutLane = [...perLane].sort((a, b) => b.breakout - a.breakout)[0]?.lane ?? null;
  const weakLane = [...perLane].sort((a, b) => a.ai - b.ai)[0]?.lane ?? null;

  const exhibition_time_score = clamp(0, 100, avg(timeNorm) * 100);
  const exhibition_st_score = clamp(0, 100, avg(stNorm) * 100);
  const exhibition_gap_score = clamp(0, 100, avg(gapNorm) * 100);
  const exhibition_rank_score = clamp(0, 100, avg(rankNorm) * 100);
  const exhibition_breakout_score = clamp(0, 100, (Math.max(...perLane.map((p) => p.breakout)) / 1.8) * 100);
  const exhibition_ai_score = clamp(
    0,
    100,
    exhibition_time_score * 0.32 +
      exhibition_st_score * 0.24 +
      exhibition_gap_score * 0.14 +
      exhibition_rank_score * 0.14 +
      exhibition_breakout_score * 0.16
  );

  return {
    exhibition_time_score: Number(exhibition_time_score.toFixed(2)),
    exhibition_st_score: Number(exhibition_st_score.toFixed(2)),
    exhibition_gap_score: Number(exhibition_gap_score.toFixed(2)),
    exhibition_rank_score: Number(exhibition_rank_score.toFixed(2)),
    exhibition_breakout_score: Number(exhibition_breakout_score.toFixed(2)),
    exhibition_ai_score: Number(exhibition_ai_score.toFixed(2)),
    top_exhibition_lane: topEx,
    stable_st_lane: Number(stableSt) || null,
    breakout_lane: breakoutLane,
    weak_lane: weakLane
  };
}
