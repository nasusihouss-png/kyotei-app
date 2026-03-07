function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function uniq(list) {
  return [...new Set((Array.isArray(list) ? list : []).map((x) => Number(x)).filter((x) => x >= 1 && x <= 6))];
}

export function analyzeRoleCandidates({ ranking, headSelection, partnerSelection }) {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!rows.length) {
    return {
      head_candidates: [],
      second_candidates: [],
      third_candidates: [],
      fade_lanes: []
    };
  }

  const winProb = headSelection?.win_prob_by_lane || {};
  const fade = uniq(partnerSelection?.fade_lanes);
  const mainPartners = uniq(partnerSelection?.main_partners);
  const backupPartners = uniq(partnerSelection?.backup_partners);

  const roleRows = rows.map((row) => {
    const lane = toNum(row?.racer?.lane);
    const score = toNum(row?.score);
    const f = row?.features || {};
    const p = toNum(winProb?.[lane], 0);
    const stRank = toNum(f.st_rank, 6);
    const exRank = toNum(f.exhibition_rank, 6);
    const entryAdv = toNum(f.entry_advantage_score, 0);
    const trend = toNum(f.motor_trend_score, 0);

    const headScore = score * 0.52 + p * 100 * 0.38 + (7 - stRank) * 2.4 + (7 - exRank) * 2.1;
    const secondScore = score * 0.36 + (7 - stRank) * 4.0 + entryAdv * 3.2 + trend * 2.1 + (mainPartners.includes(lane) ? 8 : 0);
    const thirdScore = score * 0.24 + (7 - exRank) * 3.2 + entryAdv * 2.2 + (backupPartners.includes(lane) ? 8 : 0) + (mainPartners.includes(lane) ? 4 : 0);

    return {
      lane,
      headScore,
      secondScore,
      thirdScore
    };
  });

  const head_candidates = roleRows
    .filter((r) => !fade.includes(r.lane))
    .sort((a, b) => b.headScore - a.headScore)
    .slice(0, 3)
    .map((r) => r.lane);

  const second_candidates = roleRows
    .filter((r) => !fade.includes(r.lane))
    .sort((a, b) => b.secondScore - a.secondScore)
    .slice(0, 4)
    .map((r) => r.lane);

  const third_candidates = roleRows
    .filter((r) => !fade.includes(r.lane))
    .sort((a, b) => b.thirdScore - a.thirdScore)
    .slice(0, 4)
    .map((r) => r.lane);

  const summary = `頭:${head_candidates.join(",")} / 2着:${second_candidates.join(",")} / 3着:${third_candidates.join(",")}`;

  return {
    head_candidates,
    second_candidates,
    third_candidates,
    fade_lanes: fade,
    summary
  };
}
