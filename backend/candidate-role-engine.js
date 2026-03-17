function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function uniq(list) {
  return [...new Set((Array.isArray(list) ? list : []).map((x) => Number(x)).filter((x) => x >= 1 && x <= 6))];
}

export function analyzeRoleCandidates({
  ranking,
  headSelection,
  partnerSelection,
  exhibitionAI,
  raceFlow,
  playerStartProfiles,
  partnerPrecision
}) {
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

  const flowMode = String(raceFlow?.race_flow_mode || "");
  const nigeProb = toNum(raceFlow?.nige_prob, 0);
  const sashiProb = toNum(raceFlow?.sashi_prob, 0);
  const makuriProb = toNum(raceFlow?.makuri_prob, 0) + toNum(raceFlow?.makurizashi_prob, 0);

  const roleRows = rows.map((row) => {
    const lane = toNum(row?.racer?.lane);
    const actualLane = toNum(row?.features?.actual_lane ?? row?.racer?.entryCourse ?? row?.racer?.lane);
    const score = toNum(row?.score);
    const f = row?.features || {};
    const p = toNum(winProb?.[lane], 0);
    const stRank = toNum(f.st_rank, 6);
    const exRank = toNum(f.exhibition_rank, 6);
    const entryAdv = toNum(f.entry_advantage_score, 0);
    const trend = toNum(f.motor_trend_score, 0);
    const sp = playerStartProfiles?.by_lane?.[String(lane)] || {};

    let headScore = score * 0.52 + p * 100 * 0.38 + (7 - stRank) * 2.4 + (7 - exRank) * 2.1;
    let secondScore =
      score * 0.36 + (7 - stRank) * 4.0 + entryAdv * 3.2 + trend * 2.1 + (mainPartners.includes(lane) ? 8 : 0);
    let thirdScore =
      score * 0.24 +
      (7 - exRank) * 3.2 +
      entryAdv * 2.2 +
      (backupPartners.includes(lane) ? 8 : 0) +
      (mainPartners.includes(lane) ? 4 : 0);

    if (actualLane === toNum(exhibitionAI?.top_exhibition_lane, 0)) headScore += 10;
    if (actualLane === toNum(exhibitionAI?.stable_st_lane, 0)) secondScore += 8;
    if (actualLane === toNum(exhibitionAI?.breakout_lane, 0)) {
      secondScore += 5;
      thirdScore += 6;
    }
    if (actualLane === toNum(exhibitionAI?.weak_lane, 0)) {
      headScore -= 7;
      secondScore -= 4;
      thirdScore -= 4;
    }

    if (flowMode === "nige") {
      if (actualLane === 1) headScore += 10 * Math.max(0.4, nigeProb);
      if (actualLane === 2) secondScore += 8 * Math.max(0.35, nigeProb);
      if (actualLane === 3) thirdScore += 5 * Math.max(0.3, nigeProb);
    } else if (flowMode === "sashi") {
      if (actualLane === 2) headScore += 9 * Math.max(0.35, sashiProb);
      if (actualLane === 1) secondScore += 6 * Math.max(0.3, sashiProb);
      if (actualLane === 3 || actualLane === 4) thirdScore += 5 * Math.max(0.25, sashiProb);
    } else if (flowMode === "makuri" || flowMode === "makurizashi") {
      if (actualLane === 3 || actualLane === 4) headScore += 9 * Math.max(0.35, makuriProb);
      if (actualLane === 1 || actualLane === 2) secondScore += 4 * Math.max(0.25, makuriProb);
      if (actualLane >= 4) thirdScore += 5 * Math.max(0.25, makuriProb);
    }

    headScore += toNum(sp.start_stability_score, 50) * 0.06;
    secondScore += toNum(sp.start_attack_score, 50) * 0.07;
    thirdScore += toNum(sp.makuri_style_score, 50) * 0.04;

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

  const secondBlend = uniq([...(partnerPrecision?.second_candidates || []), ...second_candidates]).slice(0, 4);
  const thirdBlend = uniq([...(partnerPrecision?.third_candidates || []), ...third_candidates]).slice(0, 4);

  const summary = `head:${head_candidates.join(",")} / 2nd:${secondBlend.join(",")} / 3rd:${thirdBlend.join(",")}`;

  return {
    head_candidates,
    second_candidates: secondBlend,
    third_candidates: thirdBlend,
    fade_lanes: fade,
    summary
  };
}
