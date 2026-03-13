function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function rankQuality(rank, maxRank = 6) {
  const r = toNum(rank, maxRank);
  return clamp(0, 1, (maxRank + 1 - r) / maxRank);
}

export function analyzePlayerStartProfiles({ ranking }) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const profiles = rows.map((row) => {
    const lane = toNum(row?.racer?.lane, 0);
    const f = row?.features || {};
    const racer = row?.racer || {};

    const stRankQ = rankQuality(f.st_rank);
    const exRankQ = rankQuality(f.exhibition_rank);
    const stInv = toNum(f.st_inv, 0);
    const avgSt = toNum(racer.avgSt ?? f.avg_st, 0);
    const classScore = toNum(f.class_score, 2);
    const entryAdv = toNum(f.entry_advantage_score, 0);
    const motorTrend = toNum(f.motor_trend_score, 0);
    const localDiff = toNum(f.local_minus_nation, 0);
    const slitAlertFlag = toNum(f.slit_alert_flag, 0);
    const displayTimeDeltaVsLeft = toNum(f.display_time_delta_vs_left, 0);
    const avgStRankDeltaVsLeft = toNum(f.avg_st_rank_delta_vs_left, 0);
    const slitAttackBoost = slitAlertFlag
      ? Math.min(12, 6 + displayTimeDeltaVsLeft * 20 + avgStRankDeltaVsLeft * 1.5)
      : 0;

    const start_attack_score = clamp(
      0,
      100,
      stRankQ * 34 + exRankQ * 22 + stInv * 14 + entryAdv * 1.3 + Math.max(0, motorTrend) * 3 + slitAttackBoost
    );
    const start_stability_score = clamp(
      0,
      100,
      (avgSt > 0 ? clamp(0, 1, (0.25 - avgSt) / 0.18) * 28 : 12) +
        exRankQ * 24 +
        stRankQ * 24 +
        classScore * 6 +
        Math.max(-2, localDiff) * 2
    );

    const nige_style_score = clamp(
      0,
      100,
      (lane === 1 ? 22 : 4) + start_stability_score * 0.55 + classScore * 4 + Math.max(0, localDiff) * 2.5
    );
    const sashi_style_score = clamp(
      0,
      100,
      (lane === 2 ? 20 : lane === 1 ? 8 : 6) + start_attack_score * 0.52 + entryAdv * 1.7 + classScore * 3 + slitAttackBoost * 0.45
    );
    const makuri_style_score = clamp(
      0,
      100,
      (lane === 3 || lane === 4 ? 22 : lane >= 5 ? 12 : 6) +
        start_attack_score * 0.5 +
        Math.max(0, motorTrend) * 4 +
        Math.max(0, entryAdv) * 1.4 +
        slitAttackBoost * 0.75
    );

    const styleRows = [
      { key: "nige", score: nige_style_score },
      { key: "sashi", score: sashi_style_score },
      { key: "makuri", score: makuri_style_score }
    ].sort((a, b) => b.score - a.score);

    return {
      lane,
      start_attack_score: Number(start_attack_score.toFixed(2)),
      start_stability_score: Number(start_stability_score.toFixed(2)),
      nige_style_score: Number(nige_style_score.toFixed(2)),
      sashi_style_score: Number(sashi_style_score.toFixed(2)),
      makuri_style_score: Number(makuri_style_score.toFixed(2)),
      slit_alert_flag: slitAlertFlag ? 1 : 0,
      slit_attack_boost: Number(slitAttackBoost.toFixed(2)),
      player_start_profile: styleRows[0]?.key || "sashi"
    };
  });

  const by_lane = {};
  profiles.forEach((p) => {
    by_lane[String(p.lane)] = p;
  });

  return {
    profiles,
    by_lane
  };
}
