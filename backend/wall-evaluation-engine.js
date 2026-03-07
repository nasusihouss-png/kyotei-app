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

function laneRow(ranking, lane) {
  return (
    (ranking || []).find((r) => toNum(r?.racer?.lane) === lane) || {
      score: 0,
      features: {},
      racer: { lane }
    }
  );
}

function summarize(strength, breakRisk, racePattern) {
  if (strength >= 70 && breakRisk < 38) {
    return "2コース壁は強め、まくり抑制傾向";
  }
  if (strength >= 55 && breakRisk < 55) {
    return "2コース壁は標準、展開次第";
  }
  if (racePattern === "makuri" || racePattern === "makurizashi") {
    return "2コース壁弱め、3-4のまくり通過注意";
  }
  return "2コース壁やや不安、3-4攻撃警戒";
}

export function evaluateLane2Wall({
  ranking,
  raceIndexes,
  racePattern
}) {
  const l2 = laneRow(ranking, 2);
  const l3 = laneRow(ranking, 3);
  const l4 = laneRow(ranking, 4);

  const l2Score = toNum(l2?.score);
  const l2StQ = rankQuality(l2?.features?.st_rank);
  const l2ExQ = rankQuality(l2?.features?.exhibition_rank);
  const l2CourseFit = toNum(l2?.features?.course_fit_score);
  const l2MotorTotal = toNum(l2?.features?.motor_total_score);
  const l2MotorTrend = toNum(l2?.features?.motor_trend_score);
  const l2EntryAdv = toNum(l2?.features?.entry_advantage_score);

  const l3Attack =
    toNum(l3?.score) * 0.46 +
    toNum(l3?.features?.motor_total_score) * 0.9 +
    toNum(l3?.features?.motor_trend_score) * 0.8 +
    toNum(l3?.features?.entry_advantage_score) * 1.25 +
    rankQuality(l3?.features?.st_rank) * 10;

  const l4Attack =
    toNum(l4?.score) * 0.43 +
    toNum(l4?.features?.motor_total_score) * 0.85 +
    toNum(l4?.features?.motor_trend_score) * 0.78 +
    toNum(l4?.features?.entry_advantage_score) * 1.15 +
    rankQuality(l4?.features?.st_rank) * 9;

  const attackStrength = (l3Attack + l4Attack) / 2;
  const makuriIndex = toNum(raceIndexes?.makuri_index);

  let patternPenalty = 0;
  if (racePattern === "makuri") patternPenalty += 6;
  if (racePattern === "makurizashi") patternPenalty += 4;
  if (racePattern === "escape") patternPenalty -= 2;

  const wallStrengthRaw =
    44 +
    l2Score * 0.16 +
    l2StQ * 20 +
    l2ExQ * 16 +
    l2CourseFit * 1.0 +
    l2MotorTotal * 0.42 +
    l2MotorTrend * 0.36 +
    l2EntryAdv * 1.6 -
    attackStrength * 0.085 -
    makuriIndex * 0.12 -
    patternPenalty;

  const wallStrength = Number(clamp(0, 100, wallStrengthRaw).toFixed(2));

  const wallBreakRiskRaw =
    100 - wallStrength +
    Math.max(0, attackStrength - l2Score) * 0.1 +
    Math.max(0, makuriIndex - 50) * 0.5 +
    Math.max(0, -l2EntryAdv) * 2.6 +
    patternPenalty * 1.2;

  const wallBreakRisk = Number(clamp(0, 100, wallBreakRiskRaw).toFixed(2));

  return {
    wall_strength: wallStrength,
    wall_break_risk: wallBreakRisk,
    summary: summarize(wallStrength, wallBreakRisk, racePattern)
  };
}
