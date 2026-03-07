function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function round4(value) {
  return Number(value.toFixed(4));
}

function calcMotorBaseScore(f) {
  // Motor has higher influence than boat by requirement.
  return f.motor2_rate * 0.22 + f.boat2_rate * 0.1;
}

function calcMotorExhibitionScore(f) {
  const rankBonusMap = {
    1: 4,
    2: 2.5,
    3: 1.5,
    4: 0.5,
    5: -0.5,
    6: -1.5
  };

  const rankBonus = rankBonusMap[f.exhibition_rank] ?? 0;
  const gap = toNumber(f.exhibition_gap_from_best, 0);

  // Small gap to best exhibition is positive, large gap is negative.
  const gapScore = (0.12 - gap) * 18;

  // Additional penalty for clearly slow exhibition.
  const slowPenalty = gap >= 0.15 ? -2 : 0;

  return rankBonus + gapScore + slowPenalty;
}

function calcMotorMomentumScore(f) {
  const motor = toNumber(f.motor2_rate, 0);
  const rank = toNumber(f.exhibition_rank, 99);
  const motorGap = toNumber(f.motor_gap_from_best, 0);
  const exhibitionGap = toNumber(f.exhibition_gap_from_best, 0);

  let m = 0;

  if (motor >= 40 && rank <= 2) m += 3;
  else if (motor >= 35 && rank <= 2) m += 2.2;
  else if (motor >= 30 && rank <= 3) m += 1.2;

  if (motor <= 20 && rank >= 5) m -= 2.2;
  else if (motor <= 25 && rank >= 4) m -= 1.2;

  if (motorGap <= 3 && exhibitionGap <= 0.05) m += 1.5;
  if (motorGap >= 15 && exhibitionGap >= 0.12) m -= 1.8;

  return m;
}

export function applyMotorPerformanceFeatures(racersWithFeatures) {
  return (racersWithFeatures || []).map((item) => {
    const f = item.features || {};

    const motor_base_score = round4(calcMotorBaseScore(f));
    const motor_exhibition_score = round4(calcMotorExhibitionScore(f));
    const motor_momentum_score = round4(calcMotorMomentumScore(f));
    const motor_total_score = round4(
      motor_base_score + motor_exhibition_score + motor_momentum_score
    );

    return {
      ...item,
      features: {
        ...f,
        motor_base_score,
        motor_exhibition_score,
        motor_momentum_score,
        motor_total_score
      }
    };
  });
}
