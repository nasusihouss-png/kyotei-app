function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function round4(value) {
  return Number(value.toFixed(4));
}

function calcMotorRateBias(f) {
  const motor = toNumber(f.motor2_rate, 0);
  const boat = toNumber(f.boat2_rate, 0);
  // Relative motor-vs-boat balance + absolute motor strength.
  return (motor - boat) * 0.06 + (motor - 30) * 0.04;
}

function calcExhibitionBias(f) {
  const expectedGap = 0.1 - toNumber(f.motor2_rate, 0) / 1000;
  const actualGap = toNumber(f.exhibition_gap_from_best, 0);
  // Better than expected exhibition => positive.
  return (expectedGap - actualGap) * 20;
}

function calcTrendUpScore(f, racePattern) {
  const exhibitionRank = toNumber(f.exhibition_rank, 99);
  const stRank = toNumber(f.st_rank, 99);
  const motorTotal = toNumber(f.motor_total_score, 0);

  let score = 0;
  if (exhibitionRank <= 2) score += 1.8;
  else if (exhibitionRank <= 3) score += 1.0;

  if (motorTotal >= 14) score += 1.4;
  else if (motorTotal >= 10) score += 0.8;

  if (stRank <= 2) score += 1.0;
  else if (stRank <= 3) score += 0.5;

  if (racePattern === "makuri" || racePattern === "makurizashi") {
    score += 0.3;
  }

  return score;
}

function calcTrendDownScore(f) {
  const motorBase = toNumber(f.motor_base_score, 0);
  const exhibitionRank = toNumber(f.exhibition_rank, 99);
  const exhibitionGap = toNumber(f.exhibition_gap_from_best, 0);

  let penalty = 0;

  if (motorBase >= 9 && exhibitionRank >= 5) penalty += 1.5;
  else if (motorBase >= 7 && exhibitionRank >= 4) penalty += 0.9;

  if (exhibitionGap >= 0.12) penalty += 1.2;
  else if (exhibitionGap >= 0.08) penalty += 0.6;

  return penalty;
}

export function applyMotorTrendFeatures(racersWithFeatures, context = {}) {
  const racePattern = context.racePattern || "standard";

  return (racersWithFeatures || []).map((item) => {
    const f = item.features || {};

    const motor_rate_bias = round4(calcMotorRateBias(f));
    const exhibition_bias = round4(calcExhibitionBias(f));
    const trend_up_score = round4(calcTrendUpScore(f, racePattern));
    const trend_down_score = round4(calcTrendDownScore(f));
    const motor_trend_score = round4(
      motor_rate_bias + exhibition_bias + trend_up_score - trend_down_score
    );

    return {
      ...item,
      features: {
        ...f,
        motor_rate_bias,
        exhibition_bias,
        trend_up_score,
        trend_down_score,
        motor_trend_score
      }
    };
  });
}
