const LANE_BONUS = {
  1: 18,
  2: 10,
  3: 7,
  4: 4,
  5: 2,
  6: 1
};

export function calcRacerScore(f) {
  let score = 0;
  score += LANE_BONUS[f.lane] ?? 0;
  score += f.class_score * 4.0;
  score += f.nationwide_win_rate * 1.8;
  score += f.local_win_rate * 2.2;
  score += f.motor2_rate * 0.32;
  score += f.boat2_rate * 0.18;
  score += f.st_inv * 35;
  score += f.local_minus_nation * 1.2;

  // Exhibition bonuses
  if (f.exhibition_rank === 1) score += 6;
  else if (f.exhibition_rank === 2) score += 3;

  // Exhibition ST bonus
  if (f.st_rank === 1) score += 5;

  // Tilt bonus (+0.5)
  score += f.tilt_bonus || 0;

  // Course change penalty
  if (f.course_change === 1) score -= 3;

  // Strong-wind penalty for outside lanes
  if (f.is_outer === 1 && (f.wind_speed || 0) >= 6) {
    score -= 4;
  }

  // Course performance adjustment
  score += f.course_fit_score || 0;

  // Motor-performance adjustment
  score += f.motor_total_score || 0;

  // Entry dynamics adjustment
  score += f.entry_advantage_score || 0;

  // Motor trend adjustment
  score += f.motor_trend_score || 0;

  // Venue adjustment (lane bias by venue profile)
  score += f.venue_lane_adjustment || 0;

  return score;
}

export function rankRace(racersWithFeatures) {
  return [...(racersWithFeatures || [])]
    .map((item) => ({
      ...item,
      score: calcRacerScore(item.features)
    }))
    .sort((a, b) => b.score - a.score)
    .map((item, idx) => ({
      rank: idx + 1,
      score: Number(item.score.toFixed(4)),
      racer: item.racer,
      features: item.features
    }));
}
