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
  score += f.st_inv * 24;
  score += (f.expected_actual_st_inv || 0) * 16;
  score += f.local_minus_nation * 1.2;

  // Exhibition bonuses
  if (f.exhibition_rank === 1) score += 8;
  else if (f.exhibition_rank === 2) score += 4;
  score += Math.max(0, 7 - (f.exhibition_rank ?? 6)) * 1.6;
  score += Math.max(0, f.lap_attack_strength || 0) * 0.22;
  if (f.lap_attack_flag === 1) score += 3.5;

  // Exhibition ST bonus
  if (f.st_rank === 1) score += 7;
  else if (f.st_rank === 2) score += 3;
  if (f.expected_actual_st_rank === 1) score += 4;
  else if (f.expected_actual_st_rank === 2) score += 2;

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
  score -= f.f_hold_caution_penalty || 0;

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
