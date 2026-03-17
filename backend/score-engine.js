const LANE_BONUS = {
  1: 18,
  2: 10,
  3: 7,
  4: 4,
  5: 2,
  6: 1
};

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function isUsableField(f, field) {
  return !!f?.prediction_field_meta?.[field]?.is_usable;
}

function buildFetchedSignalScoreBreakdown(f) {
  const lapTimeContribution = Number(
    clamp(
      0,
      28,
      (isUsableField(f, "lapTime") ? Math.max(0, 7 - (f.lap_time_rank ?? 6)) * 3.1 : 0) +
        (isUsableField(f, "lapTime") ? Math.max(0, (f.lap_time_delta_vs_front ?? 0)) * 95 : 0) +
        Math.max(0, f.lap_attack_strength || 0) * 0.5
    ).toFixed(2)
  );
  const startExhibitionContribution = Number(
    clamp(
      0,
      18,
      (isUsableField(f, "exhibitionST") ? Math.max(0, 7 - (f.st_rank ?? 6)) * 2 : 0) +
        Math.max(0, 7 - (f.expected_actual_st_rank ?? 6)) * 2 +
        (f.slit_alert_flag ? 2.5 : 0)
    ).toFixed(2)
  );
  const motor2renContribution = Number(
    clamp(0, 20, (isUsableField(f, "motor2ren") ? (f.motor2_rate || 0) * 0.24 : 0) + Math.max(0, 7 - (f.exhibition_rank ?? 6)) * 0.8).toFixed(2)
  );
  const motor3renContribution = Number(
    clamp(0, 12, isUsableField(f, "motor3ren") && Number.isFinite(f.motor3_rate) ? f.motor3_rate * 0.12 : 0).toFixed(2)
  );
  return {
    lap_time_contribution: lapTimeContribution,
    exhibition_st_contribution: startExhibitionContribution,
    motor_2ren_contribution: motor2renContribution,
    motor_3ren_contribution: motor3renContribution,
    total:
      lapTimeContribution +
      startExhibitionContribution +
      motor2renContribution +
      motor3renContribution
  };
}

export function calcRacerScore(f) {
  let score = 0;
  const actualLane = Number.isFinite(Number(f?.actual_lane)) ? Number(f.actual_lane) : Number(f?.lane || 0);
  score += LANE_BONUS[actualLane] ?? 0;
  score += f.class_score * 4.0;
  score += f.nationwide_win_rate * 1.8;
  score += f.local_win_rate * 2.2;
  score += (isUsableField(f, "motor2ren") && Number.isFinite(f.motor2_rate) ? f.motor2_rate : 0) * 0.26;
  score += (isUsableField(f, "motor3ren") && Number.isFinite(f.motor3_rate) ? f.motor3_rate : 0) * 0.06;
  score += f.boat2_rate * 0.18;
  score += f.st_inv * 24;
  score += (f.expected_actual_st_inv || 0) * 16;
  score += f.local_minus_nation * 1.2;

  // Exhibition bonuses
  if (f.exhibition_rank === 1) score += 7;
  else if (f.exhibition_rank === 2) score += 3.5;

  // Exhibition ST bonus
  if (f.st_rank === 1) score += 4;
  else if (f.st_rank === 2) score += 2;
  if (f.expected_actual_st_rank === 1) score += 3;
  else if (f.expected_actual_st_rank === 2) score += 1.5;

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

  const fetchedSignalBreakdown = buildFetchedSignalScoreBreakdown(f);
  score += fetchedSignalBreakdown.total;

  return score;
}

export function rankRace(racersWithFeatures) {
  const ranked = [...(racersWithFeatures || [])]
    .map((item) => {
      const fetchedSignalBreakdown = buildFetchedSignalScoreBreakdown(item.features);
      return {
        ...item,
        score: calcRacerScore(item.features),
        fetchedSignalBreakdown
      };
    })
    .sort((a, b) => b.score - a.score);
  const signalOnlyRanks = [...ranked]
    .sort((a, b) => b.fetchedSignalBreakdown.total - a.fetchedSignalBreakdown.total)
    .map((item, idx) => [Number.isFinite(Number(item?.features?.actual_lane)) ? Number(item.features.actual_lane) : item.racer?.lane, idx + 1]);
  const signalRankMap = new Map(signalOnlyRanks);
  return ranked
    .map((item, idx) => ({
      rank: idx + 1,
      score: Number(item.score.toFixed(4)),
      racer: item.racer,
      features: {
        ...item.features,
        fetched_signal_score_breakdown: {
          ...item.fetchedSignalBreakdown,
          signal_only_rank: signalRankMap.get(Number.isFinite(Number(item?.features?.actual_lane)) ? Number(item.features.actual_lane) : item.racer?.lane) ?? null,
          final_rank: idx + 1
        }
      }
    }));
}
