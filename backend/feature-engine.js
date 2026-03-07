const CLASS_SCORE = {
  A1: 4,
  A2: 3,
  B1: 2,
  B2: 1
};

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function buildAscendingRanks(valuesByLane) {
  const valid = valuesByLane
    .filter((v) => Number.isFinite(v.value))
    .sort((a, b) => a.value - b.value);

  const rankMap = new Map();
  valid.forEach((v, idx) => {
    rankMap.set(v.lane, idx + 1);
  });
  return rankMap;
}

export function buildFeatures(racer) {
  const lane = toNumber(racer?.lane, 0);
  const class_score = CLASS_SCORE[racer?.class] ?? 0;
  const nationwide_win_rate = toNumber(racer?.nationwideWinRate, 0);
  const local_win_rate = toNumber(racer?.localWinRate, 0);
  const motor2_rate = toNumber(racer?.motor2Rate, 0);
  const boat2_rate = toNumber(racer?.boat2Rate, 0);
  const weight = toNumber(racer?.weight, 0);
  const avg_st_raw = racer?.avgSt;
  const avg_st = Number.isFinite(Number(avg_st_raw)) ? Number(avg_st_raw) : null;
  const local_minus_nation = local_win_rate - nationwide_win_rate;
  const motor_boat_avg = (motor2_rate + boat2_rate) / 2;
  const st_inv = avg_st && avg_st > 0 ? 1 / avg_st : 0;
  const is_inner = lane >= 1 && lane <= 3 ? 1 : 0;
  const is_outer = lane >= 5 && lane <= 6 ? 1 : 0;
  const exhibition_time_raw = racer?.exhibitionTime;
  const exhibition_time =
    Number.isFinite(Number(exhibition_time_raw)) ? Number(exhibition_time_raw) : null;
  const exhibition_st_raw = racer?.exhibitionSt;
  const exhibition_st =
    Number.isFinite(Number(exhibition_st_raw)) ? Number(exhibition_st_raw) : null;
  const entry_course_raw = racer?.entryCourse;
  const entry_course =
    Number.isFinite(Number(entry_course_raw)) ? Number(entry_course_raw) : null;
  const tilt_raw = racer?.tilt;
  const tilt = Number.isFinite(Number(tilt_raw)) ? Number(tilt_raw) : null;
  const wind_speed_raw = racer?.windSpeed;
  const wind_speed =
    Number.isFinite(Number(wind_speed_raw)) ? Number(wind_speed_raw) : 0;
  const course_change = entry_course && lane ? (entry_course !== lane ? 1 : 0) : 0;
  const tilt_bonus = tilt === 0.5 ? 2 : 0;

  return {
    lane,
    class_score,
    nationwide_win_rate,
    local_win_rate,
    motor2_rate,
    boat2_rate,
    weight,
    avg_st,
    local_minus_nation,
    motor_boat_avg,
    st_inv,
    is_inner,
    is_outer,
    exhibition_time,
    exhibition_st,
    entry_course,
    tilt,
    wind_speed,
    course1_win_rate: null,
    course1_2rate: null,
    course2_2rate: null,
    course3_3rate: null,
    course4_3rate: null,
    course_fit_score: 0,
    motor_base_score: 0,
    motor_exhibition_score: 0,
    motor_momentum_score: 0,
    motor_total_score: 0,
    motor_rate_bias: 0,
    exhibition_bias: 0,
    trend_up_score: 0,
    trend_down_score: 0,
    motor_trend_score: 0,
    course_change_score: 0,
    kado_bonus: 0,
    deep_in_penalty: 0,
    entry_chaos_bonus: 0,
    entry_advantage_score: 0,
    venue_inner_lane_multiplier: 1,
    venue_lane_adjustment: 0,
    venue_chaos_adjustment: 0,
    exhibition_rank: null,
    st_rank: null,
    course_change,
    tilt_bonus,
    exhibition_gap_from_best: 0,
    motor_gap_from_best: 0,
    local_gap_from_best: 0,
    nation_gap_from_best: 0
  };
}

export function buildRaceFeatures(racers, raceContext = {}) {
  const raceWindSpeed = Number.isFinite(Number(raceContext?.windSpeed))
    ? Number(raceContext.windSpeed)
    : 0;

  const base = (racers || []).map((racer) => {
    const racerWithContext = {
      ...racer,
      windSpeed: raceWindSpeed
    };

    return {
      racer: racerWithContext,
      features: buildFeatures(racerWithContext)
    };
  });

  const bestMotor = Math.max(...base.map((x) => x.features.motor2_rate), 0);
  const bestLocal = Math.max(...base.map((x) => x.features.local_win_rate), 0);
  const bestNation = Math.max(...base.map((x) => x.features.nationwide_win_rate), 0);
  const bestExhibition = Math.min(
    ...base
      .map((x) => x.features.exhibition_time)
      .filter((v) => Number.isFinite(v)),
    Number.POSITIVE_INFINITY
  );
  const exhibitionRanks = buildAscendingRanks(
    base.map((x) => ({ lane: x.features.lane, value: x.features.exhibition_time }))
  );
  const stRanks = buildAscendingRanks(
    base.map((x) => ({ lane: x.features.lane, value: x.features.exhibition_st }))
  );

  return base.map((item) => {
    const f = item.features;
    return {
      ...item,
      features: {
        ...f,
        exhibition_rank: exhibitionRanks.get(f.lane) ?? null,
        st_rank: stRanks.get(f.lane) ?? null,
        exhibition_gap_from_best:
          Number.isFinite(bestExhibition) && Number.isFinite(f.exhibition_time)
            ? f.exhibition_time - bestExhibition
            : 0,
        motor_gap_from_best: bestMotor - f.motor2_rate,
        local_gap_from_best: bestLocal - f.local_win_rate,
        nation_gap_from_best: bestNation - f.nationwide_win_rate
      }
    };
  });
}
