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

function toNullableNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
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
  const motor3_rate = toNumber(racer?.motor3Rate, 0);
  const boat2_rate = toNumber(racer?.boat2Rate, 0);
  const weight = toNumber(racer?.weight, 0);
  const avg_st_raw = racer?.avgSt;
  const avg_st = Number.isFinite(Number(avg_st_raw)) ? Number(avg_st_raw) : null;
  const local_minus_nation = local_win_rate - nationwide_win_rate;
  const motor_boat_avg = (motor2_rate + boat2_rate) / 2;
  const st_inv = avg_st && avg_st > 0 ? 1 / avg_st : 0;
  const is_inner = lane >= 1 && lane <= 3 ? 1 : 0;
  const is_outer = lane >= 5 && lane <= 6 ? 1 : 0;
  const exhibition_time_raw = racer?.kyoteiBiyoriExhibitionTime ?? racer?.exhibitionTime;
  const exhibition_time =
    Number.isFinite(Number(exhibition_time_raw)) ? Number(exhibition_time_raw) : null;
  const exhibition_st_raw = racer?.kyoteiBiyoriExhibitionSt ?? racer?.exhibitionSt;
  const exhibition_st =
    Number.isFinite(Number(exhibition_st_raw)) ? Number(exhibition_st_raw) : null;
  const lap_time = toNullableNumber(racer?.kyoteiBiyoriLapTime ?? racer?.lapTime);
  const lap_exhibition_score = toNullableNumber(racer?.kyoteiBiyoriLapExhibitionScore ?? racer?.lapExhibitionScore);
  const entry_course_raw = racer?.entryCourse;
  const entry_course =
    Number.isFinite(Number(entry_course_raw)) ? Number(entry_course_raw) : null;
  const tilt_raw = racer?.tilt;
  const tilt = Number.isFinite(Number(tilt_raw)) ? Number(tilt_raw) : null;
  const wind_speed_raw = racer?.windSpeed;
  const wind_speed =
    Number.isFinite(Number(wind_speed_raw)) ? Number(wind_speed_raw) : 0;
  const course1_win_rate = toNullableNumber(racer?.course1WinRate ?? racer?.course1_win_rate);
  const course1_2rate = toNullableNumber(racer?.course1_2rate ?? racer?.course1_2Rate);
  const course2_2rate = toNullableNumber(racer?.course2_2rate ?? racer?.course2_2Rate);
  const course3_3rate = toNullableNumber(racer?.course3_3rate ?? racer?.course3_3Rate);
  const course4_3rate = toNullableNumber(racer?.course4_3rate ?? racer?.course4_3Rate);
  const course_change = entry_course && lane ? (entry_course !== lane ? 1 : 0) : 0;
  const tilt_bonus = tilt === 0.5 ? 2 : 0;
  const laneFirstRate = toNullableNumber(racer?.lane1stAvg ?? racer?.laneFirstRate);
  const lane2RenRate = toNullableNumber(racer?.lane2renAvg ?? racer?.lane2RenRate);
  const lane3RenRate = toNullableNumber(racer?.lane3renAvg ?? racer?.lane3RenRate);
  const lapExStretch = toNullableNumber(racer?.lapExStretch ?? racer?.kyoteiBiyoriLapExhibitionScore ?? racer?.lapExhibitionScore);

  return {
    lane,
    class_score,
    nationwide_win_rate,
    local_win_rate,
    motor2_rate,
    motor3_rate,
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
    lap_time,
    lap_exhibition_score,
    stretch_foot_label: racer?.kyoteiBiyoriStretchFootLabel ?? racer?.stretchFootLabel ?? null,
    entry_course,
    tilt,
    wind_speed,
    official_nationwide_win_rate: toNullableNumber(racer?.officialNationwideWinRate ?? racer?.nationwideWinRate),
    official_local_win_rate: toNullableNumber(racer?.officialLocalWinRate ?? racer?.localWinRate),
    player_recent_3_months_strength: toNullableNumber(racer?.playerRecent3MonthsStrength),
    player_current_season_strength: toNullableNumber(racer?.playerCurrentSeasonStrength),
    player_strength_blended: toNullableNumber(racer?.playerStrengthBlended),
    player_stat_confidence: toNullableNumber(racer?.playerStatConfidence),
    recent_3_months_sample_size: toNumber(racer?.recent3MonthsSampleSize, 0),
    current_season_sample_size: toNumber(racer?.currentSeasonSampleSize, 0),
    player_stat_fallback_used: toNumber(racer?.playerStatFallbackUsed, 0),
    course1_win_rate,
    course1_2rate,
    course2_2rate,
    course3_3rate,
    course4_3rate,
    laneFirstRate,
    lane2RenRate,
    lane3RenRate,
    lane_avg_st: avg_st,
    lane_st_rank: null,
    hidden_f_flag: 0,
    unresolved_f_count: Math.max(0, toNumber(racer?.fHoldCount, 0)),
    start_caution_penalty: 0,
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
    avg_st_rank: null,
    expected_actual_st_rank: null,
    course_change,
    tilt_bonus,
    exhibition_gap_from_best: 0,
    motor_gap_from_best: 0,
    local_gap_from_best: 0,
    nation_gap_from_best: 0,
    left_neighbor_exists: 0,
    display_time_delta_vs_left: null,
    avg_st_rank_delta_vs_left: null,
    slit_alert_flag: 0,
    front_boat_exists: 0,
    lap_time_delta_vs_front: null,
    lap_time_rank: null,
    lap_attack_flag: 0,
    lap_attack_strength: 0,
    f_hold_count: Math.max(0, toNumber(racer?.fHoldCount, 0)),
    f_hold_bias_applied: 0,
    expected_actual_st_adjustment: 0,
    expected_actual_st: null,
    expected_actual_st_inv: 0,
    f_hold_caution_penalty: 0,
    kyoteibiyori_fetched: toNumber(racer?.kyoteiBiyoriFetched, 0),
    lap_time_gap_from_best: 0,
    motor_true: 0,
    motor_form: {
      lapTime: lap_time,
      lapExStretch,
      exhibitionTime: exhibition_time
    },
    lane_fit_1st: laneFirstRate,
    lane_fit_2ren: lane2RenRate,
    lane_fit_3ren: lane3RenRate,
    lane_fit_local: local_win_rate,
    lane_fit_grade: class_score * 20
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
  const avgStRanks = buildAscendingRanks(
    base.map((x) => ({ lane: x.features.lane, value: x.features.avg_st }))
  );
  const expectedActualStByLane = base.map((item) => {
    const lane = item.features.lane;
    const fHoldCount = Math.max(0, toNumber(item.racer?.fHoldCount ?? item.features?.f_hold_count, 0));
    const insideBias = lane >= 1 && lane <= 3 ? 0.01 : lane === 4 ? 0.005 : 0;
    const baseOffset = fHoldCount > 0 ? 0.02 + Math.min(0.02, (fHoldCount - 1) * 0.01) : 0;
    const adjustment = Number((baseOffset + insideBias).toFixed(3));
    const exhibitionSt = Number.isFinite(item.features.exhibition_st) ? item.features.exhibition_st : null;
    const avgSt = Number.isFinite(item.features.avg_st) ? item.features.avg_st : null;
    const expectedActualSt =
      Number.isFinite(exhibitionSt) ? Number((exhibitionSt + adjustment).toFixed(3))
        : Number.isFinite(avgSt) ? Number((avgSt + adjustment).toFixed(3))
          : null;
    return {
      lane,
      fHoldCount,
      adjustment,
      expectedActualSt
    };
  });
  const expectedActualStRanks = buildAscendingRanks(
    expectedActualStByLane.map((row) => ({ lane: row.lane, value: row.expectedActualSt }))
  );
  const lapTimeRanks = buildAscendingRanks(
    base.map((x) => ({ lane: x.features.lane, value: x.features.lap_time }))
  );
  const bestLapTime = Math.min(
    ...base
      .map((x) => x.features.lap_time)
      .filter((v) => Number.isFinite(v)),
    Number.POSITIVE_INFINITY
  );
  const expectedActualStByLaneMap = new Map(expectedActualStByLane.map((row) => [row.lane, row]));
  const byLane = new Map(base.map((item) => [item.features.lane, item]));
  const byCourse = new Map(
    base
      .map((item) => [toNumber(item.features.entry_course, null), item])
      .filter((row) => Number.isInteger(row[0]) && row[0] >= 1)
  );

  return base.map((item) => {
    const f = item.features;
    const leftLane = Number.isFinite(f.lane) ? f.lane - 1 : null;
    const leftItem = Number.isFinite(leftLane) && leftLane >= 1 ? byLane.get(leftLane) : null;
    const leftFeatures = leftItem?.features || {};
    const selfExhibitionTime = Number.isFinite(f.exhibition_time) ? f.exhibition_time : null;
    const leftExhibitionTime = Number.isFinite(leftFeatures?.exhibition_time) ? leftFeatures.exhibition_time : null;
    const selfAvgStRank = avgStRanks.get(f.lane) ?? null;
    const expectedActualStMeta = expectedActualStByLaneMap.get(f.lane) || {};
    const leftAvgStRank = Number.isFinite(leftLane) ? (avgStRanks.get(leftLane) ?? null) : null;
    const displayTimeDeltaVsLeft =
      Number.isFinite(selfExhibitionTime) && Number.isFinite(leftExhibitionTime)
        ? Number((leftExhibitionTime - selfExhibitionTime).toFixed(3))
        : null;
    const avgStRankDeltaVsLeft =
      Number.isFinite(selfAvgStRank) && Number.isFinite(leftAvgStRank)
        ? leftAvgStRank - selfAvgStRank
        : null;
    const slitAlertFlag =
      Number.isFinite(displayTimeDeltaVsLeft) &&
      displayTimeDeltaVsLeft >= 0.1 &&
      Number.isFinite(avgStRankDeltaVsLeft) &&
      avgStRankDeltaVsLeft > 0
        ? 1
        : 0;
    const frontCourse = Number.isFinite(f.entry_course) && f.entry_course > 1 ? f.entry_course - 1 : null;
    const frontItem = Number.isFinite(frontCourse) && frontCourse >= 1
      ? byCourse.get(frontCourse) || leftItem
      : leftItem;
    const frontFeatures = frontItem?.features || {};
    const selfLapTime = Number.isFinite(f.lap_time)
      ? f.lap_time
      : Number.isFinite(f.exhibition_time)
        ? f.exhibition_time
        : null;
    const frontLapTime = Number.isFinite(frontFeatures?.lap_time)
      ? frontFeatures.lap_time
      : Number.isFinite(frontFeatures?.exhibition_time)
        ? frontFeatures.exhibition_time
        : null;
    const lapTimeDeltaVsFront =
      Number.isFinite(selfLapTime) && Number.isFinite(frontLapTime)
        ? Number((frontLapTime - selfLapTime).toFixed(3))
        : null;
    const lapAttackStrength = Number.isFinite(lapTimeDeltaVsFront)
      ? Number(Math.max(0, (lapTimeDeltaVsFront - 0.02) * 100 + toNumber(f.lap_exhibition_score, 0) * 0.8).toFixed(2))
      : 0;
    const lapAttackFlag =
      Number.isFinite(lapTimeDeltaVsFront) &&
      lapTimeDeltaVsFront >= 0.05 &&
      (
        toNumber(f.slit_alert_flag, 0) === 1 ||
        (Number.isFinite(avgStRankDeltaVsLeft) && avgStRankDeltaVsLeft >= 0) ||
        (expectedActualStRanks.get(f.lane) ?? 6) <= ((frontItem && expectedActualStRanks.get(frontItem.features.lane)) ?? 6)
      )
        ? 1
        : 0;
    return {
      ...item,
      features: {
        ...f,
        exhibition_rank: exhibitionRanks.get(f.lane) ?? null,
        st_rank: stRanks.get(f.lane) ?? null,
        avg_st_rank: selfAvgStRank,
        lane_st_rank: selfAvgStRank,
        expected_actual_st_rank: expectedActualStRanks.get(f.lane) ?? null,
        exhibition_gap_from_best:
          Number.isFinite(bestExhibition) && Number.isFinite(f.exhibition_time)
            ? f.exhibition_time - bestExhibition
            : 0,
        motor_gap_from_best: bestMotor - f.motor2_rate,
        local_gap_from_best: bestLocal - f.local_win_rate,
        nation_gap_from_best: bestNation - f.nationwide_win_rate,
        left_neighbor_exists: leftItem ? 1 : 0,
        display_time_delta_vs_left: displayTimeDeltaVsLeft,
        avg_st_rank_delta_vs_left: avgStRankDeltaVsLeft,
        slit_alert_flag: slitAlertFlag,
        front_boat_exists: frontItem ? 1 : 0,
        lap_time_delta_vs_front: lapTimeDeltaVsFront,
        lap_time_rank: lapTimeRanks.get(f.lane) ?? null,
        lap_attack_flag: lapAttackFlag,
        lap_attack_strength: lapAttackStrength,
        lap_time_gap_from_best:
          Number.isFinite(bestLapTime) && Number.isFinite(f.lap_time)
            ? Number((f.lap_time - bestLapTime).toFixed(3))
            : 0,
        f_hold_count: expectedActualStMeta.fHoldCount ?? 0,
        f_hold_bias_applied: (expectedActualStMeta.fHoldCount ?? 0) > 0 ? 1 : 0,
        hidden_f_flag: (expectedActualStMeta.fHoldCount ?? 0) > 0 ? 1 : 0,
        unresolved_f_count: expectedActualStMeta.fHoldCount ?? 0,
        expected_actual_st_adjustment: expectedActualStMeta.adjustment ?? 0,
        expected_actual_st: expectedActualStMeta.expectedActualSt ?? null,
        expected_actual_st_inv:
          Number.isFinite(expectedActualStMeta.expectedActualSt) && expectedActualStMeta.expectedActualSt > 0
            ? Number((1 / expectedActualStMeta.expectedActualSt).toFixed(6))
            : 0,
        f_hold_caution_penalty:
          (expectedActualStMeta.fHoldCount ?? 0) > 0
            ? Number((2 + (expectedActualStMeta.adjustment ?? 0) * 120).toFixed(2))
            : 0,
        start_caution_penalty:
          (expectedActualStMeta.fHoldCount ?? 0) > 0
            ? Number((2 + (expectedActualStMeta.adjustment ?? 0) * 120).toFixed(2))
            : 0,
        motor_true: Number((
          toNumber(f.motor_total_score, 0) * 0.62 +
          toNumber(f.motor2_rate, 0) * 0.24 +
          toNumber(f.motor3_rate, 0) * 0.14
        ).toFixed(2)),
        motor_form: {
          lapTime: Number.isFinite(f.lap_time) ? f.lap_time : null,
          lapExStretch: Number.isFinite(f.lap_exhibition_score) ? f.lap_exhibition_score : null,
          exhibitionTime: Number.isFinite(f.exhibition_time) ? f.exhibition_time : null
        },
        lane_fit_1st: Number.isFinite(f.laneFirstRate) ? f.laneFirstRate : null,
        lane_fit_2ren: Number.isFinite(f.lane2RenRate) ? f.lane2RenRate : null,
        lane_fit_3ren: Number.isFinite(f.lane3RenRate) ? f.lane3RenRate : null,
        lane_fit_local: Number.isFinite(f.local_win_rate) ? f.local_win_rate : null,
        lane_fit_grade: Number.isFinite(f.class_score) ? f.class_score * 20 : null
      }
    };
  });
}
