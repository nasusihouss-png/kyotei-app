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

function normalizeLapMetricForModel(value) {
  const n = toNullableNumber(value);
  if (n === null) return null;
  if (n > 30 && n < 50) {
    const normalized = Number((n - 29.5).toFixed(2));
    return normalized > 0 && normalized < 20 ? normalized : null;
  }
  return n;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function getPredictionFieldMeta(racer, field) {
  const meta = racer?.predictionFieldMeta?.[field];
  return meta && typeof meta === "object"
    ? meta
    : { value: null, source: null, confidence: 0, is_usable: false, reason: "missing" };
}

function getUsablePredictionValue(racer, field, fallbackValue = null) {
  const meta = getPredictionFieldMeta(racer, field);
  return meta?.is_usable ? toNullableNumber(meta.value) : fallbackValue;
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

function computeStartStabilityScore({ avgSt, exhibitionSt, avgStRank, fHoldCount }) {
  const avgStComponent =
    Number.isFinite(avgSt) && avgSt > 0
      ? clamp(0, 100, (0.22 - avgSt) / 0.08 * 100)
      : null;
  const exhibitionStComponent =
    Number.isFinite(exhibitionSt) && exhibitionSt > 0
      ? clamp(0, 100, (0.22 - exhibitionSt) / 0.08 * 100)
      : null;
  const avgStRankComponent =
    Number.isFinite(avgStRank)
      ? clamp(0, 100, (7 - avgStRank) / 6 * 100)
      : 50;
  const fPenalty = Number.isFinite(fHoldCount) ? Math.min(24, fHoldCount * 10) : 0;

  return Number(
    clamp(
      0,
      100,
      (Number.isFinite(avgStComponent) ? avgStComponent : 50) * 0.52 +
        (Number.isFinite(exhibitionStComponent) ? exhibitionStComponent : 50) * 0.24 +
        avgStRankComponent * 0.14 -
        fPenalty
    ).toFixed(2)
  );
}

export function buildFeatures(racer) {
  const lane = toNumber(racer?.lane, 0);
  const class_score = CLASS_SCORE[racer?.class] ?? 0;
  const nationwide_win_rate = toNumber(racer?.nationwideWinRate, 0);
  const local_win_rate = toNumber(racer?.localWinRate, 0);
  const boat2_rate = toNumber(racer?.boat2Rate, 0);
  const weight = toNumber(racer?.weight, 0);
  const avg_st_raw = racer?.avgSt;
  const avg_st = Number.isFinite(Number(avg_st_raw)) ? Number(avg_st_raw) : null;
  const prediction_field_meta = {
    lapTime: getPredictionFieldMeta(racer, "lapTime"),
    exhibitionST: getPredictionFieldMeta(racer, "exhibitionST"),
    exhibitionTime: getPredictionFieldMeta(racer, "exhibitionTime"),
    lapExStretch: getPredictionFieldMeta(racer, "lapExStretch"),
    motor2ren: getPredictionFieldMeta(racer, "motor2ren"),
    motor3ren: getPredictionFieldMeta(racer, "motor3ren"),
    lane1stScore: getPredictionFieldMeta(racer, "lane1stScore"),
    lane2renScore: getPredictionFieldMeta(racer, "lane2renScore"),
    lane3renScore: getPredictionFieldMeta(racer, "lane3renScore"),
    lane1stAvg: getPredictionFieldMeta(racer, "lane1stAvg"),
    lane2renAvg: getPredictionFieldMeta(racer, "lane2renAvg"),
    lane3renAvg: getPredictionFieldMeta(racer, "lane3renAvg"),
    fCount: getPredictionFieldMeta(racer, "fCount")
  };
  const exhibition_time = getUsablePredictionValue(racer, "exhibitionTime", null);
  const exhibition_st = getUsablePredictionValue(racer, "exhibitionST", null);
  const lap_time = normalizeLapMetricForModel(
    getUsablePredictionValue(racer, "lapTime", racer?.lapRaw ?? racer?.kyoteiBiyoriLapTimeRaw ?? null)
  );
  const lap_exhibition_score = getUsablePredictionValue(racer, "lapExStretch", null);
  const lap_source = racer?.lapSource ?? racer?.kyoteiBiyoriLapSource ?? prediction_field_meta.lapTime?.source ?? null;
  const lap_raw = toNullableNumber(racer?.lapRaw ?? racer?.kyoteiBiyoriLapTimeRaw ?? racer?.lapTimeRaw);
  const entry_course_raw = racer?.entryCourse;
  const entry_course =
    Number.isFinite(Number(entry_course_raw)) ? Number(entry_course_raw) : null;
  const actual_lane = entry_course || lane || null;
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
  const laneFirstRate = getUsablePredictionValue(racer, "lane1stScore", getUsablePredictionValue(racer, "lane1stAvg", null));
  const lane2RenRate = getUsablePredictionValue(racer, "lane2renScore", getUsablePredictionValue(racer, "lane2renAvg", null));
  const lane3RenRate = getUsablePredictionValue(racer, "lane3renScore", getUsablePredictionValue(racer, "lane3renAvg", null));
  const lapExStretch = getUsablePredictionValue(racer, "lapExStretch", null);
  const motor2_rate = prediction_field_meta.motor2ren?.is_usable
    ? toNullableNumber(prediction_field_meta.motor2ren.value)
    : null;
  const motor3_rate = prediction_field_meta.motor3ren?.is_usable
    ? toNullableNumber(prediction_field_meta.motor3ren.value)
    : null;
  const f_hold_count = prediction_field_meta.fCount?.is_usable
    ? Math.max(0, toNumber(prediction_field_meta.fCount.value, 0))
    : null;
  const avgStRankValue = toNullableNumber(
    racer?.avgStRank ?? racer?.avg_st_rank ?? racer?.["laneStRank"] ?? racer?.lane_st_rank
  );
  const local_minus_nation = local_win_rate - nationwide_win_rate;
  const motor_boat_avg = ((Number.isFinite(motor2_rate) ? motor2_rate : 0) + boat2_rate) / 2;
  const st_inv = avg_st && avg_st > 0 ? 1 / avg_st : 0;
  const is_inner = actual_lane >= 1 && actual_lane <= 3 ? 1 : 0;
  const is_outer = actual_lane >= 5 && actual_lane <= 6 ? 1 : 0;
  const boat3_rate = toNullableNumber(racer?.boat3Rate);
  const player_strength_blended = toNullableNumber(racer?.playerStrengthBlended);
  const player_strength_component = Number.isFinite(player_strength_blended)
    ? clamp(0, 100, player_strength_blended)
    : clamp(0, 100, nationwide_win_rate * 10 + class_score * 6);
  const national_component = clamp(0, 100, nationwide_win_rate * 10);
  const local_component = clamp(0, 100, local_win_rate * 10);
  const class_component = clamp(0, 100, class_score * 20);
  const motor2_component = Number.isFinite(motor2_rate) ? clamp(0, 100, motor2_rate) : null;
  const motor3_component = Number.isFinite(motor3_rate) ? clamp(0, 100, motor3_rate) : null;
  const boat2_component = Number.isFinite(boat2_rate) ? clamp(0, 100, boat2_rate) : null;
  const boat3_component = Number.isFinite(boat3_rate) ? clamp(0, 100, boat3_rate) : null;
  const base_strength_score = Number(
    clamp(
      0,
      100,
      player_strength_component * 0.34 +
        national_component * 0.28 +
        local_component * 0.14 +
        class_component * 0.1 +
        (Number.isFinite(motor2_component) ? motor2_component : 50) * 0.08 +
        (Number.isFinite(boat2_component) ? boat2_component : 50) * 0.06
    ).toFixed(2)
  );
  const local_fit_score = Number(
    clamp(
      0,
      100,
      local_component * 0.58 +
        clamp(0, 100, 50 + local_minus_nation * 10) * 0.22 +
        (Number.isFinite(course1_win_rate) ? clamp(0, 100, course1_win_rate) : 50) * 0.12 +
        (Number.isFinite(course1_2rate) ? clamp(0, 100, course1_2rate) : 50) * 0.08
    ).toFixed(2)
  );
  const motor_strength_score = Number(
    clamp(
      0,
      100,
      (Number.isFinite(motor2_component) ? motor2_component : 50) * 0.42 +
        (Number.isFinite(motor3_component) ? motor3_component : 50) * 0.22 +
        (Number.isFinite(boat2_component) ? boat2_component : 50) * 0.22 +
        (Number.isFinite(boat3_component) ? boat3_component : 50) * 0.08 +
      (Number.isFinite(lap_time) ? clamp(0, 100, (6.9 - lap_time) / 0.25 * 100) : 50) * 0.06
    ).toFixed(2)
  );
  const start_stability_score = computeStartStabilityScore({
    avgSt: avg_st,
    exhibitionSt: exhibition_st,
    avgStRank: avgStRankValue,
    fHoldCount: f_hold_count
  });

  return {
    lane,
    original_lane: lane || null,
    boat_number: lane || null,
    actual_lane,
    class_score,
    nationwide_win_rate,
    local_win_rate,
    motor2_rate,
    motor3_rate,
    boat2_rate,
    boat3_rate,
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
    turning_ability: toNullableNumber(racer?.kyoteiBiyoriMawariashi ?? racer?.mawariashi),
    straight_line_power: toNullableNumber(racer?.kyoteiBiyoriNobiashi ?? racer?.nobiashi),
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
    base_strength_score,
    local_fit_score,
    motor_strength_score,
    start_stability_score,
    prediction_field_meta,
    lane_avg_st: avg_st,
    lane_st_rank: avgStRankValue,
    hidden_f_flag: Number.isFinite(f_hold_count) && f_hold_count > 0 ? 1 : 0,
    unresolved_f_count: Number.isFinite(f_hold_count) ? f_hold_count : null,
    start_caution_penalty: Number.isFinite(f_hold_count) && f_hold_count > 0 ? Number((0.04 + Math.min(0.08, f_hold_count * 0.02)).toFixed(3)) : 0,
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
    f_hold_count,
    f_hold_bias_applied: 0,
    expected_actual_st_adjustment: 0,
    expected_actual_st: null,
    expected_actual_st_inv: 0,
    f_hold_caution_penalty: 0,
    kyoteibiyori_fetched: toNumber(racer?.kyoteiBiyoriFetched, 0),
    lap_time_gap_from_best: 0,
    lap_rank: null,
    lap_gap_from_best: 0,
    lap_source,
    lap_raw,
    lap_stretch_foot: lap_exhibition_score,
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
    lane_fit_grade: class_score * 20,
    lane_reassignment_applied: Number.isInteger(actual_lane) && actual_lane !== lane ? 1 : 0
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
    base.map((x) => ({ lane: toNumber(x.features.actual_lane, x.features.lane), value: x.features.exhibition_time }))
  );
  const stRanks = buildAscendingRanks(
    base.map((x) => ({ lane: toNumber(x.features.actual_lane, x.features.lane), value: x.features.exhibition_st }))
  );
  const avgStRanks = buildAscendingRanks(
    base.map((x) => ({ lane: toNumber(x.features.actual_lane, x.features.lane), value: x.features.avg_st }))
  );
  const expectedActualStByLane = base.map((item) => {
    const lane = toNumber(item.features.actual_lane, item.features.lane);
    const fHoldMeta = item.features?.prediction_field_meta?.fCount || {};
    const fHoldCount = fHoldMeta?.is_usable
      ? Math.max(0, toNumber(item.features?.f_hold_count ?? item.racer?.fHoldCount, 0))
      : null;
    const insideBias = lane >= 1 && lane <= 3 ? 0.01 : lane === 4 ? 0.005 : 0;
    const baseOffset = Number.isFinite(fHoldCount) && fHoldCount > 0 ? 0.02 + Math.min(0.02, (fHoldCount - 1) * 0.01) : 0;
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
    base.map((x) => ({ lane: toNumber(x.features.actual_lane, x.features.lane), value: x.features.lap_time }))
  );
  const bestLapTime = Math.min(
    ...base
      .map((x) => x.features.lap_time)
      .filter((v) => Number.isFinite(v)),
    Number.POSITIVE_INFINITY
  );
  const expectedActualStByLaneMap = new Map(expectedActualStByLane.map((row) => [row.lane, row]));
  const byLane = new Map(
    base.map((item) => [toNumber(item.features.actual_lane, item.features.lane), item])
  );
  const byCourse = new Map(
    base
      .map((item) => [toNumber(item.features.entry_course, null), item])
      .filter((row) => Number.isInteger(row[0]) && row[0] >= 1)
  );
  const laneFitSourceByOriginalLane = new Map(
    base.map((item) => [
      toNumber(item.features.original_lane, null),
      {
        laneFirstRate: item.features.laneFirstRate,
        lane2RenRate: item.features.lane2RenRate,
        lane3RenRate: item.features.lane3RenRate,
        course1_win_rate: item.features.course1_win_rate,
        course1_2rate: item.features.course1_2rate,
        course2_2rate: item.features.course2_2rate,
        course3_3rate: item.features.course3_3rate,
        course4_3rate: item.features.course4_3rate
      }
    ])
  );

  return base.map((item) => {
    const f = item.features;
    const actualLane = toNumber(f.actual_lane, f.lane);
    const reassignedLaneContext = laneFitSourceByOriginalLane.get(actualLane) || {};
    const leftLane = Number.isFinite(actualLane) ? actualLane - 1 : null;
    const leftItem = Number.isFinite(leftLane) && leftLane >= 1 ? byLane.get(leftLane) : null;
    const leftFeatures = leftItem?.features || {};
    const selfExhibitionTime = Number.isFinite(f.exhibition_time) ? f.exhibition_time : null;
    const leftExhibitionTime = Number.isFinite(leftFeatures?.exhibition_time) ? leftFeatures.exhibition_time : null;
    const selfAvgStRank = avgStRanks.get(actualLane) ?? null;
    const expectedActualStMeta = expectedActualStByLaneMap.get(actualLane) || {};
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
    const frontCourse = Number.isFinite(actualLane) && actualLane > 1 ? actualLane - 1 : null;
    const frontItem = Number.isFinite(frontCourse) && frontCourse >= 1
      ? byCourse.get(frontCourse) || leftItem
      : leftItem;
    const frontFeatures = frontItem?.features || {};
    const selfLapTime = Number.isFinite(f.lap_time) ? f.lap_time : null;
    const frontLapTime = Number.isFinite(frontFeatures?.lap_time) ? frontFeatures.lap_time : null;
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
        (expectedActualStRanks.get(actualLane) ?? 6) <= ((frontItem && expectedActualStRanks.get(toNumber(frontItem.features.actual_lane, frontItem.features.lane))) ?? 6)
      )
        ? 1
        : 0;
    return {
      ...item,
      features: {
        ...f,
        actual_lane: actualLane,
        exhibition_rank: exhibitionRanks.get(actualLane) ?? null,
        st_rank: stRanks.get(actualLane) ?? null,
        avg_st_rank: selfAvgStRank,
        lane_st_rank: selfAvgStRank,
        start_stability_score: computeStartStabilityScore({
          avgSt: f.avg_st,
          exhibitionSt: f.exhibition_st,
          avgStRank: selfAvgStRank,
          fHoldCount: expectedActualStMeta.fHoldCount ?? f.f_hold_count
        }),
        expected_actual_st_rank: expectedActualStRanks.get(actualLane) ?? null,
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
        lap_time_rank: lapTimeRanks.get(actualLane) ?? null,
        lap_rank: lapTimeRanks.get(actualLane) ?? null,
        lap_attack_flag: lapAttackFlag,
        lap_attack_strength: lapAttackStrength,
        lap_time_gap_from_best:
          Number.isFinite(bestLapTime) && Number.isFinite(f.lap_time)
            ? Number((f.lap_time - bestLapTime).toFixed(3))
            : 0,
        lap_gap_from_best:
          Number.isFinite(bestLapTime) && Number.isFinite(f.lap_time)
            ? Number((f.lap_time - bestLapTime).toFixed(3))
            : 0,
        lap_stretch_foot: Number.isFinite(f.lap_exhibition_score) ? f.lap_exhibition_score : null,
        f_hold_count: Number.isFinite(expectedActualStMeta.fHoldCount) ? expectedActualStMeta.fHoldCount : null,
        f_hold_bias_applied: Number.isFinite(expectedActualStMeta.fHoldCount) && expectedActualStMeta.fHoldCount > 0 ? 1 : 0,
        hidden_f_flag: Number.isFinite(expectedActualStMeta.fHoldCount) && expectedActualStMeta.fHoldCount > 0 ? 1 : 0,
        unresolved_f_count: Number.isFinite(expectedActualStMeta.fHoldCount) ? expectedActualStMeta.fHoldCount : null,
        expected_actual_st_adjustment: expectedActualStMeta.adjustment ?? 0,
        expected_actual_st: expectedActualStMeta.expectedActualSt ?? null,
        expected_actual_st_inv:
          Number.isFinite(expectedActualStMeta.expectedActualSt) && expectedActualStMeta.expectedActualSt > 0
            ? Number((1 / expectedActualStMeta.expectedActualSt).toFixed(6))
            : 0,
        f_hold_caution_penalty:
          Number.isFinite(expectedActualStMeta.fHoldCount) && expectedActualStMeta.fHoldCount > 0
            ? Number((2 + (expectedActualStMeta.adjustment ?? 0) * 120).toFixed(2))
            : 0,
        start_caution_penalty:
          Number.isFinite(expectedActualStMeta.fHoldCount) && expectedActualStMeta.fHoldCount > 0
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
        laneFirstRate: Number.isFinite(reassignedLaneContext.laneFirstRate) ? reassignedLaneContext.laneFirstRate : f.laneFirstRate,
        lane2RenRate: Number.isFinite(reassignedLaneContext.lane2RenRate) ? reassignedLaneContext.lane2RenRate : f.lane2RenRate,
        lane3RenRate: Number.isFinite(reassignedLaneContext.lane3RenRate) ? reassignedLaneContext.lane3RenRate : f.lane3RenRate,
        course1_win_rate: Number.isFinite(reassignedLaneContext.course1_win_rate) ? reassignedLaneContext.course1_win_rate : f.course1_win_rate,
        course1_2rate: Number.isFinite(reassignedLaneContext.course1_2rate) ? reassignedLaneContext.course1_2rate : f.course1_2rate,
        course2_2rate: Number.isFinite(reassignedLaneContext.course2_2rate) ? reassignedLaneContext.course2_2rate : f.course2_2rate,
        course3_3rate: Number.isFinite(reassignedLaneContext.course3_3rate) ? reassignedLaneContext.course3_3rate : f.course3_3rate,
        course4_3rate: Number.isFinite(reassignedLaneContext.course4_3rate) ? reassignedLaneContext.course4_3rate : f.course4_3rate,
        lane_fit_1st: Number.isFinite(reassignedLaneContext.laneFirstRate) ? reassignedLaneContext.laneFirstRate : (Number.isFinite(f.laneFirstRate) ? f.laneFirstRate : null),
        lane_fit_2ren: Number.isFinite(reassignedLaneContext.lane2RenRate) ? reassignedLaneContext.lane2RenRate : (Number.isFinite(f.lane2RenRate) ? f.lane2RenRate : null),
        lane_fit_3ren: Number.isFinite(reassignedLaneContext.lane3RenRate) ? reassignedLaneContext.lane3RenRate : (Number.isFinite(f.lane3RenRate) ? f.lane3RenRate : null),
        lane_fit_local: Number.isFinite(f.local_win_rate) ? f.local_win_rate : null,
        lane_fit_grade: Number.isFinite(f.class_score) ? f.class_score * 20 : null,
        lane_assignment_debug: {
          original_lane: f.original_lane ?? null,
          actual_lane: actualLane,
          course_change_occurred: Number.isInteger(actualLane) && Number.isInteger(f.original_lane) ? actualLane !== f.original_lane : false,
          lane_scores_reassigned: !!(
            Number.isFinite(reassignedLaneContext.laneFirstRate) ||
            Number.isFinite(reassignedLaneContext.lane2RenRate) ||
            Number.isFinite(reassignedLaneContext.lane3RenRate)
          )
        }
      }
    };
  });
}
