import {
  getVenueEscapeFailPressure,
  getVenueLaneBiasScore,
  getVenueScenarioContext,
  getVenueStyleMatchScore
} from "./src/services/venue-scenario-bias.js";

function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 4) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(Number(value).toFixed(digits));
}

const STYLE_LABELS = {
  nige: "イン逃げ型",
  sashi: "差し型",
  makuri: "まくり型",
  makurisashi: "まくり差し型",
  tenkai_machi: "展開待ち型",
  outside_entry: "外連入型",
  start_attack: "スタート勝負型",
  stable_hold: "安定残し型"
};

function weightedAverage(values) {
  const present = (Array.isArray(values) ? values : []).filter(
    (row) => Number.isFinite(Number(row?.value)) && Number.isFinite(Number(row?.weight)) && Number(row.weight) > 0
  );
  if (!present.length) return null;
  const total = present.reduce((sum, row) => sum + Number(row.weight), 0);
  return total > 0 ? present.reduce((sum, row) => sum + Number(row.value) * Number(row.weight), 0) / total : null;
}

function coverageValue(coverage = {}, fieldName, fallback = null) {
  const meta = coverage?.[fieldName];
  if (meta?.status === "ok" || meta?.status === "fallback") {
    if (Number.isFinite(Number(meta?.normalized))) return Number(meta.normalized);
    if (Number.isFinite(Number(meta?.value))) return Number(meta.value);
  }
  return Number.isFinite(Number(fallback)) ? Number(fallback) : null;
}

function normalize(value, min, max) {
  if (!Number.isFinite(Number(value))) return null;
  return clamp(0, 1, (Number(value) - min) / Math.max(1e-9, max - min));
}

function invertNormalize(value, min, max) {
  const normalized = normalize(value, min, max);
  return normalized === null ? null : 1 - normalized;
}

function normalizeMap(inputMap) {
  const entries = Object.entries(inputMap || {}).map(([key, value]) => [key, Math.max(0.000001, Number(value) || 0.000001)]);
  const total = entries.reduce((sum, [, value]) => sum + value, 0);
  if (!(total > 0)) return Object.fromEntries(entries.map(([key]) => [key, 0]));
  let running = 0;
  return Object.fromEntries(
    entries.map(([key, value], index) => {
      if (index === entries.length - 1) return [key, round(Math.max(0, 1 - running), 4)];
      const normalized = round(value / total, 4);
      running += normalized;
      return [key, normalized];
    })
  );
}

function entropy(values) {
  const list = Object.values(values || {}).filter((value) => Number.isFinite(Number(value)) && Number(value) > 0);
  if (!list.length) return 0;
  return -list.reduce((sum, value) => sum + Number(value) * Math.log(Number(value)), 0);
}

function courseRateByLane(features = {}, lane) {
  if (lane === 1) return toNum(features?.course1_win_rate ?? features?.course1_2rate, null);
  if (lane === 2) return toNum(features?.course2_2rate, null);
  if (lane === 3) return toNum(features?.course3_3rate, null);
  if (lane === 4) return toNum(features?.course4_3rate, null);
  const courseFit = toNum(features?.course_fit_score, null);
  return courseFit === null ? null : clamp(0, 100, 45 + courseFit * 7);
}

function buildStyleScores(profile) {
  return {
    nige: weightedAverage([
      { value: profile.courseHeadRate, weight: 0.42 },
      { value: profile.lane1stRate, weight: 0.24 },
      { value: profile.stabilityRate, weight: 0.16 },
      { value: profile.recentPerformanceIndex, weight: 0.18 }
    ]),
    sashi: weightedAverage([
      { value: profile.sashiRate, weight: 0.34 },
      { value: profile.course2Rate, weight: 0.28 },
      { value: profile.lane2RenRate, weight: 0.18 },
      { value: profile.supportRate, weight: 0.2 }
    ]),
    makuri: weightedAverage([
      { value: profile.makuriRate, weight: 0.32 },
      { value: profile.attackRate, weight: 0.26 },
      { value: profile.lane1stRate, weight: 0.16 },
      { value: profile.recentPerformanceIndex, weight: 0.26 }
    ]),
    makurisashi: weightedAverage([
      { value: profile.makuriSashiRate, weight: 0.34 },
      { value: profile.attackRate, weight: 0.22 },
      { value: profile.supportRate, weight: 0.2 },
      { value: profile.lane3RenRate, weight: 0.24 }
    ]),
    tenkai_machi: weightedAverage([
      { value: profile.supportRate, weight: 0.32 },
      { value: profile.stability, weight: 0.28 },
      { value: profile.course3Rate, weight: 0.2 },
      { value: profile.recentPerformanceIndex, weight: 0.2 }
    ]),
    outside_entry: weightedAverage([
      { value: profile.zentsukeTendency, weight: 0.36 },
      { value: profile.breakoutRate, weight: 0.22 },
      { value: profile.attackRate, weight: 0.22 },
      { value: profile.course3Rate, weight: 0.2 }
    ]),
    start_attack: weightedAverage([
      { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.42 },
      { value: profile.attackRate, weight: 0.26 },
      { value: profile.breakoutRate, weight: 0.16 },
      { value: profile.recentPerformanceIndex, weight: 0.16 }
    ]),
    stable_hold: weightedAverage([
      { value: profile.stability, weight: 0.38 },
      { value: profile.supportRate, weight: 0.24 },
      { value: profile.lane3RenRate, weight: 0.18 },
      { value: profile.recentPerformanceIndex, weight: 0.2 }
    ])
  };
}

function determinePrimaryStyle(styleScores = {}) {
  const styleEntries = Object.entries(styleScores)
    .map(([key, value]) => ({ key, value: Number(value) || 0 }))
    .sort((a, b) => b.value - a.value);
  const winner = styleEntries[0]?.key || "tenkai_machi";
  const labels = {
    nige: "イン逃げ型",
    sashi: "差し型",
    makuri: "まくり型",
    makurisashi: "まくり差し型",
    tenkai_machi: "展開待ち型",
    outside_entry: "外連入型",
    start_attack: "スタート勝負型",
    stable_hold: "安定残し型"
  };
  return {
    code: winner,
    label: labels[winner] || "展開待ち型"
  };
}

function determinePrimaryStyleEnhanced(styleScores = {}) {
  const styleEntries = Object.entries(styleScores)
    .map(([key, value]) => ({ key, value: Number(value) || 0 }))
    .sort((a, b) => b.value - a.value);
  const winner = styleEntries[0]?.key || "tenkai_machi";
  return {
    code: winner,
    label: STYLE_LABELS[winner] || STYLE_LABELS.tenkai_machi
  };
}

function buildStyleReasons(profile = {}, styleCode = "") {
  const reasons = [];
  const pushReason = (label, value, formatter = (input) => input) => {
    if (value === null || value === undefined || value === "") return;
    reasons.push(`${label}:${formatter(value)}`);
  };
  const toPct = (value) => `${round(value, 1)}%`;
  const toMetric = (value) => String(round(value, 2));

  pushReason("全国勝率", profile.nationwideWinRate, toMetric);
  pushReason("当地勝率", profile.localWinRate, toMetric);
  pushReason("平均ST", profile.avgSt, toMetric);
  if (Number.isFinite(Number(profile.fCount)) || Number.isFinite(Number(profile.lCount))) {
    reasons.push(`F/L:${Number(profile.fCount || 0)}/${Number(profile.lCount || 0)}`);
  }

  if (styleCode === "nige") {
    pushReason("イン実績", profile.courseHeadRate, toPct);
    pushReason("1着率", profile.lane1stRate, toPct);
  } else if (styleCode === "sashi") {
    pushReason("差し率", profile.sashiRate, toPct);
    pushReason("2連率", profile.lane2RenRate, toPct);
    pushReason("得意コース", profile.course2Rate, toPct);
  } else if (styleCode === "makuri") {
    pushReason("まくり率", profile.makuriRate, toPct);
    pushReason("攻撃指数", profile.attackRate, toPct);
    pushReason("最近成績", profile.recentPerformanceIndex, toPct);
  } else if (styleCode === "makurisashi") {
    pushReason("まくり差し率", profile.makuriSashiRate, toPct);
    pushReason("差し支援", profile.supportRate, toPct);
    pushReason("3連率", profile.lane3RenRate, toPct);
  } else if (styleCode === "outside_entry") {
    pushReason("前づけ傾向", profile.zentsukeTendency, toPct);
    pushReason("外進入再現", profile.outsideEntryRepro, toPct);
    pushReason("突破率", profile.breakoutRate, toPct);
  } else if (styleCode === "start_attack") {
    pushReason("スタート攻撃", profile.attackRate, toPct);
    pushReason("突破率", profile.breakoutRate, toPct);
    pushReason("最近成績", profile.recentPerformanceIndex, toPct);
  } else if (styleCode === "stable_hold") {
    pushReason("安定指数", profile.stability, toPct);
    pushReason("残し率", profile.lane3RenRate, toPct);
    pushReason("最近成績", profile.recentPerformanceIndex, toPct);
  } else {
    pushReason("展開待ち適性", profile.supportRate, toPct);
    pushReason("得意コース", profile.course3Rate ?? profile.course2Rate, toPct);
    pushReason("最近成績", profile.recentPerformanceIndex, toPct);
  }

  if (reasons.length < 6) {
    pushReason("決まり手再現", profile.kimariteHistoryIndex, toPct);
    pushReason("得意技", profile.styleScores?.[styleCode], toPct);
  }
  return reasons.slice(0, 6);
}

function buildLaneProfile(row = {}) {
  const racer = row?.racer || {};
  const features = row?.features || {};
  const coverage = features?.coverage_report && typeof features.coverage_report === "object" ? features.coverage_report : {};
  const lane = Number(racer?.lane);
  const avgSt = toNum(features?.avg_st ?? racer?.avgSt, null);
  const motor2 = toNum(features?.motor2_rate ?? racer?.motor2Rate, null);
  const boat2 = toNum(features?.boat2_rate ?? racer?.boat2Rate, null);
  const motor3 = coverageValue(coverage, "motor_3ren", toNum(features?.motor3_rate ?? racer?.motor3Rate, null));
  const motorTotal = toNum(features?.motor_total_score, null);
  const courseFit = toNum(features?.course_fit_score, null);
  const entryAdvantage = toNum(features?.entry_advantage_score, null);
  const courseRate = courseRateByLane(features, lane);
  const lane1stRate = coverageValue(coverage, "lane_1st_rate", null);
  const lane2RenRate = coverageValue(coverage, "lane_2ren_rate", null);
  const lane3RenRate = coverageValue(coverage, "lane_3ren_rate", null);
  const exhibitionSt = coverageValue(coverage, "exhibition_st", toNum(racer?.exhibitionSt, null));
  const exhibitionTime = coverageValue(coverage, "exhibition_time", toNum(racer?.exhibitionTime, null));
  const lapTime = coverageValue(coverage, "lapTime", toNum(racer?.lapTime ?? racer?.kyoteiBiyoriLapTime, null));
  const lapRank = toNum(features?.lap_rank ?? features?.lap_time_rank ?? racer?.lapRank, null);
  const lapGapFromBest = toNum(features?.lap_gap_from_best ?? features?.lap_time_gap_from_best ?? racer?.lapGapFromBest, null);
  const lapStretchFoot = toNum(features?.lap_stretch_foot ?? features?.lap_exhibition_score ?? racer?.lapExStretch, null);
  const recentPerformanceIndex = weightedAverage([
    { value: normalize(features?.nationwide_win_rate ?? racer?.nationwideWinRate, 4, 8.5) === null ? null : normalize(features?.nationwide_win_rate ?? racer?.nationwideWinRate, 4, 8.5) * 100, weight: 0.28 },
    { value: normalize(features?.local_win_rate ?? racer?.localWinRate, 4, 8.5) === null ? null : normalize(features?.local_win_rate ?? racer?.localWinRate, 4, 8.5) * 100, weight: 0.24 },
    { value: normalize(motorTotal, 0, 18) === null ? null : normalize(motorTotal, 0, 18) * 100, weight: 0.18 },
    { value: normalize(motor3, 25, 75) === null ? null : normalize(motor3, 25, 75) * 100, weight: 0.15 },
    { value: invertNormalize(avgSt, 0.11, 0.24) === null ? null : invertNormalize(avgSt, 0.11, 0.24) * 100, weight: 0.15 }
  ]);
  const stability = weightedAverage([
    { value: invertNormalize(avgSt, 0.11, 0.24) === null ? null : invertNormalize(avgSt, 0.11, 0.24) * 100, weight: 0.36 },
    { value: normalize(courseFit, -2, 8) === null ? null : normalize(courseFit, -2, 8) * 100, weight: 0.22 },
    { value: normalize(motorTotal, 0, 18) === null ? null : normalize(motorTotal, 0, 18) * 100, weight: 0.2 },
    { value: normalize(boat2, 20, 60) === null ? null : normalize(boat2, 20, 60) * 100, weight: 0.22 }
  ]);
  const attackRate = weightedAverage([
    { value: normalize(entryAdvantage, 0, 14) === null ? null : normalize(entryAdvantage, 0, 14) * 100, weight: 0.34 },
    { value: normalize(motorTotal, 0, 18) === null ? null : normalize(motorTotal, 0, 18) * 100, weight: 0.22 },
    { value: invertNormalize(avgSt, 0.11, 0.24) === null ? null : invertNormalize(avgSt, 0.11, 0.24) * 100, weight: 0.2 },
    { value: courseRate, weight: 0.14 },
    { value: recentPerformanceIndex, weight: 0.1 }
  ]);
  const supportRate = weightedAverage([
    { value: lane2RenRate ?? courseRate, weight: 0.34 },
    { value: normalize(motor2, 20, 60) === null ? null : normalize(motor2, 20, 60) * 100, weight: 0.22 },
    { value: invertNormalize(avgSt, 0.11, 0.24) === null ? null : invertNormalize(avgSt, 0.11, 0.24) * 100, weight: 0.14 },
    { value: stability, weight: 0.12 },
    { value: recentPerformanceIndex, weight: 0.18 }
  ]);

  const profile = {
    lane,
    nationwideWinRate: toNum(features?.nationwide_win_rate ?? racer?.nationwideWinRate, null),
    localWinRate: toNum(features?.local_win_rate ?? racer?.localWinRate, null),
    avgSt,
    lateRate: toNum(features?.late_start_rate ?? racer?.lateStartRate, 0),
    fCount: toNum(features?.f_hold_count ?? racer?.fHoldCount, 0),
    lCount: toNum(features?.l_hold_count ?? racer?.lHoldCount, 0),
    motor2,
    motor3,
    boat2,
    motorTotal,
    courseHeadRate: lane === 1 ? courseRate : null,
    course2Rate: lane === 2 ? courseRate : lane2RenRate,
    course3Rate: lane >= 3 ? courseRate : lane3RenRate,
    lane1stRate,
    lane2RenRate,
    lane3RenRate,
    courseFit,
    entryAdvantage,
    stability,
    attackRate,
    supportRate,
    recentPerformanceIndex,
    exhibitionSt,
    exhibitionTime,
    lapTime,
    lapRank,
    lapGapFromBest,
    lapStretchFoot,
    stabilityRate: toNum(coverage?.stability_rate?.value, null),
    breakoutRate: toNum(coverage?.breakout_rate?.value, null),
    sashiRate: toNum(coverage?.sashi_rate?.value, null),
    makuriRate: toNum(coverage?.makuri_rate?.value, null),
    makuriSashiRate: toNum(coverage?.makurisashi_rate?.value, null),
    zentsukeTendency: toNum(coverage?.zentsuke_tendency?.value, null),
    kimariteHistoryIndex: weightedAverage([
      { value: toNum(coverage?.sashi_rate?.value, null), weight: 0.24 },
      { value: toNum(coverage?.makuri_rate?.value, null), weight: 0.24 },
      { value: toNum(coverage?.makurisashi_rate?.value, null), weight: 0.22 },
      { value: toNum(coverage?.breakout_rate?.value, null), weight: 0.15 },
      { value: courseRate, weight: 0.15 }
    ]),
    outsideEntryRepro: weightedAverage([
      { value: toNum(coverage?.zentsuke_tendency?.value, null), weight: 0.48 },
      { value: toNum(coverage?.breakout_rate?.value, null), weight: 0.26 },
      { value: courseRate, weight: 0.26 }
    ])
  };
  profile.styleScores = buildStyleScores(profile);
  profile.style = determinePrimaryStyleEnhanced(profile.styleScores);
  profile.styleScore = round(profile.styleScores?.[profile.style?.code] || 0, 1);
  profile.styleReasons = buildStyleReasons(profile, profile.style?.code);
  return profile;
}

function buildScenarioReproScore(profile, venueContext = {}, raceContext = {}) {
  const venueBias = getVenueLaneBiasScore(venueContext, profile.lane);
  const venueKimariteFit = getVenueStyleMatchScore(venueContext, profile.lane, profile.style?.code);
  const styleFit = weightedAverage([
    { value: profile.styleScores?.nige, weight: profile.lane === 1 ? 0.38 : 0.08 },
    { value: profile.styleScores?.sashi, weight: profile.lane === 2 ? 0.28 : 0.12 },
    { value: profile.styleScores?.makuri, weight: profile.lane === 3 ? 0.24 : 0.14 },
    { value: profile.styleScores?.makurisashi, weight: profile.lane === 4 ? 0.22 : 0.14 },
    { value: profile.styleScores?.outside_entry, weight: profile.lane >= 5 ? 0.24 : 0.08 },
    { value: profile.styleScores?.tenkai_machi, weight: 0.12 }
  ]);
  const escapeFailConditionResist = weightedAverage([
    { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.36 },
    { value: 100 - clamp(0, 100, ((profile.fCount || 0) * 28) + ((profile.lCount || 0) * 12)), weight: 0.18 },
    { value: profile.stability, weight: 0.24 },
    { value: profile.supportRate, weight: 0.22 }
  ]);
  const lane56EntryRepro = profile.lane >= 5
    ? weightedAverage([
        { value: profile.outsideEntryRepro, weight: 0.52 },
        { value: venueContext?.lane56_renyuu_intrusion_rate, weight: 0.3 },
        { value: raceContext?.outsideHeadPressure, weight: 0.18 }
      ])
    : weightedAverage([
        { value: profile.outsideEntryRepro, weight: 0.6 },
        { value: venueContext?.lane56_renyuu_intrusion_rate, weight: 0.4 }
      ]);
  const escapeFailConditionScore = profile.lane === 1
    ? clamp(0, 100, 100 - toNum(venueContext?.escape_fail_pattern?.total_risk, 28))
    : weightedAverage([
        { value: getVenueEscapeFailPressure(venueContext, profile.lane), weight: 0.54 },
        { value: profile.attackRate, weight: 0.24 },
        { value: profile.supportRate, weight: 0.22 }
      ]);
  const preferredCourseFit = weightedAverage([
    { value: profile.courseHeadRate ?? profile.course2Rate ?? profile.course3Rate, weight: 0.48 },
    { value: profile.courseFit === null ? null : normalize(profile.courseFit, -2, 8) * 100, weight: 0.28 },
    { value: profile.lane1stRate ?? profile.lane2RenRate ?? profile.lane3RenRate, weight: 0.24 }
  ]);
  const preferredTechniqueFit = profile.styleScores?.[profile.style?.code] ?? null;
  const venueWinLineFit = weightedAverage([
    { value: clamp(0, 100, 50 + toNum(venueContext?.venue_escape_bias, 0) * (profile.lane === 1 ? 2.8 : 0.4)), weight: 0.26 },
    { value: clamp(0, 100, 50 + toNum(venueContext?.venue_sashi_bias, 0) * (profile.lane === 2 ? 2.4 : 0.5)), weight: 0.18 },
    { value: clamp(0, 100, 50 + toNum(venueContext?.venue_makuri_bias, 0) * (profile.lane === 3 ? 2.5 : 0.6)), weight: 0.18 },
    { value: clamp(0, 100, 50 + toNum(venueContext?.venue_makurizashi_bias, 0) * (profile.lane === 4 ? 2.5 : 0.6)), weight: 0.18 },
    { value: clamp(0, 100, 50 + toNum(venueContext?.venue_outer_3rd_bias, 0) * (profile.lane >= 5 ? 2.2 : 0.4)), weight: 0.2 }
  ]);
  const optionalDisplaySupport = weightedAverage([
    { value: invertNormalize(profile.exhibitionSt, 0.08, 0.22) === null ? null : invertNormalize(profile.exhibitionSt, 0.08, 0.22) * 100, weight: 0.28 },
    { value: invertNormalize(profile.exhibitionTime, 6.55, 7.15) === null ? null : invertNormalize(profile.exhibitionTime, 6.55, 7.15) * 100, weight: 0.34 },
    { value: invertNormalize(profile.lapTime, 6.55, 7.15) === null ? null : invertNormalize(profile.lapTime, 6.55, 7.15) * 100, weight: 0.38 }
  ]);
  return weightedAverage([
    { value: venueBias, weight: 0.14 },
    { value: venueKimariteFit, weight: 0.1 },
    { value: styleFit, weight: 0.18 },
    { value: profile.kimariteHistoryIndex, weight: 0.12 },
    { value: lane56EntryRepro, weight: 0.08 },
    { value: escapeFailConditionScore, weight: 0.08 },
    { value: escapeFailConditionResist, weight: 0.08 },
    { value: profile.recentPerformanceIndex, weight: 0.1 },
    { value: preferredCourseFit, weight: 0.07 },
    { value: preferredTechniqueFit, weight: 0.05 },
    { value: venueWinLineFit, weight: 0.06 },
    { value: optionalDisplaySupport, weight: 0.06 }
  ]);
}

function describeTop6Scenario(profiles = [], top6 = [], chaosValue = 0) {
  const ranked = [...profiles]
    .map((profile) => ({ lane: profile.lane, score: Number(profile.scenarioReproScore) || 0 }))
    .sort((a, b) => b.score - a.score);
  const headLane = Number(String(top6?.[0]?.combo || "").split("-")[0]) || ranked[0]?.lane || 1;
  if (headLane === 1) return chaosValue <= 0.34 ? "stable_escape" : "escape_with_attack_pressure";
  if (headLane === 2) return "boat2_sashi_flow";
  if (headLane === 3) return "boat3_attack_flow";
  if (headLane === 4) return "boat4_cado_flow";
  return "outside_mix_flow";
}

function buildMotorStReadiness(profile = {}) {
  return weightedAverage([
    {
      value: normalize(weightedAverage([{ value: profile.motor2, weight: 0.58 }, { value: profile.boat2, weight: 0.42 }]), 20, 60) === null
        ? null
        : normalize(weightedAverage([{ value: profile.motor2, weight: 0.58 }, { value: profile.boat2, weight: 0.42 }]), 20, 60) * 100,
      weight: 0.62
    },
    {
      value: invertNormalize(profile.avgSt, 0.11, 0.24) === null
        ? null
        : invertNormalize(profile.avgSt, 0.11, 0.24) * 100,
      weight: 0.38
    }
  ]);
}

function venueHeadAdjustment(profile, venueContext = {}) {
  if (profile.lane === 1) {
    const readiness = buildMotorStReadiness(profile);
    const synergyReady = (readiness || 0) >= 52;
    return clamp(
      0,
      100,
      50 +
        toNum(venueContext?.lane1_head_boost, 0) * 1.8 +
        (synergyReady ? toNum(venueContext?.lane1_motor_st_synergy_boost, 0) * 2.2 : 0)
    );
  }
  if (profile.lane >= 5) {
    return clamp(0, 100, 50 - toNum(venueContext?.lane56_head_penalty, 0) * 2.4);
  }
  if (profile.lane === 3) {
    return clamp(0, 100, 50 + toNum(venueContext?.lane3_attack_boost, 0) * 1.3);
  }
  if (profile.lane === 4) {
    return clamp(0, 100, 50 + toNum(venueContext?.lane4_develop_boost, 0) * 0.8);
  }
  return 50;
}

function venueSecondAdjustment(profile, venueContext = {}) {
  if (profile.lane === 2) {
    return clamp(0, 100, 50 + toNum(venueContext?.lane2_second_boost, 0) * 2);
  }
  if (profile.lane === 3) {
    return clamp(0, 100, 50 + toNum(venueContext?.lane3_second_boost, 0) * 1.8 + toNum(venueContext?.lane3_attack_boost, 0) * 0.6);
  }
  if (profile.lane === 4) {
    return clamp(0, 100, 50 + toNum(venueContext?.lane4_develop_boost, 0) * 1.4);
  }
  return profile.lane >= 5
    ? clamp(0, 100, 50 - toNum(venueContext?.lane56_head_penalty, 0) * 0.8)
    : 50;
}

function venueThirdAdjustment(profile, venueContext = {}) {
  if (profile.lane >= 5) {
    return clamp(
      0,
      100,
      50 + toNum(venueContext?.venue_outer_3rd_bias, 0) * 1.6 + toNum(venueContext?.volatility_boost, 0) * 0.7
    );
  }
  if (profile.lane === 3) {
    return clamp(0, 100, 50 + toNum(venueContext?.lane3_attack_boost, 0) * 0.8);
  }
  if (profile.lane === 4) {
    return clamp(0, 100, 50 + toNum(venueContext?.lane4_develop_boost, 0));
  }
  return 50;
}

function buildBoat1SecondKeep(profile = {}, venueContext = {}, raceContext = {}) {
  const motorStReadiness = buildMotorStReadiness(profile);
  const styleCode = String(profile?.style?.code || "");
  const styleFit = ["nige", "stable_hold", "start_attack"].includes(styleCode)
    ? 72
    : styleCode === "tenkai_machi"
      ? 58
      : 44;
  const antiCollapse = weightedAverage([
    { value: 100 - toNum(raceContext?.lane3Makuri, 50), weight: 0.48 },
    { value: 100 - toNum(raceContext?.lane4Breakout, 50), weight: 0.26 },
    { value: 100 - getVenueEscapeFailPressure(venueContext, 1), weight: 0.26 }
  ]);
  const keepScore = round(
    clamp(
      0,
      100,
      weightedAverage([
        {
          value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100,
          weight: 0.22
        },
        { value: motorStReadiness, weight: 0.24 },
        { value: toNum(venueContext?.one_course_trust, 50), weight: 0.16 },
        { value: antiCollapse, weight: 0.18 },
        { value: styleFit, weight: 0.12 },
        { value: profile.stability, weight: 0.08 }
      ]) || 0
    ),
    1
  );
  const reasons = [];
  if ((invertNormalize(profile.avgSt, 0.11, 0.24) || 0) >= 0.45) reasons.push("STが大きく遅れていない");
  if ((motorStReadiness || 0) >= 52) reasons.push("機力が中以上");
  if (toNum(venueContext?.one_course_trust, 0) >= 64) reasons.push("場が内有利");
  if ((antiCollapse || 0) >= 52) reasons.push("まくられ切るリスクが高すぎない");
  if (styleFit >= 58) reasons.push("styleがイン逃げ型/安定残し型/スタート勝負型寄り");
  return {
    score: keepScore,
    reason: reasons.length > 0 ? reasons.join(" / ") : "boat1 second keep is neutral"
  };
}

function calibrateSecondGivenHeadOne(baseSecondMap = {}, profiles = [], venueContext = {}, boat1SecondKeep = { score: 0 }) {
  const laneProfiles = new Map((Array.isArray(profiles) ? profiles : []).map((profile) => [profile.lane, profile]));
  const calibratedRaw = {};
  for (const lane of [2, 3, 4, 5, 6]) {
    const profile = laneProfiles.get(lane) || {};
    const baseProbability = Number(baseSecondMap?.[lane]) || 0.0001;
    const venueLaneBias = getVenueLaneBiasScore(venueContext, lane);
    const courseFit = normalize(profile.course2Rate ?? profile.course3Rate ?? profile.courseHeadRate, 18, 80);
    const styleBias =
      lane === 2
        ? weightedAverage([{ value: profile.styleScores?.sashi, weight: 0.62 }, { value: profile.supportRate, weight: 0.38 }])
        : lane === 3
          ? weightedAverage([{ value: profile.styleScores?.makuri, weight: 0.52 }, { value: profile.styleScores?.makurisashi, weight: 0.48 }])
          : lane === 4
            ? weightedAverage([{ value: profile.styleScores?.makurisashi, weight: 0.58 }, { value: profile.supportRate, weight: 0.42 }])
            : weightedAverage([{ value: profile.supportRate, weight: 0.54 }, { value: profile.attackRate, weight: 0.46 }]);
    const venueAdjustment =
      lane === 2
        ? 1 + toNum(venueContext?.lane2_second_boost, 0) * 0.015 + Math.max(0, toNum(venueContext?.one_course_trust, 50) - 60) * 0.003
        : lane === 3
          ? 1 + (toNum(venueContext?.lane3_attack_boost, 0) + toNum(venueContext?.lane3_second_boost, 0)) * 0.01
          : lane === 4
            ? 1 + toNum(venueContext?.lane4_develop_boost, 0) * 0.012 + Math.max(0, toNum(venueContext?.volatility_boost, 0)) * 0.003
            : 1 + Math.max(0, toNum(venueContext?.volatility_boost, 0)) * 0.006 - Math.max(0, toNum(venueContext?.lane56_head_penalty, 0)) * 0.008;
    const boat1KeepTransfer =
      lane === 2
        ? 1 + Math.max(0, (toNum(boat1SecondKeep?.score, 0) - 50)) * 0.0026
        : lane === 4
          ? 1 + Math.max(0, (toNum(boat1SecondKeep?.score, 0) - 50)) * 0.0014
          : lane === 3
            ? 1 - Math.max(0, (toNum(boat1SecondKeep?.score, 0) - 50)) * 0.0008
            : 1;
    const shapeSupport = weightedAverage([
      { value: venueLaneBias, weight: 0.28 },
      { value: courseFit === null ? null : courseFit * 100, weight: 0.24 },
      { value: styleBias, weight: 0.28 },
      { value: profile.recentPerformanceIndex, weight: 0.2 }
    ]);
    calibratedRaw[lane] = Math.max(
      0.0001,
      baseProbability *
        venueAdjustment *
        boat1KeepTransfer *
        (1 + Math.max(-0.18, ((shapeSupport || 50) - 50) / 180))
    );
  }
  return normalizeMap(calibratedRaw);
}

function buildExactaShapeBias(secondGivenHeadProbabilities = {}, venueContext = {}, boat1SecondKeep = { score: 0 }) {
  return {
    lane2_sashi_bias: round((Number(secondGivenHeadProbabilities?.[2]) || 0) * 100, 1),
    lane3_attack_bias: round((Number(secondGivenHeadProbabilities?.[3]) || 0) * 100, 1),
    lane4_develop_bias: round((Number(secondGivenHeadProbabilities?.[4]) || 0) * 100, 1),
    inner_remain_bias: round(
      weightedAverage([
        { value: toNum(venueContext?.one_course_trust, 50), weight: 0.44 },
        { value: toNum(venueContext?.two_course_sashi_remain_rate, 50), weight: 0.32 },
        { value: toNum(boat1SecondKeep?.score, 50), weight: 0.24 }
      ]),
      1
    ),
    policy: venueContext?.buyPolicy?.code || "balanced_standard"
  };
}

function buildNearTieSecondDiagnostics({
  all120 = [],
  secondGivenHeadProbabilities = {},
  top6Coverage = 0,
  chaosLevel = 0,
  venueContext = {},
  outsideRiskProxy = 0
} = {}) {
  const topSecondCandidates = [2, 3, 4, 5, 6]
    .map((lane) => ({ lane, probability: Number(secondGivenHeadProbabilities?.[lane]) || 0 }))
    .sort((a, b) => b.probability - a.probability);
  const lane2 = topSecondCandidates.find((row) => row.lane === 2) || { lane: 2, probability: 0 };
  const lane3 = topSecondCandidates.find((row) => row.lane === 3) || { lane: 3, probability: 0 };
  const topGap = Math.abs((topSecondCandidates[0]?.probability || 0) - (topSecondCandidates[1]?.probability || 0));
  const secondGap23 = Math.abs((lane2?.probability || 0) - (lane3?.probability || 0));
  const combo124 = (Array.isArray(all120) ? all120 : []).find((row) => row.combo === "1-2-4")?.probability || 0;
  const combo134 = (Array.isArray(all120) ? all120 : []).find((row) => row.combo === "1-3-4")?.probability || 0;
  const comboGap = Math.abs(combo124 - combo134);
  const comboGapScore = round(
    clamp(0, 1, 1 - (comboGap / Math.max(0.0001, Math.max(combo124, combo134, 0.0001)))),
    4
  );
  const volatileVenue = toNum(venueContext?.volatility_boost, 0) >= 6;
  const topTwoTieThreshold = 0.075;
  const secondThirdTieThreshold = 0.058;
  const comboPreserveThreshold = 0.74;
  const topTwoTied = topGap <= topTwoTieThreshold;
  const secondThirdTied =
    topSecondCandidates.length >= 3 &&
    Math.abs((topSecondCandidates[1]?.probability || 0) - (topSecondCandidates[2]?.probability || 0)) <= secondThirdTieThreshold;
  const nearTieCandidateCount = topSecondCandidates.filter((row, index) => {
    if (index === 0) return true;
    if (index === 1) return Math.abs((topSecondCandidates[0]?.probability || 0) - row.probability) <= topTwoTieThreshold;
    if (index === 2) return secondThirdTied || Math.abs((topSecondCandidates[1]?.probability || 0) - row.probability) <= secondThirdTieThreshold;
    return false;
  }).length;
  const lowCoverage = top6Coverage <= 0.16;
  const veryHighChaos = chaosLevel >= 1;
  const preserve =
    (
      (
        comboGapScore >= comboPreserveThreshold &&
        secondGap23 <= 0.065 &&
        (
          lowCoverage ||
          chaosLevel >= 0.55 ||
          outsideRiskProxy >= 0.42 ||
          volatileVenue
        ) &&
        toNum(venueContext?.one_course_trust, 0) >= 56 &&
        toNum(venueContext?.three_course_attack_success_rate, 0) >= 40
      ) ||
      (
        nearTieCandidateCount >= 2 &&
        lowCoverage &&
        (veryHighChaos || outsideRiskProxy >= 0.38 || volatileVenue)
      )
    );
  return {
    near_tie_second_candidates: topSecondCandidates.filter((row, index) => {
      if (index === 0) return true;
      if (index === 1) return Math.abs((topSecondCandidates[0]?.probability || 0) - row.probability) <= topTwoTieThreshold;
      if (index === 2) {
        return secondThirdTied || Math.abs((topSecondCandidates[1]?.probability || 0) - row.probability) <= secondThirdTieThreshold;
      }
      return false;
    }),
    close_combo_preserved: preserve,
    combo_gap_score: comboGapScore,
    combo_gap: round(comboGap, 4),
    combo_124_probability: round(combo124, 4),
    combo_134_probability: round(combo134, 4),
    second_gap_2_vs_3: round(secondGap23, 4),
    top_second_gap: round(topGap, 4),
    top_two_tied: topTwoTied,
    second_third_tied: secondThirdTied,
    near_tie_candidate_count: nearTieCandidateCount,
    top_two_tie_threshold: topTwoTieThreshold,
    second_third_tie_threshold: secondThirdTieThreshold
  };
}

function preserveCloseSecondCombos(topRows = [], allRows = [], diagnostics = {}) {
  if (!diagnostics?.close_combo_preserved) {
    return Array.isArray(topRows) ? topRows : [];
  }
  const current = [...(Array.isArray(topRows) ? topRows : [])];
  const byCombo = new Map((Array.isArray(allRows) ? allRows : []).map((row) => [row.combo, row]));
  const priorityCombos = diagnostics?.second_third_tied || diagnostics?.near_tie_candidate_count >= 3
    ? ["1-2-4", "1-3-4", "1-2-3", "1-3-2", "1-4-2", "1-4-3"]
    : ["1-2-4", "1-3-4"];
  for (const combo of priorityCombos) {
    if (current.some((row) => row.combo === combo)) continue;
    const candidate = byCombo.get(combo);
    if (!candidate) continue;
    if (current.length >= 6) current.pop();
    current.push(candidate);
    current.sort((a, b) => (Number(b?.probability) || 0) - (Number(a?.probability) || 0));
  }
  return current.slice(0, 6);
}

function firstPlaceScore(profile, venueContext = {}, raceContext = {}) {
  const laneBias = getVenueLaneBiasScore(venueContext, profile.lane);
  const venueStyleFit = getVenueStyleMatchScore(venueContext, profile.lane, profile.style?.code);
  const flSafety = invertNormalize((profile.fCount || 0) * 0.9 + (profile.lCount || 0) * 0.45, 0, 2.5);
  const escapeThreatPenalty = profile.lane === 1
    ? weightedAverage([
        { value: 100 - toNum(raceContext?.lane3Makuri, 50), weight: 0.44 },
        { value: 100 - toNum(raceContext?.lane4Breakout, 50), weight: 0.28 },
        { value: 100 - toNum(raceContext?.outsideHeadPressure, 50), weight: 0.18 },
        { value: 100 - getVenueEscapeFailPressure(venueContext, 1), weight: 0.1 }
      ])
    : null;
  return weightedAverage([
    { value: normalize(profile.nationwideWinRate, 4, 8.5) === null ? null : normalize(profile.nationwideWinRate, 4, 8.5) * 100, weight: 0.14 },
    { value: normalize(profile.localWinRate, 4, 8.5) === null ? null : normalize(profile.localWinRate, 4, 8.5) * 100, weight: 0.12 },
    { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.12 },
    { value: flSafety === null ? null : flSafety * 100, weight: 0.06 },
    { value: normalize(weightedAverage([{ value: profile.motor2, weight: 0.58 }, { value: profile.boat2, weight: 0.42 }]), 20, 60) === null ? null : normalize(weightedAverage([{ value: profile.motor2, weight: 0.58 }, { value: profile.boat2, weight: 0.42 }]), 20, 60) * 100, weight: 0.12 },
    { value: normalize(profile.courseHeadRate ?? profile.course2Rate ?? profile.course3Rate, 18, 80) === null ? null : normalize(profile.courseHeadRate ?? profile.course2Rate ?? profile.course3Rate, 18, 80) * 100, weight: 0.14 },
    { value: laneBias, weight: 0.08 },
    { value: venueStyleFit, weight: 0.04 },
    { value: profile.recentPerformanceIndex, weight: 0.08 },
    { value: profile.scenarioReproScore, weight: 0.08 },
    { value: venueHeadAdjustment(profile, venueContext), weight: 0.08 },
    { value: escapeThreatPenalty, weight: 0.04 },
    { value: invertNormalize(profile.lapTime, 6.55, 7.15) === null ? null : invertNormalize(profile.lapTime, 6.55, 7.15) * 100, weight: 0.02 },
    { value: invertNormalize(profile.lapRank, 1, 6) === null ? null : invertNormalize(profile.lapRank, 1, 6) * 100, weight: 0.02 },
    { value: invertNormalize(profile.lapGapFromBest, 0, 0.25) === null ? null : invertNormalize(profile.lapGapFromBest, 0, 0.25) * 100, weight: 0.02 }
  ]);
}

function secondPlaceScore(profile, headProfile, raceContext = {}, venueContext = {}) {
  const laneRelation = clamp(0, 100, 82 - Math.abs(profile.lane - headProfile.lane) * 8);
  const outsideBoost = profile.lane >= 5 ? toNum(raceContext?.outside2ndPressure, 0) : 0;
  const venueLaneBias = getVenueLaneBiasScore(venueContext, profile.lane);
  const venueStyleFit = getVenueStyleMatchScore(venueContext, profile.lane, profile.style?.code);
  const boat1SecondKeep = profile.lane === 1 ? buildBoat1SecondKeep(profile, venueContext, raceContext).score : null;
  return weightedAverage([
    { value: profile.supportRate, weight: 0.26 },
    { value: normalize(profile.course2Rate ?? profile.course3Rate, 18, 80) === null ? null : normalize(profile.course2Rate ?? profile.course3Rate, 18, 80) * 100, weight: 0.2 },
    { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.12 },
    { value: normalize(profile.motor2, 20, 60) === null ? null : normalize(profile.motor2, 20, 60) * 100, weight: 0.11 },
    { value: profile.styleScores?.sashi, weight: 0.1 },
    { value: profile.styleScores?.makurisashi, weight: 0.08 },
    { value: venueLaneBias, weight: 0.05 },
    { value: venueStyleFit, weight: 0.03 },
    { value: venueSecondAdjustment(profile, venueContext), weight: 0.06 },
    { value: boat1SecondKeep, weight: profile.lane === 1 ? 0.08 : 0 },
    { value: laneRelation, weight: 0.05 },
    { value: outsideBoost, weight: 0.03 },
    { value: profile.recentPerformanceIndex, weight: 0.05 },
    { value: invertNormalize(profile.lapGapFromBest, 0, 0.25) === null ? null : invertNormalize(profile.lapGapFromBest, 0, 0.25) * 100, weight: 0.03 },
    { value: profile.lapStretchFoot, weight: 0.02 }
  ]);
}

function thirdPlaceScore(profile, headProfile, secondProfile, raceContext = {}, venueContext = {}) {
  const developmentGap = clamp(
    0,
    100,
    100 - Math.abs(
      (profile.supportRate || 50) -
      weightedAverage([
        { value: headProfile.attackRate || 50, weight: 0.46 },
        { value: secondProfile.supportRate || 50, weight: 0.54 }
      ])
    )
  );
  const outsideThirdBoost = profile.lane >= 5 ? toNum(raceContext?.outside3rdPressure, 0) : 0;
  const venueLaneBias = getVenueLaneBiasScore(venueContext, profile.lane);
  const venueStyleFit = getVenueStyleMatchScore(venueContext, profile.lane, profile.style?.code);
  return weightedAverage([
    { value: normalize(profile.course3Rate ?? profile.course2Rate, 18, 85) === null ? null : normalize(profile.course3Rate ?? profile.course2Rate, 18, 85) * 100, weight: 0.24 },
    { value: profile.stability, weight: 0.2 },
    { value: profile.supportRate, weight: 0.18 },
    { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.1 },
    { value: normalize(profile.motor2, 20, 60) === null ? null : normalize(profile.motor2, 20, 60) * 100, weight: 0.1 },
    { value: profile.styleScores?.tenkai_machi, weight: 0.08 },
    { value: venueLaneBias, weight: 0.04 },
    { value: venueStyleFit, weight: 0.02 },
    { value: venueThirdAdjustment(profile, venueContext), weight: 0.05 },
    { value: developmentGap, weight: 0.08 },
    { value: outsideThirdBoost, weight: 0.02 },
    { value: invertNormalize(profile.lapGapFromBest, 0, 0.25) === null ? null : invertNormalize(profile.lapGapFromBest, 0, 0.25) * 100, weight: 0.04 },
    { value: profile.lapStretchFoot, weight: 0.04 }
  ]);
}

function aggregateFinishProbabilitiesFromAll120(profiles, all120 = []) {
  const first = Object.fromEntries(profiles.map((profile) => [profile.lane, 0]));
  const second = Object.fromEntries(profiles.map((profile) => [profile.lane, 0]));
  const third = Object.fromEntries(profiles.map((profile) => [profile.lane, 0]));

  for (const row of Array.isArray(all120) ? all120 : []) {
    const probability = Number(row?.probability) || 0;
    const [firstLane, secondLane, thirdLane] = String(row?.combo || "")
      .split("-")
      .map((value) => Number(value));
    if (Number.isInteger(firstLane)) first[firstLane] = (first[firstLane] || 0) + probability;
    if (Number.isInteger(secondLane)) second[secondLane] = (second[secondLane] || 0) + probability;
    if (Number.isInteger(thirdLane)) third[thirdLane] = (third[thirdLane] || 0) + probability;
  }

  return {
    first: normalizeMap(first),
    second: normalizeMap(second),
    third: normalizeMap(third)
  };
}

function buildCandidateRows(probMap = {}, profiles = [], place) {
  const styleByLane = new Map(profiles.map((profile) => [profile.lane, profile.style?.label || null]));
  return Object.entries(probMap)
    .map(([lane, probability]) => ({
      lane: Number(lane),
      probability: round(probability, 4),
      rate: round(Number(probability) * 100, 1),
      style: styleByLane.get(Number(lane)) || null,
      place
    }))
    .sort((a, b) => b.probability - a.probability);
}

export function buildFormationSuggestion(sortedCombos = [], finishProbabilities = {}, chaosLevel = 0, top6Coverage = 0, venueContext = {}, diagnostics = {}) {
  const venueName = String(venueContext?.venue_name || "");
  const optionalFormationBoost = toNum(venueContext?.optional_formation_trigger_boost, 0);
  const firstEntries = Object.entries(finishProbabilities?.first || {})
    .map(([lane, probability]) => [Number(lane), Number(probability) || 0])
    .sort((a, b) => b[1] - a[1]);
  const secondEntries = Object.entries(finishProbabilities?.second || {})
    .map(([lane, probability]) => [Number(lane), Number(probability) || 0])
    .sort((a, b) => b[1] - a[1]);
  const thirdEntries = Object.entries(finishProbabilities?.third || {})
    .map(([lane, probability]) => [Number(lane), Number(probability) || 0])
    .sort((a, b) => b[1] - a[1]);
  const topHeadProbability = firstEntries[0]?.[1] || 0;
  const secondHeadProbability = firstEntries[1]?.[1] || 0;
  const headGap = topHeadProbability - secondHeadProbability;
  const outerHeadShare = firstEntries
    .filter(([lane]) => lane >= 3)
    .reduce((sum, [, probability]) => sum + probability, 0);
  const outsideBreakRiskProxy = clamp(
    0,
    1,
    Math.max(
      toNum(venueContext?.venue_outside_break_risk, 0) / 100,
      toNum(venueContext?.lane56_renyuu_intrusion_rate, 0) / 100,
      chaosLevel * 0.85
    )
  );
  const extremelyLowCoverage = top6Coverage <= 0.12 + Math.max(0, optionalFormationBoost) * 0.002;
  const lowCoverageWithChaos = chaosLevel >= Math.max(0.9, 1 - Math.max(0, optionalFormationBoost) * 0.02) && top6Coverage <= 0.15 + Math.max(0, optionalFormationBoost) * 0.003;
  const lowCoverage = top6Coverage <= 0.43 + Math.max(0, optionalFormationBoost) * 0.004;
  const weakHeadDominance = topHeadProbability <= 0.52;
  const topHeadNotRunaway = topHeadProbability <= 0.6 || headGap <= 0.12;
  const multipleViableHeads = secondHeadProbability >= 0.18 || Math.abs(topHeadProbability - secondHeadProbability) <= 0.1;
  const outerInvolvementElevated = outerHeadShare >= 0.34;
  const highChaos = chaosLevel >= Math.max(0.3, 0.39 - Math.max(0, optionalFormationBoost) * 0.01);
  const mediumOutsideRisk = outsideBreakRiskProxy >= Math.max(0.32, 0.42 - Math.max(0, optionalFormationBoost) * 0.008);
  const elevatedOutsideBreakRisk = outsideBreakRiskProxy >= Math.max(0.38, 0.48 - Math.max(0, optionalFormationBoost) * 0.008);
  const shouldSuggest =
    extremelyLowCoverage ||
    lowCoverageWithChaos ||
    lowCoverage ||
    weakHeadDominance ||
    topHeadNotRunaway ||
    multipleViableHeads ||
    outerInvolvementElevated ||
    highChaos ||
    mediumOutsideRisk ||
    elevatedOutsideBreakRisk;
  const reasons = [];
  if (extremelyLowCoverage) reasons.push("top6 coverage is extremely low");
  else if (lowCoverageWithChaos) reasons.push("top6 coverage is low under high chaos");
  else if (lowCoverage) reasons.push("top6 coverage is low");
  if (multipleViableHeads || weakHeadDominance || topHeadNotRunaway) reasons.push("top head candidate is not dominant");
  if (outerInvolvementElevated) reasons.push("outer lanes have increased involvement risk");
  if (highChaos) reasons.push("chaos level is high");
  if (elevatedOutsideBreakRisk || mediumOutsideRisk) reasons.push("outside break risk is elevated");
  if (diagnostics?.close_combo_preserved) reasons.push("1-2-4 and 1-3-4 are close");
  if (diagnostics?.close_combo_preserved) reasons.push("close second-place combo preserved into top6");
  if (diagnostics?.top_two_tied || diagnostics?.second_third_tied) reasons.push("2nd-place candidates 2/3/4 are tightly clustered");

  const firstCandidateLimit =
    venueName === "Omura" || venueName === "Amagasaki"
      ? 2
      : chaosLevel >= 0.55 || venueName === "Tamagawa" || venueName === "Ashiya"
        ? 3
        : 2;
  const secondCandidateLimit =
    venueName === "Tamagawa" || venueName === "Ashiya"
      ? 5
      : 4;
  const thirdCandidateLimit =
    venueName === "Tamagawa" || venueName === "Ashiya"
      ? 5
      : 4;
  const firstCandidates = firstEntries
    .map(([lane]) => Number(lane))
    .slice(0, firstCandidateLimit);
  if (venueName === "Omura" || venueName === "Amagasaki") {
    while (firstCandidates.length > 0 && firstCandidates[firstCandidates.length - 1] >= 5) {
      firstCandidates.pop();
    }
  }
  const secondCandidates = secondEntries
    .map(([lane]) => Number(lane))
    .slice(0, secondCandidateLimit);
  const thirdCandidates = thirdEntries
    .map(([lane]) => Number(lane))
    .slice(0, thirdCandidateLimit);
  if (venueName === "Tokuyama" || venueName === "Suminoe") {
    for (const lane of [3, 4]) {
      if (!secondCandidates.includes(lane)) secondCandidates.push(lane);
      if (!thirdCandidates.includes(lane)) thirdCandidates.push(lane);
    }
  }
  if (venueName === "Ashiya" || venueName === "Tamagawa") {
    for (const lane of [5, 6]) {
      if (!thirdCandidates.includes(lane)) thirdCandidates.push(lane);
    }
  }
  if (!shouldSuggest) {
    return {
      active: false,
      size: 0,
      combos: [],
      first_candidates: [...new Set(firstCandidates)].slice(0, firstCandidateLimit),
      second_candidates: [...new Set(secondCandidates)].slice(0, secondCandidateLimit + 1),
      third_candidates: [...new Set(thirdCandidates)].slice(0, thirdCandidateLimit + 1),
      formation_string: null,
      reason: null,
      reasons: [],
      trigger_flags: {
        low_top6_coverage: false,
        extremely_low_top6_coverage: false,
        low_top6_coverage_with_chaos: false,
        weak_head_dominance: false,
        top_head_not_runaway: false,
        multiple_viable_heads: false,
        outer_involvement_elevated: false,
        outer_head_share_3to6: round(outerHeadShare, 4),
        high_chaos: false,
        medium_outside_break_risk: false,
        elevated_outside_break_risk: false,
        close_combo_preserved: diagnostics?.close_combo_preserved === true,
        combo_gap_score: diagnostics?.combo_gap_score ?? null
      }
    };
  }

  const targetSize =
    venueName === "Tamagawa" || venueName === "Ashiya"
      ? 18
      : venueName === "Omura" || venueName === "Amagasaki"
        ? 14
        : venueName === "Tokuyama" || venueName === "Suminoe"
          ? 16
        : highChaos || outerInvolvementElevated
          ? 16
          : 14;
  const filtered = sortedCombos.filter((row) => {
    const [first, second, third] = String(row.combo).split("-").map(Number);
    return firstCandidates.includes(first) && secondCandidates.includes(second) && thirdCandidates.includes(third) && first !== second && second !== third && first !== third;
  });
  const combos = (filtered.length >= 10 ? filtered : sortedCombos.filter((row) => {
    const [first, second, third] = String(row.combo).split("-").map(Number);
    return first !== second && second !== third && first !== third;
  }))
    .slice(0, targetSize)
    .map((row, index) => ({
    rank: index + 1,
    combo: row.combo,
    probability: round(row.probability, 4)
  }));

  return {
    active: combos.length > 0,
    size: combos.length,
    combos,
    first_candidates: [...new Set(firstCandidates)].slice(0, firstCandidateLimit),
    second_candidates: [...new Set(secondCandidates)].slice(0, secondCandidateLimit + 1),
    third_candidates: [...new Set(thirdCandidates)].slice(0, thirdCandidateLimit + 1),
    formation_string: `${[...new Set(firstCandidates)].slice(0, firstCandidateLimit).join("")}-${[...new Set(secondCandidates)].slice(0, secondCandidateLimit + 1).join("")}-${[...new Set(thirdCandidates)].slice(0, thirdCandidateLimit + 1).join("")}`,
    reason: reasons[0] || "high payout window",
    reasons,
    trigger_flags: {
      low_top6_coverage: lowCoverage,
      extremely_low_top6_coverage: extremelyLowCoverage,
      low_top6_coverage_with_chaos: lowCoverageWithChaos,
      weak_head_dominance: weakHeadDominance,
      top_head_not_runaway: topHeadNotRunaway,
      multiple_viable_heads: multipleViableHeads,
      outer_involvement_elevated: outerInvolvementElevated,
      outer_head_share_3to6: round(outerHeadShare, 4),
      high_chaos: highChaos,
      medium_outside_break_risk: mediumOutsideRisk,
      elevated_outside_break_risk: elevatedOutsideBreakRisk,
      close_combo_preserved: diagnostics?.close_combo_preserved === true,
      combo_gap_score: diagnostics?.combo_gap_score ?? null
    }
  };
}

export function buildTop6Prediction({ ranking = [], race = null } = {}) {
  const profiles = (Array.isArray(ranking) ? ranking : [])
    .map((row) => buildLaneProfile(row))
    .filter((row) => Number.isInteger(row?.lane))
    .sort((a, b) => a.lane - b.lane);
  if (profiles.length !== 6) return null;

  const venueContext = getVenueScenarioContext(race?.venueId);
  profiles.forEach((profile) => {
    profile.scenarioReproScore = buildScenarioReproScore(profile, venueContext);
  });

  const lane3 = profiles.find((row) => row.lane === 3) || {};
  const lane4 = profiles.find((row) => row.lane === 4) || {};
  const lane5 = profiles.find((row) => row.lane === 5) || {};
  const lane6 = profiles.find((row) => row.lane === 6) || {};
  const raceContext = {
    lane3Makuri: weightedAverage([
      { value: lane3.attackRate, weight: 0.42 },
      { value: lane3.makuriRate, weight: 0.36 },
      { value: lane3.makuriSashiRate, weight: 0.22 }
    ]),
    lane4Breakout: weightedAverage([
      { value: lane4.attackRate, weight: 0.42 },
      { value: lane4.breakoutRate, weight: 0.34 },
      { value: lane4.zentsukeTendency, weight: 0.24 }
    ]),
    outsideHeadPressure: weightedAverage([
      { value: lane5.attackRate, weight: 0.22 },
      { value: lane6.attackRate, weight: 0.22 },
      { value: lane5.makuriRate, weight: 0.16 },
      { value: lane6.makuriRate, weight: 0.16 },
      { value: lane5.scenarioReproScore, weight: 0.1 },
      { value: lane6.scenarioReproScore, weight: 0.1 },
      { value: venueContext?.outer_renyuu_entry_rate, weight: 0.04 },
      { value: clamp(0, 100, 50 - toNum(venueContext?.lane56_head_penalty, 0) * 2 + toNum(venueContext?.volatility_boost, 0) * 0.5), weight: 0.06 }
    ]),
    outside2ndPressure: weightedAverage([
      { value: lane5.supportRate, weight: 0.34 },
      { value: lane6.supportRate, weight: 0.34 },
      { value: lane5.attackRate, weight: 0.16 },
      { value: lane6.attackRate, weight: 0.1 },
      { value: clamp(0, 100, 50 + toNum(venueContext?.venue_outer_3rd_bias, 0) * 1.4 + toNum(venueContext?.volatility_boost, 0) * 0.8), weight: 0.06 }
    ]),
    outside3rdPressure: weightedAverage([
      { value: lane5.course3Rate, weight: 0.4 },
      { value: lane6.course3Rate, weight: 0.4 },
      { value: lane5.stability, weight: 0.1 },
      { value: lane6.stability, weight: 0.06 },
      { value: venueContext?.outer_renyuu_entry_rate, weight: 0.04 },
      { value: clamp(0, 100, 50 + toNum(venueContext?.venue_outer_3rd_bias, 0) * 2 + toNum(venueContext?.volatility_boost, 0)), weight: 0.04 }
    ])
  };
  profiles.forEach((profile) => {
    profile.scenarioReproScore = buildScenarioReproScore(profile, venueContext, raceContext);
  });
  const boat1Profile = profiles.find((profile) => profile.lane === 1) || {};
  const boat1SecondKeep = buildBoat1SecondKeep(boat1Profile, venueContext, raceContext);

  const headProbMap = normalizeMap(
    Object.fromEntries(
      profiles.map((profile) => [profile.lane, Math.max(0.001, (firstPlaceScore(profile, venueContext, raceContext) || 1) / 100)])
    )
  );

  const secondMap = {};
  for (const head of profiles) {
    secondMap[head.lane] = normalizeMap(
      Object.fromEntries(
        profiles
          .filter((profile) => profile.lane !== head.lane)
          .map((profile) => [profile.lane, Math.max(0.001, (secondPlaceScore(profile, head, raceContext, venueContext) || 1) / 100)])
      )
    );
  }
  secondMap[1] = calibrateSecondGivenHeadOne(secondMap[1], profiles, venueContext, boat1SecondKeep);

  const thirdMap = {};
  for (const head of profiles) {
    thirdMap[head.lane] = {};
    for (const second of profiles.filter((row) => row.lane !== head.lane)) {
      thirdMap[head.lane][second.lane] = normalizeMap(
        Object.fromEntries(
          profiles
            .filter((profile) => profile.lane !== head.lane && profile.lane !== second.lane)
            .map((profile) => [profile.lane, Math.max(0.001, (thirdPlaceScore(profile, head, second, raceContext, venueContext) || 1) / 100)])
        )
      );
    }
  }

  const comboRows = [];
  for (const head of profiles) {
    for (const second of profiles.filter((row) => row.lane !== head.lane)) {
      for (const third of profiles.filter((row) => row.lane !== head.lane && row.lane !== second.lane)) {
        comboRows.push({
          combo: `${head.lane}-${second.lane}-${third.lane}`,
          probability:
            (headProbMap[head.lane] || 0) *
            (secondMap[head.lane]?.[second.lane] || 0) *
            (thirdMap[head.lane]?.[second.lane]?.[third.lane] || 0)
        });
      }
    }
  }

  const normalized120 = normalizeMap(Object.fromEntries(comboRows.map((row) => [row.combo, row.probability])));
  const all120 = Object.entries(normalized120)
    .map(([combo, probability]) => ({ combo, probability: round(probability, 4) }))
    .sort((a, b) => b.probability - a.probability);
  const preTop6 = all120.slice(0, 6);
  const top6CoverageSeed = round(preTop6.reduce((sum, row) => sum + Number(row.probability || 0), 0), 4);
  const strongestHead = Math.max(...Object.values(headProbMap).map((value) => Number(value) || 0));
  const chaosValue = clamp(0, 1, 1 - top6CoverageSeed + Math.min(0.25, entropy(headProbMap) / 2));
  const outsideRiskProxy = clamp(
    0,
    1,
    Math.max(
      toNum(venueContext?.venue_outside_break_risk, 0) / 100,
      toNum(venueContext?.lane56_renyuu_intrusion_rate, 0) / 100,
      chaosValue * 0.85
    )
  );
  const secondGivenHeadProbabilities = secondMap[1];
  const nearTieDiagnostics = buildNearTieSecondDiagnostics({
    all120,
    secondGivenHeadProbabilities,
    top6Coverage: top6CoverageSeed,
    chaosLevel: chaosValue,
    venueContext,
    outsideRiskProxy
  });
  const top6 = preserveCloseSecondCombos(preTop6, all120, nearTieDiagnostics).map((row, index) => ({
    ...row,
    tier: index < 2 ? "本命" : index < 4 ? "対抗" : "抑え",
    rank: index + 1
  }));

  const top6Coverage = round(top6.reduce((sum, row) => sum + Number(row.probability || 0), 0), 4);
  const top6ScenarioScore = round(
    weightedAverage(profiles.map((profile) => ({
      value: profile.scenarioReproScore,
      weight:
        profile.lane === 1
          ? 1.25 + Math.max(0, toNum(venueContext?.venue_escape_bias, 0)) * 0.01
          : profile.lane === 3 || profile.lane === 4
            ? 1 + Math.max(0, toNum(venueContext?.venue_makuri_bias, 0) + toNum(venueContext?.venue_makurizashi_bias, 0)) * 0.008
            : profile.lane >= 5
              ? 1 + Math.max(0, toNum(venueContext?.venue_outer_3rd_bias, 0)) * 0.01
              : 1
    }))),
    1
  );
  const confidence = round(
    clamp(
      0,
      1,
      top6Coverage * 0.62 +
      strongestHead * 0.18 +
      (1 - (toNum(raceContext.outsideHeadPressure, 50) / 100)) * 0.06 +
      (1 - (toNum(raceContext.lane3Makuri, 50) / 100)) * 0.04 +
      (1 - (toNum(raceContext.lane4Breakout, 50) / 100)) * 0.04 +
      Math.min(0.06, (((top6ScenarioScore || 50) - 50) / 100)) +
      Math.min(0.06, (((weightedAverage(profiles.map((profile) => ({ value: profile.recentPerformanceIndex, weight: 1 }))) || 50) - 50) / 100))
    ),
    4
  );
  const finishProbabilities = aggregateFinishProbabilitiesFromAll120(profiles, all120);
  const firstPlaceCandidateRates = buildCandidateRows(finishProbabilities.first, profiles, "first");
  const secondPlaceCandidateRates = buildCandidateRows(finishProbabilities.second, profiles, "second");
  const thirdPlaceCandidateRates = buildCandidateRows(finishProbabilities.third, profiles, "third");
  const exactaShapeBias = buildExactaShapeBias(secondGivenHeadProbabilities, venueContext, boat1SecondKeep);
  const formationSuggestion = buildFormationSuggestion(all120, finishProbabilities, chaosValue, top6Coverage, venueContext, nearTieDiagnostics);
  const top6Scenario = describeTop6Scenario(profiles, top6, chaosValue);

  return {
    head_prob_1: headProbMap[1] || 0,
    head_prob_2: headProbMap[2] || 0,
    head_prob_3: headProbMap[3] || 0,
    head_prob_4: headProbMap[4] || 0,
    head_prob_5: headProbMap[5] || 0,
    head_prob_6: headProbMap[6] || 0,
    head_candidate_ranking: Object.entries(headProbMap)
      .map(([lane, probability]) => ({ lane: Number(lane), probability: round(probability, 4) }))
      .sort((a, b) => b.probability - a.probability),
    winProbabilities: finishProbabilities.first,
    secondProbabilities: finishProbabilities.second,
    thirdProbabilities: finishProbabilities.third,
    first_place_candidate_rates: firstPlaceCandidateRates,
    second_place_candidate_rates: secondPlaceCandidateRates,
    third_place_candidate_rates: thirdPlaceCandidateRates,
    finish_probabilities: finishProbabilities,
    boat1_second_keep_score: boat1SecondKeep.score,
    boat1_second_keep_reason: boat1SecondKeep.reason,
    second_given_head_probabilities: secondGivenHeadProbabilities,
    exacta_shape_bias: exactaShapeBias,
    near_tie_second_candidates: nearTieDiagnostics.near_tie_second_candidates,
    close_combo_preserved: nearTieDiagnostics.close_combo_preserved,
    combo_gap_score: nearTieDiagnostics.combo_gap_score,
    all_120_combinations: all120,
    top6,
    top6_coverage: top6Coverage,
    confidence,
    chaos_level: round(chaosValue, 4),
    chaos_label: chaosValue >= 0.52 ? "高" : chaosValue >= 0.34 ? "中" : "低",
    top6Scenario,
    top6ScenarioScore,
    scenario_repro_score: top6ScenarioScore,
    main_ticket: top6[0] || null,
    optionalFormation16: formationSuggestion,
    formationReason:
      Array.isArray(formationSuggestion?.reasons) && formationSuggestion.reasons.length > 0
        ? formationSuggestion.reasons.join("; ")
        : formationSuggestion?.reason || null,
    wide_formation_suggestion: formationSuggestion,
    venueBiasProfile: venueContext?.venueBiasProfile || venueContext?.venue_bias_profile || null,
    buyPolicy: venueContext?.buyPolicy || null,
    venueAdjustmentReason: Array.isArray(venueContext?.venueAdjustmentReason) ? venueContext.venueAdjustmentReason : [],
    conditional_probabilities: {
      first: headProbMap,
      second: secondMap,
      third: thirdMap
    },
    lane_styles: profiles.map((profile) => ({
      lane: profile.lane,
      style: profile.style?.label || STYLE_LABELS[profile.style?.code] || STYLE_LABELS.tenkai_machi,
      style_code: profile.style?.code || "tenkai_machi",
      style_score: round(profile.styleScore ?? profile.styleScores?.[profile.style?.code] ?? 0, 1),
      style_reasons: Array.isArray(profile.styleReasons) && profile.styleReasons.length > 0
        ? profile.styleReasons
        : buildStyleReasons(profile, profile.style?.code || "tenkai_machi"),
      scenario_repro_score: round(profile.scenarioReproScore, 1)
    })),
    venue_scenario_bias: venueContext,
    scenario_repro_scores: profiles.map((profile) => ({
      lane: profile.lane,
      score: round(profile.scenarioReproScore, 1),
      style: profile.style?.label || STYLE_LABELS[profile.style?.code] || STYLE_LABELS.tenkai_machi
    }))
  };
}
