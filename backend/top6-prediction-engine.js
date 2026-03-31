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
  return weightedAverage([
    { value: profile.supportRate, weight: 0.26 },
    { value: normalize(profile.course2Rate ?? profile.course3Rate, 18, 80) === null ? null : normalize(profile.course2Rate ?? profile.course3Rate, 18, 80) * 100, weight: 0.2 },
    { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.12 },
    { value: normalize(profile.motor2, 20, 60) === null ? null : normalize(profile.motor2, 20, 60) * 100, weight: 0.11 },
    { value: profile.styleScores?.sashi, weight: 0.1 },
    { value: profile.styleScores?.makurisashi, weight: 0.08 },
    { value: venueLaneBias, weight: 0.05 },
    { value: venueStyleFit, weight: 0.03 },
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

function buildFormationSuggestion(sortedCombos = [], finishProbabilities = {}, chaosLevel = 0, top6Coverage = 0) {
  const firstCandidates = (Object.entries(finishProbabilities?.first || {}))
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .map(([lane]) => Number(lane))
    .slice(0, chaosLevel >= 0.55 ? 3 : 2);
  const secondCandidates = (Object.entries(finishProbabilities?.second || {}))
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .map(([lane]) => Number(lane))
    .slice(0, 4);
  const thirdCandidates = (Object.entries(finishProbabilities?.third || {}))
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .map(([lane]) => Number(lane))
    .slice(0, 4);
  const shouldSuggest = chaosLevel >= 0.38 || top6Coverage <= 0.43 || (finishProbabilities?.first?.[1] || 0) <= 0.5;
  if (!shouldSuggest) {
    return {
      active: false,
      size: 0,
      combos: [],
      first_candidates: firstCandidates,
      second_candidates: secondCandidates,
      third_candidates: thirdCandidates,
      formation_string: null,
      reason: null
    };
  }

  const filtered = sortedCombos.filter((row) => {
    const [first, second, third] = String(row.combo).split("-").map(Number);
    return firstCandidates.includes(first) && secondCandidates.includes(second) && thirdCandidates.includes(third);
  });
  const combos = (filtered.length >= 12 ? filtered : sortedCombos).slice(0, 16).map((row, index) => ({
    rank: index + 1,
    combo: row.combo,
    probability: round(row.probability, 4)
  }));

  return {
    active: combos.length > 0,
    size: combos.length,
    combos,
    first_candidates: firstCandidates,
    second_candidates: secondCandidates,
    third_candidates: thirdCandidates,
    formation_string: `${firstCandidates.join("")}-${secondCandidates.join("")}-${thirdCandidates.join("")}`,
    reason: chaosLevel >= 0.52
      ? "high_chaos"
      : top6Coverage <= 0.43
        ? "low_top6_coverage"
        : "high_payout_window"
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
      { value: venueContext?.outer_renyuu_entry_rate, weight: 0.04 }
    ]),
    outside2ndPressure: weightedAverage([
      { value: lane5.supportRate, weight: 0.34 },
      { value: lane6.supportRate, weight: 0.34 },
      { value: lane5.attackRate, weight: 0.16 },
      { value: lane6.attackRate, weight: 0.16 }
    ]),
    outside3rdPressure: weightedAverage([
      { value: lane5.course3Rate, weight: 0.4 },
      { value: lane6.course3Rate, weight: 0.4 },
      { value: lane5.stability, weight: 0.1 },
      { value: lane6.stability, weight: 0.06 },
      { value: venueContext?.outer_renyuu_entry_rate, weight: 0.04 }
    ])
  };
  profiles.forEach((profile) => {
    profile.scenarioReproScore = buildScenarioReproScore(profile, venueContext, raceContext);
  });

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
  const top6 = all120.slice(0, 6).map((row, index) => ({
    ...row,
    tier: index < 2 ? "本命" : index < 4 ? "対抗" : "抑え",
    rank: index + 1
  }));

  const top6Coverage = round(top6.reduce((sum, row) => sum + Number(row.probability || 0), 0), 4);
  const strongestHead = Math.max(...Object.values(headProbMap).map((value) => Number(value) || 0));
  const chaosValue = clamp(0, 1, 1 - top6Coverage + Math.min(0.25, entropy(headProbMap) / 2));
  const top6ScenarioScore = round(
    weightedAverage(profiles.map((profile) => ({ value: profile.scenarioReproScore, weight: profile.lane === 1 ? 1.25 : 1 }))),
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
  const formationSuggestion = buildFormationSuggestion(all120, finishProbabilities, chaosValue, top6Coverage);
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
    formationReason: formationSuggestion?.reason || null,
    wide_formation_suggestion: formationSuggestion,
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
