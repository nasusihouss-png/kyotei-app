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

const VENUE_FINISH_PATTERN_CORRECTIONS = Object.freeze({
  24: Object.freeze({
    name: "Omura",
    lane1FirstBoost: 0.11,
    lane1SecondBoost: 0.15,
    lane2SecondBoost: 0.08,
    lane3SecondBoost: 0.05,
    lane4ThirdBoost: 0.12,
    centerAttackFirstBoost: -0.02,
    outerPenalty: 0.12,
    optionalActivationBias: -0.04,
    preserveCombos: ["1-2-4", "1-3-4", "2-1-3", "3-1-2"],
    venueFitReason: "Omura correction keeps lane 1 in the race and protects the 1-2-4 / 1-3-4 near-tie shape."
  }),
  18: Object.freeze({
    name: "Tokuyama",
    lane1FirstBoost: 0.05,
    lane1SecondBoost: 0.07,
    lane2SecondBoost: 0.08,
    lane3SecondBoost: 0.09,
    lane4ThirdBoost: 0.1,
    centerAttackFirstBoost: 0.05,
    outerPenalty: 0.08,
    optionalActivationBias: 0.015,
    preserveCombos: ["1-2-4", "1-3-4", "3-1-2"],
    venueFitReason: "Tokuyama correction keeps the 1-2 / 1-3 base while preserving center attack pressure."
  }),
  21: Object.freeze({
    name: "Ashiya",
    lane1FirstBoost: 0.03,
    lane1SecondBoost: 0.05,
    lane2SecondBoost: 0.06,
    lane3SecondBoost: 0.07,
    lane4ThirdBoost: 0.08,
    centerAttackFirstBoost: 0.04,
    outerPenalty: 0.06,
    optionalActivationBias: 0.03,
    preserveCombos: ["1-2-4", "1-3-4", "3-1-2", "2-1-3"],
    venueFitReason: "Ashiya correction allows medium chaos but still organizes the 2/3/4 second cluster first."
  }),
  13: Object.freeze({
    name: "Amagasaki",
    lane1FirstBoost: 0.1,
    lane1SecondBoost: 0.13,
    lane2SecondBoost: 0.09,
    lane3SecondBoost: 0.07,
    lane4ThirdBoost: 0.08,
    centerAttackFirstBoost: -0.01,
    outerPenalty: 0.1,
    optionalActivationBias: -0.03,
    preserveCombos: ["1-2-4", "1-3-4", "2-1-3"],
    venueFitReason: "Amagasaki correction leans on lane 1 motor/ST strength and rescues lane 1 remain patterns."
  }),
  5: Object.freeze({
    name: "Tamagawa",
    lane1FirstBoost: 0.01,
    lane1SecondBoost: 0.1,
    lane2SecondBoost: 0.07,
    lane3SecondBoost: 0.1,
    lane4ThirdBoost: 0.07,
    centerAttackFirstBoost: 0.07,
    outerPenalty: 0.03,
    optionalActivationBias: 0.06,
    preserveCombos: ["1-2-4", "1-3-4", "3-1-2", "2-1-3"],
    venueFitReason: "Tamagawa correction keeps medium-chaos center pressure while tidying the 2/3 second cluster."
  }),
  12: Object.freeze({
    name: "Suminoe",
    lane1FirstBoost: 0.04,
    lane1SecondBoost: 0.11,
    lane2SecondBoost: 0.05,
    lane3SecondBoost: 0.1,
    lane4ThirdBoost: 0.11,
    centerAttackFirstBoost: 0.08,
    outerPenalty: 0.06,
    optionalActivationBias: 0.03,
    preserveCombos: ["1-2-4", "1-3-4", "3-1-2", "2-1-3"],
    venueFitReason: "Suminoe correction shows the center attack path while preserving lane 1 second-place remain lines."
  })
});

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

function getVenueFinishPatternCorrection(venueContext = {}) {
  const venueId = Number(venueContext?.venue_id || venueContext?.venueId || 0);
  return VENUE_FINISH_PATTERN_CORRECTIONS[venueId] || null;
}

function isInnerLaneStrongEnough(profile = {}) {
  const lane1st = toNum(profile?.lane1stRate, 0);
  const lane2ren = toNum(profile?.lane2RenRate, 0);
  const lane3ren = toNum(profile?.lane3RenRate, 0);
  const styleScore = toNum(profile?.styleScore ?? profile?.styleScores?.[profile?.style?.code], 0);
  const motorReadiness = toNum(buildMotorStReadiness(profile), 0);
  return lane1st >= 48 || lane2ren >= 60 || lane3ren >= 70 || styleScore >= 58 || motorReadiness >= 56;
}

function applyVenueFinishPatternCorrectionToCombos(
  rows = [],
  {
    venueContext = {},
    boat1Profile = {},
    boat1SecondKeep = { score: 0 },
    headProbMap = {},
    nearTieDiagnostics = {}
  } = {}
) {
  const correction = getVenueFinishPatternCorrection(venueContext);
  if (!correction) return Array.isArray(rows) ? rows : [];

  const boat1KeepScore = toNum(boat1SecondKeep?.score, 0);
  const lane1HeadShare = toNum(headProbMap?.[1], 0);
  const weakLane1HeadLock =
    lane1HeadShare <= 0.58 ||
    nearTieDiagnostics?.near_tie_candidate_count >= 2 ||
    nearTieDiagnostics?.top_two_tied === true;
  const lane1RemainReady =
    boat1KeepScore >= 56 &&
    isInnerLaneStrongEnough(boat1Profile) &&
    toNum(venueContext?.one_course_trust, 0) >= 56;
  const preserved = new Set(correction.preserveCombos || []);

  const corrected = (Array.isArray(rows) ? rows : []).map((row) => {
    const combo = String(row?.combo || "");
    const probability = Number(row?.probability) || 0;
    const [first, second, third] = combo.split("-").map((value) => Number(value));
    if (!Number.isInteger(first) || !Number.isInteger(second) || !Number.isInteger(third)) {
      return { ...row, probability };
    }

    let multiplier = 1;
    if (first === 1) multiplier *= 1 + correction.lane1FirstBoost;
    if (second === 1 && lane1RemainReady && weakLane1HeadLock) {
      multiplier *= 1 + correction.lane1SecondBoost;
    }
    if (first === 1 && second === 2) multiplier *= 1 + correction.lane2SecondBoost;
    if (first === 1 && second === 3) multiplier *= 1 + correction.lane3SecondBoost;
    if (first === 1 && [2, 3].includes(second) && third === 4) {
      multiplier *= 1 + correction.lane4ThirdBoost;
    }
    if ([3, 4].includes(first)) multiplier *= 1 + correction.centerAttackFirstBoost;
    if (first >= 5) multiplier *= 1 - correction.outerPenalty;
    if (second >= 5) multiplier *= 1 - correction.outerPenalty * 0.7;
    if (preserved.has(combo) && nearTieDiagnostics?.near_tie_candidate_count >= 2) {
      multiplier *= 1.08;
    }

    return {
      ...row,
      probability: probability * Math.max(0.6, multiplier)
    };
  });

  const normalized = normalizeMap(
    Object.fromEntries(corrected.map((row) => [row.combo, row.probability]))
  );
  return Object.entries(normalized)
    .map(([combo, probability]) => ({ combo, probability: round(probability, 4) }))
    .sort((a, b) => b.probability - a.probability);
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

function buildBoat1SecondKeepEnhanced(profile = {}, venueContext = {}, raceContext = {}) {
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

function calibrateSecondGivenHeadOneEnhanced(baseSecondMap = {}, profiles = [], venueContext = {}, boat1SecondKeep = { score: 0 }) {
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

function buildBoat1SecondKeep(profile = {}, venueContext = {}, raceContext = {}) {
  const correction = getVenueFinishPatternCorrection(venueContext);
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
        { value: profile.stability, weight: 0.08 },
        { value: correction ? 50 + correction.lane1SecondBoost * 100 : null, weight: 0.08 }
      ]) || 0
    ),
    1
  );
  const reasons = [];
  if ((invertNormalize(profile.avgSt, 0.11, 0.24) || 0) >= 0.45) reasons.push("boat 1 ST is stable enough to remain in the trifecta");
  if ((motorStReadiness || 0) >= 52) reasons.push("boat 1 motor and ST readiness are above the remain threshold");
  if (toNum(venueContext?.one_course_trust, 0) >= 64) reasons.push("venue profile favors inside boats staying in the race");
  if ((antiCollapse || 0) >= 52) reasons.push("lane 3 / lane 4 pressure is not strong enough to erase boat 1");
  if (styleFit >= 58) reasons.push("boat 1 style profile fits an inside hold or remain scenario");
  if (correction?.lane1SecondBoost > 0.09) reasons.push("venue correction explicitly boosts lane 1 second-place remain lines");
  return {
    score: keepScore,
    reason: reasons.length > 0 ? reasons.join(" / ") : "boat1 second keep is neutral"
  };
}

function calibrateSecondGivenHeadOne(baseSecondMap = {}, profiles = [], venueContext = {}, boat1SecondKeep = { score: 0 }) {
  const correction = getVenueFinishPatternCorrection(venueContext);
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
    const correctionAdjustment =
      lane === 2
        ? 1 + toNum(correction?.lane2SecondBoost, 0) * 0.7
        : lane === 3
          ? 1 + toNum(correction?.lane3SecondBoost, 0) * 0.7
          : lane === 4
            ? 1 + toNum(correction?.lane4ThirdBoost, 0) * 0.25
            : 1 - toNum(correction?.outerPenalty, 0) * 0.45;
    const boat1KeepTransfer =
      lane === 2
        ? 1 + Math.max(0, (toNum(boat1SecondKeep?.score, 0) - 50)) * 0.0028
        : lane === 4
          ? 1 + Math.max(0, (toNum(boat1SecondKeep?.score, 0) - 50)) * 0.0016
          : lane === 3
            ? 1 + Math.max(0, (toNum(boat1SecondKeep?.score, 0) - 50)) * 0.0006
            : 1 - Math.max(0, toNum(correction?.outerPenalty, 0)) * 0.08;
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
        correctionAdjustment *
        boat1KeepTransfer *
        (1 + Math.max(-0.18, ((shapeSupport || 50) - 50) / 180))
    );
  }
  return normalizeMap(calibratedRaw);
}

function scoreBlend(values = []) {
  const blended = weightedAverage(values);
  return blended === null ? null : clamp(0, 100, blended);
}

function buildRemainScores({
  profiles = [],
  venueContext = {},
  raceContext = {},
  secondGivenHeadProbabilities = {},
  boat1SecondKeep = { score: 0 }
} = {}) {
  const laneProfiles = new Map((Array.isArray(profiles) ? profiles : []).map((profile) => [profile.lane, profile]));
  const lane2 = laneProfiles.get(2) || {};
  const lane3 = laneProfiles.get(3) || {};
  const lane4 = laneProfiles.get(4) || {};
  const lane2Score = scoreBlend([
    { value: (Number(secondGivenHeadProbabilities?.[2]) || 0) * 100, weight: 0.28 },
    { value: lane2.styleScores?.sashi, weight: 0.2 },
    { value: lane2.supportRate, weight: 0.18 },
    { value: lane2.course2Rate, weight: 0.14 },
    { value: getVenueLaneBiasScore(venueContext, 2), weight: 0.1 },
    { value: toNum(venueContext?.two_course_sashi_remain_rate, 50), weight: 0.06 },
    { value: toNum(boat1SecondKeep?.score, 50), weight: 0.04 }
  ]);
  const lane3Score = scoreBlend([
    { value: (Number(secondGivenHeadProbabilities?.[3]) || 0) * 100, weight: 0.2 },
    { value: lane3.styleScores?.makuri, weight: 0.2 },
    { value: lane3.styleScores?.makurisashi, weight: 0.16 },
    { value: lane3.attackRate, weight: 0.18 },
    { value: lane3.course3Rate, weight: 0.1 },
    { value: toNum(venueContext?.three_course_attack_success_rate, 50), weight: 0.1 },
    { value: invertNormalize(lane3.avgSt, 0.11, 0.24) === null ? null : invertNormalize(lane3.avgSt, 0.11, 0.24) * 100, weight: 0.06 }
  ]);
  const lane4Score = scoreBlend([
    { value: (Number(secondGivenHeadProbabilities?.[4]) || 0) * 100, weight: 0.22 },
    { value: lane4.styleScores?.makurisashi, weight: 0.22 },
    { value: lane4.supportRate, weight: 0.16 },
    { value: lane4.breakoutRate, weight: 0.12 },
    { value: lane4.course3Rate, weight: 0.1 },
    { value: toNum(venueContext?.four_course_develop_sashi_rate, 50), weight: 0.1 },
    { value: toNum(raceContext?.lane4Breakout, 50), weight: 0.08 }
  ]);
  return {
    lane2_sashi_keep_score: round(lane2Score, 1),
    lane3_attack_keep_score: round(lane3Score, 1),
    lane4_tenkaisashi_score: round(lane4Score, 1)
  };
}

function buildRacePatternSummary({
  boat1HeadProbability = 0,
  remainScores = {},
  chaosValue = 0,
  outsideRiskProxy = 0,
  venueContext = {}
} = {}) {
  const lane2Score = toNum(remainScores?.lane2_sashi_keep_score, 0);
  const lane3Score = toNum(remainScores?.lane3_attack_keep_score, 0);
  const lane4Score = toNum(remainScores?.lane4_tenkaisashi_score, 0);
  if (chaosValue >= 0.68 || outsideRiskProxy >= 0.58) {
    return { racePattern: "chaotic_spread", racePatternScore: round(Math.max(chaosValue, outsideRiskProxy) * 100, 1) };
  }
  if (outsideRiskProxy >= 0.46 && toNum(venueContext?.venue_outer_3rd_bias, 0) >= 8) {
    return {
      racePattern: "outer_3rd_invasion",
      racePatternScore: round(scoreBlend([
        { value: outsideRiskProxy * 100, weight: 0.56 },
        { value: toNum(venueContext?.venue_outer_3rd_bias, 50), weight: 0.24 },
        { value: toNum(venueContext?.lane56_renyuu_intrusion_rate, 50), weight: 0.2 }
      ]), 1)
    };
  }
  if (boat1HeadProbability >= 0.54 && lane2Score >= 58 && lane2Score >= lane3Score - 1) {
    return {
      racePattern: "escape_stable",
      racePatternScore: round(scoreBlend([
        { value: boat1HeadProbability * 100, weight: 0.46 },
        { value: lane2Score, weight: 0.18 },
        { value: toNum(venueContext?.one_course_trust, 50), weight: 0.24 },
        { value: (1 - chaosValue) * 100, weight: 0.12 }
      ]), 1)
    };
  }
  if (lane2Score >= lane3Score + 3 && lane2Score >= lane4Score + 2) {
    return { racePattern: "sashi_keep", racePatternScore: round(lane2Score, 1) };
  }
  if (lane3Score >= lane2Score - 1 && lane3Score >= lane4Score + 1) {
    return { racePattern: "attack_keep", racePatternScore: round(lane3Score, 1) };
  }
  if (lane4Score >= 54) {
    return { racePattern: "tenkai_sashi", racePatternScore: round(lane4Score, 1) };
  }
  return {
    racePattern: boat1HeadProbability >= 0.5 ? "escape_stable" : "chaotic_spread",
    racePatternScore: round(scoreBlend([
      { value: boat1HeadProbability * 100, weight: 0.52 },
      { value: (1 - chaosValue) * 100, weight: 0.28 },
      { value: lane2Score, weight: 0.2 }
    ]), 1)
  };
}

function buildPressureIntentSummary({
  raceContext = {},
  venueContext = {},
  chaosValue = 0,
  boat1SecondKeep = { score: 0 },
  remainScores = {}
} = {}) {
  const attackIntentScore = scoreBlend([
    { value: toNum(raceContext?.lane3Makuri, 50), weight: 0.28 },
    { value: toNum(raceContext?.lane4Breakout, 50), weight: 0.24 },
    { value: toNum(raceContext?.outsideHeadPressure, 50), weight: 0.12 },
    { value: toNum(remainScores?.lane3_attack_keep_score, 50), weight: 0.2 },
    { value: toNum(remainScores?.lane4_tenkaisashi_score, 50), weight: 0.1 },
    { value: toNum(venueContext?.volatility_boost, 0) * 6 + 40, weight: 0.06 }
  ]);
  const safeRunBias = scoreBlend([
    { value: toNum(boat1SecondKeep?.score, 50), weight: 0.34 },
    { value: toNum(remainScores?.lane2_sashi_keep_score, 50), weight: 0.18 },
    { value: toNum(venueContext?.one_course_trust, 50), weight: 0.26 },
    { value: (1 - chaosValue) * 100, weight: 0.14 },
    { value: 100 - toNum(raceContext?.outsideHeadPressure, 50), weight: 0.08 }
  ]);
  const pressureMode =
    chaosValue >= 0.62 || attackIntentScore >= 63
      ? "attack_pressure"
      : safeRunBias >= 62
        ? "safe_control"
        : "balanced";
  return {
    pressure_mode: pressureMode,
    attack_intent_score: round(attackIntentScore, 1),
    safe_run_bias: round(safeRunBias, 1)
  };
}

function buildConfidenceSummary({
  top6Coverage = 0,
  chaosValue = 0,
  top6ScenarioScore = 50,
  outsideRiskProxy = 0,
  strongestHead = 0,
  nearTieDiagnostics = {},
  venueContext = {},
  racePatternScore = 50
} = {}) {
  const clusteringPenalty = nearTieDiagnostics?.near_tie_candidate_count >= 3 ? 46 : nearTieDiagnostics?.near_tie_candidate_count >= 2 ? 58 : 70;
  const predictionStabilityScore = scoreBlend([
    { value: top6Coverage * 100, weight: 0.32 },
    { value: (1 - chaosValue) * 100, weight: 0.22 },
    { value: toNum(top6ScenarioScore, 50), weight: 0.2 },
    { value: (1 - outsideRiskProxy) * 100, weight: 0.12 },
    { value: clusteringPenalty, weight: 0.06 },
    { value: toNum(venueContext?.one_course_trust, 50), weight: 0.08 }
  ]);
  const confidenceScore = scoreBlend([
    { value: predictionStabilityScore, weight: 0.42 },
    { value: strongestHead * 100, weight: 0.16 },
    { value: top6Coverage * 100, weight: 0.18 },
    { value: toNum(top6ScenarioScore, 50), weight: 0.14 },
    { value: toNum(racePatternScore, 50), weight: 0.1 }
  ]);
  const confidenceBand = confidenceScore >= 72 ? "high" : confidenceScore >= 54 ? "medium" : "low";
  const reasons = [];
  if (predictionStabilityScore >= 70) reasons.push("prediction stability is high");
  else if (predictionStabilityScore <= 46) reasons.push("prediction stability is weak");
  if (nearTieDiagnostics?.near_tie_candidate_count >= 2) reasons.push("near-tie second-place structure needs wider coverage");
  if (top6Coverage >= 0.24) reasons.push("top6 coverage is healthy");
  if (outsideRiskProxy >= 0.42) reasons.push("outside break risk is elevated");
  return {
    confidence_band: confidenceBand,
    confidence_score: round(confidenceScore, 1),
    prediction_stability_score: round(predictionStabilityScore, 1),
    buy_confidence_reason: reasons.join("; ") || "confidence is neutral"
  };
}

function buildPreliminaryBetMode({
  confidenceBand = "medium",
  confidenceScore = 50,
  predictionStabilityScore = 50,
  optionalFormation = null,
  optionalFormationActive = false,
  top6Coverage = 0,
  chaosValue = 0,
  venueContext = {}
} = {}) {
  const venueName = String(venueContext?.venue_name || venueContext?.venueBiasProfile?.venue_name || "");
  const buyPolicyCode = String(venueContext?.buyPolicy?.code || "");
  const isOmura = venueName === "Omura";
  const isAshiya = venueName === "Ashiya";
  const isTokuyama = venueName === "Tokuyama";
  const isAmagasaki = venueName === "Amagasaki";
  const isTamagawa = venueName === "Tamagawa";
  const isSuminoe = venueName === "Suminoe";
  const triggerFlags = optionalFormation?.trigger_flags && typeof optionalFormation.trigger_flags === "object"
    ? optionalFormation.trigger_flags
    : {};
  const strictOptionalReady =
    optionalFormationActive &&
    triggerFlags.low_top6_coverage === true &&
    triggerFlags.near_tie_second_234 === true &&
    triggerFlags.strong_near_tie_second_234 === true &&
    triggerFlags.rescue_evidence_strong === true &&
    triggerFlags.top_head_not_runaway === true &&
    triggerFlags.venue_allows_optional === true &&
    triggerFlags.enough_top6_coverage !== true &&
    triggerFlags.hard_inside_shape !== true &&
    triggerFlags.clean_inside_order !== true &&
    triggerFlags.ashiya_strict_gate !== false &&
    triggerFlags.tokuyama_strict_gate !== false;
  const optionalRescueEvidence =
    triggerFlags.rescue_evidence_strong === true ||
    triggerFlags.strong_near_tie_second_234 === true;
  const top6CoverageFloor =
    isOmura ? 0.12
      : isAmagasaki ? 0.135
        : isAshiya ? 0.155
          : isTokuyama ? 0.15
            : isTamagawa ? 0.148
              : isSuminoe ? 0.15
                : 0.145;
  const stableCoverageFloor =
    isOmura ? 0.152
      : isAmagasaki ? 0.158
        : isAshiya ? 0.18
          : isTokuyama ? 0.17
            : isTamagawa ? 0.165
              : isSuminoe ? 0.165
                : 0.18;
  const top6ConfidenceFloor =
    isOmura ? 47
      : isAmagasaki ? 49
        : isAshiya ? 52
          : isTokuyama ? 51
            : isTamagawa ? 50
              : isSuminoe ? 50
                : 50;
  const stabilityFloor =
    isOmura ? 48
      : isAmagasaki ? 50
        : isAshiya ? 54
          : isTokuyama ? 53
            : isTamagawa ? 52
              : isSuminoe ? 52
                : 52;
  const optionalConfidenceFloor =
    isAshiya ? 58
      : isTokuyama ? 57
        : isTamagawa ? 56
          : isSuminoe ? 55
            : isOmura || isAmagasaki ? 999
              : 55;
  const optionalCoverageCeiling =
    isAshiya ? 0.135
      : isTokuyama ? 0.148
        : isTamagawa ? 0.155
          : isSuminoe ? 0.158
            : 0.16;
  const stableTop6Buy =
    top6Coverage >= stableCoverageFloor &&
    (
      (confidenceBand === "high" && predictionStabilityScore >= Math.max(60, stabilityFloor + 8)) ||
      (confidenceScore >= Math.max(54, top6ConfidenceFloor + 2) && predictionStabilityScore >= Math.max(56, stabilityFloor + 2))
    );
  const borderlineTop6Buy =
    !optionalFormationActive &&
    top6Coverage >= top6CoverageFloor &&
    confidenceScore >= top6ConfidenceFloor &&
    predictionStabilityScore >= stabilityFloor;
  const insideFocusedTop6Buy =
    (isOmura || isAmagasaki) &&
    !optionalFormationActive &&
    (
      triggerFlags.hard_inside_shape === true ||
      triggerFlags.clean_inside_order === true ||
      buyPolicyCode === "inside_head_focus"
    ) &&
    top6Coverage >= top6CoverageFloor &&
    confidenceScore >= top6ConfidenceFloor &&
    predictionStabilityScore >= stabilityFloor;
  if (confidenceBand === "high" && predictionStabilityScore >= 70 && top6Coverage >= 0.22) {
    return {
      recommendedBetMode: "buy_top6",
      skipRiskReason: null,
      modeCalibrationReason: `${venueName || "default"} calibration keeps this race in buy_top6 because coverage and stability are already strong.`
    };
  }
  if (
    confidenceScore >= optionalConfidenceFloor &&
    strictOptionalReady &&
    optionalRescueEvidence &&
    (chaosValue >= 0.45 || top6Coverage <= optionalCoverageCeiling)
  ) {
    return {
      recommendedBetMode: "buy_top6_plus_optional16",
      skipRiskReason: null,
      modeCalibrationReason:
        isAshiya
          ? "Ashiya calibration allows optional16 only because coverage is very thin and the 2/3/4 near-tie rescue signal is unusually clear."
          : isTokuyama
            ? "Tokuyama calibration allows optional16 because low coverage and 1-2-4 / 1-3-4 rescue evidence are both strong."
            : `${venueName || "default"} calibration allows optional16 because low coverage and near-tie rescue evidence are both present.`
    };
  }
  if (insideFocusedTop6Buy || stableTop6Buy || borderlineTop6Buy) {
    return {
      recommendedBetMode: "buy_top6",
      skipRiskReason: null,
      modeCalibrationReason:
        isOmura
          ? "Omura calibration lifts this race to buy_top6 because the inside-head remain shape is organized enough without optional16."
          : isAshiya
            ? "Ashiya calibration keeps this at buy_top6 because optional16 evidence is not strong enough to justify widening."
            : isTokuyama
              ? "Tokuyama calibration keeps this at buy_top6 because the 3-4 attack shape is readable without optional16 expansion."
              : `${venueName || "default"} calibration keeps this race in buy_top6 because top6 evidence is sufficient without widening.`
    };
  }
  const skipReason =
    top6Coverage < top6CoverageFloor
      ? `${venueName || "This venue"} calibration still sees top6 coverage as too thin without optional16 support`
      : confidenceScore < top6ConfidenceFloor
        ? `${venueName || "This venue"} calibration still sees confidence as too weak for buy_top6`
        : `${venueName || "This venue"} calibration still sees race shape as too unstable for buy_top6`;
  return {
    recommendedBetMode: "skip",
    skipRiskReason: skipReason,
    modeCalibrationReason:
      isOmura
        ? "Omura calibration suppresses optional16 but still skips races when the inside-head read does not clear the minimum coverage floor."
        : isAshiya
          ? "Ashiya calibration now withholds optional16 unless the tie signal is exceptional, so weaker races fall back to skip."
          : isTokuyama
            ? "Tokuyama calibration now needs both attack pressure and rescue evidence, so weaker races remain skip."
            : `${venueName || "default"} calibration leaves this race on skip because venue-specific buy evidence is not strong enough.`
  };
}

function buildNearTieSecondDiagnosticsEnhanced({
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

function preserveCloseSecondCombosEnhanced(topRows = [], allRows = [], diagnostics = {}) {
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

function buildFormationSuggestionEnhanced(sortedCombos = [], finishProbabilities = {}, chaosLevel = 0, top6Coverage = 0, venueContext = {}, diagnostics = {}) {
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

function buildNearTieSecondDiagnostics({
  all120 = [],
  secondGivenHeadProbabilities = {},
  top6Coverage = 0,
  chaosLevel = 0,
  venueContext = {},
  outsideRiskProxy = 0
} = {}) {
  const correction = getVenueFinishPatternCorrection(venueContext);
  const topSecondCandidates = [2, 3, 4, 5, 6]
    .map((lane) => ({ lane, probability: Number(secondGivenHeadProbabilities?.[lane]) || 0 }))
    .sort((a, b) => b.probability - a.probability);
  const lane2 = topSecondCandidates.find((row) => row.lane === 2) || { lane: 2, probability: 0 };
  const lane3 = topSecondCandidates.find((row) => row.lane === 3) || { lane: 3, probability: 0 };
  const lane4 = topSecondCandidates.find((row) => row.lane === 4) || { lane: 4, probability: 0 };
  const topGap = Math.abs((topSecondCandidates[0]?.probability || 0) - (topSecondCandidates[1]?.probability || 0));
  const secondGap23 = Math.abs((lane2?.probability || 0) - (lane3?.probability || 0));
  const clusterGap34 = Math.abs((lane3?.probability || 0) - (lane4?.probability || 0));
  const combo124 = (Array.isArray(all120) ? all120 : []).find((row) => row.combo === "1-2-4")?.probability || 0;
  const combo134 = (Array.isArray(all120) ? all120 : []).find((row) => row.combo === "1-3-4")?.probability || 0;
  const combo213 = (Array.isArray(all120) ? all120 : []).find((row) => row.combo === "2-1-3")?.probability || 0;
  const combo312 = (Array.isArray(all120) ? all120 : []).find((row) => row.combo === "3-1-2")?.probability || 0;
  const comboGap = Math.abs(combo124 - combo134);
  const comboGapScore = round(
    clamp(0, 1, 1 - (comboGap / Math.max(0.0001, Math.max(combo124, combo134, 0.0001)))),
    4
  );
  const topTwoTieThreshold = 0.075 + Math.max(0, toNum(correction?.optionalActivationBias, 0)) * 0.08;
  const secondThirdTieThreshold = 0.058 + Math.max(0, toNum(correction?.optionalActivationBias, 0)) * 0.05;
  const topTwoTied = topGap <= topTwoTieThreshold;
  const secondThirdTied =
    topSecondCandidates.length >= 3 &&
    Math.abs((topSecondCandidates[1]?.probability || 0) - (topSecondCandidates[2]?.probability || 0)) <= secondThirdTieThreshold;
  const nearTieCandidates = topSecondCandidates.filter((row, index) => {
    if (index === 0) return true;
    if (index === 1) return Math.abs((topSecondCandidates[0]?.probability || 0) - row.probability) <= topTwoTieThreshold;
    if (index === 2) {
      return secondThirdTied || Math.abs((topSecondCandidates[1]?.probability || 0) - row.probability) <= secondThirdTieThreshold;
    }
    return false;
  });
  const lowCoverage = top6Coverage <= 0.16;
  const volatileVenue = toNum(venueContext?.volatility_boost, 0) >= 6;
  const preserve =
    (
      comboGapScore >= 0.72 &&
      secondGap23 <= 0.07 &&
      (lowCoverage || chaosLevel >= 0.55 || outsideRiskProxy >= 0.38 || volatileVenue)
    ) ||
    (
      nearTieCandidates.length >= 3 &&
      (clusterGap34 <= 0.05 || combo213 >= 0.006 || combo312 >= 0.006)
    );

  return {
    near_tie_second_candidates: nearTieCandidates,
    close_combo_preserved: preserve,
    combo_gap_score: comboGapScore,
    combo_gap: round(comboGap, 4),
    combo_124_probability: round(combo124, 4),
    combo_134_probability: round(combo134, 4),
    combo_213_probability: round(combo213, 4),
    combo_312_probability: round(combo312, 4),
    second_gap_2_vs_3: round(secondGap23, 4),
    cluster_gap_3_vs_4: round(clusterGap34, 4),
    top_second_gap: round(topGap, 4),
    top_two_tied: topTwoTied,
    second_third_tied: secondThirdTied,
    near_tie_candidate_count: nearTieCandidates.length,
    top_two_tie_threshold: topTwoTieThreshold,
    second_third_tie_threshold: secondThirdTieThreshold,
    venue_fit_reason: correction?.venueFitReason || null,
    preserve_combos: correction?.preserveCombos || ["1-2-4", "1-3-4"]
  };
}

function preserveCloseSecondCombos(topRows = [], allRows = [], diagnostics = {}) {
  if (!diagnostics?.close_combo_preserved) {
    return Array.isArray(topRows) ? topRows : [];
  }
  const current = [...(Array.isArray(topRows) ? topRows : [])];
  const byCombo = new Map((Array.isArray(allRows) ? allRows : []).map((row) => [row.combo, row]));
  const priorityCombos = Array.isArray(diagnostics?.preserve_combos) && diagnostics.preserve_combos.length > 0
    ? diagnostics.preserve_combos
    : diagnostics?.second_third_tied || diagnostics?.near_tie_candidate_count >= 3
      ? ["1-2-4", "1-3-4", "1-2-3", "1-3-2", "1-4-2", "1-4-3", "2-1-3", "3-1-2"]
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

export function buildFormationSuggestion(sortedCombos = [], finishProbabilities = {}, chaosLevel = 0, top6Coverage = 0, venueContext = {}, diagnostics = {}) {
  const correction = getVenueFinishPatternCorrection(venueContext);
  const venueName = String(venueContext?.venue_name || "");
  const isOmura = venueName === "Omura";
  const isAshiya = venueName === "Ashiya";
  const isTokuyama = venueName === "Tokuyama";
  const optionalFormationBoost = toNum(venueContext?.optional_formation_trigger_boost, 0) + toNum(correction?.optionalActivationBias, 0) * 100;
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
  const nearTieCluster = toNum(diagnostics?.near_tie_candidate_count, 0) >= 2;
  const venueShapeFits = !!correction;
  const nearTieSecond234 =
    Array.isArray(diagnostics?.near_tie_second_candidates) &&
    diagnostics.near_tie_second_candidates.filter((row) => [2, 3, 4].includes(Number(row?.lane))).length >= 2;
  const strongNearTieSecond234 =
    nearTieSecond234 &&
    (
      toNum(diagnostics?.second_gap_2_vs_3, 1) <= 0.055 ||
      toNum(diagnostics?.cluster_gap_3_vs_4, 1) <= 0.05 ||
      toNum(diagnostics?.near_tie_candidate_count, 0) >= 3
    );
  const lane1RemainWindow =
    (toNum(diagnostics?.combo_213_probability, 0) >= 0.006 || toNum(diagnostics?.combo_312_probability, 0) >= 0.006) &&
    topHeadProbability <= 0.58;
  const rescueEvidenceStrong =
    diagnostics?.close_combo_preserved === true ||
    toNum(diagnostics?.combo_gap_score, 0) >= 0.76 ||
    lane1RemainWindow;
  const extremelyLowCoverage = top6Coverage <= 0.12 + Math.max(0, optionalFormationBoost) * 0.002;
  const lowCoverageWithChaos = chaosLevel >= Math.max(0.9, 1 - Math.max(0, optionalFormationBoost) * 0.02) && top6Coverage <= 0.15 + Math.max(0, optionalFormationBoost) * 0.003;
  const lowCoverage = top6Coverage <= 0.18 + Math.max(0, optionalFormationBoost) * 0.0015;
  const enoughCoverage = top6Coverage >= 0.2;
  const ashiyaStrictLowCoverage = top6Coverage <= 0.132;
  const tokuyamaStrictLowCoverage = top6Coverage <= 0.148;
  const ashiyaMultiTieReady =
    toNum(diagnostics?.near_tie_candidate_count, 0) >= 3 &&
    toNum(diagnostics?.second_gap_2_vs_3, 1) <= 0.045 &&
    toNum(diagnostics?.cluster_gap_3_vs_4, 1) <= 0.047;
  const tokuyamaAttackRescueReady =
    (
      toNum(diagnostics?.combo_124_probability, 0) >= 0.01 ||
      toNum(diagnostics?.combo_134_probability, 0) >= 0.01
    ) &&
    toNum(diagnostics?.combo_gap_score, 0) >= 0.79;
  const weakHeadDominance = topHeadProbability <= 0.52;
  const topHeadNotRunaway = topHeadProbability <= 0.6 || headGap <= 0.12;
  const weakHeadAxis = topHeadProbability <= 0.57 || headGap <= 0.09;
  const hardInsideShape =
    (venueName === "Omura" || venueName === "Amagasaki") &&
    topHeadProbability >= 0.57 &&
    topHeadNotRunaway === false;
  const cleanInsideOrder =
    topHeadProbability >= 0.55 &&
    !nearTieCluster &&
    toNum(diagnostics?.combo_gap_score, 0) < 0.68;
  const multipleViableHeads = secondHeadProbability >= 0.18 || Math.abs(topHeadProbability - secondHeadProbability) <= 0.1;
  const outerInvolvementElevated = outerHeadShare >= 0.34;
  const highChaos = chaosLevel >= Math.max(0.3, 0.39 - Math.max(0, optionalFormationBoost) * 0.01);
  const mediumOutsideRisk = outsideBreakRiskProxy >= Math.max(0.32, 0.42 - Math.max(0, optionalFormationBoost) * 0.008);
  const elevatedOutsideBreakRisk = outsideBreakRiskProxy >= Math.max(0.38, 0.48 - Math.max(0, optionalFormationBoost) * 0.008);
  const venueAllowsOptional = venueShapeFits && toNum(correction?.optionalActivationBias, 0) > -0.02;
  const ashiyaStrictGate =
    !isAshiya ||
    (
      ashiyaStrictLowCoverage &&
      ashiyaMultiTieReady &&
      strongNearTieSecond234 &&
      rescueEvidenceStrong &&
      weakHeadAxis &&
      topHeadProbability <= 0.55 &&
      toNum(diagnostics?.combo_gap_score, 0) >= 0.82
    );
  const tokuyamaStrictGate =
    !isTokuyama ||
    (
      tokuyamaStrictLowCoverage &&
      strongNearTieSecond234 &&
      rescueEvidenceStrong &&
      weakHeadAxis &&
      tokuyamaAttackRescueReady
    );
  const venueTriggeredOptional =
    lowCoverage &&
    nearTieCluster &&
    nearTieSecond234 &&
    rescueEvidenceStrong &&
    topHeadNotRunaway &&
    venueAllowsOptional &&
    ashiyaStrictGate &&
    tokuyamaStrictGate;
  const shouldSuppressOptional =
    enoughCoverage ||
    hardInsideShape ||
    cleanInsideOrder ||
    !nearTieCluster ||
    !nearTieSecond234 ||
    !rescueEvidenceStrong ||
    !topHeadNotRunaway ||
    !venueShapeFits ||
    !venueAllowsOptional ||
    !ashiyaStrictGate ||
    !tokuyamaStrictGate;
  const shouldSuggest =
    !shouldSuppressOptional &&
    (
      extremelyLowCoverage ||
      lowCoverageWithChaos ||
      venueTriggeredOptional ||
      (lowCoverage && lane1RemainWindow && nearTieSecond234) ||
      (lowCoverage && multipleViableHeads && rescueEvidenceStrong) ||
      (lowCoverage && highChaos && mediumOutsideRisk)
    );
  const reasons = [];
  const suppressionReasons = [];
  if (extremelyLowCoverage) reasons.push("top6 coverage is extremely low");
  else if (lowCoverageWithChaos) reasons.push("top6 coverage is low under high chaos");
  else if (lowCoverage) reasons.push("top6 coverage is low");
  if (venueTriggeredOptional) reasons.push("venue-specific near-tie second cluster is active");
  if (lane1RemainWindow) reasons.push("lane 1 second-place remain window is open");
  if (multipleViableHeads || weakHeadDominance || topHeadNotRunaway) reasons.push("top head candidate is not dominant");
  if (outerInvolvementElevated) reasons.push("outer lanes have increased involvement risk");
  if (highChaos) reasons.push("chaos level is high");
  if (elevatedOutsideBreakRisk || mediumOutsideRisk) reasons.push("outside break risk is elevated");
  if (diagnostics?.close_combo_preserved) reasons.push("1-2-4 / 1-3-4 preservation is active");
  if (diagnostics?.venue_fit_reason) reasons.push(diagnostics.venue_fit_reason);
  if (enoughCoverage) suppressionReasons.push("top6 coverage is already sufficient");
  if (hardInsideShape) suppressionReasons.push("inside-favored venue still points to a strong lane 1 remain shape");
  if (cleanInsideOrder) suppressionReasons.push("1-2 / 1-3 ordering is already organized inside top6");
  if (!nearTieCluster || !nearTieSecond234) suppressionReasons.push("near-tie second cluster around 2/3/4 is not strong enough");
  if (!rescueEvidenceStrong) suppressionReasons.push("rescue evidence for 1-2-4 / 1-3-4 is not strong enough");
  if (!topHeadNotRunaway) suppressionReasons.push("head candidate is too dominant to justify optional expansion");
  if (!venueAllowsOptional && venueShapeFits) suppressionReasons.push("venue correction does not currently permit optional16 expansion");
  if (isOmura) suppressionReasons.push("Omura calibration keeps optional16 heavily suppressed so strong inside-head races stay in top6 only.");
  if (isAshiya && !ashiyaStrictGate) suppressionReasons.push("Ashiya now requires very low coverage, a multi-lane 2/3/4 tie, and weak 1-head dominance before optional16 can activate");
  if (isTokuyama && !tokuyamaStrictGate) suppressionReasons.push("Tokuyama now requires both lower coverage and stronger 1-2-4 / 1-3-4 rescue evidence before optional16 can activate");

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
  const firstCandidates = firstEntries.map(([lane]) => Number(lane)).slice(0, firstCandidateLimit);
  if (venueName === "Omura" || venueName === "Amagasaki") {
    while (firstCandidates.length > 0 && firstCandidates[firstCandidates.length - 1] >= 5) firstCandidates.pop();
  }
  const secondCandidates = secondEntries.map(([lane]) => Number(lane)).slice(0, secondCandidateLimit);
  const thirdCandidates = thirdEntries.map(([lane]) => Number(lane)).slice(0, thirdCandidateLimit);
  if (nearTieCluster) {
    for (const row of diagnostics?.near_tie_second_candidates || []) {
      if (!secondCandidates.includes(row.lane)) secondCandidates.push(row.lane);
    }
  }
  if (lane1RemainWindow && !secondCandidates.includes(1)) secondCandidates.push(1);
  if (venueName === "Tokuyama" || venueName === "Suminoe") {
    for (const lane of [3, 4]) {
      if (!secondCandidates.includes(lane)) secondCandidates.push(lane);
      if (!thirdCandidates.includes(lane)) thirdCandidates.push(lane);
    }
  }
  if ((venueName === "Ashiya" || venueName === "Tamagawa") && !lane1RemainWindow) {
    for (const lane of [5, 6]) {
      if (!thirdCandidates.includes(lane)) thirdCandidates.push(lane);
    }
  }
  if (lane1RemainWindow) {
    for (const lane of [2, 3]) {
      if (!firstCandidates.includes(lane) && firstCandidates.length < firstCandidateLimit + 1) firstCandidates.push(lane);
    }
  }
  if (!shouldSuggest) {
    return {
      active: false,
      size: 0,
      combos: [],
      first_candidates: [...new Set(firstCandidates)].slice(0, firstCandidateLimit + (lane1RemainWindow ? 1 : 0)),
      second_candidates: [...new Set(secondCandidates)].slice(0, secondCandidateLimit + 1),
      third_candidates: [...new Set(thirdCandidates)].slice(0, thirdCandidateLimit + 1),
      formation_string: null,
      reason: suppressionReasons[0] || "top6 is sufficient",
      reasons: suppressionReasons,
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
        venue_triggered_optional: venueTriggeredOptional,
        venue_allows_optional: venueAllowsOptional,
        near_tie_second_234: nearTieSecond234,
        strong_near_tie_second_234: strongNearTieSecond234,
        rescue_evidence_strong: rescueEvidenceStrong,
        lane1_second_window: lane1RemainWindow,
        hard_inside_shape: hardInsideShape,
        clean_inside_order: cleanInsideOrder,
        enough_top6_coverage: enoughCoverage,
        ashiya_strict_gate: ashiyaStrictGate,
        tokuyama_strict_gate: tokuyamaStrictGate,
        close_combo_preserved: diagnostics?.close_combo_preserved === true,
        combo_gap_score: diagnostics?.combo_gap_score ?? null
      }
    };
  }

  const targetSize = Math.min(
    16,
    venueName === "Omura" || venueName === "Amagasaki"
      ? 14
      : venueName === "Tokuyama" || venueName === "Suminoe" || venueName === "Tamagawa" || venueName === "Ashiya"
        ? 16
        : highChaos || outerInvolvementElevated
          ? 16
          : 14
  );
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
    first_candidates: [...new Set(firstCandidates)].slice(0, firstCandidateLimit + (lane1RemainWindow ? 1 : 0)),
    second_candidates: [...new Set(secondCandidates)].slice(0, secondCandidateLimit + 1),
    third_candidates: [...new Set(thirdCandidates)].slice(0, thirdCandidateLimit + 1),
    formation_string: `${[...new Set(firstCandidates)].slice(0, firstCandidateLimit + (lane1RemainWindow ? 1 : 0)).join("")}-${[...new Set(secondCandidates)].slice(0, secondCandidateLimit + 1).join("")}-${[...new Set(thirdCandidates)].slice(0, thirdCandidateLimit + 1).join("")}`,
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
      venue_triggered_optional: venueTriggeredOptional,
      venue_allows_optional: venueAllowsOptional,
      near_tie_second_234: nearTieSecond234,
      strong_near_tie_second_234: strongNearTieSecond234,
      rescue_evidence_strong: rescueEvidenceStrong,
      lane1_second_window: lane1RemainWindow,
      hard_inside_shape: hardInsideShape,
      clean_inside_order: cleanInsideOrder,
      enough_top6_coverage: enoughCoverage,
      ashiya_strict_gate: ashiyaStrictGate,
      tokuyama_strict_gate: tokuyamaStrictGate,
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
  const baseAll120 = Object.entries(normalized120)
    .map(([combo, probability]) => ({ combo, probability: round(probability, 4) }))
    .sort((a, b) => b.probability - a.probability);
  const preCorrectionTop6 = baseAll120.slice(0, 6);
  const top6CoverageSeed = round(preCorrectionTop6.reduce((sum, row) => sum + Number(row.probability || 0), 0), 4);
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
    all120: baseAll120,
    secondGivenHeadProbabilities,
    top6Coverage: top6CoverageSeed,
    chaosLevel: chaosValue,
    venueContext,
    outsideRiskProxy
  });
  const all120 = applyVenueFinishPatternCorrectionToCombos(baseAll120, {
    venueContext,
    boat1Profile,
    boat1SecondKeep,
    headProbMap,
    nearTieDiagnostics
  });
  const preTop6 = all120.slice(0, 6);
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
  const remainScores = buildRemainScores({
    profiles,
    venueContext,
    raceContext,
    secondGivenHeadProbabilities,
    boat1SecondKeep
  });
  const racePatternSummary = buildRacePatternSummary({
    boat1HeadProbability: headProbMap[1] || 0,
    remainScores,
    chaosValue,
    outsideRiskProxy,
    venueContext
  });
  const pressureIntentSummary = buildPressureIntentSummary({
    raceContext,
    venueContext,
    chaosValue,
    boat1SecondKeep,
    remainScores
  });
  const confidenceSummary = buildConfidenceSummary({
    top6Coverage,
    chaosValue,
    top6ScenarioScore,
    outsideRiskProxy,
    strongestHead,
    nearTieDiagnostics,
    venueContext,
    racePatternScore: racePatternSummary.racePatternScore
  });
  const formationSuggestion = buildFormationSuggestion(all120, finishProbabilities, chaosValue, top6Coverage, venueContext, nearTieDiagnostics);
  const top6Scenario = describeTop6Scenario(profiles, top6, chaosValue);
  const preliminaryBetMode = buildPreliminaryBetMode({
    confidenceBand: confidenceSummary.confidence_band,
    confidenceScore: confidenceSummary.confidence_score,
    predictionStabilityScore: confidenceSummary.prediction_stability_score,
    optionalFormation: formationSuggestion,
    optionalFormationActive: formationSuggestion?.active === true,
    top6Coverage,
    chaosValue,
    venueContext
  });
  const similarRacePatternScore = round(scoreBlend([
    { value: toNum(racePatternSummary.racePatternScore, 50), weight: 0.34 },
    { value: toNum(confidenceSummary.prediction_stability_score, 50), weight: 0.28 },
    { value: (1 - chaosValue) * 100, weight: 0.18 },
    { value: (1 - outsideRiskProxy) * 100, weight: 0.1 },
    { value: toNum(venueContext?.one_course_trust, 50), weight: 0.1 }
  ]), 1);

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
    racePattern: racePatternSummary.racePattern,
    racePatternScore: racePatternSummary.racePatternScore,
    lane2_sashi_keep_score: remainScores.lane2_sashi_keep_score,
    lane3_attack_keep_score: remainScores.lane3_attack_keep_score,
    lane4_tenkaisashi_score: remainScores.lane4_tenkaisashi_score,
    pressure_mode: pressureIntentSummary.pressure_mode,
    attack_intent_score: pressureIntentSummary.attack_intent_score,
    safe_run_bias: pressureIntentSummary.safe_run_bias,
    similarRacePatternScore: similarRacePatternScore,
    near_tie_second_candidates: nearTieDiagnostics.near_tie_second_candidates,
    close_combo_preserved: nearTieDiagnostics.close_combo_preserved,
    combo_gap_score: nearTieDiagnostics.combo_gap_score,
    all_120_combinations: all120,
    top6,
    top6_coverage: top6Coverage,
    confidence,
    confidence_band: confidenceSummary.confidence_band,
    confidence_score: confidenceSummary.confidence_score,
    prediction_stability_score: confidenceSummary.prediction_stability_score,
    buy_confidence_reason: confidenceSummary.buy_confidence_reason,
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
    recommendedBetMode: preliminaryBetMode.recommendedBetMode,
    skipRiskReason: preliminaryBetMode.skipRiskReason,
    wide_formation_suggestion: formationSuggestion,
    venueBiasProfile: venueContext?.venueBiasProfile || venueContext?.venue_bias_profile || null,
    buyPolicy: venueContext?.buyPolicy || null,
    venueAdjustmentReason: [
      ...(Array.isArray(venueContext?.venueAdjustmentReason) ? venueContext.venueAdjustmentReason : []),
      ...(nearTieDiagnostics?.venue_fit_reason ? [nearTieDiagnostics.venue_fit_reason] : []),
      ...(boat1SecondKeep?.reason ? [`boat1 remain: ${boat1SecondKeep.reason}`] : []),
      ...(preliminaryBetMode?.modeCalibrationReason ? [preliminaryBetMode.modeCalibrationReason] : [])
    ],
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
