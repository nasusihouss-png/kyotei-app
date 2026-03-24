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

function weightedAverage(values) {
  const present = values.filter((row) => Number.isFinite(Number(row?.value)) && Number.isFinite(Number(row?.weight)) && row.weight > 0);
  if (!present.length) return null;
  const total = present.reduce((sum, row) => sum + row.weight, 0);
  return total > 0 ? present.reduce((sum, row) => sum + row.value * row.weight, 0) / total : null;
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
  return Object.fromEntries(entries.map(([key, value], index) => {
    if (index === entries.length - 1) return [key, round(Math.max(0, 1 - running), 4)];
    const normalized = round(value / total, 4);
    running += normalized;
    return [key, normalized];
  }));
}

function entropy(values) {
  const list = Object.values(values || {}).filter((value) => Number.isFinite(Number(value)) && Number(value) > 0);
  if (!list.length) return 0;
  return -list.reduce((sum, value) => sum + value * Math.log(value), 0);
}

function buildLaneProfile(row = {}) {
  const racer = row?.racer || {};
  const features = row?.features || {};
  const coverage = features?.coverage_report && typeof features.coverage_report === "object" ? features.coverage_report : {};
  const lane = Number(racer?.lane);
  const course1HeadRate = toNum(features?.course1_win_rate ?? racer?.course1WinRate, null);
  const lane2Rate =
    lane === 1
      ? toNum(features?.course1_2rate, null)
      : lane === 2
        ? toNum(features?.course2_2rate, null)
        : lane === 3
          ? toNum(features?.course3_3rate, null)
          : lane === 4
            ? toNum(features?.course4_3rate, null)
            : toNum(features?.course_fit_score, null) === null
              ? null
              : clamp(0, 100, 45 + Number(features.course_fit_score) * 7);
  const lane3Rate =
    lane === 3
      ? toNum(features?.course3_3rate, null)
      : lane === 4
        ? toNum(features?.course4_3rate, null)
        : lane2Rate;
  const avgSt = toNum(features?.avg_st ?? racer?.avgSt, null);
  const motor2 = toNum(features?.motor2_rate ?? racer?.motor2Rate, null);
  const boat2 = toNum(features?.boat2_rate ?? racer?.boat2Rate, null);
  const motor3 = coverageValue(coverage, "motor_3ren", toNum(features?.motor3_rate ?? racer?.motor3Rate, null));
  const motorTotal = toNum(features?.motor_total_score, null);
  const courseFit = toNum(features?.course_fit_score, null);
  const entryAdvantage = toNum(features?.entry_advantage_score, null);
  const exhibitionSt = coverageValue(coverage, "exhibition_st", toNum(racer?.exhibitionSt, null));
  const exhibitionTime = coverageValue(coverage, "exhibition_time", toNum(racer?.exhibitionTime, null));
  const lapTime = coverageValue(coverage, "lapTime", toNum(racer?.lapTime ?? racer?.kyoteiBiyoriLapTime, null));
  const lane1stRate = coverageValue(coverage, "lane_1st_rate", null);
  const lane2RenRate = coverageValue(coverage, "lane_2ren_rate", null);
  const lane3RenRate = coverageValue(coverage, "lane_3ren_rate", null);
  const stability = weightedAverage([
    { value: invertNormalize(avgSt, 0.11, 0.24) === null ? null : invertNormalize(avgSt, 0.11, 0.24) * 100, weight: 0.4 },
    { value: normalize(courseFit, -2, 8) === null ? null : normalize(courseFit, -2, 8) * 100, weight: 0.25 },
    { value: normalize(motorTotal, 0, 18) === null ? null : normalize(motorTotal, 0, 18) * 100, weight: 0.2 },
    { value: normalize(boat2, 20, 60) === null ? null : normalize(boat2, 20, 60) * 100, weight: 0.15 }
  ]);

  return {
    lane,
    nationwideWinRate: toNum(features?.nationwide_win_rate ?? racer?.nationwideWinRate, null),
    localWinRate: toNum(features?.local_win_rate ?? racer?.localWinRate, null),
    avgSt,
    lateRate: toNum(features?.late_start_rate ?? racer?.lateStartRate, 0),
    fCount: toNum(features?.f_hold_count ?? racer?.fHoldCount, 0),
    lCount: toNum(features?.l_hold_count ?? racer?.lHoldCount, 0),
    motor2,
    boat2,
    motor3,
    motorTotal,
    courseHeadRate: lane === 1 ? course1HeadRate : lane2Rate,
    course2Rate: lane2Rate,
    course3Rate: lane3Rate,
    lane1stRate,
    lane2RenRate,
    lane3RenRate,
    courseFit,
    entryAdvantage,
    stability,
    exhibitionSt,
    exhibitionTime,
    lapTime,
    attackRate: weightedAverage([
      { value: normalize(entryAdvantage, 0, 14) === null ? null : normalize(entryAdvantage, 0, 14) * 100, weight: 0.4 },
      { value: normalize(motorTotal, 0, 18) === null ? null : normalize(motorTotal, 0, 18) * 100, weight: 0.25 },
      { value: invertNormalize(avgSt, 0.11, 0.24) === null ? null : invertNormalize(avgSt, 0.11, 0.24) * 100, weight: 0.2 },
      { value: lane2Rate, weight: 0.15 }
    ]),
    supportRate: weightedAverage([
      { value: lane2Rate, weight: 0.38 },
      { value: normalize(motor2, 20, 60) === null ? null : normalize(motor2, 20, 60) * 100, weight: 0.24 },
      { value: invertNormalize(avgSt, 0.11, 0.24) === null ? null : invertNormalize(avgSt, 0.11, 0.24) * 100, weight: 0.18 },
      { value: weightedAverage([{ value: normalize(courseFit, -2, 8) === null ? null : normalize(courseFit, -2, 8) * 100, weight: 0.5 }, { value: stability, weight: 0.5 }]), weight: 0.2 }
    ]),
    stabilityRate: toNum(coverage?.stability_rate?.value, null),
    breakoutRate: toNum(coverage?.breakout_rate?.value, null),
    sashiRate: toNum(coverage?.sashi_rate?.value, null),
    makuriRate: toNum(coverage?.makuri_rate?.value, null),
    makuriSashiRate: toNum(coverage?.makurisashi_rate?.value, null),
    zentsukeTendency: toNum(coverage?.zentsuke_tendency?.value, null),
    fallbackFields: Object.entries(coverage)
      .filter(([, meta]) => meta?.fallback_used)
      .map(([key]) => key)
  };
}

function buildScenarioReproScore(profile, venueInsideBias = 0.62) {
  const venueBiasScore =
    profile.lane === 1
      ? venueInsideBias * 100
      : profile.lane >= 5
        ? clamp(24, 86, 44 + (profile.entryAdvantage || 50) * 0.38)
        : clamp(20, 84, 32 + (profile.courseFit || 0) * 4 + (profile.course2Rate || 50) * 0.45);
  const styleScore =
    profile.lane === 1
      ? weightedAverage([
          { value: profile.courseHeadRate, weight: 0.35 },
          { value: profile.lane1stRate, weight: 0.2 },
          { value: profile.stabilityRate, weight: 0.2 },
          { value: profile.stability, weight: 0.25 }
        ])
      : profile.lane === 2
        ? weightedAverage([
            { value: profile.sashiRate, weight: 0.32 },
            { value: profile.course2Rate, weight: 0.28 },
            { value: profile.lane2RenRate, weight: 0.22 },
            { value: profile.supportRate, weight: 0.18 }
          ])
        : profile.lane === 3
          ? weightedAverage([
              { value: profile.makuriRate, weight: 0.26 },
              { value: profile.makuriSashiRate, weight: 0.24 },
              { value: profile.attackRate, weight: 0.22 },
              { value: profile.lane1stRate, weight: 0.12 },
              { value: profile.lane3RenRate, weight: 0.16 }
            ])
          : profile.lane === 4
            ? weightedAverage([
                { value: profile.breakoutRate, weight: 0.24 },
                { value: profile.zentsukeTendency, weight: 0.2 },
                { value: profile.attackRate, weight: 0.22 },
                { value: profile.lane2RenRate, weight: 0.14 },
                { value: profile.lane3RenRate, weight: 0.2 }
              ])
            : weightedAverage([
                { value: profile.attackRate, weight: 0.24 },
                { value: profile.makuriRate, weight: 0.18 },
                { value: profile.lane2RenRate, weight: 0.16 },
                { value: profile.lane3RenRate, weight: 0.18 },
                { value: profile.supportRate, weight: 0.24 }
              ]);
  const optionalSpeedScore = weightedAverage([
    { value: invertNormalize(profile.exhibitionSt, 0.08, 0.22) === null ? null : invertNormalize(profile.exhibitionSt, 0.08, 0.22) * 100, weight: 0.28 },
    { value: invertNormalize(profile.exhibitionTime, 6.55, 7.15) === null ? null : invertNormalize(profile.exhibitionTime, 6.55, 7.15) * 100, weight: 0.34 },
    { value: invertNormalize(profile.lapTime, 6.55, 7.15) === null ? null : invertNormalize(profile.lapTime, 6.55, 7.15) * 100, weight: 0.38 }
  ]);
  return weightedAverage([
    { value: styleScore, weight: 0.48 },
    { value: venueBiasScore, weight: 0.14 },
    { value: normalize(profile.motor3, 25, 75) === null ? null : normalize(profile.motor3, 25, 75) * 100, weight: 0.08 },
    { value: normalize(profile.motorTotal, 0, 18) === null ? null : normalize(profile.motorTotal, 0, 18) * 100, weight: 0.1 },
    { value: optionalSpeedScore, weight: 0.2 }
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

function firstPlaceScore(profile, venueInsideBias = 0.62, raceContext = {}) {
  const laneBias = profile.lane === 1 ? venueInsideBias * 100 : clamp(18, 84, 78 - (profile.lane - 1) * 10);
  const delayedPenalty = normalize(profile.lateRate, 0, 0.3) === null ? null : (1 - normalize(profile.lateRate, 0, 0.3)) * 100;
  const flSafety = invertNormalize((profile.fCount || 0) * 0.9 + (profile.lCount || 0) * 0.45, 0, 2.5);
  const escapeThreatPenalty = profile.lane === 1
    ? weightedAverage([
        { value: 100 - toNum(raceContext?.lane3Makuri, 50), weight: 0.4 },
        { value: 100 - toNum(raceContext?.lane4Breakout, 50), weight: 0.25 },
        { value: 100 - toNum(raceContext?.outsideHeadPressure, 50), weight: 0.35 }
      ])
    : null;
  return weightedAverage([
    { value: normalize(profile.nationwideWinRate, 4, 8.5) === null ? null : normalize(profile.nationwideWinRate, 4, 8.5) * 100, weight: 0.18 },
    { value: normalize(profile.localWinRate, 4, 8.5) === null ? null : normalize(profile.localWinRate, 4, 8.5) * 100, weight: 0.15 },
    { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.14 },
    { value: delayedPenalty, weight: 0.08 },
    { value: flSafety === null ? null : flSafety * 100, weight: 0.08 },
    { value: normalize(weightedAverage([{ value: profile.motor2, weight: 0.6 }, { value: profile.boat2, weight: 0.4 }]), 20, 60) === null ? null : normalize(weightedAverage([{ value: profile.motor2, weight: 0.6 }, { value: profile.boat2, weight: 0.4 }]), 20, 60) * 100, weight: 0.14 },
    { value: normalize(profile.courseHeadRate, 18, 80) === null ? null : normalize(profile.courseHeadRate, 18, 80) * 100, weight: 0.15 },
    { value: laneBias, weight: 0.05 },
    { value: escapeThreatPenalty, weight: 0.03 },
    { value: profile.scenarioReproScore, weight: 0.08 },
    { value: invertNormalize(profile.lapTime, 6.55, 7.15) === null ? null : invertNormalize(profile.lapTime, 6.55, 7.15) * 100, weight: 0.04 }
  ]);
}

function secondPlaceScore(profile, headProfile, raceContext = {}) {
  const laneRelation = clamp(0, 100, 82 - Math.abs(profile.lane - headProfile.lane) * 9);
  const headPressure = clamp(0, 100, (headProfile.attackRate || 50) * 0.35 + 45);
  const outsideBoost = profile.lane >= 5 ? toNum(raceContext?.outside2ndPressure, 0) : 0;
  return weightedAverage([
    { value: profile.supportRate, weight: 0.28 },
    { value: normalize(profile.course2Rate, 18, 80) === null ? null : normalize(profile.course2Rate, 18, 80) * 100, weight: 0.24 },
    { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.15 },
    { value: normalize(profile.motor2, 20, 60) === null ? null : normalize(profile.motor2, 20, 60) * 100, weight: 0.13 },
    { value: laneRelation, weight: 0.08 },
    { value: clamp(0, 100, 100 - Math.abs((profile.attackRate || 50) - headPressure)), weight: 0.09 },
    { value: outsideBoost, weight: 0.03 },
    { value: profile.scenarioReproScore, weight: 0.07 }
  ]);
}

function thirdPlaceScore(profile, headProfile, secondProfile, raceContext = {}) {
  const developmentGap = clamp(0, 100, 100 - Math.abs((profile.supportRate || 50) - weightedAverage([
    { value: headProfile.attackRate || 50, weight: 0.45 },
    { value: secondProfile.supportRate || 50, weight: 0.55 }
  ])));
  const outsideThirdBoost = profile.lane >= 5 ? toNum(raceContext?.outside3rdPressure, 0) : 0;
  return weightedAverage([
    { value: normalize(profile.course3Rate, 18, 85) === null ? null : normalize(profile.course3Rate, 18, 85) * 100, weight: 0.28 },
    { value: profile.stability, weight: 0.22 },
    { value: invertNormalize(profile.avgSt, 0.11, 0.24) === null ? null : invertNormalize(profile.avgSt, 0.11, 0.24) * 100, weight: 0.14 },
    { value: normalize(profile.motor2, 20, 60) === null ? null : normalize(profile.motor2, 20, 60) * 100, weight: 0.14 },
    { value: developmentGap, weight: 0.18 },
    { value: outsideThirdBoost, weight: 0.04 },
    { value: profile.scenarioReproScore, weight: 0.08 }
  ]);
}

export function buildTop6Prediction({ ranking = [], race = null } = {}) {
  const profiles = (Array.isArray(ranking) ? ranking : [])
    .map((row) => buildLaneProfile(row))
    .filter((row) => Number.isInteger(row?.lane))
    .sort((a, b) => a.lane - b.lane);
  if (profiles.length !== 6) {
    return null;
  }

  const venueInsideBias = Number(VENUE_BIAS_BY_ID?.[Number(race?.venueId)] ?? 0.62);
  const lane3 = profiles.find((row) => row.lane === 3) || {};
  const lane4 = profiles.find((row) => row.lane === 4) || {};
  const lane5 = profiles.find((row) => row.lane === 5) || {};
  const lane6 = profiles.find((row) => row.lane === 6) || {};
  profiles.forEach((profile) => {
    profile.scenarioReproScore = buildScenarioReproScore(profile, venueInsideBias);
  });
  const raceContext = {
    lane3Makuri: weightedAverage([
      { value: lane3.attackRate, weight: 0.45 },
      { value: lane3.makuriRate, weight: 0.35 },
      { value: lane3.makuriSashiRate, weight: 0.2 }
    ]),
    lane4Breakout: weightedAverage([
      { value: lane4.attackRate, weight: 0.4 },
      { value: lane4.breakoutRate, weight: 0.35 },
      { value: lane4.zentsukeTendency, weight: 0.25 }
    ]),
    outsideHeadPressure: weightedAverage([
      { value: lane5.attackRate, weight: 0.24 },
      { value: lane6.attackRate, weight: 0.24 },
      { value: lane5.makuriRate, weight: 0.16 },
      { value: lane6.makuriRate, weight: 0.16 },
      { value: lane5.scenarioReproScore, weight: 0.1 },
      { value: lane6.scenarioReproScore, weight: 0.1 }
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
      { value: lane6.stability, weight: 0.1 }
    ])
  };
  const headProbMap = normalizeMap(
    Object.fromEntries(profiles.map((profile) => [profile.lane, Math.max(0.001, (firstPlaceScore(profile, venueInsideBias, raceContext) || 1) / 100)]))
  );

  const secondMap = {};
  for (const head of profiles) {
    secondMap[head.lane] = normalizeMap(
      Object.fromEntries(
        profiles
          .filter((profile) => profile.lane !== head.lane)
          .map((profile) => [profile.lane, Math.max(0.001, (secondPlaceScore(profile, head, raceContext) || 1) / 100)])
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
            .map((profile) => [profile.lane, Math.max(0.001, (thirdPlaceScore(profile, head, second, raceContext) || 1) / 100)])
        )
      );
    }
  }

  const allCombos = [];
  for (const head of profiles) {
    for (const second of profiles.filter((row) => row.lane !== head.lane)) {
      for (const third of profiles.filter((row) => row.lane !== head.lane && row.lane !== second.lane)) {
        const combo = `${head.lane}-${second.lane}-${third.lane}`;
        const probability =
          (headProbMap[head.lane] || 0) *
          (secondMap[head.lane]?.[second.lane] || 0) *
          (thirdMap[head.lane]?.[second.lane]?.[third.lane] || 0);
        allCombos.push({
          combo,
          probability
        });
      }
    }
  }

  const normalized120 = normalizeMap(Object.fromEntries(allCombos.map((row) => [row.combo, row.probability])));
  const sorted = Object.entries(normalized120)
    .map(([combo, probability]) => ({ combo, probability }))
    .sort((a, b) => b.probability - a.probability);
  const top6 = sorted.slice(0, 6).map((row, index) => ({
    ...row,
    tier: index < 2 ? "本命" : index < 4 ? "対抗" : "抑え",
    rank: index + 1
  }));
  const top6Coverage = round(top6.reduce((sum, row) => sum + row.probability, 0), 4);
  const chaosValue = clamp(0, 1, 1 - top6Coverage + Math.min(0.25, entropy(headProbMap) / 2));
  const strongestHead = Math.max(...Object.values(headProbMap));
  const top6ScenarioScore = round(
    weightedAverage(profiles.map((profile) => ({ value: profile.scenarioReproScore, weight: profile.lane === 1 ? 1.3 : 1 }))),
    1
  );
  const confidence = round(
    clamp(
      0,
      1,
      top6Coverage * 0.66 +
      strongestHead * 0.22 +
      (1 - (toNum(raceContext.outsideHeadPressure, 50) / 100)) * 0.06 +
      (1 - (toNum(raceContext.lane3Makuri, 50) / 100)) * 0.03 +
      (1 - (toNum(raceContext.lane4Breakout, 50) / 100)) * 0.03 +
      Math.min(0.05, (((top6ScenarioScore || 50) - 50) / 100))
    ),
    4
  );
  const top6Scenario = describeTop6Scenario(profiles, top6, chaosValue);

  return {
    head_prob_1: headProbMap[1] || 0,
    head_prob_2: headProbMap[2] || 0,
    head_prob_3: headProbMap[3] || 0,
    head_prob_4: headProbMap[4] || 0,
    head_prob_5: headProbMap[5] || 0,
    head_prob_6: headProbMap[6] || 0,
    head_candidate_ranking: Object.entries(headProbMap)
      .map(([lane, probability]) => ({ lane: Number(lane), probability }))
      .sort((a, b) => b.probability - a.probability),
    top6,
    top6_coverage: top6Coverage,
    confidence,
    chaos_level: round(chaosValue, 4),
    top6Scenario,
    top6ScenarioScore,
    scenario_repro_score: top6ScenarioScore,
    chaos_label: chaosValue >= 0.52 ? "高" : chaosValue >= 0.34 ? "中" : "低",
    main_ticket: top6[0] || null,
    conditional_probabilities: {
      first: headProbMap,
      second: secondMap,
      third: thirdMap
    },
    scenario_repro_scores: profiles.map((profile) => ({
      lane: profile.lane,
      score: round(profile.scenarioReproScore, 1)
    }))
  };
}

const VENUE_BIAS_BY_ID = {
  1: 0.63, 2: 0.64, 3: 0.51, 4: 0.58, 5: 0.62, 6: 0.64, 7: 0.71, 8: 0.67,
  9: 0.57, 10: 0.68, 11: 0.64, 12: 0.69, 13: 0.62, 14: 0.56, 15: 0.7, 16: 0.63,
  17: 0.61, 18: 0.7, 19: 0.73, 20: 0.67, 21: 0.68, 22: 0.64, 23: 0.66, 24: 0.76
};
