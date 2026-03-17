function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toInt(value, fallback = null) {
  const n = Number.parseInt(value, 10);
  return Number.isInteger(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeRows(rows) {
  const normalized = safeArray(rows)
    .map((row) => ({
      lane: toInt(row?.lane, null),
      weight: toNum(row?.weight ?? row?.probability, 0)
    }))
    .filter((row) => Number.isInteger(row.lane) && row.lane >= 1 && row.lane <= 6);
  const total = normalized.reduce((sum, row) => sum + Math.max(0, row.weight), 0);
  if (total <= 0) return normalized.map((row) => ({ ...row, weight: 0 }));
  return normalized
    .map((row) => ({
      lane: row.lane,
      weight: Number((Math.max(0, row.weight) / total).toFixed(4))
    }))
    .sort((a, b) => b.weight - a.weight || a.lane - b.lane);
}

function mapByLane(rows) {
  return new Map(normalizeRows(rows).map((row) => [row.lane, row.weight]));
}

function pickTopLanes(rows, limit, exclude = []) {
  const blocked = new Set(exclude);
  return normalizeRows(rows)
    .filter((row) => !blocked.has(row.lane))
    .slice(0, limit)
    .map((row) => row.lane);
}

function round(value, digits = 4) {
  return Number(toNum(value, 0).toFixed(digits));
}

function average(values) {
  const valid = values.filter((value) => Number.isFinite(value));
  if (valid.length === 0) return null;
  return valid.reduce((sum, value) => sum + value, 0) / valid.length;
}

const LANE_FINISH_PRIORS = {
  first: { 1: 1.22, 2: 1.08, 3: 1.01, 4: 0.94, 5: 0.86, 6: 0.8 },
  second: { 1: 1.1, 2: 1.08, 3: 1.02, 4: 0.95, 5: 0.9, 6: 0.86 },
  third: { 1: 1.04, 2: 1.03, 3: 1.01, 4: 0.97, 5: 0.93, 6: 0.9 }
};

function normalizeAgainstPeers(value, values) {
  if (!Number.isFinite(value)) return 0;
  const valid = values.filter((entry) => Number.isFinite(entry));
  if (valid.length < 2) return 0;
  const min = Math.min(...valid);
  const max = Math.max(...valid);
  const avg = average(valid);
  const spread = Math.max(0.05, max - min);
  return clamp(-1, 1, (value - avg) / spread);
}

function buildRoleSpecificFinishBonuses(lane, row, laneMap) {
  const leftLane = lane > 1 ? laneMap.get(lane - 1) || null : null;
  const leftGap = Number.isFinite(row?.ex_time_left_gap_advantage) ? row.ex_time_left_gap_advantage : 0;
  const positiveLeftGap = Math.max(0, leftGap);
  const turningNorm = normalizeAgainstPeers(
    row?.turning_ability,
    [...laneMap.values()].map((entry) => entry?.turning_ability)
  );
  const straightNorm = normalizeAgainstPeers(
    row?.straight_line_power,
    [...laneMap.values()].map((entry) => entry?.straight_line_power)
  );
  const styleProfile = row?.style_profile || {};
  const makuriHeadStyle = clamp(
    0,
    1,
    (toNum(styleProfile?.makuri, 0) * 0.62 + toNum(styleProfile?.nige, 0) * 0.24) / 100
  );
  const sashiSecondStyle = clamp(
    0,
    1,
    (toNum(styleProfile?.sashi, 0) * 0.54 + toNum(styleProfile?.makuri_sashi, 0) * 0.38) / 100
  );
  const survivalThirdStyle = clamp(
    0,
    1,
    (toNum(styleProfile?.nuki, 0) * 0.5 + toNum(styleProfile?.makuri_sashi, 0) * 0.22 + toNum(styleProfile?.sashi, 0) * 0.14) / 100
  );

  const firstPlaceBonus = clamp(
    -0.08,
    0.22,
    positiveLeftGap * 0.95 +
      Math.max(0, straightNorm) * 0.12 +
      Math.max(0, turningNorm) * 0.05 +
      makuriHeadStyle * 0.14 -
      Math.max(0, -straightNorm) * 0.04
  );
  const secondPlaceBonus = clamp(
    -0.08,
    0.24,
    positiveLeftGap * 0.72 +
      Math.max(0, turningNorm) * 0.13 +
      Math.max(0, straightNorm) * 0.08 +
      sashiSecondStyle * 0.16 -
      Math.max(0, -turningNorm) * 0.04
  );
  const thirdPlaceBonus = clamp(
    -0.08,
    0.22,
    positiveLeftGap * 0.28 +
      Math.max(0, turningNorm) * 0.15 +
      Math.max(0, straightNorm) * 0.04 +
      survivalThirdStyle * 0.13 -
      Math.max(0, -turningNorm) * 0.03
  );

  return {
    left_boat_lane: leftLane?.lane ?? null,
    leftGapAttackSupport: round(positiveLeftGap, 4),
    turningAbilityDelta: round(turningNorm, 4),
    straightLineDelta: round(straightNorm, 4),
    styleRoleFit: {
      first: round(makuriHeadStyle, 4),
      second: round(sashiSecondStyle, 4),
      third: round(survivalThirdStyle, 4)
    },
    firstPlaceBonus: round(firstPlaceBonus, 4),
    secondPlaceBonus: round(secondPlaceBonus, 4),
    thirdPlaceBonus: round(thirdPlaceBonus, 4)
  };
}

function buildThirdPlaceExclusion(row, laneMap) {
  const reasons = [];
  const laneFit3 = toNum(row?.lane_fit_3ren, 0);
  const turning = toNum(row?.turning_ability, 0);
  const straight = toNum(row?.straight_line_power, 0);
  const turningAvg = average([...laneMap.values()].map((entry) => entry?.turning_ability));
  const straightAvg = average([...laneMap.values()].map((entry) => entry?.straight_line_power));
  const nuki = toNum(row?.style_profile?.nuki, 0);
  const lateRisk = toNum(row?.late_risk, 0);
  const hiddenF = toNum(row?.hidden_F_flag, 0);
  const safeRunBias = toNum(row?.safe_run_bias, 0);

  if (laneFit3 > 0 && laneFit3 < 38) reasons.push("weak_lane3ren");
  if (Number.isFinite(turningAvg) && turning <= turningAvg - 0.35) reasons.push("weak_turning");
  if (Number.isFinite(straightAvg) && straight <= straightAvg - 0.4) reasons.push("weak_straight_retention");
  if (lateRisk >= 0.22) reasons.push("late_risk");
  if (hiddenF === 1) reasons.push("hidden_f");
  if (safeRunBias <= 0.025 && nuki < 18 && toInt(row?.lane, 0) >= 5) reasons.push("poor_flow_in_profile");

  const penalty = clamp(0, 0.28, reasons.length * 0.045 + (reasons.includes("weak_lane3ren") ? 0.05 : 0));
  return {
    reasons,
    penalty: round(penalty, 4)
  };
}

function inferScenarioHeadLane(scenario, escapeScore) {
  switch (scenario) {
    case "boat2_direct_makuri":
      return escapeScore >= 0.34 ? 1 : 2;
    case "boat3_makuri":
      return escapeScore >= 0.36 ? 1 : 3;
    case "boat3_makuri_sashi":
      return 1;
    case "boat4_cado_attack":
      return escapeScore >= 0.37 ? 1 : 4;
    case "outer_mix_chaos":
      return 1;
    case "boat2_sashi":
    case "boat1_escape":
    default:
      return 1;
  }
}

function buildCompatibilityWithHead(headLane, row, headRow, escapeScore) {
  if (!Number.isInteger(headLane) || row?.lane === headLane) {
    return {
      second_bonus: 0,
      third_bonus: 0,
      reasons: []
    };
  }

  const lane = toInt(row?.lane, 0);
  const head = headRow || {};
  const sashi = toNum(row?.style_profile?.sashi, 0) / 100;
  const makuriSashi = toNum(row?.style_profile?.makuri_sashi, 0) / 100;
  const nuki = toNum(row?.style_profile?.nuki, 0) / 100;
  const laneFit2 = toNum(row?.lane_fit_2ren, 0) / 100;
  const laneFit3 = toNum(row?.lane_fit_3ren, 0) / 100;
  const startEdge = Math.max(0, toNum(row?.start_edge, 0));
  const straight = Math.max(0, toNum(row?.finish_role_bonuses?.straightLineDelta, 0));
  const turning = Math.max(0, toNum(row?.finish_role_bonuses?.turningAbilityDelta, 0));
  const attackReadiness = Math.max(0, toNum(row?.attack_readiness_bonus, 0));
  const safeRunBias = toNum(row?.safe_run_bias, 0);
  const reasons = [];

  let secondBonus = 0;
  let thirdBonus = 0;

  if (headLane === 1) {
    secondBonus += laneFit2 * 0.1 + sashi * 0.12 + makuriSashi * 0.09 + startEdge * 0.35;
    thirdBonus += laneFit3 * 0.08 + nuki * 0.08 + safeRunBias * 0.18;
    if (lane === 2 || lane === 3) {
      secondBonus += 0.035;
      reasons.push("inner_partner_second");
    }
    if (lane === 4) {
      thirdBonus += 0.03 + attackReadiness * 0.22;
      reasons.push("outer_attack_flow_third");
    }
    if (lane >= 5) {
      thirdBonus += 0.02 + nuki * 0.05;
      reasons.push("outer_residual_third");
    }
  } else {
    const distance = Math.abs(lane - headLane);
    secondBonus += laneFit2 * 0.08 + turning * 0.06 + Math.max(0, 0.045 - distance * 0.01);
    thirdBonus += laneFit3 * 0.08 + straight * 0.05 + safeRunBias * 0.12;
    if (lane < headLane) reasons.push("inside_of_head");
    if (lane > headLane) reasons.push("outside_residual");
  }

  secondBonus += Math.min(0.08, attackReadiness * 0.5);
  thirdBonus += Math.min(0.07, attackReadiness * 0.28 + nuki * 0.04);
  if (toNum(head?.lane_fit_1st, 0) >= 55 && headLane === 1) reasons.push("stable_head_shape");

  return {
    second_bonus: round(clamp(-0.04, 0.24, secondBonus), 4),
    third_bonus: round(clamp(-0.04, 0.22, thirdBonus), 4),
    reasons
  };
}

function buildFinishRoleScores(laneContexts, laneMap, baseRoleProbabilities, scenarioRows, escapeScore, laneFinishPriors) {
  const headScenarioSupport = new Map();
  for (const scenarioRow of safeArray(scenarioRows)) {
    const headLane = inferScenarioHeadLane(String(scenarioRow?.scenario || ""), escapeScore);
    headScenarioSupport.set(headLane, toNum(headScenarioSupport.get(headLane), 0) + toNum(scenarioRow?.probability, 0));
  }
  const sortedHeadCandidates = [...headScenarioSupport.entries()]
    .sort((a, b) => b[1] - a[1] || a[0] - b[0])
    .map(([lane, probability]) => ({ lane, probability: round(probability, 4) }))
    .slice(0, 3);
  const primaryHeadLane = sortedHeadCandidates[0]?.lane || 1;

  for (const row of laneContexts) {
    const lane = row.lane;
    const laneFit1 = toNum(row?.lane_fit_1st, 0) / 100;
    const laneFit2 = toNum(row?.lane_fit_2ren, 0) / 100;
    const laneFit3 = toNum(row?.lane_fit_3ren, 0) / 100;
    const motor2 = row?.prediction_data_usage?.motor2ren?.used ? toNum(row?.motor_raw?.motor2ren, 0) / 100 : 0;
    const motor3 = row?.prediction_data_usage?.motor3ren?.used ? toNum(row?.motor_raw?.motor3ren, 0) / 100 : 0;
    const motor3Proxy = motor3 > 0 ? motor3 : clamp(0, 1, laneFit3 * 0.58 + Math.max(0, toNum(row?.safe_run_bias, 0)) * 0.42);
    const turning = Math.max(-0.25, toNum(row?.finish_role_bonuses?.turningAbilityDelta, 0));
    const straight = Math.max(-0.25, toNum(row?.finish_role_bonuses?.straightLineDelta, 0));
    const secondStyle = toNum(row?.finish_role_bonuses?.styleRoleFit?.second, 0);
    const thirdStyle = toNum(row?.finish_role_bonuses?.styleRoleFit?.third, 0);
    const firstStyle = toNum(row?.finish_role_bonuses?.styleRoleFit?.first, 0);
    const attackReadiness = toNum(row?.attack_readiness_bonus, 0);
    const hiddenF = toNum(row?.hidden_F_flag, 0);
    const lateRisk = toNum(row?.late_risk, 0);
    const survivalAfterAttackBonus = clamp(0, 0.16, attackReadiness * 0.42 + Math.max(0, turning) * 0.05 + Math.max(0, straight) * 0.04);
    const flowInBonus = clamp(0, 0.16, (lane >= 4 ? 0.025 : 0) + Math.max(0, turning) * 0.06 + Math.max(0, toNum(row?.safe_run_bias, 0)) * 0.18);
    const outerSurvivalBonus = clamp(0, 0.14, (lane >= 4 ? 0.03 : 0) + Math.max(0, straight) * 0.05 + thirdStyle * 0.06);
    const thirdExclusion = buildThirdPlaceExclusion(row, laneMap);
    row.third_place_exclusion = thirdExclusion;

    const compatibility = {};
    for (const headCandidate of sortedHeadCandidates) {
      compatibility[String(headCandidate.lane)] = buildCompatibilityWithHead(
        headCandidate.lane,
        row,
        laneMap.get(headCandidate.lane),
        escapeScore
      );
    }
    row.compatibility_with_head = compatibility;
    const primaryCompatibility = compatibility[String(primaryHeadLane)] || { second_bonus: 0, third_bonus: 0 };

    const firstPlaceScore =
      toNum(baseRoleProbabilities?.first?.[lane], 0) * 0.44 +
      laneFit1 * 0.24 +
      (toNum(row?.motor_true, 0) / 100) * 0.14 +
      Math.max(0, toNum(row?.start_edge, 0)) * 0.18 +
      toNum(row?.finish_role_bonuses?.firstPlaceBonus, 0) * 0.34 +
      firstStyle * 0.08 +
      toNum(laneFinishPriors?.first?.[lane], 1) * 0.08 -
      lateRisk * 0.12 -
      hiddenF * (lane === 1 ? 0.1 : 0.08);
    const secondPlaceScore =
      toNum(baseRoleProbabilities?.second?.[lane], 0) * 0.34 +
      laneFit2 * 0.28 +
      motor2 * 0.18 +
      Math.max(0, turning) * 0.12 +
      secondStyle * 0.13 +
      survivalAfterAttackBonus * 0.18 +
      toNum(primaryCompatibility?.second_bonus, 0) * 0.26 +
      toNum(row?.finish_role_bonuses?.secondPlaceBonus, 0) * 0.22 -
      lateRisk * 0.1 -
      hiddenF * 0.04;
    const thirdPlaceScore =
      toNum(baseRoleProbabilities?.third?.[lane], 0) * 0.28 +
      laneFit3 * 0.28 +
      motor3Proxy * 0.16 +
      Math.max(0, turning) * 0.13 +
      Math.max(0, straight) * 0.09 +
      flowInBonus * 0.16 +
      outerSurvivalBonus * 0.14 +
      thirdStyle * 0.08 +
      toNum(primaryCompatibility?.third_bonus, 0) * 0.18 +
      toNum(row?.finish_role_bonuses?.thirdPlaceBonus, 0) * 0.18 -
      toNum(thirdExclusion?.penalty, 0);

    row.finish_role_scores = {
      first_place_score: round(Math.max(0.0001, firstPlaceScore), 4),
      second_place_score: round(Math.max(0.0001, secondPlaceScore), 4),
      third_place_score: round(Math.max(0.0001, thirdPlaceScore), 4),
      survival_after_attack_bonus: round(survivalAfterAttackBonus, 4),
      flow_in_bonus: round(flowInBonus, 4),
      outer_survival_bonus: round(outerSurvivalBonus, 4),
      third_place_proxy_used: motor3 > 0 ? "motor3ren" : "survival_proxy",
      primary_head_lane: primaryHeadLane
    };
    row.second_place_bonus_breakdown = {
      lane2renScore: round(laneFit2, 4),
      motor2ren: round(motor2, 4),
      turning_bonus: round(Math.max(0, turning), 4),
      style_bonus: round(secondStyle, 4),
      compatibility_with_head: round(toNum(primaryCompatibility?.second_bonus, 0), 4),
      survival_after_attack_bonus: round(survivalAfterAttackBonus, 4)
    };
    row.third_place_bonus_breakdown = {
      lane3renScore: round(laneFit3, 4),
      motor3ren_or_proxy: round(motor3Proxy, 4),
      turning_bonus: round(Math.max(0, turning), 4),
      straight_retention_bonus: round(Math.max(0, straight), 4),
      flow_in_bonus: round(flowInBonus, 4),
      outer_survival_bonus: round(outerSurvivalBonus, 4),
      compatibility_with_head: round(toNum(primaryCompatibility?.third_bonus, 0), 4),
      exclusion_penalty: round(toNum(thirdExclusion?.penalty, 0), 4)
    };
  }

  return {
    headCandidates: sortedHeadCandidates,
    primaryHeadLane
  };
}

function buildTopExactaCandidates({ enhancement, firstProbs, secondProbs, limit = 4 }) {
  const firstRows = normalizeRows(firstProbs);
  const secondRows = normalizeRows(secondProbs);
  const firstMap = new Map(firstRows.map((row) => [row.lane, row.weight]));
  const secondMap = new Map(secondRows.map((row) => [row.lane, row.weight]));
  const orderRows = safeArray(enhancement?.treeOrderProbabilities || enhancement?.stage5_ticketing?.order_probabilities);
  const exactaMap = new Map();

  for (const row of orderRows) {
    const combo = normalizeCombo(row?.combo);
    if (!combo) continue;
    const [a, b] = combo.split("-").map((value) => toInt(value, null));
    if (!Number.isInteger(a) || !Number.isInteger(b)) continue;
    const key = `${a}-${b}`;
    const existing = exactaMap.get(key);
    exactaMap.set(key, {
      combo: key,
      probability: round(toNum(existing?.probability, 0) + toNum(row?.probability, 0), 6),
      source: "scenario_tree"
    });
  }

  for (const firstRow of firstRows.slice(0, 4)) {
    for (const secondRow of secondRows.slice(0, 5)) {
      if (firstRow.lane === secondRow.lane) continue;
      const key = `${firstRow.lane}-${secondRow.lane}`;
      const baseline = toNum(firstMap.get(firstRow.lane), 0) * toNum(secondMap.get(secondRow.lane), 0) * 1.08;
      const existing = exactaMap.get(key);
      exactaMap.set(key, {
        combo: key,
        probability: round(Math.max(toNum(existing?.probability, 0), baseline), 6),
        source: existing?.source || "role_distribution"
      });
    }
  }

  return [...exactaMap.values()]
    .sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0))
    .slice(0, limit)
    .map((row, index) => ({
      rank: index + 1,
      combo: row.combo,
      probability: round(row.probability, 4),
      source: row.source
    }));
}

function buildStylePressure(lane, profile) {
  const style = profile?.style_profile || {};
  const sashi = toNum(style.sashi, 0);
  const makuri = toNum(style.makuri, 0);
  const makuriSashi = toNum(style.makuri_sashi, 0);
  const nuki = toNum(style.nuki, 0);
  const laneWeight = lane === 2 ? 1.05 : lane === 3 || lane === 4 ? 1.12 : lane >= 5 ? 0.88 : 0.72;
  return round((sashi * 0.22 + makuri * 0.36 + makuriSashi * 0.28 + nuki * 0.08) * laneWeight, 2);
}

function buildMotivation(row, race) {
  const lane = toInt(row?.racer?.lane, 0);
  const grade = String(race?.grade || race?.raceGrade || "").toUpperCase();
  const classScore = toNum(row?.features?.class_score, 0);
  const localRate = toNum(row?.features?.local_win_rate, 0);
  const laneFit3 = toNum(row?.features?.lane_fit_3ren, 0);
  const isOuter = lane >= 4;
  const motivationAttack = clamp(
    0,
    1,
    (classScore <= 2 ? 0.08 : 0.03) +
      (isOuter ? 0.03 : 0) +
      (localRate >= 5.8 ? 0.02 : 0) +
      (/SG|G1/.test(grade) ? 0.01 : 0)
  );
  const safeRunBias = clamp(
    0,
    1,
    (laneFit3 >= 55 ? 0.06 : 0.02) +
      (lane === 1 ? 0.04 : 0) +
      (classScore >= 3 ? 0.02 : 0)
  );
  return {
    motivation_attack: round(motivationAttack, 3),
    safe_run_bias: round(safeRunBias, 3)
  };
}

function normalizeCombo(combo) {
  if (typeof combo !== "string") return null;
  const parts = combo.split("-").map((value) => toInt(value, null));
  if (parts.length !== 3 || parts.some((value) => !Number.isInteger(value))) return null;
  if (new Set(parts).size !== 3) return null;
  return parts.join("-");
}

function normalizeScenarioRows(rows) {
  const total = safeArray(rows).reduce((sum, row) => sum + Math.max(0, toNum(row?.probability, 0)), 0);
  if (total <= 0) {
    return safeArray(rows).map((row) => ({
      ...row,
      probability: 0
    }));
  }
  return safeArray(rows)
    .map((row) => ({
      ...row,
      probability: round(Math.max(0, toNum(row?.probability, 0)) / total, 4)
    }))
    .sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0));
}

function buildIntermediateEvents(laneMap, escapeScore, outerAttackPressure) {
  const lane1 = laneMap.get(1) || {};
  const lane2 = laneMap.get(2) || {};
  const lane3 = laneMap.get(3) || {};
  const lane4 = laneMap.get(4) || {};
  const boat1Out = toNum(lane1.start_edge, 0) < 0.015 || toNum(lane1.hidden_F_flag, 0) === 1 ? 1 : 0;
  const boat1Hollow = toNum(lane1.late_risk, 0) >= 0.16 || escapeScore < 0.24 ? 1 : 0;
  const boat2Out = toNum(lane2.start_edge, 0) >= 0.045 ? 1 : 0;
  const boat2Hollow = toNum(lane2.late_risk, 0) >= 0.16 ? 1 : 0;
  const boat3AttackReady =
    (toNum(lane3.style_profile?.makuri, 0) >= 58 || toNum(lane3.style_profile?.makuri_sashi, 0) >= 58) &&
    toNum(lane3.start_edge, 0) >= 0.03
      ? 1
      : 0;
  const boat4CadoReady =
    toNum(lane4.style_profile?.makuri, 0) >= 60 &&
    toNum(lane4.start_edge, 0) >= 0.035 &&
    toNum(lane4.lane_fit_1st, 0) >= 38
      ? 1
      : 0;
  const innerStable = escapeScore >= 0.28 && boat1Out === 0 && boat1Hollow === 0 ? 1 : 0;
  const innerCollapse = boat1Out === 1 || boat1Hollow === 1 || outerAttackPressure >= 0.18 ? 1 : 0;
  const outerPressure = outerAttackPressure >= 0.14 || boat3AttackReady === 1 || boat4CadoReady === 1 ? 1 : 0;
  return {
    boat1_out: boat1Out,
    boat1_hollow: boat1Hollow,
    boat2_out: boat2Out,
    boat2_hollow: boat2Hollow,
    boat3_attack_ready: boat3AttackReady,
    boat4_cado_ready: boat4CadoReady,
    inner_stable: innerStable,
    inner_collapse: innerCollapse,
    outer_pressure: outerPressure
  };
}

function roleScenarioWeightsForLane(lane, row, scenario, events, escapeScore) {
  let first = 1;
  let second = 1;
  let third = 1;
  const laneFit1 = toNum(row?.lane_fit_1st, 0) / 100;
  const laneFit2 = toNum(row?.lane_fit_2ren, 0) / 100;
  const laneFit3 = toNum(row?.lane_fit_3ren, 0) / 100;
  const lapBoost = row?.prediction_data_usage?.lapTime?.used
    ? Math.max(0, 6.82 - toNum(row?.motor_form?.lapTime, 6.82)) * 0.6
    : 0;
  const stretchBoost = row?.prediction_data_usage?.lapExStretch?.used
    ? Math.max(0, toNum(row?.motor_form?.lapExStretch, 0)) * 0.02
    : 0;
  const motor2 = row?.prediction_data_usage?.motor2ren?.used ? toNum(row?.motor_raw?.motor2ren, 0) / 100 : 0;
  const motor3 = row?.prediction_data_usage?.motor3ren?.used ? toNum(row?.motor_raw?.motor3ren, 0) / 100 : 0;
  const motorTrue = toNum(row?.motor_true, 0) / 100;
  const startEdge = Math.max(-0.08, toNum(row?.start_edge, 0));
  const lateRisk = toNum(row?.late_risk, 0);
  const hiddenF = toNum(row?.hidden_F_flag, 0);
  const roleBonus = row?.finish_role_bonuses || {
    firstPlaceBonus: 0,
    secondPlaceBonus: 0,
    thirdPlaceBonus: 0
  };
  const finishRoleScores = row?.finish_role_scores || {};
  const headLane = inferScenarioHeadLane(scenario, escapeScore);
  const compatibility = row?.compatibility_with_head?.[String(headLane)] || {
    second_bonus: 0,
    third_bonus: 0
  };
  const attackCarryoverSecond = clamp(0, 0.12, toNum(row?.attack_readiness_bonus, 0) * 0.46);
  const attackCarryoverThird = clamp(0, 0.1, toNum(row?.attack_readiness_bonus, 0) * 0.28);
  const thirdExclusionPenalty = toNum(row?.third_place_exclusion?.penalty, 0);

  first += laneFit1 * 0.28 + motorTrue * 0.12 + lapBoost * 0.16 + startEdge * 0.7 - lateRisk * 0.28 - hiddenF * 0.16;
  second += laneFit2 * 0.3 + motor2 * 0.22 + startEdge * 0.3 + stretchBoost * 0.15 - lateRisk * 0.16 - hiddenF * 0.06;
  third += laneFit3 * 0.34 + motor3 * 0.24 + stretchBoost * 0.22 + toNum(row?.safe_run_bias, 0) * 0.4 - lateRisk * 0.12 - hiddenF * 0.03;
  first += toNum(roleBonus.firstPlaceBonus, 0);
  second += toNum(roleBonus.secondPlaceBonus, 0);
  third += toNum(roleBonus.thirdPlaceBonus, 0);
  first += toNum(finishRoleScores.first_place_score, 0) * 0.22;
  second += toNum(finishRoleScores.second_place_score, 0) * 0.3 + toNum(compatibility.second_bonus, 0) + attackCarryoverSecond;
  third += toNum(finishRoleScores.third_place_score, 0) * 0.34 + toNum(compatibility.third_bonus, 0) + attackCarryoverThird - thirdExclusionPenalty;

  if (lane === 1) first += 0.18 + escapeScore * 0.14;

  switch (scenario) {
    case "boat1_escape":
      if (lane === 1) first += 0.32;
      if (lane === 2 || lane === 3) second += 0.12;
      if (lane >= 2 && lane <= 4) third += 0.12;
      break;
    case "boat2_sashi":
      if (lane === 2) {
        second += 0.26;
        first += events.boat1_hollow ? 0.06 : -0.04;
      }
      if (lane === 1) {
        first += 0.16;
        second += 0.08;
      }
      if (lane === 3 || lane === 4) third += 0.1;
      break;
    case "boat2_direct_makuri":
      if (lane === 2) first += 0.22;
      if (lane === 1) {
        first -= 0.16;
        second += 0.14;
      }
      if (lane === 3 || lane === 4) second += 0.1;
      break;
    case "boat3_makuri":
      if (lane === 3) first += 0.26;
      if (lane === 1) {
        first -= 0.12;
        second += 0.15;
      }
      if (lane === 2) {
        second += 0.06;
        third += 0.12;
      }
      if (lane === 3) second += attackCarryoverSecond * 0.8;
      break;
    case "boat3_makuri_sashi":
      if (lane === 3) {
        second += 0.22;
        first += events.boat1_hollow ? 0.08 : -0.02;
      }
      if (lane === 1) {
        first += 0.1;
        second += 0.05;
      }
      if (lane === 2 || lane === 4) third += 0.1;
      break;
    case "boat4_cado_attack":
      if (lane === 4) first += 0.24;
      if (lane === 1) {
        first -= 0.09;
        second += 0.14;
      }
      if (lane === 3) third += 0.1;
      if (lane === 4) {
        second += attackCarryoverSecond * 0.65;
        third += attackCarryoverThird * 0.55;
      }
      break;
    case "outer_mix_chaos":
      if (lane >= 3) first += 0.08;
      if (lane === 1) {
        first -= 0.08;
        second += 0.04;
        third += 0.06;
      }
      break;
    default:
      break;
  }

  return {
    first: Math.max(0.05, first),
    second: Math.max(0.05, second),
    third: Math.max(0.05, third)
  };
}

function aggregateScenarioFinishProbabilities(scenarioRows, laneMap, baseProbs, events, escapeScore, laneFinishPriors = LANE_FINISH_PRIORS) {
  const finishProbabilitiesByScenario = [];
  const laneSet = [...new Set([
    ...Object.keys(baseProbs?.first || {}).map((lane) => toInt(lane, null)),
    ...Object.keys(baseProbs?.second || {}).map((lane) => toInt(lane, null)),
    ...Object.keys(baseProbs?.third || {}).map((lane) => toInt(lane, null)),
    ...[...laneMap.keys()]
  ].filter(Number.isInteger))].sort((a, b) => a - b);

  const aggregatedFirst = new Map();
  const aggregatedSecond = new Map();
  const aggregatedThird = new Map();
  const orderMap = new Map();

  for (const scenarioRow of safeArray(scenarioRows)) {
    const scenario = String(scenarioRow?.scenario || "");
    const scenarioProbability = toNum(scenarioRow?.probability, 0);
    const firstRows = [];
    const secondRows = [];
    const thirdRows = [];
    const roleBonusByLane = {};
    for (const lane of laneSet) {
      const row = laneMap.get(lane) || {};
      const roleWeights = roleScenarioWeightsForLane(lane, row, scenario, events, escapeScore);
      roleBonusByLane[String(lane)] = row?.finish_role_bonuses || {
        firstPlaceBonus: 0,
        secondPlaceBonus: 0,
        thirdPlaceBonus: 0
      };
      firstRows.push({
        lane,
        weight: toNum(baseProbs?.first?.[lane], 0) * roleWeights.first * toNum(laneFinishPriors?.first?.[lane], 1)
      });
      secondRows.push({
        lane,
        weight: toNum(baseProbs?.second?.[lane], 0) * roleWeights.second * toNum(laneFinishPriors?.second?.[lane], 1)
      });
      thirdRows.push({
        lane,
        weight: toNum(baseProbs?.third?.[lane], 0) * roleWeights.third * toNum(laneFinishPriors?.third?.[lane], 1)
      });
    }
    const normalizedFirst = normalizeRows(firstRows);
    const normalizedSecond = normalizeRows(secondRows);
    const normalizedThird = normalizeRows(thirdRows);

    finishProbabilitiesByScenario.push({
      scenario,
      probability: scenarioProbability,
      first: normalizedFirst,
      second: normalizedSecond,
      third: normalizedThird,
      role_bonus_by_lane: roleBonusByLane
    });

    for (const row of normalizedFirst) {
      aggregatedFirst.set(row.lane, toNum(aggregatedFirst.get(row.lane), 0) + scenarioProbability * toNum(row.weight, 0));
    }
    for (const row of normalizedSecond) {
      aggregatedSecond.set(row.lane, toNum(aggregatedSecond.get(row.lane), 0) + scenarioProbability * toNum(row.weight, 0));
    }
    for (const row of normalizedThird) {
      aggregatedThird.set(row.lane, toNum(aggregatedThird.get(row.lane), 0) + scenarioProbability * toNum(row.weight, 0));
    }

    const topFirst = normalizedFirst.slice(0, 3);
    const topSecond = normalizedSecond.slice(0, 4);
    const topThird = normalizedThird.slice(0, 5);
    for (const a of topFirst) {
      for (const b of topSecond) {
        for (const c of topThird) {
          const combo = normalizeCombo(`${a.lane}-${b.lane}-${c.lane}`);
          if (!combo) continue;
          const orderProbability = scenarioProbability * toNum(a.weight, 0) * toNum(b.weight, 0) * toNum(c.weight, 0) * 7.8;
          const existing = orderMap.get(combo);
          orderMap.set(combo, {
            combo,
            probability: round(toNum(existing?.probability, 0) + orderProbability, 6),
            scenario_support: [
              ...new Set([...
                safeArray(existing?.scenario_support),
                scenario
              ])
            ]
          });
        }
      }
    }
  }

  return {
    finishProbabilitiesByScenario,
    aggregatedFinishProbabilities: {
      first: normalizeRows([...aggregatedFirst.entries()].map(([lane, weight]) => ({ lane, weight }))),
      second: normalizeRows([...aggregatedSecond.entries()].map(([lane, weight]) => ({ lane, weight }))),
      third: normalizeRows([...aggregatedThird.entries()].map(([lane, weight]) => ({ lane, weight })))
    },
    orderProbabilities: [...orderMap.values()]
      .sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0))
      .slice(0, 24)
  };
}

function buildFieldUsage(meta) {
  return {
    used: !!meta?.is_usable && !!meta?.source,
    source: meta?.source || null,
    confidence: round(toNum(meta?.confidence, 0), 3),
    is_usable: !!meta?.is_usable,
    reason: meta?.reason || (!meta?.source ? "missing" : "skipped"),
    source_section: meta?.source_section || null,
    source_row_label: meta?.source_row_label || null,
    source_period_label: meta?.source_period_label || null,
    source_boat_column: meta?.source_boat_column || null,
    raw_cell_text: meta?.raw_cell_text || null
  };
}

function buildVenueBiasContext({ race, raceFlow, laneContexts }) {
  const windSpeed = Math.max(0, toNum(race?.windSpeed, 0));
  const waveHeight = Math.max(0, toNum(race?.waveHeight, 0));
  const entryChanged = !!raceFlow?.entry_changed;
  const insideLocal = average(safeArray([1, 2, 3]).map((lane) => toNum(laneContexts.find((row) => row.lane === lane)?.lane_fit_local, NaN)));
  const outerLocal = average(safeArray([4, 5, 6]).map((lane) => toNum(laneContexts.find((row) => row.lane === lane)?.lane_fit_local, NaN)));
  const insideStrengthDelta = Number.isFinite(insideLocal) && Number.isFinite(outerLocal) ? insideLocal - outerLocal : 0;
  const venueEscapeBias = clamp(-0.04, 0.08, insideStrengthDelta * 0.012 + (entryChanged ? -0.015 : 0.02) - windSpeed * 0.003);
  const venueOuterAttackBias = clamp(0, 0.08, Math.max(0, -insideStrengthDelta) * 0.01 + Math.max(0, windSpeed - 4) * 0.006 + (String(raceFlow?.formation_pattern || "").includes("outside") ? 0.018 : 0));
  const turn1NarrowPenalty = clamp(0, 0.08, waveHeight * 0.01 + (entryChanged ? 0.015 : 0));
  const strongWindCaution = clamp(0, 0.12, Math.max(0, windSpeed - 5) * 0.02 + waveHeight * 0.01);
  const stabilityBoardBias = clamp(-0.03, 0.08, (windSpeed <= 3 ? 0.03 : 0) + (waveHeight <= 2 ? 0.025 : 0) - (entryChanged ? 0.03 : 0));
  return {
    venue_escape_bias: round(venueEscapeBias, 4),
    venue_outer_attack_bias: round(venueOuterAttackBias, 4),
    turn1_narrow_penalty: round(turn1NarrowPenalty, 4),
    strong_wind_caution: round(strongWindCaution, 4),
    stability_board_bias: round(stabilityBoardBias, 4)
  };
}

function buildStartPatternContext(laneMap) {
  const lane1 = laneMap.get(1) || {};
  const lane2 = laneMap.get(2) || {};
  const lane3 = laneMap.get(3) || {};
  const lane4 = laneMap.get(4) || {};
  const lane12Ahead = toNum(lane1.start_edge, 0) >= 0.03 && toNum(lane2.start_edge, 0) >= 0.02 ? 1 : 0;
  const middleDent = toNum(lane2.late_risk, 0) >= 0.16 && toNum(lane3.start_edge, 0) >= 0.03 ? 1 : 0;
  const twoThreeLate = toNum(lane2.late_risk, 0) >= 0.18 && toNum(lane3.late_risk, 0) >= 0.16 ? 1 : 0;
  const outerAttackWindow = (toNum(lane3.start_edge, 0) >= 0.035 || toNum(lane4.start_edge, 0) >= 0.035) && (toNum(lane3.slit_alert_flag, 0) === 1 || toNum(lane4.slit_alert_flag, 0) === 1 || middleDent === 1) ? 1 : 0;
  return {
    lane12_ahead: lane12Ahead,
    middle_dent: middleDent,
    two_three_late: twoThreeLate,
    outer_attack_window: outerAttackWindow
  };
}

export function buildHitRateEnhancementContext({
  ranking,
  race,
  raceFlow,
  playerStartProfile,
  roleProbabilityLayers,
  confidence
}) {
  const rows = safeArray(ranking);
  const profileByLane = playerStartProfile?.by_lane || {};
  const laneContexts = rows.map((row) => {
    const lane = toInt(row?.racer?.lane, null);
    const features = row?.features || {};
    const profile = profileByLane[String(lane)] || {};
    const styleProfile = profile?.style_profile || {};
    const laneAvgSt = Number.isFinite(features?.avg_st) ? features.avg_st : null;
    const laneStRank = Number.isFinite(features?.avg_st_rank) ? features.avg_st_rank : null;
    const exhibitionSt = Number.isFinite(features?.exhibition_st) ? features.exhibition_st : null;
    const predictionFieldMeta = features?.prediction_field_meta || {};
    const exhibitionTime = predictionFieldMeta?.exhibitionTime?.is_usable && Number.isFinite(features?.exhibition_time)
      ? features.exhibition_time
      : null;
    const laneFit1st = average([
      Number.isFinite(features?.lane_fit_1st) ? features.lane_fit_1st : null,
      Number.isFinite(features?.laneFirstRate) ? features.laneFirstRate : null
    ]);
    const laneFit2ren = average([
      Number.isFinite(features?.lane_fit_2ren) ? features.lane_fit_2ren : null,
      Number.isFinite(features?.lane2RenRate) ? features.lane2RenRate : null
    ]);
    const laneFit3ren = average([
      Number.isFinite(features?.lane_fit_3ren) ? features.lane_fit_3ren : null,
      Number.isFinite(features?.lane3RenRate) ? features.lane3RenRate : null
    ]);
    const predictionDataUsage = {
      lapTime: buildFieldUsage(predictionFieldMeta?.lapTime),
      exhibitionST: buildFieldUsage(predictionFieldMeta?.exhibitionST),
      exhibitionTime: buildFieldUsage(predictionFieldMeta?.exhibitionTime),
      lapExStretch: buildFieldUsage(predictionFieldMeta?.lapExStretch),
      motor2ren: buildFieldUsage(predictionFieldMeta?.motor2ren),
      motor3ren: buildFieldUsage(predictionFieldMeta?.motor3ren),
      lane1stAvg: buildFieldUsage(predictionFieldMeta?.lane1stAvg),
      lane2renAvg: buildFieldUsage(predictionFieldMeta?.lane2renAvg),
      lane3renAvg: buildFieldUsage(predictionFieldMeta?.lane3renAvg),
      fCount: buildFieldUsage(predictionFieldMeta?.fCount)
    };
    const motorTrue = Number.isFinite(features?.motor_true)
      ? features.motor_true
      : (
          toNum(features?.motor_total_score, 0) * 0.6 +
          toNum(features?.motor2_rate, 0) * 0.22 +
          toNum(features?.motor3_rate, 0) * 0.12 +
          Math.max(0, 7 - toNum(features?.lap_time, 7)) * 12
        );
    const launchStateBonus = clamp(
      -0.06,
      0.08,
      toNum(profile?.start_attack_score, 0) / 100 * 0.08 -
      toNum(features?.f_hold_caution_penalty, 0) / 100
    );
    const currentExhibitionEdge = Number.isFinite(exhibitionSt) ? clamp(-0.08, 0.08, (0.18 - exhibitionSt) * 0.55) : 0;
    const startEdge = clamp(
      -0.24,
      0.24,
      (Number.isFinite(laneAvgSt) ? (0.18 - laneAvgSt) * 0.7 : 0) +
      (Number.isFinite(laneStRank) ? (4 - laneStRank) * 0.03 : 0) +
      currentExhibitionEdge +
      launchStateBonus
    );
    const hiddenF = toNum(features?.hidden_f_flag, toNum(features?.f_hold_bias_applied, 0)) > 0 ? 1 : 0;
    const unresolvedFCount = Math.max(0, toNum(features?.unresolved_f_count, features?.f_hold_count));
    const startCautionPenalty = Math.max(
      0,
      toNum(features?.start_caution_penalty, features?.f_hold_caution_penalty)
    );
    const lateRisk = clamp(
      0,
      1,
      (Number.isFinite(laneStRank) && laneStRank >= 4 ? 0.14 : 0.02) +
      (hiddenF ? 0.12 : 0) +
      Math.min(0.18, unresolvedFCount * 0.05) +
      (Number.isFinite(exhibitionSt) && exhibitionSt >= 0.18 ? 0.12 : 0) +
      Math.min(0.18, startCautionPenalty / 100)
    );
    const recentLPenalty =
      predictionFieldMeta?.exhibitionST?.raw_cell_text && /^L/i.test(String(predictionFieldMeta.exhibitionST.raw_cell_text))
        ? 0.08
        : 0;
    const windStartInstability = clamp(0, 0.12, Math.max(0, toNum(race?.windSpeed, 0) - 4) * 0.015);
    const boardStartCaution = clamp(0, 0.1, toNum(race?.waveHeight, 0) * 0.012);
    const stylePressure = buildStylePressure(lane, profile);
    const motivation = buildMotivation(row, race);
    return {
      lane,
      style_profile: styleProfile,
      player_start_profile: profile?.player_start_profile || null,
      lane_avgST: laneAvgSt,
      lane_STrank: laneStRank,
      exhibition_st: exhibitionSt,
      current_exhibition_ST_edge: round(currentExhibitionEdge, 4),
      launch_state_bonus: round(launchStateBonus, 4),
      start_edge: round(startEdge, 4),
      late_risk: round(clamp(0, 1, lateRisk + recentLPenalty + windStartInstability + boardStartCaution), 4),
      hidden_F_flag: hiddenF,
      unresolved_F_count: unresolvedFCount,
      start_caution_penalty: round(startCautionPenalty, 2),
      recent_L_penalty: round(recentLPenalty, 4),
      wind_start_instability: round(windStartInstability, 4),
      board_start_caution: round(boardStartCaution, 4),
      lane_fit_1st: laneFit1st === null ? null : round(laneFit1st, 2),
      lane_fit_2ren: laneFit2ren === null ? null : round(laneFit2ren, 2),
      lane_fit_3ren: laneFit3ren === null ? null : round(laneFit3ren, 2),
      lane_fit_local: Number.isFinite(features?.lane_fit_local) ? round(features.lane_fit_local, 2) : null,
      lane_fit_grade: Number.isFinite(features?.lane_fit_grade) ? round(features.lane_fit_grade, 2) : null,
      prediction_data_usage: predictionDataUsage,
      motor_raw: {
        motor2ren: predictionDataUsage.motor2ren.used && Number.isFinite(features?.motor2_rate) ? round(features.motor2_rate, 2) : null,
        motor3ren: predictionDataUsage.motor3ren.used && Number.isFinite(features?.motor3_rate) ? round(features.motor3_rate, 2) : null
      },
      motor_true: round(motorTrue, 2),
      motor_form: {
        lapTime: predictionDataUsage.lapTime.used && Number.isFinite(features?.lap_time) ? round(features.lap_time, 2) : null,
        lapExStretch: predictionDataUsage.lapExStretch.used && Number.isFinite(features?.lap_exhibition_score) ? round(features.lap_exhibition_score, 2) : null,
        exhibitionTime: Number.isFinite(exhibitionTime) ? round(exhibitionTime, 2) : null
      },
      turning_ability: Number.isFinite(features?.turning_ability) ? round(features.turning_ability, 2) : null,
      straight_line_power: Number.isFinite(features?.straight_line_power) ? round(features.straight_line_power, 2) : null,
      style_pressure: stylePressure,
      slit_alert_flag: toNum(features?.slit_alert_flag, 0),
      motivation_attack: motivation.motivation_attack,
      safe_run_bias: motivation.safe_run_bias
    };
  }).filter((row) => Number.isInteger(row.lane));

  const laneMap = new Map(laneContexts.map((row) => [row.lane, row]));
  const venueBias = buildVenueBiasContext({ race, raceFlow, laneContexts });
  const bestAdjustedExTime = Math.min(
    ...laneContexts
      .map((row) => Number.isFinite(row?.motor_form?.exhibitionTime) ? row.motor_form.exhibitionTime + venueBias.strong_wind_caution * 0.08 : Number.POSITIVE_INFINITY)
  );
  laneContexts.forEach((row) => {
    const exTime = row?.motor_form?.exhibitionTime;
    const windAdjusted = Number.isFinite(exTime) ? round(exTime + venueBias.strong_wind_caution * 0.08, 3) : null;
    const leftLaneRow = row.lane > 1 ? laneMap.get(row.lane - 1) || null : null;
    row.wind_adjusted_ex_time = windAdjusted;
    row.ex_time_relative_gap = Number.isFinite(windAdjusted) && Number.isFinite(bestAdjustedExTime)
      ? round(windAdjusted - bestAdjustedExTime, 3)
      : null;
    row.ex_time_left_gap_advantage =
      leftLaneRow && Number.isFinite(windAdjusted) && Number.isFinite(leftLaneRow?.wind_adjusted_ex_time ?? leftLaneRow?.motor_form?.exhibitionTime)
        ? round((leftLaneRow.wind_adjusted_ex_time ?? leftLaneRow.motor_form.exhibitionTime) - windAdjusted, 3)
        : null;
    row.boat1_ex_time_warning = row.lane === 1 && Number.isFinite(row.ex_time_relative_gap) && row.ex_time_relative_gap >= 0.07 ? 1 : 0;
    row.venue_specific_ex_time_mode = venueBias.strong_wind_caution >= 0.05 ? "wind_sensitive" : "standard";
  });
  laneContexts.forEach((row) => {
    row.finish_role_bonuses = buildRoleSpecificFinishBonuses(row.lane, row, laneMap);
    row.attack_readiness_bonus = round(
      clamp(
        0,
        0.18,
        Math.max(0, toNum(row.finish_role_bonuses?.leftGapAttackSupport, 0)) * 0.42 +
          Math.max(0, toNum(row.finish_role_bonuses?.straightLineDelta, 0)) * 0.04 +
          Math.max(0, toNum(row.finish_role_bonuses?.turningAbilityDelta, 0)) * 0.03 +
          Math.max(
            toNum(row.finish_role_bonuses?.styleRoleFit?.first, 0),
            toNum(row.finish_role_bonuses?.styleRoleFit?.second, 0)
          ) * 0.04
      ),
      4
    );
  });
  const lane1 = laneMap.get(1) || {};
  const startPatternContext = buildStartPatternContext(laneMap);
  const outerAttackPressure = safeArray([3, 4, 5, 6]).reduce((sum, lane) => {
    const row = laneMap.get(lane);
    if (!row) return sum;
    return sum +
      toNum(row.style_pressure, 0) * (lane === 3 || lane === 4 ? 0.0035 : 0.0024) +
      Math.max(0, toNum(row.start_edge, 0)) * (lane === 3 || lane === 4 ? 0.24 : 0.16) +
      toNum(row.hidden_F_flag, 0) * 0.025;
  }, 0);
  const lane2AllowNige = laneMap.get(2)
    ? clamp(0, 0.2, (toNum(laneMap.get(2)?.style_profile?.sashi, 0) / 100) * 0.08 - (toNum(laneMap.get(2)?.style_profile?.makuri, 0) / 100) * 0.04)
    : 0;
  const escapeScore = clamp(
    0,
    1,
    0.18 +
    toNum(lane1.lane_fit_1st, 0) / 100 * 0.34 +
    toNum(lane1.motor_true, 0) / 100 * 0.12 +
    Math.max(0, toNum(lane1.start_edge, 0)) * 0.24 +
    venueBias.venue_escape_bias +
    venueBias.stability_board_bias +
    lane2AllowNige -
    outerAttackPressure * (0.55 + venueBias.venue_outer_attack_bias) -
    venueBias.turn1_narrow_penalty -
    toNum(lane1.boat1_ex_time_warning, 0) * 0.08 -
    toNum(lane1.hidden_F_flag, 0) * 0.08 -
    toNum(lane1.late_risk, 0) * 0.12
  );

  const scenarioProbabilities = [
    {
      scenario: "boat1_escape",
      probability: round(clamp(0, 1, escapeScore + Math.max(0, toNum(lane1.safe_run_bias, 0) - 0.02)), 4)
    },
    {
      scenario: "boat2_sashi",
      probability: round(clamp(
        0,
        1,
        (toNum(laneMap.get(2)?.style_profile?.sashi, 0) / 100) * 0.54 +
        Math.max(0, toNum(laneMap.get(2)?.start_edge, 0)) * 0.55 -
        toNum(laneMap.get(2)?.late_risk, 0) * 0.28 +
        toNum(laneMap.get(2)?.attack_readiness_bonus, 0) * 0.36 +
        (startPatternContext.lane12_ahead ? 0.04 : 0) +
        (1 - escapeScore) * 0.24
      ), 4)
    },
    {
      scenario: "boat2_direct_makuri",
      probability: round(clamp(
        0,
        1,
        (toNum(laneMap.get(2)?.style_profile?.makuri, 0) / 100) * 0.56 +
        Math.max(0, toNum(laneMap.get(2)?.start_edge, 0)) * 0.62 -
        toNum(laneMap.get(2)?.hidden_F_flag, 0) * 0.12 +
        toNum(laneMap.get(2)?.attack_readiness_bonus, 0) * 0.44 +
        (startPatternContext.middle_dent ? 0.06 : 0) +
        Math.max(0, 0.42 - escapeScore) * 0.55
      ), 4)
    },
    {
      scenario: "boat3_makuri",
      probability: round(clamp(
        0,
        1,
        (toNum(laneMap.get(3)?.style_profile?.makuri, 0) / 100) * 0.56 +
        Math.max(0, toNum(laneMap.get(3)?.start_edge, 0)) * 0.6 -
        toNum(laneMap.get(3)?.late_risk, 0) * 0.26 +
        toNum(laneMap.get(3)?.attack_readiness_bonus, 0) * 0.42 +
        (startPatternContext.outer_attack_window ? 0.05 : 0) +
        Math.max(0, 0.45 - escapeScore) * 0.36
      ), 4)
    },
    {
      scenario: "boat3_makuri_sashi",
      probability: round(clamp(
        0,
        1,
        (toNum(laneMap.get(3)?.style_profile?.makuri_sashi, 0) / 100) * 0.6 +
        Math.max(0, toNum(laneMap.get(3)?.start_edge, 0)) * 0.42 -
        toNum(laneMap.get(3)?.late_risk, 0) * 0.18 +
        toNum(laneMap.get(3)?.attack_readiness_bonus, 0) * 0.28 +
        (startPatternContext.two_three_late ? 0.04 : 0)
      ), 4)
    },
    {
      scenario: "boat4_cado_attack",
      probability: round(clamp(
        0,
        1,
        (toNum(laneMap.get(4)?.style_profile?.makuri, 0) / 100) * 0.64 +
        (toNum(laneMap.get(4)?.lane_fit_1st, 0) / 100) * 0.24 +
        Math.max(0, toNum(laneMap.get(4)?.start_edge, 0)) * 0.55 -
        toNum(laneMap.get(4)?.hidden_F_flag, 0) * 0.1 +
        toNum(laneMap.get(4)?.attack_readiness_bonus, 0) * 0.4 +
        venueBias.venue_outer_attack_bias +
        Math.max(0, 0.44 - escapeScore) * 0.28
      ), 4)
    },
    {
      scenario: "outer_mix_chaos",
      probability: round(clamp(
        0,
        1,
        outerAttackPressure * 0.34 +
        safeArray([2, 3, 4, 5, 6]).reduce((sum, lane) => sum + toNum(laneMap.get(lane)?.late_risk, 0), 0) * 0.12 +
        venueBias.strong_wind_caution +
        Math.max(0, 0.34 - escapeScore) * 0.46
      ), 4)
    }
  ];
  const normalizedScenarioProbabilities = normalizeScenarioRows(scenarioProbabilities);
  const intermediateEvents = buildIntermediateEvents(laneMap, escapeScore, outerAttackPressure);
  const laneFinishPriors = {
    first: { ...LANE_FINISH_PRIORS.first },
    second: { ...LANE_FINISH_PRIORS.second },
    third: { ...LANE_FINISH_PRIORS.third }
  };
  laneFinishPriors.first[1] = round(
    clamp(1.18, 1.36, toNum(laneFinishPriors.first[1], 1.22) + 0.08 + escapeScore * 0.12),
    4
  );
  laneFinishPriors.second[1] = round(
    clamp(1.08, 1.22, toNum(laneFinishPriors.second[1], 1.1) + Math.max(0, escapeScore - 0.24) * 0.08),
    4
  );
  laneFinishPriors.third[1] = round(
    clamp(1.02, 1.16, toNum(laneFinishPriors.third[1], 1.04) + Math.max(0, escapeScore - 0.22) * 0.06),
    4
  );
  const baseRoleProbabilities = {
    first: Object.fromEntries(normalizeRows(roleProbabilityLayers?.first_place_probability_json).map((row) => [row.lane, row.weight])),
    second: Object.fromEntries(normalizeRows(roleProbabilityLayers?.second_place_probability_json).map((row) => [row.lane, row.weight])),
    third: Object.fromEntries(normalizeRows(roleProbabilityLayers?.third_place_probability_json).map((row) => [row.lane, row.weight]))
  };
  const finishRoleFramework = buildFinishRoleScores(
    laneContexts,
    laneMap,
    baseRoleProbabilities,
    normalizedScenarioProbabilities,
    escapeScore,
    laneFinishPriors
  );
  const treeAggregation = aggregateScenarioFinishProbabilities(
    normalizedScenarioProbabilities,
    laneMap,
    baseRoleProbabilities,
    intermediateEvents,
    escapeScore,
    laneFinishPriors
  );
  const topExactaCandidates = buildTopExactaCandidates({
    enhancement: { treeOrderProbabilities: treeAggregation.orderProbabilities, stage5_ticketing: { order_probabilities: treeAggregation.orderProbabilities } },
    firstProbs: treeAggregation.aggregatedFinishProbabilities.first,
    secondProbs: treeAggregation.aggregatedFinishProbabilities.second,
    limit: 4
  });

  const darkHorseAlerts = [
    toNum(laneMap.get(4)?.lane_fit_1st, 0) >= 46 && String(rows.find((row) => toInt(row?.racer?.lane, null) === 4)?.racer?.class || "") === "B1"
      ? { lane: 4, type: "4_HEAD_CAUTION", reason: "B1 x 4-course x lane_fit_1st" }
      : null,
    toNum(laneMap.get(6)?.lane_fit_3ren, 0) >= 42
      ? { lane: 6, type: "6_THIRD_CAUTION", reason: "6-course x lane_fit_3ren" }
      : null,
    toNum(laneMap.get(2)?.style_profile?.makuri, 0) >= 58 && toNum(laneMap.get(2)?.start_edge, 0) >= 0.05
      ? { lane: 2, type: "1_COLLAPSE_CAUTION", reason: "2-course direct makuri tendency" }
      : null
  ].filter(Boolean);

  return {
    stage1_static: {
      style_profile_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.style_profile])),
      escape_score: round(escapeScore, 4),
      lane2_allow_nige: round(lane2AllowNige, 4),
      outer_attack_pressure: round(outerAttackPressure, 4),
      venue_bias: venueBias,
      boat1_prior_boost: round(Math.max(0, laneFinishPriors.first[1] - LANE_FINISH_PRIORS.first[1]), 4),
      lane_finish_priors: laneFinishPriors,
      hidden_F_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
        hidden_F_flag: row.hidden_F_flag,
        unresolved_F_count: row.unresolved_F_count,
        start_caution_penalty: row.start_caution_penalty
      }])),
      motor_true_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.motor_true])),
      motivation_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
        motivation_attack: row.motivation_attack,
        safe_run_bias: row.safe_run_bias
      }]))
    },
    stage2_dynamic: {
      start_development_states: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
        lane_avgST: row.lane_avgST,
        lane_STrank: row.lane_STrank,
        exhibition_st: row.exhibition_st,
        exhibition_time: row.motor_form?.exhibitionTime ?? null,
        ex_time_relative_gap: row.ex_time_relative_gap,
        wind_adjusted_ex_time: row.wind_adjusted_ex_time,
        boat1_ex_time_warning: row.boat1_ex_time_warning,
        start_edge: row.start_edge,
        late_risk: row.late_risk,
        hidden_F_flag: row.hidden_F_flag,
        start_caution_penalty: row.start_caution_penalty,
        recent_L_penalty: row.recent_L_penalty,
        wind_start_instability: row.wind_start_instability,
        board_start_caution: row.board_start_caution,
        launch_state_bonus: row.launch_state_bonus,
        style_attack_readiness: round(
          Math.max(
            toNum(row.style_profile?.sashi, 0),
            toNum(row.style_profile?.makuri, 0),
            toNum(row.style_profile?.makuri_sashi, 0)
          ) / 100,
          4
        )
      }])),
      start_edge_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.start_edge])),
      late_risk_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.late_risk])),
      motor_form_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.motor_form])),
      exhibition_time_context_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
        ex_time_relative_gap: row.ex_time_relative_gap,
        wind_adjusted_ex_time: row.wind_adjusted_ex_time,
        boat1_ex_time_warning: row.boat1_ex_time_warning,
        venue_specific_ex_time_mode: row.venue_specific_ex_time_mode
      }])),
      lane_fit_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
        lane_fit_1st: row.lane_fit_1st,
        lane_fit_2ren: row.lane_fit_2ren,
        lane_fit_3ren: row.lane_fit_3ren,
        lane_fit_local: row.lane_fit_local,
        lane_fit_grade: row.lane_fit_grade
      }])),
      finish_role_bonuses_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.finish_role_bonuses])),
      finish_role_scores_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.finish_role_scores])),
      second_place_bonus_breakdown_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.second_place_bonus_breakdown])),
      third_place_bonus_breakdown_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.third_place_bonus_breakdown])),
      third_place_exclusion_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.third_place_exclusion]))
    },
    stage3_scenarios: {
      selected_scenario_probabilities: normalizedScenarioProbabilities,
      intermediate_events: intermediateEvents,
      start_pattern_context: startPatternContext
    },
    stage4_opponents: {
      head_candidate_set: finishRoleFramework.headCandidates.map((row) => row.lane),
      second_candidate_set: normalizeRows(laneContexts.map((row) => ({ lane: row.lane, weight: toNum(row?.finish_role_scores?.second_place_score, 0) }))).slice(0, 4).map((row) => row.lane),
      third_candidate_set: normalizeRows(laneContexts.map((row) => ({ lane: row.lane, weight: toNum(row?.finish_role_scores?.third_place_score, 0) }))).slice(0, 5).map((row) => row.lane),
      compatibility_with_head: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.compatibility_with_head])),
      primary_head_lane: finishRoleFramework.primaryHeadLane,
      escape_sim_support: roleProbabilityLayers?.boat1_escape_probability || null
    },
    stage5_ticketing: {
      selected_ticket_shape: null,
      shape_reason: null,
      finish_probabilities_by_scenario: treeAggregation.finishProbabilitiesByScenario,
      aggregated_finish_probabilities: treeAggregation.aggregatedFinishProbabilities,
      order_probabilities: treeAggregation.orderProbabilities,
      top_exacta_candidates: topExactaCandidates
    },
    confidence: toNum(confidence, 0),
    by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row])),
    dark_horse_alerts: darkHorseAlerts,
    race_flow_summary: raceFlow || null,
    scenarioProbabilities: normalizedScenarioProbabilities,
    finishProbabilitiesByScenario: treeAggregation.finishProbabilitiesByScenario,
    aggregatedFinishProbabilities: treeAggregation.aggregatedFinishProbabilities,
    treeOrderProbabilities: treeAggregation.orderProbabilities,
    topExactaCandidates,
    startDevelopmentStates: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
      start_edge: row.start_edge,
      late_risk: row.late_risk,
      hidden_F_flag: row.hidden_F_flag,
      style_profile: row.style_profile,
      ex_time_relative_gap: row.ex_time_relative_gap,
      wind_adjusted_ex_time: row.wind_adjusted_ex_time,
      recent_L_penalty: row.recent_L_penalty
    }])),
    intermediateEvents,
    venueBias,
    startPatternContext
  };
}

export function applyHitRateEnhancementToProbabilities({
  firstProbs,
  secondProbs,
  thirdProbs,
  enhancement
}) {
  const treeAggregated = enhancement?.aggregatedFinishProbabilities || enhancement?.stage5_ticketing?.aggregated_finish_probabilities;
  if (treeAggregated?.first && treeAggregated?.second && treeAggregated?.third) {
    return {
      first: normalizeRows(treeAggregated.first),
      second: normalizeRows(treeAggregated.second),
      third: normalizeRows(treeAggregated.third)
    };
  }
  const firstMap = mapByLane(firstProbs);
  const secondMap = mapByLane(secondProbs);
  const thirdMap = mapByLane(thirdProbs);
  const laneRows = enhancement?.by_lane || {};
  const escapeScore = toNum(enhancement?.stage1_static?.escape_score, 0);
  const scenarioMap = new Map(safeArray(enhancement?.stage3_scenarios?.selected_scenario_probabilities).map((row) => [row.scenario, toNum(row?.probability, 0)]));
  const primaryHeadLane = toInt(enhancement?.stage4_opponents?.primary_head_lane, 1);

  const applyByRole = (baseMap, role) => normalizeRows([...baseMap.entries()].map(([lane, weight]) => {
    const row = laneRows[String(lane)] || {};
    const lanePrior = toNum(
      enhancement?.stage1_static?.lane_finish_priors?.[role]?.[lane],
      toNum(LANE_FINISH_PRIORS?.[role]?.[lane], 1)
    );
    let multiplier = 1;
    if (role === "first") {
      multiplier +=
        (lane === 1 ? 0.1 + escapeScore * 0.18 : 0) +
        toNum(row.start_edge, 0) * 0.22 +
        (toNum(row.lane_fit_1st, 0) / 100) * 0.18 +
        (toNum(row.motor_true, 0) / 100) * 0.09 +
        toNum(row.finish_role_bonuses?.firstPlaceBonus, 0) * 0.7 +
        toNum(row.motivation_attack, 0) * 0.05 -
        toNum(row.late_risk, 0) * 0.28 -
        toNum(row.hidden_F_flag, 0) * (lane === 1 ? 0.18 : 0.12);
      if (lane === 2) {
        multiplier += scenarioMap.get("boat2_sashi") * 0.12 + scenarioMap.get("boat2_direct_makuri") * 0.16;
      }
      if (lane === 3) {
        multiplier += scenarioMap.get("boat3_makuri") * 0.14 + scenarioMap.get("boat3_makuri_sashi") * 0.14;
      }
      if (lane === 4) {
        multiplier += scenarioMap.get("boat4_cado_attack") * 0.17;
      }
        toNum(row.finish_role_scores?.first_place_score, 0) * 0.2;
    } else if (role === "second") {
      const compatibility = row?.compatibility_with_head?.[String(primaryHeadLane)] || { second_bonus: 0 };
      multiplier +=
        (lane === 1 ? Math.max(0, 0.46 - escapeScore) * 0.24 : 0) +
        toNum(row.start_edge, 0) * 0.12 +
        (toNum(row.lane_fit_2ren, 0) / 100) * 0.22 +
        (toNum(row.motor_raw?.motor2ren, 0) / 100) * 0.18 +
        toNum(row.finish_role_scores?.second_place_score, 0) * 0.26 +
        toNum(row.finish_role_bonuses?.secondPlaceBonus, 0) * 0.7 +
        toNum(compatibility.second_bonus, 0) * 0.6 +
        toNum(row.second_place_bonus_breakdown?.survival_after_attack_bonus, 0) * 0.18 +
        toNum(row.safe_run_bias, 0) * 0.06 -
        toNum(row.late_risk, 0) * 0.18 -
        toNum(row.hidden_F_flag, 0) * 0.06;
      if (lane === 2) multiplier += scenarioMap.get("boat2_sashi") * 0.18;
      if (lane === 4) multiplier += scenarioMap.get("boat4_cado_attack") * 0.08;
    } else {
      const compatibility = row?.compatibility_with_head?.[String(primaryHeadLane)] || { third_bonus: 0 };
      multiplier +=
        (lane === 1 ? Math.max(0, 0.42 - escapeScore) * 0.1 : 0) +
        (toNum(row.lane_fit_3ren, 0) / 100) * 0.26 +
        (toNum(row.motor_raw?.motor3ren, 0) / 100) * 0.2 +
        toNum(row.finish_role_scores?.third_place_score, 0) * 0.28 +
        toNum(row.finish_role_bonuses?.thirdPlaceBonus, 0) * 0.7 +
        toNum(compatibility.third_bonus, 0) * 0.55 +
        toNum(row.third_place_bonus_breakdown?.flow_in_bonus, 0) * 0.2 +
        toNum(row.third_place_bonus_breakdown?.outer_survival_bonus, 0) * 0.16 +
        toNum(row.safe_run_bias, 0) * 0.1 +
        (toNum(row.style_profile?.nuki, 0) / 100) * 0.04 -
        toNum(row.third_place_exclusion?.penalty, 0) * 0.8 -
        toNum(row.late_risk, 0) * 0.12 -
        toNum(row.hidden_F_flag, 0) * 0.03;
    }
    return {
      lane,
      weight: Math.max(0.0001, weight * lanePrior * Math.max(0.55, multiplier))
    };
  }));

  return {
    first: applyByRole(firstMap, "first"),
    second: applyByRole(secondMap, "second"),
    third: applyByRole(thirdMap, "third")
  };
}

export function buildScenarioTreeOrderCandidates(enhancement, confidence) {
  const confFactor = clamp(0.9, 1.08, toNum(confidence, 50) / 64);
  return safeArray(enhancement?.treeOrderProbabilities || enhancement?.stage5_ticketing?.order_probabilities)
    .map((row, index) => ({
      combo: normalizeCombo(row?.combo),
      probability: round(toNum(row?.probability, 0) * confFactor, 4),
      reason_tags: [
        "SCENARIO_TREE",
        ...safeArray(row?.scenario_support).map((scenario) => `SCENARIO_${scenario.toUpperCase()}`)
      ],
      scenario_support: safeArray(row?.scenario_support),
      rank_bonus: round(0.0015 - index * 0.00008, 4)
    }))
    .filter((row) => row.combo)
    .sort((a, b) => toNum(b?.probability, 0) - toNum(a?.probability, 0))
    .slice(0, 18);
}

export function buildEnhancedTrifectaShapeRecommendation({
  firstProbs,
  secondProbs,
  thirdProbs,
  enhancement,
  confidence
}) {
  const firstRows = normalizeRows(firstProbs);
  const secondRows = normalizeRows(secondProbs);
  const thirdRows = normalizeRows(thirdProbs);
  const laneRows = enhancement?.by_lane || {};
  const topFirst = firstRows[0] || { lane: null, weight: 0 };
  const nextFirst = firstRows[1] || { lane: null, weight: 0 };
  const firstDominance = topFirst.weight - nextFirst.weight;
  const escapeScore = toNum(enhancement?.stage1_static?.escape_score, 0);
  const chaosProb = toNum(
    safeArray(enhancement?.stage3_scenarios?.selected_scenario_probabilities).find((row) => row?.scenario === "outer_mix_chaos")?.probability,
    0
  );
  const targetTicketCount = 8;
  const selectedScenarioMap = new Map(
    safeArray(enhancement?.stage3_scenarios?.selected_scenario_probabilities).map((row) => [row.scenario, toNum(row?.probability, 0)])
  );
  const topExacta = safeArray(enhancement?.topExactaCandidates || enhancement?.stage5_ticketing?.top_exacta_candidates);

  const templates = [];
  if (topFirst.lane === 1) {
    const secondLanes = pickTopLanes(secondRows, 2, [1]);
    const secondLanesWide = pickTopLanes(secondRows, 3, [1]);
    const thirdLanes = pickTopLanes(thirdRows, 4, [1]);
    const secondConcentration = secondLanes.reduce((sum, lane) => sum + toNum(laneRows[String(lane)]?.finish_role_scores?.second_place_score, 0), 0);
    const thirdConcentration = thirdLanes.reduce((sum, lane) => sum + toNum(laneRows[String(lane)]?.finish_role_scores?.third_place_score, 0), 0);
    templates.push({
      shape: `1-${secondLanes.join("")}-${[...new Set([...secondLanes, ...thirdLanes])].join("")}`,
      first: [1],
      second: secondLanes,
      third: [...new Set([...secondLanes, ...thirdLanes])],
      why: "boat1 escape with concentrated partners",
      score: 0.78 + escapeScore * 0.42 + firstDominance * 0.32 + secondConcentration * 0.14 + thirdConcentration * 0.1
    });
    templates.push({
      shape: "1-23-2345",
      first: [1],
      second: pickTopLanes(secondRows, 2, [1]),
      third: pickTopLanes(thirdRows, 4, [1]),
      why: "practical 1-fixed spread for stable inside race",
      score: 0.74 + escapeScore * 0.38 + Math.max(0, 0.28 - chaosProb) * 0.16
    });
    templates.push({
      shape: "1-234-234",
      first: [1],
      second: secondLanesWide,
      third: pickTopLanes(thirdRows, 3, [1]),
      why: "practical 1-fixed 6-ticket class shape",
      score: 0.73 + escapeScore * 0.34 + Math.max(0, firstDominance) * 0.18
    });
    if (secondLanes.includes(3)) {
      templates.push({
        shape: "1-3-24",
        first: [1],
        second: [3],
        third: [2, 4],
        why: "boat3 attack support with compact cover",
        score: 0.58 + toNum(secondRows.find((row) => row.lane === 3)?.weight, 0) * 0.3 + toNum(laneRows["3"]?.finish_role_scores?.second_place_score, 0) * 0.22
      });
    }
    templates.push({
      shape: "1-24-234",
      first: [1],
      second: pickTopLanes(secondRows, 2, [1]),
      third: pickTopLanes(thirdRows, 4, [1]),
      why: "boat1 first with wider third survivor coverage",
      score: 0.62 + escapeScore * 0.26
    });
    templates.push({
      shape: "1-2-34",
      first: [1],
      second: [2],
      third: [3, 4],
      why: "stable inside-first tie-break shape",
      score: 0.52 + Math.max(0, 0.5 - chaosProb) * 0.22
    });
    templates.push({
      shape: "1-4-23",
      first: [1],
      second: [4],
      third: [2, 3],
      why: "boat1 survives while outer pressure stays in partner lane",
      score: 0.44 + selectedScenarioMap.get("boat4_cado_attack") * 0.24 + Math.max(0, escapeScore - 0.24) * 0.12 + toNum(laneRows["4"]?.finish_role_scores?.second_place_score, 0) * 0.18
    });
  }
  const lane4HeadWeight = toNum(firstRows.find((row) => row.lane === 4)?.weight, 0);
  if (lane4HeadWeight >= 0.16 || safeArray(enhancement?.dark_horse_alerts).some((row) => row?.type === "4_HEAD_CAUTION")) {
    templates.push({
      shape: "4-1-235",
      first: [4],
      second: [1],
      third: [2, 3, 5],
      why: "4-cado alert support, controlled head hedge",
      score: 0.34 + lane4HeadWeight * 0.45 + toNum(laneRows["1"]?.finish_role_scores?.second_place_score, 0) * 0.12
    });
    templates.push({
      shape: "1-4-235",
      first: [1],
      second: [4],
      third: [2, 3, 5],
      why: "4-cado pressure retained as partner support",
      score: 0.42 + toNum(secondRows.find((row) => row.lane === 4)?.weight, 0) * 0.4 + toNum(laneRows["4"]?.finish_role_scores?.second_place_score, 0) * 0.18
    });
  }

  const chosen = templates
    .filter((row) => safeArray(row.first).length && safeArray(row.second).length && safeArray(row.third).length)
    .map((row) => {
      const combos = [];
      for (const a of row.first) {
        for (const b of row.second) {
          for (const c of row.third) {
            const combo = normalizeCombo(`${a}-${b}-${c}`);
            if (combo) combos.push(combo);
          }
        }
      }
      const expandedTickets = [...new Set(combos)];
      if (expandedTickets.length < 6) {
        const fallbackSeconds = pickTopLanes(secondRows, 4, safeArray(row.first));
        const fallbackThirds = pickTopLanes(thirdRows, 5, safeArray(row.first));
        for (const a of safeArray(row.first)) {
          for (const b of fallbackSeconds) {
            for (const c of fallbackThirds) {
              if (expandedTickets.length >= 8) break;
              const combo = normalizeCombo(`${a}-${b}-${c}`);
              if (combo && !expandedTickets.includes(combo)) expandedTickets.push(combo);
            }
            if (expandedTickets.length >= 8) break;
          }
          if (expandedTickets.length >= 8) break;
        }
      }
      const ticketCount = expandedTickets.length;
      const exactaSupport = expandedTickets.reduce((sum, combo) => {
        const exacta = combo.split("-").slice(0, 2).join("-");
        const exactaRow = topExacta.find((item) => item?.combo === exacta);
        return sum + toNum(exactaRow?.probability, 0);
      }, 0);
      const countPenalty = Math.abs(ticketCount - targetTicketCount) * 0.035;
      return {
        ...row,
        expandedTickets,
        ticketCount,
        score: row.score + exactaSupport * 0.18 - countPenalty
      };
    })
    .sort((a, b) => b.score - a.score)[0] || null;

  if (!chosen) {
    return {
      shape: null,
      selected_shape: null,
      expanded_tickets: [],
      reason_tags: [],
      concentration_metrics: {
        first_place_dominance: round(firstDominance, 4),
        escape_score: round(escapeScore, 4),
        chaos_probability: round(chaosProb, 4),
        confidence: round(confidence, 2)
      }
    };
  }
  return {
    shape: chosen.shape,
    selected_shape: chosen.shape,
    first: chosen.first,
    second: chosen.second,
    third: chosen.third,
    expanded_tickets: chosen.expandedTickets.slice(0, 9),
    reason_tags: [
      "HIT_RATE_ENHANCED_SHAPE",
      topFirst.lane === 1 ? "INSIDE_FIRST_REALISM" : "CONTROLLED_COUNTER",
      chosen.why
    ],
    why_shape_chosen: chosen.why,
    concentration_metrics: {
      first_place_dominance: round(firstDominance, 4),
      escape_score: round(escapeScore, 4),
      chaos_probability: round(chaosProb, 4),
      confidence: round(confidence, 2),
      target_ticket_count: targetTicketCount,
      actual_ticket_count: chosen.ticketCount
    }
  };
}

export function buildEnhancedShapeBasedTrifectaTickets({
  shapeRecommendation,
  firstProbs,
  secondProbs,
  thirdProbs,
  enhancement,
  confidence
}) {
  if (!shapeRecommendation?.selected_shape || !safeArray(shapeRecommendation?.expanded_tickets).length) return [];
  const firstMap = mapByLane(firstProbs);
  const secondMap = mapByLane(secondProbs);
  const thirdMap = mapByLane(thirdProbs);
  const laneRows = enhancement?.by_lane || {};
  const treeOrderMap = new Map(
    safeArray(enhancement?.treeOrderProbabilities || enhancement?.stage5_ticketing?.order_probabilities).map((row) => [
      normalizeCombo(row?.combo),
      toNum(row?.probability, 0)
    ])
  );
  const confidenceFactor = clamp(0.9, 1.12, toNum(confidence, 50) / 60);
  return safeArray(shapeRecommendation.expanded_tickets)
    .map((combo, index) => {
      const normalized = normalizeCombo(combo);
      if (!normalized) return null;
      const [a, b, c] = normalized.split("-").map((value) => toInt(value, null));
      const roleSupport =
        (toNum(laneRows[String(a)]?.lane_fit_1st, 0) / 100) * 0.18 +
        (toNum(laneRows[String(b)]?.lane_fit_2ren, 0) / 100) * 0.14 +
        (toNum(laneRows[String(c)]?.lane_fit_3ren, 0) / 100) * 0.12 +
        (toNum(laneRows[String(c)]?.motor_raw?.motor3ren, 0) / 100) * 0.08;
      const probability = clamp(
        0,
        1,
        (
          toNum(treeOrderMap.get(normalized), 0) * 4.4 +
          toNum(firstMap.get(a), 0) *
          toNum(secondMap.get(b), 0) *
          toNum(thirdMap.get(c), 0) *
          (8.2 + roleSupport * 2.8)
        ) *
          confidenceFactor
      );
      return {
        combo: normalized,
        prob: round(probability + Math.max(0, 0.0014 - index * 0.0001), 4),
        recommended_bet: Math.max(100, Math.round((300 - index * 25) / 100) * 100),
        ticket_type: "shape_main",
        explanation_tags: [...new Set([...
          safeArray(shapeRecommendation.reason_tags),
          `SHAPE_${shapeRecommendation.selected_shape}`
        ])],
        explanation_summary: `Recommended Shape: ${shapeRecommendation.selected_shape}`,
        shape_label: shapeRecommendation.selected_shape,
        shape_rank_bonus: round(0.0014 - index * 0.0001, 4)
      };
    })
    .filter(Boolean)
    .sort((a, b) => toNum(b?.prob, 0) - toNum(a?.prob, 0))
    .slice(0, 10);
}
