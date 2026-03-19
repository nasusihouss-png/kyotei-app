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

function getNormalizedAvgStRank(features) {
  if (Number.isFinite(features?.avg_st_rank)) return features.avg_st_rank;
  if (Number.isFinite(features?.lane_st_rank)) return features.lane_st_rank;
  if (Number.isFinite(features?.lane_STrank)) return features.lane_STrank;
  return null;
}

function buildIppansenLaneSupport(row) {
  const actualLane = actualLaneOf(row);
  const ippansen2renRate = clamp(0, 1, toNum(row?.ippansen_lane_2ren_rate, 0) / 100);
  const ippansen3renRate = clamp(0, 1, toNum(row?.ippansen_lane_3ren_rate, 0) / 100);
  const laneFit2 = clamp(0, 1, toNum(row?.lane_fit_2ren, 0) / 100);
  const laneFit3 = clamp(0, 1, toNum(row?.lane_fit_3ren, 0) / 100);
  const outerResidualBoost = actualLane >= 5 ? 0.03 : actualLane === 4 ? 0.018 : 0;
  const middleLanePartnerBoost = actualLane >= 2 && actualLane <= 4 ? 0.014 : 0;

  return {
    ippansen_2ren_support: round(
      clamp(0, 0.24, ippansen2renRate * 0.16 + ippansen3renRate * 0.05 + laneFit2 * 0.12 + laneFit3 * 0.03 + middleLanePartnerBoost + outerResidualBoost * 0.4),
      4
    ),
    ippansen_3ren_support: round(
      clamp(0, 0.28, ippansen3renRate * 0.2 + ippansen2renRate * 0.05 + laneFit3 * 0.12 + laneFit2 * 0.03 + outerResidualBoost + (actualLane === 3 ? 0.012 : 0)),
      4
    )
  };
}

function buildStDeltaProfile(row) {
  const actualLane = actualLaneOf(row);
  const avgSt = Number.isFinite(row?.lane_avgST) ? row.lane_avgST : null;
  const exhibitionSt = Number.isFinite(row?.exhibition_st) ? row.exhibition_st : null;
  const stDelta =
    Number.isFinite(avgSt) && Number.isFinite(exhibitionSt)
      ? round(avgSt - exhibitionSt, 4)
      : null;

  if (!Number.isFinite(stDelta) || actualLane === 1) {
    return {
      st_delta: stDelta,
      st_delta_bucket: Number.isFinite(stDelta) ? "boat1_neutral" : null,
      attack_st_delta_bonus: 0,
      instability_st_delta_penalty: 0,
      stable_finish_bonus: 0
    };
  }

  let stDeltaBucket = "stable_finish";
  let attackStDeltaBonus = 0;
  let instabilityStDeltaPenalty = 0;
  let stableFinishBonus = 0;

  if (stDelta >= 0.03) {
    stDeltaBucket = "strong_attack";
    attackStDeltaBonus = clamp(0, 0.16, 0.042 + (stDelta - 0.03) * 1.55);
    instabilityStDeltaPenalty = clamp(0, 0.1, 0.018 + (stDelta - 0.03) * 1.05);
  } else if (stDelta >= 0.01) {
    stDeltaBucket = "moderate_attack";
    attackStDeltaBonus = clamp(0, 0.11, 0.018 + (stDelta - 0.01) * 1.45);
    instabilityStDeltaPenalty = clamp(0, 0.05, Math.max(0, stDelta - 0.018) * 0.85);
    stableFinishBonus = clamp(0, 0.03, 0.012 - Math.max(0, stDelta - 0.01) * 0.25);
  } else if (stDelta > -0.01) {
    stDeltaBucket = "stable_finish";
    stableFinishBonus = clamp(0, 0.13, 0.052 - Math.abs(stDelta) * 1.9);
  } else {
    stDeltaBucket = "cautious";
    stableFinishBonus = clamp(0, 0.05, 0.016 - Math.max(0, Math.abs(stDelta) - 0.01) * 0.18);
  }

  return {
    st_delta: stDelta,
    st_delta_bucket: stDeltaBucket,
    attack_st_delta_bonus: round(attackStDeltaBonus, 4),
    instability_st_delta_penalty: round(instabilityStDeltaPenalty, 4),
    stable_finish_bonus: round(stableFinishBonus, 4)
  };
}

function buildMonsterMotorProfile(row) {
  const actualLane = actualLaneOf(row);
  const motor2Rate = row?.prediction_data_usage?.motor2ren?.used ? toNum(row?.motor_raw?.motor2ren, NaN) : NaN;
  if (!Number.isFinite(motor2Rate)) {
    return {
      monster_motor_class: null,
      monster_motor_first_bonus: 0,
      monster_motor_second_bonus: 0,
      monster_motor_attack_support: 0
    };
  }

  let monsterMotorClass = null;
  let baseFirstBonus = 0;
  let baseSecondBonus = 0;
  let baseAttackSupport = 0;
  if (motor2Rate >= 60) {
    monsterMotorClass = "exceptional";
    baseFirstBonus = 0.14;
    baseSecondBonus = 0.085;
    baseAttackSupport = 0.075;
  } else if (motor2Rate >= 55) {
    monsterMotorClass = "very_strong";
    baseFirstBonus = 0.105;
    baseSecondBonus = 0.065;
    baseAttackSupport = 0.055;
  } else if (motor2Rate >= 50) {
    monsterMotorClass = "strong";
    baseFirstBonus = 0.072;
    baseSecondBonus = 0.045;
    baseAttackSupport = 0.038;
  }

  if (!monsterMotorClass) {
    return {
      monster_motor_class: null,
      monster_motor_first_bonus: 0,
      monster_motor_second_bonus: 0,
      monster_motor_attack_support: 0
    };
  }

  const lapSupport = row?.prediction_data_usage?.lapTime?.used
    ? clamp(0, 0.055, Math.max(0, 6.82 - toNum(row?.motor_form?.lapTime, 6.82)) * 0.18)
    : 0;
  const exTimeSupport = Number.isFinite(row?.ex_time_relative_gap)
    ? clamp(0, 0.045, Math.max(0, 0.08 - toNum(row?.ex_time_relative_gap, 1)) * 0.42)
    : 0;
  const exhibitionStSupport = Number.isFinite(row?.exhibition_st)
    ? clamp(0, 0.03, Math.max(0, 0.17 - toNum(row?.exhibition_st, 0.17)) * 0.34)
    : 0;
  const laneSupport = actualLane === 1 ? 0.028 : actualLane === 2 ? 0.022 : actualLane === 3 ? 0.012 : actualLane === 4 ? 0.006 : 0;

  return {
    monster_motor_class: monsterMotorClass,
    monster_motor_first_bonus: round(clamp(0, 0.22, baseFirstBonus + lapSupport + exTimeSupport + exhibitionStSupport + laneSupport), 4),
    monster_motor_second_bonus: round(clamp(0, 0.13, baseSecondBonus + lapSupport * 0.35 + exTimeSupport * 0.28 + laneSupport * 0.55), 4),
    monster_motor_attack_support: round(clamp(0, 0.12, baseAttackSupport + exTimeSupport * 0.46 + exhibitionStSupport * 0.28 + Math.max(0, toNum(row?.attack_st_delta_bonus, 0)) * 0.32), 4)
  };
}

function actualLaneOf(row) {
  return toInt(row?.actual_lane ?? row?.lane, null);
}

function boatNumberOf(row) {
  return toInt(row?.boat_number ?? row?.lane, null);
}

const LANE_FINISH_PRIORS = {
  first: { 1: 1.28, 2: 1.1, 3: 1.02, 4: 0.93, 5: 0.84, 6: 0.77 },
  second: { 1: 1.12, 2: 1.1, 3: 1.03, 4: 0.95, 5: 0.89, 6: 0.84 },
  third: { 1: 1.06, 2: 1.04, 3: 1.02, 4: 0.97, 5: 0.92, 6: 0.88 }
};

const ACTUAL_ENTRY_TUNING = {
  deep_in_escape_penalty: 0.045,
  weak_wall_penalty: 0.028,
  stable_inner_bonus: 0.028,
  actual_two_sashi_boost: 0.18,
  actual_four_cado_boost: 0.18
};

const BOAT3_HEAD_REBALANCE = {
  weak_alignment_gate: 0.72,
  medium_alignment_gate: 0.86,
  strong_alignment_gate: 1,
  boat1_survival_restore: 0.018,
  boat2_second_recovery: 0.065
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

function buildRoleSpecificFinishBonuses(row, actualLaneMap) {
  const actualLane = actualLaneOf(row);
  const leftLane = actualLane > 1 ? actualLaneMap.get(actualLane - 1) || null : null;
  const leftGap = Number.isFinite(row?.ex_time_left_gap_advantage) ? row.ex_time_left_gap_advantage : 0;
  const positiveLeftGap = Math.max(0, leftGap);
  const turningNorm = normalizeAgainstPeers(
    row?.turning_ability,
    [...actualLaneMap.values()].map((entry) => entry?.turning_ability)
  );
  const straightNorm = normalizeAgainstPeers(
    row?.straight_line_power,
    [...actualLaneMap.values()].map((entry) => entry?.straight_line_power)
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
    left_boat_number: boatNumberOf(leftLane),
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
  if (safeRunBias <= 0.025 && nuki < 18 && actualLaneOf(row) >= 5) reasons.push("poor_flow_in_profile");

  const penalty = clamp(0, 0.28, reasons.length * 0.045 + (reasons.includes("weak_lane3ren") ? 0.05 : 0));
  return {
    reasons,
    penalty: round(penalty, 4)
  };
}

function inferScenarioHeadBoat(scenario, escapeScore, actualLaneMap) {
  const lane1Boat = boatNumberOf(actualLaneMap.get(1)) || 1;
  const lane2Boat = boatNumberOf(actualLaneMap.get(2)) || 2;
  const lane3Boat = boatNumberOf(actualLaneMap.get(3)) || 3;
  const lane4Boat = boatNumberOf(actualLaneMap.get(4)) || 4;
  switch (scenario) {
    case "boat2_direct_makuri":
      return escapeScore >= 0.34 ? lane1Boat : lane2Boat;
    case "boat3_makuri":
      return escapeScore >= 0.36 ? lane1Boat : lane3Boat;
    case "boat3_makuri_sashi":
      return lane1Boat;
    case "boat4_cado_attack":
      return escapeScore >= 0.37 ? lane1Boat : lane4Boat;
    case "outer_mix_chaos":
      return lane1Boat;
    case "boat2_sashi":
    case "boat1_escape":
    default:
      return lane1Boat;
  }
}

function buildCompatibilityWithHead(headBoat, row, headRow, escapeScore) {
  if (!Number.isInteger(headBoat) || boatNumberOf(row) === headBoat) {
    return {
      second_bonus: 0,
      third_bonus: 0,
      reasons: []
    };
  }

  const lane = actualLaneOf(row);
  const head = headRow || {};
  const headActualLane = actualLaneOf(head);
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
  const lateRisk = toNum(row?.late_risk, 0);
  const hiddenF = toNum(row?.hidden_F_flag, 0);
  const actualFourSecondCarryover = toNum(row?.actual_four_partner_second_carryover, 0) + toNum(row?.actual_four_self_second_carryover, 0);
  const actualFourThirdCarryover = toNum(row?.actual_four_partner_third_carryover, 0) + toNum(row?.actual_four_self_third_carryover, 0);
  const reasons = [];

  let secondBonus = 0;
  let thirdBonus = 0;

  if (headActualLane === 1) {
    secondBonus += laneFit2 * 0.1 + sashi * 0.12 + makuriSashi * 0.09 + startEdge * 0.35;
    thirdBonus += laneFit3 * 0.08 + nuki * 0.08 + safeRunBias * 0.18;
    if (lane === 2 || lane === 3) {
      secondBonus += lane === 2 ? 0.052 : 0.038;
      reasons.push(lane === 2 ? "head1_to_2_compatibility" : "inner_partner_second");
    }
    if (lane === 4) {
      thirdBonus += 0.03 + attackReadiness * 0.22;
      reasons.push("outer_attack_flow_third");
    }
    if (lane >= 5) {
      thirdBonus += 0.02 + nuki * 0.05;
      reasons.push("outer_residual_third");
    }
  } else if (headActualLane === 4) {
    const collapseRisk = lateRisk >= 0.18 || hiddenF === 1;
    secondBonus += laneFit2 * 0.08 + turning * 0.06 + Math.max(0, 0.04 - Math.abs(lane - 4) * 0.01);
    thirdBonus += laneFit3 * 0.08 + straight * 0.05 + safeRunBias * 0.12;

    if (lane === 3) {
      if (!collapseRisk && toNum(row?.lane3_survival_flag, 0) === 1) {
        secondBonus += 0.055 + actualFourSecondCarryover * 0.95 + Math.max(0, turning) * 0.04;
        reasons.push("head4_lane3_second_flow");
      } else {
        reasons.push(collapseRisk ? "head4_lane3_second_blocked_by_collapse" : "head4_lane3_second_not_stable");
      }
    }
    if (lane === 2) {
      if (laneFit3 >= 0.34 && !collapseRisk && toNum(row?.lane2_flow_in_flag, 0) === 1) {
        thirdBonus += 0.05 + actualFourThirdCarryover * 0.95 + safeRunBias * 0.05;
        reasons.push("head4_lane2_third_residual");
      } else {
        reasons.push("head4_lane2_third_not_strong_enough");
      }
    }
    if (lane === 1) {
      secondBonus += 0.018;
      thirdBonus += 0.022;
      reasons.push("head4_inside_survival");
    }
    if (lane >= 5) {
      thirdBonus += 0.018 + nuki * 0.03;
      reasons.push("head4_outer_residual");
    }
  } else {
    const distance = Math.abs(lane - headActualLane);
    secondBonus += laneFit2 * 0.08 + turning * 0.06 + Math.max(0, 0.045 - distance * 0.01);
    thirdBonus += laneFit3 * 0.08 + straight * 0.05 + safeRunBias * 0.12;
    if (lane < headActualLane) reasons.push("inside_of_head");
    if (lane > headActualLane) reasons.push("outside_residual");
  }

  secondBonus += Math.min(0.08, attackReadiness * 0.5);
  thirdBonus += Math.min(0.07, attackReadiness * 0.28 + nuki * 0.04);
  if (toNum(head?.lane_fit_1st, 0) >= 55 && headActualLane === 1) reasons.push("stable_head_shape");

  return {
    second_bonus: round(clamp(-0.04, 0.24, secondBonus), 4),
    third_bonus: round(clamp(-0.04, 0.22, thirdBonus), 4),
    reasons
  };
}

function buildFinishRoleScores(laneContexts, laneMap, actualLaneMap, baseRoleProbabilities, scenarioRows, escapeScore, laneFinishPriors) {
  const headScenarioSupport = new Map();
  for (const scenarioRow of safeArray(scenarioRows)) {
    const headBoat = inferScenarioHeadBoat(String(scenarioRow?.scenario || ""), escapeScore, actualLaneMap);
    headScenarioSupport.set(headBoat, toNum(headScenarioSupport.get(headBoat), 0) + toNum(scenarioRow?.probability, 0));
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
    const startEdge = Math.max(0, toNum(row?.start_edge, 0));
    const safeRunBias = Math.max(0, toNum(row?.safe_run_bias, 0));
    const ippansen2renSupport = Math.max(0, toNum(row?.ippansen_2ren_support, 0));
    const ippansen3renSupport = Math.max(0, toNum(row?.ippansen_3ren_support, 0));
    const attackStDeltaBonus = Math.max(0, toNum(row?.attack_st_delta_bonus, 0));
    const instabilityStDeltaPenalty = Math.max(0, toNum(row?.instability_st_delta_penalty, 0));
    const stableFinishBonus = Math.max(0, toNum(row?.stable_finish_bonus, 0));
    const monsterMotorFirstBonus = Math.max(0, toNum(row?.monster_motor_first_bonus, 0));
    const monsterMotorSecondBonus = Math.max(0, toNum(row?.monster_motor_second_bonus, 0));
    const likelyHeadSurvivalContext = primaryHeadLane === 1
      ? clamp(0, 0.14, Math.max(0, 0.5 - escapeScore) * 0.12 + laneFit2 * 0.04 + (lane > 1 && lane < 5 ? 0.015 : 0))
      : clamp(0, 0.08, Math.max(0, 0.34 - toNum(headScenarioSupport.get(primaryHeadLane), 0)) * 0.18);
    const attackHeadabilityProxy = clamp(
      0,
      0.86,
      laneFit1 * 0.48 +
        firstStyle * 0.24 +
        startEdge * 0.18 +
        Math.max(0, straight) * 0.08
    );
    const attackButNotWinCarryover = clamp(
      0,
      0.14,
      attackReadiness * 0.48 +
        Math.max(0, straight) * 0.04 +
        Math.max(0, turning) * 0.03
    ) * (1 - attackHeadabilityProxy);
    const survivalAfterAttackBonus = clamp(0, 0.16, attackReadiness * 0.42 + Math.max(0, turning) * 0.05 + Math.max(0, straight) * 0.04);
    const actualLane = actualLaneOf(row);
    const actualFourCaseStrength = toNum(row?.actual_four_attack_case_strength, 0);
    const actualFourSecondCarryover = toNum(row?.actual_four_partner_second_carryover, 0) + toNum(row?.actual_four_self_second_carryover, 0);
    const actualFourThirdCarryover = toNum(row?.actual_four_partner_third_carryover, 0) + toNum(row?.actual_four_self_third_carryover, 0);
    const lapHeadBoost = row?.prediction_data_usage?.lapTime?.used
      ? clamp(0, 0.22, Math.max(0, 6.84 - toNum(row?.motor_form?.lapTime, 6.84)) * 0.85)
      : 0;
    const boat1BaseFirstBonus = boatNumberOf(row) === 1 && actualLane === 1
      ? clamp(
          0,
          0.29,
          0.115 +
            Math.max(0, 0.36 - escapeScore) * 0.11 +
            lapHeadBoost * 0.5 +
            Math.max(0, startEdge) * 0.14 +
            laneFit1 * 0.07 +
            toNum(row?.venue_inside_finish_bias, 0) * 0.65
        )
      : 0;
    const boat1SurvivalBonus = boatNumberOf(row) === 1 && actualLane === 1
      ? clamp(0, 0.18, 0.05 + stableFinishBonus * 0.3 + laneFit2 * 0.05 + laneFit3 * 0.035 + toNum(row?.venue_inside_second_third_bias, 0) * 0.62)
      : 0;
    row.boat1_base_first_bonus = round(boat1BaseFirstBonus, 4);
    row.boat1_survival_bonus = round(boat1SurvivalBonus, 4);
    const tunedAttackButNotWinCarryover = clamp(
      0,
      0.2,
      attackButNotWinCarryover + (actualLane === 4 ? actualFourCaseStrength * 0.18 : 0)
    );
    const tunedSurvivalAfterAttackBonus = clamp(
      0,
      0.22,
      survivalAfterAttackBonus + (actualLane === 4 ? actualFourCaseStrength * 0.14 : 0)
    );
    const insideSecondThirdRecoveryBefore = {
      second: 0,
      third: 0
    };
    const boat2SecondRecoveryBefore = 0;
    const boat2SecondRecoveryAfter = actualLane === 2 ? toNum(row?.actual_boat2_second_recovery, 0) : 0;
    const insideSecondThirdRecoveryAfter = {
      second:
        actualLane === 2 && primaryHeadLane === 1
          ? clamp(0, 0.115, laneFit2 * 0.072 + Math.max(0, turning) * 0.04 + Math.max(0, 0.52 - escapeScore) * 0.02 + toNum(row?.venue_inside_second_third_bias, 0) * 0.76)
          : 0,
      third:
        actualLane === 3 && primaryHeadLane === 1
          ? clamp(0, 0.1, laneFit3 * 0.064 + Math.max(0, turning) * 0.032 + Math.max(0, straight) * 0.022 + toNum(row?.venue_inside_second_third_bias, 0) * 0.66)
          : 0
    };
    row.inside_second_third_recovery = {
      second: round(insideSecondThirdRecoveryAfter.second, 4),
      third: round(insideSecondThirdRecoveryAfter.third, 4)
    };
    const flowInBonus = clamp(0, 0.17, (actualLane >= 4 ? 0.028 : 0) + Math.max(0, turning) * 0.065 + safeRunBias * 0.19);
    const outerSurvivalBonus = clamp(0, 0.15, (actualLane >= 4 ? 0.034 : 0) + Math.max(0, straight) * 0.054 + thirdStyle * 0.062 + ippansen3renSupport * 0.2 + stableFinishBonus * 0.15);
    const residualTendency = clamp(
      0,
      0.17,
      (actualLane >= 5 ? 0.034 : actualLane === 4 ? 0.022 : 0) +
        thirdStyle * 0.084 +
        safeRunBias * 0.17 +
        Math.max(0, turning) * 0.034 +
        ippansen3renSupport * 0.19
    );
    const thirdExclusion = buildThirdPlaceExclusion(row, actualLaneMap);
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
      lapHeadBoost * 0.3 +
      monsterMotorFirstBonus * 0.78 +
      (toNum(row?.motor_true, 0) / 100) * 0.14 +
      startEdge * 0.18 +
      toNum(row?.finish_role_bonuses?.firstPlaceBonus, 0) * 0.34 +
      firstStyle * 0.08 +
      toNum(laneFinishPriors?.first?.[lane], 1) * 0.08 +
      boat1BaseFirstBonus -
      lateRisk * 0.12 -
      hiddenF * (lane === 1 ? 0.1 : 0.08);
    const secondPlaceScoreBeforeTuning =
      toNum(baseRoleProbabilities?.second?.[lane], 0) * 0.34 +
      laneFit2 * 0.28 +
      ippansen2renSupport * 0.34 +
      motor2 * 0.18 +
      monsterMotorSecondBonus * 0.62 +
      Math.max(0, turning) * 0.12 +
      secondStyle * 0.13 +
      likelyHeadSurvivalContext * 0.16 +
      tunedAttackButNotWinCarryover * 0.22 +
      attackStDeltaBonus * 0.26 +
      stableFinishBonus * 0.22 +
      tunedSurvivalAfterAttackBonus * 0.18 +
      toNum(primaryCompatibility?.second_bonus, 0) * 0.26 +
      toNum(row?.finish_role_bonuses?.secondPlaceBonus, 0) * 0.22 -
      instabilityStDeltaPenalty * 0.22 -
      lateRisk * 0.1 -
      hiddenF * 0.04;
    const thirdPlaceScoreBeforeTuning =
      toNum(baseRoleProbabilities?.third?.[lane], 0) * 0.28 +
      laneFit3 * 0.28 +
      ippansen3renSupport * 0.36 +
      motor3Proxy * 0.16 +
      Math.max(0, turning) * 0.13 +
      Math.max(0, straight) * 0.09 +
      flowInBonus * 0.16 +
      outerSurvivalBonus * 0.14 +
      residualTendency * 0.16 +
      tunedAttackButNotWinCarryover * 0.1 +
      attackStDeltaBonus * 0.16 +
      stableFinishBonus * 0.28 +
      thirdStyle * 0.08 +
      toNum(primaryCompatibility?.third_bonus, 0) * 0.18 +
      toNum(row?.finish_role_bonuses?.thirdPlaceBonus, 0) * 0.18 -
      instabilityStDeltaPenalty * 0.16 -
      toNum(thirdExclusion?.penalty, 0);
    const secondPlaceScore = secondPlaceScoreBeforeTuning + actualFourSecondCarryover + insideSecondThirdRecoveryAfter.second + boat2SecondRecoveryAfter + boat1SurvivalBonus * 0.4;
    const thirdPlaceScore = thirdPlaceScoreBeforeTuning + actualFourThirdCarryover + insideSecondThirdRecoveryAfter.third + boat1SurvivalBonus * 0.28;

    row.finish_role_scores_before_tuning = {
      second_place_score: round(Math.max(0.0001, secondPlaceScoreBeforeTuning), 4),
      third_place_score: round(Math.max(0.0001, thirdPlaceScoreBeforeTuning), 4)
    };
    row.finish_role_scores = {
      first_place_score: round(Math.max(0.0001, firstPlaceScore), 4),
      second_place_score: round(Math.max(0.0001, secondPlaceScore), 4),
      third_place_score: round(Math.max(0.0001, thirdPlaceScore), 4),
      lap_head_boost: round(lapHeadBoost, 4),
      monster_motor_class: row?.monster_motor_class || null,
      monster_motor_first_bonus: round(monsterMotorFirstBonus, 4),
      boat1_base_first_bonus: round(boat1BaseFirstBonus, 4),
      boat1_survival_bonus: round(boat1SurvivalBonus, 4),
      survival_after_attack_bonus: round(tunedSurvivalAfterAttackBonus, 4),
      likely_head_survival_context: round(likelyHeadSurvivalContext, 4),
      attack_but_not_win_carryover: round(tunedAttackButNotWinCarryover, 4),
      ippansen_2ren_support: round(ippansen2renSupport, 4),
      ippansen_3ren_support: round(ippansen3renSupport, 4),
      attack_st_delta_bonus: round(attackStDeltaBonus, 4),
      instability_st_delta_penalty: round(instabilityStDeltaPenalty, 4),
      stable_finish_bonus: round(stableFinishBonus, 4),
      flow_in_bonus: round(flowInBonus, 4),
      outer_survival_bonus: round(outerSurvivalBonus, 4),
      residual_tendency: round(residualTendency, 4),
      actual_four_second_carryover: round(actualFourSecondCarryover, 4),
      inside_second_recovery: round(insideSecondThirdRecoveryAfter.second, 4),
      inside_third_recovery: round(insideSecondThirdRecoveryAfter.third, 4),
      inside_second_third_recovery: {
        second: round(insideSecondThirdRecoveryAfter.second, 4),
        third: round(insideSecondThirdRecoveryAfter.third, 4)
      },
      boat2_second_recovery: round(actualLane === 2 ? boat2SecondRecoveryAfter : 0, 4),
      actual_four_third_carryover: round(actualFourThirdCarryover, 4),
      third_place_proxy_used: motor3 > 0 ? "motor3ren" : "survival_proxy",
      primary_head_lane: primaryHeadLane
    };
    row.second_place_bonus_breakdown = {
      lane2renScore: round(laneFit2, 4),
      motor2ren: round(motor2, 4),
      monster_motor_first_bonus: round(monsterMotorFirstBonus, 4),
      turning_bonus: round(Math.max(0, turning), 4),
      style_bonus: round(secondStyle, 4),
      ippansen_2ren_support: round(ippansen2renSupport, 4),
      st_delta: round(toNum(row?.st_delta, 0), 4),
      attack_st_delta_bonus: round(attackStDeltaBonus, 4),
      instability_st_delta_penalty: round(instabilityStDeltaPenalty, 4),
      stable_finish_bonus: round(stableFinishBonus, 4),
      compatibility_with_head: round(toNum(primaryCompatibility?.second_bonus, 0), 4),
      likely_head_survival_context: round(likelyHeadSurvivalContext, 4),
      attack_but_not_win_carryover: round(tunedAttackButNotWinCarryover, 4),
      survival_after_attack_bonus: round(tunedSurvivalAfterAttackBonus, 4),
      actual_four_second_carryover: round(actualFourSecondCarryover, 4),
      boat1_survival_bonus: round(boat1SurvivalBonus, 4),
      inside_second_recovery_before: round(insideSecondThirdRecoveryBefore.second, 4),
      inside_second_recovery_after: round(insideSecondThirdRecoveryAfter.second, 4),
      boat2_second_recovery_before: round(boat2SecondRecoveryBefore, 4),
      boat2_second_recovery_after: round(actualLane === 2 ? boat2SecondRecoveryAfter : 0, 4)
    };
    row.third_place_bonus_breakdown = {
      lane3renScore: round(laneFit3, 4),
      motor3ren_or_proxy: round(motor3Proxy, 4),
      third_place_proxy_used: motor3 > 0 ? "motor3ren" : "survival_proxy",
      ippansen_3ren_support: round(ippansen3renSupport, 4),
      st_delta: round(toNum(row?.st_delta, 0), 4),
      attack_st_delta_bonus: round(attackStDeltaBonus, 4),
      instability_st_delta_penalty: round(instabilityStDeltaPenalty, 4),
      stable_finish_bonus: round(stableFinishBonus, 4),
      turning_bonus: round(Math.max(0, turning), 4),
      straight_retention_bonus: round(Math.max(0, straight), 4),
      attack_but_not_win_carryover: round(tunedAttackButNotWinCarryover, 4),
      flow_in_bonus: round(flowInBonus, 4),
      outer_survival_bonus: round(outerSurvivalBonus, 4),
      residual_tendency: round(residualTendency, 4),
      actual_four_third_carryover: round(actualFourThirdCarryover, 4),
      boat1_survival_bonus: round(boat1SurvivalBonus, 4),
      inside_second_third_recovery_before: insideSecondThirdRecoveryBefore,
      inside_second_third_recovery_after: {
        second: round(insideSecondThirdRecoveryAfter.second, 4),
        third: round(insideSecondThirdRecoveryAfter.third, 4)
      },
      compatibility_with_head: round(toNum(primaryCompatibility?.third_bonus, 0), 4),
      exclusion_penalty: round(toNum(thirdExclusion?.penalty, 0), 4)
    };
    row.first_place_bonus_breakdown = {
      lap_head_boost: round(lapHeadBoost, 4),
      monster_motor_first_bonus: round(monsterMotorFirstBonus, 4),
      boat1_base_first_bonus: round(boat1BaseFirstBonus, 4),
      start_edge: round(startEdge, 4),
      lane1stScore: round(laneFit1, 4),
      inside_lane_prior: round(toNum(laneFinishPriors?.first?.[lane], 1), 4)
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

function buildUpsetSupport({ laneMap, enhancementBase, scenarioRows, aggregatedFinishProbabilities, topExactaCandidates }) {
  const lane1 = laneMap.get(1) || {};
  const lane2 = laneMap.get(2) || {};
  const lane3 = laneMap.get(3) || {};
  const lane4 = laneMap.get(4) || {};
  const scenarioMap = new Map(safeArray(scenarioRows).map((row) => [String(row?.scenario || ""), toNum(row?.probability, 0)]));
  const weakBoat1Factors = {
    weak_lane1stScore: toNum(lane1?.lane_fit_1st, 0) > 0 && toNum(lane1?.lane_fit_1st, 0) < 52,
    unstable_exhibition_st: toNum(lane1?.late_risk, 0) >= 0.18 || toNum(lane1?.exhibition_st, 1) >= 0.17,
    weak_lap_time: lane1?.prediction_data_usage?.lapTime?.used && toNum(lane1?.motor_form?.lapTime, 0) >= 6.79,
    hidden_f_risk: toNum(lane1?.hidden_F_flag, 0) === 1 || toNum(lane1?.unresolved_F_count, 0) > 0,
    weak_escape_score: toNum(enhancementBase?.escape_score, 0) < 0.24
  };
  const strongAttackerFactors = {
    boat2: {
      active:
        scenarioMap.get("boat2_sashi") >= 0.14 ||
        scenarioMap.get("boat2_direct_makuri") >= 0.12,
      sashi_pressure: round(toNum(lane2?.style_profile?.sashi, 0) / 100, 4),
      direct_makuri_pressure: round(toNum(lane2?.style_profile?.makuri, 0) / 100, 4),
      ex_time_left_advantage: round(Math.max(0, toNum(lane2?.ex_time_left_gap_advantage, 0)), 4)
    },
    boat3: {
      active:
        scenarioMap.get("boat3_makuri") >= 0.13 ||
        scenarioMap.get("boat3_makuri_sashi") >= 0.13,
      makuri_pressure: round(toNum(lane3?.style_profile?.makuri, 0) / 100, 4),
      makuri_sashi_pressure: round(toNum(lane3?.style_profile?.makuri_sashi, 0) / 100, 4),
      ex_time_left_advantage: round(Math.max(0, toNum(lane3?.ex_time_left_gap_advantage, 0)), 4)
    },
    boat4: {
      active: scenarioMap.get("boat4_cado_attack") >= 0.12,
      cado_pressure: round(toNum(lane4?.style_profile?.makuri, 0) / 100, 4),
      straight_support: round(Math.max(0, toNum(lane4?.finish_role_bonuses?.straightLineDelta, 0)), 4),
      turning_support: round(Math.max(0, toNum(lane4?.finish_role_bonuses?.turningAbilityDelta, 0)), 4)
    }
  };
  const activeAttackers = [2, 3, 4].filter((lane) => strongAttackerFactors[`boat${lane}`]?.active);
  const chaosFactors = {
    multiple_attackers: activeAttackers.length >= 2,
    inner_collapse_risk: !!enhancementBase?.intermediate_events?.inner_collapse,
    outside_pressure_support: toNum(enhancementBase?.outer_attack_pressure, 0) >= 0.14,
    conflicting_scenario_pressure: scenarioMap.get("boat2_direct_makuri") >= 0.1 && scenarioMap.get("boat3_makuri") >= 0.1
  };
  const weakBoat1Count = Object.values(weakBoat1Factors).filter(Boolean).length;
  const chaosCount = Object.values(chaosFactors).filter(Boolean).length;
  const attackerCount = activeAttackers.length;
  const upsetScore = round(
    weakBoat1Count * 0.22 +
    attackerCount * 0.18 +
    chaosCount * 0.16 +
    Math.max(0, 0.28 - toNum(enhancementBase?.escape_score, 0)) * 1.2,
    4
  );
  const classification =
    upsetScore >= 0.9 || (weakBoat1Count >= 3 && attackerCount >= 2)
      ? "chaotic"
      : upsetScore >= 0.52 || (weakBoat1Count >= 2 && attackerCount >= 1)
        ? "semi-chaotic"
        : "stable";

  const secondMap = new Map(normalizeRows(aggregatedFinishProbabilities?.second).map((row) => [row.lane, row.weight]));
  const thirdMap = new Map(normalizeRows(aggregatedFinishProbabilities?.third).map((row) => [row.lane, row.weight]));
  const firstMap = new Map(normalizeRows(aggregatedFinishProbabilities?.first).map((row) => [row.lane, row.weight]));
  const upsetHeadPool = [2, 3, 4, 5, 6]
    .map((lane) => {
      const row = laneMap.get(lane) || {};
      const laneWeight = lane <= 4 ? 1 : 0.65;
      const rareOuterGate = lane <= 4 || (toNum(firstMap.get(lane), 0) >= 0.09 && toNum(row?.attack_readiness_bonus, 0) >= 0.06);
      if (!rareOuterGate) return null;
      const score =
        toNum(firstMap.get(lane), 0) * 0.5 +
        toNum(row?.finish_role_scores?.first_place_score, 0) * 0.24 +
        toNum(row?.attack_readiness_bonus, 0) * 0.34 +
        toNum(row?.compatibility_with_head?.["1"]?.second_bonus, 0) * 0.08 +
        laneWeight * 0.06;
      return { lane, score: round(score, 4) };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score || a.lane - b.lane)
    .slice(0, classification === "chaotic" ? 3 : classification === "semi-chaotic" ? 2 : 0);

  const upsetScenarios = upsetHeadPool.map((head) => {
    const partners = [1, 2, 3, 4, 5, 6]
      .filter((lane) => lane !== head.lane)
      .map((lane) => {
        const row = laneMap.get(lane) || {};
        return {
          lane,
          score: round(
            toNum(secondMap.get(lane), 0) * 0.42 +
            toNum(row?.finish_role_scores?.second_place_score, 0) * 0.26 +
            toNum(row?.compatibility_with_head?.[String(head.lane)]?.second_bonus, 0) * 0.28 +
            (lane === 1 ? 0.04 : 0),
            4
          )
        };
      })
      .sort((a, b) => b.score - a.score || a.lane - b.lane)
      .slice(0, 2);
    return {
      head_lane: head.lane,
      head_score: head.score,
      partner_lanes: partners.map((row) => row.lane),
      exacta_pairs: partners.map((row) => `${head.lane}-${row.lane}`)
    };
  });

  const upsetExactaPairs = [...new Set(
    upsetScenarios.flatMap((row) => row.exacta_pairs)
  )]
    .map((combo) => {
      const topRow = safeArray(topExactaCandidates).find((row) => row?.combo === combo);
      const [a, b] = combo.split("-").map((value) => toInt(value, null));
      return {
        combo,
        probability: round(
          Math.max(
            toNum(topRow?.probability, 0),
            toNum(firstMap.get(a), 0) * toNum(secondMap.get(b), 0) * 1.6
          ),
          4
        ),
        source: topRow?.source || "upset_support"
      };
    })
    .sort((a, b) => b.probability - a.probability)
    .slice(0, classification === "chaotic" ? 4 : classification === "semi-chaotic" ? 3 : 0);

  const upsetTrifectaTickets = [];
  const upsetThirdCandidateScores = new Map();
  for (const scenario of upsetScenarios) {
    const thirdCandidates = [1, 2, 3, 4, 5, 6]
      .filter((lane) => lane !== scenario.head_lane && !scenario.partner_lanes.includes(lane))
      .map((lane) => ({
        lane,
        score: round(
          toNum(thirdMap.get(lane), 0) * 0.42 +
          toNum(laneMap.get(lane)?.finish_role_scores?.third_place_score, 0) * 0.26 +
          toNum(laneMap.get(lane)?.compatibility_with_head?.[String(scenario.head_lane)]?.third_bonus, 0) * 0.22 -
          toNum(laneMap.get(lane)?.third_place_exclusion?.penalty, 0) * 0.32,
          4
        )
      }))
      .sort((a, b) => b.score - a.score || a.lane - b.lane)
      .slice(0, classification === "chaotic" ? 2 : 1);

    for (const thirdRow of thirdCandidates) {
      upsetThirdCandidateScores.set(
        thirdRow.lane,
        round(Math.max(toNum(upsetThirdCandidateScores.get(thirdRow.lane), 0), toNum(thirdRow.score, 0)), 4)
      );
    }

    for (const secondLane of scenario.partner_lanes) {
      for (const thirdLane of thirdCandidates.map((row) => row.lane)) {
        const combo = normalizeCombo(`${scenario.head_lane}-${secondLane}-${thirdLane}`);
        if (combo) upsetTrifectaTickets.push(combo);
      }
    }
  }
  const compactUpsetTrifecta = [...new Set(upsetTrifectaTickets)].slice(0, classification === "chaotic" ? 5 : classification === "semi-chaotic" ? 3 : 0);
  const bigUpsetProbability = round(clamp(0, 0.65, upsetScore * 0.24 + (classification === "chaotic" ? 0.14 : classification === "semi-chaotic" ? 0.06 : 0.01)), 4);
  const firstCandidates = upsetHeadPool.map((row) => row.lane).slice(0, classification === "chaotic" ? 3 : classification === "semi-chaotic" ? 2 : 1);
  const secondCandidates = [...new Set(
    upsetScenarios
      .flatMap((row) => safeArray(row?.partner_lanes))
      .concat(toNum(secondMap.get(1), 0) >= 0.12 ? [1] : [])
  )]
    .slice(0, classification === "chaotic" ? 4 : classification === "semi-chaotic" ? 3 : 2);
  const thirdCandidates = [...new Set(
    [...upsetThirdCandidateScores.entries()]
      .sort((a, b) => b[1] - a[1] || a[0] - b[0])
      .map(([lane]) => lane)
      .concat(
        normalizeRows(aggregatedFinishProbabilities?.third)
          .map((row) => row.lane)
      )
  )]
    .filter((lane) => !firstCandidates.includes(lane) || lane === 1)
    .slice(0, classification === "chaotic" ? 5 : classification === "semi-chaotic" ? 4 : 3);
  const formationString =
    firstCandidates.length && secondCandidates.length && thirdCandidates.length
      ? `${firstCandidates.join("")}-${secondCandidates.join("")}-${thirdCandidates.join("")}`
      : null;

  return {
    classification,
    upset_score: upsetScore,
    big_upset_probability: bigUpsetProbability,
    weak_boat1_factors: weakBoat1Factors,
    strong_attacker_factors: strongAttackerFactors,
    chaos_factors: chaosFactors,
    chosen_upset_heads: upsetScenarios,
    upset_exacta_pairs: upsetExactaPairs,
    upset_trifecta_tickets: compactUpsetTrifecta.map((combo, index) => ({
      combo,
      rank: index + 1,
      bucket: classification === "chaotic" ? "big_upset" : "medium_upset"
    })),
    medium_upset: {
      shown: classification === "semi-chaotic",
      exacta_pairs: classification === "semi-chaotic" ? upsetExactaPairs.slice(0, 3) : [],
      trifecta_tickets: classification === "semi-chaotic" ? compactUpsetTrifecta.slice(0, 3) : []
    },
    big_upset: {
      shown: classification === "chaotic",
      exacta_pairs: classification === "chaotic" ? upsetExactaPairs.slice(0, 4) : [],
      trifecta_tickets: classification === "chaotic" ? compactUpsetTrifecta.slice(0, 5) : []
    },
    upset_formation: {
      first_candidates: firstCandidates,
      second_candidates: secondCandidates,
      third_candidates: thirdCandidates,
      formation_string: formationString
    }
  };
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

function buildIntermediateEvents(actualLaneMap, escapeScore, outerAttackPressure) {
  const lane1 = actualLaneMap.get(1) || {};
  const lane2 = actualLaneMap.get(2) || {};
  const lane3 = actualLaneMap.get(3) || {};
  const lane4 = actualLaneMap.get(4) || {};
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
    actual_lane1_boat: boatNumberOf(lane1),
    actual_lane2_boat: boatNumberOf(lane2),
    actual_lane4_boat: boatNumberOf(lane4),
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

function roleScenarioWeightsForLane(lane, row, scenario, events, escapeScore, actualLaneMap) {
  let first = 1;
  let second = 1;
  let third = 1;
  const actualLane = actualLaneOf(row);
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
  const headBoat = inferScenarioHeadBoat(scenario, escapeScore, actualLaneMap);
  const compatibility = row?.compatibility_with_head?.[String(headBoat)] || {
    second_bonus: 0,
    third_bonus: 0
  };
  const attackCarryoverSecond = clamp(0, 0.12, toNum(row?.attack_readiness_bonus, 0) * 0.46);
  const attackCarryoverThird = clamp(0, 0.1, toNum(row?.attack_readiness_bonus, 0) * 0.28);
  const thirdExclusionPenalty = toNum(row?.third_place_exclusion?.penalty, 0);
  const ippansen2renSupport = toNum(row?.ippansen_2ren_support, 0);
  const ippansen3renSupport = toNum(row?.ippansen_3ren_support, 0);
  const attackStDeltaBonus = toNum(row?.attack_st_delta_bonus, 0);
  const instabilityStDeltaPenalty = toNum(row?.instability_st_delta_penalty, 0);
  const stableFinishBonus = toNum(row?.stable_finish_bonus, 0);
  const monsterMotorFirstBonus = toNum(row?.monster_motor_first_bonus, 0);
  const boat1BaseFirstBonus = boatNumberOf(row) === 1 && actualLane === 1
    ? toNum(row?.boat1_base_first_bonus, 0)
    : 0;
  const boat1SurvivalBonus = boatNumberOf(row) === 1 && actualLane === 1
    ? toNum(row?.boat1_survival_bonus, 0)
    : 0;

  first += laneFit1 * 0.3 + motorTrue * 0.12 + lapBoost * 0.22 + monsterMotorFirstBonus * 0.62 + startEdge * 0.72 + boat1BaseFirstBonus - lateRisk * 0.28 - hiddenF * 0.16;
  second += laneFit2 * 0.34 + ippansen2renSupport * 0.3 + motor2 * 0.22 + startEdge * 0.32 + stretchBoost * 0.15 + attackStDeltaBonus * 0.16 + stableFinishBonus * 0.14 + boat1SurvivalBonus * 0.42 - instabilityStDeltaPenalty * 0.12 - lateRisk * 0.16 - hiddenF * 0.06;
  third += laneFit3 * 0.34 + ippansen3renSupport * 0.36 + motor3 * 0.24 + stretchBoost * 0.22 + toNum(row?.safe_run_bias, 0) * 0.4 + attackStDeltaBonus * 0.08 + stableFinishBonus * 0.2 + boat1SurvivalBonus * 0.3 - instabilityStDeltaPenalty * 0.08 - lateRisk * 0.12 - hiddenF * 0.03;
  first += toNum(roleBonus.firstPlaceBonus, 0);
  second += toNum(roleBonus.secondPlaceBonus, 0);
  third += toNum(roleBonus.thirdPlaceBonus, 0);
  first += toNum(finishRoleScores.first_place_score, 0) * 0.22;
  second += toNum(finishRoleScores.second_place_score, 0) * 0.3 + toNum(compatibility.second_bonus, 0) + attackCarryoverSecond;
  third += toNum(finishRoleScores.third_place_score, 0) * 0.34 + toNum(compatibility.third_bonus, 0) + attackCarryoverThird - thirdExclusionPenalty;

  if (actualLane === 1) first += 0.2 + escapeScore * 0.16;

  switch (scenario) {
    case "boat1_escape":
      if (actualLane === 1) first += 0.32;
      if (actualLane === 2 || actualLane === 3) second += actualLane === 2 ? 0.15 : 0.12;
      if (actualLane >= 2 && actualLane <= 4) third += 0.12 + ippansen3renSupport * 0.06 + stableFinishBonus * 0.05;
      if (actualLane >= 4) third += ippansen3renSupport * 0.08;
      break;
    case "boat2_sashi":
      if (actualLane === 2) {
        second += 0.26 + attackStDeltaBonus * 0.08;
        first += events.boat1_hollow ? 0.06 : -0.04;
      }
      if (actualLane === 1) {
        first += 0.16;
        second += 0.08;
      }
      if (actualLane === 3 || actualLane === 4) third += 0.1 + stableFinishBonus * 0.04;
      break;
    case "boat2_direct_makuri":
      if (actualLane === 2) first += 0.22;
      if (actualLane === 1) {
        first -= 0.16;
        second += 0.14;
      }
      if (actualLane === 3 || actualLane === 4) second += 0.1 + attackStDeltaBonus * 0.06;
      break;
    case "boat3_makuri":
      if (actualLane === 3) first += 0.26;
      if (actualLane === 1) {
        first -= 0.12;
        second += 0.15;
      }
      if (actualLane === 2) {
        second += 0.06 + stableFinishBonus * 0.04;
        third += 0.12 + ippansen3renSupport * 0.04;
      }
      if (actualLane === 3) second += attackCarryoverSecond * 0.8 + attackStDeltaBonus * 0.08;
      break;
    case "boat3_makuri_sashi":
      if (actualLane === 3) {
        second += 0.22;
        first += events.boat1_hollow ? 0.08 : -0.02;
      }
      if (actualLane === 1) {
        first += 0.1;
        second += 0.05;
      }
      if (actualLane === 2 || actualLane === 4) third += 0.1 + stableFinishBonus * 0.05;
      break;
    case "boat4_cado_attack":
      if (actualLane === 4) first += 0.24;
      if (actualLane === 1) {
        first -= 0.09;
        second += 0.14;
      }
      if (actualLane === 3) third += 0.1 + stableFinishBonus * 0.04;
      if (actualLane === 4) {
        second += attackCarryoverSecond * 0.65 + attackStDeltaBonus * 0.08;
        third += attackCarryoverThird * 0.55 + ippansen3renSupport * 0.05;
      }
      break;
    case "outer_mix_chaos":
      if (actualLane >= 3) first += 0.08;
      if (actualLane === 1) {
        first -= 0.08;
        second += 0.04;
        third += 0.06;
      }
      if (actualLane >= 4) {
        second += attackStDeltaBonus * 0.05;
        third += ippansen3renSupport * 0.08 + stableFinishBonus * 0.04;
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

function aggregateScenarioFinishProbabilities(scenarioRows, laneMap, actualLaneMap, baseProbs, events, escapeScore, laneFinishPriors = LANE_FINISH_PRIORS) {
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
      const roleWeights = roleScenarioWeightsForLane(lane, row, scenario, events, escapeScore, actualLaneMap);
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

function normalizeVenueName(value) {
  return String(value || "").trim().toLowerCase();
}

function isStrongInsideVenue(value) {
  const normalized = normalizeVenueName(value);
  return [
    "omura",
    "tokuyama",
    "ashiya",
    "suminoe",
    "wakamatsu",
    "tamagawa",
    "大村",
    "徳山",
    "芦屋",
    "住之江",
    "若松",
    "多摩川"
  ].includes(normalized);
}

function buildVenueBiasContext({ race, raceFlow, laneContexts }) {
  const windSpeed = Math.max(0, toNum(race?.windSpeed, 0));
  const waveHeight = Math.max(0, toNum(race?.waveHeight, 0));
  const entryChanged = !!raceFlow?.entry_changed;
  const venueName = race?.venueName || race?.stadiumName || race?.venue || null;
  const strongInsideVenue = isStrongInsideVenue(venueName);
  const insideLocal = average(safeArray([1, 2, 3]).map((lane) => toNum(laneContexts.find((row) => actualLaneOf(row) === lane)?.lane_fit_local, NaN)));
  const outerLocal = average(safeArray([4, 5, 6]).map((lane) => toNum(laneContexts.find((row) => actualLaneOf(row) === lane)?.lane_fit_local, NaN)));
  const insideStrengthDelta = Number.isFinite(insideLocal) && Number.isFinite(outerLocal) ? insideLocal - outerLocal : 0;
  const venueEscapeBias = clamp(
    -0.04,
    0.095,
    insideStrengthDelta * 0.012 +
      (entryChanged ? -0.015 : 0.02) -
      windSpeed * 0.003 +
      (strongInsideVenue ? 0.022 : 0)
  );
  const venueInsideFinishBias = clamp(
    0,
    0.07,
    Math.max(0, insideStrengthDelta) * 0.008 +
      (entryChanged ? 0 : 0.012) +
      (strongInsideVenue ? 0.026 : 0.01)
  );
  const venueInsideSecondThirdBias = clamp(
    0,
    0.055,
    Math.max(0, insideStrengthDelta) * 0.006 +
      (entryChanged ? 0 : 0.008) +
      (strongInsideVenue ? 0.02 : 0.008)
  );
  const venueOuterAttackBias = clamp(0, 0.08, Math.max(0, -insideStrengthDelta) * 0.01 + Math.max(0, windSpeed - 4) * 0.006 + (String(raceFlow?.formation_pattern || "").includes("outside") ? 0.018 : 0));
  const turn1NarrowPenalty = clamp(0, 0.08, waveHeight * 0.01 + (entryChanged ? 0.015 : 0));
  const strongWindCaution = clamp(0, 0.12, Math.max(0, windSpeed - 5) * 0.02 + waveHeight * 0.01);
  const stabilityBoardBias = clamp(-0.03, 0.08, (windSpeed <= 3 ? 0.03 : 0) + (waveHeight <= 2 ? 0.025 : 0) - (entryChanged ? 0.03 : 0));
  return {
    venue_name: venueName,
    strong_inside_venue: strongInsideVenue ? 1 : 0,
    venue_escape_bias: round(venueEscapeBias, 4),
    venue_inside_finish_bias: round(venueInsideFinishBias, 4),
    venue_inside_second_third_bias: round(venueInsideSecondThirdBias, 4),
    venue_outer_attack_bias: round(venueOuterAttackBias, 4),
    turn1_narrow_penalty: round(turn1NarrowPenalty, 4),
    strong_wind_caution: round(strongWindCaution, 4),
    stability_board_bias: round(stabilityBoardBias, 4)
  };
}

function buildStartPatternContext(actualLaneMap) {
  const lane1 = actualLaneMap.get(1) || {};
  const lane2 = actualLaneMap.get(2) || {};
  const lane3 = actualLaneMap.get(3) || {};
  const lane4 = actualLaneMap.get(4) || {};
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
    const actualLane = toInt(row?.features?.actual_lane ?? row?.racer?.entryCourse ?? lane, lane);
    const features = row?.features || {};
    const profile = profileByLane[String(actualLane)] || {};
    const styleProfile = profile?.style_profile || {};
    const laneAvgSt = Number.isFinite(features?.avg_st) ? features.avg_st : null;
    const avgStRank = getNormalizedAvgStRank(features);
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
    const ippansenLane2renRate = Number.isFinite(row?.racer?.lane2renRate_ippansen)
      ? row.racer.lane2renRate_ippansen
      : Number.isFinite(features?.lane2renRate_ippansen)
        ? features.lane2renRate_ippansen
        : null;
    const ippansenLane3renRate = Number.isFinite(row?.racer?.lane3renRate_ippansen)
      ? row.racer.lane3renRate_ippansen
      : Number.isFinite(features?.lane3renRate_ippansen)
        ? features.lane3renRate_ippansen
        : null;
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
      (Number.isFinite(avgStRank) ? (4 - avgStRank) * 0.03 : 0) +
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
      (Number.isFinite(avgStRank) && avgStRank >= 4 ? 0.14 : 0.02) +
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
    const stylePressure = buildStylePressure(actualLane, profile);
    const motivation = buildMotivation(row, race);
    return {
      lane,
      boat_number: lane,
      actual_lane: actualLane,
      course_change_occurred: actualLane !== lane,
      style_profile: styleProfile,
      player_start_profile: profile?.player_start_profile || null,
      lane_avgST: laneAvgSt,
      avg_st_rank: avgStRank,
      lane_st_rank: avgStRank,
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
      ippansen_lane_2ren_rate: ippansenLane2renRate === null ? null : round(ippansenLane2renRate, 2),
      ippansen_lane_3ren_rate: ippansenLane3renRate === null ? null : round(ippansenLane3renRate, 2),
      ippansen_2ren_support: 0,
      ippansen_3ren_support: 0,
      st_delta: null,
      st_delta_bucket: null,
      attack_st_delta_bonus: 0,
      instability_st_delta_penalty: 0,
      stable_finish_bonus: 0,
      monster_motor_class: null,
      monster_motor_first_bonus: 0,
      lane_fit_local: Number.isFinite(features?.lane_fit_local) ? round(features.lane_fit_local, 2) : null,
      lane_fit_grade: Number.isFinite(features?.lane_fit_grade) ? round(features.lane_fit_grade, 2) : null,
      prediction_data_usage: predictionDataUsage,
      lapTime_source: predictionDataUsage.lapTime,
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
  const actualLaneMap = new Map(laneContexts.map((row) => [row.actual_lane, row]));
  const venueBias = buildVenueBiasContext({ race, raceFlow, laneContexts });
  const bestAdjustedExTime = Math.min(
    ...laneContexts
      .map((row) => Number.isFinite(row?.motor_form?.exhibitionTime) ? row.motor_form.exhibitionTime + venueBias.strong_wind_caution * 0.08 : Number.POSITIVE_INFINITY)
  );
  laneContexts.forEach((row) => {
    const exTime = row?.motor_form?.exhibitionTime;
    const windAdjusted = Number.isFinite(exTime) ? round(exTime + venueBias.strong_wind_caution * 0.08, 3) : null;
    const leftLaneRow = row.actual_lane > 1 ? actualLaneMap.get(row.actual_lane - 1) || null : null;
    row.wind_adjusted_ex_time = windAdjusted;
    row.ex_time_relative_gap = Number.isFinite(windAdjusted) && Number.isFinite(bestAdjustedExTime)
      ? round(windAdjusted - bestAdjustedExTime, 3)
      : null;
    row.ex_time_left_gap_advantage =
      leftLaneRow && Number.isFinite(windAdjusted) && Number.isFinite(leftLaneRow?.wind_adjusted_ex_time ?? leftLaneRow?.motor_form?.exhibitionTime)
        ? round((leftLaneRow.wind_adjusted_ex_time ?? leftLaneRow.motor_form.exhibitionTime) - windAdjusted, 3)
        : null;
    row.boat1_ex_time_warning = row.actual_lane === 1 && Number.isFinite(row.ex_time_relative_gap) && row.ex_time_relative_gap >= 0.07 ? 1 : 0;
    row.venue_specific_ex_time_mode = venueBias.strong_wind_caution >= 0.05 ? "wind_sensitive" : "standard";
  });
  laneContexts.forEach((row) => {
    row.venue_name = venueBias.venue_name || null;
    row.venue_escape_bias = venueBias.venue_escape_bias;
    row.venue_inside_finish_bias = venueBias.venue_inside_finish_bias;
    row.venue_inside_second_third_bias = venueBias.venue_inside_second_third_bias;
    row.finish_role_bonuses = buildRoleSpecificFinishBonuses(row, actualLaneMap);
    Object.assign(row, buildIppansenLaneSupport(row));
    Object.assign(row, buildStDeltaProfile(row));
    Object.assign(row, buildMonsterMotorProfile(row));
    row.attack_readiness_bonus = round(
      clamp(
        0,
        0.22,
        Math.max(0, toNum(row.finish_role_bonuses?.leftGapAttackSupport, 0)) * 0.42 +
          Math.max(0, toNum(row.finish_role_bonuses?.straightLineDelta, 0)) * 0.04 +
          Math.max(0, toNum(row.finish_role_bonuses?.turningAbilityDelta, 0)) * 0.03 +
          toNum(row.attack_st_delta_bonus, 0) * 0.65 -
          toNum(row.instability_st_delta_penalty, 0) * 0.22 +
          toNum(row.monster_motor_attack_support, 0) * 0.48 +
          Math.max(
            toNum(row.finish_role_bonuses?.styleRoleFit?.first, 0),
            toNum(row.finish_role_bonuses?.styleRoleFit?.second, 0)
          ) * 0.04
      ),
      4
    );
  });
  const boat1Row = laneMap.get(1) || {};
  const actualLane1Row = actualLaneMap.get(1) || {};
  const actualLane2Row = actualLaneMap.get(2) || {};
  const actualLane3Row = actualLaneMap.get(3) || {};
  const actualLane4Row = actualLaneMap.get(4) || {};
  const entryStructureChanged = laneContexts.some((row) => row.course_change_occurred);
  const startPatternContext = buildStartPatternContext(actualLaneMap);
  const outerAttackPressure = safeArray([3, 4, 5, 6]).reduce((sum, lane) => {
    const row = actualLaneMap.get(lane);
    if (!row) return sum;
    return sum +
      toNum(row.style_pressure, 0) * (lane === 3 || lane === 4 ? 0.0035 : 0.0024) +
      Math.max(0, toNum(row.start_edge, 0)) * (lane === 3 || lane === 4 ? 0.24 : 0.16) +
      toNum(row.monster_motor_attack_support, 0) * (lane === 3 || lane === 4 ? 0.26 : 0.18) +
      toNum(row.hidden_F_flag, 0) * 0.025;
  }, 0);
  const lane2AllowNige = actualLane2Row
    ? clamp(0, 0.2, (toNum(actualLane2Row?.style_profile?.sashi, 0) / 100) * 0.08 - (toNum(actualLane2Row?.style_profile?.makuri, 0) / 100) * 0.04)
    : 0;
  const weakWallPenalty = actualLane2Row && actualLane2Row.boat_number !== 2 ? ACTUAL_ENTRY_TUNING.weak_wall_penalty : 0;
  const stableInnerBonus =
    actualLane2Row && actualLane3Row && toNum(actualLane2Row.late_risk, 0) < 0.15 && toNum(actualLane3Row.late_risk, 0) < 0.16
      ? ACTUAL_ENTRY_TUNING.stable_inner_bonus
      : 0;
  const deepInDepthRisk = Math.max(0, toNum(boat1Row.actual_lane, 1) - 1);
  const deepWeakStartPressure = clamp(
    0,
    0.08,
    Math.max(0, 0.03 - Math.max(0, toNum(actualLane1Row.start_edge, 0))) * 1.3 +
      toNum(actualLane1Row.late_risk, 0) * 0.12
  );
  const weakActualTwoPressure = clamp(
    0,
    0.08,
    (toNum(actualLane2Row?.style_profile?.sashi, 0) / 100) * 0.05 +
      Math.max(0, toNum(actualLane2Row?.start_edge, 0)) * 0.08
  );
  const deepOutsidePressure = clamp(0, 0.08, outerAttackPressure * 0.22);
  const deepMitigationLap = boat1Row?.prediction_data_usage?.lapTime?.used
    ? clamp(0, 0.05, Math.max(0, 6.82 - toNum(boat1Row?.motor_form?.lapTime, 6.82)) * 0.45)
    : 0;
  const deepMitigationMotor = boat1Row?.prediction_data_usage?.motor2ren?.used
    ? clamp(0, 0.04, (toNum(boat1Row?.motor_raw?.motor2ren, 0) / 100) * 0.05)
    : 0;
  const weakActualTwoAttacker = clamp(
    0,
    0.03,
    Math.max(0, 0.08 - (toNum(actualLane2Row?.attack_readiness_bonus, 0) * 0.25 + Math.max(0, toNum(actualLane2Row?.start_edge, 0)) * 0.18))
  );
  const deepInEscapePenalty = deepInDepthRisk > 0
    ? clamp(
        0,
        0.16,
        deepInDepthRisk * ACTUAL_ENTRY_TUNING.deep_in_escape_penalty +
          deepWeakStartPressure +
          weakActualTwoPressure +
          deepOutsidePressure +
          weakWallPenalty -
          deepMitigationLap -
          deepMitigationMotor -
          stableInnerBonus * 0.6 -
          weakActualTwoAttacker
      )
    : 0;
  const actualFourAttackGate = entryStructureChanged || boatNumberOf(actualLane4Row) !== 4 || startPatternContext.outer_attack_window ? 1 : 0.24;
  const actualFourStartReady = clamp(0, 0.08, Math.max(0, toNum(actualLane4Row?.start_edge, 0)) * 0.18);
  const actualFourExTimeReady = clamp(
    0,
    0.07,
    Math.max(0, toNum(actualLane4Row?.ex_time_left_gap_advantage, 0)) * 0.08 +
      Math.max(0, 0.08 - Math.max(0, toNum(actualLane4Row?.ex_time_relative_gap, 0))) * 0.06
  );
  const actualFourMotorSupport = actualLane4Row?.prediction_data_usage?.motor2ren?.used
    ? clamp(0, 0.06, (toNum(actualLane4Row?.motor_raw?.motor2ren, 0) / 100) * 0.08)
    : 0;
  const actualFourLaneSupport = clamp(
    0,
    0.06,
    (toNum(actualLane4Row?.lane_fit_2ren, 0) / 100) * 0.04 +
      (toNum(actualLane4Row?.lane_fit_3ren, 0) / 100) * 0.03
  );
  const actualFourAttackCaseStrength = clamp(
    0,
    0.2,
    (
      (toNum(actualLane4Row?.style_profile?.makuri, 0) / 100) * 0.05 +
      (toNum(actualLane4Row?.style_profile?.makuri_sashi, 0) / 100) * 0.03 +
      actualFourStartReady +
      actualFourExTimeReady +
      actualFourMotorSupport +
      actualFourLaneSupport +
      Math.max(0, toNum(actualLane4Row?.finish_role_bonuses?.straightLineDelta, 0)) * 0.05 +
      Math.max(0, toNum(actualLane4Row?.attack_readiness_bonus, 0)) * 0.12
    ) * actualFourAttackGate
  );
  const isStrongCado =
    actualFourAttackCaseStrength >= 0.13 &&
    actualFourStartReady >= 0.032 &&
    actualFourExTimeReady >= 0.024 &&
    actualFourMotorSupport >= 0.018 &&
    toNum(actualLane4Row?.finish_role_bonuses?.straightLineDelta, 0) >= 0.03;
  const lane3SurvivalFlag =
    toNum(actualLane3Row?.lane_fit_3ren, 0) >= 40 &&
    (toNum(actualLane3Row?.turning_ability, 0) >= 5.2 || Math.max(0, toNum(actualLane3Row?.finish_role_bonuses?.turningAbilityDelta, 0)) >= 0.08) &&
    toNum(actualLane3Row?.late_risk, 0) < 0.18;
  const lane2FlowInFlag =
    toNum(actualLane2Row?.lane_fit_3ren, 0) >= 34 &&
    toNum(actualLane2Row?.lane_fit_2ren, 0) < 46 &&
    (toNum(actualLane2Row?.safe_run_bias, 0) >= 0.02 || Math.max(0, toNum(actualLane2Row?.finish_role_bonuses?.turningAbilityDelta, 0)) >= 0.05) &&
    toNum(actualLane2Row?.late_risk, 0) < 0.2;
  const boat1StrongSurvivalFlag =
    toNum(actualLane1Row?.start_edge, 0) >= 0.02 &&
    toNum(boat1Row?.motor_true, 0) >= 52 &&
    toNum(actualLane1Row?.late_risk, 0) < 0.16 &&
    toNum(actualLane1Row?.hidden_F_flag, 0) === 0;
  const boat3HeadAlignmentStrength = clamp(
    0,
    0.2,
    (toNum(actualLane3Row?.style_profile?.makuri, 0) / 100) * 0.06 +
      (toNum(actualLane3Row?.style_profile?.makuri_sashi, 0) / 100) * 0.05 +
      Math.max(0, toNum(actualLane3Row?.start_edge, 0)) * 0.08 +
      Math.max(0, toNum(actualLane3Row?.ex_time_left_gap_advantage, 0)) * 0.07 +
      Math.max(0, toNum(actualLane3Row?.attack_readiness_bonus, 0)) * 0.12 -
      toNum(actualLane3Row?.late_risk, 0) * 0.06
  );
  const boat3HeadAlignmentGate = boat3HeadAlignmentStrength >= 0.11 && !boat1StrongSurvivalFlag
    ? BOAT3_HEAD_REBALANCE.strong_alignment_gate
    : boat3HeadAlignmentStrength >= 0.08 && !boat1StrongSurvivalFlag
      ? BOAT3_HEAD_REBALANCE.medium_alignment_gate
      : BOAT3_HEAD_REBALANCE.weak_alignment_gate;
  const boat2SecondRecoveryBefore = 0;
  const boat2SecondRecoveryAfter =
    boat1StrongSurvivalFlag && toNum(actualLane2Row?.lane_fit_2ren, 0) >= 42
      ? clamp(
          0,
          BOAT3_HEAD_REBALANCE.boat2_second_recovery,
          (toNum(actualLane2Row?.lane_fit_2ren, 0) / 100) * 0.038 +
            Math.max(0, 6.82 - toNum(actualLane2Row?.motor_form?.lapTime, 6.82)) * 0.024 +
            (actualLane2Row?.prediction_data_usage?.motor2ren?.used ? (toNum(actualLane2Row?.motor_raw?.motor2ren, 0) / 100) * 0.028 : 0) +
            toNum(actualLane2Row?.monster_motor_second_bonus, 0) * 0.34
        )
      : 0;
  const head4FinalDecision = isStrongCado && lane3SurvivalFlag && lane2FlowInFlag
    ? "4-3-2"
    : boat1StrongSurvivalFlag
      ? "4-1-3"
      : "4-1-3";
  const escapeScoreBeforeActualEntryAdjust = clamp(
    0,
    1,
    0.18 +
    toNum(actualLane1Row.lane_fit_1st, 0) / 100 * 0.34 +
    toNum(boat1Row.motor_true, 0) / 100 * 0.12 +
    Math.max(0, toNum(actualLane1Row.start_edge, 0)) * 0.24 +
    venueBias.venue_escape_bias +
    venueBias.venue_inside_finish_bias * 0.55 +
    venueBias.stability_board_bias +
    stableInnerBonus +
    lane2AllowNige -
    outerAttackPressure * (0.55 + venueBias.venue_outer_attack_bias) -
    venueBias.turn1_narrow_penalty -
    toNum(actualLane1Row.boat1_ex_time_warning, 0) * 0.08 -
    toNum(boat1Row.hidden_F_flag, 0) * 0.08 -
    toNum(actualLane1Row.late_risk, 0) * 0.12
  );
  const boat1FinalSurvivalBonusBefore = clamp(
    0,
    0.1,
    Math.max(0, toNum(actualLane1Row?.start_edge, 0)) * 0.18 +
      (boat1Row?.prediction_data_usage?.motor2ren?.used ? (toNum(boat1Row?.motor_raw?.motor2ren, 0) / 100) * 0.035 : 0) +
      (boat1Row?.prediction_data_usage?.lapTime?.used ? Math.max(0, 6.82 - toNum(boat1Row?.motor_form?.lapTime, 6.82)) * 0.12 : 0) +
      stableInnerBonus * 0.45 +
      venueBias.venue_inside_finish_bias * 0.38 +
      venueBias.venue_inside_second_third_bias * 0.2
  );
  const boat1FinalSurvivalBonusAfter = clamp(
    0,
    0.1,
    boat1FinalSurvivalBonusBefore + (isStrongCado ? 0.008 : 0.016) + (boat3HeadAlignmentGate < 1 ? BOAT3_HEAD_REBALANCE.boat1_survival_restore : 0)
  );
  const escapeScore = clamp(
    0,
    1,
    escapeScoreBeforeActualEntryAdjust -
      deepInEscapePenalty -
      clamp(0, 0.055, actualFourAttackCaseStrength * 0.34) -
      weakWallPenalty +
      stableInnerBonus * 0.4 +
      boat1FinalSurvivalBonusAfter * 0.45
  );
  const boat3HeadPromotionBefore = clamp(
    0,
    1,
    (toNum(actualLane3Row?.style_profile?.makuri, 0) / 100) * 0.56 +
      Math.max(0, toNum(actualLane3Row?.start_edge, 0)) * 0.6 -
      toNum(actualLane3Row?.late_risk, 0) * 0.26 +
      toNum(actualLane3Row?.attack_readiness_bonus, 0) * 0.42 +
      (startPatternContext.outer_attack_window ? 0.05 : 0) +
      Math.max(0, 0.45 - escapeScoreBeforeActualEntryAdjust) * 0.36
  );
  const actualTwoSashiBasePriority = clamp(
    0,
    1,
    (toNum(actualLane2Row?.lane_fit_2ren, 0) / 100) * 0.28 +
      (toNum(actualLane2Row?.style_profile?.sashi, 0) / 100) * 0.32 +
      (toNum(actualLane2Row?.style_profile?.makuri_sashi, 0) / 100) * 0.14 +
      Math.max(0, toNum(actualLane2Row?.start_edge, 0)) * 0.42 +
      Math.max(0, toNum(actualLane2Row?.ex_time_left_gap_advantage, 0)) * 0.38 +
      (deepInEscapePenalty > 0 ? 0.06 : 0) +
      Math.max(0, 0.42 - escapeScore) * 0.18 -
      toNum(actualLane2Row?.late_risk, 0) * 0.24
  );
  const actualTwoBoostGate = entryStructureChanged || boatNumberOf(actualLane2Row) !== 2 || deepInDepthRisk > 0 ? 1 : 0.22;
  const actualTwoSashiBoost = clamp(
    0,
    ACTUAL_ENTRY_TUNING.actual_two_sashi_boost,
    ((toNum(actualLane2Row?.lane_fit_2ren, 0) / 100) * 0.05 +
      (toNum(actualLane2Row?.style_profile?.sashi, 0) / 100) * 0.06 +
      Math.max(0, toNum(actualLane2Row?.start_edge, 0)) * 0.08 +
      Math.max(0, toNum(actualLane2Row?.ex_time_left_gap_advantage, 0)) * 0.07 +
      (actualLane2Row?.prediction_data_usage?.motor2ren?.used ? (toNum(actualLane2Row?.motor_raw?.motor2ren, 0) / 100) * 0.04 : 0) +
      Math.max(0, toNum(actualLane2Row?.turning_ability, 0) - 5) * 0.005 +
      Math.min(0.04, deepInEscapePenalty * 0.5) -
      toNum(actualLane2Row?.late_risk, 0) * 0.06) * actualTwoBoostGate
  );
  const actualLane2SashiPriority = clamp(0, 1, actualTwoSashiBasePriority + actualTwoSashiBoost);
  const actualFourCadoBasePriority = clamp(
    0,
    1,
    (toNum(actualLane4Row?.style_profile?.makuri, 0) / 100) * 0.34 +
      (toNum(actualLane4Row?.lane_fit_1st, 0) / 100) * 0.18 +
      Math.max(0, toNum(actualLane4Row?.start_edge, 0)) * 0.34 +
      Math.max(0, toNum(actualLane4Row?.ex_time_left_gap_advantage, 0)) * 0.24 +
      Math.max(0, toNum(actualLane4Row?.finish_role_bonuses?.straightLineDelta, 0)) * 0.08 +
      venueBias.venue_outer_attack_bias +
      (startPatternContext.outer_attack_window ? 0.05 : 0) -
      toNum(actualLane4Row?.late_risk, 0) * 0.16 -
      toNum(actualLane4Row?.hidden_F_flag, 0) * 0.08
  );
  const actualFourBoostGate = entryStructureChanged || boatNumberOf(actualLane4Row) !== 4 || startPatternContext.outer_attack_window ? 1 : 0.18;
  const actualFourCadoBoost = clamp(
    0,
    ACTUAL_ENTRY_TUNING.actual_four_cado_boost,
    ((toNum(actualLane4Row?.style_profile?.makuri, 0) / 100) * 0.06 +
      (toNum(actualLane4Row?.style_profile?.makuri_sashi, 0) / 100) * 0.035 +
      Math.max(0, toNum(actualLane4Row?.finish_role_bonuses?.straightLineDelta, 0)) * 0.08 +
      Math.max(0, toNum(actualLane4Row?.ex_time_left_gap_advantage, 0)) * 0.06 +
      Math.max(0, toNum(actualLane4Row?.start_edge, 0)) * 0.08 +
      (actualLane4Row?.prediction_data_usage?.motor2ren?.used ? (toNum(actualLane4Row?.motor_raw?.motor2ren, 0) / 100) * 0.04 : 0) +
      actualFourAttackCaseStrength * 0.48 +
      (startPatternContext.outer_attack_window ? 0.03 : 0) -
      toNum(actualLane4Row?.late_risk, 0) * 0.05) * actualFourBoostGate
  );
  const actualLane4CadoPriority = clamp(0, 1, actualFourCadoBasePriority + actualFourCadoBoost);
  const actualFourPartnerSecondCarryover = clamp(0, 0.08, actualFourAttackCaseStrength * 0.34);
  const actualFourPartnerThirdCarryover = clamp(0, 0.07, actualFourAttackCaseStrength * 0.28);
  if (actualLane4Row && typeof actualLane4Row === "object") {
    actualLane4Row.actual_four_attack_case_strength = round(actualFourAttackCaseStrength, 4);
    actualLane4Row.is_strong_cado = isStrongCado ? 1 : 0;
    actualLane4Row.actual_four_self_second_carryover = round(clamp(0, 0.08, actualFourAttackCaseStrength * 0.2), 4);
    actualLane4Row.actual_four_self_third_carryover = round(clamp(0, 0.06, actualFourAttackCaseStrength * 0.16), 4);
  }
  if (actualLane3Row && typeof actualLane3Row === "object") {
    actualLane3Row.lane3_survival_flag = lane3SurvivalFlag ? 1 : 0;
    actualLane3Row.actual_four_partner_second_carryover = round(actualFourPartnerSecondCarryover, 4);
    actualLane3Row.actual_four_partner_third_carryover = round(clamp(0, 0.05, actualFourAttackCaseStrength * 0.12), 4);
  }
  if (actualLane2Row && typeof actualLane2Row === "object") {
    actualLane2Row.lane2_flow_in_flag = lane2FlowInFlag ? 1 : 0;
    actualLane2Row.actual_boat2_second_recovery = round(boat2SecondRecoveryAfter, 4);
    actualLane2Row.actual_four_partner_second_carryover = round(clamp(0, 0.05, actualFourAttackCaseStrength * 0.12), 4);
    actualLane2Row.actual_four_partner_third_carryover = round(actualFourPartnerThirdCarryover, 4);
  }
  const nonBoat1Rows = laneContexts.filter((row) => row.lane !== 1);
  const attackStDeltaPressure = clamp(
    0,
    0.22,
    nonBoat1Rows.reduce((sum, row) => sum + toNum(row?.attack_st_delta_bonus, 0), 0) * 0.55
  );
  const instabilityStDeltaPressure = clamp(
    0,
    0.18,
    nonBoat1Rows.reduce((sum, row) => sum + toNum(row?.instability_st_delta_penalty, 0), 0) * 0.5
  );
  const stableFinishShape = clamp(
    0,
    0.16,
    nonBoat1Rows.reduce((sum, row) => sum + toNum(row?.stable_finish_bonus, 0), 0) * 0.44
  );
  const ippansenSecondResidualPressure = clamp(
    0,
    0.18,
    nonBoat1Rows.reduce((sum, row) => sum + toNum(row?.ippansen_2ren_support, 0), 0) * 0.34
  );
  const ippansenThirdResidualPressure = clamp(
    0,
    0.2,
    nonBoat1Rows.reduce((sum, row) => sum + toNum(row?.ippansen_3ren_support, 0), 0) * 0.36
  );

  const scenarioProbabilities = [
    {
      scenario: "boat1_escape",
      probability: round(clamp(0, 1, escapeScore + Math.max(0, toNum(boat1Row.safe_run_bias, 0) - 0.02) + stableFinishShape * 0.18 - instabilityStDeltaPressure * 0.08), 4)
    },
    {
      scenario: "boat2_sashi",
      probability: round(actualLane2SashiPriority, 4)
    },
    {
      scenario: "boat2_direct_makuri",
      probability: round(clamp(
        0,
        1,
        (toNum(actualLane2Row?.style_profile?.makuri, 0) / 100) * 0.56 +
        Math.max(0, toNum(actualLane2Row?.start_edge, 0)) * 0.62 -
        toNum(actualLane2Row?.hidden_F_flag, 0) * 0.12 +
        toNum(actualLane2Row?.attack_st_delta_bonus, 0) * 0.44 -
        toNum(actualLane2Row?.instability_st_delta_penalty, 0) * 0.18 +
        toNum(actualLane2Row?.attack_readiness_bonus, 0) * 0.44 +
        (startPatternContext.middle_dent ? 0.06 : 0) +
        Math.max(0, 0.42 - escapeScore) * 0.55
      ), 4)
    },
    {
      scenario: "boat3_makuri",
      probability: round(clamp(
        0,
        1,
        boat3HeadPromotionBefore * boat3HeadAlignmentGate + toNum(actualLane3Row?.attack_st_delta_bonus, 0) * 0.18 - toNum(actualLane3Row?.instability_st_delta_penalty, 0) * 0.08
      ), 4)
    },
    {
      scenario: "boat3_makuri_sashi",
      probability: round(clamp(
        0,
        1,
        ((toNum(actualLane3Row?.style_profile?.makuri_sashi, 0) / 100) * 0.6 +
          Math.max(0, toNum(actualLane3Row?.start_edge, 0)) * 0.42 -
          toNum(actualLane3Row?.late_risk, 0) * 0.18 +
          toNum(actualLane3Row?.attack_st_delta_bonus, 0) * 0.22 -
          toNum(actualLane3Row?.instability_st_delta_penalty, 0) * 0.1 +
          toNum(actualLane3Row?.attack_readiness_bonus, 0) * 0.28 +
          (startPatternContext.two_three_late ? 0.04 : 0)) * boat3HeadAlignmentGate
      ), 4)
    },
    {
      scenario: "boat4_cado_attack",
      probability: round(clamp(0, 1, actualLane4CadoPriority + toNum(actualLane4Row?.attack_st_delta_bonus, 0) * 0.2 - toNum(actualLane4Row?.instability_st_delta_penalty, 0) * 0.1), 4)
    },
    {
      scenario: "outer_mix_chaos",
      probability: round(clamp(
        0,
        1,
        outerAttackPressure * 0.34 +
        attackStDeltaPressure * 0.38 +
        ippansenSecondResidualPressure * 0.16 +
        ippansenThirdResidualPressure * 0.18 +
        safeArray([2, 3, 4, 5, 6]).reduce((sum, lane) => sum + toNum(actualLaneMap.get(lane)?.late_risk, 0), 0) * 0.12 +
        instabilityStDeltaPressure * 0.34 -
        stableFinishShape * 0.12 +
        venueBias.strong_wind_caution +
        Math.max(0, 0.34 - escapeScore) * 0.46
      ), 4)
    }
  ];
  const normalizedScenarioProbabilities = normalizeScenarioRows(scenarioProbabilities);
  const intermediateEvents = buildIntermediateEvents(actualLaneMap, escapeScore, outerAttackPressure);
  const laneFinishPriors = {
    first: { ...LANE_FINISH_PRIORS.first },
    second: { ...LANE_FINISH_PRIORS.second },
    third: { ...LANE_FINISH_PRIORS.third }
  };
  laneFinishPriors.first[1] = round(
    clamp(
      1.18,
      1.38,
      toNum(laneFinishPriors.first[1], 1.22) +
        0.08 +
        escapeScore * 0.12 +
        toNum(venueBias?.venue_inside_finish_bias, 0) * 0.65 +
        toNum(venueBias?.venue_escape_bias, 0) * 0.38
    ),
    4
  );
  laneFinishPriors.second[1] = round(
    clamp(
      1.08,
      1.24,
      toNum(laneFinishPriors.second[1], 1.1) +
        Math.max(0, escapeScore - 0.24) * 0.08 +
        toNum(venueBias?.venue_inside_second_third_bias, 0) * 0.5
    ),
    4
  );
  laneFinishPriors.third[1] = round(
    clamp(
      1.02,
      1.18,
      toNum(laneFinishPriors.third[1], 1.04) +
        Math.max(0, escapeScore - 0.22) * 0.06 +
        toNum(venueBias?.venue_inside_second_third_bias, 0) * 0.4
    ),
    4
  );
  laneFinishPriors.second[2] = round(
    clamp(
      1.08,
      1.18,
      toNum(laneFinishPriors.second[2], 1.06) + toNum(venueBias?.venue_inside_second_third_bias, 0) * 0.44
    ),
    4
  );
  laneFinishPriors.third[2] = round(
    clamp(
      1.02,
      1.12,
      toNum(laneFinishPriors.third[2], 1.02) + toNum(venueBias?.venue_inside_second_third_bias, 0) * 0.24
    ),
    4
  );
  laneFinishPriors.third[3] = round(
    clamp(
      1.01,
      1.1,
      toNum(laneFinishPriors.third[3], 1.01) + toNum(venueBias?.venue_inside_second_third_bias, 0) * 0.18
    ),
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
    actualLaneMap,
    baseRoleProbabilities,
    normalizedScenarioProbabilities,
    escapeScore,
    laneFinishPriors
  );
  const treeAggregation = aggregateScenarioFinishProbabilities(
    normalizedScenarioProbabilities,
    laneMap,
    actualLaneMap,
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
  const upsetSupport = buildUpsetSupport({
    laneMap,
    enhancementBase: {
      escape_score: escapeScore,
      outer_attack_pressure: outerAttackPressure,
      intermediate_events: intermediateEvents
    },
    scenarioRows: normalizedScenarioProbabilities,
    aggregatedFinishProbabilities: treeAggregation.aggregatedFinishProbabilities,
    topExactaCandidates
  });

  const darkHorseAlerts = [
    toNum(actualLane4Row?.lane_fit_1st, 0) >= 46 && String(rows.find((row) => toInt(row?.racer?.lane, null) === boatNumberOf(actualLane4Row))?.racer?.class || "") === "B1"
      ? { lane: boatNumberOf(actualLane4Row) || 4, type: "4_HEAD_CAUTION", reason: "B1 x actual 4-course x lane_fit_1st" }
      : null,
    toNum(actualLaneMap.get(6)?.lane_fit_3ren, 0) >= 42
      ? { lane: boatNumberOf(actualLaneMap.get(6)) || 6, type: "6_THIRD_CAUTION", reason: "actual 6-course x lane_fit_3ren" }
      : null,
    toNum(actualLane2Row?.style_profile?.makuri, 0) >= 58 && toNum(actualLane2Row?.start_edge, 0) >= 0.05
      ? { lane: boatNumberOf(actualLane2Row) || 2, type: "1_COLLAPSE_CAUTION", reason: "actual 2-course direct makuri tendency" }
      : null
  ].filter(Boolean);

  return {
    stage1_static: {
      style_profile_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.style_profile])),
      actual_lane_assignment: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
        boat_number: row.boat_number,
        actual_lane: row.actual_lane,
        course_change_occurred: row.course_change_occurred
      }])),
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
        avg_st_rank: row.avg_st_rank,
        lane_st_rank: row.lane_st_rank,
        exhibition_st: row.exhibition_st,
        st_delta: row.st_delta,
        st_delta_bucket: row.st_delta_bucket,
        exhibition_time: row.motor_form?.exhibitionTime ?? null,
        ex_time_relative_gap: row.ex_time_relative_gap,
        wind_adjusted_ex_time: row.wind_adjusted_ex_time,
        boat1_ex_time_warning: row.boat1_ex_time_warning,
        start_edge: row.start_edge,
        late_risk: row.late_risk,
        attack_st_delta_bonus: row.attack_st_delta_bonus,
        instability_st_delta_penalty: row.instability_st_delta_penalty,
        stable_finish_bonus: row.stable_finish_bonus,
        hidden_F_flag: row.hidden_F_flag,
        start_caution_penalty: row.start_caution_penalty,
        recent_L_penalty: row.recent_L_penalty,
        wind_start_instability: row.wind_start_instability,
        board_start_caution: row.board_start_caution,
        launch_state_bonus: row.launch_state_bonus,
        monster_motor_class: row.monster_motor_class,
        monster_motor_first_bonus: row.monster_motor_first_bonus,
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
        ippansen_2ren_support: row.ippansen_2ren_support,
        ippansen_3ren_support: row.ippansen_3ren_support,
        lane_fit_local: row.lane_fit_local,
        lane_fit_grade: row.lane_fit_grade
      }])),
      finish_role_bonuses_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.finish_role_bonuses])),
      finish_role_scores_before_tuning_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.finish_role_scores_before_tuning || null])),
      finish_role_scores_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.finish_role_scores])),
      second_place_bonus_breakdown_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.second_place_bonus_breakdown])),
      third_place_bonus_breakdown_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.third_place_bonus_breakdown])),
      third_place_exclusion_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.third_place_exclusion]))
    },
    stage3_scenarios: {
      selected_scenario_probabilities: normalizedScenarioProbabilities,
      intermediate_events: intermediateEvents,
      start_pattern_context: startPatternContext,
      actual_lane_reassignment: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
        original_boat_number: row.boat_number,
        actual_lane: row.actual_lane,
        course_change_occurred: row.course_change_occurred
      }])),
      actual_lane_map: Object.fromEntries(
        [...actualLaneMap.entries()]
          .sort((a, b) => a[0] - b[0])
          .map(([actualLane, row]) => [String(actualLane), boatNumberOf(row)])
      ),
      recalculated_priorities: {
        boat3_head: {
          boat_number: boatNumberOf(actualLane3Row),
          actual_lane: actualLaneOf(actualLane3Row),
          before_adjustment: round(boat3HeadPromotionBefore, 4),
          after_adjustment: round(boat3HeadPromotionBefore * boat3HeadAlignmentGate, 4),
          alignment_strength: round(boat3HeadAlignmentStrength, 4),
          alignment_gate: round(boat3HeadAlignmentGate, 4),
          contributors: {
            start_edge: round(Math.max(0, toNum(actualLane3Row?.start_edge, 0)), 4),
            left_gap_advantage: round(Math.max(0, toNum(actualLane3Row?.ex_time_left_gap_advantage, 0)), 4),
            attack_readiness: round(Math.max(0, toNum(actualLane3Row?.attack_readiness_bonus, 0)), 4),
            late_risk_penalty: round(toNum(actualLane3Row?.late_risk, 0) * 0.06, 4),
            boat1_survival_block: boat1StrongSurvivalFlag ? 1 : 0
          }
        },
        boat1_escape: {
          boat_number: boat1Row.boat_number || 1,
          actual_lane: boat1Row.actual_lane || 1,
          before_adjustment: round(escapeScoreBeforeActualEntryAdjust, 4),
          after_adjustment: round(escapeScore, 4),
          boat1_final_survival_bonus_before: round(boat1FinalSurvivalBonusBefore, 4),
          boat1_final_survival_bonus_after: round(boat1FinalSurvivalBonusAfter, 4),
          deep_in_depth_risk: round(deepInDepthRisk, 4),
          deep_in_escape_penalty: round(deepInEscapePenalty, 4),
          weak_wall_penalty: round(weakWallPenalty, 4),
          stable_inner_bonus: round(stableInnerBonus, 4),
          contributors: {
            deep_weak_start_pressure: round(deepWeakStartPressure, 4),
            actual_two_sashi_pressure: round(weakActualTwoPressure, 4),
            actual_four_attack_pressure: round(clamp(0, 0.07, actualFourAttackCaseStrength * 0.42), 4),
            outside_attack_pressure: round(deepOutsidePressure, 4),
            lap_time_mitigation: round(deepMitigationLap, 4),
            motor2ren_mitigation: round(deepMitigationMotor, 4),
            weak_actual_two_attacker_mitigation: round(weakActualTwoAttacker, 4)
          },
          actual_escape_score: round(escapeScore, 4),
          actual_lane_pressure_from_outside: round(outerAttackPressure, 4)
        },
        actual_two_course_sashi: {
          boat_number: boatNumberOf(actualLane2Row),
          actual_lane: actualLaneOf(actualLane2Row),
          before_adjustment: round(actualTwoSashiBasePriority, 4),
          actual_two_course_boat: boatNumberOf(actualLane2Row),
          actual_two_sashi_boost: round(actualTwoSashiBoost, 4),
          tuning_cap: round(ACTUAL_ENTRY_TUNING.actual_two_sashi_boost, 4),
          structure_gate: round(actualTwoBoostGate, 4),
          contributors: {
            lane2ren_score: round(toNum(actualLane2Row?.lane_fit_2ren, 0) / 100, 4),
            sashi_tendency: round(toNum(actualLane2Row?.style_profile?.sashi, 0) / 100, 4),
            exhibition_st_edge: round(Math.max(0, toNum(actualLane2Row?.start_edge, 0)), 4),
            left_gap_advantage: round(Math.max(0, toNum(actualLane2Row?.ex_time_left_gap_advantage, 0)), 4),
            motor2ren_support: round(actualLane2Row?.prediction_data_usage?.motor2ren?.used ? (toNum(actualLane2Row?.motor_raw?.motor2ren, 0) / 100) : 0, 4),
            turning_support: round(Math.max(0, toNum(actualLane2Row?.turning_ability, 0) - 5) * 0.005, 4),
            deep_in_pressure_bonus: round(Math.min(0.04, deepInEscapePenalty * 0.5), 4),
            late_risk_penalty: round(toNum(actualLane2Row?.late_risk, 0) * 0.06, 4)
          },
          priority: round(actualLane2SashiPriority, 4)
        },
        boat2_second_place_recovery: {
          boat_number: boatNumberOf(actualLane2Row),
          actual_lane: actualLaneOf(actualLane2Row),
          before_adjustment: round(boat2SecondRecoveryBefore, 4),
          after_adjustment: round(boat2SecondRecoveryAfter, 4),
        contributors: {
          lane2ren_score: round(toNum(actualLane2Row?.lane_fit_2ren, 0) / 100, 4),
          lap_time_support: round(Math.max(0, 6.82 - toNum(actualLane2Row?.motor_form?.lapTime, 6.82)) * 0.024, 4),
          motor2ren_support: round(actualLane2Row?.prediction_data_usage?.motor2ren?.used ? (toNum(actualLane2Row?.motor_raw?.motor2ren, 0) / 100) * 0.028 : 0, 4),
          monster_motor_second_bonus: round(toNum(actualLane2Row?.monster_motor_second_bonus, 0) * 0.34, 4),
          boat1_survival_context: boat1StrongSurvivalFlag ? 1 : 0
        }
      },
        actual_four_course_cado: {
          boat_number: boatNumberOf(actualLane4Row),
          actual_lane: actualLaneOf(actualLane4Row),
          before_adjustment: round(actualFourCadoBasePriority, 4),
          actual_four_course_boat: boatNumberOf(actualLane4Row),
          actual_four_cado_boost_before: 0,
          actual_four_cado_boost: round(actualFourCadoBoost, 4),
          tuning_cap: round(ACTUAL_ENTRY_TUNING.actual_four_cado_boost, 4),
          structure_gate: round(actualFourBoostGate, 4),
          contributors: {
            makuri_tendency: round(toNum(actualLane4Row?.style_profile?.makuri, 0) / 100, 4),
            makuri_sashi_tendency: round(toNum(actualLane4Row?.style_profile?.makuri_sashi, 0) / 100, 4),
            exhibition_st_readiness: round(actualFourStartReady, 4),
            exhibition_time_support: round(actualFourExTimeReady, 4),
            straight_support: round(Math.max(0, toNum(actualLane4Row?.finish_role_bonuses?.straightLineDelta, 0)), 4),
            left_gap_advantage: round(Math.max(0, toNum(actualLane4Row?.ex_time_left_gap_advantage, 0)), 4),
            start_readiness: round(Math.max(0, toNum(actualLane4Row?.start_edge, 0)), 4),
            motor_support: round(actualLane4Row?.prediction_data_usage?.motor2ren?.used ? (toNum(actualLane4Row?.motor_raw?.motor2ren, 0) / 100) : 0, 4),
            lane_support: round(actualFourLaneSupport, 4),
            attack_case_strength: round(actualFourAttackCaseStrength, 4),
            outer_attack_window_bonus: startPatternContext.outer_attack_window ? 0.03 : 0,
            late_risk_penalty: round(toNum(actualLane4Row?.late_risk, 0) * 0.05, 4)
          },
          priority: round(actualLane4CadoPriority, 4)
        }
      }
    },
    stage4_opponents: {
      head_candidate_set: finishRoleFramework.headCandidates.map((row) => row.lane),
      second_candidate_set: normalizeRows(laneContexts.map((row) => ({ lane: row.lane, weight: toNum(row?.finish_role_scores?.second_place_score, 0) }))).slice(0, 4).map((row) => row.lane),
      third_candidate_set: normalizeRows(laneContexts.map((row) => ({ lane: row.lane, weight: toNum(row?.finish_role_scores?.third_place_score, 0) }))).slice(0, 5).map((row) => row.lane),
      compatibility_with_head: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.compatibility_with_head])),
      head4_partner_debug: {
        head4_boat: boatNumberOf(actualLane4Row),
        is_strong_cado: isStrongCado,
        lane3_survival_flag: lane3SurvivalFlag,
        lane2_flow_in_flag: lane2FlowInFlag,
        boat1_survival_flag: boat1StrongSurvivalFlag,
        final_decision: head4FinalDecision,
        lane3_for_second: {
          boat: boatNumberOf(actualLane3Row),
          second_bonus: round(toNum(actualLane3Row?.compatibility_with_head?.["4"]?.second_bonus, 0), 4),
          promoted: toNum(actualLane3Row?.compatibility_with_head?.["4"]?.second_bonus, 0) >= 0.08,
          reasons: safeArray(actualLane3Row?.compatibility_with_head?.["4"]?.reasons)
        },
        lane2_for_third: {
          boat: boatNumberOf(actualLane2Row),
          third_bonus: round(toNum(actualLane2Row?.compatibility_with_head?.["4"]?.third_bonus, 0), 4),
          promoted: toNum(actualLane2Row?.compatibility_with_head?.["4"]?.third_bonus, 0) >= 0.07,
          reasons: safeArray(actualLane2Row?.compatibility_with_head?.["4"]?.reasons)
        }
      },
      primary_head_lane: finishRoleFramework.primaryHeadLane,
      escape_sim_support: roleProbabilityLayers?.boat1_escape_probability || null
    },
    stage5_ticketing: {
      selected_ticket_shape: null,
      shape_reason: null,
      finish_probabilities_by_scenario: treeAggregation.finishProbabilitiesByScenario,
      aggregated_finish_probabilities: treeAggregation.aggregatedFinishProbabilities,
      order_probabilities: treeAggregation.orderProbabilities,
      top_exacta_candidates: topExactaCandidates,
      upset_support: upsetSupport
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
    upsetSupport,
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
    startPatternContext,
    actualEntryTuning: ACTUAL_ENTRY_TUNING
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
      multiplier += toNum(row.finish_role_scores?.first_place_score, 0) * 0.2;
    } else if (role === "second") {
      const compatibility = row?.compatibility_with_head?.[String(primaryHeadLane)] || { second_bonus: 0 };
      multiplier +=
        (lane === 1 ? Math.max(0, 0.46 - escapeScore) * 0.24 : 0) +
        toNum(row.start_edge, 0) * 0.12 +
        (toNum(row.lane_fit_2ren, 0) / 100) * 0.22 +
        toNum(row.ippansen_2ren_support, 0) * 0.28 +
        (toNum(row.motor_raw?.motor2ren, 0) / 100) * 0.18 +
        toNum(row.finish_role_scores?.second_place_score, 0) * 0.26 +
        toNum(row.finish_role_bonuses?.secondPlaceBonus, 0) * 0.7 +
        toNum(compatibility.second_bonus, 0) * 0.6 +
        toNum(row.finish_role_scores?.likely_head_survival_context, 0) * 0.22 +
        toNum(row.finish_role_scores?.attack_but_not_win_carryover, 0) * 0.18 +
        toNum(row.second_place_bonus_breakdown?.survival_after_attack_bonus, 0) * 0.18 +
        toNum(row.attack_st_delta_bonus, 0) * 0.16 +
        toNum(row.stable_finish_bonus, 0) * 0.14 +
        toNum(row.safe_run_bias, 0) * 0.06 -
        toNum(row.instability_st_delta_penalty, 0) * 0.14 -
        toNum(row.late_risk, 0) * 0.18 -
        toNum(row.hidden_F_flag, 0) * 0.06;
      if (lane === 2) multiplier += scenarioMap.get("boat2_sashi") * 0.18;
      if (lane === 4) multiplier += scenarioMap.get("boat4_cado_attack") * 0.08;
    } else {
      const compatibility = row?.compatibility_with_head?.[String(primaryHeadLane)] || { third_bonus: 0 };
      multiplier +=
        (lane === 1 ? Math.max(0, 0.42 - escapeScore) * 0.1 : 0) +
        (toNum(row.lane_fit_3ren, 0) / 100) * 0.26 +
        toNum(row.ippansen_3ren_support, 0) * 0.3 +
        (toNum(row.motor_raw?.motor3ren, 0) / 100) * 0.2 +
        toNum(row.finish_role_scores?.third_place_score, 0) * 0.28 +
        toNum(row.finish_role_bonuses?.thirdPlaceBonus, 0) * 0.7 +
        toNum(compatibility.third_bonus, 0) * 0.55 +
        toNum(row.finish_role_scores?.attack_but_not_win_carryover, 0) * 0.08 +
        toNum(row.third_place_bonus_breakdown?.flow_in_bonus, 0) * 0.2 +
        toNum(row.third_place_bonus_breakdown?.outer_survival_bonus, 0) * 0.16 +
        toNum(row.third_place_bonus_breakdown?.residual_tendency, 0) * 0.18 +
        toNum(row.attack_st_delta_bonus, 0) * 0.1 +
        toNum(row.stable_finish_bonus, 0) * 0.2 +
        toNum(row.safe_run_bias, 0) * 0.1 +
        (toNum(row.style_profile?.nuki, 0) / 100) * 0.04 -
        toNum(row.instability_st_delta_penalty, 0) * 0.1 -
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

function deriveShapeHintFromRoleMaps({ firstLane, secondWeights, thirdWeights }) {
  const secondRows = normalizeRows(
    Object.entries(secondWeights || {}).map(([lane, weight]) => ({ lane: toInt(lane, null), weight }))
  ).filter((row) => row.lane !== firstLane);
  const thirdRows = normalizeRows(
    Object.entries(thirdWeights || {}).map(([lane, weight]) => ({ lane: toInt(lane, null), weight }))
  ).filter((row) => row.lane !== firstLane);
  if (!Number.isInteger(firstLane) || !secondRows.length || !thirdRows.length) return null;
  const second = secondRows.slice(0, 2).map((row) => row.lane);
  const third = [...new Set([...second, ...thirdRows.slice(0, 4).map((row) => row.lane)])].filter((lane) => lane !== firstLane);
  if (!second.length || !third.length) return null;
  return `${firstLane}-${second.join("")}-${third.join("")}`;
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
  const preTuningScoreMap = enhancement?.stage2_dynamic?.finish_role_scores_before_tuning_by_lane || {};
  const preEscapePriority = toNum(enhancement?.stage3_scenarios?.recalculated_priorities?.boat1_escape?.before_adjustment, 0);
  const preActualFourPriority = toNum(enhancement?.stage3_scenarios?.recalculated_priorities?.actual_four_course_cado?.before_adjustment, 0);
  const preBoat3HeadPriority = toNum(enhancement?.stage3_scenarios?.recalculated_priorities?.boat3_head?.before_adjustment, 0);
  const postBoat3HeadPriority = toNum(enhancement?.stage3_scenarios?.recalculated_priorities?.boat3_head?.after_adjustment, 0);
  const head4Decision = enhancement?.stage4_opponents?.head4_partner_debug?.final_decision || null;
  const isStrongCado = !!enhancement?.stage4_opponents?.head4_partner_debug?.is_strong_cado;
  const lane3SurvivalFlag = !!enhancement?.stage4_opponents?.head4_partner_debug?.lane3_survival_flag;
  const lane2FlowInFlag = !!enhancement?.stage4_opponents?.head4_partner_debug?.lane2_flow_in_flag;
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
      shape: "4-3-125",
      first: [4],
      second: [3],
      third: [1, 2, 5],
      why: "actual lane4 attack with 3-2 residual carryover",
      score: 0.4 + lane4HeadWeight * 0.5 + toNum(laneRows["3"]?.finish_role_scores?.second_place_score, 0) * 0.24 + toNum(laneRows["2"]?.finish_role_scores?.third_place_score, 0) * 0.18 + (head4Decision === "4-3-2" ? 0.1 : -0.06)
    });
    templates.push({
      shape: "4-3-12",
      first: [4],
      second: [3],
      third: [1, 2],
      why: "head4 with lane3 second and lane2 residual third",
      score: 0.46 + lane4HeadWeight * 0.52 + toNum(laneRows["3"]?.compatibility_with_head?.["4"]?.second_bonus, 0) * 0.34 + toNum(laneRows["2"]?.compatibility_with_head?.["4"]?.third_bonus, 0) * 0.28 + (isStrongCado && lane3SurvivalFlag && lane2FlowInFlag ? 0.14 : -0.1)
    });
    templates.push({
      shape: "4-13-123",
      first: [4],
      second: [1, 3],
      third: [1, 2, 3],
      why: "head4 spread with lane3 second emphasis and lane2 third retention",
      score: 0.42 + lane4HeadWeight * 0.44 + toNum(laneRows["3"]?.finish_role_scores?.second_place_score, 0) * 0.2 + toNum(laneRows["2"]?.finish_role_scores?.third_place_score, 0) * 0.16 + (head4Decision === "4-1-3" ? 0.12 : -0.06)
    });
    templates.push({
      shape: "4-1-235",
      first: [4],
      second: [1],
      third: [2, 3, 5],
      why: "4-cado alert support, controlled head hedge",
      score: 0.34 + lane4HeadWeight * 0.45 + toNum(laneRows["1"]?.finish_role_scores?.second_place_score, 0) * 0.12 + (head4Decision === "4-1-3" ? 0.14 : -0.05)
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
      const secondRoleConcentration = safeArray(row.second).reduce(
        (sum, lane) => sum + toNum(laneRows[String(lane)]?.finish_role_scores?.second_place_score, 0),
        0
      );
      const thirdRoleConcentration = safeArray(row.third).reduce(
        (sum, lane) => sum + toNum(laneRows[String(lane)]?.finish_role_scores?.third_place_score, 0),
        0
      );
      const countPenalty = Math.abs(ticketCount - targetTicketCount) * 0.035;
      return {
        ...row,
        expandedTickets,
        ticketCount,
        secondRoleConcentration: round(secondRoleConcentration, 4),
        thirdRoleConcentration: round(thirdRoleConcentration, 4),
        score: row.score + exactaSupport * 0.18 + secondRoleConcentration * 0.08 + thirdRoleConcentration * 0.06 - countPenalty
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
      },
    tuning_debug: {
      before_shape_hint: deriveShapeHintFromRoleMaps({
        firstLane: preActualFourPriority > preEscapePriority ? 4 : topFirst.lane,
        secondWeights: Object.fromEntries(Object.entries(preTuningScoreMap || {}).map(([lane, row]) => [lane, toNum(row?.second_place_score, 0)])),
        thirdWeights: Object.fromEntries(Object.entries(preTuningScoreMap || {}).map(([lane, row]) => [lane, toNum(row?.third_place_score, 0)]))
      }),
      before_top_ticket_hints: [],
      boat3_before_top_ticket_hint: preBoat3HeadPriority > preEscapePriority ? "3-head pressure" : "inside-favored",
      boat3_after_top_ticket_hint: postBoat3HeadPriority > escapeScore ? "3-head pressure" : "inside-favored",
      after_shape: null,
      after_top_ticket_hints: []
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
      actual_ticket_count: chosen.ticketCount,
      second_role_concentration: round(chosen.secondRoleConcentration, 4),
      third_role_concentration: round(chosen.thirdRoleConcentration, 4)
    },
    tuning_debug: {
      before_shape_hint: deriveShapeHintFromRoleMaps({
        firstLane: preActualFourPriority > preEscapePriority ? 4 : topFirst.lane,
        secondWeights: Object.fromEntries(Object.entries(preTuningScoreMap || {}).map(([lane, row]) => [lane, toNum(row?.second_place_score, 0)])),
        thirdWeights: Object.fromEntries(Object.entries(preTuningScoreMap || {}).map(([lane, row]) => [lane, toNum(row?.third_place_score, 0)]))
      }),
      before_top_ticket_hints: deriveShapeHintFromRoleMaps({
        firstLane: preActualFourPriority > preEscapePriority ? 4 : topFirst.lane,
        secondWeights: Object.fromEntries(Object.entries(preTuningScoreMap || {}).map(([lane, row]) => [lane, toNum(row?.second_place_score, 0)])),
        thirdWeights: Object.fromEntries(Object.entries(preTuningScoreMap || {}).map(([lane, row]) => [lane, toNum(row?.third_place_score, 0)]))
      }) ? [deriveShapeHintFromRoleMaps({
        firstLane: preActualFourPriority > preEscapePriority ? 4 : topFirst.lane,
        secondWeights: Object.fromEntries(Object.entries(preTuningScoreMap || {}).map(([lane, row]) => [lane, toNum(row?.second_place_score, 0)])),
        thirdWeights: Object.fromEntries(Object.entries(preTuningScoreMap || {}).map(([lane, row]) => [lane, toNum(row?.third_place_score, 0)]))
      })] : [],
      boat3_before_top_ticket_hint: preBoat3HeadPriority > preEscapePriority ? "3-head pressure" : "inside-favored",
      boat3_after_top_ticket_hint: postBoat3HeadPriority > escapeScore ? "3-head pressure" : "inside-favored",
      after_shape: chosen.shape,
      after_top_ticket_hints: safeArray(chosen.expandedTickets).slice(0, 3)
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
