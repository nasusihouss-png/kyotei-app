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
      late_risk: round(lateRisk, 4),
      hidden_F_flag: hiddenF,
      unresolved_F_count: unresolvedFCount,
      start_caution_penalty: round(startCautionPenalty, 2),
      lane_fit_1st: laneFit1st === null ? null : round(laneFit1st, 2),
      lane_fit_2ren: laneFit2ren === null ? null : round(laneFit2ren, 2),
      lane_fit_3ren: laneFit3ren === null ? null : round(laneFit3ren, 2),
      lane_fit_local: Number.isFinite(features?.lane_fit_local) ? round(features.lane_fit_local, 2) : null,
      lane_fit_grade: Number.isFinite(features?.lane_fit_grade) ? round(features.lane_fit_grade, 2) : null,
      motor_raw: {
        motor2ren: Number.isFinite(features?.motor2_rate) ? round(features.motor2_rate, 2) : null,
        motor3ren: Number.isFinite(features?.motor3_rate) ? round(features.motor3_rate, 2) : null
      },
      motor_true: round(motorTrue, 2),
      motor_form: {
        lapTime: Number.isFinite(features?.lap_time) ? round(features.lap_time, 2) : null,
        lapExStretch: Number.isFinite(features?.lap_exhibition_score) ? round(features.lap_exhibition_score, 2) : null,
        exhibitionTime: Number.isFinite(features?.exhibition_time) ? round(features.exhibition_time, 2) : null
      },
      style_pressure: stylePressure,
      motivation_attack: motivation.motivation_attack,
      safe_run_bias: motivation.safe_run_bias
    };
  }).filter((row) => Number.isInteger(row.lane));

  const laneMap = new Map(laneContexts.map((row) => [row.lane, row]));
  const lane1 = laneMap.get(1) || {};
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
    lane2AllowNige -
    outerAttackPressure * 0.55 -
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
        toNum(laneMap.get(3)?.late_risk, 0) * 0.18
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
        Math.max(0, 0.34 - escapeScore) * 0.46
      ), 4)
    }
  ].sort((a, b) => b.probability - a.probability);

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
      start_edge_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.start_edge])),
      late_risk_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.late_risk])),
      motor_form_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row.motor_form])),
      lane_fit_by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), {
        lane_fit_1st: row.lane_fit_1st,
        lane_fit_2ren: row.lane_fit_2ren,
        lane_fit_3ren: row.lane_fit_3ren,
        lane_fit_local: row.lane_fit_local,
        lane_fit_grade: row.lane_fit_grade
      }]))
    },
    stage3_scenarios: {
      selected_scenario_probabilities: scenarioProbabilities
    },
    stage4_opponents: {
      head_candidate_set: pickTopLanes(roleProbabilityLayers?.first_place_probability_json, 3),
      second_candidate_set: pickTopLanes(roleProbabilityLayers?.second_place_probability_json, 4),
      third_candidate_set: pickTopLanes(roleProbabilityLayers?.third_place_probability_json, 5),
      escape_sim_support: roleProbabilityLayers?.boat1_escape_probability || null
    },
    stage5_ticketing: {
      selected_ticket_shape: null,
      shape_reason: null
    },
    confidence: toNum(confidence, 0),
    by_lane: Object.fromEntries(laneContexts.map((row) => [String(row.lane), row])),
    dark_horse_alerts: darkHorseAlerts,
    race_flow_summary: raceFlow || null
  };
}

export function applyHitRateEnhancementToProbabilities({
  firstProbs,
  secondProbs,
  thirdProbs,
  enhancement
}) {
  const firstMap = mapByLane(firstProbs);
  const secondMap = mapByLane(secondProbs);
  const thirdMap = mapByLane(thirdProbs);
  const laneRows = enhancement?.by_lane || {};
  const escapeScore = toNum(enhancement?.stage1_static?.escape_score, 0);
  const scenarioMap = new Map(safeArray(enhancement?.stage3_scenarios?.selected_scenario_probabilities).map((row) => [row.scenario, toNum(row?.probability, 0)]));

  const applyByRole = (baseMap, role) => normalizeRows([...baseMap.entries()].map(([lane, weight]) => {
    const row = laneRows[String(lane)] || {};
    let multiplier = 1;
    if (role === "first") {
      multiplier +=
        (lane === 1 ? 0.08 + escapeScore * 0.16 : 0) +
        toNum(row.start_edge, 0) * 0.22 +
        (toNum(row.lane_fit_1st, 0) / 100) * 0.18 +
        (toNum(row.motor_true, 0) / 100) * 0.09 +
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
    } else if (role === "second") {
      multiplier +=
        (lane === 1 ? Math.max(0, 0.46 - escapeScore) * 0.24 : 0) +
        toNum(row.start_edge, 0) * 0.12 +
        (toNum(row.lane_fit_2ren, 0) / 100) * 0.22 +
        (toNum(row.motor_raw?.motor2ren, 0) / 100) * 0.18 +
        toNum(row.safe_run_bias, 0) * 0.06 -
        toNum(row.late_risk, 0) * 0.18 -
        toNum(row.hidden_F_flag, 0) * 0.06;
      if (lane === 2) multiplier += scenarioMap.get("boat2_sashi") * 0.18;
      if (lane === 4) multiplier += scenarioMap.get("boat4_cado_attack") * 0.08;
    } else {
      multiplier +=
        (lane === 1 ? Math.max(0, 0.42 - escapeScore) * 0.1 : 0) +
        (toNum(row.lane_fit_3ren, 0) / 100) * 0.26 +
        (toNum(row.motor_raw?.motor3ren, 0) / 100) * 0.2 +
        toNum(row.safe_run_bias, 0) * 0.1 +
        (toNum(row.style_profile?.nuki, 0) / 100) * 0.04 -
        toNum(row.late_risk, 0) * 0.12 -
        toNum(row.hidden_F_flag, 0) * 0.03;
    }
    return {
      lane,
      weight: Math.max(0.0001, weight * Math.max(0.55, multiplier))
    };
  }));

  return {
    first: applyByRole(firstMap, "first"),
    second: applyByRole(secondMap, "second"),
    third: applyByRole(thirdMap, "third")
  };
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
  const topFirst = firstRows[0] || { lane: null, weight: 0 };
  const nextFirst = firstRows[1] || { lane: null, weight: 0 };
  const firstDominance = topFirst.weight - nextFirst.weight;
  const escapeScore = toNum(enhancement?.stage1_static?.escape_score, 0);
  const chaosProb = toNum(
    safeArray(enhancement?.stage3_scenarios?.selected_scenario_probabilities).find((row) => row?.scenario === "outer_mix_chaos")?.probability,
    0
  );

  const templates = [];
  if (topFirst.lane === 1) {
    const secondLanes = pickTopLanes(secondRows, 2, [1]);
    const thirdLanes = pickTopLanes(thirdRows, 3, [1]);
    templates.push({
      shape: `1-${secondLanes.join("")}-${[...new Set([...secondLanes, ...thirdLanes])].join("")}`,
      first: [1],
      second: secondLanes,
      third: [...new Set([...secondLanes, ...thirdLanes])],
      why: "boat1 escape with concentrated partners",
      score: 0.72 + escapeScore * 0.4 + firstDominance * 0.28
    });
    if (secondLanes.includes(3)) {
      templates.push({
        shape: "1-3-24",
        first: [1],
        second: [3],
        third: [2, 4],
        why: "boat3 attack support with compact cover",
        score: 0.58 + toNum(secondRows.find((row) => row.lane === 3)?.weight, 0) * 0.3
      });
    }
    templates.push({
      shape: "1-24-234",
      first: [1],
      second: pickTopLanes(secondRows, 2, [1]),
      third: pickTopLanes(thirdRows, 3, [1]),
      why: "boat1 first with wider third survivor coverage",
      score: 0.54 + escapeScore * 0.24
    });
    templates.push({
      shape: "1-2-34",
      first: [1],
      second: [2],
      third: [3, 4],
      why: "stable inside-first tie-break shape",
      score: 0.49 + Math.max(0, 0.5 - chaosProb) * 0.22
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
      score: 0.34 + lane4HeadWeight * 0.45
    });
    templates.push({
      shape: "1-4-235",
      first: [1],
      second: [4],
      third: [2, 3, 5],
      why: "4-cado pressure retained as partner support",
      score: 0.42 + toNum(secondRows.find((row) => row.lane === 4)?.weight, 0) * 0.4
    });
  }

  const chosen = templates
    .filter((row) => safeArray(row.first).length && safeArray(row.second).length && safeArray(row.third).length)
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
  const expanded = [];
  for (const a of chosen.first) {
    for (const b of chosen.second) {
      for (const c of chosen.third) {
        const combo = normalizeCombo(`${a}-${b}-${c}`);
        if (combo) expanded.push(combo);
      }
    }
  }
  const expandedTickets = [...new Set(expanded)];
  return {
    shape: chosen.shape,
    selected_shape: chosen.shape,
    first: chosen.first,
    second: chosen.second,
    third: chosen.third,
    expanded_tickets: expandedTickets,
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
      confidence: round(confidence, 2)
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
        toNum(firstMap.get(a), 0) *
          toNum(secondMap.get(b), 0) *
          toNum(thirdMap.get(c), 0) *
          (8.2 + roleSupport * 2.8) *
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
