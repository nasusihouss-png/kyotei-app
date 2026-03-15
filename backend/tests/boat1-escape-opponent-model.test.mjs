import assert from "node:assert/strict";
import { __testHooks } from "../src/routes/race.js";

const {
  buildSeparatedCandidateDistributions,
  buildPlayerStatProfileFromHistory,
  buildPredictionFeatureBundle,
  buildRoleProbabilityLayers,
  buildBoat3WeakStHeadSuppressionContext,
  getLaunchStateConfig,
  getVenueLaunchMicroCalibration,
  getFHolderPenaltyByRole,
  computeStartAdvantageScore,
  computeMotor2renStrength,
  computeLapExhibitionStrength,
  computeFinishOverrideStrength,
  applyFinishOverrideStrength,
  computeLaunchStateScores,
  classifyLaunchStates,
  buildIntermediateDevelopmentEvents,
  computeRaceScenarioProbabilities,
  computeFinishProbsByScenario,
  combineScenarioAndFinishProbs,
  buildTopRecommendedTickets,
  computeUpsetRiskScore,
  shouldShowUpsetAlert,
  buildUpsetAlert,
  computeBoat1EscapeProbability,
  computeAttackScenarioProbabilities,
  computeFirstPlaceProbabilities,
  computeSecondPlaceProbabilities,
  computeSurvivalProbabilities,
  computeThirdPlaceProbabilities,
  buildEvidenceBiasTable,
  applyEvidenceBiasConfirmationToRoleProbabilities,
  composeFinishOrderCandidates,
  generateMainTrifectaTickets,
  generateExactaCoverTickets,
  generateBackupUrasujiTickets,
  buildExactaCoverageSnapshot,
  buildParticipationDecision,
  buildBackupUrasujiRecommendationsSnapshot
} = __testHooks;

function makeRow(lane, overrides = {}) {
  const features = {
    exhibition_rank: lane,
    lap_time_delta_vs_front: lane >= 2 ? 0.02 : 0,
    lap_attack_flag: 0,
    lap_attack_strength: 0,
    motor_total_score: 8 - lane * 0.2,
    motor_trend_score: 2,
    expected_actual_st_rank: lane,
    st_rank: lane,
    class_score: 6.5 - lane * 0.15,
    nationwide_win_rate: 6.4 - lane * 0.12,
    local_win_rate: 6.1 - lane * 0.1,
    entry_advantage_score: lane === 4 ? 2.5 : 0,
    display_time_delta_vs_left: lane >= 2 ? 0.01 : 0,
    avg_st_rank_delta_vs_left: lane >= 2 ? 0.5 : 0,
    slit_alert_flag: 0,
    f_hold_caution_penalty: 0,
    venue_lane_adjustment: lane <= 4 ? 1.2 : -0.8,
    course_fit_score: lane <= 4 ? 1.8 : 0.6
  };
  return {
    racer: {
      lane,
      exhibitionTime: 6.8 + lane * 0.01
    },
    score: 100 - lane * 7,
    features: {
      ...features,
      ...(overrides.features || {})
    }
  };
}

function laneOrder(rows, count = 3) {
  return rows.slice(0, count).map((row) => row.lane);
}

const learningWeights = {};
const race = { venueId: 5, venueName: "Tamagawa" };

const insideRows = [
  makeRow(1, { features: { exhibition_rank: 1, motor_total_score: 10.5, expected_actual_st_rank: 1 } }),
  makeRow(2, { features: { exhibition_rank: 2, motor_total_score: 9.8, expected_actual_st_rank: 2 } }),
  makeRow(3, { features: { exhibition_rank: 3, motor_total_score: 9.2, expected_actual_st_rank: 3 } }),
  makeRow(4, { features: { exhibition_rank: 4, motor_total_score: 8.8, expected_actual_st_rank: 4 } }),
  makeRow(5, { features: { exhibition_rank: 5, motor_total_score: 7.9, expected_actual_st_rank: 5, lap_attack_strength: 4.5 } }),
  makeRow(6, { features: { exhibition_rank: 6, motor_total_score: 7.2, expected_actual_st_rank: 6, lap_attack_strength: 4.8 } })
];

const insideHeadScenario = {
  head_distribution_json: [
    { lane: 1, weight: 0.62 },
    { lane: 2, weight: 0.16 },
    { lane: 3, weight: 0.12 },
    { lane: 4, weight: 0.06 },
    { lane: 5, weight: 0.03 },
    { lane: 6, weight: 0.01 }
  ],
  second_distribution_json: [
    { lane: 2, weight: 0.38 },
    { lane: 3, weight: 0.32 },
    { lane: 4, weight: 0.18 },
    { lane: 5, weight: 0.07 },
    { lane: 6, weight: 0.05 }
  ],
  main_head_lane: 1,
  attack_head_lane: null,
  survival_residual_score: 44,
  boat1_priority_mode_applied: 1
};

const insideEscape = {
  escape_pattern_applied: true,
  formation_pattern: "inside_lead",
  escape_second_place_bias_json: { 2: 1.0, 3: 0.8, 4: 0.4 }
};

const noAttack = {
  attack_scenario_applied: 0,
  attack_scenario_type: null,
  attack_scenario_score: 0
};

const insideDistributions = buildSeparatedCandidateDistributions({
  ranking: insideRows,
  tickets: [],
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: noAttack,
  learningWeights,
  race
});

assert.equal(insideDistributions.first_place_distribution_json[0].lane, 1, "boat 1 should stay top first-place candidate");
assert.deepEqual(
  laneOrder(insideDistributions.boat1_second_place_distribution_json, 3),
  [2, 3, 4],
  "strong inside formation should keep 2/3/4 as core second-place lanes"
);
assert.equal(
  insideDistributions.first_place_probability_json[0].lane,
  1,
  "first-place probability should keep boat 1 as the default head"
);
assert.ok(
  insideDistributions.boat1_escape_probability >= 0.45,
  "boat 1 escape probability should stay meaningfully high in a stable inside race"
);
assert.ok(
  insideDistributions.f_holder_role_penalty_summary_json &&
    typeof insideDistributions.f_holder_role_penalty_summary_json === "object",
  "candidate distributions should expose role-based F-holder penalty summary"
);

const lane3AttackRows = [
  ...insideRows.slice(0, 2),
  makeRow(3, { features: { exhibition_rank: 1, lap_time_delta_vs_front: 0.09, lap_attack_flag: 1, lap_attack_strength: 10, slit_alert_flag: 1, motor_total_score: 11.2, expected_actual_st_rank: 1 } }),
  makeRow(4, { features: { exhibition_rank: 4, lap_time_delta_vs_front: 0.04, lap_attack_strength: 6.5, motor_total_score: 9.3 } }),
  insideRows[4],
  insideRows[5]
];
const lane3Attack = {
  attack_scenario_applied: 1,
  attack_scenario_type: "three_makuri",
  attack_scenario_score: 72
};
const lane3Distributions = buildSeparatedCandidateDistributions({
  ranking: lane3AttackRows,
  tickets: [],
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: lane3Attack,
  learningWeights,
  race
});
assert.equal(lane3Distributions.first_place_distribution_json[0].lane, 1, "lane 3 attack should not automatically replace boat 1 head");
assert.equal(lane3Distributions.boat1_second_place_distribution_json[0].lane, 3, "lane 3 attack should raise 3 for second place under 1-head");
assert.ok(
  lane3Distributions.attack_scenario_probability_json.some((row) => row.scenario === "boat3_makuri"),
  "attack scenario probabilities should expose lane 3 makuri as attack context"
);

const lane4PressureRows = [
  ...insideRows.slice(0, 3),
  makeRow(4, { features: { exhibition_rank: 2, lap_time_delta_vs_front: 0.08, lap_attack_flag: 1, lap_attack_strength: 9, motor_total_score: 10.6, entry_advantage_score: 11 } }),
  insideRows[4],
  insideRows[5]
];
const lane4Pressure = {
  attack_scenario_applied: 1,
  attack_scenario_type: "four_cado_makuri_sashi",
  attack_scenario_score: 69
};
const lane4Distributions = buildSeparatedCandidateDistributions({
  ranking: lane4PressureRows,
  tickets: [],
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: lane4Pressure,
  learningWeights,
  race
});
assert.ok(
  laneOrder(lane4Distributions.boat1_second_place_distribution_json, 2).includes(4),
  "lane 4 corner pressure should keep 1-4-x live as a second-place family"
);
assert.ok(
  laneOrder(lane4Distributions.first_place_probability_json, 3).includes(1),
  "lane 4 pressure should still leave boat 1 alive in first-place probabilities"
);

const laneAwareStartRows = [
  makeRow(1, { features: { expected_actual_st: 0.13, expected_actual_st_rank: 3, exhibition_st: 0.13, display_time_delta_vs_left: 0 } }),
  makeRow(5, { features: { expected_actual_st: 0.13, expected_actual_st_rank: 3, exhibition_st: 0.13, display_time_delta_vs_left: 0 } })
];
const laneAwareLaunchScores = computeLaunchStateScores(laneAwareStartRows);
const lane1Start = laneAwareLaunchScores.find((row) => row.lane === 1);
const lane5Start = laneAwareLaunchScores.find((row) => row.lane === 5);
assert.ok(
  Number(lane1Start?.final_start_advantage_score || 0) > Number(lane5Start?.final_start_advantage_score || 0),
  "lane-aware ST interpretation should still favor inner lanes for the same ST"
);
assert.ok(
  Number(lane1Start?.lane_base_advantage || 0) > Number(lane5Start?.lane_base_advantage || 0),
  "start advantage logging should expose lane-prior advantage by lane"
);

const mildBoat1F = getFHolderPenaltyByRole({ f_hold_caution_penalty: 1.4, f_hold_count: 1 }, 1);
const strongOuterF = getFHolderPenaltyByRole({ f_hold_caution_penalty: 1.4, f_hold_count: 1 }, 4);
assert.ok(
  mildBoat1F.first_penalty < strongOuterF.first_penalty,
  "boat 1 F-holder penalty should stay milder than non-boat1 first-place penalty"
);
assert.ok(
  strongOuterF.second_penalty < strongOuterF.first_penalty && strongOuterF.third_penalty < strongOuterF.second_penalty,
  "non-boat1 F-holder penalties should be strongest for first, smaller for second, minimal for third"
);

const nonBoat1FRows = [
  makeRow(1, { features: { exhibition_rank: 2, motor_total_score: 10.1, expected_actual_st_rank: 2 } }),
  makeRow(2, { features: { exhibition_rank: 3, motor_total_score: 9.5, expected_actual_st_rank: 3 } }),
  makeRow(3, { features: { exhibition_rank: 2, motor_total_score: 9.9, expected_actual_st_rank: 2, f_hold_caution_penalty: 1.8, f_hold_count: 1 } }),
  makeRow(4, { features: { exhibition_rank: 4, motor_total_score: 8.9, expected_actual_st_rank: 4 } }),
  makeRow(5, { features: { exhibition_rank: 5, motor_total_score: 8.1, expected_actual_st_rank: 5 } }),
  makeRow(6, { features: { exhibition_rank: 6, motor_total_score: 7.7, expected_actual_st_rank: 6 } })
];
const nonBoat1FDistributions = buildSeparatedCandidateDistributions({
  ranking: nonBoat1FRows,
  tickets: [],
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: noAttack,
  learningWeights,
  race
});
const lane3FirstWeight = Number(
  nonBoat1FDistributions.first_place_distribution_json.find((row) => row.lane === 3)?.weight || 0
);
const lane3SecondWeight = Number(
  nonBoat1FDistributions.second_place_distribution_json.find((row) => row.lane === 3)?.weight || 0
);
const lane3ThirdWeight = Number(
  nonBoat1FDistributions.third_place_distribution_json.find((row) => row.lane === 3)?.weight || 0
);
assert.ok(
  lane3SecondWeight > 0 && lane3ThirdWeight > 0,
  "non-boat1 F-holders should still remain reasonably alive for second/third"
);
assert.ok(
  lane3FirstWeight < lane3SecondWeight,
  "non-boat1 F-holders should be penalized mainly for first-place rather than top-3 survival"
);

const exactaSnapshot = buildExactaCoverageSnapshot({
  ranking: lane3AttackRows,
  recommendedBets: [{ combo: "1-3-2", prob: 0.12, recommended_bet: 200 }],
  optimizedTickets: [{ combo: "1-3-4", prob: 0.14, recommended_bet: 300 }],
  finalRecommendedSnapshot: { items: [{ combo: "1-3-2", prob: 0.12, recommended_bet: 200 }] },
  boat1HeadSnapshot: { items: [{ combo: "1-3-4", prob: 0.14, recommended_bet: 200 }] },
  headScenarioBalanceAnalysis: {
    ...insideHeadScenario,
    ...lane3Distributions
  },
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: lane3Attack,
  learningWeights,
  race
});
assert.equal(exactaSnapshot.shown, true, "exacta cover should be shown when 1-head partner focus is justified");
assert.ok(
  exactaSnapshot.items.every((row) => row.combo.startsWith("1-")),
  "boat-1 escape exacta should stay focused on 1-head covers"
);
assert.ok(
  exactaSnapshot.items.some((row) => row.combo.startsWith("1-3")),
  "lane 3 attack should produce 1-3 exacta cover when boat 1 still survives"
);

const diffuseExactaSnapshot = buildExactaCoverageSnapshot({
  ranking: insideRows,
  recommendedBets: [],
  optimizedTickets: [],
  finalRecommendedSnapshot: { items: [] },
  boat1HeadSnapshot: { items: [] },
  headScenarioBalanceAnalysis: {
    ...insideHeadScenario,
    main_head_lane: 1,
    boat1_second_place_distribution_json: [
      { lane: 2, weight: 0.2 },
      { lane: 3, weight: 0.16 },
      { lane: 4, weight: 0.15 },
      { lane: 5, weight: 0.13 },
      { lane: 6, weight: 0.12 }
    ]
  },
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: noAttack,
  learningWeights,
  race
});
assert.equal(diffuseExactaSnapshot.shown, false, "exacta cover should stay hidden when second-place focus is too diffuse");

const outerAggressiveRows = [
  makeRow(1, { features: { exhibition_rank: 2, motor_total_score: 9.8, expected_actual_st_rank: 2 } }),
  makeRow(2, { features: { exhibition_rank: 3, motor_total_score: 9.3, expected_actual_st_rank: 3 } }),
  makeRow(3, { features: { exhibition_rank: 4, motor_total_score: 9.0, expected_actual_st_rank: 4 } }),
  makeRow(4, { features: { exhibition_rank: 5, motor_total_score: 8.6, expected_actual_st_rank: 5 } }),
  makeRow(5, { features: { exhibition_rank: 1, lap_attack_flag: 1, lap_attack_strength: 12, motor_total_score: 10.8, expected_actual_st_rank: 1 } }),
  makeRow(6, { features: { exhibition_rank: 1, lap_attack_flag: 1, lap_attack_strength: 13, motor_total_score: 11.0, expected_actual_st_rank: 1 } })
];
const outerGuardDistributions = buildSeparatedCandidateDistributions({
  ranking: outerAggressiveRows,
  tickets: [],
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: noAttack,
  learningWeights,
  race
});
const topTwoFirstLanes = laneOrder(outerGuardDistributions.first_place_probability_json, 2);
assert.ok(
  !topTwoFirstLanes.includes(5) && !topTwoFirstLanes.includes(6),
  "boats 5/6 should not be over-promoted to top first-place probabilities by one strong metric alone"
);

const explicitRoleLayers = buildRoleProbabilityLayers({
  rows: lane3AttackRows,
  candidateDistributions: lane3Distributions,
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: lane3Attack
});
assert.equal(
  explicitRoleLayers.role_probability_summary_json.main_head_lane,
  1,
  "role probability summary should keep boat 1 as main head in a survivable lane 3 attack race"
);
const featureBundle = buildPredictionFeatureBundle({
  ranking: lane3AttackRows,
  race,
  entryMeta: { predicted_entry_order: [1, 2, 3, 4, 5, 6], actual_entry_order: [1, 2, 3, 4, 5, 6] },
  learningWeights,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: lane3Attack,
  headScenarioBalanceAnalysis: insideHeadScenario,
  candidateDistributions: lane3Distributions
});
const firstPlaceProbabilities = computeFirstPlaceProbabilities(featureBundle);
const secondPlaceProbabilities = computeSecondPlaceProbabilities(
  featureBundle,
  computeBoat1EscapeProbability(featureBundle),
  computeAttackScenarioProbabilities(featureBundle),
  firstPlaceProbabilities
);
const survivalProbabilities = computeSurvivalProbabilities(featureBundle);
const thirdPlaceProbabilities = computeThirdPlaceProbabilities(
  featureBundle,
  firstPlaceProbabilities,
  secondPlaceProbabilities,
  computeAttackScenarioProbabilities(featureBundle),
  survivalProbabilities
);
const evidenceBiasTable = buildEvidenceBiasTable({
  featureBundle,
  firstProbs: firstPlaceProbabilities,
  secondProbs: secondPlaceProbabilities,
  thirdProbs: thirdPlaceProbabilities,
  attackProbs: computeAttackScenarioProbabilities(featureBundle),
  survivalProbs: survivalProbabilities
});
assert.equal(
  evidenceBiasTable.confirmation_flags.main_head_candidate,
  1,
  "stable inside races should still confirm boat 1 as the main head candidate"
);
assert.ok(
  evidenceBiasTable.boat_summary["3"].second_support_score > evidenceBiasTable.boat_summary["3"].head_support_score,
  "lane 3 attack support should raise second-place confirmation more than head confirmation"
);
const confirmedRoleProbabilities = applyEvidenceBiasConfirmationToRoleProbabilities({
  featureBundle,
  firstProbs: firstPlaceProbabilities,
  secondProbs: secondPlaceProbabilities,
  thirdProbs: thirdPlaceProbabilities,
  evidenceBiasTable
});
const finishOrderCandidates = composeFinishOrderCandidates({
  featureBundle,
  firstProbs: confirmedRoleProbabilities.confirmed_first_place_probability_json,
  secondProbs: confirmedRoleProbabilities.confirmed_second_place_probability_json,
  thirdProbs: confirmedRoleProbabilities.confirmed_third_place_probability_json,
  attackProbs: computeAttackScenarioProbabilities(featureBundle),
  survivalProbs: survivalProbabilities
});
const mainRoleTickets = generateMainTrifectaTickets(finishOrderCandidates, 68);
assert.ok(
  mainRoleTickets.some((row) => row.combo.startsWith("1-3-")),
  "role-based main trifecta generation should keep 1-3-x available in lane 3 attack races"
);
const roleBasedExacta = generateExactaCoverTickets(firstPlaceProbabilities, secondPlaceProbabilities, 68);
assert.ok(
  roleBasedExacta.every((row) => row.combo.startsWith("1-")),
  "role-based exacta cover should stay aligned with boat 1 as the main head"
);

const backupUrasujiSnapshot = buildBackupUrasujiRecommendationsSnapshot({
  recommendedBets: [
    { combo: "1-3-4", prob: 0.11, recommended_bet: 200 },
    { combo: "1-2-3", prob: 0.108, recommended_bet: 200 }
  ],
  optimizedTickets: [
    { combo: "1-3-4", prob: 0.12, recommended_bet: 300 }
  ],
  candidateDistributions: lane3Distributions
});
assert.equal(
  backupUrasujiSnapshot.shown,
  true,
  "optional urasuji backup should appear only when the opponent model actually flags it"
);
const roleBasedBackupUrasuji = generateBackupUrasujiTickets(
  finishOrderCandidates,
  computeAttackScenarioProbabilities(featureBundle),
  68
);
assert.ok(
  roleBasedBackupUrasuji.length <= 2,
  "role-based backup urasuji should stay compact"
);
assert.ok(
  evidenceBiasTable.per_group_rankings.motor.length > 0 &&
    evidenceBiasTable.per_group_rankings.exhibition.length > 0,
  "evidence bias table should expose grouped rankings instead of raw-column vote counting"
);

const outsideLeadOnlyRows = [
  makeRow(1, { features: { exhibition_rank: 2, motor_total_score: 10.1, expected_actual_st_rank: 2 } }),
  makeRow(2, { features: { exhibition_rank: 4, motor_total_score: 8.6, expected_actual_st_rank: 4 } }),
  makeRow(3, { features: { exhibition_rank: 4, motor_total_score: 8.8, expected_actual_st_rank: 4, lap_attack_strength: 6.2 } }),
  makeRow(4, { features: { exhibition_rank: 3, motor_total_score: 9.1, expected_actual_st_rank: 3, entry_advantage_score: 6 } }),
  makeRow(5, { features: { exhibition_rank: 2, entry_advantage_score: 6.5 } }),
  makeRow(6, { features: { exhibition_rank: 2, entry_advantage_score: 6.8 } })
];
const outsideLeadOnlyEscape = {
  escape_pattern_applied: false,
  formation_pattern: "outside_lead",
  escape_second_place_bias_json: {}
};
const outsideLeadOnlyHeadScenario = {
  head_distribution_json: [
    { lane: 1, weight: 0.42 },
    { lane: 4, weight: 0.2 },
    { lane: 5, weight: 0.15 },
    { lane: 6, weight: 0.12 },
    { lane: 3, weight: 0.07 },
    { lane: 2, weight: 0.04 }
  ],
  second_distribution_json: [
    { lane: 4, weight: 0.24 },
    { lane: 5, weight: 0.19 },
    { lane: 3, weight: 0.18 },
    { lane: 6, weight: 0.17 },
    { lane: 2, weight: 0.12 }
  ],
  main_head_lane: 1,
  attack_head_lane: 4,
  survival_residual_score: 36
};
const outsideLeadOnlyAttack = {
  attack_scenario_applied: 1,
  attack_scenario_type: "outside_lead",
  attack_scenario_score: 71
};
const outsideLeadOnlyDistributions = buildSeparatedCandidateDistributions({
  ranking: outsideLeadOnlyRows,
  tickets: [],
  headScenarioBalanceAnalysis: outsideLeadOnlyHeadScenario,
  escapePatternAnalysis: outsideLeadOnlyEscape,
  attackScenarioAnalysis: outsideLeadOnlyAttack,
  learningWeights,
  race
});
assert.ok(
  !laneOrder(outsideLeadOnlyDistributions.first_place_probability_json, 2).includes(5) &&
    !laneOrder(outsideLeadOnlyDistributions.first_place_probability_json, 2).includes(6),
  "outside_lead alone should not create 5/6 main first-place candidates"
);
assert.ok(
  outsideLeadOnlyDistributions.attack_scenario_probability_json.length > 0,
  "outside_lead should still contribute to attack scenario probability"
);
const outsideLeadOnlyEvidenceTable = buildEvidenceBiasTable({
  featureBundle: buildPredictionFeatureBundle({
    ranking: outsideLeadOnlyRows,
    race,
    entryMeta: { predicted_entry_order: [1, 2, 3, 4, 5, 6], actual_entry_order: [1, 2, 3, 4, 5, 6] },
    learningWeights,
    escapePatternAnalysis: outsideLeadOnlyEscape,
    attackScenarioAnalysis: outsideLeadOnlyAttack,
    headScenarioBalanceAnalysis: outsideLeadOnlyHeadScenario,
    candidateDistributions: outsideLeadOnlyDistributions
  }),
  firstProbs: outsideLeadOnlyDistributions.first_place_probability_json,
  secondProbs: outsideLeadOnlyDistributions.second_place_probability_json,
  thirdProbs: outsideLeadOnlyDistributions.third_place_probability_json,
  attackProbs: outsideLeadOnlyDistributions.attack_scenario_probability_json,
  survivalProbs: outsideLeadOnlyDistributions.survival_probability_json
});
assert.ok(
  outsideLeadOnlyEvidenceTable.boat_summary["5"].warnings.includes("OUTER_SUPPORT_NARROW"),
  "outside-heavy support should be marked as narrow instead of over-counted as broad independent evidence"
);

const strongOuterRows = [
  makeRow(1, { features: { exhibition_rank: 5, motor_total_score: 7.8, expected_actual_st_rank: 5, f_hold_caution_penalty: 1.8 } }),
  makeRow(2, { features: { exhibition_rank: 5, motor_total_score: 7.9, expected_actual_st_rank: 5, f_hold_caution_penalty: 1.4 } }),
  makeRow(3, { features: { exhibition_rank: 2, motor_total_score: 9.9, expected_actual_st_rank: 2, lap_attack_strength: 9.2, slit_alert_flag: 1 } }),
  makeRow(4, { features: { exhibition_rank: 1, motor_total_score: 10.4, expected_actual_st_rank: 1, lap_attack_strength: 10.1, slit_alert_flag: 1, entry_advantage_score: 10 } }),
  makeRow(5, { features: { exhibition_rank: 1, motor_total_score: 10.9, motor_trend_score: 3.1, expected_actual_st_rank: 1, lap_attack_flag: 1, lap_attack_strength: 12.4, avg_st_rank_delta_vs_left: 1.4, slit_alert_flag: 1, entry_advantage_score: 8.5, course_fit_score: 2.1, outer_head_support_score: 84 } }),
  makeRow(6, { features: { exhibition_rank: 2, motor_total_score: 10.2, motor_trend_score: 2.9, expected_actual_st_rank: 2, lap_attack_flag: 1, lap_attack_strength: 10.8, avg_st_rank_delta_vs_left: 1.2, slit_alert_flag: 1, entry_advantage_score: 7.4, course_fit_score: 1.8, outer_head_support_score: 80 } })
];
const strongOuterHeadScenario = {
  head_distribution_json: [
    { lane: 4, weight: 0.28 },
    { lane: 5, weight: 0.24 },
    { lane: 1, weight: 0.2 },
    { lane: 6, weight: 0.14 },
    { lane: 3, weight: 0.1 },
    { lane: 2, weight: 0.04 }
  ],
  second_distribution_json: [
    { lane: 5, weight: 0.25 },
    { lane: 4, weight: 0.24 },
    { lane: 6, weight: 0.19 },
    { lane: 3, weight: 0.18 },
    { lane: 1, weight: 0.14 }
  ],
  main_head_lane: 4,
  attack_head_lane: 5,
  survival_residual_score: 14
};
const strongOuterAttack = {
  attack_scenario_applied: 1,
  attack_scenario_type: "outside_lead",
  attack_scenario_score: 86,
  four_cado_makuri_score: 78
};
const strongOuterDistributions = buildSeparatedCandidateDistributions({
  ranking: strongOuterRows,
  tickets: [],
  headScenarioBalanceAnalysis: strongOuterHeadScenario,
  escapePatternAnalysis: outsideLeadOnlyEscape,
  attackScenarioAnalysis: strongOuterAttack,
  learningWeights,
  race
});
assert.ok(
  strongOuterDistributions.outside_head_promotion_gate_json.by_lane["5"].evidence_count >= 4,
  "strong outer setup should accumulate multiple evidence categories"
);
assert.equal(
  strongOuterDistributions.outside_head_promotion_gate_json.by_lane["5"].allowed_as_main_head,
  1,
  "genuine outer setup with strong inner collapse should allow lane 5 main-head consideration"
);
const strongOuterFeatureBundle = buildPredictionFeatureBundle({
  ranking: strongOuterRows,
  race,
  entryMeta: { predicted_entry_order: [1, 2, 3, 4, 5, 6], actual_entry_order: [1, 2, 3, 4, 5, 6] },
  learningWeights,
  escapePatternAnalysis: outsideLeadOnlyEscape,
  attackScenarioAnalysis: strongOuterAttack,
  headScenarioBalanceAnalysis: strongOuterHeadScenario,
  candidateDistributions: strongOuterDistributions
});
const strongOuterEvidenceTable = buildEvidenceBiasTable({
  featureBundle: strongOuterFeatureBundle,
  firstProbs: computeFirstPlaceProbabilities(strongOuterFeatureBundle),
  secondProbs: computeSecondPlaceProbabilities(
    strongOuterFeatureBundle,
    computeBoat1EscapeProbability(strongOuterFeatureBundle),
    computeAttackScenarioProbabilities(strongOuterFeatureBundle),
    computeFirstPlaceProbabilities(strongOuterFeatureBundle)
  ),
  thirdProbs: computeThirdPlaceProbabilities(
    strongOuterFeatureBundle,
    computeFirstPlaceProbabilities(strongOuterFeatureBundle),
    computeSecondPlaceProbabilities(
      strongOuterFeatureBundle,
      computeBoat1EscapeProbability(strongOuterFeatureBundle),
      computeAttackScenarioProbabilities(strongOuterFeatureBundle),
      computeFirstPlaceProbabilities(strongOuterFeatureBundle)
    ),
    computeAttackScenarioProbabilities(strongOuterFeatureBundle),
    computeSurvivalProbabilities(strongOuterFeatureBundle)
  ),
  attackProbs: computeAttackScenarioProbabilities(strongOuterFeatureBundle),
  survivalProbs: computeSurvivalProbabilities(strongOuterFeatureBundle)
});
assert.ok(
  strongOuterEvidenceTable.boat_summary["5"].independent_evidence_count >= 3,
  "broad independent group support should be counted once per group and confirm strong outer setups"
);

const chaosFeatureBundle = buildPredictionFeatureBundle({
  ranking: strongOuterRows,
  race,
  entryMeta: { predicted_entry_order: [1, 2, 3, 4, 5, 6], actual_entry_order: [1, 2, 3, 4, 5, 6], entry_changed: true },
  learningWeights,
  escapePatternAnalysis: outsideLeadOnlyEscape,
  attackScenarioAnalysis: strongOuterAttack,
  headScenarioBalanceAnalysis: {
    ...strongOuterHeadScenario,
    chaos_risk_score: 88,
    head_confidence: 48,
    outer_head_guard_applied: 1
  },
  candidateDistributions: strongOuterDistributions
});
const chaosFinishOrderCandidates = composeFinishOrderCandidates({
  featureBundle: chaosFeatureBundle,
  firstProbs: computeFirstPlaceProbabilities(chaosFeatureBundle),
  secondProbs: computeSecondPlaceProbabilities(
    chaosFeatureBundle,
    computeBoat1EscapeProbability(chaosFeatureBundle),
    computeAttackScenarioProbabilities(chaosFeatureBundle),
    computeFirstPlaceProbabilities(chaosFeatureBundle)
  ),
  thirdProbs: computeThirdPlaceProbabilities(
    chaosFeatureBundle,
    computeFirstPlaceProbabilities(chaosFeatureBundle),
    computeSecondPlaceProbabilities(
      chaosFeatureBundle,
      computeBoat1EscapeProbability(chaosFeatureBundle),
      computeAttackScenarioProbabilities(chaosFeatureBundle),
      computeFirstPlaceProbabilities(chaosFeatureBundle)
    ),
    computeAttackScenarioProbabilities(chaosFeatureBundle),
    computeSurvivalProbabilities(chaosFeatureBundle)
  ),
  attackProbs: computeAttackScenarioProbabilities(chaosFeatureBundle),
  survivalProbs: computeSurvivalProbabilities(chaosFeatureBundle)
});
assert.ok(
  chaosFinishOrderCandidates.every((row) => !row.combo.startsWith("5-") && !row.combo.startsWith("6-")),
  "chaos / low-head-confidence races should not output aggressive 5/6-head main candidates"
);

const messyRows = [
  makeRow(1, { features: { exhibition_st: null, exhibition_time: null, f_hold_caution_penalty: 2.8 } }),
  makeRow(2, { features: { exhibition_st: null, exhibition_time: null, lap_attack_strength: 8, slit_alert_flag: 1, f_hold_caution_penalty: 2.4 } }),
  makeRow(3, { features: { exhibition_st: null, exhibition_time: null, lap_attack_strength: 8.5, slit_alert_flag: 1, f_hold_caution_penalty: 2.2 } }),
  makeRow(4, { features: { exhibition_st: null, exhibition_time: null, lap_attack_strength: 8.7, slit_alert_flag: 1, f_hold_caution_penalty: 2.0 } }),
  makeRow(5, { features: { exhibition_st: null, exhibition_time: null, lap_attack_strength: 9.2, slit_alert_flag: 1, f_hold_caution_penalty: 2.1 } }),
  makeRow(6, { features: { exhibition_st: null, exhibition_time: null, lap_attack_strength: 9.6, slit_alert_flag: 1, f_hold_caution_penalty: 2.3 } })
];
const messyDecision = buildParticipationDecision({
  raceDecision: { mode: "WATCH", confidence: 46, factors: { formation_pattern_clarity_score: 36 } },
  raceRisk: { recommendation: "WATCH" },
  raceStructure: { head_stability_score: 44, chaos_risk_score: 88, formation_pattern_clarity_score: 36 },
  entryMeta: { severity: "high", entry_changed: true },
  confidenceScores: {
    head_fixed_confidence_pct: 52,
    recommended_bet_confidence_pct: 45,
    confidence_reason_tags: ["INSUFFICIENT_EXHIBITION_DATA", "ENTRY_CHANGE_PENALTY", "ST_CHAOS"],
    f_hold_caution_score: 18,
    segment_participation_correction: 0
  },
  scenarioSuggestions: { scenario_confidence: 34 },
  raceFlow: { slit_alert_lanes: [2, 3, 4, 5], race_flow_mode: "makuri" },
  escapePatternAnalysis: { ...insideEscape, escape_pattern_confidence: 34 },
  attackScenarioAnalysis: { attack_scenario_applied: 1, attack_scenario_type: "outside_lead", attack_scenario_score: 74 },
  headScenarioBalanceAnalysis: {
    ...insideHeadScenario,
    main_head_lane: 1,
    second_head_lane: 5,
    survival_residual_score: 20,
    attack_dominance_margin: 8,
    outer_head_guard_applied: 1,
    second_place_distribution_json: [
      { lane: 2, weight: 0.22 },
      { lane: 3, weight: 0.2 },
      { lane: 4, weight: 0.18 },
      { lane: 5, weight: 0.17 },
      { lane: 6, weight: 0.15 }
    ],
    third_place_distribution_json: [
      { lane: 2, weight: 0.18 },
      { lane: 3, weight: 0.17 },
      { lane: 4, weight: 0.16 },
      { lane: 5, weight: 0.15 },
      { lane: 6, weight: 0.14 }
    ],
    boat1_second_place_distribution_json: [
      { lane: 2, weight: 0.2 },
      { lane: 3, weight: 0.19 },
      { lane: 4, weight: 0.17 },
      { lane: 5, weight: 0.16 },
      { lane: 6, weight: 0.15 }
    ],
    boat1_third_place_distribution_json: [
      { lane: 2, weight: 0.17 },
      { lane: 3, weight: 0.16 },
      { lane: 4, weight: 0.15 },
      { lane: 5, weight: 0.15 },
      { lane: 6, weight: 0.14 }
    ]
  },
  roleProbabilityLayers: {
    first_place_probability_json: [
      { lane: 1, weight: 0.31 },
      { lane: 3, weight: 0.22 },
      { lane: 4, weight: 0.17 },
      { lane: 5, weight: 0.15 },
      { lane: 6, weight: 0.1 }
    ],
    second_place_probability_json: [
      { lane: 2, weight: 0.22 },
      { lane: 3, weight: 0.2 },
      { lane: 4, weight: 0.19 },
      { lane: 5, weight: 0.16 },
      { lane: 6, weight: 0.13 }
    ],
    third_place_probability_json: [
      { lane: 2, weight: 0.18 },
      { lane: 3, weight: 0.17 },
      { lane: 4, weight: 0.16 },
      { lane: 5, weight: 0.15 },
      { lane: 6, weight: 0.14 }
    ],
    boat1_second_place_probability_json: [
      { lane: 2, weight: 0.2 },
      { lane: 3, weight: 0.19 },
      { lane: 4, weight: 0.17 },
      { lane: 5, weight: 0.16 },
      { lane: 6, weight: 0.15 }
    ],
    boat1_third_place_probability_json: [
      { lane: 2, weight: 0.17 },
      { lane: 3, weight: 0.16 },
      { lane: 4, weight: 0.15 },
      { lane: 5, weight: 0.15 },
      { lane: 6, weight: 0.14 }
    ]
  },
  ranking: messyRows,
  racers: messyRows.map((row) => row.racer),
  exactaSnapshot: { items: [], shown: false },
  learningWeights,
  race
});
assert.equal(
  messyDecision.decision,
  "not_recommended",
  "messy low-quality races should degrade to Skip/Not Recommended instead of Participate"
);

const weakStLane3Rows = [
  makeRow(1, { features: { exhibition_rank: 1, expected_actual_st: 0.12, expected_actual_st_rank: 1, motor_total_score: 10.4 } }),
  makeRow(2, { features: { exhibition_rank: 2, expected_actual_st: 0.13, expected_actual_st_rank: 2, motor_total_score: 9.7 } }),
  makeRow(3, {
    features: {
      exhibition_rank: 4,
      expected_actual_st: 0.18,
      expected_actual_st_rank: 5,
      st_rank: 5,
      motor_total_score: 8.9,
      lap_attack_strength: 4.2,
      slit_alert_flag: 0,
      entry_advantage_score: 2
    }
  }),
  makeRow(4),
  makeRow(5),
  makeRow(6)
];
const weakStSuppression = buildBoat3WeakStHeadSuppressionContext({
  rows: weakStLane3Rows,
  headScenarioBalanceAnalysis: insideHeadScenario,
  attackScenarioAnalysis: noAttack,
  outsideHeadPromotionContext: {
    inner_collapse_score: 32,
    by_lane: new Map()
  }
});
assert.equal(
  weakStSuppression.applied,
  1,
  "boat 3 weak-ST suppression should trigger when lane 3 is clearly slower than lanes 1 and 2 in a stable inside race"
);
const weakStDistributions = buildSeparatedCandidateDistributions({
  ranking: weakStLane3Rows,
  tickets: [],
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: noAttack,
  learningWeights,
  race
});
assert.equal(
  weakStDistributions.boat3_weak_st_head_suppressed,
  1,
  "candidate distributions should log boat 3 weak-ST head suppression"
);
assert.ok(
  laneOrder(weakStDistributions.first_place_probability_json, 2)[0] === 1 &&
    !laneOrder(weakStDistributions.first_place_probability_json, 2).includes(3),
  "lane 3 should not stay near the top of first-place probabilities when weak-ST suppression applies"
);

const playerHistoryProfile = buildPlayerStatProfileFromHistory({
  raceDate: "2026-03-15",
  historyRows: [
    { race_date: "2026-03-01", start_lane: 1, finish_1: 1, finish_2: 2, finish_3: 3 },
    { race_date: "2026-02-20", start_lane: 1, finish_1: 2, finish_2: 1, finish_3: 3 },
    { race_date: "2026-01-14", start_lane: 1, finish_1: 1, finish_2: 3, finish_3: 2 },
    { race_date: "2025-12-10", start_lane: 1, finish_1: 3, finish_2: 1, finish_3: 2 },
    { race_date: "2025-08-10", start_lane: 1, finish_1: 1, finish_2: 2, finish_3: 3 }
  ]
});
assert.equal(
  playerHistoryProfile.recent_3_months_sample_size,
  3,
  "recent player window should only count races from the last 3 months"
);
assert.equal(
  playerHistoryProfile.current_season_sample_size,
  3,
  "current season player window should only count races from the current season"
);
assert.ok(
  playerHistoryProfile.recent_3_months_start === "2025-12-15" &&
    playerHistoryProfile.current_season_start === "2026-01-01",
  "player stat profile should expose the exact recent and current-season date windows"
);

const topRecommendedTickets = buildTopRecommendedTickets({
  finalRecommendedBets: [
    { combo: "1-2-3", prob: 0.19 },
    { combo: "1-3-2", prob: 0.16 }
  ],
  exactaBets: [
    { combo: "1-2", prob: 0.19, exacta_head_score: 78, exacta_partner_score: 72 },
    { combo: "1-3", prob: 0.14, exacta_head_score: 72, exacta_partner_score: 68 }
  ],
  backupUrasujiBets: [
    { combo: "3-1-4", prob: 0.08 }
  ],
  maxItems: 10
});
assert.equal(topRecommendedTickets.length, 3, "top recommended tickets should now keep only trifecta rows");
assert.equal(topRecommendedTickets[0].ticket, "1-2-3", "main trifecta should stay at the top when hit rate is tied");
assert.ok(
  topRecommendedTickets.every((row) => row.ticket_type === "trifecta"),
  "top recommended tickets should exclude exacta from the final visible list"
);
assert.ok(
  topRecommendedTickets.every((row, idx, arr) => idx === 0 || arr[idx - 1].estimated_hit_rate >= row.estimated_hit_rate),
  "top recommended tickets should be sorted by estimated hit rate descending"
);

const upsetRiskScore = computeUpsetRiskScore({
  confidenceScores: {
    head_fixed_confidence_pct: 58,
    recommended_bet_confidence_pct: 54
  },
  participationDecision: {
    decision: "watch",
    participation_score_components: {
      race_stability_score: 38,
      prediction_readability_score: 42,
      partner_clarity_score: 36,
      quality_gate_applied: 1
    }
  },
  roleProbabilityLayers: {
    boat1_escape_probability: 0.41,
    first_place_probability_json: [{ lane: 1, weight: 0.33 }, { lane: 3, weight: 0.24 }, { lane: 4, weight: 0.18 }],
    second_place_probability_json: [{ lane: 3, weight: 0.22 }, { lane: 4, weight: 0.2 }, { lane: 2, weight: 0.18 }],
    third_place_probability_json: [{ lane: 4, weight: 0.18 }, { lane: 5, weight: 0.17 }, { lane: 2, weight: 0.16 }]
  },
  attackScenarioAnalysis: {
    attack_scenario_label: "4カド捲り注意",
    attack_scenario_score: 74
  },
  headScenarioBalanceAnalysis: insideHeadScenario,
  outsideHeadPromotionGate: {
    inner_collapse_score: 64,
    by_lane: {
      "5": { matched_evidence_categories: ["entry_shape_advantage", "lap_exhibition_advantage", "strong_motor"] },
      "6": { matched_evidence_categories: ["clear_exhibition_st_advantage", "learning_correction_match", "inner_collapse_evidence"] }
    }
  }
});
assert.ok(upsetRiskScore >= 62, "risky messy races should cross the upset-alert threshold");
assert.equal(
  shouldShowUpsetAlert({
    upsetRiskScore,
    confidenceScores: { head_fixed_confidence_pct: 58 },
    boat1EscapeProbability: 0.41,
    participationDecision: { decision: "watch" }
  }),
  true,
  "upset alert should be shown when risk is materially high"
);
const upsetAlert = buildUpsetAlert({
  upsetRiskScore,
  showUpsetAlert: true,
  attackScenarioAnalysis: { attack_scenario_label: "4カド捲り注意" },
  escapePatternAnalysis: { formation_pattern: "outside_lead" },
  roleProbabilityLayers: {
    first_place_probability_json: [{ lane: 1, weight: 0.33 }, { lane: 4, weight: 0.22 }, { lane: 5, weight: 0.15 }],
    second_place_probability_json: [{ lane: 4, weight: 0.23 }, { lane: 5, weight: 0.18 }, { lane: 2, weight: 0.15 }]
  },
  outsideHeadPromotionGate: {
    inner_collapse_score: 64,
    by_lane: {
      "5": { matched_evidence_categories: ["entry_shape_advantage", "lap_exhibition_advantage", "strong_motor"] }
    }
  },
  isRecommendedRace: false,
  backupUrasujiBets: [{ combo: "4-1-5", prob: 0.08 }],
  finalRecommendedBets: [{ combo: "1-4-5", prob: 0.11 }]
});
assert.equal(upsetAlert.shown, true, "upset alert payload should exist for risky races");
assert.equal(upsetAlert.reference_only, true, "watch/skip races should only expose upset tickets as weak reference");

const launchConfig = getLaunchStateConfig();
assert.equal(
  launchConfig.score_thresholds.LAUNCH_SCORE_STRONG_OUT,
  28,
  "launch state config should expose conservative default strong_out threshold"
);

const neutralLaunchRows = [
  makeRow(1, { features: { expected_actual_st: 0.12, expected_actual_st_rank: 3, exhibition_st: 0.12, display_time_delta_vs_left: 0, entry_advantage_score: 0, lap_attack_strength: 0, slit_alert_flag: 0 } }),
  makeRow(2, { features: { expected_actual_st: 0.121, expected_actual_st_rank: 4, exhibition_st: 0.121, display_time_delta_vs_left: 0, entry_advantage_score: 0, lap_attack_strength: 0, slit_alert_flag: 0 } }),
  makeRow(3, { features: { expected_actual_st: 0.122, expected_actual_st_rank: 4, exhibition_st: 0.122, display_time_delta_vs_left: 0, entry_advantage_score: 0, lap_attack_strength: 0, slit_alert_flag: 0 } })
];
const neutralLaunchStates = classifyLaunchStates(computeLaunchStateScores(neutralLaunchRows));
assert.equal(
  neutralLaunchStates.find((row) => row.lane === 3)?.label,
  "neutral",
  "small ST differences should remain neutral"
);

const strongHollowRows = [
  makeRow(1, { features: { expected_actual_st: 0.1, expected_actual_st_rank: 1, exhibition_st: 0.1 } }),
  makeRow(2, { features: { expected_actual_st: 0.11, expected_actual_st_rank: 2, exhibition_st: 0.11 } }),
  makeRow(3, { features: { expected_actual_st: 0.17, expected_actual_st_rank: 6, exhibition_st: 0.17, display_time_delta_vs_left: -0.02 } })
];
const strongHollowStates = classifyLaunchStates(computeLaunchStateScores(strongHollowRows));
assert.ok(
  ["hollow", "strong_hollow"].includes(strongHollowStates.find((row) => row.lane === 3)?.label),
  "clearly slower boats should fall into hollow-side states"
);

const launchStateRows = [
  makeRow(1, {
    features: {
      expected_actual_st: 0.11,
      expected_actual_st_rank: 1,
      exhibition_st: 0.11,
      exhibition_time: 6.72
    }
  }),
  makeRow(2, {
    features: {
      expected_actual_st: 0.12,
      expected_actual_st_rank: 2,
      exhibition_st: 0.12,
      exhibition_time: 6.73
    }
  }),
  makeRow(3, {
    features: {
      expected_actual_st: 0.145,
      expected_actual_st_rank: 5,
      exhibition_st: 0.145,
      exhibition_time: 6.77,
      lap_attack_flag: 0,
      lap_attack_strength: 1.5,
      slit_alert_flag: 0,
      display_time_delta_vs_left: -0.01
    }
  }),
  makeRow(4, {
    features: {
      expected_actual_st: 0.1,
      expected_actual_st_rank: 1,
      exhibition_st: 0.1,
      exhibition_time: 6.7,
      lap_attack_flag: 1,
      lap_attack_strength: 9.2,
      slit_alert_flag: 1,
      display_time_delta_vs_left: 0.08,
      entry_advantage_score: 10
    }
  }),
  makeRow(5, {
    features: {
      expected_actual_st: 0.12,
      expected_actual_st_rank: 2,
      exhibition_st: 0.12,
      exhibition_time: 6.71,
      lap_attack_strength: 8.4
    }
  }),
  makeRow(6, {
    features: {
      expected_actual_st: 0.14,
      expected_actual_st_rank: 5,
      exhibition_st: 0.14,
      exhibition_time: 6.76
    }
  })
];
const launchStateScores = computeLaunchStateScores(launchStateRows);
const launchStateLabels = classifyLaunchStates(launchStateScores);
assert.equal(
  launchStateLabels.find((row) => row.lane === 4)?.label,
  "strong_out",
  "lane 4 should become strong_out when it clearly jumps after lane 3 weakens"
);
assert.ok(
  ["hollow", "strong_hollow"].includes(launchStateLabels.find((row) => row.lane === 3)?.label),
  "lane 3 should move into a hollow state when its launch alignment weakens"
);
assert.ok(
  launchStateScores.every((row) => Number.isFinite(Number(row.final_launch_state_score))),
  "launch state rows should expose final launch-state scores"
);
assert.ok(
  launchStateScores.every((row) => Object.prototype.hasOwnProperty.call(row, "neighbor_margin_component")),
  "launch state rows should expose weighted component breakdowns"
);

const intermediateEvents = buildIntermediateDevelopmentEvents({
  launchStateScores,
  rows: launchStateRows,
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape
});
assert.ok(
  intermediateEvents.boat4_cado_ready >= 0.45,
  "boat4_out after boat3_hollow should raise 4-cado readiness"
);
assert.ok(
  intermediateEvents.weak_wall_on_3 >= 0.35,
  "boat3_hollow should weaken the wall on 3 for development reading"
);
assert.equal(
  intermediateEvents.triggered_flags.boat4_cado_ready,
  1,
  "boat4_cado_ready should require 4-out plus 3-weak/hollow context"
);

const launchScenarioProbabilities = computeRaceScenarioProbabilities({
  intermediateEvents,
  rows: launchStateRows,
  attackScenarioAnalysis: lane4Pressure,
  escapePatternAnalysis: insideEscape,
  outsideHeadPromotionContext: {
    inner_collapse_score: 38,
    by_lane: new Map()
  },
  headScenarioBalanceAnalysis: insideHeadScenario
});
const launchScenarioMap = new Map(launchScenarioProbabilities.map((row) => [row.scenario, row.probability]));
assert.ok(
  Number(launchScenarioMap.get("boat4_cado_attack") || 0) > Number(launchScenarioMap.get("boat3_makuri") || 0),
  "boat4_out after boat3_hollow should favor 4-cado over 3-makuri"
);

const stableLaunchStateRows = [
  makeRow(1, { features: { expected_actual_st: 0.1, expected_actual_st_rank: 1, exhibition_st: 0.1 } }),
  makeRow(2, { features: { expected_actual_st: 0.12, expected_actual_st_rank: 2, exhibition_st: 0.12 } }),
  makeRow(3, { features: { expected_actual_st: 0.14, expected_actual_st_rank: 4, exhibition_st: 0.14 } }),
  makeRow(4, { features: { expected_actual_st: 0.15, expected_actual_st_rank: 5, exhibition_st: 0.15 } }),
  makeRow(5, { features: { expected_actual_st: 0.16, expected_actual_st_rank: 6, exhibition_st: 0.16 } }),
  makeRow(6, { features: { expected_actual_st: 0.145, expected_actual_st_rank: 3, exhibition_st: 0.145 } })
];
const stableLaunchScores = computeLaunchStateScores(stableLaunchStateRows);
const stableLaunchEvents = buildIntermediateDevelopmentEvents({
  launchStateScores: stableLaunchScores,
  rows: stableLaunchStateRows,
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape
});
const stableScenarioProbabilities = computeRaceScenarioProbabilities({
  intermediateEvents: stableLaunchEvents,
  rows: stableLaunchStateRows,
  attackScenarioAnalysis: noAttack,
  escapePatternAnalysis: insideEscape,
  outsideHeadPromotionContext: {
    inner_collapse_score: 22,
    by_lane: new Map()
  },
  headScenarioBalanceAnalysis: insideHeadScenario
});
const stableScenarioMap = new Map(stableScenarioProbabilities.map((row) => [row.scenario, row.probability]));
assert.ok(
  Number(stableScenarioMap.get("boat1_escape") || 0) >= 0.5,
  "boat1_out with inner stability should strengthen the boat1 escape scenario"
);
assert.equal(
  stableLaunchEvents.triggered_flags.inner_stable,
  1,
  "inside-stable races should explicitly trigger inner_stable"
);

const boat3LaunchRows = [
  makeRow(1, { features: { expected_actual_st: 0.11, expected_actual_st_rank: 1, exhibition_st: 0.11 } }),
  makeRow(2, { features: { expected_actual_st: 0.145, expected_actual_st_rank: 5, exhibition_st: 0.145, display_time_delta_vs_left: -0.01 } }),
  makeRow(3, {
    features: {
      expected_actual_st: 0.105,
      expected_actual_st_rank: 1,
      exhibition_st: 0.105,
      lap_attack_flag: 1,
      lap_attack_strength: 8.8,
      slit_alert_flag: 1,
      display_time_delta_vs_left: 0.07
    }
  }),
  makeRow(4, { features: { expected_actual_st: 0.13, expected_actual_st_rank: 4, exhibition_st: 0.13 } }),
  makeRow(5, { features: { expected_actual_st: 0.135, expected_actual_st_rank: 5, exhibition_st: 0.135 } }),
  makeRow(6, { features: { expected_actual_st: 0.14, expected_actual_st_rank: 6, exhibition_st: 0.14 } })
];
const boat3CandidateDistributions = buildSeparatedCandidateDistributions({
  ranking: boat3LaunchRows,
  tickets: [],
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: lane3Attack,
  learningWeights,
  race
});
const boat3FeatureBundle = buildPredictionFeatureBundle({
  ranking: boat3LaunchRows,
  race,
  entryMeta: { predicted_entry_order: [1, 2, 3, 4, 5, 6], actual_entry_order: [1, 2, 3, 4, 5, 6] },
  learningWeights,
  escapePatternAnalysis: insideEscape,
  attackScenarioAnalysis: lane3Attack,
  headScenarioBalanceAnalysis: insideHeadScenario,
  candidateDistributions: boat3CandidateDistributions
});
const boat3FirstProbs = computeFirstPlaceProbabilities(boat3FeatureBundle);
const boat3SecondProbs = computeSecondPlaceProbabilities(
  boat3FeatureBundle,
  computeBoat1EscapeProbability(boat3FeatureBundle),
  computeAttackScenarioProbabilities(boat3FeatureBundle),
  boat3FirstProbs
);
const boat3AttackProbs = computeAttackScenarioProbabilities(boat3FeatureBundle);
const boat3SurvivalProbs = computeSurvivalProbabilities(boat3FeatureBundle);
const boat3ThirdProbs = computeThirdPlaceProbabilities(
  boat3FeatureBundle,
  boat3FirstProbs,
  boat3SecondProbs,
  boat3AttackProbs,
  boat3SurvivalProbs
);
const boat3LaunchScenarioMap = new Map(
  boat3FeatureBundle.launch_context.race_scenario_probabilities_json.map((row) => [row.scenario, row.probability])
);
assert.ok(
  Number(boat3LaunchScenarioMap.get("boat3_makuri") || 0) >= 0.18,
  "boat3_out should still raise boat3 attack scenarios"
);
assert.equal(
  boat3FirstProbs[0]?.lane,
  1,
  "boat3_out should not force a 3-head recommendation when boat1 survival remains strong"
);
assert.equal(
  boat3FeatureBundle.launch_context.intermediate_development_events_json.triggered_flags.boat3_attack_ready,
  1,
  "boat3_attack_ready should require both 3-out and 2-weak/hollow context"
);
const boat3ConditionalFinish = computeFinishProbsByScenario({
  scenarioProbabilities: boat3FeatureBundle.launch_context.race_scenario_probabilities_json,
  firstPlaceProbability: boat3FirstProbs,
  secondPlaceProbability: boat3SecondProbs,
  thirdPlaceProbability: boat3ThirdProbs,
  boat1EscapeProbability: computeBoat1EscapeProbability(boat3FeatureBundle)
});
const boat3ScenarioCandidates = combineScenarioAndFinishProbs({
  scenarioProbabilities: boat3FeatureBundle.launch_context.race_scenario_probabilities_json,
  conditionalFinishProbs: boat3ConditionalFinish
});
assert.ok(
  boat3ScenarioCandidates.some((row) => row.combo.startsWith("1-3-")),
  "scenario-conditioned finish mapping should keep 1-3-x alive in boat3 attack races"
);

const hollowUpsetRisk = computeUpsetRiskScore({
  confidenceScores: {
    head_fixed_confidence_pct: 56,
    recommended_bet_confidence_pct: 52
  },
  participationDecision: {
    decision: "watch",
    participation_score_components: {
      race_stability_score: 40,
      prediction_readability_score: 43,
      partner_clarity_score: 35,
      quality_gate_applied: 1
    }
  },
  roleProbabilityLayers: {
    boat1_escape_probability: 0.38,
    first_place_probability_json: [{ lane: 1, weight: 0.35 }, { lane: 4, weight: 0.22 }, { lane: 5, weight: 0.15 }],
    second_place_probability_json: [{ lane: 4, weight: 0.21 }, { lane: 5, weight: 0.19 }, { lane: 3, weight: 0.16 }],
    third_place_probability_json: [{ lane: 5, weight: 0.18 }, { lane: 4, weight: 0.17 }, { lane: 2, weight: 0.15 }]
  },
  attackScenarioAnalysis: lane4Pressure,
  headScenarioBalanceAnalysis: {
    ...insideHeadScenario,
    launch_state_labels_json: [
      { lane: 1, label: "hollow" },
      { lane: 4, label: "strong_out" },
      { lane: 5, label: "out" }
    ]
  },
  outsideHeadPromotionGate: {
    inner_collapse_score: 67,
    by_lane: {
      "4": { matched_evidence_categories: ["entry_shape_advantage", "clear_exhibition_st_advantage", "lap_exhibition_advantage"] },
      "5": { matched_evidence_categories: ["outer_mix_ready", "lap_exhibition_advantage", "learning_correction_match"] }
    }
  }
});
assert.ok(
  hollowUpsetRisk >= 62,
  "boat1_hollow plus outer pressure should raise upset risk into the alert zone"
);

const tamagawaVenueCalibration = getVenueLaunchMicroCalibration({ race: { venueId: 5 } });
assert.ok(
  tamagawaVenueCalibration.values.boat1_escape_bias > 0,
  "inside-favorable venues should be able to slightly boost boat1 escape conversion"
);

const neutralVenueEvents = buildIntermediateDevelopmentEvents({
  launchStateScores: stableLaunchScores,
  rows: stableLaunchStateRows,
  race: { venueId: 999 },
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  venueCalibration: getVenueLaunchMicroCalibration({ race: { venueId: 999 } })
});
const tamagawaVenueEvents = buildIntermediateDevelopmentEvents({
  launchStateScores: stableLaunchScores,
  rows: stableLaunchStateRows,
  race: { venueId: 5 },
  headScenarioBalanceAnalysis: insideHeadScenario,
  escapePatternAnalysis: insideEscape,
  venueCalibration: tamagawaVenueCalibration
});
assert.ok(
  tamagawaVenueEvents.inner_stable > neutralVenueEvents.inner_stable,
  "venue micro-calibration should only slightly raise inner_stable at inside-favorable venues"
);
assert.ok(
  tamagawaVenueEvents.inner_stable - neutralVenueEvents.inner_stable <= 4.1,
  "venue inner_stable adjustment should stay tightly capped"
);

const neutralScenarioProbabilities = computeRaceScenarioProbabilities({
  intermediateEvents: stableLaunchEvents,
  rows: stableLaunchStateRows,
  race: { venueId: 999 },
  attackScenarioAnalysis: noAttack,
  escapePatternAnalysis: insideEscape,
  outsideHeadPromotionContext: {
    inner_collapse_score: 22,
    by_lane: new Map()
  },
  headScenarioBalanceAnalysis: insideHeadScenario,
  venueCalibration: getVenueLaunchMicroCalibration({ race: { venueId: 999 } })
});
const venueScenarioProbabilities = computeRaceScenarioProbabilities({
  intermediateEvents: stableLaunchEvents,
  rows: stableLaunchStateRows,
  race: { venueId: 5 },
  attackScenarioAnalysis: noAttack,
  escapePatternAnalysis: insideEscape,
  outsideHeadPromotionContext: {
    inner_collapse_score: 22,
    by_lane: new Map()
  },
  headScenarioBalanceAnalysis: insideHeadScenario,
  venueCalibration: tamagawaVenueCalibration
});
const neutralScenarioProbMap = new Map(neutralScenarioProbabilities.map((row) => [row.scenario, row.probability]));
const venueScenarioProbMap = new Map(venueScenarioProbabilities.map((row) => [row.scenario, row.probability]));
assert.ok(
  Number(venueScenarioProbMap.get("boat1_escape") || 0) >= Number(neutralScenarioProbMap.get("boat1_escape") || 0),
  "venue calibration should keep boat1 escape stable or slightly stronger at inside-favorable venues"
);
assert.ok(
  Math.abs(Number(venueScenarioProbMap.get("boat1_escape") || 0) - Number(neutralScenarioProbMap.get("boat1_escape") || 0)) <= 0.05,
  "venue scenario calibration should stay small and conservative"
);

const edgyOutsideProbabilities = computeRaceScenarioProbabilities({
  intermediateEvents: {
    ...intermediateEvents,
    outer_mix_ready: 46
  },
  rows: launchStateRows,
  race: { venueId: 10 },
  attackScenarioAnalysis: lane4Pressure,
  escapePatternAnalysis: { ...insideEscape, formation_pattern: "outside_lead" },
  outsideHeadPromotionContext: {
    inner_collapse_score: 40,
    by_lane: new Map()
  },
  headScenarioBalanceAnalysis: insideHeadScenario,
  venueCalibration: getVenueLaunchMicroCalibration({ race: { venueId: 10 } })
});
const edgyOutsideNeutralProbabilities = computeRaceScenarioProbabilities({
  intermediateEvents: {
    ...intermediateEvents,
    outer_mix_ready: 46
  },
  rows: launchStateRows,
  race: { venueId: 999 },
  attackScenarioAnalysis: lane4Pressure,
  escapePatternAnalysis: { ...insideEscape, formation_pattern: "outside_lead" },
  outsideHeadPromotionContext: {
    inner_collapse_score: 40,
    by_lane: new Map()
  },
  headScenarioBalanceAnalysis: insideHeadScenario,
  venueCalibration: getVenueLaunchMicroCalibration({ race: { venueId: 999 } })
});
const edgyOutsideMap = new Map(edgyOutsideProbabilities.map((row) => [row.scenario, row.probability]));
const edgyOutsideNeutralMap = new Map(edgyOutsideNeutralProbabilities.map((row) => [row.scenario, row.probability]));
assert.ok(
  Math.abs(Number(edgyOutsideMap.get("chaos_outer_mix") || 0) - Number(edgyOutsideNeutralMap.get("chaos_outer_mix") || 0)) <= 0.04,
  "venue bias should only change outside-mix interpretation slightly"
);

const neutralUpsetRisk = computeUpsetRiskScore({
  confidenceScores: {
    head_fixed_confidence_pct: 56,
    recommended_bet_confidence_pct: 52
  },
  participationDecision: {
    decision: "watch",
    participation_score_components: {
      race_stability_score: 40,
      prediction_readability_score: 43,
      partner_clarity_score: 35,
      quality_gate_applied: 1
    }
  },
  roleProbabilityLayers: {
    boat1_escape_probability: 0.38,
    first_place_probability_json: [{ lane: 1, weight: 0.35 }, { lane: 4, weight: 0.22 }, { lane: 5, weight: 0.15 }],
    second_place_probability_json: [{ lane: 4, weight: 0.21 }, { lane: 5, weight: 0.19 }, { lane: 3, weight: 0.16 }],
    third_place_probability_json: [{ lane: 5, weight: 0.18 }, { lane: 4, weight: 0.17 }, { lane: 2, weight: 0.15 }]
  },
  attackScenarioAnalysis: lane4Pressure,
  headScenarioBalanceAnalysis: {
    ...insideHeadScenario,
    launch_venue_calibration_json: getVenueLaunchMicroCalibration({ race: { venueId: 999 } }),
    launch_state_labels_json: [
      { lane: 1, label: "hollow" },
      { lane: 4, label: "strong_out" },
      { lane: 5, label: "out" }
    ]
  },
  outsideHeadPromotionGate: {
    inner_collapse_score: 67,
    by_lane: {
      "4": { matched_evidence_categories: ["entry_shape_advantage", "clear_exhibition_st_advantage", "lap_exhibition_advantage"] },
      "5": { matched_evidence_categories: ["outer_mix_ready", "lap_exhibition_advantage", "learning_correction_match"] }
    }
  }
});
assert.ok(
  Math.abs(hollowUpsetRisk - neutralUpsetRisk) <= 4.1,
  "venue upset calibration should remain tightly capped"
);

const lapStrongFeature = {
  exhibition_rank: 1,
  lap_time_delta_vs_front: 0.09,
  lap_attack_strength: 11,
  exhibition_time: 6.67,
  motor2_rate: 52,
  motor3_rate: 67,
  player_recent_3_months_strength: 5.8,
  player_current_season_strength: 5.4,
  player_strength_blended: 5.7,
  course_fit_score: 2.1,
  venue_lane_adjustment: 1.1
};
const supportOnlyFeature = {
  exhibition_rank: 3,
  lap_time_delta_vs_front: 0.03,
  lap_attack_strength: 4.8,
  exhibition_time: 6.75,
  motor2_rate: 58,
  motor3_rate: 70,
  player_recent_3_months_strength: 4.9,
  player_current_season_strength: 5.0,
  player_strength_blended: 4.95,
  course_fit_score: 1.7,
  venue_lane_adjustment: 0.8
};
assert.ok(
  computeLapExhibitionStrength(lapStrongFeature) > computeLapExhibitionStrength(supportOnlyFeature),
  "strong lap exhibition should be the highest-impact override input"
);
assert.ok(
  computeMotor2renStrength(supportOnlyFeature) > computeMotor2renStrength({
    ...supportOnlyFeature,
    motor2_rate: 41
  }),
  "motor 2-ren should materially strengthen top-2 override support"
);

const outsideOverride = computeFinishOverrideStrength({
  exhibition_rank: 1,
  lap_time_delta_vs_front: 0.1,
  lap_attack_strength: 12,
  exhibition_time: 6.66,
  motor2_rate: 60,
  motor3_rate: 74,
  player_recent_3_months_strength: 6.1,
  player_current_season_strength: 5.8,
  player_strength_blended: 6,
  course_fit_score: 2,
  venue_lane_adjustment: 1.5
});
const mediumOutsideOverride = computeFinishOverrideStrength({
  exhibition_rank: 2,
  lap_time_delta_vs_front: 0.05,
  lap_attack_strength: 7.5,
  exhibition_time: 6.72,
  motor2_rate: 48,
  motor3_rate: 63,
  player_recent_3_months_strength: 5.1,
  player_current_season_strength: 5,
  player_strength_blended: 5.05,
  course_fit_score: 1.3,
  venue_lane_adjustment: 0.9
});
const overrideApplied = applyFinishOverrideStrength(
  {
    first: [{ lane: 1, weight: 0.28 }, { lane: 5, weight: 0.18 }, { lane: 3, weight: 0.14 }],
    second: [{ lane: 5, weight: 0.17 }, { lane: 3, weight: 0.16 }, { lane: 2, weight: 0.14 }],
    third: [{ lane: 5, weight: 0.15 }, { lane: 4, weight: 0.14 }, { lane: 2, weight: 0.13 }]
  },
  new Map([
    [1, computeFinishOverrideStrength({ motor2_rate: 44, motor3_rate: 58, player_strength_blended: 4.8, exhibition_time: 6.74 })],
    [5, mediumOutsideOverride],
    [3, computeFinishOverrideStrength({ ...supportOnlyFeature, motor2_rate: 49 })],
    [2, computeFinishOverrideStrength({ ...supportOnlyFeature, motor2_rate: 54 })],
    [4, computeFinishOverrideStrength({ ...supportOnlyFeature, motor2_rate: 52 })]
  ]),
  {
    boat1_escape_probability: 0.64,
    boat1_lane_first_prior: 0.28
  }
);
const firstAfterOverride = new Map(overrideApplied.first.map((row) => [row.lane, row]));
assert.equal(
  firstAfterOverride.get(5)?.finish_override_detail?.boat1_prior_blocked_outside_head_promotion,
  1,
  "boat 1 prior should still block weak outside-head override even with strong lap/motor strength"
);

const alignedOutsideOverride = applyFinishOverrideStrength(
  {
    first: [{ lane: 1, weight: 0.23 }, { lane: 5, weight: 0.2 }, { lane: 4, weight: 0.16 }],
    second: [{ lane: 5, weight: 0.18 }, { lane: 4, weight: 0.17 }, { lane: 2, weight: 0.13 }],
    third: [{ lane: 4, weight: 0.16 }, { lane: 5, weight: 0.15 }, { lane: 2, weight: 0.12 }]
  },
  new Map([
    [1, computeFinishOverrideStrength({ motor2_rate: 43, motor3_rate: 56, player_strength_blended: 4.7, exhibition_time: 6.76 })],
    [5, outsideOverride],
    [4, computeFinishOverrideStrength({ ...lapStrongFeature, motor2_rate: 57, motor3_rate: 71 })],
    [2, computeFinishOverrideStrength({ ...supportOnlyFeature, motor2_rate: 51 })]
  ]),
  {
    boat1_escape_probability: 0.39,
    boat1_lane_first_prior: 0.23
  }
);
assert.ok(
  new Map(alignedOutsideOverride.first.map((row) => [row.lane, row])).get(5)?.weight >
    0.2,
  "aligned strong lap + motor2ren cases should still allow meaningful outside finish override"
);

const overrideFinishRows = [
  makeRow(1, { features: { exhibition_rank: 2, motor2_rate: 45, motor3_rate: 58, exhibition_time: 6.73, player_strength_blended: 5.2 } }),
  makeRow(3, { features: { ...lapStrongFeature, motor2_rate: 56, motor3_rate: 72 } }),
  makeRow(4, { features: { ...supportOnlyFeature, motor2_rate: 53, motor3_rate: 68 } })
];
const overrideFinish = computeFinishProbsByScenario({
  scenarioProbabilities: [{ scenario: "boat3_makuri", probability: 0.21 }],
  firstPlaceProbability: [{ lane: 1, weight: 0.28 }, { lane: 3, weight: 0.16 }, { lane: 4, weight: 0.12 }],
  secondPlaceProbability: [{ lane: 3, weight: 0.19 }, { lane: 4, weight: 0.16 }, { lane: 2, weight: 0.14 }],
  thirdPlaceProbability: [{ lane: 4, weight: 0.15 }, { lane: 2, weight: 0.14 }, { lane: 3, weight: 0.13 }],
  boat1EscapeProbability: 0.51,
  rows: overrideFinishRows
});
const overrideStrengthMap = overrideFinish[0]?.finish_override_strength_json || {};
assert.ok(
  Number(overrideStrengthMap["3"]?.lap_exhibition_contribution || 0) >
    Number(overrideStrengthMap["4"]?.lap_exhibition_contribution || 0),
  "finish override logging should expose lap exhibition contribution by lane"
);
assert.ok(
  overrideFinish[0].second.some((row) => row.lane === 3 && Number(row?.finish_override_detail?.second_place_override_applied || 0) > 0),
  "strong lap + motor2ren should improve conditional top-2 finish support"
);

console.log("boat1-escape-opponent-model tests passed");
