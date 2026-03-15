import assert from "node:assert/strict";
import { __testHooks } from "../src/routes/race.js";

const {
  buildSeparatedCandidateDistributions,
  buildRoleProbabilityLayers,
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

console.log("boat1-escape-opponent-model tests passed");
