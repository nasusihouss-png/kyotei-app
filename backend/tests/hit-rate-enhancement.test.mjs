import assert from "node:assert/strict";
import {
  applyHitRateEnhancementToProbabilities,
  buildEnhancedShapeBasedTrifectaTickets,
  buildEnhancedTrifectaShapeRecommendation,
  buildHitRateEnhancementContext
} from "../src/services/hit-rate-enhancement.js";

const ranking = [
  {
    racer: { lane: 1, class: "A1" },
    features: {
      avg_st: 0.14,
      avg_st_rank: 1,
      exhibition_st: 0.11,
      lane_fit_1st: 62,
      lane_fit_2ren: 71,
      lane_fit_3ren: 79,
      laneFirstRate: 62,
      lane2RenRate: 71,
      lane3RenRate: 79,
      local_win_rate: 6.3,
      class_score: 4,
      motor2_rate: 48,
      motor3_rate: 64,
      motor_total_score: 58,
      lap_time: 6.72,
      lap_exhibition_score: 7.2,
      exhibition_time: 6.74,
      f_hold_bias_applied: 0,
      f_hold_count: 0,
      f_hold_caution_penalty: 0
    }
  },
  {
    racer: { lane: 2, class: "A2" },
    features: {
      avg_st: 0.15,
      avg_st_rank: 2,
      exhibition_st: 0.12,
      lane_fit_1st: 34,
      lane_fit_2ren: 56,
      lane_fit_3ren: 69,
      laneFirstRate: 34,
      lane2RenRate: 56,
      lane3RenRate: 69,
      local_win_rate: 5.8,
      class_score: 3,
      motor2_rate: 46,
      motor3_rate: 60,
      motor_total_score: 49,
      lap_time: 6.76,
      lap_exhibition_score: 6.6,
      exhibition_time: 6.77,
      f_hold_bias_applied: 0,
      f_hold_count: 0,
      f_hold_caution_penalty: 0
    }
  },
  {
    racer: { lane: 3, class: "A2" },
    features: {
      avg_st: 0.16,
      avg_st_rank: 3,
      exhibition_st: 0.13,
      lane_fit_1st: 27,
      lane_fit_2ren: 48,
      lane_fit_3ren: 61,
      laneFirstRate: 27,
      lane2RenRate: 48,
      lane3RenRate: 61,
      local_win_rate: 5.4,
      class_score: 3,
      motor2_rate: 44,
      motor3_rate: 58,
      motor_total_score: 46,
      lap_time: 6.75,
      lap_exhibition_score: 6.8,
      exhibition_time: 6.76,
      f_hold_bias_applied: 0,
      f_hold_count: 0,
      f_hold_caution_penalty: 0
    }
  },
  {
    racer: { lane: 4, class: "B1" },
    features: {
      avg_st: 0.14,
      avg_st_rank: 1,
      exhibition_st: 0.11,
      lane_fit_1st: 48,
      lane_fit_2ren: 41,
      lane_fit_3ren: 50,
      laneFirstRate: 48,
      lane2RenRate: 41,
      lane3RenRate: 50,
      local_win_rate: 5.1,
      class_score: 2,
      motor2_rate: 43,
      motor3_rate: 55,
      motor_total_score: 44,
      lap_time: 6.73,
      lap_exhibition_score: 7.0,
      exhibition_time: 6.75,
      f_hold_bias_applied: 0,
      f_hold_count: 0,
      f_hold_caution_penalty: 0
    }
  }
];

const playerStartProfile = {
  by_lane: {
    "1": { player_start_profile: "nige", style_profile: { nige: 88, sashi: 28, makuri: 16, makuri_sashi: 22, nuki: 51 }, start_attack_score: 72 },
    "2": { player_start_profile: "sashi", style_profile: { nige: 18, sashi: 82, makuri: 61, makuri_sashi: 39, nuki: 25 }, start_attack_score: 77 },
    "3": { player_start_profile: "makuri_sashi", style_profile: { nige: 10, sashi: 46, makuri: 74, makuri_sashi: 79, nuki: 20 }, start_attack_score: 73 },
    "4": { player_start_profile: "makuri", style_profile: { nige: 8, sashi: 34, makuri: 84, makuri_sashi: 55, nuki: 19 }, start_attack_score: 81 }
  }
};

const roleProbabilityLayers = {
  first_place_probability_json: [{ lane: 1, weight: 0.43 }, { lane: 2, weight: 0.19 }, { lane: 3, weight: 0.17 }, { lane: 4, weight: 0.14 }],
  second_place_probability_json: [{ lane: 2, weight: 0.29 }, { lane: 3, weight: 0.24 }, { lane: 1, weight: 0.19 }, { lane: 4, weight: 0.16 }],
  third_place_probability_json: [{ lane: 3, weight: 0.23 }, { lane: 2, weight: 0.22 }, { lane: 4, weight: 0.18 }, { lane: 1, weight: 0.17 }],
  boat1_escape_probability: 0.57
};

const enhancement = buildHitRateEnhancementContext({
  ranking,
  race: { grade: "G2" },
  raceFlow: { tendency: "inside_favored" },
  playerStartProfile,
  roleProbabilityLayers,
  confidence: 66
});

assert.equal(enhancement.stage1_static.escape_score > 0.2, true);
assert.equal(enhancement.stage3_scenarios.selected_scenario_probabilities.some((row) => row.scenario === "boat4_cado_attack"), true);
assert.equal(Array.isArray(enhancement.dark_horse_alerts), true);

const enhanced = applyHitRateEnhancementToProbabilities({
  firstProbs: roleProbabilityLayers.first_place_probability_json,
  secondProbs: roleProbabilityLayers.second_place_probability_json,
  thirdProbs: roleProbabilityLayers.third_place_probability_json,
  enhancement
});

assert.equal(enhanced.first[0].lane, 1, "boat 1 should remain the strongest head in a normal inside race");
assert.equal(enhanced.second.some((row) => row.lane === 2), true);

const shape = buildEnhancedTrifectaShapeRecommendation({
  firstProbs: enhanced.first,
  secondProbs: enhanced.second,
  thirdProbs: enhanced.third,
  enhancement,
  confidence: 66
});

assert.equal(typeof shape.selected_shape, "string");
assert.equal(shape.expanded_tickets.every((combo) => combo.split("-").length === 3), true);

const tickets = buildEnhancedShapeBasedTrifectaTickets({
  shapeRecommendation: shape,
  firstProbs: enhanced.first,
  secondProbs: enhanced.second,
  thirdProbs: enhanced.third,
  enhancement,
  confidence: 66
});

assert.equal(tickets.length <= 10, true);
assert.equal(new Set(tickets.map((row) => row.combo)).size, tickets.length, "ticket list should not contain duplicate combos");
