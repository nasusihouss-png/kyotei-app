import assert from "node:assert/strict";
import { __testHooks } from "../src/routes/race.js";

const {
  computeEvaluationMetrics,
  buildEvaluationSummary,
  buildConfidenceCalibration,
  buildOutsideHeadMonitoring,
  buildBoat1EscapeDiagnostics
} = __testHooks;

const rows = [
  {
    race_id: "r1",
    prediction_timestamp: "2026-03-10T09:00:00.000Z",
    verified_at: "2026-03-10T10:00:00.000Z",
    venue_name: "Tamagawa",
    formation_pattern: "inside_lead",
    scenario_type: "boat1_escape",
    attack_scenario_type: "boat3_makuri",
    hit_flag: 1,
    head_hit: 1,
    exacta_hit: 1,
    second_place_miss: 0,
    third_place_miss: 0,
    second_third_swap: 0,
    third_place_noise: 0,
    partner_selection_miss: 0,
    structure_near_but_order_miss: 0,
    participation_decision: "Participate",
    boat1_escape_probability: 0.84,
    head_confidence: 88,
    bet_confidence: 82,
    miss_head: 0,
    miss_second: 0,
    miss_third: 0,
    attack_read_correct_but_finish_wrong: 0,
    boat1_escape_correct_but_opponent_wrong: 0,
    miss_pattern_tags: [],
    first_place_probability_json: [{ lane: 1, weight: 0.72 }, { lane: 2, weight: 0.14 }],
    final_recommended_bets_snapshot: [{ combo: "1-2-3" }, { combo: "1-3-2" }],
    role_based_main_trifecta_tickets_snapshot: [{ combo: "1-2-3" }, { combo: "1-3-2" }],
    confirmed_result: "1-2-3"
  },
  {
    race_id: "r2",
    prediction_timestamp: "2026-03-11T09:00:00.000Z",
    verified_at: "2026-03-11T10:00:00.000Z",
    venue_name: "Tamagawa",
    formation_pattern: "outside_lead",
    scenario_type: "outside_attack",
    attack_scenario_type: "boat5_makuri",
    hit_flag: 0,
    head_hit: 0,
    exacta_hit: 0,
    second_place_miss: 1,
    third_place_miss: 1,
    second_third_swap: 0,
    third_place_noise: 1,
    partner_selection_miss: 1,
    structure_near_but_order_miss: 0,
    participation_decision: "Watch",
    boat1_escape_probability: 0.31,
    head_confidence: 54,
    bet_confidence: 51,
    miss_head: 1,
    miss_second: 1,
    miss_third: 1,
    attack_read_correct_but_finish_wrong: 0,
    boat1_escape_correct_but_opponent_wrong: 0,
    miss_pattern_tags: ["outer_head_overpromotion"],
    first_place_probability_json: [{ lane: 5, weight: 0.34 }, { lane: 1, weight: 0.2 }],
    final_recommended_bets_snapshot: [{ combo: "5-1-2" }, { combo: "5-3-1" }],
    role_based_main_trifecta_tickets_snapshot: [{ combo: "5-1-2" }, { combo: "5-3-1" }],
    confirmed_result: "1-3-5"
  },
  {
    race_id: "r3",
    prediction_timestamp: "2026-03-12T09:00:00.000Z",
    verified_at: "2026-03-12T10:00:00.000Z",
    venue_name: "Heiwajima",
    formation_pattern: "inside_lead",
    scenario_type: "boat1_escape",
    attack_scenario_type: "boat3_makuri",
    hit_flag: 0,
    head_hit: 1,
    exacta_hit: 0,
    second_place_miss: 1,
    third_place_miss: 0,
    second_third_swap: 0,
    third_place_noise: 0,
    partner_selection_miss: 1,
    structure_near_but_order_miss: 1,
    participation_decision: "Participate",
    boat1_escape_probability: 0.78,
    head_confidence: 77,
    bet_confidence: 69,
    miss_head: 0,
    miss_second: 1,
    miss_third: 0,
    attack_read_correct_but_finish_wrong: 1,
    boat1_escape_correct_but_opponent_wrong: 1,
    miss_pattern_tags: ["structure_near_miss"],
    first_place_probability_json: [{ lane: 1, weight: 0.65 }, { lane: 3, weight: 0.19 }],
    final_recommended_bets_snapshot: [{ combo: "1-2-4" }, { combo: "1-3-4" }],
    role_based_main_trifecta_tickets_snapshot: [{ combo: "1-2-4" }, { combo: "1-3-4" }],
    confirmed_result: "1-3-4"
  }
];

const overall = computeEvaluationMetrics(rows);
assert.equal(overall.verified_race_count, 3);
assert.equal(overall.trifecta_hit_count, 1);
assert.equal(overall.head_hit_count, 2);
assert.equal(overall.boat1_escape_prediction_count, 2);
assert.equal(overall.boat1_escape_opponent_hit_count, 1);
assert.equal(overall.participated_races, 2);

const confidenceBins = buildConfidenceCalibration(rows, "bet_confidence", "hit_flag");
assert.ok(confidenceBins.some((row) => row.bucket === "0.8-0.9" && row.race_count === 1));
assert.ok(confidenceBins.some((row) => row.bucket === "0.6-0.7" && row.race_count === 1));

const outsideMonitoring = buildOutsideHeadMonitoring(rows);
assert.equal(outsideMonitoring.boat5_main_head_count, 1);
assert.equal(outsideMonitoring.outside_head_recommendation_count, 1);
assert.equal(outsideMonitoring.outside_lead_overpromotion_count, 1);

const boat1Diagnostics = buildBoat1EscapeDiagnostics(rows);
assert.equal(boat1Diagnostics.boat1_escape_prediction_count, 2);
assert.equal(boat1Diagnostics.attack_read_correct_but_finish_wrong_count, 1);
assert.ok(boat1Diagnostics.family_capture_rows.some((row) => row.family === "1-3-x" && row.capture_rate === 50));

const filteredSummary = buildEvaluationSummary(rows, {
  filters: {
    venue: "Tamagawa",
    date_from: null,
    date_to: null,
    recommendation_level: "all",
    formation_pattern: "all",
    only_participated: 0,
    only_recommended: 0,
    only_boat1_escape_predicted: 0,
    only_outside_head_cases: 0
  }
});

assert.equal(filteredSummary.filtered_race_count, 2);
assert.equal(filteredSummary.overall.verified_race_count, 2);
assert.ok(filteredSummary.segmented_tables.venue.some((row) => row.segment_key === "Tamagawa"));
assert.ok(filteredSummary.miss_categories.some((row) => row.category === "outside_head_overpromotion" && row.count === 1));
assert.ok(filteredSummary.confidence_calibration.bet_confidence_bins.length >= 5);

console.log("evaluation dashboard tests passed");
