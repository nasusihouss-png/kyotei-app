import assert from "node:assert/strict";
import { buildTop6Prediction } from "../top6-prediction-engine.js";

function makeRow(lane, overrides = {}) {
  return {
    racer: {
      lane,
      avgSt: 0.12 + lane * 0.01,
      nationwideWinRate: 7 - lane * 0.3,
      localWinRate: 6.8 - lane * 0.28,
      motor2Rate: 42 - lane * 2,
      boat2Rate: 39 - lane * 1.8,
      exhibitionSt: 0.09 + lane * 0.01,
      exhibitionTime: 6.7 + lane * 0.03,
      lapTime: 6.74 + lane * 0.03,
      fHoldCount: lane === 6 ? 1 : 0
    },
    features: {
      avg_st: 0.12 + lane * 0.01,
      nationwide_win_rate: 7 - lane * 0.3,
      local_win_rate: 6.8 - lane * 0.28,
      motor2_rate: 42 - lane * 2,
      boat2_rate: 39 - lane * 1.8,
      motor_total_score: 12 - lane * 0.7,
      motor3_rate: 56 - lane * 2,
      course_fit_score: 5 - lane * 0.4,
      entry_advantage_score: lane <= 4 ? 7 - lane * 0.7 : 3 - (lane - 5) * 0.3,
      course1_win_rate: lane === 1 ? 58 : null,
      course1_2rate: lane === 1 ? 72 : null,
      course2_2rate: lane === 2 ? 54 : null,
      course3_3rate: lane === 3 ? 57 : null,
      course4_3rate: lane === 4 ? 49 : null,
      f_hold_count: lane === 6 ? 1 : 0,
      coverage_report: {
        lane_1st_rate: { status: "ok", value: 64 - lane * 4 },
        lane_2ren_rate: { status: "ok", value: 72 - lane * 4 },
        lane_3ren_rate: { status: "ok", value: 79 - lane * 4 },
        motor_3ren: { status: "ok", value: 56 - lane * 2 },
        sashi_rate: { status: "ok", value: 28 + lane },
        makuri_rate: { status: "ok", value: 30 + lane },
        makurisashi_rate: { status: "ok", value: 26 + lane },
        breakout_rate: { status: "ok", value: 24 + lane },
        stability_rate: { status: "ok", value: 60 - lane },
        zentsuke_tendency: { status: "ok", value: 18 + lane },
        exhibition_st: { status: "ok", value: 0.09 + lane * 0.01, normalized: 0.09 + lane * 0.01 },
        exhibition_time: { status: "ok", value: 6.7 + lane * 0.03, normalized: 6.7 + lane * 0.03 },
        lapTime: { status: "ok", value: 6.74 + lane * 0.03, normalized: 6.74 + lane * 0.03 }
      }
    },
    ...overrides
  };
}

const ranking = [1, 2, 3, 4, 5, 6].map((lane) => makeRow(lane));
const result = buildTop6Prediction({
  ranking,
  race: { venueId: 5 }
});
const strongInsideVenueResult = buildTop6Prediction({
  ranking,
  race: { venueId: 24 }
});
const looseInsideVenueResult = buildTop6Prediction({
  ranking,
  race: { venueId: 3 }
});

const aggregatedFromAll120 = result.all_120_combinations.reduce(
  (acc, row) => {
    const [first, second, third] = String(row?.combo || "").split("-").map(Number);
    const probability = Number(row?.probability) || 0;
    acc.winProbabilities[first] += probability;
    acc.secondProbabilities[second] += probability;
    acc.thirdProbabilities[third] += probability;
    return acc;
  },
  {
    winProbabilities: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0 },
    secondProbabilities: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0 },
    thirdProbabilities: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0 }
  }
);

assert.ok(result);
assert.equal(result.top6.length, 6);
assert.equal(result.all_120_combinations.length, 120);
assert.ok(Number.isFinite(Number(result.top6_coverage)));
assert.ok(Number.isFinite(Number(result.confidence)));
assert.ok(Number.isFinite(Number(result.chaos_level)));
assert.equal(typeof result.top6Scenario, "string");
assert.ok(Number.isFinite(Number(result.top6ScenarioScore)));
assert.equal(result.scenario_repro_scores.length, 6);
assert.equal(result.lane_styles.length, 6);
assert.ok(Number.isFinite(Number(result.lane_styles[0]?.style_score)));
assert.ok(Array.isArray(result.lane_styles[0]?.style_reasons));
assert.equal(typeof result.venue_scenario_bias?.one_course_trust, "number");
assert.equal(typeof result.venue_scenario_bias?.two_course_sashi_remain_rate, "number");
assert.equal(typeof result.venue_scenario_bias?.three_course_attack_success_rate, "number");
assert.equal(typeof result.venue_scenario_bias?.four_course_develop_sashi_rate, "number");
assert.equal(typeof result.venue_scenario_bias?.lane56_renyuu_intrusion_rate, "number");
assert.equal(typeof result.venue_scenario_bias?.escape_fail_pattern?.total_risk, "number");
assert.equal(result.first_place_candidate_rates.length, 6);
assert.equal(result.second_place_candidate_rates.length, 6);
assert.equal(result.third_place_candidate_rates.length, 6);
assert.equal(Object.keys(result.winProbabilities || {}).length, 6);
assert.equal(Object.keys(result.secondProbabilities || {}).length, 6);
assert.equal(Object.keys(result.thirdProbabilities || {}).length, 6);
for (const lane of [1, 2, 3, 4, 5, 6]) {
  assert.equal(
    Number((result.winProbabilities?.[lane] || 0).toFixed(4)),
    Number((aggregatedFromAll120.winProbabilities?.[lane] || 0).toFixed(4))
  );
  assert.equal(
    Number((result.secondProbabilities?.[lane] || 0).toFixed(4)),
    Number((aggregatedFromAll120.secondProbabilities?.[lane] || 0).toFixed(4))
  );
  assert.equal(
    Number((result.thirdProbabilities?.[lane] || 0).toFixed(4)),
    Number((aggregatedFromAll120.thirdProbabilities?.[lane] || 0).toFixed(4))
  );
}
assert.equal(typeof result.optionalFormation16?.active, "boolean");
assert.equal(result.formationReason, result.optionalFormation16?.reason ?? null);
assert.equal(
  Number((result.head_prob_1 + result.head_prob_2 + result.head_prob_3 + result.head_prob_4 + result.head_prob_5 + result.head_prob_6).toFixed(4)),
  1
);
assert.equal(
  Number(Object.values(result.finish_probabilities.first).reduce((sum, value) => sum + Number(value || 0), 0).toFixed(4)),
  1
);
assert.equal(
  Number(Object.values(result.finish_probabilities.second).reduce((sum, value) => sum + Number(value || 0), 0).toFixed(4)),
  1
);
assert.equal(
  Number(Object.values(result.finish_probabilities.third).reduce((sum, value) => sum + Number(value || 0), 0).toFixed(4)),
  1
);
assert.equal(Number(result.top6.reduce((sum, row) => sum + Number(row.probability || 0), 0).toFixed(4)), Number(result.top6_coverage.toFixed(4)));
assert.ok(String(result.top6[0]?.tier || "").trim().length > 0);
assert.ok(String(result.lane_styles[0]?.style || "").trim().length > 0);
assert.ok(typeof result.wide_formation_suggestion?.active === "boolean");
assert.notEqual(strongInsideVenueResult.top6ScenarioScore, looseInsideVenueResult.top6ScenarioScore);
assert.ok((strongInsideVenueResult.winProbabilities?.[1] || 0) > (looseInsideVenueResult.winProbabilities?.[1] || 0));
assert.ok(
  (strongInsideVenueResult.scenario_repro_scores?.find((row) => row.lane === 1)?.score || 0) >
  (looseInsideVenueResult.scenario_repro_scores?.find((row) => row.lane === 1)?.score || 0)
);

console.log("top6-prediction-engine ok");
