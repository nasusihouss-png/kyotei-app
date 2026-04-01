import assert from "node:assert/strict";
import { buildFormationSuggestion, buildTop6Prediction } from "../top6-prediction-engine.js";
import { getVenueScenarioContext } from "../src/services/venue-scenario-bias.js";

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
const tokuyamaResult = buildTop6Prediction({
  ranking,
  race: { venueId: 18 }
});
const ashiyaResult = buildTop6Prediction({
  ranking,
  race: { venueId: 21 }
});
const amagasakiResult = buildTop6Prediction({
  ranking,
  race: { venueId: 13 }
});
const tamagawaResult = buildTop6Prediction({
  ranking,
  race: { venueId: 5 }
});
const suminoeResult = buildTop6Prediction({
  ranking,
  race: { venueId: 12 }
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
assert.equal(typeof result.venueBiasProfile, "object");
assert.equal(typeof result.buyPolicy?.code, "string");
assert.ok(Array.isArray(result.venueAdjustmentReason));
assert.ok(Number.isFinite(Number(result.boat1_second_keep_score)));
assert.equal(typeof result.boat1_second_keep_reason, "string");
assert.equal(typeof result.second_given_head_probabilities, "object");
assert.equal(typeof result.exacta_shape_bias, "object");
assert.ok(Array.isArray(result.near_tie_second_candidates));
assert.equal(typeof result.close_combo_preserved, "boolean");
assert.ok(result.combo_gap_score === null || Number.isFinite(Number(result.combo_gap_score)));
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
assert.equal(
  result.formationReason,
  Array.isArray(result.optionalFormation16?.reasons) && result.optionalFormation16.reasons.length > 0
    ? result.optionalFormation16.reasons.join("; ")
    : result.optionalFormation16?.reason ?? null
);
assert.ok((result.optionalFormation16?.size || 0) <= 18);
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
assert.ok((amagasakiResult.winProbabilities?.[1] || 0) >= (tamagawaResult.winProbabilities?.[1] || 0));
assert.ok(((amagasakiResult.head_prob_5 || 0) + (amagasakiResult.head_prob_6 || 0)) <= ((tamagawaResult.head_prob_5 || 0) + (tamagawaResult.head_prob_6 || 0)));
assert.ok((amagasakiResult.secondProbabilities?.[2] || 0) >= (tamagawaResult.secondProbabilities?.[2] || 0));
assert.ok((strongInsideVenueResult.boat1_second_keep_score || 0) >= (looseInsideVenueResult.boat1_second_keep_score || 0));
assert.ok((amagasakiResult.second_given_head_probabilities?.[2] || 0) >= (tamagawaResult.second_given_head_probabilities?.[2] || 0));
assert.ok((tokuyamaResult.winProbabilities?.[3] || 0) >= (strongInsideVenueResult.winProbabilities?.[3] || 0));
assert.ok((suminoeResult.secondProbabilities?.[4] || 0) >= (strongInsideVenueResult.secondProbabilities?.[4] || 0));
assert.ok(
  ((tokuyamaResult.second_place_candidate_rates || []).find((row) => row.lane === 3)?.rate || 0) > 0 ||
  ((tokuyamaResult.second_place_candidate_rates || []).find((row) => row.lane === 4)?.rate || 0) > 0
);
assert.ok((ashiyaResult.optionalFormation16?.second_candidates?.length || 0) >= 4);
assert.ok((tamagawaResult.optionalFormation16?.third_candidates?.length || 0) >= 4);
assert.ok(
  ((suminoeResult.optionalFormation16?.second_candidates || []).includes(3)) ||
  ((suminoeResult.optionalFormation16?.second_candidates || []).includes(4))
);
assert.equal(result.optionalFormation16?.active, true);
assert.ok((result.optionalFormation16?.size || 0) >= 12);
assert.ok((result.optionalFormation16?.size || 0) <= 18);
assert.ok(String(result.formationReason || "").length > 0);
const inactiveFormation = buildFormationSuggestion(
  [
    { combo: "1-2-3", probability: 0.2 },
    { combo: "1-2-4", probability: 0.16 },
    { combo: "1-3-2", probability: 0.12 },
    { combo: "1-3-4", probability: 0.1 }
  ],
  {
    first: { 1: 0.68, 2: 0.12, 3: 0.08, 4: 0.05, 5: 0.04, 6: 0.03 },
    second: { 1: 0.05, 2: 0.38, 3: 0.24, 4: 0.17, 5: 0.09, 6: 0.07 },
    third: { 1: 0.04, 2: 0.2, 3: 0.29, 4: 0.24, 5: 0.13, 6: 0.1 }
  },
  0.18,
  0.56,
  { venue_name: "Omura", venue_outside_break_risk: 18, lane56_renyuu_intrusion_rate: 16 }
);
assert.equal(inactiveFormation.active, false);
assert.equal(inactiveFormation.reason, null);
assert.equal(inactiveFormation.size, 0);
assert.deepEqual(inactiveFormation.combos, []);

const lowCoverageFormation = buildFormationSuggestion(
  [
    { combo: "1-2-3", probability: 0.021 },
    { combo: "1-3-2", probability: 0.018 },
    { combo: "2-1-3", probability: 0.016 },
    { combo: "2-3-1", probability: 0.014 },
    { combo: "3-1-2", probability: 0.012 },
    { combo: "3-2-1", probability: 0.010 },
    { combo: "4-1-2", probability: 0.009 },
    { combo: "4-2-1", probability: 0.008 },
    { combo: "5-1-2", probability: 0.007 },
    { combo: "6-1-2", probability: 0.006 },
    { combo: "1-4-2", probability: 0.005 },
    { combo: "2-4-1", probability: 0.004 }
  ],
  {
    first: { 1: 0.33, 2: 0.2, 3: 0.17, 4: 0.12, 5: 0.1, 6: 0.08 },
    second: { 1: 0.18, 2: 0.24, 3: 0.22, 4: 0.16, 5: 0.11, 6: 0.09 },
    third: { 1: 0.13, 2: 0.21, 3: 0.22, 4: 0.17, 5: 0.15, 6: 0.12 }
  },
  1,
  0.0862,
  { venue_name: "Amagasaki", venue_outside_break_risk: 34, lane56_renyuu_intrusion_rate: 19 }
);
assert.equal(lowCoverageFormation.active, true);
assert.equal(lowCoverageFormation.trigger_flags?.extremely_low_top6_coverage, true);
assert.equal(lowCoverageFormation.trigger_flags?.low_top6_coverage_with_chaos, true);
assert.ok((lowCoverageFormation.first_candidates || []).length >= 1);
assert.ok((lowCoverageFormation.combos || []).length >= 10);

const ashiyaBoostedFormation = buildFormationSuggestion(
  [
    { combo: "1-2-3", probability: 0.08 },
    { combo: "1-3-2", probability: 0.07 },
    { combo: "2-1-3", probability: 0.06 },
    { combo: "3-1-2", probability: 0.05 },
    { combo: "1-2-4", probability: 0.04 },
    { combo: "4-1-2", probability: 0.03 },
    { combo: "1-3-4", probability: 0.029 },
    { combo: "1-4-3", probability: 0.028 },
    { combo: "2-3-1", probability: 0.027 },
    { combo: "3-2-1", probability: 0.026 },
    { combo: "2-4-1", probability: 0.025 },
    { combo: "4-2-1", probability: 0.024 },
    { combo: "1-5-3", probability: 0.023 },
    { combo: "1-6-3", probability: 0.022 },
    { combo: "3-4-1", probability: 0.021 },
    { combo: "4-3-1", probability: 0.02 },
    { combo: "2-5-1", probability: 0.019 },
    { combo: "2-6-1", probability: 0.018 },
    { combo: "3-5-1", probability: 0.017 },
    { combo: "4-6-1", probability: 0.016 }
  ],
  {
    first: { 1: 0.36, 2: 0.18, 3: 0.17, 4: 0.12, 5: 0.09, 6: 0.08 },
    second: { 1: 0.14, 2: 0.25, 3: 0.21, 4: 0.17, 5: 0.12, 6: 0.11 },
    third: { 1: 0.12, 2: 0.2, 3: 0.21, 4: 0.18, 5: 0.15, 6: 0.14 }
  },
  0.31,
  0.45,
  getVenueScenarioContext(21)
);
const omuraTightFormation = buildFormationSuggestion(
  [
    { combo: "1-2-3", probability: 0.08 },
    { combo: "1-3-2", probability: 0.07 },
    { combo: "2-1-3", probability: 0.06 },
    { combo: "3-1-2", probability: 0.05 },
    { combo: "1-2-4", probability: 0.04 },
    { combo: "4-1-2", probability: 0.03 },
    { combo: "1-3-4", probability: 0.029 },
    { combo: "1-4-3", probability: 0.028 },
    { combo: "2-3-1", probability: 0.027 },
    { combo: "3-2-1", probability: 0.026 },
    { combo: "2-4-1", probability: 0.025 },
    { combo: "4-2-1", probability: 0.024 },
    { combo: "1-5-3", probability: 0.023 },
    { combo: "1-6-3", probability: 0.022 },
    { combo: "3-4-1", probability: 0.021 },
    { combo: "4-3-1", probability: 0.02 },
    { combo: "2-5-1", probability: 0.019 },
    { combo: "2-6-1", probability: 0.018 },
    { combo: "3-5-1", probability: 0.017 },
    { combo: "4-6-1", probability: 0.016 }
  ],
  {
    first: { 1: 0.36, 2: 0.18, 3: 0.17, 4: 0.12, 5: 0.09, 6: 0.08 },
    second: { 1: 0.14, 2: 0.25, 3: 0.21, 4: 0.17, 5: 0.12, 6: 0.11 },
    third: { 1: 0.12, 2: 0.2, 3: 0.21, 4: 0.18, 5: 0.15, 6: 0.14 }
  },
  0.31,
  0.45,
  getVenueScenarioContext(24)
);
assert.equal(ashiyaBoostedFormation.active, true);
assert.ok((ashiyaBoostedFormation.size || 0) >= (omuraTightFormation.size || 0));
assert.ok((ashiyaBoostedFormation.third_candidates || []).length >= (omuraTightFormation.third_candidates || []).length);

const nearTieRanking = [1, 2, 3, 4, 5, 6].map((lane) =>
  makeRow(lane, {
    racer: {
      lane,
      avgSt: lane === 1 ? 0.13 : lane === 2 ? 0.14 : lane === 3 ? 0.139 : lane === 4 ? 0.145 : 0.17 + lane * 0.005,
      nationwideWinRate: lane === 1 ? 7.1 : lane === 2 ? 6.3 : lane === 3 ? 6.28 : lane === 4 ? 6.0 : 5.1,
      localWinRate: lane === 1 ? 6.9 : lane === 2 ? 6.15 : lane === 3 ? 6.12 : lane === 4 ? 5.95 : 4.9,
      motor2Rate: lane === 1 ? 44 : lane === 2 ? 39 : lane === 3 ? 40 : lane === 4 ? 38 : 30,
      boat2Rate: lane === 1 ? 41 : lane === 2 ? 37 : lane === 3 ? 37 : lane === 4 ? 36 : 29
    },
    features: {
      avg_st: lane === 1 ? 0.13 : lane === 2 ? 0.14 : lane === 3 ? 0.139 : lane === 4 ? 0.145 : 0.17 + lane * 0.005,
      nationwide_win_rate: lane === 1 ? 7.1 : lane === 2 ? 6.3 : lane === 3 ? 6.28 : lane === 4 ? 6.0 : 5.1,
      local_win_rate: lane === 1 ? 6.9 : lane === 2 ? 6.15 : lane === 3 ? 6.12 : lane === 4 ? 5.95 : 4.9,
      motor2_rate: lane === 1 ? 44 : lane === 2 ? 39 : lane === 3 ? 40 : lane === 4 ? 38 : 30,
      boat2_rate: lane === 1 ? 41 : lane === 2 ? 37 : lane === 3 ? 37 : lane === 4 ? 36 : 29,
      motor_total_score: lane === 1 ? 11.8 : lane === 2 ? 10.4 : lane === 3 ? 10.5 : lane === 4 ? 10.1 : 6.2,
      motor3_rate: lane === 1 ? 56 : lane === 2 ? 51 : lane === 3 ? 52 : lane === 4 ? 50 : 38,
      course_fit_score: lane === 1 ? 5.2 : lane === 2 ? 4.7 : lane === 3 ? 4.75 : lane === 4 ? 4.6 : 2.7,
      entry_advantage_score: lane === 1 ? 6.6 : lane === 2 ? 5.9 : lane === 3 ? 5.95 : lane === 4 ? 5.5 : 2.4,
      course1_win_rate: lane === 1 ? 60 : null,
      course1_2rate: lane === 1 ? 74 : null,
      course2_2rate: lane === 2 ? 57 : null,
      course3_3rate: lane === 3 ? 58 : null,
      course4_3rate: lane === 4 ? 54 : null,
      f_hold_count: 0,
      coverage_report: {
        lane_1st_rate: { status: "ok", value: lane === 1 ? 61 : lane === 2 ? 28 : lane === 3 ? 29 : lane === 4 ? 19 : 8 },
        lane_2ren_rate: { status: "ok", value: lane === 1 ? 70 : lane === 2 ? 55 : lane === 3 ? 54 : lane === 4 ? 46 : 20 },
        lane_3ren_rate: { status: "ok", value: lane === 1 ? 78 : lane === 2 ? 64 : lane === 3 ? 65 : lane === 4 ? 58 : 24 },
        motor_3ren: { status: "ok", value: lane === 1 ? 56 : lane === 2 ? 51 : lane === 3 ? 52 : lane === 4 ? 50 : 38 },
        sashi_rate: { status: "ok", value: lane === 2 ? 36 : lane === 3 ? 29 : 24 + lane },
        makuri_rate: { status: "ok", value: lane === 3 ? 38 : lane === 4 ? 31 : 22 + lane },
        makurisashi_rate: { status: "ok", value: lane === 3 ? 35 : lane === 4 ? 33 : 20 + lane },
        breakout_rate: { status: "ok", value: lane === 4 ? 34 : 20 + lane },
        stability_rate: { status: "ok", value: lane === 1 ? 67 : lane === 2 ? 61 : lane === 3 ? 60 : lane === 4 ? 57 : 46 },
        zentsuke_tendency: { status: "ok", value: lane === 4 ? 29 : 18 + lane },
        exhibition_st: { status: "ok", value: lane === 1 ? 0.11 : lane === 2 ? 0.12 : lane === 3 ? 0.119 : lane === 4 ? 0.124 : 0.16, normalized: lane === 1 ? 0.11 : lane === 2 ? 0.12 : lane === 3 ? 0.119 : lane === 4 ? 0.124 : 0.16 },
        exhibition_time: { status: "ok", value: lane === 1 ? 6.74 : lane === 2 ? 6.78 : lane === 3 ? 6.775 : lane === 4 ? 6.8 : 6.92, normalized: lane === 1 ? 6.74 : lane === 2 ? 6.78 : lane === 3 ? 6.775 : lane === 4 ? 6.8 : 6.92 },
        lapTime: { status: "ok", value: lane === 1 ? 6.76 : lane === 2 ? 6.8 : lane === 3 ? 6.795 : lane === 4 ? 6.81 : 6.96, normalized: lane === 1 ? 6.76 : lane === 2 ? 6.8 : lane === 3 ? 6.795 : lane === 4 ? 6.81 : 6.96 }
      }
    }
  })
);
const nearTieResult = buildTop6Prediction({
  ranking: nearTieRanking,
  race: { venueId: 5 }
});
assert.equal(nearTieResult.close_combo_preserved, true);
assert.ok((nearTieResult.near_tie_second_candidates || []).length >= 2);
assert.ok((nearTieResult.top6 || []).some((row) => row.combo === "1-2-4"));
assert.ok((nearTieResult.top6 || []).some((row) => row.combo === "1-3-4"));
assert.ok((nearTieResult.optionalFormation16?.combos || []).some((row) => row.combo === "1-2-4"));
assert.ok((nearTieResult.optionalFormation16?.combos || []).some((row) => row.combo === "1-3-4"));
assert.ok(String(nearTieResult.formationReason || "").includes("1-2-4 and 1-3-4 are close"));
assert.ok(String(nearTieResult.formationReason || "").includes("close second-place combo preserved into top6"));
assert.ok(String(nearTieResult.formationReason || "").includes("2nd-place candidates 2/3/4 are tightly clustered"));

console.log("top6-prediction-engine ok");
