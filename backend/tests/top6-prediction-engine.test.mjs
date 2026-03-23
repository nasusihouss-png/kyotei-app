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
      fHoldCount: lane === 6 ? 1 : 0
    },
    features: {
      avg_st: 0.12 + lane * 0.01,
      nationwide_win_rate: 7 - lane * 0.3,
      local_win_rate: 6.8 - lane * 0.28,
      motor2_rate: 42 - lane * 2,
      boat2_rate: 39 - lane * 1.8,
      motor_total_score: 12 - lane * 0.7,
      course_fit_score: 5 - lane * 0.4,
      entry_advantage_score: lane <= 4 ? 7 - lane * 0.7 : 3 - (lane - 5) * 0.3,
      course1_win_rate: lane === 1 ? 58 : null,
      course1_2rate: lane === 1 ? 72 : null,
      course2_2rate: lane === 2 ? 54 : null,
      course3_3rate: lane === 3 ? 57 : null,
      course4_3rate: lane === 4 ? 49 : null,
      f_hold_count: lane === 6 ? 1 : 0
    },
    ...overrides
  };
}

const result = buildTop6Prediction({
  ranking: [1, 2, 3, 4, 5, 6].map((lane) => makeRow(lane)),
  race: { venueId: 5 }
});

assert.ok(result);
assert.equal(result.top6.length, 6);
assert.ok(Number.isFinite(Number(result.top6_coverage)));
assert.ok(Number.isFinite(Number(result.confidence)));
assert.ok(Number.isFinite(Number(result.chaos_level)));
assert.equal(
  Number((result.head_prob_1 + result.head_prob_2 + result.head_prob_3 + result.head_prob_4 + result.head_prob_5 + result.head_prob_6).toFixed(4)),
  1
);
assert.equal(Number(result.top6.reduce((sum, row) => sum + Number(row.probability || 0), 0).toFixed(4)), Number(result.top6_coverage.toFixed(4)));
assert.ok(["本命", "対抗", "抑え"].includes(result.top6[0].tier));

console.log("top6-prediction-engine ok");
