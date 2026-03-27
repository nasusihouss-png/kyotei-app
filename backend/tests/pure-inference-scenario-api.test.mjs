import assert from "node:assert/strict";
import { buildPureInferencePredictionPayload } from "../src/routes/race.js";

function makeRacer(lane) {
  return {
    lane,
    name: `Racer-${lane}`,
    avgSt: 0.12 + lane * 0.01,
    nationwideWinRate: 7 - lane * 0.25,
    localWinRate: 6.7 - lane * 0.22,
    motor2Rate: 42 - lane * 2,
    boat2Rate: 39 - lane * 1.5,
    exhibitionSt: 0.08 + lane * 0.01,
    exhibitionTime: 6.7 + lane * 0.02,
    lapTime: 6.75 + lane * 0.02,
    fHoldCount: lane === 6 ? 1 : 0,
    featureSnapshot: {
      avg_st: 0.12 + lane * 0.01,
      nationwide_win_rate: 7 - lane * 0.25,
      local_win_rate: 6.7 - lane * 0.22,
      motor2_rate: 42 - lane * 2,
      boat2_rate: 39 - lane * 1.5,
      motor3_rate: 58 - lane * 2,
      motor_total_score: 11 - lane * 0.6,
      course_fit_score: 5 - lane * 0.35,
      entry_advantage_score: lane <= 4 ? 7 - lane * 0.6 : 3,
      course1_win_rate: lane === 1 ? 59 : null,
      course1_2rate: lane === 1 ? 73 : null,
      course2_2rate: lane === 2 ? 55 : null,
      course3_3rate: lane === 3 ? 58 : null,
      course4_3rate: lane === 4 ? 50 : null,
      coverage_report: {
        lapTime: { status: "ok", value: 6.75 + lane * 0.02, normalized: 6.75 + lane * 0.02, required: false },
        exhibition_st: { status: "ok", value: 0.08 + lane * 0.01, normalized: 0.08 + lane * 0.01, required: false },
        exhibition_time: { status: "ok", value: 6.7 + lane * 0.02, normalized: 6.7 + lane * 0.02, required: false },
        motor_3ren: { status: "ok", value: 58 - lane * 2, normalized: 58 - lane * 2, required: false },
        lane_1st_rate: { status: "ok", value: 65 - lane * 3, normalized: 65 - lane * 3, required: false },
        lane_2ren_rate: { status: "ok", value: 72 - lane * 3, normalized: 72 - lane * 3, required: false },
        lane_3ren_rate: { status: "ok", value: 80 - lane * 3, normalized: 80 - lane * 3, required: false },
        stability_rate: { status: "ok", value: 62 - lane, normalized: 62 - lane, required: false },
        breakout_rate: { status: "ok", value: 38 + lane, normalized: 38 + lane, required: false },
        sashi_rate: { status: "ok", value: 28 + lane, normalized: 28 + lane, required: false },
        makuri_rate: { status: "ok", value: 30 + lane, normalized: 30 + lane, required: false },
        makurisashi_rate: { status: "ok", value: 26 + lane, normalized: 26 + lane, required: false },
        zentsuke_tendency: { status: "ok", value: 22 + lane, normalized: 22 + lane, required: false }
      }
    }
  };
}

const data = {
  race: { date: "2026-03-24", venueId: 13, venueName: "Amagasaki", raceNo: 1 },
  racers: [1, 2, 3, 4, 5, 6].map(makeRacer),
  source: {
    local_snapshots: {
      index_snapshot_status: "READY",
      feature_snapshot: true
    },
    coverage_report: {
      summary: {
        total: 0,
        ok: 0,
        fallback: 0,
        broken_pipeline: 0,
        missing: 0,
        not_published: 0,
        required_broken_pipeline: 0,
        required_missing: 0,
        optional_issues: 0
      },
      fields: {}
    },
    coverage_report_summary: {
      total: 0,
      ok: 0,
      fallback: 0,
      broken_pipeline: 0,
      missing: 0,
      not_published: 0,
      required_broken_pipeline: 0,
      required_missing: 0,
      optional_issues: 0
    }
  }
};

const payload = buildPureInferencePredictionPayload(data);

assert.ok(payload?.pureTop6Prediction);
assert.equal(typeof payload.pureTop6Prediction.top6Scenario, "string");
assert.equal(typeof payload.pureTop6Prediction.top6ScenarioScore, "number");
assert.equal(typeof payload.pureTop6Prediction.scenario_repro_score, "number");
assert.ok(Array.isArray(payload.pureTop6Prediction.scenario_repro_scores));
assert.ok(Array.isArray(payload.pureTop6Prediction.first_place_candidate_rates));
assert.equal(typeof payload.pureTop6Prediction.winProbabilities, "object");
assert.equal(typeof payload.pureTop6Prediction.optionalFormation16, "object");
assert.equal(typeof payload.prediction.top6Scenario, "string");
assert.equal(typeof payload.prediction.top6ScenarioScore, "number");
assert.equal(typeof payload.prediction.scenario_repro_score, "number");
assert.equal(payload.prediction.source_summary?.freshness_status, "stale");
assert.equal(payload.prediction.source_summary?.refreshed_now, false);
assert.equal(typeof payload.prediction.source_summary?.field_coverage_report, "object");
assert.ok(Array.isArray(payload.broken_fields_required));
assert.ok(Array.isArray(payload.broken_fields_optional));
assert.equal(payload.freshness_status, "stale");
assert.equal(payload.refreshed_now, false);

console.log("pure-inference-scenario-api ok");
