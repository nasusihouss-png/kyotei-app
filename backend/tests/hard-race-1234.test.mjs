import assert from "node:assert/strict";
import { buildHardRace1234Response } from "../src/services/hard-race-1234-pure.js";

const baseData = {
  source: {
    racelistUrl: "https://www.boatrace.jp/owpc/pc/race/racelist?rno=1&jcd=05&hd=20260321",
    beforeinfoUrl: "https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno=1&jcd=05&hd=20260321",
    official_fetch_status: { racelist: "success", beforeinfo: "success" },
    kyotei_biyori: {
      ok: false,
      tried_urls: [],
      field_diagnostics: { populated_fields: [], failed_fields: ["sashi_rate", "makuri_rate"] }
    }
  },
  race: { date: "2026-03-21", venueId: 5, venueName: "Tamagawa", raceNo: 1 },
  racers: [
    { lane: 1, name: "A", class: "A1", nationwideWinRate: 7.2, localWinRate: 7.6, avgSt: 0.13, fHoldCount: 0, lHoldCount: 0, motor2Rate: 41, motor3Rate: 58, boat2Rate: 39, boat3Rate: 54, entryCourse: 1 },
    { lane: 2, name: "B", class: "A2", nationwideWinRate: 6.4, localWinRate: 6.7, avgSt: 0.15, fHoldCount: 0, lHoldCount: 0, motor2Rate: 38, motor3Rate: 52, boat2Rate: 36, boat3Rate: 49, entryCourse: 2 },
    { lane: 3, name: "C", class: "A2", nationwideWinRate: 6.1, localWinRate: 6.0, avgSt: 0.14, fHoldCount: 0, lHoldCount: 0, motor2Rate: 37, motor3Rate: 50, boat2Rate: 34, boat3Rate: 48, entryCourse: 3 },
    { lane: 4, name: "D", class: "B1", nationwideWinRate: 5.7, localWinRate: 5.6, avgSt: 0.16, fHoldCount: 0, lHoldCount: 0, motor2Rate: 35, motor3Rate: 47, boat2Rate: 33, boat3Rate: 44, entryCourse: 4 },
    { lane: 5, name: "E", class: "B1", nationwideWinRate: 5.2, localWinRate: 5.0, avgSt: 0.18, fHoldCount: 0, lHoldCount: 0, motor2Rate: 29, motor3Rate: 40, boat2Rate: 31, boat3Rate: 42, entryCourse: 5 },
    { lane: 6, name: "F", class: "B1", nationwideWinRate: 4.9, localWinRate: 4.6, avgSt: 0.19, fHoldCount: 1, lHoldCount: 0, motor2Rate: 27, motor3Rate: 37, boat2Rate: 28, boat3Rate: 39, entryCourse: 6 }
  ]
};

const result = await buildHardRace1234Response({
  data: baseData,
  date: "2026-03-21",
  venueId: 5,
  raceNo: 1
});

assert.ok(["READY", "FALLBACK", "BROKEN_PIPELINE"].includes(result.data_status));
assert.equal(result.confidence_status, result.data_status);
assert.ok(Number.isFinite(Number(result.hard_race_score)));
assert.ok(Number.isFinite(Number(result.boat1_escape_trust)));
assert.ok(Number.isFinite(Number(result.opponent_234_fit)));
assert.ok(Number.isFinite(Number(result.pair23_fit)));
assert.ok(Number.isFinite(Number(result.pair24_fit)));
assert.ok(Number.isFinite(Number(result.pair34_fit)));
assert.ok(Number.isFinite(Number(result.kill_escape_risk)));
assert.ok(Number.isFinite(Number(result.shape_shuffle_risk)));
assert.ok(Number.isFinite(Number(result.box_hit_score)));
assert.ok(Number.isFinite(Number(result.shape_focus_score)));
assert.ok(Number.isFinite(Number(result.fixed1234_total_probability)));
assert.ok(Number.isFinite(Number(result.top4_fixed1234_probability)));
assert.ok(Number.isFinite(Number(result.fixed1234_shape_concentration)));
assert.ok(Number.isFinite(Number(result.head_prob_1)));
assert.ok(Number.isFinite(Number(result.head_prob_2)));
assert.ok(Number.isFinite(Number(result.head_prob_3)));
assert.ok(Number.isFinite(Number(result.head_prob_4)));
assert.ok(Number.isFinite(Number(result.head_prob_5)));
assert.ok(Number.isFinite(Number(result.head_prob_6)));
assert.ok(Number.isFinite(Number(result.outside_head_risk)));
assert.ok(Number.isFinite(Number(result.outside_2nd_risk)));
assert.ok(Number.isFinite(Number(result.outside_3rd_risk)));
assert.ok(Number.isFinite(Number(result.outside_box_break_risk)));
assert.ok(Number.isFinite(Number(result.p_123)));
assert.ok(Number.isFinite(Number(result.p_124)));
assert.ok(Number.isFinite(Number(result.p_132)));
assert.ok(Number.isFinite(Number(result.p_134)));
assert.ok(Number.isFinite(Number(result.p_142)));
assert.ok(Number.isFinite(Number(result.p_143)));
assert.equal(Object.keys(result.fixed1234_matrix).length, 6);
assert.equal(
  result.fixed1234_shape_concentration,
  Number((result.top4_fixed1234_probability / result.fixed1234_total_probability).toFixed(4))
);
assert.ok(["BUY-4", "BUY-6", "BORDERLINE", "SKIP"].includes(result.decision));
assert.ok(typeof result.fallback_used?.used === "boolean");
assert.ok(Array.isArray(result.fallback_used?.fields));
assert.ok(Array.isArray(result.head_candidates));
assert.ok(Array.isArray(result.head_opponents));
assert.ok(typeof result.hard_mode?.active === "boolean");
assert.ok(typeof result.open_mode?.active === "boolean");
assert.ok(result.source_summary?.fallback);
assert.equal(result.source_summary?.mode, "pure_inference");
assert.equal(typeof result.features?.p1_escape, "number");
assert.equal(typeof result.features?.top4_share_within_fixed1234, "number");
assert.equal(typeof result.features?.conditional_probabilities?.p_1st_1, "number");
assert.equal(typeof result.operational_pick, "string");
assert.ok(Array.isArray(result.missing_fields));
assert.equal(typeof result.features?.evaluation_targets?.y_box6, "number");
assert.equal(typeof result.features?.evaluation_targets?.y_top4, "number");
assert.equal(typeof result.features?.evaluation_targets?.y_buy6, "number");
assert.equal(typeof result.features?.evaluation_targets?.y_buy4, "number");

console.log("hard-race-1234 ok");
