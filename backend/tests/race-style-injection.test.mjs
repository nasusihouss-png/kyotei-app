import assert from "node:assert/strict";
import { applyLaneStylesToRacers } from "../src/routes/race.js";

const racers = [
  { lane: 1, name: "Racer-1" },
  { lane: 2, name: "Racer-2" }
];

const styled = applyLaneStylesToRacers(racers, {
  lane_styles: [],
  scenario_repro_scores: [
    { lane: 1, score: 57.2, style: "差し型" },
    { lane: 2, score: 52.4, style: "まくり型" }
  ]
});

assert.equal(styled[0].lane, 1);
assert.equal(styled[0].name, "Racer-1");
assert.equal(styled[0].style, "差し型");
assert.equal(styled[0].style_score, 57.2);
assert.ok(Array.isArray(styled[0].style_reasons));

assert.equal(styled[1].style, "まくり型");
assert.equal(styled[1].style_score, 52.4);
assert.ok(Array.isArray(styled[1].style_reasons));

const fallback = applyLaneStylesToRacers([{ lane: 3, name: "Racer-3" }], {
  lane_styles: [],
  scenario_repro_scores: []
});

assert.equal(fallback[0].style, "unknown");
assert.equal(fallback[0].style_score, null);
assert.deepEqual(fallback[0].style_reasons, []);

console.log("race-style-injection ok");
