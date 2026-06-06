import assert from "node:assert/strict";

import {
  buildDecisionConditionedVenueBiasFromRaces,
  decisionSampleStatus,
  getDecisionConditionedStats,
  getHeadDecisionComboStats,
  normalizeDecisionKey
} from "./venue-bias-engine.js";

assert.equal(normalizeDecisionKey("まくり差し"), "makuriSashi");
assert.equal(normalizeDecisionKey("makuri"), "makuri");
assert.equal(normalizeDecisionKey("差し"), "sashi");
assert.equal(normalizeDecisionKey("逃げ"), "escape");
assert.equal(decisionSampleStatus(50), "ok");
assert.equal(decisionSampleStatus(20), "small_sample");
assert.equal(decisionSampleStatus(5), "very_small_sample");
assert.equal(decisionSampleStatus(4), "insufficient");

const venueBias = buildDecisionConditionedVenueBiasFromRaces([
  {
    venueId: 24,
    result: { winningDecision: "まくり", winnerBoat: 3 },
    entries: [
      { boat: 3, finishPosition: 1 },
      { boat: 1, finishPosition: 2 },
      { boat: 4, finishPosition: 3 }
    ]
  },
  {
    venueId: 24,
    result: { winningDecision: "まくり", winnerBoat: 3 },
    entries: [
      { boat: 3, finishPosition: 1 },
      { boat: 4, finishPosition: 2 },
      { boat: 5, finishPosition: 3 }
    ]
  },
  {
    venueId: 24,
    result: { winningDecision: "まくり差し", winnerBoat: 4 },
    entries: [
      { boat: 4, finishPosition: 1 },
      { boat: 1, finishPosition: 2 },
      { boat: 5, finishPosition: 3 }
    ]
  },
  {
    venueId: 3,
    result: { winningDecision: "差し", winnerBoat: 2 },
    entries: [
      { boat: 2, finishPosition: 1 },
      { boat: 1, finishPosition: 2 },
      { boat: 3, finishPosition: 3 }
    ]
  }
], 24);

assert.equal(venueBias.decisionConditionedStats.makuri.sampleCount, 2);
assert.equal(venueBias.decisionConditionedStats.makuri.boat1SecondRate, 0.5);
assert.equal(venueBias.decisionConditionedStats.makuri.insideResidualRate, 0.5);
assert.equal(venueBias.decisionConditionedStats.makuri.outsideLinkedRate, 1);
assert.equal(venueBias.headDecisionComboStats["4"].makuriSashi.exactaRates["4-1"], 1);

const normalizedDecisionStats = getDecisionConditionedStats(venueBias, 24);
const normalizedHeadStats = getHeadDecisionComboStats(venueBias, 24);
assert.equal(normalizedDecisionStats.makuri.sampleCount, 2);
assert.equal(normalizedHeadStats["4"].makuriSashi.secondRates["1"], 1);

console.log("venue-bias-engine ok");
