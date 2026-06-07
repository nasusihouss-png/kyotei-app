import assert from "node:assert/strict";

import { buildSlitFormation, buildSlitFormationDebug } from "./slit-formation-engine.js";

function entries(overrides = {}) {
  return [1, 2, 3, 4, 5, 6].map((boat) => ({
    boat,
    course: boat,
    exST: 0.13 + boat * 0.01,
    avgST: 0.15,
    straightTime: 7.4 + boat * 0.02,
    turnTime: 4.4 + boat * 0.03,
    lapTime: 18.3 + boat * 0.04,
    motorPercentileAtVenue: 0.5,
    sampleStatus: "ok",
    ...(overrides[boat] || {})
  }));
}

const poorAvgFastExhibition = buildSlitFormation(entries({
  3: {
    exST: 0.03,
    avgST: 0.28,
    currentSeasonAvgST: 0.29,
    lateStartRate: 0.24,
    straightTime: 6.68,
    motorPercentileAtVenue: 0.8
  }
}));
const reliableFastStart = buildSlitFormation(entries({
  3: {
    exST: 0.03,
    avgST: 0.09,
    currentSeasonAvgST: 0.08,
    lateStartRate: 0.02,
    straightTime: 6.68,
    motorPercentileAtVenue: 0.8
  }
}));
assert.equal(poorAvgFastExhibition.byBoat["3"].exhibitionGoodHistoryPoor, true);
assert.ok(
  reliableFastStart.byBoat["3"].attackStartScore > poorAvgFastExhibition.byBoat["3"].attackStartScore,
  "good exST + good avg/current-season ST should lift attackStartScore more than exhibition alone"
);
assert.ok(
  reliableFastStart.byBoat["3"].startReliabilityScore > poorAvgFastExhibition.byBoat["3"].startReliabilityScore,
  "long-term start reliability must be part of slit scoring"
);

const lateBoat2 = buildSlitFormation(entries({
  2: { avgST: 0.25, lateStartRate: 0.36, currentSeasonLateStartRate: 0.38 }
}));
const stableBoat2 = buildSlitFormation(entries({
  2: { avgST: 0.09, lateStartRate: 0.02, currentSeasonLateStartRate: 0.02, startStabilityRate: 0.86 }
}));
assert.ok(
  stableBoat2.byBoat["2"].wallFormationScore > lateBoat2.byBoat["2"].wallFormationScore,
  "high lateStartRate should reduce wallFormationScore"
);
assert.ok(lateBoat2.lateRiskBoats.includes(2), "boat2 late risk should be listed");

const centerPressure = buildSlitFormation(entries({
  3: { exST: 0.04, avgST: 0.09, straightTime: 6.66, makuriRate: 0.4 },
  4: { exST: 0.05, avgST: 0.1, straightTime: 6.7, makuriSashiRate: 0.36 }
}));
assert.ok(["boat3_pressure", "center_pressure"].includes(centerPressure.slitPattern));
assert.ok(centerPressure.attackTriggerCandidates.includes(3));

const missingAvg = buildSlitFormation(entries({
  1: { avgST: null, currentSeasonAvgST: null, lateStartRate: null },
  2: { avgST: null, currentSeasonAvgST: null, lateStartRate: null }
}));
assert.equal(missingAvg.rows.length, 6);
assert.ok(Number.isFinite(missingAvg.byBoat["1"].slitScore));
assert.ok(buildSlitFormationDebug(missingAvg).rows.length === 6);

console.log("slit-formation-engine ok");
