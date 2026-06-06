import assert from "node:assert/strict";

import { runPredictionBacktest } from "./prediction-backtester.js";

const races = [
  {
    date: "2026-05-01",
    venueId: 24,
    raceNo: 1,
    entries: [
      { boat: 1, course: 1, racerName: "A", finishPosition: 1, motor2Rate: 44, exST: 0.08, exTime: 6.7, lapTime: 36.8, straightTime: 7.1, turnTime: 5.2 },
      { boat: 2, course: 2, racerName: "B", finishPosition: 2, motor2Rate: 33, exST: 0.12, exTime: 6.78, lapTime: 37.1, straightTime: 7.3, turnTime: 5.4 },
      { boat: 3, course: 3, racerName: "C", finishPosition: 3, motor2Rate: 30, exST: 0.14, exTime: 6.82, lapTime: 37.2, straightTime: 7.34, turnTime: 5.5 }
    ],
    result: { winnerBoat: 1, winningDecision: "逃げ" }
  },
  {
    date: "2026-05-02",
    venueId: 24,
    raceNo: 2,
    entries: [
      { boat: 1, course: 1, racerName: "D", finishPosition: 3, motor2Rate: 18, exST: 0.2, exTime: 6.9, lapTime: 37.8, straightTime: 7.6, turnTime: 5.9 },
      { boat: 3, course: 3, racerName: "E", finishPosition: 2, motor2Rate: 50, exST: 0.05, exTime: 6.72, lapTime: 36.9, straightTime: 7.0, turnTime: 5.25 },
      { boat: 4, course: 4, racerName: "F", finishPosition: 1, motor2Rate: 56, exST: 0.07, exTime: 6.71, lapTime: 36.7, straightTime: 6.98, turnTime: 5.18 }
    ],
    result: { winnerBoat: 4, winningDecision: "まくり差し" }
  }
];

const result = runPredictionBacktest({ races, dateFrom: "2026-05-01", dateTo: "2026-05-31", venueId: 24 });
assert.equal(result.ok, true);
assert.equal(result.sampleRaceCount, 2);
assert.ok(Object.prototype.hasOwnProperty.call(result.hitRates, "headTop1"));
assert.ok(Array.isArray(result.calibration.coefficientSuggestions));
assert.ok(result.debug.raceRows.length > 0);

console.log("prediction-backtester ok");
