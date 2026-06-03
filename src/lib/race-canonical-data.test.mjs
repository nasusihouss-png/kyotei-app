import assert from "node:assert/strict";
import {
  buildCanonicalRaceData,
  buildTableDisplayPreview
} from "./race-data-merge.js";
import { buildRacePrediction } from "./kyotei-openapi-engine.js";
import { getPlayerBoatTableRows } from "../components/player-boat-table-model.js";

function program(overridesByBoat = {}) {
  return {
    race_stadium_number: 13,
    race_number: 1,
    boats: [1, 2, 3, 4, 5, 6].map((boat) => ({
      racer_boat_number: boat,
      racer_name: `Racer ${boat}`,
      racer_number: `10${boat}`,
      racer_class_number: "A1",
      racer_national_top_1_percent: 6,
      racer_local_top_1_percent: 5,
      racer_assigned_motor_top_2_percent: 30 + boat,
      racer_average_start_timing: 0.15,
      lapTime: null,
      straightTime: null,
      turnTime: null,
      ...(overridesByBoat[boat] || {})
    }))
  };
}

function originalRows() {
  return [
    { boat: 1, lapTime: 18.22, straightTime: 7.43, turnTime: 4.45 },
    { boat: 2, lapTime: 18.38, straightTime: 7.55, turnTime: 4.38 },
    { boat: 3, lapTime: 18.59, straightTime: 7.61, turnTime: 4.48 },
    { boat: 4, lapTime: 18.43, straightTime: 7.53, turnTime: 4.8 },
    { boat: 5, lapTime: 18.57, straightTime: 7.61, turnTime: 4.73 },
    { boat: 6, lapTime: 18.32, straightTime: 7.48, turnTime: 4.58 }
  ];
}

{
  const canonical = buildCanonicalRaceData({
    date: "2026-06-03",
    venueId: 13,
    raceNo: 1,
    program: program(),
    originalExhibition: { ok: true, rows: originalRows() }
  });
  assert.equal(canonical.entries.length, 6);
  assert.equal(canonical.entries[0].lapTime, 18.22);
  assert.equal(canonical.entries[0].straightTime, 7.43);
  assert.equal(canonical.entries[0].turnTime, 4.45);
}

{
  const canonical = buildCanonicalRaceData({
    baseProgram: program(),
    originalExhibitionRows: originalRows(),
    debug: { source: "frontend_state" }
  });
  assert.equal(canonical.debug.originalExhibitionRows.length, 6);
  assert.equal(canonical.entries[0].lapTime, 18.22);
  assert.equal(canonical.debug.canonicalPreview[0].straightTime, 7.43);
}

{
  const canonical = buildCanonicalRaceData({
    program: program({
      1: { lapTime: null, straightTime: null, turnTime: null },
      2: { lapTime: 18.1, straightTime: 7.2, turnTime: 4.2 }
    }),
    originalExhibition: { ok: true, rows: originalRows() }
  });
  const boat1 = canonical.entries.find((row) => row.boat === 1);
  const boat2 = canonical.entries.find((row) => row.boat === 2);
  assert.equal(boat1.lapTime, 18.22);
  assert.equal(boat1.straightTime, 7.43);
  assert.equal(boat1.turnTime, 4.45);
  assert.equal(boat2.lapTime, 18.1);
  assert.equal(boat2.straightTime, 7.2);
  assert.equal(boat2.turnTime, 4.2);
}

{
  const canonical = buildCanonicalRaceData({
    program: program(),
    originalExhibition: { ok: true, rows: originalRows() }
  });
  const tableEntries = getPlayerBoatTableRows(canonical.entries);
  const tablePreview = buildTableDisplayPreview(tableEntries);
  assert.deepEqual(tablePreview[0], {
    boat: 1,
    lapTime: 18.22,
    straightTime: 7.43,
    turnTime: 4.45
  });
}

{
  const canonical = buildCanonicalRaceData({
    program: program(),
    originalExhibition: { ok: true, rows: originalRows() }
  });
  const inputBoat1 = canonical.debug.predictionInputProgram.boats.find((row) => Number(row.boat) === 1);
  assert.equal(inputBoat1.lapTime, canonical.entries[0].lapTime);
  assert.equal(inputBoat1.racer_lap_time, canonical.entries[0].lapTime);
  const prediction = buildRacePrediction(canonical.debug.predictionInputProgram, null);
  const scoredBoat1 = prediction.scoredBoats.find((row) => row.boat === 1);
  assert.equal(scoredBoat1.lapTime, canonical.entries[0].lapTime);
  assert.equal(scoredBoat1.straightTime, canonical.entries[0].straightTime);
  assert.equal(scoredBoat1.turnTime, canonical.entries[0].turnTime);
}

console.log("race-canonical-data ok");
