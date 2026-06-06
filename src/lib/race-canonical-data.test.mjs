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

function tendencyRows() {
  return [
    {
      boat: 1,
      course: 1,
      racerId: "101",
      last6mRaceCount: 30,
      allCourseLast6mRaceCount: 44,
      courseSpecificLast6mRaceCount: 30,
      sampleStatus: "ok",
      matchMethod: "racerId",
      courseSource: "actual",
      allCourseWinRate: 0.27,
      allCourseSashiRate: 0.08,
      allCourseAvgST: 0.16,
      escapeRate: 0.62,
      beatenBySashiRate: 0.18,
      beatenByMakuriRate: 0.1,
      beatenByMakuriSashiRate: 0.07,
      avgST: null
    },
    {
      boat: 2,
      course: 2,
      racerId: "102",
      last6mRaceCount: 28,
      sashiRate: 0.22,
      makuriRate: 0.08,
      makuriSashiRate: 0.12
    },
    {
      boat: 5,
      course: 6,
      coursePredicted: false,
      racerId: "105",
      last6mRaceCount: 12,
      sashiRate: 0.05
    }
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
    exST: null,
    exTime: null,
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

{
  const canonical = buildCanonicalRaceData({
    program: program({
      1: { racer_start_timing: ".15", racer_exhibition_time: 0 },
      2: { exhibitionST: "F.03", exhibitionTime: "6.76" }
    }),
    conditions: {
      windDirection: "追い風",
      windSpeed: 5,
      waveHeight: 3,
      weather: "晴"
    }
  });
  const boat1 = canonical.entries.find((row) => row.boat === 1);
  const boat2 = canonical.entries.find((row) => row.boat === 2);
  assert.equal(boat1.exST, 0.15, "exST parsing should accept .15");
  assert.equal(boat1.exTime, null, "missing/not-run exhibition time 0 must stay null");
  assert.equal(boat2.exST, -0.03, "F.03 should normalize to a signed start timing");
  assert.equal(boat2.exTime, 6.76, "exTime parsing should accept numeric strings");
  assert.equal(canonical.conditions.windSpeed, 5);
  assert.equal(canonical.debug.predictionInputProgram.race_wind, 5);
  assert.equal(canonical.debug.canonicalPreview[0].exTime, null);
}

{
  const canonical = buildCanonicalRaceData({
    program: program({
      1: { escapeRate: null, beatenBySashiRate: null },
      2: { sashiRate: 0.31 },
      3: { playerTendency: { makuriRate: 22 } }
    }),
    originalExhibitionRows: originalRows(),
    tendencyRows: tendencyRows()
  });
  const boat1 = canonical.entries.find((row) => row.boat === 1);
  const boat2 = canonical.entries.find((row) => row.boat === 2);
  const boat3 = canonical.entries.find((row) => row.boat === 3);
  const boat5 = canonical.entries.find((row) => row.boat === 5);
  const predictionBoat1 = canonical.debug.predictionInputProgram.boats.find((row) => Number(row.boat) === 1);
  assert.equal(boat1.escapeRate, 0.62);
  assert.equal(boat1.beatenBySashiRate, 0.18);
  assert.equal(boat1.avgST, null);
  assert.equal(boat1.allCourseLast6mRaceCount, 44);
  assert.equal(boat1.courseSpecificLast6mRaceCount, 30);
  assert.equal(boat1.sampleStatus, "ok");
  assert.equal(boat1.allCourseWinRate, 0.27);
  assert.equal(boat1.allCourseSashiRate, 0.08);
  assert.equal(boat1.allCourseAvgST, 0.16);
  assert.equal(boat2.sashiRate, 0.31, "non-null canonical values must not be overwritten");
  assert.equal(boat3.makuriRate, 0.22, "legacy percent tendency values should normalize to 0-1");
  assert.equal(boat5.course, 6, "confirmed tendency course should replace predicted lane order");
  assert.equal(boat5.coursePredicted, false);
  assert.equal(predictionBoat1.playerTendency.beatenByMakuriRate, 0.1);
  assert.equal(predictionBoat1.playerTendency.sampleStatus, "ok");
  assert.equal(predictionBoat1.playerTendency.allCourseWinRate, 0.27);
  assert.equal(canonical.debug.canonicalTendencyCounts.escapeRate, 1);
  assert.equal(canonical.debug.tendencyPreview[0].last6mRaceCount, 30);
}

{
  const canonical = buildCanonicalRaceData({
    program: program(),
    tendencyRows: [
      {
        boat: 6,
        course: 2,
        coursePredicted: false,
        racerId: "101",
        sashiRate: 0.44
      }
    ]
  });
  const boat1 = canonical.entries.find((row) => row.boat === 1);
  const boat6 = canonical.entries.find((row) => row.boat === 6);
  assert.equal(boat1.sashiRate, 0.44, "racerId match should merge even when boat number differs");
  assert.equal(boat6.sashiRate, null, "boat fallback must not merge a different racerId");
}

console.log("race-canonical-data ok");
