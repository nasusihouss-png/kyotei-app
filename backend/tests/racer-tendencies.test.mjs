import assert from "node:assert/strict";
import {
  calculateRacerCourseTendency,
  matchHistoryRowsForTarget
} from "../src/services/racer-tendencies.js";

{
  const row = calculateRacerCourseTendency(
    { boat: 1, course: 1, racerId: "1001", racerName: "Inside" },
    [
      { lane: 1, entry_course: 1, finish_1: 1, decision_type: "escape" },
      { lane: 1, entry_course: 1, finish_1: 2, decision_type: "sashi" },
      { lane: 1, entry_course: 1, finish_1: 3, decision_type: "makuri" },
      { lane: 1, entry_course: 1, finish_1: 4, decision_type: "makuri-sashi" }
    ]
  );
  assert.equal(row.last6mRaceCount, 4);
  assert.equal(row.escapeRate, 0.25);
  assert.equal(row.beatenBySashiRate, 0.25);
  assert.equal(row.beatenByMakuriRate, 0.25);
  assert.equal(row.beatenByMakuriSashiRate, 0.25);
  assert.equal(row.sampleStatus, "small_sample");
  assert.equal(row.avgST, null, "start timing must remain null when actual timing is unavailable");
}

{
  const row = calculateRacerCourseTendency(
    { boat: 3, course: 3, racerId: "1003", racerName: "Center" },
    [
      { lane: 3, entry_course: 3, finish_1: 3, decision_type: "sashi" },
      { lane: 3, entry_course: 3, finish_1: 3, decision_type: "makuri" },
      { lane: 3, entry_course: 3, finish_1: 3, decision_type: "makuri-sashi" },
      { lane: 3, entry_course: 3, finish_1: 1, decision_type: "escape" }
    ]
  );
  assert.equal(row.sashiRate, 0.25);
  assert.equal(row.makuriRate, 0.25);
  assert.equal(row.makuriSashiRate, 0.25);
  assert.equal(row.escapeRate, null);
}

{
  const match = matchHistoryRowsForTarget(
    { racer_id: "1001", racerName: "ID Match" },
    [
      { touban: "１００１", player_name: "Different Name", lane: 1 },
      { racerNo: "1002", racerName: "ID Match", lane: 2 }
    ]
  );
  assert.equal(match.matchMethod, "racerId");
  assert.equal(match.rows.length, 1);
  assert.equal(match.rows[0].racerId, "1001");
}

{
  const match = matchHistoryRowsForTarget(
    { racerName: "渡邉　俊介" },
    [
      { player_name: "渡邉 俊介", lane: 6 },
      { player_name: "別の選手", lane: 1 }
    ]
  );
  assert.equal(match.matchMethod, "racerName");
  assert.equal(match.rows.length, 1);
}

{
  const match = matchHistoryRowsForTarget(
    { racerId: "1001", racerName: "同名選手" },
    [{ racerId: "9999", racerName: "同名選手", lane: 1 }]
  );
  assert.equal(match.matchMethod, "none", "a conflicting racer ID must not fall back to name matching");
  assert.equal(match.rows.length, 0);
}

{
  const row = calculateRacerCourseTendency(
    { boat: 1, course: 1, racerId: "1001", racerName: "Missing technique" },
    [{ lane: 1, entry_course: 1, finish_1: 1, decision_type: null }],
    "racerId"
  );
  assert.equal(row.last6mRaceCount, 1);
  assert.equal(row.escapeRate, null, "missing winning technique must not create a zero rate");
  assert.equal(row.sampleStatus, "very_small_sample");
  assert.equal(row.matchMethod, "racerId");
  assert.equal(row.debug.matchedTechniqueCount, 0);
  assert.equal(row.debug.matchedHistorySamples[0].includedInAggregation, true);
  assert.equal(row.debug.matchedHistorySamples[0].rateExclusionReason, "missing_decision");
}

{
  const row = calculateRacerCourseTendency(
    { boat: 3, course: 3, racerId: "1003", racerName: "Predicted course" },
    [{ lane: 3, entry_course: 0, finish_1: 3, decision_type: "まくり" }],
    "racerId"
  );
  assert.equal(row.last6mRaceCount, 1);
  assert.equal(row.makuriRate, 1);
  assert.equal(row.sampleStatus, "very_small_sample");
  assert.equal(row.debug.predictedCourseHistoryCount, 1);
}

{
  const row = calculateRacerCourseTendency(
    { boat: 2, course: 2, racerId: "2002", racerName: "Entry only" },
    [
      { raceDate: "2026-01-01", venueId: 1, raceNo: 1, lane: 2, entry_course: 2 },
      { raceDate: "2026-02-01", venueId: 1, raceNo: 2, lane: 2, entry_course: 2, finish_1: 1, decision_type: null }
    ],
    "racerId"
  );
  assert.equal(row.last6mRaceCount, 2, "course-matched entry candidates must count without a decision");
  assert.equal(row.courseSpecificLast6mRaceCount, 2);
  assert.equal(row.allCourseLast6mRaceCount, 2);
  assert.equal(row.sashiRate, null);
  assert.equal(row.sampleStatus, "very_small_sample");
  assert.equal(row.debug.matchedHistorySamples[0].rateExclusionReason, "missing_result");
  assert.equal(row.debug.matchedHistorySamples[1].rateExclusionReason, "missing_decision");
}

{
  const row = calculateRacerCourseTendency(
    { boat: 2, course: 2, racerId: "2002", racerName: "Frame fallback" },
    [{ frame: 2, finish_1: 2, decision: "sashi" }],
    "racerId"
  );
  assert.equal(row.last6mRaceCount, 1, "frame must be usable as a course fallback");
  assert.equal(row.sashiRate, 1);
  assert.equal(row.debug.boatFallbackCourseHistoryCount, 1);
  assert.equal(row.debug.matchedHistorySamples[0].matchedCourseSource, "boat_fallback");
}

{
  const row = calculateRacerCourseTendency(
    { boat: 1, course: 1, racerId: "1001", racerName: "Inside sashi loss" },
    [
      { lane: 1, entry_course: 1, finish_1: 2, decision: "sashi" },
      { lane: 1, entry_course: 1, finish_1: 1, decision: null }
    ],
    "racerId"
  );
  assert.equal(row.last6mRaceCount, 2);
  assert.equal(row.beatenBySashiRate, 0.5);
}

{
  const row = calculateRacerCourseTendency(
    { boat: 2, course: 2, racerId: "2002", racerName: "Course two sashi" },
    [
      { lane: 2, entry_course: 2, finish_1: 2, winMethod: "sashi" },
      { lane: 3, entry_course: 3, finish_1: 3, winMethod: "makuri" }
    ],
    "racerId"
  );
  assert.equal(row.allCourseLast6mRaceCount, 2);
  assert.equal(row.courseSpecificLast6mRaceCount, 1);
  assert.equal(row.sashiRate, 1);
  assert.equal(row.allCourseWinRate, 1);
  assert.equal(row.allCourseSashiRate, 0.5);
  assert.equal(row.allCourseMakuriRate, 0.5);
  assert.equal(row.debug.matchedHistorySamples[1].includedInAggregation, false);
  assert.equal(row.debug.matchedHistorySamples[1].exclusionReason, "course_mismatch");
}

{
  const row = calculateRacerCourseTendency(
    { boat: 3, course: 3, racerId: "3003", racerName: "Sample bands" },
    Array.from({ length: 10 }, (_, index) => ({
      lane: 3,
      entry_course: 3,
      finish_1: index % 2 === 0 ? 3 : 1,
      decision: index % 2 === 0 ? "makuri" : "escape",
      avg_st: 0.14 + (index * 0.001)
    })),
    "racerId"
  );
  assert.equal(row.sampleStatus, "ok");
  assert.equal(row.courseSpecificLast6mRaceCount, 10);
  assert.equal(row.allCourseWinRate, 0.5);
  assert.equal(row.allCourseMakuriRate, 0.5);
  assert.equal(row.allCourseAvgST, 0.145);
}

console.log("racer-tendencies ok");
