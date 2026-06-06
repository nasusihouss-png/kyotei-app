import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  historyCacheRacesToRows,
  loadHistoryCache,
  normalizeBackfilledResultRace,
  normalizeWinningDecision,
  runHistoryBackfill
} from "../src/services/race-history-backfill.js";
import {
  calculateRacerCourseTendency,
  matchHistoryRowsForTarget
} from "../src/services/racer-tendencies.js";

assert.equal(normalizeWinningDecision(1), "逃げ");
assert.equal(normalizeWinningDecision("まくり差し"), "まくり差し");
assert.equal(normalizeWinningDecision("差し"), "差し");

{
  const race = normalizeBackfilledResultRace({
    date: "20260115",
    stadium_number: 24,
    number: 3,
    technique_number: 4,
    boats: [{
      racer_boat_number: 3,
      racer_course_number: 3,
      racer_number: "3003",
      racer_name: "Center Racer",
      racer_place_number: 1,
      racer_start_timing: 0.11
    }]
  });
  assert.equal(race.date, "2026-01-15");
  assert.equal(race.result.winnerBoat, 3);
  assert.equal(race.result.winningDecision, "まくり差し");
  assert.equal(race.entries[0].course, 3);
  assert.equal(race.entries[0].startTiming, 0.11);
}

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "kyotei-history-backfill-"));
let networkCalls = 0;
try {
  const httpGet = async (url) => {
    networkCalls += 1;
    const compactDate = url.match(/(\d{8})\.json/)?.[1];
    return {
      data: {
        results: [{
          date: compactDate,
          stadium_number: 24,
          number: 1,
          technique_number: 2,
          boats: [{
            racer_boat_number: 2,
            racer_course_number: 2,
            racer_number: "2002",
            racer_name: "Sashi Racer",
            racer_place_number: 1,
            racer_start_timing: 0.13
          }]
        }]
      }
    };
  };

  const first = await runHistoryBackfill({
    date: "2026-02-01",
    venueId: 24,
    months: 1,
    dataDir: tempDir,
    httpGet,
    concurrency: 8
  });
  assert.equal(first.ok, true);
  assert.equal(first.fetchedRaceCount, 31);
  assert.equal(first.fetchedEntryCount, 31);
  assert.equal(first.fetchedResultCount, 31);
  assert.equal(fs.readdirSync(tempDir).length, 31);

  const cache = loadHistoryCache({
    periodStart: "2026-01-01",
    periodEnd: "2026-02-01",
    dataDir: tempDir
  });
  assert.equal(cache.races.length, 31, "normalized history cache must be loadable");

  const historyRows = historyCacheRacesToRows(cache.races);
  const matched = matchHistoryRowsForTarget(
    { boat: 2, course: 2, racerId: "2002", racerName: "Sashi Racer" },
    historyRows
  );
  const tendency = calculateRacerCourseTendency(
    { boat: 2, course: 2, racerId: "2002", racerName: "Sashi Racer" },
    matched.rows,
    matched.matchMethod
  );
  assert.equal(tendency.courseSpecificLast6mRaceCount, 31);
  assert.equal(tendency.sashiRate, 1, "backfilled winning decision must reach tendency calculation");
  assert.equal(tendency.avgST, 0.13);
  assert.equal(tendency.sampleStatus, "ok");

  const callsAfterFirstRun = networkCalls;
  const second = await runHistoryBackfill({
    date: "2026-02-01",
    venueId: 24,
    months: 1,
    dataDir: tempDir,
    httpGet,
    concurrency: 8
  });
  assert.equal(second.cacheHitCount, 31);
  assert.equal(networkCalls, callsAfterFirstRun, "cached dates must not be refetched");

  const forced = await runHistoryBackfill({
    date: "2026-02-01",
    venueId: 24,
    months: 1,
    force: true,
    dataDir: tempDir,
    httpGet,
    concurrency: 8
  });
  assert.equal(forced.cacheWriteCount, 31);
  assert.equal(networkCalls, callsAfterFirstRun + 31, "force=1 must refresh cached dates");
} finally {
  fs.rmSync(tempDir, { recursive: true, force: true });
}

const allVenueTempDir = fs.mkdtempSync(path.join(os.tmpdir(), "kyotei-all-venue-history-"));
let allVenueNetworkCalls = 0;
try {
  const httpGet = async (url) => {
    allVenueNetworkCalls += 1;
    const compactDate = url.match(/(\d{8})\.json/)?.[1];
    return {
      data: {
        results: [
          {
            date: compactDate,
            stadium_number: 1,
            number: 4,
            technique_number: 1,
            boats: [{
              racer_boat_number: 1,
              racer_course_number: 1,
              racer_number: "1001",
              racer_name: "All Venue Target",
              racer_place_number: 1,
              racer_start_timing: 0.09
            }]
          },
          {
            date: compactDate,
            stadium_number: 24,
            number: 1,
            technique_number: 3,
            boats: [{
              racer_boat_number: 3,
              racer_course_number: 3,
              racer_number: "9999",
              racer_name: "Other Racer",
              racer_place_number: 1,
              racer_start_timing: 0.16
            }]
          }
        ]
      }
    };
  };

  const result = await runHistoryBackfill({
    date: "2026-02-01",
    venueId: 24,
    months: 1,
    allVenues: true,
    targetRacers: [{ boat: 1, course: 1, racerId: "1001", racerName: "All Venue Target" }],
    dataDir: allVenueTempDir,
    httpGet,
    concurrency: 8
  });
  assert.equal(result.ok, true);
  assert.equal(result.allVenues, true);
  assert.equal(result.targetRacerCount, 1);
  assert.equal(result.scannedDateCount, 31);
  assert.equal(result.scannedVenueCount, 2);
  assert.equal(result.scannedRaceCount, 62);
  assert.equal(result.scannedEntryCount, 62);
  assert.equal(result.matchedEntryCount, 31, "all-venue backfill must match target history outside the selected venue");
  assert.equal(result.matchedByRacer["1001"], 31);

  const cache = loadHistoryCache({
    periodStart: "2026-01-01",
    periodEnd: "2026-02-01",
    dataDir: allVenueTempDir
  });
  const matched = matchHistoryRowsForTarget(
    { boat: 1, course: 1, racerId: "1001", racerName: "All Venue Target" },
    historyCacheRacesToRows(cache.races)
  );
  const tendency = calculateRacerCourseTendency(
    { boat: 1, course: 1, racerId: "1001", racerName: "All Venue Target" },
    matched.rows,
    matched.matchMethod
  );
  assert.equal(tendency.courseSpecificLast6mRaceCount, 31);
  assert.equal(tendency.escapeRate, 1);
  assert.equal(tendency.sampleStatus, "ok");

  const callsAfterAllVenueRun = allVenueNetworkCalls;
  const cached = await runHistoryBackfill({
    date: "2026-02-01",
    venueId: 24,
    months: 1,
    allVenues: true,
    targetRacers: [{ boat: 1, course: 1, racerId: "1001", racerName: "All Venue Target" }],
    dataDir: allVenueTempDir,
    httpGet,
    concurrency: 8
  });
  assert.equal(cached.cacheHitCount, 31);
  assert.equal(cached.matchedEntryCount, 31);
  assert.equal(allVenueNetworkCalls, callsAfterAllVenueRun, "all-venue day marker cache must prevent refetching");
} finally {
  fs.rmSync(allVenueTempDir, { recursive: true, force: true });
}

{
  const tendency = calculateRacerCourseTendency(
    { boat: 4, course: 4, racerId: "4004", racerName: "No History" },
    []
  );
  assert.equal(tendency.sampleStatus, "insufficient_history");
  assert.equal(tendency.makuriSashiRate, null);
}

console.log("race-history-backfill ok");
