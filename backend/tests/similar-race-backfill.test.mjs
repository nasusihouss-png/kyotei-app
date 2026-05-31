import assert from "node:assert/strict";
import db from "../db.js";
import { backfillSimilarRaceFeatures } from "../similar-race-feature-store.js";
import { __testHooks } from "../src/routes/race.js";

const backupTable = "similar_race_features_backfill_test_backup";
const historyRaceId = "20990701_24_1";
const currentRaceId = "20990702_24_1";

const prediction = {
  pure_top6_prediction: {
    racePattern: "escape_stable",
    racePatternScore: 74,
    head_prob_1: 0.6,
    near_tie_second_candidates: [
      { lane: 2, probability: 0.33 },
      { lane: 3, probability: 0.31 }
    ],
    close_combo_preserved: true,
    chaos_level: 0.31,
    top6_coverage: 0.44,
    top6Scenario: "1-nige",
    top6ScenarioScore: 71,
    second_given_head_probabilities: { 2: 0.33, 3: 0.31, 4: 0.2 },
    top6: [
      { combo: "1-2-3", probability: 0.13 },
      { combo: "1-3-2", probability: 0.11 },
      { combo: "1-2-4", probability: 0.08 },
      { combo: "1-4-2", probability: 0.06 },
      { combo: "2-1-3", probability: 0.05 },
      { combo: "3-1-2", probability: 0.04 }
    ],
    lane_styles: [
      { lane: 1, style_code: "nige", style_score: 78 },
      { lane: 2, style_code: "sashi", style_score: 66 },
      { lane: 3, style_code: "makuri", style_score: 62 },
      { lane: 4, style_code: "tenkai_machi", style_score: 58 }
    ],
    venue_scenario_bias: { one_course_trust: 64 },
    confidence_score: 70,
    prediction_stability_score: 68,
    recommendedBetMode: "buy_top6"
  },
  hardRace1234: {
    hardScenario: "escape_with_pressure",
    hardScenarioScore: 63,
    boat1_head_pre: 0.6,
    outside_break_risk_pre: 0.22
  }
};

const currentRace = {
  date: "2099-07-02",
  venueId: 24,
  venueName: "Omura",
  raceNo: 1,
  racers: [
    { lane: 1, style_code: "nige", style_score: 78, lapTime: 6.72, exhibitionTime: 6.71 },
    { lane: 2, style_code: "sashi", style_score: 66, lapTime: 6.75, exhibitionTime: 6.74 },
    { lane: 3, style_code: "makuri", style_score: 62, lapTime: 6.77, exhibitionTime: 6.76 },
    { lane: 4, style_code: "tenkai_machi", style_score: 58, lapTime: 6.8, exhibitionTime: 6.79 },
    { lane: 5, style_code: "stable_hold", style_score: 48, lapTime: 6.84, exhibitionTime: 6.83 },
    { lane: 6, style_code: "outside_entry", style_score: 42, lapTime: 6.88, exhibitionTime: 6.87 }
  ]
};

function insertRaceFixture() {
  db.prepare(`
    INSERT OR REPLACE INTO races (race_id, race_date, venue_id, venue_name, race_no)
    VALUES (?, ?, ?, ?, ?)
  `).run(historyRaceId, "2099-07-01", 24, "Omura", 1);
  db.prepare(`
    INSERT INTO prediction_logs (
      race_id,
      race_key,
      race_date,
      venue_code,
      venue_name,
      race_no,
      prediction_timestamp,
      prediction_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    historyRaceId,
    historyRaceId,
    "2099-07-01",
    24,
    "Omura",
    1,
    "2099-07-01T09:00:00.000Z",
    JSON.stringify(prediction)
  );
  db.prepare(`
    INSERT OR REPLACE INTO results (race_id, finish_1, finish_2, finish_3, payout_3t)
    VALUES (?, ?, ?, ?, ?)
  `).run(historyRaceId, 1, 2, 3, 1200);
}

try {
  db.exec(`DROP TABLE IF EXISTS ${backupTable}`);
  db.exec(`CREATE TEMP TABLE ${backupTable} AS SELECT * FROM similar_race_features`);
  db.exec("DELETE FROM similar_race_features");
  db.prepare("DELETE FROM prediction_logs WHERE race_id IN (?, ?)").run(historyRaceId, currentRaceId);
  db.prepare("DELETE FROM results WHERE race_id IN (?, ?)").run(historyRaceId, currentRaceId);
  db.prepare("DELETE FROM races WHERE race_id IN (?, ?)").run(historyRaceId, currentRaceId);
  insertRaceFixture();

  const dryRun = backfillSimilarRaceFeatures({
    dateFrom: "2099-07-01",
    dateTo: "2099-07-01",
    venueIds: [24],
    dryRun: true,
    limit: 20,
    progressEvery: 0
  });
  assert.equal(dryRun.backfillInsertedCount, 1);
  assert.equal(dryRun.backfillUpdatedCount, 0);
  assert.equal(dryRun.backfillDryRun, true);
  assert.equal(db.prepare("SELECT COUNT(*) AS count FROM similar_race_features WHERE race_id = ?").get(historyRaceId).count, 0);

  const summary = backfillSimilarRaceFeatures({
    dateFrom: "2099-07-01",
    dateTo: "2099-07-01",
    venueIds: [24],
    limit: 20,
    progressEvery: 0
  });
  assert.equal(summary.backfillInsertedCount, 1);
  assert.equal(summary.backfillUpdatedCount, 0);
  assert.equal(summary.backfillVenueIds[0], 24);
  assert.equal(summary.backfillRange.dateFrom, "2099-07-01");

  const stored = db.prepare(`
    SELECT race_id, final_result, settled, head_hit, top6_hit, optional16_hit
    FROM similar_race_features
    WHERE race_id = ?
  `).get(historyRaceId);
  assert.equal(stored.race_id, historyRaceId);
  assert.equal(stored.final_result, "1-2-3");
  assert.equal(stored.settled, 1);
  assert.equal(stored.head_hit, 1);
  assert.equal(stored.top6_hit, 1);
  assert.equal(stored.optional16_hit, 0);

  const support = __testHooks.buildSimilarRaceSupport({
    race: currentRace,
    pureTop6Prediction: prediction.pure_top6_prediction,
    hardRace1234: prediction.hardRace1234
  });
  assert.equal(support.similarRaceCurrentKey, currentRaceId);
  assert.equal(support.similarRaceExcludedSelf, true);
  assert.equal(support.similarRaceMatchedCount, 1);
  assert.deepEqual(support.similarRaceMatchedKeys, [historyRaceId]);
  assert.equal(support.similarRaceHistoryAvailable, true);
  assert.equal(support.similarRaceSupport?.basis, "history_supported");

  console.log("similar-race-backfill ok");
} finally {
  db.prepare("DELETE FROM prediction_logs WHERE race_id IN (?, ?)").run(historyRaceId, currentRaceId);
  db.prepare("DELETE FROM results WHERE race_id IN (?, ?)").run(historyRaceId, currentRaceId);
  db.prepare("DELETE FROM races WHERE race_id IN (?, ?)").run(historyRaceId, currentRaceId);
  const backupExists = db.prepare("SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name = ?").get(backupTable);
  if (backupExists) {
    db.exec("DELETE FROM similar_race_features");
    db.exec(`INSERT INTO similar_race_features SELECT * FROM ${backupTable}`);
    db.exec(`DROP TABLE IF EXISTS ${backupTable}`);
  }
}
