import assert from "node:assert/strict";
import db from "../db.js";
import {
  SIMILAR_RACE_STORAGE_PATH,
  updateSimilarRaceFeatureOutcome,
  upsertSimilarRaceFeatureSnapshot
} from "../similar-race-feature-store.js";
import { __testHooks } from "../src/routes/race.js";

const raceId = "20990501_24_1_similar_support_fixture";
const canonicalRaceId = "20990501_24_1";
const stableRaceId = "20990502_24_2";
const backupTable = "similar_race_features_test_backup";

const pureTop6Prediction = {
  racePattern: "escape_stable",
  racePatternScore: 72,
  head_prob_1: 0.58,
  near_tie_second_candidates: [
    { lane: 2, probability: 0.34 },
    { lane: 3, probability: 0.31 }
  ],
  close_combo_preserved: true,
  chaos_level: 0.34,
  top6_coverage: 0.42,
  top6Scenario: "1-nige",
  top6ScenarioScore: 69,
  second_given_head_probabilities: { 2: 0.34, 3: 0.31, 4: 0.18 },
  top6: [
    { combo: "1-2-3", probability: 0.12 },
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
  venue_scenario_bias: {
    one_course_trust: 64
  },
  confidence_score: 70,
  prediction_stability_score: 68,
  recommendedBetMode: "buy_top6"
};

const race = {
  date: "2099-05-01",
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

try {
  db.exec(`DROP TABLE IF EXISTS ${backupTable}`);
  db.exec(`CREATE TEMP TABLE ${backupTable} AS SELECT * FROM similar_race_features`);
  db.exec("DELETE FROM similar_race_features");

  upsertSimilarRaceFeatureSnapshot({
    raceId,
    race,
    prediction: {
      pure_top6_prediction: pureTop6Prediction,
      hardRace1234: {
        boat1_head_pre: 0.58,
        outside_break_risk_pre: 0.22
      }
    }
  });
  updateSimilarRaceFeatureOutcome({
    raceId,
    date: "2099-05-01",
    venueId: 24,
    raceNo: 1,
    finalResult: "1-2-3",
    headHit: true,
    betHit: true,
    top6Hit: true
  });

  const selfOnlySupport = __testHooks.buildSimilarRaceSupport({
    race,
    pureTop6Prediction,
    hardRace1234: {
      boat1_head_pre: 0.58,
      outside_break_risk_pre: 0.22
    }
  });

  assert.equal(selfOnlySupport.similarRaceCurrentKey, canonicalRaceId);
  assert.equal(selfOnlySupport.similarRaceExcludedSelf, true);
  assert.deepEqual(selfOnlySupport.similarRaceMatchedKeys, []);
  assert.equal(selfOnlySupport.similarRaceMatchedCount, 0);
  assert.equal(selfOnlySupport.similarRaceHistoryAvailable, false);
  assert.equal(selfOnlySupport.similarRaceSupport?.basis, "heuristic_no_history");

  const storedPredictionOnly = upsertSimilarRaceFeatureSnapshot({
    raceId: null,
    race: {
      ...race,
      date: "2099-05-02",
      raceNo: 2
    },
    prediction: {
      pure_top6_prediction: pureTop6Prediction,
      hardRace1234: {
        boat1_head_pre: 0.58,
        outside_break_risk_pre: 0.22
      }
    }
  });
  assert.equal(storedPredictionOnly.race_id, stableRaceId);
  const predictionOnlyRow = db.prepare("SELECT race_id, settled, final_result FROM similar_race_features WHERE race_id = ?").get(stableRaceId);
  assert.equal(predictionOnlyRow.race_id, stableRaceId);
  assert.equal(predictionOnlyRow.settled, 0);
  assert.equal(predictionOnlyRow.final_result, null);

  updateSimilarRaceFeatureOutcome({
    date: "2099-05-02",
    venueId: 24,
    raceNo: 2,
    finalResult: "1-2-3",
    headHit: true,
    betHit: true
  });
  const settledRow = db.prepare("SELECT final_result, head_hit, top6_hit, optional16_hit, settled FROM similar_race_features WHERE race_id = ?").get(stableRaceId);
  assert.equal(settledRow.final_result, "1-2-3");
  assert.equal(settledRow.head_hit, 1);
  assert.equal(settledRow.top6_hit, 1);
  assert.equal(settledRow.settled, 1);

  upsertSimilarRaceFeatureSnapshot({
    raceId,
    race,
    prediction: {
      pure_top6_prediction: pureTop6Prediction,
      hardRace1234: {
        boat1_head_pre: 0.58,
        outside_break_risk_pre: 0.22
      }
    }
  });
  updateSimilarRaceFeatureOutcome({
    raceId,
    date: "2099-05-01",
    venueId: 24,
    raceNo: 1,
    finalResult: "1-2-3",
    headHit: true,
    betHit: true,
    top6Hit: true
  });

  const support = __testHooks.buildSimilarRaceSupport({
    race,
    pureTop6Prediction,
    hardRace1234: {
      boat1_head_pre: 0.58,
      outside_break_risk_pre: 0.22
    }
  });

  assert.equal(support.similarRaceSearchExecuted, true);
  assert.equal(support.similarRaceStoragePath, SIMILAR_RACE_STORAGE_PATH);
  assert.equal(support.similarRaceHistoryAvailable, true);
  assert.equal(support.similarRaceMatchedCount, 1);
  assert.equal(support.similarRaceCount, 1);
  assert.equal(support.similarRaceCurrentKey, canonicalRaceId);
  assert.deepEqual(support.similarRaceMatchedKeys, [stableRaceId]);
  assert.equal(support.similarRaceExcludedSelf, true);
  assert.ok(support.similarRaceSettleEligibleCount > 0);
  assert.equal(support.similarRaceSupport?.basis, "history_supported");
  assert.ok(Array.isArray(support.similarRaceExamples));
  assert.ok(support.similarRaceExamples.length > 0);
  assert.equal(support.similarRaceHitBias?.bet_hit_rate, 100);
  assert.ok(support.similarRaceQueryKey?.venueId === 24);

  console.log("similar-race-support ok");
} finally {
  try {
    const backupExists = db.prepare("SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name = ?").get(backupTable);
    if (backupExists) {
      db.exec("DELETE FROM similar_race_features");
      db.exec(`INSERT INTO similar_race_features SELECT * FROM ${backupTable}`);
      db.exec(`DROP TABLE IF EXISTS ${backupTable}`);
    }
  } catch {
    db.prepare("DELETE FROM similar_race_features WHERE race_id = ?").run(raceId);
    db.prepare("DELETE FROM similar_race_features WHERE race_id = ?").run(canonicalRaceId);
    db.prepare("DELETE FROM similar_race_features WHERE race_id = ?").run(stableRaceId);
  }
}
