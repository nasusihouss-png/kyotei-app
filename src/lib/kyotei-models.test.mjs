import assert from "node:assert/strict";
import {
  buildDashboardAggregationModel,
  buildPlayerMasterPersistenceModel,
  buildPredictionExplanationModel,
  buildQuickInputModel,
  buildRaceLogPersistenceModel,
  buildTicketsModel
} from "./kyotei-models.js";

const quickInput = buildQuickInputModel({
  date: "2026-05-01",
  venueId: 24,
  venue: "Omura",
  raceNo: 1,
  racers: [{ racerName: "A", racerId: "1001" }]
});

assert.equal(quickInput.racers.length, 6);
assert.equal(quickInput.racers[0].racerName, "A");
assert.ok(quickInput.autoFetchedFields.includes("lap_time"));
assert.ok(quickInput.autoFetchedFields.includes("boat1_second_keep_probability"));

const predictionExplanation = buildPredictionExplanationModel({
  data: {
    participationDecision: { decision: "recommended", recommended_bet_mode: "top6" }
  },
  pureTop6Prediction: {
    racePattern: "inside_stable",
    racePatternScore: 71,
    top6Scenario: "1-nige",
    top6ScenarioScore: 65,
    scenario_repro_score: 0.68,
    confidence: 0.74,
    lane_styles: [{ lane: 1, style: "keeper", style_score: 82 }],
    first_place_candidate_rates: [{ lane: 1, probability: 0.62 }],
    second_given_head_probabilities: { 2: 0.34, 3: 0.31 },
    boat1_second_keep_score: 58
  }
});

for (const key of [
  "racePattern",
  "racePatternScore",
  "top6Scenario",
  "top6ScenarioScore",
  "hardScenario",
  "hardScenarioScore",
  "scenario_repro_score",
  "style",
  "style_score",
  "Pr1",
  "Pr2",
  "Pr3",
  "boat1_second_keep_score",
  "second_given_head_probabilities",
  "confidence_score",
  "confidence_band",
  "buyPolicy",
  "recommendedBetMode",
  "skipRiskReason"
]) {
  assert.ok(Object.hasOwn(predictionExplanation, key), `missing ${key}`);
}

const tickets = buildTicketsModel({
  pureTop6Prediction: {
    top6: [
      { combo: "1-2-3", probability: 0.12 },
      { combo: "1-3-2", probability: 0.11 },
      { combo: "1-2-4", probability: 0.1 },
      { combo: "1-4-2", probability: 0.09 },
      { combo: "2-1-3", probability: 0.08 },
      { combo: "3-1-2", probability: 0.07 }
    ],
    optionalFormation16: {
      active: true,
      combos: [{ combo: "1-3-4" }]
    },
    formationReason: "cover lane 3 attack"
  }
});

assert.equal(tickets.top6.length, 6);
assert.equal(tickets.optionalFormation16.combos.length, 1);
assert.equal(tickets.top6[0].tier, "main");
assert.equal(typeof tickets.formationReason, "string");

const inactiveTickets = buildTicketsModel({
  pureTop6Prediction: {
    top6: [{ combo: "1-2-3", probability: 0.12 }],
    optionalFormation16: {
      active: false,
      combos: [],
      reason: "top6 coverage is already sufficient"
    },
    formationReason: "top6 coverage is already sufficient"
  }
});

assert.deepEqual(inactiveTickets.optionalFormation16, []);
assert.equal(inactiveTickets.formationReason, null);

const log = buildRaceLogPersistenceModel({
  quickInput,
  predictionExplanation,
  tickets,
  actualResult: "1-2-3",
  payout: 950
});

assert.equal(log.hit, true);
assert.equal(log.top6[0], "1-2-3");
assert.equal(log.scenarioSnapshot.top6Scenario, "1-nige");

const dashboard = buildDashboardAggregationModel([log]);
assert.equal(dashboard.raceCount, 1);
assert.equal(dashboard.top6HitRate, 1);
assert.equal(dashboard.venueWise[0].venue, "Omura");

const masterRows = buildPlayerMasterPersistenceModel([
  {
    racerName: "A",
    racerId: "1001",
    style: "keeper",
    preferredCourse: 1,
    laneAdjustments: { 1: 4 },
    keepPositionAdjustments: { second: 2 }
  }
]);

assert.equal(masterRows[0].style, "keeper");
assert.equal(masterRows[0].baseAdjustment, 0);

console.log("kyotei-models ok");
