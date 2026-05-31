import assert from "node:assert/strict";
import {
  buildExhibitionFeatures,
  buildTodayRanking,
  buildRacePrediction,
  DEFAULT_SCORING_CONFIG,
  inspectPreviewExhibitionStatus,
  isExhibitionAvailable,
  plackettLuceTrifecta,
  screenInsideEscapeCandidates,
  shrinkRate,
  softmax
} from "./kyotei-openapi-engine.js";

function program(overrides = {}) {
  const boatOverrides = overrides.boatOverrides || {};
  return {
    race_date: "2026-05-31",
    race_stadium_number: 24,
    race_number: 1,
    race_closed_at: "10:30",
    boats: Array.from({ length: 6 }, (_, index) => {
      const boat = index + 1;
      return {
        racer_boat_number: boat,
        racer_name: `Racer ${boat}`,
        racer_number: 5000 + boat,
        racer_class_number: boat === 1 ? 1 : boat === 2 ? 2 : 3,
        racer_average_start_timing: 0.14 + index * 0.01,
        racer_national_top_1_percent: boat === 1 ? 7.2 : 4.8,
        racer_national_top_2_percent: boat === 1 ? 58 : 34,
        racer_local_top_1_percent: boat === 1 ? 6.8 : 4.4,
        racer_local_top_2_percent: boat === 1 ? 54 : 30,
        racer_assigned_motor_top_2_percent: boat === 1 ? 42 : 31,
        racer_assigned_boat_top_2_percent: 30,
        racer_flying_count: 0,
        racer_late_count: 0,
        ...(boatOverrides[boat] || {})
      };
    }),
    ...Object.fromEntries(Object.entries(overrides).filter(([key]) => key !== "boatOverrides"))
  };
}

function preview({ complete = true, fastBoat = null } = {}) {
  return {
    race_wind: 2,
    race_wave: 1,
    boats: Object.fromEntries(
      Array.from({ length: 6 }, (_, index) => {
        const boat = index + 1;
        const time = fastBoat === boat ? 6.70 : 6.82 + index * 0.01;
        return [
          String(boat),
          {
            racer_boat_number: boat,
            racer_course_number: complete ? boat : null,
            racer_start_timing: complete ? 0.11 + index * 0.01 : null,
            racer_exhibition_time: complete ? time : 0
          }
        ];
      })
    )
  };
}

assert.equal(shrinkRate(0.8, 0, 0.5, 10), 0.5);
assert.equal(shrinkRate(0.8, 10, 0.5, 10), 0.65);

const softmaxRows = softmax([{ boat: 1, score: 1 }, { boat: 2, score: 2 }]);
assert.equal(Math.round(softmaxRows.reduce((sum, row) => sum + row.probability, 0) * 1e12) / 1e12, 1);

const plRows = plackettLuceTrifecta(Array.from({ length: 6 }, (_, i) => ({ boat: i + 1, score: 6 - i })));
assert.equal(plRows.length, 120);
assert.ok(Math.abs(plRows.reduce((sum, row) => sum + row.probability, 0) - 1) < 1e-12);

const missingPreview = preview({ complete: false });
assert.equal(isExhibitionAvailable(missingPreview), false);
assert.deepEqual(buildExhibitionFeatures(missingPreview).usedFields, []);
assert.equal(inspectPreviewExhibitionStatus(missingPreview).exhibitionNotRun, true);

const basePrediction = buildRacePrediction(program(), missingPreview);
const noisyMissingPreview = {
  ...missingPreview,
  boats: Object.fromEntries(
    Object.entries(missingPreview.boats).map(([key, row]) => [
      key,
      {
        ...row,
        racer_exhibition_time: 0,
        racer_course_number: null,
        racer_start_timing: 0
      }
    ])
  )
};
assert.equal(isExhibitionAvailable(noisyMissingPreview), false);
const repeatedBase = buildRacePrediction(program(), noisyMissingPreview);
assert.deepEqual(
  basePrediction.scoredBoats.map((row) => Number(row.score.toFixed(8))),
  repeatedBase.scoredBoats.map((row) => Number(row.score.toFixed(8))),
  "unconfirmed exhibition zero/null values must not affect scores"
);

const reflected = buildRacePrediction(program(), preview({ complete: true, fastBoat: 4 }));
assert.equal(reflected.exhibition.status, "exhibition_reflected");
assert.ok(
  reflected.scoredBoats.find((row) => row.boat === 4).score >
    basePrediction.scoredBoats.find((row) => row.boat === 4).score,
  "complete exhibition data should affect scores"
);

const keyedPreviewWithoutBoatNumber = {
  race_wind: 1,
  boats: Object.fromEntries(
    Array.from({ length: 6 }, (_, index) => {
      const boat = index + 1;
      return [String(boat), {
        racer_course_number: boat,
        racer_start_timing: 0.12 + index * 0.01,
        racer_exhibition_time: 6.77 + index * 0.01
      }];
    })
  )
};
const keyedPrediction = buildRacePrediction(program(), keyedPreviewWithoutBoatNumber);
assert.equal(keyedPrediction.exhibition.status, "exhibition_reflected");
assert.equal(keyedPrediction.exhibition.exhibitionStartByBoat[1], 0.12);
assert.equal(keyedPrediction.exhibition.exhibitionTimeByBoat[1], 6.77);
assert.equal(keyedPrediction.exhibition.diagnostics.boatShape, "object_by_boat_number");

const unifiedKeyPreview = {
  boats: {
    1: { exST: 0.10, exTime: 6.66, racer_course_number: 1 },
    2: { exST: 0.12, exTime: 6.74, racer_course_number: 2 },
    3: { exST: 0.13, exTime: 6.75, racer_course_number: 3 },
    4: { exST: 0.14, exTime: 6.76, racer_course_number: 4 },
    5: { exST: 0.15, exTime: 6.77, racer_course_number: 5 },
    6: { exST: 0.16, exTime: 6.78, racer_course_number: 6 }
  }
};
const unifiedKeyPrediction = buildRacePrediction(
  program({
    boatOverrides: {
      3: { straightTime: 6.88, lapTime: 36.8, motor2Rate: 41 }
    }
  }),
  unifiedKeyPreview
);
assert.equal(unifiedKeyPrediction.exhibition.status, "exhibition_reflected");
assert.equal(unifiedKeyPrediction.exhibition.exhibitionStartByBoat, null);
assert.equal(unifiedKeyPrediction.exhibition.exhibitionTimeByBoat, null);
assert.deepEqual(unifiedKeyPrediction.exhibition.usedFields, ["entry_course"]);
assert.equal(unifiedKeyPrediction.scoredBoats.find((row) => row.boat === 3)?.straightTime, 6.88);

const boat1Probability = basePrediction.firstPlaceProbabilities.find((row) => row.boat === 1).probability;
assert.ok(boat1Probability > 0.2, "top_1 points must be used as 1-8 point values, not divided as percent");

const candidates = screenInsideEscapeCandidates([program({ race_number: 1 })], {}, {
  ...structuredClone(DEFAULT_SCORING_CONFIG),
  insideCandidateThreshold: 0
});
assert.equal(candidates[0].recommended.length, 5);
assert.ok(candidates[0].recommended.every((row) => row.boats[0] === 1));

const ranking = buildTodayRanking(
  Array.from({ length: 12 }, (_, index) => program({ race_number: index + 1 })),
  { "24-1": preview({ complete: true }) },
  { limit: 20 }
);
assert.equal(ranking.length, 12);
assert.ok(ranking[0].confidenceScore >= ranking[ranking.length - 1].confidenceScore);
assert.equal(ranking[0].tickets.length, 6);
assert.ok(ranking[0].mainHead.boat >= 1 && ranking[0].mainHead.boat <= 6);
assert.ok(Array.isArray(ranking[0].attention));

const upsetProgram = program({
  boatOverrides: {
    1: {
      racer_national_top_1_percent: 4.2,
      racer_average_start_timing: 0.24,
      racer_assigned_motor_top_2_percent: 20
    },
    3: {
      racer_national_top_1_percent: 6.8,
      racer_average_start_timing: 0.12,
      makuriRate: 68,
      makuriSashiRate: 62
    },
    4: {
      racer_national_top_1_percent: 6.4,
      racer_average_start_timing: 0.12,
      makuriSashiRate: 70
    }
  }
});
const upsetPrediction = buildRacePrediction(upsetProgram, preview({ complete: true, fastBoat: 4 }));
assert.equal(upsetPrediction.tickets.trifecta.slice(0, 6).length, 6);
assert.ok(Array.isArray(upsetPrediction.developmentScenarios));
assert.equal(upsetPrediction.developmentScenarios.length, 8);
assert.ok(upsetPrediction.extraTickets.length <= 6);
assert.ok(upsetPrediction.upsetScenarios.every((row) => Array.isArray(row.recommendedExtraTickets)));

console.log("kyotei-openapi-engine ok");
