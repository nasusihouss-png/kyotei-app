import assert from "node:assert/strict";
import {
  buildExhibitionFeatures,
  buildConfidenceScore,
  buildBoat4OpportunityRanking,
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
assert.deepEqual(buildExhibitionFeatures(missingPreview).usedFields, ["weather"]);
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
      racer_assigned_motor_top_2_percent: 20,
      playerTendency: {
        escapeRate: 31,
        avgStartTiming: 0.24,
        lateStartRate: 24
      }
    },
    2: {
      playerTendency: {
        nigashiRate: 32,
        sashiRate: 67
      }
    },
    3: {
      racer_national_top_1_percent: 6.8,
      racer_average_start_timing: 0.12,
      straightTime: 6.72,
      playerTendency: {
        makuriRate: 68,
        makuriSashiRate: 62
      }
    },
    4: {
      racer_national_top_1_percent: 6.4,
      racer_average_start_timing: 0.12,
      playerTendency: {
        makuriSashiRate: 70
      }
    }
  }
});
const upsetPrediction = buildRacePrediction(upsetProgram, preview({ complete: true, fastBoat: 4 }));
assert.equal(upsetPrediction.tickets.trifecta.slice(0, 6).length, 6);
assert.ok(Array.isArray(upsetPrediction.developmentScenarios));
assert.equal(upsetPrediction.developmentScenarios.length, 8);
assert.ok(upsetPrediction.extraTickets.length <= 6);
assert.ok(upsetPrediction.upsetScenarios.every((row) => Array.isArray(row.recommendedExtraTickets)));
assert.equal(upsetPrediction.scoredBoats.find((row) => row.boat === 1)?.playerTendency?.escapeRate, 31);
assert.equal(upsetPrediction.scoredBoats.find((row) => row.boat === 2)?.racerCourseStats?.sashiRate, 67);
assert.ok(upsetPrediction.upsetReasons.some((reason) => String(reason).includes("2号艇の逃がし率")));
assert.ok(upsetPrediction.upsetReasons.some((reason) => String(reason).includes("3号艇")));

const tendencyRanking = buildTodayRanking([upsetProgram], { "24-1": preview({ complete: true, fastBoat: 4 }) }, { limit: 1 });
assert.ok(tendencyRanking[0].attention.some((row) => String(row).includes("出遅れ率") || String(row).includes("穴候補")));

function originalMetricProgram(metricOverrides = {}) {
  const defaults = {
    1: { exST: 0.11, exTime: 6.72, lapTime: 18.25, straightTime: 7.48, turnTime: 4.45, racer_assigned_motor_top_2_percent: 42 },
    2: { exST: 0.13, exTime: 6.78, lapTime: 18.38, straightTime: 7.55, turnTime: 4.5, racer_assigned_motor_top_2_percent: 35 },
    3: { exST: 0.14, exTime: 6.8, lapTime: 18.46, straightTime: 7.58, turnTime: 4.55, racer_assigned_motor_top_2_percent: 33 },
    4: { exST: 0.15, exTime: 6.82, lapTime: 18.5, straightTime: 7.61, turnTime: 4.6, racer_assigned_motor_top_2_percent: 32 },
    5: { exST: 0.16, exTime: 6.84, lapTime: 18.56, straightTime: 7.64, turnTime: 4.66, racer_assigned_motor_top_2_percent: 30 },
    6: { exST: 0.17, exTime: 6.86, lapTime: 18.6, straightTime: 7.68, turnTime: 4.7, racer_assigned_motor_top_2_percent: 28 }
  };
  return program({
    boatOverrides: Object.fromEntries(
      [1, 2, 3, 4, 5, 6].map((boat) => [boat, { ...defaults[boat], ...(metricOverrides[boat] || {}) }])
    )
  });
}

const boat1StrongOriginal = buildRacePrediction(originalMetricProgram({
  1: { lapTime: 18.1, turnTime: 4.3 },
  2: { lapTime: 18.55, turnTime: 4.68 },
  3: { lapTime: 18.58, turnTime: 4.72 }
}), null);
const boat1WeakOriginal = buildRacePrediction(originalMetricProgram({
  1: { lapTime: 18.75, turnTime: 4.82 },
  2: { lapTime: 18.2, turnTime: 4.35 },
  3: { lapTime: 18.25, turnTime: 4.38 }
}), null);
assert.ok(
  boat1StrongOriginal.scoredBoats.find((row) => row.boat === 1).scoreParts.roleFeatureBoost >
    boat1WeakOriginal.scoredBoats.find((row) => row.boat === 1).scoreParts.roleFeatureBoost,
  "lapTime/turnTime should lift boat 1 inside-keep scoring"
);
assert.ok(
  boat1StrongOriginal.developmentScenarios.find((row) => row.attacker === 1).probabilityScore >
    boat1WeakOriginal.developmentScenarios.find((row) => row.attacker === 1).probabilityScore,
  "lapTime/turnTime should affect 1残し scenario evaluation"
);

const boat3StraightStrong = buildRacePrediction(originalMetricProgram({
  3: { exST: 0.06, straightTime: 7.25, turnTime: 4.42 },
  1: { exST: 0.15, straightTime: 7.58 },
  2: { exST: 0.16, straightTime: 7.6 }
}), null);
const boat3StraightWeak = buildRacePrediction(originalMetricProgram({
  3: { exST: 0.18, straightTime: 7.8, turnTime: 4.7 }
}), null);
assert.ok(
  boat3StraightStrong.developmentScenarios.find((row) => row.scenarioName === "3号艇まくりシナリオ").upsetScore >
    boat3StraightWeak.developmentScenarios.find((row) => row.scenarioName === "3号艇まくりシナリオ").upsetScore,
  "straightTime + exST should lift 3/4 attack scenario scoring"
);
assert.ok(
  boat3StraightStrong.extraTickets.some((ticket) => ticket.combo.startsWith("3-1-") || ticket.combo.startsWith("3-4-")),
  "strong 3 straight/exST should create 3-head upset candidates"
);

const boat2TurnStrong = buildRacePrediction(originalMetricProgram({
  2: { turnTime: 4.2, lapTime: 18.18, exST: 0.08 },
  1: { turnTime: 4.72, lapTime: 18.62 }
}), null);
const boat2TurnWeak = buildRacePrediction(originalMetricProgram({
  2: { turnTime: 4.82, lapTime: 18.7, exST: 0.16 }
}), null);
assert.ok(
  boat2TurnStrong.developmentScenarios.find((row) => row.scenarioName === "2号艇差しシナリオ").upsetScore >
    boat2TurnWeak.developmentScenarios.find((row) => row.scenarioName === "2号艇差しシナリオ").upsetScore,
  "turnTime should lift sashi scenario scoring"
);

const boat4TurnStrong = buildRacePrediction(originalMetricProgram({
  3: { exST: 0.07, straightTime: 7.28 },
  4: { straightTime: 7.26, turnTime: 4.22, exST: 0.09 }
}), null);
assert.ok(
  boat4TurnStrong.developmentScenarios.find((row) => row.scenarioName === "4号艇まくり差しシナリオ").reasons.some((reason) => String(reason).includes("まわり足")),
  "turnTime should appear in makuri-sashi scenario reasons"
);

const missingOriginalPrediction = buildRacePrediction(program(), null);
assert.equal(missingOriginalPrediction.scoredBoats.length, 6);
assert.equal(missingOriginalPrediction.featureScores.allOriginalExhibitionTimesComplete, false);
assert.ok(
  buildConfidenceScore(missingOriginalPrediction).warnings.some((warning) => warning === "周回・直線・まわり足データ未取得のため、展示ST・展示タイム・モーター中心で予想"),
  "missing original exhibition values should not crash and should warn"
);

const noTendencyPrediction = buildRacePrediction(originalMetricProgram(), null);
const sashiTendencyPrediction = buildRacePrediction(originalMetricProgram({
  1: { playerTendency: { beatenBySashiRate: 0.34 } },
  2: { turnTime: 4.22, playerTendency: { sashiRate: 0.35 } }
}), null);
assert.ok(
  sashiTendencyPrediction.developmentScenarios.find((row) => row.scenarioName === "2号艇差しシナリオ").upsetScore >
    noTendencyPrediction.developmentScenarios.find((row) => row.scenarioName === "2号艇差しシナリオ").upsetScore,
  "boat 1 beatenBySashiRate and boat 2 sashiRate should lift the 2-sashi scenario"
);
assert.ok(
  sashiTendencyPrediction.extraTickets.some((ticket) => ticket.combo.startsWith("2-1-") || ticket.combo.startsWith("2-3-")),
  "strong 2-sashi tendency should generate 2-head upset tickets"
);

const makuriTendencyPrediction = buildRacePrediction(originalMetricProgram({
  1: { playerTendency: { beatenByMakuriRate: 0.28 } },
  2: { playerTendency: { lateStartRate: 0.24 } },
  3: { exST: 0.06, straightTime: 7.22, playerTendency: { makuriRate: 0.32 } },
  4: { exST: 0.08, straightTime: 7.28, playerTendency: { makuriRate: 0.24 } }
}), null);
assert.ok(
  makuriTendencyPrediction.developmentScenarios.find((row) => row.scenarioName === "3号艇まくりシナリオ").upsetScore >
    noTendencyPrediction.developmentScenarios.find((row) => row.scenarioName === "3号艇まくりシナリオ").upsetScore,
  "boat 1 beatenByMakuriRate should lift 3/4 makuri scenarios"
);
assert.ok(
  makuriTendencyPrediction.extraTickets.some((ticket) => ticket.combo.startsWith("3-1-") || ticket.combo.startsWith("3-4-")),
  "boat 3 makuriRate should generate 3-head upset tickets"
);

const okSampleSashiPrediction = buildRacePrediction(program({
  boatOverrides: {
    1: { playerTendency: { beatenBySashiRate: 0.6, courseSpecificLast6mRaceCount: 12, sampleStatus: "ok" } },
    2: { playerTendency: { sashiRate: 0.8, courseSpecificLast6mRaceCount: 12, sampleStatus: "ok" } }
  }
}), null);
const verySmallSashiPrediction = buildRacePrediction(program({
  boatOverrides: {
    1: { playerTendency: { beatenBySashiRate: 0.6, courseSpecificLast6mRaceCount: 1, sampleStatus: "very_small_sample" } },
    2: { playerTendency: { sashiRate: 0.8, courseSpecificLast6mRaceCount: 1, sampleStatus: "very_small_sample" } }
  }
}), null);
const smallSashiPrediction = buildRacePrediction(program({
  boatOverrides: {
    1: { playerTendency: { beatenBySashiRate: 0.6, courseSpecificLast6mRaceCount: 5, sampleStatus: "small_sample" } },
    2: { playerTendency: { sashiRate: 0.8, courseSpecificLast6mRaceCount: 5, sampleStatus: "small_sample" } }
  }
}), null);
const insufficientSashiPrediction = buildRacePrediction(program({
  boatOverrides: {
    1: { playerTendency: { beatenBySashiRate: 0.6, courseSpecificLast6mRaceCount: 0, sampleStatus: "insufficient_history" } },
    2: { playerTendency: { sashiRate: 0.8, courseSpecificLast6mRaceCount: 0, sampleStatus: "insufficient_history" } }
  }
}), null);
const scenarioScore = (prediction, name) =>
  prediction.developmentScenarios.find((row) => row.scenarioName === name)?.upsetScore ?? 0;
assert.ok(
  scenarioScore(okSampleSashiPrediction, "2号艇差しシナリオ") >
    scenarioScore(smallSashiPrediction, "2号艇差しシナリオ") &&
    scenarioScore(smallSashiPrediction, "2号艇差しシナリオ") >=
    scenarioScore(verySmallSashiPrediction, "2号艇差しシナリオ"),
  "tendency scenario weighting should decrease as sample size gets smaller"
);
assert.ok(
  scenarioScore(verySmallSashiPrediction, "2号艇差しシナリオ") >=
    scenarioScore(insufficientSashiPrediction, "2号艇差しシナリオ"),
  "very small samples may be a weak reference while insufficient samples must have no course-specific lift"
);
assert.equal(
  verySmallSashiPrediction.extraTickets.some((ticket) => ticket.combo.startsWith("2-")),
  false,
  "very small tendency samples must not independently generate strong 2-head upset tickets"
);
assert.ok(
  buildConfidenceScore(verySmallSashiPrediction).warnings.includes("直近6か月のコース別戦法データはサンプル不足のため、展示ST・展示タイム・周回・直線・まわり足を中心に評価しています。")
);
assert.ok(
  verySmallSashiPrediction.scenario.main.text.includes("直近6か月のコース別戦法データはサンプル不足")
);
assert.equal(
  insufficientSashiPrediction.scoredBoats.find((row) => row.boat === 2)?.scoreParts?.techniqueBoost,
  0,
  "insufficient course-specific tendency must not affect scoring"
);

const allCourseFallbackPrediction = buildRacePrediction(program({
  boatOverrides: {
    2: {
      playerTendency: {
        courseSpecificLast6mRaceCount: 0,
        allCourseLast6mRaceCount: 20,
        sampleStatus: "insufficient_history",
        allCourseWinRate: 0.7,
        allCourseSashiRate: 0.4
      }
    }
  }
}), null);
const allCourseFallbackBoost = allCourseFallbackPrediction.scoredBoats.find((row) => row.boat === 2)?.scoreParts?.allCourseReferenceBoost ?? 0;
assert.ok(allCourseFallbackBoost > 0 && allCourseFallbackBoost < 0.02, "all-course rates should remain a weak fallback reference");

const nullTendencyPrediction = buildRacePrediction(program({
  boatOverrides: Object.fromEntries(
    [1, 2, 3, 4, 5, 6].map((boat) => [boat, {
      playerTendency: {
        escapeRate: null,
        beatenBySashiRate: null,
        beatenByMakuriRate: null,
        sashiRate: null,
        makuriRate: null,
        makuriSashiRate: null
      }
    }])
  )
}), null);
assert.equal(nullTendencyPrediction.scoredBoats.length, 6);
assert.equal(nullTendencyPrediction.tendencySummary.available, false);
assert.ok(
  buildConfidenceScore(nullTendencyPrediction).warnings.includes("直近6か月の戦法データ未取得のため、展示・モーター中心で予想")
);

const raceFlowPrediction = buildRacePrediction(originalMetricProgram({
  3: { exST: 0.06, straightTime: 6.7, turnTime: 4.3 },
  4: { straightTime: 6.72, turnTime: 4.22 }
}), null);
assert.equal(raceFlowPrediction.raceFlowScenario.scenarios.length, 6);
assert.equal(raceFlowPrediction.raceFlowScenario.scenarioFamilies.length, 7);
assert.ok(raceFlowPrediction.raceFlowScenario.mainScenarioGroup);
assert.ok(Array.isArray(raceFlowPrediction.scenarioFamilyPreview));
assert.ok(raceFlowPrediction.wallScorePreview.length >= 3);
assert.ok(raceFlowPrediction.headPartnerSplitPreview.some((row) => row.boat === 4));
assert.ok(Number.isFinite(raceFlowPrediction.headPartnerSplitPreview.find((row) => row.boat === 4).attackerScore));
assert.ok(Number.isFinite(raceFlowPrediction.headPartnerSplitPreview.find((row) => row.boat === 4).beneficiaryScore));
assert.ok(Number.isFinite(raceFlowPrediction.headPartnerSplitPreview.find((row) => row.boat === 4).residualScore));
assert.ok(Array.isArray(raceFlowPrediction.venueNormalizedExhibitionMetrics));
assert.ok(raceFlowPrediction.venueNormalizedExhibitionMetrics.length >= 6);
const normalizedBoat3 = raceFlowPrediction.venueNormalizedExhibitionMetrics.find((row) => row.boat === 3);
assert.ok(normalizedBoat3.straightTime.rank >= 1);
assert.ok(Number.isFinite(normalizedBoat3.straightTime.deltaFromBest));
assert.ok(Number.isFinite(normalizedBoat3.straightTime.venueDayNormalizedScore));
assert.ok(Number.isFinite(normalizedBoat3.straightTime.percentile));
assert.ok(Array.isArray(raceFlowPrediction.ticketAdjustmentLog));

const boat4OpportunityRows = buildBoat4OpportunityRanking([
  originalMetricProgram({
    1: { lapTime: 18.74, turnTime: 4.78, racer_assigned_motor_top_2_percent: 22 },
    2: { exST: 0.19, turnTime: 4.72, racer_average_start_timing: 0.22, playerTendency: { lateStartRate: 0.24, sampleStatus: "ok", courseSpecificLast6mRaceCount: 12 } },
    3: { exST: 0.06, straightTime: 6.7, turnTime: 4.34, playerTendency: { makuriRate: 0.34, makuriSashiRate: 0.28, sampleStatus: "ok", courseSpecificLast6mRaceCount: 12 } },
    4: { exST: 0.08, straightTime: 6.69, turnTime: 4.2, racer_assigned_motor_top_2_percent: 55, playerTendency: { makuriSashiRate: 0.42, sampleStatus: "ok", courseSpecificLast6mRaceCount: 12 } }
  })
], {}, { limit: 1 });
assert.equal(boat4OpportunityRows.length, 1);
assert.ok(["高", "中"].includes(boat4OpportunityRows[0].strengthLabel));
assert.ok(boat4OpportunityRows[0].recommendedTickets.includes("4-1-flow"));
assert.ok(boat4OpportunityRows[0].recommendedTickets.includes("4-2-flow"));
assert.ok(boat4OpportunityRows[0].debug.threeAttackFourBenefitScore > 0);

const calmConditionPrediction = buildRacePrediction({
  ...originalMetricProgram(),
  conditions: { windDirection: null, windSpeed: 1, waveHeight: 1, weather: "sunny" }
}, null);
const strongWindPrediction = buildRacePrediction({
  ...originalMetricProgram(),
  conditions: { windDirection: "crosswind", windSpeed: 7, waveHeight: 1, weather: "sunny" }
}, null);
const calmConfidence = buildConfidenceScore(calmConditionPrediction);
const strongWindConfidence = buildConfidenceScore(strongWindPrediction);
assert.ok(
  strongWindConfidence.factors.raceFlowQualityAdjustment < calmConfidence.factors.raceFlowQualityAdjustment,
  "strong wind should reduce confidence quality adjustment slightly"
);
assert.ok(
  strongWindPrediction.conditionAdjustmentLog.some((row) => row.type === "wind" && row.level === "strong"),
  "strong wind should be recorded in the condition adjustment log"
);

console.log("kyotei-openapi-engine ok");
