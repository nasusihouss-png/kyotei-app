import assert from "node:assert/strict";

import {
  DEFAULT_SCORING_CONFIG,
  getVenueScoringConfig,
  mergeScoringConfig,
  motorStrengthLabel,
  weightedAverageFromWeights
} from "./scoring-config.js";

assert.equal(DEFAULT_SCORING_CONFIG.scoringCoefficients.headScore.exST, 18);
assert.equal(DEFAULT_SCORING_CONFIG.scoringCoefficients.partnerResidualScore.motorRank, 18);
assert.equal(DEFAULT_SCORING_CONFIG.scoringCoefficients.fourHeadOpportunity.boat4MotorRank, 16);

const merged = mergeScoringConfig({
  scoringCoefficients: {
    headScore: { exST: 21 }
  }
});
assert.equal(merged.scoringCoefficients.headScore.exST, 21);
assert.equal(merged.scoringCoefficients.headScore.turnTime, 12);

const venueAdjusted = getVenueScoringConfig({
  ...DEFAULT_SCORING_CONFIG,
  venueScoringOverrides: {
    "24": {
      headScore: { venueBias: 2 },
      fourHeadOpportunity: { venue4HeadBias: 3 }
    }
  }
}, 24);
assert.equal(venueAdjusted.scoringCoefficients.headScore.venueBias, 12);
assert.equal(venueAdjusted.scoringCoefficients.fourHeadOpportunity.venue4HeadBias, 13);

const weighted = weightedAverageFromWeights({ a: 1, b: 0 }, { a: 3, b: 1 }, 0.5);
assert.equal(weighted.score, 0.75);
assert.equal(motorStrengthLabel(0.9), "top");
assert.equal(motorStrengthLabel(0.1), "weak");

console.log("scoring-config ok");
