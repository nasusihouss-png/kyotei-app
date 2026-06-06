export const DEFAULT_SCORING_COEFFICIENTS = {
  headScore: {
    exST: 18,
    exTime: 8,
    lapTime: 10,
    straightTime: 12,
    turnTime: 12,
    motorRank: 16,
    motor2Rate: 8,
    racerTendency: 10,
    venueBias: 10,
    weatherCondition: 6,
    wallScore: 8,
    attackTriggerScore: 8
  },
  partnerResidualScore: {
    lapTime: 18,
    turnTime: 18,
    motorRank: 18,
    motor2Rate: 8,
    straightTime: 10,
    exTime: 6,
    exST: 5,
    racerTendency: 12,
    venueBias: 10,
    insideResidual: 10,
    weatherCondition: 8
  },
  fourHeadOpportunity: {
    boat3AttackTrigger: 18,
    boat2SashiFailure: 14,
    boat1CollapseRisk: 14,
    boat4TurnTime: 16,
    boat4StraightTime: 12,
    boat4MotorRank: 16,
    boat4MakuriSashiTendency: 10,
    venue4HeadBias: 10,
    conditionAdjustment: 6
  },
  scenarioScores: {
    escape_1: {
      exST: 16,
      lapTime: 16,
      turnTime: 18,
      motorRank: 14,
      racerTendency: 14,
      wallScore: 12,
      venueBias: 10
    },
    sashi_2: {
      exST: 16,
      turnTime: 22,
      lapTime: 10,
      motorRank: 12,
      racerTendency: 18,
      venueBias: 10,
      boat1Weakness: 12
    },
    makuri_3: {
      exST: 22,
      straightTime: 22,
      motorRank: 14,
      racerTendency: 16,
      wallWeakness: 16,
      venueBias: 10
    },
    makuri_sashi_3: {
      exST: 14,
      straightTime: 14,
      turnTime: 24,
      motorRank: 14,
      racerTendency: 16,
      wallWeakness: 10,
      venueBias: 8
    },
    four_beneficiary_head: {
      boat3AttackTrigger: 18,
      boat1CollapseRisk: 16,
      turnTime: 18,
      straightTime: 14,
      motorRank: 16,
      racerTendency: 10,
      venueBias: 8
    },
    outside_follow_5_6: {
      lapTime: 18,
      straightTime: 18,
      turnTime: 12,
      motorRank: 18,
      flowCollapse: 18,
      venueBias: 8
    }
  }
};

export const DEFAULT_SCORING_CONFIG = {
  shrinkK: 24,
  baseWeights: {
    laneBias: 0.28,
    class: 0.2,
    nationalWinRatePoint: 0.34,
    localWinRatePoint: 0.16,
    motor2Rate: 0.13,
    boat2Rate: 0.05,
    averageStartTiming: 0.16,
    flyingPenalty: 0.08,
    latePenalty: 0.04
  },
  exhibitionWeights: {
    exhibitionTimeZ: 0.18,
    exhibitionStartTiming: 0.08,
    entryCourse: 0.16,
    windBias: 0.04,
    makuriAlert: 0.1
  },
  originalExhibitionWeights: {
    roleFeatureBoost: 0.26,
    outsideFirstPlaceDampening: 0.42
  },
  professionalFactorWeights: {
    motorRankHead: 0.18,
    motorRankResidual: 0.12,
    startReliabilityHead: 0.11,
    earlyStartAttack: 0.05,
    fRiskHeadPenalty: 0.08,
    exhibitionHistoryContradictionPenalty: 0.06
  },
  scoringCoefficients: DEFAULT_SCORING_COEFFICIENTS,
  headScoreWeights: DEFAULT_SCORING_COEFFICIENTS.headScore,
  partnerScoreWeights: DEFAULT_SCORING_COEFFICIENTS.partnerResidualScore,
  residualScoreWeights: DEFAULT_SCORING_COEFFICIENTS.partnerResidualScore,
  beneficiaryScoreWeights: DEFAULT_SCORING_COEFFICIENTS.partnerResidualScore,
  fourHeadOpportunityWeights: DEFAULT_SCORING_COEFFICIENTS.fourHeadOpportunity,
  venueBiasWeights: {
    head: 10,
    partner: 10,
    decisionConditioned: 12,
    headConditionedCombo: 12
  },
  conditionWeights: {
    windStabilityPenalty: 6,
    waveTurnStability: 8,
    tailwindCenterAttack: 5,
    roughWaterOutsideHeadPenalty: 7
  },
  ticketGateThresholds: {
    mainScore: 72,
    secondaryScore: 60,
    upsetScore: 48,
    minHeadSupport: 0.42,
    minScenarioSupport: 0.36,
    minPartnerSupport: 0.38,
    minThirdSupport: 0.34,
    outsideHeadMinSupport: 0.66
  },
  buyDecisionThresholds: {
    buyConfidence: 58,
    lightConfidence: 45,
    passConfidence: 40,
    scenarioCloseGap: 0.035,
    minMainTicketsForBuy: 1
  },
  venueScoringOverrides: {
    "24": {
      headScore: {
        venueBias: 0,
        outsideHeadPenalty: 1
      },
      headScoreWeights: {
        venueBias: 0,
        outsideHeadPenalty: 1
      },
      fourHeadOpportunity: {
        venue4HeadBias: 0
      },
      fourHeadOpportunityWeights: {
        venue4HeadBias: 0
      },
      ticketGateThresholds: {
        outsideHeadMinSupport: 0.68
      }
    }
  },
  screeningWeights: {
    boat1Strength: 0.26,
    startTrust: 0.16,
    courseEscape: 0.18,
    localFit: 0.1,
    venueInsideAdvantage: 0.14,
    weakWall: 0.12,
    wind: 0.04
  },
  makuriAlertSeconds: 0.07,
  insideCandidateThreshold: 62
};

function isPlainObject(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

export function deepMerge(base = {}, override = {}) {
  const next = { ...(base || {}) };
  for (const [key, value] of Object.entries(override || {})) {
    if (isPlainObject(value) && isPlainObject(next[key])) {
      next[key] = deepMerge(next[key], value);
    } else if (isPlainObject(value)) {
      next[key] = deepMerge({}, value);
    } else {
      next[key] = value;
    }
  }
  return next;
}

function addNumericDeltas(base = {}, deltas = {}) {
  const next = deepMerge({}, base);
  for (const [key, value] of Object.entries(deltas || {})) {
    if (isPlainObject(value)) {
      next[key] = addNumericDeltas(isPlainObject(next[key]) ? next[key] : {}, value);
    } else if (typeof value === "number" && typeof next[key] === "number") {
      next[key] += value;
    } else {
      next[key] = value;
    }
  }
  return next;
}

export function mergeScoringConfig(config = {}) {
  const merged = deepMerge(DEFAULT_SCORING_CONFIG, config || {});
  const aliases = {
    headScoreWeights: "headScore",
    partnerScoreWeights: "partnerResidualScore",
    residualScoreWeights: "partnerResidualScore",
    beneficiaryScoreWeights: "partnerResidualScore",
    fourHeadOpportunityWeights: "fourHeadOpportunity"
  };
  for (const [alias, target] of Object.entries(aliases)) {
    if (isPlainObject(config?.[alias])) {
      merged.scoringCoefficients[target] = deepMerge(merged.scoringCoefficients[target] || {}, config[alias]);
    }
  }
  merged.headScoreWeights = merged.scoringCoefficients.headScore;
  merged.partnerScoreWeights = merged.scoringCoefficients.partnerResidualScore;
  merged.residualScoreWeights = merged.scoringCoefficients.partnerResidualScore;
  merged.beneficiaryScoreWeights = merged.scoringCoefficients.partnerResidualScore;
  merged.fourHeadOpportunityWeights = merged.scoringCoefficients.fourHeadOpportunity;
  return merged;
}

export function getVenueScoringConfig(config = DEFAULT_SCORING_CONFIG, venueId = null) {
  const merged = mergeScoringConfig(config);
  const key = String(venueId ?? merged.stadiumNumber ?? "");
  const override = merged.venueScoringOverrides?.[key];
  if (!override) return merged;
  const next = deepMerge({}, merged);
  const headOverride = addNumericDeltas(override.headScoreWeights || {}, override.headScore || {});
  const partnerOverride = addNumericDeltas(
    addNumericDeltas(override.partnerScoreWeights || {}, override.residualScoreWeights || {}),
    override.partnerResidualScore || {}
  );
  const fourHeadOverride = addNumericDeltas(override.fourHeadOpportunityWeights || {}, override.fourHeadOpportunity || {});
  next.scoringCoefficients = {
    ...next.scoringCoefficients,
    headScore: addNumericDeltas(next.scoringCoefficients?.headScore || {}, headOverride),
    partnerResidualScore: addNumericDeltas(next.scoringCoefficients?.partnerResidualScore || {}, partnerOverride),
    fourHeadOpportunity: addNumericDeltas(next.scoringCoefficients?.fourHeadOpportunity || {}, fourHeadOverride),
    scenarioScores: addNumericDeltas(next.scoringCoefficients?.scenarioScores || {}, override.scenarioScores || {})
  };
  next.headScoreWeights = next.scoringCoefficients.headScore;
  next.partnerScoreWeights = next.scoringCoefficients.partnerResidualScore;
  next.residualScoreWeights = next.scoringCoefficients.partnerResidualScore;
  next.beneficiaryScoreWeights = next.scoringCoefficients.partnerResidualScore;
  next.fourHeadOpportunityWeights = next.scoringCoefficients.fourHeadOpportunity;
  next.ticketGateThresholds = deepMerge(next.ticketGateThresholds || {}, override.ticketGateThresholds || {});
  next.buyDecisionThresholds = deepMerge(next.buyDecisionThresholds || {}, override.buyDecisionThresholds || {});
  next.venueOverrideApplied = { venueId: key, override };
  return next;
}

export function normalizeWeightSet(weights = {}) {
  const entries = Object.entries(weights || {}).filter(([, value]) => Number.isFinite(Number(value)) && Number(value) !== 0);
  const total = entries.reduce((sum, [, value]) => sum + Math.abs(Number(value)), 0);
  if (total <= 0) return {};
  return Object.fromEntries(entries.map(([key, value]) => [key, Number(value) / total]));
}

export function weightedAverageFromWeights(values = {}, weights = {}, fallback = null) {
  let weighted = 0;
  let total = 0;
  const used = {};
  for (const [key, weight] of Object.entries(weights || {})) {
    const value = values[key];
    const n = Number(value);
    const w = Number(weight);
    if (!Number.isFinite(n) || !Number.isFinite(w) || w === 0) continue;
    weighted += n * w;
    total += Math.abs(w);
    used[key] = { value: n, weight: w, contribution: n * w };
  }
  return {
    score: total > 0 ? weighted / total : fallback,
    used,
    weightTotal: total
  };
}

export function motorStrengthLabel(percentile) {
  const n = Number(percentile);
  if (!Number.isFinite(n)) return "average";
  if (n >= 0.82) return "top";
  if (n >= 0.62) return "above_average";
  if (n >= 0.38) return "average";
  if (n >= 0.18) return "below_average";
  return "weak";
}
