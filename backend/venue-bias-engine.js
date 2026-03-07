import { getVenueAdjustments } from "./venue-adjustment-engine.js";

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function thresholdsByMode(modeRaw) {
  const mode = String(modeRaw || "active").trim().toLowerCase();
  if (mode === "conservative") return { skip: 97, micro: 90, small: 60 };
  if (mode === "standard") return { skip: 97, micro: 90, small: 60 };
  return { skip: 97, micro: 90, small: 60 };
}

function recommendationFromRisk(riskScore, modeRaw) {
  const t = thresholdsByMode(modeRaw);
  if (riskScore > t.skip) return "SKIP";
  if (riskScore > t.micro) return "MICRO BET";
  if (riskScore > t.small) return "SMALL BET";
  return "FULL BET";
}

const VENUE_PROFILE_TABLE = {
  default: {
    style: "balanced",
    innerBias: 0,
    chaosBias: 0,
    summaryJa: "標準的な水面傾向"
  },
  inner_strong: {
    style: "inner",
    innerBias: 6,
    chaosBias: -6,
    summaryJa: "イン有利傾向"
  },
  chaos_prone: {
    style: "chaos",
    innerBias: -4,
    chaosBias: 7,
    summaryJa: "波乱傾向が強め"
  },
  mixed_outer: {
    style: "mixed",
    innerBias: -1,
    chaosBias: 3,
    summaryJa: "差し・まくり混在傾向"
  }
};

const VENUE_TO_PROFILE = {
  // stable inner
  2: "inner_strong",
  4: "inner_strong",
  10: "inner_strong",
  11: "inner_strong",
  14: "inner_strong",
  16: "inner_strong",
  18: "inner_strong",
  22: "inner_strong",
  // volatile
  3: "chaos_prone",
  5: "chaos_prone",
  9: "chaos_prone",
  13: "chaos_prone",
  17: "chaos_prone",
  20: "chaos_prone",
  21: "chaos_prone",
  24: "chaos_prone",
  // mixed
  1: "mixed_outer",
  6: "mixed_outer",
  7: "mixed_outer",
  8: "mixed_outer",
  12: "mixed_outer",
  15: "mixed_outer",
  19: "mixed_outer",
  23: "mixed_outer"
};

function getProfileByVenue(venueId) {
  const key = VENUE_TO_PROFILE[toNum(venueId, 0)] || "default";
  return VENUE_PROFILE_TABLE[key] || VENUE_PROFILE_TABLE.default;
}

function makeSummary({
  venueName,
  venue_style_bias,
  venue_inner_reliability,
  venue_chaos_factor
}) {
  const styleText =
    venue_style_bias === "inner"
      ? "イン重視"
      : venue_style_bias === "chaos"
        ? "波乱注意"
        : venue_style_bias === "mixed"
          ? "差し・まくり警戒"
          : "バランス型";
  const innerText =
    venue_inner_reliability >= 60 ? "イン信頼やや高め" : venue_inner_reliability <= 45 ? "イン信頼低め" : "イン信頼標準";
  const chaosText =
    venue_chaos_factor >= 60 ? "荒れ度高め" : venue_chaos_factor <= 42 ? "荒れ度低め" : "荒れ度標準";
  return `${venueName || "この場"}: ${styleText} / ${innerText} / ${chaosText}`;
}

export function analyzeVenueBias({ race, raceIndexes, ranking }) {
  const venue = getVenueAdjustments(race?.venueId);
  const venueProfile = getProfileByVenue(race?.venueId);

  const innerMul = toNum(venue?.innerLaneMultiplier, 1);
  const chaosAdj = toNum(venue?.chaosAdjustment, 0);
  const areIndex = toNum(raceIndexes?.are_index, 50);

  const top3 = (Array.isArray(ranking) ? ranking : []).slice(0, 3);
  const topInnerCount = top3.filter((r) => toNum(r?.racer?.lane, 0) <= 3).length;

  const venue_inner_reliability = clamp(
    0,
    100,
    50 +
      (innerMul - 1) * 120 +
      topInnerCount * 5 -
      (venue?.isVolatileVenue ? 12 : 0) +
      toNum(venueProfile.innerBias, 0)
  );
  const venue_chaos_factor = clamp(
    0,
    100,
    50 +
      chaosAdj * 2 +
      areIndex * 0.15 -
      (innerMul - 1) * 40 +
      toNum(venueProfile.chaosBias, 0)
  );
  const venue_bias_score = clamp(
    0,
    100,
    50 + (venue_inner_reliability - 50) * 0.45 - (venue_chaos_factor - 50) * 0.35
  );

  let venue_style_bias = venueProfile.style || "balanced";
  if (venue_inner_reliability >= 62 && venue_chaos_factor <= 48) venue_style_bias = "inner";
  else if (venue_chaos_factor >= 60) venue_style_bias = "chaos";
  else if (venue_style_bias !== "inner" && venue_style_bias !== "chaos") venue_style_bias = "mixed";

  const summary = makeSummary({
    venueName: race?.venueName,
    venue_style_bias,
    venue_inner_reliability,
    venue_chaos_factor
  });

  return {
    venue_bias_score: Number(venue_bias_score.toFixed(2)),
    venue_inner_reliability: Number(venue_inner_reliability.toFixed(2)),
    venue_chaos_factor: Number(venue_chaos_factor.toFixed(2)),
    venue_style_bias,
    summary
  };
}

export function applyVenueBiasToStructure({ raceStructure, venueBias }) {
  const structure = { ...(raceStructure || {}) };
  const bias = venueBias || {};

  const head = toNum(structure.head_stability_score, 50);
  const top3 = toNum(structure.top3_concentration_score, 50);
  const chaos = toNum(structure.chaos_risk_score, 50);

  const headAdj =
    (toNum(bias.venue_inner_reliability, 50) - 50) * 0.28 + (toNum(bias.venue_bias_score, 50) - 50) * 0.12;
  const chaosAdj = (toNum(bias.venue_chaos_factor, 50) - 50) * 0.35;

  const head_stability_score = clamp(0, 100, head + headAdj);
  const chaos_risk_score = clamp(0, 100, chaos + chaosAdj);
  const race_structure_score = clamp(
    0,
    100,
    head_stability_score * 0.4 + top3 * 0.35 + (100 - chaos_risk_score) * 0.25
  );

  return {
    ...structure,
    head_stability_score: Number(head_stability_score.toFixed(2)),
    chaos_risk_score: Number(chaos_risk_score.toFixed(2)),
    race_structure_score: Number(race_structure_score.toFixed(2))
  };
}

export function applyVenueBiasToRisk({ raceRisk, venueBias }) {
  const risk = { ...(raceRisk || {}) };
  const bias = venueBias || {};

  const currentRisk = toNum(risk.risk_score, 50);
  const chaosPush = (toNum(bias.venue_chaos_factor, 50) - 50) * 0.22;
  const innerRelief = (toNum(bias.venue_inner_reliability, 50) - 50) * 0.18;
  const biasPull = (toNum(bias.venue_bias_score, 50) - 50) * 0.08;
  const adjustedRisk = clamp(0, 100, currentRisk + chaosPush - innerRelief - biasPull);

  const participationMode = risk.participation_mode || "active";
  return {
    ...risk,
    risk_score: Number(adjustedRisk.toFixed(0)),
    recommendation: recommendationFromRisk(adjustedRisk, participationMode)
  };
}
