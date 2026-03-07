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

export function analyzeVenueBias({ race, raceIndexes, ranking }) {
  const venue = getVenueAdjustments(race?.venueId);
  const innerMul = toNum(venue?.innerLaneMultiplier, 1);
  const chaosAdj = toNum(venue?.chaosAdjustment, 0);
  const areIndex = toNum(raceIndexes?.are_index, 50);

  const top3 = (Array.isArray(ranking) ? ranking : []).slice(0, 3);
  const topInnerCount = top3.filter((r) => toNum(r?.racer?.lane, 0) <= 3).length;

  const venue_inner_reliability = clamp(
    0,
    100,
    50 + (innerMul - 1) * 120 + topInnerCount * 5 - (venue?.isVolatileVenue ? 12 : 0)
  );
  const venue_chaos_factor = clamp(0, 100, 50 + chaosAdj * 2 + areIndex * 0.15 - (innerMul - 1) * 40);
  const venue_bias_score = clamp(
    0,
    100,
    50 + (venue_inner_reliability - 50) * 0.45 - (venue_chaos_factor - 50) * 0.35
  );

  let venue_style_bias = "balanced";
  if (venue_inner_reliability >= 62 && venue_chaos_factor <= 48) venue_style_bias = "inner";
  else if (venue_chaos_factor >= 60) venue_style_bias = "chaos";

  return {
    venue_bias_score: Number(venue_bias_score.toFixed(2)),
    venue_inner_reliability: Number(venue_inner_reliability.toFixed(2)),
    venue_chaos_factor: Number(venue_chaos_factor.toFixed(2)),
    venue_style_bias
  };
}

export function applyVenueBiasToStructure({ raceStructure, venueBias }) {
  const structure = { ...(raceStructure || {}) };
  const bias = venueBias || {};

  const head = toNum(structure.head_stability_score, 50);
  const top3 = toNum(structure.top3_concentration_score, 50);
  const chaos = toNum(structure.chaos_risk_score, 50);

  const headAdj = (toNum(bias.venue_inner_reliability, 50) - 50) * 0.28 + (toNum(bias.venue_bias_score, 50) - 50) * 0.12;
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
