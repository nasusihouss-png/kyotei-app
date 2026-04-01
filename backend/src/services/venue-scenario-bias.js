import { getVenueBiasProfile } from "../config/venue-bias-profiles.js";

function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 1) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(Number(value).toFixed(digits));
}

function weightedAverage(values = []) {
  const present = (Array.isArray(values) ? values : []).filter(
    (row) => Number.isFinite(Number(row?.value)) && Number.isFinite(Number(row?.weight)) && Number(row.weight) > 0
  );
  if (!present.length) return null;
  const total = present.reduce((sum, row) => sum + Number(row.weight), 0);
  return total > 0
    ? present.reduce((sum, row) => sum + Number(row.value) * Number(row.weight), 0) / total
    : null;
}

const VENUE_INNER_BIAS = Object.freeze({
  1: 0.63, 2: 0.64, 3: 0.51, 4: 0.58, 5: 0.62, 6: 0.64, 7: 0.71, 8: 0.67,
  9: 0.57, 10: 0.68, 11: 0.64, 12: 0.69, 13: 0.62, 14: 0.56, 15: 0.7, 16: 0.63,
  17: 0.61, 18: 0.7, 19: 0.73, 20: 0.67, 21: 0.68, 22: 0.64, 23: 0.66, 24: 0.76
});

function buildVenueScenarioProfile(venueId) {
  const numericVenueId = Number(venueId) || 0;
  const biasProfile = getVenueBiasProfile(numericVenueId);
  const insideBias = toNum(VENUE_INNER_BIAS[numericVenueId], 0.62);
  const oneCourseTrust = clamp(45, 84, insideBias * 100 + toNum(biasProfile.venue_escape_bias, 0) + toNum(biasProfile.venue_inside_stability, 0) * 0.5);
  const twoCourseSashiRemainRate = clamp(28, 70, 40 + (0.66 - insideBias) * 70 + toNum(biasProfile.venue_sashi_bias, 0) + toNum(biasProfile.venue_inside_stability, 0) * 0.18);
  const threeCourseAttackSuccessRate = clamp(24, 76, 44 + (0.62 - insideBias) * 82 + toNum(biasProfile.venue_makuri_bias, 0) + toNum(biasProfile.venue_start_importance, 0) * 0.35);
  const fourCourseDevelopSashiRate = clamp(22, 72, 39 + (0.61 - insideBias) * 78 + toNum(biasProfile.venue_makurizashi_bias, 0) + toNum(biasProfile.venue_entry_change_bias, 0) * 0.28);
  const outerRenyuuEntryRate = clamp(10, 52, 18 + (0.64 - insideBias) * 88 + toNum(biasProfile.venue_outer_3rd_bias, 0) + toNum(biasProfile.venue_entry_change_bias, 0) * 0.32);
  const escapeFailBoat3Attack = clamp(10, 74, 28 + (0.63 - insideBias) * 85 + toNum(biasProfile.venue_makuri_bias, 0) + toNum(biasProfile.venue_start_importance, 0) * 0.3);
  const escapeFailBoat4Develop = clamp(10, 72, 24 + (0.62 - insideBias) * 80 + toNum(biasProfile.venue_makurizashi_bias, 0) + toNum(biasProfile.venue_entry_change_bias, 0) * 0.34);
  const escapeFailOutsideIntrusion = clamp(8, 64, 16 + (0.64 - insideBias) * 90 + toNum(biasProfile.venue_outer_3rd_bias, 0) + toNum(biasProfile.venue_outside_break_risk, 0) * 0.42);
  const escapeFailTotal = clamp(
    8,
    74,
    weightedAverage([
      { value: escapeFailBoat3Attack, weight: 0.36 },
      { value: escapeFailBoat4Develop, weight: 0.3 },
      { value: escapeFailOutsideIntrusion, weight: 0.34 }
    ])
  );
  const nigeTendency = clamp(35, 92, oneCourseTrust + toNum(biasProfile.venue_escape_bias, 0) * 0.8 + toNum(biasProfile.venue_123_box_tightness, 0) * 0.3);
  const sashiTendency = clamp(24, 82, twoCourseSashiRemainRate + 4 + toNum(biasProfile.venue_sashi_bias, 0) * 0.7);
  const makuriTendency = clamp(20, 84, threeCourseAttackSuccessRate + 2 + toNum(biasProfile.venue_makuri_bias, 0) * 0.75);
  const makurisashiTendency = clamp(20, 84, fourCourseDevelopSashiRate + 3 + toNum(biasProfile.venue_makurizashi_bias, 0) * 0.75);
  const outsideTendency = clamp(10, 68, outerRenyuuEntryRate + 6 + toNum(biasProfile.venue_outer_3rd_bias, 0) * 0.6);
  const buyPolicy = Object.freeze({
    code: biasProfile.buy_policy_code || "balanced_standard",
    label: biasProfile.buy_policy_label || "Balanced Standard",
    focus: biasProfile.buy_policy_focus || "head balance",
    optional_formation_size:
      biasProfile.buy_policy_code === "inside_head_focus"
        ? "tight"
        : biasProfile.buy_policy_code === "attack_34_capture"
          ? "attack_34"
          : biasProfile.buy_policy_code === "wide_coverage_watch"
            ? "wide"
            : "balanced",
    head_bias:
      biasProfile.buy_policy_code === "inside_head_focus"
        ? "lane1"
        : biasProfile.buy_policy_code === "attack_34_capture"
          ? "lane3_lane4_attack"
          : biasProfile.buy_policy_code === "wide_coverage_watch"
            ? "spread_with_outer_3rd"
            : "balanced",
    notes:
      Array.isArray(biasProfile.venue_adjustment_reason) && biasProfile.venue_adjustment_reason.length > 0
        ? biasProfile.venue_adjustment_reason
        : []
  });

  return Object.freeze({
    venue_id: numericVenueId || null,
    venue_name: biasProfile.venue_name || null,
    inside_bias: round(insideBias, 3),
    one_course_trust: round(oneCourseTrust, 1),
    two_course_sashi_remain_rate: round(twoCourseSashiRemainRate, 1),
    three_course_attack_success_rate: round(threeCourseAttackSuccessRate, 1),
    four_course_develop_sashi_rate: round(fourCourseDevelopSashiRate, 1),
    outer_renyuu_entry_rate: round(outerRenyuuEntryRate, 1),
    lane56_renyuu_intrusion_rate: round(outerRenyuuEntryRate, 1),
    venue_bias_profile: biasProfile,
    venue_escape_bias: round(toNum(biasProfile.venue_escape_bias, 0), 1),
    venue_sashi_bias: round(toNum(biasProfile.venue_sashi_bias, 0), 1),
    venue_makuri_bias: round(toNum(biasProfile.venue_makuri_bias, 0), 1),
    venue_makurizashi_bias: round(toNum(biasProfile.venue_makurizashi_bias, 0), 1),
    venue_outer_3rd_bias: round(toNum(biasProfile.venue_outer_3rd_bias, 0), 1),
    venue_entry_change_bias: round(toNum(biasProfile.venue_entry_change_bias, 0), 1),
    venue_start_importance: round(toNum(biasProfile.venue_start_importance, 0), 1),
    venue_inside_stability: round(toNum(biasProfile.venue_inside_stability, 0), 1),
    venue_outside_break_risk: round(toNum(biasProfile.venue_outside_break_risk, 0), 1),
    venue_123_box_tightness: round(toNum(biasProfile.venue_123_box_tightness, 0), 1),
    lane1_head_boost: round(toNum(biasProfile.lane1_head_boost, 0), 1),
    lane1_motor_st_synergy_boost: round(toNum(biasProfile.lane1_motor_st_synergy_boost, 0), 1),
    lane56_head_penalty: round(toNum(biasProfile.lane56_head_penalty, 0), 1),
    lane2_second_boost: round(toNum(biasProfile.lane2_second_boost, 0), 1),
    lane3_second_boost: round(toNum(biasProfile.lane3_second_boost, 0), 1),
    lane3_attack_boost: round(toNum(biasProfile.lane3_attack_boost, 0), 1),
    lane4_develop_boost: round(toNum(biasProfile.lane4_develop_boost, 0), 1),
    volatility_boost: round(toNum(biasProfile.volatility_boost, 0), 1),
    optional_formation_trigger_boost: round(toNum(biasProfile.optional_formation_trigger_boost, 0), 1),
    venueBiasProfile: biasProfile,
    buyPolicy,
    venueAdjustmentReason:
      Array.isArray(biasProfile.venue_adjustment_reason) && biasProfile.venue_adjustment_reason.length > 0
        ? [...biasProfile.venue_adjustment_reason]
        : [],
    kimarite_tendency: Object.freeze({
      nige: round(nigeTendency, 1),
      sashi: round(sashiTendency, 1),
      makuri: round(makuriTendency, 1),
      makurisashi: round(makurisashiTendency, 1),
      outside_entry: round(outsideTendency, 1),
      stable_hold: round(clamp(18, 78, (oneCourseTrust + twoCourseSashiRemainRate + fourCourseDevelopSashiRate) / 3), 1)
    }),
    escape_fail_pattern: Object.freeze({
      boat3_attack_risk: round(escapeFailBoat3Attack, 1),
      boat4_develop_risk: round(escapeFailBoat4Develop, 1),
      outside_intrusion_risk: round(escapeFailOutsideIntrusion, 1),
      total_risk: round(escapeFailTotal, 1)
    })
  });
}

export function getVenueScenarioContext(venueId) {
  return buildVenueScenarioProfile(venueId);
}

export function getVenueLaneBiasScore(venueContext = {}, lane) {
  if (lane === 1) return toNum(venueContext?.one_course_trust, 62);
  if (lane === 2) return toNum(venueContext?.two_course_sashi_remain_rate, 44);
  if (lane === 3) return toNum(venueContext?.three_course_attack_success_rate, 42);
  if (lane === 4) return toNum(venueContext?.four_course_develop_sashi_rate, 40);
  return clamp(
    10,
    56,
    toNum(venueContext?.outer_renyuu_entry_rate, 24) +
      toNum(venueContext?.venue_outer_3rd_bias, 0) * 0.45
  );
}

export function getVenueStyleMatchScore(venueContext = {}, lane, styleCode = "") {
  const kimarite = venueContext?.kimarite_tendency || {};
  const laneBias = getVenueLaneBiasScore(venueContext, lane);
  const mappedStyle =
    styleCode ||
    (lane === 1
      ? "nige"
      : lane === 2
        ? "sashi"
        : lane === 3
          ? "makuri"
          : lane === 4
            ? "makurisashi"
            : "outside_entry");
  return weightedAverage([
    { value: laneBias, weight: 0.52 },
    { value: toNum(kimarite[mappedStyle], laneBias), weight: 0.34 },
    { value: clamp(20, 80, 50 + toNum(venueContext?.venue_start_importance, 0) * (mappedStyle === "makuri" ? 2.4 : mappedStyle === "makurisashi" ? 1.8 : 0.6)), weight: 0.14 }
  ]);
}

export function getVenueEscapeFailPressure(venueContext = {}, lane) {
  const fail = venueContext?.escape_fail_pattern || {};
  if (lane === 1) return toNum(fail.total_risk, 28);
  if (lane === 3) return toNum(fail.boat3_attack_risk, 30);
  if (lane === 4) return toNum(fail.boat4_develop_risk, 26);
  if (lane >= 5) return toNum(fail.outside_intrusion_risk, 22);
  return weightedAverage([
    { value: toNum(fail.boat3_attack_risk, 30), weight: 0.46 },
    { value: toNum(fail.boat4_develop_risk, 26), weight: 0.34 },
    { value: toNum(fail.outside_intrusion_risk, 22), weight: 0.2 }
  ]);
}

export function getVenueBuyPolicy(venueContext = {}) {
  return venueContext?.buyPolicy || venueContext?.buy_policy || null;
}

export function getVenueAdjustmentReason(venueContext = {}) {
  return Array.isArray(venueContext?.venueAdjustmentReason)
    ? venueContext.venueAdjustmentReason
    : Array.isArray(venueContext?.venue_adjustment_reason)
      ? venueContext.venue_adjustment_reason
      : [];
}

export { VENUE_INNER_BIAS };
