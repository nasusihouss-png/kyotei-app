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

const VENUE_OVERRIDES = Object.freeze({
  3: { lane2: -4, lane3: 8, lane4: 7, outer: 8, boat3Attack: 8, boat4Develop: 7, outsideIntrusion: 9, makuri: 7, makurisashi: 5 },
  7: { lane1: 7, lane2: 4, lane3: -5, lane4: -4, outer: -6, escapeFail: -8, nige: 8, sashi: 4 },
  10: { lane1: 4, lane2: 3, lane3: -2, lane4: -1, outer: -2, nige: 5, sashi: 3 },
  12: { lane1: 5, lane2: 2, lane3: -3, lane4: -1, outer: -3, nige: 6, sashi: 2 },
  14: { lane1: -4, lane2: 2, lane3: 5, lane4: 6, outer: 5, boat3Attack: 5, boat4Develop: 7, makuri: 4, makurisashi: 6 },
  15: { lane1: 6, lane2: 2, lane3: -3, lane4: -2, outer: -4, escapeFail: -5, nige: 7 },
  18: { lane1: 5, lane2: 1, lane3: -3, lane4: -3, outer: -4, nige: 5 },
  19: { lane1: 8, lane2: 3, lane3: -6, lane4: -5, outer: -8, escapeFail: -9, nige: 9, sashi: 3 },
  21: { lane1: 5, lane2: 2, lane3: -2, lane4: -2, outer: -3, nige: 5 },
  24: { lane1: 10, lane2: 5, lane3: -8, lane4: -7, outer: -10, escapeFail: -11, nige: 11, sashi: 5 }
});

function buildVenueScenarioProfile(venueId) {
  const numericVenueId = Number(venueId) || 0;
  const insideBias = toNum(VENUE_INNER_BIAS[numericVenueId], 0.62);
  const override = VENUE_OVERRIDES[numericVenueId] || {};
  const oneCourseTrust = clamp(45, 82, insideBias * 100 + toNum(override.lane1, 0));
  const twoCourseSashiRemainRate = clamp(28, 68, 40 + (0.66 - insideBias) * 70 + toNum(override.lane2, 0));
  const threeCourseAttackSuccessRate = clamp(24, 72, 44 + (0.62 - insideBias) * 82 + toNum(override.lane3, 0));
  const fourCourseDevelopSashiRate = clamp(22, 68, 39 + (0.61 - insideBias) * 78 + toNum(override.lane4, 0));
  const outerRenyuuEntryRate = clamp(10, 48, 18 + (0.64 - insideBias) * 88 + toNum(override.outer, 0));
  const escapeFailBoat3Attack = clamp(10, 72, 28 + (0.63 - insideBias) * 85 + toNum(override.boat3Attack, 0));
  const escapeFailBoat4Develop = clamp(10, 70, 24 + (0.62 - insideBias) * 80 + toNum(override.boat4Develop, 0));
  const escapeFailOutsideIntrusion = clamp(8, 62, 16 + (0.64 - insideBias) * 90 + toNum(override.outsideIntrusion, 0));
  const escapeFailTotal = clamp(
    8,
    72,
    weightedAverage([
      { value: escapeFailBoat3Attack, weight: 0.38 },
      { value: escapeFailBoat4Develop, weight: 0.28 },
      { value: escapeFailOutsideIntrusion, weight: 0.34 }
    ]) + toNum(override.escapeFail, 0)
  );
  const nigeTendency = clamp(35, 88, oneCourseTrust + toNum(override.nige, 0));
  const sashiTendency = clamp(24, 76, twoCourseSashiRemainRate + 4 + toNum(override.sashi, 0));
  const makuriTendency = clamp(20, 78, threeCourseAttackSuccessRate + 2 + toNum(override.makuri, 0));
  const makurisashiTendency = clamp(20, 76, fourCourseDevelopSashiRate + 3 + toNum(override.makurisashi, 0));
  const outsideTendency = clamp(10, 62, outerRenyuuEntryRate + 6 + toNum(override.outside, 0));

  return Object.freeze({
    venue_id: numericVenueId || null,
    inside_bias: round(insideBias, 3),
    one_course_trust: round(oneCourseTrust, 1),
    two_course_sashi_remain_rate: round(twoCourseSashiRemainRate, 1),
    three_course_attack_success_rate: round(threeCourseAttackSuccessRate, 1),
    four_course_develop_sashi_rate: round(fourCourseDevelopSashiRate, 1),
    outer_renyuu_entry_rate: round(outerRenyuuEntryRate, 1),
    lane56_renyuu_intrusion_rate: round(outerRenyuuEntryRate, 1),
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
  return toNum(venueContext?.outer_renyuu_entry_rate, 24);
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
    { value: laneBias, weight: 0.58 },
    { value: toNum(kimarite[mappedStyle], laneBias), weight: 0.42 }
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

export { VENUE_INNER_BIAS };
