const DEFAULT_LAYOUT_BIAS = {
  headRates: { 1: 0.5, 2: 0.16, 3: 0.12, 4: 0.1, 5: 0.07, 6: 0.05 },
  scenarioFollowerBias: {
    escape_1: { second: { 2: 0.34, 3: 0.25, 4: 0.18, 5: 0.13, 6: 0.1 } },
    makuri_3: { second: { 1: 0.34, 4: 0.28, 5: 0.18, 2: 0.12, 6: 0.08 } },
    four_beneficiary: { second: { 1: 0.32, 2: 0.22, 5: 0.2, 6: 0.14, 3: 0.12 } }
  },
  head4SecondBias: { 1: 0.32, 2: 0.22, 3: 0.12, 5: 0.2, 6: 0.14 },
  head3SecondBias: { 1: 0.34, 4: 0.28, 5: 0.18, 2: 0.12, 6: 0.08 },
  head2SecondBias: { 1: 0.42, 3: 0.22, 4: 0.17, 5: 0.12, 6: 0.07 }
};

const DEFAULT_PROFILE = {
  venueId: null,
  name: "unknown",
  waterType: "unknown",
  hasTideInfluence: false,
  layoutType: "standard",
  straightLengthType: "standard",
  turnDifficulty: "medium",
  insideAdvantage: 0.55,
  centerAttackAdvantage: 0.5,
  outsideFollowAdvantage: 0.45,
  roughWaterSensitivity: 0.5,
  venueLayoutBias: DEFAULT_LAYOUT_BIAS
};

const VENUE_PROFILE_OVERRIDES = {
  "24": {
    venueId: 24,
    name: "Omura",
    waterType: "seawater",
    hasTideInfluence: true,
    insideAdvantage: 0.58,
    centerAttackAdvantage: 0.52,
    outsideFollowAdvantage: 0.46,
    roughWaterSensitivity: 0.58
  }
};

function finiteNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeWaterType(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) return null;
  if (/sea|salt|ocean|海|汽水/.test(text)) return "seawater";
  if (/river|fresh|lake|淡水|川|湖/.test(text)) return "freshwater";
  return text;
}

function deepMerge(base = {}, override = {}) {
  const next = { ...(base || {}) };
  for (const [key, value] of Object.entries(override || {})) {
    if (value && typeof value === "object" && !Array.isArray(value) && next[key] && typeof next[key] === "object" && !Array.isArray(next[key])) {
      next[key] = deepMerge(next[key], value);
    } else {
      next[key] = value;
    }
  }
  return next;
}

export function getVenueProfile(venueId = null, conditions = {}) {
  const key = String(venueId ?? "");
  const override = VENUE_PROFILE_OVERRIDES[key] || {};
  const base = deepMerge(DEFAULT_PROFILE, override);
  const conditionWaterType = normalizeWaterType(conditions?.waterType ?? conditions?.water_type);
  const waterType = conditionWaterType || normalizeWaterType(base.waterType) || "unknown";
  const tideLevel = finiteNumber(conditions?.tideLevel ?? conditions?.tide_level, null);
  const tideDirection = conditions?.tideDirection ?? conditions?.tide_direction ?? null;
  const tidePhase = conditions?.tidePhase ?? conditions?.tide_phase ?? null;
  const hasTideSignal = tideLevel !== null || tideDirection !== null || tidePhase !== null;
  return {
    ...base,
    venueId: finiteNumber(venueId, base.venueId),
    waterType,
    hasTideInfluence: Boolean(base.hasTideInfluence || waterType === "seawater" || hasTideSignal),
    tideImpactKnown: hasTideSignal,
    tideLevel,
    tideDirection,
    tidePhase
  };
}

export { DEFAULT_PROFILE as DEFAULT_VENUE_PROFILE, DEFAULT_LAYOUT_BIAS, VENUE_PROFILE_OVERRIDES };
