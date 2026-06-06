const DECISIONS = ["makuri", "makuriSashi", "sashi", "escape"];
const BOATS = [1, 2, 3, 4, 5, 6];

const VENUE_BIAS_GROUPS = {
  inner: new Set([2, 4, 10, 11, 14, 16, 18, 22]),
  attack: new Set([3, 5, 9, 13, 17, 20, 21, 24]),
  mixed: new Set([1, 6, 7, 8, 12, 15, 19, 23])
};

const VENUE_BIAS_TEMPLATES = {
  inner: {
    headRates: { 1: 0.58, 2: 0.16, 3: 0.11, 4: 0.08, 5: 0.05, 6: 0.02 },
    scenarioFollowerBias: {
      makuri: { insideResidualRate: 0.62, outsideLinkedRate: 0.38, boat1SecondRate: 0.48 },
      makuriSashi: { insideResidualRate: 0.66, outsideLinkedRate: 0.34, boat1SecondRate: 0.52 },
      sashi: { insideResidualRate: 0.72, outsideLinkedRate: 0.28, boat1SecondRate: 0.62 }
    },
    head4SecondBias: { 1: 0.44, 2: 0.24, 3: 0.12, 5: 0.14, 6: 0.06 },
    head3SecondBias: { 1: 0.46, 2: 0.18, 4: 0.22, 5: 0.1, 6: 0.04 },
    head2SecondBias: { 1: 0.58, 3: 0.18, 4: 0.12, 5: 0.08, 6: 0.04 },
    weights: { insideResidual: 1.12, straight: 0.92, turn: 1.08, fourBeneficiary: 0.9, outsideFollow: 0.82 }
  },
  attack: {
    headRates: { 1: 0.47, 2: 0.17, 3: 0.15, 4: 0.12, 5: 0.07, 6: 0.02 },
    scenarioFollowerBias: {
      makuri: { insideResidualRate: 0.38, outsideLinkedRate: 0.62, boat1SecondRate: 0.28 },
      makuriSashi: { insideResidualRate: 0.42, outsideLinkedRate: 0.58, boat1SecondRate: 0.34 },
      sashi: { insideResidualRate: 0.58, outsideLinkedRate: 0.42, boat1SecondRate: 0.48 }
    },
    head4SecondBias: { 1: 0.25, 2: 0.18, 3: 0.12, 5: 0.29, 6: 0.16 },
    head3SecondBias: { 1: 0.25, 2: 0.12, 4: 0.34, 5: 0.2, 6: 0.09 },
    head2SecondBias: { 1: 0.46, 3: 0.24, 4: 0.18, 5: 0.08, 6: 0.04 },
    weights: { insideResidual: 0.86, straight: 1.14, turn: 1.06, fourBeneficiary: 1.18, outsideFollow: 1.14 }
  },
  mixed: {
    headRates: { 1: 0.52, 2: 0.16, 3: 0.13, 4: 0.1, 5: 0.07, 6: 0.02 },
    scenarioFollowerBias: {
      makuri: { insideResidualRate: 0.5, outsideLinkedRate: 0.5, boat1SecondRate: 0.38 },
      makuriSashi: { insideResidualRate: 0.52, outsideLinkedRate: 0.48, boat1SecondRate: 0.42 },
      sashi: { insideResidualRate: 0.64, outsideLinkedRate: 0.36, boat1SecondRate: 0.54 }
    },
    head4SecondBias: { 1: 0.34, 2: 0.22, 3: 0.12, 5: 0.22, 6: 0.1 },
    head3SecondBias: { 1: 0.35, 2: 0.16, 4: 0.28, 5: 0.15, 6: 0.06 },
    head2SecondBias: { 1: 0.52, 3: 0.2, 4: 0.16, 5: 0.08, 6: 0.04 },
    weights: { insideResidual: 1, straight: 1, turn: 1, fourBeneficiary: 1, outsideFollow: 1 }
  }
};

function finiteNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 4) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  const scale = 10 ** digits;
  return Math.round(n * scale) / scale;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function rate(count, total) {
  const c = finiteNumber(count, 0);
  const t = finiteNumber(total, 0);
  return t > 0 ? round(c / t, 4) : null;
}

function normalizeRate(value) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  return clamp(Math.abs(n) > 1 ? n / 100 : n, 0, 1);
}

function venueBiasGroup(venueId) {
  const id = Number(venueId) || 0;
  if (VENUE_BIAS_GROUPS.inner.has(id)) return "inner";
  if (VENUE_BIAS_GROUPS.attack.has(id)) return "attack";
  return "mixed";
}

function decisionFromTemplate(template = {}, decision) {
  const row = template.scenarioFollowerBias?.[decision] || {};
  return {
    sampleCount: 24,
    sampleStatus: "small_sample",
    boat1SecondRate: row.boat1SecondRate ?? null,
    boat1ThirdRate: null,
    insideResidualRate: row.insideResidualRate ?? null,
    outsideLinkedRate: row.outsideLinkedRate ?? null,
    commonExacta: [],
    rareExacta: [],
    commonTrifecta: [],
    rareTrifecta: []
  };
}

function headSecondBiasToStats(head, decision, secondBias = {}) {
  const exactaRates = Object.fromEntries(
    Object.entries(secondBias).map(([boat, value]) => [`${head}-${boat}`, value])
  );
  return {
    sampleCount: 24,
    sampleStatus: "small_sample",
    secondRates: { ...secondBias },
    thirdRates: {},
    exactaRates,
    trifectaRates: {}
  };
}

export function getEstimatedVenueBias(venueId) {
  const group = venueBiasGroup(venueId);
  const template = VENUE_BIAS_TEMPLATES[group] || VENUE_BIAS_TEMPLATES.mixed;
  return {
    venueId: Number(venueId) || null,
    source: "estimated_static_venue_bias",
    group,
    headRates: { ...template.headRates },
    scenarioFollowerBias: JSON.parse(JSON.stringify(template.scenarioFollowerBias)),
    head4SecondBias: { ...template.head4SecondBias },
    head3SecondBias: { ...template.head3SecondBias },
    head2SecondBias: { ...template.head2SecondBias },
    venueFeatureWeights: { ...template.weights },
    decisionConditionedStats: {
      makuri: decisionFromTemplate(template, "makuri"),
      makuriSashi: decisionFromTemplate(template, "makuriSashi"),
      sashi: decisionFromTemplate(template, "sashi"),
      escape: {
        sampleCount: 24,
        sampleStatus: "small_sample",
        commonSecondBoats: [],
        commonThirdBoats: []
      }
    },
    headDecisionComboStats: {
      "2": { sashi: headSecondBiasToStats(2, "sashi", template.head2SecondBias) },
      "3": { makuri: headSecondBiasToStats(3, "makuri", template.head3SecondBias) },
      "4": { makuriSashi: headSecondBiasToStats(4, "makuriSashi", template.head4SecondBias) }
    }
  };
}

export function decisionSampleStatus(sampleCount) {
  const n = finiteNumber(sampleCount, 0);
  if (n >= 50) return "ok";
  if (n >= 20) return "small_sample";
  if (n >= 5) return "very_small_sample";
  return "insufficient";
}

export function decisionSampleWeight(sampleCount) {
  return ({
    ok: 1,
    small_sample: 0.45,
    very_small_sample: 0.16,
    insufficient: 0
  })[decisionSampleStatus(sampleCount)] ?? 0;
}

export function normalizeDecisionKey(value) {
  const text = String(value ?? "").trim().toLowerCase().replace(/[\s_-]+/g, "");
  if (!text) return null;
  if (text.includes("makurisashi") || text.includes("まくり差し") || text.includes("まくりざし")) return "makuriSashi";
  if (text.includes("makuri") || text.includes("まくり")) return "makuri";
  if (text.includes("sashi") || text.includes("差し")) return "sashi";
  if (text.includes("escape") || text.includes("nige") || text.includes("逃げ")) return "escape";
  return null;
}

function blankDecisionStats() {
  return {
    sampleCount: 0,
    sampleStatus: "insufficient",
    boat1SecondRate: null,
    boat1ThirdRate: null,
    insideResidualRate: null,
    outsideLinkedRate: null,
    commonExacta: [],
    rareExacta: [],
    commonTrifecta: [],
    rareTrifecta: []
  };
}

function blankHeadDecisionStats() {
  return {
    sampleCount: 0,
    sampleStatus: "insufficient",
    secondRates: {},
    thirdRates: {},
    exactaRates: {},
    trifectaRates: {}
  };
}

function makeCounter() {
  return {
    sampleCount: 0,
    boat1Second: 0,
    boat1Third: 0,
    insideResidual: 0,
    outsideLinked: 0,
    exacta: new Map(),
    trifecta: new Map()
  };
}

function makeHeadCounter() {
  return {
    sampleCount: 0,
    second: new Map(),
    third: new Map(),
    exacta: new Map(),
    trifecta: new Map()
  };
}

function incMap(map, key, amount = 1) {
  if (!key) return;
  map.set(String(key), (map.get(String(key)) || 0) + amount);
}

function mapRates(map, total) {
  return Object.fromEntries([...map.entries()].map(([key, count]) => [key, rate(count, total)]));
}

function commonRare(map, total) {
  const rows = [...map.entries()]
    .map(([combo, count]) => ({ combo, count, rate: rate(count, total) }))
    .sort((a, b) => b.count - a.count || String(a.combo).localeCompare(String(b.combo)));
  return {
    common: rows.slice(0, 6),
    rare: rows.filter((row) => row.count <= Math.max(1, total * 0.03)).slice(0, 6)
  };
}

function normalizeEntryBoat(entry = {}) {
  return finiteNumber(entry.boat ?? entry.boatNumber ?? entry.lane ?? entry.frame ?? entry.racer_boat_number, null);
}

function normalizeFinish(entry = {}) {
  return finiteNumber(entry.finishPosition ?? entry.finish ?? entry.rank ?? entry.arrival ?? entry.resultRank ?? entry["着順"], null);
}

function top3FromRace(race = {}) {
  const explicit = race?.result?.top3 || race?.top3 || race?.result?.finishOrder;
  if (Array.isArray(explicit) && explicit.length >= 3) {
    return explicit.slice(0, 3).map((value) => finiteNumber(value, null));
  }
  const entries = safeArray(race.entries)
    .map((entry) => ({ boat: normalizeEntryBoat(entry), finish: normalizeFinish(entry) }))
    .filter((entry) => Number.isInteger(entry.boat) && Number.isInteger(entry.finish) && entry.finish >= 1)
    .sort((a, b) => a.finish - b.finish);
  return entries.slice(0, 3).map((entry) => entry.boat);
}

function winnerBoatFromRace(race = {}, top3 = []) {
  return finiteNumber(
    race?.result?.winnerBoat ??
      race?.winnerBoat ??
      race?.result?.headBoat ??
      top3[0],
    null
  );
}

function decisionFromRace(race = {}) {
  return normalizeDecisionKey(
    race?.result?.winningDecision ??
      race?.result?.winningTechnique ??
      race?.result?.kimarite ??
      race?.result?.decision ??
      race?.result?.winMethod ??
      race?.result?.["決まり手"] ??
      race?.winningDecision ??
      race?.kimarite ??
      race?.decision
  );
}

function finalizeDecisionCounter(counter) {
  const total = counter.sampleCount;
  const exacta = commonRare(counter.exacta, total);
  const trifecta = commonRare(counter.trifecta, total);
  return {
    sampleCount: total,
    sampleStatus: decisionSampleStatus(total),
    boat1SecondRate: rate(counter.boat1Second, total),
    boat1ThirdRate: rate(counter.boat1Third, total),
    insideResidualRate: rate(counter.insideResidual, total),
    outsideLinkedRate: rate(counter.outsideLinked, total),
    commonExacta: exacta.common,
    rareExacta: exacta.rare,
    commonTrifecta: trifecta.common,
    rareTrifecta: trifecta.rare
  };
}

function finalizeHeadCounter(counter) {
  const total = counter.sampleCount;
  return {
    sampleCount: total,
    sampleStatus: decisionSampleStatus(total),
    secondRates: mapRates(counter.second, total),
    thirdRates: mapRates(counter.third, total),
    exactaRates: mapRates(counter.exacta, total),
    trifectaRates: mapRates(counter.trifecta, total)
  };
}

export function buildDecisionConditionedVenueBiasFromRaces(races = [], venueId = null) {
  const byDecision = Object.fromEntries(DECISIONS.map((key) => [key, makeCounter()]));
  const byHeadDecision = {};

  for (const race of safeArray(races)) {
    if (venueId !== null && venueId !== undefined && Number(race?.venueId ?? race?.venue_id) !== Number(venueId)) continue;
    const decision = decisionFromRace(race);
    if (!decision || !byDecision[decision]) continue;
    const top3 = top3FromRace(race);
    if (top3.length < 3 || top3.some((boat) => !Number.isInteger(boat))) continue;
    const head = winnerBoatFromRace(race, top3);
    const second = top3[1];
    const third = top3[2];
    const exacta = `${head}-${second}`;
    const trifecta = `${head}-${second}-${third}`;
    const counter = byDecision[decision];
    counter.sampleCount += 1;
    if (second === 1) counter.boat1Second += 1;
    if (third === 1) counter.boat1Third += 1;
    if ([second, third].some((boat) => boat === 1 || boat === 2)) counter.insideResidual += 1;
    if ([second, third].some((boat) => boat >= 3 && boat <= 6)) counter.outsideLinked += 1;
    incMap(counter.exacta, exacta);
    incMap(counter.trifecta, trifecta);

    if (!byHeadDecision[String(head)]) byHeadDecision[String(head)] = {};
    if (!byHeadDecision[String(head)][decision]) byHeadDecision[String(head)][decision] = makeHeadCounter();
    const headCounter = byHeadDecision[String(head)][decision];
    headCounter.sampleCount += 1;
    incMap(headCounter.second, second);
    incMap(headCounter.third, third);
    incMap(headCounter.exacta, exacta);
    incMap(headCounter.trifecta, trifecta);
  }

  return {
    decisionConditionedStats: Object.fromEntries(
      DECISIONS.map((decision) => [decision, finalizeDecisionCounter(byDecision[decision])])
    ),
    headDecisionComboStats: Object.fromEntries(
      Object.entries(byHeadDecision).map(([head, decisions]) => [
        head,
        Object.fromEntries(Object.entries(decisions).map(([decision, counter]) => [decision, finalizeHeadCounter(counter)]))
      ])
    )
  };
}

function normalizeDecisionStats(row = null) {
  if (!row || typeof row !== "object") return blankDecisionStats();
  return {
    sampleCount: finiteNumber(row.sampleCount ?? row.sample_count, 0),
    sampleStatus: row.sampleStatus || row.sample_status || decisionSampleStatus(row.sampleCount ?? row.sample_count),
    boat1SecondRate: normalizeRate(row.boat1SecondRate ?? row.boat1_second_rate),
    boat1ThirdRate: normalizeRate(row.boat1ThirdRate ?? row.boat1_third_rate),
    insideResidualRate: normalizeRate(row.insideResidualRate ?? row.inside_residual_rate),
    outsideLinkedRate: normalizeRate(row.outsideLinkedRate ?? row.outside_linked_rate),
    commonExacta: safeArray(row.commonExacta ?? row.common_exacta),
    rareExacta: safeArray(row.rareExacta ?? row.rare_exacta),
    commonTrifecta: safeArray(row.commonTrifecta ?? row.common_trifecta),
    rareTrifecta: safeArray(row.rareTrifecta ?? row.rare_trifecta)
  };
}

function normalizeHeadDecisionStats(row = null) {
  if (!row || typeof row !== "object") return blankHeadDecisionStats();
  return {
    sampleCount: finiteNumber(row.sampleCount ?? row.sample_count, 0),
    sampleStatus: row.sampleStatus || row.sample_status || decisionSampleStatus(row.sampleCount ?? row.sample_count),
    secondRates: Object.fromEntries(Object.entries(row.secondRates ?? row.second_rates ?? {}).map(([boat, value]) => [boat, normalizeRate(value)])),
    thirdRates: Object.fromEntries(Object.entries(row.thirdRates ?? row.third_rates ?? {}).map(([boat, value]) => [boat, normalizeRate(value)])),
    exactaRates: Object.fromEntries(Object.entries(row.exactaRates ?? row.exacta_rates ?? {}).map(([combo, value]) => [combo, normalizeRate(value)])),
    trifectaRates: Object.fromEntries(Object.entries(row.trifectaRates ?? row.trifecta_rates ?? {}).map(([combo, value]) => [combo, normalizeRate(value)]))
  };
}

function venueRoot(venueBias = null, stadiumNumber = null) {
  if (!venueBias || typeof venueBias !== "object") return {};
  return venueBias[String(stadiumNumber)] || venueBias[stadiumNumber] || venueBias;
}

export function getDecisionConditionedStats(venueBias = null, stadiumNumber = null) {
  const root = venueRoot(venueBias, stadiumNumber);
  const source =
    root.decisionConditionedStats ||
    root.decision_conditioned_stats ||
    root.venueBiasProfile?.decisionConditionedStats ||
    root.venue_bias_profile?.decisionConditionedStats ||
    {};
  return Object.fromEntries(DECISIONS.map((decision) => [decision, normalizeDecisionStats(source?.[decision])]));
}

export function getHeadDecisionComboStats(venueBias = null, stadiumNumber = null) {
  const root = venueRoot(venueBias, stadiumNumber);
  const source =
    root.headDecisionComboStats ||
    root.head_decision_combo_stats ||
    root.venueBiasProfile?.headDecisionComboStats ||
    root.venue_bias_profile?.headDecisionComboStats ||
    {};
  const normalized = {};
  for (const boat of BOATS) {
    const byDecision = source?.[String(boat)] || source?.[boat] || {};
    normalized[String(boat)] = Object.fromEntries(
      DECISIONS.map((decision) => [decision, normalizeHeadDecisionStats(byDecision?.[decision])])
    );
  }
  return normalized;
}

export function weightedDecisionRate(row = {}, field, fallback = 0.5) {
  const rateValue = normalizeRate(row?.[field]);
  if (rateValue === null) return fallback;
  const weight = decisionSampleWeight(row?.sampleCount);
  return fallback + (rateValue - fallback) * weight;
}

export function decisionStatsPreview(venueBias = null, stadiumNumber = null) {
  return {
    decisionConditionedStats: getDecisionConditionedStats(venueBias, stadiumNumber),
    headDecisionComboStats: getHeadDecisionComboStats(venueBias, stadiumNumber)
  };
}
