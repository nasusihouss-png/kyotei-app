const DEFAULT_FETCHED_DATA_FIELDS = [
  "national_performance",
  "local_performance",
  "lane_based_rates",
  "winning_technique_tendencies",
  "motor_rate",
  "boat_rate",
  "pre_race_info",
  "wind_direction",
  "wind_speed",
  "wave_height",
  "lap_time",
  "exhibition_time",
  "st",
  "turning_performance",
  "straight_performance",
  "sectional_point_rate",
  "push_situation",
  "entry_change",
  "racer_preferred_lane",
  "racer_preferred_technique",
  "boat1_second_keep_probability"
];

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function asNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function compactObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}

function normalizeCombo(combo) {
  if (Array.isArray(combo)) return combo.map((x) => String(x).trim()).filter(Boolean).join("-");
  return String(combo ?? "").trim();
}

function normalizeTicket(row = {}, index = 0, fallbackTier = "contender") {
  const combo = normalizeCombo(row.combo ?? row.selection ?? row.trifecta ?? row.value);
  return {
    combo,
    probability: asNumber(row.probability ?? row.prob ?? row.hit_rate, null),
    score: asNumber(row.score ?? row.ev ?? row.expected_value, null),
    tier: String(row.tier || row.ticket_tier || fallbackTier || "contender").toLowerCase(),
    rank: asNumber(row.rank, index + 1),
    reason: row.reason || row.formationReason || row.reason_text || ""
  };
}

function normalizeProbabilityRows(source = {}) {
  if (Array.isArray(source)) {
    return source.map((row) => ({
      lane: asNumber(row?.lane ?? row?.boat ?? row?.course, null),
      probability: asNumber(row?.probability ?? row?.prob ?? row?.rate, null)
    }));
  }
  return Object.entries(compactObject(source)).map(([lane, probability]) => ({
    lane: asNumber(lane, null),
    probability: asNumber(probability, null)
  }));
}

export function buildQuickInputModel({
  date = "",
  venueId = "",
  venue = "",
  raceNo = "",
  racers = []
} = {}) {
  const normalizedRacers = Array.from({ length: 6 }, (_, index) => {
    const row = safeArray(racers)[index] || {};
    const lane = index + 1;
    return {
      lane,
      racerName: String(row.racerName ?? row.name ?? row.playerName ?? "").trim(),
      racerId: String(row.racerId ?? row.registrationNo ?? row.registration_no ?? "").trim()
    };
  });

  return {
    date: String(date || "").trim(),
    venueId: venueId === "" || venueId === null || venueId === undefined ? "" : Number(venueId),
    venue: String(venue || "").trim(),
    raceNo: raceNo === "" || raceNo === null || raceNo === undefined ? "" : Number(raceNo),
    racers: normalizedRacers,
    manualFields: ["date", "venue", "raceNo", "racers"],
    autoFetchedFields: DEFAULT_FETCHED_DATA_FIELDS
  };
}

export function buildPredictionExplanationModel({
  data = {},
  prediction = {},
  pureTop6Prediction = {}
} = {}) {
  const pure = compactObject(pureTop6Prediction);
  const pred = compactObject(prediction);
  const source = compactObject(data);
  const firstRates = normalizeProbabilityRows(
    pure.first_place_candidate_rates || pure.winProbabilities || pred.winProbabilities || source.winProbabilities
  );
  const secondRates = normalizeProbabilityRows(
    pure.second_place_candidate_rates || pure.secondProbabilities || pred.secondProbabilities || source.secondProbabilities
  );
  const thirdRates = normalizeProbabilityRows(
    pure.third_place_candidate_rates || pure.thirdProbabilities || pred.thirdProbabilities || source.thirdProbabilities
  );
  const styleRows = safeArray(pure.lane_styles || source.lane_styles || pred.lane_styles);
  const primaryStyle = styleRows[0] || {};
  const confidenceScore = asNumber(
    pure.confidence ?? pred.confidence_score ?? source.confidenceScores?.bet_confidence ?? source.confidenceScores?.confidence,
    null
  );
  const buyPolicy =
    source.participationDecision?.decision ||
    source.raceDecision?.decision ||
    source.recommendation_label ||
    pred.buyPolicy ||
    "";

  return {
    racePattern: pure.racePattern || pred.racePattern || source.racePattern?.pattern || source.likelyWinningPattern || "",
    racePatternScore: asNumber(pure.racePatternScore ?? pred.racePatternScore ?? source.racePattern?.score, null),
    top6Scenario: pure.top6Scenario || pred.top6Scenario || source.scenarioSuggestions?.primary_scenario || "",
    top6ScenarioScore: asNumber(pure.top6ScenarioScore ?? pred.top6ScenarioScore, null),
    hardScenario: pure.hardScenario || source.hardRaceResponseContract?.scenario || source.hardRace1234?.scenario || "",
    hardScenarioScore: asNumber(pure.hardScenarioScore ?? source.hardRaceResponseContract?.score, null),
    scenario_repro_score: asNumber(pure.scenario_repro_score ?? pred.scenario_repro_score, null),
    style: primaryStyle.style || pred.style || "",
    style_score: asNumber(primaryStyle.style_score ?? pred.style_score, null),
    Pr1: firstRates,
    Pr2: secondRates,
    Pr3: thirdRates,
    boat1_second_keep_score: asNumber(pure.boat1_second_keep_score ?? pred.boat1_second_keep_score, null),
    second_given_head_probabilities: compactObject(
      pure.second_given_head_probabilities || pred.second_given_head_probabilities || source.second_given_head_probabilities
    ),
    confidence_score: confidenceScore,
    confidence_band:
      pure.confidence_band ||
      pred.confidence_band ||
      (confidenceScore === null ? "" : confidenceScore >= 0.72 ? "high" : confidenceScore >= 0.5 ? "medium" : "low"),
    buyPolicy,
    recommendedBetMode:
      source.participationDecision?.recommended_bet_mode ||
      source.raceDecision?.recommendedBetMode ||
      pred.recommendedBetMode ||
      "",
    skipRiskReason:
      source.participationDecision?.skip_reason ||
      source.raceRisk?.skip_reason ||
      source.raceRisk?.reason ||
      pred.skipRiskReason ||
      ""
  };
}

export function buildTicketsModel({ pureTop6Prediction = {}, prediction = {}, data = {} } = {}) {
  const pure = compactObject(pureTop6Prediction);
  const top6Source =
    safeArray(pure.top6).length > 0
      ? pure.top6
      : safeArray(pure.tickets).length > 0
        ? pure.tickets
        : safeArray(data?.ticketOptimization?.final_tickets).length > 0
          ? data.ticketOptimization.final_tickets.slice(0, 6)
          : safeArray(prediction?.recommended_bets).slice(0, 6);

  const formation = compactObject(pure.optionalFormation16 || pure.wide_formation_suggestion || data.optionalFormation16);
  const formationCombos = safeArray(formation.combos).slice(0, 16).map((row, index) => normalizeTicket(row, index, "cover"));
  const formationActive = formation.active === true && formationCombos.length > 0;
  const formationReason = formationActive
    ? pure.formationReason || formation.reason || prediction.formationReason || null
    : null;

  return {
    top6: top6Source.slice(0, 6).map((row, index) =>
      normalizeTicket(row, index, index === 0 ? "main" : index < 3 ? "contender" : "cover")
    ),
    optionalFormation16: formationActive
      ? {
        active: true,
        formation: formation.formation_string || formation.formation || "",
        size: asNumber(formation.size, formationCombos.length),
        combos: formationCombos
      }
      : [],
    formationReason,
    tiers: ["main", "contender", "cover"]
  };
}

export function buildRaceLogPersistenceModel({
  predictionTimestamp = new Date().toISOString(),
  quickInput = {},
  predictionExplanation = {},
  tickets = {},
  actualResult = "",
  payout = 0,
  skipped = false
} = {}) {
  const top6 = safeArray(tickets.top6).map((row) => row.combo).filter(Boolean);
  const optionalFormation16 = safeArray(tickets.optionalFormation16?.combos).map((row) => row.combo).filter(Boolean);
  const actual = normalizeCombo(actualResult);
  const hit = actual ? top6.includes(actual) || optionalFormation16.includes(actual) : null;

  return {
    predictionTimestamp,
    venue: quickInput.venue || quickInput.venueId || "",
    raceNo: quickInput.raceNo || "",
    top6,
    optionalFormation16,
    confidence: predictionExplanation.confidence_score ?? null,
    buyPolicy: predictionExplanation.buyPolicy || "",
    actualResult: actual,
    hit,
    payout: asNumber(payout, 0),
    skipped: Boolean(skipped),
    scenarioSnapshot: predictionExplanation
  };
}

export function buildDashboardAggregationModel(logs = []) {
  const rows = safeArray(logs);
  const total = rows.length;
  const settled = rows.filter((row) => row.hit === true || row.hit === false);
  const hitCount = settled.filter((row) => row.hit === true).length;
  const skipped = rows.filter((row) => row.skipped);
  const invested = rows.reduce((sum, row) => sum + (row.skipped ? 0 : 100), 0);
  const payout = rows.reduce((sum, row) => sum + Number(row.payout || 0), 0);
  const venueMap = new Map();
  const missPatternCounts = {};

  for (const row of rows) {
    const venue = String(row.venue || "unknown");
    const venueAgg = venueMap.get(venue) || { venue, races: 0, hits: 0, payout: 0, invested: 0 };
    venueAgg.races += 1;
    if (row.hit === true) venueAgg.hits += 1;
    venueAgg.payout += Number(row.payout || 0);
    venueAgg.invested += row.skipped ? 0 : 100;
    venueMap.set(venue, venueAgg);
    if (row.hit === false && row.actualResult) {
      missPatternCounts[row.actualResult] = (missPatternCounts[row.actualResult] || 0) + 1;
    }
  }

  return {
    raceCount: total,
    top6HitRate: settled.length ? hitCount / settled.length : 0,
    optionalFormation16HitRate: settled.length ? hitCount / settled.length : 0,
    hardRaceHitRate: settled.length ? hitCount / settled.length : 0,
    skipAccuracy: skipped.length ? skipped.filter((row) => row.hit !== false).length / skipped.length : 0,
    roi: invested > 0 ? payout / invested : 0,
    venueWise: [...venueMap.values()].map((row) => ({
      ...row,
      hitRate: row.races ? row.hits / row.races : 0,
      roi: row.invested ? row.payout / row.invested : 0
    })),
    missTendency: Object.entries(missPatternCounts)
      .map(([combo, count]) => ({ combo, count }))
      .sort((a, b) => b.count - a.count),
    venueBiasReviewSuggestions: [...venueMap.values()]
      .filter((row) => row.races >= 3 && row.hits / row.races < 0.35)
      .map((row) => `${row.venue}: review lane/technique bias and skip threshold`)
  };
}

export function buildPlayerMasterPersistenceModel(rows = []) {
  return safeArray(rows).map((row) => ({
    racerName: String(row.racerName ?? row.name ?? row.playerName ?? "").trim(),
    racerId: String(row.racerId ?? row.registrationNo ?? row.registration_no ?? "").trim(),
    style: String(row.style ?? "").trim(),
    preferredCourse: row.preferredCourse ?? row.preferred_course ?? null,
    preferredWinningTechnique: String(row.preferredWinningTechnique ?? row.preferred_technique ?? "").trim(),
    preferredVenue: String(row.preferredVenue ?? row.preferred_venue ?? "").trim(),
    baseAdjustment: asNumber(row.baseAdjustment ?? row.base_adjustment, 0),
    laneAdjustments: compactObject(row.laneAdjustments || row.lane_adjustments),
    keepPositionAdjustments: compactObject(row.keepPositionAdjustments || row.keep_position_adjustments)
  }));
}

export { DEFAULT_FETCHED_DATA_FIELDS };
