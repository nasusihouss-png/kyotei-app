function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function uniqCombos(rows) {
  const set = new Set();
  const out = [];
  for (const row of rows || []) {
    const combo = normalizeCombo(row?.combo ?? row);
    if (!combo || set.has(combo)) continue;
    set.add(combo);
    out.push(combo);
  }
  return out;
}

function rankMap(ranking) {
  const map = new Map();
  for (const row of ranking || []) {
    const lane = Number(row?.racer?.lane ?? row?.lane);
    const rank = Number(row?.rank);
    if (Number.isFinite(lane) && Number.isFinite(rank)) map.set(lane, rank);
  }
  return map;
}

function classifyScenario({ raceFlow, raceIndexes, raceDecision, entryMeta, startSignals }) {
  const mode = String(raceFlow?.race_flow_mode || "");
  const nige = toNum(raceFlow?.nige_prob, 0);
  const sashi = toNum(raceFlow?.sashi_prob, 0);
  const makuri = toNum(raceFlow?.makuri_prob, 0);
  const mkzs = toNum(raceFlow?.makurizashi_prob, 0);
  const flowConf = toNum(raceFlow?.flow_confidence, 0) * 100;
  const chaos = toNum(raceIndexes?.are_index, 50);
  const headStability = toNum(raceDecision?.factors?.head_stability_score, 50);
  const entrySeverity = String(entryMeta?.severity || "none");
  const startStability = toNum(startSignals?.stability_score, 50);
  const fastLane = toNum(startSignals?.fastest_st_lane, 0);

  let scenarioType = "balanced_standard";
  if (entrySeverity === "high" || chaos >= 75) scenarioType = "entry_change_unstable";
  else if (nige >= 0.55 && headStability >= 58 && startStability >= 52) scenarioType = "strong_escape";
  else if (nige >= 0.45 && sashi >= 0.22) scenarioType = "escape_with_challenger";
  else if (makuri + mkzs >= 0.45 || mode === "makuri" || mode === "makurizashi") scenarioType = "outside_attack";
  else if (fastLane >= 4 && startStability < 56) scenarioType = "fast_outside_pressure";
  else if (mode === "sashi" || sashi >= 0.3) scenarioType = "inside_weak_outside_attack";

  const confidence = clamp(
    25,
    95,
    35 +
      flowConf * 0.3 +
      startStability * 0.25 +
      headStability * 0.2 -
      Math.max(0, chaos - 55) * 0.35 -
      (entrySeverity === "high" ? 12 : entrySeverity === "medium" ? 6 : 0)
  );

  return {
    scenario_type: scenarioType,
    scenario_confidence: Number(confidence.toFixed(2))
  };
}

function chooseLongshots(allCombos, usedSet, rankingMap) {
  const longshots = [];
  for (const combo of allCombos) {
    if (usedSet.has(combo)) continue;
    const lanes = combo.split("-").map((v) => Number(v));
    if (lanes.some((l) => (rankingMap.get(l) || 9) >= 5)) {
      longshots.push(combo);
    }
    if (longshots.length >= 3) break;
  }
  return longshots;
}

export function buildScenarioSuggestions({
  ranking,
  raceFlow,
  raceIndexes,
  raceDecision,
  entryMeta,
  startSignals,
  ticketOptimization,
  betPlan,
  ticketGenerationV2
}) {
  const scenario = classifyScenario({
    raceFlow,
    raceIndexes,
    raceDecision,
    entryMeta,
    startSignals
  });
  const rankLookup = rankMap(ranking);

  const optimized = Array.isArray(ticketOptimization?.optimized_tickets)
    ? ticketOptimization.optimized_tickets
    : [];
  const planRows = Array.isArray(betPlan?.recommended_bets) ? betPlan.recommended_bets : [];
  const primary = Array.isArray(ticketGenerationV2?.primary_tickets) ? ticketGenerationV2.primary_tickets : [];
  const secondary = Array.isArray(ticketGenerationV2?.secondary_tickets) ? ticketGenerationV2.secondary_tickets : [];

  const seedRows = [
    ...optimized.map((r) => ({ ...r, combo: normalizeCombo(r.combo) })),
    ...planRows.map((r) => ({ ...r, combo: normalizeCombo(r.combo) }))
  ].filter((r) => r.combo);

  const allCombos = uniqCombos([
    ...seedRows.map((r) => r.combo),
    ...primary,
    ...secondary
  ]);

  let main_picks = [];
  let backup_picks = [];
  let longshot_picks = [];

  if (scenario.scenario_type === "strong_escape") {
    main_picks = uniqCombos([...primary, ...seedRows.filter((r) => String(r.ticket_type || "").toLowerCase() === "main").map((r) => r.combo)]).slice(0, 4);
    backup_picks = uniqCombos([...secondary, ...seedRows.filter((r) => String(r.ticket_type || "").toLowerCase() !== "main").map((r) => r.combo)]).slice(0, 4);
  } else if (scenario.scenario_type === "entry_change_unstable") {
    main_picks = uniqCombos([...seedRows.map((r) => r.combo)]).slice(0, 2);
    backup_picks = uniqCombos([...secondary, ...primary]).slice(0, 3);
  } else if (scenario.scenario_type === "outside_attack" || scenario.scenario_type === "fast_outside_pressure") {
    main_picks = uniqCombos([...seedRows.filter((r) => String(r.ticket_type || "").toLowerCase() !== "longshot").map((r) => r.combo), ...primary]).slice(0, 3);
    backup_picks = uniqCombos([...secondary, ...seedRows.map((r) => r.combo)]).slice(0, 4);
  } else {
    main_picks = uniqCombos([...primary, ...seedRows.map((r) => r.combo)]).slice(0, 3);
    backup_picks = uniqCombos([...secondary, ...seedRows.map((r) => r.combo)]).slice(0, 4);
  }

  const used = new Set([...main_picks, ...backup_picks]);
  longshot_picks = chooseLongshots(allCombos, used, rankLookup);

  const fallback = main_picks.length === 0 && backup_picks.length === 0;
  if (fallback) {
    main_picks = allCombos.slice(0, 3);
    backup_picks = allCombos.slice(3, 6);
  }

  const bucketByCombo = {};
  for (const combo of main_picks) bucketByCombo[combo] = "main";
  for (const combo of backup_picks) if (!bucketByCombo[combo]) bucketByCombo[combo] = "backup";
  for (const combo of longshot_picks) if (!bucketByCombo[combo]) bucketByCombo[combo] = "longshot";

  const summary = `${scenario.scenario_type} (${scenario.scenario_confidence.toFixed(1)})`;

  return {
    scenario_type: scenario.scenario_type,
    scenario_confidence: scenario.scenario_confidence,
    main_picks,
    backup_picks,
    longshot_picks,
    bucket_by_combo: bucketByCombo,
    fallback_used: fallback,
    summary
  };
}
