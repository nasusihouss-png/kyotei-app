import db from "./db.js";
import { ensurePredictionSnapshotColumns, nowIso } from "./prediction-snapshot-store.js";
import { ensureSimilarRaceFeatureTable, upsertSimilarRaceFeatureSnapshot } from "./similar-race-feature-store.js";

function ensurePredictionLogColumns() {
  const cols = db.prepare("PRAGMA table_info(prediction_logs)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("race_decision_json")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN race_decision_json TEXT");
  }
  if (!names.has("race_date")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN race_date TEXT");
  }
  if (!names.has("venue_code")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN venue_code INTEGER");
  }
  if (!names.has("venue_name")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN venue_name TEXT");
  }
  if (!names.has("race_no")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN race_no INTEGER");
  }
}

ensurePredictionSnapshotColumns();
ensurePredictionLogColumns();
ensureSimilarRaceFeatureTable();

const insertPredictionLog = db.prepare(`
  INSERT INTO prediction_logs (
    race_id,
    race_key,
    race_date,
    venue_code,
    venue_name,
    race_no,
    prediction_timestamp,
    model_version,
    race_pattern,
    buy_type,
    risk_score,
    recommendation,
    top3_json,
    prediction_json,
    race_decision_json,
    probabilities_json,
    ev_analysis_json,
    bet_plan_json
  ) VALUES (
    @race_id,
    @race_key,
    @race_date,
    @venue_code,
    @venue_name,
    @race_no,
    @prediction_timestamp,
    @model_version,
    @race_pattern,
    @buy_type,
    @risk_score,
    @recommendation,
    @top3_json,
    @prediction_json,
    @race_decision_json,
    @probabilities_json,
    @ev_analysis_json,
    @bet_plan_json
  )
`);

function cloneJson(value, fallback) {
  try {
    return JSON.parse(JSON.stringify(value ?? fallback));
  } catch {
    return fallback;
  }
}

function toNumOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizePredictionSnapshot({
  raceId,
  race,
  prediction,
  raceDecision,
  probabilities,
  ev_analysis,
  bet_plan
}) {
  const basePrediction = cloneJson(prediction, {}) || {};
  const snapshotCreatedAt = basePrediction?.snapshot_created_at || nowIso();
  const raceKey = basePrediction?.race_key || String(raceId || "");
  const snapshotContext = basePrediction?.snapshot_context && typeof basePrediction.snapshot_context === "object"
    ? cloneJson(basePrediction.snapshot_context, {})
    : {};
  const players = Array.isArray(snapshotContext?.players) ? cloneJson(snapshotContext.players, []) : [];
  const playerSummary = Array.isArray(snapshotContext?.player_summary) ? cloneJson(snapshotContext.player_summary, players) : players;
  const startDisplay = snapshotContext?.start_display && typeof snapshotContext.start_display === "object"
    ? cloneJson(snapshotContext.start_display, null)
    : null;
  const finalRecommendedBetsSnapshot = Array.isArray(basePrediction?.final_recommended_bets_snapshot)
    ? cloneJson(basePrediction.final_recommended_bets_snapshot, [])
    : [];
  const aiBetsFullSnapshot =
    basePrediction?.ai_bets_full_snapshot && typeof basePrediction.ai_bets_full_snapshot === "object"
      ? cloneJson(basePrediction.ai_bets_full_snapshot, {})
      : {};

  const normalizedPrediction = {
    ...basePrediction,
    snapshot_created_at: snapshotCreatedAt,
    race_key: raceKey,
    model_version: basePrediction?.model_version ?? null,
    predicted_entry_order: Array.isArray(basePrediction?.predicted_entry_order) ? cloneJson(basePrediction.predicted_entry_order, []) : [],
    actual_entry_order: Array.isArray(basePrediction?.actual_entry_order) ? cloneJson(basePrediction.actual_entry_order, []) : [],
    top3: Array.isArray(basePrediction?.top3) ? cloneJson(basePrediction.top3, []) : [],
    final_recommended_bets_snapshot: finalRecommendedBetsSnapshot,
    final_recommended_bets_count: Number.isFinite(Number(basePrediction?.final_recommended_bets_count))
      ? Number(basePrediction.final_recommended_bets_count)
      : finalRecommendedBetsSnapshot.length,
    final_recommended_bets_snapshot_source: basePrediction?.final_recommended_bets_snapshot_source || null,
    ai_bets_display_snapshot: Array.isArray(basePrediction?.ai_bets_display_snapshot)
      ? cloneJson(basePrediction.ai_bets_display_snapshot, [])
      : finalRecommendedBetsSnapshot,
    ai_bets_full_snapshot: {
      recommended_bets: Array.isArray(aiBetsFullSnapshot?.recommended_bets)
        ? cloneJson(aiBetsFullSnapshot.recommended_bets, [])
        : Array.isArray(bet_plan?.recommended_bets)
          ? cloneJson(bet_plan.recommended_bets, [])
          : [],
      optimized_tickets: Array.isArray(aiBetsFullSnapshot?.optimized_tickets)
        ? cloneJson(aiBetsFullSnapshot.optimized_tickets, [])
        : [],
      ticket_generation_v2: aiBetsFullSnapshot?.ticket_generation_v2 && typeof aiBetsFullSnapshot.ticket_generation_v2 === "object"
        ? cloneJson(aiBetsFullSnapshot.ticket_generation_v2, { primary_tickets: [], secondary_tickets: [] })
        : { primary_tickets: [], secondary_tickets: [] },
      scenario_suggestions: aiBetsFullSnapshot?.scenario_suggestions && typeof aiBetsFullSnapshot.scenario_suggestions === "object"
        ? cloneJson(aiBetsFullSnapshot.scenario_suggestions, { main_picks: [], backup_picks: [], longshot_picks: [] })
        : { main_picks: [], backup_picks: [], longshot_picks: [] }
    },
    snapshot_context: {
      ...snapshotContext,
      race_key: snapshotContext?.race_key || raceKey,
      race_date: snapshotContext?.race_date || race?.date || null,
      venue_code: Number.isFinite(Number(snapshotContext?.venue_code))
        ? Number(snapshotContext.venue_code)
        : Number.isFinite(Number(race?.venueId))
          ? Number(race.venueId)
          : null,
      venue_name: snapshotContext?.venue_name || race?.venueName || null,
      race_no: Number.isFinite(Number(snapshotContext?.race_no))
        ? Number(snapshotContext.race_no)
        : Number.isFinite(Number(race?.raceNo))
          ? Number(race.raceNo)
          : null,
      race_name: snapshotContext?.race_name || race?.raceName || null,
      weather: snapshotContext?.weather || race?.weather || null,
      wind_speed: toNumOrNull(snapshotContext?.wind_speed ?? race?.windSpeed),
      wind_direction: snapshotContext?.wind_direction || race?.windDirection || race?.windDir || null,
      wave_height: toNumOrNull(snapshotContext?.wave_height ?? race?.waveHeight),
      players,
      player_summary: playerSummary,
      start_display: startDisplay
    },
    race_decision_snapshot: cloneJson(raceDecision, {}),
    probabilities_snapshot: cloneJson(probabilities, []),
    ev_analysis_snapshot: cloneJson(ev_analysis, {}),
    bet_plan_snapshot: cloneJson(bet_plan, {})
  };

  return normalizedPrediction;
}

export function savePredictionLog({
  raceId,
  race,
  racePattern,
  buyType,
  raceRisk,
  prediction,
  raceDecision,
  probabilities,
  ev_analysis,
  bet_plan
}) {
  const normalizedPrediction = normalizePredictionSnapshot({
    raceId,
    race,
    prediction,
    raceDecision,
    probabilities,
    ev_analysis,
    bet_plan
  });
  const insertInfo = insertPredictionLog.run({
    race_id: raceId,
    race_key: normalizedPrediction.race_key ?? String(raceId || ""),
    race_date: normalizedPrediction?.snapshot_context?.race_date ?? race?.date ?? null,
    venue_code: Number.isFinite(Number(normalizedPrediction?.snapshot_context?.venue_code))
      ? Number(normalizedPrediction.snapshot_context.venue_code)
      : Number.isFinite(Number(race?.venueId))
        ? Number(race.venueId)
        : null,
    venue_name: normalizedPrediction?.snapshot_context?.venue_name ?? race?.venueName ?? null,
    race_no: Number.isFinite(Number(normalizedPrediction?.snapshot_context?.race_no))
      ? Number(normalizedPrediction.snapshot_context.race_no)
      : Number.isFinite(Number(race?.raceNo))
        ? Number(race.raceNo)
        : null,
    prediction_timestamp: normalizedPrediction?.snapshot_created_at ?? nowIso(),
    model_version: normalizedPrediction?.model_version ?? null,
    race_pattern: racePattern ?? null,
    buy_type: buyType ?? null,
    risk_score: raceRisk?.risk_score ?? null,
    recommendation: raceRisk?.recommendation ?? null,
    top3_json: JSON.stringify(normalizedPrediction?.top3 ?? []),
    prediction_json: JSON.stringify(normalizedPrediction ?? {}),
    race_decision_json: JSON.stringify(cloneJson(raceDecision, {})),
    probabilities_json: JSON.stringify(cloneJson(probabilities, [])),
    ev_analysis_json: JSON.stringify(cloneJson(ev_analysis, {})),
    bet_plan_json: JSON.stringify(cloneJson(bet_plan, {}))
  });
  upsertSimilarRaceFeatureSnapshot({
    raceId,
    race,
    prediction: normalizedPrediction,
    predictionSnapshotId: insertInfo?.lastInsertRowid ?? null
  });
  return insertInfo;
}
