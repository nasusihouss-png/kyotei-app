import db from "./db.js";
import { ensurePredictionSnapshotColumns } from "./prediction-snapshot-store.js";

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
  insertPredictionLog.run({
    race_id: raceId,
    race_key: prediction?.race_key ?? String(raceId || ""),
    race_date: race?.date ?? null,
    venue_code: Number.isFinite(Number(race?.venueId)) ? Number(race.venueId) : null,
    venue_name: race?.venueName ?? null,
    race_no: Number.isFinite(Number(race?.raceNo)) ? Number(race.raceNo) : null,
    prediction_timestamp: prediction?.snapshot_created_at ?? new Date().toISOString(),
    model_version: prediction?.model_version ?? null,
    race_pattern: racePattern ?? null,
    buy_type: buyType ?? null,
    risk_score: raceRisk?.risk_score ?? null,
    recommendation: raceRisk?.recommendation ?? null,
    top3_json: JSON.stringify(prediction?.top3 ?? []),
    prediction_json: JSON.stringify(prediction ?? {}),
    race_decision_json: JSON.stringify(raceDecision ?? {}),
    probabilities_json: JSON.stringify(probabilities ?? []),
    ev_analysis_json: JSON.stringify(ev_analysis ?? {}),
    bet_plan_json: JSON.stringify(bet_plan ?? {})
  });
}
