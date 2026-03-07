import db from "./db.js";

function ensurePredictionLogColumns() {
  const cols = db.prepare("PRAGMA table_info(prediction_logs)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("race_decision_json")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN race_decision_json TEXT");
  }
}

ensurePredictionLogColumns();

const insertPredictionLog = db.prepare(`
  INSERT INTO prediction_logs (
    race_id,
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
