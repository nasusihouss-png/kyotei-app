import db from "./db.js";

function nowIso() {
  return new Date().toISOString();
}

function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function safeJson(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

db.exec(`
  CREATE TABLE IF NOT EXISTS prediction_feature_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id TEXT NOT NULL UNIQUE,
    race_date TEXT,
    venue_id INTEGER,
    venue_name TEXT,
    race_no INTEGER,
    race_grade TEXT,
    weather TEXT,
    wind REAL,
    wave REAL,
    motor_rate_avg REAL,
    boat_rate_avg REAL,
    avg_st_avg REAL,
    exhibition_time_avg REAL,
    start_display_st_json TEXT,
    start_display_signature TEXT,
    predicted_entry_order_json TEXT,
    actual_entry_order_json TEXT,
    entry_changed INTEGER,
    entry_change_type TEXT,
    ranking_score REAL,
    recommendation_score REAL,
    confidence REAL,
    recommendation_mode TEXT,
    prediction_snapshot_json TEXT,
    prediction_before_entry_change_json TEXT,
    prediction_after_entry_change_json TEXT,
    actual_result TEXT,
    hit_flag INTEGER,
    settled_bet_hit_count INTEGER,
    settled_bet_count INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS prediction_feature_log_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id TEXT NOT NULL,
    race_key TEXT,
    race_date TEXT,
    venue_id INTEGER,
    venue_name TEXT,
    race_no INTEGER,
    race_grade TEXT,
    weather TEXT,
    wind REAL,
    wave REAL,
    motor_rate_avg REAL,
    boat_rate_avg REAL,
    avg_st_avg REAL,
    exhibition_time_avg REAL,
    start_display_order_json TEXT,
    start_display_st_json TEXT,
    start_display_timing_json TEXT,
    start_display_raw_json TEXT,
    start_display_signature TEXT,
    predicted_entry_order_json TEXT,
    actual_entry_order_json TEXT,
    entry_changed INTEGER,
    entry_change_type TEXT,
    ranking_score REAL,
    recommendation_score REAL,
    confidence REAL,
    recommendation_mode TEXT,
    prediction_snapshot_json TEXT,
    prediction_before_entry_change_json TEXT,
    prediction_after_entry_change_json TEXT,
    source_timestamp TEXT,
    learning_run_id INTEGER,
    learned_at TEXT,
    actual_result TEXT,
    hit_flag INTEGER,
    settled_bet_hit_count INTEGER,
    settled_bet_count INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )
`);

function ensurePredictionFeatureEventColumns() {
  const cols = db.prepare("PRAGMA table_info(prediction_feature_log_events)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("start_display_order_json")) {
    db.exec("ALTER TABLE prediction_feature_log_events ADD COLUMN start_display_order_json TEXT");
  }
  if (!names.has("start_display_timing_json")) {
    db.exec("ALTER TABLE prediction_feature_log_events ADD COLUMN start_display_timing_json TEXT");
  }
  if (!names.has("start_display_raw_json")) {
    db.exec("ALTER TABLE prediction_feature_log_events ADD COLUMN start_display_raw_json TEXT");
  }
}

ensurePredictionFeatureEventColumns();

const upsertSnapshotStmt = db.prepare(`
  INSERT INTO prediction_feature_logs (
    race_id,
    race_date,
    venue_id,
    venue_name,
    race_no,
    race_grade,
    weather,
    wind,
    wave,
    motor_rate_avg,
    boat_rate_avg,
    avg_st_avg,
    exhibition_time_avg,
    start_display_order_json,
    start_display_st_json,
    start_display_timing_json,
    start_display_raw_json,
    start_display_signature,
    predicted_entry_order_json,
    actual_entry_order_json,
    entry_changed,
    entry_change_type,
    ranking_score,
    recommendation_score,
    confidence,
    recommendation_mode,
    prediction_snapshot_json,
    prediction_before_entry_change_json,
    prediction_after_entry_change_json,
    updated_at
  ) VALUES (
    @race_id,
    @race_date,
    @venue_id,
    @venue_name,
    @race_no,
    @race_grade,
    @weather,
    @wind,
    @wave,
    @motor_rate_avg,
    @boat_rate_avg,
    @avg_st_avg,
    @exhibition_time_avg,
    @start_display_order_json,
    @start_display_st_json,
    @start_display_timing_json,
    @start_display_raw_json,
    @start_display_signature,
    @predicted_entry_order_json,
    @actual_entry_order_json,
    @entry_changed,
    @entry_change_type,
    @ranking_score,
    @recommendation_score,
    @confidence,
    @recommendation_mode,
    @prediction_snapshot_json,
    @prediction_before_entry_change_json,
    @prediction_after_entry_change_json,
    @updated_at
  )
  ON CONFLICT(race_id) DO UPDATE SET
    race_date = excluded.race_date,
    venue_id = excluded.venue_id,
    venue_name = excluded.venue_name,
    race_no = excluded.race_no,
    race_grade = excluded.race_grade,
    weather = excluded.weather,
    wind = excluded.wind,
    wave = excluded.wave,
    motor_rate_avg = excluded.motor_rate_avg,
    boat_rate_avg = excluded.boat_rate_avg,
    avg_st_avg = excluded.avg_st_avg,
    exhibition_time_avg = excluded.exhibition_time_avg,
    start_display_st_json = excluded.start_display_st_json,
    start_display_signature = excluded.start_display_signature,
    predicted_entry_order_json = excluded.predicted_entry_order_json,
    actual_entry_order_json = excluded.actual_entry_order_json,
    entry_changed = excluded.entry_changed,
    entry_change_type = excluded.entry_change_type,
    ranking_score = excluded.ranking_score,
    recommendation_score = excluded.recommendation_score,
    confidence = excluded.confidence,
    recommendation_mode = excluded.recommendation_mode,
    prediction_snapshot_json = excluded.prediction_snapshot_json,
    prediction_before_entry_change_json = excluded.prediction_before_entry_change_json,
    prediction_after_entry_change_json = excluded.prediction_after_entry_change_json,
    updated_at = excluded.updated_at
`);

const attachSettlementStmt = db.prepare(`
  UPDATE prediction_feature_logs
  SET
    actual_result = @actual_result,
    hit_flag = @hit_flag,
    settled_bet_hit_count = @settled_bet_hit_count,
    settled_bet_count = @settled_bet_count,
    updated_at = @updated_at
  WHERE race_id = @race_id
`);

const insertSnapshotEventStmt = db.prepare(`
  INSERT INTO prediction_feature_log_events (
    race_id,
    race_key,
    race_date,
    venue_id,
    venue_name,
    race_no,
    race_grade,
    weather,
    wind,
    wave,
    motor_rate_avg,
    boat_rate_avg,
    avg_st_avg,
    exhibition_time_avg,
    start_display_st_json,
    start_display_signature,
    predicted_entry_order_json,
    actual_entry_order_json,
    entry_changed,
    entry_change_type,
    ranking_score,
    recommendation_score,
    confidence,
    recommendation_mode,
    prediction_snapshot_json,
    prediction_before_entry_change_json,
    prediction_after_entry_change_json,
    source_timestamp
  ) VALUES (
    @race_id,
    @race_key,
    @race_date,
    @venue_id,
    @venue_name,
    @race_no,
    @race_grade,
    @weather,
    @wind,
    @wave,
    @motor_rate_avg,
    @boat_rate_avg,
    @avg_st_avg,
    @exhibition_time_avg,
    @start_display_st_json,
    @start_display_signature,
    @predicted_entry_order_json,
    @actual_entry_order_json,
    @entry_changed,
    @entry_change_type,
    @ranking_score,
    @recommendation_score,
    @confidence,
    @recommendation_mode,
    @prediction_snapshot_json,
    @prediction_before_entry_change_json,
    @prediction_after_entry_change_json,
    @source_timestamp
  )
`);

const attachSettlementEventsStmt = db.prepare(`
  UPDATE prediction_feature_log_events
  SET
    actual_result = @actual_result,
    hit_flag = @hit_flag,
    settled_bet_hit_count = @settled_bet_hit_count,
    settled_bet_count = @settled_bet_count
  WHERE race_id = @race_id
    AND (actual_result IS NULL OR TRIM(actual_result) = '')
`);

const selectLatestPredictionTop3Stmt = db.prepare(`
  SELECT top3_json
  FROM prediction_logs
  WHERE race_id = ?
  ORDER BY id DESC
  LIMIT 1
`);

function avgBy(racers, key) {
  const rows = Array.isArray(racers) ? racers : [];
  const values = rows.map((r) => toNum(r?.[key], null)).filter((v) => Number.isFinite(v));
  if (!values.length) return null;
  return Number((values.reduce((a, b) => a + b, 0) / values.length).toFixed(4));
}

export function savePredictionFeatureLog({
  raceId,
  race,
  racers,
  startDisplay,
  entryMeta,
  raceDecision,
  predictionSnapshot,
  predictionBeforeEntryChange,
  predictionAfterEntryChange
}) {
  if (!raceId) return;
  const payload = {
    race_id: String(raceId),
    race_key: String(raceId),
    race_date: race?.date || null,
    venue_id: toNum(race?.venueId, null),
    venue_name: race?.venueName || null,
    race_no: toNum(race?.raceNo, null),
    race_grade: race?.grade || race?.raceGrade || null,
    weather: race?.weather || null,
    wind: toNum(race?.windSpeed, null),
    wave: toNum(race?.waveHeight, null),
    motor_rate_avg: avgBy(racers, "motor2Rate"),
    boat_rate_avg: avgBy(racers, "boat2Rate"),
    avg_st_avg: avgBy(racers, "avgSt"),
    exhibition_time_avg: avgBy(racers, "exhibitionTime"),
    start_display_order_json: JSON.stringify(startDisplay?.start_display_order || []),
    start_display_st_json: JSON.stringify(startDisplay?.start_display_st || {}),
    start_display_timing_json: JSON.stringify(startDisplay?.start_display_timing || {}),
    start_display_raw_json: JSON.stringify(startDisplay?.start_display_raw || {}),
    start_display_signature: startDisplay?.start_display_signature || null,
    predicted_entry_order_json: JSON.stringify(entryMeta?.predicted_entry_order || []),
    actual_entry_order_json: JSON.stringify(entryMeta?.actual_entry_order || []),
    entry_changed: entryMeta?.entry_changed ? 1 : 0,
    entry_change_type: entryMeta?.entry_change_type || null,
    ranking_score: toNum(raceDecision?.race_select_score, null),
    recommendation_score: toNum(raceDecision?.race_select_score, null),
    confidence: toNum(raceDecision?.confidence, null),
    recommendation_mode: raceDecision?.mode || null,
    prediction_snapshot_json: JSON.stringify(predictionSnapshot || {}),
    prediction_before_entry_change_json: JSON.stringify(predictionBeforeEntryChange || {}),
    prediction_after_entry_change_json: JSON.stringify(predictionAfterEntryChange || {}),
    source_timestamp: startDisplay?.source_fetched_at || null,
    updated_at: nowIso()
  };
  upsertSnapshotStmt.run(payload);
  insertSnapshotEventStmt.run(payload);
}

export function attachPredictionFeatureLogSettlement({
  raceId,
  actualResult,
  settledBetHitCount = null,
  settledBetCount = null
}) {
  if (!raceId || !actualResult) return;

  const top3Row = selectLatestPredictionTop3Stmt.get(String(raceId));
  const top3 = safeJson(top3Row?.top3_json, []);
  const predictedCombo =
    Array.isArray(top3) && top3.length >= 3
      ? `${toNum(top3[0], 0)}-${toNum(top3[1], 0)}-${toNum(top3[2], 0)}`
      : null;
  const actualCombo = String(actualResult || "");
  const hitFlag = predictedCombo && actualCombo && predictedCombo === actualCombo ? 1 : 0;

  attachSettlementStmt.run({
    race_id: String(raceId),
    actual_result: actualCombo,
    hit_flag: hitFlag,
    settled_bet_hit_count: toNum(settledBetHitCount, null),
    settled_bet_count: toNum(settledBetCount, null),
    updated_at: nowIso()
  });
  attachSettlementEventsStmt.run({
    race_id: String(raceId),
    actual_result: actualCombo,
    hit_flag: hitFlag,
    settled_bet_hit_count: toNum(settledBetHitCount, null),
    settled_bet_count: toNum(settledBetCount, null)
  });
}
