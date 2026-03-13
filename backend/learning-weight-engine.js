import db from "./db.js";
import { buildVerifiedLearningRows } from "./prediction-snapshot-store.js";

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function nowIso() {
  return new Date().toISOString();
}

function safeJsonParse(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

export const DEFAULT_BATCH_WEIGHTS = {
  ranking_weight: 1.0,
  recommendation_threshold: 52,
  venue_correction_weight: 1.0,
  grade_correction_weight: 1.0,
  entry_changed_penalty: 6,
  start_signal_weight: 1.0,
  venue_score_adjustments: {},
  grade_score_adjustments: {},
  start_signature_score_adjustments: {},
  segmented_corrections: {},
  segmented_learning_metadata: {
    learned_segment_count: 0,
    by_type: {}
  },
  confidence_calibration: {
    high_min: 80,
    medium_min: 60
  }
};

export const AUTO_LEARNING_MIN_NEW_READY = 2;
export const AUTO_LEARNING_MIN_READY_TOTAL = 8;
let continuousLearningInFlight = false;
let queuedContinuousLearningRequest = false;

function ensureLearningTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS learning_weight_runs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      mode TEXT NOT NULL,
      sample_size INTEGER NOT NULL DEFAULT 0,
      base_weights_json TEXT NOT NULL,
      suggested_weights_json TEXT NOT NULL,
      applied_weights_json TEXT,
      summary TEXT,
      reverted_from_run_id INTEGER
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS learning_weight_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      active_weights_json TEXT NOT NULL,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      last_run_id INTEGER
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS learning_segment_corrections (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      learning_run_id INTEGER NOT NULL,
      learned_segment_type TEXT NOT NULL,
      learned_segment_key TEXT NOT NULL,
      confidence_bucket TEXT,
      sample_count INTEGER NOT NULL DEFAULT 0,
      hit_rate REAL,
      head_hit_rate REAL,
      bet_hit_rate REAL,
      confidence_error REAL,
      correction_values_json TEXT NOT NULL,
      calibration_adjustment_json TEXT,
      learned_at TEXT DEFAULT CURRENT_TIMESTAMP,
      calibrated_at TEXT
    )
  `);
  const segmentCols = db.prepare(`PRAGMA table_info(learning_segment_corrections)`).all();
  const segmentNames = new Set(segmentCols.map((c) => String(c.name)));
  if (!segmentNames.has("confidence_bucket")) {
    db.exec("ALTER TABLE learning_segment_corrections ADD COLUMN confidence_bucket TEXT");
  }
  if (!segmentNames.has("calibration_adjustment_json")) {
    db.exec("ALTER TABLE learning_segment_corrections ADD COLUMN calibration_adjustment_json TEXT");
  }
  if (!segmentNames.has("calibrated_at")) {
    db.exec("ALTER TABLE learning_segment_corrections ADD COLUMN calibrated_at TEXT");
  }
  const row = db.prepare(`SELECT id FROM learning_weight_state WHERE id = 1`).get();
  if (!row) {
    db.prepare(`
      INSERT INTO learning_weight_state (id, active_weights_json, updated_at, last_run_id)
      VALUES (1, ?, ?, NULL)
    `).run(JSON.stringify(DEFAULT_BATCH_WEIGHTS), nowIso());
  }
  db.exec(`
    CREATE TABLE IF NOT EXISTS learning_runtime_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      last_processed_verification_log_id INTEGER NOT NULL DEFAULT 0,
      last_learning_run_id INTEGER,
      last_learning_run_at TEXT,
      last_verified_records_used INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);
  const runtime = db.prepare(`SELECT id FROM learning_runtime_state WHERE id = 1`).get();
  if (!runtime) {
    db.prepare(
      `
      INSERT INTO learning_runtime_state (
        id,
        last_processed_verification_log_id,
        last_learning_run_id,
        last_learning_run_at,
        last_verified_records_used,
        updated_at
      ) VALUES (1, 0, NULL, NULL, 0, ?)
    `
    ).run(nowIso());
  }
  const runtimeCols = db.prepare(`PRAGMA table_info(learning_runtime_state)`).all();
  const runtimeColNames = new Set(runtimeCols.map((c) => String(c.name)));
  if (!runtimeColNames.has("last_remaining_learning_ready")) {
    db.exec("ALTER TABLE learning_runtime_state ADD COLUMN last_remaining_learning_ready INTEGER NOT NULL DEFAULT 0");
  }
  if (!runtimeColNames.has("last_used_learning_ready_count")) {
    db.exec("ALTER TABLE learning_runtime_state ADD COLUMN last_used_learning_ready_count INTEGER NOT NULL DEFAULT 0");
  }
  if (!runtimeColNames.has("last_learning_trigger_mode")) {
    db.exec("ALTER TABLE learning_runtime_state ADD COLUMN last_learning_trigger_mode TEXT");
  }
  if (!runtimeColNames.has("learning_job_running")) {
    db.exec("ALTER TABLE learning_runtime_state ADD COLUMN learning_job_running INTEGER NOT NULL DEFAULT 0");
  }
  if (!runtimeColNames.has("queued_auto_learning")) {
    db.exec("ALTER TABLE learning_runtime_state ADD COLUMN queued_auto_learning INTEGER NOT NULL DEFAULT 0");
  }
}

ensureLearningTables();

function ensureVerificationLearningColumns() {
  const cols = db.prepare(`PRAGMA table_info(race_verification_logs)`).all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("is_invalid_verification")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN is_invalid_verification INTEGER NOT NULL DEFAULT 0");
  }
  if (!names.has("exclude_from_learning")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN exclude_from_learning INTEGER NOT NULL DEFAULT 0");
  }
}

function loadLearningDataset() {
  return buildVerifiedLearningRows();
}

function buildGlobalFromDataset(rows) {
  const dataset = Array.isArray(rows) ? rows : [];
  const races = dataset.length;
  const hits = dataset.filter((row) => toNum(row?.hit_flag, 0) === 1).length;
  const hitRate = races > 0 ? (hits / races) * 100 : 0;
  return {
    races,
    hits,
    hitRate: Number(hitRate.toFixed(2))
  };
}

function loadGlobalHitRate() {
  return buildGlobalFromDataset(loadLearningDataset());
}

function buildBucketAdjustments({ column, minSample, scale, cap }) {
  const dataset = loadLearningDataset();
  const global = buildGlobalFromDataset(dataset);
  const bucketMap = new Map();
  for (const row of dataset) {
    const keyValue = row?.[column];
    if (keyValue === null || keyValue === undefined || String(keyValue).trim() === "") continue;
    const key = String(keyValue);
    const bucket = bucketMap.get(key) || { key, hits: 0, races: 0 };
    bucket.races += 1;
    if (toNum(row?.hit_flag, 0) === 1) bucket.hits += 1;
    bucketMap.set(key, bucket);
  }
  const rows = Array.from(bucketMap.values());

  const out = {};
  for (const row of rows) {
    const key = String(row.key);
    const races = toNum(row.races, 0);
    if (races < minSample) continue;
    const hits = toNum(row.hits, 0);
    const hitRate = races > 0 ? (hits / races) * 100 : 0;
    const delta = hitRate - global.hitRate;
    const adj = clamp(-cap, cap, (delta / 10) * scale);
    out[key] = Number(adj.toFixed(3));
  }
  return {
    global,
    adjustments: out
  };
}

function buildEntryChangedPenalty(basePenalty) {
  const rows = loadLearningDataset().reduce((acc, row) => {
    const key = toNum(row?.entry_changed, 0);
    const bucket = acc.get(key) || { entry_changed: key, hits: 0, races: 0 };
    bucket.races += 1;
    if (toNum(row?.hit_flag, 0) === 1) bucket.hits += 1;
    acc.set(key, bucket);
    return acc;
  }, new Map());
  const map = rows;
  const changed = map.get(1);
  const normal = map.get(0);
  if (!changed || !normal) return basePenalty;
  if (toNum(changed.races, 0) < 25 || toNum(normal.races, 0) < 25) return basePenalty;
  const changedRate = (toNum(changed.hits, 0) / Math.max(1, toNum(changed.races, 1))) * 100;
  const normalRate = (toNum(normal.hits, 0) / Math.max(1, toNum(normal.races, 1))) * 100;
  const delta = normalRate - changedRate;
  const next = basePenalty + clamp(-1.5, 3.0, delta * 0.12);
  return Number(clamp(3, 12, next).toFixed(3));
}

function buildRecommendationThreshold(baseThreshold) {
  const global = loadGlobalHitRate();
  if (global.races < 40) return baseThreshold;
  let next = baseThreshold;
  if (global.hitRate < 24) next += 2.2;
  else if (global.hitRate < 30) next += 1.0;
  else if (global.hitRate > 40) next -= 1.0;
  return Number(clamp(44, 68, next).toFixed(2));
}

function buildRankingWeight(base) {
  const row = loadLearningDataset()
    .reduce((acc, item) => {
      const mode = String(item?.recommendation_mode || "").toUpperCase();
      const bucket = acc.get(mode) || { recommendation_mode: mode, hits: 0, races: 0 };
      bucket.races += 1;
      if (toNum(item?.hit_flag, 0) === 1) bucket.hits += 1;
      acc.set(mode, bucket);
      return acc;
    }, new Map())
    .get("FULL_BET");
  if (!row || toNum(row.races, 0) < 25) return base;
  const rate = (toNum(row.hits, 0) / Math.max(1, toNum(row.races, 1))) * 100;
  const delta = clamp(-0.08, 0.08, (rate - 33) / 200);
  return Number(clamp(0.88, 1.18, base + delta).toFixed(4));
}

function buildStartSignalWeight(base) {
  const row = loadLearningDataset().reduce(
    (acc, item) => {
      if (!item?.start_display_signature) return acc;
      acc.races += 1;
      if (toNum(item?.hit_flag, 0) === 1) acc.hits += 1;
      return acc;
    },
    { hits: 0, races: 0 }
  );
  const races = toNum(row?.races, 0);
  if (races < 40) return base;
  const rate = (toNum(row?.hits, 0) / Math.max(1, races)) * 100;
  const delta = clamp(-0.1, 0.1, (rate - 32) / 180);
  return Number(clamp(0.85, 1.2, base + delta).toFixed(4));
}

function loadVerificationMismatchInsights() {
  const rows = loadLearningDataset().map((row) => ({
    race_id: row.race_id,
    hit_miss: row.hit_miss,
    mismatch_categories_json: JSON.stringify(row.mismatch_categories || [])
  }));

  const total = Array.isArray(rows) ? rows.length : 0;
  const bucket = {
    HEAD_MISS: 0,
    PARTNER_MISS: 0,
    ENTRY_CHANGE_IMPACT: 0,
    EXHIBITION_WEIGHT_BIAS: 0,
    MOTOR_WEIGHT_BIAS: 0,
    PLAYER_WEIGHT_BIAS: 0,
    UNDERSPREAD: 0,
    OVERSPREAD: 0
  };

  for (const row of rows) {
    const categories = safeJsonParse(row?.mismatch_categories_json, []);
    const set = new Set(Array.isArray(categories) ? categories.map((x) => String(x || "").toUpperCase()) : []);
    for (const code of Object.keys(bucket)) {
      if (set.has(code)) bucket[code] += 1;
    }
  }

  const rate = (value) => (total > 0 ? Number(((value / total) * 100).toFixed(2)) : 0);
  return {
    sample_size: total,
    counts: bucket,
    rates: Object.fromEntries(Object.entries(bucket).map(([k, v]) => [k, rate(v)]))
  };
}

function getVerificationQueueStats() {
  ensureVerificationLearningColumns();
  const rows = db
    .prepare(
      `
      SELECT
        id,
        learning_ready,
        verification_status,
        mismatch_categories_json,
        is_invalid_verification,
        exclude_from_learning
      FROM race_verification_logs
      ORDER BY id ASC
    `
    )
    .all();
  const total = Array.isArray(rows) ? rows.length : 0;
  const maxId = total > 0 ? toNum(rows[rows.length - 1]?.id, 0) : 0;
  let learningReadyTotal = 0;
  for (const row of rows) {
    if (isLearningReadyVerificationRow(row)) learningReadyTotal += 1;
  }
  return {
    rows,
    total,
    maxId,
    learningReadyTotal
  };
}

function isLearningReadyVerificationRow(row) {
  if (Number(row?.is_invalid_verification) === 1) return false;
  if (Number(row?.exclude_from_learning) === 1) return false;
  const categories = safeJsonParse(row?.mismatch_categories_json, []);
  const explicitReady = Number(row?.learning_ready) === 1;
  const verified = String(row?.verification_status || "").toUpperCase().startsWith("VERIFIED");
  return verified && (explicitReady || (Array.isArray(categories) && categories.length > 0));
}

function countPendingLearningReady(rows, lastProcessedId) {
  return (Array.isArray(rows) ? rows : []).filter((row) => {
    if (toNum(row?.id, 0) <= toNum(lastProcessedId, 0)) return false;
    return isLearningReadyVerificationRow(row);
  }).length;
}

function updateLearningRuntimeState({
  lastProcessedVerificationLogId,
  runId,
  runAt,
  usedVerificationCount,
  remainingLearningReady,
  triggerMode = null,
  learningJobRunning = 0,
  queuedAutoLearning = 0
}) {
  db.prepare(
    `
    UPDATE learning_runtime_state
    SET
      last_processed_verification_log_id = ?,
      last_learning_run_id = ?,
      last_learning_run_at = ?,
      last_verified_records_used = ?,
      last_used_learning_ready_count = ?,
      last_remaining_learning_ready = ?,
      last_learning_trigger_mode = ?,
      learning_job_running = ?,
      queued_auto_learning = ?,
      updated_at = ?
    WHERE id = 1
  `
  ).run(
    toNum(lastProcessedVerificationLogId, 0),
    toNum(runId, null),
    runAt || nowIso(),
    toNum(usedVerificationCount, 0),
    toNum(usedVerificationCount, 0),
    toNum(remainingLearningReady, 0),
    triggerMode || null,
    toNum(learningJobRunning, 0),
    toNum(queuedAutoLearning, 0),
    nowIso()
  );
}

function markLearningJobState({ running, queued, triggerMode = null } = {}) {
  db.prepare(
    `
    UPDATE learning_runtime_state
    SET
      learning_job_running = ?,
      queued_auto_learning = ?,
      last_learning_trigger_mode = COALESCE(?, last_learning_trigger_mode),
      updated_at = ?
    WHERE id = 1
  `
  ).run(
    running ? 1 : 0,
    queued ? 1 : 0,
    triggerMode || null,
    nowIso()
  );
}

function loadPredictionFeatureInsights() {
  const rows = loadLearningDataset().map((row) => ({
    hit_flag: row.hit_flag,
    motor_rate_avg: row.motor_rate_avg,
    exhibition_time_avg: row.exhibition_time_avg,
    avg_st_avg: row.avg_st_avg,
    ranking_score: row.recommendation_score,
    confidence: row.bet_confidence ?? row.confidence,
    entry_changed: row.entry_changed
  }));
  const bucket = {
    hit: { count: 0, motor: 0, exTime: 0, avgSt: 0, rank: 0, conf: 0, entryChanged: 0 },
    miss: { count: 0, motor: 0, exTime: 0, avgSt: 0, rank: 0, conf: 0, entryChanged: 0 }
  };
  for (const row of rows) {
    const key = toNum(row?.hit_flag, 0) === 1 ? "hit" : "miss";
    const b = bucket[key];
    b.count += 1;
    b.motor += toNum(row?.motor_rate_avg, 0);
    b.exTime += toNum(row?.exhibition_time_avg, 0);
    b.avgSt += toNum(row?.avg_st_avg, 0);
    b.rank += toNum(row?.ranking_score, 0);
    b.conf += toNum(row?.confidence, 0);
    b.entryChanged += toNum(row?.entry_changed, 0) ? 1 : 0;
  }
  const avg = (sum, count) => (count > 0 ? sum / count : 0);
  const hit = bucket.hit;
  const miss = bucket.miss;
  return {
    sample_size: hit.count + miss.count,
    hit_count: hit.count,
    miss_count: miss.count,
    deltas: {
      motor_rate: Number((avg(hit.motor, hit.count) - avg(miss.motor, miss.count)).toFixed(4)),
      exhibition_time: Number((avg(miss.exTime, miss.count) - avg(hit.exTime, hit.count)).toFixed(4)),
      avg_st: Number((avg(miss.avgSt, miss.count) - avg(hit.avgSt, hit.count)).toFixed(4)),
      ranking_score: Number((avg(hit.rank, hit.count) - avg(miss.rank, miss.count)).toFixed(4)),
      confidence: Number((avg(hit.conf, hit.count) - avg(miss.conf, miss.count)).toFixed(4)),
      entry_changed_rate: Number((avg(miss.entryChanged, miss.count) - avg(hit.entryChanged, hit.count)).toFixed(4))
    }
  };
}

function joinPattern(value) {
  return Array.isArray(value) && value.length ? value.join("-") : null;
}

function confidenceBandKey(confidence) {
  const c = toNum(confidence, null);
  if (!Number.isFinite(c)) return null;
  const cfg = DEFAULT_BATCH_WEIGHTS.confidence_calibration || {};
  const highMin = toNum(cfg.high_min, 80);
  const mediumMin = toNum(cfg.medium_min, 60);
  if (c >= highMin) return "high";
  if (c >= mediumMin) return "medium";
  return "low";
}

function scenarioMatchBucket(score) {
  const s = toNum(score, null);
  if (!Number.isFinite(s)) return null;
  if (s >= 70) return "high";
  if (s >= 55) return "medium";
  return "low";
}

function overlapBucket(overlapLanes) {
  const count = Array.isArray(overlapLanes) ? overlapLanes.length : 0;
  if (count >= 2) return "strong";
  if (count === 1) return "partial";
  return "none";
}

function fHoldZoneKey(playerContext) {
  const players = Array.isArray(playerContext) ? playerContext : [];
  const inside = players.some((row) => Number(row?.f_hold_bias_applied) === 1 && Number(row?.lane) >= 1 && Number(row?.lane) <= 3);
  const outside = players.some((row) => Number(row?.f_hold_bias_applied) === 1 && Number(row?.lane) >= 4);
  if (inside && outside) return "mixed";
  if (inside) return "inside";
  if (outside) return "outside";
  return "none";
}

function buildSegmentEntries(row) {
  const segments = [];
  const add = (type, key) => {
    if (key === null || key === undefined || String(key).trim() === "") return;
    segments.push({ type, key: String(key) });
  };

  add("venue", row?.venue_id ?? row?.venue_name);
  add("predicted_entry_pattern", joinPattern(row?.predicted_entry_order));
  add("actual_entry_pattern", joinPattern(row?.actual_entry_order));
  add("entry_change_present", toNum(row?.entry_changed, 0) ? "changed" : "unchanged");
  add("entry_type", row?.entry_change_type || null);
  add("formation_pattern", row?.formation_pattern || null);
  add("scenario_type", row?.scenario_type || null);
  add("scenario_match_bucket", scenarioMatchBucket(row?.scenario_match_score));
  add("has_f_hold", toNum(row?.f_hold_lane_count, 0) > 0 ? "yes" : "no");
  add("f_hold_zone", fHoldZoneKey(row?.player_context));
  add("motor_exhibition_overlap_bucket", overlapBucket(row?.overlap_lanes));
  add("participation_decision_state", row?.participation_decision || null);
  const headBand = confidenceBandKey(row?.head_confidence);
  const betBand = confidenceBandKey(row?.bet_confidence ?? row?.confidence);
  add("head_confidence_band", headBand);
  add("bet_confidence_band", betBand);
  if (headBand) add("head_confidence_behavior", `${headBand}_${toNum(row?.head_hit, 0) === 1 ? "hit" : "miss"}`);
  if (betBand) add("bet_confidence_behavior", `${betBand}_${toNum(row?.bet_hit, 0) === 1 ? "hit" : "miss"}`);
  return segments;
}

function buildSegmentCorrectionPack(dataset) {
  const rows = Array.isArray(dataset) ? dataset : [];
  const globalHitRate = rows.length
    ? (rows.filter((row) => toNum(row?.hit_flag, 0) === 1).length / rows.length) * 100
    : 0;
  const globalHeadHitRate = rows.length
    ? (rows.filter((row) => toNum(row?.head_hit, 0) === 1).length / rows.length) * 100
    : 0;
  const segmentStats = new Map();

  for (const row of rows) {
    const entries = buildSegmentEntries(row);
    const betConfidencePct = toNum(row?.bet_confidence ?? row?.confidence, 50);
    const headConfidencePct = toNum(row?.head_confidence, betConfidencePct);
    const expectedBet = toNum(row?.bet_hit ?? row?.hit_flag, 0) === 1 ? 100 : 0;
    const expectedHead = toNum(row?.head_hit, 0) === 1 ? 100 : 0;
    const confidenceError = betConfidencePct - expectedBet;
    const headConfidenceError = headConfidencePct - expectedHead;
    for (const entry of entries) {
      const mapKey = `${entry.type}::${entry.key}`;
      const bucket = segmentStats.get(mapKey) || {
        type: entry.type,
        key: entry.key,
        sample_count: 0,
        hit_count: 0,
        head_hit_count: 0,
        bet_hit_count: 0,
        confidence_error_sum: 0,
        head_confidence_error_sum: 0
      };
      bucket.sample_count += 1;
      bucket.hit_count += toNum(row?.hit_flag, 0) === 1 ? 1 : 0;
      bucket.head_hit_count += toNum(row?.head_hit, 0) === 1 ? 1 : 0;
      bucket.bet_hit_count += toNum(row?.bet_hit, 0) === 1 ? 1 : 0;
      bucket.confidence_error_sum += confidenceError;
      bucket.head_confidence_error_sum += headConfidenceError;
      segmentStats.set(mapKey, bucket);
    }
  }

  const minSamplesByType = {
    venue: 12,
    predicted_entry_pattern: 10,
    actual_entry_pattern: 10,
    entry_change_present: 12,
    entry_type: 10,
    formation_pattern: 10,
    scenario_type: 10,
    scenario_match_bucket: 10,
    has_f_hold: 10,
    f_hold_zone: 8,
    motor_exhibition_overlap_bucket: 10,
    participation_decision_state: 10,
    head_confidence_band: 12,
    bet_confidence_band: 12,
    head_confidence_behavior: 12,
    bet_confidence_behavior: 12
  };

  const grouped = {};
  const persistedRows = [];
  for (const bucket of segmentStats.values()) {
    const minSample = toNum(minSamplesByType[bucket.type], 12);
    if (bucket.sample_count < minSample) continue;
    const hitRate = (bucket.hit_count / Math.max(1, bucket.sample_count)) * 100;
    const headHitRate = (bucket.head_hit_count / Math.max(1, bucket.sample_count)) * 100;
    const betHitRate = (bucket.bet_hit_count / Math.max(1, bucket.sample_count)) * 100;
    const confidenceError = bucket.confidence_error_sum / Math.max(1, bucket.sample_count);
    const headConfidenceError = bucket.head_confidence_error_sum / Math.max(1, bucket.sample_count);
    const performanceDelta = hitRate - globalHitRate;
    const headDelta = headHitRate - globalHeadHitRate;
    const isHighMiss = (bucket.type === "head_confidence_behavior" || bucket.type === "bet_confidence_behavior") && String(bucket.key).includes("high_miss");
    const isLowHit = (bucket.type === "head_confidence_behavior" || bucket.type === "bet_confidence_behavior") && String(bucket.key).includes("low_hit");
    const calibrationTilt = isHighMiss ? -1.35 : isLowHit ? 0.85 : 0;
    const correctionValues = {
      head_confidence_correction: Number(clamp(-6, 6, headDelta * 0.18 - headConfidenceError * 0.04 + calibrationTilt).toFixed(3)),
      bet_confidence_correction: Number(clamp(-6, 6, performanceDelta * 0.22 - confidenceError * 0.05 + calibrationTilt).toFixed(3)),
      participate_watch_skip_correction: Number(clamp(-5, 5, performanceDelta * 0.14 + calibrationTilt * 0.8).toFixed(3)),
      second_place_bias_correction: Number(
        clamp(-3, 3, (bucket.type === "formation_pattern" || bucket.type === "venue" ? performanceDelta * 0.08 : 0)).toFixed(3)
      ),
      caution_penalty_correction: Number(clamp(-2.5, 2.5, confidenceError * 0.02 - performanceDelta * 0.03).toFixed(3)),
      f_hold_penalty_adjustment: Number(clamp(-2, 2, (bucket.type === "has_f_hold" || bucket.type === "f_hold_zone" ? confidenceError * 0.018 : 0)).toFixed(3)),
      pattern_strength_adjustment: Number(
        clamp(-3, 3, (bucket.type === "formation_pattern" || bucket.type === "scenario_type" ? performanceDelta * 0.09 + headDelta * 0.04 : 0)).toFixed(3)
      ),
      recommendation_score_adjustment: Number(clamp(-4, 4, performanceDelta * 0.12).toFixed(3)),
      entry_changed_penalty_delta: Number(clamp(-2, 2, (bucket.type === "entry_change_present" || bucket.type === "entry_type" ? -performanceDelta * 0.06 : 0)).toFixed(3)),
      motor_lap_overlap_adjustment: Number(clamp(-3, 3, (bucket.type === "motor_exhibition_overlap_bucket" ? performanceDelta * 0.08 : 0)).toFixed(3))
    };

    if (!grouped[bucket.type]) grouped[bucket.type] = {};
    grouped[bucket.type][bucket.key] = {
      sample_count: bucket.sample_count,
      hit_rate: Number(hitRate.toFixed(2)),
      head_hit_rate: Number(headHitRate.toFixed(2)),
      bet_hit_rate: Number(betHitRate.toFixed(2)),
      confidence_error: Number(confidenceError.toFixed(3)),
      head_confidence_error: Number(headConfidenceError.toFixed(3)),
      correction_values: correctionValues
    };
    persistedRows.push({
      learned_segment_type: bucket.type,
      learned_segment_key: bucket.key,
      confidence_bucket:
        bucket.type === "head_confidence_behavior" || bucket.type === "head_confidence_band"
          ? String(bucket.key).split("_")[0]
          : bucket.type === "bet_confidence_behavior" || bucket.type === "bet_confidence_band"
            ? String(bucket.key).split("_")[0]
            : null,
      sample_count: bucket.sample_count,
      hit_rate: Number(hitRate.toFixed(2)),
      head_hit_rate: Number(headHitRate.toFixed(2)),
      bet_hit_rate: Number(betHitRate.toFixed(2)),
      confidence_error: Number(confidenceError.toFixed(3)),
      correction_values_json: correctionValues,
      calibration_adjustment_json: {
        head_confidence_raw_delta: correctionValues.head_confidence_correction,
        bet_confidence_raw_delta: correctionValues.bet_confidence_correction,
        participate_watch_skip_delta: correctionValues.participate_watch_skip_correction
      }
    });
  }

  const byType = Object.fromEntries(
    Object.entries(grouped).map(([type, entries]) => [type, Object.keys(entries).length])
  );

  return {
    grouped,
    persistedRows,
    metadata: {
      learned_segment_count: persistedRows.length,
      by_type: byType
    }
  };
}

function persistSegmentCorrections(runId, segmentRows) {
  const rows = Array.isArray(segmentRows) ? segmentRows : [];
  if (!rows.length || !toNum(runId, 0)) return 0;
  const stmt = db.prepare(
    `
    INSERT INTO learning_segment_corrections (
      learning_run_id,
      learned_segment_type,
      learned_segment_key,
      confidence_bucket,
      sample_count,
      hit_rate,
      head_hit_rate,
      bet_hit_rate,
      confidence_error,
      correction_values_json,
      calibration_adjustment_json,
      learned_at,
      calibrated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `
  );
  const tx = db.transaction((items) => {
    for (const row of items) {
      const timestamp = nowIso();
      stmt.run(
        toNum(runId, 0),
        String(row.learned_segment_type || ""),
        String(row.learned_segment_key || ""),
        row.confidence_bucket ? String(row.confidence_bucket) : null,
        toNum(row.sample_count, 0),
        toNum(row.hit_rate, null),
        toNum(row.head_hit_rate, null),
        toNum(row.bet_hit_rate, null),
        toNum(row.confidence_error, null),
        JSON.stringify(row.correction_values_json || {}),
        JSON.stringify(row.calibration_adjustment_json || {}),
        timestamp,
        timestamp
      );
    }
  });
  tx(rows);
  return rows.length;
}

export function getActiveLearningWeights() {
  const row = db.prepare(`SELECT active_weights_json FROM learning_weight_state WHERE id = 1`).get();
  return {
    ...DEFAULT_BATCH_WEIGHTS,
    ...safeJsonParse(row?.active_weights_json, {})
  };
}

function persistLearningRun({
  mode,
  sampleSize,
  baseWeights,
  suggestedWeights,
  appliedWeights,
  summary,
  revertedFromRunId = null
}) {
  const info = db
    .prepare(
      `
      INSERT INTO learning_weight_runs (
        mode,
        sample_size,
        base_weights_json,
        suggested_weights_json,
        applied_weights_json,
        summary,
        reverted_from_run_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `
    )
    .run(
      String(mode),
      toNum(sampleSize, 0),
      JSON.stringify(baseWeights || {}),
      JSON.stringify(suggestedWeights || {}),
      appliedWeights ? JSON.stringify(appliedWeights) : null,
      String(summary || ""),
      revertedFromRunId
    );
  return toNum(info?.lastInsertRowid, null);
}

function updateActiveState(appliedWeights, runId) {
  db.prepare(
    `
    UPDATE learning_weight_state
    SET active_weights_json = ?, updated_at = ?, last_run_id = ?
    WHERE id = 1
  `
  ).run(JSON.stringify(appliedWeights || {}), nowIso(), runId);
}

export function runLearningBatch({ apply = false, dryRun = true } = {}) {
  const base = getActiveLearningWeights();
  const dataset = loadLearningDataset();
  const venuePack = buildBucketAdjustments({
    column: "venue_id",
    minSample: 25,
    scale: 1.8,
    cap: 4.0
  });
  const gradePack = buildBucketAdjustments({
    column: "race_grade",
    minSample: 18,
    scale: 1.4,
    cap: 3.0
  });
  const signaturePack = buildBucketAdjustments({
    column: "start_display_signature",
    minSample: 20,
    scale: 1.2,
    cap: 2.5
  });
  const verificationPack = loadVerificationMismatchInsights();
  const featurePack = loadPredictionFeatureInsights();
  const segmentPack = buildSegmentCorrectionPack(dataset);

  const sampleSize = toNum(venuePack?.global?.races, 0);
  const suggested = {
    ...base,
    ranking_weight: buildRankingWeight(toNum(base.ranking_weight, 1)),
    recommendation_threshold: buildRecommendationThreshold(toNum(base.recommendation_threshold, 52)),
    venue_correction_weight: Number(clamp(0.8, 1.25, toNum(base.venue_correction_weight, 1)).toFixed(4)),
    grade_correction_weight: Number(clamp(0.8, 1.25, toNum(base.grade_correction_weight, 1)).toFixed(4)),
    entry_changed_penalty: buildEntryChangedPenalty(toNum(base.entry_changed_penalty, 6)),
    start_signal_weight: buildStartSignalWeight(toNum(base.start_signal_weight, 1)),
    venue_score_adjustments: venuePack.adjustments,
    grade_score_adjustments: gradePack.adjustments,
    start_signature_score_adjustments: signaturePack.adjustments,
    segmented_corrections: segmentPack.grouped,
    segmented_learning_metadata: segmentPack.metadata
  };
  const verificationAdjustments = [];
  const vr = verificationPack?.rates || {};
  if (toNum(verificationPack?.sample_size, 0) >= 20) {
    if (toNum(vr.HEAD_MISS, 0) >= 45) {
      suggested.ranking_weight = Number(clamp(0.88, 1.18, toNum(suggested.ranking_weight, 1) - 0.03).toFixed(4));
      verificationAdjustments.push("ranking_weight:-0.03(HEAD_MISS)");
    } else if (toNum(vr.HEAD_MISS, 0) <= 28) {
      suggested.ranking_weight = Number(clamp(0.88, 1.18, toNum(suggested.ranking_weight, 1) + 0.01).toFixed(4));
      verificationAdjustments.push("ranking_weight:+0.01(HEAD_STABLE)");
    }

    if (toNum(vr.ENTRY_CHANGE_IMPACT, 0) >= 20) {
      const bump = clamp(0.3, 1.4, toNum(vr.ENTRY_CHANGE_IMPACT, 0) / 20);
      suggested.entry_changed_penalty = Number(
        clamp(3, 12, toNum(suggested.entry_changed_penalty, 6) + bump).toFixed(3)
      );
      verificationAdjustments.push(`entry_changed_penalty:+${bump.toFixed(2)}`);
    }

    if (toNum(vr.EXHIBITION_WEIGHT_BIAS, 0) >= 16) {
      suggested.start_signal_weight = Number(
        clamp(0.85, 1.2, toNum(suggested.start_signal_weight, 1) - 0.03).toFixed(4)
      );
      verificationAdjustments.push("start_signal_weight:-0.03(EXHIBITION_BIAS)");
    }

    const mpBias = Math.max(toNum(vr.MOTOR_WEIGHT_BIAS, 0), toNum(vr.PLAYER_WEIGHT_BIAS, 0));
    if (mpBias >= 14) {
      suggested.recommendation_threshold = Number(
        clamp(44, 68, toNum(suggested.recommendation_threshold, 52) + 0.8).toFixed(2)
      );
      verificationAdjustments.push("recommendation_threshold:+0.8(MOTOR/PLAYER_BIAS)");
    }

    const underRate = toNum(vr.UNDERSPREAD, 0);
    const overRate = toNum(vr.OVERSPREAD, 0);
    if (underRate >= overRate + 8) {
      suggested.recommendation_threshold = Number(
        clamp(44, 68, toNum(suggested.recommendation_threshold, 52) - 0.6).toFixed(2)
      );
      verificationAdjustments.push("recommendation_threshold:-0.6(UNDERSPREAD)");
    } else if (overRate >= underRate + 8) {
      suggested.recommendation_threshold = Number(
        clamp(44, 68, toNum(suggested.recommendation_threshold, 52) + 0.6).toFixed(2)
      );
      verificationAdjustments.push("recommendation_threshold:+0.6(OVERSPREAD)");
    }
  }
  if (toNum(featurePack?.sample_size, 0) >= 40) {
    const fd = featurePack?.deltas || {};
    // Faster exhibition/avg_st separation in hit races -> trust start signal slightly more.
    if (toNum(fd.exhibition_time, 0) >= 0.03 || toNum(fd.avg_st, 0) >= 0.015) {
      suggested.start_signal_weight = Number(
        clamp(0.85, 1.2, toNum(suggested.start_signal_weight, 1) + 0.015).toFixed(4)
      );
      verificationAdjustments.push("start_signal_weight:+0.015(FEATURE_INSIGHT)");
    }
    // If ranking/confidence separation is weak, tighten recommendation threshold slightly.
    if (toNum(fd.ranking_score, 0) <= 3.5 && toNum(fd.confidence, 0) <= 2.0) {
      suggested.recommendation_threshold = Number(
        clamp(44, 68, toNum(suggested.recommendation_threshold, 52) + 0.5).toFixed(2)
      );
      verificationAdjustments.push("recommendation_threshold:+0.5(WEAK_SEPARATION)");
    }
    // Entry-changed misses are relatively frequent -> stronger entry penalty.
    if (toNum(fd.entry_changed_rate, 0) >= 0.08) {
      suggested.entry_changed_penalty = Number(
        clamp(3, 12, toNum(suggested.entry_changed_penalty, 6) + 0.4).toFixed(3)
      );
      verificationAdjustments.push("entry_changed_penalty:+0.4(FEATURE_INSIGHT)");
    }
  }

  const summary = `sample=${sampleSize}, global_hit=${toNum(
    venuePack?.global?.hitRate,
    0
  ).toFixed(2)}%, venueAdj=${Object.keys(venuePack.adjustments).length}, gradeAdj=${
    Object.keys(gradePack.adjustments).length
  }, sigAdj=${Object.keys(signaturePack.adjustments).length}, verified=${toNum(
    verificationPack?.sample_size,
    0
  )}, vAdj=${verificationAdjustments.length}`;

  if (!apply || dryRun) {
    const runId = persistLearningRun({
      mode: "dry_run",
      sampleSize,
      baseWeights: base,
      suggestedWeights: suggested,
      appliedWeights: null,
      summary
    });
    return {
      mode: "dry_run",
      run_id: runId,
      sample_size: sampleSize,
      base_weights: base,
      suggested_weights: suggested,
      applied_weights: null,
      summary,
      verification_insights: {
        ...verificationPack,
        adjustments_applied: verificationAdjustments
      },
      feature_insights: featurePack,
      segment_learning: segmentPack.metadata
    };
  }

  const runId = persistLearningRun({
    mode: "applied",
    sampleSize,
    baseWeights: base,
    suggestedWeights: suggested,
    appliedWeights: suggested,
    summary
  });
  persistSegmentCorrections(runId, segmentPack.persistedRows);
  updateActiveState(suggested, runId);
  return {
    mode: "applied",
    run_id: runId,
    sample_size: sampleSize,
    base_weights: base,
    suggested_weights: suggested,
    applied_weights: suggested,
    summary,
    verification_insights: {
      ...verificationPack,
      adjustments_applied: verificationAdjustments
    },
    feature_insights: featurePack,
    segment_learning: segmentPack.metadata
  };
}

export function applyLearningBatchManually({ apply = true, dryRun = false } = {}) {
  const queue = getVerificationQueueStats();
  const runtime = db.prepare(`SELECT * FROM learning_runtime_state WHERE id = 1`).get();
  const lastProcessed = toNum(runtime?.last_processed_verification_log_id, 0);
  const usedVerificationCount = countPendingLearningReady(queue?.rows || [], lastProcessed);
  const batch = runLearningBatch({ apply, dryRun });
  const runAt = nowIso();

  if (apply && !dryRun) {
    updateLearningRuntimeState({
      lastProcessedVerificationLogId: toNum(queue?.maxId, 0),
      runId: batch?.run_id,
      runAt,
      usedVerificationCount,
      remainingLearningReady: 0,
      triggerMode: "manual",
      learningJobRunning: 0,
      queuedAutoLearning: 0
    });
  }

  return {
    ...batch,
    learning_runtime: {
      last_learning_run_at: apply && !dryRun ? runAt : runtime?.last_learning_run_at || null,
      last_learning_run_id: apply && !dryRun ? toNum(batch?.run_id, null) : toNum(runtime?.last_learning_run_id, null),
      used_verification_count: usedVerificationCount,
      remaining_unlearned_count: apply && !dryRun ? 0 : countPendingLearningReady(queue?.rows || [], lastProcessed),
      threshold: {
        min_learning_ready_total: AUTO_LEARNING_MIN_READY_TOTAL,
        min_new_learning_ready: AUTO_LEARNING_MIN_NEW_READY
      },
      manual_run: true
    }
  };
}

export function rollbackLearningWeights({ runId = null } = {}) {
  let targetRun = null;
  if (runId) {
    targetRun = db.prepare(`SELECT * FROM learning_weight_runs WHERE id = ? LIMIT 1`).get(toNum(runId, 0));
  } else {
    const state = db.prepare(`SELECT last_run_id FROM learning_weight_state WHERE id = 1`).get();
    targetRun = state?.last_run_id
      ? db.prepare(`SELECT * FROM learning_weight_runs WHERE id = ? LIMIT 1`).get(state.last_run_id)
      : null;
  }
  if (!targetRun) {
    return {
      ok: false,
      message: "No target learning run found for rollback"
    };
  }
  const baseWeights = safeJsonParse(targetRun.base_weights_json, DEFAULT_BATCH_WEIGHTS);
  const rollbackRunId = persistLearningRun({
    mode: "rollback",
    sampleSize: toNum(targetRun.sample_size, 0),
    baseWeights: safeJsonParse(targetRun.applied_weights_json, baseWeights),
    suggestedWeights: baseWeights,
    appliedWeights: baseWeights,
    summary: `rollback from run ${targetRun.id}`,
    revertedFromRunId: targetRun.id
  });
  updateActiveState(baseWeights, rollbackRunId);
  return {
    ok: true,
    rolled_back_from_run_id: toNum(targetRun.id, null),
    rollback_run_id: rollbackRunId,
    active_weights: baseWeights
  };
}

export function getLatestLearningRun() {
  const latest = db.prepare(`SELECT * FROM learning_weight_runs ORDER BY id DESC LIMIT 1`).get();
  const state = db.prepare(`SELECT * FROM learning_weight_state WHERE id = 1`).get();
  const runtime = db.prepare(`SELECT * FROM learning_runtime_state WHERE id = 1`).get();
  const activeWeights = {
    ...DEFAULT_BATCH_WEIGHTS,
    ...safeJsonParse(state?.active_weights_json, {})
  };
  const queue = getVerificationQueueStats();
  const lastProcessedId = toNum(runtime?.last_processed_verification_log_id, 0);
  const pending = Math.max(0, toNum(queue?.maxId, 0) - lastProcessedId);
  const learningReadyPending = countPendingLearningReady(queue?.rows || [], lastProcessedId);
  return {
    latest_run: latest
      ? {
          id: latest.id,
          created_at: latest.created_at,
          mode: latest.mode,
          sample_size: latest.sample_size,
          base_weights: safeJsonParse(latest.base_weights_json, {}),
          suggested_weights: safeJsonParse(latest.suggested_weights_json, {}),
          applied_weights: safeJsonParse(latest.applied_weights_json, {}),
          summary: latest.summary,
          reverted_from_run_id: latest.reverted_from_run_id
        }
      : null,
    active_weights: activeWeights,
    active_updated_at: state?.updated_at || null,
    active_last_run_id: state?.last_run_id || null,
    segment_learning: activeWeights?.segmented_learning_metadata || {
      learned_segment_count: 0,
      by_type: {}
    },
    continuous_learning: {
      last_processed_verification_log_id: lastProcessedId,
      last_learning_run_id: runtime?.last_learning_run_id || null,
      last_learning_run_at: runtime?.last_learning_run_at || null,
      last_verified_records_used: toNum(runtime?.last_verified_records_used, 0),
      last_remaining_learning_ready: toNum(runtime?.last_remaining_learning_ready, learningReadyPending),
      last_learning_trigger_mode: runtime?.last_learning_trigger_mode || null,
      learning_job_running: toNum(runtime?.learning_job_running, 0),
      queued_auto_learning: toNum(runtime?.queued_auto_learning, 0),
      verification_total: toNum(queue?.total, 0),
      verification_pending: pending,
      learning_ready_total: toNum(queue?.learningReadyTotal, 0),
      learning_ready_pending: learningReadyPending,
      threshold: {
        min_learning_ready_total: AUTO_LEARNING_MIN_READY_TOTAL,
        min_new_learning_ready: AUTO_LEARNING_MIN_NEW_READY
      }
    }
  };
}

export function runContinuousLearningIfNeeded({
  minNewLearningReady = AUTO_LEARNING_MIN_NEW_READY,
  minLearningReadyTotal = AUTO_LEARNING_MIN_READY_TOTAL
} = {}) {
  if (continuousLearningInFlight) {
    queuedContinuousLearningRequest = true;
    markLearningJobState({ running: true, queued: true, triggerMode: "auto" });
    return {
      triggered: false,
      queued: true,
      reason: "already_running"
    };
  }

  const runtime = db.prepare(`SELECT * FROM learning_runtime_state WHERE id = 1`).get();
  const queue = getVerificationQueueStats();
  const total = toNum(queue?.total, 0);
  const maxId = toNum(queue?.maxId, 0);
  const learningReadyTotal = toNum(queue?.learningReadyTotal, 0);
  const lastProcessed = toNum(runtime?.last_processed_verification_log_id, 0);
  const newVerified = Math.max(0, maxId - lastProcessed);
  const newLearningReady = countPendingLearningReady(queue?.rows || [], lastProcessed);

  if (learningReadyTotal < minLearningReadyTotal) {
    return {
      triggered: false,
      queued: false,
      reason: "insufficient_learning_ready_total",
      total_verified: total,
      new_verified: newVerified,
      learning_ready_total: learningReadyTotal,
      new_learning_ready: newLearningReady
    };
  }

  if (newLearningReady < minNewLearningReady) {
    return {
      triggered: false,
      queued: false,
      reason: "not_enough_new_learning_ready",
      total_verified: total,
      new_verified: newVerified,
      learning_ready_total: learningReadyTotal,
      new_learning_ready: newLearningReady
    };
  }

  continuousLearningInFlight = true;
  queuedContinuousLearningRequest = false;
  markLearningJobState({ running: true, queued: false, triggerMode: "auto" });
  try {
    const batch = runLearningBatch({
      apply: true,
      dryRun: false
    });
    const runAt = nowIso();
    updateLearningRuntimeState({
      lastProcessedVerificationLogId: maxId,
      runId: batch?.run_id,
      runAt,
      usedVerificationCount: newLearningReady,
      remainingLearningReady: 0,
      triggerMode: "auto",
      learningJobRunning: 0,
      queuedAutoLearning: queuedContinuousLearningRequest ? 1 : 0
    });

    const result = {
      triggered: true,
      queued: false,
      reason: "applied",
      total_verified: total,
      new_verified: newVerified,
      learning_ready_total: learningReadyTotal,
      new_learning_ready: newLearningReady,
      run_id: toNum(batch?.run_id, null),
      summary: batch?.summary || null,
      run_at: runAt,
      used_verification_count: newLearningReady,
      remaining_unlearned_count: 0,
      threshold: {
        min_learning_ready_total: minLearningReadyTotal,
        min_new_learning_ready: minNewLearningReady
      }
    };

    return result;
  } finally {
    continuousLearningInFlight = false;
    const shouldRerun = queuedContinuousLearningRequest;
    queuedContinuousLearningRequest = false;
    markLearningJobState({ running: false, queued: false, triggerMode: "auto" });
    if (shouldRerun) {
      runContinuousLearningIfNeeded({ minNewLearningReady, minLearningReadyTotal });
    }
  }
}
