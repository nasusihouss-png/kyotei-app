import db from "./db.js";

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
  start_signature_score_adjustments: {}
};

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
  const row = db.prepare(`SELECT id FROM learning_weight_state WHERE id = 1`).get();
  if (!row) {
    db.prepare(`
      INSERT INTO learning_weight_state (id, active_weights_json, updated_at, last_run_id)
      VALUES (1, ?, ?, NULL)
    `).run(JSON.stringify(DEFAULT_BATCH_WEIGHTS), nowIso());
  }
}

ensureLearningTables();

function loadGlobalHitRate() {
  const row = db
    .prepare(
      `
      SELECT
        COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hits,
        COALESCE(COUNT(*), 0) AS races
      FROM prediction_feature_logs
      WHERE hit_flag IN (0, 1)
    `
    )
    .get();
  const races = toNum(row?.races, 0);
  const hits = toNum(row?.hits, 0);
  const hitRate = races > 0 ? (hits / races) * 100 : 0;
  return {
    races,
    hits,
    hitRate: Number(hitRate.toFixed(2))
  };
}

function buildBucketAdjustments({ column, minSample, scale, cap }) {
  const global = loadGlobalHitRate();
  const rows = db
    .prepare(
      `
      SELECT
        ${column} AS key,
        COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hits,
        COALESCE(COUNT(*), 0) AS races
      FROM prediction_feature_logs
      WHERE hit_flag IN (0, 1)
        AND ${column} IS NOT NULL
        AND TRIM(CAST(${column} AS TEXT)) <> ''
      GROUP BY ${column}
    `
    )
    .all();

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
  const rows = db
    .prepare(
      `
      SELECT
        entry_changed,
        COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hits,
        COALESCE(COUNT(*), 0) AS races
      FROM prediction_feature_logs
      WHERE hit_flag IN (0, 1)
      GROUP BY entry_changed
    `
    )
    .all();
  const map = new Map(rows.map((r) => [toNum(r.entry_changed, 0), r]));
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
  const row = db
    .prepare(
      `
      SELECT
        recommendation_mode,
        COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hits,
        COALESCE(COUNT(*), 0) AS races
      FROM prediction_feature_logs
      WHERE hit_flag IN (0, 1)
      GROUP BY recommendation_mode
    `
    )
    .all()
    .find((r) => String(r.recommendation_mode || "").toUpperCase() === "FULL_BET");
  if (!row || toNum(row.races, 0) < 25) return base;
  const rate = (toNum(row.hits, 0) / Math.max(1, toNum(row.races, 1))) * 100;
  const delta = clamp(-0.08, 0.08, (rate - 33) / 200);
  return Number(clamp(0.88, 1.18, base + delta).toFixed(4));
}

function buildStartSignalWeight(base) {
  const row = db
    .prepare(
      `
      SELECT
        COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hits,
        COALESCE(COUNT(*), 0) AS races
      FROM prediction_feature_logs
      WHERE hit_flag IN (0, 1)
        AND start_display_signature IS NOT NULL
        AND TRIM(start_display_signature) <> ''
    `
    )
    .get();
  const races = toNum(row?.races, 0);
  if (races < 40) return base;
  const rate = (toNum(row?.hits, 0) / Math.max(1, races)) * 100;
  const delta = clamp(-0.1, 0.1, (rate - 32) / 180);
  return Number(clamp(0.85, 1.2, base + delta).toFixed(4));
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
    start_signature_score_adjustments: signaturePack.adjustments
  };

  const summary = `sample=${sampleSize}, global_hit=${toNum(
    venuePack?.global?.hitRate,
    0
  ).toFixed(2)}%, venueAdj=${Object.keys(venuePack.adjustments).length}, gradeAdj=${
    Object.keys(gradePack.adjustments).length
  }, sigAdj=${Object.keys(signaturePack.adjustments).length}`;

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
      summary
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
  updateActiveState(suggested, runId);
  return {
    mode: "applied",
    run_id: runId,
    sample_size: sampleSize,
    base_weights: base,
    suggested_weights: suggested,
    applied_weights: suggested,
    summary
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
    active_weights: {
      ...DEFAULT_BATCH_WEIGHTS,
      ...safeJsonParse(state?.active_weights_json, {})
    },
    active_updated_at: state?.updated_at || null,
    active_last_run_id: state?.last_run_id || null
  };
}
