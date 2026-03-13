import db from "./db.js";

export function nowIso() {
  return new Date().toISOString();
}

export function safeJsonParse(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

export function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

export function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function normalizeBetSnapshotItems(items) {
  return (Array.isArray(items) ? items : [])
    .map((row) => {
      const combo = normalizeCombo(row?.combo ?? row);
      if (!combo || combo.split("-").length !== 3) return null;
      return {
        ...(row && typeof row === "object" ? row : {}),
        combo,
        prob: Number.isFinite(Number(row?.prob)) ? Number(row.prob) : null,
        odds: Number.isFinite(Number(row?.odds)) ? Number(row.odds) : null,
        ev: Number.isFinite(Number(row?.ev)) ? Number(row.ev) : null,
        ticket_type: row?.ticket_type || "backup",
        recommended_bet: Number.isFinite(Number(row?.recommended_bet))
          ? Number(row.recommended_bet)
          : Number.isFinite(Number(row?.bet))
            ? Number(row.bet)
            : null,
        explanation_tags: Array.isArray(row?.explanation_tags) ? [...row.explanation_tags] : [],
        trap_flags: Array.isArray(row?.trap_flags) ? [...row.trap_flags] : []
      };
    })
    .filter(Boolean);
}

export function recoverFinalRecommendedBetsSnapshot({ prediction, betPlan } = {}) {
  const predictionObj = prediction && typeof prediction === "object" ? prediction : {};
  const betPlanObj = betPlan && typeof betPlan === "object" ? betPlan : {};
  const candidates = [
    {
      source: predictionObj?.final_recommended_bets_snapshot_source || "prediction_final_recommended_bets_snapshot",
      items: normalizeBetSnapshotItems(predictionObj?.final_recommended_bets_snapshot)
    },
    {
      source: "prediction_ai_bets_display_snapshot",
      items: normalizeBetSnapshotItems(predictionObj?.ai_bets_display_snapshot)
    },
    {
      source: "prediction_ai_bets_full_snapshot.recommended_bets",
      items: normalizeBetSnapshotItems(predictionObj?.ai_bets_full_snapshot?.recommended_bets)
    },
    {
      source: "bet_plan_json.recommended_bets",
      items: normalizeBetSnapshotItems(betPlanObj?.recommended_bets)
    }
  ];

  for (const candidate of candidates) {
    if (candidate.items.length > 0) return candidate;
  }

  return {
    source: "missing_final_recommended_bets_snapshot",
    items: []
  };
}

export function backfillPredictionSnapshotFinalBets(row) {
  if (!row) return null;
  const prediction = safeJsonParse(row?.prediction_json, {});
  const betPlan = safeJsonParse(row?.bet_plan_json, {});
  const existing = normalizeBetSnapshotItems(prediction?.final_recommended_bets_snapshot);
  if (existing.length > 0) {
    return {
      row,
      prediction,
      betPlan,
      recovered: {
        source: prediction?.final_recommended_bets_snapshot_source || "prediction_final_recommended_bets_snapshot",
        items: existing,
        backfilled: false
      }
    };
  }

  const recovered = recoverFinalRecommendedBetsSnapshot({ prediction, betPlan });
  if (recovered.items.length === 0) {
    return {
      row,
      prediction,
      betPlan,
      recovered: {
        ...recovered,
        backfilled: false
      }
    };
  }

  const nextPrediction = {
    ...prediction,
    final_recommended_bets_snapshot: recovered.items,
    final_recommended_bets_count: recovered.items.length,
    final_recommended_bets_snapshot_source: `backfilled:${recovered.source}`,
    ai_bets_display_snapshot:
      Array.isArray(prediction?.ai_bets_display_snapshot) && prediction.ai_bets_display_snapshot.length > 0
        ? prediction.ai_bets_display_snapshot
        : recovered.items
  };

  return {
    row: {
      ...row,
      prediction_json: JSON.stringify(nextPrediction)
    },
    prediction: nextPrediction,
    betPlan,
    recovered: {
      source: `backfilled:${recovered.source}`,
      items: recovered.items,
      backfilled: true
    }
  };
}

export function ensurePredictionSnapshotColumns() {
  const cols = db.prepare("PRAGMA table_info(prediction_logs)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("race_key")) db.exec("ALTER TABLE prediction_logs ADD COLUMN race_key TEXT");
  if (!names.has("prediction_timestamp")) db.exec("ALTER TABLE prediction_logs ADD COLUMN prediction_timestamp TEXT");
  if (!names.has("model_version")) db.exec("ALTER TABLE prediction_logs ADD COLUMN model_version TEXT");
}

export function ensureVerificationRecordColumns() {
  const cols = db.prepare("PRAGMA table_info(race_verification_logs)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("prediction_snapshot_id")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN prediction_snapshot_id INTEGER");
  }
  if (!names.has("verified_against_snapshot_id")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN verified_against_snapshot_id INTEGER");
  }
  if (!names.has("verification_status")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN verification_status TEXT");
  }
  if (!names.has("verification_reason")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN verification_reason TEXT");
  }
  if (!names.has("confirmed_result")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN confirmed_result TEXT");
  }
  if (!names.has("head_hit")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN head_hit INTEGER");
  }
  if (!names.has("bet_hit")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN bet_hit INTEGER");
  }
  if (!names.has("learning_ready")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN learning_ready INTEGER");
  }
}

ensurePredictionSnapshotColumns();

export function getPredictionSnapshot({ raceId, snapshotId = null } = {}) {
  if (Number.isFinite(Number(snapshotId))) {
    const row = db
      .prepare(
        `
        SELECT *
        FROM prediction_logs
        WHERE id = ?
        LIMIT 1
      `
      )
      .get(Number(snapshotId));
    return backfillPredictionSnapshotFinalBets(row)?.row || row;
  }
  if (!raceId) return null;
  const row = db
    .prepare(
      `
      SELECT *
      FROM prediction_logs
      WHERE race_id = ?
      ORDER BY id DESC
      LIMIT 1
    `
    )
    .get(String(raceId));
  return backfillPredictionSnapshotFinalBets(row)?.row || row;
}

export function listPredictionSnapshots({ limit = 300 } = {}) {
  return db
    .prepare(
      `
      SELECT *
      FROM prediction_logs
      ORDER BY COALESCE(prediction_timestamp, created_at, '') DESC, id DESC
      LIMIT ?
    `
    )
    .all(Number(limit))
    .map((row) => backfillPredictionSnapshotFinalBets(row)?.row || row);
}

export function mapPredictionSnapshotRow(row) {
  const prediction = safeJsonParse(row?.prediction_json, {});
  const betPlan = safeJsonParse(row?.bet_plan_json, {});
  const raceDecision = safeJsonParse(row?.race_decision_json, {});
  const probabilities = safeJsonParse(row?.probabilities_json, []);
  const evAnalysis = safeJsonParse(row?.ev_analysis_json, {});
  const snapshotContext =
    prediction?.snapshot_context && typeof prediction.snapshot_context === "object"
      ? prediction.snapshot_context
      : {};
  const learningContext =
    prediction?.learning_context && typeof prediction.learning_context === "object"
      ? prediction.learning_context
      : {};

  return {
    row,
    id: Number.isFinite(Number(row?.id)) ? Number(row.id) : null,
    race_id: String(row?.race_id || prediction?.race_key || ""),
    race_key: String(row?.race_key || prediction?.race_key || row?.race_id || ""),
    prediction_timestamp: row?.prediction_timestamp || prediction?.snapshot_created_at || row?.created_at || nowIso(),
    model_version: row?.model_version || prediction?.model_version || null,
    race_date: row?.race_date || snapshotContext?.race_date || null,
    venue_code: Number.isFinite(Number(row?.venue_code))
      ? Number(row.venue_code)
      : toInt(snapshotContext?.venue_code, null),
    venue_name: row?.venue_name || snapshotContext?.venue_name || null,
    race_no: Number.isFinite(Number(row?.race_no)) ? Number(row.race_no) : toInt(snapshotContext?.race_no, null),
    prediction,
    betPlan,
    raceDecision,
    probabilities,
    evAnalysis,
    snapshotContext,
    learningContext
  };
}

export function listLatestVerificationRecords() {
  ensureVerificationRecordColumns();
  const rows = db
    .prepare(
      `
      SELECT *
      FROM race_verification_logs
      ORDER BY id DESC
    `
    )
    .all();

  const seen = new Set();
  const latest = [];
  for (const row of rows) {
    const summary = safeJsonParse(row?.verification_summary_json, {});
    const snapshotId = Number.isFinite(Number(row?.verified_against_snapshot_id))
      ? Number(row.verified_against_snapshot_id)
      : Number.isFinite(Number(row?.prediction_snapshot_id))
        ? Number(row.prediction_snapshot_id)
        : Number.isFinite(Number(summary?.verified_against_snapshot_id))
          ? Number(summary.verified_against_snapshot_id)
          : Number.isFinite(Number(summary?.prediction_snapshot_id))
            ? Number(summary.prediction_snapshot_id)
            : null;
    const key = snapshotId ? `snapshot:${snapshotId}` : `race:${String(row?.race_id || "")}`;
    if (seen.has(key)) continue;
    seen.add(key);
    latest.push({
      ...row,
      summary
    });
  }
  return latest;
}

export function buildVerifiedLearningRows() {
  ensureVerificationRecordColumns();
  const snapshots = listPredictionSnapshots({ limit: 100000 }).map(mapPredictionSnapshotRow);
  const snapshotById = new Map(
    snapshots
      .filter((row) => Number.isFinite(Number(row.id)))
      .map((row) => [Number(row.id), row])
  );
  const verificationRows = listLatestVerificationRecords();

  return verificationRows
    .map((verificationRow) => {
      const summary = verificationRow.summary || {};
      const snapshotId = Number.isFinite(Number(verificationRow?.verified_against_snapshot_id))
        ? Number(verificationRow.verified_against_snapshot_id)
        : Number.isFinite(Number(verificationRow?.prediction_snapshot_id))
          ? Number(verificationRow.prediction_snapshot_id)
          : Number.isFinite(Number(summary?.verified_against_snapshot_id))
            ? Number(summary.verified_against_snapshot_id)
            : Number.isFinite(Number(summary?.prediction_snapshot_id))
              ? Number(summary.prediction_snapshot_id)
              : null;
      const snapshot = snapshotId ? snapshotById.get(snapshotId) : null;
      const status = String(
        verificationRow?.verification_status ||
          summary?.verification_status ||
          ""
      ).toUpperCase();
      if (!status.startsWith("VERIFIED")) return null;
      if (!snapshot) return null;

      const mismatchCategories = Array.isArray(summary?.mismatch_categories)
        ? summary.mismatch_categories
        : safeJsonParse(verificationRow?.mismatch_categories_json, []);
      const hitMiss = String(verificationRow?.hit_miss || summary?.hit_miss || "").toUpperCase();
      const context = snapshot.snapshotContext || {};
      const learning = snapshot.learningContext || {};

      return {
        race_id: snapshot.race_id,
        prediction_snapshot_id: snapshot.id,
        verified_against_snapshot_id: snapshotId,
        prediction_timestamp: snapshot.prediction_timestamp,
        verified_at: verificationRow?.verified_at || null,
        verification_status: status,
        hit_miss: hitMiss,
        hit_flag:
          Number.isFinite(Number(verificationRow?.bet_hit)) ? Number(verificationRow.bet_hit) : hitMiss === "HIT" ? 1 : 0,
        head_hit:
          Number.isFinite(Number(verificationRow?.head_hit))
            ? Number(verificationRow.head_hit)
            : summary?.head_correct === true
              ? 1
              : summary?.head_correct === false
                ? 0
                : null,
        bet_hit:
          Number.isFinite(Number(verificationRow?.bet_hit))
            ? Number(verificationRow.bet_hit)
            : hitMiss === "HIT"
              ? 1
              : hitMiss === "MISS"
                ? 0
                : null,
        learning_ready:
          Number.isFinite(Number(verificationRow?.learning_ready))
            ? Number(verificationRow.learning_ready)
            : Array.isArray(mismatchCategories) && mismatchCategories.length > 0
              ? 1
              : 0,
        mismatch_categories: Array.isArray(mismatchCategories) ? mismatchCategories : [],
        confirmed_result: verificationRow?.confirmed_result || summary?.confirmed_result_canonical || null,
        recommendation_mode: summary?.recommendation_mode || snapshot.raceDecision?.mode || null,
        recommendation_score: toNum(summary?.recommendation_score, learning?.recommendation_score ?? null),
        confidence: toNum(summary?.bet_confidence, learning?.bet_confidence ?? learning?.confidence ?? null),
        head_confidence: toNum(summary?.head_confidence, learning?.head_confidence ?? null),
        bet_confidence: toNum(summary?.bet_confidence, learning?.bet_confidence ?? null),
        entry_changed: learning?.entry_changed ? 1 : 0,
        entry_change_type: learning?.entry_change_type || null,
        venue_id: toInt(context?.venue_code, null),
        venue_name: context?.venue_name || null,
        race_grade: context?.race_grade || null,
        weather: context?.weather || null,
        wind: toNum(context?.wind_speed, null),
        wave: toNum(context?.wave_height, null),
        motor_rate_avg: toNum(learning?.motor_rate_avg, null),
        boat_rate_avg: toNum(learning?.boat_rate_avg, null),
        avg_st_avg: toNum(learning?.avg_st_avg, null),
        exhibition_time_avg: toNum(learning?.exhibition_time_avg, null),
        start_display_signature: learning?.start_display_signature || null,
        scenario_labels: Array.isArray(learning?.scenario_labels) ? learning.scenario_labels : [],
        player_context: Array.isArray(context?.players) ? context.players : [],
        snapshot
      };
    })
    .filter(Boolean);
}
