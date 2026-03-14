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
  if (!names.has("is_hidden_from_results")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN is_hidden_from_results INTEGER NOT NULL DEFAULT 0");
  }
  if (!names.has("is_invalid_verification")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN is_invalid_verification INTEGER NOT NULL DEFAULT 0");
  }
  if (!names.has("exclude_from_learning")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN exclude_from_learning INTEGER NOT NULL DEFAULT 0");
  }
  if (!names.has("invalid_reason")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN invalid_reason TEXT");
  }
  if (!names.has("invalidated_at")) {
    db.exec("ALTER TABLE race_verification_logs ADD COLUMN invalidated_at TEXT");
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

export function listLatestVerificationRecords({ includeInvalidated = false } = {}) {
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
    const invalidated =
      Number(row?.is_invalid_verification) === 1 ||
      Number(row?.is_hidden_from_results) === 1 ||
      Number(row?.exclude_from_learning) === 1;
    if (invalidated && !includeInvalidated) continue;
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
      if (
        Number(verificationRow?.is_invalid_verification) === 1 ||
        Number(verificationRow?.exclude_from_learning) === 1
      ) return null;

      const mismatchCategories = Array.isArray(summary?.mismatch_categories)
        ? summary.mismatch_categories
        : safeJsonParse(verificationRow?.mismatch_categories_json, []);
      const hitMiss = String(verificationRow?.hit_miss || summary?.hit_miss || "").toUpperCase();
      const context = snapshot.snapshotContext || {};
      const learning = snapshot.learningContext || {};
      const players = Array.isArray(context?.players) ? context.players : [];
      const featureContributionSummary =
        learning?.feature_contribution_summary && typeof learning.feature_contribution_summary === "object"
          ? learning.feature_contribution_summary
          : {};
      const participationScoreComponents =
        learning?.participation_score_components && typeof learning.participation_score_components === "object"
          ? learning.participation_score_components
          : {};
      const headConfidenceCalibrated = toNum(
        summary?.head_confidence,
        learning?.head_confidence_calibrated ?? learning?.head_confidence ?? null
      );
      const betConfidenceCalibrated = toNum(
        summary?.bet_confidence,
        learning?.bet_confidence_calibrated ?? learning?.bet_confidence ?? learning?.confidence ?? null
      );
      const headConfidenceRaw = toNum(learning?.head_confidence_raw, headConfidenceCalibrated);
      const betConfidenceRaw = toNum(learning?.bet_confidence_raw, betConfidenceCalibrated);
      const headHitValue =
        Number.isFinite(Number(verificationRow?.head_hit))
          ? Number(verificationRow.head_hit)
          : summary?.head_correct === true
            ? 1
            : summary?.head_correct === false
              ? 0
              : null;
      const betHitValue =
        Number.isFinite(Number(verificationRow?.bet_hit))
          ? Number(verificationRow.bet_hit)
          : hitMiss === "HIT"
            ? 1
            : hitMiss === "MISS"
              ? 0
              : null;
      const headExpected = headHitValue === 1 ? 100 : headHitValue === 0 ? 0 : null;
      const betExpected = betHitValue === 1 ? 100 : betHitValue === 0 ? 0 : null;

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
          headHitValue,
        bet_hit:
          betHitValue,
        structure_hit: summary?.second_third_correct === true ? 1 : summary?.second_third_correct === false ? 0 : null,
        learning_ready:
          Number(verificationRow?.exclude_from_learning) === 1
            ? 0
            : Number.isFinite(Number(verificationRow?.learning_ready))
            ? Number(verificationRow.learning_ready)
            : Array.isArray(mismatchCategories) && mismatchCategories.length > 0
              ? 1
              : 0,
        is_hidden_from_results: Number(verificationRow?.is_hidden_from_results) === 1 ? 1 : 0,
        is_invalid_verification: Number(verificationRow?.is_invalid_verification) === 1 ? 1 : 0,
        exclude_from_learning: Number(verificationRow?.exclude_from_learning) === 1 ? 1 : 0,
        invalid_reason: verificationRow?.invalid_reason || summary?.invalid_reason || null,
        invalidated_at: verificationRow?.invalidated_at || summary?.invalidated_at || null,
        mismatch_categories: Array.isArray(mismatchCategories) ? mismatchCategories : [],
        confirmed_result: verificationRow?.confirmed_result || summary?.confirmed_result_canonical || null,
        verification_reason: verificationRow?.verification_reason || summary?.verification_reason || null,
        recommendation_mode: summary?.recommendation_mode || snapshot.raceDecision?.mode || null,
        recommendation_score: toNum(summary?.recommendation_score, learning?.recommendation_score ?? null),
        confidence: toNum(summary?.bet_confidence, learning?.bet_confidence ?? learning?.confidence ?? null),
        head_confidence: headConfidenceCalibrated,
        bet_confidence: betConfidenceCalibrated,
        head_confidence_raw: headConfidenceRaw,
        head_confidence_calibrated: headConfidenceCalibrated,
        bet_confidence_raw: betConfidenceRaw,
        bet_confidence_calibrated: betConfidenceCalibrated,
        head_confidence_bucket: learning?.head_confidence_bucket || null,
        bet_confidence_bucket: learning?.bet_confidence_bucket || null,
        confidence_bucket: learning?.confidence_bucket || learning?.bet_confidence_bucket || null,
        head_confidence_error: Number.isFinite(headExpected) ? Number((headConfidenceCalibrated - headExpected).toFixed(3)) : null,
        bet_confidence_error: Number.isFinite(betExpected) ? Number((betConfidenceCalibrated - betExpected).toFixed(3)) : null,
        confidence_error: Number.isFinite(betExpected) ? Number((betConfidenceCalibrated - betExpected).toFixed(3)) : null,
        confidence_calibration_applied: toNum(learning?.confidence_calibration_applied, 0),
        confidence_calibration_source: learning?.confidence_calibration_source || null,
        confidence_calibration_segments: Array.isArray(learning?.confidence_calibration_segments)
          ? learning.confidence_calibration_segments
          : [],
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
        scenario_type: learning?.scenario_type || context?.scenario_type || null,
        scenario_match_score: toNum(learning?.scenario_match_score, null),
        attack_scenario_type: learning?.attack_scenario_type || context?.attack_scenario_type || null,
        attack_scenario_label: learning?.attack_scenario_label || context?.attack_scenario_label || null,
        attack_scenario_score: toNum(learning?.attack_scenario_score ?? context?.attack_scenario_score, null),
        attack_scenario_reason_tags: Array.isArray(learning?.attack_scenario_reason_tags)
          ? learning.attack_scenario_reason_tags
          : Array.isArray(context?.attack_scenario_reason_tags)
            ? context.attack_scenario_reason_tags
            : [],
        attack_scenario_applied: toNum(learning?.attack_scenario_applied ?? context?.attack_scenario_applied, 0),
        two_sashi_score: toNum(learning?.two_sashi_score ?? context?.two_sashi_score, null),
        three_makuri_score: toNum(learning?.three_makuri_score ?? context?.three_makuri_score, null),
        three_makuri_sashi_score: toNum(learning?.three_makuri_sashi_score ?? context?.three_makuri_sashi_score, null),
        four_cado_makuri_score: toNum(learning?.four_cado_makuri_score ?? context?.four_cado_makuri_score, null),
        four_cado_makuri_sashi_score: toNum(learning?.four_cado_makuri_sashi_score ?? context?.four_cado_makuri_sashi_score, null),
        main_scenario_type: learning?.main_scenario_type || context?.main_scenario_type || null,
        counter_scenario_type: learning?.counter_scenario_type || context?.counter_scenario_type || null,
        survival_scenario_type: learning?.survival_scenario_type || context?.survival_scenario_type || null,
        head_distribution_json: learning?.head_distribution_json || context?.head_distribution_json || [],
        survival_guard_applied: toNum(learning?.survival_guard_applied ?? context?.survival_guard_applied, 0),
        removed_candidate_reason_tags: Array.isArray(learning?.removed_candidate_reason_tags)
          ? learning.removed_candidate_reason_tags
          : Array.isArray(context?.removed_candidate_reason_tags)
            ? context.removed_candidate_reason_tags
            : [],
        boat1_head_bets_snapshot: Array.isArray(learning?.boat1_head_bets_snapshot)
          ? learning.boat1_head_bets_snapshot
          : Array.isArray(context?.boat1_head_bets_snapshot)
            ? context.boat1_head_bets_snapshot
            : [],
        boat1_head_score: toNum(learning?.boat1_head_score ?? context?.boat1_head_score, null),
        boat1_survival_residual_score: toNum(
          learning?.boat1_survival_residual_score ?? context?.boat1_survival_residual_score,
          null
        ),
        boat1_head_section_shown: toNum(learning?.boat1_head_section_shown ?? context?.boat1_head_section_shown, 0),
        boat1_head_reason_tags: Array.isArray(learning?.boat1_head_reason_tags)
          ? learning.boat1_head_reason_tags
          : Array.isArray(context?.boat1_head_reason_tags)
            ? context.boat1_head_reason_tags
            : [],
        predicted_entry_order: Array.isArray(context?.entry?.predicted_entry_order) ? context.entry.predicted_entry_order : [],
        actual_entry_order: Array.isArray(context?.entry?.actual_entry_order) ? context.entry.actual_entry_order : [],
        start_display_st: context?.entry?.start_exhibition_st || {},
        queue_formation_st: context?.entry?.start_display_timing || {},
        formation_pattern: learning?.formation_pattern || context?.formation_pattern || null,
        escape_pattern_applied: toNum(learning?.escape_pattern_applied ?? context?.escape_pattern_applied, 0),
        escape_second_place_bias_json: learning?.escape_second_place_bias_json || context?.escape_second_place_bias_json || {},
        participation_decision: learning?.participation_decision || null,
        participation_decision_reason: learning?.participation_decision_reason || null,
        participation_score_components: participationScoreComponents,
        feature_contribution_summary: featureContributionSummary,
        contender_signals: learning?.contender_signals || context?.contender_signals || {},
        top2_exhibition_boats: Array.isArray(learning?.contender_signals?.exhibition_top2) ? learning.contender_signals.exhibition_top2 : [],
        top2_motor_boats: Array.isArray(learning?.contender_signals?.motor_top2) ? learning.contender_signals.motor_top2 : [],
        overlap_lanes: Array.isArray(learning?.contender_signals?.overlap_lanes) ? learning.contender_signals.overlap_lanes : [],
        player_context: players,
        player_feature_count: players.length,
        left_neighbor_alert_count: players.filter((row) => Number(row?.slit_alert_flag) === 1).length,
        f_hold_lane_count: players.filter((row) => Number(row?.f_hold_bias_applied) === 1).length,
        final_recommended_bets_snapshot: Array.isArray(snapshot.prediction?.final_recommended_bets_snapshot)
          ? snapshot.prediction.final_recommended_bets_snapshot
          : [],
        snapshot
      };
    })
    .filter(Boolean);
}
