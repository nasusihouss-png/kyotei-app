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
  if (!names.has("actual_top3_json")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN actual_top3_json TEXT");
  }
  if (!names.has("winning_trifecta")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN winning_trifecta TEXT");
  }
  if (!names.has("actual_result_json")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN actual_result_json TEXT");
  }
  if (!names.has("result_json")) {
    db.exec("ALTER TABLE prediction_logs ADD COLUMN result_json TEXT");
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
    actual_top3_json,
    winning_trifecta,
    actual_result_json,
    result_json,
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
    @actual_top3_json,
    @winning_trifecta,
    @actual_result_json,
    @result_json,
    @prediction_json,
    @race_decision_json,
    @probabilities_json,
    @ev_analysis_json,
    @bet_plan_json
  )
`);

const updatePredictionLogResultSnapshotStmt = db.prepare(`
  UPDATE prediction_logs
  SET
    actual_top3_json = COALESCE(@actual_top3_json, actual_top3_json),
    winning_trifecta = COALESCE(@winning_trifecta, winning_trifecta),
    actual_result_json = COALESCE(@actual_result_json, actual_result_json),
    result_json = COALESCE(@result_json, result_json)
  WHERE id = (
    SELECT id
    FROM prediction_logs
    WHERE race_id = @race_id
    ORDER BY id DESC
    LIMIT 1
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

function toTrimmedStringOrNull(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text || null;
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function normalizeTop3(values = []) {
  const rows = (Array.isArray(values) ? values : [])
    .map((value) => toNumOrNull(value))
    .filter((value) => Number.isInteger(value))
    .slice(0, 3);
  return rows.length === 3 ? rows : null;
}

function parseTop3FromCombo(comboLike) {
  const digits = String(comboLike || "").match(/[1-6]/g) || [];
  if (digits.length < 3) return null;
  return digits.slice(0, 3).map((value) => Number(value));
}

function buildRaceIdCandidates({ raceId = null, race = null } = {}) {
  const values = [];
  const push = (value) => {
    const text = toTrimmedStringOrNull(value);
    if (!text || values.includes(text)) return;
    values.push(text);
  };
  push(raceId);
  const raceDate = toTrimmedStringOrNull(race?.date);
  const venueId = toNumOrNull(race?.venueId);
  const raceNo = toNumOrNull(race?.raceNo);
  if (raceDate && Number.isInteger(venueId) && Number.isInteger(raceNo)) {
    const compactDate = raceDate.replace(/-/g, "");
    push(`${compactDate}_${venueId}_${raceNo}`);
    push(`${compactDate}_${venueId}_${String(raceNo).padStart(2, "0")}`);
    push(`${compactDate}-${String(venueId).padStart(2, "0")}-${String(raceNo).padStart(2, "0")}`);
    push(`${compactDate}-${venueId}-${String(raceNo).padStart(2, "0")}`);
    push(`${compactDate}-${venueId}-${raceNo}`);
  }
  return values;
}

function resolveSavedOfficialResult({ raceId, race } = {}) {
  const raceDate = toTrimmedStringOrNull(race?.date);
  const venueId = toNumOrNull(race?.venueId);
  const raceNo = toNumOrNull(race?.raceNo);
  const raceIdCandidates = buildRaceIdCandidates({ raceId, race });
  const resultByRaceId = raceIdCandidates.length > 0
    ? db.prepare(`
        SELECT race_id, finish_1, finish_2, finish_3, payout_3t
        FROM results
        WHERE race_id IN (${raceIdCandidates.map(() => "?").join(",")})
        ORDER BY rowid DESC
        LIMIT 1
      `).get(...raceIdCandidates)
    : null;
  const resultByMeta =
    raceDate && Number.isInteger(venueId) && Number.isInteger(raceNo)
      ? db.prepare(`
          SELECT re.race_id, re.finish_1, re.finish_2, re.finish_3, re.payout_3t
          FROM results re
          INNER JOIN races ra
            ON ra.race_id = re.race_id
          WHERE ra.race_date = ?
            AND ra.venue_id = ?
            AND ra.race_no = ?
          ORDER BY re.created_at DESC
          LIMIT 1
        `).get(raceDate, venueId, raceNo)
      : null;
  const resultRow = resultByRaceId || resultByMeta || null;
  const resultTop3 = normalizeTop3([resultRow?.finish_1, resultRow?.finish_2, resultRow?.finish_3]);
  if (resultTop3) {
    const winningTrifecta = resultTop3.join("-");
    return {
      actualTop3: resultTop3,
      winningTrifecta,
      actualResult: winningTrifecta,
      result: winningTrifecta,
      payout3t: toNumOrNull(resultRow?.payout_3t),
      source: "results"
    };
  }

  const verificationRow = raceIdCandidates.length > 0
    ? db.prepare(`
        SELECT confirmed_result
        FROM race_verification_logs
        WHERE race_id IN (${raceIdCandidates.map(() => "?").join(",")})
        ORDER BY id DESC
        LIMIT 1
      `).get(...raceIdCandidates)
    : null;
  const verificationTop3 = normalizeTop3(parseTop3FromCombo(verificationRow?.confirmed_result));
  if (verificationTop3) {
    const winningTrifecta = verificationTop3.join("-");
    return {
      actualTop3: verificationTop3,
      winningTrifecta,
      actualResult: winningTrifecta,
      result: winningTrifecta,
      payout3t: null,
      source: "race_verification_logs"
    };
  }

  const startDisplayRow = raceIdCandidates.length > 0
    ? db.prepare(`
        SELECT fetched_result, settled_result
        FROM race_start_displays
        WHERE race_id IN (${raceIdCandidates.map(() => "?").join(",")})
        ORDER BY updated_at DESC
        LIMIT 1
      `).get(...raceIdCandidates)
    : null;
  const displayCombo = normalizeCombo(startDisplayRow?.settled_result || startDisplayRow?.fetched_result);
  const startDisplayTop3 = normalizeTop3(parseTop3FromCombo(displayCombo));
  if (startDisplayTop3) {
    const winningTrifecta = startDisplayTop3.join("-");
    return {
      actualTop3: startDisplayTop3,
      winningTrifecta,
      actualResult: winningTrifecta,
      result: winningTrifecta,
      payout3t: null,
      source: "race_start_displays"
    };
  }

  return {
    actualTop3: null,
    winningTrifecta: null,
    actualResult: null,
    result: null,
    payout3t: null,
    source: null
  };
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
  const savedOfficialResult = resolveSavedOfficialResult({ raceId, race });
  if (savedOfficialResult.actualTop3) {
    normalizedPrediction.actualTop3 = cloneJson(savedOfficialResult.actualTop3, []);
    normalizedPrediction.top3_result = cloneJson(savedOfficialResult.actualTop3, []);
    normalizedPrediction.winningTrifecta = savedOfficialResult.winningTrifecta;
    normalizedPrediction.actualResult = savedOfficialResult.actualResult;
    normalizedPrediction.result = savedOfficialResult.result;
    normalizedPrediction.result_source = savedOfficialResult.source;
    normalizedPrediction.payout_3t = savedOfficialResult.payout3t;
  } else {
    normalizedPrediction.actualTop3 = null;
    normalizedPrediction.top3_result = null;
    normalizedPrediction.winningTrifecta = null;
    normalizedPrediction.actualResult = null;
    normalizedPrediction.result = null;
    normalizedPrediction.result_source = null;
    normalizedPrediction.payout_3t = null;
  }
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
    actual_top3_json: JSON.stringify(savedOfficialResult.actualTop3 ?? null),
    winning_trifecta: savedOfficialResult.winningTrifecta ?? null,
    actual_result_json: JSON.stringify(
      savedOfficialResult.actualTop3
        ? {
            top3: savedOfficialResult.actualTop3,
            winningTrifecta: savedOfficialResult.actualResult,
            source: savedOfficialResult.source,
            payout_3t: savedOfficialResult.payout3t
          }
        : null
    ),
    result_json: JSON.stringify(
      savedOfficialResult.actualTop3
        ? {
            top3: savedOfficialResult.actualTop3,
            winningTrifecta: savedOfficialResult.result,
            source: savedOfficialResult.source,
            payout_3t: savedOfficialResult.payout3t
          }
        : null
    ),
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

export function updatePredictionLogResultSnapshot({
  raceId,
  actualTop3 = null,
  winningTrifecta = null,
  resultSource = null,
  payout3t = null
} = {}) {
  if (!raceId) return;
  const normalizedTop3 = normalizeTop3(actualTop3);
  const normalizedWinningTrifecta = normalizeCombo(winningTrifecta);
  if (!normalizedTop3 && !normalizedWinningTrifecta) return;
  const finalCombo = normalizedWinningTrifecta || normalizedTop3.join("-");
  updatePredictionLogResultSnapshotStmt.run({
    race_id: String(raceId),
    actual_top3_json: JSON.stringify(normalizedTop3 ?? null),
    winning_trifecta: finalCombo,
    actual_result_json: JSON.stringify(
      normalizedTop3
        ? {
            top3: normalizedTop3,
            winningTrifecta: finalCombo,
            source: resultSource || null,
            payout_3t: toNumOrNull(payout3t)
          }
        : null
    ),
    result_json: JSON.stringify(
      normalizedTop3
        ? {
            top3: normalizedTop3,
            winningTrifecta: finalCombo,
            source: resultSource || null,
            payout_3t: toNumOrNull(payout3t)
          }
        : null
    )
  });
}
