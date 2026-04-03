import db from "./db.js";

export const SIMILAR_RACE_STORAGE_PATH = "sqlite:similar_race_features";

function safeJsonParse(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function toText(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text || null;
}

function nowIso() {
  return new Date().toISOString();
}

function parseRaceIdMeta(raceId) {
  const text = toText(raceId);
  if (!text) return {};
  let match = text.match(/^(\d{4})(\d{2})(\d{2})[_-](\d{1,2})[_-](\d{1,2})$/);
  if (match) {
    return {
      race_date: `${match[1]}-${match[2]}-${match[3]}`,
      venue_code: toInt(match[4], null),
      race_no: toInt(match[5], null)
    };
  }
  match = text.match(/^(\d{8})[-_](\d{1,2})[-_](\d{1,2})$/);
  if (match) {
    const dateText = String(match[1]);
    return {
      race_date: `${dateText.slice(0, 4)}-${dateText.slice(4, 6)}-${dateText.slice(6, 8)}`,
      venue_code: toInt(match[2], null),
      race_no: toInt(match[3], null)
    };
  }
  return {};
}

export function ensureSimilarRaceFeatureTable() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS similar_race_features (
      race_id TEXT PRIMARY KEY,
      prediction_snapshot_id INTEGER,
      race_date TEXT,
      venue_code INTEGER,
      venue_name TEXT,
      race_no INTEGER,
      race_pattern TEXT,
      race_pattern_score REAL,
      boat1_head_pre REAL,
      second_cluster_score REAL,
      near_tie_count INTEGER,
      chaos_level REAL,
      top6_coverage REAL,
      outside_break_risk_pre REAL,
      venue_bias_score REAL,
      venue_bias_json TEXT,
      avg_lap_time REAL,
      avg_exhibition_time REAL,
      entry_signature TEXT,
      predicted_entry_order_json TEXT,
      actual_entry_order_json TEXT,
      entry_confirmed INTEGER,
      style_signature TEXT,
      style_signature_json TEXT,
      style_score_avg REAL,
      lane_rate_json TEXT,
      hard_scenario TEXT,
      hard_scenario_score REAL,
      hard_race_index REAL,
      top6_scenario TEXT,
      top6_scenario_score REAL,
      second_given_head_json TEXT,
      near_tie_second_json TEXT,
      top6_json TEXT,
      optional_active INTEGER NOT NULL DEFAULT 0,
      optional_size INTEGER,
      formation_reason TEXT,
      predicted_head INTEGER,
      racers_feature_json TEXT,
      confidence_score REAL,
      prediction_stability_score REAL,
      recommended_bet_mode TEXT,
      final_result TEXT,
      head_hit INTEGER,
      bet_hit INTEGER,
      top6_hit INTEGER,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
  `);
  const cols = db.prepare("PRAGMA table_info(similar_race_features)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  const addColumn = (name, sqlType) => {
    if (!names.has(name)) db.exec(`ALTER TABLE similar_race_features ADD COLUMN ${name} ${sqlType}`);
  };
  addColumn("venue_bias_score", "REAL");
  addColumn("venue_bias_json", "TEXT");
  addColumn("avg_lap_time", "REAL");
  addColumn("avg_exhibition_time", "REAL");
  addColumn("entry_signature", "TEXT");
  addColumn("predicted_entry_order_json", "TEXT");
  addColumn("actual_entry_order_json", "TEXT");
  addColumn("entry_confirmed", "INTEGER");
  addColumn("style_score_avg", "REAL");
  addColumn("lane_rate_json", "TEXT");
  addColumn("hard_scenario", "TEXT");
  addColumn("hard_scenario_score", "REAL");
  addColumn("hard_race_index", "REAL");
  addColumn("top6_scenario", "TEXT");
  addColumn("top6_scenario_score", "REAL");
  addColumn("second_given_head_json", "TEXT");
  addColumn("near_tie_second_json", "TEXT");
  addColumn("optional_size", "INTEGER");
  addColumn("formation_reason", "TEXT");
  addColumn("predicted_head", "INTEGER");
  addColumn("racers_feature_json", "TEXT");
}

ensureSimilarRaceFeatureTable();

const upsertFeatureStmt = db.prepare(`
  INSERT INTO similar_race_features (
    race_id,
    prediction_snapshot_id,
    race_date,
    venue_code,
    venue_name,
    race_no,
    race_pattern,
    race_pattern_score,
    boat1_head_pre,
    second_cluster_score,
    near_tie_count,
    chaos_level,
    top6_coverage,
    outside_break_risk_pre,
    venue_bias_score,
    venue_bias_json,
    avg_lap_time,
    avg_exhibition_time,
    entry_signature,
    predicted_entry_order_json,
    actual_entry_order_json,
    entry_confirmed,
    style_signature,
    style_signature_json,
    style_score_avg,
    lane_rate_json,
    hard_scenario,
    hard_scenario_score,
    hard_race_index,
    top6_scenario,
    top6_scenario_score,
    second_given_head_json,
    near_tie_second_json,
    top6_json,
    optional_active,
    optional_size,
    formation_reason,
    predicted_head,
    racers_feature_json,
    confidence_score,
    prediction_stability_score,
    recommended_bet_mode,
    final_result,
    head_hit,
    bet_hit,
    top6_hit,
    created_at,
    updated_at
  ) VALUES (
    @race_id,
    @prediction_snapshot_id,
    @race_date,
    @venue_code,
    @venue_name,
    @race_no,
    @race_pattern,
    @race_pattern_score,
    @boat1_head_pre,
    @second_cluster_score,
    @near_tie_count,
    @chaos_level,
    @top6_coverage,
    @outside_break_risk_pre,
    @venue_bias_score,
    @venue_bias_json,
    @avg_lap_time,
    @avg_exhibition_time,
    @entry_signature,
    @predicted_entry_order_json,
    @actual_entry_order_json,
    @entry_confirmed,
    @style_signature,
    @style_signature_json,
    @style_score_avg,
    @lane_rate_json,
    @hard_scenario,
    @hard_scenario_score,
    @hard_race_index,
    @top6_scenario,
    @top6_scenario_score,
    @second_given_head_json,
    @near_tie_second_json,
    @top6_json,
    @optional_active,
    @optional_size,
    @formation_reason,
    @predicted_head,
    @racers_feature_json,
    @confidence_score,
    @prediction_stability_score,
    @recommended_bet_mode,
    @final_result,
    @head_hit,
    @bet_hit,
    @top6_hit,
    COALESCE(@created_at, CURRENT_TIMESTAMP),
    @updated_at
  )
  ON CONFLICT(race_id) DO UPDATE SET
    prediction_snapshot_id=excluded.prediction_snapshot_id,
    race_date=excluded.race_date,
    venue_code=excluded.venue_code,
    venue_name=excluded.venue_name,
    race_no=excluded.race_no,
    race_pattern=excluded.race_pattern,
    race_pattern_score=excluded.race_pattern_score,
    boat1_head_pre=excluded.boat1_head_pre,
    second_cluster_score=excluded.second_cluster_score,
    near_tie_count=excluded.near_tie_count,
    chaos_level=excluded.chaos_level,
    top6_coverage=excluded.top6_coverage,
    outside_break_risk_pre=excluded.outside_break_risk_pre,
    venue_bias_score=excluded.venue_bias_score,
    venue_bias_json=excluded.venue_bias_json,
    avg_lap_time=excluded.avg_lap_time,
    avg_exhibition_time=excluded.avg_exhibition_time,
    entry_signature=excluded.entry_signature,
    predicted_entry_order_json=excluded.predicted_entry_order_json,
    actual_entry_order_json=excluded.actual_entry_order_json,
    entry_confirmed=excluded.entry_confirmed,
    style_signature=excluded.style_signature,
    style_signature_json=excluded.style_signature_json,
    style_score_avg=excluded.style_score_avg,
    lane_rate_json=excluded.lane_rate_json,
    hard_scenario=excluded.hard_scenario,
    hard_scenario_score=excluded.hard_scenario_score,
    hard_race_index=excluded.hard_race_index,
    top6_scenario=excluded.top6_scenario,
    top6_scenario_score=excluded.top6_scenario_score,
    second_given_head_json=excluded.second_given_head_json,
    near_tie_second_json=excluded.near_tie_second_json,
    top6_json=excluded.top6_json,
    optional_active=excluded.optional_active,
    optional_size=excluded.optional_size,
    formation_reason=excluded.formation_reason,
    predicted_head=excluded.predicted_head,
    racers_feature_json=excluded.racers_feature_json,
    confidence_score=excluded.confidence_score,
    prediction_stability_score=excluded.prediction_stability_score,
    recommended_bet_mode=excluded.recommended_bet_mode,
    final_result=COALESCE(excluded.final_result, similar_race_features.final_result),
    head_hit=COALESCE(excluded.head_hit, similar_race_features.head_hit),
    bet_hit=COALESCE(excluded.bet_hit, similar_race_features.bet_hit),
    top6_hit=COALESCE(excluded.top6_hit, similar_race_features.top6_hit),
    updated_at=excluded.updated_at
`);

const updateOutcomeStmt = db.prepare(`
  UPDATE similar_race_features
  SET
    final_result = COALESCE(@final_result, final_result),
    head_hit = COALESCE(@head_hit, head_hit),
    bet_hit = COALESCE(@bet_hit, bet_hit),
    top6_hit = COALESCE(@top6_hit, top6_hit),
    updated_at = @updated_at
  WHERE race_id = @race_id
`);

function normalizeTop6Combos(value) {
  return (Array.isArray(value) ? value : [])
    .map((row) => {
      const combo = typeof row === "string" ? row : row?.combo;
      const probability = typeof row === "object" ? toNum(row?.probability, null) : null;
      return combo ? { combo: String(combo), probability } : null;
    })
    .filter(Boolean)
    .slice(0, 6);
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function buildStyleSignature(laneStyles = []) {
  const rows = (Array.isArray(laneStyles) ? laneStyles : [])
    .map((row) => ({
      lane: toInt(row?.lane, null),
      style_code: toText(row?.style_code ?? row?.style)
    }))
    .filter((row) => Number.isInteger(row.lane) && row.style_code);
  return {
    signature: rows.slice(0, 4).map((row) => `${row.lane}:${row.style_code}`).join("|"),
    rows
  };
}

function round(value, digits = 3) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const base = 10 ** digits;
  return Math.round(n * base) / base;
}

function mean(values = []) {
  const usable = values.map((value) => toNum(value, null)).filter((value) => value !== null);
  if (usable.length === 0) return null;
  return usable.reduce((sum, value) => sum + value, 0) / usable.length;
}

function normalizeOrder(values = []) {
  return (Array.isArray(values) ? values : [])
    .map((value) => toInt(value, null))
    .filter(Number.isInteger)
    .slice(0, 6);
}

function normalizeNearTieCandidates(values = []) {
  return (Array.isArray(values) ? values : [])
    .map((value) => {
      if (typeof value === "object" && value) {
        return toInt(value?.lane ?? value?.boat ?? value?.candidate, null);
      }
      return toInt(value, null);
    })
    .filter(Number.isInteger)
    .slice(0, 4);
}

function buildEntrySignature(predictedEntryOrder = [], actualEntryOrder = []) {
  const predicted = normalizeOrder(predictedEntryOrder);
  const actual = normalizeOrder(actualEntryOrder);
  return {
    predicted,
    actual,
    signature: `${predicted.join("-") || "none"}|${actual.join("-") || "none"}`
  };
}

function summarizeRacersForStorage(racers = []) {
  const rows = (Array.isArray(racers) ? racers : [])
    .map((racer) => ({
      lane: toInt(racer?.lane, null),
      predicted_entry: toInt(racer?.predicted_entry ?? racer?.entry ?? racer?.predictedEntry, null),
      actual_entry: toInt(racer?.actual_entry ?? racer?.actualEntry ?? racer?.entryCourse, null),
      entry_confirmed:
        racer?.entry_confirmed === true || racer?.entryConfirmed === true
          ? 1
          : racer?.entry_confirmed === false || racer?.entryConfirmed === false
            ? 0
            : null,
      style: toText(racer?.style_code ?? racer?.style),
      style_score: toNum(racer?.style_score, null),
      lap_time: toNum(
        racer?.lapTime ??
          racer?.lap_time ??
          racer?.kyoteiBiyoriLapTime,
        null
      ),
      exhibition_time: toNum(
        racer?.exhibitionTime ??
          racer?.exhibition_time,
        null
      ),
      lane1st: toNum(
        racer?.lane1stScore ??
          racer?.lane1stAvg ??
          racer?.lane1stRate ??
          racer?.laneFirstRate,
        null
      ),
      lane2ren: toNum(
        racer?.lane2renScore ??
          racer?.lane2renAvg ??
          racer?.lane2RenRate,
        null
      ),
      lane3ren: toNum(
        racer?.lane3renScore ??
          racer?.lane3renAvg ??
          racer?.lane3RenRate,
        null
      )
    }))
    .filter((row) => Number.isInteger(row.lane))
    .sort((a, b) => a.lane - b.lane)
    .slice(0, 6);
  return {
    rows,
    avgLapTime: round(mean(rows.map((row) => row.lap_time)), 4),
    avgExhibitionTime: round(mean(rows.map((row) => row.exhibition_time)), 4),
    styleScoreAvg: round(mean(rows.map((row) => row.style_score)), 3),
    laneRateSummary: {
      lane1st: round(mean(rows.map((row) => row.lane1st)), 3),
      lane2ren: round(mean(rows.map((row) => row.lane2ren)), 3),
      lane3ren: round(mean(rows.map((row) => row.lane3ren)), 3)
    }
  };
}

function resolvePredictedHead({ pure = {}, predictionObj = {} } = {}) {
  const direct = toInt(
    pure?.head_candidate_ranking?.[0]?.lane ??
      predictionObj?.head_candidate_ranking?.[0]?.lane,
    null
  );
  if (Number.isInteger(direct)) return direct;
  const comboText = toText(pure?.top6?.[0]?.combo ?? predictionObj?.top6?.[0]?.combo);
  if (comboText) {
    const first = toInt((comboText.match(/[1-6]/) || [])[0], null);
    if (Number.isInteger(first)) return first;
  }
  const winRows = pure?.first_place_candidate_rates ?? predictionObj?.first_place_candidate_rates;
  if (Array.isArray(winRows) && winRows.length > 0) {
    return toInt(winRows[0]?.lane, null);
  }
  return null;
}

export function extractSimilarRaceFeatureSnapshot({ raceId, race = null, prediction = null, predictionSnapshotId = null } = {}) {
  const raceIdMeta = parseRaceIdMeta(raceId);
  const predictionObj = prediction && typeof prediction === "object" ? prediction : {};
  const pure = predictionObj?.pure_top6_prediction && typeof predictionObj.pure_top6_prediction === "object"
    ? predictionObj.pure_top6_prediction
    : predictionObj?.pureTop6Prediction && typeof predictionObj.pureTop6Prediction === "object"
      ? predictionObj.pureTop6Prediction
      : predictionObj;
  const hard = predictionObj?.hardRace1234 && typeof predictionObj.hardRace1234 === "object"
    ? predictionObj.hardRace1234
    : {};
  const snapshotContext = predictionObj?.snapshot_context && typeof predictionObj.snapshot_context === "object"
    ? predictionObj.snapshot_context
    : {};
  const nearTieCount = Array.isArray(pure?.near_tie_second_candidates) ? pure.near_tie_second_candidates.length : 0;
  const styleMeta = buildStyleSignature(pure?.lane_styles || predictionObj?.lane_styles || []);
  const nearTieCandidates = normalizeNearTieCandidates(pure?.near_tie_second_candidates || predictionObj?.near_tie_second_candidates || []);
  const top6Rows = normalizeTop6Combos(pure?.top6 || predictionObj?.top6 || []);
  const predictedEntryOrder = normalizeOrder(
    predictionObj?.predicted_entry_order ??
      snapshotContext?.predicted_entry_order ??
      race?.predicted_entry_order
  );
  const actualEntryOrder = normalizeOrder(
    predictionObj?.actual_entry_order ??
      snapshotContext?.actual_entry_order ??
      race?.actual_entry_order
  );
  const entryMeta = buildEntrySignature(predictedEntryOrder, actualEntryOrder);
  const racersForStorage = summarizeRacersForStorage(
    race?.racers ??
      snapshotContext?.players ??
      predictionObj?.racers ??
      []
  );
  const clusterScore =
    nearTieCount >= 3 ? 100
      : nearTieCount === 2 ? 78
        : pure?.close_combo_preserved === true ? 68
          : 42;
  const venueBiasScore = toNum(
    pure?.venue_scenario_bias?.one_course_trust ??
      predictionObj?.venue_scenario_bias?.one_course_trust,
    null
  );
  const entryConfirmed = racersForStorage.rows.some((row) => row.entry_confirmed === 1)
    ? 1
    : racersForStorage.rows.some((row) => row.entry_confirmed === 0)
      ? 0
      : null;
  const optionalFormation =
    pure?.optionalFormation16 && typeof pure.optionalFormation16 === "object" && !Array.isArray(pure.optionalFormation16)
      ? pure.optionalFormation16
      : predictionObj?.optionalFormation16 && typeof predictionObj.optionalFormation16 === "object" && !Array.isArray(predictionObj.optionalFormation16)
        ? predictionObj.optionalFormation16
        : null;
  return {
    race_id: toText(raceId),
    prediction_snapshot_id: toInt(predictionSnapshotId, null),
    race_date: toText(race?.date ?? predictionObj?.snapshot_context?.race_date ?? raceIdMeta.race_date),
    venue_code: toInt(race?.venueId ?? predictionObj?.snapshot_context?.venue_code ?? raceIdMeta.venue_code, null),
    venue_name: toText(race?.venueName ?? predictionObj?.snapshot_context?.venue_name),
    race_no: toInt(race?.raceNo ?? predictionObj?.snapshot_context?.race_no ?? raceIdMeta.race_no, null),
    race_pattern: toText(pure?.racePattern ?? predictionObj?.racePattern ?? predictionObj?.race_pattern) || "mixed",
    race_pattern_score: toNum(pure?.racePatternScore ?? predictionObj?.racePatternScore, null),
    boat1_head_pre: toNum(hard?.boat1_head_pre ?? pure?.head_prob_1 ?? predictionObj?.head_prob_1, null),
    second_cluster_score: clusterScore,
    near_tie_count: nearTieCount,
    chaos_level: toNum(pure?.chaos_level ?? predictionObj?.chaos_level, null),
    top6_coverage: toNum(pure?.top6_coverage ?? predictionObj?.top6_coverage, null),
    outside_break_risk_pre: toNum(hard?.outside_break_risk_pre ?? predictionObj?.outside_break_risk_pre, null),
    venue_bias_score: venueBiasScore,
    venue_bias_json: JSON.stringify(
      pure?.venueBiasProfile ??
        pure?.venue_scenario_bias ??
        predictionObj?.venueBiasProfile ??
        predictionObj?.venue_scenario_bias ??
        {}
    ),
    avg_lap_time: racersForStorage.avgLapTime,
    avg_exhibition_time: racersForStorage.avgExhibitionTime,
    entry_signature: entryMeta.signature,
    predicted_entry_order_json: JSON.stringify(entryMeta.predicted),
    actual_entry_order_json: JSON.stringify(entryMeta.actual),
    entry_confirmed: entryConfirmed,
    style_signature: styleMeta.signature || null,
    style_signature_json: JSON.stringify(styleMeta.rows),
    style_score_avg: racersForStorage.styleScoreAvg,
    lane_rate_json: JSON.stringify(racersForStorage.laneRateSummary),
    hard_scenario: toText(hard?.hardScenario ?? predictionObj?.hardScenario),
    hard_scenario_score: toNum(hard?.hardScenarioScore ?? hard?.scenario_repro_score ?? predictionObj?.hardScenarioScore, null),
    hard_race_index: toNum(hard?.hard_race_index ?? predictionObj?.hard_race_index, null),
    top6_scenario: toText(pure?.top6Scenario ?? predictionObj?.top6Scenario),
    top6_scenario_score: toNum(pure?.top6ScenarioScore ?? pure?.scenario_repro_score ?? predictionObj?.top6ScenarioScore, null),
    second_given_head_json: JSON.stringify(
      pure?.second_given_head_probabilities ??
        predictionObj?.second_given_head_probabilities ??
        {}
    ),
    near_tie_second_json: JSON.stringify(nearTieCandidates),
    top6_json: JSON.stringify(top6Rows),
    optional_active: pure?.optionalFormation16?.active === true || predictionObj?.optionalFormation16?.active === true ? 1 : 0,
    optional_size: toInt(optionalFormation?.size, null),
    formation_reason: toText(
      pure?.formationReason ??
        predictionObj?.formationReason ??
        optionalFormation?.reason
    ),
    predicted_head: resolvePredictedHead({ pure, predictionObj }),
    racers_feature_json: JSON.stringify(racersForStorage.rows),
    confidence_score: toNum(pure?.confidence_score ?? predictionObj?.confidence_score, null),
    prediction_stability_score: toNum(pure?.prediction_stability_score ?? predictionObj?.prediction_stability_score, null),
    recommended_bet_mode: toText(pure?.recommendedBetMode ?? predictionObj?.recommendedBetMode),
    final_result: null,
    head_hit: null,
    bet_hit: null,
    top6_hit: null,
    created_at: null,
    updated_at: nowIso()
  };
}

export function upsertSimilarRaceFeatureSnapshot(args = {}) {
  ensureSimilarRaceFeatureTable();
  const snapshot = extractSimilarRaceFeatureSnapshot(args);
  if (!snapshot?.race_id) return null;
  upsertFeatureStmt.run(snapshot);
  return snapshot;
}

export function updateSimilarRaceFeatureOutcome({ raceId, finalResult = null, headHit = null, betHit = null, top6Hit = null } = {}) {
  ensureSimilarRaceFeatureTable();
  if (!toText(raceId)) return;
  updateOutcomeStmt.run({
    race_id: toText(raceId),
    final_result: toText(finalResult),
    head_hit: headHit === null || headHit === undefined ? null : (toNum(headHit, 0) ? 1 : 0),
    bet_hit: betHit === null || betHit === undefined ? null : (toNum(betHit, 0) ? 1 : 0),
    top6_hit: top6Hit === null || top6Hit === undefined ? null : (toNum(top6Hit, 0) ? 1 : 0),
    updated_at: nowIso()
  });
}

export function backfillSimilarRaceFeatures({ limit = 4000 } = {}) {
  ensureSimilarRaceFeatureTable();
  const raceMetaRows = db.prepare(`
    SELECT race_id, race_date, venue_id, venue_name, race_no
    FROM races
  `).all();
  const resultRows = db.prepare(`
    SELECT race_id, finish_1, finish_2, finish_3
    FROM results
  `).all();
  const startDisplayRows = db.prepare(`
    SELECT race_id, settled_result, fetched_result
    FROM race_start_displays
  `).all();
  const settlementRows = db.prepare(`
    SELECT race_id, MAX(hit_flag) AS bet_hit
    FROM settlement_logs
    GROUP BY race_id
  `).all();
  const latestVerificationRows = db.prepare(`
    SELECT rv.race_id, rv.confirmed_result, rv.head_hit, rv.bet_hit
    FROM race_verification_logs rv
    INNER JOIN (
      SELECT race_id, MAX(id) AS max_id
      FROM race_verification_logs
      GROUP BY race_id
    ) latest
      ON latest.max_id = rv.id
  `).all();
  const predictionRows = db.prepare(`
    SELECT
      pl.id,
      pl.race_id,
      pl.race_date,
      pl.venue_code,
      pl.venue_name,
      pl.race_no,
      pl.prediction_json,
      rv.confirmed_result,
      rv.head_hit,
      rv.bet_hit
    FROM prediction_logs pl
    LEFT JOIN (
      SELECT race_id, MAX(id) AS max_id
      FROM race_verification_logs
      GROUP BY race_id
    ) latest
      ON latest.race_id = pl.race_id
    LEFT JOIN race_verification_logs rv
      ON rv.id = latest.max_id
    ORDER BY pl.id DESC
    LIMIT ?
  `).all(Number(limit));
  const featureRows = db.prepare(`
    SELECT
      NULL AS id,
      race_id,
      race_date,
      venue_id AS venue_code,
      venue_name,
      race_no,
      prediction_snapshot_json AS prediction_json,
      actual_result AS confirmed_result,
      hit_flag AS bet_hit,
      NULL AS head_hit
    FROM prediction_feature_logs
    WHERE prediction_snapshot_json IS NOT NULL
    ORDER BY updated_at DESC
    LIMIT ?
  `).all(Math.max(200, Math.trunc(Number(limit) / 4) || 200));
  const featureEventRows = db.prepare(`
    SELECT
      NULL AS id,
      race_id,
      race_date,
      venue_id AS venue_code,
      venue_name,
      race_no,
      prediction_snapshot_json AS prediction_json,
      actual_result AS confirmed_result,
      hit_flag AS bet_hit,
      NULL AS head_hit
    FROM prediction_feature_log_events
    WHERE prediction_snapshot_json IS NOT NULL
    ORDER BY created_at DESC
    LIMIT ?
  `).all(Math.max(200, Math.trunc(Number(limit) / 4) || 200));
  const raceMetaByRace = new Map(raceMetaRows.map((row) => [String(row.race_id), row]));
  const resultByRace = new Map(resultRows.map((row) => [String(row.race_id), row]));
  const startDisplayByRace = new Map(startDisplayRows.map((row) => [String(row.race_id), row]));
  const settlementByRace = new Map(settlementRows.map((row) => [String(row.race_id), row]));
  const verificationByRace = new Map(latestVerificationRows.map((row) => [String(row.race_id), row]));
  const rows = [...predictionRows, ...featureRows, ...featureEventRows];
  const existingRows = db.prepare(`
    SELECT race_id, final_result, head_hit, bet_hit, top6_hit
    FROM similar_race_features
  `).all();
  const existingByRace = new Map(existingRows.map((row) => [String(row.race_id), row]));
  const seenRaceIds = new Set();
  let inserted = 0;
  for (const row of rows) {
    const raceId = toText(row?.race_id);
    if (!raceId || seenRaceIds.has(raceId)) continue;
    seenRaceIds.add(raceId);
    const raceMeta = raceMetaByRace.get(raceId) || {};
    const resultRow = resultByRace.get(raceId) || {};
    const startDisplayRow = startDisplayByRace.get(raceId) || {};
    const settlementRow = settlementByRace.get(raceId) || {};
    const verificationRow = verificationByRace.get(raceId) || {};
    const prediction = safeJsonParse(row?.prediction_json, {});
    const storedActualResult =
      toText(row?.confirmed_result) ||
      toText(verificationRow?.confirmed_result) ||
      ((
        Number.isInteger(toInt(resultRow?.finish_1, null)) &&
        Number.isInteger(toInt(resultRow?.finish_2, null)) &&
        Number.isInteger(toInt(resultRow?.finish_3, null))
      )
        ? [
            toInt(resultRow?.finish_1, null),
            toInt(resultRow?.finish_2, null),
            toInt(resultRow?.finish_3, null)
          ].join("-")
        : null) ||
      toText(startDisplayRow?.settled_result) ||
      toText(startDisplayRow?.fetched_result);
    const snapshot = extractSimilarRaceFeatureSnapshot({
      raceId,
      race: {
        date: row?.race_date ?? raceMeta?.race_date,
        venueId: row?.venue_code ?? raceMeta?.venue_id,
        venueName: row?.venue_name ?? raceMeta?.venue_name,
        raceNo: row?.race_no ?? raceMeta?.race_no
      },
      prediction,
      predictionSnapshotId: row?.id
    });
    if (!snapshot?.race_id) continue;
    const existing = existingByRace.get(String(snapshot.race_id));
    const actual = toText(storedActualResult);
    if (actual) {
      snapshot.final_result = actual;
      snapshot.head_hit = toNum(row?.head_hit ?? verificationRow?.head_hit, null);
      snapshot.bet_hit = toNum(row?.bet_hit ?? verificationRow?.bet_hit ?? settlementRow?.bet_hit, null);
      const top6Rows = safeJsonParse(snapshot.top6_json, []);
      if (snapshot.head_hit === null && Number.isInteger(snapshot.predicted_head)) {
        snapshot.head_hit = String(actual).split("-")[0] === String(snapshot.predicted_head) ? 1 : 0;
      }
      snapshot.top6_hit = Array.isArray(top6Rows) && top6Rows.some((item) => item?.combo === actual) ? 1 : 0;
    } else if (existing) {
      snapshot.final_result = existing.final_result ?? null;
      snapshot.head_hit = toNum(existing.head_hit, null);
      snapshot.bet_hit = toNum(existing.bet_hit, null);
      snapshot.top6_hit = toNum(existing.top6_hit, null);
    }
    upsertFeatureStmt.run(snapshot);
    inserted += 1;
  }
  return inserted;
}
