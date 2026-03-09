import db from "./db.js";

function nowIso() {
  return new Date().toISOString();
}

function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

const upsertSnapshotStmt = db.prepare(`
  INSERT INTO race_start_displays (
    race_id,
    start_display_order_json,
    start_display_st_json,
    start_display_positions_json,
    start_display_signature,
    prediction_snapshot_json,
    updated_at
  ) VALUES (
    @race_id,
    @start_display_order_json,
    @start_display_st_json,
    @start_display_positions_json,
    @start_display_signature,
    @prediction_snapshot_json,
    @updated_at
  )
  ON CONFLICT(race_id) DO UPDATE SET
    start_display_order_json = excluded.start_display_order_json,
    start_display_st_json = excluded.start_display_st_json,
    start_display_positions_json = excluded.start_display_positions_json,
    start_display_signature = excluded.start_display_signature,
    prediction_snapshot_json = excluded.prediction_snapshot_json,
    updated_at = excluded.updated_at
`);

const updateResultFieldsStmt = db.prepare(`
  INSERT INTO race_start_displays (
    race_id,
    fetched_result,
    settled_result,
    updated_at
  ) VALUES (
    @race_id,
    @fetched_result,
    @settled_result,
    @updated_at
  )
  ON CONFLICT(race_id) DO UPDATE SET
    fetched_result = COALESCE(excluded.fetched_result, race_start_displays.fetched_result),
    settled_result = COALESCE(excluded.settled_result, race_start_displays.settled_result),
    updated_at = excluded.updated_at
`);

function buildStartDisplayOrder(racers) {
  const rows = Array.isArray(racers) ? racers : [];
  return rows
    .map((r) => ({
      lane: toNum(r?.lane, null),
      entryCourse: toNum(r?.entryCourse, null)
    }))
    .filter((r) => Number.isInteger(r.lane))
    .sort((a, b) => {
      const ac = Number.isInteger(a.entryCourse) ? a.entryCourse : a.lane;
      const bc = Number.isInteger(b.entryCourse) ? b.entryCourse : b.lane;
      if (ac !== bc) return ac - bc;
      return a.lane - b.lane;
    })
    .map((r) => r.lane);
}

function buildStartDisplaySt(racers) {
  const map = {};
  (Array.isArray(racers) ? racers : []).forEach((r) => {
    const lane = toNum(r?.lane, null);
    if (!Number.isInteger(lane)) return;
    const st = toNum(r?.exhibitionST, null);
    map[String(lane)] = Number.isFinite(st) ? st : null;
  });
  return map;
}

function buildStartDisplayPositions(positions, order) {
  if (Array.isArray(positions) && positions.length > 0) return positions;
  return (Array.isArray(order) ? order : []).map((lane, idx) => ({
    lane,
    x: null,
    y: idx
  }));
}

export function saveRaceStartDisplaySnapshot({
  raceId,
  racers,
  predictionSnapshot,
  startDisplayPositions
}) {
  if (!raceId) return null;
  const order = buildStartDisplayOrder(racers);
  const stMap = buildStartDisplaySt(racers);
  const positions = buildStartDisplayPositions(startDisplayPositions, order);
  const signature = order.join("-");

  upsertSnapshotStmt.run({
    race_id: String(raceId),
    start_display_order_json: JSON.stringify(order),
    start_display_st_json: JSON.stringify(stMap),
    start_display_positions_json: JSON.stringify(positions),
    start_display_signature: signature || null,
    prediction_snapshot_json: JSON.stringify(predictionSnapshot || {}),
    updated_at: nowIso()
  });

  return {
    start_display_order: order,
    start_display_st: stMap,
    start_display_positions: positions,
    start_display_signature: signature || null,
    prediction_snapshot: predictionSnapshot || {}
  };
}

export function saveRaceStartDisplayResult({ raceId, fetchedResult, settledResult }) {
  if (!raceId) return;
  updateResultFieldsStmt.run({
    race_id: String(raceId),
    fetched_result: fetchedResult ? normalizeCombo(fetchedResult) : null,
    settled_result: settledResult ? normalizeCombo(settledResult) : null,
    updated_at: nowIso()
  });
}
