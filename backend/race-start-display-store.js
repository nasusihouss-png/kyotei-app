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

function ensureRaceStartDisplayColumns() {
  const cols = db.prepare("PRAGMA table_info(race_start_displays)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("start_display_layout_mode")) {
    db.exec("ALTER TABLE race_start_displays ADD COLUMN start_display_layout_mode TEXT");
  }
  if (!names.has("start_display_source")) {
    db.exec("ALTER TABLE race_start_displays ADD COLUMN start_display_source TEXT");
  }
  if (!names.has("source_fetched_at")) {
    db.exec("ALTER TABLE race_start_displays ADD COLUMN source_fetched_at TEXT");
  }
  if (!names.has("start_display_timing_json")) {
    db.exec("ALTER TABLE race_start_displays ADD COLUMN start_display_timing_json TEXT");
  }
}

ensureRaceStartDisplayColumns();

const upsertSnapshotStmt = db.prepare(`
  INSERT INTO race_start_displays (
    race_id,
    start_display_order_json,
    start_display_st_json,
    start_display_positions_json,
    start_display_signature,
    start_display_timing_json,
    start_display_layout_mode,
    start_display_source,
    source_fetched_at,
    prediction_snapshot_json,
    updated_at
  ) VALUES (
    @race_id,
    @start_display_order_json,
    @start_display_st_json,
    @start_display_positions_json,
    @start_display_signature,
    @start_display_timing_json,
    @start_display_layout_mode,
    @start_display_source,
    @source_fetched_at,
    @prediction_snapshot_json,
    @updated_at
  )
  ON CONFLICT(race_id) DO UPDATE SET
    start_display_order_json = excluded.start_display_order_json,
    start_display_st_json = excluded.start_display_st_json,
    start_display_positions_json = excluded.start_display_positions_json,
    start_display_signature = excluded.start_display_signature,
    start_display_timing_json = excluded.start_display_timing_json,
    start_display_layout_mode = excluded.start_display_layout_mode,
    start_display_source = excluded.start_display_source,
    source_fetched_at = excluded.source_fetched_at,
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
    const st = toNum(r?.exhibitionST ?? r?.exhibitionSt, null);
    map[String(lane)] = Number.isFinite(st) ? st : null;
  });
  return map;
}

function normalizeTimingRaw(raw) {
  return String(raw || "").replace(/\s+/g, "").toUpperCase();
}

function buildStartDisplayTiming(racers, stMap) {
  const map = {};
  (Array.isArray(racers) ? racers : []).forEach((r) => {
    const lane = toNum(r?.lane, null);
    if (!Number.isInteger(lane)) return;
    const raw = normalizeTimingRaw(r?.exhibitionStRaw);
    const num = toNum(r?.exhibitionST ?? r?.exhibitionSt, null);
    let type = "normal";
    let display = Number.isFinite(num) ? num.toFixed(2) : "--";
    let units = Number.isFinite(num) ? Math.max(0, Math.min(100, num * 10)) : null;
    if (/^F\d{1,2}$/.test(raw)) {
      type = "f";
      display = raw;
      units = Math.min(20, toNum(raw.slice(1), 0));
    } else if (/^L\d{1,2}$/.test(raw)) {
      type = "l";
      display = raw;
      units = Math.min(20, toNum(raw.slice(1), 0));
    } else if (raw && !Number.isFinite(num) && stMap?.[String(lane)] === null) {
      display = raw;
    }
    map[String(lane)] = {
      raw: raw || null,
      type,
      units: Number.isFinite(units) ? Number(units.toFixed(2)) : null,
      display
    };
  });
  return map;
}

function buildStartDisplayPositions(positions, order, stMap) {
  if (Array.isArray(positions) && positions.length > 0) return positions;
  const lanes = Array.isArray(order) ? order : [];
  const stValues = lanes
    .map((lane) => {
      const raw = stMap?.[String(lane)];
      const st = Number(raw);
      return Number.isFinite(st) ? st : null;
    })
    .filter((v) => v !== null);
  const minSt = stValues.length ? Math.min(...stValues) : null;
  const maxSt = stValues.length ? Math.max(...stValues) : null;
  const stRange = minSt !== null && maxSt !== null ? Math.max(0.001, maxSt - minSt) : 0.001;
  return lanes.map((lane, idx) => {
    const numericLane = toNum(lane, null);
    const st = Number(stMap?.[String(numericLane)]);
    const stShift = Number.isFinite(st) && minSt !== null ? ((st - minSt) / stRange) * 12 : 6;
    const baseX = idx * 16;
    return {
      lane: numericLane,
      // normalized x based on entry order + st gap (smaller ST = slightly forward)
      x: Number.isInteger(numericLane) ? Number((baseX + stShift).toFixed(2)) : null,
      y: idx * 48
    };
  });
}

export function saveRaceStartDisplaySnapshot({
  raceId,
  racers,
  predictionSnapshot,
  startDisplayPositions,
  sourceMeta
}) {
  if (!raceId) return null;
  const order = buildStartDisplayOrder(racers);
  const stMap = buildStartDisplaySt(racers);
  const timingMap = buildStartDisplayTiming(racers, stMap);
  const positions = buildStartDisplayPositions(startDisplayPositions, order, stMap);
  const signature = order.join("-");
  const now = nowIso();
  const startDisplaySource =
    sourceMeta?.start_display_source || (sourceMeta?.cache?.fallback ? "db_snapshot" : "official_pre_race_info");
  const sourceFetchedAt = sourceMeta?.fetched_at || now;
  const layoutMode = "normalized_entry_order";

  upsertSnapshotStmt.run({
    race_id: String(raceId),
    start_display_order_json: JSON.stringify(order),
    start_display_st_json: JSON.stringify(stMap),
    start_display_positions_json: JSON.stringify(positions),
    start_display_signature: signature || null,
    start_display_timing_json: JSON.stringify(timingMap),
    start_display_layout_mode: layoutMode,
    start_display_source: startDisplaySource,
    source_fetched_at: sourceFetchedAt,
    prediction_snapshot_json: JSON.stringify(predictionSnapshot || {}),
    updated_at: now
  });

  return {
    start_display_order: order,
    start_display_st: stMap,
    start_display_positions: positions,
    start_display_signature: signature || null,
    start_display_timing: timingMap,
    start_display_layout_mode: layoutMode,
    start_display_source: startDisplaySource,
    source_fetched_at: sourceFetchedAt,
    updated_at: now,
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
