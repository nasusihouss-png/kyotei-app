import db from "../../db.js";
import { buildRaceIdFromParts } from "../../result-utils.js";

db.exec(`
  CREATE TABLE IF NOT EXISTS race_snapshot_index (
    race_id TEXT PRIMARY KEY,
    race_date TEXT NOT NULL,
    venue_id INTEGER NOT NULL,
    race_no INTEGER NOT NULL,
    venue_name TEXT,
    snapshot_status TEXT NOT NULL DEFAULT 'SNAPSHOT_MISSING',
    entry_count INTEGER NOT NULL DEFAULT 0,
    feature_count INTEGER NOT NULL DEFAULT 0,
    generated_by TEXT,
    last_error_code TEXT,
    last_error_message TEXT,
    metadata_json TEXT,
    generated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_race_snapshot_index_date_venue_race
    ON race_snapshot_index(race_date, venue_id, race_no);
  CREATE INDEX IF NOT EXISTS idx_race_snapshot_index_status
    ON race_snapshot_index(snapshot_status, race_date);
`);

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function safeJsonParse(value, fallback = null) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function mapSnapshotIndexRow(row) {
  if (!row) return null;
  return {
    raceId: row.race_id || null,
    date: row.race_date || null,
    venueId: toInt(row.venue_id, null),
    venueName: row.venue_name || null,
    raceNo: toInt(row.race_no, null),
    snapshotStatus: row.snapshot_status || "SNAPSHOT_MISSING",
    entryCount: toInt(row.entry_count, 0),
    featureCount: toInt(row.feature_count, 0),
    generatedBy: row.generated_by || null,
    lastErrorCode: row.last_error_code || null,
    lastErrorMessage: row.last_error_message || null,
    metadata: safeJsonParse(row.metadata_json, {}),
    generatedAt: row.generated_at || null,
    updatedAt: row.updated_at || null
  };
}

export function buildSnapshotIndexStatus({ entryCount = 0, featureCount = 0, explicitStatus = null } = {}) {
  if (explicitStatus) return String(explicitStatus).toUpperCase();
  if (toInt(entryCount, 0) >= 6 && toInt(featureCount, 0) >= 6) return "READY";
  if (toInt(entryCount, 0) > 0 || toInt(featureCount, 0) > 0) return "BROKEN_PIPELINE";
  return "SNAPSHOT_MISSING";
}

export function getRaceSnapshotIndexByParts({ date, venueId, raceNo }) {
  const row = db
    .prepare(
      `
      SELECT *
      FROM race_snapshot_index
      WHERE race_date = ?
        AND venue_id = ?
        AND race_no = ?
      LIMIT 1
    `
    )
    .get(String(date || ""), toInt(venueId, null), toInt(raceNo, null));
  return mapSnapshotIndexRow(row);
}

export function upsertRaceSnapshotIndex({
  raceId = null,
  date,
  venueId,
  venueName = null,
  raceNo,
  snapshotStatus = null,
  entryCount = 0,
  featureCount = 0,
  generatedBy = "snapshot:generate",
  lastErrorCode = null,
  lastErrorMessage = null,
  metadata = {}
} = {}) {
  const resolvedRaceId =
    raceId ||
    buildRaceIdFromParts({
      date,
      venueId,
      raceNo
    });
  if (!resolvedRaceId) {
    throw new Error("snapshot index requires raceId or a valid date/venueId/raceNo");
  }

  const resolvedStatus = buildSnapshotIndexStatus({
    entryCount,
    featureCount,
    explicitStatus: snapshotStatus
  });

  db.prepare(
    `
    INSERT INTO race_snapshot_index (
      race_id,
      race_date,
      venue_id,
      race_no,
      venue_name,
      snapshot_status,
      entry_count,
      feature_count,
      generated_by,
      last_error_code,
      last_error_message,
      metadata_json,
      generated_at,
      updated_at
    ) VALUES (
      @race_id,
      @race_date,
      @venue_id,
      @race_no,
      @venue_name,
      @snapshot_status,
      @entry_count,
      @feature_count,
      @generated_by,
      @last_error_code,
      @last_error_message,
      @metadata_json,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP
    )
    ON CONFLICT(race_id) DO UPDATE SET
      race_date = excluded.race_date,
      venue_id = excluded.venue_id,
      race_no = excluded.race_no,
      venue_name = excluded.venue_name,
      snapshot_status = excluded.snapshot_status,
      entry_count = excluded.entry_count,
      feature_count = excluded.feature_count,
      generated_by = excluded.generated_by,
      last_error_code = excluded.last_error_code,
      last_error_message = excluded.last_error_message,
      metadata_json = excluded.metadata_json,
      updated_at = CURRENT_TIMESTAMP
  `
  ).run({
    race_id: resolvedRaceId,
    race_date: String(date || ""),
    venue_id: toInt(venueId, null),
    race_no: toInt(raceNo, null),
    venue_name: venueName || null,
    snapshot_status: resolvedStatus,
    entry_count: toInt(entryCount, 0),
    feature_count: toInt(featureCount, 0),
    generated_by: generatedBy || null,
    last_error_code: lastErrorCode || null,
    last_error_message: lastErrorMessage || null,
    metadata_json: JSON.stringify(metadata || {})
  });

  return getRaceSnapshotIndexByParts({ date, venueId, raceNo });
}
