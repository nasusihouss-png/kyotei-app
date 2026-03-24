import db from "../../db.js";
import { buildRaceIdFromParts } from "../../result-utils.js";
import { getRaceSnapshotIndexByParts } from "./race-snapshot-store.js";

db.exec(`
  CREATE INDEX IF NOT EXISTS idx_races_race_id ON races(race_id);
  CREATE INDEX IF NOT EXISTS idx_races_date_venue_race ON races(race_date, venue_id, race_no);
  CREATE INDEX IF NOT EXISTS idx_entries_race_id_lane ON entries(race_id, lane);
  CREATE INDEX IF NOT EXISTS idx_feature_snapshots_race_id_lane ON feature_snapshots(race_id, lane);
  CREATE INDEX IF NOT EXISTS idx_prediction_feature_log_events_race_id_id ON prediction_feature_log_events(race_id, id DESC);
  CREATE INDEX IF NOT EXISTS idx_prediction_logs_race_id_id ON prediction_logs(race_id, id DESC);
`);

function safeJsonParse(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toPositiveRangeNum(value, { min = Number.NEGATIVE_INFINITY, max = Number.POSITIVE_INFINITY } = {}, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) && n >= min && n <= max ? n : fallback;
}

function getFeaturePredictionMeta(featureSnapshot = {}, field) {
  const meta =
    featureSnapshot?.prediction_field_meta && typeof featureSnapshot.prediction_field_meta === "object"
      ? featureSnapshot.prediction_field_meta?.[field]
      : null;
  return meta && typeof meta === "object" ? meta : null;
}

function getUsableFeaturePredictionValue(featureSnapshot = {}, field, { min = Number.NEGATIVE_INFINITY, max = Number.POSITIVE_INFINITY } = {}) {
  const meta = getFeaturePredictionMeta(featureSnapshot, field);
  if (!meta?.is_usable) return null;
  return toPositiveRangeNum(meta?.normalized_numeric_value ?? meta?.value, { min, max }, null);
}

function getPredictionFeatureEventSnapshot(raceId) {
  const row = db
    .prepare(
      `
      SELECT prediction_snapshot_json
      FROM prediction_feature_log_events
      WHERE race_id = ?
      ORDER BY id DESC
      LIMIT 1
    `
    )
    .get(raceId);
  if (!row?.prediction_snapshot_json) return null;
  return safeJsonParse(row.prediction_snapshot_json, null);
}

function getPredictionLogSnapshot(raceId) {
  const row = db
    .prepare(
      `
      SELECT prediction_json
      FROM prediction_logs
      WHERE race_id = ?
      ORDER BY id DESC
      LIMIT 1
    `
    )
    .get(raceId);
  if (!row?.prediction_json) return null;
  return safeJsonParse(row.prediction_json, null);
}

function buildPlayerSnapshotMap(snapshot = null) {
  const players = Array.isArray(snapshot?.snapshot_context?.players)
    ? snapshot.snapshot_context.players
    : Array.isArray(snapshot?.ranking)
      ? snapshot.ranking.map((row) => ({
          lane: toInt(row?.racer?.lane, null),
          name: row?.racer?.name || null,
          class: row?.racer?.class || null,
          avg_st: toNum(row?.racer?.avgSt, null),
          nationwide_win_rate: toNum(row?.racer?.nationwideWinRate, null),
          local_win_rate: toNum(row?.racer?.localWinRate, null),
          motor_2rate: toNum(row?.racer?.motor2Rate, null),
          boat_2rate: toNum(row?.racer?.boat2Rate, null),
          exhibition_time: toNum(row?.racer?.exhibitionTime, null),
          exhibition_st: toNum(row?.racer?.exhibitionSt, null),
          entry_course: toInt(row?.racer?.entryCourse, null),
          f_hold_count: toInt(row?.racer?.fHoldCount, 0),
          tilt: toNum(row?.racer?.tilt, null),
          weight: toNum(row?.racer?.weight, null),
          feature_snapshot:
            row?.features && typeof row.features === "object"
              ? row.features
              : {}
        }))
      : [];

  return new Map(
    players
      .map((row) => [toInt(row?.lane, null), row])
      .filter((row) => Number.isInteger(row[0]))
  );
}

export function loadStoredRaceInferenceData({ date, venueId, raceNo, trace = null }) {
  const startedAt = Date.now();
  const raceId = buildRaceIdFromParts({ date, venueId, raceNo });
  if (!raceId) {
    return {
      ok: false,
      code: "invalid_race_key",
      message: "date / venueId / raceNo could not be converted into a race key",
      diagnostics: {
        raceId: null,
        snapshot_lookup_ms: 0,
        snapshot_load_ms: 0,
        total_ms: Date.now() - startedAt
      }
    };
  }

  if (typeof trace === "function") {
    trace("snapshot_lookup_start", { raceId, date, venueId: toInt(venueId, null), raceNo: toInt(raceNo, null) });
  }
  const snapshotLookupStartedAt = Date.now();
  const snapshotIndex = getRaceSnapshotIndexByParts({ date, venueId, raceNo });
  const raceRow = db
    .prepare(
      `
      SELECT *
      FROM races
      WHERE race_id = ?
      LIMIT 1
    `
    )
    .get(snapshotIndex?.raceId || raceId);
  const snapshotLookupMs = Date.now() - snapshotLookupStartedAt;
  if (typeof trace === "function") {
    trace("snapshot_lookup_end", {
      raceId,
      found: !!raceRow,
      snapshot_lookup_ms: snapshotLookupMs,
      snapshot_index_status: snapshotIndex?.snapshotStatus || "SNAPSHOT_MISSING"
    });
  }

  if (!raceRow) {
    const diagnosticMessage = snapshotIndex?.lastErrorMessage
      ? `precomputed race snapshot was not found (${snapshotIndex.lastErrorMessage})`
      : "precomputed race snapshot was not found";
    return {
      ok: false,
      code: "SNAPSHOT_MISSING",
      message: diagnosticMessage,
      raceId,
      snapshot: {
        raceId,
        key: {
          date: String(date || ""),
          venueId: toInt(venueId, null),
          raceNo: toInt(raceNo, null)
        },
        index: snapshotIndex
      },
      diagnostics: {
        raceId,
        snapshot_index_status: snapshotIndex?.snapshotStatus || "SNAPSHOT_MISSING",
        snapshot_lookup_ms: snapshotLookupMs,
        snapshot_load_ms: 0,
        total_ms: Date.now() - startedAt
      }
    };
  }

  if (typeof trace === "function") {
    trace("snapshot_load_start", { raceId });
  }
  const snapshotLoadStartedAt = Date.now();
  const entryRows = db
    .prepare(
      `
      SELECT *
      FROM entries
      WHERE race_id = ?
      ORDER BY lane
    `
    )
    .all(raceId);

  const featureRows = db
    .prepare(
      `
      SELECT lane, features_json
      FROM feature_snapshots
      WHERE race_id = ?
      ORDER BY lane
    `
    )
    .all(raceId);

  const featureByLane = new Map(
    featureRows.map((row) => [toInt(row?.lane, null), safeJsonParse(row?.features_json, {}) || {}])
  );

  const featureEventSnapshot = getPredictionFeatureEventSnapshot(raceId);
  const predictionLogSnapshot = getPredictionLogSnapshot(raceId);
  const playerSnapshotMap = buildPlayerSnapshotMap(featureEventSnapshot || predictionLogSnapshot);

  const racers = entryRows.map((row) => {
    const lane = toInt(row?.lane, null);
    const playerSnapshot = playerSnapshotMap.get(lane) || {};
    const featureSnapshot =
      playerSnapshot?.feature_snapshot && typeof playerSnapshot.feature_snapshot === "object"
        ? playerSnapshot.feature_snapshot
        : featureByLane.get(lane) || {};
    const predictionFieldMeta =
      featureSnapshot?.prediction_field_meta && typeof featureSnapshot.prediction_field_meta === "object"
        ? featureSnapshot.prediction_field_meta
        : {};
    const storedLapTime = getUsableFeaturePredictionValue(featureSnapshot, "lapTime", { min: 0.01, max: 20 });
    const storedExhibitionTime = getUsableFeaturePredictionValue(featureSnapshot, "exhibitionTime", { min: 4, max: 9 });
    const storedExhibitionSt = getUsableFeaturePredictionValue(featureSnapshot, "exhibitionST", { min: 0, max: 1 });

    return {
      lane,
      registrationNo: toInt(row?.registration_no, null),
      name: row?.name || playerSnapshot?.name || null,
      class: row?.class || playerSnapshot?.class || null,
      branch: row?.branch || playerSnapshot?.branch || null,
      age: toInt(row?.age, null),
      weight: toNum(row?.weight, playerSnapshot?.weight ?? null),
      avgSt: toNum(row?.avg_st, playerSnapshot?.avg_st ?? null),
      nationwideWinRate: toNum(row?.nationwide_win_rate, playerSnapshot?.nationwide_win_rate ?? null),
      localWinRate: toNum(row?.local_win_rate, playerSnapshot?.local_win_rate ?? null),
      motor2Rate: toNum(row?.motor2_rate, playerSnapshot?.motor_2rate ?? null),
      boat2Rate: toNum(row?.boat2_rate, playerSnapshot?.boat_2rate ?? null),
      exhibitionTime: storedExhibitionTime ?? toPositiveRangeNum(playerSnapshot?.exhibition_time, { min: 4, max: 9 }, null),
      exhibitionSt: storedExhibitionSt ?? toPositiveRangeNum(playerSnapshot?.exhibition_st, { min: 0, max: 1 }, null),
      entryCourse: toInt(row?.entry_course, playerSnapshot?.entry_course ?? lane),
      tilt: toNum(row?.tilt, playerSnapshot?.tilt ?? null),
      fHoldCount: toInt(row?.f_hold_count, playerSnapshot?.f_hold_count ?? 0),
      lHoldCount: toInt(playerSnapshot?.l_hold_count, null),
      lapTime: storedLapTime,
      lapTimeRaw: toPositiveRangeNum(
        getFeaturePredictionMeta(featureSnapshot, "lapTime")?.raw_cell_text ?? playerSnapshot?.lap_time_raw,
        { min: 30, max: 50 },
        null
      ),
      kyoteiBiyoriLapTime: storedLapTime,
      kyoteiBiyoriLapTimeRaw: toPositiveRangeNum(
        getFeaturePredictionMeta(featureSnapshot, "lapTime")?.raw_cell_text ?? playerSnapshot?.lap_time_raw,
        { min: 30, max: 50 },
        null
      ),
      predictionFieldMeta,
      featureSnapshot,
      playerSnapshot
    };
  });
  const snapshotLoadMs = Date.now() - snapshotLoadStartedAt;
  if (typeof trace === "function") {
    trace("snapshot_load_end", {
      raceId,
      entry_count: entryRows.length,
      feature_count: featureRows.length,
      has_prediction_feature_event_snapshot: !!featureEventSnapshot,
      has_prediction_log_snapshot: !!predictionLogSnapshot,
      snapshot_load_ms: snapshotLoadMs
    });
  }

  return {
    ok: true,
    raceId,
    race: {
      date: raceRow.race_date,
      venueId: toInt(raceRow.venue_id, null),
      venueName: raceRow.venue_name || null,
      raceNo: toInt(raceRow.race_no, null),
      raceName: raceRow.race_name || null,
      weather: raceRow.weather || null,
      windSpeed: toNum(raceRow.wind_speed, null),
      windDirection: raceRow.wind_dir || null,
      waveHeight: toNum(raceRow.wave_height, null)
    },
    racers,
    stored_snapshots: {
      prediction_feature_event_snapshot: featureEventSnapshot,
      prediction_log_snapshot: predictionLogSnapshot
    },
    source: {
      mode: "pure_inference",
      local_inference: true,
      race_id: raceId,
      official_fetch_status: {
        mode: "disabled_for_inference",
        reason: "pure_inference_uses_precomputed_snapshots_only"
      },
      kyotei_biyori: {
        mode: "disabled_for_inference",
        reason: "pure_inference_uses_precomputed_snapshots_only"
      },
      local_snapshots: {
        race_snapshot: true,
        entry_snapshot: entryRows.length,
        feature_snapshot: featureRows.length,
        prediction_feature_event_snapshot: !!featureEventSnapshot,
        prediction_log_snapshot: !!predictionLogSnapshot,
        index_snapshot_status: snapshotIndex?.snapshotStatus || null
      },
      coverage_report: snapshotIndex?.metadata?.coverage_report || null,
      coverage_report_summary: snapshotIndex?.metadata?.coverage_report_summary || null,
      load_diagnostics: {
        race_id: raceId,
        snapshot_index: snapshotIndex,
        snapshot_lookup_ms: snapshotLookupMs,
        snapshot_load_ms: snapshotLoadMs,
        total_ms: Date.now() - startedAt
      }
    },
    diagnostics: {
      raceId,
      snapshot_index: snapshotIndex,
      snapshot_lookup_ms: snapshotLookupMs,
      snapshot_load_ms: snapshotLoadMs,
      total_ms: Date.now() - startedAt
    }
  };
}
