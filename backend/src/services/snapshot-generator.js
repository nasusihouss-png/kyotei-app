import db from "../../db.js";
import { getRaceData } from "./boatrace.js";
import { saveRace } from "../../save-race.js";
import { buildRaceFeatures } from "../../feature-engine.js";
import { applyCoursePerformanceFeatures } from "../../course-performance-engine.js";
import { applyMotorPerformanceFeatures } from "../../motor-performance-engine.js";
import { applyVenueAdjustments } from "../../venue-adjustment-engine.js";
import { analyzeRacePattern } from "../../race-pattern-engine.js";
import { applyMotorTrendFeatures } from "../../motor-trend-engine.js";
import { applyEntryDynamicsFeatures } from "../../entry-dynamics-engine.js";
import { rankRace } from "../../score-engine.js";
import { saveFeatureSnapshots } from "../../save-feature-snapshots.js";

const VENUE_IDS = Array.from({ length: 24 }, (_, index) => index + 1);
const DEFAULT_RACE_NUMBERS = Array.from({ length: 12 }, (_, index) => index + 1);

db.exec(`
  CREATE INDEX IF NOT EXISTS idx_races_date_venue_race ON races(race_date, venue_id, race_no);
  CREATE INDEX IF NOT EXISTS idx_entries_race_id_lane ON entries(race_id, lane);
  CREATE INDEX IF NOT EXISTS idx_feature_snapshots_race_id_lane ON feature_snapshots(race_id, lane);
`);

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function buildFeatureRanking(data) {
  const baseFeatures = applyMotorPerformanceFeatures(
    applyCoursePerformanceFeatures(buildRaceFeatures(data.racers, data.race))
  );
  const venueAdjustedBase = applyVenueAdjustments(baseFeatures, data.race);
  const preRanking = rankRace(venueAdjustedBase.racersWithFeatures);
  const prePattern = analyzeRacePattern(preRanking);
  const trendFeatures = applyMotorTrendFeatures(venueAdjustedBase.racersWithFeatures, {
    racePattern: prePattern.race_pattern
  });
  const entryAdjusted = applyEntryDynamicsFeatures(trendFeatures, {
    racePattern: prePattern.race_pattern,
    chaos_index: prePattern.indexes.chaos_index
  });
  return rankRace(entryAdjusted.racersWithFeatures);
}

export async function generateRaceSnapshot({
  date,
  venueId,
  raceNo,
  timeoutMs = 15000,
  includeKyoteiBiyori = true,
  forceRefresh = true
}) {
  const startedAt = Date.now();
  const data = await getRaceData({
    date,
    venueId,
    raceNo,
    timeoutMs,
    includeKyoteiBiyori,
    forceRefresh,
    screeningProfile: false
  });
  const ranking = buildFeatureRanking(data);
  const raceId = saveRace(data);
  const featureSnapshotCount = saveFeatureSnapshots(raceId, ranking);

  return {
    ok: true,
    raceId,
    date: data?.race?.date || date,
    venueId: toInt(data?.race?.venueId, toInt(venueId, null)),
    raceNo: toInt(data?.race?.raceNo, toInt(raceNo, null)),
    saved: {
      race_snapshot: true,
      entry_snapshot: Array.isArray(data?.racers) ? data.racers.length : 0,
      feature_snapshot: featureSnapshotCount
    },
    timing: {
      total_ms: Date.now() - startedAt,
      upstream: data?.source?.timings || {}
    }
  };
}

export async function generateVenueSnapshots({
  date,
  venueId,
  raceNumbers = DEFAULT_RACE_NUMBERS,
  timeoutMs = 15000,
  includeKyoteiBiyori = true,
  forceRefresh = true
}) {
  const results = [];
  for (const raceNo of raceNumbers) {
    try {
      results.push(await generateRaceSnapshot({
        date,
        venueId,
        raceNo,
        timeoutMs,
        includeKyoteiBiyori,
        forceRefresh
      }));
    } catch (error) {
      results.push({
        ok: false,
        date,
        venueId: toInt(venueId, null),
        raceNo: toInt(raceNo, null),
        code: "SNAPSHOT_GENERATION_FAILED",
        message: String(error?.message || error)
      });
    }
  }
  return results;
}

export async function generateDateSnapshots({
  date,
  venueIds = VENUE_IDS,
  raceNumbers = DEFAULT_RACE_NUMBERS,
  timeoutMs = 15000,
  includeKyoteiBiyori = true,
  forceRefresh = true
}) {
  const results = [];
  for (const venueId of venueIds) {
    const venueResults = await generateVenueSnapshots({
      date,
      venueId,
      raceNumbers,
      timeoutMs,
      includeKyoteiBiyori,
      forceRefresh
    });
    results.push(...venueResults);
  }
  return results;
}

export function summarizeSnapshotGenerationResults(results = []) {
  const rows = Array.isArray(results) ? results : [];
  return {
    total: rows.length,
    ok: rows.filter((row) => row?.ok).length,
    failed: rows.filter((row) => !row?.ok).length,
    failures: rows.filter((row) => !row?.ok).map((row) => ({
      date: row?.date || null,
      venueId: row?.venueId || null,
      raceNo: row?.raceNo || null,
      code: row?.code || "SNAPSHOT_GENERATION_FAILED",
      message: row?.message || "unknown_error"
    }))
  };
}

export const snapshotGeneratorConstants = {
  VENUE_IDS,
  DEFAULT_RACE_NUMBERS
};
