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
import { upsertRaceSnapshotIndex } from "./race-snapshot-store.js";
import { attachCoverageReportToRanking, buildRaceCoverageReport } from "./snapshot-coverage.js";

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

export function summarizeCoverageDiagnostics(coverageReport = {}) {
  const fields = coverageReport?.fields && typeof coverageReport.fields === "object" ? coverageReport.fields : {};
  const fallbackFields = Object.entries(fields)
    .filter(([, meta]) => meta?.status === "fallback")
    .map(([field]) => field);
  const requiredBrokenFields = Object.entries(fields)
    .filter(([, meta]) => meta?.required === true && meta?.status === "broken_pipeline")
    .map(([field]) => field);
  const requiredMissingFields = Object.entries(fields)
    .filter(([, meta]) => meta?.required === true && (meta?.status === "missing" || meta?.status === "not_published"))
    .map(([field]) => field);
  const optionalBrokenFields = Object.entries(fields)
    .filter(([, meta]) => meta?.required !== true && meta?.status === "broken_pipeline")
    .map(([field]) => field);
  const optionalMissingFields = Object.entries(fields)
    .filter(([, meta]) => meta?.required !== true && (meta?.status === "missing" || meta?.status === "not_published"))
    .map(([field]) => field);
  const optionalIssueFields = Object.entries(fields)
    .filter(([, meta]) => meta?.required !== true && meta?.status && meta.status !== "ok")
    .map(([field]) => field);
  const lapTimeFields = Object.entries(fields).filter(([field]) => field.endsWith(".lapTime"));
  return {
    fallback_fields: fallbackFields,
    broken_fields: requiredBrokenFields,
    required_broken_fields: requiredBrokenFields,
    required_missing_fields: requiredMissingFields,
    optional_broken_fields: optionalBrokenFields,
    optional_missing_fields: optionalMissingFields,
    optional_issue_fields: optionalIssueFields,
    lap_time_ready_count: lapTimeFields.filter(([, meta]) => meta?.status === "ok").length,
    lap_time_total_count: lapTimeFields.length
  };
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
  const coverageReport = buildRaceCoverageReport({ data, ranking });
  const coverageDiagnostics = summarizeCoverageDiagnostics(coverageReport);
  const rankingWithCoverage = attachCoverageReportToRanking(ranking, coverageReport);
  const raceId = saveRace(data);
  const featureSnapshotCount = saveFeatureSnapshots(raceId, rankingWithCoverage);
  const entrySnapshotCount = Array.isArray(data?.racers) ? data.racers.length : 0;
  const sourceStatus = {
    primary_source_ok: true,
    secondary_source_ok: !!data?.source?.kyotei_biyori?.ok,
    official_fetch_status: data?.source?.official_fetch_status || {},
    kyotei_biyori: {
      ok: !!data?.source?.kyotei_biyori?.ok,
      fallback_used: !!data?.source?.kyotei_biyori?.fallback_used,
      fallback_reason: data?.source?.kyotei_biyori?.fallback_reason || data?.source?.kyotei_biyori?.error || null
    }
  };
  const brokenCount = Number(coverageReport?.summary?.required_broken_pipeline || 0) + Number(coverageReport?.summary?.required_missing || 0);
  const fallbackCount = Number(coverageReport?.summary?.fallback || 0) + Number(coverageReport?.summary?.optional_issues || 0);
  const snapshotStatus =
    brokenCount > 0 || entrySnapshotCount !== 6 || featureSnapshotCount !== 6
      ? "BROKEN_PIPELINE"
      : fallbackCount > 0
        ? "FALLBACK"
        : "READY";
  const snapshotIndex = upsertRaceSnapshotIndex({
    raceId,
    date: data?.race?.date || date,
    venueId: data?.race?.venueId || venueId,
    venueName: data?.race?.venueName || null,
    raceNo: data?.race?.raceNo || raceNo,
    snapshotStatus,
    entryCount: entrySnapshotCount,
    featureCount: featureSnapshotCount,
    generatedBy: "snapshot:generate",
    metadata: {
      timing: {
        total_ms: Date.now() - startedAt,
        upstream: data?.source?.timings || {}
      },
      coverage_report_summary: coverageReport?.summary || {},
      coverage_diagnostics: coverageDiagnostics,
      coverage_report: coverageReport || {},
      source_status: sourceStatus,
      includeKyoteiBiyori: !!includeKyoteiBiyori,
      forceRefresh: !!forceRefresh
    }
  });

  return {
    ok: true,
    raceId,
    date: data?.race?.date || date,
    venueId: toInt(data?.race?.venueId, toInt(venueId, null)),
    raceNo: toInt(data?.race?.raceNo, toInt(raceNo, null)),
    saved: {
      race_snapshot: true,
      entry_snapshot: entrySnapshotCount,
      feature_snapshot: featureSnapshotCount,
      coverage_report_summary: coverageReport?.summary || {},
      coverage_diagnostics: coverageDiagnostics
    },
    sourceStatus,
    snapshotIndex,
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
      upsertRaceSnapshotIndex({
        date,
        venueId,
        raceNo,
        snapshotStatus: "SNAPSHOT_MISSING",
        entryCount: 0,
        featureCount: 0,
        generatedBy: "snapshot:generate",
        lastErrorCode: "SNAPSHOT_GENERATION_FAILED",
        lastErrorMessage: String(error?.message || error),
        metadata: {
          failedAt: new Date().toISOString()
        }
      });
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
