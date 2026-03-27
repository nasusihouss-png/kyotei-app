import { loadStoredRaceInferenceData } from "./local-race-inference.js";
import { generateRaceSnapshot } from "./snapshot-generator.js";
import { getRaceSnapshotIndexByParts } from "./race-snapshot-store.js";

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function createRefreshError(code, message, cause = null) {
  const error = new Error(message);
  error.code = code;
  error.cause = cause || null;
  return error;
}

async function withTimeout(promiseFactory, timeoutMs, timeoutCode) {
  let timeoutHandle = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(createRefreshError(timeoutCode, `refresh attempt timed out after ${timeoutMs}ms`));
    }, Math.max(250, Number(timeoutMs) || 2500));
  });
  try {
    return await Promise.race([Promise.resolve().then(promiseFactory), timeoutPromise]);
  } finally {
    if (timeoutHandle) clearTimeout(timeoutHandle);
  }
}

function coverageHasFallback(summary = {}) {
  return (
    Number(summary?.fallback || 0) > 0 ||
    Number(summary?.optional_issues || 0) > 0 ||
    Number(summary?.broken_pipeline || 0) > 0 ||
    Number(summary?.required_broken_pipeline || 0) > 0 ||
    Number(summary?.required_missing || 0) > 0
  );
}

function deriveFreshnessStatus({ refreshedNow, coverageSummary, snapshotStatus, primarySourceOk }) {
  const snapshotText = String(snapshotStatus || "").toUpperCase();
  const fallbackDetected = coverageHasFallback(coverageSummary) || snapshotText === "FALLBACK" || snapshotText === "BROKEN_PIPELINE";
  if (refreshedNow) return fallbackDetected ? "fallback" : "refreshed";
  if (primarySourceOk && !fallbackDetected) return "latest";
  return fallbackDetected ? "fallback" : "stale";
}

function buildRefreshMeta({
  refreshedNow,
  refreshError,
  snapshotIndex,
  sourceStatus,
  coverageSummary
}) {
  const primarySourceOk = sourceStatus?.primary_source_ok === true || refreshedNow === true;
  const secondarySourceOk = sourceStatus?.secondary_source_ok === true;
  const freshnessStatus = deriveFreshnessStatus({
    refreshedNow,
    coverageSummary,
    snapshotStatus: snapshotIndex?.snapshotStatus,
    primarySourceOk
  });

  return {
    refresh_attempted: true,
    refreshed_now: !!refreshedNow,
    freshness_status: freshnessStatus,
    primary_source_ok: primarySourceOk,
    secondary_source_ok: secondarySourceOk,
    last_snapshot_updated_at: snapshotIndex?.updatedAt || snapshotIndex?.generatedAt || null,
    fallback_used: freshnessStatus === "fallback",
    refresh_error: refreshError
      ? {
          code: refreshError?.code || "LATEST_SOURCE_UNAVAILABLE",
          message: String(refreshError?.message || refreshError)
        }
      : null
  };
}

function buildTransientRefreshData({ transientData, refreshMeta, snapshotIndex }) {
  if (!transientData || typeof transientData !== "object") return null;
  return {
    ...transientData,
    source: {
      ...(transientData?.source || {}),
      refresh_meta: refreshMeta,
      local_snapshots: {
        ...(transientData?.source?.local_snapshots || {}),
        index_snapshot_status:
          transientData?.source?.local_snapshots?.index_snapshot_status ||
          snapshotIndex?.snapshotStatus ||
          "READY",
        last_snapshot_updated_at:
          refreshMeta?.last_snapshot_updated_at ||
          transientData?.source?.local_snapshots?.last_snapshot_updated_at ||
          null
      }
    },
    diagnostics: {
      ...(transientData?.diagnostics || {}),
      snapshot_index: snapshotIndex || transientData?.diagnostics?.snapshot_index || null
    }
  };
}

export async function refreshLatestRaceData({
  date,
  venueId,
  raceNo,
  timeoutMs = 6500,
  forceRefresh = true,
  trace = null
} = {}, deps = {}) {
  const generateSnapshot = deps.generateRaceSnapshot || generateRaceSnapshot;
  const loadSnapshot = deps.loadStoredRaceInferenceData || loadStoredRaceInferenceData;
  const getSnapshotIndex = deps.getRaceSnapshotIndexByParts || getRaceSnapshotIndexByParts;
  const normalizedKey = {
    date: String(date || ""),
    venueId: toInt(venueId, null),
    raceNo: toInt(raceNo, null)
  };

  let refreshError = null;
  let refreshResult = null;

  if (typeof trace === "function") {
    trace("refresh_latest_start", {
      ...normalizedKey,
      timeout_ms: Number(timeoutMs) || null
    });
  }

  try {
    refreshResult = await withTimeout(
      () =>
        generateSnapshot({
          date,
          venueId,
          raceNo,
          timeoutMs,
          includeKyoteiBiyori: true,
          forceRefresh
        }),
      timeoutMs,
      "LATEST_REFRESH_TIMEOUT"
    );
    if (!refreshResult?.ok) {
      throw createRefreshError(
        "LATEST_REFRESH_FAILED",
        refreshResult?.message || "latest snapshot refresh did not complete successfully"
      );
    }
  } catch (error) {
    refreshError = createRefreshError(
      error?.code || "LATEST_REFRESH_FAILED",
      String(error?.message || error || "latest snapshot refresh failed"),
      error
    );
  }

  const stored = loadSnapshot({ date, venueId, raceNo, trace });
  const snapshotIndex =
    stored?.diagnostics?.snapshot_index ||
    refreshResult?.snapshotIndex ||
    getSnapshotIndex({ date, venueId, raceNo });
  const coverageSummary =
    stored?.source?.coverage_report_summary ||
    refreshResult?.transientData?.source?.coverage_report_summary ||
    snapshotIndex?.metadata?.coverage_report_summary ||
    {};
  const refreshMeta = buildRefreshMeta({
    refreshedNow: !refreshError && !!refreshResult?.ok,
    refreshError,
    snapshotIndex,
    sourceStatus: refreshResult?.sourceStatus || {},
    coverageSummary
  });

  if (!stored?.ok) {
    const transientData = buildTransientRefreshData({
      transientData: refreshResult?.transientData || null,
      refreshMeta,
      snapshotIndex
    });
    if (transientData?.ok) {
      return {
        ok: true,
        refreshMeta,
        refreshError,
        snapshotIndex,
        data: transientData
      };
    }
    const sourceError = refreshError || createRefreshError(
      "LATEST_SOURCE_UNAVAILABLE",
      stored?.message || "latest public data could not be refreshed and no snapshot is available"
    );
    sourceError.statusCode = 503;
    sourceError.where = "race.route:refreshLatestRaceData";
    sourceError.route = "/api/race";
    sourceError.refreshMeta = buildRefreshMeta({
      refreshedNow: false,
      refreshError: sourceError,
      snapshotIndex,
      sourceStatus: refreshResult?.sourceStatus || {},
      coverageSummary
    });
    sourceError.snapshotLookup = stored?.snapshot || null;
    throw sourceError;
  }

  const latestTransientData = buildTransientRefreshData({
    transientData: refreshResult?.transientData || null,
    refreshMeta,
    snapshotIndex
  });
  if (latestTransientData?.ok) {
    return {
      ok: true,
      refreshMeta,
      refreshError,
      snapshotIndex,
      data: latestTransientData
    };
  }

  if (typeof trace === "function") {
    trace("refresh_latest_end", {
      ...normalizedKey,
      refreshed_now: refreshMeta.refreshed_now,
      freshness_status: refreshMeta.freshness_status,
      primary_source_ok: refreshMeta.primary_source_ok,
      secondary_source_ok: refreshMeta.secondary_source_ok,
      refresh_error_code: refreshMeta.refresh_error?.code || null
    });
  }

  return {
    ok: true,
    refreshMeta,
    refreshError,
    snapshotIndex,
    data: {
      ...stored,
      source: {
        ...(stored?.source || {}),
        refresh_meta: refreshMeta,
        local_snapshots: {
          ...(stored?.source?.local_snapshots || {}),
          last_snapshot_updated_at: refreshMeta.last_snapshot_updated_at
        }
      }
    }
  };
}
