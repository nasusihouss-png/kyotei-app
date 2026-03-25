import assert from "node:assert/strict";
import { refreshLatestRaceData } from "../src/services/refresh-latest-race-data.js";

function makeStoredSnapshot(overrides = {}) {
  return {
    ok: true,
    race: { date: "2026-03-24", venueId: 13, raceNo: 5 },
    racers: Array.from({ length: 6 }, (_, index) => ({ lane: index + 1 })),
    source: {
      coverage_report_summary: {
        total: 12,
        ok: 12,
        fallback: 0,
        broken_pipeline: 0,
        missing: 0,
        not_published: 0,
        required_broken_pipeline: 0,
        required_missing: 0,
        optional_issues: 0
      },
      local_snapshots: {
        feature_snapshot: 6
      }
    },
    diagnostics: {
      snapshot_index: {
        snapshotStatus: "READY",
        updatedAt: "2026-03-24T12:00:00.000Z",
        metadata: {
          coverage_report_summary: {
            total: 12,
            ok: 12,
            fallback: 0,
            broken_pipeline: 0,
            missing: 0,
            not_published: 0,
            required_broken_pipeline: 0,
            required_missing: 0,
            optional_issues: 0
          }
        }
      }
    },
    ...overrides
  };
}

{
  const result = await refreshLatestRaceData(
    {
      date: "2026-03-24",
      venueId: 13,
      raceNo: 5,
      timeoutMs: 1000
    },
    {
      generateRaceSnapshot: async () => ({
        ok: true,
        sourceStatus: {
          primary_source_ok: true,
          secondary_source_ok: true
        },
        snapshotIndex: {
          snapshotStatus: "READY",
          updatedAt: "2026-03-24T12:00:00.000Z"
        }
      }),
      loadStoredRaceInferenceData: () => makeStoredSnapshot(),
      getRaceSnapshotIndexByParts: () => ({
        snapshotStatus: "READY",
        updatedAt: "2026-03-24T12:00:00.000Z"
      })
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.refreshMeta.refreshed_now, true);
  assert.equal(result.refreshMeta.freshness_status, "fresh");
  assert.equal(result.refreshMeta.primary_source_ok, true);
  assert.equal(result.refreshMeta.secondary_source_ok, true);
}

{
  const result = await refreshLatestRaceData(
    {
      date: "2026-03-24",
      venueId: 13,
      raceNo: 6,
      timeoutMs: 1000
    },
    {
      generateRaceSnapshot: async () => {
        throw new Error("boatrace upstream timeout");
      },
      loadStoredRaceInferenceData: () => makeStoredSnapshot(),
      getRaceSnapshotIndexByParts: () => ({
        snapshotStatus: "READY",
        updatedAt: "2026-03-24T11:45:00.000Z",
        metadata: {
          coverage_report_summary: {
            total: 12,
            ok: 12,
            fallback: 0,
            broken_pipeline: 0,
            missing: 0,
            not_published: 0,
            required_broken_pipeline: 0,
            required_missing: 0,
            optional_issues: 0
          }
        }
      })
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.refreshMeta.refreshed_now, false);
  assert.equal(result.refreshMeta.freshness_status, "stale");
  assert.equal(result.refreshMeta.primary_source_ok, false);
  assert.equal(result.refreshMeta.refresh_error.code, "LATEST_REFRESH_FAILED");
}

{
  const result = await refreshLatestRaceData(
    {
      date: "2026-03-24",
      venueId: 13,
      raceNo: 8,
      timeoutMs: 1000
    },
    {
      generateRaceSnapshot: async () => ({
        ok: true,
        sourceStatus: {
          primary_source_ok: true,
          secondary_source_ok: false
        },
        snapshotIndex: {
          snapshotStatus: "READY",
          updatedAt: "2026-03-24T12:15:00.000Z",
          metadata: {
            coverage_report_summary: {
              total: 12,
              ok: 12,
              fallback: 0,
              broken_pipeline: 0,
              missing: 0,
              not_published: 0,
              required_broken_pipeline: 0,
              required_missing: 0,
              optional_issues: 0
            }
          }
        },
        transientData: {
          ok: true,
          race: { date: "2026-03-24", venueId: 13, raceNo: 8 },
          racers: Array.from({ length: 6 }, (_, index) => ({
            lane: index + 1,
            featureSnapshot: { score: 10 - index },
            predictionFieldMeta: {}
          })),
          source: {
            coverage_report_summary: {
              total: 12,
              ok: 12,
              fallback: 0,
              broken_pipeline: 0,
              missing: 0,
              not_published: 0,
              required_broken_pipeline: 0,
              required_missing: 0,
              optional_issues: 0
            },
            local_snapshots: {
              generated_from_latest_fetch: true
            }
          },
          diagnostics: {
            generated_from_latest_fetch: true
          }
        }
      }),
      loadStoredRaceInferenceData: () => ({
        ok: false,
        message: "precomputed race snapshot was not found"
      }),
      getRaceSnapshotIndexByParts: () => ({
        snapshotStatus: "READY",
        updatedAt: "2026-03-24T12:15:00.000Z"
      })
    }
  );

  assert.equal(result.ok, true);
  assert.equal(result.data.ok, true);
  assert.equal(result.data.racers.length, 6);
  assert.equal(result.data.source.refresh_meta?.refreshed_now, true);
  assert.equal(result.data.source.local_snapshots?.generated_from_latest_fetch, true);
  assert.equal(result.data.diagnostics?.generated_from_latest_fetch, true);
}

await assert.rejects(
  () =>
    refreshLatestRaceData(
      {
        date: "2026-03-24",
        venueId: 13,
        raceNo: 7,
        timeoutMs: 1000
      },
      {
        generateRaceSnapshot: async () => {
          throw new Error("primary source down");
        },
        loadStoredRaceInferenceData: () => ({
          ok: false,
          message: "precomputed race snapshot was not found"
        }),
        getRaceSnapshotIndexByParts: () => null
      }
    ),
  (error) => {
    assert.equal(error.code, "LATEST_REFRESH_FAILED");
    assert.equal(error.statusCode, 503);
    assert.equal(error.refreshMeta.freshness_status, "stale");
    return true;
  }
);

console.log("refresh-latest-race-data ok");
