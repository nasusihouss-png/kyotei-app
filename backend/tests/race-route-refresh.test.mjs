import assert from "node:assert/strict";
import { loadRaceDataForApiRoute } from "../src/routes/race.js";

function makeRouteTimings() {
  return {
    snapshot_lookup_ms: null,
    snapshot_load_ms: null,
    feature_generation_ms: null,
    inference_ms: null,
    official_base_fetch_ms: null,
    kyoteibiyori_fetch_ms: null,
    parsing_ms: null,
    score_calculation_ms: null,
    actual_entry_reassignment_ms: null,
    odds_fetch_ms: null,
    prediction_build_ms: null,
    total_response_ms: null
  };
}

{
  let refreshCalled = 0;
  const routeTimings = makeRouteTimings();
  const result = await loadRaceDataForApiRoute(
    {
      date: "2026-03-27",
      venueId: 13,
      raceNo: 7,
      latestRefreshTimeoutMs: 1500,
      forceRefresh: true,
      traceBase: { route: "/api/race" },
      routeTimings,
      routeStartedAt: Date.now(),
      maxRouteTimeoutMs: 8000
    },
    {
      refreshLatestRaceData: async () => {
        refreshCalled += 1;
        return {
          data: {
            ok: true,
            source: {
              refresh_meta: {
                refreshed_now: true,
                freshness_status: "refreshed",
                primary_source_ok: true,
                secondary_source_ok: true,
                fallback_used: false,
                last_snapshot_updated_at: "2026-03-27T10:00:00.000Z"
              },
              timings: {
                official_base_fetch_ms: 111,
                kyoteibiyori_fetch_ms: 222,
                parsing_ms: 333
              }
            },
            diagnostics: {
              snapshot_lookup_ms: 12,
              snapshot_load_ms: 34
            }
          }
        };
      },
      ensureRaceRouteWithinDeadline: () => {},
      logRaceRouteStage: () => {}
    }
  );

  assert.equal(refreshCalled, 1);
  assert.equal(result.data.source.refresh_meta.refreshed_now, true);
  assert.equal(routeTimings.snapshot_lookup_ms, 12);
  assert.equal(routeTimings.snapshot_load_ms, 34);
  assert.equal(routeTimings.official_base_fetch_ms, 111);
  assert.equal(routeTimings.kyoteibiyori_fetch_ms, 222);
  assert.equal(routeTimings.parsing_ms, 333);
}

{
  const routeTimings = makeRouteTimings();
  const result = await loadRaceDataForApiRoute(
    {
      date: "2026-03-27",
      venueId: 13,
      raceNo: 8,
      latestRefreshTimeoutMs: 1500,
      forceRefresh: true,
      traceBase: { route: "/api/race" },
      routeTimings,
      routeStartedAt: Date.now(),
      maxRouteTimeoutMs: 8000
    },
    {
      refreshLatestRaceData: async () => ({
        data: {
          ok: true,
          source: {
            refresh_meta: {
              refreshed_now: false,
              freshness_status: "fallback",
              primary_source_ok: false,
              secondary_source_ok: false,
              fallback_used: true,
              last_snapshot_updated_at: "2026-03-27T09:30:00.000Z"
            },
            timings: {
              official_base_fetch_ms: 444,
              kyoteibiyori_fetch_ms: 555,
              parsing_ms: 666
            }
          },
          diagnostics: {
            snapshot_lookup_ms: 21,
            snapshot_load_ms: 43
          }
        }
      }),
      ensureRaceRouteWithinDeadline: () => {},
      logRaceRouteStage: () => {}
    }
  );

  assert.equal(result.data.source.refresh_meta.fallback_used, true);
  assert.equal(result.data.source.refresh_meta.freshness_status, "fallback");
  assert.equal(routeTimings.official_base_fetch_ms, 444);
  assert.equal(routeTimings.kyoteibiyori_fetch_ms, 555);
  assert.equal(routeTimings.parsing_ms, 666);
}

{
  const routeTimings = makeRouteTimings();
  await loadRaceDataForApiRoute(
    {
      date: "2026-03-27",
      venueId: 13,
      raceNo: 9,
      latestRefreshTimeoutMs: 1500,
      forceRefresh: true,
      traceBase: { route: "/api/race" },
      routeTimings,
      routeStartedAt: Date.now(),
      maxRouteTimeoutMs: 8000
    },
    {
      refreshLatestRaceData: async () => ({
        refreshResult: {
          timing: {
            upstream: {
              official_base_fetch_ms: 777,
              kyoteibiyori_fetch_ms: 888,
              parsing_ms: 999
            }
          }
        },
        snapshotIndex: {
          metadata: {
            timing: {
              upstream: {
                official_base_fetch_ms: 777,
                kyoteibiyori_fetch_ms: 888,
                parsing_ms: 999
              }
            }
          }
        },
        data: {
          ok: true,
          source: {
            refresh_meta: {
              refreshed_now: true,
              freshness_status: "refreshed",
              primary_source_ok: true,
              secondary_source_ok: false,
              fallback_used: false,
              last_snapshot_updated_at: "2026-03-27T10:30:00.000Z"
            }
          },
          diagnostics: {
            snapshot_lookup_ms: 55,
            snapshot_load_ms: 66
          }
        }
      }),
      ensureRaceRouteWithinDeadline: () => {},
      logRaceRouteStage: () => {}
    }
  );

  assert.equal(routeTimings.official_base_fetch_ms, 777);
  assert.equal(routeTimings.kyoteibiyori_fetch_ms, 888);
  assert.equal(routeTimings.parsing_ms, 999);
}

console.log("race-route-refresh ok");
