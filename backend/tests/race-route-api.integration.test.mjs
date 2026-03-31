import assert from "node:assert/strict";
import express from "express";
import { raceRouter, resetRaceRouteRuntimeDepsForTests, setRaceRouteRuntimeDepsForTests } from "../src/routes/race.js";

function makeRacer(lane) {
  return {
    lane,
    name: `Racer-${lane}`,
    avgSt: 0.12 + lane * 0.01,
    nationwideWinRate: 7 - lane * 0.25,
    localWinRate: 6.7 - lane * 0.22,
    motor2Rate: 42 - lane * 2,
    boat2Rate: 39 - lane * 1.5,
    exhibitionSt: 0.08 + lane * 0.01,
    exhibitionTime: 6.7 + lane * 0.02,
    lapTime: 36.5 + lane * 0.1,
    lapRaw: `${(36.5 + lane * 0.1).toFixed(2)}`,
    lapSource: "kyoteibiyori.beforeinfo.lap",
    fHoldCount: lane === 6 ? 1 : 0,
    featureSnapshot: {
      avg_st: 0.12 + lane * 0.01,
      nationwide_win_rate: 7 - lane * 0.25,
      local_win_rate: 6.7 - lane * 0.22,
      motor2_rate: 42 - lane * 2,
      boat2_rate: 39 - lane * 1.5,
      motor3_rate: 58 - lane * 2,
      motor_total_score: 11 - lane * 0.6,
      course_fit_score: 5 - lane * 0.35,
      entry_advantage_score: lane <= 4 ? 7 - lane * 0.6 : 3,
      course1_win_rate: lane === 1 ? 59 : null,
      course1_2rate: lane === 1 ? 73 : null,
      course2_2rate: lane === 2 ? 55 : null,
      course3_3rate: lane === 3 ? 58 : null,
      course4_3rate: lane === 4 ? 50 : null,
      coverage_report: {
        lapTime: { status: "ok", value: 6.75 + lane * 0.02, normalized: 6.75 + lane * 0.02, required: false },
        exhibition_st: { status: "ok", value: 0.08 + lane * 0.01, normalized: 0.08 + lane * 0.01, required: false },
        exhibition_time: { status: "ok", value: 6.7 + lane * 0.02, normalized: 6.7 + lane * 0.02, required: false },
        motor_3ren: { status: "ok", value: 58 - lane * 2, normalized: 58 - lane * 2, required: false },
        lane_1st_rate: { status: "ok", value: 65 - lane * 3, normalized: 65 - lane * 3, required: false },
        lane_2ren_rate: { status: "ok", value: 72 - lane * 3, normalized: 72 - lane * 3, required: false },
        lane_3ren_rate: { status: "ok", value: 80 - lane * 3, normalized: 80 - lane * 3, required: false },
        stability_rate: { status: "ok", value: 62 - lane, normalized: 62 - lane, required: false },
        breakout_rate: { status: "ok", value: 38 + lane, normalized: 38 + lane, required: false },
        sashi_rate: { status: "ok", value: 28 + lane, normalized: 28 + lane, required: false },
        makuri_rate: { status: "ok", value: 30 + lane, normalized: 30 + lane, required: false },
        makurisashi_rate: { status: "ok", value: 26 + lane, normalized: 26 + lane, required: false },
        zentsuke_tendency: { status: "ok", value: 22 + lane, normalized: 22 + lane, required: false }
      }
    }
  };
}

function buildRouteData(refreshMeta, timings) {
  return {
    ok: true,
    raceId: "20260327-13-01",
    race: { date: "2026-03-27", venueId: 13, venueName: "Amagasaki", raceNo: 1 },
    racers: [1, 6, 3, 4, 2, 5].map(makeRacer),
    source: {
      mode: "pure_inference",
      local_inference: true,
      refresh_meta: refreshMeta,
      timings,
      fetch_timings: timings,
      local_snapshots: {
        race_snapshot: true,
        entry_snapshot: 6,
        feature_snapshot: 6,
        prediction_feature_event_snapshot: false,
        prediction_log_snapshot: false,
        index_snapshot_status: refreshMeta?.fallback_used ? "FALLBACK" : "READY",
        last_snapshot_updated_at: refreshMeta?.last_snapshot_updated_at || null
      },
      coverage_report: {
        summary: {
          total: 0,
          ok: 0,
          fallback: 0,
          broken_pipeline: 0,
          missing: 0,
          not_published: 0,
          required_broken_pipeline: 0,
          required_missing: 0,
          optional_issues: 0
        },
        fields: {}
      },
      coverage_report_summary: {
        total: 0,
        ok: 0,
        fallback: 0,
        broken_pipeline: 0,
        missing: 0,
        not_published: 0,
        required_broken_pipeline: 0,
        required_missing: 0,
        optional_issues: 0
      }
    },
    diagnostics: {
      snapshot_lookup_ms: 12,
      snapshot_load_ms: 34
    },
    stored_snapshots: {
      prediction_feature_event_snapshot: null,
      prediction_log_snapshot: null
    }
  };
}

async function withServer(run) {
  const app = express();
  app.use("/api", raceRouter);
  const server = await new Promise((resolve) => {
    const started = app.listen(0, "127.0.0.1", () => resolve(started));
  });
  try {
    const address = server.address();
    const baseUrl = `http://127.0.0.1:${address.port}`;
    await run(baseUrl);
  } finally {
    await new Promise((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())));
  }
}

{
  let called = 0;
  setRaceRouteRuntimeDepsForTests({
    refreshLatestRaceData: async () => {
      called += 1;
      return {
        data: buildRouteData(
          {
            refreshed_now: true,
            freshness_status: "refreshed",
            primary_source_ok: true,
            secondary_source_ok: true,
            fallback_used: false,
            last_snapshot_updated_at: "2026-03-27T10:00:00.000Z"
          },
          {
            official_base_fetch_ms: 111,
            kyoteibiyori_fetch_ms: 222,
            parsing_ms: 333
          }
        )
      };
    }
  });

  await withServer(async (baseUrl) => {
    const res = await fetch(`${baseUrl}/api/race?date=2026-03-27&venueId=13&raceNo=1`);
    const body = await res.json();
    assert.equal(res.status, 200);
    assert.equal(called, 1);
    assert.equal(body.refreshed_now, true);
    assert.equal(body.freshness_status, "refreshed");
    assert.equal(body.primary_source_ok, true);
    assert.equal(body.secondary_source_ok, true);
    assert.equal(body.fallback_used, false);
    assert.equal(body.routeTiming.official_base_fetch_ms, 111);
    assert.equal(body.routeTiming.kyoteibiyori_fetch_ms, 222);
    assert.equal(body.routeTiming.parsing_ms, 333);
    assert.equal(body.routeTiming.snapshot_lookup_ms, 12);
    assert.equal(body.routeTiming.snapshot_load_ms, 34);
    assert.deepEqual(body.racers.map((row) => row.lane), [1, 2, 3, 4, 5, 6]);
    assert.equal(body.racers[0].lapSource, "kyoteibiyori.beforeinfo.lap");
    assert.equal(body.racers[0].lapRaw, "36.60");
    assert.equal(body.racers[0].entry, 1);
    assert.equal(body.racers[0].entryStatus, "unconfirmed");
    assert.equal(body.racers[0].predicted_entry, 1);
    assert.equal(body.racers[0].actual_entry, null);
    assert.equal(body.racers[0].entry_confirmed, false);
    assert.equal(typeof body.racers[0].entry_confirmed, "boolean");
    assert.ok(String(body.racers[0].style || "").trim().length > 0);
    assert.equal(typeof body.racers[0].style_score, "number");
    assert.ok(Array.isArray(body.racers[0].style_reasons));
    assert.equal(body.racers[0].style, body.scenario_repro_scores[0].style);
    assert.equal(body.racers[0].style_score, body.scenario_repro_scores[0].score);
    assert.ok(Array.isArray(body.lane_styles));
    assert.ok(Array.isArray(body.scenario_style_trace));
    assert.equal(typeof body.top6Scenario, "string");
    assert.equal(typeof body.top6ScenarioScore, "number");
    assert.equal(typeof body.venue_scenario_bias?.one_course_trust, "number");
  });
}

{
  setRaceRouteRuntimeDepsForTests({
    refreshLatestRaceData: async () => ({
      data: buildRouteData(
        {
          refreshed_now: false,
          freshness_status: "fallback",
          primary_source_ok: false,
          secondary_source_ok: false,
          fallback_used: true,
          last_snapshot_updated_at: "2026-03-27T09:30:00.000Z"
        },
        {
          official_base_fetch_ms: 444,
          kyoteibiyori_fetch_ms: 555,
          parsing_ms: 666
        }
      )
    })
  });

  await withServer(async (baseUrl) => {
    const res = await fetch(`${baseUrl}/api/race?date=2026-03-27&venueId=13&raceNo=1`);
    const body = await res.json();
    assert.equal(res.status, 200);
    assert.equal(body.refreshed_now, false);
    assert.equal(body.freshness_status, "fallback");
    assert.equal(body.fallback_used, true);
    assert.equal(body.routeTiming.official_base_fetch_ms, 444);
    assert.equal(body.routeTiming.kyoteibiyori_fetch_ms, 555);
    assert.equal(body.routeTiming.parsing_ms, 666);
  });
}

resetRaceRouteRuntimeDepsForTests();
console.log("race-route-api integration ok");
