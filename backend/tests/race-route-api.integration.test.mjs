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
    assert.equal(typeof body.venueBiasProfile, "object");
    assert.equal(typeof body.buyPolicy?.code, "string");
    assert.ok(Array.isArray(body.venueAdjustmentReason));
    assert.equal(typeof body.boat1_second_keep_score, "number");
    assert.equal(typeof body.boat1_second_keep_reason, "string");
    assert.equal(typeof body.second_given_head_probabilities, "object");
    assert.equal(typeof body.exacta_shape_bias, "object");
    assert.ok(["high", "medium", "low"].includes(body.confidence_band));
    assert.equal(typeof body.confidence_score, "number");
    assert.equal(typeof body.prediction_stability_score, "number");
    assert.equal(typeof body.buy_confidence_reason, "string");
    assert.equal(typeof body.similarRaceSupport, "object");
    assert.equal(typeof body.similarRaceCount, "number");
    assert.equal(typeof body.similarRaceHitBias, "object");
    assert.ok(Array.isArray(body.similarRaceExamples));
    assert.equal(typeof body.recommendedBetMode, "string");
    assert.equal(body.similarRaceSearchExecuted, true);
    assert.equal(typeof body.similarRaceQueryKey, "object");
    assert.equal(typeof body.similarRaceStoragePath, "string");
    assert.equal(typeof body.similarRaceMatchedCount, "number");
    if (body.similarRaceCount > 0) {
      assert.equal(body.similarRaceSupport?.basis, "history_supported");
      assert.equal(typeof body.similarRaceExamples[0]?.matchedPattern, "string");
      assert.ok(Array.isArray(body.similarRaceExamples[0]?.result));
    }
    assert.ok(Array.isArray(body.near_tie_second_candidates));
    assert.equal(typeof body.close_combo_preserved, "boolean");
    assert.ok(body.combo_gap_score === null || typeof body.combo_gap_score === "number");
    assert.equal(typeof body.hardScenario, "string");
    assert.equal(typeof body.hardScenarioScore, "number");
    assert.equal(typeof body.hard_race_index, "number");
    assert.equal(typeof body.boat1_head_pre, "number");
    assert.equal(typeof body.fit_234_index, "number");
    assert.equal(typeof body.outside_break_risk_pre, "number");
    assert.ok(["BUY-6", "BUY-4", "BORDERLINE", "SKIP"].includes(body.decision));
    assert.equal(typeof body.decision_reason, "string");
    assert.ok(body.hardRace1234 && typeof body.hardRace1234 === "object");
    assert.equal(typeof body.hardRace1234.buyPolicy?.code, "string");
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

{
  setRaceRouteRuntimeDepsForTests({
    refreshLatestRaceData: async () => {
      const error = new Error("Expected exactly 6 complete racers, parsed 0");
      error.statusCode = 503;
      error.code = "invalid_racer_count";
      error.refreshMeta = {
        refreshed_now: false,
        freshness_status: "stale",
        primary_source_ok: false,
        secondary_source_ok: false,
        fallback_used: false,
        last_snapshot_updated_at: null
      };
      error.debug = {
        fetched_urls: {
          racelist: "https://www.boatrace.jp/owpc/pc/race/racelist?rno=1&jcd=24&hd=20260324",
          beforeinfo: "https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno=1&jcd=24&hd=20260324",
          kyoteibiyori: {
            index: "https://kyoteibiyori.com/race_shusso.php?date=20260324&jyo=24",
            lane_stats: "https://kyoteibiyori.com/race_shusso.html?date=20260324&jyo=24&race=1&slider=1",
            pre_race: "https://kyoteibiyori.com/race_shusso.html?date=20260324&jyo=24&race=1&slider=4"
          }
        },
        parser_stage: "fallback_row_scan",
        matched_selector_count: 0,
        raw_html_saved_path: null,
        html_head_preview: "<html><body>omura-preview</body></html>",
        parsed_ajax_row_count: 22,
        parsed_ajax_rows_count: 22,
        mapped_field_count: 12,
        unknown_type_list: ["tenji_ave_data:unknown_type_1"]
      };
      throw error;
    }
  });

  await withServer(async (baseUrl) => {
    const res = await fetch(`${baseUrl}/api/race?date=2026-03-24&venueId=24&raceNo=1`);
    const body = await res.json();
    assert.equal(res.status, 503);
    assert.equal(body.error, "race_api_failed");
    assert.equal(body.code, "invalid_racer_count");
    assert.equal(body.freshness_status, "stale");
    assert.equal(body.primary_source_ok, false);
    assert.equal(body.fallback_used, false);
    assert.equal(body.parser_stage, "fallback_row_scan");
    assert.equal(body.matched_selector_count, 0);
    assert.equal(
      body.fetched_urls?.racelist,
      "https://www.boatrace.jp/owpc/pc/race/racelist?rno=1&jcd=24&hd=20260324"
    );
    assert.equal(
      body.fetched_urls?.kyoteibiyori?.index,
      "https://kyoteibiyori.com/race_shusso.php?date=20260324&jyo=24"
    );
    assert.equal(body.raw_html_saved_path, null);
    assert.ok(String(body.html_head_preview || "").includes("omura-preview"));
    assert.equal(body.parsed_ajax_row_count, 22);
    assert.equal(body.parsed_ajax_rows_count, 22);
    assert.equal(body.mapped_field_count, 12);
    assert.ok(Array.isArray(body.unknown_type_list));
  });
}

resetRaceRouteRuntimeDepsForTests();
console.log("race-route-api integration ok");
