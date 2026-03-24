import assert from "node:assert/strict";
import {
  OPTIONAL_COVERAGE_FIELDS,
  REQUIRED_COVERAGE_FIELDS
} from "../src/services/snapshot-coverage.js";
import { summarizeCoverageDiagnostics } from "../src/services/snapshot-generator.js";

assert.ok(REQUIRED_COVERAGE_FIELDS.includes("national_win_rate"));
assert.ok(REQUIRED_COVERAGE_FIELDS.includes("boat_2ren"));
assert.ok(!REQUIRED_COVERAGE_FIELDS.includes("lane_1st_rate"));
assert.ok(!REQUIRED_COVERAGE_FIELDS.includes("lane_2ren_rate"));
assert.ok(!REQUIRED_COVERAGE_FIELDS.includes("lane_3ren_rate"));

assert.ok(OPTIONAL_COVERAGE_FIELDS.includes("lapTime"));
assert.ok(OPTIONAL_COVERAGE_FIELDS.includes("lane_1st_rate"));
assert.ok(OPTIONAL_COVERAGE_FIELDS.includes("lane_2ren_rate"));
assert.ok(OPTIONAL_COVERAGE_FIELDS.includes("lane_3ren_rate"));

const diagnostics = summarizeCoverageDiagnostics({
  fields: {
    "lane1.national_win_rate": { required: true, status: "broken_pipeline" },
    "lane1.lane_1st_rate": { required: false, status: "broken_pipeline" },
    "lane2.lane_2ren_rate": { required: false, status: "not_published" },
    "lane3.lapTime": { required: false, status: "ok" }
  }
});

assert.deepEqual(diagnostics.required_broken_fields, ["lane1.national_win_rate"]);
assert.deepEqual(diagnostics.optional_broken_fields, ["lane1.lane_1st_rate"]);
assert.deepEqual(diagnostics.optional_missing_fields, ["lane2.lane_2ren_rate"]);
assert.equal(diagnostics.lap_time_ready_count, 1);
assert.equal(diagnostics.lap_time_total_count, 1);

console.log("snapshot-coverage-classification ok");
