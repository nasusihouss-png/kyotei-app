import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { parseKyoteiBiyoriAjaxData } from "../src/services/kyoteibiyori.js";
import { buildFallbackRacersFromKyoteiBiyori } from "../src/services/boatrace.js";

const fixturePath = path.resolve("backend/tests/fixtures/omura-24-1-kyoteibiyori-ajax.json");
const payload = JSON.parse(fs.readFileSync(fixturePath, "utf8"));

const parsed = parseKyoteiBiyoriAjaxData(payload);

assert.equal(parsed.byLane.size, 6);
assert.equal(parsed.diagnostics?.parsed_ajax_rows_count, 22);
assert.ok((parsed.diagnostics?.mapped_field_count || 0) >= 12);
assert.ok(Array.isArray(parsed.diagnostics?.unknown_type_list));
assert.ok(parsed.diagnostics.unknown_type_list.some((row) => String(row).includes("tenji_ave_data:unknown_type_1")));
assert.equal(parsed.byLane.get(1)?.exhibitionTime, 6.87);
assert.equal(parsed.byLane.get(1)?.lapTimeRaw, 37.11);

const fallbackRacers = buildFallbackRacersFromKyoteiBiyori(parsed);
assert.equal(fallbackRacers.length, 6);
assert.deepEqual(fallbackRacers.map((row) => row.lane), [1, 2, 3, 4, 5, 6]);

console.log("kyoteibiyori-ajax-omura ok");
