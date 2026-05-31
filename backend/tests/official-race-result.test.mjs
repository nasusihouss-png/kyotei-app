import assert from "node:assert/strict";
import fs from "node:fs";
import {
  fetchAndStoreOfficialRaceResult,
  parseResultFromDailySummaryHtml,
  parseResultFromRaceresultHtml
} from "../src/services/official-race-result.js";
import db from "../db.js";

const officialHtml = `
  <html><body>
    <table>
      <tr><th>3\u9023\u5358</th><td>1-2-3</td><td>1,200\u5186</td></tr>
    </table>
  </body></html>
`;

const dailyHtml = `
  <html><body>
    <table>
      <tr><th>#24 Omura</th></tr>
      <tr><th>Race</th><th>1st</th><th>2nd</th><th>3rd</th><th>3t</th></tr>
      <tr>
        <td>1R</td>
        <td><div class="ng3r1"></div></td>
        <td><div class="ng3r2"></div></td>
        <td><div class="ng3r3"></div></td>
        <td>1,200\u5186</td>
      </tr>
    </table>
  </body></html>
`;

{
  const parsed = parseResultFromRaceresultHtml(officialHtml);
  assert.deepEqual(parsed.result.top3, [1, 2, 3]);
  assert.equal(parsed.result.combo, "1-2-3");
  assert.equal(parsed.result.payout3t, 1200);
  assert.equal(parsed.parserStage, "trifecta_label_row");
  assert.equal(parsed.matchedSelectorCount, 1);
}

{
  const parsed = parseResultFromDailySummaryHtml(dailyHtml, { venueId: 24, raceNo: 1 });
  assert.deepEqual(parsed.result.top3, [1, 2, 3]);
  assert.equal(parsed.result.combo, "1-2-3");
  assert.equal(parsed.result.payout3t, 1200);
  assert.equal(parsed.parserStage, "daily_summary_row");
  assert.equal(parsed.matchedSelectorCount, 1);
}

{
  const raceId = "20990601_24_1";
  db.prepare("DELETE FROM results WHERE race_id = ?").run(raceId);
  db.prepare("DELETE FROM race_start_displays WHERE race_id = ?").run(raceId);

  const calledUrls = [];
  const result = await fetchAndStoreOfficialRaceResult({
    raceId,
    date: "2099-06-01",
    venueId: 24,
    raceNo: 1,
    timeoutMs: 25,
    httpGet: async (url) => {
      calledUrls.push(url);
      if (url.includes("raceresult")) {
        throw new Error("primary source unavailable");
      }
      if (url.includes("resultlist")) {
        return { data: "<html><body>result list without final order</body></html>" };
      }
      return { data: dailyHtml };
    }
  });

  assert.deepEqual(result.actualTop3, [1, 2, 3]);
  assert.equal(result.winningTrifecta, "1-2-3");
  assert.equal(result.source, "daily_result_summary_page");
  assert.equal(calledUrls.length, 3);
  assert.equal(result.resultFetchUrls.length, 3);
  assert.equal(result.resultParserStage, "daily_summary_row");
  assert.equal(result.resultMatchedSelectorCount, 1);
  assert.ok(result.resultRawSavedPath === null || fs.existsSync(result.resultRawSavedPath));

  const stored = db.prepare("SELECT finish_1, finish_2, finish_3, payout_3t FROM results WHERE race_id = ?").get(raceId);
  assert.deepEqual([stored.finish_1, stored.finish_2, stored.finish_3], [1, 2, 3]);
  assert.equal(stored.payout_3t, 1200);

  db.prepare("DELETE FROM results WHERE race_id = ?").run(raceId);
  db.prepare("DELETE FROM race_start_displays WHERE race_id = ?").run(raceId);
}

console.log("official-race-result ok");
