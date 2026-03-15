import assert from "node:assert/strict";
import {
  parseKyoteiBiyoriAjaxData,
  parseKyoteiBiyoriPreRaceData,
  normalizeKyoteiBiyoriPreRaceFields,
  mergeKyoteiBiyoriDataIntoRaceContext
} from "../src/services/kyoteibiyori.js";

const sampleAjaxPayload = {
  chokuzen_list: [
    {
      course: 1,
      player_no: 3514,
      player_name: "山一鉄也",
      tenji: 675,
      shukai: 3623,
      mawariashi: 554,
      chokusen: 599,
      start: ".08",
      shinnyuu: 1
    },
    {
      course: 2,
      player_no: 9999,
      player_name: "木谷賢太",
      tenji: 681,
      shukai: 3643,
      mawariashi: 520,
      chokusen: 551,
      start: "F.01",
      shinnyuu: 2
    }
  ],
  oriten_ave_list: {
    "3514": {
      shukai_1_1_1_ave: "66.6667",
      shukai_1_2_1_ave: "71.4286",
      shukai_1_3_1_ave: "71.4286",
      shukai_1_1_ave: "40.0000",
      shukai_1_2_ave: "57.1429",
      shukai_1_3_ave: "68.5714"
    },
    "9999": {
      shukai_1_1_2_ave: "16.6667",
      shukai_1_2_2_ave: "41.6667",
      shukai_1_3_2_ave: "70.8333",
      shukai_1_1_ave: "33.6066",
      shukai_1_2_ave: "50.0000",
      shukai_1_3_ave: "73.7705"
    }
  }
};

const parsedAjax = parseKyoteiBiyoriAjaxData(sampleAjaxPayload);
assert.equal(parsedAjax.byLane.get(1)?.playerName, "山一鉄也");
assert.equal(parsedAjax.byLane.get(1)?.lapTimeRaw, 36.23);
assert.equal(parsedAjax.byLane.get(1)?.lapTime, 6.73);
assert.equal(parsedAjax.byLane.get(1)?.exhibitionTime, 6.75);
assert.equal(parsedAjax.byLane.get(1)?.exhibitionSt, 0.08);
assert.equal(parsedAjax.byLane.get(1)?.laneFirstRate, 66.6667);
assert.equal(parsedAjax.byLane.get(2)?.exhibitionSt, null, "F start should not become a normal ST value");

const sampleHtml = `
  <table>
    <tr>
      <th>艇番</th>
      <th>選手</th>
      <th>F</th>
      <th>モーター2連率</th>
      <th>モーター3連率</th>
    </tr>
    <tr><td>1</td><td>山一鉄也</td><td>F0</td><td>46.2%</td><td>61.0%</td></tr>
    <tr><td>2</td><td>木谷賢太</td><td>F1</td><td>41.5%</td><td>58.4%</td></tr>
  </table>
`;

const parsedHtml = parseKyoteiBiyoriPreRaceData(sampleHtml);
const mergedByLane = new Map(parsedAjax.byLane);
for (const [lane, row] of parsedHtml.byLane.entries()) {
  const cleanRow = Object.fromEntries(
    Object.entries(row).filter(([, value]) => value !== null && value !== undefined && value !== "")
  );
  mergedByLane.set(lane, {
    ...(mergedByLane.get(lane) || {}),
    ...cleanRow
  });
}
const normalized = normalizeKyoteiBiyoriPreRaceFields({
  byLane: mergedByLane,
  fieldSources: {
    ...(parsedAjax.fieldSources || {}),
    ...(parsedHtml.fieldSources || {})
  }
});

assert.equal(normalized.byLane.get(2)?.fCount, 1);

const merged = mergeKyoteiBiyoriDataIntoRaceContext({
  racers: [
    { lane: 1, name: "A", exhibitionSt: 0.13, exhibitionTime: 6.8 },
    { lane: 2, name: "B", exhibitionSt: 0.15, exhibitionTime: 6.82 }
  ],
  kyoteiBiyori: normalized
});

assert.equal(merged[0].kyoteiBiyoriFetched, 1);
assert.equal(merged[0].kyoteiBiyoriLapTimeRaw, 36.23);
assert.equal(merged[0].lapTime, 6.73);
assert.equal(merged[1].fHoldCount, 1);

console.log("kyoteibiyori-adapter tests passed");
