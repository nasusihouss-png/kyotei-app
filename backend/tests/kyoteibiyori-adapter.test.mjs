import assert from "node:assert/strict";
import {
  parseKyoteiBiyoriPreRaceData,
  normalizeKyoteiBiyoriPreRaceFields,
  mergeKyoteiBiyoriDataIntoRaceContext
} from "../src/services/kyoteibiyori.js";

const sampleHtml = `
  <table>
    <tr>
      <th>艇番</th>
      <th>選手</th>
      <th>F</th>
      <th>周回タイム</th>
      <th>周回展示</th>
      <th>展示ST</th>
      <th>展示タイム</th>
      <th>モーター2連率</th>
      <th>モーター3連率</th>
    </tr>
    <tr><td>1</td><td>Alpha</td><td>0</td><td>6.73</td><td>抜群</td><td>0.12</td><td>6.79</td><td>46.2%</td><td>61.0%</td></tr>
    <tr><td>2</td><td>Bravo</td><td>1</td><td>6.76</td><td>良い</td><td>0.14</td><td>6.81</td><td>41.5%</td><td>58.4%</td></tr>
    <tr><td>3</td><td>Charlie</td><td>0</td><td>6.82</td><td>普通</td><td>0.18</td><td>6.84</td><td>35.0%</td><td>49.0%</td></tr>
  </table>
`;

const parsed = parseKyoteiBiyoriPreRaceData(sampleHtml);
const normalized = normalizeKyoteiBiyoriPreRaceFields(parsed);

assert.equal(normalized.byLane.get(1)?.lapTime, 6.73, "adapter should parse lap time");
assert.equal(normalized.byLane.get(1)?.lapExhibitionScore, 5, "adapter should score stretch labels");
assert.equal(normalized.byLane.get(2)?.exhibitionSt, 0.14, "adapter should parse exhibition ST");
assert.equal(normalized.byLane.get(1)?.motor2Rate, 46.2, "adapter should parse motor 2-ren");
assert.equal(normalized.fieldDiagnostics.populated_fields.includes("lapTime"), true, "field diagnostics should list populated fields");

const merged = mergeKyoteiBiyoriDataIntoRaceContext({
  racers: [
    { lane: 1, name: "A", exhibitionSt: 0.13, exhibitionTime: 6.8 },
    { lane: 2, name: "B", exhibitionSt: 0.15, exhibitionTime: 6.82 },
    { lane: 3, name: "C", exhibitionSt: 0.18, exhibitionTime: 6.85 }
  ],
  kyoteiBiyori: normalized
});

assert.equal(merged[0].kyoteiBiyoriFetched, 1, "merged racer should mark kyoteibiyori fetch state");
assert.equal(merged[0].lapTime, 6.73, "merged racer should expose lap time for feature building");
assert.equal(merged[0].kyoteiBiyoriStretchFootLabel, "抜群", "merged racer should preserve stretch label");
assert.equal(merged[0].motor2Rate, 46.2, "merged racer should merge motor 2-ren if available");

console.log("kyoteibiyori-adapter tests passed");
