import assert from "node:assert/strict";
import { buildFallbackRacersFromKyoteiBiyori, parseRacersFromRacelist } from "../src/services/boatrace.js";

function buildRow(lane) {
  return `
    <tr>
      <td class="is-boatColor${lane}">${lane}</td>
      <td> </td>
      <td>
        <div class="is-fs11">45${lane}0 <span>A1</span></div>
        <div class="is-fs18 is-fBold"><a>Racer-${lane}</a></div>
        <div class="is-fs11">Ignore</div>
        <div class="is-fs11">Tokyo/2${lane}歳/5${lane}.0kg</div>
      </td>
      <td class="is-lineH2">F0<br>L0<br>0.${lane}3</td>
      <td class="is-lineH2">${6.8 - lane * 0.1}</td>
      <td class="is-lineH2">${6.7 - lane * 0.1}</td>
      <td class="is-lineH2">2連率<br>${40 - lane}</td>
      <td class="is-lineH2">2連率<br>${38 - lane}</td>
    </tr>
  `;
}

function buildSparseRow(lane) {
  return `
    <tr>
      <td class="is-boatColor${lane}">${lane}</td>
      <td> </td>
      <td>
        <div class="is-fs11">55${lane}1 <span>B1</span></div>
        <div class="is-fs18 is-fBold"><a>Sparse-${lane}</a></div>
      </td>
      <td class="is-lineH2">F0<br>L0<br>0.${lane}8</td>
      <td class="is-lineH2">${6.5 - lane * 0.05}</td>
      <td class="is-lineH2">${6.4 - lane * 0.05}</td>
      <td class="is-lineH2">2騾｣邇・br>${35 - lane}</td>
      <td class="is-lineH2">2騾｣邇・br>${33 - lane}</td>
    </tr>
  `;
}

const html = `
  <html>
    <body>
      <div class="table1 is-tableFixed__3rdadd">
        <table>
          <tbody>
            ${[1, 2, 3, 4, 5, 6].map(buildRow).join("\n")}
          </tbody>
        </table>
      </div>
    </body>
  </html>
`;

const result = parseRacersFromRacelist(html);

assert.equal(result.racers.length, 6);
assert.deepEqual(result.racers.map((row) => row.lane), [1, 2, 3, 4, 5, 6]);
assert.equal(result.parserDebug?.parser_stage, "row_scan");
assert.equal(result.parserDebug?.matched_selector_count, 6);
assert.equal(result.racers[0].name, "Racer-1");
assert.equal(result.racers[0].class, "A1");

const sparseHtml = `
  <html>
    <body>
      <table>
        <tbody>
          ${[1, 2, 3, 4, 5, 6].map(buildSparseRow).join("\n")}
        </tbody>
      </table>
    </body>
  </html>
`;

const sparseResult = parseRacersFromRacelist(sparseHtml);
assert.equal(sparseResult.racers.length, 6);
assert.deepEqual(sparseResult.racers.map((row) => row.lane), [1, 2, 3, 4, 5, 6]);
assert.equal(sparseResult.racers[0].name, "Sparse-1");
assert.equal(sparseResult.racers[0].class, "B1");
assert.equal(sparseResult.racers[0].branch, null);
assert.equal(sparseResult.racers[0].age, null);
assert.equal(sparseResult.racers[0].weight, null);

const fallbackRacers = buildFallbackRacersFromKyoteiBiyori({
  byLane: new Map(
    [1, 2, 3, 4, 5, 6].map((lane) => [
      lane,
      {
        playerName: `Kyotei-${lane}`,
        fCount: lane === 6 ? 1 : 0,
        motor2ren: 40 - lane
      }
    ])
  )
});

assert.equal(fallbackRacers.length, 6);
assert.deepEqual(fallbackRacers.map((row) => row.lane), [1, 2, 3, 4, 5, 6]);
assert.equal(fallbackRacers[0].name, "Kyotei-1");
assert.equal(fallbackRacers[0].registrationNo, null);
assert.equal(fallbackRacers[0].class, null);

console.log("boatrace-parser-omura ok");
