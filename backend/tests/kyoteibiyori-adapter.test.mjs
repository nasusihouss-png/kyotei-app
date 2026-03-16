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
      player_name: "選手A",
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
      player_name: "選手B",
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
      shukai_1_3_1_ave: "71.4286"
    },
    "9999": {
      shukai_1_1_2_ave: "16.6667",
      shukai_1_2_2_ave: "41.6667",
      shukai_1_3_2_ave: "70.8333"
    }
  }
};

const parsedAjax = parseKyoteiBiyoriAjaxData(sampleAjaxPayload);
assert.equal(parsedAjax.byLane.get(1)?.playerName, "選手A");
assert.equal(parsedAjax.byLane.get(1)?.lapTimeRaw, 36.23);
assert.equal(parsedAjax.byLane.get(1)?.lapTime, 6.73);
assert.equal(parsedAjax.byLane.get(1)?.exhibitionTime, 6.75);
assert.equal(parsedAjax.byLane.get(1)?.exhibitionSt, 0.08);
assert.equal(parsedAjax.byLane.get(1)?.laneFirstRate, 66.6667);
assert.equal(parsedAjax.byLane.get(2)?.exhibitionSt, null, "F start should not become a normal ST value");

const sampleHtml = `
  <table>
    <tr>
      <th>コース</th>
      <th>選手</th>
      <th>F</th>
      <th>モーター2連率</th>
      <th>モーター3連率</th>
    </tr>
    <tr><td>1</td><td>選手A</td><td>F0</td><td>46.2%</td><td>61.0%</td></tr>
    <tr><td>2</td><td>選手B</td><td>F1</td><td>41.5%</td><td>58.4%</td></tr>
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
  },
  fieldDebugs: parsedHtml.fieldDebugs || {}
});

assert.equal(normalized.byLane.get(2)?.fCount, 1);

const laneStatsHtml = `
  <table>
    <caption>枠別勝率</caption>
    <tr>
      <th>指標</th>
      <th>期間</th>
      <th>1号艇</th>
      <th>2号艇</th>
      <th>3号艇</th>
      <th>4号艇</th>
      <th>5号艇</th>
      <th>6号艇</th>
    </tr>
    <tr>
      <td>1着率</td>
      <td>今期</td>
      <td>55.5%</td>
      <td>44.4%</td>
      <td>33.3%</td>
      <td>22.2%</td>
      <td>11.1%</td>
      <td>10.0%</td>
    </tr>
    <tr>
      <td>1着率</td>
      <td>直近6か月</td>
      <td>60.0%</td>
      <td>50.0%</td>
      <td>40.0%</td>
      <td>30.0%</td>
      <td>20.0%</td>
      <td>10.0%</td>
    </tr>
    <tr>
      <td>1着率</td>
      <td>直近3ヶ月</td>
      <td>66.7%</td>
      <td>55.5%</td>
      <td>44.4%</td>
      <td>33.3%</td>
      <td>22.2%</td>
      <td>11.1%</td>
    </tr>
    <tr>
      <td>1着率</td>
      <td>直近1か月</td>
      <td>70.0%</td>
      <td>60.0%</td>
      <td>50.0%</td>
      <td>40.0%</td>
      <td>30.0%</td>
      <td>20.0%</td>
    </tr>
    <tr>
      <td>2連率</td>
      <td>今季</td>
      <td>71.0%</td>
      <td>61.0%</td>
      <td>51.0%</td>
      <td>41.0%</td>
      <td>31.0%</td>
      <td>21.0%</td>
    </tr>
    <tr>
      <td>2連率</td>
      <td>直近6か月</td>
      <td>69.0%</td>
      <td>59.0%</td>
      <td>49.0%</td>
      <td>39.0%</td>
      <td>29.0%</td>
      <td>19.0%</td>
    </tr>
    <tr>
      <td>2連率</td>
      <td>直近3ヶ月</td>
      <td>70.8%</td>
      <td>60.4%</td>
      <td>50.1%</td>
      <td>40.0%</td>
      <td>30.8%</td>
      <td>20.2%</td>
    </tr>
    <tr>
      <td>2連率</td>
      <td>直近1か月</td>
      <td>68.0%</td>
      <td>58.0%</td>
      <td>48.0%</td>
      <td>38.0%</td>
      <td>28.0%</td>
      <td>18.0%</td>
    </tr>
    <tr>
      <td>3連率</td>
      <td>今季</td>
      <td>89.9%</td>
      <td>78.8%</td>
      <td>67.7%</td>
      <td>56.6%</td>
      <td>45.5%</td>
      <td>34.4%</td>
    </tr>
    <tr>
      <td>3連率</td>
      <td>直近6か月</td>
      <td>87.7%</td>
      <td>76.6%</td>
      <td>65.5%</td>
      <td>54.4%</td>
      <td>43.3%</td>
      <td>32.2%</td>
    </tr>
    <tr>
      <td>3連率</td>
      <td>直近3ヶ月</td>
      <td>88.8%</td>
      <td>77.7%</td>
      <td>66.7%</td>
      <td>55.5%</td>
      <td>44.4%</td>
      <td>33.3%</td>
    </tr>
    <tr>
      <td>3連率</td>
      <td>直近1か月</td>
      <td>86.6%</td>
      <td>75.5%</td>
      <td>64.4%</td>
      <td>53.3%</td>
      <td>42.2%</td>
      <td>31.1%</td>
    </tr>
  </table>
`;

const preRaceStrictHtml = `
  <table>
    <caption>直前情報</caption>
    <tr>
      <th>項目</th>
      <th>1号艇</th>
      <th>2号艇</th>
      <th>3号艇</th>
      <th>4号艇</th>
      <th>5号艇</th>
      <th>6号艇</th>
    </tr>
    <tr>
      <td>周回</td>
      <td>36.23</td>
      <td>36.45</td>
      <td>36.50</td>
      <td>36.61</td>
      <td>36.73</td>
      <td>36.80</td>
    </tr>
    <tr>
      <td>ST</td>
      <td>.02</td>
      <td>.03</td>
      <td>.13</td>
      <td>F.01</td>
      <td>.11</td>
      <td>.09</td>
    </tr>
    <tr>
      <td>周り足</td>
      <td>6.0</td>
      <td>5.8</td>
      <td>5.6</td>
      <td>5.4</td>
      <td>5.2</td>
      <td>5.0</td>
    </tr>
    <tr>
      <td>伸び足</td>
      <td>7.0</td>
      <td>6.8</td>
      <td>6.6</td>
      <td>6.4</td>
      <td>6.2</td>
      <td>6.0</td>
    </tr>
    <tr>
      <td>モーター2連率</td>
      <td>30.8%</td>
      <td>40.0%</td>
      <td>50.5%</td>
      <td>60.1%</td>
      <td>20.2%</td>
      <td>10.9%</td>
    </tr>
    <tr>
      <td>モーター3連率</td>
      <td>66.7%</td>
      <td>55.5%</td>
      <td>44.4%</td>
      <td>33.3%</td>
      <td>22.2%</td>
      <td>11.1%</td>
    </tr>
  </table>
`;

const strictLaneStats = normalizeKyoteiBiyoriPreRaceFields(
  parseKyoteiBiyoriPreRaceData(laneStatsHtml, { mode: "lane_stats", sourceLabel: "lane_stats_tab" })
);
assert.equal(strictLaneStats.byLane.get(1)?.laneFirstRate, 63.05);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stAvg, 63.05);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_season, 55.5);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_6m, 60);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_3m, 66.7);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_1m, 70);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_sum, 252.2);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_avg, 63.05);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_weighted, 63.945);
assert.equal(strictLaneStats.byLane.get(5)?.lane2renRate_3m, 30.8);
assert.equal(strictLaneStats.byLane.get(3)?.lane3renRate_3m, 66.7);
assert.equal(strictLaneStats.byLane.get(1)?.lane2RenRate, 69.7);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renAvg, 69.7);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renRate_sum, 278.8);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renRate_avg, 69.7);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renRate_weighted, 69.78);
assert.equal(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.avg, 63.05);
assert.deepEqual(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.availablePeriods, ["season", "m6", "m3", "m1"]);
assert.equal(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.finalValue, 63.05);

const strictPreRace = normalizeKyoteiBiyoriPreRaceFields(
  parseKyoteiBiyoriPreRaceData(preRaceStrictHtml, { mode: "pre_race", sourceLabel: "pre_race_tab" })
);
assert.equal(strictPreRace.byLane.get(1)?.lapTimeRaw, 36.23);
assert.equal(strictPreRace.byLane.get(2)?.exhibitionSt, 0.03);
assert.equal(strictPreRace.byLane.get(4)?.exhibitionSt, null);
assert.equal(strictPreRace.byLane.get(1)?.lapExStretch, 6.5);
assert.equal(strictPreRace.byLane.get(1)?.motor2Rate, 30.8);
assert.equal(strictPreRace.byLane.get(1)?.motor3Rate, 66.7);
assert.deepEqual(strictPreRace.fieldDebugs["1"]?.lapExStretch?.raw, {
  mawariashi: "6.0",
  nobiashi: "7.0"
});
assert.equal(strictPreRace.fieldDebugs["1"]?.lapExStretch?.value, 6.5);
assert.equal(strictPreRace.fieldDebugs["1"]?.lapExStretch?.mawariashi?.metric, "\u5468\u308a\u8db3");
assert.equal(strictPreRace.fieldDebugs["1"]?.lapExStretch?.nobiashi?.metric, "\u4f38\u3073\u8db3");
assert.equal(strictPreRace.fieldDebugs["2"]?.exhibitionST?.section, "\u76f4\u524d\u60c5\u5831");
assert.equal(strictPreRace.fieldDebugs["2"]?.exhibitionST?.value, 0.03);

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
