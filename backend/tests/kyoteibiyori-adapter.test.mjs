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
      player_name: "驕ｸ謇帰",
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
      player_name: "驕ｸ謇毅",
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
assert.equal(typeof parsedAjax.byLane.get(1)?.playerName, "string");
assert.equal(parsedAjax.byLane.get(1)?.lapTimeRaw, 36.23);
assert.equal(parsedAjax.byLane.get(1)?.lapTime, 6.73);
assert.equal(parsedAjax.byLane.get(1)?.exhibitionTime, 6.75);
assert.equal(parsedAjax.byLane.get(1)?.exhibitionSt, 0.08);
assert.equal(parsedAjax.byLane.get(1)?.laneFirstRate, 66.6667);
assert.equal(parsedAjax.byLane.get(2)?.exhibitionSt, null, "F start should not become a normal ST value");

const sampleHtml = `
  <table>
    <tr>
      <th>繧ｳ繝ｼ繧ｹ</th>
      <th>驕ｸ謇・/th>
      <th>F</th>
      <th>繝｢繝ｼ繧ｿ繝ｼ2騾｣邇・/th>
      <th>繝｢繝ｼ繧ｿ繝ｼ3騾｣邇・/th>
    </tr>
    <tr><td>1</td><td>驕ｸ謇帰</td><td>F0</td><td>46.2%</td><td>61.0%</td></tr>
    <tr><td>2</td><td>驕ｸ謇毅</td><td>F1</td><td>41.5%</td><td>58.4%</td></tr>
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
const mergedRaceContext = mergeKyoteiBiyoriDataIntoRaceContext({
  racers: [
    { lane: 1, motor2Rate: 46.2, motor3Rate: 61.0, laneFirstRate: 63.2, lane2RenRate: 71.1, lane3RenRate: 80.2 },
    { lane: 2, motor2Rate: 41.5, motor3Rate: 58.4 }
  ],
  kyoteiBiyori: normalized
});
assert.equal(mergedRaceContext[0]?.predictionFieldMeta?.motor2ren?.is_usable, true);
assert.equal(typeof mergedRaceContext[0]?.predictionFieldMeta?.motor2ren?.source, "string");
assert.equal(typeof mergedRaceContext[0]?.predictionFieldMeta?.exhibitionTime, "object");
assert.equal(mergedRaceContext[0]?.predictionFieldMeta?.exhibitionTime?.is_usable, false);
assert.equal(mergedRaceContext[0]?.predictionFieldMeta?.lapExStretch?.is_usable, false);

const laneStatsHtml = `
  <table>
    <caption>譫蛻･蜍晉紫</caption>
    <tr>
      <th>謖・ｨ・/th>
      <th>譛滄俣</th>
      <th>1蜿ｷ濶・/th>
      <th>2蜿ｷ濶・/th>
      <th>3蜿ｷ濶・/th>
      <th>4蜿ｷ濶・/th>
      <th>5蜿ｷ濶・/th>
      <th>6蜿ｷ濶・/th>
    </tr>
    <tr>
      <td>1逹邇・/td>
      <td>莉頑悄</td>
      <td>55.5%</td>
      <td>44.4%</td>
      <td>33.3%</td>
      <td>22.2%</td>
      <td>11.1%</td>
      <td>10.0%</td>
    </tr>
    <tr>
      <td>1逹邇・/td>
      <td>逶ｴ霑・縺区怦</td>
      <td>60.0%</td>
      <td>50.0%</td>
      <td>40.0%</td>
      <td>30.0%</td>
      <td>20.0%</td>
      <td>10.0%</td>
    </tr>
    <tr>
      <td>1逹邇・/td>
      <td>逶ｴ霑・繝ｶ譛・/td>
      <td>66.7%</td>
      <td>55.5%</td>
      <td>44.4%</td>
      <td>33.3%</td>
      <td>22.2%</td>
      <td>11.1%</td>
    </tr>
    <tr>
      <td>1逹邇・/td>
      <td>逶ｴ霑・縺区怦</td>
      <td>70.0%</td>
      <td>60.0%</td>
      <td>50.0%</td>
      <td>40.0%</td>
      <td>30.0%</td>
      <td>20.0%</td>
    </tr>
    <tr>
      <td>2騾｣邇・/td>
      <td>莉雁ｭ｣</td>
      <td>71.0%</td>
      <td>61.0%</td>
      <td>51.0%</td>
      <td>41.0%</td>
      <td>31.0%</td>
      <td>21.0%</td>
    </tr>
    <tr>
      <td>2騾｣邇・/td>
      <td>逶ｴ霑・縺区怦</td>
      <td>69.0%</td>
      <td>59.0%</td>
      <td>49.0%</td>
      <td>39.0%</td>
      <td>29.0%</td>
      <td>19.0%</td>
    </tr>
    <tr>
      <td>2騾｣邇・/td>
      <td>逶ｴ霑・繝ｶ譛・/td>
      <td>70.8%</td>
      <td>60.4%</td>
      <td>50.1%</td>
      <td>40.0%</td>
      <td>30.8%</td>
      <td>20.2%</td>
    </tr>
    <tr>
      <td>2騾｣邇・/td>
      <td>逶ｴ霑・縺区怦</td>
      <td>68.0%</td>
      <td>58.0%</td>
      <td>48.0%</td>
      <td>38.0%</td>
      <td>28.0%</td>
      <td>18.0%</td>
    </tr>
    <tr>
      <td>3騾｣邇・/td>
      <td>莉雁ｭ｣</td>
      <td>89.9%</td>
      <td>78.8%</td>
      <td>67.7%</td>
      <td>56.6%</td>
      <td>45.5%</td>
      <td>34.4%</td>
    </tr>
    <tr>
      <td>3騾｣邇・/td>
      <td>逶ｴ霑・縺区怦</td>
      <td>87.7%</td>
      <td>76.6%</td>
      <td>65.5%</td>
      <td>54.4%</td>
      <td>43.3%</td>
      <td>32.2%</td>
    </tr>
    <tr>
      <td>3騾｣邇・/td>
      <td>逶ｴ霑・繝ｶ譛・/td>
      <td>88.8%</td>
      <td>77.7%</td>
      <td>66.7%</td>
      <td>55.5%</td>
      <td>44.4%</td>
      <td>33.3%</td>
    </tr>
    <tr>
      <td>3騾｣邇・/td>
      <td>逶ｴ霑・縺区怦</td>
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
    <caption>逶ｴ蜑肴ュ蝣ｱ</caption>
    <tr>
      <th>鬆・岼</th>
      <th>1蜿ｷ濶・/th>
      <th>2蜿ｷ濶・/th>
      <th>3蜿ｷ濶・/th>
      <th>4蜿ｷ濶・/th>
      <th>5蜿ｷ濶・/th>
      <th>6蜿ｷ濶・/th>
    </tr>
    <tr>
      <td>蜻ｨ蝗・/td>
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
      <td>螻慕､ｺ</td>
      <td>6.5</td>
      <td>6.4</td>
      <td>6.3</td>
      <td>6.2</td>
      <td>6.1</td>
      <td>6.0</td>
    </tr>
    <tr>
      <td>蜻ｨ繧願ｶｳ</td>
      <td>6.0</td>
      <td>5.8</td>
      <td>5.6</td>
      <td>5.4</td>
      <td>5.2</td>
      <td>5.0</td>
    </tr>
    <tr>
      <td>莨ｸ縺ｳ雜ｳ</td>
      <td>7.0</td>
      <td>6.8</td>
      <td>6.6</td>
      <td>6.4</td>
      <td>6.2</td>
      <td>6.0</td>
    </tr>
    <tr>
      <td>繝｢繝ｼ繧ｿ繝ｼ2騾｣邇・/td>
      <td>30.8%</td>
      <td>40.0%</td>
      <td>50.5%</td>
      <td>60.1%</td>
      <td>20.2%</td>
      <td>10.9%</td>
    </tr>
    <tr>
      <td>繝｢繝ｼ繧ｿ繝ｼ3騾｣邇・/td>
      <td>66.7%</td>
      <td>55.5%</td>
      <td>44.4%</td>
      <td>33.3%</td>
      <td>22.2%</td>
      <td>11.1%</td>
    </tr>
  </table>
`;

const exactLaneStatsHtml = `
  <table>
    <caption>枠別情報</caption>
    <tr>
      <th>項目</th>
      <th>期間</th>
      <th>1号艇</th>
      <th>2号艇</th>
      <th>3号艇</th>
      <th>4号艇</th>
      <th>5号艇</th>
      <th>6号艇</th>
    </tr>
    <tr><td>1着率</td><td>今期</td><td>55.5%</td><td>44.4%</td><td>33.3%</td><td>22.2%</td><td>11.1%</td><td>10.0%</td></tr>
    <tr><td>1着率</td><td>当地</td><td>60.0%</td><td>50.0%</td><td>40.0%</td><td>30.0%</td><td>20.0%</td><td>10.0%</td></tr>
    <tr><td>1着率</td><td>直近6か月</td><td>70.0%</td><td>60.0%</td><td>50.0%</td><td>40.0%</td><td>30.0%</td><td>20.0%</td></tr>
    <tr><td>1着率</td><td>直近3か月</td><td>66.7%</td><td>55.5%</td><td>44.4%</td><td>33.3%</td><td>22.2%</td><td>11.1%</td></tr>
    <tr><td>2連対率</td><td>今期</td><td>71.0%</td><td>61.0%</td><td>51.0%</td><td>41.0%</td><td>31.0%</td><td>21.0%</td></tr>
    <tr><td>2連対率</td><td>当地</td><td>69.0%</td><td>59.0%</td><td>49.0%</td><td>39.0%</td><td>29.0%</td><td>19.0%</td></tr>
    <tr><td>2連対率</td><td>直近6か月</td><td>70.8%</td><td>60.4%</td><td>50.1%</td><td>40.0%</td><td>30.8%</td><td>20.2%</td></tr>
    <tr><td>2連対率</td><td>直近3か月</td><td>68.0%</td><td>58.0%</td><td>48.0%</td><td>38.0%</td><td>28.0%</td><td>18.0%</td></tr>
    <tr><td>3連対率</td><td>今期</td><td>89.9%</td><td>78.8%</td><td>67.7%</td><td>56.6%</td><td>45.5%</td><td>34.4%</td></tr>
    <tr><td>3連対率</td><td>当地</td><td>87.7%</td><td>76.6%</td><td>65.5%</td><td>54.4%</td><td>43.3%</td><td>32.2%</td></tr>
    <tr><td>3連対率</td><td>直近6か月</td><td>88.8%</td><td>77.7%</td><td>66.7%</td><td>55.5%</td><td>44.4%</td><td>33.3%</td></tr>
    <tr><td>3連対率</td><td>直近3か月</td><td>86.6%</td><td>75.5%</td><td>64.4%</td><td>53.3%</td><td>42.2%</td><td>31.1%</td></tr>
  </table>
`;

const strictLaneStats = normalizeKyoteiBiyoriPreRaceFields(
  parseKyoteiBiyoriPreRaceData(exactLaneStatsHtml, { mode: "lane_stats", sourceLabel: "lane_stats_tab" })
);
assert.equal(strictLaneStats.byLane.get(1)?.laneFirstRate, 64.2071);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stScore, 64.2071);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stAvg, 64.2071);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_season, 55.5);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_6m, 70);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_3m, 66.7);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_1m, null);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_local, 60);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_sum, 252.2);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_avg, 63.05);
assert.equal(strictLaneStats.byLane.get(1)?.lane1stRate_weighted, 64.2071);
assert.equal(strictLaneStats.byLane.get(5)?.lane2renRate_6m, 30.8);
assert.equal(strictLaneStats.byLane.get(3)?.lane3renRate_6m, 66.7);
assert.equal(strictLaneStats.byLane.get(1)?.lane2RenRate, 69.5667);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renScore, 69.5667);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renAvg, 69.5667);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renRate_sum, 278.8);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renRate_avg, 69.7);
assert.equal(strictLaneStats.byLane.get(1)?.lane2renRate_weighted, 69.5667);
assert.equal(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.avg, 63.05);
assert.deepEqual(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.availablePeriods, ["season", "m6", "m3", "local"]);
assert.deepEqual(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.weights_used, { season: 0.2143, m6: 0.2619, m3: 0.381, local: 0.1429 });
assert.equal(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.hot_form_bonus, 0);
assert.equal(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.finalValue, 64.2071);
assert.equal(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.final_score, 64.2071);
assert.equal(strictLaneStats.fieldDebugs["1"]?.lane1stRate?.exact_verified, true);

const exactPreRaceStrictHtml = `
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
    <tr><td>周回</td><td>36.23</td><td>36.45</td><td>36.50</td><td>36.61</td><td>36.73</td><td>36.80</td></tr>
    <tr><td>ST</td><td>.02</td><td>.03</td><td>.13</td><td>F.01</td><td>.11</td><td>.09</td></tr>
    <tr><td>展示</td><td>6.5</td><td>6.4</td><td>6.3</td><td>6.2</td><td>6.1</td><td>6.0</td></tr>
    <tr><td>周り足</td><td>6.0</td><td>5.8</td><td>5.6</td><td>5.4</td><td>5.2</td><td>5.0</td></tr>
    <tr><td>直線</td><td>7.0</td><td>6.8</td><td>6.6</td><td>6.4</td><td>6.2</td><td>6.0</td></tr>
    <tr><td>モーター2連率</td><td>30.8%</td><td>40.0%</td><td>50.5%</td><td>60.1%</td><td>20.2%</td><td>10.9%</td></tr>
    <tr><td>モーター3連率</td><td>66.7%</td><td>55.5%</td><td>44.4%</td><td>33.3%</td><td>22.2%</td><td>11.1%</td></tr>
  </table>
`;

const strictPreRace = normalizeKyoteiBiyoriPreRaceFields(
  parseKyoteiBiyoriPreRaceData(exactPreRaceStrictHtml, { mode: "pre_race", sourceLabel: "pre_race_tab" })
);
assert.equal(strictPreRace.byLane.get(1)?.lapTimeRaw, 36.23);
assert.equal(strictPreRace.byLane.get(2)?.exhibitionSt, 0.03);
assert.equal(strictPreRace.byLane.get(4)?.exhibitionSt, null);
assert.equal(strictPreRace.byLane.get(1)?.lapExStretch, 6.5);
assert.equal(strictPreRace.byLane.get(1)?.exhibitionTime, 6.5);
assert.equal(strictPreRace.byLane.get(1)?.motor2Rate, 30.8);
assert.equal(strictPreRace.byLane.get(1)?.motor3Rate, 66.7);
assert.equal(strictPreRace.fieldDebugs["1"]?.lapExStretch?.raw, "6.5");
assert.equal(strictPreRace.fieldDebugs["1"]?.lapExStretch?.value, 6.5);
assert.equal(strictPreRace.fieldDebugs["1"]?.lapExStretch?.metric, "\u5c55\u793a");
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

