import assert from "node:assert/strict";
import { buildHardRace1234Response } from "../src/services/hard-race-1234.js";

const baseData = {
  source: {
    racelistUrl: "https://www.boatrace.jp/owpc/pc/race/racelist?rno=1&jcd=05&hd=20260321",
    beforeinfoUrl: "https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno=1&jcd=05&hd=20260321",
    official_fetch_status: { racelist: "success", beforeinfo: "success" },
    kyotei_biyori: {
      ok: false,
      tried_urls: [],
      field_diagnostics: { populated_fields: [], failed_fields: ["sashi_rate", "makuri_rate"] }
    }
  },
  race: { date: "2026-03-21", venueId: 5, venueName: "Tamagawa", raceNo: 1 },
  racers: [
    { lane: 1, name: "A", class: "A1", nationwideWinRate: 7.2, localWinRate: 7.6, avgSt: 0.13, fHoldCount: 0, lHoldCount: 0, motor2Rate: 41, motor3Rate: 58, boat2Rate: 39, boat3Rate: 54, entryCourse: 1 },
    { lane: 2, name: "B", class: "A2", nationwideWinRate: 6.4, localWinRate: 6.7, avgSt: 0.15, fHoldCount: 0, lHoldCount: 0, motor2Rate: 38, motor3Rate: 52, boat2Rate: 36, boat3Rate: 49, entryCourse: 2 },
    { lane: 3, name: "C", class: "A2", nationwideWinRate: 6.1, localWinRate: 6.0, avgSt: 0.14, fHoldCount: 0, lHoldCount: 0, motor2Rate: 37, motor3Rate: 50, boat2Rate: 34, boat3Rate: 48, entryCourse: 3 },
    { lane: 4, name: "D", class: "B1", nationwideWinRate: 5.7, localWinRate: 5.6, avgSt: 0.16, fHoldCount: 0, lHoldCount: 0, motor2Rate: 35, motor3Rate: 47, boat2Rate: 33, boat3Rate: 44, entryCourse: 4 },
    { lane: 5, name: "E", class: "B1", nationwideWinRate: 5.2, localWinRate: 5.0, avgSt: 0.18, fHoldCount: 0, lHoldCount: 0, motor2Rate: 29, motor3Rate: 40, boat2Rate: 31, boat3Rate: 42, entryCourse: 5 },
    { lane: 6, name: "F", class: "B1", nationwideWinRate: 4.9, localWinRate: 4.6, avgSt: 0.19, fHoldCount: 1, lHoldCount: 0, motor2Rate: 27, motor3Rate: 37, boat2Rate: 28, boat3Rate: 39, entryCourse: 6 }
  ]
};

const result = await buildHardRace1234Response({
  data: baseData,
  date: "2026-03-21",
  venueId: 5,
  raceNo: 1
});

assert.equal(result.data_status, "PARTIAL");
assert.ok(Number.isFinite(Number(result.boat1_escape_trust)));
assert.ok(Number.isFinite(Number(result.opponent_234_fit)));
assert.ok(Number.isFinite(Number(result.fixed1234_total_probability)));
assert.equal(Object.keys(result.fixed1234_matrix).length, 6);
assert.ok(["BUY-4", "BUY-6", "BORDERLINE", "SKIP"].includes(result.decision));
assert.ok(Array.isArray(result.missing_fields));

console.log("hard-race-1234 ok");
