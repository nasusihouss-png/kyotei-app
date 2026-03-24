import assert from "node:assert/strict";
import {
  buildRaceApiRequest,
  normalizeVenueIdInput,
  sanitizeRecentRaceSelections
} from "./race-request.js";

{
  const request = buildRaceApiRequest({
    apiBase: "/api",
    baseOrigin: "https://kyotei-app.onrender.com",
    date: "2026-03-23",
    venueId: 13,
    raceNo: 1
  });
  assert.equal(
    request.url.toString(),
    "https://kyotei-app.onrender.com/api/race?date=2026-03-23&venueId=13&raceNo=1"
  );
  assert.equal(request.normalized.venueId, 13);
}

assert.equal(normalizeVenueIdInput(""), null);
assert.equal(normalizeVenueIdInput("Amagasaki"), null);

assert.throws(
  () =>
    buildRaceApiRequest({
      apiBase: "/api",
      baseOrigin: "https://kyotei-app.onrender.com",
      date: "2026-03-23",
      venueId: "",
      raceNo: 1
    }),
  (error) => {
    assert.equal(error.code, "INVALID_VENUE_ID_FRONTEND");
    return true;
  }
);

assert.throws(
  () =>
    buildRaceApiRequest({
      apiBase: "/api",
      baseOrigin: "https://kyotei-app.onrender.com",
      date: "2026-03-23",
      venueId: "Amagasaki",
      raceNo: 1
    }),
  (error) => {
    assert.equal(error.code, "INVALID_VENUE_ID_FRONTEND");
    return true;
  }
);

{
  const quickVenueRequest = buildRaceApiRequest({
    apiBase: "/api",
    baseOrigin: "https://kyotei-app.onrender.com",
    date: "2026-03-23",
    venueId: 13,
    raceNo: 7
  });
  assert.equal(quickVenueRequest.url.searchParams.get("venueId"), "13");
  assert.equal(quickVenueRequest.url.searchParams.get("raceNo"), "7");
}

{
  const recentRows = sanitizeRecentRaceSelections([
    { date: "2026-03-23", venueId: 13, venueName: "Amagasaki", raceNo: 1 },
    { date: "2026-03-23", venueId: "Amagasaki", venueName: "Amagasaki", raceNo: 1 },
    { date: "2026-03-23", venueId: "", venueName: "bad", raceNo: 1 }
  ]);
  assert.equal(recentRows.length, 1);
  const recentRequest = buildRaceApiRequest({
    apiBase: "/api",
    baseOrigin: "https://kyotei-app.onrender.com",
    date: recentRows[0].date,
    venueId: recentRows[0].venueId,
    raceNo: recentRows[0].raceNo
  });
  assert.equal(recentRequest.url.searchParams.get("venueId"), "13");
}

console.log("race-request ok");
