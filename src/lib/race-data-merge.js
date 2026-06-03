import {
  buildCanonicalPreview,
  firstFinite,
  getBoatNo,
  normalizeRaceEntries
} from "./race-normalizer.js";

export function getOriginalExhibitionByBoat(originalExhibition = null) {
  const rows = Array.isArray(originalExhibition?.rows) ? originalExhibition.rows : [];
  return Object.fromEntries(
    rows
      .map((row) => {
        const boat = getBoatNo(row);
        return boat === null ? null : [String(boat), row];
      })
      .filter(Boolean)
  );
}

export function mergeOriginalExhibitionRows(baseEntries = [], originalExhibitionRows = []) {
  const originalByBoat = new Map(
    normalizeRaceEntries(originalExhibitionRows)
      .map((row) => [row.boat, row])
  );
  const merged = baseEntries.map((base) => {
    const boat = getBoatNo(base);
    if (boat === null) return base;
    const original = originalByBoat.get(boat);
    return {
      ...base,
      boat,
      lapTime: base?.lapTime ?? original?.lapTime ?? null,
      straightTime: base?.straightTime ?? original?.straightTime ?? null,
      turnTime: base?.turnTime ?? original?.turnTime ?? null,
      kyoteiBiyoriLapTime: base?.lapTime ?? original?.lapTime ?? null,
      kyoteiBiyoriStraightTime: base?.straightTime ?? original?.straightTime ?? null,
      kyoteiBiyoriTurnTime: base?.turnTime ?? original?.turnTime ?? null
    };
  });
  if (merged.length > 0) return merged;
  return normalizeRaceEntries(originalExhibitionRows);
}

export function canonicalEntriesToProgram(program = {}, entries = []) {
  const byBoat = new Map(entries.map((entry) => [entry.boat, entry]));
  const sourceRows = Array.isArray(program?.boats)
    ? program.boats
    : Array.isArray(program?.entries)
      ? program.entries
      : entries;
  const boats = sourceRows
    .map((row) => {
      const boat = getBoatNo(row);
      if (boat === null) return null;
      const entry = byBoat.get(boat) || {};
      return {
        ...row,
        boat,
        boatNumber: row?.boatNumber ?? boat,
        lane: row?.lane ?? boat,
        racer_boat_number: row?.racer_boat_number ?? boat,
        racer_name: row?.racer_name ?? entry.racerName,
        name: row?.name ?? entry.racerName,
        registrationNo: row?.registrationNo ?? entry.racerId,
        racer_number: row?.racer_number ?? entry.racerId,
        motor2Rate: firstFinite(entry.motor2Rate, row?.motor2Rate),
        motor2ren: firstFinite(entry.motor2Rate, row?.motor2ren),
        lapTime: entry.lapTime ?? null,
        lap_time: entry.lapTime ?? null,
        racer_lap_time: entry.lapTime ?? null,
        kyoteiBiyoriLapTime: entry.lapTime ?? null,
        straightTime: entry.straightTime ?? null,
        straight_time: entry.straightTime ?? null,
        racer_straight_time: entry.straightTime ?? null,
        kyoteiBiyoriStraightTime: entry.straightTime ?? null,
        turnTime: entry.turnTime ?? null,
        turn_time: entry.turnTime ?? null,
        racer_turn_time: entry.turnTime ?? null,
        kyoteiBiyoriTurnTime: entry.turnTime ?? null,
        playerTendency: row?.playerTendency || row?.racerCourseStats || {
          avgStartTiming: entry.avgST,
          lateStartRate: entry.lateStartRate,
          escapeRate: entry.escapeRate,
          nigashiRate: entry.nigashiRate,
          sashiRate: entry.sashiRate,
          makuriRate: entry.makuriRate,
          makuriSashiRate: entry.makuriSashiRate,
          course6TrifectaRate: entry.localCourseTrifectaRate
        }
      };
    })
    .filter(Boolean)
    .sort((a, b) => getBoatNo(a) - getBoatNo(b));
  return {
    ...program,
    boats,
    entries: boats
  };
}

export function buildCanonicalRaceData({
  date = "",
  venueId = null,
  raceNo = null,
  program = {},
  baseProgram = null,
  preview = null,
  originalExhibition = null,
  originalExhibitionRows = null,
  debug = {}
} = {}) {
  const sourceProgram = baseProgram || program || {};
  const normalizedOriginalRows = Array.isArray(originalExhibitionRows)
    ? originalExhibitionRows
    : Array.isArray(originalExhibition?.rows)
      ? originalExhibition.rows
      : [];
  const baseEntries = normalizeRaceEntries(sourceProgram, preview);
  const entries = mergeOriginalExhibitionRows(baseEntries, normalizedOriginalRows);
  const predictionInputProgram = canonicalEntriesToProgram(sourceProgram, entries);
  const canonicalPreview = buildCanonicalPreview(entries);
  const predictionInputPreview = buildCanonicalPreview(normalizeRaceEntries(predictionInputProgram, preview));
  return {
    date,
    venueId: Number(venueId),
    raceNo: Number(raceNo),
    entries,
    debug: {
      ...debug,
      originalExhibition,
      originalExhibitionRows: normalizedOriginalRows,
      baseEntriesCount: baseEntries.length,
      originalExhibitionRowsPreview: buildCanonicalPreview(normalizeRaceEntries(normalizedOriginalRows)),
      canonicalPreview,
      tablePreview: canonicalPreview,
      predictionInputPreview,
      predictionInputProgram
    }
  };
}

export function buildDisplayEntriesFromProgram(program = {}) {
  return normalizeRaceEntries(program);
}

export function buildDisplayEntryPreview(rows = []) {
  return buildCanonicalPreview(normalizeRaceEntries(rows));
}

export function buildTableDisplayPreview(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const boat = getBoatNo(row);
      if (boat === null) return null;
      return {
        boat,
        lapTime: row?.lapTime ?? null,
        straightTime: row?.straightTime ?? null,
        turnTime: row?.turnTime ?? null
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.boat - b.boat);
}

export function mergeDisplayEntriesIntoRows(rows = [], displayEntries = [], originalExhibitionRows = []) {
  const displayByBoat = new Map(normalizeRaceEntries(displayEntries).map((entry) => [entry.boat, entry]));
  const originalByBoat = new Map(normalizeRaceEntries(originalExhibitionRows).map((entry) => [entry.boat, entry]));
  const mergedRows = (Array.isArray(rows) ? rows : []).map((row) => {
    const boat = getBoatNo(row);
    if (boat === null) return row;
    const displayEntry = displayByBoat.get(boat);
    const original = originalByBoat.get(boat);
    return {
      ...row,
      boat,
      boatNumber: row?.boatNumber ?? boat,
      lane: row?.lane ?? boat,
      lapTime: row?.lapTime ?? displayEntry?.lapTime ?? original?.lapTime ?? null,
      straightTime: row?.straightTime ?? displayEntry?.straightTime ?? original?.straightTime ?? null,
      turnTime: row?.turnTime ?? displayEntry?.turnTime ?? original?.turnTime ?? null
    };
  });
  if (mergedRows.length > 0) return mergedRows;
  const fallback = normalizeRaceEntries(displayEntries).length > 0
    ? normalizeRaceEntries(displayEntries)
    : normalizeRaceEntries(originalExhibitionRows);
  return fallback;
}

export function mergeOriginalExhibitionIntoProgram(program = {}, originalExhibition = null, preview = null) {
  return buildCanonicalRaceData({
    program,
    preview,
    originalExhibition
  }).debug.predictionInputProgram;
}
