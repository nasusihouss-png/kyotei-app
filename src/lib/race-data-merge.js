import {
  buildCanonicalPreview,
  firstFinite,
  getBoatNo,
  normalizeRaceEntries
} from "./race-normalizer.js";
import {
  buildTendencyPreview,
  countCanonicalTendencyFields,
  mergeRacerTendenciesIntoEntries,
  normalizeRacerTendencyRows
} from "./racer-tendency-normalizer.js";

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
      const playerTendency = {
        ...(row?.racerCourseStats || {}),
        ...(row?.playerTendency || {}),
        racerId: entry.racerId ?? row?.playerTendency?.racerId ?? null,
        course: entry.course ?? row?.course ?? entry.entryLane ?? boat,
        last6mRaceCount: entry.last6mRaceCount ?? row?.playerTendency?.last6mRaceCount ?? null,
        allCourseLast6mRaceCount: entry.allCourseLast6mRaceCount ?? row?.playerTendency?.allCourseLast6mRaceCount ?? null,
        courseSpecificLast6mRaceCount: entry.courseSpecificLast6mRaceCount ?? row?.playerTendency?.courseSpecificLast6mRaceCount ?? null,
        sampleStatus: entry.sampleStatus ?? row?.playerTendency?.sampleStatus ?? null,
        matchMethod: entry.matchMethod ?? row?.playerTendency?.matchMethod ?? null,
        courseSource: entry.courseSource ?? row?.playerTendency?.courseSource ?? null,
        allCourseWinRate: entry.allCourseWinRate ?? row?.playerTendency?.allCourseWinRate ?? null,
        allCourseSashiRate: entry.allCourseSashiRate ?? row?.playerTendency?.allCourseSashiRate ?? null,
        allCourseMakuriRate: entry.allCourseMakuriRate ?? row?.playerTendency?.allCourseMakuriRate ?? null,
        allCourseMakuriSashiRate: entry.allCourseMakuriSashiRate ?? row?.playerTendency?.allCourseMakuriSashiRate ?? null,
        allCourseAvgST: entry.allCourseAvgST ?? row?.playerTendency?.allCourseAvgST ?? null,
        avgST: entry.avgST ?? row?.playerTendency?.avgST ?? null,
        avgStartTiming: entry.avgST ?? row?.playerTendency?.avgStartTiming ?? null,
        lateStartRate: entry.lateStartRate ?? row?.playerTendency?.lateStartRate ?? null,
        earlyStartRate: entry.earlyStartRate ?? row?.playerTendency?.earlyStartRate ?? null,
        escapeRate: entry.escapeRate ?? row?.playerTendency?.escapeRate ?? null,
        beatenBySashiRate: entry.beatenBySashiRate ?? row?.playerTendency?.beatenBySashiRate ?? null,
        beatenByMakuriRate: entry.beatenByMakuriRate ?? row?.playerTendency?.beatenByMakuriRate ?? null,
        beatenByMakuriSashiRate: entry.beatenByMakuriSashiRate ?? row?.playerTendency?.beatenByMakuriSashiRate ?? null,
        nigashiRate: entry.nigashiRate ?? row?.playerTendency?.nigashiRate ?? null,
        sashiRate: entry.sashiRate ?? row?.playerTendency?.sashiRate ?? null,
        makuriRate: entry.makuriRate ?? row?.playerTendency?.makuriRate ?? null,
        makuriSashiRate: entry.makuriSashiRate ?? row?.playerTendency?.makuriSashiRate ?? null,
        course6TrifectaRate: entry.localCourseTrifectaRate ?? row?.playerTendency?.course6TrifectaRate ?? null
      };
      return {
        ...row,
        boat,
        boatNumber: row?.boatNumber ?? boat,
        lane: row?.lane ?? boat,
        course: entry.course ?? row?.course ?? entry.entryLane ?? boat,
        coursePredicted: entry.coursePredicted ?? row?.coursePredicted ?? false,
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
        last6mRaceCount: entry.last6mRaceCount ?? null,
        allCourseLast6mRaceCount: entry.allCourseLast6mRaceCount ?? null,
        courseSpecificLast6mRaceCount: entry.courseSpecificLast6mRaceCount ?? null,
        sampleStatus: entry.sampleStatus ?? null,
        matchMethod: entry.matchMethod ?? null,
        courseSource: entry.courseSource ?? null,
        allCourseWinRate: entry.allCourseWinRate ?? null,
        allCourseSashiRate: entry.allCourseSashiRate ?? null,
        allCourseMakuriRate: entry.allCourseMakuriRate ?? null,
        allCourseMakuriSashiRate: entry.allCourseMakuriSashiRate ?? null,
        allCourseAvgST: entry.allCourseAvgST ?? null,
        avgST: entry.avgST ?? null,
        lateStartRate: entry.lateStartRate ?? null,
        earlyStartRate: entry.earlyStartRate ?? null,
        escapeRate: entry.escapeRate ?? null,
        beatenBySashiRate: entry.beatenBySashiRate ?? null,
        beatenByMakuriRate: entry.beatenByMakuriRate ?? null,
        beatenByMakuriSashiRate: entry.beatenByMakuriSashiRate ?? null,
        sashiRate: entry.sashiRate ?? null,
        makuriRate: entry.makuriRate ?? null,
        makuriSashiRate: entry.makuriSashiRate ?? null,
        playerTendency,
        racerCourseStats: playerTendency
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
  tendency = null,
  tendencyRows = null,
  debug = {}
} = {}) {
  const sourceProgram = baseProgram || program || {};
  const normalizedOriginalRows = Array.isArray(originalExhibitionRows)
    ? originalExhibitionRows
    : Array.isArray(originalExhibition?.rows)
      ? originalExhibition.rows
      : [];
  const baseEntries = normalizeRaceEntries(sourceProgram, preview);
  const normalizedTendencyRows = Array.isArray(tendencyRows)
    ? normalizeRacerTendencyRows(tendencyRows)
    : normalizeRacerTendencyRows(tendency?.rows);
  const entriesWithOriginal = mergeOriginalExhibitionRows(baseEntries, normalizedOriginalRows);
  const entries = mergeRacerTendenciesIntoEntries(entriesWithOriginal, normalizedTendencyRows);
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
      tendency,
      tendencyRows: normalizedTendencyRows,
      baseEntriesCount: baseEntries.length,
      originalExhibitionRowsPreview: buildCanonicalPreview(normalizeRaceEntries(normalizedOriginalRows)),
      tendencyPreview: buildTendencyPreview(normalizedTendencyRows),
      canonicalTendencyCounts: countCanonicalTendencyFields(entries),
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
