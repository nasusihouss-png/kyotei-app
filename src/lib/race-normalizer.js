export function toFiniteOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

export function firstFinite(...values) {
  for (const value of values) {
    const num = toFiniteOrNull(value);
    if (num !== null) return num;
  }
  return null;
}

export function firstText(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

export function getBoatNo(row = {}) {
  const boat = Number(
    row?.boat ??
    row?.boatNumber ??
    row?.racer_boat_number ??
    row?.entry ??
    row?.lane
  );
  return Number.isInteger(boat) && boat >= 1 && boat <= 6 ? boat : null;
}

function getPreviewRows(preview = {}) {
  const boats = preview?.boats;
  if (Array.isArray(boats)) return boats;
  if (boats && typeof boats === "object") {
    return Object.entries(boats).map(([boatKey, row]) => ({
      ...(row || {}),
      racer_boat_number: row?.racer_boat_number ?? Number(boatKey)
    }));
  }
  return [];
}

function getPreviewByBoat(preview = {}) {
  return new Map(
    getPreviewRows(preview)
      .map((row) => [getBoatNo(row), row])
      .filter(([boat]) => boat !== null)
  );
}

function normalizeTendency(row = {}) {
  const tendency = row?.playerTendency || row?.racerCourseStats || row?.player_tendency || row?.racer_course_stats || {};
  return {
    avgST: firstFinite(row?.avgST, row?.avg_st, row?.averageStartTiming, row?.racer_average_start_timing, tendency?.avgST, tendency?.avgStartTiming),
    lateStartRate: firstFinite(row?.lateStartRate, row?.late_start_rate, tendency?.lateStartRate, tendency?.late_start_rate),
    earlyStartRate: firstFinite(row?.earlyStartRate, row?.early_start_rate, tendency?.earlyStartRate, tendency?.early_start_rate),
    escapeRate: firstFinite(row?.escapeRate, row?.escape_rate, tendency?.escapeRate, tendency?.escape_rate),
    nigashiRate: firstFinite(row?.nigashiRate, row?.nigashi_rate, tendency?.nigashiRate, tendency?.nigashi_rate),
    sashiRate: firstFinite(row?.sashiRate, row?.sashi_rate, tendency?.sashiRate, tendency?.sashi_rate),
    makuriRate: firstFinite(row?.makuriRate, row?.makuri_rate, tendency?.makuriRate, tendency?.makuri_rate),
    makuriSashiRate: firstFinite(row?.makuriSashiRate, row?.makuriSashi_rate, row?.makurisashi_rate, tendency?.makuriSashiRate, tendency?.makurisashi_rate),
    localCourseWinRate: firstFinite(row?.localCourseWinRate, row?.local_course_win_rate, tendency?.localCourseWinRate, tendency?.local_course_win_rate),
    localCourseQuinellaRate: firstFinite(row?.localCourseQuinellaRate, row?.local_course_quinella_rate, tendency?.localCourseQuinellaRate, tendency?.local_course_quinella_rate),
    localCourseTrifectaRate: firstFinite(row?.localCourseTrifectaRate, row?.local_course_trifecta_rate, tendency?.localCourseTrifectaRate, tendency?.local_course_trifecta_rate)
  };
}

export function getRaceEntryRows(source = {}) {
  if (Array.isArray(source)) return source;
  if (Array.isArray(source?.entries)) return source.entries;
  if (Array.isArray(source?.boats)) return source.boats;
  return [];
}

export function normalizeRaceEntry(row = {}, previewRow = null) {
  const boat = getBoatNo(row);
  if (boat === null) return null;
  const exST = firstFinite(
    row?.exST,
    row?.exSt,
    row?.exhibitionSt,
    row?.exhibitionST,
    row?.racer_start_timing,
    previewRow?.racer_start_timing
  );
  const exTime = firstFinite(
    row?.exTime,
    row?.exhibitionTime,
    row?.racer_exhibition_time,
    previewRow?.racer_exhibition_time
  );
  const motor2Rate = firstFinite(
    row?.motor2Rate,
    row?.motor2ren,
    row?.motor_2rate,
    row?.racer_assigned_motor_top_2_percent,
    row?.kyoteiBiyoriMotor2Rate
  );
  const tendency = normalizeTendency(row);
  return {
    boat,
    lane: firstFinite(row?.lane, row?.racer_boat_number, row?.boatNumber, row?.boat) ?? boat,
    entry: firstFinite(row?.entry, row?.entryLane, row?.racer_course_number, previewRow?.racer_course_number) ?? boat,
    entryLane: firstFinite(row?.entryLane, row?.entry, row?.racer_course_number, previewRow?.racer_course_number) ?? boat,
    entryConfirmed: row?.entryConfirmed ?? row?.entry_confirmed ?? false,
    racerName: firstText(row?.racerName, row?.name, row?.racer_name, `Boat ${boat}`),
    name: firstText(row?.name, row?.racerName, row?.racer_name, `Boat ${boat}`),
    racerId: firstText(row?.racerId, row?.registrationNo, row?.racerNumber, row?.racer_number),
    registrationNo: firstText(row?.registrationNo, row?.racerId, row?.racerNumber, row?.racer_number),
    fCount: firstFinite(row?.fCount, row?.fHoldCount, row?.racer_flying_count),
    exST,
    exhibitionSt: exST,
    exhibitionST: exST,
    exTime,
    exhibitionTime: exTime,
    lapTime: firstFinite(row?.lapTime, row?.lap_time, row?.racer_lap_time, row?.kyoteiBiyoriLapTime, row?.kyoteibiyori_lap_time),
    straightTime: firstFinite(row?.straightTime, row?.straight_time, row?.racer_straight_time, row?.kyoteiBiyoriStraightTime, row?.kyoteibiyori_straight_time),
    turnTime: firstFinite(row?.turnTime, row?.turn_time, row?.racer_turn_time, row?.kyoteiBiyoriTurnTime, row?.kyoteibiyori_turn_time),
    motor2Rate,
    motor2ren: motor2Rate,
    avgST: tendency.avgST,
    lateStartRate: tendency.lateStartRate,
    earlyStartRate: tendency.earlyStartRate,
    escapeRate: tendency.escapeRate,
    nigashiRate: tendency.nigashiRate,
    sashiRate: tendency.sashiRate,
    makuriRate: tendency.makuriRate,
    makuriSashiRate: tendency.makuriSashiRate,
    localCourseWinRate: tendency.localCourseWinRate,
    localCourseQuinellaRate: tendency.localCourseQuinellaRate,
    localCourseTrifectaRate: tendency.localCourseTrifectaRate,
    playerTendency: {
      avgStartTiming: tendency.avgST,
      lateStartRate: tendency.lateStartRate,
      earlyStartRate: tendency.earlyStartRate,
      escapeRate: tendency.escapeRate,
      nigashiRate: tendency.nigashiRate,
      sashiRate: tendency.sashiRate,
      makuriRate: tendency.makuriRate,
      makuriSashiRate: tendency.makuriSashiRate,
      localCourseWinRate: tendency.localCourseWinRate,
      localCourseQuinellaRate: tendency.localCourseQuinellaRate,
      localCourseTrifectaRate: tendency.localCourseTrifectaRate
    },
    racerCourseStats: {
      avgStartTiming: tendency.avgST,
      lateStartRate: tendency.lateStartRate,
      earlyStartRate: tendency.earlyStartRate,
      escapeRate: tendency.escapeRate,
      nigashiRate: tendency.nigashiRate,
      sashiRate: tendency.sashiRate,
      makuriRate: tendency.makuriRate,
      makuriSashiRate: tendency.makuriSashiRate,
      localCourseWinRate: tendency.localCourseWinRate,
      localCourseQuinellaRate: tendency.localCourseQuinellaRate,
      localCourseTrifectaRate: tendency.localCourseTrifectaRate
    },
    raw: row
  };
}

export function normalizeRaceEntries(source = {}, preview = null) {
  const previewByBoat = getPreviewByBoat(preview || {});
  return getRaceEntryRows(source)
    .map((row) => normalizeRaceEntry(row, previewByBoat.get(getBoatNo(row)) || null))
    .filter(Boolean)
    .sort((a, b) => a.boat - b.boat);
}

export function buildCanonicalPreview(entries = []) {
  return entries
    .map((row) => ({
      boat: row?.boat ?? null,
      lapTime: row?.lapTime ?? null,
      straightTime: row?.straightTime ?? null,
      turnTime: row?.turnTime ?? null
    }))
    .filter((row) => row.boat !== null)
    .sort((a, b) => a.boat - b.boat);
}
