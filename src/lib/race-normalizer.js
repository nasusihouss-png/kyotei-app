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
    last6mRaceCount: firstFinite(row?.last6mRaceCount, row?.last_6m_race_count, tendency?.last6mRaceCount),
    allCourseLast6mRaceCount: firstFinite(row?.allCourseLast6mRaceCount, row?.all_course_last_6m_race_count, tendency?.allCourseLast6mRaceCount),
    courseSpecificLast6mRaceCount: firstFinite(row?.courseSpecificLast6mRaceCount, row?.course_specific_last_6m_race_count, tendency?.courseSpecificLast6mRaceCount),
    sampleStatus: firstText(row?.sampleStatus, row?.sample_status, tendency?.sampleStatus) || null,
    allCourseWinRate: firstFinite(row?.allCourseWinRate, row?.all_course_win_rate, tendency?.allCourseWinRate),
    allCourseSashiRate: firstFinite(row?.allCourseSashiRate, row?.all_course_sashi_rate, tendency?.allCourseSashiRate),
    allCourseMakuriRate: firstFinite(row?.allCourseMakuriRate, row?.all_course_makuri_rate, tendency?.allCourseMakuriRate),
    allCourseMakuriSashiRate: firstFinite(row?.allCourseMakuriSashiRate, row?.all_course_makuri_sashi_rate, tendency?.allCourseMakuriSashiRate),
    allCourseAvgST: firstFinite(row?.allCourseAvgST, row?.all_course_avg_st, tendency?.allCourseAvgST),
    avgST: firstFinite(row?.avgST, row?.avg_st, tendency?.avgST, tendency?.avgStartTiming),
    lateStartRate: firstFinite(row?.lateStartRate, row?.late_start_rate, tendency?.lateStartRate, tendency?.late_start_rate),
    earlyStartRate: firstFinite(row?.earlyStartRate, row?.early_start_rate, tendency?.earlyStartRate, tendency?.early_start_rate),
    escapeRate: firstFinite(row?.escapeRate, row?.escape_rate, tendency?.escapeRate, tendency?.escape_rate),
    beatenBySashiRate: firstFinite(row?.beatenBySashiRate, row?.beaten_by_sashi_rate, tendency?.beatenBySashiRate, tendency?.beaten_by_sashi_rate),
    beatenByMakuriRate: firstFinite(row?.beatenByMakuriRate, row?.beaten_by_makuri_rate, tendency?.beatenByMakuriRate, tendency?.beaten_by_makuri_rate),
    beatenByMakuriSashiRate: firstFinite(row?.beatenByMakuriSashiRate, row?.beaten_by_makuri_sashi_rate, tendency?.beatenByMakuriSashiRate, tendency?.beaten_by_makuri_sashi_rate),
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
  const course = firstFinite(row?.course, row?.entryCourse, row?.entry, row?.entryLane, row?.racer_course_number, previewRow?.racer_course_number) ?? boat;
  return {
    boat,
    lane: firstFinite(row?.lane, row?.racer_boat_number, row?.boatNumber, row?.boat) ?? boat,
    course,
    coursePredicted: row?.coursePredicted ?? row?.course_predicted ?? !(row?.entryConfirmed ?? row?.entry_confirmed ?? false),
    entry: firstFinite(row?.entry, row?.entryLane, row?.racer_course_number, previewRow?.racer_course_number) ?? course,
    entryLane: firstFinite(row?.entryLane, row?.entry, row?.racer_course_number, previewRow?.racer_course_number) ?? course,
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
    last6mRaceCount: tendency.last6mRaceCount,
    allCourseLast6mRaceCount: tendency.allCourseLast6mRaceCount,
    courseSpecificLast6mRaceCount: tendency.courseSpecificLast6mRaceCount,
    sampleStatus: tendency.sampleStatus,
    allCourseWinRate: tendency.allCourseWinRate,
    allCourseSashiRate: tendency.allCourseSashiRate,
    allCourseMakuriRate: tendency.allCourseMakuriRate,
    allCourseMakuriSashiRate: tendency.allCourseMakuriSashiRate,
    allCourseAvgST: tendency.allCourseAvgST,
    avgST: tendency.avgST,
    lateStartRate: tendency.lateStartRate,
    earlyStartRate: tendency.earlyStartRate,
    escapeRate: tendency.escapeRate,
    beatenBySashiRate: tendency.beatenBySashiRate,
    beatenByMakuriRate: tendency.beatenByMakuriRate,
    beatenByMakuriSashiRate: tendency.beatenByMakuriSashiRate,
    nigashiRate: tendency.nigashiRate,
    sashiRate: tendency.sashiRate,
    makuriRate: tendency.makuriRate,
    makuriSashiRate: tendency.makuriSashiRate,
    localCourseWinRate: tendency.localCourseWinRate,
    localCourseQuinellaRate: tendency.localCourseQuinellaRate,
    localCourseTrifectaRate: tendency.localCourseTrifectaRate,
    playerTendency: {
      last6mRaceCount: tendency.last6mRaceCount,
      allCourseLast6mRaceCount: tendency.allCourseLast6mRaceCount,
      courseSpecificLast6mRaceCount: tendency.courseSpecificLast6mRaceCount,
      sampleStatus: tendency.sampleStatus,
      allCourseWinRate: tendency.allCourseWinRate,
      allCourseSashiRate: tendency.allCourseSashiRate,
      allCourseMakuriRate: tendency.allCourseMakuriRate,
      allCourseMakuriSashiRate: tendency.allCourseMakuriSashiRate,
      allCourseAvgST: tendency.allCourseAvgST,
      avgST: tendency.avgST,
      avgStartTiming: tendency.avgST,
      lateStartRate: tendency.lateStartRate,
      earlyStartRate: tendency.earlyStartRate,
      escapeRate: tendency.escapeRate,
      beatenBySashiRate: tendency.beatenBySashiRate,
      beatenByMakuriRate: tendency.beatenByMakuriRate,
      beatenByMakuriSashiRate: tendency.beatenByMakuriSashiRate,
      nigashiRate: tendency.nigashiRate,
      sashiRate: tendency.sashiRate,
      makuriRate: tendency.makuriRate,
      makuriSashiRate: tendency.makuriSashiRate,
      localCourseWinRate: tendency.localCourseWinRate,
      localCourseQuinellaRate: tendency.localCourseQuinellaRate,
      localCourseTrifectaRate: tendency.localCourseTrifectaRate
    },
    racerCourseStats: {
      last6mRaceCount: tendency.last6mRaceCount,
      allCourseLast6mRaceCount: tendency.allCourseLast6mRaceCount,
      courseSpecificLast6mRaceCount: tendency.courseSpecificLast6mRaceCount,
      sampleStatus: tendency.sampleStatus,
      allCourseWinRate: tendency.allCourseWinRate,
      allCourseSashiRate: tendency.allCourseSashiRate,
      allCourseMakuriRate: tendency.allCourseMakuriRate,
      allCourseMakuriSashiRate: tendency.allCourseMakuriSashiRate,
      allCourseAvgST: tendency.allCourseAvgST,
      avgST: tendency.avgST,
      avgStartTiming: tendency.avgST,
      lateStartRate: tendency.lateStartRate,
      earlyStartRate: tendency.earlyStartRate,
      escapeRate: tendency.escapeRate,
      beatenBySashiRate: tendency.beatenBySashiRate,
      beatenByMakuriRate: tendency.beatenByMakuriRate,
      beatenByMakuriSashiRate: tendency.beatenByMakuriSashiRate,
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
