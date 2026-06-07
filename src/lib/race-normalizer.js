export function toNullableNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const text = value.trim();
    if (!text || text === "-") return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

export function toFiniteOrNull(value) {
  return toNullableNumber(value);
}

export function firstFinite(...values) {
  for (const value of values) {
    const num = toNullableNumber(value);
    if (num !== null) return num;
  }
  return null;
}

function parseStartTimingValue(value) {
  const direct = toNullableNumber(value);
  if (direct !== null) return direct;
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text || text === "-") return null;
  const normalized = text
    .replace(/[０-９]/g, (char) => String.fromCharCode(char.charCodeAt(0) - 0xfee0))
    .replace(/[．]/g, ".")
    .replace(/\s+/g, "");
  const match = normalized.match(/^([FL])?([+-]?(?:\d+(?:\.\d+)?|\.\d+))/i);
  if (!match) return null;
  const flag = String(match[1] || "").toUpperCase();
  const num = Number(match[2].startsWith(".") ? `0${match[2]}` : match[2]);
  if (!Number.isFinite(num)) return null;
  return flag === "F" && num > 0 ? -Math.abs(num) : num;
}

function firstStartTiming(...values) {
  for (const value of values) {
    const num = parseStartTimingValue(value);
    if (num !== null) return num;
  }
  return null;
}

function toExhibitionTimeOrNull(value) {
  const num = toNullableNumber(value);
  if (num === null || num <= 0) return null;
  return num;
}

function firstExhibitionTime(...values) {
  for (const value of values) {
    const num = toExhibitionTimeOrNull(value);
    if (num !== null) return num;
  }
  return null;
}

function firstTilt(...values) {
  for (const value of values) {
    const num = firstFinite(value);
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
    courseWinRate: firstFinite(row?.courseWinRate, row?.course_win_rate, row?.localCourseWinRate, row?.local_course_win_rate, tendency?.courseWinRate, tendency?.localCourseWinRate),
    courseQuinellaRate: firstFinite(row?.courseQuinellaRate, row?.course_quinella_rate, row?.course2Rate, row?.course_2rate, row?.localCourseQuinellaRate, row?.local_course_quinella_rate, tendency?.courseQuinellaRate, tendency?.localCourseQuinellaRate),
    courseTrifectaRate: firstFinite(row?.courseTrifectaRate, row?.course_trifecta_rate, row?.course3Rate, row?.course_3rate, row?.localCourseTrifectaRate, row?.local_course_trifecta_rate, tendency?.courseTrifectaRate, tendency?.localCourseTrifectaRate),
    recentWinRate: firstFinite(row?.recentWinRate, row?.recent_win_rate, tendency?.recentWinRate),
    recentQuinellaRate: firstFinite(row?.recentQuinellaRate, row?.recent_quinella_rate, row?.recent2Rate, row?.recent_2rate, tendency?.recentQuinellaRate),
    recentTrifectaRate: firstFinite(row?.recentTrifectaRate, row?.recent_trifecta_rate, row?.recent3Rate, row?.recent_3rate, tendency?.recentTrifectaRate),
    localVenueWinRate: firstFinite(row?.localVenueWinRate, row?.local_venue_win_rate, row?.localWinRate, row?.local_win_rate, tendency?.localVenueWinRate),
    localVenueQuinellaRate: firstFinite(row?.localVenueQuinellaRate, row?.local_venue_quinella_rate, row?.local2Rate, row?.local_2rate, tendency?.localVenueQuinellaRate),
    localVenueTrifectaRate: firstFinite(row?.localVenueTrifectaRate, row?.local_venue_trifecta_rate, row?.local3Rate, row?.local_3rate, tendency?.localVenueTrifectaRate),
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
  const exST = firstStartTiming(
    row?.exST,
    row?.exSt,
    row?.startTiming,
    row?.exhibitionSTRaw,
    row?.exhibitionStRaw,
    row?.exhibitionStSignedValue,
    row?.exhibitionSt,
    row?.exhibitionST,
    row?.exhibition_st,
    row?.racer_start_timing,
    previewRow?.racer_start_timing
  );
  const exTime = firstExhibitionTime(
    row?.exTime,
    row?.exhibitionTime,
    row?.exhibition_time,
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
  const motor3Rate = firstFinite(
    row?.motor3Rate,
    row?.motor3ren,
    row?.motor_3rate,
    row?.racer_assigned_motor_top_3_percent,
    row?.kyoteiBiyoriMotor3Rate
  );
  const tilt = firstTilt(row?.tilt, row?.tiltAngle, row?.tilt_angle, row?.racer_tilt, row?.racer_tilt_angle);
  const tiltChange = firstTilt(row?.tiltChange, row?.tilt_change, row?.racer_tilt_change);
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
    motor3Rate,
    motor3ren: motor3Rate,
    motorNo: firstText(row?.motorNo, row?.motorNumber, row?.motor_no, row?.racer_assigned_motor_number) || null,
    motorRankAtVenue: firstFinite(row?.motorRankAtVenue, row?.motor_rank_at_venue, row?.motorRank, row?.motor_rank),
    motorPercentileAtVenue: firstFinite(row?.motorPercentileAtVenue, row?.motor_percentile_at_venue, row?.motorPercentile, row?.motor_percentile),
    motorStrengthLabel: firstText(row?.motorStrengthLabel, row?.motor_strength_label) || null,
    motorRecentForm: firstText(row?.motorRecentForm, row?.motor_recent_form) || null,
    motorPartChange: firstText(row?.motorPartChange, row?.motor_part_change) || null,
    motorCompatibilityScore: firstFinite(row?.motorCompatibilityScore, row?.motor_compatibility_score),
    racerMotorCompatibilityScore: firstFinite(row?.racerMotorCompatibilityScore, row?.racer_motor_compatibility_score),
    tilt,
    tiltChange,
    tiltLabel: firstText(row?.tiltLabel, row?.tilt_label) || null,
    last6mRaceCount: tendency.last6mRaceCount,
    allCourseLast6mRaceCount: tendency.allCourseLast6mRaceCount,
    courseSpecificLast6mRaceCount: tendency.courseSpecificLast6mRaceCount,
    sampleStatus: tendency.sampleStatus,
    allCourseWinRate: tendency.allCourseWinRate,
    allCourseSashiRate: tendency.allCourseSashiRate,
    allCourseMakuriRate: tendency.allCourseMakuriRate,
    allCourseMakuriSashiRate: tendency.allCourseMakuriSashiRate,
    allCourseAvgST: tendency.allCourseAvgST,
    courseWinRate: tendency.courseWinRate,
    courseQuinellaRate: tendency.courseQuinellaRate,
    courseTrifectaRate: tendency.courseTrifectaRate,
    recentWinRate: tendency.recentWinRate,
    recentQuinellaRate: tendency.recentQuinellaRate,
    recentTrifectaRate: tendency.recentTrifectaRate,
    localVenueWinRate: tendency.localVenueWinRate,
    localVenueQuinellaRate: tendency.localVenueQuinellaRate,
    localVenueTrifectaRate: tendency.localVenueTrifectaRate,
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
      courseWinRate: tendency.courseWinRate,
      courseQuinellaRate: tendency.courseQuinellaRate,
      courseTrifectaRate: tendency.courseTrifectaRate,
      recentWinRate: tendency.recentWinRate,
      recentQuinellaRate: tendency.recentQuinellaRate,
      recentTrifectaRate: tendency.recentTrifectaRate,
      localVenueWinRate: tendency.localVenueWinRate,
      localVenueQuinellaRate: tendency.localVenueQuinellaRate,
      localVenueTrifectaRate: tendency.localVenueTrifectaRate,
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
      courseWinRate: tendency.courseWinRate,
      courseQuinellaRate: tendency.courseQuinellaRate,
      courseTrifectaRate: tendency.courseTrifectaRate,
      recentWinRate: tendency.recentWinRate,
      recentQuinellaRate: tendency.recentQuinellaRate,
      recentTrifectaRate: tendency.recentTrifectaRate,
      localVenueWinRate: tendency.localVenueWinRate,
      localVenueQuinellaRate: tendency.localVenueQuinellaRate,
      localVenueTrifectaRate: tendency.localVenueTrifectaRate,
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
      exST: row?.exST ?? null,
      exTime: row?.exTime ?? null,
      lapTime: row?.lapTime ?? null,
      straightTime: row?.straightTime ?? null,
      turnTime: row?.turnTime ?? null,
      tilt: row?.tilt ?? null,
      motorRankAtVenue: row?.motorRankAtVenue ?? null,
      motorPercentileAtVenue: row?.motorPercentileAtVenue ?? null
    }))
    .filter((row) => row.boat !== null)
    .sort((a, b) => a.boat - b.boat);
}
