import { firstFinite, firstText, getBoatNo } from "./race-normalizer.js";

export const RACER_TENDENCY_FIELDS = [
  "last6mRaceCount",
  "allCourseLast6mRaceCount",
  "courseSpecificLast6mRaceCount",
  "allCourseWinRate",
  "allCourseSashiRate",
  "allCourseMakuriRate",
  "allCourseMakuriSashiRate",
  "allCourseAvgST",
  "escapeRate",
  "beatenBySashiRate",
  "beatenByMakuriRate",
  "beatenByMakuriSashiRate",
  "sashiRate",
  "makuriRate",
  "makuriSashiRate",
  "avgST",
  "lateStartRate",
  "earlyStartRate"
];
const RACER_TENDENCY_META_FIELDS = ["sampleStatus", "matchMethod", "courseSource"];
const RACER_TENDENCY_MERGE_FIELDS = [...RACER_TENDENCY_FIELDS, ...RACER_TENDENCY_META_FIELDS];

export function normalizeRate01(value) {
  const number = firstFinite(value);
  if (number === null) return null;
  if (number >= 0 && number <= 1) return number;
  if (number >= 0 && number <= 100) return number / 100;
  return null;
}

export function normalizeRacerTendencyRow(row = {}) {
  const boat = getBoatNo(row);
  const explicitCourse = firstFinite(row?.course, row?.entryCourse, row?.entry_course, row?.racer_course_number);
  const course = explicitCourse ?? firstFinite(row?.entryLane, row?.entry, row?.lane, boat);
  const playerTendency = row?.playerTendency || row?.racerCourseStats || {};
  const rate = (...values) => normalizeRate01(firstFinite(...values));
  return {
    boat,
    course,
    coursePredicted: row?.coursePredicted === true || row?.course_predicted === true
      ? true
      : row?.coursePredicted === false || row?.course_predicted === false
        ? false
        : explicitCourse === null,
    racerId: firstText(row?.racerId, row?.racer_id, row?.registrationNo, row?.registration_no, row?.racerNumber, row?.racer_number),
    racerName: firstText(row?.racerName, row?.racer_name, row?.name),
    last6mRaceCount: firstFinite(row?.last6mRaceCount, row?.last_6m_race_count, playerTendency?.last6mRaceCount),
    allCourseLast6mRaceCount: firstFinite(row?.allCourseLast6mRaceCount, row?.all_course_last_6m_race_count, playerTendency?.allCourseLast6mRaceCount),
    courseSpecificLast6mRaceCount: firstFinite(row?.courseSpecificLast6mRaceCount, row?.course_specific_last_6m_race_count, playerTendency?.courseSpecificLast6mRaceCount),
    allCourseWinRate: rate(row?.allCourseWinRate, row?.all_course_win_rate, playerTendency?.allCourseWinRate),
    allCourseSashiRate: rate(row?.allCourseSashiRate, row?.all_course_sashi_rate, playerTendency?.allCourseSashiRate),
    allCourseMakuriRate: rate(row?.allCourseMakuriRate, row?.all_course_makuri_rate, playerTendency?.allCourseMakuriRate),
    allCourseMakuriSashiRate: rate(row?.allCourseMakuriSashiRate, row?.all_course_makuri_sashi_rate, playerTendency?.allCourseMakuriSashiRate),
    allCourseAvgST: firstFinite(row?.allCourseAvgST, row?.all_course_avg_st, playerTendency?.allCourseAvgST),
    sampleStatus: firstText(row?.sampleStatus, row?.sample_status, playerTendency?.sampleStatus) || null,
    matchMethod: firstText(row?.matchMethod, row?.match_method, playerTendency?.matchMethod) || null,
    courseSource: firstText(row?.courseSource, row?.course_source, playerTendency?.courseSource) || null,
    escapeRate: rate(row?.escapeRate, row?.escape_rate, playerTendency?.escapeRate),
    beatenBySashiRate: rate(row?.beatenBySashiRate, row?.beaten_by_sashi_rate, playerTendency?.beatenBySashiRate),
    beatenByMakuriRate: rate(row?.beatenByMakuriRate, row?.beaten_by_makuri_rate, playerTendency?.beatenByMakuriRate),
    beatenByMakuriSashiRate: rate(row?.beatenByMakuriSashiRate, row?.beaten_by_makuri_sashi_rate, playerTendency?.beatenByMakuriSashiRate),
    sashiRate: rate(row?.sashiRate, row?.sashi_rate, playerTendency?.sashiRate),
    makuriRate: rate(row?.makuriRate, row?.makuri_rate, playerTendency?.makuriRate),
    makuriSashiRate: rate(row?.makuriSashiRate, row?.makuri_sashi_rate, row?.makurisashi_rate, playerTendency?.makuriSashiRate),
    avgST: firstFinite(row?.avgST, row?.avg_st, row?.avgStartTiming, playerTendency?.avgST, playerTendency?.avgStartTiming),
    lateStartRate: rate(row?.lateStartRate, row?.late_start_rate, playerTendency?.lateStartRate),
    earlyStartRate: rate(row?.earlyStartRate, row?.early_start_rate, playerTendency?.earlyStartRate)
  };
}

export function normalizeRacerTendencyRows(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map(normalizeRacerTendencyRow)
    .filter((row) => row.boat !== null || row.racerId)
    .sort((a, b) => Number(a.boat || 99) - Number(b.boat || 99));
}

function tendencyKey(row = {}) {
  const racerId = firstText(row?.racerId, row?.registrationNo);
  const course = firstFinite(row?.course, row?.entryCourse, row?.entryLane, row?.boat);
  return racerId && course !== null ? `${racerId}|${course}` : "";
}

function tendencyRacerId(row = {}) {
  return firstText(row?.racerId, row?.registrationNo);
}

function mergeNonNull(base = {}, tendency = {}) {
  const next = { ...base };
  for (const field of RACER_TENDENCY_MERGE_FIELDS) {
    if (next[field] === null || next[field] === undefined) {
      next[field] = tendency?.[field] ?? null;
    }
  }
  return next;
}

export function mergeRacerTendenciesIntoEntries(entries = [], tendencyRows = []) {
  const normalizedRows = normalizeRacerTendencyRows(tendencyRows);
  const byKey = new Map(normalizedRows.map((row) => [tendencyKey(row), row]).filter(([key]) => key));
  const byRacerId = new Map(normalizedRows.map((row) => [tendencyRacerId(row), row]).filter(([racerId]) => racerId));
  const byBoat = new Map(normalizedRows.map((row) => [row.boat, row]).filter(([boat]) => boat !== null));
  return (Array.isArray(entries) ? entries : []).map((entry) => {
    const normalizedEntry = normalizeRacerTendencyRow(entry);
    const racerId = tendencyRacerId(normalizedEntry);
    const boatCandidate = byBoat.get(normalizedEntry.boat);
    const boatCandidateRacerId = tendencyRacerId(boatCandidate);
    const compatibleBoatCandidate =
      !racerId || !boatCandidateRacerId || racerId === boatCandidateRacerId
        ? boatCandidate
        : null;
    const tendency =
      byKey.get(tendencyKey(normalizedEntry)) ||
      byRacerId.get(racerId) ||
      compatibleBoatCandidate ||
      {};
    const normalizedBase = { ...entry };
    for (const field of RACER_TENDENCY_MERGE_FIELDS) {
      if (normalizedEntry[field] !== null && normalizedEntry[field] !== undefined) {
        normalizedBase[field] = normalizedEntry[field];
      }
    }
    const merged = mergeNonNull(normalizedBase, tendency);
    const nested = mergeNonNull(
      {
        ...(entry?.racerCourseStats || {}),
        ...(entry?.playerTendency || {})
      },
      tendency
    );
    for (const field of RACER_TENDENCY_MERGE_FIELDS) {
      if (merged[field] !== null && merged[field] !== undefined) nested[field] = merged[field];
    }
    nested.avgStartTiming = merged.avgST ?? nested.avgStartTiming ?? null;
    return {
      ...merged,
      course: tendency?.coursePredicted === false
        ? tendency?.course
        : entry?.course ?? tendency?.course ?? normalizedEntry.course ?? entry?.boat ?? null,
      coursePredicted: tendency?.coursePredicted === false
        ? false
        : entry?.coursePredicted ?? tendency?.coursePredicted ?? false,
      courseSource: tendency?.courseSource ?? entry?.courseSource ?? null,
      playerTendency: nested,
      racerCourseStats: nested
    };
  });
}

export function buildTendencyPreview(rows = []) {
  return normalizeRacerTendencyRows(rows).map((row) => ({
    boat: row.boat,
    course: row.course,
    coursePredicted: row.coursePredicted,
    racerId: row.racerId,
    last6mRaceCount: row.last6mRaceCount,
    allCourseLast6mRaceCount: row.allCourseLast6mRaceCount,
    courseSpecificLast6mRaceCount: row.courseSpecificLast6mRaceCount,
    allCourseWinRate: row.allCourseWinRate,
    allCourseSashiRate: row.allCourseSashiRate,
    allCourseMakuriRate: row.allCourseMakuriRate,
    allCourseMakuriSashiRate: row.allCourseMakuriSashiRate,
    allCourseAvgST: row.allCourseAvgST,
    sampleStatus: row.sampleStatus,
    matchMethod: row.matchMethod,
    courseSource: row.courseSource,
    escapeRate: row.escapeRate,
    beatenBySashiRate: row.beatenBySashiRate,
    beatenByMakuriRate: row.beatenByMakuriRate,
    beatenByMakuriSashiRate: row.beatenByMakuriSashiRate,
    sashiRate: row.sashiRate,
    makuriRate: row.makuriRate,
    makuriSashiRate: row.makuriSashiRate,
    avgST: row.avgST,
    lateStartRate: row.lateStartRate,
    earlyStartRate: row.earlyStartRate
  }));
}

export function countCanonicalTendencyFields(entries = []) {
  const fields = [
    "avgST",
    "escapeRate",
    "beatenBySashiRate",
    "beatenByMakuriRate",
    "beatenByMakuriSashiRate",
    "sashiRate",
    "makuriRate",
    "makuriSashiRate",
    "allCourseWinRate",
    "allCourseSashiRate",
    "allCourseMakuriRate",
    "allCourseMakuriSashiRate",
    "allCourseAvgST",
    "lateStartRate",
    "earlyStartRate"
  ];
  return Object.fromEntries(fields.map((field) => [
    field,
    (Array.isArray(entries) ? entries : []).filter((row) => row?.[field] !== null && row?.[field] !== undefined).length
  ]));
}

export function tendencyRateLift(value, threshold = 0.5) {
  const normalized = normalizeRate01(value);
  return normalized === null ? 0 : Math.max(0, normalized - threshold);
}

export function tendencyStartScore(value, fallback = null) {
  const st = firstFinite(value);
  if (st === null) return fallback;
  return Math.max(0, Math.min(1, (0.28 - st) / 0.22));
}
