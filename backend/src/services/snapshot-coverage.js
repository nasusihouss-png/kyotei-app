function toNum(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export const REQUIRED_COVERAGE_FIELDS = Object.freeze([
  "national_win_rate",
  "local_win_rate",
  "avg_st",
  "motor_2ren",
  "f_count"
]);

export const OPTIONAL_COVERAGE_FIELDS = Object.freeze([
  "lapTime",
  "exhibition_st",
  "exhibition_time",
  "l_count",
  "racer_class",
  "motor_3ren",
  "boat_2ren",
  "boat_3ren",
  "lane_1st_rate",
  "lane_2ren_rate",
  "lane_3ren_rate",
  "course_1_head_rate",
  "course_1_2ren_rate",
  "course_2_2ren_rate",
  "course_3_3ren_rate",
  "course_4_3ren_rate",
  "venue_inside_bias",
  "stability_rate",
  "breakout_rate",
  "sashi_rate",
  "makuri_rate",
  "makurisashi_rate",
  "zentsuke_tendency"
]);

function isRequiredCoverageField(fieldName) {
  return REQUIRED_COVERAGE_FIELDS.includes(String(fieldName || ""));
}

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function round(value, digits = 4) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(Number(value).toFixed(digits));
}

function normalizePercent(value, min, max) {
  const n = toNum(value, null);
  if (n === null) return null;
  return Math.max(0, Math.min(100, ((n - min) / Math.max(1e-9, max - min)) * 100));
}

function invertPercent(value, min, max) {
  const n = normalizePercent(value, min, max);
  return n === null ? null : 100 - n;
}

function weightedAverage(values) {
  const present = (Array.isArray(values) ? values : []).filter(
    (row) => Number.isFinite(Number(row?.value)) && Number.isFinite(Number(row?.weight)) && Number(row.weight) > 0
  );
  if (!present.length) return null;
  const totalWeight = present.reduce((sum, row) => sum + Number(row.weight), 0);
  return totalWeight > 0
    ? present.reduce((sum, row) => sum + Number(row.value) * Number(row.weight), 0) / totalWeight
    : null;
}

function hasPublishedRawValue(rawValue) {
  if (rawValue === null || rawValue === undefined) return false;
  const text = String(rawValue).trim();
  if (!text) return false;
  return !/^(?:[-\u2010-\u2015\u2212ー－]+|n\/a|none|null|not\s*published|未公開|未発表)$/i.test(text);
}

function normalizeCoverageNumeric(value) {
  return value === null || value === undefined || value === "" ? null : toNum(value, null);
}

function normalizeExhibitionTimeValue(value) {
  const numeric = normalizeCoverageNumeric(value);
  if (numeric === null) return null;
  return numeric > 4 && numeric < 9 ? numeric : null;
}

function buildCoverageEntry({
  value,
  source,
  sourcePriority,
  expectedFrom = [],
  fallbackValue = null,
  fallbackSource = "estimated",
  brokenIfMissing = false,
  reason = null,
  raw = null,
  normalized = null,
  required = false,
  missingStatus = null
} = {}) {
  const normalizedValue = normalizeCoverageNumeric(value);
  const normalizedFallbackValue = normalizeCoverageNumeric(fallbackValue);

  if (normalizedValue !== null) {
    return {
      status: "ok",
      value: normalizedValue,
      source: source || null,
      source_priority: sourcePriority || null,
      expected_from: expectedFrom,
      coverage: 1,
      fallback_used: false,
      required: !!required,
      reason: null,
      raw,
      normalized: normalizeCoverageNumeric(normalized) ?? normalizedValue
    };
  }

  if (normalizedFallbackValue !== null) {
    return {
      status: "fallback",
      value: normalizedFallbackValue,
      source: fallbackSource,
      source_priority: "derived",
      expected_from: expectedFrom,
      coverage: 0,
      fallback_used: true,
      required: !!required,
      reason: reason || "estimated_from_snapshot",
      raw,
      normalized: normalizeCoverageNumeric(normalized) ?? normalizedFallbackValue
    };
  }

  const status = missingStatus || (brokenIfMissing ? "broken_pipeline" : "missing");
  return {
    status,
    value: null,
    source: source || null,
    source_priority: sourcePriority || null,
    expected_from: expectedFrom,
    coverage: 0,
    fallback_used: false,
    required: !!required,
    reason: reason || (status === "not_published" ? "not_published" : brokenIfMissing ? "public_source_expected_but_missing" : "not_available"),
    raw,
    normalized: normalizeCoverageNumeric(normalized)
  };
}

function buildLapTimeCoverageEntry({ racer = {}, predictionMeta = {}, kyoteiFetchOk = false } = {}) {
  const raw = predictionMeta?.raw_cell_text ?? racer?.kyoteiBiyoriLapTimeRaw ?? racer?.lapTimeRaw ?? null;
  const normalized = toNum(
    predictionMeta?.normalized_numeric_value,
    toNum(racer?.kyoteiBiyoriLapTime, toNum(racer?.lapTime, null))
  );
  const source = predictionMeta?.source || null;
  const publishedInSource =
    predictionMeta?.published_in_source === true ||
    predictionMeta?.reason === "published_but_parse_failed" ||
    hasPublishedRawValue(raw);

  if (Number.isFinite(normalized)) {
    return {
      status: "ok",
      value: normalized,
      source,
      source_priority: source ? "secondary" : null,
      expected_from: ["kyoteibiyori.com pre-race metrics", "kyoteibiyori ajax pre-race payload"],
      coverage: 1,
      fallback_used: false,
      required: false,
      reason: null,
      raw,
      normalized
    };
  }

  if (publishedInSource) {
    return {
      status: "broken_pipeline",
      value: null,
      source,
      source_priority: source ? "secondary" : null,
      expected_from: ["kyoteibiyori.com pre-race metrics", "kyoteibiyori ajax pre-race payload"],
      coverage: 0,
      fallback_used: false,
      required: false,
      reason: "published_but_parse_failed",
      raw,
      normalized: null
    };
  }

  return {
    status: kyoteiFetchOk ? "not_published" : "missing",
    value: null,
    source,
    source_priority: source ? "secondary" : null,
    expected_from: ["kyoteibiyori.com pre-race metrics", "kyoteibiyori ajax pre-race payload"],
    coverage: 0,
    fallback_used: false,
    required: false,
    reason: kyoteiFetchOk ? "not_published" : "source_unavailable",
    raw,
    normalized: null
  };
}

function resolveMetricSource({ racerValue, featureValue, predictionSource = null, officialSource = null, derivedSource = null } = {}) {
  if (predictionSource) return predictionSource;
  if (Number.isFinite(Number(racerValue))) return officialSource;
  if (Number.isFinite(Number(featureValue))) return derivedSource;
  return null;
}

function derivePredictionMetaMissingStatus(predictionMeta = {}, kyoteiFetchOk = false) {
  if (predictionMeta?.reason === "published_but_parse_failed") return "broken_pipeline";
  return kyoteiFetchOk ? "not_published" : "missing";
}

function buildLaneProxyMetrics({ racer = {}, features = {} }) {
  const avgSt = toNum(racer?.avgSt, null);
  const localWinRate = toNum(racer?.localWinRate, null);
  const nationalWinRate = toNum(racer?.nationwideWinRate, null);
  const entryAdvantage = toNum(features?.entry_advantage_score, null);
  const motorTotal = toNum(features?.motor_total_score, null);
  const courseFit = toNum(features?.course_fit_score, null);
  const courseChange = toNum(features?.course_change_score, null);
  const startStability = weightedAverage([
    { value: invertPercent(avgSt, 0.11, 0.24), weight: 0.48 },
    { value: normalizePercent(localWinRate, 4, 8.5), weight: 0.2 },
    { value: normalizePercent(courseFit, -2, 8), weight: 0.18 },
    { value: normalizePercent(motorTotal, 0, 18), weight: 0.14 }
  ]);
  const makuriRate = weightedAverage([
    { value: normalizePercent(entryAdvantage, 0, 14), weight: 0.44 },
    { value: normalizePercent(motorTotal, 0, 18), weight: 0.26 },
    { value: invertPercent(avgSt, 0.11, 0.24), weight: 0.2 },
    { value: normalizePercent(courseChange, 0, 10), weight: 0.1 }
  ]);
  const makuriSashiRate = weightedAverage([
    { value: normalizePercent(entryAdvantage, 0, 14), weight: 0.3 },
    { value: normalizePercent(courseFit, -2, 8), weight: 0.26 },
    { value: invertPercent(avgSt, 0.11, 0.24), weight: 0.26 },
    { value: normalizePercent(localWinRate ?? nationalWinRate, 4, 8.5), weight: 0.18 }
  ]);
  const sashiRate = weightedAverage([
    { value: normalizePercent(courseFit, -2, 8), weight: 0.34 },
    { value: normalizePercent(localWinRate ?? nationalWinRate, 4, 8.5), weight: 0.26 },
    { value: invertPercent(avgSt, 0.11, 0.24), weight: 0.22 },
    { value: normalizePercent(motorTotal, 0, 18), weight: 0.18 }
  ]);
  const breakoutRate = weightedAverage([
    { value: normalizePercent(entryAdvantage, 0, 14), weight: 0.4 },
    { value: normalizePercent(courseChange, 0, 10), weight: 0.35 },
    { value: invertPercent(avgSt, 0.11, 0.24), weight: 0.25 }
  ]);
  const zentsukeTendency = weightedAverage([
    { value: normalizePercent(entryAdvantage, 0, 14), weight: 0.42 },
    { value: normalizePercent(courseChange, 0, 10), weight: 0.38 },
    { value: normalizePercent(localWinRate ?? nationalWinRate, 4, 8.5), weight: 0.2 }
  ]);

  return {
    stability_rate: round(startStability, 2),
    makuri_rate: round(makuriRate, 2),
    makurisashi_rate: round(makuriSashiRate, 2),
    sashi_rate: round(sashiRate, 2),
    breakout_rate: round(breakoutRate, 2),
    zentsuke_tendency: round(zentsukeTendency, 2)
  };
}

function buildLaneCoverage({ racer = {}, features = {}, raceSource = {} }) {
  const lane = toInt(racer?.lane, null);
  const predictionMeta = racer?.predictionFieldMeta && typeof racer.predictionFieldMeta === "object"
    ? racer.predictionFieldMeta
    : {};
  const kyoteiFetchOk = !!raceSource?.kyotei_biyori?.ok;
  const proxies = buildLaneProxyMetrics({ racer, features });

  const report = {
    lapTime: buildLapTimeCoverageEntry({
      racer,
      predictionMeta: predictionMeta?.lapTime || {},
      kyoteiFetchOk
    }),
    national_win_rate: buildCoverageEntry({
      value: racer?.nationwideWinRate,
      source: "boatrace_racelist",
      sourcePriority: "primary",
      expectedFrom: ["boatrace.jp racelist"],
      brokenIfMissing: true,
      required: isRequiredCoverageField("national_win_rate")
    }),
    local_win_rate: buildCoverageEntry({
      value: racer?.localWinRate,
      source: "boatrace_racelist",
      sourcePriority: "primary",
      expectedFrom: ["boatrace.jp racelist"],
      brokenIfMissing: true,
      required: isRequiredCoverageField("local_win_rate")
    }),
    avg_st: buildCoverageEntry({
      value: racer?.avgSt,
      source: "boatrace_racelist",
      sourcePriority: "primary",
      expectedFrom: ["boatrace.jp racelist"],
      brokenIfMissing: true,
      required: isRequiredCoverageField("avg_st")
    }),
    exhibition_st: buildCoverageEntry({
      value: predictionMeta?.exhibitionST?.is_usable ? racer?.exhibitionSt : null,
      source: predictionMeta?.exhibitionST?.source || null,
      sourcePriority: predictionMeta?.exhibitionST?.source ? "secondary" : null,
      expectedFrom: ["kyoteibiyori.com pre-race metrics"],
      brokenIfMissing: false,
      missingStatus: derivePredictionMetaMissingStatus(predictionMeta?.exhibitionST, kyoteiFetchOk),
      required: isRequiredCoverageField("exhibition_st"),
      raw: predictionMeta?.exhibitionST?.raw_cell_text ?? null,
      normalized: predictionMeta?.exhibitionST?.normalized_numeric_value ?? (predictionMeta?.exhibitionST?.is_usable ? racer?.exhibitionSt : null),
      reason: predictionMeta?.exhibitionST?.reason || null
    }),
    exhibition_time: buildCoverageEntry({
      value: normalizeExhibitionTimeValue(
        predictionMeta?.exhibitionTime?.is_usable ? racer?.exhibitionTime : predictionMeta?.exhibitionTime?.normalized_numeric_value
      ),
      source: predictionMeta?.exhibitionTime?.source || null,
      sourcePriority: predictionMeta?.exhibitionTime?.source ? "secondary" : null,
      expectedFrom: ["kyoteibiyori.com pre-race metrics"],
      brokenIfMissing: false,
      missingStatus: derivePredictionMetaMissingStatus(predictionMeta?.exhibitionTime, kyoteiFetchOk),
      required: isRequiredCoverageField("exhibition_time"),
      raw: predictionMeta?.exhibitionTime?.raw_cell_text ?? null,
      normalized: normalizeExhibitionTimeValue(
        predictionMeta?.exhibitionTime?.normalized_numeric_value ?? (predictionMeta?.exhibitionTime?.is_usable ? racer?.exhibitionTime : null)
      ),
      reason: predictionMeta?.exhibitionTime?.reason || null
    }),
    f_count: buildCoverageEntry({
      value: racer?.fHoldCount,
      source: "boatrace_racelist",
      sourcePriority: "primary",
      expectedFrom: ["boatrace.jp racelist"],
      brokenIfMissing: true,
      required: isRequiredCoverageField("f_count")
    }),
    l_count: buildCoverageEntry({
      value: racer?.lHoldCount,
      source: "boatrace_racelist",
      sourcePriority: "primary",
      expectedFrom: ["boatrace.jp racelist"],
      brokenIfMissing: false,
      required: isRequiredCoverageField("l_count")
    }),
    racer_class: buildCoverageEntry({
      value: racer?.class ? 1 : null,
      source: "boatrace_racelist",
      sourcePriority: "primary",
      expectedFrom: ["boatrace.jp racelist"],
      brokenIfMissing: true,
      fallbackSource: "none"
    }),
    motor_2ren: buildCoverageEntry({
      value: racer?.motor2Rate,
      source: predictionMeta?.motor2ren?.source || "boatrace_racelist",
      sourcePriority: predictionMeta?.motor2ren?.source ? "secondary" : "primary",
      expectedFrom: ["boatrace.jp racelist", "kyoteibiyori.com"],
      brokenIfMissing: true,
      required: isRequiredCoverageField("motor_2ren"),
      raw: predictionMeta?.motor2ren?.raw_cell_text ?? null,
      normalized: predictionMeta?.motor2ren?.normalized_numeric_value ?? racer?.motor2Rate ?? null,
      reason: predictionMeta?.motor2ren?.reason || null
    }),
    motor_3ren: buildCoverageEntry({
      value: features?.motor3_rate,
      source: predictionMeta?.motor3ren?.source || null,
      sourcePriority: predictionMeta?.motor3ren?.source ? "secondary" : null,
      expectedFrom: ["kyoteibiyori.com", "boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("motor_3ren"),
      raw: predictionMeta?.motor3ren?.raw_cell_text ?? null,
      normalized: predictionMeta?.motor3ren?.normalized_numeric_value ?? features?.motor3_rate ?? null,
      reason: predictionMeta?.motor3ren?.reason || null
    }),
    boat_2ren: buildCoverageEntry({
      value: racer?.boat2Rate,
      source: "boatrace_racelist",
      sourcePriority: "primary",
      expectedFrom: ["boatrace.jp racelist"],
      brokenIfMissing: true
    }),
    boat_3ren: buildCoverageEntry({
      value: features?.boat3_rate,
      source: null,
      sourcePriority: null,
      expectedFrom: ["boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: false
    }),
    lane_1st_rate: buildCoverageEntry({
      value: racer?.laneFirstRate ?? features?.lane_fit_1st,
      source: predictionMeta?.lane1stScore?.source || predictionMeta?.lane1stAvg?.source || null,
      sourcePriority: predictionMeta?.lane1stScore?.source || predictionMeta?.lane1stAvg?.source ? "secondary" : null,
      expectedFrom: ["kyoteibiyori.com", "boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("lane_1st_rate")
    }),
    lane_2ren_rate: buildCoverageEntry({
      value: racer?.lane2RenRate ?? features?.lane_fit_2ren,
      source: predictionMeta?.lane2renScore?.source || predictionMeta?.lane2renAvg?.source || null,
      sourcePriority: predictionMeta?.lane2renScore?.source || predictionMeta?.lane2renAvg?.source ? "secondary" : null,
      expectedFrom: ["kyoteibiyori.com", "boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("lane_2ren_rate")
    }),
    lane_3ren_rate: buildCoverageEntry({
      value: racer?.lane3RenRate ?? features?.lane_fit_3ren,
      source: predictionMeta?.lane3renScore?.source || predictionMeta?.lane3renAvg?.source || null,
      sourcePriority: predictionMeta?.lane3renScore?.source || predictionMeta?.lane3renAvg?.source ? "secondary" : null,
      expectedFrom: ["kyoteibiyori.com", "boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("lane_3ren_rate")
    }),
    course_1_head_rate: buildCoverageEntry({
      value: features?.course1_win_rate,
      source: "feature_snapshot",
      sourcePriority: "stored",
      expectedFrom: ["boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: false
    }),
    course_1_2ren_rate: buildCoverageEntry({
      value: features?.course1_2rate,
      source: "feature_snapshot",
      sourcePriority: "stored",
      expectedFrom: ["boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: false
    }),
    course_2_2ren_rate: buildCoverageEntry({
      value: features?.course2_2rate,
      source: "feature_snapshot",
      sourcePriority: "stored",
      expectedFrom: ["boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: false
    }),
    course_3_3ren_rate: buildCoverageEntry({
      value: features?.course3_3rate,
      source: "feature_snapshot",
      sourcePriority: "stored",
      expectedFrom: ["boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: false
    }),
    course_4_3ren_rate: buildCoverageEntry({
      value: features?.course4_3rate,
      source: "feature_snapshot",
      sourcePriority: "stored",
      expectedFrom: ["boatrace.jp racer/profile"],
      fallbackValue: null,
      brokenIfMissing: false
    }),
    venue_inside_bias: buildCoverageEntry({
      value: features?.venue_inner_lane_multiplier ? Number(features.venue_inner_lane_multiplier) * 100 : null,
      source: "venue_lookup",
      sourcePriority: "primary",
      expectedFrom: ["stored venue bias"],
      fallbackValue: 62,
      brokenIfMissing: false
    }),
    stability_rate: buildCoverageEntry({
      value: racer?.stabilityRate ?? features?.stability_rate,
      source: resolveMetricSource({
        racerValue: racer?.stabilityRate,
        featureValue: features?.stability_rate,
        officialSource: "kyoteibiyori.com",
        derivedSource: "feature_snapshot"
      }),
      sourcePriority: Number.isFinite(Number(racer?.stabilityRate)) ? "secondary" : Number.isFinite(Number(features?.stability_rate)) ? "stored" : null,
      expectedFrom: ["kyoteibiyori.com"],
      fallbackValue: proxies.stability_rate,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("stability_rate")
    }),
    breakout_rate: buildCoverageEntry({
      value: racer?.breakoutRate ?? features?.breakout_rate,
      source: resolveMetricSource({
        racerValue: racer?.breakoutRate,
        featureValue: features?.breakout_rate,
        officialSource: "kyoteibiyori.com",
        derivedSource: "feature_snapshot"
      }),
      sourcePriority: Number.isFinite(Number(racer?.breakoutRate)) ? "secondary" : Number.isFinite(Number(features?.breakout_rate)) ? "stored" : null,
      expectedFrom: ["kyoteibiyori.com"],
      fallbackValue: proxies.breakout_rate,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("breakout_rate")
    }),
    sashi_rate: buildCoverageEntry({
      value: racer?.sashiRate ?? features?.sashi_rate,
      source: resolveMetricSource({
        racerValue: racer?.sashiRate,
        featureValue: features?.sashi_rate,
        officialSource: "kyoteibiyori.com",
        derivedSource: "feature_snapshot"
      }),
      sourcePriority: Number.isFinite(Number(racer?.sashiRate)) ? "secondary" : Number.isFinite(Number(features?.sashi_rate)) ? "stored" : null,
      expectedFrom: ["kyoteibiyori.com"],
      fallbackValue: proxies.sashi_rate,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("sashi_rate")
    }),
    makuri_rate: buildCoverageEntry({
      value: racer?.makuriRate ?? features?.makuri_rate,
      source: resolveMetricSource({
        racerValue: racer?.makuriRate,
        featureValue: features?.makuri_rate,
        officialSource: "kyoteibiyori.com",
        derivedSource: "feature_snapshot"
      }),
      sourcePriority: Number.isFinite(Number(racer?.makuriRate)) ? "secondary" : Number.isFinite(Number(features?.makuri_rate)) ? "stored" : null,
      expectedFrom: ["kyoteibiyori.com"],
      fallbackValue: proxies.makuri_rate,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("makuri_rate")
    }),
    makurisashi_rate: buildCoverageEntry({
      value: racer?.makurisashiRate ?? racer?.makuriSashiRate ?? features?.makurisashi_rate,
      source: resolveMetricSource({
        racerValue: racer?.makurisashiRate ?? racer?.makuriSashiRate,
        featureValue: features?.makurisashi_rate,
        officialSource: "kyoteibiyori.com",
        derivedSource: "feature_snapshot"
      }),
      sourcePriority: Number.isFinite(Number(racer?.makurisashiRate ?? racer?.makuriSashiRate)) ? "secondary" : Number.isFinite(Number(features?.makurisashi_rate)) ? "stored" : null,
      expectedFrom: ["kyoteibiyori.com"],
      fallbackValue: proxies.makurisashi_rate,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("makurisashi_rate")
    }),
    zentsuke_tendency: buildCoverageEntry({
      value: racer?.zentsukeTendency ?? features?.zentsuke_tendency,
      source: resolveMetricSource({
        racerValue: racer?.zentsukeTendency,
        featureValue: features?.zentsuke_tendency,
        officialSource: "kyoteibiyori.com",
        derivedSource: "feature_snapshot"
      }),
      sourcePriority: Number.isFinite(Number(racer?.zentsukeTendency)) ? "secondary" : Number.isFinite(Number(features?.zentsuke_tendency)) ? "stored" : null,
      expectedFrom: ["kyoteibiyori.com"],
      fallbackValue: proxies.zentsuke_tendency,
      brokenIfMissing: kyoteiFetchOk,
      required: isRequiredCoverageField("zentsuke_tendency")
    })
  };

  return {
    lane,
    report
  };
}

export function buildRaceCoverageReport({ data = {}, ranking = [] } = {}) {
  const byLane = new Map((Array.isArray(ranking) ? ranking : []).map((row) => [toInt(row?.racer?.lane, null), row]));
  const laneReports = (Array.isArray(data?.racers) ? data.racers : [])
    .map((racer) => {
      const lane = toInt(racer?.lane, null);
      const rankingRow = byLane.get(lane) || {};
      return buildLaneCoverage({
        racer,
        features: rankingRow?.features || {},
        raceSource: data?.source || {}
      });
    })
    .filter((row) => Number.isInteger(row?.lane));

  const fields = {};
  const summary = {
    total: 0,
    ok: 0,
    fallback: 0,
    broken_pipeline: 0,
    missing: 0,
    not_published: 0
  };

  for (const laneRow of laneReports) {
    for (const [fieldName, meta] of Object.entries(laneRow.report || {})) {
      const key = `lane${laneRow.lane}.${fieldName}`;
      fields[key] = meta;
      summary.total += 1;
      summary[meta.status] = (summary[meta.status] || 0) + 1;
    }
  }

  summary.required_broken_pipeline = Object.values(fields).filter((meta) => meta?.required === true && meta?.status === "broken_pipeline").length;
  summary.required_missing = Object.values(fields).filter((meta) => meta?.required === true && (meta?.status === "missing" || meta?.status === "not_published")).length;
  summary.optional_issues = Object.values(fields).filter((meta) => meta?.required !== true && meta?.status !== "ok").length;

  return {
    generated_at: new Date().toISOString(),
    source_priority: {
      primary: [
        "boatrace.jp race index",
        "boatrace.jp racelist",
        "boatrace.jp beforeinfo",
        "boatrace.jp result",
        "boatrace.jp venue/racer data"
      ],
      secondary: [
        "kyoteibiyori.com lane stats",
        "kyoteibiyori.com pre-race metrics",
        "kyoteibiyori.com tendency metrics"
      ]
    },
    summary,
    fields
  };
}

export function attachCoverageReportToRanking(ranking = [], coverageReport = {}) {
  const fields = coverageReport?.fields && typeof coverageReport.fields === "object" ? coverageReport.fields : {};
  return (Array.isArray(ranking) ? ranking : []).map((row) => {
    const lane = toInt(row?.racer?.lane, null);
    if (!Number.isInteger(lane)) return row;
    const laneCoverage = Object.fromEntries(
      Object.entries(fields)
        .filter(([key]) => key.startsWith(`lane${lane}.`))
        .map(([key, value]) => [key.replace(`lane${lane}.`, ""), value])
    );
    return {
      ...row,
      features: {
        ...(row?.features || {}),
        coverage_report: laneCoverage
      }
    };
  });
}
