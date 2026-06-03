import axios from "axios";
import * as cheerio from "cheerio";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const KYOTEI_BIYORI_BASE = "https://kyoteibiyori.com";
const ORITEN_ENDPOINT = `${KYOTEI_BIYORI_BASE}/request/request_oriten_kaiseki_custom.php`;
const KYOTEI_BIYORI_DEBUG_ROOT = path.resolve(fileURLToPath(new URL("../../debug/race-parser", import.meta.url)));
const EXPECTED_FIELDS = [
  "playerName",
  "fCount",
  "lapTime",
  "lapTimeRaw",
  "lapExhibitionScore",
  "stretchFootLabel",
  "straightTime",
  "turnTime",
  "exhibitionSt",
  "exhibitionTime",
  "motor2Rate",
  "motor3Rate",
  "laneFirstRate",
  "lane2RenRate",
  "lane3RenRate"
];

const PREDICTION_FIELD_META_CONFIG = {
  lapTime: { key: "lapTime", minConfidence: 0.6, required: true },
  exhibitionST: { key: "exhibitionST", minConfidence: 0.6, required: true },
  exhibitionTime: { key: "exhibitionTime", minConfidence: 0.6, required: true },
  straightTime: { key: "straightTime", minConfidence: 0.6, required: false },
  turnTime: { key: "turnTime", minConfidence: 0.6, required: false },
  lapExStretch: { key: "lapExStretch", minConfidence: 0.6, required: true },
  motor2ren: { key: "motor2ren", minConfidence: 0.6, required: true },
  motor3ren: { key: "motor3ren", minConfidence: 0.5, required: false },
  lane1stScore: { key: "lane1stScore", minConfidence: 0.6, required: true },
  lane2renScore: { key: "lane2renScore", minConfidence: 0.6, required: true },
  lane3renScore: { key: "lane3renScore", minConfidence: 0.6, required: true },
  lane1stAvg: { key: "lane1stAvg", minConfidence: 0.6, required: true },
  lane2renAvg: { key: "lane2renAvg", minConfidence: 0.6, required: true },
  lane3renAvg: { key: "lane3renAvg", minConfidence: 0.6, required: true },
  fCount: { key: "fCount", minConfidence: 0.5, required: false }
};

function clampConfidence(value) {
  return Number(Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0)).toFixed(3));
}

function baseSourceConfidence(source) {
  if (!source) return 0;
  if (String(source).includes("request_oriten_kaiseki_custom")) return 0.97;
  if (String(source).includes("race_shusso_html")) return 0.93;
  if (String(source).includes("boatrace_profile_lane_stats")) return 0.78;
  if (String(source).includes("boatrace_racelist")) return 0.74;
  if (String(source).includes("boatrace_official")) return 0.76;
  return 0.68;
}

function isPublishedRawValue(rawValue) {
  if (rawValue === null || rawValue === undefined) return false;
  const text = normalizeSpace(String(rawValue));
  if (!text) return false;
  return !/^(?:[-\u2010-\u2015\u2212ー－]+|n\/a|none|null|not\s*published|未公開|未発表)$/i.test(text);
}

function isPublishedLapTimeRawValue(rawValue) {
  if (rawValue === null || rawValue === undefined || rawValue === "") return false;
  const numeric = Number(rawValue);
  if (Number.isFinite(numeric)) {
    return numeric >= 0 && numeric < 60;
  }
  return isPublishedRawValue(rawValue);
}

function resolveLapTimeSource({ laneSources = {}, debugEntry = null, hasParsedValue = false }) {
  if (laneSources?.lapTimeRaw) return laneSources.lapTimeRaw;
  if (laneSources?.lapTime) return laneSources.lapTime;
  if (debugEntry?.sourceLabel) return debugEntry.sourceLabel;
  if (hasParsedValue) return "race_shusso_html";
  return null;
}

function toPositiveInteger(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function isLaneFallbackName(value) {
  return /^Lane-\d+$/i.test(normalizeSpace(value || ""));
}

function pickPreferredIdentityName(row = {}, lane = null) {
  const candidates = [
    row?.playerName,
    row?.name,
    row?.player_name,
    row?.racerName,
    row?.displayName,
    row?.fallbackPlayerName,
    row?.fallbackName
  ]
    .map((value) => normalizeSpace(value))
    .filter((value) => value && !isLaneFallbackName(value));
  return candidates[0] || (Number.isInteger(lane) ? `Lane-${lane}` : null);
}

function pickPreferredIdentityRegistrationNo(row = {}) {
  const candidates = [
    row?.registrationNo,
    row?.registrationNumber,
    row?.registration_no,
    row?.registration_number,
    row?.regNo,
    row?.playerRegNo,
    row?.fallbackRegistrationNo,
    row?.fallbackRegistrationNumber
  ];
  for (const candidate of candidates) {
    const registrationNo = toPositiveInteger(candidate);
    if (registrationNo !== null) return registrationNo;
  }
  return null;
}

function pickPreferredIdentityClass(row = {}) {
  const candidates = [row?.class, row?.grade, row?.playerClass, row?.fallbackClass, row?.fallbackGrade]
    .map((value) => normalizeSpace(value))
    .filter(Boolean);
  return candidates[0] || null;
}

function makePredictionFieldMeta({
  field,
  value,
  source,
  debugEntry,
  required = false,
  minConfidence = 0.6,
  publishedInSource = false
}) {
  const hasValue = value !== null && value !== undefined && Number.isFinite(Number(value));
  if (!hasValue) {
    const reason = publishedInSource ? "published_but_parse_failed" : "not_published";
    return {
      value: null,
      source: source || null,
      confidence: 0,
      is_usable: false,
      required,
      reason,
      published_in_source: publishedInSource,
      raw_cell_text: debugEntry?.raw ?? null,
      source_section: debugEntry?.section ?? null,
      source_row_label: debugEntry?.metric ?? debugEntry?.row ?? null,
      source_period_label: debugEntry?.period ?? null,
      source_boat_column: debugEntry?.boatColumn ?? debugEntry?.column ?? null,
      normalized_numeric_value: null
    };
  }
  let confidence = baseSourceConfidence(source);
  if (debugEntry && typeof debugEntry === "object") {
    if (Number.isFinite(Number(debugEntry?.value ?? debugEntry?.avg ?? debugEntry?.finalValue))) confidence += 0.05;
    if (debugEntry?.section || debugEntry?.metric || debugEntry?.sourceLabel) confidence += 0.03;
    if (Array.isArray(debugEntry?.availablePeriods)) {
      const count = debugEntry.availablePeriods.length;
      confidence += count >= 4 ? 0.08 : count >= 2 ? 0.02 : -0.08;
    }
  }
  if (field === "motor3ren") confidence -= 0.04;
  if (field === "fCount") confidence -= 0.06;
  const normalizedConfidence = clampConfidence(confidence);
  return {
    value: Number(value),
    source: source || null,
    confidence: normalizedConfidence,
    is_usable: !!source && normalizedConfidence >= minConfidence,
    required,
    published_in_source: publishedInSource || isPublishedRawValue(debugEntry?.raw),
    raw_cell_text: debugEntry?.raw ?? null,
    source_section: debugEntry?.section ?? null,
    source_row_label: debugEntry?.metric ?? debugEntry?.row ?? null,
    source_period_label: debugEntry?.period ?? null,
    source_boat_column: debugEntry?.boatColumn ?? debugEntry?.column ?? null,
    normalized_numeric_value: Number(value),
    reason: !!source
      ? normalizedConfidence >= minConfidence
        ? "verified"
        : "confidence_below_threshold"
      : "unknown_source"
  };
}

function buildPredictionFieldMetaForLane({ lane, extra, racer, fieldSources, fieldDebugs }) {
  const laneSources = fieldSources?.[lane] || {};
  const laneDebug = fieldDebugs?.[lane] || {};
  const racerFieldSources = racer?.fieldSources && typeof racer.fieldSources === "object" ? racer.fieldSources : {};
  const authoritativeExhibitionSt = firstFiniteValue(racer?.exhibitionSt, racer?.exST, racer?.exhibitionST);
  const authoritativeExhibitionTime = normalizeExhibitionTimeForMeta(
    firstFiniteValue(racer?.exhibitionTime, racer?.exTime)
  );
  const authoritativeExhibitionStSource =
    racerFieldSources.exhibitionSt ||
    racerFieldSources.exhibitionST ||
    racer?.exhibitionSTDetail?.source ||
    (Number.isFinite(Number(authoritativeExhibitionSt)) ? "boatrace_beforeinfo" : null);
  const authoritativeExhibitionTimeSource =
    racerFieldSources.exhibitionTime ||
    racer?.exhibitionTimeDetail?.source ||
    (normalizeExhibitionTimeForMeta(authoritativeExhibitionTime) !== null ? "boatrace_beforeinfo" : null);
  const laneRawVerified = {
    lane1st: isVerifiedLaneStatDebug(extra?.lane1stDebug || laneDebug?.lane1stRate, "1着率"),
    lane2ren: isVerifiedLaneStatDebug(extra?.lane2renDebug || laneDebug?.lane2renRate, "2連対率"),
    lane3ren: isVerifiedLaneStatDebug(extra?.lane3renDebug || laneDebug?.lane3renRate, "3連対率")
  };
  const getFieldMeta = (field, options) => makePredictionFieldMeta({
    field,
    value: options.value,
    source: options.source,
    debugEntry: options.debugEntry,
    publishedInSource: options.publishedInSource === true,
    required: PREDICTION_FIELD_META_CONFIG[field]?.required,
    minConfidence: PREDICTION_FIELD_META_CONFIG[field]?.minConfidence
  });
  const lapTimeDebug = laneDebug?.lapTime || null;
  const lapTimeValue = firstFiniteValue(extra?.lapTime);
  const lapTimePublished =
    isPublishedLapTimeRawValue(lapTimeDebug?.raw) ||
    isPublishedLapTimeRawValue(extra?.lapTimeRaw);
  const lapTimeSource = resolveLapTimeSource({
    laneSources,
    debugEntry: lapTimeDebug,
    hasParsedValue: Number.isFinite(Number(lapTimeValue))
  });

  return {
    lapTime: getFieldMeta("lapTime", {
      value: lapTimeValue,
      source: lapTimeSource,
      debugEntry: lapTimeDebug,
      publishedInSource: lapTimePublished
    }),
    exhibitionST: getFieldMeta("exhibitionST", {
      value: authoritativeExhibitionSt,
      source: authoritativeExhibitionStSource,
      debugEntry: racer?.exhibitionSTDetail || null
    }),
    exhibitionTime: getFieldMeta("exhibitionTime", {
      value: authoritativeExhibitionTime,
      source: authoritativeExhibitionTimeSource,
      debugEntry: racer?.exhibitionTimeDetail || null
    }),
    straightTime: getFieldMeta("straightTime", {
      value: normalizeExhibitionTimeForMeta(extra?.straightTime ?? extra?.nobiashi ?? racer?.straightTime ?? racer?.nobiashi ?? null),
      source:
        laneSources.straightTime ||
        laneSources.nobiashi ||
        laneSources.__nobiashi ||
        (normalizeExhibitionTimeForMeta(racer?.straightTime ?? racer?.nobiashi) !== null ? "kyoteibiyori_original_exhibition.straight" : null),
      debugEntry: laneDebug?.straightTime || null
    }),
    turnTime: getFieldMeta("turnTime", {
      value: normalizeExhibitionTimeForMeta(extra?.turnTime ?? extra?.mawariashi ?? racer?.turnTime ?? racer?.mawariashi ?? null),
      source:
        laneSources.turnTime ||
        laneSources.mawariashi ||
        laneSources.__mawariashi ||
        (normalizeExhibitionTimeForMeta(racer?.turnTime ?? racer?.mawariashi) !== null ? "kyoteibiyori_original_exhibition.turn" : null),
      debugEntry: laneDebug?.turnTime || null
    }),
    lapExStretch: getFieldMeta("lapExStretch", {
      value: normalizeExhibitionTimeForMeta(extra?.lapExStretch ?? extra?.lapExhibitionScore ?? racer?.lapExStretch ?? racer?.lapExhibitionScore ?? null),
      source:
        laneSources.lapExStretch ||
        laneSources.lapExhibitionScore ||
        (normalizeExhibitionTimeForMeta(racer?.lapExStretch ?? racer?.lapExhibitionScore) !== null ? "boatrace_racelist" : null),
      debugEntry: laneDebug?.lapExStretch || null
    }),
    motor2ren: getFieldMeta("motor2ren", {
      value: extra?.motor2ren ?? extra?.motor2Rate ?? racer?.motor2ren ?? racer?.motor2Rate ?? null,
      source: laneSources.motor2Rate || (Number.isFinite(Number(racer?.motor2Rate ?? racer?.motor2ren)) ? "boatrace_official" : null),
      debugEntry: laneDebug?.motor2ren || null
    }),
    motor3ren: getFieldMeta("motor3ren", {
      value: extra?.motor3ren ?? extra?.motor3Rate ?? racer?.motor3ren ?? racer?.motor3Rate ?? null,
      source: laneSources.motor3Rate || (Number.isFinite(Number(racer?.motor3Rate ?? racer?.motor3ren)) ? "boatrace_official" : null),
      debugEntry: laneDebug?.motor3ren || null
    }),
    lane1stScore: getFieldMeta("lane1stScore", {
      value: extra?.lane1stScore ?? extra?.lane1stAvg ?? extra?.laneFirstRate ?? racer?.lane1stScore ?? racer?.lane1stAvg ?? racer?.laneFirstRate ?? null,
      source: laneRawVerified.lane1st ? (laneSources.laneFirstRate || "boatrace_profile_lane_stats_exact_raw_verified") : null,
      debugEntry: laneDebug?.lane1stRate || extra?.lane1stDebug || null
    }),
    lane2renScore: getFieldMeta("lane2renScore", {
      value: extra?.lane2renScore ?? extra?.lane2renAvg ?? extra?.lane2RenRate ?? racer?.lane2renScore ?? racer?.lane2renAvg ?? racer?.lane2RenRate ?? null,
      source: laneRawVerified.lane2ren ? (laneSources.lane2RenRate || "boatrace_profile_lane_stats_exact_raw_verified") : null,
      debugEntry: laneDebug?.lane2renRate || extra?.lane2renDebug || null
    }),
    lane3renScore: getFieldMeta("lane3renScore", {
      value: extra?.lane3renScore ?? extra?.lane3renAvg ?? extra?.lane3RenRate ?? racer?.lane3renScore ?? racer?.lane3renAvg ?? racer?.lane3RenRate ?? null,
      source: laneRawVerified.lane3ren ? (laneSources.lane3RenRate || "boatrace_profile_lane_stats_exact_raw_verified") : null,
      debugEntry: laneDebug?.lane3renRate || extra?.lane3renDebug || null
    }),
    lane1stAvg: getFieldMeta("lane1stAvg", {
      value: extra?.lane1stScore ?? extra?.lane1stAvg ?? extra?.laneFirstRate ?? racer?.lane1stScore ?? racer?.lane1stAvg ?? racer?.laneFirstRate ?? null,
      source: laneRawVerified.lane1st ? (laneSources.laneFirstRate || "boatrace_profile_lane_stats_exact_raw_verified") : null,
      debugEntry: laneDebug?.lane1stRate || extra?.lane1stDebug || null
    }),
    lane2renAvg: getFieldMeta("lane2renAvg", {
      value: extra?.lane2renScore ?? extra?.lane2renAvg ?? extra?.lane2RenRate ?? racer?.lane2renScore ?? racer?.lane2renAvg ?? racer?.lane2RenRate ?? null,
      source: laneRawVerified.lane2ren ? (laneSources.lane2RenRate || "boatrace_profile_lane_stats_exact_raw_verified") : null,
      debugEntry: laneDebug?.lane2renRate || extra?.lane2renDebug || null
    }),
    lane3renAvg: getFieldMeta("lane3renAvg", {
      value: extra?.lane3renScore ?? extra?.lane3renAvg ?? extra?.lane3RenRate ?? racer?.lane3renScore ?? racer?.lane3renAvg ?? racer?.lane3RenRate ?? null,
      source: laneRawVerified.lane3ren ? (laneSources.lane3RenRate || "boatrace_profile_lane_stats_exact_raw_verified") : null,
      debugEntry: laneDebug?.lane3renRate || extra?.lane3renDebug || null
    }),
    fCount: getFieldMeta("fCount", {
      value: extra?.fCount ?? racer?.fHoldCount ?? null,
      source: laneSources.fCount || (Number.isFinite(Number(racer?.fHoldCount)) ? "boatrace_racelist" : null),
      debugEntry: laneDebug?.fCount || null
    })
  };
}

function normalizeSpace(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeDigits(value) {
  return normalizeSpace(value).replace(/[\uFF10-\uFF19]/g, (ch) =>
    String.fromCharCode(ch.charCodeAt(0) - 0xfee0)
  );
}

function normalizeText(value) {
  return normalizeDigits(value)
    .replace(/[：]/g, ":")
    .replace(/[／]/g, "/")
    .replace(/[％]/g, "%")
    .trim();
}

function toNumber(value) {
  const cleaned = normalizeDigits(value).replace(/,/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function toFiniteNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function firstFiniteValue(...values) {
  for (const value of values) {
    const normalized = toFiniteNumberOrNull(value);
    if (normalized !== null) return normalized;
  }
  return null;
}

function parseDecimal(value) {
  const text = normalizeDigits(value).replace(/\s+/g, "");
  if (!text) return null;
  const match = text.match(/-?(?:\d+\.\d+|\d+|\.\d+)/);
  if (!match) return null;
  const normalized = match[0].startsWith(".") ? `0${match[0]}` : match[0];
  const n = Number(normalized);
  return Number.isFinite(n) ? n : null;
}

function parsePercent(value) {
  const text = normalizeDigits(value).replace(/%/g, "").trim();
  if (!text) return null;
  const match = text.match(/-?(?:\d+\.\d+|\d+)/);
  if (!match) return null;
  const n = Number(match[0]);
  return Number.isFinite(n) ? n : null;
}

function parseFCount(value) {
  const text = normalizeDigits(value);
  if (!text) return null;
  const match = text.match(/F\s*([0-9]+)/i) || text.match(/([0-9]+)/);
  if (!match) return null;
  const n = Number(match[1] || match[0]);
  return Number.isFinite(n) ? n : null;
}

function parseStartTimingRaw(value) {
  const raw = normalizeSpace(value) || null;
  if (!raw) return { raw: null, type: "missing", numeric: null, flag: null, signedValue: null };
  const normalized = normalizeDigits(raw).replace(/\s+/g, "").toUpperCase();
  const flagMatch = normalized.match(/^([FL])\.?(\d+(?:\.\d+)?|\.\d+)$/);
  if (flagMatch) {
    const rawNumeric = String(flagMatch[2] || "");
    const numeric = rawNumeric.startsWith(".")
      ? parseDecimal(rawNumeric)
      : parseDecimal(`.${rawNumeric.replace(/^\./, "")}`);
    const flag = flagMatch[1];
    const signedValue = Number.isFinite(numeric)
      ? Number((flag === "F" ? -numeric : numeric).toFixed(3))
      : null;
    return {
      raw,
      type: flag === "F" ? "flying" : "late",
      numeric,
      flag,
      signedValue
    };
  }
  const numeric = parseDecimal(normalized);
  return {
    raw,
    type: numeric === null ? "unknown" : "normal",
    numeric,
    flag: null,
    signedValue: Number.isFinite(numeric) ? numeric : null
  };
}

function parseScaledDecimal(value, divisor = 100) {
  const n = toNumber(value);
  if (n === null) return null;
  return Number((n / divisor).toFixed(2));
}

function normalizeLapTimeForModel(rawLapTime) {
  const raw = Number(rawLapTime);
  if (!Number.isFinite(raw)) return null;
  if (raw <= 30 || raw >= 50) return null;
  const normalized = Number((raw - 29.5).toFixed(2));
  return normalized > 0 && normalized < 20 ? normalized : null;
}

const AJAX_AGGREGATE_TYPE_FIELD_MAP = {
  tenji_ave_data: {
    0: "exhibitionTime"
  },
  shukai_ave_data: {
    0: "lapTimeRaw",
    10: "lapTime"
  }
};

function appendAjaxAggregateMetric(current, arrayKey, typeKey, metric) {
  const next = current && typeof current === "object" ? { ...current } : {};
  const bucket = next[arrayKey] && typeof next[arrayKey] === "object" ? { ...next[arrayKey] } : {};
  bucket[typeKey] = metric;
  next[arrayKey] = bucket;
  return next;
}

function mergeAjaxAggregateRows({ payload, byLane, fieldSources }) {
  const diagnostics = {
    parsed_ajax_rows_count: 0,
    mapped_field_count: 0,
    unknown_type_list: [],
    aggregate_sources: {}
  };

  for (const [arrayKey, typeFieldMap] of Object.entries(AJAX_AGGREGATE_TYPE_FIELD_MAP)) {
    const rows = Array.isArray(payload?.[arrayKey]) ? payload[arrayKey] : [];
    diagnostics.aggregate_sources[arrayKey] = {
      rows: rows.length,
      mapped_rows: 0
    };
    for (const row of rows) {
      const typeKey = Number(row?.type);
      const mappedField = typeFieldMap?.[typeKey] || null;
      diagnostics.parsed_ajax_rows_count += 1;
      if (!mappedField) {
        diagnostics.unknown_type_list.push(`${arrayKey}:unknown_type_${Number.isFinite(typeKey) ? typeKey : "na"}`);
      } else {
        diagnostics.aggregate_sources[arrayKey].mapped_rows += 1;
      }

      for (let lane = 1; lane <= 6; lane += 1) {
        const courseKey = `course${lane}`;
        const rawValue = row?.[courseKey];
        const numericValue = arrayKey === "tenji_ave_data"
          ? parseScaledDecimal(rawValue, 100)
          : parseScaledDecimal(rawValue, 100);
        if (numericValue === null) continue;

        const current = byLane.get(lane) || {};
        const laneFieldSources = fieldSources[lane] || {};
        const typeLabel = Number.isFinite(typeKey) ? `type_${typeKey}` : "type_unknown";
        const metric = {
          place_no: row?.place_no ?? null,
          mode: row?.mode ?? null,
          type: Number.isFinite(typeKey) ? typeKey : null,
          value: numericValue,
          raw: rawValue ?? null
        };

        current.ajaxAggregateMeta = appendAjaxAggregateMetric(
          current.ajaxAggregateMeta,
          arrayKey,
          typeLabel,
          metric
        );

        if (mappedField) {
          if (current[mappedField] === null || current[mappedField] === undefined || current[mappedField] === "") {
            current[mappedField] = numericValue;
            laneFieldSources[mappedField] = `${arrayKey}.${typeLabel}`;
            diagnostics.mapped_field_count += 1;
          }
        } else {
          current[`unknown_type_${arrayKey}_${Number.isFinite(typeKey) ? typeKey : "na"}`] = numericValue;
        }

        byLane.set(lane, current);
        fieldSources[lane] = laneFieldSources;
      }
    }
  }

  diagnostics.unknown_type_list = [...new Set(diagnostics.unknown_type_list)];
  return diagnostics;
}

function normalizeExhibitionTimeForMeta(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  return numeric > 4 && numeric < 9 ? numeric : null;
}

function makeStretchLabel({ mawariashi, chokusen }) {
  const parts = [];
  const stretch = toFiniteNumberOrNull(chokusen);
  const lap = toFiniteNumberOrNull(mawariashi);
  if (stretch !== null) parts.push(`?? ${stretch.toFixed(2)}`);
  if (lap !== null) parts.push(`?? ${lap.toFixed(2)}`);
  return parts.length > 0 ? parts.join(" / ") : null;
}

function computeLapExhibitionScore({ mawariashi, chokusen }) {
  const scores = [mawariashi, chokusen]
    .map((value) => toFiniteNumberOrNull(value))
    .filter((value) => value !== null);
  if (scores.length === 0) return null;
  return Number((scores.reduce((sum, value) => sum + value, 0) / scores.length).toFixed(2));
}

function buildFieldSourceDetail({ source = null, rowLabel = null, raw = null, normalized = null, status = null, extra = {} } = {}) {
  return {
    source,
    rowLabel,
    raw,
    normalized,
    status,
    ...extra
  };
}

function buildFieldDiagnostics(byLane, fieldSources = {}) {
  const populated = new Set();
  const missing = new Set(EXPECTED_FIELDS);
  const perLane = [];

  for (const [lane, row] of byLane.entries()) {
    const populatedFields = EXPECTED_FIELDS.filter((field) => {
      const value = row?.[field];
      return value !== null && value !== undefined && value !== "";
    });
    populatedFields.forEach((field) => {
      populated.add(field);
      missing.delete(field);
    });
    perLane.push({
      lane,
      populated_fields: populatedFields,
      missing_fields: EXPECTED_FIELDS.filter((field) => !populatedFields.includes(field)),
      field_sources: fieldSources?.[lane] || {}
    });
  }

  return {
    populated_fields: [...populated],
    failed_fields: [...missing],
    per_lane: perLane
  };
}

function buildRequiredFieldParseStatus(byLane) {
  const lanes = [...(byLane instanceof Map ? byLane.entries() : [])];
  const hasValue = (field) =>
    lanes.some(([, row]) => row?.[field] !== null && row?.[field] !== undefined && row?.[field] !== "");
  return {
    lane1stRate: hasValue("laneFirstRate"),
    lane2renRate: hasValue("lane2RenRate"),
    lane3renRate: hasValue("lane3RenRate"),
    lapTime: hasValue("lapTime") || hasValue("lapTimeRaw"),
    exhibitionST: hasValue("exhibitionSt"),
    exhibitionTime: hasValue("exhibitionTime"),
    straightTime: hasValue("straightTime") || hasValue("nobiashi") || hasValue("__nobiashi"),
    turnTime: hasValue("turnTime") || hasValue("mawariashi") || hasValue("__mawariashi"),
    lapExStretch: hasValue("lapExStretch") || hasValue("lapExhibitionScore"),
    motor2ren: hasValue("motor2ren") || hasValue("motor2Rate"),
    motor3ren: hasValue("motor3ren") || hasValue("motor3Rate")
  };
}

function buildIndexUrl({ date, venueId, raceNo }) {
  const hiduke = String(date || "").replace(/-/g, "");
  const placeNo = String(venueId || "").padStart(2, "0");
  return `${KYOTEI_BIYORI_BASE}/race_ichiran.php?place_no=${placeNo}&race_no=${Number(raceNo)}&hiduke=${hiduke}`;
}

function buildFallbackSliderUrl({ date, venueId, raceNo, slider }) {
  const hiduke = String(date || "").replace(/-/g, "");
  const placeNo = String(venueId || "").padStart(2, "0");
  return `${KYOTEI_BIYORI_BASE}/race_shusso.php?place_no=${placeNo}&race_no=${Number(raceNo)}&hiduke=${hiduke}&slider=${slider}`;
}

const HTML_DEBUG_KEYWORDS = [
  "ST",
  "\u5c55\u793a",
  "\u5468\u56de",
  "\u4e00\u5468",
  "\u5468\u56de\u30bf\u30a4\u30e0",
  "\u4e00\u5468\u30bf\u30a4\u30e0",
  "\u5468\u8db3",
  "\u307e\u308f\u308a\u8db3",
  "\u56de\u308a\u8db3",
  "\u56de\u8db3",
  "\u76f4\u7dda",
  "\u76f4\u7dda\u30bf\u30a4\u30e0",
  "\u30e2\u30fc\u30bf\u30fc2\u9023\u7387"
];

function buildKyoteiBiyoriDebugFileBase({ date, venueId, raceNo }) {
  const hiduke = String(date || "").replace(/-/g, "");
  const placeNo = String(venueId || "").padStart(2, "0");
  return `kyoteibiyori-${hiduke}-${placeNo}-${Number(raceNo)}`;
}

function addNamedHtmlArtifact(artifactCollector, fileName, html) {
  if (!artifactCollector || typeof artifactCollector !== "object") return;
  if (!html) return;
  artifactCollector.named_raw_files = {
    ...(artifactCollector.named_raw_files || {}),
    [fileName]: String(html)
  };
}

function addNamedBinaryArtifact(artifactCollector, fileName, body) {
  if (!artifactCollector || typeof artifactCollector !== "object") return;
  if (!body) return;
  artifactCollector.named_binary_files = {
    ...(artifactCollector.named_binary_files || {}),
    [fileName]: body
  };
}

function sanitizeDebugFilePart(value) {
  const text = String(value || "")
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
  return text || "response";
}

function saveRaceParserDebugFile(relativePath, body, { binary = false } = {}) {
  if (!body) return null;
  const safeRelativePath = String(relativePath || "")
    .replace(/^[\\/]+/g, "")
    .replace(/\.\.(?:[\\/]|$)/g, "");
  if (!safeRelativePath) return null;
  const targetPath = path.resolve(KYOTEI_BIYORI_DEBUG_ROOT, safeRelativePath);
  if (!targetPath.startsWith(KYOTEI_BIYORI_DEBUG_ROOT)) return null;
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });
  fs.writeFileSync(targetPath, binary ? (Buffer.isBuffer(body) ? body : Buffer.from(body)) : String(body), binary ? undefined : "utf8");
  return targetPath;
}

function inspectHtmlKeywordPresence(html) {
  const text = String(html || "");
  return Object.fromEntries(HTML_DEBUG_KEYWORDS.map((keyword) => [keyword, text.includes(keyword)]));
}

function logHtmlKeywordPresence(stage, html) {
  const presence = inspectHtmlKeywordPresence(html);
  const summary = HTML_DEBUG_KEYWORDS.map((keyword) => `${keyword}=${presence[keyword] ? "true" : "false"}`).join(" ");
  console.info(`[kyoteibiyori] ${stage} contains ${summary}`);
  return presence;
}

function logRenderedContainsForDebug(presence = {}) {
  const labels = ["周回", "一周", "直線", "まわり足", "回り足", "回足", "周足"];
  const lines = labels.map((label) => `${label}=${presence?.[label] === true ? "true" : "false"}`);
  console.log(`[kyoteibiyori] rendered contains:\n${lines.join("\n")}`);
}

function buildOriginalExhibitionLaneRows(byLane) {
  return [...(byLane instanceof Map ? byLane.entries() : [])]
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([lane, row]) => ({
      boat: Number(lane),
      lapTime: firstFiniteValue(row?.lapTime, row?.lapTimeRaw),
      straightTime: firstFiniteValue(row?.straightTime, row?.nobiashi, row?.__nobiashi),
      turnTime: firstFiniteValue(row?.turnTime, row?.mawariashi, row?.__mawariashi)
    }));
}

function buildMergedEntryDebugRows(byLane) {
  return [...(byLane instanceof Map ? byLane.entries() : [])]
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([lane, row]) => ({
      boat: Number(lane),
      exST: firstFiniteValue(row?.exST, row?.exhibitionSt, row?.exhibitionST),
      exTime: firstFiniteValue(row?.exTime, row?.exhibitionTime),
      motor2Rate: firstFiniteValue(row?.motor2Rate, row?.motor2ren),
      lapTime: firstFiniteValue(row?.lapTime, row?.lapTimeRaw),
      straightTime: firstFiniteValue(row?.straightTime, row?.nobiashi, row?.__nobiashi),
      turnTime: firstFiniteValue(row?.turnTime, row?.mawariashi, row?.__mawariashi)
    }));
}

function countDebugRowsWithField(rows, field) {
  return (Array.isArray(rows) ? rows : []).filter((row) => Number.isFinite(Number(row?.[field]))).length;
}

function pickLaneStatsDebugRows(parseResults = {}) {
  const laneStatsRows = Array.isArray(parseResults?.lane_stats_tab?.original_exhibition_rows)
    ? parseResults.lane_stats_tab.original_exhibition_rows
    : [];
  const renderedRows = Array.isArray(parseResults?.rendered_dom?.original_exhibition_rows)
    ? parseResults.rendered_dom.original_exhibition_rows
    : [];
  const laneStatsValueCount =
    countDebugRowsWithField(laneStatsRows, "lapTime") +
    countDebugRowsWithField(laneStatsRows, "straightTime") +
    countDebugRowsWithField(laneStatsRows, "turnTime");
  const renderedValueCount =
    countDebugRowsWithField(renderedRows, "lapTime") +
    countDebugRowsWithField(renderedRows, "straightTime") +
    countDebugRowsWithField(renderedRows, "turnTime");
  return renderedValueCount > laneStatsValueCount ? renderedRows : laneStatsRows;
}

function logOriginalExhibitionPipelineDebug(diagnostics = {}) {
  const renderedPresence = diagnostics?.html_contains?.rendered || {};
  const laneStatsRows = pickLaneStatsDebugRows(diagnostics?.parse_results || {});
  const mergedRows = Array.isArray(diagnostics?.merge_results?.entries) ? diagnostics.merge_results.entries : [];
  const renderedLines = ["周回", "一周", "直線", "まわり足", "回り足", "周足"]
    .map((label) => `rendered contains ${label}: ${renderedPresence?.[label] === true ? "true" : "false"}`);
  console.log(`[kyoteibiyori] original exhibition debug:\n${renderedLines.join("\n")}`);
  console.log(
    "[kyoteibiyori] parsed counts:",
    {
      laneStats: laneStatsRows.length,
      lapTime: countDebugRowsWithField(laneStatsRows, "lapTime"),
      straightTime: countDebugRowsWithField(laneStatsRows, "straightTime"),
      turnTime: countDebugRowsWithField(laneStatsRows, "turnTime")
    }
  );
  console.log(
    "[kyoteibiyori] merged counts:",
    {
      lapTime: countDebugRowsWithField(mergedRows, "lapTime"),
      straightTime: countDebugRowsWithField(mergedRows, "straightTime"),
      turnTime: countDebugRowsWithField(mergedRows, "turnTime")
    }
  );
  console.log("[kyoteibiyori] laneStats preview:", laneStatsRows.slice(0, 6));
  console.log("[kyoteibiyori] merged preview:", mergedRows.slice(0, 6));
}

function hasStaticLapStraightTurnLabels(html) {
  const presence = inspectHtmlKeywordPresence(html);
  const hasLap = !!(presence["\u5468\u56de"] || presence["\u4e00\u5468"] || presence["\u5468\u56de\u30bf\u30a4\u30e0"] || presence["\u4e00\u5468\u30bf\u30a4\u30e0"]);
  const hasStraight = !!(presence["\u76f4\u7dda"] || presence["\u76f4\u7dda\u30bf\u30a4\u30e0"]);
  const hasTurn = !!(presence["\u307e\u308f\u308a\u8db3"] || presence["\u56de\u308a\u8db3"] || presence["\u56de\u8db3"] || presence["\u5468\u8db3"]);
  return hasLap && hasStraight && hasTurn;
}

function countOriginalExhibitionFields(byLane) {
  const rows = [...(byLane instanceof Map ? byLane.values() : [])];
  const count = (predicate) => rows.filter((row) => predicate(row || {})).length;
  return {
    exST: count((row) => Number.isFinite(Number(row?.exhibitionSt))),
    exTime: count((row) => Number.isFinite(Number(row?.exhibitionTime))),
    lapTime: count((row) => Number.isFinite(Number(row?.lapTime ?? row?.lapTimeRaw))),
    straightTime: count((row) => Number.isFinite(Number(row?.straightTime ?? row?.nobiashi ?? row?.__nobiashi))),
    turnTime: count((row) => Number.isFinite(Number(row?.turnTime ?? row?.mawariashi ?? row?.__mawariashi))),
    motor2Rate: count((row) => Number.isFinite(Number(row?.motor2Rate ?? row?.motor2ren)))
  };
}

function originalExhibitionCoverageScore(counts = {}) {
  return (
    Number(counts.exST || 0) +
    Number(counts.exTime || 0) +
    Number(counts.lapTime || 0) +
    Number(counts.straightTime || 0) +
    Number(counts.turnTime || 0) +
    Number(counts.motor2Rate || 0)
  );
}

function shouldAttemptRenderedFallback({ html = "", byLane = new Map() } = {}) {
  const counts = countOriginalExhibitionFields(byLane);
  return (
    !hasStaticLapStraightTurnLabels(html) ||
    Number(counts.lapTime || 0) < 6 ||
    Number(counts.straightTime || 0) < 6 ||
    Number(counts.turnTime || 0) < 6
  );
}

const PLAYWRIGHT_ORIGINAL_EXHIBITION_KEYWORDS = [
  "\u30aa\u30ea\u5c55",
  "\u30aa\u30ea\u30b8\u30ca\u30eb\u5c55\u793a",
  "\u5c55\u793a",
  "\u76f4\u524d",
  "\u76f4\u7dda",
  "\u5468\u56de",
  "\u4e00\u5468",
  "\u307e\u308f\u308a\u8db3",
  "\u56de\u308a\u8db3",
  "\u56de\u8db3",
  "\u5468\u8db3"
];

const NETWORK_ORIGINAL_EXHIBITION_KEYWORDS = [
  "\u5468\u56de",
  "\u4e00\u5468",
  "\u76f4\u7dda",
  "\u307e\u308f\u308a\u8db3",
  "\u56de\u308a\u8db3",
  "\u56de\u8db3",
  "\u5468\u8db3",
  "lap",
  "straight",
  "turn",
  "shukai",
  "chokusen",
  "mawari"
];

function inspectKeywordPresence(text, keywords = []) {
  const source = String(text || "").toLowerCase();
  return Object.fromEntries(keywords.map((keyword) => {
    const key = String(keyword);
    return [key, source.includes(key.toLowerCase())];
  }));
}

function hasAnyKeywordPresence(presence = {}) {
  return Object.values(presence || {}).some((value) => value === true);
}

async function collectPlaywrightClickableDebug(page) {
  return page.evaluate((keywords) => {
    const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim();
    const isVisible = (el) => {
      const style = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return style && style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
    };
    const selectors = [
      "button",
      "a",
      "[role='button']",
      "[role='tab']",
      "input[type='button']",
      "input[type='submit']",
      "[onclick]",
      "[class*='btn']",
      "[class*='tab']"
    ].join(",");
    const elements = Array.from(document.querySelectorAll(selectors));
    const rows = elements.map((el, index) => {
      const tag = String(el.tagName || "").toLowerCase();
      const text = normalize(el.innerText || el.textContent || el.value || el.getAttribute("aria-label") || el.getAttribute("title") || "");
      const className = normalize(el.getAttribute("class") || "");
      const id = normalize(el.getAttribute("id") || "");
      const role = normalize(el.getAttribute("role") || "");
      const href = normalize(el.getAttribute("href") || "");
      return {
        index,
        tag,
        role,
        id,
        className: className.slice(0, 120),
        text: text.slice(0, 200),
        href: href.slice(0, 200),
        visible: isVisible(el)
      };
    }).filter((row) => row.text || row.id || row.href || row.role);
    const keywordElements = {};
    const keywordPresence = {};
    for (const keyword of keywords) {
      const matched = rows.filter((row) =>
        row.text.includes(keyword) ||
        row.id.includes(keyword) ||
        row.className.includes(keyword) ||
        row.href.includes(keyword)
      );
      keywordPresence[keyword] = matched.length > 0;
      keywordElements[keyword] = matched.slice(0, 10);
    }
    return {
      all: rows.slice(0, 150),
      keyword_presence: keywordPresence,
      keyword_elements: keywordElements
    };
  }, PLAYWRIGHT_ORIGINAL_EXHIBITION_KEYWORDS).catch((error) => ({
    all: [],
    keyword_presence: {},
    keyword_elements: {},
    error: String(error?.message || error)
  }));
}

async function pageHasOriginalExhibitionText(page) {
  return page.evaluate((keywords) => {
    const text = document.body ? document.body.innerText || document.body.textContent || "" : "";
    return keywords.some((keyword) => text.includes(keyword));
  }, ["\u5468\u56de", "\u4e00\u5468", "\u76f4\u7dda", "\u307e\u308f\u308a\u8db3", "\u56de\u308a\u8db3", "\u56de\u8db3", "\u5468\u8db3"]).catch(() => false);
}

async function waitAfterPlaywrightClick(page, timeoutMs) {
  const cappedTimeout = Math.max(1000, Number(timeoutMs) || 3000);
  await page.waitForLoadState("networkidle", { timeout: Math.min(5000, cappedTimeout) }).catch(() => null);
  await page.waitForTimeout(Math.min(3000, Math.max(1000, Math.floor(cappedTimeout / 3)))).catch(() => null);
  await page.waitForFunction(
    () => {
      const text = document.body ? document.body.innerText || document.body.textContent || "" : "";
      return /周回|一周|直線|まわり足|回り足|回足|周足/.test(text);
    },
    { timeout: Math.min(3000, cappedTimeout) }
  ).catch(() => null);
}

async function tryPlaywrightClick({ page, debug, label, locator, timeoutMs }) {
  const attempt = {
    label,
    count: 0,
    clicked: false,
    url_before: page.url(),
    url_after: null,
    error: null
  };
  try {
    const count = await locator.count().catch(() => 0);
    attempt.count = count;
    if (count > 0) {
      const target = locator.first();
      await target.scrollIntoViewIfNeeded({ timeout: Math.min(1500, timeoutMs) }).catch(() => null);
      await target.click({ timeout: Math.min(2500, timeoutMs) }).catch(async (error) => {
        attempt.error = String(error?.message || error);
        await target.click({ timeout: Math.min(2500, timeoutMs), force: true });
      });
      attempt.clicked = true;
      await waitAfterPlaywrightClick(page, timeoutMs);
    }
  } catch (error) {
    attempt.error = String(error?.message || error);
  }
  attempt.url_after = page.url();
  debug.click_attempts.push(attempt);
  return attempt.clicked && await pageHasOriginalExhibitionText(page);
}

async function tryPlaywrightEvaluateClick({ page, debug, label, keywords, timeoutMs }) {
  const attempt = {
    label,
    count: 0,
    clicked: false,
    url_before: page.url(),
    url_after: null,
    error: null
  };
  try {
    const result = await page.evaluate((targetKeywords) => {
      const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim();
      const isVisible = (el) => {
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style && style.display !== "none" && style.visibility !== "hidden" && rect.width > 0 && rect.height > 0;
      };
      const selectors = [
        "button",
        "a",
        "[role='button']",
        "[role='tab']",
        "input[type='button']",
        "input[type='submit']",
        "[onclick]",
        "[class*='btn']",
        "[class*='tab']",
        "div",
        "span"
      ].join(",");
      const candidates = Array.from(document.querySelectorAll(selectors)).filter((el) => {
        if (!isVisible(el)) return false;
        const text = normalize(el.innerText || el.textContent || el.value || el.getAttribute("aria-label") || el.getAttribute("title") || "");
        if (!targetKeywords.some((keyword) => text.includes(keyword))) return false;
        const hasClickSurface =
          typeof el.onclick === "function" ||
          el.getAttribute("onclick") ||
          ["button", "a"].includes(String(el.tagName || "").toLowerCase()) ||
          el.getAttribute("role") === "button" ||
          el.getAttribute("role") === "tab" ||
          /btn|button|tab|nav|menu/i.test(String(el.getAttribute("class") || ""));
        return !!hasClickSurface;
      });
      const target = candidates[0] || null;
      if (target) target.click();
      return {
        count: candidates.length,
        clicked: !!target,
        target_text: target ? normalize(target.innerText || target.textContent || target.value || "") : null,
        target_tag: target ? String(target.tagName || "").toLowerCase() : null,
        target_id: target ? String(target.getAttribute("id") || "") : null,
        target_class: target ? String(target.getAttribute("class") || "").slice(0, 120) : null
      };
    }, keywords);
    attempt.count = result?.count || 0;
    attempt.clicked = !!result?.clicked;
    attempt.target = result || null;
    if (attempt.clicked) await waitAfterPlaywrightClick(page, timeoutMs);
  } catch (error) {
    attempt.error = String(error?.message || error);
  }
  attempt.url_after = page.url();
  debug.click_attempts.push(attempt);
  return attempt.clicked && await pageHasOriginalExhibitionText(page);
}

async function fetchRenderedPageWithPlaywright(url, timeoutMs = 45000, options = {}) {
  let browser = null;
  const debugFileBase = options?.debugFileBase || "kyoteibiyori";
  const debug = {
    requested_url: url,
    page_url_before_click: null,
    page_url_after_click: null,
    page_title_before_click: null,
    page_title_after_click: null,
    rendered_html_length: 0,
    clickable_before: { all: [], keyword_presence: {}, keyword_elements: {} },
    clickable_after: { all: [], keyword_presence: {}, keyword_elements: {} },
    click_attempts: [],
    network_responses: [],
    network_summary_path: null,
    screenshot_path: null,
    latest_screenshot_path: null
  };
  const networkResponseTasks = [];
  const networkBodies = [];
  let networkCaptureIndex = 0;
  let interactionPhase = "before_click";

  try {
    const playwright = await import("playwright");
    const chromium = playwright?.chromium;
    if (!chromium) throw new Error("playwright_chromium_unavailable");
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage({
      locale: "ja-JP",
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    });
    const cappedTimeout = Math.max(3000, Math.min(Number(timeoutMs) || 45000, 45000));

    await page.route("**/*", (route) => {
      const resourceType = route.request().resourceType();
      if (["image", "font", "media"].includes(resourceType)) {
        return route.abort().catch(() => null);
      }
      return route.continue().catch(() => null);
    });

    page.on("response", (response) => {
      const task = (async () => {
        const request = response.request();
        const resourceType = request.resourceType();
        const responseUrl = response.url();
        const contentType = response.headers()?.["content-type"] || "";
        const lowerUrl = responseUrl.toLowerCase();
        const shouldCapture =
          ["fetch", "xhr", "document"].includes(resourceType) ||
          /json|html|text|javascript|xml/i.test(contentType) ||
          /api|ajax|request|oriten|kaiseki|tenji|shukai|chokusen|mawari/i.test(lowerUrl);
        if (!shouldCapture) return;
        const entry = {
          index: networkCaptureIndex++,
          phase: interactionPhase,
          url: responseUrl,
          method: request.method(),
          status: response.status(),
          resource_type: resourceType,
          content_type: contentType,
          body_length: null,
          contains: {},
          saved_path: null,
          error: null
        };
        try {
          const body = await response.text();
          entry.body_length = body.length;
          entry.contains = inspectKeywordPresence(body, NETWORK_ORIGINAL_EXHIBITION_KEYWORDS);
          const parsedUrl = new URL(responseUrl);
          const ext = /json/i.test(contentType) ? "json" : /html/i.test(contentType) ? "html" : "txt";
          const filePart = sanitizeDebugFilePart(`${parsedUrl.hostname}_${parsedUrl.pathname.split("/").filter(Boolean).pop() || "response"}`);
          entry.saved_path = saveRaceParserDebugFile(
            `network/${debugFileBase}-${String(entry.index).padStart(2, "0")}-${resourceType}-${filePart}.${ext}`,
            body
          );
          if (body.length <= 2_000_000) {
            networkBodies.push({ ...entry, body });
          }
        } catch (error) {
          entry.error = String(error?.message || error);
        }
        debug.network_responses.push(entry);
      })();
      networkResponseTasks.push(task);
    });

    await page.goto(url, { waitUntil: "domcontentloaded", timeout: cappedTimeout });
    await page.waitForLoadState("networkidle", { timeout: Math.min(5000, cappedTimeout) }).catch(() => null);
    await page.waitForSelector("table, .raceNaiyou, .chokuzen, .tenji, [class*='race'], [class*='tenji'], body", {
      timeout: Math.min(3000, cappedTimeout)
    }).catch(() => null);

    debug.page_url_before_click = page.url();
    debug.page_title_before_click = await page.title().catch(() => null);
    debug.clickable_before = await collectPlaywrightClickableDebug(page);

    interactionPhase = "after_click";
    const clickKeywords = [
      "\u30aa\u30ea\u5c55",
      "\u30aa\u30ea\u30b8\u30ca\u30eb\u5c55\u793a",
      "\u5c55\u793a",
      "\u76f4\u524d"
    ];
    const clickPlan = [
      ["selector:#btnOritenKaiseki", () => page.locator("#btnOritenKaiseki")],
      ["text:oriten", () => page.getByText("\u30aa\u30ea\u5c55", { exact: false })],
      ["text:original-tenji", () => page.getByText("\u30aa\u30ea\u30b8\u30ca\u30eb\u5c55\u793a", { exact: false })],
      ["role=button:oriten", () => page.getByRole("button", { name: /\u30aa\u30ea\u5c55/ })],
      ["role=tab:oriten", () => page.getByRole("tab", { name: /\u30aa\u30ea\u5c55/ })],
      ["a:oriten", () => page.locator("a").filter({ hasText: "\u30aa\u30ea\u5c55" })],
      ["button-or-tab:tenji", () => page.locator("button,[role='button'],[role='tab']").filter({ hasText: "\u5c55\u793a" })],
      ["a:tenji", () => page.locator("a").filter({ hasText: "\u5c55\u793a" })],
      ["text:chokuzen", () => page.getByText("\u76f4\u524d", { exact: false })]
    ];
    for (const [label, locatorFactory] of clickPlan) {
      const found = await tryPlaywrightClick({
        page,
        debug,
        label,
        locator: locatorFactory(),
        timeoutMs: cappedTimeout
      });
      if (found) break;
    }
    if (!(await pageHasOriginalExhibitionText(page))) {
      await tryPlaywrightEvaluateClick({
        page,
        debug,
        label: "evaluate:button-a-tab-div-keyword-click",
        keywords: clickKeywords,
        timeoutMs: cappedTimeout
      });
    }

    debug.page_url_after_click = page.url();
    debug.page_title_after_click = await page.title().catch(() => null);
    debug.clickable_after = await collectPlaywrightClickableDebug(page);
    await Promise.allSettled(networkResponseTasks);

    const html = await page.content();
    debug.rendered_html_length = html.length;
    const screenshot = await page.screenshot({ fullPage: true, type: "png" }).catch(() => null);
    if (screenshot) {
      debug.screenshot_path = saveRaceParserDebugFile(`${debugFileBase}.rendered.png`, screenshot, { binary: true });
      debug.latest_screenshot_path = saveRaceParserDebugFile("latest-rendered.png", screenshot, { binary: true });
    }
    debug.network_summary_path = saveRaceParserDebugFile(
      `network/${debugFileBase}-network-summary.json`,
      JSON.stringify(debug.network_responses, null, 2)
    );
    console.log("[kyoteibiyori] playwright rendered page:", {
      url: debug.page_url_after_click,
      title: debug.page_title_after_click,
      renderedHtmlLength: debug.rendered_html_length,
      screenshotPath: debug.screenshot_path,
      latestScreenshotPath: debug.latest_screenshot_path
    });
    console.log("[kyoteibiyori] playwright clickable before:", debug.clickable_before);
    console.log("[kyoteibiyori] playwright clickable after:", debug.clickable_after);
    console.log("[kyoteibiyori] playwright click attempts:", debug.click_attempts);
    console.log("[kyoteibiyori] playwright network responses:", debug.network_responses);
    return { html, screenshot, debug, networkBodies };
  } finally {
    if (browser) await browser.close().catch(() => null);
  }
}

async function fetchRenderedPageWithPlaywrightLegacy(url, timeoutMs = 45000, options = {}) {
  let browser = null;
  try {
    const playwright = await import("playwright");
    const chromium = playwright?.chromium;
    if (!chromium) throw new Error("playwright_chromium_unavailable");
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage({
      locale: "ja-JP",
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    });
    const cappedTimeout = Math.max(3000, Math.min(Number(timeoutMs) || 45000, 45000));
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: cappedTimeout });
    await page.waitForLoadState("networkidle", { timeout: Math.min(5000, cappedTimeout) }).catch(() => null);
    await page.waitForSelector("table, .raceNaiyou, .chokuzen, .tenji, [class*='race'], [class*='tenji'], body", {
      timeout: Math.min(3000, cappedTimeout)
    }).catch(() => null);
    await page.waitForFunction(
      () => {
        const text = document.body ? document.body.innerText || "" : "";
        return /直前情報|直前展示|展示|ST|周回|一周|直線|まわり足|回り足|周足/.test(text);
      },
      { timeout: Math.min(4000, cappedTimeout) }
    ).catch(() => null);
    const oritenButton = page.locator("#btnOritenKaiseki").first();
    if (await oritenButton.count().catch(() => 0)) {
      await oritenButton.click({ timeout: Math.min(2500, cappedTimeout) }).catch(async () => {
        await page.evaluate(() => document.querySelector("#btnOritenKaiseki")?.click()).catch(() => null);
      });
      await page.waitForLoadState("networkidle", { timeout: Math.min(5000, cappedTimeout) }).catch(() => null);
      await page.waitForFunction(
        () => {
          const text = document.body ? document.body.innerText || "" : "";
          return /展示情報/.test(text) && /周回/.test(text) && /直線/.test(text);
        },
        { timeout: Math.min(5000, cappedTimeout) }
      ).catch(() => null);
    }
    const html = await page.content();
    const screenshot = await page.screenshot({ fullPage: true, type: "png" }).catch(() => null);
    return { html, screenshot };
  } finally {
    if (browser) await browser.close().catch(() => null);
  }
}

async function fetchRenderedHtmlWithPlaywright(url, timeoutMs = 45000) {
  const page = await fetchRenderedPageWithPlaywright(url, timeoutMs);
  return page.html;
}

function parseNetworkOriginalExhibitionResponses(responses = []) {
  const byLane = new Map();
  const fieldSources = {};
  const fieldDebugs = {};
  const tableDiagnostics = [];
  const responseDiagnostics = [];

  for (const response of Array.isArray(responses) ? responses : []) {
    const body = String(response?.body || "");
    if (!body) continue;
    const contains = inspectKeywordPresence(body, NETWORK_ORIGINAL_EXHIBITION_KEYWORDS);
    const url = String(response?.url || "");
    const shouldParse =
      hasAnyKeywordPresence(contains) ||
      /oriten|kaiseki|tenji|shukai|chokusen|mawari|ajax|api|request/i.test(url);
    if (!shouldParse) continue;

    const sourceLabel = `kyoteibiyori-network:${response?.index ?? "x"}`;
    const responseResult = {
      index: response?.index ?? null,
      url,
      contains,
      parsed_lanes_before: byLane.size,
      parsed_lanes_after: null,
      json_parse_ok: false,
      html_parse_ok: false,
      error: null
    };

    try {
      if (/json/i.test(String(response?.content_type || "")) || /^\s*[\[{]/.test(body)) {
        const json = JSON.parse(body);
        const parsedJson = parseKyoteiBiyoriAjaxData(json);
        mergeLaneMaps(byLane, parsedJson.byLane, fieldSources, sourceLabel);
        responseResult.json_parse_ok = parsedJson.byLane.size > 0;
      }
    } catch (error) {
      responseResult.error = responseResult.error || String(error?.message || error);
    }

    try {
      const parsedHtml = normalizeKyoteiBiyoriPreRaceFields(
        parseKyoteiBiyoriPreRaceData(body, {
          mode: "pre_race",
          sourceLabel
        })
      );
      mergeLaneMaps(byLane, parsedHtml.byLane, fieldSources, sourceLabel);
      mergeFieldDebugMaps(fieldDebugs, parsedHtml.fieldDebugs || {});
      tableDiagnostics.push(...(parsedHtml.tableDiagnostics || []));
      responseResult.html_parse_ok = parsedHtml.byLane.size > 0;
    } catch (error) {
      responseResult.error = responseResult.error || String(error?.message || error);
    }

    responseResult.parsed_lanes_after = byLane.size;
    responseDiagnostics.push(responseResult);
  }

  return {
    byLane,
    fieldSources,
    fieldDebugs,
    tableDiagnostics,
    responseDiagnostics,
    fieldDiagnostics: buildFieldDiagnostics(byLane, fieldSources)
  };
}

async function fetchText(url, timeoutMs = 12000) {
  const response = await axios.get(url, {
    timeout: timeoutMs,
    responseType: "text",
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });
  return String(response.data || "");
}

async function fetchOritenJson({ date, venueId, raceNo, refererUrl, timeoutMs = 12000 }) {
  const payload = {
    hiduke: String(date || "").replace(/-/g, ""),
    place_no: String(venueId || "").padStart(2, "0"),
    race_no: Number(raceNo),
    mode: 2
  };
  const params = new URLSearchParams();
  params.set("data", JSON.stringify(payload));

  const response = await axios.post(ORITEN_ENDPOINT, params.toString(), {
    timeout: timeoutMs,
    responseType: "json",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "X-Requested-With": "XMLHttpRequest",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
      Referer: refererUrl || buildFallbackSliderUrl({ date, venueId, raceNo, slider: 1 })
    }
  });

  return response.data;
}

function nowMs() {
  return Date.now();
}

function elapsedMs(startedAt) {
  return nowMs() - startedAt;
}

function extractActualRaceTabLinks(indexHtml, raceNo) {
  const $ = cheerio.load(indexHtml);
  const targetRaceNo = Number(raceNo);
  const result = {
    raceBlockFound: false,
    raceRowTitle: null,
    raceNumberHref: null,
    laneStatsHref: null,
    preRaceHref: null
  };

  $(".menu_box").each((_, block) => {
    const $block = $(block);
    const titleText = normalizeText($block.find("h2.race_ichiran_h2").first().text());
    const raceMatch = titleText.match(/(\d+)\s*R/i);
    if (!raceMatch || Number(raceMatch[1]) !== targetRaceNo) return;

    result.raceBlockFound = true;
    result.raceRowTitle = titleText || null;

    $block.find("a[href]").each((__, link) => {
      const text = normalizeText($(link).text());
      const href = $(link).attr("href");
      if (!href) return;
      const absoluteHref = new URL(href, KYOTEI_BIYORI_BASE).href;
      if (/^\d+\s*R$/i.test(text)) result.raceNumberHref = absoluteHref;
      if (text === "枠別勝率") result.laneStatsHref = absoluteHref;
      if (text === "直前情報") result.preRaceHref = absoluteHref;
    });
  });

  return result;
}

function extractTableMaps(html) {
  const $ = cheerio.load(html);
  return $("table")
    .toArray()
    .map((el) => {
      const $table = $(el);
      const rows = $table
        .find("tr")
        .toArray()
        .map((tr, rowIndex) => ({
          rowIndex,
          cells: $(tr)
            .children("th,td")
            .toArray()
            .map((cell, cellIndex) => {
              const rawText = normalizeSpace($(cell).text());
              return {
                cellIndex,
                rawText,
                normalizedText: normalizeText(rawText)
              };
            })
        }))
        .filter((row) => row.cells.length > 0);
      const headers = rows[0]?.cells.map((cell) => cell.normalizedText) || [];
      return {
        $,
        $table,
        headers,
        rows,
        text: normalizeText($table.text())
      };
    })
    .filter((table) => table.headers.length > 0);
}

const FIELD_DEBUG_NAME_MAP = {
  laneFirstRate: "lane1stRate",
  lane2RenRate: "lane2renRate",
  lane3RenRate: "lane3renRate",
  lapTimeRaw: "lapTime",
  exhibitionSt: "exhibitionST",
  exhibitionTime: "exhibitionTime",
  mawariashi: "turnTime",
  straightTime: "straightTime",
  nobiashi: "straightTime",
  motor2Rate: "motor2ren",
  motor3Rate: "motor3ren",
  lapExStretch: "lapExStretch"
};

const JAPANESE_LABELS = {
  laneStatsSection: "\u67a0\u5225\u60c5\u5831",
  preRaceSection: "\u76f4\u524d\u60c5\u5831",
  motorSection: "\u30e2\u30fc\u30bf\u30fc",
  motor2: "\u30e2\u30fc\u30bf\u30fc2\u9023\u7387",
  motor3: "\u30e2\u30fc\u30bf\u30fc3\u9023\u7387",
  mawariashi: "\u307e\u308f\u308a\u8db3",
  nobiashi: "\u76f4\u7dda",
  lapTime: "\u5468\u56de",
  exhibition: "\u5c55\u793a",
  st: "ST",
  lane1st: "1\u7740\u7387",
  lane2ren: "2\u9023\u7387",
  lane3ren: "3\u9023\u7387",
  season: "\u4eca\u671f",
  m6: "\u76f4\u8fd16\u304b\u6708",
  m3: "\u76f4\u8fd13\u304b\u6708",
  m1: "\u76f4\u8fd11\u304b\u6708",
  local: "\u5f53\u5730",
  ippansen: "\u4e00\u822c\u6226",
  sgG1: "SG\uff0fG1"
};

const LABEL_ALIASES = {
  laneStatsSection: ["\u8b6b\uf8f0\u86fb\uff65\u870d\u6649\u7d2b"],
  preRaceSection: ["\u9016\uff74\u8711\u80b4\u30e5\u8763\uff71"],
  motor2: ["\u30e2\u30fc\u30bf\u30fc2\u9023\u7387", "\u30e2\u30fc\u30bf\u30fc2\u7387", "\u30e2\u30fc\u30bf\u30fc", "\u7e5d\uff62\u7e5d\uff7c\u7e67\uff7f\u7e5d\uff7c2\u9a3e\uff63\u9087\u30fb"],
  motor3: ["\u7e5d\uff62\u7e5d\uff7c\u7e67\uff7f\u7e5d\uff7c3\u9a3e\uff63\u9087\u30fb"],
  mawariashi: ["\u307e\u308f\u308a\u8db3", "\u56de\u308a\u8db3", "\u56de\u8db3", "\u5468\u308a\u8db3", "\u5468\u8db3", "\u307e\u308f\u308a\u8db3\u30bf\u30a4\u30e0", "\u873b\uff68\u7e67\u9858\uff76\uff73"],
  nobiashi: ["\u76f4\u7dda", "\u76f4\u7dda\u30bf\u30a4\u30e0", "\u4f38\u3073\u8db3", "\u83a8\uff78\u7e3a\uff73\u96dc\uff73"],
  lapTime: ["\u5468\u56de", "\u5468\u56de\u30bf\u30a4\u30e0", "\u4e00\u5468", "\u4e00\u5468\u30bf\u30a4\u30e0", "\u5468\u56de\u5c55\u793a", "\u873b\uff68\u8757\u30fb"],
  exhibition: ["\u5c55\u793a", "\u5c55\u793a\u30bf\u30a4\u30e0", "\u87bb\u6155\uff64\uff7a"],
  lane1st: ["1\u9039\u0080\u9087\u30fb"],
  lane2ren: ["2\u9a3e\uff63\u9087\u30fb"],
  lane3ren: ["3\u9a3e\uff63\u9087\u30fb"],
  season: ["\u8389\u96e9\uff6d\uff63", "\u8389\u982d\u6084", "\u8389\u96c1\uff6d\uff63", "\u8389\u9811\u6084"],
  m6: ["\u9036\uff74\u9711\uff65\u7e3a\u533a\u6026", "\u9036\uff74\u9711\uff65\u86df\u533a\u6026"],
  m3: ["\u9036\uff74\u9711\uff65\u7e5d\uff76\u8b5b\u30fb"],
  m1: ["\u9036\uff74\u9711\uff65\u7e3a\u533a\u6026"],
  local: [],
  ippansen: [],
  sgG1: []
};

const LANE_STAT_PERIODS = {
  season: {
    labels: [JAPANESE_LABELS.season, "\u4eca\u5b63", ...LABEL_ALIASES.season],
    canonical: JAPANESE_LABELS.season,
    debugKey: "season",
    defaultWeights: {
      laneFirstRate: 0.18,
      lane2RenRate: 0.18,
      lane3RenRate: 0.18
    }
  },
  m6: {
    labels: [JAPANESE_LABELS.m6, "\u76f4\u8fd1\uff16\u304b\u6708", "\u6700\u8fd16\u304b\u6708", ...LABEL_ALIASES.m6],
    canonical: JAPANESE_LABELS.m6,
    debugKey: "m6",
    defaultWeights: {
      laneFirstRate: 0.22,
      lane2RenRate: 0.22,
      lane3RenRate: 0.2
    }
  },
  m3: {
    labels: [JAPANESE_LABELS.m3, "\u76f4\u8fd1\uff13\u304b\u6708", "\u6700\u8fd13\u304b\u6708", ...LABEL_ALIASES.m3],
    canonical: JAPANESE_LABELS.m3,
    debugKey: "m3",
    defaultWeights: {
      laneFirstRate: 0.32,
      lane2RenRate: 0.28,
      lane3RenRate: 0.24
    }
  },
  m1: {
    labels: [JAPANESE_LABELS.m1, "\u76f4\u8fd1\uff11\u304b\u6708", "\u6700\u8fd11\u304b\u6708", ...LABEL_ALIASES.m1],
    canonical: JAPANESE_LABELS.m1,
    debugKey: "m1",
    defaultWeights: {
      laneFirstRate: 0.06,
      lane2RenRate: 0.06,
      lane3RenRate: 0.06
    }
  },
  local: {
    labels: [JAPANESE_LABELS.local, "\u5730\u5143", "\u5f53\u5730\u6210\u7e3e"],
    canonical: JAPANESE_LABELS.local,
    debugKey: "local",
    defaultWeights: {
      laneFirstRate: 0.12,
      lane2RenRate: 0.16,
      lane3RenRate: 0.18
    }
  },
  ippansen: {
    labels: [JAPANESE_LABELS.ippansen, "\u4e00\u822c", "\u4e00\u822c\u6226\u6210\u7e3e"],
    canonical: JAPANESE_LABELS.ippansen,
    debugKey: "ippansen",
    defaultWeights: {
      laneFirstRate: 0.1,
      lane2RenRate: 0.1,
      lane3RenRate: 0.14
    }
  },
  sg_g1: {
    labels: [JAPANESE_LABELS.sgG1, "SG/G1", "SG\uff65G1", "SG\u30fbG1"],
    canonical: JAPANESE_LABELS.sgG1,
    debugKey: "sg_g1",
    defaultWeights: {
      laneFirstRate: 0,
      lane2RenRate: 0,
      lane3RenRate: 0
    }
  }
};

const LANE_STAT_FIELD_CONFIG = {
  laneFirstRate: {
    debugField: "lane1stRate",
    metricLabel: "1着率",
    periodsKey: "lane1st_raw",
    scoreField: "lane1stScore",
    debugScoreField: "lane1stDebug",
    periodFields: {
      season: "lane1stRate_season",
      m6: "lane1stRate_6m",
      m3: "lane1stRate_3m",
      m1: "lane1stRate_1m",
      local: "lane1stRate_local",
      ippansen: "lane1stRate_ippansen",
      sg_g1: "lane1stRate_sg_g1"
    },
    sumField: "lane1stRate_sum",
    avgField: "lane1stRate_avg",
    weightedField: "lane1stRate_weighted"
  },
  lane2RenRate: {
    debugField: "lane2renRate",
    metricLabel: "2連対率",
    periodsKey: "lane2ren_raw",
    scoreField: "lane2renScore",
    debugScoreField: "lane2renDebug",
    periodFields: {
      season: "lane2renRate_season",
      m6: "lane2renRate_6m",
      m3: "lane2renRate_3m",
      m1: "lane2renRate_1m",
      local: "lane2renRate_local",
      ippansen: "lane2renRate_ippansen",
      sg_g1: "lane2renRate_sg_g1"
    },
    sumField: "lane2renRate_sum",
    avgField: "lane2renRate_avg",
    weightedField: "lane2renRate_weighted"
  },
  lane3RenRate: {
    debugField: "lane3renRate",
    metricLabel: "3連対率",
    periodsKey: "lane3ren_raw",
    scoreField: "lane3renScore",
    debugScoreField: "lane3renDebug",
    periodFields: {
      season: "lane3renRate_season",
      m6: "lane3renRate_6m",
      m3: "lane3renRate_3m",
      m1: "lane3renRate_1m",
      local: "lane3renRate_local",
      ippansen: "lane3renRate_ippansen",
      sg_g1: "lane3renRate_sg_g1"
    },
    sumField: "lane3renRate_sum",
    avgField: "lane3renRate_avg",
    weightedField: "lane3renRate_weighted"
  }
};

function compactJapaneseLabel(value) {
  return normalizeDigits(normalizeSpace(value))
    .replace(/\s+/g, "")
    .replace(/[\u30fb\uff65]/g, "")
    .trim();
}

function matchesLabel(text, label, aliases = []) {
  if (!text || !label) return false;
  const compactText = compactJapaneseLabel(text);
  if (!compactText) return false;
  const candidates = [label, ...(Array.isArray(aliases) ? aliases : [])]
    .map((entry) => compactJapaneseLabel(entry))
    .filter(Boolean);
  return candidates.some((candidate) => compactText.includes(candidate) || compactText === candidate);
}

function matchesExactLabel(text, label, aliases = []) {
  if (!text || !label) return false;
  const compactText = compactJapaneseLabel(text);
  if (!compactText) return false;
  const candidates = [label, ...(Array.isArray(aliases) ? aliases : [])]
    .map((entry) => compactJapaneseLabel(entry))
    .filter(Boolean);
  return candidates.some((candidate) => compactText === candidate);
}

function findExactLaneStatMetricLabel(value) {
  if (matchesExactLabel(value, JAPANESE_LABELS.lane1st, LABEL_ALIASES.lane1st)) return JAPANESE_LABELS.lane1st;
  if (matchesExactLabel(value, "2連対率", ["2連率"])) return "2連対率";
  if (matchesExactLabel(value, "3連対率", ["3連率"])) return "3連対率";
  return null;
}

function canonicalLaneStatMetricToField(metricLabel) {
  if (metricLabel === JAPANESE_LABELS.lane1st) return "laneFirstRate";
  if (metricLabel === "2連対率" || metricLabel === "2連率") return "lane2RenRate";
  if (metricLabel === "3連対率" || metricLabel === "3連率") return "lane3RenRate";
  return null;
}

function findExactLaneStatPeriodKey(value) {
  for (const [periodKey, config] of Object.entries(LANE_STAT_PERIODS)) {
    if ((config.labels || []).some((label) => matchesExactLabel(value, label))) return periodKey;
  }
  return null;
}

function canonicalizeExplicitSectionLabel(value) {
  const text = compactJapaneseLabel(value);
  if (!text) return null;
  if (matchesLabel(text, JAPANESE_LABELS.laneStatsSection, LABEL_ALIASES.laneStatsSection)) return JAPANESE_LABELS.laneStatsSection;
  if (matchesLabel(text, JAPANESE_LABELS.preRaceSection, LABEL_ALIASES.preRaceSection)) return JAPANESE_LABELS.preRaceSection;
  if (matchesLabel(text, JAPANESE_LABELS.motor3, LABEL_ALIASES.motor3)) return JAPANESE_LABELS.motor3;
  if (matchesLabel(text, JAPANESE_LABELS.motor2, LABEL_ALIASES.motor2)) return JAPANESE_LABELS.motor2;
  if (matchesLabel(text, JAPANESE_LABELS.motorSection)) return JAPANESE_LABELS.motorSection;
  return null;
}

function canonicalizeExplicitMetricLabel(value) {
  const text = compactJapaneseLabel(value);
  if (!text) return null;
  if (matchesLabel(text, JAPANESE_LABELS.lapTime, LABEL_ALIASES.lapTime)) return JAPANESE_LABELS.lapTime;
  if (text === compactJapaneseLabel(JAPANESE_LABELS.st)) return JAPANESE_LABELS.st;
  if (matchesLabel(text, JAPANESE_LABELS.exhibition, LABEL_ALIASES.exhibition)) return JAPANESE_LABELS.exhibition;
  if (matchesLabel(text, JAPANESE_LABELS.mawariashi, LABEL_ALIASES.mawariashi)) return JAPANESE_LABELS.mawariashi;
  if (matchesLabel(text, JAPANESE_LABELS.nobiashi, LABEL_ALIASES.nobiashi)) return JAPANESE_LABELS.nobiashi;
  if (matchesLabel(text, JAPANESE_LABELS.motor3, LABEL_ALIASES.motor3) || (matchesLabel(text, JAPANESE_LABELS.motorSection) && text.includes("3"))) return JAPANESE_LABELS.motor3;
  if (matchesLabel(text, JAPANESE_LABELS.motor2, LABEL_ALIASES.motor2) || (matchesLabel(text, JAPANESE_LABELS.motorSection) && text.includes("2"))) return JAPANESE_LABELS.motor2;
  if (matchesLabel(text, JAPANESE_LABELS.lane1st, LABEL_ALIASES.lane1st)) return JAPANESE_LABELS.lane1st;
  if (matchesLabel(text, JAPANESE_LABELS.lane2ren, LABEL_ALIASES.lane2ren)) return JAPANESE_LABELS.lane2ren;
  if (matchesLabel(text, JAPANESE_LABELS.lane3ren, LABEL_ALIASES.lane3ren)) return JAPANESE_LABELS.lane3ren;
  return null;
}

function canonicalizeExplicitTimeWindowLabel(value) {
  const text = compactJapaneseLabel(value);
  if (!text) return null;
  for (const [periodKey, config] of Object.entries(LANE_STAT_PERIODS)) {
    if ((config.labels || []).some((label) => matchesLabel(text, label))) return periodKey;
  }
  return null;
}

function normalizeLaneStatPeriodValues(periods = {}) {
  const normalized = {};
  for (const key of Object.keys(LANE_STAT_PERIODS)) {
    const value = toFiniteNumberOrNull(periods?.[key]);
    normalized[key] = value;
  }
  return normalized;
}

function getLaneStatWeight(field, periodKey) {
  return Number(LANE_STAT_PERIODS?.[periodKey]?.defaultWeights?.[field] || 0);
}

function aggregateLaneStatPeriods(field, periods = {}) {
  const normalized = normalizeLaneStatPeriodValues(periods);
  const available = Object.entries(normalized).filter(([, value]) => value !== null);
  const availablePeriods = available.map(([periodKey]) => periodKey);
  if (!available.length) {
    return {
      raw: normalized,
      sum: null,
      avg: null,
      weighted: null,
      score: null,
      weightsUsed: {},
      hotFormBonus: 0,
      availablePeriods,
      count: 0
    };
  }

  const sum = Number(available.reduce((acc, [, value]) => acc + Number(value), 0).toFixed(4));
  const avg = Number((sum / available.length).toFixed(4));
  const rawWeights = Object.fromEntries(
    available.map(([periodKey]) => [periodKey, getLaneStatWeight(field, periodKey)])
  );
  const totalWeight = Object.values(rawWeights).reduce((acc, value) => acc + Number(value || 0), 0);
  const weighted =
    totalWeight > 0
      ? Number(
          (
            available.reduce(
              (acc, [periodKey, value]) => acc + Number(value) * Number(rawWeights[periodKey] || 0),
              0
            ) / totalWeight
          ).toFixed(4)
        )
      : null;
  const weightsUsed =
    totalWeight > 0
      ? Object.fromEntries(
          Object.entries(rawWeights).map(([periodKey, value]) => [periodKey, Number((Number(value || 0) / totalWeight).toFixed(4))])
        )
      : {};
  const recentStrong =
    Number.isFinite(normalized.m1) &&
    Number.isFinite(normalized.m3) &&
    normalized.m1 >= 60 &&
    normalized.m3 >= 60;
  const recentConsistency =
    recentStrong && Math.abs(Number(normalized.m1) - Number(normalized.m3)) <= 8;
  const hotFormBonus = recentConsistency
    ? Number(
        Math.min(
          2.2,
          ((Number(normalized.m1) - 55) * 0.03) + ((Number(normalized.m3) - 55) * 0.02)
        ).toFixed(4)
      )
    : 0;
  const score = weighted === null ? null : Number((weighted + hotFormBonus).toFixed(4));

  return {
    raw: normalized,
    sum,
    avg,
    weighted,
    score,
    weightsUsed,
    hotFormBonus,
    availablePeriods,
    count: available.length
  };
}

function hydrateLaneStatAggregateFields(row = {}) {
  const next = { ...row };
  for (const [baseField, config] of Object.entries(LANE_STAT_FIELD_CONFIG)) {
    const periods = {};
    for (const [periodKey, fieldName] of Object.entries(config.periodFields)) {
      periods[periodKey] = toFiniteNumberOrNull(next?.[fieldName]);
    }
    const aggregate = aggregateLaneStatPeriods(baseField, periods);
    next[config.periodsKey] = aggregate.raw;
    next[config.sumField] = aggregate.sum;
    next[config.avgField] = aggregate.avg;
    next[config.weightedField] = aggregate.weighted;
    next[config.scoreField] = aggregate.score;
    next[baseField] = aggregate.score;
    next[config.debugScoreField] = {
      raw: aggregate.raw,
      available: aggregate.availablePeriods,
      weights_used: aggregate.weightsUsed,
      hot_form_bonus: aggregate.hotFormBonus,
      final_score: aggregate.score,
      default_score_without_hot_bonus: aggregate.weighted,
      sg_g1_reference: aggregate.raw.sg_g1
    };
    next[`${config.debugField}_available_periods`] = aggregate.availablePeriods;
    next[`${config.debugField}_period_count`] = aggregate.count;
  }
  return next;
}

function normalizeLaneStatAggregateFields(row = {}) {
  const next = { ...row };
  for (const [baseField, config] of Object.entries(LANE_STAT_FIELD_CONFIG)) {
    for (const fieldName of Object.values(config.periodFields)) {
      next[fieldName] = toFiniteNumberOrNull(next?.[fieldName]);
    }
    const normalizedPeriods = normalizeLaneStatPeriodValues(next?.[config.periodsKey] || {});
    for (const [periodKey, fieldName] of Object.entries(config.periodFields)) {
      if (next[fieldName] === null && normalizedPeriods[periodKey] !== null) {
        next[fieldName] = normalizedPeriods[periodKey];
      }
    }
    const aggregate = aggregateLaneStatPeriods(
      baseField,
      Object.fromEntries(
        Object.entries(config.periodFields).map(([periodKey, fieldName]) => [periodKey, next?.[fieldName]])
      )
    );
    next[config.periodsKey] = aggregate.raw;
    next[config.sumField] = aggregate.sum;
    next[config.avgField] = aggregate.avg;
    next[config.weightedField] = aggregate.weighted;
    next[config.scoreField] = aggregate.score ?? toFiniteNumberOrNull(next?.[config.scoreField]);
    next[config.debugScoreField] = {
      raw: aggregate.raw,
      available: aggregate.availablePeriods,
      weights_used: aggregate.weightsUsed,
      hot_form_bonus: aggregate.hotFormBonus,
      final_score: aggregate.score,
      default_score_without_hot_bonus: aggregate.weighted,
      sg_g1_reference: aggregate.raw.sg_g1
    };
    next[baseField] = aggregate.score ?? toFiniteNumberOrNull(next?.[baseField]);
    next[`${config.debugField}_available_periods`] = aggregate.availablePeriods;
    next[`${config.debugField}_period_count`] = aggregate.count;
  }
  return next;
}

function detectExplicitBoatHeaderLane(text) {
  const normalized = compactJapaneseLabel(text);
  const exact = normalized.match(/^([1-6])(?:\u53f7\u8247|\u53f7)$/);
  if (exact) return Number(exact[1]);
  const loose = normalized.match(/^([1-6])/);
  if (loose) return Number(loose[1]);
  return null;
}

function findExplicitBoatColumnHeader(table) {
  for (const row of table.rows || []) {
    const laneColumns = new Map();
    const laneHeaders = {};
    for (const cell of row.cells || []) {
      const lane = detectExplicitBoatHeaderLane(cell?.rawText);
      if (!Number.isInteger(lane)) continue;
      laneColumns.set(lane, cell.cellIndex);
      laneHeaders[lane] = normalizeSpace(cell.rawText) || `${lane}`;
    }
    if (laneColumns.size === 6) {
      return {
        headerRowIndex: row.rowIndex,
        laneColumns,
        laneHeaders
      };
    }
  }
  return null;
}

function collectTableContextLabels(table) {
  const labels = [];
  const captionText = normalizeSpace(table.$table.find("caption").first().text());
  if (captionText) labels.push(captionText);
  const tableText = normalizeSpace(table.text);
  if (tableText) labels.push(tableText);
  return labels;
}

function resolveExplicitFieldMatch({ mode = "all", rowLabels = [], tableContextLabels = [] }) {
  const rowSectionCandidates = rowLabels
    .map(canonicalizeExplicitSectionLabel)
    .filter(Boolean);
  const tableSectionCandidates = tableContextLabels
    .map(canonicalizeExplicitSectionLabel)
    .filter(Boolean);
  const metricCandidates = rowLabels
    .map(canonicalizeExplicitMetricLabel)
    .filter(Boolean);
  const timeWindowCandidates = rowLabels
    .map(canonicalizeExplicitTimeWindowLabel)
    .filter(Boolean);

  const section = rowSectionCandidates[0] || tableSectionCandidates[0] || null;
  const joinedRowLabels = normalizeSpace(rowLabels.join(" "));
  const exactLapTimeRow = rowLabels.find((label) => matchesExactLabel(label, JAPANESE_LABELS.lapTime, LABEL_ALIASES.lapTime)) || null;
  const exactStRow = rowLabels.find((label) => matchesExactLabel(label, JAPANESE_LABELS.st)) || null;
  const exactExhibitionRow = rowLabels.find((label) => matchesExactLabel(label, JAPANESE_LABELS.exhibition, LABEL_ALIASES.exhibition)) || null;
  const exactMawariashiRow = rowLabels.find((label) => matchesExactLabel(label, JAPANESE_LABELS.mawariashi, LABEL_ALIASES.mawariashi)) || null;
  const exactNobiashiRow = rowLabels.find((label) => matchesExactLabel(label, JAPANESE_LABELS.nobiashi, LABEL_ALIASES.nobiashi)) || null;
  const metric =
    metricCandidates[0] ||
    (/\u5468.*\u56de/u.test(joinedRowLabels) ? JAPANESE_LABELS.lapTime : null) ||
    (/\u5468.*\u8db3/u.test(joinedRowLabels) ? JAPANESE_LABELS.mawariashi : null) ||
    (/\u4f38.*\u8db3/u.test(joinedRowLabels) ? JAPANESE_LABELS.nobiashi : null) ||
    (/\u30e2\u30fc\u30bf\u30fc.*2/u.test(joinedRowLabels) ? JAPANESE_LABELS.motor2 : null) ||
    (/\u30e2\u30fc\u30bf\u30fc.*3/u.test(joinedRowLabels) ? JAPANESE_LABELS.motor3 : null);
  const timeWindow = timeWindowCandidates[0] || null;

  if (mode === "lane_stats") {
    const exactSection =
      rowLabels.find((label) => matchesExactLabel(label, JAPANESE_LABELS.laneStatsSection, LABEL_ALIASES.laneStatsSection)) ||
      tableContextLabels.find((label) => matchesExactLabel(label, JAPANESE_LABELS.laneStatsSection, LABEL_ALIASES.laneStatsSection)) ||
      null;
    const exactMetric = rowLabels.find((label) => findExactLaneStatMetricLabel(label)) || null;
    const exactPeriod = rowLabels.find((label) => findExactLaneStatPeriodKey(label)) || null;
    if (!exactSection || !exactMetric || !exactPeriod) return null;
    const metricLabel = findExactLaneStatMetricLabel(exactMetric);
    const period = findExactLaneStatPeriodKey(exactPeriod);
    const field = canonicalLaneStatMetricToField(metricLabel);
    if (!field || !Object.prototype.hasOwnProperty.call(LANE_STAT_PERIODS, period)) return null;
    const periodLabel = LANE_STAT_PERIODS[period]?.canonical || period;
    return {
      field,
      section: JAPANESE_LABELS.laneStatsSection,
      row: metricLabel,
      period,
      periodLabel,
      exactMatchVerified: true
    };
  }

  if (mode === "pre_race") {
    const resolvedSection =
      metric === JAPANESE_LABELS.motor2
        ? JAPANESE_LABELS.motor2
        : metric === JAPANESE_LABELS.motor3
          ? JAPANESE_LABELS.motor3
          : rowSectionCandidates[0] || JAPANESE_LABELS.preRaceSection;
    if (exactLapTimeRow || metric === JAPANESE_LABELS.lapTime) {
      return {
        field: "lapTimeRaw",
        section: resolvedSection,
        row: JAPANESE_LABELS.lapTime,
        exactMatchVerified:
          resolvedSection === JAPANESE_LABELS.preRaceSection &&
          (exactLapTimeRow ? true : matchesLabel(joinedRowLabels, JAPANESE_LABELS.lapTime, LABEL_ALIASES.lapTime))
      };
    }
    if (exactStRow) return { field: "exhibitionSt", section: resolvedSection, row: JAPANESE_LABELS.st, exactMatchVerified: true };
    if (exactExhibitionRow) return { field: "exhibitionTime", section: resolvedSection, row: JAPANESE_LABELS.exhibition, exactMatchVerified: true };
    if (exactMawariashiRow) return { field: "mawariashi", section: resolvedSection, row: JAPANESE_LABELS.mawariashi, exactMatchVerified: true };
    if (exactNobiashiRow) return { field: "nobiashi", section: resolvedSection, row: JAPANESE_LABELS.nobiashi, exactMatchVerified: true };
    if (metric === JAPANESE_LABELS.motor2) return { field: "motor2Rate", section: resolvedSection, row: JAPANESE_LABELS.motor2 };
    if (metric === JAPANESE_LABELS.motor3) return { field: "motor3Rate", section: resolvedSection, row: JAPANESE_LABELS.motor3 };
    if (section === JAPANESE_LABELS.motorSection && metric === JAPANESE_LABELS.lane2ren) {
      return { field: "motor2Rate", section: JAPANESE_LABELS.motorSection, row: JAPANESE_LABELS.lane2ren };
    }
    if (section === JAPANESE_LABELS.motorSection && metric === JAPANESE_LABELS.lane3ren) {
      return { field: "motor3Rate", section: JAPANESE_LABELS.motorSection, row: JAPANESE_LABELS.lane3ren };
    }
    return null;
  }

  return null;
}

function parseExplicitTargetCell(field, rawText) {
  if (field === "lapTimeRaw") {
    const lapTimeRaw = parseDecimal(rawText);
    return {
      fields: {
        lapTimeRaw,
        lapTime: lapTimeRaw
      },
      value: lapTimeRaw
    };
  }

  if (field === "exhibitionSt") {
    const parsed = parseStartTimingRaw(rawText);
    const value = parsed.numeric;
    return {
      fields: {
        exhibitionSt: value,
        exhibitionStRaw: parsed.raw,
        exhibitionStFlag: parsed.flag,
        exhibitionStSignedValue: parsed.signedValue
      },
      value
    };
  }

  if (field === "exhibitionTime") {
    const value = parseDecimal(rawText);
    return {
      fields: { exhibitionTime: value },
      value
    };
  }

  if (field === "mawariashi") {
    const value = parseDecimal(rawText);
    return {
      fields: { turnTime: value, mawariashi: value, __mawariashi: value },
      value
    };
  }

  if (field === "nobiashi") {
    const value = parseDecimal(rawText);
    return {
      fields: { straightTime: value, nobiashi: value, __nobiashi: value },
      value
    };
  }

  if (field === "motor2Rate") {
    const value = parsePercent(rawText);
    return {
      fields: { motor2Rate: value },
      value
    };
  }

  if (field === "motor3Rate") {
    const value = parsePercent(rawText);
    return {
      fields: { motor3Rate: value },
      value
    };
  }

  if (field === "laneFirstRate") {
    const value = parsePercent(rawText);
    return {
      fields: { laneFirstRate: value },
      value
    };
  }

  if (field === "lane2RenRate") {
    const value = parsePercent(rawText);
    return {
      fields: { lane2RenRate: value },
      value
    };
  }

  if (field === "lane3RenRate") {
    const value = parsePercent(rawText);
    return {
      fields: { lane3RenRate: value },
      value
    };
  }

  return {
    fields: {},
    value: null
  };
}

function buildLaneStatPeriodFields(field, period, value) {
  const config = LANE_STAT_FIELD_CONFIG[field];
  if (!config || !period || !Object.prototype.hasOwnProperty.call(config.periodFields, period)) {
    return { fields: {}, value };
  }
  return {
    fields: {
      [config.periodFields[period]]: value
    },
    value
  };
}

function setLaneFieldDebug(fieldDebugs, lane, field, debugEntry) {
  if (!fieldDebugs[lane]) fieldDebugs[lane] = {};
  fieldDebugs[lane][field] = debugEntry;
}

function isVerifiedLaneStatDebug(debugEntry, expectedMetricLabel) {
  if (!debugEntry || typeof debugEntry !== "object") return false;
  const periodEntries = Object.values(LANE_STAT_PERIODS)
    .map((config) => debugEntry?.[config.debugKey])
    .filter((entry) => entry && typeof entry === "object" && Number.isFinite(Number(entry?.value)));
  if (!periodEntries.length) return false;
  return periodEntries.every((entry) =>
    entry?.exact_match_verified === true &&
    entry?.section === JAPANESE_LABELS.laneStatsSection &&
    entry?.metric === expectedMetricLabel
  );
}

function parseHtmlSupplementExplicit(html, options = {}) {
  const byLane = new Map();
  const fieldSources = {};
  const fieldDebugs = {};
  const tableDiagnostics = [];
  const tables = extractTableMaps(html);

  for (const table of tables) {
    const boatHeader = findExplicitBoatColumnHeader(table);
    if (!boatHeader) continue;

    const tableContextLabels = collectTableContextLabels(table);
    const matchedTargets = [];
    const cellMatches = [];

    for (const row of table.rows || []) {
      if (row.rowIndex === boatHeader.headerRowIndex) continue;
      const firstBoatColumn = Math.min(...boatHeader.laneColumns.values());
      const rowLabels = row.cells
        .filter((cell) => cell.cellIndex < firstBoatColumn)
        .map((cell) => normalizeSpace(cell.rawText))
        .filter(Boolean);

      if (!rowLabels.length) continue;

      const target = resolveExplicitFieldMatch({
        mode: options?.mode || "all",
        rowLabels,
        tableContextLabels
      });
      if (!target) continue;

      matchedTargets.push({
        row_index: row.rowIndex,
        section_label: target.section,
        row_labels: rowLabels,
        matched_field: FIELD_DEBUG_NAME_MAP[target.field] || target.field
      });

      for (let lane = 1; lane <= 6; lane += 1) {
        const columnIndex = boatHeader.laneColumns.get(lane);
        const columnHeader = boatHeader.laneHeaders[lane] || `${lane}号艇`;
        const cell = row.cells.find((candidate) => candidate.cellIndex === columnIndex);
        const rawCellText = normalizeSpace(cell?.rawText);
        const parsedBase = parseExplicitTargetCell(target.field, rawCellText);
        const parsed =
          options?.mode === "lane_stats"
            ? buildLaneStatPeriodFields(target.field, target.period, parsedBase.value)
            : parsedBase;
        const current = byLane.get(lane) || {};

        byLane.set(lane, {
          ...current,
          ...parsed.fields
        });

        const laneFieldSources = fieldSources[lane] || {};
        for (const [fieldKey, fieldValue] of Object.entries(parsed.fields || {})) {
          if (fieldValue === null || fieldValue === undefined || fieldValue === "") continue;
          laneFieldSources[fieldKey] = options?.sourceLabel || "race_shusso_html";
        }
        fieldSources[lane] = laneFieldSources;

        const debugEntry = {
          section: target.section,
          metric: target.row,
          period: target.period ? LANE_STAT_PERIODS[target.period]?.canonical || target.period : null,
          row: target.period ? LANE_STAT_PERIODS[target.period]?.canonical || target.period : target.row,
          sourceLabel: options?.sourceLabel || "race_shusso_html",
          column: columnHeader,
          boatColumn: columnHeader,
          raw: rawCellText || null,
          value: parsed.value,
          exact_match_verified: !!target?.exactMatchVerified
        };
        if (options?.mode === "lane_stats") {
          const laneField = FIELD_DEBUG_NAME_MAP[target.field] || target.field;
          if (!fieldDebugs[lane]) fieldDebugs[lane] = {};
          if (!fieldDebugs[lane][laneField] || typeof fieldDebugs[lane][laneField] !== "object") {
            fieldDebugs[lane][laneField] = {};
          }
          fieldDebugs[lane][laneField][LANE_STAT_PERIODS[target.period]?.debugKey || target.period] = debugEntry;
        } else {
          setLaneFieldDebug(fieldDebugs, lane, FIELD_DEBUG_NAME_MAP[target.field] || target.field, debugEntry);
        }
        cellMatches.push({
          lane,
          field: FIELD_DEBUG_NAME_MAP[target.field] || target.field,
          section_label: debugEntry.section,
          metric_label: debugEntry.metric || debugEntry.row,
          period_label: debugEntry.period,
          row_label: debugEntry.row,
          column_header: debugEntry.column,
          raw_cell_text: debugEntry.raw,
          normalized_value: debugEntry.value
        });
      }
    }

    if ((options?.mode || "all") === "lane_stats") {
      for (const [lane, row] of byLane.entries()) {
        const hydrated = hydrateLaneStatAggregateFields(row);
        byLane.set(lane, hydrated);
        const laneDebug = fieldDebugs?.[lane] || {};
        for (const [baseField, config] of Object.entries(LANE_STAT_FIELD_CONFIG)) {
          const fieldDebug = laneDebug?.[config.debugField];
          if (!fieldDebug || typeof fieldDebug !== "object") continue;
          const aggregate = aggregateLaneStatPeriods(
            baseField,
            Object.fromEntries(
              Object.keys(LANE_STAT_PERIODS).map((periodKey) => [periodKey, fieldDebug?.[periodKey]?.value ?? null])
            )
          );
          laneDebug[config.debugField] = {
            season: fieldDebug?.season || null,
            m6: fieldDebug?.m6 || null,
            m3: fieldDebug?.m3 || null,
            m1: fieldDebug?.m1 || null,
            local: fieldDebug?.local || null,
            ippansen: fieldDebug?.ippansen || null,
            sg_g1: fieldDebug?.sg_g1 || null,
            sum: aggregate.sum,
            avg: aggregate.avg,
            weighted: aggregate.weighted,
            weights_used: aggregate.weightsUsed,
            hot_form_bonus: aggregate.hotFormBonus,
            availablePeriods: aggregate.availablePeriods,
            count: aggregate.count,
            finalValue: aggregate.score,
            final_score: aggregate.score,
            exact_verified: isVerifiedLaneStatDebug(fieldDebug, config.metricLabel)
          };
        }
        fieldDebugs[lane] = laneDebug;
      }
    } else if ((options?.mode || "all") === "pre_race") {
      for (const [lane, row] of byLane.entries()) {
        const nextRow = {
          ...row
        };
        byLane.set(lane, nextRow);
        const laneDebug = fieldDebugs?.[lane] || {};
        if (laneDebug?.lapExStretch) {
          laneDebug.lapExStretch = {
            ...laneDebug.lapExStretch,
            sourceLabel: options?.sourceLabel || "race_shusso_html",
            matchedRowLabel: JAPANESE_LABELS.exhibition,
            finalValue: laneDebug?.lapExStretch?.value ?? null
          };
        }
        fieldDebugs[lane] = laneDebug;
      }
    }

    tableDiagnostics.push({
      mode: options?.mode || "all",
      context_labels: tableContextLabels,
      header_row_index: boatHeader.headerRowIndex,
      boat_columns: Object.fromEntries(
        [...boatHeader.laneColumns.entries()].map(([lane, columnIndex]) => [
          String(lane),
          {
            column_index: columnIndex,
            header_text: boatHeader.laneHeaders[lane] || `${lane}号艇`
          }
        ])
      ),
      matched_targets: matchedTargets,
      cell_matches: cellMatches
    });
  }

  return { byLane, fieldSources, fieldDebugs, tableDiagnostics };
}

function detectLaneText(text) {
  const normalized = normalizeText(text);
  const direct = normalized.match(/^(?:艇番|コース|枠)?\s*([1-6])$/);
  if (direct) return Number(direct[1]);
  const loose = normalized.match(/([1-6])/);
  return loose ? Number(loose[1]) : null;
}

function detectColumnIndex(headers, patterns) {
  const idx = headers.findIndex((header) => patterns.some((pattern) => pattern.test(header)));
  return idx >= 0 ? idx : null;
}

function normalizeJapaneseColumnHeader(value) {
  return normalizeText(value)
    .replace(/[ \t\r\n\u3000]/g, "")
    .replace(/[()（）［］\[\]【】:：/／・]/g, "")
    .replace(/秒/g, "")
    .toUpperCase();
}

function findHeaderIndexByLabels(headers, labels) {
  const normalizedHeaders = headers.map((header) => normalizeJapaneseColumnHeader(header));
  const normalizedLabels = labels.map((label) => normalizeJapaneseColumnHeader(label)).filter(Boolean);
  for (const label of normalizedLabels) {
    const exactIndex = normalizedHeaders.findIndex((header) => header === label);
    if (exactIndex >= 0) return exactIndex;
  }
  for (const label of normalizedLabels) {
    const partialIndex = normalizedHeaders.findIndex((header) => header.includes(label) || label.includes(header));
    if (partialIndex >= 0) return partialIndex;
  }
  return null;
}

function findMotor2HeaderIndexByLabels(headers) {
  const specificIndex = findHeaderIndexByLabels(headers, [
    "モーター2連率",
    "モーター2率",
    "モーター2連対率",
    "モーター2連"
  ]);
  if (specificIndex !== null) return specificIndex;

  const motorOnly = normalizeJapaneseColumnHeader("モーター");
  const normalizedHeaders = headers.map((header) => normalizeJapaneseColumnHeader(header));
  const exactMotorIndex = normalizedHeaders.findIndex((header) => header === motorOnly);
  return exactMotorIndex >= 0 ? exactMotorIndex : null;
}

function setParsedHeaderField({ row, laneFieldSources, fieldDebugs, lane, sourceLabel, field, rawText, parser, debugName, rowLabel }) {
  if (rawText === null || rawText === undefined || rawText === "") return;
  const parsed = parser(rawText);
  const fields = typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
    ? parsed.fields || {}
    : { [field]: parsed };
  const value = typeof parsed === "object" && parsed !== null && Object.prototype.hasOwnProperty.call(parsed, "value")
    ? parsed.value
    : fields[field];

  Object.assign(row, fields);
  for (const [key, fieldValue] of Object.entries(fields)) {
    if (fieldValue === null || fieldValue === undefined || fieldValue === "") continue;
    laneFieldSources[key] = sourceLabel;
  }
  setLaneFieldDebug(fieldDebugs, lane, debugName || FIELD_DEBUG_NAME_MAP[field] || field, {
    section: JAPANESE_LABELS.preRaceSection,
    metric: rowLabel,
    row: rowLabel,
    sourceLabel,
    column: rowLabel,
    boatColumn: `${lane}号艇`,
    raw: normalizeSpace(rawText) || null,
    value,
    exact_match_verified: true
  });
}

function parseHtmlSupplementByJapaneseHeaders(html, options = {}) {
  const byLane = new Map();
  const fieldSources = {};
  const fieldDebugs = {};
  const tableDiagnostics = [];
  const tables = extractTableMaps(html);
  const sourceLabel = options?.sourceLabel || "race_shusso_html";

  for (const table of tables) {
    const headers = table.headers || [];
    const indexes = {
      lane: findHeaderIndexByLabels(headers, ["艇番", "艇", "枠", "枠番", "コース"]),
      playerName: findHeaderIndexByLabels(headers, ["選手名", "選手", "名前"]),
      exhibitionSt: findHeaderIndexByLabels(headers, ["ST", "展示ST"]),
      exhibitionTime: findHeaderIndexByLabels(headers, ["展示", "展示タイム"]),
      lapTime: findHeaderIndexByLabels(headers, ["周回", "周回タイム", "一周", "一周タイム", "周回展示"]),
      turnTime: findHeaderIndexByLabels(headers, ["まわり足", "回り足", "回足", "周り足", "周足", "まわり足タイム"]),
      straightTime: findHeaderIndexByLabels(headers, ["直線", "直線タイム"]),
      motor2Rate: findMotor2HeaderIndexByLabels(headers)
    };
    const hasPreRaceColumns =
      indexes.lane !== null &&
      [indexes.exhibitionSt, indexes.exhibitionTime, indexes.lapTime, indexes.turnTime, indexes.straightTime, indexes.motor2Rate].some((idx) => idx !== null);
    if (!hasPreRaceColumns) continue;

    let parsedCount = 0;
    const matchedRows = [];
    table.$table.find("tr").slice(1).each((_, tr) => {
      const values = [];
      table.$(tr)
        .children("td,th")
        .each((__, cell) => {
          values.push(normalizeSpace(table.$(cell).text()));
        });
      if (values.length < 2) return;

      const lane =
        (indexes.lane !== null ? detectLaneText(values[indexes.lane]) ?? toNumber(values[indexes.lane]) : null) ??
        values.map((value) => detectLaneText(value)).find((value) => Number.isInteger(value)) ??
        null;
      if (!Number.isInteger(lane) || lane < 1 || lane > 6) return;

      const current = byLane.get(lane) || {};
      const laneFieldSources = fieldSources[lane] || {};
      if (indexes.playerName !== null && values[indexes.playerName]) {
        current.playerName = values[indexes.playerName];
        current.name = values[indexes.playerName];
        current.racerName = values[indexes.playerName];
        laneFieldSources.playerName = sourceLabel;
      }

      setParsedHeaderField({
        row: current,
        laneFieldSources,
        fieldDebugs,
        lane,
        sourceLabel,
        field: "exhibitionSt",
        rawText: indexes.exhibitionSt !== null ? values[indexes.exhibitionSt] : null,
        rowLabel: "ST",
        debugName: "exhibitionST",
        parser: (rawText) => {
          const parsed = parseStartTimingRaw(rawText);
          return {
            fields: {
              exhibitionSt: parsed.numeric,
              exhibitionStRaw: parsed.raw,
              exhibitionStFlag: parsed.flag,
              exhibitionStSignedValue: parsed.signedValue
            },
            value: parsed.numeric
          };
        }
      });
      setParsedHeaderField({
        row: current,
        laneFieldSources,
        fieldDebugs,
        lane,
        sourceLabel,
        field: "mawariashi",
        rawText: indexes.turnTime !== null ? values[indexes.turnTime] : null,
        rowLabel: "まわり足",
        debugName: "turnTime",
        parser: (rawText) => {
          const turnTime = parseDecimal(rawText);
          return {
            fields: {
              turnTime,
              mawariashi: turnTime,
              __mawariashi: turnTime
            },
            value: turnTime
          };
        }
      });
      setParsedHeaderField({
        row: current,
        laneFieldSources,
        fieldDebugs,
        lane,
        sourceLabel,
        field: "exhibitionTime",
        rawText: indexes.exhibitionTime !== null ? values[indexes.exhibitionTime] : null,
        rowLabel: "展示",
        parser: (rawText) => parseDecimal(rawText)
      });
      setParsedHeaderField({
        row: current,
        laneFieldSources,
        fieldDebugs,
        lane,
        sourceLabel,
        field: "lapTime",
        rawText: indexes.lapTime !== null ? values[indexes.lapTime] : null,
        rowLabel: "周回",
        parser: (rawText) => {
          const lapTimeRaw = parseDecimal(rawText);
          return {
            fields: { lapTimeRaw, lapTime: lapTimeRaw },
            value: lapTimeRaw
          };
        }
      });
      setParsedHeaderField({
        row: current,
        laneFieldSources,
        fieldDebugs,
        lane,
        sourceLabel,
        field: "straightTime",
        rawText: indexes.straightTime !== null ? values[indexes.straightTime] : null,
        rowLabel: "直線",
        parser: (rawText) => {
          const straightTime = parseDecimal(rawText);
          return {
            fields: {
              straightTime,
              nobiashi: straightTime,
              __nobiashi: straightTime
            },
            value: straightTime
          };
        }
      });
      setParsedHeaderField({
        row: current,
        laneFieldSources,
        fieldDebugs,
        lane,
        sourceLabel,
        field: "motor2Rate",
        rawText: indexes.motor2Rate !== null ? values[indexes.motor2Rate] : null,
        rowLabel: "モーター2連率",
        debugName: "motor2ren",
        parser: (rawText) => parsePercent(rawText)
      });

      byLane.set(lane, current);
      fieldSources[lane] = laneFieldSources;
      parsedCount += 1;
      matchedRows.push({ lane, values });
    });

    tableDiagnostics.push({
      mode: options?.mode || "all",
      parser: "japanese_header_columns",
      headers,
      indexes,
      parsedCount,
      matched_rows: matchedRows
    });
  }

  return { byLane, fieldSources, fieldDebugs, tableDiagnostics };
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function detectLaneMarkersInBlock(text) {
  const normalized = normalizeDigits(normalizeSpace(text));
  const lanes = new Set();
  const patterns = [
    /(?:艇番|枠番|コース|course|lane)\s*[:：]?\s*([1-6])(?:\s*(?:号艇|号|艇|コース|枠))?/gi,
    /(?:^|[^0-9])([1-6])\s*(?:号艇|号|艇|コース|枠)(?=$|[^0-9])/g
  ];
  for (const pattern of patterns) {
    let match = null;
    while ((match = pattern.exec(normalized)) !== null) {
      const lane = Number(match[1]);
      if (Number.isInteger(lane) && lane >= 1 && lane <= 6) lanes.add(lane);
    }
  }
  return [...lanes];
}

function parseBlockMetricValue(text, labelPattern, parser) {
  const normalized = normalizeSpace(text);
  const valuePattern = "(F\\.?\\d+(?:\\.\\d+)?|L\\.?\\d+(?:\\.\\d+)?|[+-]?(?:\\d+\\.\\d+|\\d+|\\.\\d+)%?)";
  const re = new RegExp(`(?:${labelPattern})\\s*(?:[:：=／/\\-])?\\s*${valuePattern}`, "i");
  const match = normalized.match(re);
  if (!match) return { raw: null, parsed: null };
  const raw = match[1] || null;
  return { raw, parsed: parser(raw) };
}

function getBlockMetricConfigs() {
  return [
    {
      field: "exhibitionSt",
      debugName: "exhibitionST",
      rowLabel: "ST",
      labelPattern: "(?:展示\\s*ST|ST)",
      parser: (raw) => {
        const parsed = parseStartTimingRaw(raw);
        return {
          fields: {
            exhibitionSt: parsed.numeric,
            exhibitionStRaw: parsed.raw,
            exhibitionStFlag: parsed.flag,
            exhibitionStSignedValue: parsed.signedValue
          },
          value: parsed.numeric
        };
      }
    },
    {
      field: "exhibitionTime",
      debugName: "exhibitionTime",
      rowLabel: "展示",
      labelPattern: "(?:展示\\s*タイム|(?<!周回)展示(?!\\s*ST))",
      parser: (raw) => ({ fields: { exhibitionTime: parseDecimal(raw) }, value: parseDecimal(raw) })
    },
    {
      field: "lapTimeRaw",
      debugName: "lapTime",
      rowLabel: "周回",
      labelPattern: "(?:周回\\s*(?:タイム|展示)?|一周\\s*(?:タイム)?)",
      parser: (raw) => {
        const lapTimeRaw = parseDecimal(raw);
        return { fields: { lapTimeRaw, lapTime: lapTimeRaw }, value: lapTimeRaw };
      }
    },
    {
      field: "mawariashi",
      debugName: "turnTime",
      rowLabel: "まわり足",
      labelPattern: "(?:まわり足\\s*(?:タイム)?|回り足|回足|周り足|周足)",
      parser: (raw) => {
        const turnTime = parseDecimal(raw);
        return { fields: { turnTime, mawariashi: turnTime, __mawariashi: turnTime }, value: turnTime };
      }
    },
    {
      field: "straightTime",
      debugName: "straightTime",
      rowLabel: "直線",
      labelPattern: "(?:直線\\s*(?:タイム)?|伸び足)",
      parser: (raw) => {
        const straightTime = parseDecimal(raw);
        return { fields: { straightTime, nobiashi: straightTime, __nobiashi: straightTime }, value: straightTime };
      }
    },
    {
      field: "motor2Rate",
      debugName: "motor2ren",
      rowLabel: "モーター2連率",
      labelPattern: "(?:モーター\\s*2\\s*(?:連対率|連率|率|連)?|モーター(?!\\s*3\\s*(?:連対率|連率|率|連)?))",
      parser: (raw) => ({ fields: { motor2Rate: parsePercent(raw) }, value: parsePercent(raw) })
    }
  ];
}

function parseBlockMetrics(text) {
  const configs = getBlockMetricConfigs();
  const fields = {};
  const debugEntries = {};
  for (const config of configs) {
    const { raw, parsed } = parseBlockMetricValue(text, config.labelPattern, config.parser);
    if (!parsed) continue;
    Object.assign(fields, parsed.fields || {});
    debugEntries[config.debugName] = {
      section: JAPANESE_LABELS.preRaceSection,
      metric: config.rowLabel,
      row: config.rowLabel,
      sourceLabel: "kyoteibiyori_block",
      column: config.rowLabel,
      boatColumn: null,
      raw: raw ?? null,
      value: parsed.value,
      exact_match_verified: true
    };
  }
  return { fields, debugEntries };
}

function getParsedMetricEntries(parsed) {
  return Object.entries(parsed?.fields || {}).filter(([, value]) => value !== null && value !== undefined && value !== "");
}

function applyParsedBlockToLane({ byLane, fieldSources, fieldDebugs, cellMatches, lane, parsed, sourceLabel, parserName, rawText }) {
  const parsedEntries = getParsedMetricEntries(parsed);
  if (!parsedEntries.length) return false;

  const current = byLane.get(lane) || {};
  const laneFieldSources = fieldSources[lane] || {};
  for (const [field, value] of parsedEntries) {
    current[field] = value;
    laneFieldSources[field] = sourceLabel;
    cellMatches.push({
      lane,
      field,
      parser: parserName,
      raw_block_text: rawText,
      normalized_value: value
    });
  }
  byLane.set(lane, current);
  fieldSources[lane] = laneFieldSources;

  for (const [debugName, debugEntry] of Object.entries(parsed?.debugEntries || {})) {
    setLaneFieldDebug(fieldDebugs, lane, debugName, {
      ...debugEntry,
      sourceLabel,
      boatColumn: `${lane}号艇`
    });
  }
  return true;
}

function splitTextIntoLaneSegments(text) {
  const normalized = normalizeDigits(normalizeSpace(text));
  if (!normalized) return [];
  const markerPattern = /(?:艇番|枠番|コース|course|lane)\s*[:：]?\s*([1-6])(?:\s*(?:号艇|号|艇|コース|枠))?|([1-6])\s*(?:号艇|号|艇|コース|枠)/gi;
  const markers = [];
  let match = null;
  while ((match = markerPattern.exec(normalized)) !== null) {
    const lane = Number(match[1] || match[2]);
    if (Number.isInteger(lane) && lane >= 1 && lane <= 6) {
      markers.push({ lane, index: match.index });
    }
  }
  if (markers.length < 2) return [];
  return markers
    .map((marker, index) => {
      const next = markers[index + 1] || null;
      const segment = normalizeSpace(normalized.slice(marker.index, next ? next.index : undefined));
      return { lane: marker.lane, text: segment };
    })
    .filter((entry) => entry.text.length >= 12);
}

function extractMetricValueTokens(text) {
  const tokens = String(text || "").match(/F\.?\d+(?:\.\d+)?|L\.?\d+(?:\.\d+)?|[+-]?(?:\d+\.\d+|\.\d+|\d+)%?/gi);
  return Array.isArray(tokens) ? tokens : [];
}

function parseLabelRowsFromText(text, sourceLabel) {
  const byLane = new Map();
  const fieldSources = {};
  const fieldDebugs = {};
  const cellMatches = [];
  const $ = cheerio.load(text || "");
  $("table,script,style,noscript").remove();
  const hasMarkup = /<[a-z][\s\S]*>/i.test(String(text || ""));
  const bodyText = normalizeSpace($("body").text()) || (hasMarkup ? "" : normalizeSpace(text));
  const normalized = normalizeSpace(bodyText);
  if (!normalized) return { byLane, fieldSources, fieldDebugs, tableDiagnostics: [] };

  const configs = getBlockMetricConfigs();
  const matches = [];
  for (const config of configs) {
    const re = new RegExp(config.labelPattern, "gi");
    let match = null;
    while ((match = re.exec(normalized)) !== null) {
      if (config.field === "exhibitionTime") {
        const prefix = normalized.slice(Math.max(0, match.index - 2), match.index);
        if (/周回$/.test(prefix)) continue;
      }
      matches.push({
        config,
        index: match.index,
        end: match.index + match[0].length,
        label: match[0]
      });
    }
  }
  matches.sort((a, b) => a.index - b.index || b.end - a.end);
  if (!matches.length) return { byLane, fieldSources, fieldDebugs, tableDiagnostics: [] };

  for (let index = 0; index < matches.length; index += 1) {
    const currentMatch = matches[index];
    const nextMatch = matches.slice(index + 1).find((candidate) => candidate.index > currentMatch.end) || null;
    const slice = normalized.slice(currentMatch.end, nextMatch ? nextMatch.index : undefined);
    const tokens = extractMetricValueTokens(slice);
    if (tokens.length < 6) continue;

    for (let lane = 1; lane <= 6; lane += 1) {
      const raw = tokens[lane - 1];
      const parsed = currentMatch.config.parser(raw);
      if (!parsed) continue;
      const parsedWithDebug = {
        fields: parsed.fields || {},
        debugEntries: {
          [currentMatch.config.debugName]: {
            section: JAPANESE_LABELS.preRaceSection,
            metric: currentMatch.config.rowLabel,
            row: currentMatch.config.rowLabel,
            sourceLabel,
            column: currentMatch.config.rowLabel,
            boatColumn: `${lane}号艇`,
            raw,
            value: parsed.value,
            exact_match_verified: true
          }
        }
      };
      applyParsedBlockToLane({
        byLane,
        fieldSources,
        fieldDebugs,
        cellMatches,
        lane,
        parsed: parsedWithDebug,
        sourceLabel,
        parserName: "label_row_six_values",
        rawText: `${currentMatch.label} ${slice}`.trim()
      });
    }
  }

  return {
    byLane,
    fieldSources,
    fieldDebugs,
    tableDiagnostics: cellMatches.length > 0
      ? [{
          parser: "label_row_six_values",
          parsedCount: byLane.size,
          cell_matches: cellMatches
        }]
      : []
  };
}

function parseHtmlSupplementByBlocks(html, options = {}) {
  const byLane = new Map();
  const fieldSources = {};
  const fieldDebugs = {};
  const tableDiagnostics = [];
  const $ = cheerio.load(html || "");
  const sourceLabel = options?.sourceLabel || "race_shusso_html";
  const cellMatches = [];

  $("div,li,section,article,dl,dd,p").each((_, el) => {
    const text = normalizeSpace($(el).text());
    if (!text || text.length > 1200) return;
    const lanes = detectLaneMarkersInBlock(text);
    if (lanes.length !== 1) return;
    const lane = lanes[0];
    const parsed = parseBlockMetrics(text);
    applyParsedBlockToLane({
      byLane,
      fieldSources,
      fieldDebugs,
      cellMatches,
      lane,
      parsed,
      sourceLabel,
      parserName: "label_near_number_block",
      rawText: text
    });
  });

  const segmentCandidateTexts = [];
  $("body,main,section,article,div,ul,ol").each((_, el) => {
    const text = normalizeSpace($(el).text());
    if (!text || text.length < 80 || text.length > 8000) return;
    if (!/(?:周回|一周|直線|まわり足|回り足|周足|展示|ST)/.test(text)) return;
    if (detectLaneMarkersInBlock(text).length < 2) return;
    segmentCandidateTexts.push(text);
  });
  for (const text of segmentCandidateTexts.slice(0, 40)) {
    for (const segment of splitTextIntoLaneSegments(text)) {
      const parsed = parseBlockMetrics(segment.text);
      applyParsedBlockToLane({
        byLane,
        fieldSources,
        fieldDebugs,
        cellMatches,
        lane: segment.lane,
        parsed,
        sourceLabel,
        parserName: "lane_segment_block",
        rawText: segment.text
      });
    }
  }

  if (cellMatches.length > 0) {
    tableDiagnostics.push({
      mode: options?.mode || "all",
      parser: "label_near_number_blocks",
      parsedCount: byLane.size,
      cell_matches: cellMatches
    });
  }

  return { byLane, fieldSources, fieldDebugs, tableDiagnostics };
}

function detectBoatHeaderLane(text) {
  const normalized = normalizeDigits(normalizeSpace(text)).replace(/\s+/g, "");
  const exact = normalized.match(/^([1-6])号艇$/);
  if (exact) return Number(exact[1]);
  const compact = normalized.match(/^([1-6])(?:号|艇)?$/);
  return compact ? Number(compact[1]) : null;
}

function findBoatColumnHeader(table) {
  for (const row of table.rows || []) {
    const laneColumns = new Map();
    const laneHeaders = {};
    for (const cell of row.cells || []) {
      const lane = detectBoatHeaderLane(cell?.rawText);
      if (!Number.isInteger(lane)) continue;
      laneColumns.set(lane, cell.cellIndex);
      laneHeaders[lane] = cell.rawText || `${lane}号艇`;
    }
    if (laneColumns.size === 6) {
      return {
        headerRowIndex: row.rowIndex,
        laneColumns,
        laneHeaders
      };
    }
  }
  return null;
}

function canonicalizeSupplementRowLabel(text) {
  const normalized = normalizeText(text).replace(/\s+/g, "");
  if (!normalized) return null;
  if (/^周回(?:タイム)?$/.test(normalized)) return "周回";
  if (/^(?:展示)?ST$/.test(normalized)) return "ST";
  if (/^展示(?:タイム)?$/.test(normalized)) return "展示";
  if (/^(?:周り足|回り足|回足|まわり足|周足)$/.test(normalized)) return "周り足";
  if (/^直線$/.test(normalized)) return "直線";
  if (/^モーター?2(?:連率|連対率|連)$/.test(normalized)) return "モーター2連率";
  if (/^モーター?3(?:連率|連対率|連)$/.test(normalized)) return "モーター3連率";
  if (/^1着率$/.test(normalized)) return "1着率";
  if (/^(?:2連率|2連対率)$/.test(normalized)) return "2連率";
  if (/^(?:3連率|3連対率)$/.test(normalized)) return "3連率";
  return null;
}

function getAllowedSupplementRowLabels(mode = "all") {
  if (mode === "pre_race") return new Set(["周回", "ST", "展示", "周り足", "直線"]);
  if (mode === "lane_stats") return new Set(["モーター2連率", "モーター3連率", "1着率", "2連率", "3連率"]);
  return new Set(["周回", "ST", "展示", "周り足", "直線", "モーター2連率", "モーター3連率", "1着率", "2連率", "3連率"]);
}

function parseSupplementCell(rowLabel, rawText) {
  if (rowLabel === "周回") {
    const lapTimeRaw = parseDecimal(rawText);
    return {
      fields: {
        lapTimeRaw,
        lapTime: lapTimeRaw
      },
      parsedValue: lapTimeRaw
    };
  }
  if (rowLabel === "ST") {
    const parsed = parseStartTimingRaw(rawText);
    return {
      fields: {
        exhibitionSt: parsed.numeric,
        exhibitionStRaw: parsed.raw,
        exhibitionStFlag: parsed.flag,
        exhibitionStSignedValue: parsed.signedValue
      },
      parsedValue: parsed.numeric
    };
  }
  if (rowLabel === "展示") {
    const exhibitionTime = parseDecimal(rawText);
    return {
      fields: {
        exhibitionTime
      },
      parsedValue: exhibitionTime
    };
  }
  if (rowLabel === "周り足") {
    const mawariashi = parseDecimal(rawText);
    return {
      fields: { __mawariashi: mawariashi },
      parsedValue: mawariashi
    };
  }
  if (rowLabel === "直線") {
    const chokusen = parseDecimal(rawText);
    return {
      fields: { straightTime: chokusen, nobiashi: chokusen, __nobiashi: chokusen, __chokusen: chokusen },
      parsedValue: chokusen
    };
  }
  if (rowLabel === "モーター2連率") {
    const motor2Rate = parsePercent(rawText);
    return {
      fields: { motor2Rate },
      parsedValue: motor2Rate
    };
  }
  if (rowLabel === "モーター3連率") {
    const motor3Rate = parsePercent(rawText);
    return {
      fields: { motor3Rate },
      parsedValue: motor3Rate
    };
  }
  if (rowLabel === "1着率") {
    const laneFirstRate = parsePercent(rawText);
    return {
      fields: { laneFirstRate },
      parsedValue: laneFirstRate
    };
  }
  if (rowLabel === "2連率") {
    const lane2RenRate = parsePercent(rawText);
    return {
      fields: { lane2RenRate },
      parsedValue: lane2RenRate
    };
  }
  if (rowLabel === "3連率") {
    const lane3RenRate = parsePercent(rawText);
    return {
      fields: { lane3RenRate },
      parsedValue: lane3RenRate
    };
  }
  return {
    fields: {},
    parsedValue: null
  };
}

function finalizeSupplementLaneRow(row = {}) {
  return {
    ...row
  };
}

function parseHtmlSupplement(html) {
  const byLane = new Map();
  const fieldSources = {};
  const tableDiagnostics = [];
  const tables = extractTableMaps(html);

  const patterns = {
    lane: [/^艇番$/i, /^コース$/i, /^枠$/i],
    playerName: [/選手/i, /名前/i],
    fCount: [/^F$/i, /F数/i],
    lapTime: [/周回タイム/i, /1周タイム/i, /ラップタイム/i],
    lapExhibition: [/周回展示/i, /伸び足/i, /足色/i, /出足/i, /回り足/i],
    exhibitionSt: [/展示ST/i, /^ST$/i],
    exhibitionTime: [/展示タイム/i],
    motor2Rate: [/モーター.*2.*(?:率|連)/i, /モーター.*2連対率/i, /^2連率$/i, /^2連対率$/i],
    motor3Rate: [/モーター.*3.*(?:率|連)/i, /モーター.*3連対率/i, /^3連率$/i, /^3連対率$/i],
    laneFirstRate: [/1着率/i],
    lane2RenRate: [/2着率/i, /2連率/i, /2連対率/i],
    lane3RenRate: [/3着率/i, /3連率/i, /3連対率/i]
  };

  for (const table of tables) {
    if (!/(周回タイム|展示ST|モーター|1着率|2着率|3着率|2連率|3連率|選手|F)/.test(table.text)) continue;

    const indexes = {
      lane: detectColumnIndex(table.headers, patterns.lane),
      playerName: detectColumnIndex(table.headers, patterns.playerName),
      fCount: detectColumnIndex(table.headers, patterns.fCount),
      lapTime: detectColumnIndex(table.headers, patterns.lapTime),
      lapExhibition: detectColumnIndex(table.headers, patterns.lapExhibition),
      exhibitionSt: detectColumnIndex(table.headers, patterns.exhibitionSt),
      exhibitionTime: detectColumnIndex(table.headers, patterns.exhibitionTime),
      motor2Rate: detectColumnIndex(table.headers, patterns.motor2Rate),
      motor3Rate: detectColumnIndex(table.headers, patterns.motor3Rate),
      laneFirstRate: detectColumnIndex(table.headers, patterns.laneFirstRate),
      lane2RenRate: detectColumnIndex(table.headers, patterns.lane2RenRate),
      lane3RenRate: detectColumnIndex(table.headers, patterns.lane3RenRate)
    };
    const motorTableText = `${table.headers.join(" ")} ${table.text}`;
    if (indexes.motor2Rate === null && indexes.lane2RenRate !== null && /モーター/i.test(motorTableText)) {
      indexes.motor2Rate = indexes.lane2RenRate;
    }
    if (indexes.motor3Rate === null && indexes.lane3RenRate !== null && /モーター/i.test(motorTableText)) {
      indexes.motor3Rate = indexes.lane3RenRate;
    }

    let parsedCount = 0;
    table.$table.find("tr").slice(1).each((_, tr) => {
      const values = [];
      table.$(tr)
        .children("td,th")
        .each((__, cell) => {
          values.push(normalizeText(table.$(cell).text()));
        });
      if (values.length < 2) return;

      const lane =
        (indexes.lane !== null ? toNumber(values[indexes.lane]) : null) ??
        values.map((value) => detectLaneText(value)).find((value) => Number.isInteger(value)) ??
        null;
      if (!Number.isInteger(lane) || lane < 1 || lane > 6) return;

      const current = byLane.get(lane) || {};
      const next = {
        playerName: indexes.playerName !== null ? values[indexes.playerName] || null : null,
        fCount: indexes.fCount !== null ? parseFCount(values[indexes.fCount]) : null,
        lapTimeRaw: indexes.lapTime !== null ? parseDecimal(values[indexes.lapTime]) : null,
        exhibitionSt: indexes.exhibitionSt !== null ? parseDecimal(values[indexes.exhibitionSt]) : null,
        exhibitionTime: indexes.exhibitionTime !== null ? parseDecimal(values[indexes.exhibitionTime]) : null,
        motor2Rate: indexes.motor2Rate !== null ? parsePercent(values[indexes.motor2Rate]) : null,
        motor3Rate: indexes.motor3Rate !== null ? parsePercent(values[indexes.motor3Rate]) : null,
        laneFirstRate: indexes.laneFirstRate !== null ? parsePercent(values[indexes.laneFirstRate]) : null,
        lane2RenRate: indexes.lane2RenRate !== null ? parsePercent(values[indexes.lane2RenRate]) : null,
        lane3RenRate: indexes.lane3RenRate !== null ? parsePercent(values[indexes.lane3RenRate]) : null
      };

      if (next.lapTimeRaw !== null) next.lapTime = normalizeLapTimeForModel(next.lapTimeRaw);

      const merged = { ...current };
      const laneFieldSources = fieldSources[lane] || {};
      for (const [key, value] of Object.entries(next)) {
        if (value === null || value === undefined || value === "") continue;
        merged[key] = value;
        laneFieldSources[key] = "race_shusso_html";
      }
      byLane.set(lane, merged);
      fieldSources[lane] = laneFieldSources;
      parsedCount += 1;
    });

    tableDiagnostics.push({ headers: table.headers, parsedCount });
  }

  return { byLane, fieldSources, tableDiagnostics };
}

function parseHtmlSupplementStrict(html, options = {}) {
  const byLane = new Map();
  const fieldSources = {};
  const tableDiagnostics = [];
  const tables = extractTableMaps(html);
  const allowedRows = getAllowedSupplementRowLabels(options?.mode || "all");

  for (const table of tables) {
    const boatHeader = findBoatColumnHeader(table);
    if (!boatHeader) continue;

    let parsedCount = 0;
    const matchedRows = [];
    const cellMatches = [];
    const firstBoatColumn = Math.min(...boatHeader.laneColumns.values());

    for (const row of table.rows || []) {
      if (row.rowIndex === boatHeader.headerRowIndex) continue;
      const labelCells = row.cells.filter((cell) => cell.cellIndex < firstBoatColumn);
      const rawLabelText = labelCells.map((cell) => cell.rawText).filter(Boolean).join(" / ");
      const matchedRowLabel = canonicalizeSupplementRowLabel(rawLabelText);
      if (!matchedRowLabel || !allowedRows.has(matchedRowLabel)) continue;

      matchedRows.push({
        row_index: row.rowIndex,
        raw_label_text: rawLabelText,
        matched_row_label: matchedRowLabel
      });

      for (let lane = 1; lane <= 6; lane += 1) {
        const columnIndex = boatHeader.laneColumns.get(lane);
        const columnHeader = boatHeader.laneHeaders[lane] || `${lane}号艇`;
        const cell = row.cells.find((candidate) => candidate.cellIndex === columnIndex);
        const rawCellText = cell?.rawText || "";
        const parsed = parseSupplementCell(matchedRowLabel, rawCellText);
        const current = byLane.get(lane) || {};
        byLane.set(lane, { ...current, ...parsed.fields });

        const laneFieldSources = fieldSources[lane] || {};
        for (const [key, value] of Object.entries(parsed.fields)) {
          if (value === null || value === undefined || value === "") continue;
          laneFieldSources[key] = options?.sourceLabel || "race_shusso_html";
        }
        fieldSources[lane] = laneFieldSources;

        cellMatches.push({
          lane,
          row_label: matchedRowLabel,
          column_header: columnHeader,
          raw_cell_text: rawCellText,
          parsed_value: parsed.parsedValue
        });
      }
      parsedCount += 1;
    }

    for (let lane = 1; lane <= 6; lane += 1) {
      if (!byLane.has(lane)) continue;
      byLane.set(lane, finalizeSupplementLaneRow(byLane.get(lane)));
    }

    tableDiagnostics.push({
      mode: options?.mode || "all",
      headers: table.headers,
      header_row_index: boatHeader.headerRowIndex,
      boat_columns: Object.fromEntries(
        [...boatHeader.laneColumns.entries()].map(([lane, columnIndex]) => [
          String(lane),
          {
            column_index: columnIndex,
            header_text: boatHeader.laneHeaders[lane] || `${lane}号艇`
          }
        ])
      ),
      matched_rows: matchedRows,
      cell_matches: cellMatches,
      parsedCount
    });
  }

  return { byLane, fieldSources, tableDiagnostics };
}

export function parseKyoteiBiyoriAjaxData(payload) {
  const byLane = new Map();
  const fieldSources = {};

  const chokuzenList = Array.isArray(payload?.chokuzen_list) ? payload.chokuzen_list : [];
  const oritenAveList =
    payload?.oriten_ave_list && typeof payload.oriten_ave_list === "object"
      ? payload.oriten_ave_list
      : {};

  for (const row of chokuzenList) {
    const lane = Number(row?.course);
    if (!Number.isInteger(lane) || lane < 1 || lane > 6) continue;
    const playerNo = String(row?.player_no || "");
    const oriten = oritenAveList[playerNo] || null;

    const lapTimeRaw = parseScaledDecimal(row?.shukai, 100);
    const exhibitionTime = parseScaledDecimal(row?.tenji, 100);
    const mawariashi = parseScaledDecimal(row?.mawariashi, 100);
    const chokusen = parseScaledDecimal(row?.chokusen, 100);
    const startParsed = parseStartTimingRaw(row?.start);
    const lapExhibitionScore = computeLapExhibitionScore({ mawariashi, chokusen });
    const entryCourse = Number(row?.shinnyuu);

    const currentCourseField = (baseKey) => {
      if (!oriten) return null;
      const direct = parsePercent(oriten[`${baseKey}_${lane}_ave`]);
      return direct ?? parsePercent(oriten[`${baseKey}_ave`]);
    };

    const laneRow = {
      playerName: normalizeSpace(row?.player_name) || null,
      name: normalizeSpace(row?.player_name) || null,
      racerName: normalizeSpace(row?.player_name) || null,
      displayName: normalizeSpace(row?.player_name) || null,
      player_name: normalizeSpace(row?.player_name) || null,
      registrationNo: toPositiveInteger(row?.player_no),
      registrationNumber: toPositiveInteger(row?.player_no),
      registration_no: toPositiveInteger(row?.player_no),
      playerRegNo: toPositiveInteger(row?.player_no),
      regNo: toPositiveInteger(row?.player_no),
      class: normalizeSpace(row?.kyubetsu) || null,
      grade: normalizeSpace(row?.kyubetsu) || null,
      playerClass: normalizeSpace(row?.kyubetsu) || null,
      weight: parseDecimal(row?.taiju),
      lapTimeRaw,
      lapTime: lapTimeRaw,
      lapExStretch: lapExhibitionScore,
      lapExhibitionScore,
      stretchFootLabel: makeStretchLabel({ mawariashi, chokusen }),
      exhibitionSt: startParsed.numeric,
      exhibitionStRaw: startParsed.raw,
      exhibitionStFlag: startParsed.flag,
      exhibitionStSignedValue: startParsed.signedValue,
      exhibitionTime,
      mawariashi,
      straightTime: chokusen,
      nobiashi: chokusen,
      __nobiashi: chokusen,
      entryCourse: Number.isInteger(entryCourse) ? entryCourse : null,
      laneFirstRate: currentCourseField("shukai_1_1"),
      lane2RenRate: currentCourseField("shukai_1_2"),
      lane3RenRate: currentCourseField("shukai_1_3")
    };

    byLane.set(lane, laneRow);
    fieldSources[lane] = Object.fromEntries(
      Object.entries(laneRow)
        .filter(([, value]) => value !== null && value !== undefined && value !== "")
        .map(([key]) => [
          key,
          key.startsWith("lane")
            ? "request_oriten_kaiseki_custom.oriten_ave_list"
            : "request_oriten_kaiseki_custom.chokuzen_list"
        ])
    );
  }

  const aggregateDiagnostics = mergeAjaxAggregateRows({
    payload,
    byLane,
    fieldSources
  });

  return {
    byLane,
    fieldSources,
    diagnostics: {
      response_keys: Object.keys(payload || {}),
      chokuzen_count: chokuzenList.length,
      oriten_player_count: Object.keys(oritenAveList).length,
      lane_stats_source: "request_oriten_kaiseki_custom.oriten_ave_list",
      parsed_ajax_row_count: aggregateDiagnostics.parsed_ajax_rows_count,
      parsed_ajax_rows_count: aggregateDiagnostics.parsed_ajax_rows_count,
      mapped_field_count: aggregateDiagnostics.mapped_field_count,
      unknown_type_list: aggregateDiagnostics.unknown_type_list,
      aggregate_sources: aggregateDiagnostics.aggregate_sources
    }
  };
}

function parseKyoteiBiyoriIndexIdentities(html, raceNo) {
  const byLane = new Map();
  const fieldSources = {};
  const diagnostics = {
    race_no: Number(raceNo) || null,
    found_table: false,
    parsed_lanes: 0
  };
  if (!html) return { byLane, fieldSources, diagnostics };

  const $ = cheerio.load(html);
  const $container = $(`div.menu_box#${Number(raceNo)}`).first();
  const $table = $container.find("table.table_fixed").first();
  if (!$table.length) return { byLane, fieldSources, diagnostics };

  const rows = $table.find("tr").toArray().map((row) =>
    $(row)
      .children("th,td")
      .toArray()
      .map((cell) => normalizeSpace($(cell).text()))
  );
  if (rows.length < 5) return { byLane, fieldSources, diagnostics };

  diagnostics.found_table = true;
  const registrationRow = rows[1] || [];
  const nameRow = rows[2] || [];
  const classRow = rows[3] || [];
  const branchRow = rows[5] || [];

  for (let lane = 1; lane <= 6; lane += 1) {
    const index = lane - 1;
    const registrationNo = toPositiveInteger(registrationRow[index]);
    const name = normalizeSpace(nameRow[index]) || null;
    const racerClass = normalizeSpace(classRow[index]) || null;
    const branch = normalizeSpace(branchRow[index]) || null;
    const row = {
      lane,
      playerName: name,
      name,
      racerName: name,
      displayName: name,
      player_name: name,
      registrationNo,
      registrationNumber: registrationNo,
      registration_no: registrationNo,
      playerRegNo: registrationNo,
      regNo: registrationNo,
      class: racerClass,
      grade: racerClass,
      playerClass: racerClass,
      branch
    };
    byLane.set(lane, row);
    fieldSources[lane] = Object.fromEntries(
      Object.entries(row)
        .filter(([, value]) => value !== null && value !== undefined && value !== "")
        .map(([key]) => [key, "kyoteibiyori_race_ichiran"])
    );
  }

  diagnostics.parsed_lanes = byLane.size;
  return { byLane, fieldSources, diagnostics };
}

function buildKyoteiBiyoriPlayerIdentities({
  racers = [],
  byLane = new Map(),
  mergedByLane = new Map(),
  identityByLane = new Map(),
  entries = []
} = {}) {
  const racerByLane = new Map(
    (Array.isArray(racers) ? racers : [])
      .map((row) => [Number(row?.lane), row])
      .filter(([lane, row]) => Number.isInteger(lane) && row && typeof row === "object")
  );
  const entryByLane = new Map(
    (Array.isArray(entries) ? entries : [])
      .map((row) => [Number(row?.lane), row])
      .filter(([lane, row]) => Number.isInteger(lane) && row && typeof row === "object")
  );

  function pickStrictIdentityName(rows, numericLane) {
    const candidateKeys = ["playerName", "name", "racerName", "displayName", "player_name"];
    for (const row of rows) {
      if (!row || typeof row !== "object") continue;
      for (const key of candidateKeys) {
        const value = normalizeSpace(row?.[key]);
        if (value && !isLaneFallbackName(value)) return value;
      }
    }
    return Number.isInteger(numericLane) ? `Lane-${numericLane}` : null;
  }

  function pickStrictIdentityRegistrationNo(rows) {
    const candidateKeys = [
      "registrationNo",
      "registrationNumber",
      "registration_no",
      "registration_number",
      "regNo",
      "playerRegNo"
    ];
    for (const row of rows) {
      if (!row || typeof row !== "object") continue;
      for (const key of candidateKeys) {
        const value = toPositiveInteger(row?.[key]);
        if (value !== null) return value;
      }
    }
    return null;
  }

  function pickStrictIdentityClass(rows) {
    const candidateKeys = ["class", "grade", "playerClass"];
    for (const row of rows) {
      if (!row || typeof row !== "object") continue;
      for (const key of candidateKeys) {
        const value = normalizeSpace(row?.[key]);
        if (value) return value;
      }
    }
    return null;
  }

  return [1, 2, 3, 4, 5, 6]
    .map((numericLane) => {
      const sourceRows = [
        identityByLane.get(numericLane),
        mergedByLane.get(numericLane),
        byLane.get(numericLane),
        entryByLane.get(numericLane),
        racerByLane.get(numericLane)
      ].filter((row) => row && typeof row === "object");
      const extra = sourceRows[0] || {};
      const racer = sourceRows[sourceRows.length - 1] || sourceRows[0] || {};
      const mergedRow = {
        ...byLane.get(numericLane),
        ...mergedByLane.get(numericLane),
        ...entryByLane.get(numericLane),
        ...racer,
        ...extra
      };
      const resolvedName = pickStrictIdentityName(sourceRows, numericLane);
      const registrationNo = pickStrictIdentityRegistrationNo([...sourceRows, mergedRow]);
      const racerClass = pickStrictIdentityClass([...sourceRows, mergedRow]);
      const branch = normalizeSpace(extra?.branch || racer?.branch || mergedRow?.branch) || null;
      const age = toPositiveInteger(extra?.age ?? racer?.age ?? mergedRow?.age);
      const weight = parseDecimal(extra?.weight ?? racer?.weight ?? mergedRow?.weight ?? mergedRow?.taiju);
      if (!resolvedName && !registrationNo && !racerClass && !branch && !age && weight === null) {
        return null;
      }
      const identity = { lane: numericLane };
      if (resolvedName) identity.name = resolvedName;
      if (registrationNo) {
        identity.registrationNo = registrationNo;
        identity.registrationNumber = registrationNo;
      }
      if (racerClass) identity.class = racerClass;
      if (branch) identity.branch = branch;
      if (age) identity.age = age;
      if (weight !== null) identity.weight = weight;
      return identity;
    })
    .filter(Boolean);
}

function buildKyoteiBiyoriIdentityRows(...laneMaps) {
  const maps = laneMaps.filter((map) => map instanceof Map);
  return buildKyoteiBiyoriPlayerIdentities({
    byLane: maps[0] || new Map(),
    mergedByLane: maps[1] || new Map(),
    identityByLane: maps[0] || new Map()
  });
}

function mergeIdentityLaneMaps(target, source) {
  if (!(target instanceof Map) || !(source instanceof Map)) return;
  for (const [lane, row] of source.entries()) {
    const numericLane = Number(lane);
    if (!Number.isInteger(numericLane) || numericLane < 1 || numericLane > 6) continue;
    const current = target.get(numericLane) || {};
    const next = { ...current };
    const name = pickPreferredIdentityName(row, numericLane);
    const registrationNo = pickPreferredIdentityRegistrationNo(row);
    const racerClass = pickPreferredIdentityClass(row);
    const branch = normalizeSpace(row?.branch) || null;
    const age = toPositiveInteger(row?.age);
    const weight = parseDecimal(row?.weight ?? row?.taiju);
    if (name && !isLaneFallbackName(name)) {
      next.playerName = name;
      next.name = name;
      next.racerName = name;
      next.displayName = name;
      next.player_name = name;
    }
    if (registrationNo) {
      next.registrationNo = registrationNo;
      next.registrationNumber = registrationNo;
      next.registration_no = registrationNo;
      next.playerRegNo = registrationNo;
      next.regNo = registrationNo;
    }
    if (racerClass) {
      next.class = racerClass;
      next.grade = racerClass;
      next.playerClass = racerClass;
    }
    if (branch) next.branch = branch;
    if (age) next.age = age;
    if (weight !== null) next.weight = weight;
    target.set(numericLane, next);
  }
}

function mergeLaneMaps(target, source, fieldSources, sourceLabel) {
  for (const [lane, row] of source.entries()) {
    const current = target.get(lane) || {};
    const laneFieldSources = fieldSources[lane] || {};
    for (const [key, value] of Object.entries(row || {})) {
      if (value === null || value === undefined || value === "") continue;
      current[key] = value;
      laneFieldSources[key] = laneFieldSources[key] || sourceLabel;
    }
    target.set(lane, current);
    fieldSources[lane] = laneFieldSources;
  }
}

function mergeOriginalExhibitionFields(target, source, fieldSources, sourceLabel) {
  const fields = [
    "lapTime",
    "lapTimeRaw",
    "straightTime",
    "nobiashi",
    "__nobiashi",
    "turnTime",
    "mawariashi",
    "__mawariashi"
  ];
  let mergedFieldCount = 0;
  for (const [lane, row] of source.entries()) {
    const current = target.get(lane) || {};
    const laneFieldSources = fieldSources[lane] || {};
    for (const field of fields) {
      const value = row?.[field];
      if (value === null || value === undefined || value === "") continue;
      if (current[field] !== null && current[field] !== undefined && current[field] !== "") continue;
      current[field] = value;
      laneFieldSources[field] = laneFieldSources[field] || sourceLabel;
      mergedFieldCount += 1;
    }
    target.set(lane, current);
    fieldSources[lane] = laneFieldSources;
  }
  return mergedFieldCount;
}

function mergeFieldDebugMaps(target, source) {
  for (const [lane, row] of Object.entries(source || {})) {
    target[lane] = {
      ...(target[lane] || {}),
      ...(row || {})
    };
  }
}

export function parseKyoteiBiyoriPreRaceData(html, options = {}) {
  const baseSupplement = parseHtmlSupplement(html);
  const headerSupplement = parseHtmlSupplementByJapaneseHeaders(html, options);
  const labelRowSupplement = parseLabelRowsFromText(html, options?.sourceLabel || "race_shusso_html");
  const blockSupplement = parseHtmlSupplementByBlocks(html, options);
  const supplement = parseHtmlSupplementExplicit(html, options);
  const targetFields = new Set([
    "laneFirstRate",
    "lane2RenRate",
    "lane3RenRate",
    "lapTime",
    "lapTimeRaw",
    "exhibitionTime",
    "lapExStretch",
    "lapExhibitionScore",
    "stretchFootLabel",
    "straightTime",
    "turnTime",
    "exhibitionSt",
    "motor2Rate",
    "motor3Rate",
    "__mawariashi",
    "mawariashi",
    "__nobiashi",
    "nobiashi"
  ]);
  const byLane = new Map();
  const fieldSources = {};
  const fieldDebugs = {};

  mergeLaneMaps(byLane, baseSupplement.byLane, fieldSources, options?.sourceLabel || "race_shusso_html");
  for (const [lane, row] of byLane.entries()) {
    const cleaned = { ...(row || {}) };
    for (const field of targetFields) delete cleaned[field];
    byLane.set(lane, cleaned);
  }
  mergeLaneMaps(byLane, headerSupplement.byLane, fieldSources, options?.sourceLabel || "race_shusso_html");
  mergeFieldDebugMaps(fieldDebugs, headerSupplement.fieldDebugs);
  mergeLaneMaps(byLane, labelRowSupplement.byLane, fieldSources, options?.sourceLabel || "race_shusso_html");
  mergeFieldDebugMaps(fieldDebugs, labelRowSupplement.fieldDebugs);
  mergeLaneMaps(byLane, blockSupplement.byLane, fieldSources, options?.sourceLabel || "race_shusso_html");
  mergeFieldDebugMaps(fieldDebugs, blockSupplement.fieldDebugs);
  mergeLaneMaps(byLane, supplement.byLane, fieldSources, options?.sourceLabel || "race_shusso_html");
  mergeFieldDebugMaps(fieldDebugs, supplement.fieldDebugs);

  return {
    byLane,
    fieldSources,
    fieldDebugs,
    tableDiagnostics: [...(baseSupplement.tableDiagnostics || []), ...(headerSupplement.tableDiagnostics || []), ...(labelRowSupplement.tableDiagnostics || []), ...(blockSupplement.tableDiagnostics || []), ...(supplement.tableDiagnostics || [])],
    fieldDiagnostics: buildFieldDiagnostics(byLane, fieldSources)
  };
}

export function normalizeKyoteiBiyoriPreRaceFields(parsed) {
  const normalizedByLane = new Map();
  const fieldSources = parsed?.fieldSources || {};
  const fieldDebugs = parsed?.fieldDebugs || {};
  for (const [lane, row] of parsed?.byLane || []) {
    const laneFieldSources = fieldSources?.[lane] || {};
    const laneDebug = fieldDebugs?.[lane] || {};
    const normalizedRow = normalizeLaneStatAggregateFields({
      playerName: row?.playerName || null,
      fCount: toFiniteNumberOrNull(row?.fCount),
      lapTime: toFiniteNumberOrNull(row?.lapTime),
      lapTimeRaw: toFiniteNumberOrNull(row?.lapTimeRaw),
      lapExStretch: toFiniteNumberOrNull(row?.lapExStretch ?? row?.lapExhibitionScore),
      lapExhibitionScore: toFiniteNumberOrNull(row?.lapExhibitionScore),
      stretchFootLabel: row?.stretchFootLabel || null,
      turnTime: toFiniteNumberOrNull(row?.turnTime ?? row?.mawariashi ?? row?.__mawariashi),
      exhibitionSt: toFiniteNumberOrNull(row?.exhibitionSt),
      exhibitionTime: toFiniteNumberOrNull(row?.exhibitionTime),
      motor2ren: toFiniteNumberOrNull(row?.motor2ren ?? row?.motor2Rate),
      motor3ren: toFiniteNumberOrNull(row?.motor3ren ?? row?.motor3Rate),
      motor2Rate: toFiniteNumberOrNull(row?.motor2Rate),
      motor3Rate: toFiniteNumberOrNull(row?.motor3Rate),
      lane1stScore: toFiniteNumberOrNull(row?.lane1stScore ?? row?.laneFirstRate),
      lane2renScore: toFiniteNumberOrNull(row?.lane2renScore ?? row?.lane2RenRate),
      lane3renScore: toFiniteNumberOrNull(row?.lane3renScore ?? row?.lane3RenRate),
      lane1stAvg: toFiniteNumberOrNull(row?.lane1stAvg ?? row?.laneFirstRate ?? row?.lane1stRate_avg),
      lane2renAvg: toFiniteNumberOrNull(row?.lane2renAvg ?? row?.lane2RenRate ?? row?.lane2renRate_avg),
      lane3renAvg: toFiniteNumberOrNull(row?.lane3renAvg ?? row?.lane3RenRate ?? row?.lane3renRate_avg),
      laneFirstRate: toFiniteNumberOrNull(row?.laneFirstRate),
      lane2RenRate: toFiniteNumberOrNull(row?.lane2RenRate),
      lane3RenRate: toFiniteNumberOrNull(row?.lane3RenRate),
      lane1stRate_raw: row?.lane1stRate_raw || null,
      lane1stRate_season: row?.lane1stRate_season,
      lane1stRate_6m: row?.lane1stRate_6m,
      lane1stRate_3m: row?.lane1stRate_3m,
      lane1stRate_1m: row?.lane1stRate_1m,
      lane1stRate_local: row?.lane1stRate_local,
      lane1stRate_ippansen: row?.lane1stRate_ippansen,
      lane1stRate_sg_g1: row?.lane1stRate_sg_g1,
      lane1stRate_sum: row?.lane1stRate_sum,
      lane1stRate_avg: row?.lane1stRate_avg,
      lane1stRate_weighted: row?.lane1stRate_weighted,
      lane2renRate_raw: row?.lane2renRate_raw || null,
      lane2renRate_season: row?.lane2renRate_season,
      lane2renRate_6m: row?.lane2renRate_6m,
      lane2renRate_3m: row?.lane2renRate_3m,
      lane2renRate_1m: row?.lane2renRate_1m,
      lane2renRate_local: row?.lane2renRate_local,
      lane2renRate_ippansen: row?.lane2renRate_ippansen,
      lane2renRate_sg_g1: row?.lane2renRate_sg_g1,
      lane2renRate_sum: row?.lane2renRate_sum,
      lane2renRate_avg: row?.lane2renRate_avg,
      lane2renRate_weighted: row?.lane2renRate_weighted,
      lane3renRate_raw: row?.lane3renRate_raw || null,
      lane3renRate_season: row?.lane3renRate_season,
      lane3renRate_6m: row?.lane3renRate_6m,
      lane3renRate_3m: row?.lane3renRate_3m,
      lane3renRate_1m: row?.lane3renRate_1m,
      lane3renRate_local: row?.lane3renRate_local,
      lane3renRate_ippansen: row?.lane3renRate_ippansen,
      lane3renRate_sg_g1: row?.lane3renRate_sg_g1,
      lane3renRate_sum: row?.lane3renRate_sum,
      lane3renRate_avg: row?.lane3renRate_avg,
      lane3renRate_weighted: row?.lane3renRate_weighted,
      lane1stDebug: row?.lane1stDebug,
      lane2renDebug: row?.lane2renDebug,
      lane3renDebug: row?.lane3renDebug
    });
    const mawariashi = toFiniteNumberOrNull(row?.turnTime ?? row?.mawariashi ?? row?.__mawariashi);
    const nobiashi = toFiniteNumberOrNull(row?.straightTime ?? row?.nobiashi ?? row?.__nobiashi);
    normalizedRow.motor2Rate = normalizedRow.motor2ren;
    normalizedRow.motor3Rate = normalizedRow.motor3ren;
    normalizedRow.lapExStretch =
      normalizedRow.lapExStretch ??
      computeLapExhibitionScore({ mawariashi, chokusen: nobiashi });
    normalizedRow.lapExhibitionScore =
      toFiniteNumberOrNull(row?.lapExhibitionScore ?? row?.lapExStretch) ??
      normalizedRow.lapExStretch;
    normalizedRow.lane1stScore = normalizedRow.lane1stScore ?? normalizedRow.laneFirstRate;
    normalizedRow.lane2renScore = normalizedRow.lane2renScore ?? normalizedRow.lane2RenRate;
    normalizedRow.lane3renScore = normalizedRow.lane3renScore ?? normalizedRow.lane3RenRate;
    normalizedRow.mawariashi = mawariashi;
    normalizedRow.turnTime = mawariashi;
    normalizedRow.nobiashi = nobiashi;
    normalizedRow.straightTime = nobiashi;
    normalizedRow.stretchFootLabel =
      normalizedRow.stretchFootLabel ||
      makeStretchLabel({ mawariashi, chokusen: nobiashi });
    normalizedRow.exhibitionStRaw = row?.exhibitionStRaw ?? null;
    normalizedRow.exhibitionStFlag = row?.exhibitionStFlag ?? null;
    normalizedRow.exhibitionStSignedValue = toFiniteNumberOrNull(row?.exhibitionStSignedValue);
    normalizedRow.lapRaw = normalizedRow.lapTimeRaw;
    normalizedRow.lapSource =
      laneFieldSources?.lapTimeRaw ||
      laneFieldSources?.lapTime ||
      laneDebug?.lapTime?.sourceLabel ||
      null;
    normalizedRow.lapTimeDetail = buildFieldSourceDetail({
      source: normalizedRow.lapSource || null,
      rowLabel: laneDebug?.lapTime?.metric || laneDebug?.lapTime?.row || "周回",
      raw: laneDebug?.lapTime?.raw ?? normalizedRow.lapRaw ?? null,
      normalized: normalizedRow.lapRaw ?? null,
      status: normalizedRow.lapSource && normalizedRow.lapRaw !== null ? "ok" : laneDebug?.lapTime?.raw ? "broken_pipeline" : "not_published"
    });
    normalizedRow.exhibitionTimeDetail = buildFieldSourceDetail({
      source: laneFieldSources?.exhibitionTime || laneDebug?.exhibitionTime?.sourceLabel || null,
      rowLabel: laneDebug?.exhibitionTime?.metric || laneDebug?.exhibitionTime?.row || "展示",
      raw: laneDebug?.exhibitionTime?.raw ?? row?.exhibitionTime ?? null,
      normalized: normalizedRow.exhibitionTime,
      status: normalizedRow.exhibitionTime !== null ? "ok" : laneDebug?.exhibitionTime?.raw ? "broken_pipeline" : "not_published"
    });
    normalizedRow.turnFootDetail = buildFieldSourceDetail({
      source: laneFieldSources?.turnTime || laneFieldSources?.__mawariashi || laneFieldSources?.mawariashi || laneDebug?.turnTime?.sourceLabel || null,
      rowLabel: laneDebug?.turnTime?.metric || laneDebug?.turnTime?.row || "まわり足",
      raw: mawariashi,
      normalized: mawariashi,
      status: mawariashi !== null ? "ok" : "not_published"
    });
    normalizedRow.straightTimeDetail = buildFieldSourceDetail({
      source: laneFieldSources?.straightTime || laneFieldSources?.__nobiashi || laneFieldSources?.nobiashi || null,
      rowLabel: "直線",
      raw: nobiashi,
      normalized: nobiashi,
      status: nobiashi !== null ? "ok" : "not_published"
    });
    normalizedRow.exhibitionSTDetail = buildFieldSourceDetail({
      source: laneFieldSources?.exhibitionSt || laneDebug?.exhibitionST?.sourceLabel || null,
      rowLabel: laneDebug?.exhibitionST?.metric || laneDebug?.exhibitionST?.row || "ST",
      raw: normalizedRow.exhibitionStRaw,
      normalized: normalizedRow.exhibitionSt,
      status: normalizedRow.exhibitionStRaw ? "ok" : "not_published",
      extra: {
        flag: normalizedRow.exhibitionStFlag,
        value: normalizedRow.exhibitionSt,
        signedValue: normalizedRow.exhibitionStSignedValue
      }
    });
    normalizedRow.laneFirstRate = normalizedRow.lane1stScore;
    normalizedRow.lane2RenRate = normalizedRow.lane2renScore;
    normalizedRow.lane3RenRate = normalizedRow.lane3renScore;
    normalizedRow.lane1stAvg = normalizedRow.lane1stScore;
    normalizedRow.lane2renAvg = normalizedRow.lane2renScore;
    normalizedRow.lane3renAvg = normalizedRow.lane3renScore;
    normalizedByLane.set(Number(lane), normalizedRow);
  }
  return {
    byLane: normalizedByLane,
    fieldSources,
    fieldDebugs,
    tableDiagnostics: parsed?.tableDiagnostics || [],
    fieldDiagnostics: parsed?.fieldDiagnostics || buildFieldDiagnostics(normalizedByLane, fieldSources),
    diagnostics: parsed?.diagnostics || {}
  };
}

export function parseKyoteiBiyoriPreRaceDataWithRenderedFallback({
  staticHtml = "",
  renderedHtml = "",
  mode = "pre_race",
  staticSourceLabel = "kyoteibiyori-static",
  renderedSourceLabel = "kyoteibiyori-rendered"
} = {}) {
  const staticParsed = normalizeKyoteiBiyoriPreRaceFields(
    parseKyoteiBiyoriPreRaceData(staticHtml, { mode, sourceLabel: staticSourceLabel })
  );
  const staticCounts = countOriginalExhibitionFields(staticParsed.byLane);
  const renderedParsed = renderedHtml
    ? normalizeKyoteiBiyoriPreRaceFields(
        parseKyoteiBiyoriPreRaceData(renderedHtml, { mode, sourceLabel: renderedSourceLabel })
      )
    : null;
  const renderedCounts = renderedParsed ? countOriginalExhibitionFields(renderedParsed.byLane) : null;
  const useRendered =
    renderedParsed &&
    originalExhibitionCoverageScore(renderedCounts) > originalExhibitionCoverageScore(staticCounts);
  return {
    source: useRendered
      ? renderedSourceLabel
      : originalExhibitionCoverageScore(staticCounts) > 0
        ? staticSourceLabel
        : "none",
    byLane: useRendered ? renderedParsed.byLane : staticParsed.byLane,
    fieldSources: useRendered ? renderedParsed.fieldSources : staticParsed.fieldSources,
    fieldDebugs: useRendered ? renderedParsed.fieldDebugs : staticParsed.fieldDebugs,
    tableDiagnostics: useRendered ? renderedParsed.tableDiagnostics : staticParsed.tableDiagnostics,
    fieldDiagnostics: useRendered ? renderedParsed.fieldDiagnostics : staticParsed.fieldDiagnostics,
    diagnostics: {
      static_counts: staticCounts,
      rendered_counts: renderedCounts,
      rendered_used: !!useRendered
    }
  };
}

export function mergeKyoteiBiyoriDataIntoRaceContext({ racers, kyoteiBiyori }) {
  const byLane = kyoteiBiyori?.byLane instanceof Map ? kyoteiBiyori.byLane : new Map();
  const fieldSources = kyoteiBiyori?.fieldSources || {};
  const fieldDebugs = kyoteiBiyori?.fieldDebugs || {};
  return (racers || []).map((racer) => {
    try {
      const lane = Number(racer?.lane);
      const extra = byLane.get(lane) || {};
      const predictionFieldMeta = buildPredictionFieldMetaForLane({
        lane,
        extra,
        racer,
        fieldSources,
        fieldDebugs
      });
      const getVerifiedValue = (metaKey, ...candidates) =>
        predictionFieldMeta?.[metaKey]?.is_usable ? firstFiniteValue(...candidates) : null;
      const trustedLane1st = getVerifiedValue(
        "lane1stScore",
        extra?.lane1stScore,
        extra?.lane1stAvg,
        extra?.laneFirstRate,
        racer?.lane1stScore,
        racer?.lane1stAvg,
        racer?.laneFirstRate
      );
      const trustedLane2ren = getVerifiedValue(
        "lane2renScore",
        extra?.lane2renScore,
        extra?.lane2renAvg,
        extra?.lane2RenRate,
        racer?.lane2renScore,
        racer?.lane2renAvg,
        racer?.lane2RenRate
      );
      const trustedLane3ren = getVerifiedValue(
        "lane3renScore",
        extra?.lane3renScore,
        extra?.lane3renAvg,
        extra?.lane3RenRate,
        racer?.lane3renScore,
        racer?.lane3renAvg,
        racer?.lane3RenRate
      );
      const trustedLapTime = getVerifiedValue(
        "lapTime",
        extra?.lapTime
      );
      const displayLapTime = firstFiniteValue(
        extra?.lapTime,
        extra?.lapTimeRaw,
        racer?.lapTime,
        racer?.lapTimeRaw
      );
      const trustedTurnTime = getVerifiedValue(
        "turnTime",
        extra?.turnTime,
        extra?.mawariashi,
        extra?.__mawariashi,
        racer?.turnTime,
        racer?.mawariashi
      );
      const displayTurnTime = firstFiniteValue(
        trustedTurnTime,
        extra?.turnTime,
        extra?.mawariashi,
        extra?.__mawariashi,
        racer?.turnTime,
        racer?.mawariashi
      );
      const displayStraightTime = firstFiniteValue(
        extra?.straightTime,
        extra?.nobiashi,
        extra?.__nobiashi,
        racer?.straightTime,
        racer?.nobiashi
      );
      const resolvedName = pickPreferredIdentityName({
        playerName: extra?.playerName,
        name: extra?.name,
        fallbackPlayerName: racer?.playerName,
        fallbackName: racer?.name,
        racerName: racer?.racerName,
        displayName: racer?.displayName,
        player_name: racer?.player_name
      }, lane);
      const resolvedRegistrationNo =
        pickPreferredIdentityRegistrationNo({
          registrationNo: extra?.registrationNo,
          registrationNumber: extra?.registrationNumber,
          registration_no: extra?.registration_no,
          regNo: extra?.regNo,
          playerRegNo: extra?.playerRegNo,
          fallbackRegistrationNo: racer?.registrationNo,
          fallbackRegistrationNumber: racer?.registrationNumber,
          registration_number: racer?.registration_number,
          registration_no_fallback: racer?.registration_no,
          regNo_fallback: racer?.regNo,
          playerRegNo_fallback: racer?.playerRegNo
        }) ??
        pickPreferredIdentityRegistrationNo(racer);
      const resolvedClass =
        pickPreferredIdentityClass({
          class: extra?.class,
          grade: extra?.grade,
          fallbackClass: racer?.class,
          fallbackGrade: racer?.grade,
          playerClass: racer?.playerClass
        }) || null;
      return {
        ...racer,
        name: resolvedName,
        playerName: !isLaneFallbackName(resolvedName) ? resolvedName : racer?.playerName || null,
        racerName: !isLaneFallbackName(resolvedName) ? resolvedName : racer?.racerName || null,
        displayName: !isLaneFallbackName(resolvedName) ? resolvedName : racer?.displayName || null,
        player_name: !isLaneFallbackName(resolvedName) ? resolvedName : racer?.player_name || null,
        registrationNo: resolvedRegistrationNo,
        registrationNumber: resolvedRegistrationNo,
        registration_no: resolvedRegistrationNo,
        playerRegNo: resolvedRegistrationNo,
        class: resolvedClass,
        grade: resolvedClass,
        playerClass: resolvedClass,
        fHoldCount: extra?.fCount ?? racer?.fHoldCount ?? null,
        kyoteiBiyoriFetched: byLane.has(lane) ? 1 : 0,
        kyoteiBiyoriLapTime: displayLapTime,
        kyoteiBiyoriLapTimeRaw: extra?.lapTimeRaw ?? null,
        kyoteiBiyoriLapSource: extra?.lapSource ?? null,
        kyoteiBiyoriLapTimeDetail: extra?.lapTimeDetail ?? null,
        kyoteiBiyoriLapExhibitionScore: extra?.lapExStretch ?? extra?.lapExhibitionScore ?? null,
        kyoteiBiyoriLapExStretch: extra?.lapExStretch ?? extra?.lapExhibitionScore ?? null,
        kyoteiBiyoriStretchFootLabel: extra?.stretchFootLabel ?? null,
        kyoteiBiyoriMawariashi: displayTurnTime,
        kyoteiBiyoriTurnTime: displayTurnTime,
        kyoteiBiyoriNobiashi: displayStraightTime,
        kyoteiBiyoriStraightTime: displayStraightTime,
        kyoteiBiyoriExhibitionSt: extra?.exhibitionSt ?? null,
        kyoteiBiyoriExST: extra?.exhibitionSt ?? null,
        kyoteiBiyoriExhibitionStRaw: extra?.exhibitionStRaw ?? null,
        kyoteiBiyoriExhibitionStFlag: extra?.exhibitionStFlag ?? null,
        kyoteiBiyoriExhibitionStSignedValue: extra?.exhibitionStSignedValue ?? null,
        kyoteiBiyoriExhibitionSTDetail: extra?.exhibitionSTDetail ?? null,
        kyoteiBiyoriExhibitionTime: extra?.exhibitionTime ?? null,
        kyoteiBiyoriExTime: extra?.exhibitionTime ?? null,
        kyoteiBiyoriExhibitionTimeDetail: extra?.exhibitionTimeDetail ?? null,
        kyoteiBiyoriTurnFootDetail: extra?.turnFootDetail ?? null,
        kyoteiBiyoriStraightTimeDetail: extra?.straightTimeDetail ?? null,
        kyoteiBiyoriMotor2Rate: extra?.motor2ren ?? extra?.motor2Rate ?? null,
        kyoteiBiyoriMotor3Rate: extra?.motor3ren ?? extra?.motor3Rate ?? null,
        lapExStretch: extra?.lapExStretch ?? racer?.lapExStretch ?? null,
        mawariashi: displayTurnTime,
        turnTime: displayTurnTime,
        nobiashi: displayStraightTime,
        straightTime: displayStraightTime,
        exST: racer?.exST ?? racer?.exhibitionSt ?? extra?.exhibitionSt ?? null,
        exTime: racer?.exTime ?? racer?.exhibitionTime ?? extra?.exhibitionTime ?? null,
        motor2ren: extra?.motor2ren ?? extra?.motor2Rate ?? racer?.motor2ren ?? racer?.motor2Rate ?? null,
        motor3ren: extra?.motor3ren ?? extra?.motor3Rate ?? racer?.motor3ren ?? racer?.motor3Rate ?? null,
        lane1stScoreRawParsed: firstFiniteValue(extra?.lane1stScore, extra?.lane1stAvg, extra?.laneFirstRate),
        lane2renScoreRawParsed: firstFiniteValue(extra?.lane2renScore, extra?.lane2renAvg, extra?.lane2RenRate),
        lane3renScoreRawParsed: firstFiniteValue(extra?.lane3renScore, extra?.lane3renAvg, extra?.lane3RenRate),
        lane1stScore: trustedLane1st,
        lane2renScore: trustedLane2ren,
        lane3renScore: trustedLane3ren,
        lane1stAvg: trustedLane1st,
        lane2renAvg: trustedLane2ren,
        lane3renAvg: trustedLane3ren,
        lapTime: displayLapTime,
        lapTimeRaw: extra?.lapTimeRaw ?? null,
        lapRaw: extra?.lapRaw ?? extra?.lapTimeRaw ?? null,
        lapSource: extra?.lapSource ?? null,
        lapTimeDetail: extra?.lapTimeDetail ?? null,
        lapExhibitionScore: extra?.lapExStretch ?? extra?.lapExhibitionScore ?? racer?.lapExhibitionScore ?? null,
        stretchFootLabel: extra?.stretchFootLabel ?? racer?.stretchFootLabel ?? null,
        exhibitionSt: racer?.exhibitionSt ?? racer?.exST ?? extra?.exhibitionSt ?? null,
        exhibitionStRaw: racer?.exhibitionStRaw ?? extra?.exhibitionStRaw ?? null,
        exhibitionStFlag: racer?.exhibitionStFlag ?? extra?.exhibitionStFlag ?? null,
        exhibitionStSignedValue: racer?.exhibitionStSignedValue ?? extra?.exhibitionStSignedValue ?? null,
        exhibitionSTDetail: racer?.exhibitionSTDetail ?? extra?.exhibitionSTDetail ?? null,
        exhibitionTime: racer?.exhibitionTime ?? racer?.exTime ?? extra?.exhibitionTime ?? null,
        exhibitionTimeDetail: racer?.exhibitionTimeDetail ?? extra?.exhibitionTimeDetail ?? null,
        turnFootDetail: extra?.turnFootDetail ?? null,
        straightTimeDetail: extra?.straightTimeDetail ?? null,
        motor2Rate: extra?.motor2ren ?? extra?.motor2Rate ?? racer?.motor2Rate ?? null,
        motor3Rate: extra?.motor3ren ?? extra?.motor3Rate ?? racer?.motor3Rate ?? null,
        laneFirstRate: trustedLane1st,
        lane2RenRate: trustedLane2ren,
        lane3RenRate: trustedLane3ren,
        lane1stRate_raw: extra?.lane1stRate_raw ?? racer?.lane1stRate_raw ?? null,
        lane1stRate_season: extra?.lane1stRate_season ?? racer?.lane1stRate_season ?? null,
        lane1stRate_6m: extra?.lane1stRate_6m ?? racer?.lane1stRate_6m ?? null,
        lane1stRate_3m: extra?.lane1stRate_3m ?? racer?.lane1stRate_3m ?? null,
        lane1stRate_1m: extra?.lane1stRate_1m ?? racer?.lane1stRate_1m ?? null,
        lane1stRate_local: extra?.lane1stRate_local ?? racer?.lane1stRate_local ?? null,
        lane1stRate_ippansen: extra?.lane1stRate_ippansen ?? racer?.lane1stRate_ippansen ?? null,
        lane1stRate_sg_g1: extra?.lane1stRate_sg_g1 ?? racer?.lane1stRate_sg_g1 ?? null,
        lane1stRate_sum: extra?.lane1stRate_sum ?? racer?.lane1stRate_sum ?? null,
        lane1stRate_avg: extra?.lane1stRate_avg ?? racer?.lane1stRate_avg ?? null,
        lane1stRate_weighted: extra?.lane1stRate_weighted ?? racer?.lane1stRate_weighted ?? null,
        lane1stDebug: extra?.lane1stDebug ?? racer?.lane1stDebug ?? null,
        lane2renRate_raw: extra?.lane2renRate_raw ?? racer?.lane2renRate_raw ?? null,
        lane2renRate_season: extra?.lane2renRate_season ?? racer?.lane2renRate_season ?? null,
        lane2renRate_6m: extra?.lane2renRate_6m ?? racer?.lane2renRate_6m ?? null,
        lane2renRate_3m: extra?.lane2renRate_3m ?? racer?.lane2renRate_3m ?? null,
        lane2renRate_1m: extra?.lane2renRate_1m ?? racer?.lane2renRate_1m ?? null,
        lane2renRate_local: extra?.lane2renRate_local ?? racer?.lane2renRate_local ?? null,
        lane2renRate_ippansen: extra?.lane2renRate_ippansen ?? racer?.lane2renRate_ippansen ?? null,
        lane2renRate_sg_g1: extra?.lane2renRate_sg_g1 ?? racer?.lane2renRate_sg_g1 ?? null,
        lane2renRate_sum: extra?.lane2renRate_sum ?? racer?.lane2renRate_sum ?? null,
        lane2renRate_avg: extra?.lane2renRate_avg ?? racer?.lane2renRate_avg ?? null,
        lane2renRate_weighted: extra?.lane2renRate_weighted ?? racer?.lane2renRate_weighted ?? null,
        lane2renDebug: extra?.lane2renDebug ?? racer?.lane2renDebug ?? null,
        lane3renRate_raw: extra?.lane3renRate_raw ?? racer?.lane3renRate_raw ?? null,
        lane3renRate_season: extra?.lane3renRate_season ?? racer?.lane3renRate_season ?? null,
        lane3renRate_6m: extra?.lane3renRate_6m ?? racer?.lane3renRate_6m ?? null,
        lane3renRate_3m: extra?.lane3renRate_3m ?? racer?.lane3renRate_3m ?? null,
        lane3renRate_1m: extra?.lane3renRate_1m ?? racer?.lane3renRate_1m ?? null,
        lane3renRate_local: extra?.lane3renRate_local ?? racer?.lane3renRate_local ?? null,
        lane3renRate_ippansen: extra?.lane3renRate_ippansen ?? racer?.lane3renRate_ippansen ?? null,
        lane3renRate_sg_g1: extra?.lane3renRate_sg_g1 ?? racer?.lane3renRate_sg_g1 ?? null,
        lane3renRate_sum: extra?.lane3renRate_sum ?? racer?.lane3renRate_sum ?? null,
        lane3renRate_avg: extra?.lane3renRate_avg ?? racer?.lane3renRate_avg ?? null,
        lane3renRate_weighted: extra?.lane3renRate_weighted ?? racer?.lane3renRate_weighted ?? null,
        lane3renDebug: extra?.lane3renDebug ?? racer?.lane3renDebug ?? null,
        predictionFieldMeta
      };
    } catch {
      return {
        ...racer,
        kyoteiBiyoriFetched: 0,
        kyoteiBiyoriLapTime: null,
        kyoteiBiyoriLapTimeRaw: null,
        kyoteiBiyoriLapExhibitionScore: null,
        kyoteiBiyoriLapExStretch: null,
        kyoteiBiyoriStretchFootLabel: null,
        kyoteiBiyoriExhibitionSt: null,
        kyoteiBiyoriExhibitionTime: null,
        kyoteiBiyoriTurnTime: null,
        kyoteiBiyoriMotor2Rate: null,
        kyoteiBiyoriMotor3Rate: null,
        lapExStretch: null,
        turnTime: null,
        motor2ren: null,
        motor3ren: null,
        lane1stScore: null,
        lane2renScore: null,
        lane3renScore: null,
        lane1stAvg: null,
        lane2renAvg: null,
        lane3renAvg: null,
        predictionFieldMeta: buildPredictionFieldMetaForLane({
          lane: Number(racer?.lane),
          extra: {},
          racer,
          fieldSources: {},
          fieldDebugs: {}
        })
      };
    }
  });
}

export async function fetchKyoteiBiyoriRaceData({ date, venueId, raceNo, timeoutMs = 45000, artifactCollector = null, forceExhibition = false } = {}) {
  try {
    const startedAt = nowMs();
    const hardTimeoutMs = Math.max(250, Math.min(Number(timeoutMs) || 45000, 45000));
    const deadlineAt = startedAt + hardTimeoutMs;
    const getRemainingTimeoutMs = (capMs = hardTimeoutMs) => {
      const remaining = deadlineAt - nowMs();
      if (remaining <= 0) throw new Error("kyoteibiyori_total_timeout_exceeded");
      return Math.max(250, Math.min(remaining, capMs));
    };

    const indexUrl = buildIndexUrl({ date, venueId, raceNo });
    const diagnostics = {
      timings: {
        total_budget_ms: hardTimeoutMs,
        index_fetch_ms: null,
        ajax_fetch_ms: null,
        ajax_parse_ms: null,
        lane_stats_fetch_ms: null,
        lane_stats_parse_ms: null,
        pre_race_fetch_ms: null,
        pre_race_parse_ms: null,
        rendered_fetch_ms: null,
        rendered_parse_ms: null,
        total_ms: null
      },
      race_list_url: indexUrl,
      extracted_hrefs: {},
      actual_fetch_paths: [],
      fetch_results: {
        race_ichiran: {
          url: indexUrl,
          ok: false,
          has_placeholder: false,
          error: null
        },
        lane_stats_tab: {
          url: null,
          ok: false,
          error: null
        },
        pre_race_tab: {
          url: null,
          ok: false,
          error: null
        },
        request_oriten_kaiseki_custom: {
          endpoint: ORITEN_ENDPOINT,
          referer: null,
          ok: false,
          error: null
        },
        rendered_dom: {
          url: null,
          ok: false,
          error: null
        }
      },
      parse_results: {
        request_oriten_kaiseki_custom: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          required_fields: buildRequiredFieldParseStatus(new Map()),
          diagnostics: {}
        },
        lane_stats_tab: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        },
        pre_race_tab: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        },
        rendered_dom: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        },
        rendered_network: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: [],
          responses: []
        }
      },
      merge_results: {
        merged_lanes: 0,
        entries: []
      },
      playwright_render_debug: null,
      exhibitionFetchRoute: "none",
      playwrightStarted: false,
      playwrightFinished: false,
      playwrightError: null,
      field_sources: {},
      field_diagnostics: buildFieldDiagnostics(new Map(), {}),
      html_contains: {},
      original_exhibition_source: "none",
      original_exhibition_counts: {
        exST: 0,
        exTime: 0,
        lapTime: 0,
        straightTime: 0,
        turnTime: 0,
        motor2Rate: 0
      },
      fallback_reason: null,
      kyoteibiyori_fetch_success: false
    };

    const mergedByLane = new Map();
    const identityByLane = new Map();
    const fieldSources = {};
    const tableDiagnostics = [];
    let lastError = null;
    let indexHtml = "";
    let staticPreRaceHtml = "";
    let staticLaneStatsHtml = "";
    const debugFileBase = buildKyoteiBiyoriDebugFileBase({ date, venueId, raceNo });

    try {
      const indexStartedAt = nowMs();
      indexHtml = await fetchText(indexUrl, getRemainingTimeoutMs(1800));
      diagnostics.timings.index_fetch_ms = elapsedMs(indexStartedAt);
      diagnostics.html_contains.index_raw = logHtmlKeywordPresence("index raw", indexHtml);
      if (artifactCollector && typeof artifactCollector === "object") {
        artifactCollector.raw = {
          ...(artifactCollector.raw || {}),
          kyoteibiyori_index: indexHtml
        };
      }
      diagnostics.fetch_results.race_ichiran.ok = true;
      diagnostics.fetch_results.race_ichiran.has_placeholder = indexHtml.includes("placeholder");
      diagnostics.actual_fetch_paths.push("race_ichiran_shell");
    } catch (error) {
      lastError = error;
      diagnostics.fetch_results.race_ichiran.error = String(error?.message || error);
    }

    const extractedLinks = indexHtml ? extractActualRaceTabLinks(indexHtml, raceNo) : {};
    const parsedIndexIdentities = parseKyoteiBiyoriIndexIdentities(indexHtml, raceNo);
    mergeLaneMaps(mergedByLane, parsedIndexIdentities.byLane, fieldSources, "kyoteibiyori_race_ichiran");
    mergeIdentityLaneMaps(identityByLane, parsedIndexIdentities.byLane);
    diagnostics.extracted_hrefs = extractedLinks;
    diagnostics.parse_results.race_ichiran = {
      ok: parsedIndexIdentities.byLane.size === 6,
      parsed_lanes: parsedIndexIdentities.byLane.size,
      diagnostics: parsedIndexIdentities.diagnostics
    };

    const laneStatsUrl =
      extractedLinks?.laneStatsHref || buildFallbackSliderUrl({ date, venueId, raceNo, slider: 1 });
    const preRaceUrl =
      extractedLinks?.preRaceHref || buildFallbackSliderUrl({ date, venueId, raceNo, slider: 4 });
    if (artifactCollector && typeof artifactCollector === "object") {
      artifactCollector.fetched_urls = {
        ...(artifactCollector.fetched_urls || {}),
        kyoteibiyori: {
          index: indexUrl,
          lane_stats: laneStatsUrl,
          pre_race: preRaceUrl,
          ajax: ORITEN_ENDPOINT
        }
      };
    }
    diagnostics.fetch_results.lane_stats_tab.url = laneStatsUrl;
    diagnostics.fetch_results.pre_race_tab.url = preRaceUrl;

    const supplementalTasks = [];

    try {
      const ajaxTimeoutMs = getRemainingTimeoutMs(1800);
      supplementalTasks.push(
        (async () => {
          const ajaxFetchStartedAt = nowMs();
          const ajaxPayload = await fetchOritenJson({
            date,
            venueId,
            raceNo,
            refererUrl: laneStatsUrl,
            timeoutMs: ajaxTimeoutMs
          });
          diagnostics.timings.ajax_fetch_ms = elapsedMs(ajaxFetchStartedAt);
          if (artifactCollector && typeof artifactCollector === "object") {
            artifactCollector.raw = {
              ...(artifactCollector.raw || {}),
              kyoteibiyori_ajax: ajaxPayload
            };
          }
          const ajaxParseStartedAt = nowMs();
          const parsedAjax = parseKyoteiBiyoriAjaxData(ajaxPayload);
          diagnostics.timings.ajax_parse_ms = elapsedMs(ajaxParseStartedAt);
          mergeLaneMaps(mergedByLane, parsedAjax.byLane, fieldSources, "request_oriten_kaiseki_custom");
          mergeIdentityLaneMaps(identityByLane, parsedAjax.byLane);
          diagnostics.fetch_results.request_oriten_kaiseki_custom.ok = true;
          diagnostics.fetch_results.request_oriten_kaiseki_custom.referer = laneStatsUrl;
          diagnostics.actual_fetch_paths.push("request_oriten_kaiseki_custom(mode=2)");
          diagnostics.parse_results.request_oriten_kaiseki_custom = {
            ok: parsedAjax.byLane.size > 0,
            parsed_lanes: parsedAjax.byLane.size,
            original_exhibition_counts: countOriginalExhibitionFields(parsedAjax.byLane),
            original_exhibition_rows: buildOriginalExhibitionLaneRows(parsedAjax.byLane),
            required_fields: buildRequiredFieldParseStatus(parsedAjax.byLane),
            diagnostics: parsedAjax.diagnostics
          };
        })().catch((error) => {
          lastError = error;
          diagnostics.fetch_results.request_oriten_kaiseki_custom.error = String(error?.message || error);
        })
      );
    } catch (error) {
      lastError = error;
      diagnostics.fetch_results.request_oriten_kaiseki_custom.error = String(error?.message || error);
    }

    for (const [label, url] of [
      ["lane_stats_tab", laneStatsUrl],
      ["pre_race_tab", preRaceUrl]
    ]) {
      try {
        const tabTimeoutMs = getRemainingTimeoutMs(1800);
        supplementalTasks.push(
          (async () => {
            const fetchStartedAt = nowMs();
            const html = await fetchText(url, tabTimeoutMs);
            const fetchDurationMs = elapsedMs(fetchStartedAt);
            if (label === "pre_race_tab") {
              staticPreRaceHtml = html;
              diagnostics.html_contains.pre_race = logHtmlKeywordPresence("pre_race", html);
              diagnostics.html_contains.raw = diagnostics.html_contains.pre_race;
              addNamedHtmlArtifact(artifactCollector, `${debugFileBase}.raw.html`, html);
            } else if (label === "lane_stats_tab") {
              staticLaneStatsHtml = html;
              diagnostics.html_contains.lane_stats = logHtmlKeywordPresence("lane_stats", html);
              diagnostics.html_contains.lane_stats_raw = diagnostics.html_contains.lane_stats;
            }
            if (artifactCollector && typeof artifactCollector === "object") {
              artifactCollector.raw = {
                ...(artifactCollector.raw || {}),
                [label === "lane_stats_tab" ? "kyoteibiyori_lane_stats" : "kyoteibiyori_pre_race"]: html
              };
            }
            if (label === "lane_stats_tab") diagnostics.timings.lane_stats_fetch_ms = fetchDurationMs;
            else diagnostics.timings.pre_race_fetch_ms = fetchDurationMs;
            const parseStartedAt = nowMs();
            const parsedRaw = parseKyoteiBiyoriPreRaceData(html, {
              mode: label === "lane_stats_tab" ? "lane_stats" : "pre_race",
              sourceLabel: label
            });
            if (label === "lane_stats_tab") {
              const preRaceSupplement = parseKyoteiBiyoriPreRaceData(html, {
                mode: "pre_race",
                sourceLabel: "lane_stats_tab"
              });
              mergeLaneMaps(parsedRaw.byLane, preRaceSupplement.byLane, parsedRaw.fieldSources, "lane_stats_tab");
              mergeFieldDebugMaps(parsedRaw.fieldDebugs, preRaceSupplement.fieldDebugs);
              parsedRaw.tableDiagnostics = [
                ...(parsedRaw.tableDiagnostics || []),
                ...(preRaceSupplement.tableDiagnostics || [])
              ];
            }
            const parsed = normalizeKyoteiBiyoriPreRaceFields(
              parsedRaw
            );
            const parseDurationMs = elapsedMs(parseStartedAt);
            if (label === "lane_stats_tab") diagnostics.timings.lane_stats_parse_ms = parseDurationMs;
            else diagnostics.timings.pre_race_parse_ms = parseDurationMs;
            mergeLaneMaps(mergedByLane, parsed.byLane, fieldSources, label);
            mergeIdentityLaneMaps(identityByLane, parsed.byLane);
            tableDiagnostics.push(...(parsed.tableDiagnostics || []));
            diagnostics.actual_fetch_paths.push(`race_shusso_html(${label})`);
            diagnostics.fetch_results[label] = {
              ...(diagnostics.fetch_results[label] || {}),
              url,
              ok: true,
              error: null
            };
            diagnostics.parse_results[label] = {
              ok: parsed.byLane.size > 0,
              parsed_lanes: parsed.byLane.size,
              original_exhibition_counts: countOriginalExhibitionFields(parsed.byLane),
              original_exhibition_rows: buildOriginalExhibitionLaneRows(parsed.byLane),
              populated_fields: parsed.fieldDiagnostics?.populated_fields || [],
              failed_fields: parsed.fieldDiagnostics?.failed_fields || EXPECTED_FIELDS,
              required_fields: buildRequiredFieldParseStatus(parsed.byLane),
              table_diagnostics: parsed.tableDiagnostics || [],
              field_debugs: parsed.fieldDebugs || {}
            };
            if (label === "lane_stats_tab") {
              console.log("[kyoteibiyori] parsed laneStats:", diagnostics.parse_results[label].original_exhibition_rows);
            }
          })().catch((error) => {
            lastError = error;
            diagnostics.fetch_results[label] = {
              ...(diagnostics.fetch_results[label] || {}),
              url,
              ok: false,
              error: String(error?.message || error)
            };
          })
        );
      } catch (error) {
        lastError = error;
        diagnostics.fetch_results[label] = {
          ...(diagnostics.fetch_results[label] || {}),
          url,
          ok: false,
          error: String(error?.message || error)
        };
      }
    }

    await Promise.all(supplementalTasks);

    const staticDebugHtml = staticPreRaceHtml || staticLaneStatsHtml || indexHtml;
    if (staticDebugHtml) {
      diagnostics.html_contains.static = logHtmlKeywordPresence("static", staticDebugHtml);
      addNamedHtmlArtifact(artifactCollector, `${debugFileBase}.raw.html`, staticDebugHtml);
    }

    let renderedDomUsed = false;
    const staticOriginalCounts = countOriginalExhibitionFields(mergedByLane);
    const shouldRender = shouldAttemptRenderedFallback({
      html: staticPreRaceHtml || staticLaneStatsHtml || indexHtml,
      byLane: mergedByLane
    }) || forceExhibition === true;
    diagnostics.original_exhibition_static_counts = staticOriginalCounts;
    diagnostics.rendered_fallback_attempted = false;
    diagnostics.rendered_fallback_reason = shouldRender
      ? "static_missing_lap_straight_or_turn_coverage"
      : null;
    if (shouldRender) {
      diagnostics.rendered_fallback_attempted = true;
      diagnostics.exhibitionFetchRoute = "kyoteibiyori-playwright";
      diagnostics.playwrightStarted = true;
      diagnostics.fetch_results.rendered_dom.url = preRaceUrl;
      console.log(`[kyoteibiyori] playwright start venue=${String(venueId).padStart(2, "0")} race=${Number(raceNo)}`);
      try {
        const renderedFetchStartedAt = nowMs();
        const renderedAttemptErrors = [];
        let renderedPage = null;
        for (let attempt = 1; attempt <= 2; attempt += 1) {
          const renderedTimeoutMs = getRemainingTimeoutMs(45000);
          diagnostics.playwrightAttempts = attempt;
          try {
            renderedPage = await fetchRenderedPageWithPlaywright(preRaceUrl, renderedTimeoutMs, { debugFileBase });
            break;
          } catch (attemptError) {
            const message = String(attemptError?.message || attemptError);
            renderedAttemptErrors.push({ attempt, error: message });
            diagnostics.playwrightAttemptErrors = renderedAttemptErrors;
            diagnostics.fetch_results.rendered_dom.error = message;
            if (attempt >= 2 || deadlineAt - nowMs() <= 1000) {
              throw attemptError;
            }
          }
        }
        const renderedHtml = renderedPage?.html || "";
        diagnostics.timings.rendered_fetch_ms = elapsedMs(renderedFetchStartedAt);
        diagnostics.playwright_render_debug = renderedPage?.debug || null;
        diagnostics.playwrightFinished = true;
        diagnostics.playwrightError = null;
        console.log(`[kyoteibiyori] playwright url=${renderedPage?.debug?.page_url_after_click || renderedPage?.debug?.page_url_before_click || ""}`);
        console.log(`[kyoteibiyori] playwright title=${renderedPage?.debug?.page_title_after_click || renderedPage?.debug?.page_title_before_click || ""}`);
        console.log(`[kyoteibiyori] playwright html length=${renderedHtml.length}`);
        diagnostics.html_contains.rendered = logHtmlKeywordPresence("rendered", renderedHtml);
        logRenderedContainsForDebug(diagnostics.html_contains.rendered);
        addNamedHtmlArtifact(artifactCollector, `${debugFileBase}.rendered.html`, renderedHtml);
        addNamedBinaryArtifact(artifactCollector, `${debugFileBase}.rendered.png`, renderedPage?.screenshot || null);
        if (artifactCollector && typeof artifactCollector === "object") {
          artifactCollector.raw = {
            ...(artifactCollector.raw || {}),
            kyoteibiyori_rendered: renderedHtml
          };
        }
        const renderedParseStartedAt = nowMs();
        const renderedParsed = normalizeKyoteiBiyoriPreRaceFields(
          parseKyoteiBiyoriPreRaceData(renderedHtml, {
            mode: "pre_race",
            sourceLabel: "kyoteibiyori-rendered"
          })
        );
        diagnostics.timings.rendered_parse_ms = elapsedMs(renderedParseStartedAt);
        const renderedCounts = countOriginalExhibitionFields(renderedParsed.byLane);
        diagnostics.original_exhibition_rendered_counts = renderedCounts;
        diagnostics.fetch_results.rendered_dom.ok = true;
        diagnostics.fetch_results.rendered_dom.screenshot_captured = !!renderedPage?.screenshot;
        diagnostics.parse_results.rendered_dom = {
          ok: renderedParsed.byLane.size > 0,
          parsed_lanes: renderedParsed.byLane.size,
          original_exhibition_counts: renderedCounts,
          original_exhibition_rows: buildOriginalExhibitionLaneRows(renderedParsed.byLane),
          populated_fields: renderedParsed.fieldDiagnostics?.populated_fields || [],
          failed_fields: renderedParsed.fieldDiagnostics?.failed_fields || EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(renderedParsed.byLane),
          table_diagnostics: renderedParsed.tableDiagnostics || [],
          field_debugs: renderedParsed.fieldDebugs || {}
        };
        console.log("[kyoteibiyori] parsed laneStats:", diagnostics.parse_results.rendered_dom.original_exhibition_rows);
        const networkParsed = parseNetworkOriginalExhibitionResponses(renderedPage?.networkBodies || []);
        const networkCounts = countOriginalExhibitionFields(networkParsed.byLane);
        diagnostics.parse_results.rendered_network = {
          ok: networkParsed.byLane.size > 0,
          parsed_lanes: networkParsed.byLane.size,
          original_exhibition_counts: networkCounts,
          original_exhibition_rows: buildOriginalExhibitionLaneRows(networkParsed.byLane),
          populated_fields: networkParsed.fieldDiagnostics?.populated_fields || [],
          failed_fields: networkParsed.fieldDiagnostics?.failed_fields || EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(networkParsed.byLane),
          table_diagnostics: networkParsed.tableDiagnostics || [],
          field_debugs: networkParsed.fieldDebugs || {},
          responses: networkParsed.responseDiagnostics || []
        };
        if (originalExhibitionCoverageScore(networkCounts) > 0) {
          const mergedNetworkFieldCount = mergeOriginalExhibitionFields(
            mergedByLane,
            networkParsed.byLane,
            fieldSources,
            "kyoteibiyori-network"
          );
          if (mergedNetworkFieldCount > 0) {
            tableDiagnostics.push(...(networkParsed.tableDiagnostics || []));
            diagnostics.actual_fetch_paths.push("kyoteibiyori_network_response_original_exhibition");
            diagnostics.merge_results.network_original_exhibition_fields_merged = mergedNetworkFieldCount;
            renderedDomUsed = true;
          }
        }
        if (originalExhibitionCoverageScore(renderedCounts) > originalExhibitionCoverageScore(staticOriginalCounts)) {
          mergeLaneMaps(mergedByLane, renderedParsed.byLane, fieldSources, "kyoteibiyori-rendered");
          mergeIdentityLaneMaps(identityByLane, renderedParsed.byLane);
          tableDiagnostics.push(...(renderedParsed.tableDiagnostics || []));
          diagnostics.actual_fetch_paths.push("kyoteibiyori_rendered_dom");
          renderedDomUsed = true;
        } else {
          const mergedOriginalFieldCount = mergeOriginalExhibitionFields(
            mergedByLane,
            renderedParsed.byLane,
            fieldSources,
            "kyoteibiyori-rendered"
          );
          if (mergedOriginalFieldCount > 0) {
            mergeIdentityLaneMaps(identityByLane, renderedParsed.byLane);
            tableDiagnostics.push(...(renderedParsed.tableDiagnostics || []));
            diagnostics.actual_fetch_paths.push("kyoteibiyori_rendered_dom_partial_original_exhibition");
            diagnostics.merge_results.rendered_original_exhibition_fields_merged = mergedOriginalFieldCount;
            renderedDomUsed = true;
          }
        }
      } catch (error) {
        lastError = error;
        diagnostics.playwrightFinished = false;
        diagnostics.playwrightError = String(error?.message || error);
        console.log(`[kyoteibiyori] playwright error=${diagnostics.playwrightError}`);
        diagnostics.fetch_results.rendered_dom.error = String(error?.message || error);
      }
    }

    const fieldDiagnostics = buildFieldDiagnostics(mergedByLane, fieldSources);
    const laneStatsReady = fieldDiagnostics.per_lane.some((row) => row.populated_fields.includes("laneFirstRate"));
    const lapTimeReady = fieldDiagnostics.per_lane.some((row) => row.populated_fields.includes("lapTimeRaw"));
    const originalExhibitionCounts = countOriginalExhibitionFields(mergedByLane);
    const originalExhibitionSource =
      renderedDomUsed
        ? "kyoteibiyori-rendered"
        : originalExhibitionCoverageScore(originalExhibitionCounts) > 0
          ? "kyoteibiyori-static"
          : "none";
    if (diagnostics.exhibitionFetchRoute !== "kyoteibiyori-playwright") {
      diagnostics.exhibitionFetchRoute =
        originalExhibitionCoverageScore(originalExhibitionCounts) > 0
          ? "kyoteibiyori-static"
          : ok
            ? "fallback"
            : "none";
    }
    const requiredFieldStatus = buildRequiredFieldParseStatus(mergedByLane);
    const criticalFieldsReady = Object.entries(requiredFieldStatus)
      .filter(([field]) => field !== "motor3ren")
      .filter(([, ready]) => !!ready);
    const ok = mergedByLane.size > 0 && criticalFieldsReady.length > 0;
    const fallbackReason =
      ok
        ? null
        : lastError
          ? String(lastError.message || lastError)
          : "kyoteibiyori returned no usable prediction-critical lane-stat or pre-race fields";
    diagnostics.merge_results.merged_lanes = mergedByLane.size;
    diagnostics.merge_results.original_exhibition_counts = originalExhibitionCounts;
    diagnostics.merge_results.parsed_counts = {
      lapTime: originalExhibitionCounts.lapTime,
      straightTime: originalExhibitionCounts.straightTime,
      turnTime: originalExhibitionCounts.turnTime
    };
    diagnostics.merge_results.entries = buildMergedEntryDebugRows(mergedByLane);
    console.log("[kyoteibiyori] merged entries:", diagnostics.merge_results.entries);
    logOriginalExhibitionPipelineDebug(diagnostics);
    diagnostics.field_sources = fieldSources;
    diagnostics.field_diagnostics = fieldDiagnostics;
    diagnostics.required_field_status = requiredFieldStatus;
    diagnostics.critical_fields_ready = criticalFieldsReady.map(([field]) => field);
    diagnostics.original_exhibition_source = originalExhibitionSource;
    diagnostics.original_exhibition_counts = originalExhibitionCounts;
    diagnostics.partial_prediction_data_available = mergedByLane.size > 0 && criticalFieldsReady.length > 0;
    diagnostics.lane_stats_ready = laneStatsReady;
    diagnostics.lap_time_ready = lapTimeReady;
    diagnostics.fallback_reason = fallbackReason;
    diagnostics.kyoteibiyori_fetch_success = ok;
    diagnostics.timings.total_ms = elapsedMs(startedAt);
    mergeIdentityLaneMaps(identityByLane, mergedByLane);
    const playerIdentityRows = [...mergedByLane.entries()].map(([lane, racer]) => ({ lane: Number(lane), ...(racer || {}) }));
    const playerIdentities = buildKyoteiBiyoriPlayerIdentities({
      racers: playerIdentityRows,
      byLane: mergedByLane,
      mergedByLane,
      identityByLane,
      entries: playerIdentityRows
    });

    return {
      ok,
      url: indexUrl,
      triedUrls: [indexUrl, laneStatsUrl, preRaceUrl],
      byLane: mergedByLane,
      fieldDebugs: {
        lane_stats_tab: diagnostics.parse_results?.lane_stats_tab?.field_debugs || {},
        pre_race_tab: diagnostics.parse_results?.pre_race_tab?.field_debugs || {},
        rendered_dom: diagnostics.parse_results?.rendered_dom?.field_debugs || {}
      },
      tableDiagnostics,
      fieldDiagnostics,
      fieldSources,
      playerIdentities,
      racers: playerIdentities,
      entries: playerIdentities,
      fallbackUsed: !ok,
      fallbackReason,
      diagnostics,
      error: ok ? null : fallbackReason
    };
  } catch (error) {
    const hardTimeoutMs = Math.max(250, Math.min(Number(timeoutMs) || 45000, 45000));
    const emptyDiagnostics = {
      timings: {
        total_budget_ms: hardTimeoutMs,
        index_fetch_ms: null,
        ajax_fetch_ms: null,
        ajax_parse_ms: null,
        lane_stats_fetch_ms: null,
        lane_stats_parse_ms: null,
        pre_race_fetch_ms: null,
        pre_race_parse_ms: null,
        rendered_fetch_ms: null,
        rendered_parse_ms: null,
        total_ms: null
      },
      race_list_url: null,
      extracted_hrefs: {},
      actual_fetch_paths: [],
      fetch_results: {
        race_ichiran: {
          url: null,
          ok: false,
          has_placeholder: false,
          error: null
        },
        lane_stats_tab: {
          url: null,
          ok: false,
          error: null
        },
        pre_race_tab: {
          url: null,
          ok: false,
          error: null
        },
        request_oriten_kaiseki_custom: {
          endpoint: ORITEN_ENDPOINT,
          referer: null,
          ok: false,
          error: null
        },
        rendered_dom: {
          url: null,
          ok: false,
          error: null
        }
      },
      parse_results: {
        request_oriten_kaiseki_custom: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          required_fields: buildRequiredFieldParseStatus(new Map()),
          diagnostics: {}
        },
        lane_stats_tab: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        },
        pre_race_tab: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        },
        rendered_dom: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        },
        rendered_network: {
          ok: false,
          parsed_lanes: 0,
          original_exhibition_counts: countOriginalExhibitionFields(new Map()),
          original_exhibition_rows: [],
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: [],
          responses: []
        }
      },
      merge_results: {
        merged_lanes: 0,
        entries: []
      },
      playwright_render_debug: null,
      exhibitionFetchRoute: "none",
      playwrightStarted: false,
      playwrightFinished: false,
      playwrightError: null,
      field_sources: {},
      field_diagnostics: buildFieldDiagnostics(new Map(), {}),
      html_contains: {},
      original_exhibition_source: "none",
      original_exhibition_counts: {
        exST: 0,
        exTime: 0,
        lapTime: 0,
        straightTime: 0,
        turnTime: 0,
        motor2Rate: 0
      },
      fallback_reason: String(error?.message || error),
      kyoteibiyori_fetch_success: false
    };
    return {
      ok: false,
      url: null,
      triedUrls: [],
      byLane: new Map(),
      fieldDebugs: {},
      tableDiagnostics: [],
      fieldDiagnostics: buildFieldDiagnostics(new Map(), {}),
      fieldSources: {},
      playerIdentities: [],
      racers: [],
      entries: [],
      fallbackUsed: true,
      fallbackReason: String(error?.message || error),
      diagnostics: {
        ...emptyDiagnostics,
        fatal_error: String(error?.message || error)
      },
      error: String(error?.message || error)
    };
  }
}
