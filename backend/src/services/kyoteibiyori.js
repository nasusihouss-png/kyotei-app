import axios from "axios";
import * as cheerio from "cheerio";

const KYOTEI_BIYORI_BASE = "https://kyoteibiyori.com";
const ORITEN_ENDPOINT = `${KYOTEI_BIYORI_BASE}/request/request_oriten_kaiseki_custom.php`;
const EXPECTED_FIELDS = [
  "playerName",
  "fCount",
  "lapTime",
  "lapTimeRaw",
  "lapExhibitionScore",
  "stretchFootLabel",
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

function makePredictionFieldMeta({ field, value, source, debugEntry, required = false, minConfidence = 0.6 }) {
  const hasValue = value !== null && value !== undefined && Number.isFinite(Number(value));
  if (!hasValue) {
    return {
      value: null,
      source: source || null,
      confidence: 0,
      is_usable: false,
      required,
      reason: "missing",
      raw_cell_text: debugEntry?.raw ?? null,
      source_section: debugEntry?.section ?? null,
      source_row_label: debugEntry?.metric ?? debugEntry?.row ?? null,
      source_period_label: debugEntry?.period ?? null,
      source_boat_column: debugEntry?.boatColumn ?? debugEntry?.column ?? null
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
    required: PREDICTION_FIELD_META_CONFIG[field]?.required,
    minConfidence: PREDICTION_FIELD_META_CONFIG[field]?.minConfidence
  });

  return {
    lapTime: getFieldMeta("lapTime", {
      value: extra?.lapTime ?? racer?.lapTime ?? null,
      source: laneSources.lapTimeRaw || laneSources.lapTime || (Number.isFinite(Number(racer?.lapTime)) ? "boatrace_racelist" : null),
      debugEntry: laneDebug?.lapTime || null
    }),
    exhibitionST: getFieldMeta("exhibitionST", {
      value: extra?.exhibitionSt ?? racer?.exhibitionSt ?? null,
      source: laneSources.exhibitionSt || (Number.isFinite(Number(racer?.exhibitionSt)) ? "boatrace_racelist" : null),
      debugEntry: laneDebug?.exhibitionST || null
    }),
    exhibitionTime: getFieldMeta("exhibitionTime", {
      value: extra?.exhibitionTime ?? racer?.exhibitionTime ?? null,
      source: laneSources.exhibitionTime || (Number.isFinite(Number(racer?.exhibitionTime)) ? "boatrace_racelist" : null),
      debugEntry: laneDebug?.exhibitionTime || null
    }),
    lapExStretch: getFieldMeta("lapExStretch", {
      value: extra?.lapExStretch ?? extra?.lapExhibitionScore ?? racer?.lapExStretch ?? racer?.lapExhibitionScore ?? null,
      source: laneSources.lapExStretch || laneSources.lapExhibitionScore || null,
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
  if (!raw) return { raw: null, type: "missing", numeric: null };
  const normalized = normalizeDigits(raw).replace(/\s+/g, "").toUpperCase();
  if (/^F\.?\d+/.test(normalized)) return { raw, type: "flying", numeric: null };
  if (/^L\.?\d+/.test(normalized)) return { raw, type: "late", numeric: null };
  const numeric = parseDecimal(normalized);
  return { raw, type: numeric === null ? "unknown" : "normal", numeric };
}

function parseScaledDecimal(value, divisor = 100) {
  const n = toNumber(value);
  if (n === null) return null;
  return Number((n / divisor).toFixed(2));
}

function normalizeLapTimeForModel(rawLapTime) {
  if (!Number.isFinite(Number(rawLapTime))) return null;
  return Number((Number(rawLapTime) - 29.5).toFixed(2));
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
  mawariashi: "\u5468\u308a\u8db3",
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
  motor2: ["\u7e5d\uff62\u7e5d\uff7c\u7e67\uff7f\u7e5d\uff7c2\u9a3e\uff63\u9087\u30fb"],
  motor3: ["\u7e5d\uff62\u7e5d\uff7c\u7e67\uff7f\u7e5d\uff7c3\u9a3e\uff63\u9087\u30fb"],
  mawariashi: ["\u873b\uff68\u7e67\u9858\uff76\uff73"],
  nobiashi: ["\u83a8\uff78\u7e3a\uff73\u96dc\uff73"],
  lapTime: ["\u873b\uff68\u8757\u30fb"],
  exhibition: ["\u87bb\u6155\uff64\uff7a"],
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
  if (matchesLabel(text, JAPANESE_LABELS.motor2, LABEL_ALIASES.motor2)) return JAPANESE_LABELS.motor2;
  if (matchesLabel(text, JAPANESE_LABELS.motor3, LABEL_ALIASES.motor3)) return JAPANESE_LABELS.motor3;
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
  if (matchesLabel(text, JAPANESE_LABELS.motor2, LABEL_ALIASES.motor2) || (matchesLabel(text, JAPANESE_LABELS.motorSection) && text.includes("2"))) return JAPANESE_LABELS.motor2;
  if (matchesLabel(text, JAPANESE_LABELS.motor3, LABEL_ALIASES.motor3) || (matchesLabel(text, JAPANESE_LABELS.motorSection) && text.includes("3"))) return JAPANESE_LABELS.motor3;
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
    if (metric === JAPANESE_LABELS.lapTime) return { field: "lapTimeRaw", section: resolvedSection, row: JAPANESE_LABELS.lapTime };
    if (metric === JAPANESE_LABELS.st) return { field: "exhibitionSt", section: resolvedSection, row: JAPANESE_LABELS.st };
    if (metric === JAPANESE_LABELS.exhibition) return { field: "exhibitionTime", section: resolvedSection, row: JAPANESE_LABELS.exhibition };
    if (metric === JAPANESE_LABELS.mawariashi) return { field: "mawariashi", section: resolvedSection, row: JAPANESE_LABELS.mawariashi };
    if (metric === JAPANESE_LABELS.nobiashi) return { field: "nobiashi", section: resolvedSection, row: JAPANESE_LABELS.nobiashi };
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
        lapTime: lapTimeRaw !== null ? normalizeLapTimeForModel(lapTimeRaw) : null
      },
      value: lapTimeRaw
    };
  }

  if (field === "exhibitionSt") {
    const parsed = parseStartTimingRaw(rawText);
    const value = parsed.type === "normal" ? parsed.numeric : null;
    return {
      fields: { exhibitionSt: value },
      value
    };
  }

  if (field === "exhibitionTime") {
    const value = parseDecimal(rawText);
    return {
      fields: {
        exhibitionTime: value,
        lapExStretch: value,
        lapExhibitionScore: value
      },
      value
    };
  }

  if (field === "mawariashi") {
    const value = parseDecimal(rawText);
    return {
      fields: { __mawariashi: value },
      value
    };
  }

  if (field === "nobiashi") {
    const value = parseDecimal(rawText);
    return {
      fields: { __nobiashi: value },
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
          if (target.field === "exhibitionTime") {
            setLaneFieldDebug(fieldDebugs, lane, "lapExStretch", debugEntry);
          }
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
  if (/^(?:周り足|回り足)$/.test(normalized)) return "周り足";
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
        lapTime: lapTimeRaw !== null ? normalizeLapTimeForModel(lapTimeRaw) : null
      },
      parsedValue: lapTimeRaw
    };
  }
  if (rowLabel === "ST") {
    const parsed = parseStartTimingRaw(rawText);
    return {
      fields: {
        exhibitionSt: parsed.type === "normal" ? parsed.numeric : null
      },
      parsedValue: parsed.type === "normal" ? parsed.numeric : null
    };
  }
  if (rowLabel === "展示") {
    const exhibitionTime = parseDecimal(rawText);
    return {
      fields: {
        exhibitionTime,
        lapExStretch: exhibitionTime,
        lapExhibitionScore: exhibitionTime
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
      fields: { __chokusen: chokusen },
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

      if (next.exhibitionTime !== null) {
        next.lapExStretch = next.exhibitionTime;
        next.lapExhibitionScore = next.exhibitionTime;
      }
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
    const lapExhibitionScore = exhibitionTime;
    const entryCourse = Number(row?.shinnyuu);

    const currentCourseField = (baseKey) => {
      if (!oriten) return null;
      const direct = parsePercent(oriten[`${baseKey}_${lane}_ave`]);
      return direct ?? parsePercent(oriten[`${baseKey}_ave`]);
    };

    const laneRow = {
      playerName: normalizeSpace(row?.player_name) || null,
      lapTimeRaw,
      lapTime: normalizeLapTimeForModel(lapTimeRaw),
      lapExStretch: lapExhibitionScore,
      lapExhibitionScore,
      stretchFootLabel: null,
      exhibitionSt: startParsed.type === "normal" ? startParsed.numeric : null,
      exhibitionTime,
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

  return {
    byLane,
    fieldSources,
    diagnostics: {
      response_keys: Object.keys(payload || {}),
      chokuzen_count: chokuzenList.length,
      oriten_player_count: Object.keys(oritenAveList).length,
      lane_stats_source: "request_oriten_kaiseki_custom.oriten_ave_list"
    }
  };
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

export function parseKyoteiBiyoriPreRaceData(html, options = {}) {
  const baseSupplement = parseHtmlSupplement(html);
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
    "exhibitionSt",
    "motor2Rate",
    "motor3Rate",
    "__mawariashi",
    "__nobiashi"
  ]);
  const byLane = new Map();
  const fieldSources = {};

  mergeLaneMaps(byLane, baseSupplement.byLane, fieldSources, options?.sourceLabel || "race_shusso_html");
  for (const [lane, row] of byLane.entries()) {
    const cleaned = { ...(row || {}) };
    for (const field of targetFields) delete cleaned[field];
    byLane.set(lane, cleaned);
  }
  mergeLaneMaps(byLane, supplement.byLane, fieldSources, options?.sourceLabel || "race_shusso_html");

  return {
    byLane,
    fieldSources,
    fieldDebugs: supplement.fieldDebugs,
    tableDiagnostics: [...(baseSupplement.tableDiagnostics || []), ...(supplement.tableDiagnostics || [])],
    fieldDiagnostics: buildFieldDiagnostics(byLane, fieldSources)
  };
}

export function normalizeKyoteiBiyoriPreRaceFields(parsed) {
  const normalizedByLane = new Map();
  const fieldSources = parsed?.fieldSources || {};
  for (const [lane, row] of parsed?.byLane || []) {
    const normalizedRow = normalizeLaneStatAggregateFields({
      playerName: row?.playerName || null,
      fCount: toFiniteNumberOrNull(row?.fCount),
      lapTime: toFiniteNumberOrNull(row?.lapTime),
      lapTimeRaw: toFiniteNumberOrNull(row?.lapTimeRaw),
      lapExStretch: toFiniteNumberOrNull(row?.lapExStretch ?? row?.lapExhibitionScore),
      lapExhibitionScore: toFiniteNumberOrNull(row?.lapExhibitionScore),
      stretchFootLabel: row?.stretchFootLabel || null,
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
    normalizedRow.motor2Rate = normalizedRow.motor2ren;
    normalizedRow.motor3Rate = normalizedRow.motor3ren;
    normalizedRow.lapExhibitionScore = normalizedRow.lapExStretch;
    normalizedRow.lane1stScore = normalizedRow.lane1stScore ?? normalizedRow.laneFirstRate;
    normalizedRow.lane2renScore = normalizedRow.lane2renScore ?? normalizedRow.lane2RenRate;
    normalizedRow.lane3renScore = normalizedRow.lane3renScore ?? normalizedRow.lane3RenRate;
    normalizedRow.mawariashi = toFiniteNumberOrNull(row?.mawariashi ?? row?.__mawariashi);
    normalizedRow.nobiashi = toFiniteNumberOrNull(row?.nobiashi ?? row?.__nobiashi);
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
    fieldDebugs: parsed?.fieldDebugs || {},
    tableDiagnostics: parsed?.tableDiagnostics || [],
    fieldDiagnostics: parsed?.fieldDiagnostics || buildFieldDiagnostics(normalizedByLane, fieldSources),
    diagnostics: parsed?.diagnostics || {}
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
      return {
        ...racer,
        name: extra?.playerName || racer?.name || null,
        fHoldCount: extra?.fCount ?? racer?.fHoldCount ?? null,
        kyoteiBiyoriFetched: byLane.has(lane) ? 1 : 0,
        kyoteiBiyoriLapTime: extra?.lapTime ?? null,
        kyoteiBiyoriLapTimeRaw: extra?.lapTimeRaw ?? null,
        kyoteiBiyoriLapExhibitionScore: extra?.lapExStretch ?? extra?.lapExhibitionScore ?? null,
        kyoteiBiyoriLapExStretch: extra?.lapExStretch ?? extra?.lapExhibitionScore ?? null,
        kyoteiBiyoriStretchFootLabel: extra?.stretchFootLabel ?? null,
        kyoteiBiyoriMawariashi: extra?.mawariashi ?? null,
        kyoteiBiyoriNobiashi: extra?.nobiashi ?? null,
        kyoteiBiyoriExhibitionSt: extra?.exhibitionSt ?? null,
        kyoteiBiyoriExhibitionTime: extra?.exhibitionTime ?? null,
        kyoteiBiyoriMotor2Rate: extra?.motor2ren ?? extra?.motor2Rate ?? null,
        kyoteiBiyoriMotor3Rate: extra?.motor3ren ?? extra?.motor3Rate ?? null,
        lapExStretch: extra?.lapExStretch ?? racer?.lapExStretch ?? null,
        mawariashi: extra?.mawariashi ?? racer?.mawariashi ?? null,
        nobiashi: extra?.nobiashi ?? racer?.nobiashi ?? null,
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
        lapTime: extra?.lapTime ?? racer?.lapTime ?? null,
        lapTimeRaw: extra?.lapTimeRaw ?? racer?.lapTimeRaw ?? null,
        lapExhibitionScore: extra?.lapExStretch ?? extra?.lapExhibitionScore ?? racer?.lapExhibitionScore ?? null,
        stretchFootLabel: extra?.stretchFootLabel ?? racer?.stretchFootLabel ?? null,
        exhibitionSt: extra?.exhibitionSt ?? racer?.exhibitionSt ?? null,
        exhibitionTime: extra?.exhibitionTime ?? racer?.exhibitionTime ?? null,
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
        kyoteiBiyoriMotor2Rate: null,
        kyoteiBiyoriMotor3Rate: null,
        lapExStretch: null,
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

export async function fetchKyoteiBiyoriRaceData({ date, venueId, raceNo, timeoutMs = 12000 }) {
  try {
    const startedAt = nowMs();
    const hardTimeoutMs = Math.max(250, Math.min(Number(timeoutMs) || 12000, 4000));
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
        }
      },
      parse_results: {
        request_oriten_kaiseki_custom: {
          ok: false,
          parsed_lanes: 0,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          diagnostics: {}
        },
        lane_stats_tab: {
          ok: false,
          parsed_lanes: 0,
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        },
        pre_race_tab: {
          ok: false,
          parsed_lanes: 0,
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        }
      },
      merge_results: {
        merged_lanes: 0
      },
      field_sources: {},
      field_diagnostics: buildFieldDiagnostics(new Map(), {}),
      fallback_reason: null,
      kyoteibiyori_fetch_success: false
    };

    const mergedByLane = new Map();
    const fieldSources = {};
    const tableDiagnostics = [];
    let lastError = null;
    let indexHtml = "";

    try {
      const indexStartedAt = nowMs();
      indexHtml = await fetchText(indexUrl, getRemainingTimeoutMs(1800));
      diagnostics.timings.index_fetch_ms = elapsedMs(indexStartedAt);
      diagnostics.fetch_results.race_ichiran.ok = true;
      diagnostics.fetch_results.race_ichiran.has_placeholder = indexHtml.includes("placeholder");
      diagnostics.actual_fetch_paths.push("race_ichiran_shell");
    } catch (error) {
      lastError = error;
      diagnostics.fetch_results.race_ichiran.error = String(error?.message || error);
    }

    const extractedLinks = indexHtml ? extractActualRaceTabLinks(indexHtml, raceNo) : {};
    diagnostics.extracted_hrefs = extractedLinks;

    const laneStatsUrl =
      extractedLinks?.laneStatsHref || buildFallbackSliderUrl({ date, venueId, raceNo, slider: 1 });
    const preRaceUrl =
      extractedLinks?.preRaceHref || buildFallbackSliderUrl({ date, venueId, raceNo, slider: 4 });
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
          const ajaxParseStartedAt = nowMs();
          const parsedAjax = parseKyoteiBiyoriAjaxData(ajaxPayload);
          diagnostics.timings.ajax_parse_ms = elapsedMs(ajaxParseStartedAt);
          mergeLaneMaps(mergedByLane, parsedAjax.byLane, fieldSources, "request_oriten_kaiseki_custom");
          diagnostics.fetch_results.request_oriten_kaiseki_custom.ok = true;
          diagnostics.fetch_results.request_oriten_kaiseki_custom.referer = laneStatsUrl;
          diagnostics.actual_fetch_paths.push("request_oriten_kaiseki_custom(mode=2)");
          diagnostics.parse_results.request_oriten_kaiseki_custom = {
            ok: parsedAjax.byLane.size > 0,
            parsed_lanes: parsedAjax.byLane.size,
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
            if (label === "lane_stats_tab") diagnostics.timings.lane_stats_fetch_ms = fetchDurationMs;
            else diagnostics.timings.pre_race_fetch_ms = fetchDurationMs;
            const parseStartedAt = nowMs();
            const parsed = normalizeKyoteiBiyoriPreRaceFields(
              parseKyoteiBiyoriPreRaceData(html, {
                mode: label === "lane_stats_tab" ? "lane_stats" : "pre_race",
                sourceLabel: label
              })
            );
            const parseDurationMs = elapsedMs(parseStartedAt);
            if (label === "lane_stats_tab") diagnostics.timings.lane_stats_parse_ms = parseDurationMs;
            else diagnostics.timings.pre_race_parse_ms = parseDurationMs;
            mergeLaneMaps(mergedByLane, parsed.byLane, fieldSources, label);
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
              populated_fields: parsed.fieldDiagnostics?.populated_fields || [],
              failed_fields: parsed.fieldDiagnostics?.failed_fields || EXPECTED_FIELDS,
              required_fields: buildRequiredFieldParseStatus(parsed.byLane),
              table_diagnostics: parsed.tableDiagnostics || [],
              field_debugs: parsed.fieldDebugs || {}
            };
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

    const fieldDiagnostics = buildFieldDiagnostics(mergedByLane, fieldSources);
    const laneStatsReady = fieldDiagnostics.per_lane.some((row) => row.populated_fields.includes("laneFirstRate"));
    const lapTimeReady = fieldDiagnostics.per_lane.some((row) => row.populated_fields.includes("lapTimeRaw"));
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
    diagnostics.field_sources = fieldSources;
    diagnostics.field_diagnostics = fieldDiagnostics;
    diagnostics.required_field_status = requiredFieldStatus;
    diagnostics.critical_fields_ready = criticalFieldsReady.map(([field]) => field);
    diagnostics.partial_prediction_data_available = mergedByLane.size > 0 && criticalFieldsReady.length > 0;
    diagnostics.lane_stats_ready = laneStatsReady;
    diagnostics.lap_time_ready = lapTimeReady;
    diagnostics.fallback_reason = fallbackReason;
    diagnostics.kyoteibiyori_fetch_success = ok;
    diagnostics.timings.total_ms = elapsedMs(startedAt);

    return {
      ok,
      url: indexUrl,
      triedUrls: [indexUrl, laneStatsUrl, preRaceUrl],
      byLane: mergedByLane,
      fieldDebugs: {
        lane_stats_tab: diagnostics.parse_results?.lane_stats_tab?.field_debugs || {},
        pre_race_tab: diagnostics.parse_results?.pre_race_tab?.field_debugs || {}
      },
      tableDiagnostics,
      fieldDiagnostics,
      fieldSources,
      fallbackUsed: !ok,
      fallbackReason,
      diagnostics,
      error: ok ? null : fallbackReason
    };
  } catch (error) {
    const hardTimeoutMs = Math.max(250, Math.min(Number(timeoutMs) || 12000, 4000));
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
        }
      },
      parse_results: {
        request_oriten_kaiseki_custom: {
          ok: false,
          parsed_lanes: 0,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          diagnostics: {}
        },
        lane_stats_tab: {
          ok: false,
          parsed_lanes: 0,
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        },
        pre_race_tab: {
          ok: false,
          parsed_lanes: 0,
          populated_fields: [],
          failed_fields: EXPECTED_FIELDS,
          required_fields: buildRequiredFieldParseStatus(new Map()),
          table_diagnostics: []
        }
      },
      merge_results: {
        merged_lanes: 0
      },
      field_sources: {},
      field_diagnostics: buildFieldDiagnostics(new Map(), {}),
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
