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
  if (Number.isFinite(Number(chokusen))) parts.push(`伸び ${Number(chokusen).toFixed(2)}`);
  if (Number.isFinite(Number(mawariashi))) parts.push(`周回 ${Number(mawariashi).toFixed(2)}`);
  return parts.length > 0 ? parts.join(" / ") : null;
}

function computeLapExhibitionScore({ mawariashi, chokusen }) {
  const scores = [mawariashi, chokusen].filter((value) => Number.isFinite(Number(value))).map(Number);
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
    exhibitionST: hasValue("exhibitionSt")
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
      const headers = $table
        .find("tr")
        .first()
        .children("th,td")
        .map((_, cell) => normalizeText($(cell).text()))
        .get();
      return {
        $,
        $table,
        headers,
        text: normalizeText($table.text())
      };
    })
    .filter((table) => table.headers.length > 0);
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
    motor2Rate: [/モーター.*2連率/i, /^2連率$/i],
    motor3Rate: [/モーター.*3連率/i, /^3連率$/i],
    laneFirstRate: [/1着率/i],
    lane2RenRate: [/2着率/i, /2連率/i],
    lane3RenRate: [/3着率/i, /3連率/i]
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

      const lapExLabel = indexes.lapExhibition !== null ? values[indexes.lapExhibition] : null;
      if (lapExLabel) {
        next.stretchFootLabel = lapExLabel;
        next.lapExhibitionScore = parseDecimal(lapExLabel);
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
    const stretchFootLabel = makeStretchLabel({ mawariashi, chokusen });
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
      lapExhibitionScore,
      stretchFootLabel,
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

export function parseKyoteiBiyoriPreRaceData(html) {
  const supplement = parseHtmlSupplement(html);
  const byLane = new Map();
  const fieldSources = {};
  mergeLaneMaps(byLane, supplement.byLane, fieldSources, "race_shusso_html");
  return {
    byLane,
    fieldSources,
    tableDiagnostics: supplement.tableDiagnostics,
    fieldDiagnostics: buildFieldDiagnostics(byLane, fieldSources)
  };
}

export function normalizeKyoteiBiyoriPreRaceFields(parsed) {
  const normalizedByLane = new Map();
  const fieldSources = parsed?.fieldSources || {};
  for (const [lane, row] of parsed?.byLane || []) {
    normalizedByLane.set(Number(lane), {
      playerName: row?.playerName || null,
      fCount: toFiniteNumberOrNull(row?.fCount),
      lapTime: toFiniteNumberOrNull(row?.lapTime),
      lapTimeRaw: toFiniteNumberOrNull(row?.lapTimeRaw),
      lapExhibitionScore: toFiniteNumberOrNull(row?.lapExhibitionScore),
      stretchFootLabel: row?.stretchFootLabel || null,
      exhibitionSt: toFiniteNumberOrNull(row?.exhibitionSt),
      exhibitionTime: toFiniteNumberOrNull(row?.exhibitionTime),
      motor2Rate: toFiniteNumberOrNull(row?.motor2Rate),
      motor3Rate: toFiniteNumberOrNull(row?.motor3Rate),
      laneFirstRate: toFiniteNumberOrNull(row?.laneFirstRate),
      lane2RenRate: toFiniteNumberOrNull(row?.lane2RenRate),
      lane3RenRate: toFiniteNumberOrNull(row?.lane3RenRate)
    });
  }
  return {
    byLane: normalizedByLane,
    fieldSources,
    tableDiagnostics: parsed?.tableDiagnostics || [],
    fieldDiagnostics: parsed?.fieldDiagnostics || buildFieldDiagnostics(normalizedByLane, fieldSources),
    diagnostics: parsed?.diagnostics || {}
  };
}

export function mergeKyoteiBiyoriDataIntoRaceContext({ racers, kyoteiBiyori }) {
  const byLane = kyoteiBiyori?.byLane instanceof Map ? kyoteiBiyori.byLane : new Map();
  return (racers || []).map((racer) => {
    try {
      const lane = Number(racer?.lane);
      const extra = byLane.get(lane) || {};
      return {
        ...racer,
        name: extra?.playerName || racer?.name || null,
        fHoldCount: extra?.fCount ?? racer?.fHoldCount ?? null,
        kyoteiBiyoriFetched: byLane.has(lane) ? 1 : 0,
        kyoteiBiyoriLapTime: extra?.lapTime ?? null,
        kyoteiBiyoriLapTimeRaw: extra?.lapTimeRaw ?? null,
        kyoteiBiyoriLapExhibitionScore: extra?.lapExhibitionScore ?? null,
        kyoteiBiyoriStretchFootLabel: extra?.stretchFootLabel ?? null,
        kyoteiBiyoriExhibitionSt: extra?.exhibitionSt ?? null,
        kyoteiBiyoriExhibitionTime: extra?.exhibitionTime ?? null,
        kyoteiBiyoriMotor2Rate: extra?.motor2Rate ?? null,
        kyoteiBiyoriMotor3Rate: extra?.motor3Rate ?? null,
        lapTime: extra?.lapTime ?? racer?.lapTime ?? null,
        lapTimeRaw: extra?.lapTimeRaw ?? racer?.lapTimeRaw ?? null,
        lapExhibitionScore: extra?.lapExhibitionScore ?? racer?.lapExhibitionScore ?? null,
        stretchFootLabel: extra?.stretchFootLabel ?? racer?.stretchFootLabel ?? null,
        exhibitionSt: extra?.exhibitionSt ?? racer?.exhibitionSt ?? null,
        exhibitionTime: extra?.exhibitionTime ?? racer?.exhibitionTime ?? null,
        motor2Rate: extra?.motor2Rate ?? racer?.motor2Rate ?? null,
        motor3Rate: extra?.motor3Rate ?? racer?.motor3Rate ?? null,
        laneFirstRate: extra?.laneFirstRate ?? racer?.laneFirstRate ?? null,
        lane2RenRate: extra?.lane2RenRate ?? racer?.lane2RenRate ?? null,
        lane3RenRate: extra?.lane3RenRate ?? racer?.lane3RenRate ?? null
      };
    } catch {
      return {
        ...racer,
        kyoteiBiyoriFetched: 0,
        kyoteiBiyoriLapTime: null,
        kyoteiBiyoriLapTimeRaw: null,
        kyoteiBiyoriLapExhibitionScore: null,
        kyoteiBiyoriStretchFootLabel: null,
        kyoteiBiyoriExhibitionSt: null,
        kyoteiBiyoriExhibitionTime: null,
        kyoteiBiyoriMotor2Rate: null,
        kyoteiBiyoriMotor3Rate: null
      };
    }
  });
}

export async function fetchKyoteiBiyoriRaceData({ date, venueId, raceNo, timeoutMs = 12000 }) {
  try {
    const indexUrl = buildIndexUrl({ date, venueId, raceNo });
    const diagnostics = {
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
      indexHtml = await fetchText(indexUrl, timeoutMs);
      diagnostics.fetch_results.race_ichiran.ok = true;
      diagnostics.fetch_results.race_ichiran.has_placeholder = /データ取得中です|しばらくお待ちください/.test(indexHtml);
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

    try {
      const ajaxPayload = await fetchOritenJson({
        date,
        venueId,
        raceNo,
        refererUrl: laneStatsUrl,
        timeoutMs
      });
      const parsedAjax = parseKyoteiBiyoriAjaxData(ajaxPayload);
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
    } catch (error) {
      lastError = error;
      diagnostics.fetch_results.request_oriten_kaiseki_custom.error = String(error?.message || error);
    }

    for (const [label, url] of [
      ["lane_stats_tab", laneStatsUrl],
      ["pre_race_tab", preRaceUrl]
    ]) {
      try {
        const html = await fetchText(url, timeoutMs);
        const parsed = normalizeKyoteiBiyoriPreRaceFields(parseKyoteiBiyoriPreRaceData(html));
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
          table_diagnostics: parsed.tableDiagnostics || []
        };
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

    const fieldDiagnostics = buildFieldDiagnostics(mergedByLane, fieldSources);
    const laneStatsReady = fieldDiagnostics.per_lane.some((row) => row.populated_fields.includes("laneFirstRate"));
    const lapTimeReady = fieldDiagnostics.per_lane.some((row) => row.populated_fields.includes("lapTimeRaw"));
    const ok = mergedByLane.size > 0 && (laneStatsReady || lapTimeReady);
    const fallbackReason =
      ok
        ? null
        : lastError
          ? String(lastError.message || lastError)
          : "kyoteibiyori returned no usable lane-stat or pre-race fields";
    diagnostics.merge_results.merged_lanes = mergedByLane.size;
    diagnostics.field_sources = fieldSources;
    diagnostics.field_diagnostics = fieldDiagnostics;
    diagnostics.fallback_reason = fallbackReason;
    diagnostics.kyoteibiyori_fetch_success = ok;

    return {
      ok,
      url: indexUrl,
      triedUrls: [indexUrl, laneStatsUrl, preRaceUrl],
      byLane: mergedByLane,
      tableDiagnostics,
      fieldDiagnostics,
      fieldSources,
      fallbackUsed: !ok,
      fallbackReason,
      diagnostics,
      error: ok ? null : fallbackReason
    };
  } catch (error) {
    const emptyDiagnostics = {
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
