import axios from "axios";
import * as cheerio from "cheerio";

const KYOTEI_BIYORI_BASE = "https://kyoteibiyori.com";
const EXPECTED_FIELDS = [
  "playerName",
  "fCount",
  "lapTime",
  "lapExhibitionScore",
  "stretchFootLabel",
  "exhibitionSt",
  "exhibitionTime",
  "motor2Rate",
  "motor3Rate"
];

function normalizeSpace(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeDigits(value) {
  return normalizeSpace(value).replace(/[０-９]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) - 0xfee0));
}

function normalizeTableCellText(value) {
  return normalizeDigits(value)
    .replace(/[：:]/g, ":")
    .replace(/[／]/g, "/")
    .trim();
}

function toNumber(value) {
  const cleaned = normalizeDigits(value).replace(/,/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
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

function detectLane(cells) {
  for (const cell of cells) {
    const text = normalizeTableCellText(cell);
    const direct = text.match(/^(?:艇番|コース|枠)?\s*([1-6])$/);
    if (direct) return Number(direct[1]);
    const colored = text.match(/([1-6])\s*号艇/);
    if (colored) return Number(colored[1]);
  }
  return null;
}

function scoreStretchFoot(value) {
  const text = normalizeSpace(value);
  if (!text) return { score: null, label: null };
  const numeric = toNumber(text);
  if (numeric !== null) return { score: numeric, label: text };

  const table = [
    [/抜群|かなり良い|かなり強い|非常に良い/i, 5],
    [/上位|良い|強い|目立つ/i, 4],
    [/普通|まずまず|並/i, 3],
    [/弱め|少し弱い|やや弱い/i, 1.5],
    [/弱い|劣勢|見劣り/i, 0.5]
  ];

  for (const [pattern, score] of table) {
    if (pattern.test(text)) {
      return { score, label: text };
    }
  }
  return { score: null, label: text };
}

function isRelevantHeader(text) {
  return (
    /艇番|コース|枠|選手|周回|展示|ST|タイム|モーター|2連|3連|F/i.test(text)
  );
}

function parseHeaders($, $table) {
  const headerRow = $table.find("tr").toArray().find((tr) => {
    const headerText = $(tr)
      .children("th,td")
      .map((_, cell) => normalizeTableCellText($(cell).text()))
      .get()
      .join(" ");
    return isRelevantHeader(headerText);
  });
  if (!headerRow) return [];
  return $(headerRow)
    .children("th,td")
    .map((_, cell) => normalizeTableCellText($(cell).text()))
    .get();
}

function detectColumnIndex(headers, patterns) {
  const idx = headers.findIndex((header) => patterns.some((pattern) => pattern.test(header)));
  return idx >= 0 ? idx : null;
}

function getFieldPatterns() {
  return {
    lane: [/艇番/i, /コース/i, /枠/i],
    playerName: [/選手/i, /名前/i],
    fCount: [/F/i, /フライング/i],
    lapTime: [/周回.*タイム/i, /1周.*タイム/i, /ラップ.*タイム/i],
    lapExhibition: [/周回展示/i, /伸び足/i, /足色/i, /出足/i, /回り足/i],
    exhibitionSt: [/展示.*ST/i, /^ST$/i],
    exhibitionTime: [/展示.*タイム/i],
    motor2Rate: [/モーター.*2連/i, /^2連率$/i],
    motor3Rate: [/モーター.*3連/i, /^3連率$/i]
  };
}

function getFieldValue(cells, index) {
  if (index === null || index < 0 || index >= cells.length) return null;
  const text = normalizeTableCellText(cells[index]);
  return text || null;
}

function findRelevantTables($) {
  return $("table")
    .toArray()
    .map((el) => {
      const $table = $(el);
      const headers = parseHeaders($, $table);
      const joined = headers.join(" ");
      const bodyText = normalizeTableCellText($table.text());
      const score =
        (/(艇番|コース|枠)/i.test(joined) ? 2 : 0) +
        (/(周回.*タイム|1周.*タイム|ラップ.*タイム)/i.test(joined + bodyText) ? 3 : 0) +
        (/(周回展示|伸び足|足色|出足|回り足)/i.test(joined + bodyText) ? 2 : 0) +
        (/(展示.*ST|ST)/i.test(joined) ? 2 : 0) +
        (/(展示.*タイム)/i.test(joined) ? 2 : 0) +
        (/(モーター.*2連|モーター.*3連|2連率|3連率)/i.test(joined + bodyText) ? 1 : 0);
      return { $table, headers, score };
    })
    .filter((row) => row.score >= 3 && row.headers.length > 0)
    .sort((a, b) => b.score - a.score);
}

function parseRowsFromTable($, table) {
  const headers = table.headers;
  const patterns = getFieldPatterns();
  const indexes = {
    lane: detectColumnIndex(headers, patterns.lane),
    playerName: detectColumnIndex(headers, patterns.playerName),
    fCount: detectColumnIndex(headers, patterns.fCount),
    lapTime: detectColumnIndex(headers, patterns.lapTime),
    lapExhibition: detectColumnIndex(headers, patterns.lapExhibition),
    exhibitionSt: detectColumnIndex(headers, patterns.exhibitionSt),
    exhibitionTime: detectColumnIndex(headers, patterns.exhibitionTime),
    motor2Rate: detectColumnIndex(headers, patterns.motor2Rate),
    motor3Rate: detectColumnIndex(headers, patterns.motor3Rate)
  };

  const rows = [];
  table.$table.find("tr").each((_, tr) => {
    const cells = $(tr)
      .children("td,th")
      .map((__, cell) => normalizeTableCellText($(cell).text()))
      .get();
    if (cells.length < 2) return;
    const lane =
      (indexes.lane !== null ? toNumber(getFieldValue(cells, indexes.lane)) : null) ??
      detectLane(cells);
    if (!Number.isInteger(lane) || lane < 1 || lane > 6) return;

    const stretch = scoreStretchFoot(getFieldValue(cells, indexes.lapExhibition));
    rows.push({
      lane,
      values: {
        playerName: getFieldValue(cells, indexes.playerName),
        fCount: parseFCount(getFieldValue(cells, indexes.fCount)),
        lapTime: parseDecimal(getFieldValue(cells, indexes.lapTime)),
        lapExhibitionScore: stretch.score,
        stretchFootLabel: stretch.label,
        exhibitionSt: parseDecimal(getFieldValue(cells, indexes.exhibitionSt)),
        exhibitionTime: parseDecimal(getFieldValue(cells, indexes.exhibitionTime)),
        motor2Rate: parsePercent(getFieldValue(cells, indexes.motor2Rate)),
        motor3Rate: parsePercent(getFieldValue(cells, indexes.motor3Rate))
      }
    });
  });

  return {
    headers,
    indexes,
    rows
  };
}

function buildFieldDiagnostics(byLane) {
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
      missing_fields: EXPECTED_FIELDS.filter((field) => !populatedFields.includes(field))
    });
  }

  return {
    populated_fields: [...populated],
    failed_fields: [...missing],
    per_lane: perLane
  };
}

export function parseKyoteiBiyoriPreRaceData(html) {
  const $ = cheerio.load(html);
  const relevantTables = findRelevantTables($);
  const byLane = new Map();
  const tableDiagnostics = [];

  for (const table of relevantTables) {
    const parsedTable = parseRowsFromTable($, table);
    let parsedCount = 0;

    for (const row of parsedTable.rows) {
      const current = byLane.get(row.lane) || {};
      const merged = { ...current };
      for (const [key, value] of Object.entries(row.values)) {
        if (value !== null && value !== undefined && value !== "") {
          merged[key] = value;
        }
      }
      if (Object.keys(merged).length > 0) {
        byLane.set(row.lane, merged);
        parsedCount += 1;
      }
    }

    tableDiagnostics.push({
      headers: parsedTable.headers,
      indexes: parsedTable.indexes,
      parsedCount
    });
  }

  return {
    byLane,
    tableDiagnostics,
    fieldDiagnostics: buildFieldDiagnostics(byLane)
  };
}

export function normalizeKyoteiBiyoriPreRaceFields(parsed) {
  const normalizedByLane = new Map();
  for (const [lane, row] of parsed?.byLane || []) {
    normalizedByLane.set(Number(lane), {
      playerName: row?.playerName || null,
      fCount: Number.isFinite(Number(row?.fCount)) ? Number(row.fCount) : null,
      lapTime: Number.isFinite(Number(row?.lapTime)) ? Number(row.lapTime) : null,
      lapExhibitionScore: Number.isFinite(Number(row?.lapExhibitionScore)) ? Number(row.lapExhibitionScore) : null,
      stretchFootLabel: row?.stretchFootLabel || null,
      exhibitionSt: Number.isFinite(Number(row?.exhibitionSt)) ? Number(row.exhibitionSt) : null,
      exhibitionTime: Number.isFinite(Number(row?.exhibitionTime)) ? Number(row.exhibitionTime) : null,
      motor2Rate: Number.isFinite(Number(row?.motor2Rate)) ? Number(row.motor2Rate) : null,
      motor3Rate: Number.isFinite(Number(row?.motor3Rate)) ? Number(row.motor3Rate) : null
    });
  }

  return {
    byLane: normalizedByLane,
    tableDiagnostics: parsed?.tableDiagnostics || [],
    fieldDiagnostics: parsed?.fieldDiagnostics || buildFieldDiagnostics(normalizedByLane)
  };
}

export function mergeKyoteiBiyoriDataIntoRaceContext({ racers, kyoteiBiyori }) {
  const byLane = kyoteiBiyori?.byLane || new Map();
  return (racers || []).map((racer) => {
    const lane = Number(racer?.lane);
    const extra = byLane.get(lane) || {};
    return {
      ...racer,
      name: extra?.playerName || racer?.name || null,
      fHoldCount: extra?.fCount ?? racer?.fHoldCount ?? null,
      kyoteiBiyoriFetched: byLane.has(lane) ? 1 : 0,
      kyoteiBiyoriLapTime: extra?.lapTime ?? null,
      kyoteiBiyoriLapExhibitionScore: extra?.lapExhibitionScore ?? null,
      kyoteiBiyoriStretchFootLabel: extra?.stretchFootLabel ?? null,
      kyoteiBiyoriExhibitionSt: extra?.exhibitionSt ?? null,
      kyoteiBiyoriExhibitionTime: extra?.exhibitionTime ?? null,
      kyoteiBiyoriMotor2Rate: extra?.motor2Rate ?? null,
      kyoteiBiyoriMotor3Rate: extra?.motor3Rate ?? null,
      lapTime: extra?.lapTime ?? racer?.lapTime ?? null,
      lapExhibitionScore: extra?.lapExhibitionScore ?? racer?.lapExhibitionScore ?? null,
      stretchFootLabel: extra?.stretchFootLabel ?? racer?.stretchFootLabel ?? null,
      exhibitionSt: extra?.exhibitionSt ?? racer?.exhibitionSt ?? null,
      exhibitionTime: extra?.exhibitionTime ?? racer?.exhibitionTime ?? null,
      motor2Rate: extra?.motor2Rate ?? racer?.motor2Rate ?? null,
      motor3Rate: extra?.motor3Rate ?? racer?.motor3Rate ?? null
    };
  });
}

async function fetchHtml(url, timeoutMs = 12000) {
  const { data } = await axios.get(url, {
    timeout: timeoutMs,
    responseType: "text",
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });
  return data;
}

export async function fetchKyoteiBiyoriRaceData({ date, venueId, raceNo, timeoutMs = 12000 }) {
  const hd = String(date || "").replace(/-/g, "");
  const placeNo = String(venueId || "").padStart(2, "0");
  const rno = Number(raceNo);
  const urls = [
    `${KYOTEI_BIYORI_BASE}/race_shusso.php?hiduke=${hd}&place_no=${placeNo}&race_no=${rno}`,
    `${KYOTEI_BIYORI_BASE}/race_ichiran.php?hiduke=${hd}&place_no=${placeNo}&race_no=${rno}`,
    `${KYOTEI_BIYORI_BASE}/race_odds.php?hiduke=${hd}&place_no=${placeNo}&race_no=${rno}`
  ];

  let lastError = null;
  const triedUrls = [];

  for (const url of urls) {
    triedUrls.push(url);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const html = await fetchHtml(url, timeoutMs);
        const parsed = parseKyoteiBiyoriPreRaceData(html);
        const normalized = normalizeKyoteiBiyoriPreRaceFields(parsed);
        const fieldDiagnostics = normalized.fieldDiagnostics || buildFieldDiagnostics(normalized.byLane);
        if ((normalized?.byLane?.size || 0) > 0 && fieldDiagnostics.populated_fields.length > 0) {
          return {
            ok: true,
            url,
            triedUrls,
            byLane: normalized.byLane,
            tableDiagnostics: normalized.tableDiagnostics,
            fieldDiagnostics,
            fallbackUsed: false,
            fallbackReason: null,
            error: null
          };
        }
        lastError = new Error("kyoteibiyori parse returned no usable lane fields");
      } catch (error) {
        lastError = error;
      }
    }
  }

  return {
    ok: false,
    url: triedUrls[0] || null,
    triedUrls,
    byLane: new Map(),
    tableDiagnostics: [],
    fieldDiagnostics: {
      populated_fields: [],
      failed_fields: [...EXPECTED_FIELDS],
      per_lane: []
    },
    fallbackUsed: true,
    fallbackReason: lastError ? String(lastError.message || lastError) : "kyoteibiyori unavailable",
    error: lastError ? String(lastError.message || lastError) : "unknown kyoteibiyori error"
  };
}
