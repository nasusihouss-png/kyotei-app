import axios from "axios";
import * as cheerio from "cheerio";

const KYOTEI_BIYORI_BASE = "https://kyoteibiyori.com";

function normalizeSpace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeDigits(value) {
  return String(value || "").replace(/[\uFF10-\uFF19]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) - 0xfee0));
}

function toNumber(value) {
  const cleaned = normalizeDigits(String(value || "")).replace(/,/g, "").trim();
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseDecimal(value) {
  const text = normalizeDigits(String(value || "")).replace(/\s+/g, "").trim();
  if (!text) return null;
  const normalized = text.startsWith(".") ? `0${text}` : text;
  const n = Number(normalized);
  return Number.isFinite(n) ? n : null;
}

function scoreStretchFoot(value) {
  const text = normalizeSpace(value);
  if (!text) return { score: null, label: null };
  const numeric = toNumber(text);
  if (numeric !== null) return { score: numeric, label: text };
  const table = [
    [/◎|抜群|かなり良い|超抜/, 5],
    [/○|上位|良い|伸びる/, 4],
    [/△|普通|まずまず/, 3],
    [/弱い|見劣り|劣勢/, 1.5],
    [/×|かなり弱い|劣る/, 0.5]
  ];
  for (const [pattern, score] of table) {
    if (pattern.test(text)) return { score, label: text };
  }
  return { score: null, label: text };
}

async function fetchHtml(url, timeoutMs = 12000) {
  const { data } = await axios.get(url, {
    timeout: timeoutMs,
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });
  return data;
}

function parseHeaders($, $table) {
  return $table
    .find("tr")
    .first()
    .children("th,td")
    .map((_, cell) => normalizeSpace($(cell).text()))
    .get();
}

function findRelevantTables($) {
  return $("table")
    .toArray()
    .map((el) => {
      const $table = $(el);
      const headers = parseHeaders($, $table);
      const joined = headers.join(" ");
      const score =
        (/(周回|1周|ラップ)/.test(joined) ? 2 : 0) +
        (/(展示.*ST|ST)/.test(joined) ? 2 : 0) +
        (/(展示.*タイム|展示タイム)/.test(joined) ? 2 : 0) +
        (/(伸び|周回展示|足色)/.test(joined) ? 2 : 0) +
        (/(艇|枠|コース)/.test(joined) ? 1 : 0);
      return { $table, headers, score };
    })
    .filter((row) => row.score >= 3)
    .sort((a, b) => b.score - a.score);
}

function detectColumnIndex(headers, patterns) {
  const idx = headers.findIndex((header) => patterns.some((pattern) => pattern.test(header)));
  return idx >= 0 ? idx : null;
}

export function parseKyoteiBiyoriPreRaceData(html) {
  const $ = cheerio.load(html);
  const relevantTables = findRelevantTables($);
  const byLane = new Map();
  const tableDiagnostics = [];

  for (const table of relevantTables) {
    const headers = table.headers;
    const laneIdx = detectColumnIndex(headers, [/(艇|枠|コース|進入)/]);
    const lapTimeIdx = detectColumnIndex(headers, [/(周回タイム|1周タイム|周回|ラップ)/]);
    const lapExhibitionIdx = detectColumnIndex(headers, [/(周回展示|伸び足|足色|伸び)/]);
    const exhibitionStIdx = detectColumnIndex(headers, [/(展示.*ST|ST)/]);
    const exhibitionTimeIdx = detectColumnIndex(headers, [/(展示.*タイム|展示タイム)/]);
    const rows = table.$table.find("tr").slice(1);
    let parsedCount = 0;

    rows.each((_, tr) => {
      const cells = $(tr).children("td,th").map((__, cell) => normalizeSpace($(cell).text())).get();
      if (cells.length < 2) return;
      const laneCandidate = toNumber(cells[laneIdx ?? 0]) ?? toNumber(cells.find((cell) => /^[1-6]$/.test(normalizeDigits(cell))));
      if (!Number.isInteger(laneCandidate) || laneCandidate < 1 || laneCandidate > 6) return;
      const stretch = scoreStretchFoot(lapExhibitionIdx !== null ? cells[lapExhibitionIdx] : null);
      const current = byLane.get(laneCandidate) || {};
      const next = {
        lapTime: lapTimeIdx !== null ? parseDecimal(cells[lapTimeIdx]) : current.lapTime ?? null,
        lapExhibitionScore: lapExhibitionIdx !== null ? stretch.score : current.lapExhibitionScore ?? null,
        stretchFootLabel: lapExhibitionIdx !== null ? stretch.label : current.stretchFootLabel ?? null,
        exhibitionSt: exhibitionStIdx !== null ? parseDecimal(cells[exhibitionStIdx]) : current.exhibitionSt ?? null,
        exhibitionTime: exhibitionTimeIdx !== null ? parseDecimal(cells[exhibitionTimeIdx]) : current.exhibitionTime ?? null,
        sourceHeaders: headers
      };
      if (
        next.lapTime !== null ||
        next.lapExhibitionScore !== null ||
        next.exhibitionSt !== null ||
        next.exhibitionTime !== null
      ) {
        byLane.set(laneCandidate, {
          ...current,
          ...Object.fromEntries(Object.entries(next).filter(([, value]) => value !== null))
        });
        parsedCount += 1;
      }
    });

    tableDiagnostics.push({
      headers,
      laneIdx,
      lapTimeIdx,
      lapExhibitionIdx,
      exhibitionStIdx,
      exhibitionTimeIdx,
      parsedCount
    });
  }

  return {
    byLane,
    tableDiagnostics
  };
}

export function normalizeKyoteiBiyoriPreRaceFields(parsed) {
  const normalizedByLane = new Map();
  for (const [lane, row] of parsed?.byLane || []) {
    normalizedByLane.set(Number(lane), {
      lapTime: Number.isFinite(Number(row?.lapTime)) ? Number(row.lapTime) : null,
      lapExhibitionScore: Number.isFinite(Number(row?.lapExhibitionScore)) ? Number(row.lapExhibitionScore) : null,
      stretchFootLabel: row?.stretchFootLabel || null,
      exhibitionSt: Number.isFinite(Number(row?.exhibitionSt)) ? Number(row.exhibitionSt) : null,
      exhibitionTime: Number.isFinite(Number(row?.exhibitionTime)) ? Number(row.exhibitionTime) : null
    });
  }
  return {
    byLane: normalizedByLane,
    tableDiagnostics: parsed?.tableDiagnostics || []
  };
}

export function mergeKyoteiBiyoriDataIntoRaceContext({ racers, kyoteiBiyori }) {
  const byLane = kyoteiBiyori?.byLane || new Map();
  const mergedRacers = (racers || []).map((racer) => {
    const lane = Number(racer?.lane);
    const extra = byLane.get(lane) || {};
    return {
      ...racer,
      kyoteiBiyoriFetched: byLane.has(lane) ? 1 : 0,
      kyoteiBiyoriLapTime: extra?.lapTime ?? null,
      kyoteiBiyoriLapExhibitionScore: extra?.lapExhibitionScore ?? null,
      kyoteiBiyoriStretchFootLabel: extra?.stretchFootLabel ?? null,
      kyoteiBiyoriExhibitionSt: extra?.exhibitionSt ?? null,
      kyoteiBiyoriExhibitionTime: extra?.exhibitionTime ?? null,
      lapTime: extra?.lapTime ?? racer?.lapTime ?? null,
      lapExhibitionScore: extra?.lapExhibitionScore ?? racer?.lapExhibitionScore ?? null,
      stretchFootLabel: extra?.stretchFootLabel ?? racer?.stretchFootLabel ?? null,
      exhibitionSt: racer?.exhibitionSt ?? extra?.exhibitionSt ?? null,
      exhibitionTime: racer?.exhibitionTime ?? extra?.exhibitionTime ?? null
    };
  });
  return mergedRacers;
}

export async function fetchKyoteiBiyoriRaceData({ date, venueId, raceNo, timeoutMs = 12000 }) {
  const hd = String(date || "").replace(/-/g, "");
  const placeNo = String(venueId || "").padStart(2, "0");
  const rno = Number(raceNo);
  const urls = [
    `${KYOTEI_BIYORI_BASE}/race_shusso.php?hiduke=${hd}&place_no=${placeNo}&race_no=${rno}`,
    `${KYOTEI_BIYORI_BASE}/race_ichiran.php?hiduke=${hd}&place_no=${placeNo}&race_no=${rno}`
  ];
  let lastError = null;
  for (const url of urls) {
    try {
      const html = await fetchHtml(url, timeoutMs);
      const parsed = parseKyoteiBiyoriPreRaceData(html);
      const normalized = normalizeKyoteiBiyoriPreRaceFields(parsed);
      if ((normalized?.byLane?.size || 0) > 0) {
        return {
          ok: true,
          url,
          byLane: normalized.byLane,
          tableDiagnostics: normalized.tableDiagnostics,
          fallbackUsed: false
        };
      }
      lastError = new Error("kyoteibiyori parse returned no usable lane data");
    } catch (error) {
      lastError = error;
    }
  }
  return {
    ok: false,
    url: urls[0],
    byLane: new Map(),
    tableDiagnostics: [],
    fallbackUsed: true,
    error: lastError ? String(lastError.message || lastError) : "unknown kyoteibiyori error"
  };
}
