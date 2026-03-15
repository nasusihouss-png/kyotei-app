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

function detectLaneFromText(text) {
  const normalized = normalizeTableCellText(text);
  const direct = normalized.match(/^(?:艇番|コース|枠)?\s*([1-6])$/);
  if (direct) return Number(direct[1]);
  const withLabel = normalized.match(/([1-6])\s*号艇/);
  if (withLabel) return Number(withLabel[1]);
  return null;
}

function detectLane(cells) {
  for (const cell of cells) {
    const lane = detectLaneFromText(cell);
    if (lane) return lane;
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
    if (pattern.test(text)) return { score, label: text };
  }
  return { score: null, label: text };
}

function fetchHtml(url, timeoutMs = 12000) {
  return axios
    .get(url, {
      timeout: timeoutMs,
      responseType: "text",
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
      }
    })
    .then((response) => response.data);
}

function parseHeaders($, $table) {
  return $table
    .find("tr")
    .first()
    .children("th,td")
    .map((_, cell) => normalizeTableCellText($(cell).text()))
    .get();
}

function detectColumnIndex(headers, patterns) {
  const idx = headers.findIndex((header) => patterns.some((pattern) => pattern.test(header)));
  return idx >= 0 ? idx : null;
}

function getRelevantTables($) {
  return $("table")
    .toArray()
    .map((el) => {
      const $table = $(el);
      const headers = parseHeaders($, $table);
      const bodyText = normalizeTableCellText($table.text());
      return {
        $table,
        headers,
        bodyText
      };
    })
    .filter((row) => row.headers.length > 0);
}

function parsePreRaceTables(html) {
  const $ = cheerio.load(html);
  const byLane = new Map();
  const tableDiagnostics = [];
  const tables = getRelevantTables($);

  const fieldPatterns = {
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

  for (const table of tables) {
    const joined = `${table.headers.join(" ")} ${table.bodyText}`;
    if (!/(周回|展示|ST|モーター|F|選手)/i.test(joined)) continue;

    const indexes = {
      lane: detectColumnIndex(table.headers, fieldPatterns.lane),
      playerName: detectColumnIndex(table.headers, fieldPatterns.playerName),
      fCount: detectColumnIndex(table.headers, fieldPatterns.fCount),
      lapTime: detectColumnIndex(table.headers, fieldPatterns.lapTime),
      lapExhibition: detectColumnIndex(table.headers, fieldPatterns.lapExhibition),
      exhibitionSt: detectColumnIndex(table.headers, fieldPatterns.exhibitionSt),
      exhibitionTime: detectColumnIndex(table.headers, fieldPatterns.exhibitionTime),
      motor2Rate: detectColumnIndex(table.headers, fieldPatterns.motor2Rate),
      motor3Rate: detectColumnIndex(table.headers, fieldPatterns.motor3Rate)
    };

    let parsedCount = 0;
    table.$table.find("tr").slice(1).each((_, tr) => {
      const cells = $(tr)
        .children("td,th")
        .map((__, cell) => normalizeTableCellText($(cell).text()))
        .get();
      if (cells.length < 2) return;
      const lane =
        (indexes.lane !== null ? toNumber(cells[indexes.lane]) : null) ??
        detectLane(cells);
      if (!Number.isInteger(lane) || lane < 1 || lane > 6) return;

      const stretch = scoreStretchFoot(indexes.lapExhibition !== null ? cells[indexes.lapExhibition] : null);
      const current = byLane.get(lane) || {};
      const next = {
        playerName: indexes.playerName !== null ? cells[indexes.playerName] || null : null,
        fCount: indexes.fCount !== null ? parseFCount(cells[indexes.fCount]) : null,
        lapTime: indexes.lapTime !== null ? parseDecimal(cells[indexes.lapTime]) : null,
        lapExhibitionScore: indexes.lapExhibition !== null ? stretch.score : null,
        stretchFootLabel: indexes.lapExhibition !== null ? stretch.label : null,
        exhibitionSt: indexes.exhibitionSt !== null ? parseDecimal(cells[indexes.exhibitionSt]) : null,
        exhibitionTime: indexes.exhibitionTime !== null ? parseDecimal(cells[indexes.exhibitionTime]) : null,
        motor2Rate: indexes.motor2Rate !== null ? parsePercent(cells[indexes.motor2Rate]) : null,
        motor3Rate: indexes.motor3Rate !== null ? parsePercent(cells[indexes.motor3Rate]) : null
      };

      const merged = { ...current };
      for (const [key, value] of Object.entries(next)) {
        if (value !== null && value !== undefined && value !== "") merged[key] = value;
      }
      if (Object.keys(merged).length > 0) {
        byLane.set(lane, merged);
        parsedCount += 1;
      }
    });

    tableDiagnostics.push({
      type: "pre_race",
      headers: table.headers,
      indexes,
      parsedCount
    });
  }

  return {
    byLane,
    tableDiagnostics
  };
}

function parseLaneStatsTables(html) {
  const $ = cheerio.load(html);
  const byLane = new Map();
  const tableDiagnostics = [];
  const tables = getRelevantTables($);

  const patterns = {
    lane: [/艇番/i, /コース/i, /枠/i],
    laneFirstRate: [/1着率/i, /1着/i],
    lane2RenRate: [/2連率/i, /2連/i],
    lane3RenRate: [/3連率/i, /3連/i]
  };

  for (const table of tables) {
    const joined = `${table.headers.join(" ")} ${table.bodyText}`;
    if (!/(枠別勝率|1着率|2連率|3連率)/i.test(joined)) continue;

    const indexes = {
      lane: detectColumnIndex(table.headers, patterns.lane),
      laneFirstRate: detectColumnIndex(table.headers, patterns.laneFirstRate),
      lane2RenRate: detectColumnIndex(table.headers, patterns.lane2RenRate),
      lane3RenRate: detectColumnIndex(table.headers, patterns.lane3RenRate)
    };

    let parsedCount = 0;
    table.$table.find("tr").slice(1).each((_, tr) => {
      const cells = $(tr)
        .children("td,th")
        .map((__, cell) => normalizeTableCellText($(cell).text()))
        .get();
      if (cells.length < 2) return;

      const lane =
        (indexes.lane !== null ? toNumber(cells[indexes.lane]) : null) ??
        detectLane(cells);
      if (!Number.isInteger(lane) || lane < 1 || lane > 6) return;

      const current = byLane.get(lane) || {};
      const next = {
        laneFirstRate: indexes.laneFirstRate !== null ? parsePercent(cells[indexes.laneFirstRate]) : null,
        lane2RenRate: indexes.lane2RenRate !== null ? parsePercent(cells[indexes.lane2RenRate]) : null,
        lane3RenRate: indexes.lane3RenRate !== null ? parsePercent(cells[indexes.lane3RenRate]) : null
      };

      const merged = { ...current };
      for (const [key, value] of Object.entries(next)) {
        if (value !== null && value !== undefined && value !== "") merged[key] = value;
      }
      if (Object.keys(merged).length > 0) {
        byLane.set(lane, merged);
        parsedCount += 1;
      }
    });

    tableDiagnostics.push({
      type: "lane_stats",
      headers: table.headers,
      indexes,
      parsedCount
    });
  }

  return {
    byLane,
    tableDiagnostics
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

function mergeLaneMaps(...maps) {
  const merged = new Map();
  for (const sourceMap of maps) {
    for (const [lane, row] of sourceMap || []) {
      const current = merged.get(lane) || {};
      merged.set(lane, {
        ...current,
        ...Object.fromEntries(
          Object.entries(row || {}).filter(([, value]) => value !== null && value !== undefined && value !== "")
        )
      });
    }
  }
  return merged;
}

function buildSliderUrls({ date, venueId, raceNo }) {
  const hd = String(date || "").replace(/-/g, "");
  const placeNo = String(venueId || "").padStart(2, "0");
  const rno = Number(raceNo);
  return Array.from({ length: 6 }, (_, idx) =>
    `${KYOTEI_BIYORI_BASE}/race_shusso.php?hiduke=${hd}&place_no=${placeNo}&race_no=${rno}&slider=${idx + 1}`
  );
}

function parseTabLinksFromIndex(html) {
  const $ = cheerio.load(html);
  const links = [];
  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    const text = normalizeSpace($(el).text());
    if (!href) return;
    if (/race_shusso\.php/i.test(href) || /枠別勝率|直前情報/.test(text)) {
      try {
        links.push({
          href: new URL(href, KYOTEI_BIYORI_BASE).href,
          text
        });
      } catch {
        // ignore invalid href
      }
    }
  });
  return links;
}

function classifyTabContent(html) {
  const text = normalizeTableCellText(html);
  return {
    hasInitialPlaceholder: /データ取得中です|しばらくお待ちください/.test(text),
    hasLaneStats: /枠別勝率|1着率|2連率|3連率/.test(text),
    hasPreRace: /直前情報|周回タイム|周回展示|展示ST|展示タイム/.test(text)
  };
}

export function parseKyoteiBiyoriPreRaceData(html) {
  const preRace = parsePreRaceTables(html);
  const laneStats = parseLaneStatsTables(html);
  const byLane = mergeLaneMaps(preRace.byLane, laneStats.byLane);
  return {
    byLane,
    tableDiagnostics: [...preRace.tableDiagnostics, ...laneStats.tableDiagnostics],
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
      motor3Rate: Number.isFinite(Number(row?.motor3Rate)) ? Number(row.motor3Rate) : null,
      laneFirstRate: Number.isFinite(Number(row?.laneFirstRate)) ? Number(row.laneFirstRate) : null,
      lane2RenRate: Number.isFinite(Number(row?.lane2RenRate)) ? Number(row.lane2RenRate) : null,
      lane3RenRate: Number.isFinite(Number(row?.lane3RenRate)) ? Number(row.lane3RenRate) : null
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
      motor3Rate: extra?.motor3Rate ?? racer?.motor3Rate ?? null,
      laneFirstRate: extra?.laneFirstRate ?? racer?.laneFirstRate ?? null,
      lane2RenRate: extra?.lane2RenRate ?? racer?.lane2RenRate ?? null,
      lane3RenRate: extra?.lane3RenRate ?? racer?.lane3RenRate ?? null
    };
  });
}

export async function fetchKyoteiBiyoriRaceData({ date, venueId, raceNo, timeoutMs = 12000 }) {
  const hd = String(date || "").replace(/-/g, "");
  const placeNo = String(venueId || "").padStart(2, "0");
  const rno = Number(raceNo);
  const indexUrl = `${KYOTEI_BIYORI_BASE}/race_ichiran.php?hiduke=${hd}&place_no=${placeNo}&race_no=${rno}`;

  const diagnostics = {
    index_url: indexUrl,
    target_urls: [],
    initial_html: {
      fetched: false,
      has_placeholder: false
    },
    tab_pages: []
  };

  let indexHtml = "";
  let lastError = null;

  try {
    indexHtml = await fetchHtml(indexUrl, timeoutMs);
    diagnostics.initial_html.fetched = true;
    diagnostics.initial_html.has_placeholder = /データ取得中です|しばらくお待ちください/.test(normalizeTableCellText(indexHtml));
  } catch (error) {
    lastError = error;
  }

  const discoveredLinks = indexHtml ? parseTabLinksFromIndex(indexHtml) : [];
  const sliderUrls = buildSliderUrls({ date, venueId, raceNo });
  const targetUrls = [...new Set([...discoveredLinks.map((row) => row.href), ...sliderUrls])];
  diagnostics.target_urls = targetUrls;

  const mergedByLane = new Map();
  const allTableDiagnostics = [];

  for (const url of targetUrls) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const html = await fetchHtml(url, timeoutMs);
        const contentFlags = classifyTabContent(html);
        const parsed = parseKyoteiBiyoriPreRaceData(html);
        const normalized = normalizeKyoteiBiyoriPreRaceFields(parsed);

        for (const [lane, row] of normalized.byLane.entries()) {
          const current = mergedByLane.get(lane) || {};
          mergedByLane.set(lane, {
            ...current,
            ...Object.fromEntries(
              Object.entries(row).filter(([, value]) => value !== null && value !== undefined && value !== "")
            )
          });
        }

        allTableDiagnostics.push(...(normalized.tableDiagnostics || []));
        diagnostics.tab_pages.push({
          url,
          attempt: attempt + 1,
          has_placeholder: contentFlags.hasInitialPlaceholder,
          has_lane_stats: contentFlags.hasLaneStats,
          has_pre_race: contentFlags.hasPreRace,
          parsed_lanes: normalized.byLane.size,
          populated_fields: normalized.fieldDiagnostics?.populated_fields || []
        });
        break;
      } catch (error) {
        lastError = error;
        if (attempt === 1) {
          diagnostics.tab_pages.push({
            url,
            attempt: attempt + 1,
            error: String(error?.message || error)
          });
        }
      }
    }
  }

  const fieldDiagnostics = buildFieldDiagnostics(mergedByLane);
  const ok = mergedByLane.size > 0 && fieldDiagnostics.populated_fields.length > 0;

  return {
    ok,
    url: indexUrl,
    triedUrls: targetUrls,
    byLane: mergedByLane,
    tableDiagnostics: allTableDiagnostics,
    fieldDiagnostics,
    fallbackUsed: !ok,
    fallbackReason: ok ? null : (lastError ? String(lastError.message || lastError) : "kyoteibiyori returned no usable fields"),
    diagnostics,
    error: ok ? null : (lastError ? String(lastError.message || lastError) : "kyoteibiyori returned no usable fields")
  };
}
