import axios from "axios";
import * as cheerio from "cheerio";
import {
  fetchKyoteiBiyoriRaceData,
  mergeKyoteiBiyoriDataIntoRaceContext
} from "./kyoteibiyori.js";

const BOATRACE_BASE = "https://www.boatrace.jp";
const BOATRACE_CACHE_TTL_MS = Number(process.env.BOATRACE_CACHE_TTL_MS || 45000);
const raceDataCache = new Map();

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

function toDecimal(value) {
  const text = normalizeDigits(String(value || "")).replace(/\s+/g, "");
  if (!text) return null;
  if (/^[+-]?\.\d+$/.test(text)) {
    const n = Number(text.replace(".", "0."));
    return Number.isFinite(n) ? n : null;
  }
  if (/^[+-]?\d+\.\d+$/.test(text)) {
    const n = Number(text);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function parseStartTimingRaw(value) {
  const raw = normalizeSpace(value) || null;
  if (!raw) {
    return {
      raw: null,
      type: "missing",
      numeric: null
    };
  }
  const normalized = normalizeDigits(String(raw)).replace(/\s+/g, "").toUpperCase();
  const fMatch = normalized.match(/^F\.?(\d{1,2})$/);
  if (fMatch) {
    const n = Number(fMatch[1]);
    return {
      raw,
      type: "flying",
      numeric: Number.isFinite(n) ? Number((-(n / 100)).toFixed(2)) : null
    };
  }
  const lMatch = normalized.match(/^L\.?(\d{1,2})$/);
  if (lMatch) {
    const n = Number(lMatch[1]);
    return {
      raw,
      type: "late",
      numeric: Number.isFinite(n) ? Number((-(n / 100)).toFixed(2)) : null
    };
  }

  const decimal = toDecimal(normalized);
  if (decimal !== null) {
    return {
      raw,
      type: "normal",
      numeric: Number(decimal.toFixed(2))
    };
  }

  return {
    raw,
    type: "unknown",
    numeric: null
  };
}

function extractFirstNumber(value) {
  const text = normalizeDigits(String(value || ""));
  const match = text.match(/\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function extractLinesFromCell($, cell) {
  const $clone = $(cell).clone();
  $clone.find("br").replaceWith("\n");

  return $clone
    .text()
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter(Boolean);
}

function extractAvgStForRow($, $cells) {
  const $stCellPrimary = $cells.filter("td.is-lineH2").eq(0);
  const stLinesPrimary = extractLinesFromCell($, $stCellPrimary);
  const avgStPrimary = toNumber(stLinesPrimary[2]);
  if (avgStPrimary !== null && avgStPrimary >= 0 && avgStPrimary < 1) {
    return {
      value: avgStPrimary,
      source: "primary:td.is-lineH2:eq(0)->line[2]"
    };
  }

  // Fallback: use row-scoped ST column cell by fixed index in racelist row structure.
  const $stCellByIndex = $cells.eq(3);
  const stLinesByIndex = extractLinesFromCell($, $stCellByIndex);
  const stTextByIndex = normalizeDigits(stLinesByIndex.join(" "));
  const decimalMatches = stTextByIndex.match(/\d+\.\d+/g) || [];
  const avgStFallback = decimalMatches
    .map((v) => Number(v))
    .find((v) => Number.isFinite(v) && v >= 0 && v < 1);

  if (avgStFallback !== undefined) {
    return {
      value: avgStFallback,
      source: "fallback:td:eq(3)->first_decimal_0_to_1"
    };
  }

  return {
    value: null,
    source: "failed:none"
  };
}

function extractFHoldCountFromRow($, $cells) {
  const $stCellPrimary = $cells.filter("td.is-lineH2").eq(0);
  const stLinesPrimary = extractLinesFromCell($, $stCellPrimary);
  const normalized = normalizeDigits(stLinesPrimary.join(" ")).replace(/\s+/g, " ").trim().toUpperCase();
  if (!normalized) return 0;

  const patterns = [
    /(?:^|\s)F\.?(\d{1,2})(?:\s|$)/,
    /F\s*[:/ ]\s*(\d{1,2})/,
    /F(\d{1,2})L\d{1,2}/
  ];
  for (const pattern of patterns) {
    const match = normalized.match(pattern);
    if (match) {
      const count = Number(match[1]);
      if (Number.isFinite(count) && count >= 0) return count;
    }
  }
  return 0;
}

function normalizeDate(dateInput) {
  const compact = String(dateInput).replace(/-/g, "");
  if (!/^\d{8}$/.test(compact)) {
    throw {
      statusCode: 400,
      code: "invalid_date",
      message: "date must be YYYY-MM-DD or YYYYMMDD"
    };
  }
  return compact;
}

function normalizeVenueId(venueIdInput) {
  const venue = Number(venueIdInput);
  if (!Number.isInteger(venue) || venue < 1 || venue > 24) {
    throw {
      statusCode: 400,
      code: "invalid_venue_id",
      message: "venueId must be an integer between 1 and 24"
    };
  }
  return String(venue).padStart(2, "0");
}

function normalizeRaceNo(raceNoInput) {
  const raceNo = Number(raceNoInput);
  if (!Number.isInteger(raceNo) || raceNo < 1 || raceNo > 12) {
    throw {
      statusCode: 400,
      code: "invalid_race_no",
      message: "raceNo must be an integer between 1 and 12"
    };
  }
  return raceNo;
}

async function fetchHtml(url, timeoutMs = 15000) {
  const { data } = await axios.get(url, {
    timeout: timeoutMs,
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });

  return data;
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function getCacheKey({ date, venueId, raceNo }) {
  return `${String(date)}_${String(venueId)}_${String(raceNo)}`;
}

function getCachedRaceData(params) {
  const key = getCacheKey(params);
  const row = raceDataCache.get(key);
  if (!row) return null;
  if (Date.now() > row.expiresAt) {
    raceDataCache.delete(key);
    return null;
  }
  return cloneJson(row.data);
}

function setCachedRaceData(params, data) {
  if (!Number.isFinite(BOATRACE_CACHE_TTL_MS) || BOATRACE_CACHE_TTL_MS <= 0) return;
  const key = getCacheKey(params);
  raceDataCache.set(key, {
    data: cloneJson(data),
    expiresAt: Date.now() + BOATRACE_CACHE_TTL_MS
  });
}

function missingRequiredFields(racer) {
  const missing = [];

  if (!Number.isInteger(racer.lane) || racer.lane < 1 || racer.lane > 6) missing.push("lane");
  if (!Number.isInteger(racer.registrationNo)) missing.push("registrationNo");
  if (!racer.name) missing.push("name");
  if (!racer.class) missing.push("class");
  if (!racer.branch) missing.push("branch");
  if (racer.age === null || Number.isNaN(racer.age)) missing.push("age");
  if (racer.weight === null || Number.isNaN(racer.weight)) missing.push("weight");

  return missing;
}

function parseRacersFromRacelist(html) {
  const $ = cheerio.load(html);

  const racerBodies = $(".table1.is-tableFixed__3rdadd table tbody.is-fs12");
  const debugRows = [];

  if (racerBodies.length !== 6) {
    throw {
      statusCode: 422,
      code: "invalid_racer_count",
      message: `Expected 6 racer body sections, found ${racerBodies.length}`,
      debug: {
        stage: "select_racer_bodies",
        selectors: {
          racerBodies: ".table1.is-tableFixed__3rdadd table tbody.is-fs12"
        },
        foundRacerBodyCount: racerBodies.length,
        rows: []
      }
    };
  }

  const racers = [];

  racerBodies.each((index, tbody) => {
    const rowIndex = index + 1;
    const $firstRow = $(tbody).children("tr").first();
    const $cells = $firstRow.children("td");

    const laneCellText = normalizeSpace($cells.filter("[class*='is-boatColor']").first().text());
    const lane = toNumber(normalizeDigits(laneCellText));

    const $profileCell = $cells.eq(2);
    const $regClass = $profileCell.find("div.is-fs11").first();
    const $name = $profileCell.find("div.is-fs18.is-fBold a").first();
    const $branchAge = $profileCell.find("div.is-fs11").eq(1);

    const regClassText = normalizeSpace($regClass.text());
    const registrationNo = extractFirstNumber(regClassText);
    const racerClass = normalizeSpace($regClass.find("span").first().text()) || null;
    const name = normalizeSpace($name.text()) || null;

    const profileLines = extractLinesFromCell($, $branchAge);
    const branchLine = profileLines[0] || "";
    const ageWeightLine = profileLines[1] || "";

    const branch = normalizeSpace(branchLine.split("/")[0]) || null;

    const ageWeightMatch = normalizeDigits(ageWeightLine).match(/(\d+)\u6B73\/(\d+(?:\.\d+)?)kg/);
    const age = ageWeightMatch ? Number(ageWeightMatch[1]) : null;
    const weight = ageWeightMatch ? Number(ageWeightMatch[2]) : null;

    const $stats = $cells.filter("td.is-lineH2");
    const stLines = extractLinesFromCell($, $stats.eq(0));
    const nationwideLines = extractLinesFromCell($, $stats.eq(1));
    const localLines = extractLinesFromCell($, $stats.eq(2));
    const motorLines = extractLinesFromCell($, $stats.eq(3));
    const boatLines = extractLinesFromCell($, $stats.eq(4));
    const avgStResult = extractAvgStForRow($, $cells);
    const fHoldCount = extractFHoldCountFromRow($, $cells);

    const parsed = {
      lane,
      registrationNo,
      name,
      class: racerClass,
      branch,
      age,
      weight,
      fHoldCount,
      avgSt: avgStResult.value,
      nationwideWinRate: toNumber(nationwideLines[0]),
      localWinRate: toNumber(localLines[0]),
      motor2Rate: toNumber(motorLines[1]),
      boat2Rate: toNumber(boatLines[1])
    };

    const raw = {
      rowIndex,
      laneCellText,
      regClassText,
      classText: normalizeSpace($regClass.find("span").first().text()),
      nameText: normalizeSpace($name.text()),
      profileLines,
      stLines,
      nationwideLines,
      localLines,
      motorLines,
      boatLines,
      avgStSource: avgStResult.source,
      tdCount: $cells.length,
      statCellCount: $stats.length
    };

    const missingFields = missingRequiredFields(parsed);

    debugRows.push({
      rowIndex,
      raw,
      parsed,
      missingFields
    });

    racers.push(parsed);
  });

  const sorted = [...racers].sort((a, b) => a.lane - b.lane);
  const invalidRows = debugRows.filter((r) => r.missingFields.length > 0);

  if (invalidRows.length > 0 || sorted.length !== 6) {
    throw {
      statusCode: 422,
      code: "invalid_racer_count",
      message: `Expected exactly 6 complete racers, parsed ${6 - invalidRows.length}`,
      debug: {
        stage: "parse_racer_rows",
        selectors: {
          racerBodies: ".table1.is-tableFixed__3rdadd table tbody.is-fs12",
          laneCell: "td[class*='is-boatColor']",
          profileCell: "td:eq(2)",
          regClass: "div.is-fs11:first",
          name: "div.is-fs18.is-fBold a",
          branchAge: "div.is-fs11:eq(1)",
          statCells: "td.is-lineH2"
        },
        foundRacerBodyCount: racerBodies.length,
        rows: debugRows,
        failedRows: invalidRows.map((r) => ({
          rowIndex: r.rowIndex,
          missingFields: r.missingFields,
          parsed: r.parsed
        }))
      }
    };
  }

  return {
    racers: sorted,
    debugRows
  };
}

function parseBeforeinfo(html) {
  const $ = cheerio.load(html);

  const byLane = new Map();
  const rawByLane = new Map();
  const beforeRows = $("table.is-w748 tbody.is-fs12");

  beforeRows.each((_, tbody) => {
    const $tbody = $(tbody);
    const $row1 = $tbody.children("tr").eq(0);
    const $row2 = $tbody.children("tr").eq(1);
    const $row3 = $tbody.children("tr").eq(2);
    const $cells1 = $row1.children("td");

    const lane = toNumber(normalizeDigits($cells1.filter("[class*='is-boatColor']").first().text()));
    if (!lane) return;

    const exhibitionTime = toNumber($cells1.eq(4).text());
    const tilt = toNumber($cells1.eq(5).text());

    const row2Cells = $row2.children("td");
    const entryCourseFromRow = toNumber(row2Cells.eq(1).text());

    const row3Cells = $row3.children("td");
    const exhibitionStRawFromRow = normalizeSpace(row3Cells.eq(2).text()) || null;
    const stParsedFromRow = parseStartTimingRaw(exhibitionStRawFromRow);
    const exhibitionStFromRow = stParsedFromRow.type === "normal" ? stParsedFromRow.numeric : null;

    byLane.set(lane, {
      exhibitionTime,
      entryCourse: entryCourseFromRow,
      exhibitionSt: exhibitionStFromRow,
      exhibitionStRaw: exhibitionStRawFromRow,
      exhibitionStType: stParsedFromRow.type,
      exhibitionStNumeric: stParsedFromRow.numeric,
      tilt
    });
    rawByLane.set(lane, {
      lane,
      rawEntryCourse: normalizeSpace(row2Cells.eq(1).text()) || null,
      rawSt: exhibitionStRawFromRow,
      rawExhibitionTime: normalizeSpace($cells1.eq(4).text()) || null
    });
  });

  // Fallback source for entry course / exhibition ST from "スタート展示" block.
  const startRows = $(".table1_boatImage1");
  startRows.each((idx, el) => {
    const $el = $(el);
    const lane = toNumber($el.find(".table1_boatImage1Number").first().text());
    if (!lane) return;

    const exhibitionStRaw = normalizeSpace($el.find(".table1_boatImage1Time").first().text()) || null;
    const stParsed = parseStartTimingRaw(exhibitionStRaw);
    const exhibitionSt = stParsed.type === "normal" ? stParsed.numeric : null;
    const entryCourse = idx + 1;

    const current = byLane.get(lane) || {};
    // If the dedicated start-exhibition block has a valid ST token, always prefer it.
    // This avoids keeping generic ST-like values from other rows.
    const shouldPreferStartRowSt = stParsed.type !== "missing" && stParsed.type !== "unknown";
    byLane.set(lane, {
      ...current,
      entryCourse: current.entryCourse ?? entryCourse,
      exhibitionSt: shouldPreferStartRowSt ? exhibitionSt : current.exhibitionSt ?? exhibitionSt,
      exhibitionStRaw: shouldPreferStartRowSt ? exhibitionStRaw : current.exhibitionStRaw ?? exhibitionStRaw,
      exhibitionStType: shouldPreferStartRowSt ? stParsed.type : current.exhibitionStType ?? stParsed.type,
      exhibitionStNumeric: shouldPreferStartRowSt
        ? stParsed.numeric
        : current.exhibitionStNumeric ?? stParsed.numeric
    });
    const rawCurrent = rawByLane.get(lane) || { lane };
    rawByLane.set(lane, {
      ...rawCurrent,
      fallbackRawSt: exhibitionStRaw,
      fallbackEntryCourse: entryCourse
    });
  });

  const weather = normalizeSpace($(".weather1 .weather1_bodyUnit.is-weather .weather1_bodyUnitLabelTitle").first().text()) || null;
  const windSpeed = extractFirstNumber($(".weather1 .weather1_bodyUnit.is-wind .weather1_bodyUnitLabelData").first().text());
  const waveHeight = extractFirstNumber($(".weather1 .weather1_bodyUnit.is-wave .weather1_bodyUnitLabelData").first().text());
  const windDirectionClass = $(".weather1 .weather1_bodyUnit.is-windDirection .weather1_bodyUnitImage").attr("class") || "";
  const windDirectionMatch = windDirectionClass.match(/is-wind(\d+)/);
  const windDirection = windDirectionMatch ? `is-wind${windDirectionMatch[1]}` : null;

  return {
    byLane,
    rawByLane,
    weather: {
      weather,
      windSpeed,
      windDirection,
      waveHeight
    }
  };
}

export async function getRaceData({ date, venueId, raceNo, timeoutMs = 15000, forceRefresh = false }) {
  const cached = forceRefresh ? null : getCachedRaceData({ date, venueId, raceNo });
  if (cached) {
    return {
      ...cached,
      source: {
        ...(cached.source || {}),
        cache: {
          hit: true,
          ttl_ms: BOATRACE_CACHE_TTL_MS
        }
      }
    };
  }

  const hd = normalizeDate(date);
  const jcd = normalizeVenueId(venueId);
  const rno = normalizeRaceNo(raceNo);

  const racelistUrl = `${BOATRACE_BASE}/owpc/pc/race/racelist?rno=${rno}&jcd=${jcd}&hd=${hd}`;
  const beforeinfoUrl = `${BOATRACE_BASE}/owpc/pc/race/beforeinfo?rno=${rno}&jcd=${jcd}&hd=${hd}`;

  const [racelistHtml, beforeinfoHtml] = await Promise.all([
    fetchHtml(racelistUrl, timeoutMs),
    fetchHtml(beforeinfoUrl, timeoutMs)
  ]);

  const { racers } = parseRacersFromRacelist(racelistHtml);
  const beforeinfo = parseBeforeinfo(beforeinfoHtml);

  const mergedRacers = racers.map((racer) => {
    const b = beforeinfo.byLane.get(racer.lane) || {};
    const raw = beforeinfo.rawByLane.get(racer.lane) || {};
    return {
      ...racer,
      exhibitionTime: b.exhibitionTime ?? null,
      exhibitionSt: b.exhibitionSt ?? null,
      exhibitionStRaw: b.exhibitionStRaw ?? null,
      exhibitionStType: b.exhibitionStType ?? null,
      exhibitionStNumeric: b.exhibitionStNumeric ?? null,
      entryCourse: b.entryCourse ?? null,
      tilt: b.tilt ?? null,
      startRaw: raw
    };
  });
  let kyoteiBiyori = {
    ok: false,
    url: null,
    byLane: new Map(),
    tableDiagnostics: [],
    fallbackUsed: true,
    error: null
  };
  try {
    kyoteiBiyori = await fetchKyoteiBiyoriRaceData({
      date,
      venueId,
      raceNo,
      timeoutMs
    });
  } catch (error) {
    kyoteiBiyori = {
      ok: false,
      url: null,
      byLane: new Map(),
      tableDiagnostics: [],
      fallbackUsed: true,
      error: String(error?.message || error)
    };
  }
  const mergedWithKyoteiBiyori = mergeKyoteiBiyoriDataIntoRaceContext({
    racers: mergedRacers,
    kyoteiBiyori
  });

  const result = {
    source: {
      racelistUrl,
      beforeinfoUrl,
      kyotei_biyori: {
        ok: !!kyoteiBiyori?.ok,
        url: kyoteiBiyori?.url || null,
        tried_urls: Array.isArray(kyoteiBiyori?.triedUrls) ? kyoteiBiyori.triedUrls : [],
        fallback_used: !!kyoteiBiyori?.fallbackUsed,
        fallback_reason: kyoteiBiyori?.fallbackReason || null,
        table_diagnostics: kyoteiBiyori?.tableDiagnostics || [],
        field_diagnostics: kyoteiBiyori?.fieldDiagnostics || {
          populated_fields: [],
          failed_fields: [],
          per_lane: []
        },
        error: kyoteiBiyori?.error || null
      },
      start_display_source: "official_pre_race_info",
      fetched_at: new Date().toISOString(),
      cache: {
        hit: false,
        ttl_ms: BOATRACE_CACHE_TTL_MS
      }
    },
    race: {
      date: `${hd.slice(0, 4)}-${hd.slice(4, 6)}-${hd.slice(6, 8)}`,
      venueId: Number(venueId),
      raceNo: rno,
      weather: beforeinfo.weather.weather,
      windSpeed: beforeinfo.weather.windSpeed,
      windDirection: beforeinfo.weather.windDirection,
      waveHeight: beforeinfo.weather.waveHeight
    },
    racers: mergedWithKyoteiBiyori
  };
  setCachedRaceData({ date, venueId, raceNo }, result);
  return result;
}
