import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import axios from "axios";
import * as cheerio from "cheerio";
import { saveRaceResult } from "../../save-result.js";
import { saveRaceStartDisplayResult } from "../../race-start-display-store.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const RESULT_DEBUG_DIR = path.resolve(__dirname, "../../debug/result-parser");

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function normalizeDigits(value) {
  return String(value || "").replace(/[\uFF10-\uFF19]/g, (ch) =>
    String.fromCharCode(ch.charCodeAt(0) - 0xfee0)
  );
}

function normalizeSpace(value) {
  return normalizeDigits(value).replace(/\s+/g, " ").trim();
}

function normalizeCombo(value) {
  const digits = normalizeDigits(value).match(/[1-6]/g) || [];
  const top3 = digits.slice(0, 3);
  return top3.length === 3 && new Set(top3).size === 3 ? top3.join("-") : null;
}

function parseComboFromText(text) {
  const cleaned = normalizeSpace(text);
  const separated = cleaned.match(/([1-6])\D+([1-6])\D+([1-6])/);
  if (separated) return `${separated[1]}-${separated[2]}-${separated[3]}`;

  const compact = cleaned.match(/(?:^|\D)([1-6]{3})(?:\D|$)/);
  if (!compact) return null;
  const lanes = compact[1].split("");
  return new Set(lanes).size === 3 ? lanes.join("-") : null;
}

function parsePayoutFromText(text) {
  const cleaned = normalizeSpace(text).replace(/,/g, "");
  const yenMatch = cleaned.match(/(\d{2,8})\s*(?:yen|\u5186)/i);
  const fallbackMatch = cleaned.match(/(?:^|\D)(\d{2,8})(?:\D|$)/);
  const match = yenMatch || fallbackMatch;
  if (!match) return null;
  const payout = Number(match[1]);
  return Number.isFinite(payout) ? payout : null;
}

function parseLaneFromResultCell($cell, index = null) {
  const classAttr = String($cell.find("div").attr("class") || $cell.attr("class") || "");
  const classMatch = classAttr.match(/ng3r([1-6])/);
  if (classMatch) return Number(classMatch[1]);

  const dataAttr = Number.isInteger(index) ? $cell.attr(`data-rank-${index}`) : null;
  const dataMatch = String(dataAttr || "").match(/[1-6]/);
  if (dataMatch) return Number(dataMatch[0]);

  const textMatch = normalizeSpace($cell.text()).match(/[1-6]/);
  return textMatch ? Number(textMatch[0]) : null;
}

function resultLabelRegex() {
  return /(?:3\s*(?:\u9023\s*\u5358|rentan|trifecta)|\u4e09\s*\u9023\s*\u5358|3\s*ren\s*tan)/i;
}

function isRaceResultText(text) {
  const cleaned = normalizeSpace(text);
  return resultLabelRegex().test(cleaned) || /winning\s*trifecta/i.test(cleaned);
}

function top3FromCombo(combo) {
  const top3 = String(combo || "").split("-").map((value) => Number(value));
  return top3.length === 3 && top3.every((value) => Number.isInteger(value) && value >= 1 && value <= 6)
    ? top3
    : null;
}

function parseRowsForTrifecta($) {
  let parsed = null;
  let matchedSelectorCount = 0;

  $("table tr, .table1 tr, .is-result tr").each((_, tr) => {
    if (parsed) return false;
    const $row = $(tr);
    const rowText = normalizeSpace($row.text());
    if (!isRaceResultText(rowText)) return;
    matchedSelectorCount += 1;

    const cells = $row.children("th,td").toArray().map((cell) => normalizeSpace($(cell).text()));
    const combo =
      cells.map(parseComboFromText).find(Boolean) ||
      parseComboFromText(rowText);
    if (!combo) return;

    const payout =
      cells.map(parsePayoutFromText).find((value) => Number.isFinite(value) && value > 0) ||
      parsePayoutFromText(rowText);
    parsed = {
      top3: top3FromCombo(combo),
      combo,
      payout3t: Number.isFinite(payout) ? payout : null,
      parserStage: "trifecta_label_row"
    };
    return false;
  });

  return { parsed, matchedSelectorCount };
}

function parseResultOrderTable($) {
  const rows = [];
  $("table tr").each((_, tr) => {
    const cells = $(tr).children("th,td");
    const text = normalizeSpace($(tr).text());
    if (!cells.length || !/[1-6]/.test(text)) return;

    const rankText = normalizeSpace(cells.eq(0).text());
    const rank = toInt(rankText.match(/\d+/)?.[0], null);
    if (!Number.isInteger(rank) || rank < 1 || rank > 3) return;

    const lane =
      parseLaneFromResultCell(cells.eq(1), rank) ||
      parseLaneFromResultCell(cells.eq(2), rank) ||
      parseLaneFromResultCell(cells.eq(0), rank);
    if (!Number.isInteger(lane) || lane < 1 || lane > 6) return;
    rows.push({ rank, lane });
  });

  const top3 = [1, 2, 3].map((rank) => rows.find((row) => row.rank === rank)?.lane);
  if (top3.length !== 3 || top3.some((lane) => !Number.isInteger(lane)) || new Set(top3).size !== 3) {
    return null;
  }
  return {
    top3,
    combo: top3.join("-"),
    payout3t: null,
    parserStage: "arrival_order_table"
  };
}

function parseBodyFallback($) {
  const bodyText = normalizeSpace($("body").text());
  const labelIndex = bodyText.search(resultLabelRegex());
  const target = labelIndex >= 0 ? bodyText.slice(labelIndex, labelIndex + 240) : bodyText;
  const combo = parseComboFromText(target);
  if (!combo) return null;
  return {
    top3: top3FromCombo(combo),
    combo,
    payout3t: parsePayoutFromText(target),
    parserStage: labelIndex >= 0 ? "trifecta_body_label" : "body_digit_fallback"
  };
}

export function parseResultFromRaceresultHtml(html) {
  const $ = cheerio.load(String(html || ""));
  const rowParse = parseRowsForTrifecta($);
  const parsed =
    rowParse.parsed ||
    parseResultOrderTable($) ||
    parseBodyFallback($);

  if (!parsed?.top3 || !parsed?.combo) {
    return {
      result: null,
      parserStage: rowParse.matchedSelectorCount > 0 ? "trifecta_label_parse_failed" : "no_result_selector_matched",
      matchedSelectorCount: rowParse.matchedSelectorCount
    };
  }

  return {
    result: {
      top3: parsed.top3,
      combo: parsed.combo,
      payout3t: Number.isFinite(parsed.payout3t) ? parsed.payout3t : null
    },
    parserStage: parsed.parserStage,
    matchedSelectorCount: Math.max(rowParse.matchedSelectorCount, 1)
  };
}

export function parseResultFromDailySummaryHtml(html, { venueId, raceNo } = {}) {
  const targetVenueId = toInt(venueId, null);
  const targetRaceNo = toInt(raceNo, null);
  if (!Number.isInteger(targetVenueId) || !Number.isInteger(targetRaceNo)) {
    return { result: null, parserStage: "invalid_daily_summary_key", matchedSelectorCount: 0 };
  }

  const $ = cheerio.load(String(html || ""));
  let parsed = null;
  let matchedSelectorCount = 0;

  $("table").each((_, table) => {
    if (parsed) return false;
    const $table = $(table);
    const headingText = normalizeSpace($table.find("tr").first().text());
    const headingVenueMatch =
      headingText.match(/#\s*(\d{1,2})/) ||
      headingText.match(/(?:jcd|venue)\D{0,8}(\d{1,2})/i);
    if (headingVenueMatch && Number(headingVenueMatch[1]) !== targetVenueId) return;

    $table.find("tr").each((__, row) => {
      if (parsed) return false;
      const $cells = $(row).children("th,td");
      if ($cells.length < 4) return;

      const rowRaceNo = toInt(normalizeSpace($cells.eq(0).text()).match(/\d+/)?.[0], null);
      if (rowRaceNo !== targetRaceNo) return;
      matchedSelectorCount += 1;

      const top3 = [1, 2, 3]
        .map((rankIndex) => parseLaneFromResultCell($cells.eq(rankIndex), rankIndex))
        .filter(Number.isInteger);
      const normalizedTop3 = top3.length === 3 && new Set(top3).size === 3 ? top3 : null;
      const rowText = normalizeSpace($(row).text());
      const combo = normalizedTop3 ? normalizedTop3.join("-") : parseComboFromText(rowText);
      const payout3t =
        parsePayoutFromText($cells.eq(4).text()) ??
        parsePayoutFromText(rowText);
      if (!combo) return;

      parsed = {
        top3: top3FromCombo(combo),
        combo,
        payout3t: Number.isFinite(payout3t) ? payout3t : null
      };
      return false;
    });
  });

  return {
    result: parsed,
    parserStage: parsed ? "daily_summary_row" : "daily_summary_no_matching_row",
    matchedSelectorCount
  };
}

function buildResultUrls({ date, venueId, raceNo }) {
  const hd = String(date || "").replace(/-/g, "");
  const jcd = String(venueId).padStart(2, "0");
  const rno = Number(raceNo);
  if (!/^\d{8}$/.test(hd) || !Number.isInteger(Number(venueId)) || !Number.isInteger(rno)) return null;
  return {
    hd,
    jcd,
    rno,
    urls: {
      official_raceresult_page: `https://www.boatrace.jp/owpc/pc/race/raceresult?rno=${rno}&jcd=${jcd}&hd=${hd}`,
      official_resultlist_page: `https://www.boatrace.jp/owpc/pc/race/resultlist?jcd=${jcd}&hd=${hd}`,
      daily_result_summary_page: `https://race.kyotei24.jp/result-${hd}.html`
    }
  };
}

function safeDebugSegment(value) {
  return String(value || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 80);
}

function saveRawResultDebug({ source, date, venueId, raceNo, raw }) {
  if (!raw) return null;
  fs.mkdirSync(RESULT_DEBUG_DIR, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const fileName = [
    String(date || "").replace(/-/g, "") || "unknown_date",
    String(venueId || "unknown_venue").padStart(2, "0"),
    String(raceNo || "unknown_race"),
    safeDebugSegment(source),
    stamp
  ].join("_");
  const extension = String(raw).trim().startsWith("{") ? "json" : "html";
  const fullPath = path.join(RESULT_DEBUG_DIR, `${fileName}.${extension}`);
  fs.writeFileSync(fullPath, String(raw), "utf8");
  return fullPath;
}

async function fetchText(url, { timeoutMs, httpGet }) {
  const response = await httpGet(url, {
    timeout: timeoutMs,
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });
  return response?.data ?? "";
}

function makeDebug(attempts) {
  const lastAttempt = [...attempts].reverse().find(Boolean) || {};
  return {
    resultFetchUrls: attempts.map((attempt) => attempt.url).filter(Boolean),
    resultParserStage: lastAttempt.parserStage || lastAttempt.stage || null,
    resultMatchedSelectorCount: lastAttempt.matchedSelectorCount ?? 0,
    resultRawSavedPath: lastAttempt.rawSavedPath || null,
    resultFetchAttempts: attempts
  };
}

async function runSource({
  source,
  url,
  parser,
  parserArgs,
  date,
  venueId,
  raceNo,
  timeoutMs,
  httpGet,
  saveRawOnSuccess = false
}) {
  const attempt = {
    source,
    url,
    stage: "fetch_start",
    parserStage: null,
    matchedSelectorCount: 0,
    rawSavedPath: null,
    ok: false,
    error: null
  };

  try {
    const raw = await fetchText(url, { timeoutMs, httpGet });
    attempt.stage = "parse_start";
    const parsed = parser(raw, parserArgs);
    attempt.parserStage = parsed?.parserStage || "parse_complete";
    attempt.matchedSelectorCount = parsed?.matchedSelectorCount ?? 0;
    if (!parsed?.result?.top3) {
      attempt.stage = "parse_failed";
      attempt.rawSavedPath = saveRawResultDebug({ source, date, venueId, raceNo, raw });
      return { official: null, attempt };
    }

    attempt.stage = "parsed";
    attempt.ok = true;
    if (saveRawOnSuccess) {
      attempt.rawSavedPath = saveRawResultDebug({ source, date, venueId, raceNo, raw });
    }
    return {
      official: {
        url,
        raw,
        top3: parsed.result.top3,
        winningTrifecta: parsed.result.combo,
        payout3t: parsed.result.payout3t ?? null,
        source
      },
      attempt
    };
  } catch (error) {
    attempt.stage = "fetch_failed";
    attempt.error = String(error?.message || error);
    const raw = error?.response?.data;
    if (raw) {
      attempt.rawSavedPath = saveRawResultDebug({ source, date, venueId, raceNo, raw });
    }
    return { official: null, attempt };
  }
}

export async function fetchOfficialRaceResultPage({
  date,
  venueId,
  raceNo,
  timeoutMs = 6000,
  httpGet = axios.get
} = {}) {
  const built = buildResultUrls({ date, venueId, raceNo });
  if (!built) return null;
  const { official } = await runSource({
    source: "official_raceresult_page",
    url: built.urls.official_raceresult_page,
    parser: parseResultFromRaceresultHtml,
    parserArgs: {},
    date,
    venueId,
    raceNo,
    timeoutMs,
    httpGet
  });
  return official;
}

export async function fetchOfficialRaceResultFromDailySummary({
  date,
  venueId,
  raceNo,
  timeoutMs = 6000,
  httpGet = axios.get
} = {}) {
  const built = buildResultUrls({ date, venueId, raceNo });
  if (!built) return null;
  const { official } = await runSource({
    source: "daily_result_summary_page",
    url: built.urls.daily_result_summary_page,
    parser: parseResultFromDailySummaryHtml,
    parserArgs: { venueId, raceNo },
    date,
    venueId,
    raceNo,
    timeoutMs,
    httpGet
  });
  return official;
}

export async function fetchAndStoreOfficialRaceResult({
  raceId,
  date,
  venueId,
  raceNo,
  timeoutMs = 6000,
  httpGet = axios.get,
  saveRawOnSuccess = false
} = {}) {
  const built = buildResultUrls({ date, venueId, raceNo });
  if (!built) {
    return {
      actualTop3: null,
      winningTrifecta: null,
      source: null,
      resultFetchDebug: {
        resultFetchUrls: [],
        resultParserStage: "invalid_result_fetch_key",
        resultMatchedSelectorCount: 0,
        resultRawSavedPath: null,
        resultFetchAttempts: []
      }
    };
  }

  const sourcePlan = [
    {
      source: "official_raceresult_page",
      url: built.urls.official_raceresult_page,
      parser: parseResultFromRaceresultHtml,
      parserArgs: {}
    },
    {
      source: "official_resultlist_page",
      url: built.urls.official_resultlist_page,
      parser: parseResultFromDailySummaryHtml,
      parserArgs: { venueId, raceNo }
    },
    {
      source: "daily_result_summary_page",
      url: built.urls.daily_result_summary_page,
      parser: parseResultFromDailySummaryHtml,
      parserArgs: { venueId, raceNo }
    }
  ];

  const attempts = [];
  let official = null;
  for (const sourceSpec of sourcePlan) {
    const outcome = await runSource({
      ...sourceSpec,
      date,
      venueId,
      raceNo,
      timeoutMs,
      httpGet,
      saveRawOnSuccess
    });
    attempts.push(outcome.attempt);
    if (outcome.official?.top3) {
      official = outcome.official;
      break;
    }
  }

  const resultFetchDebug = makeDebug(attempts);
  if (!official?.top3) {
    return {
      actualTop3: null,
      winningTrifecta: null,
      source: null,
      resultFetchDebug
    };
  }

  const normalizedRaceId =
    raceId ||
    `${String(date || "").replace(/-/g, "")}_${Number(venueId)}_${Number(raceNo)}`;

  saveRaceResult({
    raceId: normalizedRaceId,
    finishOrder: official.top3,
    payout3t: official.payout3t
  });
  try {
    saveRaceStartDisplayResult({
      raceId: normalizedRaceId,
      fetchedResult: official.winningTrifecta,
      settledResult: official.winningTrifecta
    });
  } catch (error) {
    resultFetchDebug.resultStartDisplaySaveError = String(error?.message || error);
  }

  return {
    raceId: normalizedRaceId,
    actualTop3: official.top3,
    winningTrifecta: official.winningTrifecta,
    actualResult: official.winningTrifecta,
    result: official.winningTrifecta,
    payout3t: official.payout3t ?? null,
    source: official.source || "official_raceresult_page",
    url: official.url,
    resultFetchDebug,
    resultFetchUrls: resultFetchDebug.resultFetchUrls,
    resultParserStage: resultFetchDebug.resultParserStage,
    resultMatchedSelectorCount: resultFetchDebug.resultMatchedSelectorCount,
    resultRawSavedPath: resultFetchDebug.resultRawSavedPath
  };
}
