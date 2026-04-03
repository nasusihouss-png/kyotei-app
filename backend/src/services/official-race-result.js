import axios from "axios";
import * as cheerio from "cheerio";
import { saveRaceResult } from "../../save-result.js";
import { saveRaceStartDisplayResult } from "../../race-start-display-store.js";

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
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function parseLaneFromResultCell($cell, index = null) {
  const classAttr = String($cell.find("div").attr("class") || $cell.attr("class") || "");
  const classMatch = classAttr.match(/ng3r([1-6])/);
  if (classMatch) return Number(classMatch[1]);
  const textMatch = String($cell.text() || "").match(/[1-6]/);
  if (textMatch) return Number(textMatch[0]);
  if (Number.isInteger(index) && index >= 1 && index <= 3) {
    const dataAttr = $cell.attr(`data-rank-${index}`);
    const dataMatch = String(dataAttr || "").match(/[1-6]/);
    if (dataMatch) return Number(dataMatch[0]);
  }
  return null;
}

function parseComboFromText(text) {
  const cleaned = normalizeDigits(text);
  const match = cleaned.match(/([1-6])\D+([1-6])\D+([1-6])/);
  if (!match) return null;
  return `${match[1]}-${match[2]}-${match[3]}`;
}

function parsePayoutFromText(text) {
  const cleaned = normalizeDigits(text).replace(/,/g, "");
  const yenMatch = cleaned.match(/(\d+)\s*円/);
  if (yenMatch) {
    const payout = Number(yenMatch[1]);
    return Number.isFinite(payout) ? payout : null;
  }
  return null;
}

function parseAmountFromText(text) {
  const cleaned = normalizeDigits(text).replace(/,/g, "");
  const match = cleaned.match(/(\d+)/);
  if (!match) return null;
  const amount = Number(match[1]);
  return Number.isFinite(amount) ? amount : null;
}

function looksLikeOfficialResultPage(html) {
  const bodyText = normalizeSpace(normalizeDigits(cheerio.load(html)("body").text()));
  return /3\s*連\s*単|払戻金|レース結果/.test(bodyText);
}

export function parseResultFromRaceresultHtml(html) {
  const $ = cheerio.load(html);
  let combo = null;
  let payout3t = null;

  $("table tr").each((_, tr) => {
    if (combo) return false;
    const cells = $(tr).children("th,td");
    if (!cells.length) return;
    const heading = normalizeSpace(cells.eq(0).text());
    if (!/3\s*連\s*単/.test(heading)) return;

    const comboCellText = normalizeSpace(cells.eq(1).text());
    const rowText = normalizeSpace($(tr).text());
    const parsedCombo = parseComboFromText(comboCellText) || parseComboFromText(rowText);
    if (!parsedCombo) return;

    combo = parsedCombo;
    payout3t = parsePayoutFromText(normalizeSpace(cells.eq(2).text()) || rowText);
    return false;
  });

  if (!combo) {
    const bodyText = normalizeSpace(normalizeDigits($("body").text()));
    const match = bodyText.match(
      /3\s*連\s*単[^0-9]{0,80}([1-6])\D+([1-6])\D+([1-6])[^0-9]{0,80}(\d[\d,]*)\s*円/
    );
    if (match) {
      combo = `${match[1]}-${match[2]}-${match[3]}`;
      payout3t = Number(String(match[4]).replace(/,/g, ""));
    }
  }

  if (!combo) return null;
  const top3 = combo.split("-").map((value) => Number(value));
  if (top3.length !== 3 || top3.some((value) => !Number.isInteger(value))) return null;

  return {
    top3,
    combo,
    payout3t: Number.isFinite(payout3t) ? payout3t : null
  };
}

export function parseResultFromDailySummaryHtml(html, { venueId, raceNo } = {}) {
  const targetVenueId = toInt(venueId, null);
  const targetRaceNo = toInt(raceNo, null);
  if (!Number.isInteger(targetVenueId) || !Number.isInteger(targetRaceNo)) return null;

  const $ = cheerio.load(html);
  let parsed = null;

  $("table").each((_, table) => {
    if (parsed) return false;
    const $table = $(table);
    const headingText = normalizeSpace($table.find("tr").first().text());
    const headingVenueMatch = headingText.match(/#\s*(\d{1,2})/);
    if (!headingVenueMatch || Number(headingVenueMatch[1]) !== targetVenueId) return;

    $table.find("tr").slice(2).each((__, row) => {
      if (parsed) return false;
      const $cells = $(row).children("th,td");
      if ($cells.length < 5) return;

      const rowRaceNo = toInt($cells.eq(0).text(), null);
      if (rowRaceNo !== targetRaceNo) return;

      const top3 = [1, 2, 3]
        .map((rankIndex) => parseLaneFromResultCell($cells.eq(rankIndex), rankIndex))
        .filter(Number.isInteger);
      const normalizedTop3 = top3.length === 3 && new Set(top3).size === 3 ? top3 : null;
      const payout3t = parsePayoutFromText($cells.eq(4).text()) ?? parseAmountFromText($cells.eq(4).text());
      if (!normalizedTop3) return;

      parsed = {
        top3: normalizedTop3,
        combo: normalizedTop3.join("-"),
        payout3t: Number.isFinite(payout3t) ? payout3t : null
      };
      return false;
    });
  });

  return parsed;
}

export async function fetchOfficialRaceResultPage({ date, venueId, raceNo, timeoutMs = 6000 } = {}) {
  const hd = String(date || "").replace(/-/g, "");
  const jcd = String(venueId).padStart(2, "0");
  const rno = Number(raceNo);
  if (!/^\d{8}$/.test(hd) || !Number.isInteger(Number(venueId)) || !Number.isInteger(rno)) return null;

  const url = `https://www.boatrace.jp/owpc/pc/race/raceresult?rno=${rno}&jcd=${jcd}&hd=${hd}`;
  const { data } = await axios.get(url, {
    timeout: timeoutMs,
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });

  if (!looksLikeOfficialResultPage(data)) return null;
  const parsed = parseResultFromRaceresultHtml(data);
  if (!parsed) return null;

  return {
    url,
    raw: data,
    top3: parsed.top3,
    winningTrifecta: parsed.combo,
    payout3t: parsed.payout3t ?? null
  };
}

export async function fetchOfficialRaceResultFromDailySummary({ date, venueId, raceNo, timeoutMs = 6000 } = {}) {
  const hd = String(date || "").replace(/-/g, "");
  const normalizedVenueId = toInt(venueId, null);
  const normalizedRaceNo = toInt(raceNo, null);
  if (!/^\d{8}$/.test(hd) || !Number.isInteger(normalizedVenueId) || !Number.isInteger(normalizedRaceNo)) {
    return null;
  }

  const url = `https://race.kyotei24.jp/result-${hd}.html`;
  const { data } = await axios.get(url, {
    timeout: timeoutMs,
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });

  const parsed = parseResultFromDailySummaryHtml(data, {
    venueId: normalizedVenueId,
    raceNo: normalizedRaceNo
  });
  if (!parsed) return null;

  return {
    url,
    raw: data,
    top3: parsed.top3,
    winningTrifecta: parsed.combo,
    payout3t: parsed.payout3t ?? null,
    source: "daily_result_summary_page"
  };
}

export async function fetchAndStoreOfficialRaceResult({
  raceId,
  date,
  venueId,
  raceNo,
  timeoutMs = 6000
} = {}) {
  const official =
    await fetchOfficialRaceResultPage({ date, venueId, raceNo, timeoutMs }) ||
    await fetchOfficialRaceResultFromDailySummary({ date, venueId, raceNo, timeoutMs });
  if (!official?.top3) return null;

  const normalizedRaceId =
    raceId ||
    `${String(date || "").replace(/-/g, "")}_${Number(venueId)}_${Number(raceNo)}`;

  saveRaceResult({
    raceId: normalizedRaceId,
    finishOrder: official.top3,
    payout3t: official.payout3t
  });
  saveRaceStartDisplayResult({
    raceId: normalizedRaceId,
    fetchedResult: official.winningTrifecta,
    settledResult: official.winningTrifecta
  });

  return {
    raceId: normalizedRaceId,
    actualTop3: official.top3,
    winningTrifecta: official.winningTrifecta,
    actualResult: official.winningTrifecta,
    result: official.winningTrifecta,
    payout3t: official.payout3t ?? null,
    source: official.source || "official_raceresult_page",
    url: official.url
  };
}
