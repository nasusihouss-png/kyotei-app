import axios from "axios";
import * as cheerio from "cheerio";

const BOATRACE_BASE = "https://www.boatrace.jp";

function toNumber(value) {
  const cleaned = String(value || "").replace(/,/g, "").trim();
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

async function fetchHtml(url) {
  const { data } = await axios.get(url, {
    timeout: 15000,
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });
  return data;
}

function parseThirdByBlock($) {
  const thirds = [];
  $(".title7")
    .next(".table1")
    .find("thead tr")
    .first()
    .find("th.is-boatColor1, th.is-boatColor2, th.is-boatColor3, th.is-boatColor4, th.is-boatColor5, th.is-boatColor6")
    .each((_, th) => {
      const n = Number($(th).text().trim());
      if (Number.isInteger(n)) thirds.push(n);
    });

  return thirds.slice(0, 6);
}

function parseTrifectaOddsMap(html) {
  const $ = cheerio.load(html);
  const thirdByBlock = parseThirdByBlock($);
  const oddsMap = new Map();

  const rows = $(".title7")
    .next(".table1")
    .find("tbody.is-p3-0 tr");

  const firstByBlock = new Array(6).fill(null);
  const rowSpanRemain = new Array(6).fill(0);

  rows.each((_, tr) => {
    const tds = $(tr).children("td");
    let cursor = 0;

    for (let block = 0; block < 6; block += 1) {
      if (rowSpanRemain[block] <= 0) {
        const firstCell = tds.eq(cursor++);
        const first = Number(firstCell.text().trim());
        const rs = Number(firstCell.attr("rowspan") || 1);
        firstByBlock[block] = Number.isInteger(first) ? first : null;
        rowSpanRemain[block] = Number.isInteger(rs) ? rs : 1;
      }

      const secondCell = tds.eq(cursor++);
      const oddsCell = tds.eq(cursor++);

      rowSpanRemain[block] -= 1;

      const first = firstByBlock[block];
      const second = Number(secondCell.text().trim());
      const third = thirdByBlock[block];
      const odds = toNumber(oddsCell.text());

      if (
        Number.isInteger(first) &&
        Number.isInteger(second) &&
        Number.isInteger(third) &&
        Number.isFinite(odds)
      ) {
        const combo = `${first}-${second}-${third}`;
        oddsMap.set(combo, odds);
      }
    }
  });

  return oddsMap;
}

function parseExactaOddsMap(html) {
  const $ = cheerio.load(html);
  const oddsMap = new Map();

  $("table tr").each((_, tr) => {
    const text = $(tr).text().replace(/\s+/g, " ").trim();
    if (!text) return;

    const comboMatch = text.match(/([1-6])\s*[-－]\s*([1-6])/);
    if (!comboMatch) return;
    const first = Number(comboMatch[1]);
    const second = Number(comboMatch[2]);
    if (first === second) return;

    const oddsMatch = text.match(/(\d[\d,]*(?:\.\d+)?)/g);
    if (!oddsMatch || !oddsMatch.length) return;
    const odds = toNumber(oddsMatch[oddsMatch.length - 1]);
    if (!Number.isFinite(odds)) return;

    oddsMap.set(`${first}-${second}`, odds);
  });

  return oddsMap;
}

function mapToList(map) {
  return [...map.entries()]
    .map(([combo, odds]) => ({
      combo,
      odds: Number(Number(odds).toFixed(1))
    }))
    .sort((a, b) => a.odds - b.odds);
}

async function fetchSafely(url, parser) {
  try {
    const html = await fetchHtml(url);
    return {
      ok: true,
      url,
      map: parser(html)
    };
  } catch (err) {
    return {
      ok: false,
      url,
      error: err?.message || "fetch_failed",
      map: new Map()
    };
  }
}

export async function fetchRaceOddsData({ date, venueId, raceNo }) {
  const hd = String(date || "").replace(/-/g, "");
  const jcd = String(venueId).padStart(2, "0");
  const rno = Number(raceNo);

  const odds3tUrl = `${BOATRACE_BASE}/owpc/pc/race/odds3t?rno=${rno}&jcd=${jcd}&hd=${hd}`;
  const odds2tfUrl = `${BOATRACE_BASE}/owpc/pc/race/odds2tf?rno=${rno}&jcd=${jcd}&hd=${hd}`;
  const odds2tUrl = `${BOATRACE_BASE}/owpc/pc/race/odds2t?rno=${rno}&jcd=${jcd}&hd=${hd}`;

  const trifectaResult = await fetchSafely(odds3tUrl, parseTrifectaOddsMap);
  let exactaResult = await fetchSafely(odds2tfUrl, parseExactaOddsMap);
  if (!exactaResult.map.size) {
    exactaResult = await fetchSafely(odds2tUrl, parseExactaOddsMap);
  }

  return {
    trifectaMap: trifectaResult.map,
    exactaMap: exactaResult.map,
    oddsData: {
      trifecta: mapToList(trifectaResult.map),
      exacta: mapToList(exactaResult.map),
      fetched_at: new Date().toISOString(),
      source_urls: {
        trifecta: trifectaResult.url,
        exacta: exactaResult.url
      },
      fetch_status: {
        trifecta: trifectaResult.ok ? "ok" : "failed",
        exacta: exactaResult.ok ? "ok" : "failed"
      },
      errors: [
        ...(trifectaResult.ok ? [] : [{ type: "trifecta", message: trifectaResult.error }]),
        ...(exactaResult.ok ? [] : [{ type: "exacta", message: exactaResult.error }])
      ]
    }
  };
}

export async function analyzeExpectedValue({ date, venueId, raceNo, simulation }) {
  const odds = await fetchRaceOddsData({ date, venueId, raceNo });
  const oddsMap = odds.trifectaMap;

  const base = (simulation?.top_combinations || []).map((x) => {
    const combo = x.combo;
    const prob = Number(x.prob);
    const oddsValue = oddsMap.get(combo) ?? null;
    const ev = oddsValue !== null ? prob * oddsValue : null;
    return {
      combo,
      prob,
      odds: oddsValue,
      ev
    };
  });

  const best_ev_bets = base
    .filter((x) => x.odds !== null && Number.isFinite(x.ev))
    .sort((a, b) => b.ev - a.ev)
    .map((x) => ({
      combo: x.combo,
      prob: Number(x.prob.toFixed(4)),
      odds: Number(x.odds.toFixed(1)),
      ev: Number(x.ev.toFixed(4))
    }));

  return {
    ev_analysis: {
      best_ev_bets
    },
    oddsData: odds.oddsData
  };
}
