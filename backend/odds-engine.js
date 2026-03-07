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

  // Expect 6 blocks (third-place lanes).
  return thirds.slice(0, 6);
}

function parseOddsMap(html) {
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

export async function analyzeExpectedValue({ date, venueId, raceNo, simulation }) {
  const hd = String(date || "").replace(/-/g, "");
  const jcd = String(venueId).padStart(2, "0");
  const rno = Number(raceNo);
  const oddsUrl = `${BOATRACE_BASE}/owpc/pc/race/odds3t?rno=${rno}&jcd=${jcd}&hd=${hd}`;

  const html = await fetchHtml(oddsUrl);
  const oddsMap = parseOddsMap(html);

  const base = (simulation?.top_combinations || []).map((x) => {
    const combo = x.combo;
    const prob = Number(x.prob);
    const odds = oddsMap.get(combo) ?? null;
    const ev = odds !== null ? prob * odds : null;
    return {
      combo,
      prob,
      odds,
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
    oddsUrl,
    ev_analysis: {
      best_ev_bets
    }
  };
}
