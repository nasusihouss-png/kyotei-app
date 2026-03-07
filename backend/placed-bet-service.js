import db from "./db.js";
import { buildRaceIdFromParts } from "./result-utils.js";
import axios from "axios";
import * as cheerio from "cheerio";
import { saveRaceResult } from "./save-result.js";

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeCombo(combo) {
  const digits = String(combo || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function normalizeRaceDate(date) {
  const text = String(date || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{8}$/.test(text)) return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  return null;
}

function normalizeDateCompact(date) {
  const raceDate = normalizeRaceDate(date);
  return raceDate ? raceDate.replace(/-/g, "") : null;
}

function resolveRaceId({ raceId, raceDate, venueId, raceNo }) {
  if (raceId) return String(raceId);
  return buildRaceIdFromParts({
    date: raceDate,
    venueId,
    raceNo
  });
}

const insertPlacedBetStmt = db.prepare(`
  INSERT INTO placed_bets (
    race_id,
    race_date,
    venue_id,
    race_no,
    combo,
    bet_amount,
    memo,
    updated_at
  ) VALUES (
    @race_id,
    @race_date,
    @venue_id,
    @race_no,
    @combo,
    @bet_amount,
    @memo,
    @updated_at
  )
`);

const updatePlacedBetStmt = db.prepare(`
  UPDATE placed_bets
  SET
    combo = @combo,
    bet_amount = @bet_amount,
    memo = @memo,
    updated_at = @updated_at
  WHERE id = @id
`);

const deletePlacedBetStmt = db.prepare(`
  DELETE FROM placed_bets
  WHERE id = ?
`);

const listPlacedBetsStmt = db.prepare(`
  SELECT
    id,
    race_id,
    race_date,
    venue_id,
    race_no,
    combo,
    bet_amount,
    memo,
    hit_flag,
    payout,
    profit_loss,
    settled_at,
    created_at,
    updated_at
  FROM placed_bets
  ORDER BY race_date DESC, venue_id DESC, race_no DESC, id DESC
`);

const getRaceResultStmt = db.prepare(`
  SELECT race_id, finish_1, finish_2, finish_3, payout_3t
  FROM results
  WHERE race_id = ?
`);

const getSettlementLogsByRaceStmt = db.prepare(`
  SELECT combo, payout
  FROM settlement_logs
  WHERE race_id = ?
`);

const listBetsByRaceStmt = db.prepare(`
  SELECT id, combo, bet_amount
  FROM placed_bets
  WHERE race_id = ?
`);

const settleBetStmt = db.prepare(`
  UPDATE placed_bets
  SET
    hit_flag = @hit_flag,
    payout = @payout,
    profit_loss = @profit_loss,
    settled_at = @settled_at,
    updated_at = @updated_at
  WHERE id = @id
`);

const summaryRangeStmt = db.prepare(`
  SELECT
    COALESCE(SUM(bet_amount), 0) AS total_bet_amount,
    COALESCE(SUM(COALESCE(payout, 0)), 0) AS total_payout,
    COALESCE(SUM(COALESCE(profit_loss, 0)), 0) AS total_profit_loss,
    COALESCE(SUM(CASE WHEN hit_flag = 1 THEN 1 ELSE 0 END), 0) AS hit_count,
    COALESCE(SUM(CASE WHEN hit_flag = 0 THEN 1 ELSE 0 END), 0) AS miss_count
  FROM placed_bets
  WHERE race_date >= @from_date
    AND race_date <= @to_date
`);

const getRaceByIdStmt = db.prepare(`
  SELECT race_id, race_date, venue_id, race_no
  FROM races
  WHERE race_id = ?
`);

const getPlacedBetRaceMetaByIdStmt = db.prepare(`
  SELECT race_id, race_date, venue_id, race_no
  FROM placed_bets
  WHERE race_id = ?
  ORDER BY id DESC
  LIMIT 1
`);

export function createPlacedBet({
  race_id,
  race_date,
  venue_id,
  race_no,
  combo,
  bet_amount,
  memo
}) {
  const raceDate = normalizeRaceDate(race_date);
  const venueId = toInt(venue_id);
  const raceNo = toInt(race_no);
  const normalizedCombo = normalizeCombo(combo);
  const betAmount = toInt(bet_amount);
  const raceId = resolveRaceId({
    raceId: race_id,
    raceDate,
    venueId,
    raceNo
  });

  if (!raceDate || !raceId || !Number.isInteger(venueId) || !Number.isInteger(raceNo)) {
    throw {
      statusCode: 400,
      code: "invalid_bet_race_info",
      message: "race_id or race_date+venue_id+race_no are required"
    };
  }
  if (!normalizedCombo || normalizedCombo.split("-").length !== 3) {
    throw {
      statusCode: 400,
      code: "invalid_combo",
      message: "combo must be a trifecta format like 1-2-3"
    };
  }
  if (!Number.isInteger(betAmount) || betAmount <= 0) {
    throw {
      statusCode: 400,
      code: "invalid_bet_amount",
      message: "bet_amount must be a positive integer"
    };
  }

  const result = insertPlacedBetStmt.run({
    race_id: raceId,
    race_date: raceDate,
    venue_id: venueId,
    race_no: raceNo,
    combo: normalizedCombo,
    bet_amount: betAmount,
    memo: memo ? String(memo) : null,
    updated_at: nowIso()
  });

  return result.lastInsertRowid;
}

export function createPlacedBets(items) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) {
    throw {
      statusCode: 400,
      code: "invalid_items",
      message: "items must be a non-empty array"
    };
  }

  const ids = [];
  const tx = db.transaction(() => {
    for (const row of rows) {
      ids.push(createPlacedBet(row || {}));
    }
  });
  tx();
  return ids;
}

export function updatePlacedBet(id, { combo, bet_amount, memo }) {
  const betId = toInt(id);
  if (!Number.isInteger(betId)) {
    throw {
      statusCode: 400,
      code: "invalid_bet_id",
      message: "id must be an integer"
    };
  }

  const normalizedCombo = normalizeCombo(combo);
  const betAmount = toInt(bet_amount);
  if (!normalizedCombo || normalizedCombo.split("-").length !== 3) {
    throw {
      statusCode: 400,
      code: "invalid_combo",
      message: "combo must be a trifecta format like 1-2-3"
    };
  }
  if (!Number.isInteger(betAmount) || betAmount <= 0) {
    throw {
      statusCode: 400,
      code: "invalid_bet_amount",
      message: "bet_amount must be a positive integer"
    };
  }

  const result = updatePlacedBetStmt.run({
    id: betId,
    combo: normalizedCombo,
    bet_amount: betAmount,
    memo: memo ? String(memo) : null,
    updated_at: nowIso()
  });

  return result.changes;
}

export function deletePlacedBet(id) {
  const betId = toInt(id);
  if (!Number.isInteger(betId)) {
    throw {
      statusCode: 400,
      code: "invalid_bet_id",
      message: "id must be an integer"
    };
  }
  const result = deletePlacedBetStmt.run(betId);
  return result.changes;
}

export function listPlacedBets() {
  return listPlacedBetsStmt.all().map((row) => ({
    ...row,
    status:
      row.hit_flag === 1 ? "hit" : row.hit_flag === 0 ? "miss" : "unsettled"
  }));
}

function buildPayoutMapFromSettlementLogs(raceId) {
  const map = new Map();
  const rows = getSettlementLogsByRaceStmt.all(raceId);
  for (const row of rows) {
    const combo = normalizeCombo(row.combo);
    const payout = toInt(row.payout, 0);
    if (combo && payout > 0) {
      map.set(combo, payout);
    }
  }
  return map;
}

function resolveRaceMetaForSettlement({ raceId, raceDate, venueId, raceNo }) {
  const byRaces = raceId ? getRaceByIdStmt.get(raceId) : null;
  const byBets = raceId ? getPlacedBetRaceMetaByIdStmt.get(raceId) : null;

  const picked = byRaces || byBets || {};
  const finalDate = normalizeRaceDate(raceDate || picked.race_date);
  const finalVenueId = toInt(venueId ?? picked.venue_id);
  const finalRaceNo = toInt(raceNo ?? picked.race_no);
  const finalRaceId =
    raceId ||
    resolveRaceId({
      raceDate: finalDate,
      venueId: finalVenueId,
      raceNo: finalRaceNo
    });

  return {
    raceId: finalRaceId,
    raceDate: finalDate,
    venueId: finalVenueId,
    raceNo: finalRaceNo
  };
}

function normalizeDigits(value) {
  return String(value || "").replace(/[\uFF10-\uFF19]/g, (ch) =>
    String.fromCharCode(ch.charCodeAt(0) - 0xfee0)
  );
}

function normalizeSpace(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function parseComboFromText(text) {
  const cleaned = normalizeDigits(text);
  const m = cleaned.match(/([1-6])\D+([1-6])\D+([1-6])/);
  if (!m) return null;
  return `${m[1]}-${m[2]}-${m[3]}`;
}

function parsePayoutFromText(text) {
  const cleaned = normalizeDigits(text).replace(/,/g, "");
  const m = cleaned.match(/(\d+)\s*円/);
  if (!m) return null;
  const payout = Number(m[1]);
  return Number.isFinite(payout) ? payout : null;
}

function parseResultFromRaceresultHtml(html) {
  const $ = cheerio.load(html);
  let combo = null;
  let payout3t = null;

  const tableRows = $("tr");
  tableRows.each((_, tr) => {
    if (combo) return;
    const $tr = $(tr);
    const heading = normalizeSpace($tr.find("th").first().text());
    if (!heading.includes("3連単")) return;

    const text = normalizeSpace($tr.text());
    combo = parseComboFromText(text);
    payout3t = parsePayoutFromText(text);
  });

  if (!combo) {
    const text = normalizeSpace($("body").text());
    const match = text.match(/3連単[^0-9]*([1-6])\D+([1-6])\D+([1-6])[^0-9]*(\d[\d,]*)\s*円/);
    if (match) {
      combo = `${match[1]}-${match[2]}-${match[3]}`;
      payout3t = Number(String(match[4]).replace(/,/g, ""));
    }
  }

  if (!combo) return null;
  const top3 = combo.split("-").map((v) => Number(v));
  if (top3.length !== 3 || top3.some((n) => !Number.isInteger(n))) return null;

  return {
    top3,
    combo,
    payout3t: Number.isFinite(payout3t) ? payout3t : null
  };
}

async function fetchOfficialRaceResult({ raceDate, venueId, raceNo }) {
  const hd = normalizeDateCompact(raceDate);
  const jcd = String(venueId).padStart(2, "0");
  const rno = Number(raceNo);
  if (!hd || !Number.isInteger(rno) || !Number.isInteger(Number(venueId))) return null;

  const url = `https://www.boatrace.jp/owpc/pc/race/raceresult?rno=${rno}&jcd=${jcd}&hd=${hd}`;
  const { data } = await axios.get(url, {
    timeout: 15000,
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8"
    }
  });

  const parsed = parseResultFromRaceresultHtml(data);
  if (!parsed) return null;
  return {
    url,
    ...parsed
  };
}

export async function settlePlacedBetsForRace({ race_id, race_date, venue_id, race_no }) {
  const meta = resolveRaceMetaForSettlement({
    raceId: race_id,
    raceDate: race_date,
    venueId: venue_id,
    raceNo: race_no
  });
  const raceId = meta.raceId;

  if (!raceId) {
    throw {
      statusCode: 400,
      code: "invalid_race_id",
      message: "race_id or race_date+venue_id+race_no is required"
    };
  }

  let result = getRaceResultStmt.get(raceId);
  let officialFetched = false;
  let sourceUrl = null;

  if (!result) {
    const official = await fetchOfficialRaceResult(meta);
    if (official?.top3) {
      saveRaceResult({
        raceId,
        finishOrder: official.top3,
        payout3t: official.payout3t
      });
      result = getRaceResultStmt.get(raceId);
      officialFetched = true;
      sourceUrl = official.url;
    }
  }

  if (!result) {
    throw {
      statusCode: 404,
      code: "result_not_found",
      message: "Race result is not confirmed yet"
    };
  }

  const winningCombo = `${result.finish_1}-${result.finish_2}-${result.finish_3}`;
  const payoutByCombo = buildPayoutMapFromSettlementLogs(raceId);
  const defaultPayout = toInt(result.payout_3t, 0);
  const bets = listBetsByRaceStmt.all(raceId);

  let settledCount = 0;
  let hitCount = 0;

  const tx = db.transaction(() => {
    for (const bet of bets) {
      const combo = normalizeCombo(bet.combo);
      const betAmount = toInt(bet.bet_amount, 0);
      const hit = combo === winningCombo ? 1 : 0;
      if (hit) hitCount += 1;

      let payout = 0;
      if (hit) {
        const unitPayout = payoutByCombo.get(combo) ?? defaultPayout;
        const units = Math.max(1, Math.floor(betAmount / 100));
        payout = unitPayout > 0 ? unitPayout * units : 0;
      }
      const profitLoss = payout - betAmount;

      settleBetStmt.run({
        id: bet.id,
        hit_flag: hit,
        payout,
        profit_loss: profitLoss,
        settled_at: nowIso(),
        updated_at: nowIso()
      });
      settledCount += 1;
    }
  });

  tx();

  return {
    race_id: raceId,
    winning_combo: winningCombo,
    settled_count: settledCount,
    hit_count: hitCount,
    official_result_fetched: officialFetched,
    source_url: sourceUrl
  };
}

function summarizeRange(fromDate, toDate) {
  const row = summaryRangeStmt.get({
    from_date: fromDate,
    to_date: toDate
  });

  const totalBetAmount = toInt(row?.total_bet_amount, 0);
  const totalPayout = toInt(row?.total_payout, 0);
  const totalProfitLoss = toInt(row?.total_profit_loss, 0);
  const hitCount = toInt(row?.hit_count, 0);
  const missCount = toInt(row?.miss_count, 0);
  const totalSettled = hitCount + missCount;
  const hitRate = totalSettled > 0 ? Number(((hitCount / totalSettled) * 100).toFixed(2)) : 0;
  const recoveryRate =
    totalBetAmount > 0 ? Number(((totalPayout / totalBetAmount) * 100).toFixed(2)) : 0;

  return {
    total_bet_amount: totalBetAmount,
    total_payout: totalPayout,
    total_profit_loss: totalProfitLoss,
    hit_count: hitCount,
    miss_count: missCount,
    hit_rate: hitRate,
    recovery_rate: recoveryRate
  };
}

export function getPlacedBetSummaries(baseDateInput) {
  const now = baseDateInput ? new Date(baseDateInput) : new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  const today = `${yyyy}-${mm}-${dd}`;
  const monthStart = `${yyyy}-${mm}-01`;
  const yearStart = `${yyyy}-01-01`;

  return {
    today: summarizeRange(today, today),
    month: summarizeRange(monthStart, today),
    year: summarizeRange(yearStart, today)
  };
}
