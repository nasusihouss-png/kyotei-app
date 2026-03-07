import db from "./db.js";
import { comboFromTop3 } from "./result-utils.js";

const insertSettlementLog = db.prepare(`
  INSERT INTO settlement_logs (
    race_id,
    combo,
    bet_amount,
    hit_flag,
    payout,
    profit_loss
  ) VALUES (
    @race_id,
    @combo,
    @bet_amount,
    @hit_flag,
    @payout,
    @profit_loss
  )
`);

const insertManyTx = db.transaction((rows) => {
  for (const row of rows) {
    insertSettlementLog.run(row);
  }
});

const selectSettlementByRace = db.prepare(`
  SELECT id, combo, bet_amount
  FROM settlement_logs
  WHERE race_id = ?
`);

const updateSettlementHit = db.prepare(`
  UPDATE settlement_logs
  SET
    hit_flag = @hit_flag,
    payout = @payout,
    profit_loss = @profit_loss
  WHERE id = @id
`);

function toInt(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

export function saveSettlementLog({
  raceId,
  combo,
  betAmount,
  hitFlag,
  payout,
  profitLoss
}) {
  const normalizedBet = toInt(betAmount);
  const normalizedPayout = toInt(payout);
  const normalizedProfitLoss =
    profitLoss === undefined || profitLoss === null
      ? normalizedPayout - normalizedBet
      : toInt(profitLoss);

  insertSettlementLog.run({
    race_id: raceId,
    combo: String(combo ?? ""),
    bet_amount: normalizedBet,
    hit_flag: hitFlag ? 1 : 0,
    payout: normalizedPayout,
    profit_loss: normalizedProfitLoss
  });
}

export function saveSettlementLogs(raceId, settlements) {
  const rows = (Array.isArray(settlements) ? settlements : []).map((s) => {
    const normalizedBet = toInt(s?.betAmount ?? s?.bet_amount);
    const normalizedPayout = toInt(s?.payout);
    const providedProfitLoss = s?.profitLoss ?? s?.profit_loss;

    return {
      race_id: raceId,
      combo: String(s?.combo ?? ""),
      bet_amount: normalizedBet,
      hit_flag: s?.hitFlag || s?.hit_flag ? 1 : 0,
      payout: normalizedPayout,
      profit_loss:
        providedProfitLoss === undefined || providedProfitLoss === null
          ? normalizedPayout - normalizedBet
          : toInt(providedProfitLoss)
    };
  });

  if (rows.length === 0) return 0;
  insertManyTx(rows);
  return rows.length;
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

export function markSettlementHits({ raceId, actualTop3, payoutByCombo = {} }) {
  const actualCombo = comboFromTop3(actualTop3);
  if (!raceId || !actualCombo) {
    throw {
      statusCode: 400,
      code: "invalid_settlement_mark_input",
      message: "raceId and valid actualTop3 are required"
    };
  }

  const rows = selectSettlementByRace.all(raceId);
  let updated = 0;
  let hitCount = 0;

  const tx = db.transaction(() => {
    for (const row of rows) {
      const combo = normalizeCombo(row.combo);
      const hitFlag = combo === actualCombo ? 1 : 0;
      const payout = hitFlag ? toInt(payoutByCombo?.[combo]) : 0;
      const betAmount = toInt(row.bet_amount);
      const profitLoss = payout - betAmount;

      updateSettlementHit.run({
        id: row.id,
        hit_flag: hitFlag,
        payout,
        profit_loss: profitLoss
      });

      updated += 1;
      if (hitFlag) hitCount += 1;
    }
  });

  tx();

  return {
    raceId,
    actualCombo,
    updated,
    hitCount
  };
}
