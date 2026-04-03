function toInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

export function buildRaceIdFromParts({ date, venueId, raceNo }) {
  const compactDate = String(date || "").replace(/-/g, "");
  const v = toInt(venueId);
  const r = toInt(raceNo);

  if (!/^\d{8}$/.test(compactDate) || !Number.isInteger(v) || !Number.isInteger(r)) {
    return null;
  }

  return `${compactDate}_${v}_${r}`;
}

export function normalizeFinishOrder(finishOrder) {
  if (!Array.isArray(finishOrder)) return null;
  const top3 = finishOrder.slice(0, 3).map((v) => toInt(v));

  if (top3.length !== 3 || top3.some((v) => !Number.isInteger(v) || v < 1 || v > 6)) {
    return null;
  }

  if (new Set(top3).size !== 3) return null;
  return top3;
}

export function normalizeTop3OrNull(values) {
  if (!Array.isArray(values)) return null;
  const top3 = values.slice(0, 3).map((v) => toInt(v));
  if (top3.length !== 3 || top3.some((v) => !Number.isInteger(v) || v < 1 || v > 6)) {
    return null;
  }
  if (new Set(top3).size !== 3) return null;
  return top3;
}

export function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  const normalized = digits.slice(0, 3);
  return normalized.length === 3 ? normalized.join("-") : "";
}

export function compareActualTop3VsPredictedBets(actualTop3, predictedBets, options = {}) {
  const payoutByCombo = options?.payoutByCombo || {};
  const actualCombo = normalizeCombo(actualTop3.join("-"));
  const list = Array.isArray(predictedBets) ? predictedBets : [];

  const rows = list.map((bet) => {
    const combo = normalizeCombo(bet?.combo);
    const betAmount = toInt(bet?.bet ?? bet?.betAmount) ?? 0;
    const hitFlag = combo && combo === actualCombo ? 1 : 0;
    const payout = hitFlag ? toInt(payoutByCombo?.[combo]) ?? 0 : 0;
    const profitLoss = payout - betAmount;

    return {
      combo,
      betAmount,
      hitFlag,
      payout,
      profitLoss
    };
  });

  const hitCount = rows.filter((r) => r.hitFlag === 1).length;
  const totalBet = rows.reduce((sum, r) => sum + r.betAmount, 0);
  const totalPayout = rows.reduce((sum, r) => sum + r.payout, 0);

  return {
    actualTop3,
    actualCombo,
    rows,
    summary: {
      totalBets: rows.length,
      hitCount,
      totalBet,
      totalPayout,
      totalProfitLoss: totalPayout - totalBet
    }
  };
}

export function comboFromTop3(top3) {
  return normalizeCombo((Array.isArray(top3) ? top3 : []).join("-"));
}
