function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function tierFromSignals({ valueScore, overpriced, underpriced, prob }) {
  if (overpriced && valueScore < 42) return "avoid";
  if (underpriced && valueScore >= 62) return "main_value";
  if (prob >= 0.12 && valueScore < 56) return "safe_low_value";
  if (prob < 0.06 && valueScore >= 54) return "speculative";
  if (valueScore >= 58) return "main_value";
  if (valueScore <= 40) return "avoid";
  return "safe_low_value";
}

export function detectValue({ recommendedBets, ticketOptimization, raceDecision }) {
  const source =
    Array.isArray(ticketOptimization?.optimized_tickets) && ticketOptimization.optimized_tickets.length
      ? ticketOptimization.optimized_tickets
      : Array.isArray(recommendedBets)
        ? recommendedBets
        : [];

  const analyzedTickets = source.map((row) => {
    const combo = String(row?.combo || "");
    const prob = toNum(row?.prob ?? row?.probability, 0);
    const odds = toNum(row?.odds, 0);
    const ev =
      Number.isFinite(toNum(row?.ev, NaN)) ? toNum(row?.ev) : prob > 0 && odds > 0 ? prob * odds : 0;

    const fairOdds = prob > 0 ? 1 / prob : 0;
    const priceRatio = fairOdds > 0 && odds > 0 ? odds / fairOdds : 1;
    const overpriced = priceRatio < 0.88 || (prob >= 0.18 && odds <= 6.5);
    const underpriced = priceRatio > 1.12 && prob >= 0.04;

    const valueScore = clamp(
      0,
      100,
      prob * 100 * 0.42 +
        clamp(0, 1.5, ev / 1.8) * 30 +
        clamp(0, 1.6, priceRatio) * 20 +
        (toNum(row?.ticket_confidence_score, 50) - 50) * 0.16
    );

    const bet_value_tier = tierFromSignals({
      valueScore,
      overpriced,
      underpriced,
      prob
    });

    return {
      ...row,
      combo,
      prob: Number(prob.toFixed(4)),
      odds: odds > 0 ? Number(odds.toFixed(1)) : null,
      ev: Number(ev.toFixed(4)),
      value_score: Number(valueScore.toFixed(2)),
      overpriced_flag: !!overpriced,
      underpriced_flag: !!underpriced,
      bet_value_tier
    };
  });

  const avgValue =
    analyzedTickets.reduce((a, b) => a + toNum(b.value_score), 0) / Math.max(1, analyzedTickets.length);
  const lowValueCount = analyzedTickets.filter(
    (t) => t.bet_value_tier === "safe_low_value" || t.bet_value_tier === "avoid"
  ).length;
  const goodValueCount = analyzedTickets.filter((t) => t.bet_value_tier === "main_value").length;

  const mode = String(raceDecision?.mode || "").toUpperCase();
  const modePenalty = mode === "FULL_BET" ? 0 : mode === "SMALL_BET" ? 4 : 8;
  const value_balance_score = clamp(
    0,
    100,
    avgValue * 0.7 + goodValueCount * 4 - lowValueCount * 3 - modePenalty
  );
  const low_value_risk = clamp(
    0,
    100,
    (lowValueCount / Math.max(1, analyzedTickets.length)) * 100 * 0.8 + (100 - avgValue) * 0.2
  );
  const price_quality_score = clamp(0, 100, avgValue * 0.62 + (100 - low_value_risk) * 0.38);

  let summary = "Value balance is neutral.";
  if (value_balance_score >= 62 && low_value_risk <= 40) {
    summary = "Good value balance: main tickets can be weighted.";
  } else if (low_value_risk >= 60) {
    summary = "Low-value risk is elevated: reduce overpriced tickets.";
  }

  return {
    value_balance_score: Number(value_balance_score.toFixed(2)),
    low_value_risk: Number(low_value_risk.toFixed(2)),
    price_quality_score: Number(price_quality_score.toFixed(2)),
    summary,
    tickets: analyzedTickets
  };
}
