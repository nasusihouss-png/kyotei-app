function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

export function optimizeTickets({
  recommendedBets,
  probabilities,
  oddsData,
  recommendation,
  raceStructure,
  aiEnhancement
}) {
  const probMap = new Map();
  (Array.isArray(probabilities) ? probabilities : []).forEach((p) => {
    const prob = toNum(p?.p ?? p?.prob, NaN);
    if (p?.combo && Number.isFinite(prob)) probMap.set(String(p.combo), prob);
  });

  const oddsMap = new Map();
  (Array.isArray(oddsData?.trifecta) ? oddsData.trifecta : []).forEach((o) => {
    const odds = toNum(o?.odds, NaN);
    if (o?.combo && Number.isFinite(odds)) oddsMap.set(String(o.combo), odds);
  });

  const structureScore = toNum(raceStructure?.race_structure_score, 50);
  const oddsAdjusted = toNum(aiEnhancement?.odds_adjusted_ticket_score, 50);

  const optimized_tickets = (Array.isArray(recommendedBets) ? recommendedBets : [])
    .map((bet) => {
      const combo = String(bet?.combo || "");
      const prob = toNum(probMap.get(combo), 0);
      const odds = oddsMap.has(combo) ? toNum(oddsMap.get(combo), null) : null;
      const ev = Number.isFinite(toNum(bet?.ev, NaN))
        ? toNum(bet?.ev)
        : Number.isFinite(odds)
          ? prob * odds
          : null;

      const ticket_confidence_score = clamp(
        0,
        100,
        prob * 100 * 0.62 + structureScore * 0.25 + oddsAdjusted * 0.13
      );

      return {
        combo,
        prob: Number(prob.toFixed(4)),
        odds: Number.isFinite(odds) ? Number(odds.toFixed(1)) : null,
        ev: Number.isFinite(ev) ? Number(ev.toFixed(4)) : null,
        recommended_bet: Number.isFinite(toNum(bet?.roundedBet ?? bet?.bet, NaN))
          ? Math.max(100, Math.floor(toNum(bet?.roundedBet ?? bet?.bet) / 100) * 100)
          : 100,
        ticket_confidence_score: Number(ticket_confidence_score.toFixed(2))
      };
    })
    .sort((a, b) => b.prob - a.prob)
    .slice(0, 10);

  const lowConfidenceCount = optimized_tickets.filter((t) => t.ticket_confidence_score < 45).length;
  const value_warning = lowConfidenceCount >= 3 || structureScore < 45;

  const budgetMode = String(recommendation || "").toUpperCase();
  let recommended_budget_split = { primary: 0.7, secondary: 0.3 };
  if (budgetMode === "SMALL BET") recommended_budget_split = { primary: 0.8, secondary: 0.2 };
  if (budgetMode === "MICRO BET") recommended_budget_split = { primary: 0.9, secondary: 0.1 };
  if (budgetMode === "SKIP") recommended_budget_split = { primary: 0, secondary: 0 };

  const avgTicketConfidence =
    optimized_tickets.reduce((a, b) => a + toNum(b.ticket_confidence_score), 0) /
    Math.max(1, optimized_tickets.length);

  return {
    optimized_tickets,
    ticket_confidence_score: Number(avgTicketConfidence.toFixed(2)),
    odds_adjusted_ticket_score: Number(oddsAdjusted.toFixed(2)),
    value_warning,
    recommended_budget_split
  };
}
