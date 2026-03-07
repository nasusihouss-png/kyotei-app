function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function buildBetPlan(evAnalysis, bankroll = 10000) {
  const source = evAnalysis?.best_ev_bets || [];

  const recommended_bets = source
    .map((bet) => {
      const odds = toNumber(bet.odds, 0);
      const ev = toNumber(bet.ev, 0);
      if (odds <= 0) return null;

      const rawFraction = (ev - 1) / odds;
      const bet_fraction = clamp(0.01, 0.1, rawFraction);
      const bet_size = Math.round(bankroll * bet_fraction);

      return {
        combo: bet.combo,
        bet: bet_size
      };
    })
    .filter(Boolean);

  return {
    recommended_bets
  };
}
