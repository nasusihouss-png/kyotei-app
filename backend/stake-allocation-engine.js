function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function roundDown100(value) {
  if (!Number.isFinite(value) || value <= 0) return 0;
  return Math.floor(value / 100) * 100;
}

function getRaceBudgetByMode(mode) {
  const m = String(mode || "").toUpperCase().replace(/\s+/g, "_");
  if (m === "FULL_BET") return 1600;
  if (m === "SMALL_BET") return 800;
  if (m === "MICRO_BET") return 400;
  return 0;
}

function getAllocationStyle(mode, mainCount) {
  const m = String(mode || "").toUpperCase().replace(/\s+/g, "_");
  if (m === "FULL_BET") return mainCount >= 2 ? "balanced" : "main-heavy";
  if (m === "SMALL_BET") return "main-heavy";
  if (m === "MICRO_BET") return "defensive";
  return "none";
}

function getSummary(mode, allocationStyle) {
  const m = String(mode || "").toUpperCase().replace(/\s+/g, "_");
  if (m === "SKIP") return "見送り";
  if (m === "MICRO_BET") return "本線絞りで最小投資";
  if (m === "SMALL_BET") return "本線厚め、押さえ薄め";
  if (allocationStyle === "balanced") return "本線中心にバランス配分";
  return "本線厚め、押さえ控えめ";
}

function determineTicketType({ combo, prob, odds, ev, primarySet, secondarySet }) {
  if (primarySet.has(combo)) return "main";
  if (secondarySet.has(combo)) return "backup";
  if (prob < 0.04 || odds >= 40 || ev < 1.1) return "longshot";
  return "backup";
}

function typeWeightMultiplier(type, mode) {
  const m = String(mode || "").toUpperCase().replace(/\s+/g, "_");
  if (type === "main") return 1;
  if (type === "backup") return m === "MICRO_BET" ? 0.5 : 0.65;
  return m === "FULL_BET" ? 0.28 : 0.12;
}

function buildCandidateRows({ ticketOptimization, betPlan, ticketGenerationV2, mode }) {
  const primarySet = new Set(
    (Array.isArray(ticketGenerationV2?.primary_tickets) ? ticketGenerationV2.primary_tickets : []).map((v) =>
      String(v)
    )
  );
  const secondarySet = new Set(
    (Array.isArray(ticketGenerationV2?.secondary_tickets) ? ticketGenerationV2.secondary_tickets : []).map((v) =>
      String(v)
    )
  );

  const rows = Array.isArray(ticketOptimization?.optimized_tickets) && ticketOptimization.optimized_tickets.length
    ? ticketOptimization.optimized_tickets
    : Array.isArray(betPlan?.recommended_bets)
      ? betPlan.recommended_bets
      : [];

  return rows
    .map((row) => {
      const combo = String(row?.combo || "");
      if (!combo) return null;
      const prob = toNum(row?.prob ?? row?.probability, 0);
      const odds = toNum(row?.odds, 0);
      const ev = Number.isFinite(toNum(row?.ev, NaN)) ? toNum(row?.ev) : prob > 0 && odds > 0 ? prob * odds : 0;
      const ticket_type = determineTicketType({
        combo,
        prob,
        odds,
        ev,
        primarySet,
        secondarySet
      });
      const conf = toNum(row?.ticket_confidence_score, 50);
      const oddsFactor = odds > 0 ? clamp(0.2, 1.2, 16 / odds) : 0.35;
      const evNorm = clamp(0, 1.4, ev / 2.4);
      const baseWeight = clamp(0.05, 100, prob * 100 * 0.58 + evNorm * 20 + conf * 0.12 + oddsFactor * 10);
      const typeMultiplier = typeWeightMultiplier(ticket_type, mode);
      const weight = clamp(0.02, 999, baseWeight * typeMultiplier);

      return {
        combo,
        prob: Number(prob.toFixed(4)),
        odds: odds > 0 ? Number(odds.toFixed(1)) : null,
        ev: Number(ev.toFixed(4)),
        ticket_type,
        weight
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.prob - a.prob)
    .slice(0, 10);
}

export function buildStakeAllocationPlan({
  raceDecision,
  ticketOptimization,
  betPlan,
  ticketGenerationV2
}) {
  const mode = String(raceDecision?.mode || raceDecision?.recommendation || "SKIP");
  const race_budget = getRaceBudgetByMode(mode);
  const candidates = buildCandidateRows({ ticketOptimization, betPlan, ticketGenerationV2, mode });

  if (race_budget <= 0 || candidates.length === 0) {
    return {
      bankrollPlan: {
        race_budget: 0,
        mode: mode.toUpperCase().replace(/_/g, " "),
        allocation_style: "none",
        summary: "見送り"
      },
      tickets: candidates.map((t) => ({
        ...t,
        recommended_bet: 0
      }))
    };
  }

  const totalWeight = candidates.reduce((sum, t) => sum + toNum(t.weight), 0) || 1;
  let allocated = 0;

  const withStake = candidates.map((t) => {
    const ratio = t.weight / totalWeight;
    const rawStake = race_budget * ratio;
    const minStake = t.ticket_type === "main" ? 200 : 100;
    const recommended_bet = Math.max(minStake, roundDown100(rawStake));
    allocated += recommended_bet;
    return {
      ...t,
      recommended_bet
    };
  });

  if (allocated > race_budget) {
    const over = allocated - race_budget;
    let needCut = roundDown100(over);
    for (let i = withStake.length - 1; i >= 0 && needCut > 0; i -= 1) {
      const minStake = withStake[i].ticket_type === "main" ? 200 : 100;
      if (withStake[i].recommended_bet - 100 >= minStake) {
        withStake[i].recommended_bet -= 100;
        needCut -= 100;
      }
    }
  } else if (allocated < race_budget) {
    let remain = roundDown100(race_budget - allocated);
    const targets = withStake.filter((t) => t.ticket_type === "main");
    let idx = 0;
    while (remain >= 100 && targets.length) {
      const t = targets[idx % targets.length];
      t.recommended_bet += 100;
      remain -= 100;
      idx += 1;
    }
  }

  const mainCount = withStake.filter((t) => t.ticket_type === "main").length;
  const allocation_style = getAllocationStyle(mode, mainCount);

  return {
    bankrollPlan: {
      race_budget,
      mode: mode.toUpperCase().replace(/_/g, " "),
      allocation_style,
      summary: getSummary(mode, allocation_style)
    },
    tickets: withStake
  };
}
