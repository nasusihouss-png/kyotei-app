function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function laneFromCombo(combo, idx) {
  const parts = normalizeCombo(combo).split("-").map((x) => Number(x));
  return Number.isInteger(parts[idx]) ? parts[idx] : 0;
}

function calcTicketTrap(row, context) {
  const combo = normalizeCombo(row?.combo);
  const prob = toNum(row?.prob ?? row?.probability, 0);
  const odds = toNum(row?.odds, 0);
  const ev = Number.isFinite(toNum(row?.ev, NaN)) ? toNum(row?.ev) : prob > 0 && odds > 0 ? prob * odds : 0;
  const head = laneFromCombo(combo, 0);
  const flags = [];

  if (prob >= 0.16 && odds > 0 && odds <= 7.2 && ev < 1.12) flags.push("OVERBOUGHT_FAVORITE");
  if (prob <= 0.055 && odds >= 35 && ev < 1.2) flags.push("FALSE_VALUE");
  if (head >= 5 && prob < 0.07) flags.push("OUTER_LANE_TRAP");
  if (toNum(context?.chaosRisk, 0) >= 72 && prob < 0.08) flags.push("CHAOS_TRAP");

  let avoid_level = 0;
  if (flags.includes("OUTER_LANE_TRAP") || flags.includes("CHAOS_TRAP")) avoid_level += 1;
  if (flags.includes("FALSE_VALUE")) avoid_level += 1;
  if (flags.includes("OVERBOUGHT_FAVORITE")) avoid_level += 1;
  avoid_level = clamp(0, 3, avoid_level);

  return {
    combo,
    trap_flags: [...new Set(flags)],
    avoid_level
  };
}

export function detectMarketTraps({
  raceRisk,
  raceStructure,
  raceIndexes,
  recommendedBets,
  ticketOptimization,
  probabilities
}) {
  const rows = Array.isArray(ticketOptimization?.optimized_tickets) && ticketOptimization.optimized_tickets.length
    ? ticketOptimization.optimized_tickets
    : Array.isArray(recommendedBets)
      ? recommendedBets
      : [];

  const chaosRisk = toNum(raceStructure?.chaos_risk_score, toNum(raceRisk?.risk_score, 50));
  const areIndex = toNum(raceIndexes?.are_index, 50);
  const probs = (Array.isArray(probabilities) ? probabilities : [])
    .map((p) => toNum(p?.p ?? p?.prob, 0))
    .filter((x) => x > 0)
    .sort((a, b) => b - a);
  const top3Concentration = (probs[0] || 0) + (probs[1] || 0) + (probs[2] || 0);

  const ticketTraps = rows.map((row) => calcTicketTrap(row, { chaosRisk }));
  const flags = new Set();
  if (top3Concentration >= 0.36 && ticketTraps.some((t) => t.trap_flags.includes("OVERBOUGHT_FAVORITE"))) {
    flags.add("OVERBOUGHT_FAVORITE");
  }
  if (ticketTraps.filter((t) => t.trap_flags.includes("FALSE_VALUE")).length >= 2) flags.add("FALSE_VALUE");
  if (rows.length >= 9 || (rows.length >= 7 && top3Concentration < 0.28)) flags.add("SPREAD_TRAP");
  if (ticketTraps.filter((t) => t.trap_flags.includes("OUTER_LANE_TRAP")).length >= 2) flags.add("OUTER_LANE_TRAP");
  if (chaosRisk >= 72 || areIndex >= 78 || top3Concentration < 0.22) flags.add("CHAOS_TRAP");

  const flagList = [...flags];
  const raceTrapPenalty =
    (flagList.includes("OVERBOUGHT_FAVORITE") ? 16 : 0) +
    (flagList.includes("FALSE_VALUE") ? 18 : 0) +
    (flagList.includes("SPREAD_TRAP") ? 14 : 0) +
    (flagList.includes("OUTER_LANE_TRAP") ? 20 : 0) +
    (flagList.includes("CHAOS_TRAP") ? 24 : 0);
  const averageAvoid = ticketTraps.reduce((a, b) => a + toNum(b.avoid_level), 0) / Math.max(1, ticketTraps.length);
  const trap_score = clamp(0, 100, raceTrapPenalty + averageAvoid * 8 + Math.max(0, 60 - top3Concentration * 100) * 0.18);

  let summary = "Trap risk is limited.";
  if (trap_score >= 68) summary = "High trap risk: reduce stake and narrow tickets.";
  else if (trap_score >= 45) summary = "Moderate trap risk: avoid weak-value spreads.";

  return {
    trap_score: Number(trap_score.toFixed(2)),
    trap_flags: flagList,
    summary,
    ticket_traps: ticketTraps
  };
}
