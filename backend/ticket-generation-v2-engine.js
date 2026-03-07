function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function uniqueLanes(list) {
  return [...new Set((Array.isArray(list) ? list : []).map((x) => Number(x)).filter((x) => x >= 1 && x <= 6))];
}

function makeCombo(a, b, c) {
  const lanes = [a, b, c].map((x) => Number(x));
  if (lanes.some((x) => !Number.isInteger(x) || x < 1 || x > 6)) return null;
  if (new Set(lanes).size !== 3) return null;
  return lanes.join("-");
}

function addCombo(set, a, b, c) {
  const combo = makeCombo(a, b, c);
  if (combo) set.add(combo);
}

function filtered(list, excluded) {
  return uniqueLanes(list).filter((x) => !excluded.includes(x));
}

function buildHeadFixed({ mainHead, mainPartners, backupPartners, excluded }) {
  const primary = new Set();
  const secondary = new Set();

  const core = filtered(mainPartners, excluded).filter((x) => x !== mainHead);
  const backup = filtered(backupPartners, excluded).filter((x) => x !== mainHead);

  for (let i = 0; i < core.length; i += 1) {
    for (let j = i + 1; j < core.length; j += 1) {
      addCombo(primary, mainHead, core[i], core[j]);
      addCombo(primary, mainHead, core[j], core[i]);
    }
  }

  // secondary uses backup partners in addition to core
  const ext = [...new Set([...core, ...backup])];
  for (let i = 0; i < ext.length; i += 1) {
    for (let j = 0; j < ext.length; j += 1) {
      if (i === j) continue;
      addCombo(secondary, mainHead, ext[i], ext[j]);
    }
  }

  return {
    strategy_type: "head-fixed",
    primary_tickets: [...primary].slice(0, 8),
    secondary_tickets: [...secondary].filter((x) => !primary.has(x)).slice(0, 10),
    summary: `${mainHead}頭固定で${ext.join(",")}へ流す`
  };
}

function buildHeadSpread({ heads, mainPartners, backupPartners, excluded }) {
  const primary = new Set();
  const secondary = new Set();

  const core = filtered(mainPartners, excluded);
  const backup = filtered(backupPartners, excluded);

  // Primary: only core partners
  for (const h of heads) {
    for (let i = 0; i < core.length; i += 1) {
      for (let j = 0; j < core.length; j += 1) {
        if (i === j || core[i] === h || core[j] === h) continue;
        addCombo(primary, h, core[i], core[j]);
      }
    }
  }

  // Secondary: expand with backup partners
  const ext = [...new Set([...core, ...backup])];
  for (const h of heads) {
    for (let i = 0; i < ext.length; i += 1) {
      for (let j = 0; j < ext.length; j += 1) {
        if (i === j || ext[i] === h || ext[j] === h) continue;
        addCombo(secondary, h, ext[i], ext[j]);
      }
    }
  }

  return {
    strategy_type: "head-spread",
    primary_tickets: [...primary].slice(0, 10),
    secondary_tickets: [...secondary].filter((x) => !primary.has(x)).slice(0, 14),
    summary: `${heads.join(",")}頭分散で${ext.join(",")}相手`
  };
}

function buildChaosLight({ heads, mainPartners, backupPartners, excluded }) {
  const primary = new Set();
  const secondary = new Set();

  const core = filtered(mainPartners, excluded).slice(0, 3);
  const backup = filtered(backupPartners, excluded).slice(0, 2);
  const ext = [...new Set([...core, ...backup])];

  for (const h of heads.slice(0, 2)) {
    for (let i = 0; i < core.length; i += 1) {
      for (let j = i + 1; j < core.length; j += 1) {
        if (core[i] === h || core[j] === h) continue;
        addCombo(primary, h, core[i], core[j]);
        addCombo(primary, h, core[j], core[i]);
      }
    }
  }

  for (const h of heads.slice(0, 2)) {
    for (const a of ext) {
      for (const b of ext) {
        if (a === b || a === h || b === h) continue;
        addCombo(secondary, h, a, b);
      }
    }
  }

  return {
    strategy_type: "chaos-light",
    primary_tickets: [...primary].slice(0, 8),
    secondary_tickets: [...secondary].filter((x) => !primary.has(x)).slice(0, 8),
    summary: `${heads.slice(0, 2).join(",")}頭中心、点数抑制で軽め運用`
  };
}

export function generateTicketsV2({
  headSelection,
  partnerSelection,
  headConfidence,
  headPrecision,
  exhibitionAI,
  raceRisk,
  raceIndexes,
  wallEvaluation,
  venueBias,
  marketTrap
}) {
  const mainHead = Number(headSelection?.main_head);
  const secondaryHeads = uniqueLanes(headSelection?.secondary_heads);
  const fadeLanes = uniqueLanes(partnerSelection?.fade_lanes);
  const mainPartners = uniqueLanes(partnerSelection?.main_partners);
  const backupPartners = uniqueLanes(partnerSelection?.backup_partners);

  const risk = toNum(raceRisk?.risk_score);
  const recommendation = String(raceRisk?.recommendation || "").toUpperCase();
  const areIndex = toNum(raceIndexes?.are_index);
  const wallBreakRisk = toNum(wallEvaluation?.wall_break_risk, 50);

  const headFixedOk = !!headConfidence?.head_fixed_ok;
  const spreadNeeded = !!headConfidence?.head_spread_needed;
  const headWin = toNum(headPrecision?.head_win_score, 50);
  const headGap = toNum(headPrecision?.head_gap_score, 50);
  const exAI = toNum(exhibitionAI?.exhibition_ai_score, 50);
  const venueInner = toNum(venueBias?.venue_inner_reliability, 50);
  const venueChaos = toNum(venueBias?.venue_chaos_factor, 50);
  const venueStyle = String(venueBias?.venue_style_bias || "balanced");
  const trapScore = toNum(marketTrap?.trap_score, 0);

  if (recommendation === "SKIP") {
    return {
      strategy_type: "skip",
      primary_tickets: [],
      secondary_tickets: [],
      excluded_lanes: fadeLanes,
      summary: "高リスクのため見送り"
    };
  }

  let result;
  const compactMode = venueStyle === "inner" && venueInner >= 60 && venueChaos <= 55;
  const spreadMode = venueStyle === "chaos" || venueChaos >= 63;
  if (
    headFixedOk &&
    !spreadNeeded &&
    risk <= (compactMode ? 85 : 82) &&
    areIndex < (compactMode ? 70 : 68) &&
    wallBreakRisk < 60 &&
    headWin >= (compactMode ? 55 : 58) &&
    headGap >= (compactMode ? 34 : 38)
  ) {
    result = buildHeadFixed({
      mainHead,
      mainPartners: headGap >= 52 || exAI >= 66 ? mainPartners.slice(0, 2) : mainPartners,
      backupPartners,
      excluded: fadeLanes
    });
  } else if (areIndex >= 78 || risk > 90 || wallBreakRisk >= 70 || spreadMode || trapScore >= 62) {
    const heads = uniqueLanes([mainHead, ...secondaryHeads]).slice(0, 3);
    result = buildChaosLight({
      heads,
      mainPartners,
      backupPartners,
      excluded: fadeLanes
    });
  } else {
    const heads = uniqueLanes([mainHead, ...secondaryHeads]).slice(0, 3);
    result = buildHeadSpread({
      heads,
      mainPartners,
      backupPartners,
      excluded: fadeLanes
    });
  }

  const secondaryLimited = trapScore >= 62 ? result.secondary_tickets.slice(0, 6) : result.secondary_tickets;

  return {
    ...result,
    secondary_tickets: secondaryLimited,
    summary: `${result.summary} (頭精度:${headWin.toFixed(1)}/${headGap.toFixed(1)} 展示:${exAI.toFixed(1)} 場傾向:${venueStyle})`,
    excluded_lanes: fadeLanes
  };
}
