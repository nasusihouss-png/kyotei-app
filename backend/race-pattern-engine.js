function clamp01(x) {
  if (!Number.isFinite(x)) return 0;
  if (x < 0) return 0;
  if (x > 1) return 1;
  return x;
}

function toIndex(raw, scale = 20, base = 50) {
  const v = base + raw * scale;
  const clamped = Math.max(0, Math.min(100, v));
  return Number(clamped.toFixed(2));
}

function laneItemBy(ranking, lane) {
  return ranking.find((r) => r?.racer?.lane === lane) || {
    score: 0,
    features: {
      st_inv: 0,
      class_score: 0,
      local_minus_nation: 0,
      exhibition_rank: null,
      st_rank: null,
      is_outer: 0
    },
    racer: { lane }
  };
}

function safeStdDev(values) {
  const v = values.filter((x) => Number.isFinite(x));
  if (!v.length) return 0;
  const mean = v.reduce((a, b) => a + b, 0) / v.length;
  const variance = v.reduce((a, b) => a + (b - mean) ** 2, 0) / v.length;
  return Math.sqrt(variance);
}

export function analyzeRacePattern(ranking) {
  const l1 = laneItemBy(ranking, 1);
  const l2 = laneItemBy(ranking, 2);
  const l3 = laneItemBy(ranking, 3);
  const l4 = laneItemBy(ranking, 4);
  const l5 = laneItemBy(ranking, 5);
  const l6 = laneItemBy(ranking, 6);

  const s1 = l1.score;
  const s2 = l2.score;
  const s3 = l3.score;
  const s4 = l4.score;
  const s5 = l5.score;
  const s6 = l6.score;

  const st1 = l1.features.st_inv || 0;
  const st2 = l2.features.st_inv || 0;
  const st3 = l3.features.st_inv || 0;
  const st4 = l4.features.st_inv || 0;
  const wind = Number(l1.features.wind_speed || l2.features.wind_speed || 0);

  // 1) Escape: lane1 strength minus lane2/3 pressure.
  const escapeRaw =
    (s1 - (s2 + s3) / 2) * 0.06 +
    (st1 - Math.max(st2, st3)) * 0.35 +
    (l1.features.class_score - (l2.features.class_score + l3.features.class_score) / 2) * 0.25;
  const escape_index = toIndex(escapeRaw);

  // 2) Sashi: lane2 attack potential against lane1.
  const sashiRaw =
    (s2 - s1) * 0.08 +
    (st2 - st1) * 0.5 +
    (l2.features.local_minus_nation - l1.features.local_minus_nation) * 0.3 +
    ((l2.features.st_rank === 1 ? 1 : 0) - (l1.features.st_rank === 1 ? 1 : 0)) * 0.4;
  const sashi_index = toIndex(sashiRaw);

  // 3) Makuri: lane3 attack, stronger when lane3 beats lane1/2 in ST or score.
  const makuriRaw =
    (s3 - Math.max(s1, s2)) * 0.08 +
    (st3 - Math.max(st1, st2)) * 0.6 +
    (l3.features.exhibition_rank === 1 ? 0.4 : 0) +
    (l3.features.st_rank === 1 ? 0.4 : 0);
  const makuri_index = toIndex(makuriRaw);

  // 4) Makurizashi: balanced attack from lane3/lane4 pair.
  const midScore = (s3 + s4) / 2;
  const midSt = (st3 + st4) / 2;
  const makurizashiRaw =
    (midScore - s1) * 0.07 +
    (midSt - st1) * 0.5 +
    (((l3.features.st_rank || 99) <= 3 ? 1 : 0) + ((l4.features.st_rank || 99) <= 3 ? 1 : 0)) *
      0.2;
  const makurizashi_index = toIndex(makurizashiRaw);

  // 5) Chaos: lane1 weak, outside strong, or score spread tight.
  const scoreList = [s1, s2, s3, s4, s5, s6];
  const sortedScores = [...scoreList].sort((a, b) => b - a);
  const topGap = sortedScores[0] - sortedScores[2];
  const spread = safeStdDev(scoreList);
  const outsidePressure = Math.max(s5, s6) - s1;
  const l1Weakness = ((s2 + s3 + s4) / 3 - s1) * 0.08;
  const tightRaceFactor = clamp01((12 - topGap) / 12);
  const lowSpreadFactor = clamp01((8 - spread) / 8);
  const windOutsideChaos = wind >= 6 ? 0.2 : 0;
  const chaosRaw = l1Weakness + outsidePressure * 0.05 + tightRaceFactor * 0.8 + lowSpreadFactor * 0.8 + windOutsideChaos;
  const chaos_index = toIndex(chaosRaw, 22, 35);

  const indexes = {
    escape_index,
    sashi_index,
    makuri_index,
    makurizashi_index,
    chaos_index
  };

  const patternScores = [
    { key: "escape", value: escape_index },
    { key: "sashi", value: sashi_index },
    { key: "makuri", value: makuri_index },
    { key: "makurizashi", value: makurizashi_index },
    { key: "chaos", value: chaos_index }
  ].sort((a, b) => b.value - a.value);

  const race_pattern = patternScores[0].key;
  const confidenceGap = patternScores[0].value - patternScores[1].value;

  let buy_type = "standard";
  if (chaos_index >= 78) {
    buy_type = "skip";
  } else if (race_pattern === "escape" && escape_index >= 68 && confidenceGap >= 8 && chaos_index < 55) {
    buy_type = "solid";
  } else if (
    race_pattern === "chaos" ||
    race_pattern === "makuri" ||
    race_pattern === "makurizashi" ||
    chaos_index >= 62
  ) {
    buy_type = "aggressive";
  }

  return {
    race_pattern,
    buy_type,
    indexes
  };
}
