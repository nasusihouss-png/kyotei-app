function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function rankQuality(rank, maxRank = 6) {
  const r = toNum(rank, maxRank);
  return clamp(0, 1, (maxRank + 1 - r) / maxRank);
}

function laneItem(ranking, lane) {
  return (
    (ranking || []).find((r) => toNum(r?.racer?.lane) === lane) || {
      score: 0,
      features: {},
      racer: { lane }
    }
  );
}

function stdDev(values) {
  const v = (values || []).filter((x) => Number.isFinite(x));
  if (!v.length) return 0;
  const mean = v.reduce((a, b) => a + b, 0) / v.length;
  const variance = v.reduce((a, b) => a + (b - mean) ** 2, 0) / v.length;
  return Math.sqrt(variance);
}

function computeEntryChangeIntensity(ranking) {
  const rows = ranking || [];
  let changed = 0;
  let deepIn = 0;
  for (const r of rows) {
    const lane = toNum(r?.racer?.lane);
    const entry = toNum(r?.features?.entry_course);
    if (lane && entry && lane !== entry) {
      changed += 1;
      if ((lane >= 5 && entry <= 4) || (lane === 1 && entry > 1)) deepIn += 1;
    }
  }
  return {
    changed,
    deepIn,
    intensity: changed * 8 + deepIn * 6
  };
}

function buildSummary({ nige, sashi, makuri, are }) {
  const nigeText = nige >= 65 ? "逃げ有力" : nige >= 50 ? "逃げまずまず" : "逃げ弱め";
  const sashiText = sashi >= 65 ? "差し有力" : sashi >= 50 ? "差し注意" : "差し控えめ";
  const makuriText = makuri >= 65 ? "まくり有力" : makuri >= 50 ? "まくり注意" : "まくり弱め";
  const areText = are >= 70 ? "荒れ警戒強め" : are >= 55 ? "やや荒れ警戒" : "堅め寄り";
  return `${nigeText}、${sashiText}、${makuriText}、${areText}`;
}

function detectRecommendedStyle({ nige, sashi, makuri, are }) {
  if (are >= 72) return "chaos";
  const maxMain = Math.max(nige, sashi, makuri);
  const sorted = [nige, sashi, makuri].sort((a, b) => b - a);
  const gap = sorted[0] - sorted[1];
  if (gap < 6) return "mixed";
  if (maxMain === nige) return "nige-main";
  if (maxMain === sashi) return "sashi-main";
  return "makuri-main";
}

export function analyzeRaceIndexes({
  ranking,
  top3,
  racePattern,
  indexes,
  raceRisk
}) {
  const rows = ranking || [];
  const l1 = laneItem(rows, 1);
  const l2 = laneItem(rows, 2);
  const l3 = laneItem(rows, 3);
  const l4 = laneItem(rows, 4);

  const s1 = toNum(l1.score);
  const s2 = toNum(l2.score);
  const s3 = toNum(l3.score);
  const s4 = toNum(l4.score);

  const l1Ex = rankQuality(l1.features?.exhibition_rank);
  const l1St = rankQuality(l1.features?.st_rank);
  const l1Course = toNum(l1.features?.course_fit_score);
  const l1EntryAdv = toNum(l1.features?.entry_advantage_score);
  const l1EntryCourse = toNum(l1.features?.entry_course);

  const attack23 = (s2 + s3) / 2;
  const l1EntryPenalty = l1EntryCourse && l1EntryCourse !== 1 ? 8 : 0;
  const l1DeepPenalty = l1EntryAdv < -2 ? Math.abs(l1EntryAdv) * 2 : 0;

  // 1) nige_index
  let nige =
    45 +
    (s1 - attack23) * 0.45 +
    l1Ex * 11 +
    l1St * 11 +
    l1Course * 0.9 -
    l1EntryPenalty -
    l1DeepPenalty;

  // 2) sashi_index
  const l2Ex = rankQuality(l2.features?.exhibition_rank);
  const l2St = rankQuality(l2.features?.st_rank);
  const l1Weakness = clamp(0, 1, (attack23 - s1) / 25);
  let sashi =
    42 +
    (s2 - s1) * 0.35 +
    l2St * 13 +
    l2Ex * 10 +
    l1Weakness * 14;
  if (racePattern === "sashi") sashi += 8;

  // 3) makuri_index
  const l3Power =
    s3 * 0.45 +
    toNum(l3.features?.motor_total_score) * 1.1 +
    toNum(l3.features?.motor_trend_score) * 0.9 +
    toNum(l3.features?.entry_advantage_score) * 1.2;
  const l4Power =
    s4 * 0.45 +
    toNum(l4.features?.motor_total_score) * 1.1 +
    toNum(l4.features?.motor_trend_score) * 0.9 +
    toNum(l4.features?.entry_advantage_score) * 1.2;
  const lane2WallWeak = clamp(0, 1, (s3 - s2) / 20);
  let makuri = 40 + Math.max(l3Power, l4Power) * 0.22 + lane2WallWeak * 10;
  if (racePattern === "makuri") makuri += 9;
  if (racePattern === "makurizashi") makuri += 7;

  // 4) are_index
  const chaos = toNum(indexes?.chaos_index);
  const risk = toNum(raceRisk?.risk_score);
  const scoreSpread = stdDev(rows.map((r) => toNum(r.score)));
  const tightGapBoost = clamp(0, 18, (9 - scoreSpread) * 2);
  const stVar = stdDev(rows.map((r) => toNum(r.features?.st_rank)));
  const trendVar = stdDev(rows.map((r) => toNum(r.features?.motor_trend_score)));
  const entryChange = computeEntryChangeIntensity(rows);
  let are =
    25 +
    chaos * 0.45 +
    risk * 0.35 +
    entryChange.intensity +
    tightGapBoost +
    stVar * 2.5 +
    trendVar * 3.5;

  // top3 signal (small adjustment only)
  const t3 = Array.isArray(top3) ? top3 : [];
  if (t3.length === 3 && t3[0] !== 1) are += 4;

  nige = Number(clamp(0, 100, nige).toFixed(2));
  sashi = Number(clamp(0, 100, sashi).toFixed(2));
  makuri = Number(clamp(0, 100, makuri).toFixed(2));
  are = Number(clamp(0, 100, are).toFixed(2));

  return {
    nige_index: nige,
    sashi_index: sashi,
    makuri_index: makuri,
    are_index: are,
    index_summary: buildSummary({ nige, sashi, makuri, are }),
    recommended_style: detectRecommendedStyle({ nige, sashi, makuri, are })
  };
}
