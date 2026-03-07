function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function pickHeadStrategy({ escapeProb, sashiProb, makuriProb, areIndex, riskRecommendation }) {
  if (riskRecommendation === "SKIP" || areIndex >= 82) {
    return "mixed";
  }

  const maxProb = Math.max(escapeProb, sashiProb, makuriProb);
  if (maxProb === escapeProb) return "1-head";
  if (maxProb === sashiProb) return "2-head";
  return "3-head";
}

function pickCoverageLevel({
  escapeProb,
  sashiProb,
  makuriProb,
  areIndex,
  riskRecommendation
}) {
  if (riskRecommendation === "SKIP" || areIndex >= 82) return "skip";

  if (escapeProb >= 0.5 && areIndex <= 45) return "narrow";
  if (sashiProb >= 0.4 || makuriProb >= 0.4) return "medium";
  if (areIndex >= 68) return "wide";
  return "medium";
}

function buildStrategySummary({
  headStrategy,
  coverageLevel,
  escapeProb,
  sashiProb,
  makuriProb,
  areIndex
}) {
  if (coverageLevel === "skip") {
    return "荒れ警戒が強く、見送り推奨";
  }

  const headText =
    headStrategy === "1-head"
      ? "1頭固定"
      : headStrategy === "2-head"
        ? "2頭固定"
        : headStrategy === "3-head"
          ? "3頭固定"
          : "軸を固定せず";

  const flowText =
    coverageLevel === "narrow"
      ? "相手を絞って流す"
      : coverageLevel === "medium"
        ? "本線＋押さえで流す"
        : "手広くフォーメーションで構成";

  const signal =
    escapeProb >= sashiProb && escapeProb >= makuriProb
      ? "逃げ寄り"
      : sashiProb >= makuriProb
        ? "差し寄り"
        : "まくり寄り";

  const areText = areIndex >= 70 ? "荒れ警戒強め" : areIndex >= 55 ? "やや荒れ警戒" : "堅め想定";

  return `${headText}で${flowText}（${signal}・${areText}）`;
}

export function buildTicketStrategy({
  raceOutcomeProbabilities,
  raceIndexes,
  raceRisk
}) {
  const escapeProb = toNum(raceOutcomeProbabilities?.escape_success_prob);
  const sashiProb = toNum(raceOutcomeProbabilities?.sashi_success_prob);
  const makuriProb = toNum(raceOutcomeProbabilities?.makuri_success_prob);
  const areIndex = toNum(raceIndexes?.are_index);
  const riskRecommendation = String(raceRisk?.recommendation || "").toUpperCase();

  const head_strategy = pickHeadStrategy({
    escapeProb,
    sashiProb,
    makuriProb,
    areIndex,
    riskRecommendation
  });

  const coverage_level = pickCoverageLevel({
    escapeProb,
    sashiProb,
    makuriProb,
    areIndex,
    riskRecommendation
  });

  return {
    head_strategy,
    coverage_level,
    strategy_summary: buildStrategySummary({
      headStrategy: head_strategy,
      coverageLevel: coverage_level,
      escapeProb,
      sashiProb,
      makuriProb,
      areIndex
    })
  };
}
