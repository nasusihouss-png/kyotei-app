function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function variance(values) {
  const arr = (values || []).filter((v) => Number.isFinite(v));
  if (!arr.length) return 0;
  const mean = arr.reduce((a, b) => a + b, 0) / arr.length;
  return arr.reduce((a, b) => a + (b - mean) ** 2, 0) / arr.length;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function normalizeMode(raw) {
  const mode = String(raw || "active").trim().toLowerCase();
  if (mode === "conservative") return "conservative";
  if (mode === "standard") return "standard";
  return "active";
}

function modeConfig(mode) {
  if (mode === "conservative") {
    return { skipThreshold: 97, microThreshold: 90, smallThreshold: 60, modeAdjust: 4 };
  }
  if (mode === "standard") {
    return { skipThreshold: 97, microThreshold: 90, smallThreshold: 60, modeAdjust: 0 };
  }
  return { skipThreshold: 97, microThreshold: 90, smallThreshold: 60, modeAdjust: -6 };
}

function classifyDangerType({ chaosIndex, stVariance, entryVariance, motorVariance, concentrationRisk }) {
  const candidates = [
    { key: "chaos", value: chaosIndex },
    { key: "stability_break", value: stVariance },
    { key: "entry_instability", value: entryVariance },
    { key: "trend_instability", value: motorVariance },
    { key: "confidence_low", value: concentrationRisk }
  ].sort((a, b) => b.value - a.value);

  return candidates[0]?.key || "mixed";
}

function buildSkipSummary(reasonCodes, recommendation) {
  if (recommendation !== "SKIP") return "参加可能レンジ";

  const reasons = [];
  if (reasonCodes.includes("ENTRY_CHAOS_SEVERE")) reasons.push("進入乱れ");
  if (reasonCodes.includes("ST_VARIANCE_HIGH")) reasons.push("STばらつき");
  if (reasonCodes.includes("MOTOR_TREND_VARIANCE_HIGH")) reasons.push("モーター気配差");
  if (reasonCodes.includes("TOP3_CONCENTRATION_CRITICAL")) reasons.push("上位候補の集中度低下");
  if (reasonCodes.includes("ARE_INDEX_HIGH")) reasons.push("荒れ警戒");
  if (reasonCodes.includes("CHAOS_INDEX_HIGH")) reasons.push("展開混戦");
  if (reasonCodes.includes("MULTI_INSTABILITY")) reasons.push("不安定要因の重なり");
  if (reasonCodes.includes("CRITICAL_DATA_MISSING")) reasons.push("必須データ欠損");

  if (reasons.length === 0) return "高リスク要因のため見送り";
  if (reasons.length === 1) return `${reasons[0]}が大きいため見送り`;
  if (reasons.length === 2) return `${reasons[0]}と${reasons[1]}が大きいため見送り`;
  return `${reasons[0]}と${reasons[1]}が大きく、${reasons[2]}も重なるため見送り`;
}

export function evaluateRaceRisk({ indexes, racePattern, ranking, are_index, probabilities, participation_mode }) {
  const participationMode = normalizeMode(participation_mode);
  const cfg = modeConfig(participationMode);

  const chaos_index = toNumber(indexes?.chaos_index, 50);
  const areIndex = toNumber(are_index, 0);

  const stRanks = (ranking || []).map((r) => toNumber(r?.features?.st_rank, NaN));
  const validSt = stRanks.filter((v) => Number.isFinite(v));
  const stSpreadRaw = validSt.length > 0 ? Math.max(...validSt) - Math.min(...validSt) : 0;
  const st_variance = clamp(0, 100, (stSpreadRaw / 5) * 100);

  const entryScores = (ranking || []).map((r) => toNumber(r?.features?.entry_advantage_score, 0));
  const motorTrendScores = (ranking || []).map((r) => toNumber(r?.features?.motor_trend_score, 0));
  const exhibitionGaps = (ranking || []).map((r) => toNumber(r?.features?.exhibition_gap_from_best, NaN));

  const entryVarianceRaw = variance(entryScores);
  const motorVarianceRaw = variance(motorTrendScores);
  const exhibitionGapVarianceRaw = variance(exhibitionGaps);
  const validExh = exhibitionGaps.filter((v) => Number.isFinite(v));
  const exhibitionGapMean = validExh.reduce((a, b) => a + b, 0) / (validExh.length || 1);

  const entry_variance = clamp(0, 100, entryVarianceRaw * 10);
  const motor_variance = clamp(0, 100, motorVarianceRaw * 7);
  const exhibition_risk = clamp(0, 100, exhibitionGapMean * 360 + exhibitionGapVarianceRaw * 520);

  const probs = Array.isArray(probabilities) ? probabilities : [];
  const sortedProb = [...probs]
    .map((x) => toNumber(x?.p ?? x?.prob, 0))
    .filter((x) => Number.isFinite(x) && x > 0)
    .sort((a, b) => b - a);
  const top1 = sortedProb[0] || 0;
  const top3sum = (sortedProb[0] || 0) + (sortedProb[1] || 0) + (sortedProb[2] || 0);
  const concentrationRisk = clamp(0, 100, (0.26 - top1) * 85 + (0.5 - top3sum) * 70);

  let pattern_adjust = 0;
  if (racePattern === "chaos") pattern_adjust += 5;
  else if (racePattern === "makuri" || racePattern === "makurizashi") pattern_adjust += 2;
  else if (racePattern === "escape") pattern_adjust -= 3;

  const baseRisk =
    chaos_index * 0.12 +
    st_variance * 0.32 +
    entry_variance * 0.06 +
    motor_variance * 0.05 +
    areIndex * 0.04 +
    concentrationRisk * 0.12 +
    exhibition_risk * 0.08 +
    pattern_adjust +
    cfg.modeAdjust;

  let risk_score = Math.round(clamp(0, 100, baseRisk));

  const severeEntryChaos = entry_variance >= 90 && st_variance >= 78;
  const criticalLowConcentration = top1 < 0.12 || top3sum < 0.32;

  let instabilitySignals = 0;
  if (chaos_index >= 82) instabilitySignals += 1;
  if (areIndex >= 76) instabilitySignals += 1;
  if (entry_variance >= 70) instabilitySignals += 1;
  if (motor_variance >= 62) instabilitySignals += 1;
  if (st_variance >= 70) instabilitySignals += 1;
  if (criticalLowConcentration) instabilitySignals += 1;

  const criticalDataBroken =
    !Array.isArray(ranking) ||
    ranking.length < 3 ||
    !Array.isArray(probabilities) ||
    probabilities.length === 0;

  let recommendation = "FULL BET";
  if (criticalDataBroken || risk_score > cfg.skipThreshold) recommendation = "SKIP";
  else if (risk_score > cfg.microThreshold) recommendation = "MICRO BET";
  else if (risk_score > cfg.smallThreshold) recommendation = "SMALL BET";

  const skip_reason_codes = [];
  if (chaos_index >= 78) skip_reason_codes.push("CHAOS_INDEX_HIGH");
  if (areIndex >= 72) skip_reason_codes.push("ARE_INDEX_HIGH");
  if (severeEntryChaos) skip_reason_codes.push("ENTRY_CHAOS_SEVERE");
  if (motor_variance >= 60) skip_reason_codes.push("MOTOR_TREND_VARIANCE_HIGH");
  if (criticalLowConcentration) skip_reason_codes.push("TOP3_CONCENTRATION_CRITICAL");
  if (instabilitySignals >= 4) skip_reason_codes.push("MULTI_INSTABILITY");
  if (criticalDataBroken) skip_reason_codes.push("CRITICAL_DATA_MISSING");

  const danger_type = classifyDangerType({
    chaosIndex: chaos_index + areIndex * 0.35,
    stVariance: st_variance,
    entryVariance: entry_variance,
    motorVariance: motor_variance,
    concentrationRisk
  });

  const skip_confidence = Number(
    clamp(
      0,
      1,
      (risk_score / 100) * 0.5 +
        (skip_reason_codes.length / 8) * 0.3 +
        (criticalDataBroken ? 0.2 : 0)
    ).toFixed(4)
  );

  return {
    risk_score,
    recommendation,
    participation_mode: participationMode,
    skip_confidence,
    danger_type,
    skip_reason_codes,
    skip_summary: buildSkipSummary(skip_reason_codes, recommendation)
  };
}
