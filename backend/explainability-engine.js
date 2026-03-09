function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function uniq(items) {
  return Array.from(new Set((items || []).filter(Boolean)));
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function tagFromBucket(bucket) {
  const b = String(bucket || "").toLowerCase();
  if (b === "main") return "本線";
  if (b === "backup") return "押さえ";
  if (b === "longshot") return "穴";
  return "通常";
}

export function buildRaceExplainability({
  raceDecision,
  raceRisk,
  raceFlow,
  raceIndexes,
  entryMeta,
  startSignals,
  manualLapImpact,
  headSelection,
  scenarioSuggestions
}) {
  const tags = [];
  const nigeProb = toNum(raceFlow?.nige_prob, 0);
  const flowMode = String(raceFlow?.race_flow_mode || "");
  const fastestStLane = toNum(startSignals?.fastest_st_lane, 0);
  const delayedLanes = Array.isArray(startSignals?.delayed_lanes) ? startSignals.delayed_lanes : [];
  const rec = String(raceDecision?.mode || raceRisk?.recommendation || "").toUpperCase();
  const chaos = toNum(raceIndexes?.are_index, toNum(raceRisk?.risk_score, 50));

  if (nigeProb >= 0.5 || flowMode === "nige" || flowMode === "escape") tags.push("Strong Escape");
  if (fastestStLane >= 1 && fastestStLane <= 6) tags.push(`Fast ST ${fastestStLane}号艇`);
  if (delayedLanes.length) tags.push(`遅れ注意 ${delayedLanes.join("/")}`);
  if (entryMeta?.entry_changed) tags.push("Entry Changed");
  if (manualLapImpact?.enabled) tags.push("Manual Boost");
  if (chaos >= 70) tags.push("Caution");
  if (rec === "FULL_BET") tags.push("Strong Recommendation");
  else if (rec === "SMALL_BET" || rec === "MICRO BET" || rec === "MICRO_BET") tags.push("Moderate Recommendation");
  else if (rec === "SKIP") tags.push("Avoid");

  const lane = Number(headSelection?.main_head || fastestStLane || 0);
  const scenario = scenarioSuggestions?.scenario_type || flowMode || "standard";
  const summaryParts = [];
  if (nigeProb >= 0.5) summaryParts.push("イン逃げ傾向");
  if (lane >= 1 && lane <= 6) summaryParts.push(`${lane}号艇先手期待`);
  if (entryMeta?.entry_changed) summaryParts.push("進入変化で信頼度調整");
  if (manualLapImpact?.enabled && toNum(manualLapImpact?.average_adjustment, 0) > 0) summaryParts.push("手動展示評価で上方補正");
  if (chaos >= 70) summaryParts.push("波乱注意");
  if (!summaryParts.length) summaryParts.push(`${scenario}シナリオ`);

  return {
    race_tags: uniq(tags).slice(0, 8),
    race_summary: summaryParts.join(" / "),
    recommendation_label:
      rec === "FULL_BET" ? "strong" : rec === "SMALL_BET" || rec === "MICRO BET" || rec === "MICRO_BET" ? "moderate" : "avoid"
  };
}

export function buildBetExplainability({
  tickets,
  bucketByCombo,
  headSelection,
  entryMeta,
  startSignals,
  scenarioSuggestions
}) {
  const mainHead = toNum(headSelection?.main_head, 0);
  const delayed = Array.isArray(startSignals?.delayed_lanes) ? startSignals.delayed_lanes : [];
  const scenarioType = String(scenarioSuggestions?.scenario_type || "");
  const result = {};

  for (const ticket of tickets || []) {
    const combo = normalizeCombo(ticket?.combo);
    if (!combo) continue;
    const lanes = combo.split("-").map((v) => Number(v));
    const bucket = String(
      ticket?.suggestion_bucket ||
        bucketByCombo?.[combo] ||
        ticket?.ticket_type ||
        "backup"
    ).toLowerCase();
    const tags = [tagFromBucket(bucket)];

    if (mainHead && lanes[0] === mainHead) tags.push("頭本命整合");
    if (delayed.length && lanes.some((l) => delayed.includes(l))) tags.push("遅れ艇含み");
    if (entryMeta?.entry_changed) tags.push("進入変化考慮");
    if (scenarioType.includes("escape")) tags.push("逃げ本線");
    if (scenarioType.includes("outside") || scenarioType.includes("pressure")) tags.push("外伸び警戒");

    let summary = "通常構成";
    if (bucket === "main") {
      summary = `本線: ${mainHead ? `${mainHead}頭軸` : "頭軸"}と相手本線構成`;
    } else if (bucket === "backup") {
      summary = "押さえ: 進入/ST変動リスクに備える保険";
    } else if (bucket === "longshot") {
      summary = "穴: 外圧・展開ずれ時の限定カバー";
    }

    result[combo] = {
      combo,
      suggestion_bucket: bucket,
      explanation_tags: uniq(tags).slice(0, 5),
      explanation_summary: summary
    };
  }

  return result;
}

