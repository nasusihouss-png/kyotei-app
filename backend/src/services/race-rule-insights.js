import { getVenueComparisonRule } from "../config/venue-comparison-rules.js";
import { DECISION_PATTERN_RULES } from "../config/decision-pattern-rules.js";
import { INDICATOR_THRESHOLD_RULES } from "../config/indicator-threshold-rules.js";
import { getLaneRoleRule } from "../config/lane-role-rules.js";
import { getLaneWeightRule, LANE_WEIGHT_MEANINGS } from "../config/lane-weight-rules.js";

const FIXED_HARD_RACE_COMBOS = Object.freeze(["1-2-3", "1-2-4", "1-3-2", "1-3-4", "1-4-2", "1-4-3"]);

function toNum(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 1) {
  if (!Number.isFinite(Number(value))) return null;
  return Number(Number(value).toFixed(digits));
}

function normalizeScore(value) {
  const n = toNum(value, null);
  if (n === null) return null;
  if (n <= 1) return clamp(0, 100, n * 100);
  if (n <= 10) return clamp(0, 100, n * 10);
  return clamp(0, 100, n);
}

function average(values = []) {
  const usable = values.filter((value) => Number.isFinite(Number(value)));
  if (!usable.length) return null;
  return usable.reduce((sum, value) => sum + Number(value), 0) / usable.length;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeEntryOrder(order = []) {
  const normalized = safeArray(order)
    .map((lane) => Number(lane))
    .filter((lane) => Number.isInteger(lane) && lane >= 1 && lane <= 6);
  return normalized.length === 6 && new Set(normalized).size === 6 ? normalized : [];
}

function pickVenueName({ race = null, venueRule = null }) {
  return race?.venueName || venueRule?.venueName || null;
}

function getRaceSignals({ pureTop6Prediction = null, hardRace1234 = null, racers = [], prediction = null, supportSummary = null } = {}) {
  const headRanking = Array.isArray(pureTop6Prediction?.head_candidate_ranking) ? pureTop6Prediction.head_candidate_ranking : [];
  const topHeadLane = toNum(headRanking?.[0]?.lane, null);
  const headProb1 = normalizeScore(
    pureTop6Prediction?.head_prob_1 ??
    hardRace1234?.boat1_head_pre ??
    (topHeadLane === 1 ? headRanking?.[0]?.probability : null)
  );
  const secondGivenHead = pureTop6Prediction?.second_given_head_probabilities && typeof pureTop6Prediction.second_given_head_probabilities === "object"
    ? pureTop6Prediction.second_given_head_probabilities
    : prediction?.second_given_head_probabilities && typeof prediction.second_given_head_probabilities === "object"
      ? prediction.second_given_head_probabilities
      : {};
  const exhibitionStValues = racers
    .map((row) => toNum(row?.exhibitionSt, null))
    .filter((value) => value !== null)
    .sort((a, b) => a - b);
  const displayGap = exhibitionStValues.length >= 2 ? exhibitionStValues[exhibitionStValues.length - 1] - exhibitionStValues[0] : null;
  const lapScores = racers.map((row) => normalizeScore(row?.lapTime != null ? 100 - Number(row.lapTime) * 10 : null)).filter((value) => value !== null);
  const motorScores = racers.map((row) => normalizeScore(row?.motor2ren ?? row?.motor2Rate)).filter((value) => value !== null);
  const lane2Second = normalizeScore(secondGivenHead?.[2]);
  const lane3Attack = normalizeScore(pureTop6Prediction?.lane3_attack_keep_score);
  const lane4Tenkai = normalizeScore(pureTop6Prediction?.lane4_tenkaisashi_score);
  const lane2Sashi = normalizeScore(pureTop6Prediction?.lane2_sashi_keep_score);
  const chaos = normalizeScore(pureTop6Prediction?.chaos_level);
  const outsideRisk = normalizeScore(hardRace1234?.outside_break_risk_pre ?? hardRace1234?.outside_break_risk);
  const hardRaceIndex = normalizeScore(hardRace1234?.hard_race_index);
  const entryRisk = clamp(
    0,
    100,
    (prediction?.entry_changed === true ? 42 : 10) +
      (prediction?.entry_change_type && prediction.entry_change_type !== "none" ? 18 : 0) +
      (supportSummary?.recommendedBetMode === "skip" ? 8 : 0)
  );
  return {
    topHeadLane,
    headProb1,
    lane2Second,
    lane2Sashi,
    lane3Attack,
    lane4Tenkai,
    displayGap: round(displayGap, 3),
    lapFootGap: round(Math.max(0, (average(lapScores.slice(0, 2)) ?? 0) - (average(lapScores.slice(-2)) ?? 0)), 1),
    motorContribution: round(Math.max(0, (average(motorScores.slice(0, 2)) ?? 0) - (average(motorScores) ?? 0) + 50), 1),
    entryRisk: round(entryRisk, 1),
    chaos,
    outsideRisk,
    hardRaceIndex
  };
}

function classifyMetric(key, value) {
  const rule = INDICATOR_THRESHOLD_RULES[key];
  if (!rule || value === null) return { band: "unknown", comment: "未計算" };
  const numeric = Number(value);
  if (key === "entry_risk") {
    if (numeric >= rule.strong) return { band: "high_risk", comment: "進入変化を強く警戒" };
    if (numeric >= rule.caution) return { band: "watch", comment: "進入変化は中程度警戒" };
    return { band: "stable", comment: "進入変化リスクは軽め" };
  }
  if (key === "display_st_gap") {
    if (numeric >= rule.strong) return { band: "wide", comment: "展示差がはっきり" };
    if (numeric >= rule.caution) return { band: "watch", comment: "展示差あり" };
    return { band: "tight", comment: "展示差は小さい" };
  }
  if (numeric >= rule.strong) return { band: "strong", comment: "強め評価" };
  if (numeric >= rule.caution) return { band: "watch", comment: "比較対象として要確認" };
  return { band: "weak", comment: "押し材料は弱め" };
}

function buildIndicatorThresholdSummary(signals) {
  const mapping = [
    ["boat1_strength", signals.headProb1],
    ["sashi_alert", signals.lane2Sashi],
    ["makurizashi_alert", Math.max(signals.lane3Attack ?? 0, signals.lane4Tenkai ?? 0)],
    ["display_st_gap", signals.displayGap],
    ["lap_foot_gap", signals.lapFootGap],
    ["motor_contribution", signals.motorContribution],
    ["lane2_wall", signals.lane2Second],
    ["entry_risk", signals.entryRisk]
  ];
  return mapping.map(([key, rawValue]) => {
    const rule = INDICATOR_THRESHOLD_RULES[key];
    const value = rawValue ?? null;
    const classified = classifyMetric(key, value);
    return {
      key,
      label: rule?.label || key,
      value,
      unit: rule?.unit || "",
      guide: rule?.guide || "",
      band: classified.band,
      summary: classified.comment
    };
  });
}

function buildLaneEvaluationTable(racers = []) {
  return racers
    .map((racer) => {
      const lane = Number(racer?.lane);
      if (!Number.isInteger(lane)) return null;
      const weights = getLaneWeightRule(lane);
      const rule = getLaneRoleRule(lane);
      const components = {
        currentSeason: normalizeScore(racer?.playerCurrentSeasonStrength ?? racer?.current_season_strength ?? racer?.nationwideWinRate),
        recentForm: normalizeScore(racer?.playerRecent3MonthsStrength ?? racer?.recent_3_months_strength ?? racer?.exhibitionTime),
        localFit: normalizeScore(racer?.localWinRate ?? racer?.officialLocalWinRate),
        gradeFit: normalizeScore(racer?.nationwideWinRate ?? racer?.officialNationwideWinRate ?? racer?.motor2ren)
      };
      const activeWeights = Object.entries(weights).filter(([, weight]) => Number.isFinite(Number(weight)));
      const totalWeight = activeWeights.reduce((sum, [, weight]) => sum + Number(weight), 0) || 1;
      const weightedScore = activeWeights.reduce((sum, [key, weight]) => {
        const value = components[key];
        return sum + (value === null ? 0 : value * Number(weight));
      }, 0) / totalWeight;
      return {
        lane,
        name: racer?.name || `Lane-${lane}`,
        weightedScore: round(weightedScore, 1),
        weights,
        weightMeaning: LANE_WEIGHT_MEANINGS,
        componentScores: components,
        mainWinPaths: rule?.mainWinPaths || [],
        conditions: rule?.conditions || [],
        dangerSigns: rule?.dangerSigns || [],
        commonCombos: rule?.commonCombos || [],
        sourceNote: "1か月は直近代理値として recent/展示気配を使用"
      };
    })
    .filter(Boolean)
    .sort((a, b) => Number(b?.weightedScore || 0) - Number(a?.weightedScore || 0));
}

function scorePattern(rule, signals, laneEvaluationMap) {
  const headBonus = rule.head.includes(signals.topHeadLane) ? 24 : 0;
  const lane2Score = signals.lane2Sashi ?? 0;
  const lane3Score = signals.lane3Attack ?? 0;
  const lane4Score = signals.lane4Tenkai ?? 0;
  const head1 = signals.headProb1 ?? 0;
  const chaos = signals.chaos ?? 0;
  const outsideRisk = signals.outsideRisk ?? 0;
  let score = 24;
  if (rule.code === "nige") score += head1 * 0.58 + (signals.lane2Second ?? 0) * 0.18 - chaos * 0.12;
  if (rule.code === "two_sashi") score += lane2Score * 0.55 + Math.max(0, 55 - head1) * 0.35 + (laneEvaluationMap.get(2)?.weightedScore || 0) * 0.18;
  if (rule.code === "three_makuri") score += lane3Score * 0.68 + (laneEvaluationMap.get(3)?.weightedScore || 0) * 0.2 + chaos * 0.12;
  if (rule.code === "three_makuri_sashi") score += lane3Score * 0.52 + lane4Score * 0.18 + Math.max(0, 52 - head1) * 0.3;
  if (rule.code === "four_makuri") score += lane4Score * 0.7 + chaos * 0.14 + (laneEvaluationMap.get(4)?.weightedScore || 0) * 0.18;
  if (rule.code === "four_makuri_sashi") score += lane4Score * 0.56 + lane3Score * 0.16 + Math.max(0, outsideRisk - 45) * 0.22;
  if (rule.code === "five_makuri_sashi") score += (laneEvaluationMap.get(5)?.weightedScore || 0) * 0.44 + chaos * 0.18 + Math.max(0, outsideRisk - 42) * 0.24;
  if (rule.code === "five_makuri") score += (laneEvaluationMap.get(5)?.weightedScore || 0) * 0.48 + chaos * 0.24 + Math.max(0, outsideRisk - 40) * 0.26;
  if (rule.code === "lane6_head") score += (laneEvaluationMap.get(6)?.weightedScore || 0) * 0.5 + chaos * 0.34 + Math.max(0, outsideRisk - 38) * 0.24;
  score += headBonus;
  return clamp(0, 100, score);
}

function buildDecisionPatternScores(signals, laneEvaluationTable = []) {
  const laneEvaluationMap = new Map(laneEvaluationTable.map((row) => [Number(row.lane), row]));
  return DECISION_PATTERN_RULES
    .map((rule) => {
      const score = scorePattern(rule, signals, laneEvaluationMap);
      const emphasis =
        rule.code === "nige"
          ? `1頭 ${round(signals.headProb1, 1)}% / 2残り ${round(signals.lane2Second, 1)}`
          : rule.code === "two_sashi"
            ? `2差し指標 ${round(signals.lane2Sashi, 1)} / 1弱化 ${round(100 - (signals.headProb1 ?? 0), 1)}`
            : `攻め指標 ${round(Math.max(signals.lane3Attack ?? 0, signals.lane4Tenkai ?? 0), 1)} / chaos ${round(signals.chaos, 1)}`;
      return {
        ...rule,
        score: round(score, 1),
        emphasis,
        priority: score >= 70 ? "high" : score >= 55 ? "medium" : "watch"
      };
    })
    .sort((a, b) => Number(b?.score || 0) - Number(a?.score || 0));
}

function buildComparisonChecklist(venueRuleProfile, indicatorThresholdSummary = [], decisionPatternScores = []) {
  const thresholdMap = new Map(indicatorThresholdSummary.map((row) => [row.key, row]));
  const topPattern = decisionPatternScores[0] || null;
  return (venueRuleProfile?.comparisonOrder || []).map((key, index) => {
    const item = (venueRuleProfile?.compareFocus || []).find((row) => row.key === key) || { key, label: key };
    const threshold = thresholdMap.get(key === "boat1_strength" ? "boat1_strength" : key === "display_gap" ? "display_st_gap" : key === "lane2_wall" ? "lane2_wall" : key === "entry_risk" ? "entry_risk" : "makurizashi_alert");
    return {
      step: index + 1,
      key,
      label: item.label,
      benchmark: item.benchmark || null,
      reason: item.reason || null,
      currentBand: threshold?.band || "unknown",
      currentSummary: threshold?.summary || (topPattern ? `${topPattern.label} 優勢` : "未計算")
    };
  });
}

function buildPredictedDevelopmentNotes({ venueRuleProfile = null, indicatorThresholdSummary = [], laneEvaluationTable = [], decisionPatternScores = [], supportSummary = null } = {}) {
  const notes = [];
  const topPattern = decisionPatternScores[0];
  const strongestLane = laneEvaluationTable[0];
  const boat1Metric = indicatorThresholdSummary.find((row) => row.key === "boat1_strength");
  if (venueRuleProfile?.baseStance) notes.push(`${venueRuleProfile.venueName || "この場"}は「${venueRuleProfile.baseStance}」の前提で見る`);
  if (boat1Metric) notes.push(`1号艇評価は ${boat1Metric.band}。${boat1Metric.summary}`);
  if (strongestLane) notes.push(`艇番評価では ${strongestLane.lane}号艇が最上位で、勝ち筋は ${strongestLane.mainWinPaths.join(" / ") || "要比較"}`);
  if (topPattern) notes.push(`展開候補は ${topPattern.label} が最上位。${topPattern.practicalMemo}`);
  if (supportSummary?.recommendedBetMode) notes.push(`既存 buy mode 判定は ${supportSummary.recommendedBetMode} を維持し、その前提で説明を補強`);
  return notes.filter(Boolean).slice(0, 5);
}

function buildHardRaceSixRanking({ hardRace1234 = null, decisionPatternScores = [], laneEvaluationTable = [], pureTop6Prediction = null } = {}) {
  const comboMatrix = hardRace1234?.fixed1234_matrix && typeof hardRace1234.fixed1234_matrix === "object"
    ? hardRace1234.fixed1234_matrix
    : {};
  const top6Map = new Map(
    (Array.isArray(pureTop6Prediction?.top6) ? pureTop6Prediction.top6 : [])
      .map((row) => [String(row?.combo || ""), toNum(row?.probability, null)])
      .filter(([combo, probability]) => combo && probability !== null)
  );
  const topPattern = decisionPatternScores[0];
  const laneMap = new Map(laneEvaluationTable.map((row) => [Number(row.lane), row]));
  return FIXED_HARD_RACE_COMBOS.map((combo) => {
    const [head, second, third] = combo.split("-").map((value) => Number(value));
    const probability = toNum(comboMatrix[combo], null) ?? top6Map.get(combo) ?? 0;
    const headScore = laneMap.get(head)?.weightedScore ?? 45;
    const partnerScore = average([laneMap.get(second)?.weightedScore, laneMap.get(third)?.weightedScore]) ?? 45;
    const patternFit =
      topPattern && topPattern.head.includes(head)
        ? 14
        : topPattern && topPattern.partners.includes(second)
          ? 8
          : 0;
    const score = clamp(0, 100, probability * 100 * 0.62 + headScore * 0.22 + partnerScore * 0.12 + patternFit);
    return {
      combo,
      score: round(score, 1),
      probability: round(probability * 100, 1),
      reasons: [
        `${head}号艇頭の評価 ${round(headScore, 1)}`,
        `${second}-${third} の連動評価 ${round(partnerScore, 1)}`,
        topPattern ? `上位展開候補 ${topPattern.label}` : null
      ].filter(Boolean)
    };
  })
    .sort((a, b) => Number(b?.score || 0) - Number(a?.score || 0))
    .map((row, index) => ({ rank: index + 1, ...row }));
}

function buildEntryAnalysis({ entryMeta = null, venueRuleProfile = null } = {}) {
  const predictedEntryOrder = normalizeEntryOrder(entryMeta?.predicted_entry_order);
  const actualEntryOrder = normalizeEntryOrder(entryMeta?.actual_entry_order);
  const entryConfirmed = entryMeta?.validation?.validation_ok === true && entryMeta?.fallback_used !== true;
  const effectiveEntryOrder = entryConfirmed && actualEntryOrder.length === 6 ? actualEntryOrder : predictedEntryOrder;
  const changedLanes = predictedEntryOrder
    .map((lane, index) => {
      const actualIndex = actualEntryOrder.indexOf(lane);
      const actualEntry = actualIndex >= 0 ? actualIndex + 1 : null;
      return {
        lane,
        predicted_entry: index + 1,
        actual_entry: actualEntry,
        changed: Number.isInteger(actualEntry) && actualEntry !== index + 1
      };
    })
    .filter((row) => row.changed);

  return {
    predicted_entry_order: predictedEntryOrder,
    actual_entry_order: actualEntryOrder,
    entry: effectiveEntryOrder,
    predicted_entry: predictedEntryOrder,
    actual_entry: entryConfirmed ? actualEntryOrder : [],
    entry_confirmed: entryConfirmed,
    entry_changed: !!entryMeta?.entry_changed,
    entry_change_type: entryMeta?.entry_change_type || "none",
    changed_lanes: changedLanes,
    lane_map: entryMeta?.actual_lane_map || {},
    venue_entry_weight:
      Number(venueRuleProfile?.venue_bias_profile?.venue_entry_change_bias || 0) >= 8
        ? "strong"
        : Number(venueRuleProfile?.venue_bias_profile?.venue_entry_change_bias || 0) >= 4
          ? "medium"
          : "normal",
    summary:
      entryConfirmed
        ? changedLanes.length > 0
          ? `actual entry ${actualEntryOrder.join("-")} is confirmed and changed from preview`
          : `actual entry ${actualEntryOrder.join("-")} is confirmed`
        : predictedEntryOrder.length > 0
          ? `entry is not confirmed, so predicted order ${predictedEntryOrder.join("-")} remains active`
          : "entry information is incomplete"
  };
}

function buildVenueTendencySummary({ race = null, venueRuleProfile = null } = {}) {
  const venueBias = venueRuleProfile?.venue_bias_profile || {};
  return {
    venue: race?.venueName || venueRuleProfile?.venueName || null,
    inside_bias: Number(venueBias?.venue_escape_bias || 0) >= 6 ? "strong_inside" : Number(venueBias?.venue_escape_bias || 0) <= -2 ? "attack_watch" : "inside_base",
    sashi_bias: Number(venueBias?.venue_sashi_bias || 0) >= 5 ? "strong" : Number(venueBias?.venue_sashi_bias || 0) >= 2 ? "usable" : "normal",
    center_attack_bias:
      Number(venueBias?.lane3_attack_boost || 0) + Number(venueBias?.lane4_develop_boost || 0) >= 16
        ? "strong"
        : Number(venueBias?.lane3_attack_boost || 0) + Number(venueBias?.lane4_develop_boost || 0) >= 9
          ? "usable"
          : "modest",
    entry_change_sensitivity:
      Number(venueBias?.venue_entry_change_bias || 0) >= 8
        ? "high"
        : Number(venueBias?.venue_entry_change_bias || 0) >= 4
          ? "medium"
          : "low",
    second_turn_residual:
      Number(venueBias?.lane2_second_boost || 0) + Number(venueBias?.lane3_second_boost || 0) >= 10
        ? "inner_residual_alive"
        : "standard",
    outer_reach:
      Number(venueBias?.venue_outer_3rd_bias || 0) >= 4
        ? "outer_can_reach"
        : Number(venueBias?.lane56_head_penalty || 0) >= 8
          ? "outer_suppressed"
          : "outer_neutral"
  };
}

function buildRacerStyleSummary(racers = []) {
  const leaders = racers
    .map((racer) => {
      const lane = Number(racer?.lane);
      if (!Number.isInteger(lane)) return null;
      return {
        lane,
        name: racer?.name || `Lane-${lane}`,
        style: racer?.style || "unknown",
        style_score: toNum(racer?.style_score, null),
        start_type:
          Number(racer?.exhibitionSt ?? racer?.avgSt) <= 0.13
            ? "踏み込む型"
            : Number(racer?.exhibitionSt ?? racer?.avgSt) >= 0.17
              ? "慎重型"
              : "標準型"
      };
    })
    .filter(Boolean)
    .sort((a, b) => Number(b.style_score || 0) - Number(a.style_score || 0));

  return {
    leaders,
    summary: leaders.slice(0, 3).map((row) => `${row.lane} ${row.name}: ${row.start_type} / ${row.style}`)
  };
}

function scoreHardRaceComboWithDevelopment(combo, { hardRace1234 = null, laneEvaluationMap = new Map(), topPattern = null, entryAnalysis = null, racerStyleMap = new Map(), venueRuleProfile = null } = {}) {
  const [head, second, third] = String(combo || "").split("-").map((value) => Number(value));
  const probability = toNum(hardRace1234?.fixed1234_matrix?.[combo], 0);
  const effectiveEntry = normalizeEntryOrder(entryAnalysis?.entry);
  const secondFit = effectiveEntry[1] === second ? 8 : 0;
  const thirdFit = effectiveEntry[2] === third ? 5 : 0;
  const entryShiftFit =
    entryAnalysis?.entry_change_type === "lane1_lost_inside" && second >= 3
      ? 5
      : entryAnalysis?.entry_changed && second >= 3
        ? 3
        : 0;
  const styleFit =
    ((racerStyleMap.get(second) === "sashi" && second === 2) ? 6 : 0) +
    ((racerStyleMap.get(second) === "makuri" && second === 3) ? 6 : 0) +
    ((racerStyleMap.get(second) === "makuri_sashi" && second === 4) ? 7 : 0);
  const venueFit =
    ((Number(venueRuleProfile?.venue_bias_profile?.lane2_second_boost || 0) >= 5 && second === 2) ? 4 : 0) +
    ((Number(venueRuleProfile?.venue_bias_profile?.lane3_attack_boost || 0) >= 7 && second === 3) ? 6 : 0) +
    ((Number(venueRuleProfile?.venue_bias_profile?.lane4_develop_boost || 0) >= 7 && second === 4) ? 7 : 0);
  const patternFit =
    topPattern && safeArray(topPattern.head).includes(head)
      ? 12
      : topPattern && safeArray(topPattern.partners).includes(second)
        ? 7
        : 0;
  const headScore = laneEvaluationMap.get(head)?.weightedScore ?? 45;
  const partnerScore = average([laneEvaluationMap.get(second)?.weightedScore, laneEvaluationMap.get(third)?.weightedScore]) ?? 45;
  return clamp(0, 100, probability * 100 * 0.52 + headScore * 0.18 + partnerScore * 0.14 + secondFit + thirdFit + entryShiftFit + styleFit + venueFit + patternFit);
}

function buildLikelyWinningPattern({ decisionPatternScores = [], hardRaceSixRanking = [], pureTop6Prediction = null, entryAnalysis = null, laneEvaluationTable = [] } = {}) {
  const main = decisionPatternScores[0] || null;
  const rival = decisionPatternScores[1] || null;
  const upset = decisionPatternScores[2] || null;
  const secondGivenHead = pureTop6Prediction?.second_given_head_probabilities || {};
  const secondCandidates = [2, 3, 4, 5, 6]
    .map((lane) => ({ lane, value: Number(secondGivenHead?.[lane] || 0) }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 3)
    .map((row) => row.lane);
  const thirdCandidates = hardRaceSixRanking
    .slice(0, 4)
    .flatMap((row) => String(row?.combo || "").split("-").slice(2))
    .map((lane) => Number(lane))
    .filter((lane, index, arr) => Number.isInteger(lane) && arr.indexOf(lane) === index);
  return {
    main_development: main?.label || null,
    rival_development: rival?.label || null,
    upset_development: upset?.label || null,
    first_choice: Number(String(hardRaceSixRanking[0]?.combo || "1-2-3").split("-")[0]) || 1,
    second_candidates: secondCandidates,
    third_candidates: thirdCandidates,
    kimarite:
      main?.code === "nige" ? "逃げ" :
      main?.code === "two_sashi" ? "差し" :
      main?.code === "three_makuri" || main?.code === "four_makuri" ? "まくり" :
      main?.code === "three_makuri_sashi" || main?.code === "four_makuri_sashi" ? "まくり差し" :
      "展開待ち",
    reason: [
      main ? `${main.label} scored ${main.score}` : null,
      entryAnalysis?.summary || null,
      laneEvaluationTable[0] ? `lane ${laneEvaluationTable[0].lane} owns the strongest composite lane score` : null
    ].filter(Boolean),
    buy_rank: hardRaceSixRanking
  };
}

export function buildRaceRuleInsights({
  race = null,
  racers = [],
  pureTop6Prediction = null,
  prediction = null,
  hardRace1234 = null,
  supportSummary = null,
  entryMeta = null
} = {}) {
  const venueRule = getVenueComparisonRule(race?.venueId);
  const venueRuleProfile = {
    ...venueRule,
    venueName: pickVenueName({ race, venueRule }),
    focusSummary: (venueRule.emphasis || []).map((row) => `${row.label}: ${row.reason}`)
  };
  const signals = getRaceSignals({ pureTop6Prediction, hardRace1234, racers, prediction, supportSummary });
  const indicatorThresholdSummary = buildIndicatorThresholdSummary(signals);
  const laneEvaluationTable = buildLaneEvaluationTable(racers);
  const decisionPatternScores = buildDecisionPatternScores(signals, laneEvaluationTable);
  const comparisonChecklist = buildComparisonChecklist(venueRuleProfile, indicatorThresholdSummary, decisionPatternScores);
  const predictedDevelopmentNotes = buildPredictedDevelopmentNotes({
    venueRuleProfile,
    indicatorThresholdSummary,
    laneEvaluationTable,
    decisionPatternScores,
    supportSummary
  });
  const practicalMemo = predictedDevelopmentNotes.join(" / ");
  const entryAnalysis = buildEntryAnalysis({ entryMeta, venueRuleProfile });
  const venueTendencySummary = buildVenueTendencySummary({ race, venueRuleProfile });
  const racerStyleSummary = buildRacerStyleSummary(racers);
  const laneEvaluationMap = new Map(laneEvaluationTable.map((row) => [Number(row.lane), row]));
  const racerStyleMap = new Map(racerStyleSummary.leaders.map((row) => [Number(row.lane), row.style]));
  const topPattern = decisionPatternScores[0] || null;
  const hardRaceSixRanking = FIXED_HARD_RACE_COMBOS
    .map((combo) => ({
      combo,
      score: round(scoreHardRaceComboWithDevelopment(combo, {
        hardRace1234,
        laneEvaluationMap,
        topPattern,
        entryAnalysis,
        racerStyleMap,
        venueRuleProfile
      }), 1),
      probability: round(toNum(hardRace1234?.fixed1234_matrix?.[combo], 0) * 100, 1)
    }))
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .map((row, index) => ({
      rank: index + 1,
      ...row,
      reasons: [
        entryAnalysis?.entry_confirmed ? `actual entry ${entryAnalysis.actual_entry_order.join("-")}` : `predicted entry ${entryAnalysis.predicted_entry_order.join("-")}`,
        `venue ${venueTendencySummary.center_attack_bias}`,
        racerStyleMap.get(Number(String(row.combo).split("-")[1])) || null
      ].filter(Boolean)
    }));
  const likelyWinningPattern = buildLikelyWinningPattern({
    decisionPatternScores,
    hardRaceSixRanking,
    pureTop6Prediction,
    entryAnalysis,
    laneEvaluationTable
  });
  return {
    venueRuleProfile,
    indicatorThresholdSummary,
    laneEvaluationTable,
    decisionPatternScores,
    comparisonChecklist,
    entryAnalysis,
    venueTendencySummary,
    racerStyleSummary,
    predictedDevelopmentNotes,
    practicalMemo,
    hardRaceSixRanking,
    likelyWinningPattern
  };
}
