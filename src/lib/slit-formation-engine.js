const BOATS = [1, 2, 3, 4, 5, 6];

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function finiteNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

function round(value, digits = 3) {
  const n = finiteNumber(value, 0);
  const scale = 10 ** digits;
  return Math.round(n * scale) / scale;
}

function roundScore(value) {
  return round(clamp(value) * 100, 1);
}

function boatNumber(row = {}) {
  return finiteNumber(row.boat ?? row.boatNumber ?? row.lane ?? row.entry ?? row.racer_boat_number, null);
}

function optionalRate01(value) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  return clamp(Math.abs(n) > 1 ? n / 100 : n, 0, 1);
}

function startTimingScore(value, fallback = null) {
  const st = finiteNumber(value, null);
  if (st === null) return fallback;
  return clamp((0.28 - st) / 0.22, 0, 1);
}

function featureScore(row = {}, featureScores = {}, field, fallback = null) {
  const boat = boatNumber(row);
  const fromFeature = featureScores?.byBoat?.[String(boat)]?.scores?.[field];
  if (fromFeature !== null && fromFeature !== undefined) return clamp(Number(fromFeature), 0, 1);
  const fromRow = row?.featureScores?.scores?.[field];
  if (fromRow !== null && fromRow !== undefined) return clamp(Number(fromRow), 0, 1);
  return fallback;
}

function mergeTendency(row = {}) {
  return {
    ...(row.techniqueStats || {}),
    ...(row.racerCourseStats || {}),
    ...(row.playerTendency || {}),
    ...Object.fromEntries(
      [
        "avgST",
        "avgStartTiming",
        "currentSeasonAvgST",
        "currentSeasonStartCount",
        "currentSeasonLateStartRate",
        "currentSeasonEarlyStartRate",
        "currentSeasonStartStabilityRate",
        "lateStartRate",
        "earlyStartRate",
        "startStabilityRate",
        "courseWinRate",
        "courseQuinellaRate",
        "courseTrifectaRate",
        "sashiRate",
        "makuriRate",
        "makuriSashiRate",
        "sampleStatus"
      ]
        .filter((field) => row[field] !== null && row[field] !== undefined)
        .map((field) => [field, row[field]])
    )
  };
}

function sampleWeight(tendency = {}) {
  const status = String(tendency.sampleStatus || "").trim();
  if (status === "ok") return 1;
  if (status === "small_sample") return 0.45;
  if (status === "very_small_sample") return 0.16;
  if (status === "insufficient_history") return 0;
  const count = finiteNumber(tendency.courseSpecificLast6mRaceCount ?? tendency.last6mRaceCount, null);
  if (count === null) return 0.35;
  if (count >= 10) return 1;
  if (count >= 3) return 0.45;
  if (count >= 1) return 0.16;
  return 0;
}

function positiveRate(tendency = {}, field, threshold = 0.1) {
  const rate = optionalRate01(tendency[field]);
  if (rate === null) return 0;
  return Math.max(0, rate - threshold) * sampleWeight(tendency);
}

function normalizePercentile(value) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  return clamp(Math.abs(n) > 1 ? n / 100 : n, 0, 1);
}

function rowScore(entry = {}, featureScores = {}) {
  const tendency = mergeTendency(entry);
  const boat = boatNumber(entry);
  const course = finiteNumber(entry.course ?? entry.entry ?? entry.lane ?? boat, boat);
  const exST = finiteNumber(entry.exST ?? entry.exhibitionST ?? entry.exhibitionSt ?? entry.racer_start_timing, null);
  const avgST = finiteNumber(tendency.avgStartTiming ?? tendency.avgST ?? entry.averageStartTiming ?? entry.racer_average_start_timing, null);
  const currentSeasonAvgST = finiteNumber(tendency.currentSeasonAvgST ?? entry.currentSeasonAvgST, null);
  const preferredStart = currentSeasonAvgST ?? avgST ?? exST;
  const exSTScore = featureScore(entry, featureScores, "exST", startTimingScore(exST, null));
  const avgSTScore = startTimingScore(avgST, null);
  const currentSeasonSTScore = startTimingScore(currentSeasonAvgST, null);
  const preferredStartScore = startTimingScore(preferredStart, null);
  const fallbackStartScore = preferredStartScore ?? exSTScore ?? 0.5;
  const historyStartScore = currentSeasonSTScore ?? avgSTScore;
  const startHistoryAvailable = historyStartScore !== null;
  const lateRate = optionalRate01(tendency.currentSeasonLateStartRate ?? tendency.lateStartRate) ?? null;
  const earlyRate = optionalRate01(tendency.currentSeasonEarlyStartRate ?? tendency.earlyStartRate) ?? null;
  const stabilityRate = optionalRate01(tendency.currentSeasonStartStabilityRate ?? tendency.startStabilityRate) ?? null;
  const fCount = Math.max(0, finiteNumber(entry.flyingCount ?? entry.fCount ?? entry.F ?? tendency.fCount, 0));
  const lCount = Math.max(0, finiteNumber(entry.lateCount ?? entry.lCount ?? entry.L ?? tendency.lCount, 0));
  const statusText = String(entry.fStatus ?? entry.F ?? entry.lStatus ?? entry.L ?? "").toUpperCase();
  const hasFRisk = fCount > 0 || /\bF|Ｆ/.test(statusText);
  const hasLRisk = lCount > 0 || /\bL|Ｌ/.test(statusText);
  const motor = featureScore(
    entry,
    featureScores,
    "motorRank",
    normalizePercentile(entry.motorPercentileAtVenue) ?? normalizePercentile(entry.motor2Rate) ?? 0.5
  );
  const straight = featureScore(entry, featureScores, "straightTime", 0.5);
  const turn = featureScore(entry, featureScores, "turnTime", 0.5);
  const lap = featureScore(entry, featureScores, "lapTime", 0.5);
  const courseQ = optionalRate01(tendency.courseQuinellaRate);
  const courseT = optionalRate01(tendency.courseTrifectaRate);
  const startHistoryScore = historyStartScore ?? 0.5;
  const exhibitionGoodHistoryPoor = exSTScore !== null && exSTScore >= 0.72 &&
    (startHistoryAvailable && startHistoryScore <= 0.43 || (lateRate ?? 0) >= 0.18);
  const stability = stabilityRate ?? clamp(
    0.48 +
    (startHistoryScore - 0.5) * 0.32 +
    ((courseT ?? 0.5) - 0.5) * 0.2 -
    Math.max(0, (lateRate ?? 0.08) - 0.12) * 0.8 -
    (hasFRisk ? 0.08 : 0) -
    (hasLRisk ? 0.08 : 0)
  );

  const startReliabilityScore = clamp(
    0.18 +
    (historyStartScore ?? 0.5) * 0.32 +
    fallbackStartScore * 0.16 +
    stability * 0.2 +
    (courseT ?? 0.5) * 0.08 +
    motor * 0.06 -
    Math.max(0, (lateRate ?? 0.08) - 0.12) * 0.55 -
    (hasFRisk ? 0.07 : 0) -
    (hasLRisk ? 0.08 : 0) -
    (exhibitionGoodHistoryPoor ? 0.08 : 0)
  );
  const lateRiskScore = clamp(
    0.12 +
    Math.max(0, 0.48 - fallbackStartScore) * 0.75 +
    (lateRate ?? 0.08) * 1.25 +
    (hasLRisk ? 0.14 : 0) -
    stability * 0.18
  );
  const earlyRiskScore = clamp(
    0.08 +
    Math.max(0, fallbackStartScore - 0.62) * 0.62 +
    (earlyRate ?? 0.08) * 0.82 +
    (hasFRisk ? 0.12 : 0) -
    stability * 0.08
  );
  const startPressureScore = clamp(
    0.12 +
    fallbackStartScore * 0.3 +
    startReliabilityScore * 0.18 +
    straight * 0.14 +
    motor * 0.1 +
    Math.max(0, (earlyRate ?? 0.08) - 0.12) * 0.38 -
    lateRiskScore * 0.2 -
    (exhibitionGoodHistoryPoor ? 0.06 : 0)
  );
  const wallFormationScore = clamp(
    0.12 +
    startReliabilityScore * 0.26 +
    fallbackStartScore * 0.16 +
    stability * 0.16 +
    turn * 0.1 +
    motor * 0.1 +
    (courseQ ?? 0.5) * 0.06 +
    (courseT ?? 0.5) * 0.08 -
    lateRiskScore * 0.2 -
    (hasFRisk ? 0.04 : 0)
  );
  const attackTechnique = course === 2
    ? positiveRate(tendency, "sashiRate", 0.08)
    : positiveRate(tendency, "makuriRate", 0.08) + positiveRate(tendency, "makuriSashiRate", 0.07);
  const attackStartScore = clamp(
    0.08 +
    startPressureScore * 0.26 +
    fallbackStartScore * 0.14 +
    straight * 0.18 +
    motor * 0.1 +
    attackTechnique * 0.95 +
    (course >= 3 ? Math.max(0, (earlyRate ?? 0.08) - 0.1) * 0.32 : 0) -
    (hasFRisk && earlyRiskScore >= 0.45 ? 0.08 : 0) -
    Math.max(0, 0.46 - turn) * 0.1
  );
  const insideProtectionScore = clamp(
    course <= 2
      ? 0.12 + startReliabilityScore * 0.28 + wallFormationScore * 0.26 + turn * 0.14 + lap * 0.12 + motor * 0.1 - lateRiskScore * 0.18
      : 0.1 + wallFormationScore * 0.16
  );
  const outsidePressureScore = clamp(
    course >= 3
      ? 0.08 + startPressureScore * 0.28 + attackStartScore * 0.28 + straight * 0.14 + motor * 0.08 - lateRiskScore * 0.12
      : 0.08 + startPressureScore * 0.12
  );
  const slitScore = clamp(
    0.1 +
    fallbackStartScore * 0.28 +
    startReliabilityScore * 0.2 +
    startPressureScore * 0.18 +
    motor * 0.08 +
    straight * 0.08 -
    lateRiskScore * 0.18 -
    (exhibitionGoodHistoryPoor ? 0.08 : 0)
  );
  const flowWideRisk = clamp(
    Math.max(0, attackStartScore - 0.58) * 0.42 +
    Math.max(0, 0.56 - turn) * 0.34 +
    Math.max(0, 0.56 - lap) * 0.2 +
    (hasFRisk && earlyRiskScore >= 0.45 ? 0.08 : 0)
  );

  const notes = [
    exhibitionGoodHistoryPoor ? `${boat}号艇は展示STだけ速い可能性があり、平均ST/出遅れ率から過信を抑制。` : null,
    lateRiskScore >= 0.55 ? `${boat}号艇は出遅れリスクが高く、壁形成を割り引き。` : null,
    hasFRisk && earlyRiskScore >= 0.45 ? `${boat}号艇は早仕掛け傾向とFリスクがあり、攻め評価は条件付き。` : null,
    startReliabilityScore >= 0.66 && attackStartScore >= 0.62 ? `${boat}号艇は平均STと展示STがそろい、スリットから攻めの起点になりやすい。` : null
  ].filter(Boolean);

  return {
    boat,
    course,
    exST,
    avgST,
    currentSeasonAvgST,
    currentSeasonStartCount: finiteNumber(tendency.currentSeasonStartCount ?? entry.currentSeasonStartCount, null),
    lateStartRate: lateRate,
    earlyStartRate: earlyRate,
    startStabilityRate: stabilityRate,
    hasFRisk,
    hasLRisk,
    exhibitionGoodHistoryPoor,
    slitScore,
    startReliabilityScore,
    startPressureScore,
    lateRiskScore,
    earlyRiskScore,
    wallFormationScore,
    attackStartScore,
    insideProtectionScore,
    outsidePressureScore,
    flowWideRisk,
    motorScore: motor,
    straightScore: straight,
    turnScore: turn,
    lapScore: lap,
    courseQuinellaRate: courseQ,
    courseTrifectaRate: courseT,
    confidence: clamp(
      0.18 +
      (exSTScore !== null ? 0.14 : 0) +
      (avgSTScore !== null ? 0.16 : 0) +
      (currentSeasonSTScore !== null ? 0.18 : 0) +
      (lateRate !== null ? 0.11 : 0) +
      (earlyRate !== null ? 0.08 : 0) +
      (stabilityRate !== null ? 0.1 : 0) +
      (courseT !== null ? 0.08 : 0) +
      (featureScores?.byBoat?.[String(boat)] ? 0.07 : 0)
    ),
    notes,
    debug: {
      fallbackStartScore,
      avgSTScore,
      currentSeasonSTScore,
      exSTScore,
      startHistoryAvailable,
      sampleWeight: sampleWeight(tendency)
    }
  };
}

function classifyPattern(rows = []) {
  const byBoat = Object.fromEntries(rows.map((row) => [row.boat, row]));
  const b1 = byBoat[1] || {};
  const b2 = byBoat[2] || {};
  const b3 = byBoat[3] || {};
  const b4 = byBoat[4] || {};
  const b5 = byBoat[5] || {};
  const b6 = byBoat[6] || {};
  const highFRisk = rows.some((row) => row.hasFRisk && row.earlyRiskScore >= 0.45);
  const lowConfidence = rows.filter((row) => row.confidence >= 0.5).length < 3;
  if (highFRisk) return "high_f_risk";
  if ((b2.lateRiskScore ?? 0) >= 0.5 || (b2.wallFormationScore ?? 0) < 0.42) return "boat2_late";
  if ((b3.outsidePressureScore ?? 0) >= 0.64 && (b4.outsidePressureScore ?? 0) >= 0.6) return "center_pressure";
  if ((b3.outsidePressureScore ?? 0) >= 0.56 || (b3.attackStartScore ?? 0) >= 0.72) return "boat3_pressure";
  if ((b4.outsidePressureScore ?? 0) >= 0.56 || (b4.attackStartScore ?? 0) >= 0.72) return "boat4_pressure";
  if (Math.max(b5.outsidePressureScore ?? 0, b6.outsidePressureScore ?? 0) >= 0.62) return "outside_pressure";
  if ((b1.startReliabilityScore ?? 0) >= 0.6 && (b2.wallFormationScore ?? 0) >= 0.58 && Math.max(b3.outsidePressureScore ?? 0, b4.outsidePressureScore ?? 0) < 0.58) return "inside_stable";
  if (Math.max(...rows.map((row) => row.slitScore)) - Math.min(...rows.map((row) => row.slitScore)) >= 0.28) return "uneven_slit";
  if (lowConfidence) return "low_confidence";
  return "inside_stable";
}

export function buildSlitFormation(entries = [], { featureScores = null } = {}) {
  const byBoat = new Map(safeArray(entries).map((row) => [boatNumber(row), row]).filter(([boat]) => Number.isInteger(boat)));
  const rows = BOATS.map((boat) => rowScore(byBoat.get(boat) || { boat, course: boat }, featureScores || {}));
  const expectedOrder = [...rows]
    .sort((a, b) => b.slitScore - a.slitScore || a.boat - b.boat)
    .map((row) => row.boat);
  const pressureBoats = rows.filter((row) => row.startPressureScore >= 0.62 || row.outsidePressureScore >= 0.62).map((row) => row.boat);
  const lateRiskBoats = rows.filter((row) => row.lateRiskScore >= 0.5).map((row) => row.boat);
  const earlyRiskBoats = rows.filter((row) => row.earlyRiskScore >= 0.52).map((row) => row.boat);
  const wallBoats = rows.filter((row) => row.wallFormationScore >= 0.58).map((row) => row.boat);
  const brokenWallCandidates = rows.filter((row) => [2, 3, 4].includes(row.boat) && row.wallFormationScore < 0.44 || row.lateRiskScore >= 0.52).map((row) => row.boat);
  const attackTriggerCandidates = rows.filter((row) => [2, 3, 4].includes(row.boat) && row.attackStartScore >= 0.58).map((row) => row.boat);
  const slitPattern = classifyPattern(rows);
  const notes = [
    slitPattern === "inside_stable" ? "1・2号艇のスタート/壁が安定し、内残りを評価。" : null,
    slitPattern === "boat2_late" ? "2号艇の出遅れまたは壁弱化で、3/4の攻め筋が広がります。" : null,
    slitPattern === "boat3_pressure" ? "3号艇がスリットから攻めの起点になりやすい隊形です。" : null,
    slitPattern === "boat4_pressure" ? "4号艇のスタート圧があり、展開拾いだけでなく自力気味の動きも警戒。" : null,
    slitPattern === "center_pressure" ? "3・4号艇がそろって圧をかけるセンター優勢のスリット想定です。" : null,
    slitPattern === "outside_pressure" ? "外枠のスリット気配は良いですが、崩れがなければ相手/3着中心です。" : null,
    slitPattern === "high_f_risk" ? "早仕掛け傾向とFリスクがあり、スタート評価の信頼度を抑えます。" : null,
    slitPattern === "low_confidence" ? "平均ST/今期STの欠損が多く、スリット予測は低信頼です。" : null,
    ...rows.flatMap((row) => row.notes).slice(0, 4)
  ].filter(Boolean);
  const adjustmentLog = rows.map((row) => ({
    boat: row.boat,
    slitScore: roundScore(row.slitScore),
    startReliabilityScore: roundScore(row.startReliabilityScore),
    wallFormationScore: roundScore(row.wallFormationScore),
    attackStartScore: roundScore(row.attackStartScore),
    lateRiskScore: roundScore(row.lateRiskScore),
    earlyRiskScore: roundScore(row.earlyRiskScore),
    exhibitionGoodHistoryPoor: row.exhibitionGoodHistoryPoor,
    confidence: roundScore(row.confidence)
  }));
  return {
    rows,
    byBoat: Object.fromEntries(rows.map((row) => [String(row.boat), row])),
    expectedOrder,
    pressureBoats,
    lateRiskBoats,
    earlyRiskBoats,
    wallBoats,
    brokenWallCandidates,
    attackTriggerCandidates,
    slitPattern,
    notes,
    adjustmentLog,
    confidence: round(rows.reduce((sum, row) => sum + row.confidence, 0) / Math.max(1, rows.length))
  };
}

export function buildSlitFormationDebug(slitFormation = null) {
  const formation = slitFormation || buildSlitFormation([]);
  return {
    expectedSlitOrder: formation.expectedOrder,
    slitPattern: formation.slitPattern,
    pressureBoats: formation.pressureBoats,
    lateRiskBoats: formation.lateRiskBoats,
    earlyRiskBoats: formation.earlyRiskBoats,
    wallBoats: formation.wallBoats,
    brokenWallCandidates: formation.brokenWallCandidates,
    attackTriggerCandidates: formation.attackTriggerCandidates,
    confidence: formation.confidence,
    rows: safeArray(formation.rows).map((row) => ({
      boat: row.boat,
      exST: row.exST,
      avgST: row.avgST,
      currentSeasonAvgST: row.currentSeasonAvgST,
      lateStartRate: row.lateStartRate,
      earlyStartRate: row.earlyStartRate,
      startStabilityRate: row.startStabilityRate,
      startReliabilityScore: roundScore(row.startReliabilityScore),
      startPressureScore: roundScore(row.startPressureScore),
      wallFormationScore: roundScore(row.wallFormationScore),
      attackStartScore: roundScore(row.attackStartScore),
      lateRiskScore: roundScore(row.lateRiskScore),
      earlyRiskScore: roundScore(row.earlyRiskScore),
      insideProtectionScore: roundScore(row.insideProtectionScore),
      outsidePressureScore: roundScore(row.outsidePressureScore),
      flowWideRisk: roundScore(row.flowWideRisk),
      exhibitionGoodHistoryPoor: row.exhibitionGoodHistoryPoor,
      hasFRisk: row.hasFRisk,
      hasLRisk: row.hasLRisk,
      confidence: roundScore(row.confidence)
    })),
    adjustmentLog: formation.adjustmentLog,
    notes: formation.notes
  };
}
