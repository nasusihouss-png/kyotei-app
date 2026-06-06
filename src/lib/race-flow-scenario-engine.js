const BOATS = [1, 2, 3, 4, 5, 6];

const SCENARIO_LABELS = {
  escape_1: "イン逃げ成功",
  sashi_2: "2号艇差し",
  makuri_3: "3号艇まくり",
  makuri_sashi_3: "3号艇まくり差し",
  second_wave_4: "4号艇まくり差し",
  outside_follow_5_6: "5・6号艇展開突き"
};

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

function optionalRate01(value) {
  const n = finiteNumber(value, null);
  if (n === null) return null;
  return clamp(n > 1 ? n / 100 : n, 0, 1);
}

function positiveRate(tendency, field, threshold = 0.1) {
  const rate = optionalRate01(tendency?.[field]);
  if (rate === null) return 0;
  return Math.max(0, rate - threshold) * tendencySampleWeight(tendency);
}

function startTimingScore(value, fallback = 0.5) {
  const st = finiteNumber(value, null);
  if (st === null) return fallback;
  return clamp((0.28 - st) / 0.22, 0, 1);
}

function tendencySampleStatus(tendency = {}) {
  const explicit = String(tendency?.sampleStatus || "").trim();
  if (["ok", "small_sample", "very_small_sample", "insufficient_history"].includes(explicit)) {
    return explicit;
  }
  const count = finiteNumber(
    tendency?.courseSpecificLast6mRaceCount ?? tendency?.last6mRaceCount,
    null
  );
  if (count === null) return "unknown";
  if (count >= 10) return "ok";
  if (count >= 3) return "small_sample";
  if (count >= 1) return "very_small_sample";
  return "insufficient_history";
}

function tendencySampleWeight(tendency = {}) {
  return ({
    ok: 1,
    small_sample: 0.38,
    very_small_sample: 0.12,
    insufficient_history: 0,
    unknown: 0.18
  })[tendencySampleStatus(tendency)] ?? 0;
}

function tendencyHasSignal(tendency = {}) {
  if (!tendency || typeof tendency !== "object") return false;
  const fields = [
    "escapeRate",
    "beatenBySashiRate",
    "beatenByMakuriRate",
    "beatenByMakuriSashiRate",
    "sashiRate",
    "makuriRate",
    "makuriSashiRate",
    "allCourseWinRate",
    "allCourseSashiRate",
    "allCourseMakuriRate",
    "allCourseMakuriSashiRate"
  ];
  const count = finiteNumber(
    tendency.courseSpecificLast6mRaceCount ?? tendency.last6mRaceCount ?? tendency.allCourseLast6mRaceCount,
    0
  );
  return count > 0 || fields.some((field) => tendency[field] !== null && tendency[field] !== undefined);
}

function mergeTendency(row = {}) {
  const direct = {};
  for (const field of [
    "escapeRate",
    "beatenBySashiRate",
    "beatenByMakuriRate",
    "beatenByMakuriSashiRate",
    "sashiRate",
    "makuriRate",
    "makuriSashiRate",
    "avgST",
    "avgStartTiming",
    "lateStartRate",
    "earlyStartRate",
    "sampleStatus",
    "last6mRaceCount",
    "courseSpecificLast6mRaceCount",
    "allCourseLast6mRaceCount",
    "allCourseWinRate",
    "allCourseSashiRate",
    "allCourseMakuriRate",
    "allCourseMakuriSashiRate",
    "allCourseAvgST"
  ]) {
    if (row[field] !== null && row[field] !== undefined) direct[field] = row[field];
  }
  return {
    ...(row.techniqueStats || {}),
    ...(row.racerCourseStats || {}),
    ...(row.playerTendency || {}),
    ...direct
  };
}

function boatNumber(row = {}) {
  return finiteNumber(row.boat ?? row.boatNumber ?? row.lane ?? row.entry ?? row.racer_boat_number, null);
}

function buildBoatMap(entries = []) {
  const map = new Map();
  for (const row of safeArray(entries)) {
    const boat = boatNumber(row);
    if (Number.isInteger(boat) && boat >= 1 && boat <= 6) map.set(boat, row);
  }
  return map;
}

function featureScore(row = {}, featureScores = {}, field, fallback = 0.5) {
  const boat = boatNumber(row);
  const fromRow = row?.featureScores?.scores?.[field];
  if (fromRow !== null && fromRow !== undefined) return clamp(Number(fromRow), 0, 1);
  const fromAll = featureScores?.byBoat?.[String(boat)]?.scores?.[field];
  if (fromAll !== null && fromAll !== undefined) return clamp(Number(fromAll), 0, 1);
  return fallback;
}

function roleScore(row = {}, featureScores = {}, fallback = 0.5) {
  const boat = boatNumber(row);
  const fromRow = row?.featureScores?.roleScore;
  if (fromRow !== null && fromRow !== undefined) return clamp(Number(fromRow), 0, 1);
  const fromAll = featureScores?.byBoat?.[String(boat)]?.roleScore;
  if (fromAll !== null && fromAll !== undefined) return clamp(Number(fromAll), 0, 1);
  return fallback;
}

function normalizeVenueNode(node, course) {
  if (!node || typeof node !== "object") return null;
  if (Array.isArray(node)) {
    return node.find((row) => Number(row?.course ?? row?.lane ?? row?.boat) === course) || null;
  }
  return node[String(course)] || node[course] || null;
}

function venueCourseBias(venueBias, stadiumNumber, course) {
  if (!venueBias || typeof venueBias !== "object") return null;
  const candidates = [
    normalizeVenueNode(venueBias, course),
    normalizeVenueNode(venueBias.courses, course),
    normalizeVenueNode(venueBias.lanes, course),
    normalizeVenueNode(venueBias[String(stadiumNumber)], course),
    normalizeVenueNode(venueBias?.[String(stadiumNumber)]?.courses, course),
    normalizeVenueNode(venueBias?.[String(stadiumNumber)]?.lanes, course),
    venueBias[`${stadiumNumber}-${course}`],
    venueBias[`${stadiumNumber}:${course}`]
  ];
  return candidates.find((row) => row && typeof row === "object") || null;
}

function venueRate(venueBias, stadiumNumber, course, fields = []) {
  const row = venueCourseBias(venueBias, stadiumNumber, course);
  if (!row) return null;
  for (const field of fields) {
    const rate = optionalRate01(row[field]);
    if (rate !== null) return rate;
  }
  return null;
}

function normalizeRaceConditions(source = {}) {
  const root = source && typeof source === "object" ? source : {};
  const text = String(root.windDirection ?? root.wind_direction ?? root.windDir ?? "");
  const windSpeed = finiteNumber(root.windSpeed ?? root.wind ?? root.race_wind, null);
  const waveHeight = finiteNumber(root.waveHeight ?? root.wave ?? root.race_wave, null);
  const directionLower = text.toLowerCase();
  const isTailwind = /tail|追/.test(directionLower);
  const isHeadwind = /head|向/.test(directionLower);
  const isCrosswind = /cross|横/.test(directionLower);
  const windLevel = windSpeed === null ? 0 : windSpeed >= 7 ? 1 : windSpeed >= 5 ? 0.58 : 0;
  const waveLevel = waveHeight === null ? 0 : waveHeight >= 8 ? 1 : waveHeight >= 5 ? 0.58 : 0;
  return {
    windDirection: root.windDirection ?? root.wind_direction ?? root.windDir ?? null,
    windSpeed,
    waveHeight,
    weather: root.weather ?? null,
    windLevel,
    waveLevel,
    isTailwind,
    isHeadwind,
    isCrosswind,
    available: windSpeed !== null || waveHeight !== null || root.weather !== null || root.windDirection !== null
  };
}

function boatMetrics(row = {}, featureScores = {}) {
  const tendency = mergeTendency(row);
  const tendencyStart = tendency.avgStartTiming ?? tendency.avgST ?? tendency.allCourseAvgST;
  const lateRate = optionalRate01(tendency.lateStartRate);
  return {
    boat: boatNumber(row),
    course: finiteNumber(row.course ?? row.lane ?? row.boat, boatNumber(row)),
    exST: featureScore(row, featureScores, "exST", 0.5),
    exTime: featureScore(row, featureScores, "exTime", 0.5),
    lapTime: featureScore(row, featureScores, "lapTime", 0.5),
    straightTime: featureScore(row, featureScores, "straightTime", 0.5),
    turnTime: featureScore(row, featureScores, "turnTime", 0.5),
    motor2Rate: featureScore(row, featureScores, "motor2Rate", optionalRate01(row.motor2Rate) ?? 0.5),
    roleScore: roleScore(row, featureScores, 0.5),
    tendency,
    sampleStatus: tendencySampleStatus(tendency),
    sampleWeight: tendencySampleWeight(tendency),
    startHistoryScore: startTimingScore(tendencyStart, 0.5),
    lateRate: lateRate ?? 0.08,
    flyingPenalty: Math.max(0, finiteNumber(row.flyingCount ?? row.fCount ?? row.F, 0)) * 0.08,
    raw: row
  };
}

function wallScore(metric = {}) {
  return clamp(
    0.18 +
    metric.exST * 0.24 +
    metric.straightTime * 0.14 +
    metric.turnTime * 0.12 +
    metric.motor2Rate * 0.13 +
    metric.startHistoryScore * 0.12 +
    (1 - metric.lateRate) * 0.11 -
    metric.flyingPenalty
  );
}

function supportCount(checks = []) {
  return checks.filter(Boolean).length;
}

function scenarioRow({ id, attacker, score, triggerScore, beneficiaries = [], partners = [], reasons = [], patterns = [] }) {
  return {
    id,
    label: SCENARIO_LABELS[id] || id,
    attacker,
    score: roundScore(score),
    score01: round(clamp(score)),
    triggerScore: roundScore(triggerScore ?? score),
    beneficiaries,
    partners,
    patterns,
    reasons: reasons.filter(Boolean)
  };
}

function expandPattern(pattern = [], baseTicketCombos = new Set()) {
  const [first, second, third] = pattern;
  const rows = [];
  const thirds = third === "flow"
    ? BOATS.filter((boat) => boat !== first && boat !== second)
    : [third];
  for (const t of thirds) {
    if (!Number.isInteger(t) || t < 1 || t > 6 || t === first || t === second) continue;
    const combo = `${first}-${second}-${t}`;
    if (baseTicketCombos.has(combo)) continue;
    rows.push(combo);
  }
  return rows;
}

function buildHeadCandidateRows(scoreByBoat = {}) {
  return Object.values(scoreByBoat)
    .filter((row) => row.canBeHead)
    .sort((a, b) => b.headScore - a.headScore || a.boat - b.boat)
    .slice(0, 4)
    .map((row) => ({ boat: row.boat, score: roundScore(row.headScore), reasons: row.headReasons }));
}

function buildPartnerRows(scoreByBoat = {}) {
  return Object.values(scoreByBoat)
    .sort((a, b) => Math.max(b.secondScore, b.thirdScore) - Math.max(a.secondScore, a.thirdScore) || a.boat - b.boat)
    .slice(0, 5)
    .map((row) => ({
      boat: row.boat,
      secondScore: roundScore(row.secondScore),
      thirdScore: roundScore(row.thirdScore),
      reasons: row.partnerReasons
    }));
}

function buildDangerRows(scoreByBoat = {}) {
  return Object.values(scoreByBoat)
    .filter((row) => !row.canBeHead && (row.scenarioTriggerScore >= 0.5 || row.beneficiaryScore >= 0.48 || row.boat >= 5 && row.thirdScore >= 0.55))
    .sort((a, b) => Math.max(b.scenarioTriggerScore, b.beneficiaryScore, b.thirdScore) - Math.max(a.scenarioTriggerScore, a.beneficiaryScore, a.thirdScore))
    .slice(0, 4)
    .map((row) => ({
      boat: row.boat,
      reason: row.dangerReason,
      triggerScore: roundScore(row.scenarioTriggerScore),
      beneficiaryScore: roundScore(row.beneficiaryScore)
    }));
}

export function buildRaceFlowScenarioModel({
  entries = [],
  featureScores = {},
  venueBias = null,
  stadiumNumber = null,
  raceConditions = null
} = {}) {
  const map = buildBoatMap(entries);
  const metrics = {};
  for (const boat of BOATS) {
    metrics[boat] = boatMetrics(map.get(boat) || { boat }, featureScores);
  }

  const tendencyAvailable = Object.values(metrics).some((row) => tendencyHasSignal(row.tendency));
  const venueAvailable = BOATS.some((boat) => venueCourseBias(venueBias, stadiumNumber, boat));
  const dataWarnings = [];
  if (!tendencyAvailable) dataWarnings.push("戦法データが不足しているため、展開シナリオは展示・足色中心で評価");
  if (!venueAvailable) dataWarnings.push("会場バイアスが未取得のため、全国共通の展開評価で補正");
  const conditions = normalizeRaceConditions(raceConditions || {});

  const walls = {
    2: wallScore(metrics[2]),
    3: wallScore(metrics[3]),
    4: wallScore(metrics[4])
  };

  const vEscape = venueRate(venueBias, stadiumNumber, 1, ["escapeRate", "nigeRate", "winRate"]) ?? 0.5;
  const vSashi2 = venueRate(venueBias, stadiumNumber, 2, ["sashiRate", "decisionSashiRate"]) ?? 0.5;
  const vMakuri3 = venueRate(venueBias, stadiumNumber, 3, ["makuriRate", "decisionMakuriRate"]) ?? 0.5;
  const vMakuriSashi3 = venueRate(venueBias, stadiumNumber, 3, ["makuriSashiRate", "decisionMakuriSashiRate"]) ?? 0.5;
  const vMakuriSashi4 = venueRate(venueBias, stadiumNumber, 4, ["makuriSashiRate", "decisionMakuriSashiRate"]) ?? 0.5;

  const b1 = metrics[1];
  const b2 = metrics[2];
  const b3 = metrics[3];
  const b4 = metrics[4];
  const b5 = metrics[5];
  const b6 = metrics[6];

  const b1Escape = positiveRate(b1.tendency, "escapeRate", 0.42);
  const b1BeatenBySashi = positiveRate(b1.tendency, "beatenBySashiRate", 0.08);
  const b1BeatenByMakuri = positiveRate(b1.tendency, "beatenByMakuriRate", 0.06);
  const b1BeatenByMakuriSashi = positiveRate(b1.tendency, "beatenByMakuriSashiRate", 0.05);
  const weakBoat2Wall = 1 - walls[2];
  const weakBoat3Wall = 1 - walls[3];
  const boat1WeakFoot = Math.max(0, 0.52 - b1.lapTime) + Math.max(0, 0.52 - b1.turnTime);
  const roughWaterStability1 = conditions.waveLevel * Math.max(0, ((b1.lapTime + b1.turnTime) / 2) - 0.55) * 0.12;
  const headwindEscapeDrag = conditions.isHeadwind
    ? conditions.windLevel * (Math.max(0, 0.56 - b1.exST) + Math.max(0, 0.56 - b1.turnTime)) * 0.09
    : 0;
  const tailwindCenterAttack = conditions.isTailwind ? conditions.windLevel * 0.08 : 0;
  const crosswindStability = conditions.isCrosswind ? conditions.windLevel * 0.04 : 0;
  const waveAggressionPenalty = conditions.waveLevel * 0.08;
  const conditionAdjustmentLog = [];

  const escape1 = clamp(
    0.24 +
    b1.exST * 0.13 +
    b1.exTime * 0.09 +
    b1.lapTime * 0.15 +
    b1.turnTime * 0.16 +
    b1.motor2Rate * 0.08 +
    b1Escape * 0.55 +
    walls[2] * 0.12 +
    (vEscape - 0.5) * 0.12 -
    b1BeatenBySashi * 0.65 -
    b1BeatenByMakuri * 0.65 -
    b1BeatenByMakuriSashi * 0.62 -
    weakBoat2Wall * 0.08 -
    boat1WeakFoot * 0.12 +
    roughWaterStability1 -
    headwindEscapeDrag +
    crosswindStability * Math.max(0, b1.turnTime - 0.5)
  );

  const sashi2Support = supportCount([
    b2.turnTime >= 0.64,
    b2.exST >= 0.62,
    positiveRate(b2.tendency, "sashiRate", 0.1) > 0,
    b1BeatenBySashi > 0,
    weakBoat2Wall < 0.46,
    vSashi2 >= 0.56
  ]);
  const sashi2 = clamp(
    0.13 +
    b2.exST * 0.17 +
    b2.turnTime * 0.24 +
    b2.lapTime * 0.08 +
    positiveRate(b2.tendency, "sashiRate", 0.08) * 0.95 +
    b1BeatenBySashi * 0.72 +
    Math.max(0, 0.55 - b1.turnTime) * 0.14 +
    conditions.isHeadwind * conditions.windLevel * b2.turnTime * 0.05 +
    conditions.waveLevel * Math.max(0, b2.turnTime - 0.55) * 0.07 +
    (vSashi2 - 0.5) * 0.16 -
    Math.max(0, b2.lateRate - 0.12) * b2.sampleWeight * 0.18
  );

  const makuri3Support = supportCount([
    b3.exST >= 0.64,
    b3.straightTime >= 0.64,
    positiveRate(b3.tendency, "makuriRate", 0.08) > 0,
    b1BeatenByMakuri > 0,
    weakBoat2Wall >= 0.42,
    vMakuri3 >= 0.56
  ]);
  const makuri3 = clamp(
    0.1 +
    b3.exST * 0.2 +
    b3.straightTime * 0.24 +
    positiveRate(b3.tendency, "makuriRate", 0.08) * 0.9 +
    b1BeatenByMakuri * 0.7 +
    weakBoat2Wall * 0.22 +
    weakBoat3Wall * 0.08 +
    tailwindCenterAttack * ((b3.exST + b3.straightTime) / 2) -
    conditions.waveLevel * Math.max(0, 0.58 - b3.turnTime) * 0.09 -
    (conditions.isCrosswind ? conditions.windLevel * Math.max(0, 0.58 - b3.turnTime) * 0.06 : 0) -
    (vMakuri3 - 0.5) * 0.16 -
    Math.max(0, 0.45 - b3.turnTime) * 0.06
  );

  const makuriSashi3Support = supportCount([
    b3.turnTime >= 0.62,
    b3.straightTime >= 0.56,
    positiveRate(b3.tendency, "makuriSashiRate", 0.07) > 0,
    b1BeatenByMakuriSashi > 0,
    weakBoat2Wall >= 0.34,
    vMakuriSashi3 >= 0.56
  ]);
  const makuriSashi3 = clamp(
    0.11 +
    b3.exST * 0.13 +
    b3.straightTime * 0.12 +
    b3.turnTime * 0.27 +
    positiveRate(b3.tendency, "makuriSashiRate", 0.07) * 0.92 +
    b1BeatenByMakuriSashi * 0.72 +
    weakBoat2Wall * 0.13 +
    conditions.waveLevel * Math.max(0, b3.turnTime - 0.55) * 0.07 +
    (conditions.isCrosswind ? conditions.windLevel * Math.max(0, b3.turnTime - 0.55) * 0.05 : 0) +
    (vMakuriSashi3 - 0.5) * 0.16
  );

  const attack3 = Math.max(makuri3, makuriSashi3);
  const b4DirectSupport = supportCount([
    b4.exST >= 0.6,
    b4.straightTime >= 0.62,
    b4.turnTime >= 0.62,
    positiveRate(b4.tendency, "makuriSashiRate", 0.07) > 0,
    attack3 >= 0.5,
    vMakuriSashi4 >= 0.56
  ]);
  const secondWave4 = clamp(
    0.08 +
    b4.exST * 0.1 +
    b4.straightTime * 0.19 +
    b4.turnTime * 0.25 +
    positiveRate(b4.tendency, "makuriSashiRate", 0.07) * 0.78 +
    attack3 * 0.24 +
    weakBoat3Wall * 0.08 +
    b1BeatenByMakuriSashi * 0.42 +
    conditions.waveLevel * Math.max(0, b4.turnTime - 0.55) * 0.1 +
    conditions.isTailwind * conditions.windLevel * Math.max(0, b4.straightTime - 0.55) * 0.04 +
    (vMakuriSashi4 - 0.5) * 0.15
  );

  const outside5 = clamp(0.1 + b5.lapTime * 0.17 + b5.straightTime * 0.16 + b5.turnTime * 0.11 + b5.motor2Rate * 0.08 + Math.max(sashi2, attack3, secondWave4) * 0.18 - waveAggressionPenalty * 0.35);
  const outside6 = clamp(0.08 + b6.lapTime * 0.15 + b6.straightTime * 0.15 + b6.turnTime * 0.1 + b6.motor2Rate * 0.08 + Math.max(sashi2, attack3, secondWave4) * 0.15 - waveAggressionPenalty * 0.42);
  const outsideFollow = Math.max(outside5, outside6);
  if (conditions.windLevel > 0) {
    conditionAdjustmentLog.push({
      type: "wind",
      level: conditions.windLevel >= 1 ? "strong" : "medium",
      windSpeed: conditions.windSpeed,
      windDirection: conditions.windDirection,
      note: conditions.isTailwind
        ? "追い風気味のためSTと直線が良いセンター攻めを軽く加点"
        : conditions.isHeadwind
          ? "向かい風気味のため差し・ターン安定を軽く重視"
          : "強めの風のためターン安定と信頼度を軽く調整"
    });
  }
  if (conditions.waveLevel > 0) {
    conditionAdjustmentLog.push({
      type: "wave",
      level: conditions.waveLevel >= 1 ? "strong" : "medium",
      waveHeight: conditions.waveHeight,
      note: "波高あり。外の一撃頭を少し抑え、周回・まわり足の安定を重視"
    });
  }

  const scoreByBoat = {};
  const headScore1 = escape1;
  const headScore2 = sashi2 * (sashi2Support >= 2 ? 0.95 : 0.62);
  const headScore3 = Math.max(makuri3 * (makuri3Support >= 2 ? 0.94 : 0.62), makuriSashi3 * (makuriSashi3Support >= 2 ? 0.9 : 0.62));
  const headScore4Raw = secondWave4 * (b4DirectSupport >= 3 ? 0.82 : b4DirectSupport >= 2 ? 0.62 : 0.42);
  const headScore4 = b4DirectSupport >= 2 ? headScore4Raw : Math.min(headScore4Raw, 0.42);
  const outsideHeadCap = 0.31;
  const headScore5 = Math.min(outside5 * 0.35, outsideHeadCap);
  const headScore6 = Math.min(outside6 * 0.32, outsideHeadCap - 0.02);

  const beneficiary4 = clamp(0.12 + attack3 * 0.38 + b4.turnTime * 0.2 + b4.straightTime * 0.16 + positiveRate(b4.tendency, "makuriSashiRate", 0.07) * 0.48);
  const beneficiary5 = outside5;
  const beneficiary6 = outside6;

  const rows = [
    {
      boat: 1,
      headScore: headScore1,
      secondScore: clamp(0.22 + escape1 * 0.28 + sashi2 * 0.18 + attack3 * 0.16 + b1.lapTime * 0.12 + b1.turnTime * 0.12),
      thirdScore: clamp(0.2 + escape1 * 0.18 + Math.max(sashi2, attack3, secondWave4) * 0.2 + b1.lapTime * 0.11 + b1.turnTime * 0.11),
      scenarioTriggerScore: escape1,
      beneficiaryScore: clamp(escape1 * 0.42 + b1.lapTime * 0.18 + b1.turnTime * 0.18),
      supportFactors: supportCount([b1.lapTime >= 0.62, b1.turnTime >= 0.62, b1Escape > 0, walls[2] >= 0.55]),
      canBeHead: headScore1 >= 0.42,
      headReasons: [
        b1.lapTime >= 0.62 && b1.turnTime >= 0.62 ? "周回とまわり足が上位" : null,
        b1Escape > 0 ? "逃げ率の支えあり" : null
      ].filter(Boolean),
      partnerReasons: ["1残し評価"],
      dangerReason: "1着信頼が割れた場合の残し"
    },
    {
      boat: 2,
      headScore: headScore2,
      secondScore: clamp(0.22 + sashi2 * 0.3 + b2.turnTime * 0.18 + walls[2] * 0.13 + b2.lapTime * 0.08),
      thirdScore: clamp(0.21 + sashi2 * 0.18 + b2.turnTime * 0.14 + b2.lapTime * 0.1 + walls[2] * 0.08),
      scenarioTriggerScore: sashi2,
      beneficiaryScore: clamp(sashi2 * 0.36 + walls[2] * 0.24 + b2.turnTime * 0.14),
      supportFactors: sashi2Support,
      canBeHead: headScore2 >= 0.48 && sashi2Support >= 2,
      headReasons: [
        b2.turnTime >= 0.64 ? "まわり足が良く差し向き" : null,
        positiveRate(b2.tendency, "sashiRate", 0.08) > 0 ? "差し率の支えあり" : null
      ].filter(Boolean),
      partnerReasons: ["2差し・2着残り"],
      dangerReason: "差し筋はあるが頭支持が不足"
    },
    {
      boat: 3,
      headScore: headScore3,
      secondScore: clamp(0.22 + attack3 * 0.3 + b3.straightTime * 0.12 + b3.turnTime * 0.14),
      thirdScore: clamp(0.2 + attack3 * 0.22 + b3.straightTime * 0.12 + b3.turnTime * 0.14),
      scenarioTriggerScore: attack3,
      beneficiaryScore: clamp(makuriSashi3 * 0.3 + b3.turnTime * 0.18 + b3.straightTime * 0.12),
      supportFactors: Math.max(makuri3Support, makuriSashi3Support),
      canBeHead: headScore3 >= 0.5 && Math.max(makuri3Support, makuriSashi3Support) >= 2,
      headReasons: [
        b3.exST >= 0.64 && b3.straightTime >= 0.64 ? "STと直線が良い" : null,
        b3.turnTime >= 0.62 ? "まくり差しの足もある" : null
      ].filter(Boolean),
      partnerReasons: ["センター攻め後の2・3着"],
      dangerReason: "攻め足はあるが頭までは条件不足"
    },
    {
      boat: 4,
      headScore: headScore4,
      secondScore: clamp(0.22 + secondWave4 * 0.32 + beneficiary4 * 0.18 + b4.turnTime * 0.12 + b4.straightTime * 0.1),
      thirdScore: clamp(0.24 + secondWave4 * 0.28 + beneficiary4 * 0.18 + b4.turnTime * 0.12 + b4.straightTime * 0.1),
      scenarioTriggerScore: secondWave4,
      beneficiaryScore: beneficiary4,
      supportFactors: b4DirectSupport,
      canBeHead: headScore4 >= 0.5 && b4DirectSupport >= 2,
      headReasons: [
        attack3 >= 0.5 ? "3攻め後の差し場あり" : null,
        b4.turnTime >= 0.62 ? "まわり足上位" : null,
        b4.straightTime >= 0.62 ? "直線も支え" : null
      ].filter(Boolean),
      partnerReasons: ["3攻め後のまくり差し・相手筆頭"],
      dangerReason: "展開の恩恵は大きいが頭条件は限定"
    },
    {
      boat: 5,
      headScore: headScore5,
      secondScore: clamp(0.14 + outside5 * 0.24 + b5.lapTime * 0.1 + b5.straightTime * 0.1),
      thirdScore: clamp(0.2 + outside5 * 0.36 + b5.lapTime * 0.12 + b5.straightTime * 0.12),
      scenarioTriggerScore: outside5,
      beneficiaryScore: beneficiary5,
      supportFactors: supportCount([b5.lapTime >= 0.62, b5.straightTime >= 0.62, b5.turnTime >= 0.62]),
      canBeHead: false,
      headReasons: [],
      partnerReasons: ["頭より2・3着の展開突き"],
      dangerReason: "足色は穴相手向き。頭へは過大評価しない"
    },
    {
      boat: 6,
      headScore: headScore6,
      secondScore: clamp(0.12 + outside6 * 0.22 + b6.lapTime * 0.1 + b6.straightTime * 0.1),
      thirdScore: clamp(0.18 + outside6 * 0.34 + b6.lapTime * 0.12 + b6.straightTime * 0.12),
      scenarioTriggerScore: outside6,
      beneficiaryScore: beneficiary6,
      supportFactors: supportCount([b6.lapTime >= 0.62, b6.straightTime >= 0.62, b6.turnTime >= 0.62]),
      canBeHead: false,
      headReasons: [],
      partnerReasons: ["頭より3着穴の展開突き"],
      dangerReason: "外枠は頭ではなく相手穴評価"
    }
  ];

  for (const row of rows) scoreByBoat[row.boat] = row;

  const scenarios = [
    scenarioRow({
      id: "escape_1",
      attacker: 1,
      score: escape1,
      beneficiaries: [1, 2, 3],
      partners: [2, 3, 4],
      patterns: [[1, 2, "flow"], [1, 3, "flow"], [1, 4, "flow"]],
      reasons: [
        b1.lapTime >= 0.62 && b1.turnTime >= 0.62 ? "1号艇は周回とまわり足が上位で、イン残し評価を上げます。" : null,
        b1BeatenBySashi > 0 || b1BeatenByMakuri > 0 ? "1号艇の被差し・被まくり傾向は逃げ信頼度を抑えます。" : null
      ]
    }),
    scenarioRow({
      id: "sashi_2",
      attacker: 2,
      score: sashi2,
      beneficiaries: [1, 2, 3],
      partners: [1, 3, 4],
      patterns: [[2, 1, "flow"], [2, 3, "flow"], [2, 4, "flow"]],
      reasons: [
        b2.turnTime >= 0.64 ? "2号艇はまわり足が良く、差しと2着残りを上げます。" : null,
        positiveRate(b2.tendency, "sashiRate", 0.08) > 0 ? "2号艇の差し率がサンプル重み込みでプラス。" : null,
        b1BeatenBySashi > 0 ? "1号艇の差され率があり、2差し警戒。" : null
      ]
    }),
    scenarioRow({
      id: "makuri_3",
      attacker: 3,
      score: makuri3,
      beneficiaries: [3, 4, 5],
      partners: [1, 4, 5],
      patterns: [[3, 1, "flow"], [3, 4, "flow"]],
      reasons: [
        b3.exST >= 0.64 && b3.straightTime >= 0.64 ? "3号艇はSTと直線が良く、センター攻めの可能性があります。" : null,
        weakBoat2Wall >= 0.42 ? "2号艇の壁が弱く、3号艇の攻め筋が広がります。" : null,
        positiveRate(b3.tendency, "makuriRate", 0.08) > 0 ? "3号艇のまくり率を軽く加点。" : null
      ]
    }),
    scenarioRow({
      id: "makuri_sashi_3",
      attacker: 3,
      score: makuriSashi3,
      beneficiaries: [1, 3, 4],
      partners: [1, 4, 5],
      patterns: [[3, 1, "flow"], [3, 4, "flow"], [1, 3, "flow"]],
      reasons: [
        b3.turnTime >= 0.62 ? "3号艇はまわり足が良く、まくり差しと2・3着残りを上げます。" : null,
        b1BeatenByMakuriSashi > 0 ? "1号艇のまくり差され率を警戒。" : null
      ]
    }),
    scenarioRow({
      id: "second_wave_4",
      attacker: 4,
      score: secondWave4,
      beneficiaries: [4, 1, 3],
      partners: [1, 3, 5],
      patterns: [[4, 1, "flow"], [4, 3, "flow"], [3, 4, "flow"], [1, 4, "flow"]],
      reasons: [
        attack3 >= 0.5 ? "3号艇が攻めると4号艇に差し場が生まれます。" : null,
        b4.turnTime >= 0.62 ? "4号艇はまわり足が良く、3が攻めた後のまくり差し展開に注意。" : null,
        b4.straightTime >= 0.62 ? "4号艇の直線も展開突きの支え。" : null
      ]
    }),
    scenarioRow({
      id: "outside_follow_5_6",
      attacker: null,
      score: outsideFollow,
      beneficiaries: [5, 6],
      partners: [5, 6],
      patterns: [[1, 5, "flow"], [1, 6, "flow"], [3, 5, "flow"], [4, 5, "flow"]],
      reasons: [
        outside5 >= 0.58 ? "5号艇は周回・直線を2・3着穴で評価。" : null,
        outside6 >= 0.56 ? "6号艇は頭より3着穴で評価。" : null,
        "5・6号艇は頭候補に過大評価せず、相手穴中心です。"
      ]
    })
  ].sort((a, b) => b.score - a.score);

  const headCandidates = buildHeadCandidateRows(scoreByBoat);
  const partnerCandidates = buildPartnerRows(scoreByBoat);
  const dangerousButNotHead = buildDangerRows(scoreByBoat);
  const topScenario = scenarios[0];
  const secondaryScenario = scenarios.find((row) => row.id !== topScenario?.id) || null;
  const upsetScenario = scenarios.find((row) => row.id !== "escape_1" && row.score >= 52) || null;
  const explanations = [
    topScenario ? `本線展開は「${topScenario.label}」。${topScenario.reasons[0] || "展示足と進入のバランスから評価しています。"}` : null,
    secondaryScenario ? `対抗展開は「${secondaryScenario.label}」。${secondaryScenario.reasons[0] || "相手候補の連動を見ます。"}` : null,
    upsetScenario && upsetScenario.id !== topScenario?.id ? `穴展開は「${upsetScenario.label}」。${upsetScenario.reasons[0] || "隊形が崩れた時だけ押さえます。"}` : null,
    dangerousButNotHead.length > 0 ? `危険だが頭ではない艇: ${dangerousButNotHead.map((row) => `${row.boat}号艇`).join("、")}。相手穴中心で扱います。` : null,
    ...conditionAdjustmentLog.map((row) => row.note),
    !tendencyAvailable ? "戦法データ不足のため、展示ST・展示タイム・周回・直線・まわり足を中心に展開評価しています。" : null
  ].filter(Boolean);

  const wallScores = [2, 3, 4].map((boat) => ({
    boat,
    wallScore: roundScore(walls[boat]),
    sampleStatus: metrics[boat].sampleStatus,
    lateStartRate: metrics[boat].lateRate
  }));
  const headPartnerSplit = Object.values(scoreByBoat)
    .sort((a, b) => a.boat - b.boat)
    .map((row) => ({
      boat: row.boat,
      headScore: roundScore(row.headScore),
      secondScore: roundScore(row.secondScore),
      thirdScore: roundScore(row.thirdScore),
      scenarioTriggerScore: roundScore(row.scenarioTriggerScore),
      beneficiaryScore: roundScore(row.beneficiaryScore),
      supportFactors: row.supportFactors,
      canBeHead: row.canBeHead,
      dangerReason: row.dangerReason
    }));

  const ticketAdjustmentLog = [];
  if (escape1 < 0.48) {
    ticketAdjustmentLog.push({
      action: "demote",
      target: "1-head-heavy",
      reason: "1号艇の逃げ信頼度が低く、2/3/4頭の警戒を上げます。"
    });
  }
  for (const row of dangerousButNotHead) {
    ticketAdjustmentLog.push({
      action: "demote",
      target: `${row.boat}-head`,
      reason: row.reason
    });
  }

  return {
    available: safeArray(entries).length > 0,
    dataWarnings,
    scenarios,
    mainScenario: topScenario || null,
    secondaryScenario,
    upsetScenario,
    wallScores,
    headCandidates,
    partnerCandidates,
    dangerousButNotHead,
    headPartnerSplit,
    scoreByBoat: Object.fromEntries(
      Object.entries(scoreByBoat).map(([boat, row]) => [boat, {
        boat: row.boat,
        headScore: row.headScore,
        secondScore: row.secondScore,
        thirdScore: row.thirdScore,
        scenarioTriggerScore: row.scenarioTriggerScore,
        beneficiaryScore: row.beneficiaryScore,
        supportFactors: row.supportFactors,
        canBeHead: row.canBeHead,
        dangerReason: row.dangerReason
      }])
    ),
    ticketAdjustmentLog,
    conditionAdjustmentLog,
    explanations,
    quality: {
      tendencyAvailable,
      venueAvailable,
      conditionAvailable: conditions.available,
      confidenceAdjustment: dataWarnings.length * -1.5 - (conditions.windLevel > 0 || conditions.waveLevel > 0 ? 0.8 : 0)
    }
  };
}

export function applyRaceFlowScenarioAdjustments(entries = [], model = {}) {
  const byBoat = model?.scoreByBoat || {};
  return safeArray(entries).map((row) => {
    const boat = boatNumber(row);
    const scores = byBoat[String(boat)] || byBoat[boat] || {};
    const head = finiteNumber(scores.headScore, 0.5);
    const second = finiteNumber(scores.secondScore, 0.5);
    const third = finiteNumber(scores.thirdScore, 0.5);
    const trigger = finiteNumber(scores.scenarioTriggerScore, 0.5);
    const beneficiary = finiteNumber(scores.beneficiaryScore, 0.5);
    const outsideDampening = boat >= 5 ? 0.35 : 1;
    let adjustment = (
      (head - 0.5) * 0.16 * outsideDampening +
      (trigger - 0.5) * 0.045 * outsideDampening +
      (beneficiary - 0.5) * 0.035 +
      (Math.max(second, third) - 0.5) * 0.02
    );
    if (boat >= 5) adjustment = Math.min(adjustment, 0.035);
    adjustment = clamp(adjustment, -0.12, boat >= 5 ? 0.035 : 0.14);
    return {
      ...row,
      baseScoreBeforeRaceFlow: row.score,
      score: finiteNumber(row.score, 0) + adjustment,
      raceFlowAdjustment: round(adjustment, 4),
      raceFlowHeadScore: roundScore(head),
      raceFlowSecondScore: roundScore(second),
      raceFlowThirdScore: roundScore(third),
      raceFlowScenarioTriggerScore: roundScore(trigger),
      raceFlowBeneficiaryScore: roundScore(beneficiary),
      raceFlowCanBeHead: scores.canBeHead === true
    };
  });
}

export function buildRaceFlowScenarioTickets(model = {}, baseTickets = [], limit = 6) {
  const baseTicketCombos = new Set(safeArray(baseTickets).map((ticket) => ticket?.combo).filter(Boolean));
  const rows = [];
  const seen = new Set(baseTicketCombos);
  const scenarios = safeArray(model?.scenarios)
    .filter((scenario) => scenario.id !== "escape_1" && scenario.score >= 54)
    .sort((a, b) => b.score - a.score);
  for (const scenario of scenarios) {
    for (const pattern of safeArray(scenario.patterns)) {
      for (const combo of expandPattern(pattern, baseTicketCombos)) {
        if (seen.has(combo)) continue;
        seen.add(combo);
        rows.push({
          combo,
          boats: combo.split("-").map((value) => Number(value)),
          probability: round(clamp((scenario.score / 100) * 0.07), 4),
          sourcePattern: scenario.label,
          scenarioId: scenario.id,
          scenarioName: scenario.label,
          upsetScore: scenario.score
        });
        model.ticketAdjustmentLog?.push?.({
          action: "promote",
          target: combo,
          scenarioId: scenario.id,
          reason: `${scenario.label}の展開スコアが高いため追加候補へ昇格`
        });
        if (rows.length >= limit) return rows;
      }
    }
  }
  return rows;
}
