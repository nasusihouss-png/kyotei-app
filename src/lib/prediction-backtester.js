import {
  buildBoat4OpportunityRanking,
  buildConfidenceScore,
  buildRacePrediction,
  checkPredictionConsistency,
  DEFAULT_SCORING_CONFIG
} from "./kyotei-openapi-engine.js";
import { mergeScoringConfig } from "./scoring-config.js";

function finiteNumber(value, fallback = null) {
  if (value === null || value === undefined || value === "") return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

function normalizeDecision(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) return null;
  if (/まくり差|makuri.?sashi/.test(text)) return "makuriSashi";
  if (/まくり|makuri/.test(text)) return "makuri";
  if (/差し|sashi/.test(text)) return "sashi";
  if (/逃げ|nige|escape/.test(text)) return "escape";
  if (/抜き|nuki/.test(text)) return "nuki";
  if (/恵まれ|megumare/.test(text)) return "megumare";
  return text;
}

function raceKey(race = {}) {
  return `${race.date || race.raceDate || ""}|${race.venueId ?? race.race_stadium_number ?? ""}|${race.raceNo ?? race.race_number ?? ""}`;
}

function programFromHistoryRace(race = {}) {
  return {
    race_date: race.date ?? race.raceDate ?? null,
    race_stadium_number: finiteNumber(race.venueId ?? race.race_stadium_number, null),
    race_number: finiteNumber(race.raceNo ?? race.race_number, null),
    race_closed_at: race.closedAt ?? null,
    conditions: race.conditions || race.raceConditions || null,
    boats: safeArray(race.entries).map((entry) => ({
      racer_boat_number: finiteNumber(entry.boat ?? entry.lane, null),
      racer_course_number: finiteNumber(entry.course ?? entry.entryCourse ?? entry.boat, null),
      racer_name: entry.racerName ?? entry.name ?? null,
      racer_number: entry.racerId ?? entry.racerNo ?? entry.registrationNo ?? null,
      racer_start_timing: finiteNumber(entry.startTiming ?? entry.exST ?? entry.exhibitionSt, null),
      racer_average_start_timing: finiteNumber(entry.avgST ?? entry.avgStartTiming, null),
      racer_flying_count: finiteNumber(entry.flyingCount ?? entry.fCount, 0),
      racer_assigned_motor_number: entry.motorNo ?? entry.motorNumber ?? null,
      racer_assigned_motor_top_2_percent: finiteNumber(entry.motor2Rate ?? entry.motor_2rate, null),
      motorRankAtVenue: finiteNumber(entry.motorRankAtVenue ?? entry.motorRank, null),
      motorPercentileAtVenue: finiteNumber(entry.motorPercentileAtVenue ?? entry.motorPercentile, null),
      exST: finiteNumber(entry.exST ?? entry.exhibitionSt, null),
      exTime: finiteNumber(entry.exTime ?? entry.exhibitionTime, null),
      lapTime: finiteNumber(entry.lapTime, null),
      straightTime: finiteNumber(entry.straightTime, null),
      turnTime: finiteNumber(entry.turnTime, null),
      playerTendency: entry.playerTendency || entry.racerCourseStats || entry.techniqueStats || null
    })).filter((entry) => Number.isInteger(entry.racer_boat_number))
  };
}

function actualFromRace(race = {}) {
  const entries = safeArray(race.entries);
  const byFinish = entries
    .map((entry) => ({
      boat: finiteNumber(entry.boat ?? entry.lane, null),
      finish: finiteNumber(entry.finishPosition ?? entry.finish ?? entry.rank ?? entry.resultRank, null)
    }))
    .filter((row) => Number.isInteger(row.boat) && Number.isInteger(row.finish))
    .sort((a, b) => a.finish - b.finish);
  const winnerBoat = finiteNumber(race.result?.winnerBoat, null) ?? byFinish[0]?.boat ?? null;
  const trifecta = byFinish.length >= 3 ? byFinish.slice(0, 3).map((row) => row.boat).join("-") : null;
  return {
    winnerBoat,
    secondBoat: byFinish[1]?.boat ?? null,
    thirdBoat: byFinish[2]?.boat ?? null,
    trifecta,
    winnerDecision: normalizeDecision(race.result?.winningDecision ?? race.result?.decision ?? race.result?.kimarite),
    boat1InTop3: byFinish.slice(0, 3).some((row) => row.boat === 1)
  };
}

function hitRate(numerator, denominator) {
  return denominator > 0 ? numerator / denominator : null;
}

function pearson(rows = [], xKey, yKey) {
  const pairs = rows
    .map((row) => [finiteNumber(row[xKey], null), finiteNumber(row[yKey], null)])
    .filter(([x, y]) => x !== null && y !== null);
  if (pairs.length < 3) return null;
  const meanX = pairs.reduce((sum, [x]) => sum + x, 0) / pairs.length;
  const meanY = pairs.reduce((sum, [, y]) => sum + y, 0) / pairs.length;
  let num = 0;
  let denX = 0;
  let denY = 0;
  for (const [x, y] of pairs) {
    num += (x - meanX) * (y - meanY);
    denX += (x - meanX) ** 2;
    denY += (y - meanY) ** 2;
  }
  const den = Math.sqrt(denX * denY);
  return den > 0 ? num / den : null;
}

function buildCalibrationSummary(factorRows = [], raceRows = []) {
  const factorKeys = [
    "exSTScore",
    "turnTimeScore",
    "straightTimeScore",
    "motorRankScore",
    "motor2RateScore",
    "startReliabilityScore",
    "venueBiasContribution"
  ];
  const correlations = Object.fromEntries(factorKeys.map((key) => [key, {
    head: pearson(factorRows, key, "isWinner"),
    involvement: pearson(factorRows, key, "isTop3"),
    fourHead: pearson(factorRows.filter((row) => row.boat === 4), key, "isWinner")
  }]));
  const falsePositiveCases = raceRows
    .filter((row) => row.predictedHead !== row.actualHead && row.topHeadProbability >= 0.32)
    .slice(0, 12);
  const falseNegativeCases = raceRows
    .filter((row) => row.actualHead === 4 && row.boat4OpportunityScore < 48)
    .slice(0, 12);
  const coefficientSuggestions = [];
  if (factorRows.length < 20) {
    coefficientSuggestions.push({
      factor: "sampleSize",
      target: "all",
      currentWeight: null,
      suggestion: "hold",
      reason: "Sample size is small; keep coefficient changes manual and conservative."
    });
  }
  if ((correlations.turnTimeScore?.fourHead ?? 0) > (correlations.exSTScore?.fourHead ?? 0) + 0.08) {
    coefficientSuggestions.push({
      factor: "turnTime",
      target: "fourHeadOpportunity/residualScore",
      currentWeight: DEFAULT_SCORING_CONFIG.scoringCoefficients?.fourHeadOpportunity?.boat4TurnTime ?? null,
      suggestion: "increase for fourHeadOpportunity/residualScore",
      reason: "turnTime correlates with 4-head beneficiary cases more than exST in this sample."
    });
  }
  if ((correlations.exSTScore?.head ?? 0) < 0 && falsePositiveCases.length > 0) {
    coefficientSuggestions.push({
      factor: "exST",
      target: "headScore",
      currentWeight: DEFAULT_SCORING_CONFIG.scoringCoefficients?.headScore?.exST ?? null,
      suggestion: "decrease",
      reason: "High false positives for head prediction when exST correlation is weak or negative."
    });
  }
  if ((correlations.motorRankScore?.involvement ?? 0) > 0.12) {
    coefficientSuggestions.push({
      factor: "motorRank",
      target: "partnerResidualScore",
      currentWeight: DEFAULT_SCORING_CONFIG.scoringCoefficients?.partnerResidualScore?.motorRank ?? null,
      suggestion: "increase for residualScore",
      reason: "Strong motorRank correlates with 2nd/3rd involvement."
    });
  }
  return {
    correlations,
    falsePositiveCases,
    falseNegativeCases,
    coefficientSuggestions
  };
}

export function runPredictionBacktest({
  races = [],
  dateFrom = null,
  dateTo = null,
  venueId = null,
  config = DEFAULT_SCORING_CONFIG
} = {}) {
  const scoringConfig = mergeScoringConfig(config || {});
  const targetVenue = finiteNumber(venueId, null);
  const filtered = safeArray(races).filter((race) => {
    const date = String(race.date ?? race.raceDate ?? "");
    if (dateFrom && date < dateFrom) return false;
    if (dateTo && date > dateTo) return false;
    if (targetVenue !== null && finiteNumber(race.venueId, null) !== targetVenue) return false;
    return safeArray(race.entries).length >= 3;
  });
  const factorRows = [];
  const raceRows = [];
  let headTop1 = 0;
  let headTop2 = 0;
  let trifectaMain = 0;
  let trifectaMainPlusUpset = 0;
  let secondPartnerHit = 0;
  let partnerTop2Hit = 0;
  let fourHeadActual = 0;
  let fourHeadDetected = 0;
  let fourHeadPredicted = 0;
  let fourHeadTruePositive = 0;
  let falseThreeHead = 0;
  let falseOutsideHead = 0;
  let missed1Residual = 0;
  let missed4Beneficiary = 0;
  let passDecisionCorrect = 0;
  let avoidBadRaceCorrect = 0;
  let scenarioDecisionMatches = 0;
  let scenarioDecisionCount = 0;

  for (const race of filtered) {
    const actual = actualFromRace(race);
    if (!actual.winnerBoat) continue;
    const program = programFromHistoryRace(race);
    if (program.boats.length < 3) continue;
    const prediction = buildRacePrediction(program, null, scoringConfig);
    const confidence = buildConfidenceScore(prediction);
    const consistency = checkPredictionConsistency({ ...prediction, confidence });
    const finalDecision = prediction.finalPrediction?.buyDecision || (confidence.score < 45 || consistency.referenceOnly ? "pass" : "buy");
    const firstRows = safeArray(prediction.firstPlaceProbabilities);
    const predictedHead = firstRows[0]?.boat ?? null;
    const top2 = firstRows.slice(0, 2).map((row) => row.boat);
    const groupedMainTickets = safeArray(prediction.ticketGroups?.mainTickets);
    const groupedReferenceTickets = safeArray(prediction.ticketGroups?.referenceTickets);
    const mainTicketRows = groupedMainTickets.length > 0
      ? groupedMainTickets
      : groupedReferenceTickets.length > 0
        ? []
        : safeArray(prediction.tickets?.trifecta).slice(0, 6);
    const mainTickets = mainTicketRows.map((ticket) => ticket.combo);
    const partnerTop2 = safeArray(prediction.finalPrediction?.partnerCandidates || prediction.partnerCandidates).slice(0, 2).map((row) => Number(row.boat));
    const allTickets = new Set([
      ...mainTickets,
      ...safeArray(prediction.ticketGroups?.secondaryTickets).map((ticket) => ticket.combo),
      ...safeArray(prediction.ticketGroups?.upsetTickets).map((ticket) => ticket.combo),
      ...groupedReferenceTickets.map((ticket) => ticket.combo),
      ...safeArray(prediction.extraTickets).map((ticket) => ticket.combo)
    ]);
    const boat4Row = buildBoat4OpportunityRanking([program], {}, { limit: 1, config: scoringConfig })[0] || {};
    const boat4OpportunityScore = finiteNumber(boat4Row.boat4HeadOpportunityScore, 0);
    const topScenario = prediction.raceFlowScenario?.mainScenarioGroup || prediction.raceFlowScenario?.mainScenario || null;
    const expectedDecision = topScenario?.id === "escape_1"
      ? "escape"
      : topScenario?.id === "sashi_2"
        ? "sashi"
        : topScenario?.id === "makuri_3"
          ? "makuri"
          : ["makuri_sashi_3", "makuriSashi_4", "four_beneficiary"].includes(topScenario?.id)
            ? "makuriSashi"
            : null;

    if (predictedHead === actual.winnerBoat) headTop1 += 1;
    if (top2.includes(actual.winnerBoat)) headTop2 += 1;
    if (actual.secondBoat && mainTicketRows.some((ticket) => Number(ticket.boats?.[0]) === actual.winnerBoat && Number(ticket.boats?.[1]) === actual.secondBoat)) {
      secondPartnerHit += 1;
    }
    if (actual.secondBoat && partnerTop2.includes(Number(actual.secondBoat))) partnerTop2Hit += 1;
    if (actual.trifecta && mainTickets.includes(actual.trifecta)) trifectaMain += 1;
    if (actual.trifecta && allTickets.has(actual.trifecta)) trifectaMainPlusUpset += 1;
    const predictedFourHead = boat4OpportunityScore >= 48 || safeArray(prediction.finalPrediction?.headCandidates).some((row) => row.boat === 4);
    if (predictedFourHead) fourHeadPredicted += 1;
    if (actual.winnerBoat === 4) {
      fourHeadActual += 1;
      if (predictedFourHead) {
        fourHeadDetected += 1;
        fourHeadTruePositive += 1;
      }
      if (!predictedFourHead) missed4Beneficiary += 1;
    }
    if (predictedHead === 3 && actual.winnerBoat !== 3) falseThreeHead += 1;
    if (predictedHead >= 5 && actual.winnerBoat !== predictedHead) falseOutsideHead += 1;
    if (actual.boat1InTop3 && ![predictedHead, ...partnerTop2].includes(1)) missed1Residual += 1;
    const avoidRecommended = finalDecision === "pass" || confidence.score < 45 || consistency.referenceOnly;
    const badRace = actual.trifecta ? !mainTickets.includes(actual.trifecta) : predictedHead !== actual.winnerBoat;
    if (avoidRecommended === badRace) avoidBadRaceCorrect += 1;
    if ((finalDecision === "pass") === badRace) passDecisionCorrect += 1;
    if (expectedDecision && actual.winnerDecision) {
      scenarioDecisionCount += 1;
      if (expectedDecision === actual.winnerDecision) scenarioDecisionMatches += 1;
    }

    raceRows.push({
      raceKey: raceKey(race),
      actualHead: actual.winnerBoat,
      predictedHead,
      topHeadProbability: finiteNumber(firstRows[0]?.probability, 0),
      confidenceScore: confidence.score,
      buyDecision: finalDecision,
      boat4OpportunityScore,
      partnerTop2,
      boat1InTop3: actual.boat1InTop3,
      predictedFourHead,
      mainScenarioId: topScenario?.id ?? null,
      actualDecision: actual.winnerDecision,
      predictedDecision: expectedDecision
    });

    for (const boat of safeArray(prediction.scoredBoats)) {
      const feature = prediction.featureScores?.byBoat?.[String(boat.boat)] || {};
      const finish = safeArray(race.entries).find((entry) => Number(entry.boat) === Number(boat.boat))?.finishPosition;
      factorRows.push({
        raceKey: raceKey(race),
        boat: boat.boat,
        isWinner: boat.boat === actual.winnerBoat ? 1 : 0,
        isTop3: finiteNumber(finish, 99) <= 3 ? 1 : 0,
        exSTScore: feature.scores?.exST ?? null,
        turnTimeScore: feature.scores?.turnTime ?? null,
        straightTimeScore: feature.scores?.straightTime ?? null,
        motorRankScore: feature.scores?.motorRank ?? null,
        motor2RateScore: feature.scores?.motor2Rate ?? null,
        startReliabilityScore: boat.professionalFactors?.startReliability?.avgSTScore ?? null,
        venueBiasContribution: boat.scoreParts?.venueBiasBoost ?? 0
      });
    }
  }

  const sampleRaceCount = raceRows.length;
  return {
    ok: true,
    sampleRaceCount,
      hitRates: {
      headTop1: hitRate(headTop1, sampleRaceCount),
      headTop2: hitRate(headTop2, sampleRaceCount),
      partnerTop2: hitRate(partnerTop2Hit, sampleRaceCount),
      secondPartner: hitRate(secondPartnerHit, sampleRaceCount),
      trifectaMain: hitRate(trifectaMain, sampleRaceCount),
      trifectaMainPlusUpset: hitRate(trifectaMainPlusUpset, sampleRaceCount),
      fourHeadDetection: hitRate(fourHeadDetected, fourHeadActual),
      fourHeadPrecision: hitRate(fourHeadTruePositive, fourHeadPredicted),
      fourHeadRecall: hitRate(fourHeadTruePositive, fourHeadActual),
      falseThreeHeadRate: hitRate(falseThreeHead, sampleRaceCount),
      falseOutsideHeadRate: hitRate(falseOutsideHead, sampleRaceCount),
      missed1ResidualRate: hitRate(missed1Residual, sampleRaceCount),
      missed4BeneficiaryRate: hitRate(missed4Beneficiary, fourHeadActual),
      avoidBadRaceAccuracy: hitRate(avoidBadRaceCorrect, sampleRaceCount),
      passDecisionAccuracy: hitRate(passDecisionCorrect, sampleRaceCount),
      scenarioDecision: hitRate(scenarioDecisionMatches, scenarioDecisionCount)
    },
    calibration: buildCalibrationSummary(factorRows, raceRows),
    debug: {
      dateFrom,
      dateTo,
      venueId: targetVenue,
      fourHeadActualCount: fourHeadActual,
      missed1ResidualCount: missed1Residual,
      missed4BeneficiaryCount: missed4Beneficiary,
      scenarioDecisionCount,
      raceRows: raceRows.slice(0, 50),
      factorRows: factorRows.slice(0, 50)
    }
  };
}
