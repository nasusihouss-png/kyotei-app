function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function comboBoats(combo) {
  return String(combo || "")
    .split("-")
    .map((value) => Number(value))
    .filter((value) => Number.isInteger(value));
}

function uniqueTopBoats(tickets, index, limit = 4) {
  const seen = new Set();
  const rows = [];
  for (const ticket of safeArray(tickets)) {
    const boat = Array.isArray(ticket?.boats) ? Number(ticket.boats[index]) : comboBoats(ticket?.combo)[index];
    if (!boat || seen.has(boat)) continue;
    seen.add(boat);
    rows.push(boat);
    if (rows.length >= limit) break;
  }
  return rows;
}

function boatLabelList(boats) {
  const normalized = safeArray(boats)
    .map((row) => typeof row === "object" ? row.boat : row)
    .filter((boat) => Number.isInteger(Number(boat)));
  return normalized.length > 0 ? normalized.map((boat) => `${boat}号艇`).join(" / ") : "-";
}

function scenarioLabel(scenario) {
  if (!scenario) return "-";
  const label = scenario.label || scenario.id || "-";
  return scenario.score == null ? label : `${label} ${Number(scenario.score).toFixed(1)}`;
}

function comboList(tickets, limit = 4) {
  return safeArray(tickets).slice(0, limit).map((row) => row.combo).filter(Boolean).join(" / ") || "-";
}

export default function PredictionSummary({
  prediction = null,
  predictionExplanation = {},
  formatPercentDisplay
}) {
  if (!prediction) return null;
  const finalPrediction = prediction.finalPrediction || prediction;
  const groups = prediction.ticketGroups || {};
  const mainTickets = safeArray(finalPrediction.mainTickets || groups.mainTickets);
  const secondaryTickets = safeArray(finalPrediction.secondaryTickets || groups.secondaryTickets);
  const upsetTickets = safeArray(finalPrediction.upsetTickets || groups.upsetTickets);
  const referenceTickets = safeArray(finalPrediction.referenceTickets || groups.referenceTickets);
  const candidateTickets = [
    ...mainTickets,
    ...secondaryTickets,
    ...upsetTickets,
    ...referenceTickets,
    ...safeArray(prediction?.tickets?.trifecta).slice(0, 10)
  ];
  const raceFlow = prediction?.raceFlowScenario || {};
  const headCandidates = safeArray(finalPrediction.headCandidates).length > 0
    ? safeArray(finalPrediction.headCandidates)
    : safeArray(raceFlow.headCandidates);
  const partnerCandidates = safeArray(finalPrediction.partnerCandidates).length > 0
    ? safeArray(finalPrediction.partnerCandidates).map((row) => ({ boat: row.partner ?? row.boat, ...row }))
    : safeArray(raceFlow.partnerCandidates);
  const dangerousButNotHead = safeArray(raceFlow.dangerousButNotHead);
  const firstCandidates = headCandidates.length > 0
    ? headCandidates.slice(0, 3).map((row) => row.boat)
    : safeArray(prediction?.firstPlaceProbabilities).slice(0, 3).map((row) => row.boat);
  const secondCandidates = partnerCandidates.length > 0
    ? partnerCandidates.slice(0, 4).map((row) => row.boat)
    : uniqueTopBoats(candidateTickets, 1, 4);
  const thirdCandidates = partnerCandidates.length > 0
    ? partnerCandidates.slice(0, 5).map((row) => row.boat)
    : uniqueTopBoats(candidateTickets, 2, 5);
  const confidence = finalPrediction.confidence?.score != null
    ? Number(finalPrediction.confidence.score) / 100
    : predictionExplanation?.confidence_score ?? null;
  const confidenceLabel = confidence === null || confidence === undefined
    ? "-"
    : formatPercentDisplay(confidence);
  const confidenceBand = predictionExplanation?.confidence_band || "-";
  const noBuyRecommended = finalPrediction.buyDecision === "pass" || groups.noBuyRecommended === true;
  const buyDecisionLabel = finalPrediction.buyDecision === "buy"
    ? "買い"
    : finalPrediction.buyDecision === "light"
      ? "軽め"
      : finalPrediction.buyDecision === "pass"
        ? "見送り"
        : "-";
  const upsetText = upsetTickets.length > 0
    ? prediction?.upsetAlert || "展開穴あり"
    : noBuyRecommended
      ? "見送り推奨"
      : "低め";

  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>Prediction Summary</h2>
      </div>
      <div className="betting-summary-grid">
        <div className="betting-summary-item primary">
          <span>1着候補</span>
          <strong>{boatLabelList(firstCandidates)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>2着候補</span>
          <strong>{boatLabelList(secondCandidates)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>3着候補</span>
          <strong>{boatLabelList(thirdCandidates)}</strong>
        </div>
        <div className="betting-summary-item primary">
          <span>本線</span>
          <strong>{comboList(mainTickets, 3)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>対抗</span>
          <strong>{comboList(secondaryTickets, 4)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>穴</span>
          <strong>{comboList(upsetTickets, 4)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>信頼度</span>
          <strong>{confidenceLabel} / {confidenceBand}</strong>
        </div>
        <div className={`betting-summary-item ${finalPrediction.buyDecision === "pass" ? "warning" : finalPrediction.buyDecision === "buy" ? "primary" : ""}`}>
          <span>Buy Decision</span>
          <strong>{buyDecisionLabel}</strong>
        </div>
        <div className={`betting-summary-item ${upsetTickets.length > 0 || noBuyRecommended ? "warning" : ""}`}>
          <span>荒れ警報</span>
          <strong>{upsetText}</strong>
        </div>
        <div className="betting-summary-item">
          <span>本線展開</span>
          <strong>{scenarioLabel(finalPrediction.mainScenario || raceFlow.mainScenarioGroup || raceFlow.mainScenario)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>派生展開</span>
          <strong>{scenarioLabel(finalPrediction.secondaryScenario || raceFlow.derivedScenarioGroup || raceFlow.secondaryScenario)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>参考券</span>
          <strong>{comboList(referenceTickets, 3)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>危険だが頭ではない</span>
          <strong>{boatLabelList(dangerousButNotHead)}</strong>
        </div>
      </div>
      {noBuyRecommended ? (
        <div className="warning-banner compact-warning" style={{ marginTop: 10 }}>
          {finalPrediction.explanation?.summary || groups.noBuyReason || "見送り推奨: レースの軸が割れており、無理に買うレースではありません。"}
        </div>
      ) : null}
      {finalPrediction.dataQuality?.overall ? (
        <div className="notice-banner compact-warning" style={{ marginTop: 10 }}>
          Data quality: {finalPrediction.dataQuality.overall}
          {safeArray(finalPrediction.warnings).length > 0 ? ` / ${safeArray(finalPrediction.warnings).slice(0, 2).join(" / ")}` : ""}
        </div>
      ) : null}
      {predictionExplanation?.skipRiskReason ? (
        <div className="warning-banner compact-warning" style={{ marginTop: 10 }}>
          {predictionExplanation.skipRiskReason}
        </div>
      ) : null}
      {confidence !== null && Number(confidence) < 0.3 ? (
        <div className="warning-banner compact-warning" style={{ marginTop: 10 }}>
          信頼度が低いため、買い目は参考程度です。見送りも検討してください。
        </div>
      ) : null}
    </section>
  );
}
