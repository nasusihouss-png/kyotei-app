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
    const boat = comboBoats(ticket?.combo)[index];
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

export default function PredictionSummary({
  prediction = null,
  predictionExplanation = {},
  formatPercentDisplay
}) {
  if (!prediction) return null;
  const tickets = safeArray(prediction?.tickets?.trifecta);
  const raceFlow = prediction?.raceFlowScenario || {};
  const headCandidates = safeArray(raceFlow.headCandidates);
  const partnerCandidates = safeArray(raceFlow.partnerCandidates);
  const dangerousButNotHead = safeArray(raceFlow.dangerousButNotHead);
  const firstCandidates = headCandidates.length > 0
    ? headCandidates.slice(0, 3).map((row) => row.boat)
    : safeArray(prediction?.firstPlaceProbabilities).slice(0, 3).map((row) => row.boat);
  const secondCandidates = partnerCandidates.length > 0
    ? partnerCandidates.slice(0, 4).map((row) => row.boat)
    : uniqueTopBoats(tickets.slice(0, 10), 1, 4);
  const thirdCandidates = partnerCandidates.length > 0
    ? partnerCandidates.slice(0, 5).map((row) => row.boat)
    : uniqueTopBoats(tickets.slice(0, 10), 2, 5);
  const confidence = predictionExplanation?.confidence_score ?? null;
  const confidenceLabel = confidence === null || confidence === undefined
    ? "-"
    : formatPercentDisplay(confidence);
  const confidenceBand = predictionExplanation?.confidence_band || "-";
  const upsetText = safeArray(prediction?.extraTickets).length > 0
    ? prediction?.upsetAlert || "展開穴あり"
    : "低め";
  const main = tickets[0]?.combo || "-";
  const counter = tickets.slice(1, 4).map((row) => row.combo).filter(Boolean).join(" / ") || "-";
  const upset = safeArray(prediction?.extraTickets).slice(0, 4).map((row) => row.combo).join(" / ") ||
    tickets.slice(4, 6).map((row) => row.combo).join(" / ") ||
    "-";

  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>予想サマリー</h2>
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
          <strong>{main}</strong>
        </div>
        <div className="betting-summary-item">
          <span>対抗</span>
          <strong>{counter}</strong>
        </div>
        <div className="betting-summary-item">
          <span>穴</span>
          <strong>{upset}</strong>
        </div>
        <div className="betting-summary-item">
          <span>信頼度</span>
          <strong>{confidenceLabel} / {confidenceBand}</strong>
        </div>
        <div className={`betting-summary-item ${safeArray(prediction?.extraTickets).length > 0 ? "warning" : ""}`}>
          <span>荒れ警報</span>
          <strong>{upsetText}</strong>
        </div>
        <div className="betting-summary-item">
          <span>本線展開</span>
          <strong>{scenarioLabel(raceFlow.mainScenario)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>対抗展開</span>
          <strong>{scenarioLabel(raceFlow.secondaryScenario)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>穴展開</span>
          <strong>{scenarioLabel(raceFlow.upsetScenario)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>危険だが頭ではない</span>
          <strong>{boatLabelList(dangerousButNotHead)}</strong>
        </div>
      </div>
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
