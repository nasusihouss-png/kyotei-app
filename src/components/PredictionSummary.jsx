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
  return safeArray(boats).length > 0 ? boats.map((boat) => `${boat}号艇`).join(" / ") : "-";
}

export default function PredictionSummary({
  prediction = null,
  predictionExplanation = {},
  formatPercentDisplay,
  formatMaybeNumber
}) {
  if (!prediction) return null;
  const tickets = safeArray(prediction?.tickets?.trifecta);
  const firstCandidates = safeArray(prediction?.firstPlaceProbabilities)
    .slice(0, 3)
    .map((row) => row.boat);
  const secondCandidates = uniqueTopBoats(tickets.slice(0, 10), 1, 4);
  const thirdCandidates = uniqueTopBoats(tickets.slice(0, 10), 2, 5);
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
      </div>
      {predictionExplanation?.skipRiskReason ? (
        <div className="warning-banner compact-warning" style={{ marginTop: 10 }}>
          {predictionExplanation.skipRiskReason}
        </div>
      ) : null}
      {confidence !== null && Number(confidence) < 0.3 ? (
        <div className="warning-banner compact-warning" style={{ marginTop: 10 }}>
          信頼度が低いため、買い目は参考扱いです。
        </div>
      ) : null}
    </section>
  );
}
