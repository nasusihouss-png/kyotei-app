function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function ticketReason(ticket) {
  if (ticket?.referenceOnly) return "参考: 条件付き";
  return ticket?.displayReason || safeArray(ticket?.reasons).slice(0, 3).join(" / ") || "-";
}

function ticketScenario(ticket) {
  return ticket?.scenarioId || ticket?.scenarioName || ticket?.decisionScenarioId || "-";
}

function TicketRow({ ticket, label, formatPercentDisplay }) {
  if (!ticket) return null;
  return (
    <div className="bet-ticket-row">
      <strong>{ticket.combo || "-"}</strong>
      <span>{ticket.grade || label}</span>
      <span>{ticket.probability == null ? "-" : formatPercentDisplay(ticket.probability)}</span>
      <small className="muted">理由: {ticketReason(ticket)} / scenario: {ticketScenario(ticket)}</small>
    </div>
  );
}

function TicketPanel({ title, emptyText, tickets, label, formatPercentDisplay }) {
  return (
    <div className="ticket-panel">
      <strong>{title}</strong>
      {tickets.length > 0 ? (
        <div className="bet-ticket-list">
          {tickets.map((ticket) => (
            <TicketRow
              key={`${label}-${ticket.combo}`}
              ticket={ticket}
              label={label}
              formatPercentDisplay={formatPercentDisplay}
            />
          ))}
        </div>
      ) : (
        <p className="muted strategy-line">{emptyText}</p>
      )}
    </div>
  );
}

export default function TicketList({
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
  const fallbackTickets = safeArray(prediction?.tickets?.trifecta).slice(0, 6);
  const confidence = Number(predictionExplanation?.confidence_score ?? NaN);
  const noBuyRecommended = finalPrediction.buyDecision === "pass" || groups.noBuyRecommended === true || (Number.isFinite(confidence) && confidence < 0.3);
  const noBuyReason = finalPrediction.explanation?.summary || groups.noBuyReason || "見送り推奨: レースの軸が割れており、無理に買うレースではありません。";
  const displayedReference = referenceTickets.length > 0
    ? referenceTickets
    : noBuyRecommended
      ? fallbackTickets.map((ticket) => ({ ...ticket, referenceOnly: true }))
      : [];

  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>Tickets</h2>
        {finalPrediction.buyDecision ? <p className="muted">判定: {finalPrediction.buyDecision}</p> : null}
      </div>
      {noBuyRecommended ? (
        <div className="warning-banner compact-warning">
          <strong>見送り推奨</strong>
          <div>{noBuyReason}</div>
          {safeArray(finalPrediction.warnings || groups.noBuyReasons).length > 0 ? (
            <small>{safeArray(finalPrediction.warnings || groups.noBuyReasons).slice(0, 4).join(" / ")}</small>
          ) : null}
        </div>
      ) : null}
      <div className="betting-ticket-grid">
        <TicketPanel
          title="本線買い目（A）"
          emptyText="A評価の買い目なし。無理に6点へ増やしません。"
          tickets={mainTickets}
          label="A"
          formatPercentDisplay={formatPercentDisplay}
        />
        <TicketPanel
          title="対抗買い目（B）"
          emptyText="B評価の対抗券はありません。"
          tickets={secondaryTickets}
          label="B"
          formatPercentDisplay={formatPercentDisplay}
        />
        <TicketPanel
          title="展開穴追加候補（C）"
          emptyText="強い追加穴はありません。"
          tickets={upsetTickets}
          label="C"
          formatPercentDisplay={formatPercentDisplay}
        />
        {displayedReference.length > 0 ? (
          <TicketPanel
            title="参考買い目"
            emptyText="参考券なし"
            tickets={displayedReference}
            label="参考"
            formatPercentDisplay={formatPercentDisplay}
          />
        ) : null}
      </div>
    </section>
  );
}
