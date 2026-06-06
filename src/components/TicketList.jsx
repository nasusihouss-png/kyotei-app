function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function ticketReason(ticket) {
  if (ticket?.referenceOnly) return "参考: 条件付き";
  return ticket?.displayReason || safeArray(ticket?.reasons).slice(0, 3).join(" / ") || "-";
}

function TicketRow({ ticket, label, formatPercentDisplay }) {
  if (!ticket) return null;
  return (
    <div className="bet-ticket-row">
      <strong>{ticket.combo || "-"}</strong>
      <span>{label}</span>
      <span>{ticket.probability == null ? "-" : formatPercentDisplay(ticket.probability)}</span>
      <small className="muted">{ticketReason(ticket)}</small>
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
  const groups = prediction.ticketGroups || {};
  const mainTickets = safeArray(groups.mainTickets);
  const secondaryTickets = safeArray(groups.secondaryTickets);
  const upsetTickets = safeArray(groups.upsetTickets);
  const referenceTickets = safeArray(groups.referenceTickets);
  const fallbackTickets = safeArray(prediction?.tickets?.trifecta).slice(0, 6);
  const confidence = Number(predictionExplanation?.confidence_score ?? NaN);
  const noBuyRecommended = groups.noBuyRecommended === true || (Number.isFinite(confidence) && confidence < 0.3);
  const noBuyReason = groups.noBuyReason || "見送り推奨: レースの軸が割れており、無理に買うレースではありません。";
  const displayedReference = referenceTickets.length > 0
    ? referenceTickets
    : noBuyRecommended
      ? fallbackTickets.map((ticket) => ({ ...ticket, referenceOnly: true }))
      : [];

  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>Tickets</h2>
      </div>
      {noBuyRecommended ? (
        <div className="warning-banner compact-warning">
          <strong>見送り推奨</strong>
          <div>{noBuyReason}</div>
          {safeArray(groups.noBuyReasons).length > 0 ? (
            <small>{groups.noBuyReasons.join(" / ")}</small>
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
