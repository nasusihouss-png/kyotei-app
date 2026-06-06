function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function TicketRow({ ticket, label, formatPercentDisplay }) {
  if (!ticket) return null;
  return (
    <div className="bet-ticket-row">
      <strong>{ticket.combo || "-"}</strong>
      <span>{label}</span>
      <span>{ticket.probability == null ? "-" : formatPercentDisplay(ticket.probability)}</span>
    </div>
  );
}

export default function TicketList({
  prediction = null,
  predictionExplanation = {},
  formatPercentDisplay
}) {
  if (!prediction) return null;
  const basicTickets = safeArray(prediction?.tickets?.trifecta).slice(0, 6);
  const extraTickets = safeArray(prediction?.extraTickets).slice(0, 8);
  const confidence = Number(predictionExplanation?.confidence_score ?? NaN);
  const shouldSkip = Number.isFinite(confidence) && confidence < 0.3;

  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>買い目</h2>
      </div>
      {shouldSkip ? (
        <div className="warning-banner compact-warning">
          見送り判断: 信頼度が低いため、無理に買わないレースです。
        </div>
      ) : null}
      <div className="betting-ticket-grid">
        <div className="ticket-panel">
          <strong>基本買い目6点</strong>
          <div className="bet-ticket-list">
            {basicTickets.map((ticket) => (
              <TicketRow
                key={`basic-${ticket.combo}`}
                ticket={ticket}
                label="3連単"
                formatPercentDisplay={formatPercentDisplay}
              />
            ))}
          </div>
        </div>
        <div className="ticket-panel">
          <strong>展開穴追加候補</strong>
          {extraTickets.length > 0 ? (
            <>
              {prediction?.upsetAlert ? <p className="muted strategy-line">{prediction.upsetAlert}</p> : null}
              <div className="bet-ticket-list">
                {extraTickets.map((ticket) => (
                  <TicketRow
                    key={`extra-${ticket.combo}`}
                    ticket={ticket}
                    label={ticket.sourcePattern || "展開穴"}
                    formatPercentDisplay={formatPercentDisplay}
                  />
                ))}
              </div>
            </>
          ) : (
            <p className="muted strategy-line">強い追加穴はなし。本線6点を中心に判断します。</p>
          )}
        </div>
      </div>
    </section>
  );
}
