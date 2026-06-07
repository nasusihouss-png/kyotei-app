function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function boatList(boats = []) {
  const rows = safeArray(boats)
    .map((boat) => Number(typeof boat === "object" ? boat.boat : boat))
    .filter((boat) => Number.isInteger(boat) && boat >= 1 && boat <= 6);
  return rows.length ? rows.map((boat) => `${boat}号艇`).join(" / ") : "-";
}

const PATTERN_LABELS = {
  inside_stable: "内安定",
  boat2_late: "2遅れ警戒",
  boat3_pressure: "3攻め",
  boat4_pressure: "4攻め",
  center_pressure: "センター優勢",
  outside_pressure: "外圧あり",
  uneven_slit: "スリット不揃い",
  high_f_risk: "Fリスク注意",
  low_confidence: "低信頼"
};

export default function SlitFormationSummary({ prediction = null }) {
  const formation = prediction?.slitFormation || prediction?.raceFlowScenario?.slitFormation || null;
  if (!formation) return null;
  const rows = safeArray(formation.rows);
  const beneficiaryBoats = rows
    .filter((row) => Number(row?.boat) === 4 && (
      Number(row?.attackStartScore ?? 0) >= 0.55 ||
      Number(row?.flowWideRisk ?? 0) >= 0.45
    ))
    .map((row) => row.boat);
  const impact = safeArray(formation.notes).slice(0, 2).join(" ");
  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>スリット予測</h2>
      </div>
      <div className="betting-summary-grid compact-grid">
        <div className="betting-summary-item primary">
          <span>予測隊形</span>
          <strong>{boatList(formation.expectedOrder)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>攻め起点</span>
          <strong>{boatList(formation.attackTriggerCandidates)}</strong>
        </div>
        <div className="betting-summary-item">
          <span>壁候補</span>
          <strong>{boatList(formation.wallBoats)}</strong>
        </div>
        <div className={`betting-summary-item ${safeArray(formation.lateRiskBoats).length ? "warning" : ""}`}>
          <span>出遅れ警戒</span>
          <strong>{boatList(formation.lateRiskBoats)}</strong>
        </div>
        <div className={`betting-summary-item ${safeArray(formation.earlyRiskBoats).length ? "warning" : ""}`}>
          <span>先行警戒</span>
          <strong>{boatList(formation.earlyRiskBoats)}</strong>
        </div>
        <div className="betting-summary-item primary">
          <span>スリットパターン</span>
          <strong>{PATTERN_LABELS[formation.slitPattern] || formation.slitPattern || "-"}</strong>
        </div>
        <div className="betting-summary-item">
          <span>展開利</span>
          <strong>{boatList(beneficiaryBoats.length ? beneficiaryBoats : formation.pressureBoats)}</strong>
        </div>
      </div>
      {impact ? (
        <div className="notice-banner compact-warning" style={{ marginTop: 10 }}>
          {impact}
        </div>
      ) : null}
    </section>
  );
}
