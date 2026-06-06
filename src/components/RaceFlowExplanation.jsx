function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function scenarioLabel(scenario) {
  if (!scenario) return "-";
  const label = scenario.label || scenario.id || "-";
  return scenario.score == null ? label : `${label} / ${Number(scenario.score).toFixed(1)}`;
}

function ScenarioLine({ title, scenario, text }) {
  return (
    <div className="flow-line">
      <strong>{title}</strong>
      <p>{text || safeArray(scenario?.reasons)[0] || "-"}</p>
      <span>{scenarioLabel(scenario)}</span>
    </div>
  );
}

export default function RaceFlowExplanation({ prediction = null }) {
  if (!prediction) return null;
  const finalPrediction = prediction.finalPrediction || prediction;
  const raceFlow = prediction?.raceFlowScenario || {};
  const explanations = finalPrediction.explanation || {};
  const commonWarnings = safeArray(finalPrediction.commonCaseWarnings);
  const ticketReasoning = safeArray(finalPrediction.ticketReasoning);

  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>Race Flow Explanation</h2>
      </div>
      <div className="race-flow-list">
        <ScenarioLine
          title="本線シナリオ"
          scenario={finalPrediction.mainScenario || raceFlow.mainScenarioGroup || raceFlow.mainScenario}
          text={explanations.summary}
        />
        <ScenarioLine
          title="対抗 / 派生シナリオ"
          scenario={finalPrediction.secondaryScenario || raceFlow.derivedScenarioGroup || raceFlow.secondaryScenario}
          text={explanations.raceFlow}
        />
        <ScenarioLine
          title="穴・崩れ筋"
          scenario={finalPrediction.upsetScenario || raceFlow.upsetScenario}
          text={commonWarnings.join(" ") || safeArray(prediction.upsetReasons).join(" / ")}
        />
      </div>
      <div className="why-ticket-box">
        <strong>なぜその買い目か</strong>
        <p>
          {explanations.ticket ||
            ticketReasoning.slice(0, 4).map((row) => `${row.ticket}: ${row.reason}`).join(" / ") ||
            "最終判定のチケット根拠が不足しています。"}
        </p>
      </div>
    </section>
  );
}
