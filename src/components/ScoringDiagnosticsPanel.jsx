export default function ScoringDiagnosticsPanel({
  prediction = null,
  debug = {},
  safePrettyJson
}) {
  if (!prediction) return null;
  return (
    <section className="card">
      <details className="practical-details">
        <summary>係数・精度診断</summary>
        <div className="debug-grid">
          <div>
            <div className="muted">current scoring weights</div>
            <pre className="json-preview">{safePrettyJson(prediction.currentScoringWeights || debug.currentScoringWeights || null)}</pre>
          </div>
          <div>
            <div className="muted">major factor contribution by boat</div>
            <pre className="json-preview">{safePrettyJson(prediction.coefficientContributionByBoat || debug.coefficientContributionByBoat || [])}</pre>
          </div>
          <div>
            <div className="muted">scenario score breakdown</div>
            <pre className="json-preview">{safePrettyJson({
              mainScenarioGroup: prediction.mainScenarioGroup,
              derivedScenarioGroup: prediction.derivedScenarioGroup,
              scenarioFamilies: prediction.scenarioFamilyPreview || prediction.raceFlowScenario?.scenarioFamilies || []
            })}</pre>
          </div>
          <div>
            <div className="muted">backtest summary</div>
            <pre className="json-preview">{safePrettyJson(prediction.backtestCalibrationSummary || debug.backtestCalibrationSummary || null)}</pre>
          </div>
          <div>
            <div className="muted">coefficient warning</div>
            <pre className="json-preview">{safePrettyJson({
              warning: prediction.coefficientWarning || [],
              finalScenarioConsistencyCheck: prediction.finalScenarioConsistencyCheck || null,
              venueOverrideApplied: prediction.venueOverrideApplied || null
            })}</pre>
          </div>
        </div>
      </details>
    </section>
  );
}
