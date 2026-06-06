import KyoteibiyoriDebugPanel from "./KyoteibiyoriDebugPanel.jsx";

export default function AdvancedDebugPanel({
  enabled = false,
  debug = {},
  originalExhibition = null,
  currentRacerTendency = null,
  openApiModel = null,
  data = null,
  displayPreview = [],
  safePrettyJson
}) {
  if (!enabled) return null;
  return (
    <section className="card advanced-debug-shell">
      <div className="section-head compact-head">
        <h2>詳細・デバッグ</h2>
      </div>
      <details className="practical-details">
        <summary>詳細データ</summary>
        <div className="debug-grid">
          <div>
            <div className="muted">display preview</div>
            <pre className="json-preview">{safePrettyJson(displayPreview)}</pre>
          </div>
          <div>
            <div className="muted">canonical preview</div>
            <pre className="json-preview">{safePrettyJson(debug.canonicalPreview)}</pre>
          </div>
          <div>
            <div className="muted">table preview</div>
            <pre className="json-preview">{safePrettyJson(debug.tablePreview)}</pre>
          </div>
          <div>
            <div className="muted">prediction input preview</div>
            <pre className="json-preview">{safePrettyJson(debug.predictionInputPreview)}</pre>
          </div>
          <div>
            <div className="muted">race conditions</div>
            <pre className="json-preview">{safePrettyJson({
              ok: debug.raceConditionsOk,
              error: debug.raceConditionsError,
              source: debug.raceConditionsSource,
              windDirection: debug.windDirection,
              windSpeed: debug.windSpeed,
              waveHeight: debug.waveHeight,
              weather: debug.weather,
              temperature: debug.temperature,
              waterTemperature: debug.waterTemperature,
              conditions: debug.raceConditions,
              debug: debug.raceConditionsDebug
            })}</pre>
          </div>
        </div>
      </details>
      <details className="practical-details">
        <summary>予想エンジン内部スコア</summary>
        <div className="debug-grid">
          <div>
            <div className="muted">feature score preview</div>
            <pre className="json-preview">{safePrettyJson(debug.featureScorePreview)}</pre>
          </div>
          <div>
            <div className="muted">scenario score preview</div>
            <pre className="json-preview">{safePrettyJson(debug.scenarioScorePreview)}</pre>
          </div>
          <div>
            <div className="muted">race flow scenario scores</div>
            <pre className="json-preview">{safePrettyJson(debug.raceFlowScenarioPreview)}</pre>
          </div>
          <div>
            <div className="muted">wall scores</div>
            <pre className="json-preview">{safePrettyJson(debug.wallScorePreview)}</pre>
          </div>
          <div>
            <div className="muted">head / partner split</div>
            <pre className="json-preview">{safePrettyJson(debug.headPartnerSplitPreview)}</pre>
          </div>
          <div>
            <div className="muted">ticket adjustment log</div>
            <pre className="json-preview">{safePrettyJson(debug.ticketAdjustmentLog)}</pre>
          </div>
          <div>
            <div className="muted">condition adjustment log</div>
            <pre className="json-preview">{safePrettyJson(debug.conditionAdjustmentLog)}</pre>
          </div>
          <div>
            <div className="muted">tendency score preview</div>
            <pre className="json-preview">{safePrettyJson(debug.tendencyScorePreview)}</pre>
          </div>
        </div>
      </details>
      <details className="practical-details">
        <summary>取得ログ</summary>
        <KyoteibiyoriDebugPanel
          debug={debug}
          originalExhibition={originalExhibition}
          safePrettyJson={safePrettyJson}
        />
      </details>
      <details className="practical-details">
        <summary>生データJSON</summary>
        <div className="debug-grid">
          <div>
            <div className="muted">originalExhibition</div>
            <pre className="json-preview">{safePrettyJson(originalExhibition)}</pre>
          </div>
          <div>
            <div className="muted">tendency</div>
            <pre className="json-preview">{safePrettyJson(currentRacerTendency)}</pre>
          </div>
          <div>
            <div className="muted">openApiModel</div>
            <pre className="json-preview">{safePrettyJson(openApiModel)}</pre>
          </div>
          <div>
            <div className="muted">legacy data</div>
            <pre className="json-preview">{safePrettyJson(data)}</pre>
          </div>
        </div>
      </details>
    </section>
  );
}
