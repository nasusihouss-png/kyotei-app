function localSafeArray(value) {
  return Array.isArray(value) ? value : [];
}

function localPrettyJson(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch {
    return "{}";
  }
}

export default function KyoteibiyoriDebugPanel({
  debug = {},
  originalExhibition = null,
  safePrettyJson = localPrettyJson
}) {
  return (
    <div className="quick-sheet-panel" style={{ marginTop: 12 }}>
      <strong>kyoteibiyori parser debug</strong>
      <div className="kv-list" style={{ marginTop: 10 }}>
        <div className="kv-row"><span>current raceKey</span><strong>{debug.currentRaceKey || "-"}</strong></div>
        <div className="kv-row"><span>isFetching</span><strong>{debug.isFetching ? "true" : "false"}</strong></div>
        <div className="kv-row"><span>requestId</span><strong>{debug.requestId ?? "-"}</strong></div>
        <div className="kv-row"><span>backendConnected</span><strong>{debug.backendConnected ? "true" : "false"}</strong></div>
        <div className="kv-row"><span>originalExhibition ok</span><strong>{debug.originalExhibitionOk ? "true" : "false"}</strong></div>
        <div className="kv-row"><span>originalExhibition backendConnected</span><strong>{debug.originalExhibitionBackendConnected ? "true" : "false"}</strong></div>
        <div className="kv-row"><span>originalExhibition error</span><strong>{debug.originalExhibitionError || "-"}</strong></div>
        <div className="kv-row"><span>originalExhibition source</span><strong>{debug.originalExhibitionSource || "-"}</strong></div>
        <div className="kv-row"><span>exhibitionFetchRoute</span><strong>{debug.exhibitionFetchRoute || "none"}</strong></div>
        <div className="kv-row"><span>playwrightStarted</span><strong>{debug.playwrightStarted ? "true" : "false"}</strong></div>
        <div className="kv-row"><span>playwrightFinished</span><strong>{debug.playwrightFinished ? "true" : "false"}</strong></div>
        <div className="kv-row"><span>playwrightError</span><strong>{debug.playwrightError || "-"}</strong></div>
        <div className="kv-row"><span>playwright page.url()</span><strong>{debug.pageUrl || "-"}</strong></div>
        <div className="kv-row"><span>playwright page.title()</span><strong>{debug.pageTitle || "-"}</strong></div>
        <div className="kv-row"><span>renderedHtml length</span><strong>{debug.renderedHtmlLength ?? "-"}</strong></div>
        <div className="kv-row"><span>rendered PNG</span><strong>{debug.latestScreenshotPath || debug.screenshotPath || "-"}</strong></div>
        <div className="kv-row"><span>network summary</span><strong>{debug.networkSummaryPath || "-"}</strong></div>
        {[
          ["rendered contains 周回", debug.renderedContains?.lap],
          ["rendered contains 一周", debug.renderedContains?.oneLap],
          ["rendered contains 直線", debug.renderedContains?.straight],
          ["rendered contains まわり足", debug.renderedContains?.turnMawari],
          ["rendered contains 回り足", debug.renderedContains?.turnMawariAlt],
          ["rendered contains 周足", debug.renderedContains?.turnShuashi]
        ].map(([label, value]) => (
          <div className="kv-row" key={`visible-kyotei-rendered-${label}`}>
            <span>{label}</span>
            <strong>{value ? "true" : "false"}</strong>
          </div>
        ))}
        {[
          ["base entries count", debug.baseEntriesCount],
          ["originalExhibition rows count", debug.originalExhibitionRowsCount],
          ["canonical lapTime count", debug.canonicalLapTimeCount],
          ["canonical straightTime count", debug.canonicalStraightTimeCount],
          ["canonical turnTime count", debug.canonicalTurnTimeCount],
          ["display lapTime count", debug.displayLapTimeCount],
          ["display straightTime count", debug.displayStraightTimeCount],
          ["display turnTime count", debug.displayTurnTimeCount],
          ["parsed laneStats count", debug.parsedLaneStatsCount]
        ].map(([label, value]) => (
          <div className="kv-row" key={`visible-kyotei-count-${label}`}>
            <span>{label}</span>
            <strong>{Number(value || 0)} / 6</strong>
          </div>
        ))}
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">originalExhibition rows preview</div>
        <pre className="json-preview">{safePrettyJson(debug.originalExhibitionRowsPreview)}</pre>
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">canonical preview</div>
        <pre className="json-preview">{safePrettyJson(debug.canonicalPreview)}</pre>
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">table preview</div>
        <pre className="json-preview">{safePrettyJson(debug.tablePreview)}</pre>
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">prediction input preview</div>
        <pre className="json-preview">{safePrettyJson(debug.predictionInputPreview)}</pre>
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">received originalExhibition response</div>
        <pre className="json-preview">{safePrettyJson(originalExhibition || null)}</pre>
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">laneStats preview</div>
        <pre className="json-preview">{safePrettyJson(debug.laneStatsPreview)}</pre>
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">display preview</div>
        <pre className="json-preview">{safePrettyJson(debug.displayPreview)}</pre>
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">network responses</div>
        <pre className="json-preview">{safePrettyJson(localSafeArray(debug.networkResponses).map((response) => ({
          index: response?.index,
          phase: response?.phase,
          status: response?.status,
          resource_type: response?.resource_type,
          body_length: response?.body_length,
          contains: response?.contains,
          url: response?.url,
          saved_path: response?.saved_path,
          error: response?.error
        })))}</pre>
      </div>
      <div style={{ marginTop: 10 }}>
        <div className="muted">network parse responses</div>
        <pre className="json-preview">{safePrettyJson(debug.networkParseResponses)}</pre>
      </div>
    </div>
  );
}
