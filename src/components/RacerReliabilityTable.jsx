function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function pct(value, formatMaybeNumber) {
  if (value === null || value === undefined || value === "") return "-";
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  return `${formatMaybeNumber(Math.abs(number) <= 1 ? number * 100 : number, 1)}%`;
}

function scorePct(value, formatMaybeNumber) {
  if (value === null || value === undefined || value === "") return "-";
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  return `${formatMaybeNumber(Math.abs(number) <= 1 ? number * 100 : number, 1)}`;
}

export default function RacerReliabilityTable({
  entries = [],
  prediction = null,
  formatMaybeNumber
}) {
  const scoreByBoat = prediction?.raceFlowScenario?.scoreByBoat || {};
  const rows = safeArray(entries).map((entry, idx) => {
    const boat = Number(entry?.boat ?? entry?.boatNumber ?? entry?.lane ?? idx + 1);
    const score = scoreByBoat[String(boat)] || scoreByBoat[boat] || {};
    const tendency = entry?.playerTendency || entry?.racerCourseStats || {};
    return {
      boat,
      name: entry?.racerName ?? entry?.name ?? "-",
      courseWinRate: entry?.courseWinRate ?? tendency?.courseWinRate ?? score?.courseWinRate ?? null,
      courseQuinellaRate: entry?.courseQuinellaRate ?? tendency?.courseQuinellaRate ?? score?.courseQuinellaRate ?? null,
      courseTrifectaRate: entry?.courseTrifectaRate ?? tendency?.courseTrifectaRate ?? score?.courseTrifectaRate ?? null,
      reliabilityScore: score?.reliabilityScore ?? entry?.reliabilityScore ?? null,
      wallScore: score?.wallScore ?? entry?.wallScore ?? null,
      blockingScore: score?.blockingScore ?? entry?.blockingScore ?? null,
      source: score?.reliabilityProfile?.source ?? "-"
    };
  });

  return (
    <details className="card practical-details">
      <summary>選手信頼度・壁評価</summary>
      <div className="table-wrap compact-table-wrap" style={{ marginTop: 10 }}>
        <table className="compact-data-table">
          <thead>
            <tr>
              <th>艇番</th>
              <th>選手</th>
              <th>コース別1着率</th>
              <th>コース別2連対率</th>
              <th>コース別3連対率</th>
              <th>信頼度</th>
              <th>壁スコア</th>
              <th>ブロックスコア</th>
              <th>信頼度ソース</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`reliability-${row.boat}`}>
                <td>{row.boat || "-"}</td>
                <td>{row.name}</td>
                <td>{pct(row.courseWinRate, formatMaybeNumber)}</td>
                <td>{pct(row.courseQuinellaRate, formatMaybeNumber)}</td>
                <td>{pct(row.courseTrifectaRate, formatMaybeNumber)}</td>
                <td>{scorePct(row.reliabilityScore, formatMaybeNumber)}</td>
                <td>{scorePct(row.wallScore, formatMaybeNumber)}</td>
                <td>{scorePct(row.blockingScore, formatMaybeNumber)}</td>
                <td>{row.source}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  );
}
