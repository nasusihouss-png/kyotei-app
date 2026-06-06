function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function pct(value, formatMaybeNumber) {
  if (value === null || value === undefined || value === "") return "-";
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  return `${formatMaybeNumber(Math.abs(number) <= 1 ? number * 100 : number, 1)}%`;
}

function sampleLabel(status) {
  if (status === "ok") return "十分";
  if (status === "small_sample") return "少サンプル";
  if (status === "very_small_sample") return "極少サンプル";
  if (status === "insufficient_history") return "履歴不足";
  return "-";
}

export default function RacerTendencyTable({
  rows = [],
  historySource = "-",
  formatMaybeNumber
}) {
  return (
    <details className="card practical-details">
      <summary>選手別・コース別 戦法データ</summary>
      <div className="table-wrap compact-table-wrap" style={{ marginTop: 10 }}>
        <table className="compact-data-table">
          <thead>
            <tr>
              <th>艇</th>
              <th>選手</th>
              <th>コース別件数</th>
              <th>全コース件数</th>
              <th>サンプル状態</th>
              <th>照合</th>
              <th>履歴ソース</th>
              <th>逃げ率</th>
              <th>差され率</th>
              <th>まくられ率</th>
              <th>まくり差され率</th>
              <th>差し率</th>
              <th>まくり率</th>
              <th>まくり差し率</th>
            </tr>
          </thead>
          <tbody>
            {safeArray(rows).map((row, idx) => {
              const tendency = row?.playerTendency || row?.racerCourseStats || {};
              const valueOf = (field) => row?.[field] ?? tendency?.[field] ?? null;
              return (
                <tr key={`tendency-compact-${row?.boatNumber ?? row?.boat ?? row?.lane ?? idx}`}>
                  <td>{row?.boatNumber ?? row?.boat ?? row?.lane ?? "-"}</td>
                  <td>{row?.racerName ?? row?.name ?? "-"}</td>
                  <td>{valueOf("courseSpecificLast6mRaceCount") ?? valueOf("last6mRaceCount") ?? "-"}</td>
                  <td>{valueOf("allCourseLast6mRaceCount") ?? "-"}</td>
                  <td>{sampleLabel(valueOf("sampleStatus"))}</td>
                  <td>{valueOf("matchMethod") ?? tendency?.debug?.matchMethod ?? "-"}</td>
                  <td>{historySource}</td>
                  <td>{pct(valueOf("escapeRate"), formatMaybeNumber)}</td>
                  <td>{pct(valueOf("beatenBySashiRate"), formatMaybeNumber)}</td>
                  <td>{pct(valueOf("beatenByMakuriRate"), formatMaybeNumber)}</td>
                  <td>{pct(valueOf("beatenByMakuriSashiRate"), formatMaybeNumber)}</td>
                  <td>{pct(valueOf("sashiRate"), formatMaybeNumber)}</td>
                  <td>{pct(valueOf("makuriRate"), formatMaybeNumber)}</td>
                  <td>{pct(valueOf("makuriSashiRate"), formatMaybeNumber)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </details>
  );
}
