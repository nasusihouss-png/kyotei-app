import { buildTableDisplayPreview } from "../lib/race-data-merge.js";
import { getPlayerBoatTableRows } from "./player-boat-table-model.js";

function defaultFormatComparisonValue(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return num.toFixed(digits);
}

function safeSetHas(setLike, value) {
  return !!setLike && typeof setLike.has === "function" ? setLike.has(value) : false;
}

export { getPlayerBoatTableRows };

export default function PlayerBoatTable({
  entries = [],
  boatMeta = {},
  playerMetricLeaders = {},
  formatComparisonValue = defaultFormatComparisonValue
}) {
  const rows = getPlayerBoatTableRows(entries);
  return (
    <div className="table-wrap premium-player-table-wrap">
      <table className="premium-player-table">
        <thead>
          <tr>
            <th>艇番</th>
            <th>進入</th>
            <th>選手名</th>
            <th>F</th>
            <th>ST</th>
            <th>展示</th>
            <th>周回</th>
            <th>直線</th>
            <th>まわり足</th>
            <th>モーター2連率</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => {
            const boat = row?.boat ?? row?.boatNumber ?? row?.lane ?? idx + 1;
            const entryLane = Number.isFinite(Number(row?.entryLane)) ? Number(row.entryLane) : null;
            return (
              <tr key={`player-compare-${boat ?? idx}`}>
                <td>
                  <div className="player-boat-cell">
                    <span className={`combo-dot ${boatMeta[boat]?.className || ""}`}>{boat ?? "--"}</span>
                  </div>
                </td>
                <td>
                  <div className="player-boat-cell">
                    <span className={`combo-dot ${entryLane !== null ? (boatMeta[entryLane]?.className || "") : ""}`}>
                      {entryLane !== null ? entryLane : "?"}
                    </span>
                  </div>
                  <div className="muted" style={{ marginTop: 4 }}>
                    {row?.entry ?? row?.predictedEntry ?? "-"}
                    {row?.entryConfirmed ? "" : " / predicted"}
                  </div>
                </td>
                <td>
                  <div className="player-name-cell">
                    <strong>{row?.name || row?.racerName || "-"}</strong>
                    {row?.entryConfirmed
                      ? row?.courseChanged
                        ? <div className="muted">Moved from lane {boat} to entry {entryLane ?? "-"}</div>
                        : <div className="muted">No course change</div>
                      : <div className="muted">Entry not confirmed. Showing predicted entry while keeping lane order fixed at 1-6</div>}
                  </div>
                </td>
                <td>
                  <span className={`f-count-badge ${Number(row?.fCount) > 0 ? "has-f" : ""}`}>F{row?.fCount ?? "--"}</span>
                </td>
                <td className={safeSetHas(playerMetricLeaders?.exhibitionSt, row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.exST ?? row?.exhibitionSt, 2)}</td>
                <td className={safeSetHas(playerMetricLeaders?.exhibitionTime, row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.exTime ?? row?.exhibitionTime, 2)}</td>
                <td className={safeSetHas(playerMetricLeaders?.lapTime, row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.lapTime, 2)}</td>
                <td className={safeSetHas(playerMetricLeaders?.straightTime, row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.straightTime, 2)}</td>
                <td className={safeSetHas(playerMetricLeaders?.turnTime, row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.turnTime, 2)}</td>
                <td className={safeSetHas(playerMetricLeaders?.motor2Rate, row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.motor2ren ?? row?.motor2Rate, 2)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function buildPlayerBoatTablePreview(entries = []) {
  return buildTableDisplayPreview(entries);
}
