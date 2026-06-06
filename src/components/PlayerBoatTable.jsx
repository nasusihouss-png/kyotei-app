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

function sampleStatusLabel(status) {
  if (status === "ok") return "十分";
  if (status === "small_sample") return "少";
  if (status === "very_small_sample") return "極少";
  if (status === "insufficient_history") return "不足";
  return "-";
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
    <div className="table-wrap premium-player-table-wrap compact-player-table-wrap">
      <table className="premium-player-table">
        <thead>
          <tr>
            <th>艇番</th>
            <th>進入</th>
            <th>選手名</th>
            <th>F</th>
            <th>展示ST</th>
            <th>展示タイム</th>
            <th>周回</th>
            <th>直線</th>
            <th>まわり足</th>
            <th>モーター2連率</th>
            <th>コース別件数</th>
            <th>サンプル状態</th>
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
                </td>
                <td>
                  <div className="player-name-cell">
                    <strong>{row?.name || row?.racerName || "-"}</strong>
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
                <td>{row?.courseSpecificLast6mRaceCount ?? row?.last6mRaceCount ?? "-"}</td>
                <td>{sampleStatusLabel(row?.sampleStatus ?? row?.playerTendency?.sampleStatus ?? row?.racerCourseStats?.sampleStatus)}</td>
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
