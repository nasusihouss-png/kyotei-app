import fs from "fs";
import path from "path";
import db from "./db.js";

function safeJsonParse(value, fallback = {}) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toFinishPosition(resultRow, lane) {
  if (!resultRow) return null;
  if (resultRow.finish_1 === lane) return 1;
  if (resultRow.finish_2 === lane) return 2;
  if (resultRow.finish_3 === lane) return 3;
  return 4;
}

function normalizeTrainingRow(snapshot, resultRow) {
  const lane = Number(snapshot.lane);
  const finishPosition = toFinishPosition(resultRow, lane);
  const top3_1 = resultRow?.finish_1 ?? null;
  const top3_2 = resultRow?.finish_2 ?? null;
  const top3_3 = resultRow?.finish_3 ?? null;
  const top3Combo =
    Number.isInteger(top3_1) && Number.isInteger(top3_2) && Number.isInteger(top3_3)
      ? `${top3_1}-${top3_2}-${top3_3}`
      : null;

  return {
    race_id: snapshot.race_id,
    lane,
    registration_no: snapshot.registration_no ?? null,
    name: snapshot.name ?? null,
    class: snapshot.class ?? null,
    prediction_score: snapshot.prediction_score ?? null,
    prediction_rank: snapshot.prediction_rank ?? null,
    predicted_top3_flag: snapshot.predicted_top3_flag ?? 0,
    ...safeJsonParse(snapshot.features_json, {}),
    label_finish_position: finishPosition,
    label_is_win: finishPosition === 1 ? 1 : 0,
    label_is_place2: finishPosition !== null && finishPosition <= 2 ? 1 : 0,
    label_is_top3: finishPosition !== null && finishPosition <= 3 ? 1 : 0,
    label_top3_1: top3_1,
    label_top3_2: top3_2,
    label_top3_3: top3_3,
    label_top3_combo: top3Combo
  };
}

export function exportTrainingRows(options = {}) {
  const { settledOnly = true } = options;

  const snapshots = db
    .prepare(
      `
      SELECT
        race_id,
        lane,
        registration_no,
        name,
        class,
        prediction_score,
        prediction_rank,
        predicted_top3_flag,
        features_json
      FROM feature_snapshots
      ORDER BY race_id, lane
    `
    )
    .all();

  const resultRows = db
    .prepare(
      `
      SELECT race_id, finish_1, finish_2, finish_3
      FROM results
    `
    )
    .all();

  const resultMap = new Map(resultRows.map((r) => [r.race_id, r]));

  const rows = snapshots
    .map((snapshot) => normalizeTrainingRow(snapshot, resultMap.get(snapshot.race_id)))
    .filter((row) => (settledOnly ? row.label_finish_position !== null : true));

  return rows;
}

export function exportTrainingRowsToJsonFile(filePath, options = {}) {
  const rows = exportTrainingRows(options);
  const abs = path.resolve(filePath);
  fs.writeFileSync(abs, JSON.stringify(rows, null, 2), "utf-8");
  return { filePath: abs, rowCount: rows.length };
}
