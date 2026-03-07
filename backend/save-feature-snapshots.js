import db from "./db.js";

const upsertFeatureSnapshot = db.prepare(`
  INSERT INTO feature_snapshots (
    race_id,
    lane,
    registration_no,
    name,
    class,
    prediction_score,
    prediction_rank,
    predicted_top3_flag,
    features_json
  ) VALUES (
    @race_id,
    @lane,
    @registration_no,
    @name,
    @class,
    @prediction_score,
    @prediction_rank,
    @predicted_top3_flag,
    @features_json
  )
  ON CONFLICT(race_id, lane) DO UPDATE SET
    registration_no=excluded.registration_no,
    name=excluded.name,
    class=excluded.class,
    prediction_score=excluded.prediction_score,
    prediction_rank=excluded.prediction_rank,
    predicted_top3_flag=excluded.predicted_top3_flag,
    features_json=excluded.features_json
`);

const upsertManyTx = db.transaction((rows) => {
  for (const row of rows) {
    upsertFeatureSnapshot.run(row);
  }
});

export function saveFeatureSnapshots(raceId, ranking) {
  const rows = (Array.isArray(ranking) ? ranking : []).map((item) => {
    const lane = Number(item?.racer?.lane);
    return {
      race_id: raceId,
      lane: Number.isInteger(lane) ? lane : null,
      registration_no: item?.racer?.registrationNo ?? null,
      name: item?.racer?.name ?? null,
      class: item?.racer?.class ?? null,
      prediction_score: Number.isFinite(Number(item?.score)) ? Number(item.score) : null,
      prediction_rank: Number.isInteger(item?.rank) ? item.rank : null,
      predicted_top3_flag: Number.isInteger(item?.rank) && item.rank <= 3 ? 1 : 0,
      features_json: JSON.stringify(item?.features ?? {})
    };
  }).filter((row) => Number.isInteger(row.lane));

  if (rows.length === 0) return 0;
  upsertManyTx(rows);
  return rows.length;
}
