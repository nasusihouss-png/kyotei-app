import db from "./db.js";

const upsertResult = db.prepare(`
  INSERT INTO results (
    race_id,
    finish_1,
    finish_2,
    finish_3,
    payout_2t,
    payout_3t,
    decision_type
  ) VALUES (
    @race_id,
    @finish_1,
    @finish_2,
    @finish_3,
    @payout_2t,
    @payout_3t,
    @decision_type
  )
  ON CONFLICT(race_id) DO UPDATE SET
    finish_1=excluded.finish_1,
    finish_2=excluded.finish_2,
    finish_3=excluded.finish_3,
    payout_2t=excluded.payout_2t,
    payout_3t=excluded.payout_3t,
    decision_type=excluded.decision_type
`);

function toIntOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

export function saveRaceResult({
  raceId,
  finishOrder,
  payout2t = null,
  payout3t = null,
  decisionType = null
}) {
  if (!raceId) {
    throw {
      statusCode: 400,
      code: "invalid_race_id",
      message: "raceId is required"
    };
  }

  if (!Array.isArray(finishOrder) || finishOrder.length < 3) {
    throw {
      statusCode: 400,
      code: "invalid_finish_order",
      message: "finishOrder must include at least top 3 lanes"
    };
  }

  upsertResult.run({
    race_id: raceId,
    finish_1: toIntOrNull(finishOrder[0]),
    finish_2: toIntOrNull(finishOrder[1]),
    finish_3: toIntOrNull(finishOrder[2]),
    payout_2t: toIntOrNull(payout2t),
    payout_3t: toIntOrNull(payout3t),
    decision_type: decisionType ?? null
  });

  return raceId;
}
