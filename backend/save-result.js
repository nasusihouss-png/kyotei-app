import db from "./db.js";
import { buildRaceIdFromParts, normalizeCombo, normalizeTop3OrNull } from "./result-utils.js";

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

export function fetchSavedRaceResult({ raceId = null, date = null, venueId = null, raceNo = null } = {}) {
  const normalizedRaceId =
    raceId ||
    buildRaceIdFromParts({
      date,
      venueId,
      raceNo
    });

  const row = normalizedRaceId
    ? db.prepare(
      `
        SELECT race_id, finish_1, finish_2, finish_3, payout_2t, payout_3t, decision_type
        FROM results
        WHERE race_id = ?
        LIMIT 1
      `
    ).get(normalizedRaceId)
    : (
      String(date || "").trim() &&
      Number.isInteger(toIntOrNull(venueId)) &&
      Number.isInteger(toIntOrNull(raceNo))
        ? db.prepare(
          `
            SELECT re.race_id, re.finish_1, re.finish_2, re.finish_3, re.payout_2t, re.payout_3t, re.decision_type
            FROM results re
            INNER JOIN races ra
              ON ra.race_id = re.race_id
            WHERE ra.race_date = ?
              AND ra.venue_id = ?
              AND ra.race_no = ?
            ORDER BY re.created_at DESC, re.rowid DESC
            LIMIT 1
          `
        ).get(String(date || "").trim(), toIntOrNull(venueId), toIntOrNull(raceNo))
        : null
    );

  const actualTop3 = normalizeTop3OrNull([row?.finish_1, row?.finish_2, row?.finish_3]);
  const winningTrifecta = actualTop3 ? normalizeCombo(actualTop3.join("-")) : "";
  if (!actualTop3 || !winningTrifecta) return null;

  return {
    raceId: row?.race_id || normalizedRaceId || null,
    actualTop3,
    winningTrifecta,
    actualResult: winningTrifecta,
    result: winningTrifecta,
    payout2t: toIntOrNull(row?.payout_2t),
    payout3t: toIntOrNull(row?.payout_3t),
    decisionType: row?.decision_type ?? null,
    source: "results"
  };
}
