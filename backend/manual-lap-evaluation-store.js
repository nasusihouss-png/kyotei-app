import db from "./db.js";

const SCORE_FIELDS = [
  "straight_line_score",
  "turn_entry_score",
  "turn_exit_score",
  "acceleration_score",
  "stability_score"
];

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function normalizeMemo(value) {
  const text = String(value || "").trim();
  return text ? text.slice(0, 300) : null;
}

function normalizeOneLaneScore(raw) {
  const item = raw && typeof raw === "object" ? raw : {};
  const out = {};
  let filled = 0;
  let sum = 0;
  for (const field of SCORE_FIELDS) {
    const n = toInt(item[field], null);
    if (Number.isInteger(n)) {
      const v = clamp(0, 2, n);
      out[field] = v;
      filled += 1;
      sum += v;
    } else {
      out[field] = null;
    }
  }
  return {
    ...out,
    filled_count: filled,
    total_score: filled > 0 ? sum : null,
    quality_score: filled > 0 ? Number(((sum / (filled * 2)) * 100).toFixed(2)) : null
  };
}

function normalizeScores(scoresByLane) {
  const src = scoresByLane && typeof scoresByLane === "object" ? scoresByLane : {};
  const out = {};
  for (let lane = 1; lane <= 6; lane += 1) {
    out[String(lane)] = normalizeOneLaneScore(src[String(lane)] || src[lane]);
  }
  return out;
}

function buildSummary(normalized) {
  const lanes = Object.entries(normalized || {}).map(([lane, row]) => ({
    lane: Number(lane),
    quality_score: Number(row?.quality_score),
    filled_count: Number(row?.filled_count || 0)
  }));
  const scored = lanes.filter((x) => Number.isFinite(x.quality_score));
  const avg = scored.length
    ? Number((scored.reduce((acc, row) => acc + row.quality_score, 0) / scored.length).toFixed(2))
    : null;
  const top = scored.slice().sort((a, b) => b.quality_score - a.quality_score)[0] || null;
  const weak = scored.slice().sort((a, b) => a.quality_score - b.quality_score)[0] || null;
  return {
    scored_lane_count: scored.length,
    average_quality_score: avg,
    top_lane: top?.lane || null,
    weak_lane: weak?.lane || null
  };
}

export function getManualLapEvaluation(raceId) {
  const rid = String(raceId || "").trim();
  if (!rid) return null;
  const row = db
    .prepare(
      `
      SELECT race_id, score_scale, scores_json, race_memo, created_at, updated_at
      FROM manual_lap_exhibitions
      WHERE race_id = ?
    `
    )
    .get(rid);
  if (!row) return null;
  let parsed = {};
  try {
    parsed = JSON.parse(row.scores_json || "{}");
  } catch {
    parsed = {};
  }
  const scores_by_lane = normalizeScores(parsed);
  return {
    race_id: row.race_id,
    score_scale: row.score_scale || "0-2",
    scores_by_lane,
    race_memo: row.race_memo || "",
    summary: buildSummary(scores_by_lane),
    created_at: row.created_at || null,
    updated_at: row.updated_at || null
  };
}

export function saveManualLapEvaluation({ raceId, scoresByLane, raceMemo }) {
  const rid = String(raceId || "").trim();
  if (!rid) {
    throw new Error("raceId is required");
  }
  const normalized = normalizeScores(scoresByLane);
  const memo = normalizeMemo(raceMemo);
  db.prepare(
    `
    INSERT INTO manual_lap_exhibitions (
      race_id,
      score_scale,
      scores_json,
      race_memo,
      updated_at
    ) VALUES (
      @race_id,
      '0-2',
      @scores_json,
      @race_memo,
      CURRENT_TIMESTAMP
    )
    ON CONFLICT(race_id) DO UPDATE SET
      score_scale = excluded.score_scale,
      scores_json = excluded.scores_json,
      race_memo = excluded.race_memo,
      updated_at = CURRENT_TIMESTAMP
  `
  ).run({
    race_id: rid,
    scores_json: JSON.stringify(normalized),
    race_memo: memo
  });
  return getManualLapEvaluation(rid);
}

export function applyManualLapToRanking(ranking, manualLapEvaluation) {
  const rows = Array.isArray(ranking) ? ranking : [];
  const scores = manualLapEvaluation?.scores_by_lane || {};
  if (!rows.length || !scores || typeof scores !== "object") {
    return {
      ranking: rows,
      manualLapImpact: {
        enabled: false,
        applied_lane_count: 0,
        average_adjustment: 0,
        note: "manual_lap_unavailable"
      }
    };
  }

  const adjustedRows = rows.map((row) => {
    const lane = Number(row?.racer?.lane ?? row?.lane);
    const laneScore = scores[String(lane)] || null;
    const q = Number(laneScore?.quality_score);
    const quality = Number.isFinite(q) ? q : null;
    const adjustment = Number.isFinite(quality)
      ? Number((((quality - 50) / 50) * 6).toFixed(3))
      : 0;
    const baseScore = Number(row?.score || 0);
    const nextScore = Number((baseScore + adjustment).toFixed(3));
    return {
      ...row,
      score: nextScore,
      features: {
        ...(row?.features || {}),
        manual_lap_quality_score: quality,
        manual_lap_adjustment: adjustment
      }
    };
  });

  adjustedRows.sort((a, b) => Number(b?.score || 0) - Number(a?.score || 0));
  const normalizedRanking = adjustedRows.map((row, idx) => ({
    ...row,
    rank: idx + 1
  }));
  const applied = normalizedRanking.filter((row) =>
    Number.isFinite(Number(row?.features?.manual_lap_quality_score))
  );
  const avgAdj = applied.length
    ? Number(
        (
          applied.reduce((acc, row) => acc + Number(row?.features?.manual_lap_adjustment || 0), 0) / applied.length
        ).toFixed(3)
      )
    : 0;

  return {
    ranking: normalizedRanking,
    manualLapImpact: {
      enabled: applied.length > 0,
      applied_lane_count: applied.length,
      average_adjustment: avgAdj,
      note: applied.length > 0 ? "manual_lap_applied" : "manual_lap_empty"
    }
  };
}

