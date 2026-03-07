function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function rankQuality(rank, maxRank = 6) {
  const r = toNum(rank, maxRank);
  return clamp(0, 1, (maxRank + 1 - r) / maxRank);
}

function normalize(values) {
  const arr = (Array.isArray(values) ? values : []).map((x) => toNum(x, 0));
  if (!arr.length) return [];
  const min = Math.min(...arr);
  const max = Math.max(...arr);
  if (max === min) return arr.map(() => 0.5);
  return arr.map((v) => (v - min) / (max - min));
}

export function evaluatePartnerPrecision({ ranking, headSelection, raceFlow, playerStartProfile }) {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!rows.length) {
    return {
      second_candidates: [],
      third_candidates: [],
      second_place_fit_score: 0,
      third_place_fit_score: 0,
      residual_lane_score: 0,
      outside_third_score: 0,
      residual_inside_lanes: [],
      outside_third_lanes: []
    };
  }

  const flowMode = String(raceFlow?.race_flow_mode || "");
  const mainHead = toNum(headSelection?.main_head, 0);
  const scoreNorm = normalize(rows.map((r) => toNum(r?.score, 0)));

  const laneRows = rows.map((row, idx) => {
    const lane = toNum(row?.racer?.lane, 0);
    const f = row?.features || {};
    const sp = playerStartProfile?.by_lane?.[String(lane)] || {};
    const stQ = rankQuality(f.st_rank);
    const exQ = rankQuality(f.exhibition_rank);
    const entryAdv = toNum(f.entry_advantage_score, 0);
    const motor = toNum(f.motor_total_score, 0);
    const trend = toNum(f.motor_trend_score, 0);
    const courseFit = toNum(f.course_fit_score, 0);
    const isInner = lane <= 3 ? 1 : 0;
    const isOuter = lane >= 4 ? 1 : 0;

    let secondFit =
      scoreNorm[idx] * 34 +
      stQ * 21 +
      exQ * 14 +
      Math.max(-2, entryAdv) * 2.2 +
      motor * 0.75 +
      trend * 0.65 +
      courseFit * 0.42 +
      toNum(sp.start_attack_score, 50) * 0.11;

    let thirdFit =
      scoreNorm[idx] * 22 +
      stQ * 15 +
      exQ * 20 +
      Math.max(-2, entryAdv) * 1.8 +
      motor * 0.6 +
      trend * 0.75 +
      courseFit * 0.35 +
      toNum(sp.makuri_style_score, 50) * 0.12;

    if (lane === mainHead) {
      secondFit -= 16;
      thirdFit -= 20;
    }
    if (isInner) secondFit += 4;
    if (isOuter) thirdFit += 6;
    if (flowMode === "nige") {
      if (lane === 2 || lane === 3) secondFit += 8;
      if (lane >= 4) thirdFit += 4;
    } else if (flowMode === "sashi") {
      if (lane === 1 || lane === 3) secondFit += 6;
      if (lane === 4 || lane === 5) thirdFit += 5;
    } else if (flowMode === "makuri" || flowMode === "makurizashi") {
      if (lane === 2 || lane === 3 || lane === 4) secondFit += 5;
      if (lane >= 4) thirdFit += 8;
    }

    return {
      lane,
      secondFit: clamp(0, 100, secondFit),
      thirdFit: clamp(0, 100, thirdFit),
      isInner,
      isOuter
    };
  });

  const second_candidates = [...laneRows]
    .sort((a, b) => b.secondFit - a.secondFit)
    .slice(0, 4)
    .map((x) => x.lane);
  const third_candidates = [...laneRows]
    .sort((a, b) => b.thirdFit - a.thirdFit)
    .slice(0, 4)
    .map((x) => x.lane);

  const residual_inside_lanes = [...laneRows]
    .filter((x) => x.isInner && x.lane !== mainHead)
    .sort((a, b) => (b.secondFit + b.thirdFit) / 2 - (a.secondFit + a.thirdFit) / 2)
    .slice(0, 2)
    .map((x) => x.lane);
  const outside_third_lanes = [...laneRows]
    .filter((x) => x.isOuter && x.thirdFit >= 56)
    .sort((a, b) => b.thirdFit - a.thirdFit)
    .map((x) => x.lane);

  const second_place_fit_score = clamp(
    0,
    100,
    laneRows.reduce((a, b) => a + b.secondFit, 0) / Math.max(1, laneRows.length)
  );
  const third_place_fit_score = clamp(
    0,
    100,
    laneRows.reduce((a, b) => a + b.thirdFit, 0) / Math.max(1, laneRows.length)
  );
  const residual_lane_score = clamp(
    0,
    100,
    laneRows
      .filter((x) => x.isInner)
      .reduce((a, b) => a + (b.secondFit * 0.6 + b.thirdFit * 0.4), 0) / Math.max(1, laneRows.filter((x) => x.isInner).length)
  );
  const outside_third_score = clamp(
    0,
    100,
    laneRows.filter((x) => x.isOuter).reduce((a, b) => a + b.thirdFit, 0) / Math.max(1, laneRows.filter((x) => x.isOuter).length)
  );

  return {
    second_candidates,
    third_candidates,
    second_place_fit_score: Number(second_place_fit_score.toFixed(2)),
    third_place_fit_score: Number(third_place_fit_score.toFixed(2)),
    residual_lane_score: Number(residual_lane_score.toFixed(2)),
    outside_third_score: Number(outside_third_score.toFixed(2)),
    residual_inside_lanes,
    outside_third_lanes
  };
}
