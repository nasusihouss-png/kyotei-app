function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function topLanesBy(rows, selector, count = 2, desc = true) {
  return [...rows]
    .map((r) => ({ lane: toNum(r?.racer?.lane, 0), value: selector(r) }))
    .filter((x) => Number.isFinite(x.lane) && Number.isFinite(x.value))
    .sort((a, b) => (desc ? b.value - a.value : a.value - b.value))
    .slice(0, count)
    .map((x) => x.lane);
}

function uniq(arr) {
  return Array.from(new Set(arr || []));
}

export function applyContenderSynergy(ranking) {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!rows.length) {
    return {
      ranking: rows,
      contenderSignals: {
        exhibition_top2: [],
        motor_top2: [],
        player_top2: [],
        entry_adv_top2: [],
        overlap_lanes: [],
        core_contenders: [],
        contender_concentration: 0
      }
    };
  }

  const exhibitionTop2 = topLanesBy(
    rows,
    (r) => {
      const rank = toNum(r?.features?.exhibition_rank, 99);
      const exTime = toNum(r?.racer?.exhibitionTime ?? r?.features?.exhibition_time, 9.99);
      return -(rank * 10 + exTime);
    },
    2,
    true
  );
  const motorTop2 = topLanesBy(
    rows,
    (r) => toNum(r?.features?.motor_total_score, 0),
    2,
    true
  );
  const playerTop2 = topLanesBy(
    rows,
    (r) => toNum(r?.score, 0),
    2,
    true
  );
  const entryAdvTop2 = topLanesBy(
    rows,
    (r) => toNum(r?.features?.entry_advantage_score, 0),
    2,
    true
  );

  const overlapLanes = exhibitionTop2.filter((l) => motorTop2.includes(l));
  const coreContenders = uniq([...exhibitionTop2, ...motorTop2, ...playerTop2, ...entryAdvTop2]).slice(0, 6);

  const adjusted = rows.map((row) => {
    const lane = toNum(row?.racer?.lane, 0);
    let bonus = 0;
    if (exhibitionTop2.includes(lane)) bonus += 7;
    if (motorTop2.includes(lane)) bonus += 5;
    if (playerTop2.includes(lane)) bonus += 3;
    if (entryAdvTop2.includes(lane)) bonus += 2;
    if (overlapLanes.includes(lane)) bonus += 4.5;

    const avgSt = toNum(row?.racer?.avgSt ?? row?.features?.avg_st, NaN);
    const exSt = toNum(row?.racer?.exhibitionSt ?? row?.features?.exhibition_st, NaN);
    if (Number.isFinite(avgSt) && avgSt > 0 && avgSt <= 0.15) bonus += 1.6;
    if (Number.isFinite(exSt) && exSt > 0 && exSt <= 0.14) bonus += 1.4;

    const score = Number((toNum(row?.score, 0) + bonus).toFixed(3));
    return {
      ...row,
      score,
      features: {
        ...(row?.features || {}),
        contender_bonus: Number(bonus.toFixed(3)),
        contender_core_flag: coreContenders.includes(lane) ? 1 : 0
      }
    };
  });

  adjusted.sort((a, b) => toNum(b?.score, 0) - toNum(a?.score, 0));
  const reranked = adjusted.map((row, idx) => ({
    ...row,
    rank: idx + 1
  }));
  const top3 = reranked.slice(0, 3).map((r) => toNum(r?.racer?.lane, 0));
  const inCore = top3.filter((lane) => coreContenders.includes(lane)).length;
  const concentration = Number(((inCore / 3) * 100).toFixed(2));

  return {
    ranking: reranked,
    contenderSignals: {
      exhibition_top2: exhibitionTop2,
      motor_top2: motorTop2,
      player_top2: playerTop2,
      entry_adv_top2: entryAdvTop2,
      overlap_lanes: overlapLanes,
      core_contenders: coreContenders,
      contender_concentration: concentration
    }
  };
}

