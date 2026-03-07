function safeInvRank(rank) {
  const r = Number(rank);
  if (!Number.isFinite(r) || r <= 0) return 0;
  return 1 / r;
}

function softmax(items, scoreKey = "strength") {
  if (!items.length) return [];
  const maxScore = Math.max(...items.map((x) => x[scoreKey]));
  const exps = items.map((x) => Math.exp(x[scoreKey] - maxScore));
  const sum = exps.reduce((a, b) => a + b, 0);
  return items.map((x, i) => ({
    ...x,
    p: sum > 0 ? exps[i] / sum : 0
  }));
}

function calcStrength(features) {
  const class_score = Number(features.class_score || 0);
  const motor_total_score = Number(features.motor_total_score || 0);
  const course_fit_score = Number(features.course_fit_score || 0);
  const exhibition_rank = safeInvRank(features.exhibition_rank);
  const st_rank = safeInvRank(features.st_rank);

  return (
    class_score * 0.35 +
    motor_total_score * 0.3 +
    course_fit_score * 0.15 +
    exhibition_rank * 0.1 +
    st_rank * 0.1
  );
}

export function runRaceSimulation(ranking, topN = 10) {
  const racers = (ranking || []).map((r) => ({
    lane: r?.racer?.lane,
    features: r?.features || {}
  })).filter((x) => Number.isInteger(x.lane));

  if (racers.length < 3) {
    return { top_combinations: [] };
  }

  const withStrength = racers.map((r) => ({
    ...r,
    strength: calcStrength(r.features)
  }));

  const firstDist = softmax(withStrength);
  const combos = [];

  for (const first of firstDist) {
    const rem1 = withStrength.filter((x) => x.lane !== first.lane);
    const secondDist = softmax(rem1);

    for (const second of secondDist) {
      const rem2 = rem1.filter((x) => x.lane !== second.lane);
      const thirdDist = softmax(rem2);

      for (const third of thirdDist) {
        const prob = first.p * second.p * third.p;
        combos.push({
          combo: `${first.lane}-${second.lane}-${third.lane}`,
          prob
        });
      }
    }
  }

  const top_combinations = combos
    .sort((a, b) => b.prob - a.prob)
    .slice(0, topN)
    .map((x) => ({
      combo: x.combo,
      prob: Number(x.prob.toFixed(4))
    }));

  return { top_combinations };
}
