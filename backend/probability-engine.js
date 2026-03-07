function softmax(items) {
  if (!items.length) return [];
  const mean = items.reduce((a, b) => a + b.score, 0) / items.length;
  const variance =
    items.reduce((a, b) => a + (b.score - mean) ** 2, 0) / items.length;
  const std = Math.sqrt(variance) || 1;
  const normalized = items.map((x) => ({
    ...x,
    nscore: (x.score - mean) / std
  }));
  const maxScore = Math.max(...normalized.map((x) => x.nscore));
  const exps = normalized.map((x) => Math.exp(x.nscore - maxScore));
  const sum = exps.reduce((a, b) => a + b, 0);
  return normalized.map((x, i) => ({
    ...x,
    p: sum > 0 ? exps[i] / sum : 0
  }));
}

export function estimateTrifectaProbabilities(ranking, topN = 10) {
  const base = (ranking || []).map((r) => ({
    lane: r?.racer?.lane,
    score: Number(r?.score ?? 0)
  }));

  const lanes = base.filter((x) => Number.isInteger(x.lane));
  if (lanes.length < 3) return [];

  const firstDist = softmax(lanes);
  const results = [];

  for (const first of firstDist) {
    const remainingAfterFirst = lanes.filter((x) => x.lane !== first.lane);
    const secondDist = softmax(remainingAfterFirst);

    for (const second of secondDist) {
      const remainingAfterSecond = remainingAfterFirst.filter((x) => x.lane !== second.lane);
      const thirdDist = softmax(remainingAfterSecond);

      for (const third of thirdDist) {
        const p = first.p * second.p * third.p;
        results.push({
          combo: `${first.lane}-${second.lane}-${third.lane}`,
          p
        });
      }
    }
  }

  return results
    .sort((a, b) => b.p - a.p)
    .slice(0, topN)
    .map((x) => ({
      combo: x.combo,
      p: Number(x.p.toFixed(4))
    }));
}
