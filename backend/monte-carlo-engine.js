function softmax(values) {
  if (!values.length) return [];
  const max = Math.max(...values);
  const exps = values.map((v) => Math.exp(v - max));
  const sum = exps.reduce((a, b) => a + b, 0) || 1;
  return exps.map((v) => v / sum);
}

function weightedPick(items, weights) {
  let r = Math.random();
  for (let i = 0; i < items.length; i += 1) {
    r -= weights[i];
    if (r <= 0) return i;
  }
  return items.length - 1;
}

function normalizeSimulationCount(simulationsInput) {
  const n = Number(simulationsInput);
  if (!Number.isFinite(n)) return 8000;
  return Math.max(5000, Math.min(10000, Math.trunc(n)));
}

function buildBasePool(ranking) {
  const base = (ranking || [])
    .map((r) => ({
      lane: Number(r?.racer?.lane),
      score: Number(r?.score ?? 0)
    }))
    .filter((x) => Number.isInteger(x.lane));

  if (base.length < 3) return [];

  const mean = base.reduce((a, b) => a + b.score, 0) / base.length;
  const variance = base.reduce((a, b) => a + (b.score - mean) ** 2, 0) / base.length;
  const std = Math.sqrt(variance) || 1;

  const logits = base.map((x) => (x.score - mean) / std);
  const probs = softmax(logits);

  return base.map((x, idx) => ({
    lane: x.lane,
    baseProb: probs[idx]
  }));
}

function sampleTrifecta(pool) {
  const rem = [...pool];
  const picked = [];

  for (let pos = 0; pos < 3; pos += 1) {
    const weightsRaw = rem.map((x) => x.baseProb);
    const weightSum = weightsRaw.reduce((a, b) => a + b, 0) || 1;
    const weights = weightsRaw.map((w) => w / weightSum);

    const idx = weightedPick(rem, weights);
    picked.push(rem[idx].lane);
    rem.splice(idx, 1);
  }

  return `${picked[0]}-${picked[1]}-${picked[2]}`;
}

export function simulateTrifectaProbabilities(ranking, options = {}) {
  const topN = Number.isInteger(options?.topN) ? options.topN : 10;
  const simulations = normalizeSimulationCount(options?.simulations);
  const pool = buildBasePool(ranking);

  if (pool.length < 3) {
    return {
      simulations,
      probabilities: [],
      top_combinations: []
    };
  }

  const counts = new Map();
  for (let i = 0; i < simulations; i += 1) {
    const combo = sampleTrifecta(pool);
    counts.set(combo, (counts.get(combo) || 0) + 1);
  }

  const all = [...counts.entries()]
    .map(([combo, count]) => ({
      combo,
      p: count / simulations
    }))
    .sort((a, b) => b.p - a.p);

  const probabilities = all.slice(0, topN).map((x) => ({
    combo: x.combo,
    p: Number(x.p.toFixed(4))
  }));

  const top_combinations = probabilities.map((x) => ({
    combo: x.combo,
    prob: x.p
  }));

  return {
    simulations,
    probabilities,
    top_combinations
  };
}
