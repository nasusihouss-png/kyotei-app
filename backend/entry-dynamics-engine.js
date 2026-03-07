function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function round4(value) {
  return Number(value.toFixed(4));
}

function calcCourseChangeScore(f) {
  const lane = toNumber(f.lane, 0);
  const entryCourse = Number.isFinite(Number(f.entry_course))
    ? Number(f.entry_course)
    : lane;
  const delta = entryCourse - lane;

  if (delta === 0) return 0;

  // Moved inward
  if (delta < 0) {
    const steps = Math.abs(delta);
    if (lane >= 5) return 1.0 * steps;
    if (lane === 4) return 0.6 * steps;
    if (lane === 3) return 0.3 * steps;
    if (lane === 2) return -0.2 * steps;
    return 0;
  }

  // Moved outward
  if (f.is_inner === 1) return -1.2 * delta;
  if (f.is_outer === 1) return -0.3 * delta;
  return -0.7 * delta;
}

function calcKadoBonus(f, racePattern) {
  const entryCourse = Number.isFinite(Number(f.entry_course))
    ? Number(f.entry_course)
    : Number(f.lane || 0);
  if (entryCourse !== 4) return 0;

  const st = Number(f.exhibition_st);
  let bonus = 1.5;
  if (Number.isFinite(st) && st > 0) {
    if (st <= 0.12) bonus += 1.5;
    else if (st <= 0.18) bonus += 0.8;
    else bonus += 0.3;
  }

  if (racePattern === "makuri" || racePattern === "makurizashi") {
    bonus += 0.4;
  }

  return bonus;
}

function calcDeepInPenalty(f) {
  const lane = toNumber(f.lane, 0);
  const entryCourse = Number.isFinite(Number(f.entry_course))
    ? Number(f.entry_course)
    : lane;

  let penalty = 0;

  // Lane1 losing inside is a major negative.
  if (lane === 1 && entryCourse !== 1) {
    penalty -= 4;
  }

  // Outside boats forcing too deep inward can be unstable.
  if ((lane === 5 || lane === 6) && entryCourse <= 3) {
    penalty -= (4 - entryCourse) * 1.2;
  }

  return penalty;
}

function calcEntryChaosBonus(f, context) {
  const lane = toNumber(f.lane, 0);
  const entryCourse = Number.isFinite(Number(f.entry_course))
    ? Number(f.entry_course)
    : lane;
  const changed = entryCourse !== lane;

  if (!changed) return 0;

  let bonus = 0.6;
  if (context.racePattern === "chaos") bonus += 0.5;
  if ((context.chaos_index || 0) >= 65) bonus += 0.4;

  const st = Number(f.exhibition_st);
  if (changed && Number.isFinite(st) && st > 0 && st <= 0.12) {
    bonus += 0.3;
  }

  return bonus;
}

function computeChaosBoost(items) {
  const changedCount = items.filter((x) => x.changed).length;
  const inwardAggressive = items.filter(
    (x) => x.lane >= 4 && x.entryCourse <= 3 && x.changed
  ).length;
  const lane1Lost = items.some((x) => x.lane === 1 && x.entryCourse !== 1);

  let boost = changedCount * 2 + inwardAggressive * 1.5 + (lane1Lost ? 4 : 0);
  if (changedCount >= 3) boost += 3;

  return Math.min(20, Number(boost.toFixed(2)));
}

export function applyEntryDynamicsFeatures(
  racersWithFeatures,
  context = { racePattern: "standard", chaos_index: 50 }
) {
  const internal = [];

  const enriched = (racersWithFeatures || []).map((item) => {
    const f = item.features || {};
    const lane = toNumber(f.lane, 0);
    const entryCourse = Number.isFinite(Number(f.entry_course))
      ? Number(f.entry_course)
      : lane;

    const course_change_score = round4(calcCourseChangeScore(f));
    const kado_bonus = round4(calcKadoBonus(f, context.racePattern));
    const deep_in_penalty = round4(calcDeepInPenalty(f));
    const entry_chaos_bonus = round4(calcEntryChaosBonus(f, context));
    const entry_advantage_score = round4(
      course_change_score + kado_bonus + deep_in_penalty + entry_chaos_bonus
    );

    internal.push({
      lane,
      entryCourse,
      changed: entryCourse !== lane
    });

    return {
      ...item,
      features: {
        ...f,
        course_change_score,
        kado_bonus,
        deep_in_penalty,
        entry_chaos_bonus,
        entry_advantage_score
      }
    };
  });

  const chaosBoost = computeChaosBoost(internal);

  return {
    racersWithFeatures: enriched,
    chaosBoost
  };
}
