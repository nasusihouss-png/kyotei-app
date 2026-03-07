function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function qualityFromRank(rank, maxRank = 6) {
  if (!Number.isFinite(rank) || rank <= 0) return 0.5;
  return clamp(0, 1, (maxRank + 1 - rank) / maxRank);
}

export function analyzePreRaceForm({ ranking, race }) {
  const rows = Array.isArray(ranking) ? ranking : [];
  if (!rows.length) {
    return {
      exhibition_quality_score: 0,
      entry_advantage_score: 0,
      pre_race_form_score: 0
    };
  }

  const wind = toNum(race?.windSpeed, 0);
  const wave = toNum(race?.waveHeight, 0);

  const perLane = rows.map((row) => {
    const f = row?.features || {};
    const lane = toNum(row?.racer?.lane, 0);
    const exQ = qualityFromRank(toNum(f.exhibition_rank, NaN));
    const stQ = qualityFromRank(toNum(f.st_rank, NaN));
    const exGapPenalty = Math.max(0, toNum(f.exhibition_gap_from_best, 0) * 36);
    const entryAdv = toNum(f.entry_advantage_score, 0);
    const kadoBonus = toNum(f.kado_bonus, 0);
    const deepPenalty = Math.abs(Math.min(0, toNum(f.deep_in_penalty, 0)));

    const windWavePenalty =
      lane >= 5 ? (wind >= 6 ? 8 : 0) + (wave >= 4 ? 5 : 0) : 0;

    const exhibition_quality = clamp(
      0,
      100,
      exQ * 52 + stQ * 44 - exGapPenalty
    );

    const entry_quality = clamp(
      0,
      100,
      52 + entryAdv * 11 + kadoBonus * 6 - deepPenalty * 9 - windWavePenalty
    );

    const preRaceForm = clamp(
      0,
      100,
      exhibition_quality * 0.58 + entry_quality * 0.42
    );

    return {
      lane,
      exhibition_quality_score: Number(exhibition_quality.toFixed(2)),
      entry_advantage_score: Number(entry_quality.toFixed(2)),
      pre_race_form_score: Number(preRaceForm.toFixed(2))
    };
  });

  const avg = (key) =>
    perLane.reduce((a, b) => a + toNum(b[key]), 0) / Math.max(1, perLane.length);

  const wind_risk_score = clamp(
    0,
    100,
    wind * 7.5 + wave * 9 + (wind >= 6 ? 10 : 0) + (wave >= 5 ? 8 : 0)
  );

  const exhibition_quality_score = Number(avg("exhibition_quality_score").toFixed(2));
  const entry_advantage_score = Number(avg("entry_advantage_score").toFixed(2));
  const pre_race_form_score = Number(avg("pre_race_form_score").toFixed(2));

  let summary = "直前気配は標準";
  if (pre_race_form_score >= 70 && wind_risk_score < 45) summary = "直前気配は良好、内寄り重視で狙える";
  else if (pre_race_form_score < 55 || wind_risk_score >= 65) summary = "直前気配に不安、点数を絞って慎重運用";

  return {
    exhibition_quality_score,
    entry_advantage_score,
    pre_race_form_score,
    wind_risk_score: Number(wind_risk_score.toFixed(2)),
    summary,
    per_lane: perLane
  };
}
