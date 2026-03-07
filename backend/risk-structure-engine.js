function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function cloneRisk(risk) {
  return {
    ...(risk || {}),
    skip_reason_codes: Array.isArray(risk?.skip_reason_codes) ? [...risk.skip_reason_codes] : []
  };
}

export function refineRaceRiskWithStructure({
  raceRisk,
  headConfidence,
  preRaceForm,
  roleCandidates,
  probabilities,
  ranking
}) {
  const next = cloneRisk(raceRisk);
  const riskScore = toNum(next.risk_score, 50);
  const headConf = toNum(headConfidence?.head_confidence, 0.5);
  const preRace = toNum(preRaceForm?.pre_race_form_score, 50);
  const fadeLanes = Array.isArray(roleCandidates?.fade_lanes) ? roleCandidates.fade_lanes : [];

  const probs = (Array.isArray(probabilities) ? probabilities : [])
    .map((x) => toNum(x?.p ?? x?.prob))
    .filter((x) => x > 0)
    .sort((a, b) => b - a);
  const top1 = probs[0] || 0;
  const top3 = (probs[0] || 0) + (probs[1] || 0) + (probs[2] || 0);

  const topRows = (Array.isArray(ranking) ? ranking : []).slice(0, 3);
  const outerTopCount = topRows.filter((r) => toNum(r?.racer?.lane) >= 5).length;
  const outerDependence = outerTopCount >= 2;

  const lowHeadConfidence = headConf < 0.54;
  const unstablePreRace = preRace < 52;
  const lowConcentration = top1 < 0.15 || top3 < 0.45;

  if (lowHeadConfidence) next.skip_reason_codes.push("HEAD_CONFIDENCE_LOW");
  if (unstablePreRace) next.skip_reason_codes.push("PRE_RACE_FORM_UNSTABLE");
  if (lowConcentration) next.skip_reason_codes.push("TOP3_CONCENTRATION_WEAK");
  if (outerDependence) next.skip_reason_codes.push("OUTER_LANE_DEPENDENCE");
  if (fadeLanes.length >= 3) next.skip_reason_codes.push("ROLE_SPREAD_UNCERTAIN");

  const warningCount = [
    lowHeadConfidence,
    unstablePreRace,
    lowConcentration,
    outerDependence,
    fadeLanes.length >= 3
  ].filter(Boolean).length;

  // Keep hit-rate focus: escalate caution before hard skip.
  if (next.recommendation === "FULL BET" && (warningCount >= 2 || riskScore > 58)) {
    next.recommendation = "SMALL BET";
  }
  if (riskScore > 88 && warningCount >= 4) {
    next.recommendation = "SKIP";
  }

  if (next.recommendation === "SKIP") {
    next.skip_summary = "頭信頼度・直前気配・展開集中度が弱く、見送り推奨";
  } else if (next.recommendation === "SMALL BET") {
    next.skip_summary = "警戒要素あり。点数を絞って小口運用";
  }

  return next;
}
