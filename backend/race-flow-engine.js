function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(min, max, value) {
  return Math.max(min, Math.min(max, value));
}

function softmax(values) {
  if (!values.length) return [];
  const max = Math.max(...values);
  const exps = values.map((v) => Math.exp(v - max));
  const sum = exps.reduce((a, b) => a + b, 0) || 1;
  return exps.map((v) => v / sum);
}

function laneRow(ranking, lane) {
  return (ranking || []).find((r) => toNum(r?.racer?.lane, 0) === lane) || {
    score: 0,
    features: {},
    racer: { lane }
  };
}

export function analyzeRaceFlow({ ranking, raceIndexes, racePattern, raceRisk, playerStartProfiles }) {
  const l1 = laneRow(ranking, 1);
  const l2 = laneRow(ranking, 2);
  const l3 = laneRow(ranking, 3);
  const l4 = laneRow(ranking, 4);

  const s1 = toNum(l1.score);
  const s2 = toNum(l2.score);
  const s3 = toNum(l3.score);
  const s4 = toNum(l4.score);

  const e1 = 7 - toNum(l1.features?.exhibition_rank, 6);
  const e2 = 7 - toNum(l2.features?.exhibition_rank, 6);
  const e3 = 7 - toNum(l3.features?.exhibition_rank, 6);
  const e4 = 7 - toNum(l4.features?.exhibition_rank, 6);
  const st1 = 7 - toNum(l1.features?.st_rank, 6);
  const st2 = 7 - toNum(l2.features?.st_rank, 6);
  const st3 = 7 - toNum(l3.features?.st_rank, 6);
  const st4 = 7 - toNum(l4.features?.st_rank, 6);
  const expSt1 = 7 - toNum(l1.features?.expected_actual_st_rank, 6);
  const expSt2 = 7 - toNum(l2.features?.expected_actual_st_rank, 6);
  const expSt3 = 7 - toNum(l3.features?.expected_actual_st_rank, 6);
  const expSt4 = 7 - toNum(l4.features?.expected_actual_st_rank, 6);
  const lap1 = 7 - toNum(l1.features?.lap_time_rank, 6);
  const lap2 = 7 - toNum(l2.features?.lap_time_rank, 6);
  const lap3 = 7 - toNum(l3.features?.lap_time_rank, 6);
  const lap4 = 7 - toNum(l4.features?.lap_time_rank, 6);
  const motor21 = toNum(l1.features?.motor2_rate, 0);
  const motor22 = toNum(l2.features?.motor2_rate, 0);
  const motor23 = toNum(l3.features?.motor2_rate, 0);
  const motor24 = toNum(l4.features?.motor2_rate, 0);
  const slit2 = toNum(l2.features?.slit_alert_flag, 0);
  const slit3 = toNum(l3.features?.slit_alert_flag, 0);
  const slit4 = toNum(l4.features?.slit_alert_flag, 0);
  const fHold1 = toNum(l1.features?.f_hold_bias_applied, 0);
  const fHold2 = toNum(l2.features?.f_hold_bias_applied, 0);
  const fHold3 = toNum(l3.features?.f_hold_bias_applied, 0);
  const fHold4 = toNum(l4.features?.f_hold_bias_applied, 0);
  const fHoldAdj1 = toNum(l1.features?.expected_actual_st_adjustment, 0);
  const fHoldAdj2 = toNum(l2.features?.expected_actual_st_adjustment, 0);
  const fHoldAdj3 = toNum(l3.features?.expected_actual_st_adjustment, 0);
  const fHoldAdj4 = toNum(l4.features?.expected_actual_st_adjustment, 0);
  const slitDelta2 = toNum(l2.features?.display_time_delta_vs_left, 0);
  const slitDelta3 = toNum(l3.features?.display_time_delta_vs_left, 0);
  const slitDelta4 = toNum(l4.features?.display_time_delta_vs_left, 0);
  const slitBoost2 = slit2 ? Math.min(0.22, 0.08 + slitDelta2 * 0.5) : 0;
  const slitBoost34 = [
    slit3 ? Math.min(0.24, 0.09 + slitDelta3 * 0.5) : 0,
    slit4 ? Math.min(0.24, 0.09 + slitDelta4 * 0.5) : 0
  ];
  const launchEdge1 = st1 * 0.9 + expSt1 * 1.05;
  const launchEdge2 = st2 * 0.95 + expSt2 * 1.1 + slitBoost2 * 18;
  const launchEdge3 = st3 * 0.95 + expSt3 * 1.12 + slitBoost34[0] * 18;
  const launchEdge4 = st4 * 0.92 + expSt4 * 1.08 + slitBoost34[1] * 18;

  const nigeIdx = toNum(raceIndexes?.nige_index, 50);
  const sashiIdx = toNum(raceIndexes?.sashi_index, 50);
  const makuriIdx = toNum(raceIndexes?.makuri_index, 50);
  const makurizashiIdx = toNum(raceIndexes?.makurizashi_index, 50);
  const chaosIdx = toNum(raceIndexes?.are_index ?? raceIndexes?.chaos_index, 50);
  const risk = toNum(raceRisk?.risk_score, 50);
  const p1 = playerStartProfiles?.by_lane?.["1"] || {};
  const p2 = playerStartProfiles?.by_lane?.["2"] || {};
  const p3 = playerStartProfiles?.by_lane?.["3"] || {};
  const p4 = playerStartProfiles?.by_lane?.["4"] || {};

  let nigeLogit =
    nigeIdx * 0.06 +
    (s1 - (s2 + s3) * 0.5) * 0.045 +
    (e1 + launchEdge1) * 0.17 -
    Math.max(0, launchEdge2 - launchEdge1) * 0.045 -
    Math.max(0, launchEdge3 - launchEdge1) * 0.03 -
    (chaosIdx - 50) * 0.03 +
    toNum(p1.nige_style_score, 50) * 0.006 +
    toNum(p1.start_stability_score, 50) * 0.005;
  nigeLogit -= fHold1 ? Math.min(0.18, 0.06 + fHoldAdj1 * 2.4) : 0;

  let sashiLogit =
    sashiIdx * 0.06 +
    (s2 - s1) * 0.035 +
    (e2 + launchEdge2) * 0.16 +
    Math.max(0, (launchEdge2 + e2) - (launchEdge1 + e1)) * 0.095 +
    Math.max(0, motor22 - motor21) * 0.003 +
    toNum(p2.sashi_style_score, 50) * 0.006 +
    toNum(p2.start_attack_score, 50) * 0.004 +
    slitBoost2;
  sashiLogit -= fHold2 ? Math.min(0.16, 0.05 + fHoldAdj2 * 2.1) : 0;

  let makuriLogit =
    makuriIdx * 0.06 +
    (Math.max(s3, s4) - Math.max(s1, s2)) * 0.03 +
    (e3 + launchEdge3) * 0.14 +
    (e4 + launchEdge4) * 0.085 +
    Math.max(lap3, lap4) * 0.05 +
    Math.max(motor23, motor24) * 0.002 +
    (toNum(p3.makuri_style_score, 50) + toNum(p4.makuri_style_score, 50)) * 0.003 +
    Math.max(...slitBoost34);
  makuriLogit -= Math.max(
    fHold3 ? Math.min(0.15, 0.04 + fHoldAdj3 * 2) : 0,
    fHold4 ? Math.min(0.15, 0.04 + fHoldAdj4 * 2) : 0
  );

  let makurizashiLogit =
    makurizashiIdx * 0.06 +
    ((s3 + s4) * 0.5 - s1) * 0.028 +
    ((e3 + launchEdge3 + e4 + launchEdge4) * 0.5) * 0.13 +
    ((lap3 + lap4) * 0.5) * 0.04 +
    (toNum(p3.start_attack_score, 50) + toNum(p4.start_attack_score, 50)) * 0.0025 +
    (slitBoost34[0] * 0.7 + slitBoost34[1] * 0.7);
  makurizashiLogit -=
    (fHold3 ? Math.min(0.12, 0.04 + fHoldAdj3 * 1.8) : 0) +
    (fHold4 ? Math.min(0.12, 0.04 + fHoldAdj4 * 1.8) : 0);

  let chaosLogit = chaosIdx * 0.06 + risk * 0.025;

  if (racePattern === "escape") nigeLogit += 0.42;
  if (racePattern === "sashi") sashiLogit += 0.42;
  if (racePattern === "makuri") makuriLogit += 0.4;
  if (racePattern === "makurizashi") makurizashiLogit += 0.4;
  if (racePattern === "chaos") chaosLogit += 0.46;

  const probs = softmax([nigeLogit, sashiLogit, makuriLogit, makurizashiLogit, chaosLogit]);
  const [nige, sashi, makuri, makurizashi, chaos] = probs;

  const sorted = [
    { key: "nige", p: nige },
    { key: "sashi", p: sashi },
    { key: "makuri", p: makuri },
    { key: "makurizashi", p: makurizashi },
    { key: "chaos", p: chaos }
  ].sort((a, b) => b.p - a.p);

  const race_flow_mode = sorted[0]?.key || "chaos";
  const flow_confidence = clamp(0, 1, toNum(sorted[0]?.p, 0) - toNum(sorted[1]?.p, 0) + 0.35);

  return {
    race_flow_mode,
    nige_prob: Number(toNum(nige, 0).toFixed(4)),
    sashi_prob: Number(toNum(sashi, 0).toFixed(4)),
    makuri_prob: Number(toNum(makuri, 0).toFixed(4)),
    makurizashi_prob: Number(toNum(makurizashi, 0).toFixed(4)),
    chaos_prob: Number(toNum(chaos, 0).toFixed(4)),
    flow_confidence: Number(flow_confidence.toFixed(4)),
    slit_alert_lanes: [slit2 ? 2 : null, slit3 ? 3 : null, slit4 ? 4 : null].filter(Boolean),
    f_hold_caution_lanes: [fHold1 ? 1 : null, fHold2 ? 2 : null, fHold3 ? 3 : null, fHold4 ? 4 : null].filter(Boolean),
    start_signal_debug: {
      by_lane: {
        1: { launch_edge: Number(launchEdge1.toFixed(2)), exhibition_rank_signal: e1, st_rank_signal: st1, expected_actual_st_signal: expSt1, lap_rank_signal: lap1, motor2_signal: motor21 },
        2: { launch_edge: Number(launchEdge2.toFixed(2)), exhibition_rank_signal: e2, st_rank_signal: st2, expected_actual_st_signal: expSt2, lap_rank_signal: lap2, motor2_signal: motor22 },
        3: { launch_edge: Number(launchEdge3.toFixed(2)), exhibition_rank_signal: e3, st_rank_signal: st3, expected_actual_st_signal: expSt3, lap_rank_signal: lap3, motor2_signal: motor23 },
        4: { launch_edge: Number(launchEdge4.toFixed(2)), exhibition_rank_signal: e4, st_rank_signal: st4, expected_actual_st_signal: expSt4, lap_rank_signal: lap4, motor2_signal: motor24 }
      },
      scenario_driver_scores: {
        boat1_escape: Number(nigeLogit.toFixed(3)),
        boat2_sashi: Number(sashiLogit.toFixed(3)),
        boat3_makuri: Number((makuriLogit + launchEdge3 * 0.03).toFixed(3)),
        boat3_makuri_sashi: Number((makurizashiLogit + launchEdge3 * 0.02).toFixed(3)),
        boat4_cado_attack: Number((makuriLogit + launchEdge4 * 0.04).toFixed(3))
      }
    }
  };
}
