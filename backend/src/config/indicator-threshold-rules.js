export const INDICATOR_THRESHOLD_RULES = Object.freeze({
  boat1_strength: Object.freeze({
    label: "1号艇の強弱",
    unit: "%",
    strong: 55,
    caution: 42,
    guide: "頭率55%以上で強気、42%未満は逃げ固定を慎重に"
  }),
  sashi_alert: Object.freeze({
    label: "差し率警戒",
    unit: "pt",
    strong: 62,
    caution: 50,
    guide: "2差し系が60超なら差し残り警戒"
  }),
  makurizashi_alert: Object.freeze({
    label: "まくり差し警戒",
    unit: "pt",
    strong: 60,
    caution: 48,
    guide: "3-4の展開差し系が60超で中穴警戒"
  }),
  display_st_gap: Object.freeze({
    label: "展示ST差",
    unit: "s",
    strong: 0.04,
    caution: 0.02,
    guide: "0.04以上の差は直前気配差として強く反映"
  }),
  lap_foot_gap: Object.freeze({
    label: "周回 / 周足 / 伸び差",
    unit: "pt",
    strong: 10,
    caution: 5,
    guide: "差が10pt以上なら足差を明確とみなす"
  }),
  motor_contribution: Object.freeze({
    label: "モーター貢献P",
    unit: "pt",
    strong: 60,
    caution: 48,
    guide: "60以上は機力支えあり"
  }),
  lane2_wall: Object.freeze({
    label: "2の壁性能",
    unit: "pt",
    strong: 58,
    caution: 48,
    guide: "58以上なら差し残りの壁として信頼"
  }),
  entry_risk: Object.freeze({
    label: "進入変化リスク",
    unit: "pt",
    strong: 60,
    caution: 35,
    guide: "高いほど進入由来の不確実性が強い"
  })
});
