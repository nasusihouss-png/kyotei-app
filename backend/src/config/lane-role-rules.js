export const LANE_ROLE_RULES = Object.freeze({
  1: Object.freeze({
    lane: 1,
    mainWinPaths: ["逃げ", "イン残し"],
    conditions: ["先マイできる展示ST", "モーター貢献が平均以上"],
    dangerSigns: ["展示STで見劣る", "2の壁が強く差されやすい", "進入不安"],
    commonCombos: ["1-2-3", "1-2-4", "1-3-2"]
  }),
  2: Object.freeze({
    lane: 2,
    mainWinPaths: ["差し", "差し残り"],
    conditions: ["壁性能が高い", "1の行き足が弱い"],
    dangerSigns: ["3の攻め圧が強い", "展示で伸び負け"],
    commonCombos: ["2-1-3", "1-2-3", "1-2-4"]
  }),
  3: Object.freeze({
    lane: 3,
    mainWinPaths: ["まくり", "まくり差し"],
    conditions: ["攻め足が明確", "2の壁が薄い"],
    dangerSigns: ["2が残る", "4に展開を取られる"],
    commonCombos: ["3-2-4", "3-4-2", "1-3-4"]
  }),
  4: Object.freeze({
    lane: 4,
    mainWinPaths: ["4まくり", "4まくり差し", "展開差し"],
    conditions: ["カド攻め気配", "3の攻めに連動できる"],
    dangerSigns: ["展示で伸び不足", "外に包まれる"],
    commonCombos: ["4-3-5", "4-5-3", "1-4-3"]
  }),
  5: Object.freeze({
    lane: 5,
    mainWinPaths: ["5まくり差し", "外伸び一撃"],
    conditions: ["外足優勢", "中枠が攻めて展開が開く"],
    dangerSigns: ["1が強い", "進入で深くなる"],
    commonCombos: ["5-4-3", "5-3-4", "1-5-4"]
  }),
  6: Object.freeze({
    lane: 6,
    mainWinPaths: ["6頭大穴", "外伸び連動"],
    conditions: ["乱戦", "進入変化", "外伸び顕著"],
    dangerSigns: ["イン優勢", "足不足", "展開待ち"],
    commonCombos: ["6-5-4", "6-4-5", "1-6-4"]
  })
});

export function getLaneRoleRule(lane) {
  return LANE_ROLE_RULES[Number(lane)] || null;
}
