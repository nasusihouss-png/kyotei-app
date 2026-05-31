export const DECISION_PATTERN_RULES = Object.freeze([
  Object.freeze({
    code: "nige",
    label: "逃げ",
    head: [1],
    partners: [2, 3, 4],
    representativeCombos: ["1-2-3", "1-2-4", "1-3-2"],
    characteristics: "1が先マイして内側で完結しやすい基本形",
    rivalShape: "2差し残り、3の差し返し",
    practicalMemo: "1が強い時は2と3の並び順を優先して詰める"
  }),
  Object.freeze({
    code: "two_sashi",
    label: "2差し",
    head: [2],
    partners: [1, 3, 4],
    representativeCombos: ["2-1-3", "2-1-4", "2-3-1"],
    characteristics: "2が壁を使って差し抜ける形",
    rivalShape: "1残り、3の攻め残り",
    practicalMemo: "1が甘くて2の壁性能が高い時にだけ強く買う"
  }),
  Object.freeze({
    code: "three_makuri",
    label: "3まくり",
    head: [3],
    partners: [4, 5, 2],
    representativeCombos: ["3-4-1", "3-4-2", "3-5-1"],
    characteristics: "3が外を叩いて主導権を取る形",
    rivalShape: "4の続き足、2の差し残り",
    practicalMemo: "3の展示気配と2の壁不在が両方見えた時に上げる"
  }),
  Object.freeze({
    code: "three_makuri_sashi",
    label: "3まくり差し",
    head: [3],
    partners: [2, 4, 5],
    representativeCombos: ["3-2-4", "3-4-2", "3-2-5"],
    characteristics: "3が内を絞って差し場を拾う形",
    rivalShape: "2残り、4展開差し",
    practicalMemo: "3の攻め足はあるが全速まくり一辺倒でない時に有効"
  }),
  Object.freeze({
    code: "four_makuri",
    label: "4まくり",
    head: [4],
    partners: [5, 3, 6],
    representativeCombos: ["4-5-1", "4-3-1", "4-5-3"],
    characteristics: "4カドから一気に攻め切る形",
    rivalShape: "5の続き足、3の差し残り",
    practicalMemo: "4が攻め切るなら5の連動も一緒に見る"
  }),
  Object.freeze({
    code: "four_makuri_sashi",
    label: "4まくり差し",
    head: [4],
    partners: [3, 5, 2],
    representativeCombos: ["4-3-5", "4-5-3", "4-2-5"],
    characteristics: "4が展開を拾って抜ける形",
    rivalShape: "3の攻め残り、2の差し残り",
    practicalMemo: "3が攻めて4が差す並びを意識して2着3着を組む"
  }),
  Object.freeze({
    code: "five_makuri_sashi",
    label: "5まくり差し",
    head: [5],
    partners: [4, 3, 6],
    representativeCombos: ["5-4-3", "5-3-4", "5-4-6"],
    characteristics: "外から展開だけ拾って抜ける高配当形",
    rivalShape: "4の攻め残り、6の連動",
    practicalMemo: "外伸びと中枠攻めが重なった時だけ候補化する"
  }),
  Object.freeze({
    code: "five_makuri",
    label: "5まくり",
    head: [5],
    partners: [6, 4, 3],
    representativeCombos: ["5-6-4", "5-4-6", "5-6-3"],
    characteristics: "5が全速で叩く波乱形",
    rivalShape: "6の連動、4の展開拾い",
    practicalMemo: "5頭は進入と外足が強い時だけ限定で扱う"
  }),
  Object.freeze({
    code: "lane6_head",
    label: "6頭",
    head: [6],
    partners: [5, 4, 3],
    representativeCombos: ["6-5-4", "6-4-5", "6-5-3"],
    characteristics: "相当な乱戦でだけ成立する大穴形",
    rivalShape: "5の連動、4の展開差し",
    practicalMemo: "6頭は乱戦・進入変化・外足優勢が揃った時だけ残す"
  })
]);

export function getDecisionPatternRule(code) {
  return DECISION_PATTERN_RULES.find((rule) => rule.code === code) || null;
}
