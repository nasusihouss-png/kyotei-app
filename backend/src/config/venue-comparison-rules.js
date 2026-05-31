const DEFAULT_COMPARISON_STEPS = Object.freeze([
  { key: "boat1_strength", label: "1号艇の強弱", benchmark: "頭率55%以上なら軸寄り", reason: "的中率重視では最初に1の逃げ可否を切るため" },
  { key: "lane2_wall", label: "2の壁性能", benchmark: "2着残し55前後が目安", reason: "差し残りと1-2本線の整理に直結するため" },
  { key: "lane34_attack", label: "3-4の攻め筋", benchmark: "攻め指標60以上で警戒", reason: "2-3-4クラスタの読み違いを減らすため" },
  { key: "display_gap", label: "展示ST / 周回差", benchmark: "差が明確なら機力優先", reason: "直前気配で逆転候補を拾うため" }
]);

const VENUE_RULES = Object.freeze({
  24: Object.freeze({
    venueId: 24,
    venueName: "大村",
    type: "inside_stable",
    baseStance: "1軸先行。逃げ残りを崩すより、2-3の残り順を丁寧に比較する",
    predictionAxis: "イン有利の中で2-3-4の残り順を詰める",
    comparisonOrder: ["boat1_strength", "lane2_wall", "display_gap", "lane34_attack"],
    emphasis: [
      { label: "1号艇の逃げ信頼度", reason: "内の押し切りが崩れにくい" },
      { label: "2号艇の差し残り", reason: "本線の2着候補として残りやすい" },
      { label: "3号艇の差し返し", reason: "2と比較して差が小さい時に1-3-4救済が必要" }
    ],
    compareFocus: DEFAULT_COMPARISON_STEPS
  }),
  18: Object.freeze({
    venueId: 18,
    venueName: "徳山",
    type: "attack_balance",
    baseStance: "1中心は維持しつつ、3-4の攻め残りを早めに警戒する",
    predictionAxis: "1残りと3-4攻めの両立を比較する",
    comparisonOrder: ["boat1_strength", "lane34_attack", "lane2_wall", "display_gap"],
    emphasis: [
      { label: "3号艇のまくり / まくり差し", reason: "攻め筋が2着3着まで残りやすい" },
      { label: "4号艇の展開差し", reason: "3が攻めた時の残り目として有効" },
      { label: "2号艇の壁性能", reason: "差し場を作るか止めるかで本線が変わる" }
    ],
    compareFocus: DEFAULT_COMPARISON_STEPS
  }),
  16: Object.freeze({
    venueId: 16,
    venueName: "児島",
    type: "turn_balance",
    baseStance: "機力差と旋回足を優先し、無理に波乱へ寄せない",
    predictionAxis: "ターン回りの優劣で残り順を詰める",
    comparisonOrder: ["display_gap", "boat1_strength", "lane2_wall", "lane34_attack"],
    emphasis: [
      { label: "周回 / 周足", reason: "ターン後の残り方に差が出やすい" },
      { label: "1号艇の行き足", reason: "逃げ切りか差されるかの分岐点" },
      { label: "2-3の比較", reason: "差し残りか攻め残りかを分ける" }
    ],
    compareFocus: DEFAULT_COMPARISON_STEPS
  }),
  1: Object.freeze({
    venueId: 1,
    venueName: "桐生",
    type: "balanced_night",
    baseStance: "イン有利を基礎にしつつ、展示気配差をしっかり反映する",
    predictionAxis: "1軸か差し抜けかを展示差で補正する",
    comparisonOrder: ["display_gap", "boat1_strength", "lane2_wall", "lane34_attack"],
    emphasis: [
      { label: "展示ST差", reason: "直前気配で1の信頼度が変わりやすい" },
      { label: "周回差", reason: "ターン出口の押し返しを見たい" },
      { label: "2号艇の差し", reason: "本線の差し残り候補" }
    ],
    compareFocus: DEFAULT_COMPARISON_STEPS
  }),
  19: Object.freeze({
    venueId: 19,
    venueName: "下関",
    type: "center_alert",
    baseStance: "1残りを基準に、3-4の攻めが見える時だけ波乱側を足す",
    predictionAxis: "イン残りと中穴攻めの境目を見極める",
    comparisonOrder: ["boat1_strength", "lane34_attack", "display_gap", "lane2_wall"],
    emphasis: [
      { label: "3号艇の攻め足", reason: "まくり差しに直結しやすい" },
      { label: "4号艇の展開利", reason: "差し場を拾うと2着3着に残る" },
      { label: "1号艇のスタート", reason: "先マイできるかで全体像が変わる" }
    ],
    compareFocus: DEFAULT_COMPARISON_STEPS
  }),
  5: Object.freeze({
    venueId: 5,
    venueName: "多摩川",
    type: "mid_chaos",
    baseStance: "中枠の残り目を消しすぎず、気配差で絞る",
    predictionAxis: "1残り前提でも2-3-4の横並びを丁寧に比較する",
    comparisonOrder: ["lane34_attack", "display_gap", "boat1_strength", "lane2_wall"],
    emphasis: [
      { label: "2-3-4クラスタ", reason: "2着が横並びになりやすい" },
      { label: "展示気配差", reason: "横並びの中で優先順位を付ける" },
      { label: "1号艇の安定感", reason: "逃げ切り信頼が低い時だけ波乱へ寄せる" }
    ],
    compareFocus: DEFAULT_COMPARISON_STEPS
  }),
  2: Object.freeze({
    venueId: 2,
    venueName: "戸田",
    type: "entry_sensitive",
    baseStance: "進入とスタート差を最優先し、1固定を急がない",
    predictionAxis: "進入変化と差し場の有無を先に見る",
    comparisonOrder: ["entry_risk", "display_gap", "lane2_wall", "boat1_strength"],
    emphasis: [
      { label: "進入変化リスク", reason: "並び次第で本線が変わりやすい" },
      { label: "2号艇の差し足", reason: "1を崩す本命筋になりやすい" },
      { label: "1号艇の強弱", reason: "強い時だけ逃げ固定へ寄せる" }
    ],
    compareFocus: [
      ...DEFAULT_COMPARISON_STEPS,
      { key: "entry_risk", label: "進入変化リスク", benchmark: "変化ありなら慎重", reason: "コース変化の影響を強く受けやすい" }
    ]
  }),
  12: Object.freeze({
    venueId: 12,
    venueName: "住之江",
    type: "attack_balance",
    baseStance: "1-2本線を残しつつ、3-4の攻め筋を厚めに比較する",
    predictionAxis: "1の残りと3-4の攻め残りを両にらみで見る",
    comparisonOrder: ["lane34_attack", "boat1_strength", "lane2_wall", "display_gap"],
    emphasis: [
      { label: "3-4の攻め圧", reason: "中心に攻め足があると順番が崩れやすい" },
      { label: "2号艇の壁", reason: "攻めを止めるか残すかの境界" },
      { label: "1号艇の逃げ質", reason: "押し切りか残り目までかを分ける" }
    ],
    compareFocus: DEFAULT_COMPARISON_STEPS
  })
});

export function getVenueComparisonRule(venueId) {
  const numericVenueId = Number(venueId) || 0;
  return VENUE_RULES[numericVenueId] || {
    venueId: numericVenueId || null,
    venueName: null,
    type: "balanced",
    baseStance: "1の強弱から入り、2-3-4の残り順を比較する",
    predictionAxis: "イン有利か中枠攻めかを比較する",
    comparisonOrder: ["boat1_strength", "lane2_wall", "lane34_attack", "display_gap"],
    emphasis: [],
    compareFocus: DEFAULT_COMPARISON_STEPS
  };
}

export { VENUE_RULES };
