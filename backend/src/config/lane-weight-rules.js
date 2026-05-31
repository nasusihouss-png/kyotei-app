export const LANE_WEIGHT_RULES = Object.freeze({
  1: Object.freeze({ currentSeason: 45, recentForm: 10, localFit: 15, gradeFit: 30 }),
  2: Object.freeze({ currentSeason: 40, recentForm: 10, localFit: 15, gradeFit: 35 }),
  3: Object.freeze({ currentSeason: 35, recentForm: 15, localFit: 10, gradeFit: 40 }),
  4: Object.freeze({ currentSeason: 35, recentForm: 15, localFit: 10, gradeFit: 40 }),
  5: Object.freeze({ currentSeason: 35, recentForm: 15, localFit: 15, gradeFit: 35 }),
  6: Object.freeze({ currentSeason: 30, recentForm: 15, localFit: 20, gradeFit: 35 })
});

export const LANE_WEIGHT_MEANINGS = Object.freeze({
  currentSeason: "今期 = 総合の土台",
  recentForm: "1か月 = 直近気配",
  localFit: "当地 = 水面相性",
  gradeFit: "一般戦 = 今回条件との一致"
});

export function getLaneWeightRule(lane) {
  return LANE_WEIGHT_RULES[Number(lane)] || LANE_WEIGHT_RULES[1];
}
