import assert from "node:assert/strict";

import {
  applyRaceFlowScenarioAdjustments,
  buildRaceFlowScenarioModel,
  buildRaceFlowScenarioTickets
} from "./race-flow-scenario-engine.js";

const DEFAULT_SCORES = {
  exST: 0.5,
  exTime: 0.5,
  lapTime: 0.5,
  straightTime: 0.5,
  turnTime: 0.5,
  motor2Rate: 0.5
};

function entries(tendencyByBoat = {}) {
  return [1, 2, 3, 4, 5, 6].map((boat) => ({
    boat,
    course: boat,
    score: boat === 1 ? 0.8 : 0.55 - boat * 0.02,
    motor2Rate: 35,
    playerTendency: tendencyByBoat[boat] || null
  }));
}

function featureScores(overrides = {}) {
  return {
    byBoat: Object.fromEntries(
      [1, 2, 3, 4, 5, 6].map((boat) => [
        String(boat),
        {
          scores: {
            ...DEFAULT_SCORES,
            ...(overrides[boat] || {})
          },
          roleScore: 0.5
        }
      ])
    )
  };
}

function scenario(model, id) {
  return model.scenarios.find((row) => row.id === id);
}

function split(model, boat) {
  return model.headPartnerSplit.find((row) => row.boat === boat);
}

const strongBoat3 = {
  3: { exST: 0.9, straightTime: 0.9, turnTime: 0.58 }
};
const weakBoat2Wall = buildRaceFlowScenarioModel({
  entries: entries({
    3: { sampleStatus: "ok", courseSpecificLast6mRaceCount: 12, makuriRate: 0.34 }
  }),
  featureScores: featureScores({
    ...strongBoat3,
    2: { exST: 0.12, straightTime: 0.15, turnTime: 0.2, motor2Rate: 0.2 }
  })
});
const strongBoat2Wall = buildRaceFlowScenarioModel({
  entries: entries({
    3: { sampleStatus: "ok", courseSpecificLast6mRaceCount: 12, makuriRate: 0.34 }
  }),
  featureScores: featureScores({
    ...strongBoat3,
    2: { exST: 0.9, straightTime: 0.85, turnTime: 0.82, motor2Rate: 0.8 }
  })
});
assert.ok(
  scenario(weakBoat2Wall, "makuri_3").score > scenario(strongBoat2Wall, "makuri_3").score,
  "weak 2 wall should raise the 3 makuri scenario"
);

const boat4StrongAfter3Attack = buildRaceFlowScenarioModel({
  entries: entries({
    3: { sampleStatus: "ok", courseSpecificLast6mRaceCount: 13, makuriRate: 0.3 },
    4: { sampleStatus: "ok", courseSpecificLast6mRaceCount: 14, makuriSashiRate: 0.28 }
  }),
  featureScores: featureScores({
    3: { exST: 0.88, straightTime: 0.9, turnTime: 0.66 },
    4: { straightTime: 0.84, turnTime: 0.9, exST: 0.66 }
  })
});
const boat4WeakAfter3Attack = buildRaceFlowScenarioModel({
  entries: entries({
    3: { sampleStatus: "ok", courseSpecificLast6mRaceCount: 13, makuriRate: 0.3 },
    4: { sampleStatus: "ok", courseSpecificLast6mRaceCount: 14, makuriSashiRate: 0.28 }
  }),
  featureScores: featureScores({
    3: { exST: 0.88, straightTime: 0.9, turnTime: 0.66 },
    4: { straightTime: 0.25, turnTime: 0.22, exST: 0.55 }
  })
});
assert.ok(
  scenario(boat4StrongAfter3Attack, "second_wave_4").score >
    scenario(boat4WeakAfter3Attack, "second_wave_4").score,
  "3 attack plus 4 turn/straight should raise the 4 second-wave scenario"
);
assert.ok(split(boat4StrongAfter3Attack, 4).beneficiaryScore > split(boat4WeakAfter3Attack, 4).beneficiaryScore);

const boat2Sashi = buildRaceFlowScenarioModel({
  entries: entries({
    1: { sampleStatus: "ok", courseSpecificLast6mRaceCount: 16, beatenBySashiRate: 0.24 },
    2: { sampleStatus: "ok", courseSpecificLast6mRaceCount: 18, sashiRate: 0.42 }
  }),
  featureScores: featureScores({
    2: { exST: 0.82, turnTime: 0.88, lapTime: 0.68 }
  })
});
const boat2Neutral = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores()
});
assert.ok(
  scenario(boat2Sashi, "sashi_2").score > scenario(boat2Neutral, "sashi_2").score,
  "2 sashi tendency and turnTime should raise the 2 sashi scenario"
);

const boat4NoSupport = buildRaceFlowScenarioModel({
  entries: entries({
    4: { sampleStatus: "insufficient_history", courseSpecificLast6mRaceCount: 0 }
  }),
  featureScores: featureScores({
    4: { straightTime: 0.9, turnTime: 0.9 }
  })
});
assert.equal(split(boat4NoSupport, 4).canBeHead, false, "4 should not become a head candidate from foot alone");
assert.equal(split(boat4StrongAfter3Attack, 4).canBeHead, true, "4 can be promoted only with scenario support");

const outsideStrong = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores({
    5: { lapTime: 0.95, straightTime: 0.95, turnTime: 0.88, motor2Rate: 0.9 },
    6: { lapTime: 0.93, straightTime: 0.92, turnTime: 0.86, motor2Rate: 0.88 }
  })
});
assert.equal(outsideStrong.headCandidates.some((row) => row.boat >= 5), false);
assert.ok(
  outsideStrong.dangerousButNotHead.some((row) => row.boat >= 5),
  "outside boats should be treated as dangerous partners, not overpromoted heads"
);

const missingTendency = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores()
});
assert.equal(missingTendency.scenarios.length, 6);
assert.ok(missingTendency.dataWarnings.length >= 1);

const tailwindAttack = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores({
    3: { exST: 0.86, straightTime: 0.9, turnTime: 0.55 }
  }),
  raceConditions: { windDirection: "追い風", windSpeed: 7, waveHeight: 1 }
});
const calmAttack = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores({
    3: { exST: 0.86, straightTime: 0.9, turnTime: 0.55 }
  }),
  raceConditions: { windDirection: null, windSpeed: 1, waveHeight: 1 }
});
assert.ok(
  scenario(tailwindAttack, "makuri_3").score > scenario(calmAttack, "makuri_3").score,
  "strong tailwind should lightly boost 3 attack when ST and straight are strong"
);
assert.ok(tailwindAttack.conditionAdjustmentLog.length > 0);

const tailwindEnglishAttack = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores({
    3: { exST: 0.86, straightTime: 0.9, turnTime: 0.55 }
  }),
  raceConditions: { windDirection: "tailwind", windSpeed: 7, waveHeight: 1 }
});
assert.ok(
  scenario(tailwindEnglishAttack, "makuri_3").score > scenario(calmAttack, "makuri_3").score,
  "tailwind keyword should boost 3 attack with strong exST and straightTime"
);

const highWaveStableInside = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores({
    1: { lapTime: 0.9, turnTime: 0.9, exST: 0.62 }
  }),
  raceConditions: { windDirection: "headwind", windSpeed: 2, waveHeight: 8 }
});
const calmStableInside = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores({
    1: { lapTime: 0.9, turnTime: 0.9, exST: 0.62 }
  }),
  raceConditions: { windDirection: null, windSpeed: 1, waveHeight: 1 }
});
assert.ok(
  scenario(highWaveStableInside, "escape_1").score > scenario(calmStableInside, "escape_1").score,
  "high wave should increase the value of strong lapTime/turnTime for inside residual"
);
assert.ok(highWaveStableInside.conditionAdjustmentLog.some((row) => row.type === "wave" && row.level === "strong"));

const missingConditions = buildRaceFlowScenarioModel({
  entries: entries(),
  featureScores: featureScores(),
  raceConditions: null
});
assert.equal(missingConditions.scenarios.length, 6);

const adjusted = applyRaceFlowScenarioAdjustments(entries(), boat2Sashi);
assert.equal(adjusted.length, 6);
assert.ok(adjusted.every((row) => Number.isFinite(row.score)));

const scenarioTickets = buildRaceFlowScenarioTickets(boat4StrongAfter3Attack, [], 4);
assert.ok(scenarioTickets.length > 0);
assert.ok(scenarioTickets.every((ticket) => !ticket.combo.startsWith("5-") && !ticket.combo.startsWith("6-")));
