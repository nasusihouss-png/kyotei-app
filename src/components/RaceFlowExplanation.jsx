function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function ScenarioLine({ title, scenario, fallback }) {
  const reasons = safeArray(scenario?.reasons);
  return (
    <div className="flow-line">
      <strong>{title}</strong>
      <p>{reasons[0] || fallback || scenario?.text || "-"}</p>
      <span>
        {scenario?.label || scenario?.id || "-"}
        {scenario?.score == null ? "" : ` / ${Number(scenario.score).toFixed(1)}`}
      </span>
    </div>
  );
}

export default function RaceFlowExplanation({ prediction = null }) {
  if (!prediction) return null;
  const raceFlow = prediction?.raceFlowScenario || {};
  const scenarios = safeArray(raceFlow.scenarios);
  const byId = Object.fromEntries(scenarios.map((row) => [row.id, row]));
  const explanations = safeArray(raceFlow.explanations);
  const upsetReasons = safeArray(prediction?.upsetReasons);

  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>レース展開</h2>
      </div>
      <div className="race-flow-list">
        <ScenarioLine
          title="イン逃げ想定"
          scenario={byId.escape_1}
          fallback="1号艇の周回・まわり足、2号艇の壁、展示STから逃げ信頼度を見ます。"
        />
        <ScenarioLine
          title="2差し警戒"
          scenario={byId.sashi_2}
          fallback="2号艇のまわり足と差し率、1号艇の差され率を組み合わせて警戒します。"
        />
        <ScenarioLine
          title="3まくり / 3まくり差し"
          scenario={(byId.makuri_3?.score || 0) >= (byId.makuri_sashi_3?.score || 0) ? byId.makuri_3 : byId.makuri_sashi_3}
          fallback="3号艇の展示ST・直線・まわり足と、2号艇の壁の強さを見ます。"
        />
        <ScenarioLine
          title="4まくり差し"
          scenario={byId.second_wave_4}
          fallback="3号艇が攻めた後、4号艇が差し場を拾えるかを評価します。"
        />
        <ScenarioLine
          title="5・6の展開突き"
          scenario={byId.outside_follow_5_6}
          fallback="5・6号艇は頭より2着・3着の穴相手として扱います。"
        />
      </div>
      <div className="why-ticket-box">
        <strong>なぜその買い目か</strong>
        <p>
          {explanations.length > 0
            ? explanations.join(" ")
            : "本線は1Mの主導権を取る艇を軸に、対抗は差し・まくり差しで隊形が崩れる筋を押さえます。"}
          {upsetReasons.length > 0 ? ` 注意材料: ${upsetReasons.join(" / ")}` : ""}
        </p>
      </div>
    </section>
  );
}
