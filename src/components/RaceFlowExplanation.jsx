function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function ScenarioLine({ title, scenario }) {
  return (
    <div className="flow-line">
      <strong>{title}</strong>
      <p>{scenario?.text || "-"}</p>
      <span>{safeArray(scenario?.tickets).join(" / ") || "-"}</span>
    </div>
  );
}

export default function RaceFlowExplanation({ prediction = null }) {
  if (!prediction) return null;
  const scenario = prediction?.scenario || {};
  const upsetReasons = safeArray(prediction?.upsetReasons);
  return (
    <section className="card practical-section">
      <div className="section-head compact-head">
        <h2>レース展開</h2>
      </div>
      <div className="race-flow-list">
        <ScenarioLine title="イン逃げ想定" scenario={scenario.main} />
        <ScenarioLine title="2差し警戒" scenario={scenario.counter} />
        <ScenarioLine title="3まくり / 3まくり差し" scenario={scenario.counter} />
        <ScenarioLine title="4まくり差し" scenario={scenario.upset} />
        <ScenarioLine title="5・6の展開突き" scenario={scenario.upset} />
      </div>
      <div className="why-ticket-box">
        <strong>買い目理由</strong>
        <p>
          本線は1Mの主導権を取る艇を軸に、対抗は差し・まくり差しで隊形が崩れる筋を押さえます。
          {upsetReasons.length > 0 ? ` 注意点: ${upsetReasons.join(" / ")}` : " 大きな穴材料が少ない場合は基本6点中心です。"}
        </p>
      </div>
    </section>
  );
}
