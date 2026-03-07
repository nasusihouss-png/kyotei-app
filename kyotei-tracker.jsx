import { useState, useEffect } from "react";

// ── 競艇場マスター（stadium_number順） ───────────────────────────────────
const VENUES = [
  { id: 1,  name: "桐生" },
  { id: 2,  name: "戸田" },
  { id: 3,  name: "江戸川" },
  { id: 4,  name: "平和島" },
  { id: 5,  name: "多摩川" },
  { id: 6,  name: "浜名湖" },
  { id: 7,  name: "蒲郡" },
  { id: 8,  name: "常滑" },
  { id: 9,  name: "津" },
  { id: 10, name: "三国" },
  { id: 11, name: "びわこ" },
  { id: 12, name: "住之江" },
  { id: 13, name: "尼崎" },
  { id: 14, name: "鳴門" },
  { id: 15, name: "丸亀" },
  { id: 16, name: "児島" },
  { id: 17, name: "宮島" },
  { id: 18, name: "徳山" },
  { id: 19, name: "下関" },
  { id: 20, name: "若松" },
  { id: 21, name: "芦屋" },
  { id: 22, name: "福岡" },
  { id: 23, name: "唐津" },
  { id: 24, name: "大村" },
];

const BET_TYPES = [
  { id: "trifecta",      label: "3連単",  nums: 3, apiKey: "trifecta",      desc: "1-2-3着 順番通り" },
  { id: "trio",          label: "3連複",  nums: 3, apiKey: "trio",          desc: "1-2-3着 組み合わせ" },
  { id: "exacta",        label: "2連単",  nums: 2, apiKey: "exacta",        desc: "1-2着 順番通り" },
  { id: "quinella",      label: "2連複",  nums: 2, apiKey: "quinella",      desc: "1-2着 組み合わせ" },
  { id: "quinella_place",label: "拡連複", nums: 2, apiKey: "quinella_place", desc: "3着以内2艇" },
  { id: "win",           label: "単勝",   nums: 1, apiKey: "win",           desc: "1着を当てる" },
  { id: "place",         label: "複勝",   nums: 1, apiKey: "place",         desc: "2着以内" },
];

const LC = ["#e53935","#1e88e5","#fdd835","#43a047","#8e24aa","#f4511e"];
const LT = ["#fff","#fff","#111","#fff","#fff","#fff"];

const STORAGE_KEY = "kyotei_v5";
const loadData = () => { try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]"); } catch { return []; } };

// ── 期間フィルター ───────────────────────────────────────────────────────
function inPeriod(dateStr, p) {
  const d = new Date(dateStr), now = new Date();
  if (p === "日") return d.toDateString() === now.toDateString();
  if (p === "週") {
    const s = new Date(now); s.setDate(now.getDate() - now.getDay());
    const e = new Date(s); e.setDate(s.getDate() + 6);
    return d >= s && d <= e;
  }
  if (p === "月") return d.getFullYear() === now.getFullYear() && d.getMonth() === now.getMonth();
  if (p === "年") return d.getFullYear() === now.getFullYear();
  return true;
}

// ── 的中チェック ─────────────────────────────────────────────────────────
// result = [1着艇番, 2着艇番, 3着艇番]（数値）
function checkHit(betType, betNums, result) {
  if (!result || result.length < 2) return null;
  const [r1, r2, r3] = result;
  const [b1, b2, b3] = betNums;
  switch (betType) {
    case "win":           return b1 === r1;
    case "place":         return b1 === r1 || b1 === r2;
    case "exacta":        return b1 === r1 && b2 === r2;
    case "quinella":      return [r1,r2].includes(b1) && [r1,r2].includes(b2) && b1 !== b2;
    case "quinella_place": return [r1,r2,r3].includes(b1) && [r1,r2,r3].includes(b2) && b1 !== b2;
    case "trifecta":      return b1 === r1 && b2 === r2 && b3 === r3;
    case "trio": {
      const s = new Set([b1, b2, b3]);
      return r3 ? s.has(r1) && s.has(r2) && s.has(r3) : false;
    }
    default: return null;
  }
}

// ── BoatraceOpenAPI から実結果取得 ────────────────────────────────────────
// https://boatraceopenapi.github.io/results/v2/YYYY/YYYYMMDD.json
// race_stadium_number = venue.id, race_number = raceNo
// payouts.trifecta[].combination / payout  (payout は払戻金額: 100円あたり)
// boats[].racer_boat_number, racer_place_number → 着順を復元

async function fetchFromOpenAPI(date, venueId, raceNo) {
  const d = date.replace(/-/g, ""); // "2025-07-15" → "20250715"
  const year = d.slice(0, 4);
  const url = `https://boatraceopenapi.github.io/results/v2/${year}/${d}.json`;

  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const json = await res.json();

  // 対象レースを探す
  const raceData = (json.results || []).find(
    r => r.race_stadium_number === venueId && r.race_number === raceNo
  );
  if (!raceData) return null;

  // 着順復元: racer_place_number でソートして racer_boat_number を取り出す
  const sorted = [...raceData.boats].sort((a, b) => a.racer_place_number - b.racer_place_number);
  const finishOrder = sorted.map(b => b.racer_boat_number); // [1着艇番, 2着艇番, ...]

  // payouts を整形: { trifecta: { "1-5-3": 126.9, ... }, ... }
  const payouts = {};
  for (const [type, items] of Object.entries(raceData.payouts || {})) {
    payouts[type] = {};
    for (const item of items) {
      // payout は100円あたりの払戻額 → 倍率に変換
      payouts[type][item.combination] = item.payout / 100;
    }
  }

  return { finishOrder, payouts };
}

// 買い目の combination キーを生成
function makeCombKey(betType, betNums) {
  const [b1, b2, b3] = betNums;
  switch (betType) {
    case "win":           return String(b1);
    case "place":         return String(b1);
    case "exacta":        return `${b1}-${b2}`;
    case "quinella":      return [b1, b2].sort((a,b) => a-b).join("=");
    case "quinella_place": return [b1, b2].sort((a,b) => a-b).join("=");
    case "trifecta":      return `${b1}-${b2}-${b3}`;
    case "trio":          return [b1, b2, b3].sort((a,b) => a-b).join("=");
    default: return "";
  }
}

// APIキーから payouts を引く（quinella_place は複数候補あり）
function getOddsFromPayouts(betType, betNums, payouts, apiKey) {
  const pDict = payouts?.[apiKey] || {};
  const key = makeCombKey(betType, betNums);
  return pDict[key] ?? null;
}

// ── 舟券バッジ ────────────────────────────────────────────────────────────
function TicketRow({ ticket, result, onRemove, showRemove }) {
  const hit = result ? checkHit(ticket.betType, ticket.betNums, result) : null;
  const payout = hit === true && ticket.odds ? Math.floor(ticket.amount * ticket.odds) : hit === false ? 0 : null;
  const pnl = payout !== null ? payout - ticket.amount : null;
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 7, padding: "8px 10px", flexWrap: "wrap",
      background: hit === true ? "rgba(0,220,100,0.07)" : hit === false ? "rgba(255,60,80,0.06)" : "rgba(255,255,255,0.03)",
      border: `1px solid ${hit === true ? "rgba(0,220,100,0.22)" : hit === false ? "rgba(255,60,80,0.17)" : "rgba(255,255,255,0.07)"}`,
      borderRadius: 8,
    }}>
      <span style={{ fontSize: 11, fontWeight: 700, color: "#5bc8ff", minWidth: 40 }}>{BET_TYPES.find(b => b.id === ticket.betType)?.label}</span>
      <div style={{ display: "flex", gap: 3 }}>
        {ticket.betNums.map((n, i) => (
          <span key={i} style={{ width: 20, height: 20, borderRadius: 3, background: LC[n-1], color: LT[n-1], display: "inline-flex", alignItems: "center", justifyContent: "center", fontSize: 12, fontWeight: 900 }}>{n}</span>
        ))}
      </div>
      <span style={{ fontSize: 12, color: "#7a9abb", fontFamily: "monospace" }}>{ticket.amount.toLocaleString()}円</span>
      {ticket.odds != null && <span style={{ fontSize: 12, color: "#f5c842", fontFamily: "monospace" }}>{ticket.odds}倍</span>}
      {hit === true && <span style={{ fontSize: 11, color: "#00e87a", fontWeight: 700 }}>🎯 的中</span>}
      {hit === false && <span style={{ fontSize: 11, color: "#ff4d6d" }}>💦 ハズレ</span>}
      {pnl !== null && (
        <span style={{ marginLeft: "auto", fontFamily: "monospace", fontSize: 13, fontWeight: 700, color: pnl >= 0 ? "#00e87a" : "#ff4d6d" }}>
          {pnl >= 0 ? "+" : ""}{pnl.toLocaleString()}円
        </span>
      )}
      {showRemove && (
        <button onClick={onRemove} style={{ background: "rgba(255,60,80,0.12)", border: "1px solid rgba(255,60,80,0.22)", borderRadius: 5, color: "#ff4d6d", cursor: "pointer", fontSize: 11, padding: "3px 7px", fontWeight: 700 }}>✕</button>
      )}
    </div>
  );
}

// ── メインコンポーネント ──────────────────────────────────────────────────
export default function App() {
  const [races, setRaces]   = useState(loadData);
  const [tab, setTab]       = useState("input");
  const [filterP, setFilterP] = useState("月");

  // 入力フォーム
  const [date, setDate]       = useState(() => new Date().toISOString().slice(0, 10));
  const [venueId, setVenueId] = useState(1);
  const [raceNo, setRaceNo]   = useState(1);
  const [betType, setBetType] = useState("trifecta");
  const [betNums, setBetNums] = useState([]);
  const [amount, setAmount]   = useState(100);
  const [ticketOdds, setTicketOdds] = useState("");
  const [cart, setCart]       = useState([]);

  // 検索
  const [searching, setSearching] = useState(false);
  const [searchMsg, setSearchMsg] = useState("");

  // 編集モーダル
  const [editModal, setEditModal]   = useState(null);
  const [mResult, setMResult]       = useState(["", "", ""]);
  const [mTickets, setMTickets]     = useState([]);

  useEffect(() => { localStorage.setItem(STORAGE_KEY, JSON.stringify(races)); }, [races]);

  const maxNums = BET_TYPES.find(b => b.id === betType)?.nums || 3;
  const venueObj = VENUES.find(v => v.id === Number(venueId));

  const toggleNum = (n) => {
    if (betNums.includes(n)) setBetNums(betNums.filter(x => x !== n));
    else if (betNums.length < maxNums) setBetNums([...betNums, n]);
  };

  const addToCart = () => {
    if (betNums.length < maxNums) { alert(`艇番を${maxNums}つ選んでください`); return; }
    setCart(p => [...p, {
      id: Date.now(),
      betType, betNums: [...betNums],
      amount: Number(amount),
      odds: ticketOdds ? parseFloat(ticketOdds) : null,
    }]);
    setBetNums([]); setTicketOdds("");
  };

  const confirmRace = () => {
    if (!cart.length) { alert("舟券を1枚以上追加してください"); return; }
    setRaces(p => [{
      id: Date.now(),
      date, venueId: Number(venueId), venueName: venueObj?.name || "",
      raceNo: Number(raceNo),
      tickets: [...cart],
      result: null,
      payouts: null,
      createdAt: Date.now(),
    }, ...p]);
    setCart([]); setBetNums([]); setSearchMsg("");
    setTab("list");
  };

  // ── BoatraceOpenAPI から結果取得して的中判定 ──────────────────────────
  const doFetch = async (race) => {
    setSearching(true);
    setSearchMsg(`${race.venueName} ${race.raceNo}R のデータを取得中...`);
    try {
      const data = await fetchFromOpenAPI(race.date, race.venueId, race.raceNo);
      if (!data) {
        setSearchMsg(`⚠️ ${race.venueName} ${race.raceNo}R のデータが見つかりませんでした（まだ未開催か日付をご確認ください）`);
        setSearching(false);
        return;
      }

      const { finishOrder, payouts } = data;
      const result = finishOrder.slice(0, 3);

      // 各チケットのオッズを自動取得
      const updTickets = race.tickets.map(t => {
        const apiKeyInfo = BET_TYPES.find(b => b.id === t.betType);
        const autoOdds = getOddsFromPayouts(t.betType, t.betNums, payouts, apiKeyInfo?.apiKey || t.betType);
        return { ...t, odds: autoOdds ?? t.odds };
      });

      setRaces(p => p.map(r =>
        r.id === race.id ? { ...r, result, payouts, tickets: updTickets } : r
      ));
      setSearchMsg(`✅ ${race.venueName} ${race.raceNo}R: ${result.join("-")} 取得・的中判定完了！`);
    } catch (e) {
      setSearchMsg(`❌ 取得失敗: ${e.message} — 手動編集でご入力ください`);
    }
    setSearching(false);
    setTimeout(() => setSearchMsg(""), 7000);
  };

  const openEdit = (race) => {
    setEditModal(race);
    setMResult(race.result ? race.result.map(String) : ["", "", ""]);
    setMTickets(race.tickets.map(t => ({ ...t })));
  };
  const saveEdit = () => {
    const result = mResult.map(Number).filter(n => n >= 1 && n <= 6);
    setRaces(p => p.map(r =>
      r.id === editModal.id
        ? { ...r, result: result.length >= 2 ? result : null, tickets: mTickets.map(t => ({ ...t, odds: t.odds ? parseFloat(t.odds) : null })) }
        : r
    ));
    setEditModal(null);
  };

  const deleteRace = (id) => { if (confirm("このレースの記録を削除しますか？")) setRaces(p => p.filter(r => r.id !== id)); };

  // ── 損益計算 ─────────────────────────────────────────────────────────
  const racePnl = (race) => {
    if (!race.result) return null;
    let total = 0;
    for (const t of race.tickets) {
      const hit = checkHit(t.betType, t.betNums, race.result);
      total -= t.amount; // 常にマイナス
      if (hit === true && t.odds) {
        total += Math.floor(t.amount * t.odds); // 払戻プラス
      }
    }
    return total;
  };

  const pStats = (p) => {
    const list = races.filter(r => p === "全期間" || inPeriod(r.date, p));
    const settled = list.filter(r => r.result);
    const allTix = settled.flatMap(r => r.tickets.map(t => ({ ...t, result: r.result })));
    const hits = allTix.filter(t => checkHit(t.betType, t.betNums, t.result) === true).length;
    const totalBet = list.reduce((s, r) => s + r.tickets.reduce((a, t) => a + t.amount, 0), 0);
    // 損益 = 払戻合計 - 投資合計
    const totalPayout = settled.flatMap(r => r.tickets.map(t => {
      const hit = checkHit(t.betType, t.betNums, r.result);
      return (hit === true && t.odds) ? Math.floor(t.amount * t.odds) : 0;
    })).reduce((a, b) => a + b, 0);
    const totalBetSettled = settled.reduce((s, r) => s + r.tickets.reduce((a, t) => a + t.amount, 0), 0);
    const pnl = totalPayout - totalBetSettled;
    return { count: list.length, settled: settled.length, tix: allTix.length, hits, hitRate: allTix.length ? (hits / allTix.length * 100).toFixed(1) : "—", pnl, totalBet };
  };

  const listRaces = races.filter(r => filterP === "全期間" || inPeriod(r.date, filterP));
  const cartTotal = cart.reduce((s, t) => s + t.amount, 0);
  const periodLabels = { 日: "本日", 週: "今週", 月: "今月", 年: "今年" };

  return (
    <div style={{ minHeight: "100vh", background: "#070d1a", color: "#d5e2ef", fontFamily: "'Noto Sans JP', sans-serif" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@400;500;700;900&family=DM+Mono:wght@500;700&display=swap');
        *{box-sizing:border-box;margin:0;padding:0;}
        ::-webkit-scrollbar{width:4px;} ::-webkit-scrollbar-thumb{background:#1a2e50;border-radius:3px;}
        @keyframes spin{to{transform:rotate(360deg);}}
        @keyframes up{from{opacity:0;transform:translateY(8px);}to{opacity:1;transform:none;}}
        @keyframes pop{0%{transform:scale(.88);}60%{transform:scale(1.05);}100%{transform:scale(1);}}
        input,select{background:#0b1624;border:1px solid rgba(91,200,255,0.18);border-radius:7px;color:#d5e2ef;padding:8px 11px;font-family:inherit;font-size:14px;outline:none;transition:border .15s;width:100%;}
        input:focus,select:focus{border-color:rgba(91,200,255,0.45);}
        .btn{border:none;cursor:pointer;font-family:inherit;font-weight:700;border-radius:8px;transition:all .15s;}
        .btn:hover{filter:brightness(1.12);transform:translateY(-1px);}
        .btn:active{transform:translateY(0);}
        .btn:disabled{opacity:.3;cursor:not-allowed;transform:none;filter:none;}
        .tab-btn{background:none;border:none;cursor:pointer;font-family:inherit;font-size:13px;font-weight:700;padding:10px 16px;color:#254060;letter-spacing:.5px;border-bottom:2px solid transparent;transition:all .15s;}
        .tab-btn.on{color:#5bc8ff;border-bottom-color:#5bc8ff;}
        .card{background:rgba(255,255,255,0.028);border:1px solid rgba(91,200,255,0.1);border-radius:12px;padding:15px;}
        .slabel{font-size:10px;color:#1a4060;letter-spacing:3px;font-weight:700;margin-bottom:9px;}
        .rcard{animation:up .25s ease both;background:rgba(255,255,255,0.022);border:1px solid rgba(91,200,255,0.09);border-radius:12px;padding:14px;margin-bottom:9px;}
        .mono{font-family:'DM Mono',monospace;}
        .pnl-pos{color:#00e87a;font-family:'DM Mono',monospace;font-weight:700;}
        .pnl-neg{color:#ff4d6d;font-family:'DM Mono',monospace;font-weight:700;}
        .msg-ok{background:rgba(0,220,100,0.07);border:1px solid rgba(0,220,100,0.2);color:#00e87a;}
        .msg-err{background:rgba(255,60,80,0.07);border:1px solid rgba(255,60,80,0.2);color:#ff4d6d;}
        .msg-warn{background:rgba(255,220,0,0.05);border:1px solid rgba(255,220,0,0.15);color:#f5c842;}
      `}</style>

      {/* HEADER */}
      <div style={{ background: "rgba(3,7,18,0.97)", borderBottom: "1px solid rgba(91,200,255,0.1)", position: "sticky", top: 0, zIndex: 50, backdropFilter: "blur(14px)" }}>
        <div style={{ maxWidth: 920, margin: "0 auto", padding: "0 16px" }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "11px 0 6px" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ fontSize: 22 }}>🚤</span>
              <div>
                <div className="mono" style={{ fontSize: 15, fontWeight: 700, color: "#5bc8ff", letterSpacing: 3 }}>BOAT RACE LOG</div>
                <div style={{ fontSize: 9, color: "#0e2a45", letterSpacing: 3 }}>公式データ自動取得・収支管理</div>
              </div>
            </div>
            {/* 期間別損益サマリー */}
            <div style={{ display: "flex", gap: 14 }}>
              {["日", "週", "月", "年"].map(p => {
                const st = pStats(p);
                return (
                  <div key={p} style={{ textAlign: "center" }}>
                    <div style={{ fontSize: 9, color: "#0e2a45" }}>{periodLabels[p]}</div>
                    <div className={st.pnl >= 0 ? "pnl-pos" : "pnl-neg"} style={{ fontSize: 13 }}>
                      {st.pnl >= 0 ? "+" : ""}{st.pnl.toLocaleString()}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
          <div style={{ display: "flex", borderTop: "1px solid rgba(255,255,255,0.04)" }}>
            {[["input", "✏️ 投票入力"], ["list", "📋 記録一覧"], ["summary", "📊 集計"]].map(([id, l]) => (
              <button key={id} className={`tab-btn ${tab === id ? "on" : ""}`} onClick={() => setTab(id)}>{l}</button>
            ))}
          </div>
        </div>
      </div>

      <div style={{ maxWidth: 920, margin: "0 auto", padding: "18px 16px" }}>

        {/* ═══ 投票入力 ═══ */}
        {tab === "input" && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, alignItems: "start" }}>

            {/* 左：設定 */}
            <div style={{ display: "flex", flexDirection: "column", gap: 11 }}>
              {/* レース情報 */}
              <div className="card">
                <div className="slabel">▸ レース情報</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  <div>
                    <div style={{ fontSize: 11, color: "#1e4a6a", marginBottom: 3 }}>日付</div>
                    <input type="date" value={date} onChange={e => setDate(e.target.value)} />
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 70px", gap: 8 }}>
                    <div>
                      <div style={{ fontSize: 11, color: "#1e4a6a", marginBottom: 3 }}>競艇場</div>
                      <select value={venueId} onChange={e => setVenueId(Number(e.target.value))}>
                        {VENUES.map(v => <option key={v.id} value={v.id}>{v.name}</option>)}
                      </select>
                    </div>
                    <div>
                      <div style={{ fontSize: 11, color: "#1e4a6a", marginBottom: 3 }}>レース</div>
                      <select value={raceNo} onChange={e => setRaceNo(e.target.value)}>
                        {Array.from({ length: 12 }, (_, i) => i + 1).map(n => (
                          <option key={n} value={n}>{n}R</option>
                        ))}
                      </select>
                    </div>
                  </div>
                </div>
              </div>

              {/* 賭け式 */}
              <div className="card">
                <div className="slabel">▸ 賭け式</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 5 }}>
                  {BET_TYPES.map(bt => (
                    <button key={bt.id} onClick={() => { setBetType(bt.id); setBetNums([]); }} style={{
                      background: betType === bt.id ? "rgba(91,200,255,0.12)" : "rgba(255,255,255,0.025)",
                      border: `1px solid ${betType === bt.id ? "rgba(91,200,255,0.42)" : "rgba(255,255,255,0.07)"}`,
                      borderRadius: 7, padding: "7px 9px", cursor: "pointer", textAlign: "left", transition: "all .12s",
                    }}>
                      <div style={{ fontSize: 13, fontWeight: 700, color: betType === bt.id ? "#5bc8ff" : "#6aabbb" }}>{bt.label}</div>
                      <div style={{ fontSize: 10, color: "#1a3a50", marginTop: 1 }}>{bt.desc}</div>
                    </button>
                  ))}
                </div>
              </div>

              {/* 艇番 */}
              <div className="card">
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 9 }}>
                  <div className="slabel" style={{ margin: 0 }}>▸ 艇番選択</div>
                  <div style={{ fontSize: 11, color: "#1e4a6a" }}>{betNums.length}/{maxNums}着</div>
                </div>
                <div style={{ display: "flex", gap: 7, justifyContent: "center", marginBottom: 8 }}>
                  {[1, 2, 3, 4, 5, 6].map(n => {
                    const idx = betNums.indexOf(n); const on = idx !== -1;
                    return (
                      <button key={n} onClick={() => toggleNum(n)} style={{
                        width: 40, height: 40, borderRadius: "50%",
                        border: `2px solid ${on ? LC[n-1] : "rgba(255,255,255,0.15)"}`,
                        cursor: "pointer", fontFamily: "monospace", fontSize: 14, fontWeight: 900,
                        display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
                        background: on ? LC[n-1] : "transparent", color: on ? LT[n-1] : LC[n-1],
                        boxShadow: on ? `0 0 10px ${LC[n-1]}55` : "none",
                        transform: on ? "scale(1.08)" : "scale(1)", transition: "all .1s",
                      }}>
                        {on && maxNums > 1
                          ? <div style={{ lineHeight: 1, textAlign: "center" }}><div style={{ fontSize: 7 }}>{idx+1}着</div><div>{n}</div></div>
                          : n}
                      </button>
                    );
                  })}
                </div>
                {betNums.length > 0 && (
                  <div className="mono" style={{ textAlign: "center", fontSize: 14, color: "#5bc8ff", fontWeight: 700 }}>
                    {BET_TYPES.find(b => b.id === betType)?.label}：{betNums.join("-")}
                  </div>
                )}
              </div>

              {/* 金額 */}
              <div className="card">
                <div className="slabel">▸ 金額 / オッズ（任意）</div>
                <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginBottom: 8 }}>
                  {[100, 200, 300, 500, 1000, 2000, 3000, 5000].map(a => (
                    <button key={a} onClick={() => setAmount(a)} style={{
                      background: amount === a ? "rgba(91,200,255,0.15)" : "rgba(255,255,255,0.04)",
                      border: `1px solid ${amount === a ? "rgba(91,200,255,0.4)" : "rgba(255,255,255,0.07)"}`,
                      borderRadius: 5, padding: "4px 8px", cursor: "pointer",
                      color: amount === a ? "#5bc8ff" : "#2a5070", fontSize: 11, fontWeight: 700,
                    }}>{a.toLocaleString()}</button>
                  ))}
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 10 }}>
                  <div>
                    <div style={{ fontSize: 11, color: "#1e4a6a", marginBottom: 3 }}>賭け金（円）</div>
                    <input type="number" value={amount} min={100} step={100} onChange={e => setAmount(Number(e.target.value))} />
                  </div>
                  <div>
                    <div style={{ fontSize: 11, color: "#1e4a6a", marginBottom: 3 }}>オッズ（任意）</div>
                    <input type="number" step="0.1" placeholder="自動取得もできます" value={ticketOdds} onChange={e => setTicketOdds(e.target.value)} />
                  </div>
                </div>
                <button className="btn" onClick={addToCart} disabled={betNums.length < maxNums} style={{
                  width: "100%", padding: 11, fontSize: 14,
                  background: betNums.length >= maxNums ? "linear-gradient(135deg,#1555a0,#2299dd)" : "rgba(255,255,255,0.05)",
                  color: betNums.length >= maxNums ? "#fff" : "#1a3a50",
                }}>
                  ＋ カートに追加
                </button>
              </div>
            </div>

            {/* 右：カート */}
            <div>
              <div className="card">
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                  <div>
                    <div className="slabel" style={{ margin: 0 }}>▸ 購入リスト</div>
                    <div style={{ fontSize: 11, color: "#1e4a6a", marginTop: 2 }}>{venueObj?.name} {raceNo}R — {cart.length}枚</div>
                  </div>
                  <div className="mono" style={{ fontSize: 16, fontWeight: 700, color: "#f5c842" }}>{cartTotal.toLocaleString()}円</div>
                </div>

                {cart.length === 0 ? (
                  <div style={{ textAlign: "center", padding: "34px 0", color: "#0e2535" }}>
                    <div style={{ fontSize: 38, marginBottom: 8 }}>🗒️</div>
                    <div style={{ fontSize: 13 }}>左で舟券を選んでカートに追加</div>
                    <div style={{ fontSize: 11, marginTop: 4, color: "#091825" }}>何枚でも追加できます</div>
                  </div>
                ) : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {cart.map(t => (
                      <div key={t.id} style={{ animation: "pop .18s ease" }}>
                        <TicketRow ticket={t} result={null} showRemove onRemove={() => setCart(p => p.filter(x => x.id !== t.id))} />
                      </div>
                    ))}
                  </div>
                )}

                {cart.length > 0 && (
                  <div style={{ marginTop: 14, borderTop: "1px solid rgba(255,255,255,0.06)", paddingTop: 12 }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                      <span style={{ fontSize: 13, color: "#3a6a80" }}>合計投資額</span>
                      <span className="mono" style={{ fontSize: 18, fontWeight: 700, color: "#f5c842" }}>{cartTotal.toLocaleString()}円</span>
                    </div>
                    <button className="btn" onClick={confirmRace} style={{ width: "100%", padding: 13, fontSize: 15, background: "linear-gradient(135deg,#0d6b3a,#00b860)", color: "#fff", letterSpacing: 1 }}>
                      🏁 このレースを登録する
                    </button>
                    <div style={{ fontSize: 11, color: "#0e2535", textAlign: "center", marginTop: 7 }}>
                      登録後に「結果取得」で自動的に着順・オッズ・的中を反映
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* ═══ 記録一覧 ═══ */}
        {tab === "list" && (
          <div>
            {/* フィルター & メッセージ */}
            <div style={{ display: "flex", gap: 5, marginBottom: 10, flexWrap: "wrap", alignItems: "center" }}>
              {["全期間", "日", "週", "月", "年"].map(p => (
                <button key={p} className="btn" onClick={() => setFilterP(p)} style={{
                  padding: "6px 12px", fontSize: 12,
                  background: filterP === p ? "rgba(91,200,255,0.15)" : "rgba(255,255,255,0.04)",
                  border: `1px solid ${filterP === p ? "rgba(91,200,255,0.4)" : "rgba(255,255,255,0.08)"}`,
                  color: filterP === p ? "#5bc8ff" : "#1e4060",
                }}>{p === "全期間" ? p : periodLabels[p]}</button>
              ))}
              <div style={{ marginLeft: "auto", fontSize: 12, color: "#1e4060" }}>{listRaces.length}レース</div>
            </div>

            {/* メッセージバー */}
            {searchMsg && (
              <div className={`${searchMsg.startsWith("✅") ? "msg-ok" : searchMsg.startsWith("❌") ? "msg-err" : "msg-warn"}`}
                style={{ marginBottom: 10, padding: "9px 13px", borderRadius: 8, fontSize: 12, display: "flex", alignItems: "center", gap: 7 }}>
                {searching && <span style={{ display: "inline-block", width: 11, height: 11, border: "2px solid currentColor", borderTopColor: "transparent", borderRadius: "50%", animation: "spin .7s linear infinite" }} />}
                {searchMsg}
              </div>
            )}

            {/* 期間内の損益サマリー */}
            {listRaces.length > 0 && (() => {
              const s = pStats(filterP);
              const totalBetSettled = listRaces.filter(r => r.result).reduce((a, r) => a + r.tickets.reduce((b, t) => b + t.amount, 0), 0);
              return (
                <div style={{ display: "flex", gap: 12, marginBottom: 12, padding: "10px 14px", background: "rgba(255,255,255,0.025)", border: "1px solid rgba(91,200,255,0.1)", borderRadius: 10, flexWrap: "wrap" }}>
                  <div style={{ fontSize: 12, color: "#2a5070" }}>レース数: <span style={{ color: "#9aafcc", fontWeight: 700 }}>{listRaces.length}</span></div>
                  <div style={{ fontSize: 12, color: "#2a5070" }}>投資総額: <span className="mono" style={{ color: "#f5c842" }}>{listRaces.reduce((a, r) => a + r.tickets.reduce((b, t) => b + t.amount, 0), 0).toLocaleString()}円</span></div>
                  <div style={{ fontSize: 12, color: "#2a5070" }}>確定損益: <span className={s.pnl >= 0 ? "pnl-pos" : "pnl-neg"}>{s.pnl >= 0 ? "+" : ""}{s.pnl.toLocaleString()}円</span></div>
                  <div style={{ fontSize: 12, color: "#2a5070" }}>的中率: <span style={{ color: "#9aafcc", fontWeight: 700 }}>{s.hitRate}%</span></div>
                </div>
              );
            })()}

            {listRaces.length === 0 ? (
              <div style={{ textAlign: "center", padding: "60px 0", color: "#0e2535" }}>
                <div style={{ fontSize: 44, marginBottom: 12 }}>🌊</div>
                <div>記録がありません</div>
              </div>
            ) : listRaces.map((race, ri) => {
              const pnl = racePnl(race);
              const totalBet = race.tickets.reduce((s, t) => s + t.amount, 0);
              const hitCount = race.result ? race.tickets.filter(t => checkHit(t.betType, t.betNums, race.result) === true).length : null;
              return (
                <div key={race.id} className="rcard" style={{ animationDelay: `${ri * 0.03}s` }}>
                  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 10 }}>
                    <div>
                      <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 5, flexWrap: "wrap" }}>
                        <span className="mono" style={{ fontSize: 12, color: "#1a4060" }}>{race.date}</span>
                        <span style={{ fontWeight: 700, color: "#9aafcc", fontSize: 14 }}>{race.venueName}</span>
                        <span style={{ color: "#2a5070", fontSize: 13 }}>{race.raceNo}R</span>
                        <span style={{ fontSize: 10, background: "rgba(255,255,255,0.04)", padding: "2px 6px", borderRadius: 4, color: "#2a5070" }}>{race.tickets.length}枚</span>
                      </div>
                      {race.result ? (
                        <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
                          <span style={{ fontSize: 11, color: "#1a4060" }}>着順:</span>
                          {race.result.slice(0, 3).map((n, i) => (
                            <span key={i} style={{ width: 22, height: 22, borderRadius: 4, background: LC[n-1], color: LT[n-1], display: "inline-flex", alignItems: "center", justifyContent: "center", fontSize: 13, fontWeight: 900 }}>{n}</span>
                          ))}
                          <span style={{ fontSize: 11, color: "#1e5070" }}>{hitCount}/{race.tickets.length}枚的中</span>
                        </div>
                      ) : (
                        <span style={{ fontSize: 11, color: "#0e2535", fontStyle: "italic" }}>結果未取得</span>
                      )}
                    </div>

                    {/* 右側: 金額 & ボタン */}
                    <div style={{ textAlign: "right", flexShrink: 0 }}>
                      <div style={{ fontSize: 11, color: "#1a4060" }}>
                        投資: <span className="mono" style={{ color: "#ff4d6d" }}>−{totalBet.toLocaleString()}円</span>
                      </div>
                      {pnl !== null && (
                        <div className={pnl >= 0 ? "pnl-pos" : "pnl-neg"} style={{ fontSize: 16 }}>
                          {pnl >= 0 ? "+" : ""}{pnl.toLocaleString()}円
                        </div>
                      )}
                      <div style={{ display: "flex", gap: 5, marginTop: 7, justifyContent: "flex-end", flexWrap: "wrap" }}>
                        {/* 結果取得ボタン */}
                        <button className="btn" onClick={() => doFetch(race)} disabled={searching} style={{
                          padding: "5px 11px", fontSize: 12,
                          background: race.result ? "rgba(91,200,255,0.07)" : "linear-gradient(135deg,rgba(91,200,255,0.2),rgba(0,150,255,0.15))",
                          color: "#5bc8ff", border: "1px solid rgba(91,200,255,0.3)",
                          display: "flex", alignItems: "center", gap: 5,
                        }}>
                          {searching
                            ? <span style={{ display: "inline-block", width: 10, height: 10, border: "2px solid #5bc8ff", borderTopColor: "transparent", borderRadius: "50%", animation: "spin .7s linear infinite" }} />
                            : "🔍"}
                          {race.result ? "再取得" : "結果取得"}
                        </button>
                        <button className="btn" onClick={() => openEdit(race)} style={{ padding: "5px 10px", fontSize: 11, background: "rgba(255,255,255,0.05)", color: "#7aafcc", border: "1px solid rgba(255,255,255,0.1)" }}>✏️</button>
                        <button className="btn" onClick={() => deleteRace(race.id)} style={{ padding: "5px 10px", fontSize: 11, background: "rgba(255,60,80,0.08)", color: "#ff4d6d", border: "1px solid rgba(255,60,80,0.2)" }}>🗑</button>
                      </div>
                    </div>
                  </div>

                  {/* 舟券一覧 */}
                  <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                    {race.tickets.map(t => <TicketRow key={t.id} ticket={t} result={race.result} />)}
                  </div>

                  {/* レース合計 */}
                  {race.result && (
                    <div style={{ marginTop: 10, paddingTop: 9, borderTop: "1px solid rgba(255,255,255,0.05)", display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
                      <div style={{ fontSize: 12, color: "#1a4060" }}>
                        払戻合計: <span className="mono" style={{ color: "#f5c842" }}>
                          {race.tickets.filter(t => checkHit(t.betType, t.betNums, race.result) === true && t.odds)
                            .reduce((s, t) => s + Math.floor(t.amount * t.odds), 0).toLocaleString()}円
                        </span>
                      </div>
                      <div style={{ fontSize: 12, color: "#1a4060" }}>
                        このレース損益:
                        <span className={pnl >= 0 ? "pnl-pos" : "pnl-neg"} style={{ marginLeft: 6, fontSize: 14 }}>
                          {pnl >= 0 ? "+" : ""}{pnl.toLocaleString()}円
                        </span>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {/* ═══ 集計 ═══ */}
        {tab === "summary" && (
          <div>
            {/* 期間サマリー */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 9, marginBottom: 16 }}>
              {["日", "週", "月", "年"].map(p => {
                const st = pStats(p);
                return (
                  <div key={p} className="card" style={{ textAlign: "center" }}>
                    <div style={{ fontSize: 10, color: "#0e2a40", letterSpacing: 2, marginBottom: 5 }}>{periodLabels[p]}</div>
                    <div className={st.pnl >= 0 ? "pnl-pos" : "pnl-neg"} style={{ fontSize: 20, marginBottom: 2 }}>
                      {st.pnl >= 0 ? "+" : ""}{st.pnl.toLocaleString()}<span style={{ fontSize: 10, fontFamily: "sans-serif", fontWeight: 400, color: "#1a3a50" }}>円</span>
                    </div>
                    <div style={{ fontSize: 10, color: "#1a3a50" }}>{st.count}R / {st.tix}枚購入</div>
                    <div style={{ fontSize: 10, color: "#1a3a50" }}>的中率 {st.hitRate}%</div>
                    <div style={{ fontSize: 10, color: "#1a3a50" }}>投資 {st.totalBet.toLocaleString()}円</div>
                  </div>
                );
              })}
            </div>

            {/* 賭け式別 */}
            <div className="card" style={{ marginBottom: 11 }}>
              <div className="slabel">▸ 賭け式別成績</div>
              {BET_TYPES.map(bt => {
                const tix = races.flatMap(r => r.result ? r.tickets.filter(t => t.betType === bt.id).map(t => ({ ...t, result: r.result })) : []);
                if (!tix.length) return null;
                const hits = tix.filter(t => checkHit(t.betType, t.betNums, t.result) === true).length;
                const invested = tix.reduce((s, t) => s + t.amount, 0);
                const payout = tix.reduce((s, t) => {
                  const h = checkHit(t.betType, t.betNums, t.result);
                  return h === true && t.odds ? s + Math.floor(t.amount * t.odds) : s;
                }, 0);
                const pnl = payout - invested;
                const hr = (hits / tix.length * 100).toFixed(1);
                return (
                  <div key={bt.id} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                    <div style={{ width: 46, fontSize: 12, fontWeight: 700, color: "#5aaabb" }}>{bt.label}</div>
                    <div style={{ width: 30, fontSize: 11, color: "#1a3a50" }}>{tix.length}枚</div>
                    <div style={{ flex: 1, height: 6, background: "rgba(255,255,255,0.06)", borderRadius: 3, overflow: "hidden" }}>
                      <div style={{ width: `${hr}%`, height: "100%", background: parseFloat(hr) > 25 ? "#00e87a" : parseFloat(hr) > 12 ? "#f5c842" : "#ff4d6d", borderRadius: 3, transition: "width .5s" }} />
                    </div>
                    <div style={{ width: 36, fontSize: 11, color: "#5aaabb", textAlign: "right" }}>{hr}%</div>
                    <div className="mono" style={{ width: 80, fontSize: 12, fontWeight: 700, color: pnl >= 0 ? "#00e87a" : "#ff4d6d", textAlign: "right" }}>
                      {pnl >= 0 ? "+" : ""}{pnl.toLocaleString()}
                    </div>
                  </div>
                );
              })}
              {!races.length && <div style={{ color: "#0e2535", fontSize: 13 }}>まだデータがありません</div>}
            </div>

            {/* 会場別 */}
            <div className="card">
              <div className="slabel">▸ 会場別成績</div>
              {VENUES.filter(v => races.some(r => r.venueId === v.id)).map(v => {
                const vr = races.filter(r => r.venueId === v.id);
                const pnl = vr.reduce((s, r) => s + (racePnl(r) || 0), 0);
                const tix = vr.flatMap(r => r.result ? r.tickets.map(t => ({ ...t, result: r.result })) : []);
                const hits = tix.filter(t => checkHit(t.betType, t.betNums, t.result) === true).length;
                const hr = tix.length ? (hits / tix.length * 100).toFixed(1) : "—";
                return (
                  <div key={v.id} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                    <div style={{ width: 46, fontSize: 12, fontWeight: 700, color: "#5aaabb" }}>{v.name}</div>
                    <div style={{ width: 30, fontSize: 11, color: "#1a3a50" }}>{vr.length}R</div>
                    <div style={{ flex: 1, height: 6, background: "rgba(255,255,255,0.06)", borderRadius: 3, overflow: "hidden" }}>
                      <div style={{ width: `${hr}%`, height: "100%", background: parseFloat(hr) > 25 ? "#00e87a" : parseFloat(hr) > 12 ? "#f5c842" : "#ff4d6d", borderRadius: 3 }} />
                    </div>
                    <div style={{ width: 36, fontSize: 11, color: "#5aaabb", textAlign: "right" }}>{hr}%</div>
                    <div className="mono" style={{ width: 80, fontSize: 12, fontWeight: 700, color: pnl >= 0 ? "#00e87a" : "#ff4d6d", textAlign: "right" }}>
                      {pnl >= 0 ? "+" : ""}{pnl.toLocaleString()}
                    </div>
                  </div>
                );
              })}
              {!races.some(r => VENUES.find(v => v.id === r.venueId)) && <div style={{ color: "#0e2535", fontSize: 13 }}>まだデータがありません</div>}
            </div>
          </div>
        )}
      </div>

      {/* ═══ 手動編集モーダル ═══ */}
      {editModal && (
        <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.8)", zIndex: 200, display: "flex", alignItems: "center", justifyContent: "center", padding: 16, backdropFilter: "blur(6px)" }}
          onClick={e => e.target === e.currentTarget && setEditModal(null)}>
          <div style={{ background: "#0c1828", border: "1px solid rgba(91,200,255,0.2)", borderRadius: 14, padding: 20, width: "100%", maxWidth: 500, maxHeight: "88vh", overflowY: "auto" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
              <div style={{ fontWeight: 700, color: "#5bc8ff", fontSize: 15 }}>{editModal.venueName} {editModal.raceNo}R 手動編集</div>
              <button onClick={() => setEditModal(null)} style={{ background: "none", border: "none", color: "#2a5070", cursor: "pointer", fontSize: 20 }}>✕</button>
            </div>

            <div style={{ marginBottom: 14 }}>
              <div className="slabel">▸ 着順（手動入力）</div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
                {["1着", "2着", "3着"].map((l, i) => (
                  <div key={i}>
                    <div style={{ fontSize: 11, color: "#1e4a6a", marginBottom: 3 }}>{l}</div>
                    <select value={mResult[i]} onChange={e => { const a = [...mResult]; a[i] = e.target.value; setMResult(a); }}>
                      <option value="">-</option>
                      {[1, 2, 3, 4, 5, 6].map(n => <option key={n} value={n}>{n}号艇</option>)}
                    </select>
                  </div>
                ))}
              </div>
            </div>

            <div style={{ marginBottom: 16 }}>
              <div className="slabel">▸ 各舟券のオッズ（手動入力）</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {mTickets.map((t, i) => {
                  const res = mResult.map(Number).filter(n => n >= 1 && n <= 6);
                  const hit = res.length >= 2 ? checkHit(t.betType, t.betNums, res) : null;
                  return (
                    <div key={t.id} style={{
                      display: "flex", alignItems: "center", gap: 7, padding: "8px 10px",
                      background: hit === true ? "rgba(0,220,100,0.06)" : hit === false ? "rgba(255,60,80,0.05)" : "rgba(255,255,255,0.025)",
                      border: `1px solid ${hit === true ? "rgba(0,220,100,0.2)" : hit === false ? "rgba(255,60,80,0.15)" : "rgba(255,255,255,0.07)"}`,
                      borderRadius: 8,
                    }}>
                      <span style={{ fontSize: 11, fontWeight: 700, color: "#5bc8ff", minWidth: 40 }}>{BET_TYPES.find(b => b.id === t.betType)?.label}</span>
                      <div style={{ display: "flex", gap: 3 }}>
                        {t.betNums.map((n, j) => (
                          <span key={j} style={{ width: 19, height: 19, borderRadius: 3, background: LC[n-1], color: LT[n-1], display: "inline-flex", alignItems: "center", justifyContent: "center", fontSize: 11, fontWeight: 900 }}>{n}</span>
                        ))}
                      </div>
                      <span className="mono" style={{ fontSize: 11, color: "#3a6a80" }}>{t.amount.toLocaleString()}円</span>
                      {hit === true && <span style={{ fontSize: 11, color: "#00e87a", fontWeight: 700 }}>🎯</span>}
                      {hit === false && <span style={{ fontSize: 11, color: "#ff4d6d" }}>💦</span>}
                      <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 5 }}>
                        <span style={{ fontSize: 11, color: "#1a3a50" }}>倍率</span>
                        <input type="number" step="0.1" placeholder="—" value={t.odds || ""} onChange={e => {
                          const u = [...mTickets]; u[i] = { ...u[i], odds: e.target.value ? e.target.value : null }; setMTickets(u);
                        }} style={{ width: 70, padding: "5px 8px", fontSize: 13 }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            <div style={{ display: "flex", gap: 8 }}>
              <button className="btn" onClick={() => setEditModal(null)} style={{ flex: 1, padding: 10, background: "rgba(255,255,255,0.05)", color: "#2a5070", border: "1px solid rgba(255,255,255,0.08)", fontSize: 13 }}>キャンセル</button>
              <button className="btn" onClick={saveEdit} style={{ flex: 2, padding: 10, background: "linear-gradient(135deg,#1555a0,#2299dd)", color: "#fff", fontSize: 14 }}>保存する</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
