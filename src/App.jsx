import { useEffect, useMemo, useState } from "react";
import "./App.css";

const API_BASE_URL = String(import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");
const API_BASE = API_BASE_URL ? `${API_BASE_URL}/api` : "/api";

const VENUES = [
  { id: 1, name: "Kiryu" },
  { id: 2, name: "Toda" },
  { id: 3, name: "Edogawa" },
  { id: 4, name: "Heiwajima" },
  { id: 5, name: "Tamagawa" },
  { id: 6, name: "Hamanako" },
  { id: 7, name: "Gamagori" },
  { id: 8, name: "Tokoname" },
  { id: 9, name: "Tsu" },
  { id: 10, name: "Mikuni" },
  { id: 11, name: "Biwako" },
  { id: 12, name: "Suminoe" },
  { id: 13, name: "Amagasaki" },
  { id: 14, name: "Naruto" },
  { id: 15, name: "Marugame" },
  { id: 16, name: "Kojima" },
  { id: 17, name: "Miyajima" },
  { id: 18, name: "Tokuyama" },
  { id: 19, name: "Shimonoseki" },
  { id: 20, name: "Wakamatsu" },
  { id: 21, name: "Ashiya" },
  { id: 22, name: "Fukuoka" },
  { id: 23, name: "Karatsu" },
  { id: 24, name: "Omura" }
];

const BOAT_META = {
  1: { label: "1", className: "lane-1", text: "white" },
  2: { label: "2", className: "lane-2", text: "black" },
  3: { label: "3", className: "lane-3", text: "red" },
  4: { label: "4", className: "lane-4", text: "blue" },
  5: { label: "5", className: "lane-5", text: "yellow" },
  6: { label: "6", className: "lane-6", text: "green" }
};

async function fetchRaceData(date, venueId, raceNo) {
  const url = new URL(`${API_BASE}/race`);
  url.searchParams.set("date", date);
  url.searchParams.set("venueId", String(venueId));
  url.searchParams.set("raceNo", String(raceNo));

  const response = await fetch(url.toString());
  if (!response.ok) throw new Error("Failed to fetch race data");
  return response.json();
}

async function fetchStatsData() {
  const response = await fetch(`${API_BASE}/stats`);
  if (!response.ok) throw new Error("Failed to fetch stats");
  return response.json();
}

async function fetchHistoryData() {
  const response = await fetch(`${API_BASE}/results-history?limit=100`);
  if (!response.ok) throw new Error("Failed to fetch results history");
  return response.json();
}

async function submitRaceResult(payload) {
  const response = await fetch(`${API_BASE}/race/result`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err?.message || "Failed to save race result");
  }

  return response.json();
}

async function fetchPlacedBets() {
  const response = await fetch(`${API_BASE}/placed-bets`);
  if (!response.ok) throw new Error("Failed to fetch placed bets");
  return response.json();
}

async function fetchPlacedBetSummaries() {
  const response = await fetch(`${API_BASE}/placed-bets/summaries`);
  if (!response.ok) throw new Error("Failed to fetch bet summaries");
  return response.json();
}

async function createPlacedBet(payload) {
  const response = await fetch(`${API_BASE}/placed-bets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err?.message || "Failed to create placed bet");
  }
  return response.json();
}

async function createPlacedBetsBulk(items) {
  const response = await fetch(`${API_BASE}/placed-bets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items })
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err?.message || "Failed to create placed bets");
  }
  return response.json();
}

async function updatePlacedBetApi(id, payload) {
  const response = await fetch(`${API_BASE}/placed-bets/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err?.message || "Failed to update placed bet");
  }
  return response.json();
}

async function deletePlacedBetApi(id) {
  const response = await fetch(`${API_BASE}/placed-bets/${id}`, {
    method: "DELETE"
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err?.message || "Failed to delete placed bet");
  }
  return response.json();
}

async function settlePlacedBets(payload) {
  const response = await fetch(`${API_BASE}/placed-bets/settle`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err?.message || "Failed to settle placed bets");
  }
  return response.json();
}

function formatMaybeNumber(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (Number.isNaN(num)) return String(value);
  return num.toFixed(digits);
}

function getRiskClass(recommendation) {
  if (recommendation === "SKIP") return "risk-skip";
  if (recommendation === "MICRO BET") return "risk-micro";
  if (recommendation === "FULL BET") return "risk-full";
  return "risk-small";
}

function getBetStatusClass(status) {
  if (status === "hit") return "status-hit";
  if (status === "miss") return "status-miss";
  return "status-unsettled";
}

function getProfitClass(value) {
  const num = Number(value || 0);
  if (num > 0) return "profit-positive";
  if (num < 0) return "profit-negative";
  return "profit-neutral";
}

function roundBetTo100(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return 100;
  return Math.max(100, Math.floor(num / 100) * 100);
}

function parseLane(value) {
  if (value === null || value === undefined) return null;
  const asNum = Number(value);
  if (Number.isFinite(asNum)) return asNum;
  const match = String(value).match(/[1-6]/);
  return match ? Number(match[0]) : null;
}

function normalizeCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  return digits.slice(0, 3).join("-");
}

function makeRaceKey({ race_id, race_date, venue_id, race_no }) {
  if (race_id) return String(race_id);
  return `${race_date || "unknown"}_${venue_id || "v"}_${race_no || "r"}`;
}

function splitCombo(combo) {
  return String(combo || "")
    .split("-")
    .map((v) => Number(v))
    .filter((v) => Number.isInteger(v) && v >= 1 && v <= 6);
}

function ComboBadge({ combo }) {
  const lanes = splitCombo(combo);
  if (lanes.length !== 3) return <span>{combo || "-"}</span>;

  return (
    <span className="combo-badge">
      {lanes.map((lane, idx) => (
        <span key={`${combo}-${lane}-${idx}`} className={`combo-dot ${BOAT_META[lane]?.className || ""}`}>
          {lane}
        </span>
      ))}
    </span>
  );
}

function LanePills({ lanes }) {
  const list = Array.isArray(lanes) ? lanes.filter((v) => Number.isInteger(Number(v))) : [];
  if (!list.length) return <span>-</span>;
  return (
    <span className="combo-badge">
      {list.map((lane, idx) => (
        <span key={`${lane}-${idx}`} className={`combo-dot ${BOAT_META[lane]?.className || ""}`}>
          {lane}
        </span>
      ))}
    </span>
  );
}

export default function App() {
  const adminMode = useMemo(() => {
    if (typeof window === "undefined") return false;
    const params = new URLSearchParams(window.location.search);
    return params.get("admin") === "1";
  }, []);
  const [screen, setScreen] = useState("predict");
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [venueId, setVenueId] = useState(1);
  const [raceNo, setRaceNo] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [data, setData] = useState(null);

  const [statsLoading, setStatsLoading] = useState(false);
  const [stats, setStats] = useState(null);
  const [history, setHistory] = useState([]);
  const [perfError, setPerfError] = useState("");

  const [resultForm, setResultForm] = useState({
    raceId: "",
    finish1: "",
    finish2: "",
    finish3: "",
    payoutCombo: "",
    payoutAmount: ""
  });
  const [resultSaving, setResultSaving] = useState(false);
  const [showAdminResultTool, setShowAdminResultTool] = useState(false);

  const [journalLoading, setJournalLoading] = useState(false);
  const [journalError, setJournalError] = useState("");
  const [journalNotice, setJournalNotice] = useState("");
  const [placedBets, setPlacedBets] = useState([]);
  const [betSummaries, setBetSummaries] = useState(null);
  const [betSaving, setBetSaving] = useState(false);
  const [settlingRaceId, setSettlingRaceId] = useState("");
  const [editingBetId, setEditingBetId] = useState(null);
  const [editingDraft, setEditingDraft] = useState({ combo: "", bet_amount: "", memo: "" });
  const [journalForm, setJournalForm] = useState({
    race_id: "",
    race_date: new Date().toISOString().slice(0, 10),
    venue_id: 1,
    race_no: 1,
    combo: "",
    bet_amount: 100,
    memo: ""
  });
  const [builderSlots, setBuilderSlots] = useState({ first: null, second: null, third: null });
  const [quickBetAmount, setQuickBetAmount] = useState(100);
  const [pendingTickets, setPendingTickets] = useState([]);
  const [journalFilter, setJournalFilter] = useState("all");

  const laneButtons = useMemo(
    () => [
      { lane: 1, className: "lane-btn lane-1", label: "1" },
      { lane: 2, className: "lane-btn lane-2", label: "2" },
      { lane: 3, className: "lane-btn lane-3", label: "3" },
      { lane: 4, className: "lane-btn lane-4", label: "4" },
      { lane: 5, className: "lane-btn lane-5", label: "5" },
      { lane: 6, className: "lane-btn lane-6", label: "6" }
    ],
    []
  );

  const venueName = useMemo(() => VENUES.find((v) => v.id === Number(venueId))?.name || "-", [venueId]);

  const race = data?.race || {};
  const racers = Array.isArray(data?.racers) ? data.racers : [];
  const prediction = data?.prediction || {};
  const ranking = Array.isArray(prediction?.ranking) ? prediction.ranking : [];
  const top3 = Array.isArray(prediction?.top3) ? prediction.top3 : [];
  const evBets = Array.isArray(data?.ev_analysis?.best_ev_bets) ? data.ev_analysis.best_ev_bets.slice(0, 3) : [];
  const recommendedBets = Array.isArray(data?.bet_plan?.recommended_bets) ? data.bet_plan.recommended_bets : [];
  const oddsData = data?.oddsData || {};
  const trifectaOddsList = Array.isArray(oddsData?.trifecta) ? oddsData.trifecta : [];
  const exactaOddsList = Array.isArray(oddsData?.exacta) ? oddsData.exacta : [];
  const aiEnhancement = data?.aiEnhancement || {};
  const raceRisk = data?.raceRisk || {};
  const probabilities = Array.isArray(data?.probabilities) ? data.probabilities : [];
  const raceOutcomeProbabilities = data?.raceOutcomeProbabilities || {};
  const raceIndexes = data?.raceIndexes || {};
  const ticketStrategy = data?.ticketStrategy || {};
  const preRaceAnalysis = data?.preRaceAnalysis || data?.preRaceForm || {};
  const headSelection = data?.headSelection || {};
  const partnerSelection = data?.partnerSelection || {};
  const roleCandidates = data?.roleCandidates || {};
  const raceStructure = data?.raceStructure || {};
  const wallEvaluation = data?.wallEvaluation || {};
  const headConfidence = data?.headConfidence || {};
  const ticketGenerationV2 = data?.ticketGenerationV2 || {};
  const ticketOptimization = data?.ticketOptimization || {};
  const raceDecision = data?.raceDecision || {};
  const skipReasonCodes = Array.isArray(raceRisk?.skip_reason_codes) ? raceRisk.skip_reason_codes : [];

  const racersByLane = useMemo(() => {
    const map = new Map();
    racers.forEach((r) => {
      const lane = Number(r?.lane);
      if (Number.isFinite(lane)) map.set(lane, r);
    });
    return map;
  }, [racers]);

  const normalizedRanking = useMemo(
    () =>
      ranking.map((row, idx) => {
        const lane = parseLane(row?.lane ?? row?.boatNo ?? row?.teiban ?? row?.course ?? row?.entryCourse);
        const fromRace = racersByLane.get(lane) || racers.find((r) => parseLane(r?.lane) === idx + 1) || {};
        return {
          rank: row?.rank,
          lane: Number.isFinite(lane) ? lane : parseLane(fromRace?.lane),
          name: row?.name ?? row?.racerName ?? row?.playerName ?? fromRace?.name ?? null,
          class: row?.class ?? row?.grade ?? row?.racerClass ?? fromRace?.class ?? null,
          score: row?.score
        };
      }),
    [ranking, racersByLane, racers]
  );

  const probabilityByCombo = useMemo(() => {
    const map = new Map();
    evBets.forEach((b) => {
      const prob = Number(b?.prob);
      if (b?.combo && Number.isFinite(prob)) map.set(b.combo, prob);
    });
    probabilities.forEach((b) => {
      const prob = Number(b?.p ?? b?.prob);
      if (b?.combo && Number.isFinite(prob) && !map.has(b.combo)) map.set(b.combo, prob);
    });
    return map;
  }, [evBets, probabilities]);

  const oddsByCombo = useMemo(() => {
    const map = new Map();
    trifectaOddsList.forEach((row) => {
      const odds = Number(row?.odds);
      if (row?.combo && Number.isFinite(odds)) map.set(String(row.combo), odds);
    });
    return map;
  }, [trifectaOddsList]);

  const recommendedBetsByProb = useMemo(
    () =>
      recommendedBets
        .map((bet) => {
          const prob = probabilityByCombo.get(bet?.combo);
          const evSource = evBets.find((e) => e?.combo === bet?.combo);
          return {
            ...bet,
            prob: Number.isFinite(prob) ? prob : null,
            ev: evSource?.ev,
            odds: oddsByCombo.get(bet?.combo) ?? null,
            roundedBet: roundBetTo100(bet?.bet)
          };
        })
        .sort((a, b) => (Number.isFinite(b?.prob) ? b.prob : -1) - (Number.isFinite(a?.prob) ? a.prob : -1)),
    [recommendedBets, probabilityByCombo, evBets, oddsByCombo]
  );
  const simulatedCombos = useMemo(
    () => (Array.isArray(data?.simulation?.top_combinations) ? data.simulation.top_combinations.slice(0, 5) : []),
    [data]
  );

  const currentRaceKey = useMemo(
    () =>
      makeRaceKey({
        race_id: journalForm.race_id,
        race_date: journalForm.race_date,
        venue_id: journalForm.venue_id,
        race_no: journalForm.race_no
      }),
    [journalForm.race_id, journalForm.race_date, journalForm.venue_id, journalForm.race_no]
  );

  const pendingTicketsForCurrentRace = useMemo(
    () => pendingTickets.filter((t) => t.raceKey === currentRaceKey),
    [pendingTickets, currentRaceKey]
  );
  const builderCombo = useMemo(() => {
    const lanes = [builderSlots.first, builderSlots.second, builderSlots.third];
    if (lanes.some((v) => !Number.isInteger(v))) return "";
    if (new Set(lanes).size !== 3) return "";
    return lanes.join("-");
  }, [builderSlots]);

  useEffect(() => {
    if (!journalNotice) return;
    const timer = setTimeout(() => setJournalNotice(""), 1800);
    return () => clearTimeout(timer);
  }, [journalNotice]);

  const loadPerformance = async () => {
    setStatsLoading(true);
    setPerfError("");
    try {
      const [statsData, historyData] = await Promise.all([fetchStatsData(), fetchHistoryData()]);
      setStats(statsData);
      setHistory(Array.isArray(historyData?.items) ? historyData.items : []);
    } catch (e) {
      setPerfError(e.message || "Failed to load performance data");
    } finally {
      setStatsLoading(false);
    }
  };

  const loadJournal = async () => {
    setJournalLoading(true);
    setJournalError("");
    try {
      const [betsData, summaryData] = await Promise.all([
        fetchPlacedBets(),
        fetchPlacedBetSummaries()
      ]);
      setPlacedBets(Array.isArray(betsData?.items) ? betsData.items : []);
      setBetSummaries(summaryData || null);
    } catch (e) {
      setJournalError(e.message || "Failed to load bet journal");
    } finally {
      setJournalLoading(false);
    }
  };

  useEffect(() => {
    if (screen === "performance") {
      loadPerformance();
    }
  }, [screen]);

  useEffect(() => {
    if (screen === "journal") {
      loadJournal();
    }
  }, [screen]);

  const onFetch = async () => {
    setLoading(true);
    setError("");
    try {
      const result = await fetchRaceData(date, venueId, raceNo);
      setData(result);
      setResultForm((prev) => ({ ...prev, raceId: result?.raceId || prev.raceId }));
    } catch (e) {
      setError(e.message || "Failed to fetch race data");
    } finally {
      setLoading(false);
    }
  };

  const onSubmitResult = async () => {
    if (!resultForm.raceId) {
      setPerfError("raceId is required");
      return;
    }

    const finishOrder = [resultForm.finish1, resultForm.finish2, resultForm.finish3].map((v) => Number(v));
    if (finishOrder.some((v) => !Number.isInteger(v) || v < 1 || v > 6) || new Set(finishOrder).size !== 3) {
      setPerfError("Finish order must be 3 unique lanes (1-6)");
      return;
    }

    const payoutByCombo = {};
    if (resultForm.payoutCombo && resultForm.payoutAmount) {
      payoutByCombo[resultForm.payoutCombo] = Number(resultForm.payoutAmount);
    }

    const predictedBets = recommendedBetsByProb.map((b) => ({ combo: b.combo, bet: b.roundedBet }));

    setResultSaving(true);
    setPerfError("");
    try {
      await submitRaceResult({
        raceId: resultForm.raceId,
        finishOrder,
        predictedBets,
        payoutByCombo
      });
      await loadPerformance();
      setResultForm((prev) => ({ ...prev, finish1: "", finish2: "", finish3: "", payoutCombo: "", payoutAmount: "" }));
    } catch (e) {
      setPerfError(e.message || "Failed to save result");
    } finally {
      setResultSaving(false);
    }
  };

  const onBuilderSlotClick = (slot, lane) => {
    setBuilderSlots((prev) => {
      const next = { ...prev };
      next[slot] = prev[slot] === lane ? null : lane;
      const values = [next.first, next.second, next.third].filter((v) => Number.isInteger(v));
      if (values.length === 3 && new Set(values).size !== 3) {
        setJournalError("1st/2nd/3rd must be different lanes.");
      } else {
        setJournalError("");
      }
      return next;
    });
  };

  const upsertPendingTicket = (ticket) => {
    const combo = normalizeCombo(ticket?.combo);
    if (!combo || combo.split("-").length !== 3) return;
    const rounded = roundBetTo100(ticket?.bet_amount ?? journalForm.bet_amount);
    const raceContext = {
      race_id: ticket?.race_id || journalForm.race_id || "",
      race_date: ticket?.race_date || journalForm.race_date,
      venue_id: Number(ticket?.venue_id ?? journalForm.venue_id),
      race_no: Number(ticket?.race_no ?? journalForm.race_no)
    };
    const raceKey = makeRaceKey(raceContext);
    setPendingTickets((prev) => {
      const idx = prev.findIndex((x) => x.raceKey === raceKey && x.combo === combo);
      const nextTicket = {
        ...raceContext,
        raceKey,
        combo,
        bet_amount: rounded,
        memo: ticket?.memo ?? journalForm.memo ?? "",
        prob: Number.isFinite(Number(ticket?.prob)) ? Number(ticket.prob) : null,
        ev: Number.isFinite(Number(ticket?.ev)) ? Number(ticket.ev) : null,
        odds: Number.isFinite(Number(ticket?.odds)) ? Number(ticket.odds) : null
      };
      if (idx >= 0) {
        const existing = prev[idx];
        if (Number(existing.bet_amount) === rounded) {
          setJournalNotice("Duplicate ticket skipped");
          return prev;
        }
        const copied = [...prev];
        copied[idx] = { ...existing, bet_amount: rounded };
        setJournalNotice("Ticket amount updated");
        return copied;
      }
      setJournalNotice("ベット記録に追加しました");
      return [...prev, nextTicket];
    });
  };

  const onAddPendingTicket = () => {
    const combo = normalizeCombo(journalForm.combo) || builderCombo;
    if (!combo || combo.split("-").length !== 3) {
      setJournalError("Please build a valid 3-lane combo before adding.");
      return;
    }
    upsertPendingTicket({
      combo,
      bet_amount: journalForm.bet_amount,
      memo: journalForm.memo
    });
    setJournalError("");
    setBuilderSlots({ first: null, second: null, third: null });
    setJournalForm((prev) => ({ ...prev, combo: "", memo: "" }));
  };

  const onRemovePendingTicket = (raceKey, combo) => {
    setPendingTickets((prev) => prev.filter((x) => !(x.raceKey === raceKey && x.combo === combo)));
  };

  const onUpdatePendingTicket = (raceKey, combo, nextAmount) => {
    const rounded = roundBetTo100(nextAmount);
    setPendingTickets((prev) =>
      prev.map((x) => (x.raceKey === raceKey && x.combo === combo ? { ...x, bet_amount: rounded } : x))
    );
  };

  const onSavePendingTickets = async () => {
    let tickets = [...pendingTicketsForCurrentRace];
    const combo = normalizeCombo(journalForm.combo);
    if (tickets.length === 0 && combo && combo.split("-").length === 3) {
      tickets = [
        {
          combo,
          bet_amount: roundBetTo100(journalForm.bet_amount),
          memo: journalForm.memo || ""
        }
      ];
    }

    if (!tickets.length) {
      setJournalError("Add at least one ticket before saving.");
      return;
    }

    setBetSaving(true);
    setJournalError("");
    try {
      await createPlacedBetsBulk(
        tickets.map((t) => ({
          race_id: t.race_id || journalForm.race_id || undefined,
          race_date: t.race_date || journalForm.race_date,
          venue_id: Number(t.venue_id ?? journalForm.venue_id),
          race_no: Number(t.race_no ?? journalForm.race_no),
          combo: t.combo,
          bet_amount: roundBetTo100(t.bet_amount),
          bought_odds: Number.isFinite(Number(t.odds)) ? Number(t.odds) : null,
          memo: t.memo
        }))
      );
      await loadJournal();
      await loadPerformance();
      setPendingTickets((prev) => prev.filter((x) => x.raceKey !== currentRaceKey));
      setBuilderSlots({ first: null, second: null, third: null });
      setJournalForm((prev) => ({
        ...prev,
        combo: "",
        bet_amount: 100,
        memo: ""
      }));
      setJournalNotice("ベット記録に保存しました");
    } catch (e) {
      setJournalError(e.message || "Failed to save placed bets");
    } finally {
      setBetSaving(false);
    }
  };

  const onUsePredictedTicket = (bet) => {
    const combo = normalizeCombo(bet?.combo);
    if (!combo || combo.split("-").length !== 3) return;

    const selectedRaceDate = race.date || date;
    const selectedVenueId = Number(race.venueId ?? venueId);
    const selectedRaceNo = Number(race.raceNo ?? raceNo);
    const selectedRaceId =
      data?.raceId ||
      `${String(selectedRaceDate || "").replace(/-/g, "")}_${selectedVenueId}_${selectedRaceNo}`;

    const defaultAmount = roundBetTo100(bet?.roundedBet ?? bet?.bet ?? 100);
    setQuickBetAmount(defaultAmount);

    setJournalForm((prev) => ({
      ...prev,
      race_id: selectedRaceId,
      race_date: selectedRaceDate,
      venue_id: selectedVenueId,
      race_no: selectedRaceNo,
      combo,
      bet_amount: defaultAmount
    }));
    const [a, b, c] = combo.split("-").map((v) => Number(v));
    setBuilderSlots({ first: a, second: b, third: c });
    upsertPendingTicket({
      race_id: selectedRaceId,
      race_date: selectedRaceDate,
      venue_id: selectedVenueId,
      race_no: selectedRaceNo,
      combo,
      bet_amount: defaultAmount,
      prob: bet?.prob,
      ev: bet?.ev,
      odds: bet?.odds
    });
  };

  const onStartEditBet = (bet) => {
    setEditingBetId(bet.id);
    setEditingDraft({
      combo: bet.combo || "",
      bet_amount: bet.bet_amount ?? "",
      memo: bet.memo || ""
    });
  };

  const onSaveEditBet = async (id) => {
    try {
      await updatePlacedBetApi(id, {
        combo: editingDraft.combo,
        bet_amount: Number(editingDraft.bet_amount),
        memo: editingDraft.memo
      });
      setEditingBetId(null);
      await loadJournal();
    } catch (e) {
      setJournalError(e.message || "Failed to update bet");
    }
  };

  const onDeleteBet = async (id) => {
    const ok = window.confirm("Delete this ticket?");
    if (!ok) return;
    try {
      await deletePlacedBetApi(id);
      await loadJournal();
    } catch (e) {
      setJournalError(e.message || "Failed to delete bet");
    }
  };

  const onSettleRace = async (group) => {
    const raceId = group?.raceId;
    setSettlingRaceId(String(raceId));
    setJournalError("");
    try {
      await settlePlacedBets({
        race_id: raceId,
        race_date: group?.raceDate,
        venue_id: group?.venueId,
        race_no: group?.raceNo
      });
      await loadJournal();
      await loadPerformance();
    } catch (e) {
      setJournalError(e.message || "Failed to settle race");
    } finally {
      setSettlingRaceId("");
    }
  };

  const groupedPlacedBets = useMemo(() => {
    const groups = new Map();
    for (const bet of placedBets) {
      const fallbackKey = `${bet.race_date || "unknown"}_${bet.venue_id || "v"}_${bet.race_no || "r"}`;
      const key = bet.race_id || fallbackKey;
      const list = groups.get(key) || [];
      list.push(bet);
      groups.set(key, list);
    }
    return [...groups.entries()].map(([raceId, bets]) => {
      const first = bets[0] || {};
      const totals = bets.reduce(
        (acc, b) => {
          acc.bet += Number(b.bet_amount || 0);
          acc.payout += Number(b.payout || 0);
          acc.pl += Number(b.profit_loss || 0);
          return acc;
        },
        { bet: 0, payout: 0, pl: 0 }
      );
      const unsettled = bets.some((b) => b.status === "unsettled");
      const hitCount = bets.filter((b) => b.status === "hit").length;
      const missCount = bets.filter((b) => b.status === "miss").length;
      return {
        raceId,
        raceDate: first.race_date,
        venueId: first.venue_id,
        raceNo: first.race_no,
        raceIdText: first.race_id || raceId,
        bets,
        totals,
        unsettled,
        hitCount,
        missCount
      };
    }).sort((a, b) => String(b.raceDate || "").localeCompare(String(a.raceDate || "")));
  }, [placedBets]);

  const filteredGroupedBets = useMemo(() => {
    const now = new Date();
    const today = now.toISOString().slice(0, 10);
    const weekAgo = new Date(now);
    weekAgo.setDate(now.getDate() - 7);
    const weekAgoStr = weekAgo.toISOString().slice(0, 10);
    const monthStart = `${today.slice(0, 7)}-01`;

    return groupedPlacedBets.filter((group) => {
      const raceDate = String(group.raceDate || "");
      if (journalFilter === "all") return true;
      if (journalFilter === "today") return raceDate === today;
      if (journalFilter === "week") return raceDate >= weekAgoStr;
      if (journalFilter === "month") return raceDate >= monthStart;
      if (journalFilter === "unsettled") return group.unsettled;
      if (journalFilter === "hits") return group.hitCount > 0;
      if (journalFilter === "misses") return group.missCount > 0;
      return true;
    });
  }, [groupedPlacedBets, journalFilter]);

  const allTimeSummary = useMemo(() => {
    return placedBets.reduce(
      (acc, b) => {
        const bet = Number(b.bet_amount || 0);
        const payout = Number(b.payout || 0);
        const pl = Number(b.profit_loss || 0);
        acc.total_bet_amount += bet;
        acc.total_payout += payout;
        acc.total_profit_loss += pl;
        if (b.status === "hit") acc.hit_count += 1;
        if (b.status === "miss") acc.miss_count += 1;
        return acc;
      },
      { total_bet_amount: 0, total_payout: 0, total_profit_loss: 0, hit_count: 0, miss_count: 0 }
    );
  }, [placedBets]);

  useEffect(() => {
    const onKeyDown = (e) => {
      const target = e.target;
      const tag = target?.tagName?.toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select") return;
      if (e.ctrlKey || e.metaKey || e.altKey) return;

      const k = String(e.key || "").toLowerCase();
      if (k === "a") {
        e.preventDefault();
        onAddPendingTicket();
      } else if (k === "s") {
        e.preventDefault();
        onSavePendingTickets();
      } else if (k === "r") {
        e.preventDefault();
        const targetGroup = filteredGroupedBets.find((g) => g.unsettled);
        if (targetGroup) onSettleRace(targetGroup);
      } else if (k === "d") {
        e.preventDefault();
        const last = pendingTicketsForCurrentRace[pendingTicketsForCurrentRace.length - 1];
        if (last) onRemovePendingTicket(last.raceKey, last.combo);
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [
    filteredGroupedBets,
    pendingTicketsForCurrentRace,
    onAddPendingTicket,
    onSavePendingTickets,
    onSettleRace,
    onRemovePendingTicket
  ]);

  return (
    <div className="app-shell">
      <div className="app-container">
        <section className="topbar card">
          <div>
            <h1>ボートレース予想ダッシュボード</h1>
            <p>予想・投票記録・精算を1画面で管理</p>
          </div>
          <div className="screen-tabs">
            <button className={screen === "predict" ? "tab on" : "tab"} onClick={() => setScreen("predict")}>予想</button>
            <button className={screen === "performance" ? "tab on" : "tab"} onClick={() => setScreen("performance")}>実績</button>
            <button className={screen === "journal" ? "tab on" : "tab"} onClick={() => setScreen("journal")}>ベット記録</button>
          </div>
        </section>

        {screen === "predict" && (
          <>
            <section className="card">
              <div className="controls-grid">
                <label><span>日付</span><input type="date" value={date} onChange={(e) => setDate(e.target.value)} /></label>
                <label><span>場</span><select value={venueId} onChange={(e) => setVenueId(Number(e.target.value))}>{VENUES.map((v) => <option key={v.id} value={v.id}>{v.id} - {v.name}</option>)}</select></label>
                <label><span>レース</span><select value={raceNo} onChange={(e) => setRaceNo(Number(e.target.value))}>{Array.from({ length: 12 }, (_, i) => i + 1).map((n) => <option key={n} value={n}>{n}R</option>)}</select></label>
                <button className="fetch-btn" onClick={onFetch} disabled={loading}>{loading ? "取得中..." : "予想を取得"}</button>
              </div>
            </section>

            {error && <div className="error-banner">{error}</div>}
            {journalNotice && <div className="notice-banner">{journalNotice}</div>}

            {!data ? (
              <section className="card empty-state">レースを取得すると予想ダッシュボードを表示します。</section>
            ) : (
              <>
                <section className="card">
                  <h2>レース情報</h2>
                  <div className="metric-grid">
                    <div className="metric-item"><span>日付</span><strong>{race.date || date}</strong></div>
                    <div className="metric-item"><span>場</span><strong>{race.venueId ?? venueId} ({venueName})</strong></div>
                    <div className="metric-item"><span>レース</span><strong>{race.raceNo ?? raceNo}R</strong></div>
                    <div className="metric-item"><span>天候</span><strong>{race.weather || "-"}</strong></div>
                    <div className="metric-item"><span>風速</span><strong>{race.windSpeed ?? "-"}</strong></div>
                    <div className="metric-item"><span>波高</span><strong>{race.waveHeight ?? "-"}</strong></div>
                  </div>
                </section>

                <section className={`card recommendation ${getRiskClass(raceRisk.recommendation)}`}>
                  <h2>総合推奨</h2>
                  <div className="recommend-grid">
                    <div><span>展開パターン</span><strong>{data.racePattern || "-"}</strong></div>
                    <div><span>買いタイプ</span><strong>{data.buyType || "-"}</strong></div>
                    <div><span>推奨</span><strong className={`status-pill ${getRiskClass(raceRisk.recommendation)}`}>{raceRisk.recommendation || "-"}</strong></div>
                    <div><span>リスクスコア</span><strong>{raceRisk.risk_score ?? "-"}</strong></div>
                    <div><span>参加モード</span><strong>{raceRisk.participation_mode || "-"}</strong></div>
                  </div>
                </section>

                <section className="analysis-grid">
                  <article className="card analysis-card">
                    <h2>決着確率</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>逃げ成功</span><strong>{formatMaybeNumber((raceOutcomeProbabilities.escape_success_prob ?? 0) * 100, 1)}%</strong></div>
                      <div className="kv-row"><span>差し成功</span><strong>{formatMaybeNumber((raceOutcomeProbabilities.sashi_success_prob ?? 0) * 100, 1)}%</strong></div>
                      <div className="kv-row"><span>まくり成功</span><strong>{formatMaybeNumber((raceOutcomeProbabilities.makuri_success_prob ?? 0) * 100, 1)}%</strong></div>
                    </div>
                  </article>

                  <article className="card analysis-card">
                    <h2>レース指数</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>逃げ指数</span><strong>{formatMaybeNumber(raceIndexes.nige_index, 2)}</strong></div>
                      <div className="kv-row"><span>差し指数</span><strong>{formatMaybeNumber(raceIndexes.sashi_index, 2)}</strong></div>
                      <div className="kv-row"><span>まくり指数</span><strong>{formatMaybeNumber(raceIndexes.makuri_index, 2)}</strong></div>
                      <div className="kv-row"><span>荒れ指数</span><strong>{formatMaybeNumber(raceIndexes.are_index, 2)}</strong></div>
                    </div>
                    <p className="muted strategy-line">{raceIndexes.index_summary || "-"}</p>
                    <p className="muted">推奨スタイル: <strong>{raceIndexes.recommended_style || "-"}</strong></p>
                  </article>

                  <article className={`card analysis-card risk-detail ${getRiskClass(raceRisk.recommendation)}`}>
                    <h2>リスク判定</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>判定</span><strong>{raceRisk.recommendation || "-"}</strong></div>
                      <div className="kv-row"><span>リスクスコア</span><strong>{formatMaybeNumber(raceRisk.risk_score, 2)}</strong></div>
                      <div className="kv-row"><span>見送り信頼度</span><strong>{formatMaybeNumber((raceRisk.skip_confidence ?? 0) * 100, 1)}%</strong></div>
                      <div className="kv-row"><span>危険タイプ</span><strong>{raceRisk.danger_type || "-"}</strong></div>
                    </div>
                    <div className="chips-wrap">
                      {skipReasonCodes.length === 0 ? <span className="chip">特記事項なし</span> : skipReasonCodes.map((code) => <span className="chip" key={code}>{code}</span>)}
                    </div>
                    <p className="muted strategy-line">{raceRisk.skip_summary || "-"}</p>
                  </article>

                  <article className="card analysis-card">
                    <h2>レース判定AI</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>mode</span><strong>{raceDecision.mode || "-"}</strong></div>
                      <div className="kv-row"><span>confidence</span><strong>{formatMaybeNumber(raceDecision.confidence, 2)}</strong></div>
                      <div className="kv-row"><span>race_select_score</span><strong>{formatMaybeNumber(raceDecision.race_select_score, 2)}</strong></div>
                    </div>
                    <div className="chips-wrap">
                      {(raceDecision.reason_codes || []).length === 0
                        ? <span className="chip">NO_REASON</span>
                        : (raceDecision.reason_codes || []).map((code) => <span className="chip" key={`rd-${code}`}>{code}</span>)}
                    </div>
                    <p className="muted strategy-line">{raceDecision.summary || "-"}</p>
                  </article>

                  <article className="card analysis-card">
                    <h2>チケット戦略</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>頭戦略</span><strong>{ticketStrategy.head_strategy || "-"}</strong></div>
                      <div className="kv-row"><span>カバー範囲</span><strong>{ticketStrategy.coverage_level || "-"}</strong></div>
                    </div>
                    <p className="muted strategy-line">{ticketStrategy.strategy_summary || "-"}</p>
                  </article>

                  <article className="card analysis-card">
                    <h2>頭・相手選定</h2>
                    <div className="kv-list">
                      <div className="kv-row">
                        <span>頭本命</span>
                        <strong><LanePills lanes={[Number(headSelection?.main_head)]} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>頭対抗</span>
                        <strong><LanePills lanes={headSelection?.secondary_heads || []} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>相手本線</span>
                        <strong><LanePills lanes={partnerSelection?.main_partners || []} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>押さえ</span>
                        <strong><LanePills lanes={partnerSelection?.backup_partners || []} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>消し</span>
                        <strong><LanePills lanes={partnerSelection?.fade_lanes || []} /></strong>
                      </div>
                    </div>
                    <div className="win-prob-list">
                      {Object.entries(headSelection?.win_prob_by_lane || {})
                        .map(([lane, prob]) => ({ lane: Number(lane), prob: Number(prob) }))
                        .sort((a, b) => a.lane - b.lane)
                        .map((row) => (
                          <div key={`win-${row.lane}`} className="win-prob-row">
                            <span className={`combo-dot ${BOAT_META[row.lane]?.className || ""}`}>{row.lane}</span>
                            <span>{(row.prob * 100).toFixed(1)}%</span>
                          </div>
                        ))}
                    </div>
                  </article>

                  <article className="card analysis-card">
                    <h2>直前気配</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>exhibition_quality_score</span><strong>{formatMaybeNumber(preRaceAnalysis.exhibition_quality_score, 2)}</strong></div>
                      <div className="kv-row"><span>entry_advantage_score</span><strong>{formatMaybeNumber(preRaceAnalysis.entry_advantage_score, 2)}</strong></div>
                      <div className="kv-row"><span>pre_race_form_score</span><strong>{formatMaybeNumber(preRaceAnalysis.pre_race_form_score, 2)}</strong></div>
                      <div className="kv-row"><span>wind_risk_score</span><strong>{formatMaybeNumber(preRaceAnalysis.wind_risk_score, 2)}</strong></div>
                    </div>
                    <p className="muted strategy-line">{preRaceAnalysis.summary || "-"}</p>
                  </article>

                  <article className="card analysis-card">
                    <h2>役割候補</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>頭候補</span><strong><LanePills lanes={roleCandidates.head_candidates || []} /></strong></div>
                      <div className="kv-row"><span>2着候補</span><strong><LanePills lanes={roleCandidates.second_candidates || []} /></strong></div>
                      <div className="kv-row"><span>3着候補</span><strong><LanePills lanes={roleCandidates.third_candidates || []} /></strong></div>
                      <div className="kv-row"><span>消し</span><strong><LanePills lanes={roleCandidates.fade_lanes || []} /></strong></div>
                    </div>
                    <p className="muted strategy-line">{roleCandidates.summary || "-"}</p>
                  </article>

                  <article className="card analysis-card">
                    <h2>レース構造</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>head_stability_score</span><strong>{formatMaybeNumber(raceStructure.head_stability_score, 2)}</strong></div>
                      <div className="kv-row"><span>top3_concentration_score</span><strong>{formatMaybeNumber(raceStructure.top3_concentration_score, 2)}</strong></div>
                      <div className="kv-row"><span>chaos_risk_score</span><strong>{formatMaybeNumber(raceStructure.chaos_risk_score, 2)}</strong></div>
                      <div className="kv-row"><span>race_structure_score</span><strong>{formatMaybeNumber(raceStructure.race_structure_score, 2)}</strong></div>
                    </div>
                    <p className="muted strategy-line">{raceStructure.summary || "-"}</p>
                  </article>

                  <article className="card analysis-card">
                    <h2>2コース壁評価</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>壁強度</span><strong>{formatMaybeNumber(wallEvaluation.wall_strength, 2)}</strong></div>
                      <div className="kv-row"><span>壁突破リスク</span><strong>{formatMaybeNumber(wallEvaluation.wall_break_risk, 2)}</strong></div>
                    </div>
                    <p className="muted strategy-line">{wallEvaluation.summary || "-"}</p>
                  </article>

                  <article className="card analysis-card">
                    <h2>頭信頼度</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>頭信頼度</span><strong>{formatMaybeNumber((headConfidence.head_confidence ?? 0) * 100, 1)}%</strong></div>
                      <div className="kv-row"><span>頭固定可否</span><strong>{headConfidence.head_fixed_ok ? "固定向き" : "固定注意"}</strong></div>
                      <div className="kv-row"><span>分散必要性</span><strong>{headConfidence.head_spread_needed ? "分散推奨" : "絞り可"}</strong></div>
                    </div>
                    <p className="muted strategy-line">{headConfidence.summary || "-"}</p>
                  </article>

                  <article className="card analysis-card">
                    <h2>的中重視スコア</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>hit_mode_score</span><strong>{formatMaybeNumber(aiEnhancement.hit_mode_score, 2)}</strong></div>
                      <div className="kv-row"><span>solid_ticket_score</span><strong>{formatMaybeNumber(aiEnhancement.solid_ticket_score, 2)}</strong></div>
                      <div className="kv-row"><span>inner_reliability_score</span><strong>{formatMaybeNumber(aiEnhancement.inner_reliability_score, 2)}</strong></div>
                      <div className="kv-row"><span>odds_adjusted_ticket_score</span><strong>{formatMaybeNumber(aiEnhancement.odds_adjusted_ticket_score, 2)}</strong></div>
                    </div>
                  </article>
                </section>

                <section className="dashboard-grid">
                  <article className="card">
                    <h2>EV上位買い目</h2>
                    <div className="list-stack">
                      {evBets.map((bet, idx) => (
                        <div key={`${bet.combo}-${idx}`} className="list-row list-row-actions">
                          <strong><ComboBadge combo={bet.combo} /></strong>
                          <span>p {formatMaybeNumber(bet.prob, 3)}</span>
                          <span>odds {formatMaybeNumber(bet.odds, 1)}</span>
                          <span>ev {formatMaybeNumber(bet.ev, 2)}</span>
                          <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket(bet)}>
                            記録に追加
                          </button>
                        </div>
                      ))}
                    </div>
                  </article>

                  <article className="card">
                    <h2>推奨買い目（確率順）</h2>
                    <div className="list-stack">
                      {recommendedBetsByProb.map((bet, idx) => (
                        <div key={`${bet.combo}-${idx}`} className="list-row list-row-actions">
                          <strong><ComboBadge combo={bet.combo} /></strong>
                          <span>p {Number.isFinite(bet.prob) ? formatMaybeNumber(bet.prob, 3) : "-"}</span>
                          <span>odds {Number.isFinite(bet.odds) ? formatMaybeNumber(bet.odds, 1) : "-"}</span>
                          <span>ev {formatMaybeNumber(bet.ev, 2)}</span>
                          <span>金額 JPY {bet.roundedBet.toLocaleString()}</span>
                          <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket(bet)}>
                            記録に追加
                          </button>
                        </div>
                      ))}
                    </div>
                  </article>

                  <article className="card">
                    <h2>オッズ取得</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>取得時刻</span><strong>{oddsData?.fetched_at ? new Date(oddsData.fetched_at).toLocaleString() : "-"}</strong></div>
                      <div className="kv-row"><span>3連単件数</span><strong>{trifectaOddsList.length}</strong></div>
                      <div className="kv-row"><span>2連単件数</span><strong>{exactaOddsList.length}</strong></div>
                    </div>
                    <div className="list-stack">
                      {trifectaOddsList.slice(0, 5).map((row, idx) => (
                        <div key={`odds3-${idx}`} className="list-row">
                          <strong><ComboBadge combo={row.combo} /></strong>
                          <span>odds {formatMaybeNumber(row.odds, 1)}</span>
                          <span>-</span>
                          <span>-</span>
                        </div>
                      ))}
                    </div>
                  </article>

                  <article className="card">
                    <h2>最適化チケット</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>ticket_confidence_score</span><strong>{formatMaybeNumber(ticketOptimization.ticket_confidence_score, 2)}</strong></div>
                      <div className="kv-row"><span>odds_adjusted_ticket_score</span><strong>{formatMaybeNumber(ticketOptimization.odds_adjusted_ticket_score, 2)}</strong></div>
                      <div className="kv-row"><span>value_warning</span><strong>{ticketOptimization.value_warning ? "true" : "false"}</strong></div>
                      <div className="kv-row"><span>budget_split</span><strong>{ticketOptimization.recommended_budget_split ? `${Math.round((ticketOptimization.recommended_budget_split.primary || 0) * 100)} / ${Math.round((ticketOptimization.recommended_budget_split.secondary || 0) * 100)}` : "-"}</strong></div>
                    </div>
                    <div className="list-stack">
                      {(ticketOptimization.optimized_tickets || []).slice(0, 6).map((row, idx) => (
                        <div key={`opt-${row.combo}-${idx}`} className="list-row list-row-actions">
                          <strong><ComboBadge combo={row.combo} /></strong>
                          <span>p {formatMaybeNumber(row.prob, 3)}</span>
                          <span>odds {formatMaybeNumber(row.odds, 1)}</span>
                          <span>ev {formatMaybeNumber(row.ev, 2)}</span>
                          <span>conf {formatMaybeNumber(row.ticket_confidence_score, 1)}</span>
                          <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket({ combo: row.combo, prob: row.prob, odds: row.odds, ev: row.ev, bet: row.recommended_bet })}>
                            記録に追加
                          </button>
                        </div>
                      ))}
                    </div>
                  </article>

                  <article className="card ranking-card">
                    <h2>AI総合評価ランキング</h2>
                    <p className="top3">予想される上位着順: {top3.length ? top3.join("-") : "-"}</p>
                    <div className="list-stack">
                      {normalizedRanking.map((racer, idx) => (
                        <div key={`${racer.lane}-${idx}`} className="list-row ranking-row">
                          <span>#{racer.rank ?? idx + 1}</span>
                          <span className={`combo-dot ${BOAT_META[racer.lane]?.className || ""}`}>{racer.lane ?? "-"}</span>
                          <span>{racer.name ?? "-"}</span>
                          <span>{racer.class ?? "-"}</span>
                          <strong>{formatMaybeNumber(racer.score, 2)}</strong>
                        </div>
                      ))}
                    </div>
                  </article>

                  {simulatedCombos.length > 0 && (
                    <article className="card">
                      <h2>シミュレーション上位</h2>
                      <div className="list-stack">
                        {simulatedCombos.map((row, idx) => (
                          <div key={`${row.combo}-${idx}`} className="list-row list-row-actions">
                            <strong><ComboBadge combo={row.combo} /></strong>
                            <span>p {formatMaybeNumber(row.prob, 4)}</span>
                            <span>odds {Number.isFinite(oddsByCombo.get(row.combo)) ? formatMaybeNumber(oddsByCombo.get(row.combo), 1) : "-"}</span>
                            <span>-</span>
                            <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket({ combo: row.combo, prob: row.prob, odds: oddsByCombo.get(row.combo), bet: 100 })}>
                              記録に追加
                            </button>
                          </div>
                        ))}
                      </div>
                    </article>
                  )}

                  <article className="card">
                    <h2>戦略チケット（V2）</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>戦略タイプ</span><strong>{ticketGenerationV2.strategy_type || "-"}</strong></div>
                      <div className="kv-row"><span>除外艇</span><strong><LanePills lanes={ticketGenerationV2.excluded_lanes || []} /></strong></div>
                    </div>
                    <p className="muted strategy-line">{ticketGenerationV2.summary || "-"}</p>
                    <div className="list-stack">
                      {(ticketGenerationV2.primary_tickets || []).slice(0, 8).map((combo, idx) => (
                        <div key={`tgv2-p-${combo}-${idx}`} className="list-row list-row-actions">
                          <strong><ComboBadge combo={combo} /></strong>
                          <span>本線</span>
                          <span>-</span>
                          <span>-</span>
                          <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket({ combo, bet: 100 })}>
                            記録に追加
                          </button>
                        </div>
                      ))}
                      {(ticketGenerationV2.secondary_tickets || []).slice(0, 8).map((combo, idx) => (
                        <div key={`tgv2-s-${combo}-${idx}`} className="list-row list-row-actions">
                          <strong><ComboBadge combo={combo} /></strong>
                          <span>押さえ</span>
                          <span>-</span>
                          <span>-</span>
                          <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket({ combo, bet: 100 })}>
                            記録に追加
                          </button>
                        </div>
                      ))}
                    </div>
                  </article>
                </section>

                <section className="card">
                  <h2>選手比較表</h2>
                  <div className="table-wrap">
                    <table>
                      <thead><tr><th>艇番</th><th>選手名</th><th>級別</th><th>全国勝率</th><th>当地勝率</th><th>モーター2連率</th><th>展示タイム</th><th>展示ST</th><th>進入</th></tr></thead>
                      <tbody>
                        {racers.map((racer, idx) => <tr key={`${racer.lane}-${idx}`}><td>{racer.lane ?? "-"}</td><td>{racer.name || "-"}</td><td>{racer.class || "-"}</td><td>{formatMaybeNumber(racer.nationwideWinRate, 2)}</td><td>{formatMaybeNumber(racer.localWinRate, 2)}</td><td>{formatMaybeNumber(racer.motor2Rate, 2)}</td><td>{formatMaybeNumber(racer.exhibitionTime, 2)}</td><td>{formatMaybeNumber(racer.exhibitionST, 2)}</td><td>{racer.entryCourse ?? "-"}</td></tr>)}
                      </tbody>
                    </table>
                  </div>
                </section>
              </>
            )}
          </>
        )}

        {screen === "performance" && (
          <>
            {perfError && <div className="error-banner">{perfError}</div>}

            <section className="card">
              <div className="result-form-grid">
                <h2>精算ワークフロー</h2>
                <p className="muted">
                  通常運用はベット記録タブで完結します。買い目登録後にレース単位で
                  <strong> 結果確認 / 精算</strong> を実行してください。
                </p>
                <div className="row-actions">
                  <button className="fetch-btn secondary" onClick={loadPerformance} disabled={statsLoading}>
                    {statsLoading ? "更新中..." : "実績を更新"}
                  </button>
                  {adminMode && (
                    <button
                      className="fetch-btn secondary"
                      onClick={() => setShowAdminResultTool((v) => !v)}
                    >
                      {showAdminResultTool ? "管理者入力を隠す" : "管理者入力を表示"}
                    </button>
                  )}
                </div>
              </div>
            </section>

            {adminMode && showAdminResultTool && (
              <section className="card">
                <div className="result-form-grid">
                  <h2>管理者用 手動結果入力（予備）</h2>
                  <div className="controls-grid">
                    <label><span>レースID</span><input value={resultForm.raceId} onChange={(e) => setResultForm((p) => ({ ...p, raceId: e.target.value }))} placeholder="YYYYMMDD_venue_race" /></label>
                    <label><span>1着</span><input type="number" min="1" max="6" value={resultForm.finish1} onChange={(e) => setResultForm((p) => ({ ...p, finish1: e.target.value }))} /></label>
                    <label><span>2着</span><input type="number" min="1" max="6" value={resultForm.finish2} onChange={(e) => setResultForm((p) => ({ ...p, finish2: e.target.value }))} /></label>
                    <label><span>3着</span><input type="number" min="1" max="6" value={resultForm.finish3} onChange={(e) => setResultForm((p) => ({ ...p, finish3: e.target.value }))} /></label>
                  </div>
                  <div className="controls-grid">
                    <label><span>払戻組番（任意）</span><input value={resultForm.payoutCombo} onChange={(e) => setResultForm((p) => ({ ...p, payoutCombo: e.target.value }))} placeholder="1-2-3" /></label>
                    <label><span>払戻金（任意）</span><input type="number" value={resultForm.payoutAmount} onChange={(e) => setResultForm((p) => ({ ...p, payoutAmount: e.target.value }))} placeholder="例: 5240" /></label>
                    <button className="fetch-btn" onClick={onSubmitResult} disabled={resultSaving}>{resultSaving ? "保存中..." : "結果を保存"}</button>
                  </div>
                </div>
            </section>
            )}

            <section className="stats-grid">
              <article className="card stat-card"><span>対象レース数</span><strong>{stats?.total_races ?? 0}</strong></article>
              <article className="card stat-card"><span>購入総額</span><strong>JPY {(stats?.total_bets ?? 0).toLocaleString()}</strong></article>
              <article className="card stat-card"><span>的中率</span><strong>{formatMaybeNumber(stats?.hit_rate, 2)}%</strong></article>
              <article className="card stat-card"><span>回収率</span><strong>{formatMaybeNumber(stats?.recovery_rate, 2)}%</strong></article>
              <article className="card stat-card"><span>総損益</span><strong>JPY {(stats?.total_profit_loss ?? 0).toLocaleString()}</strong></article>
              <article className="card stat-card"><span>平均EV</span><strong>{formatMaybeNumber(stats?.average_ev_of_placed_bets, 4)}</strong></article>
            </section>

            <section className="card">
              <h2>推奨タイプ別実績</h2>
              <div className="stats-grid">
                {["FULL BET", "SMALL BET", "MICRO BET", "SKIP"].map((k) => {
                  const s = stats?.by_recommendation_type?.[k] || {};
                  return <div key={k} className="card mini-stat"><h3>{k}</h3><p>レース: {s.total_races ?? 0}</p><p>購入: JPY {(s.total_bets ?? 0).toLocaleString()}</p><p>的中: {formatMaybeNumber(s.hit_rate, 2)}%</p><p>回収: {formatMaybeNumber(s.recovery_rate, 2)}%</p><p>損益: JPY {(s.total_profit_loss ?? 0).toLocaleString()}</p></div>;
                })}
              </div>
            </section>

            <section className="card">
              <h2>レース結果トラッキング</h2>
              {history.length === 0 ? <p className="muted">履歴データはまだありません。</p> : (
                <div className="history-stack">
                  {history.map((h) => (
                    <div key={h.race_id} className="history-item">
                      <div className="history-head">
                        <strong>{h.race_date} {h.venue_name || h.venue_id} {h.race_no}R</strong>
                        <span className={h.hit_miss === "HIT" ? "badge hit" : h.hit_miss === "MISS" ? "badge miss" : "badge pending"}>{h.hit_miss}</span>
                      </div>
                      <div className="history-grid">
                        <div>予想上位: {Array.isArray(h.predicted_top3) && h.predicted_top3.length ? h.predicted_top3.join("-") : "-"}</div>
                        <div>確定結果: {Array.isArray(h.actual_top3) && h.actual_top3.length ? h.actual_top3.join("-") : "-"}</div>
                        <div>購入額: JPY {(h.totals?.bet_amount ?? 0).toLocaleString()}</div>
                        <div>払戻: JPY {(h.totals?.payout ?? 0).toLocaleString()}</div>
                        <div>損益: JPY {(h.totals?.profit_loss ?? 0).toLocaleString()}</div>
                      </div>
                      {Array.isArray(h.bets) && h.bets.length > 0 && (
                        <div className="table-wrap">
                          <table>
                            <thead><tr><th>買い目</th><th>購入額</th><th>結果</th><th>払戻</th><th>損益</th></tr></thead>
                            <tbody>
                              {h.bets.map((b, i) => <tr key={`${h.race_id}-${i}`}><td>{b.combo}</td><td>JPY {(b.bet_amount ?? 0).toLocaleString()}</td><td>{b.hit_flag ? "HIT" : "MISS"}</td><td>JPY {(b.payout ?? 0).toLocaleString()}</td><td>JPY {(b.profit_loss ?? 0).toLocaleString()}</td></tr>)}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </section>
          </>
        )}

        {screen === "journal" && (
          <>
            {journalError && <div className="error-banner">{journalError}</div>}
            {journalNotice && <div className="notice-banner">{journalNotice}</div>}

            <section className="card">
              <h2>ベット入力</h2>
              <div className="controls-grid">
                <label>
                  <span>レースID（任意）</span>
                  <input
                    value={journalForm.race_id}
                    onChange={(e) => setJournalForm((p) => ({ ...p, race_id: e.target.value }))}
                    placeholder="YYYYMMDD_venue_race"
                  />
                </label>
                <label>
                  <span>日付</span>
                  <input
                    type="date"
                    value={journalForm.race_date}
                    onChange={(e) => setJournalForm((p) => ({ ...p, race_date: e.target.value }))}
                  />
                </label>
                <label>
                  <span>場</span>
                  <select
                    value={journalForm.venue_id}
                    onChange={(e) => setJournalForm((p) => ({ ...p, venue_id: Number(e.target.value) }))}
                  >
                    {VENUES.map((v) => (
                      <option key={v.id} value={v.id}>
                        {v.id} - {v.name}
                      </option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>レース</span>
                  <select
                    value={journalForm.race_no}
                    onChange={(e) => setJournalForm((p) => ({ ...p, race_no: Number(e.target.value) }))}
                  >
                    {Array.from({ length: 12 }, (_, i) => i + 1).map((n) => (
                      <option key={n} value={n}>
                        {n}R
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="controls-grid" style={{ marginTop: 10 }}>
                <label>
                  <span>買い目</span>
                  <input
                    value={journalForm.combo}
                    onChange={(e) => {
                      const value = e.target.value;
                      const combo = normalizeCombo(value);
                      setJournalForm((p) => ({ ...p, combo: value }));
                      if (combo.split("-").length === 3) {
                        setBuilderLanes(combo.split("-").map((v) => Number(v)));
                      }
                    }}
                    placeholder="1-2-3"
                  />
                </label>
                <label>
                  <span>購入額</span>
                  <input
                    type="number"
                    min="100"
                    step="100"
                    value={quickBetAmount}
                    onChange={(e) => {
                      const next = roundBetTo100(e.target.value);
                      setQuickBetAmount(next);
                      setJournalForm((p) => ({ ...p, bet_amount: next }));
                    }}
                  />
                </label>
                <label>
                  <span>メモ</span>
                  <input
                    value={journalForm.memo}
                    onChange={(e) => setJournalForm((p) => ({ ...p, memo: e.target.value }))}
                    placeholder="任意メモ"
                  />
                </label>
                <button className="fetch-btn secondary" onClick={onAddPendingTicket} disabled={betSaving}>
                  チケット追加
                </button>
              </div>

              <div className="builder-panel">
                <p className="muted">ビジュアルチケットビルダー</p>
                <div className="preset-row">
                  {[100, 200, 500, 1000].map((amount) => (
                    <button
                      key={amount}
                      type="button"
                      className={`preset-btn ${quickBetAmount === amount ? "on" : ""}`}
                      onClick={() => {
                        setQuickBetAmount(amount);
                        setJournalForm((p) => ({ ...p, bet_amount: amount }));
                      }}
                    >
                      JPY {amount}
                    </button>
                  ))}
                </div>
                {[
                  { key: "first", title: "1着" },
                  { key: "second", title: "2着" },
                  { key: "third", title: "3着" }
                ].map((row) => (
                  <div key={row.key} className="builder-row">
                    <span>{row.title}</span>
                    <div className="lane-builder-grid">
                      {laneButtons.map((btn) => (
                        <button
                          key={`${row.key}-${btn.lane}`}
                          className={`${btn.className}${builderSlots[row.key] === btn.lane ? " selected" : ""}`}
                          onClick={() => onBuilderSlotClick(row.key, btn.lane)}
                          type="button"
                        >
                          {btn.label}
                        </button>
                      ))}
                    </div>
                  </div>
                ))}
                <p className="builder-current">
                  選択中の組番: <strong><ComboBadge combo={builderCombo || normalizeCombo(journalForm.combo)} /></strong>
                </p>
              </div>

              <div className="pending-list">
                <div className="pending-head">
                  <h3>選択中チケット</h3>
                  <button className="fetch-btn" onClick={onSavePendingTickets} disabled={betSaving}>
                    {betSaving ? "保存中..." : "まとめて保存"}
                  </button>
                </div>
                {pendingTicketsForCurrentRace.length === 0 ? (
                  <p className="muted">まだチケットがありません。</p>
                ) : (
                  <div className="list-stack">
                    {pendingTicketsForCurrentRace.map((ticket) => (
                      <div key={`${ticket.raceKey}-${ticket.combo}`} className="list-row list-row-actions">
                        <strong><ComboBadge combo={ticket.combo} /></strong>
                        <span>
                          JPY
                          <input
                            type="number"
                            min="100"
                            step="100"
                            value={ticket.bet_amount}
                            onChange={(e) => onUpdatePendingTicket(ticket.raceKey, ticket.combo, e.target.value)}
                          />
                        </span>
                        <span>p {Number.isFinite(ticket.prob) ? formatMaybeNumber(ticket.prob, 3) : "-"}</span>
                        <span>odds {Number.isFinite(ticket.odds) ? formatMaybeNumber(ticket.odds, 1) : "-"}</span>
                        <span>ev {Number.isFinite(ticket.ev) ? formatMaybeNumber(ticket.ev, 2) : "-"}</span>
                        <button
                          className="fetch-btn secondary"
                          type="button"
                          onClick={() => onRemovePendingTicket(ticket.raceKey, ticket.combo)}
                        >
                          削除
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div className="controls-grid" style={{ marginTop: 10 }}>
                <button
                  className="fetch-btn secondary"
                  onClick={() => setPendingTickets((prev) => prev.filter((x) => x.raceKey !== currentRaceKey))}
                  disabled={betSaving}
                >
                  選択をクリア
                </button>
                <div className="shortcut-hint">ショートカット: A 追加 / S 保存 / R 精算 / D 直近削除</div>
              </div>
            </section>

            <section className="stats-grid">
              <article className="card stat-card">
                <span>今日</span>
                <strong>Bet JPY {(betSummaries?.today?.total_bet_amount ?? 0).toLocaleString()}</strong>
                <small>払戻 JPY {(betSummaries?.today?.total_payout ?? 0).toLocaleString()}</small>
                <small>P/L JPY {(betSummaries?.today?.total_profit_loss ?? 0).toLocaleString()}</small>
                <small>Hit {formatMaybeNumber(betSummaries?.today?.hit_rate, 2)}%</small>
                <small>Recovery {formatMaybeNumber(betSummaries?.today?.recovery_rate, 2)}%</small>
              </article>
              <article className="card stat-card">
                <span>今月</span>
                <strong>Bet JPY {(betSummaries?.month?.total_bet_amount ?? 0).toLocaleString()}</strong>
                <small>払戻 JPY {(betSummaries?.month?.total_payout ?? 0).toLocaleString()}</small>
                <small>P/L JPY {(betSummaries?.month?.total_profit_loss ?? 0).toLocaleString()}</small>
                <small>Hit {formatMaybeNumber(betSummaries?.month?.hit_rate, 2)}%</small>
                <small>Recovery {formatMaybeNumber(betSummaries?.month?.recovery_rate, 2)}%</small>
              </article>
              <article className="card stat-card">
                <span>今年</span>
                <strong>Bet JPY {(betSummaries?.year?.total_bet_amount ?? 0).toLocaleString()}</strong>
                <small>払戻 JPY {(betSummaries?.year?.total_payout ?? 0).toLocaleString()}</small>
                <small>P/L JPY {(betSummaries?.year?.total_profit_loss ?? 0).toLocaleString()}</small>
                <small>Hit {formatMaybeNumber(betSummaries?.year?.hit_rate, 2)}%</small>
                <small>Recovery {formatMaybeNumber(betSummaries?.year?.recovery_rate, 2)}%</small>
              </article>
              <article className="card stat-card">
                <span>通算</span>
                <strong>Bet JPY {(allTimeSummary?.total_bet_amount ?? 0).toLocaleString()}</strong>
                <small>払戻 JPY {(allTimeSummary?.total_payout ?? 0).toLocaleString()}</small>
                <small className={getProfitClass(allTimeSummary?.total_profit_loss)}>P/L JPY {(allTimeSummary?.total_profit_loss ?? 0).toLocaleString()}</small>
                <small>Hit {formatMaybeNumber((allTimeSummary?.hit_count || 0) + (allTimeSummary?.miss_count || 0) > 0 ? ((allTimeSummary?.hit_count || 0) / ((allTimeSummary?.hit_count || 0) + (allTimeSummary?.miss_count || 0))) * 100 : 0, 2)}%</small>
                <small>Recovery {formatMaybeNumber((allTimeSummary?.total_bet_amount || 0) > 0 ? ((allTimeSummary?.total_payout || 0) / (allTimeSummary?.total_bet_amount || 0)) * 100 : 0, 2)}%</small>
              </article>
            </section>

            <section className="card">
              <div className="section-head">
                <h2>ベットジャーナル</h2>
                <div className="filter-chips">
                  {[
                    ["all", "全件"],
                    ["today", "今日"],
                    ["week", "今週"],
                    ["month", "今月"],
                    ["unsettled", "未精算"],
                    ["hits", "的中"],
                    ["misses", "ハズレ"]
                  ].map(([key, label]) => (
                    <button
                      key={key}
                      className={`chip-btn ${journalFilter === key ? "on" : ""}`}
                      onClick={() => setJournalFilter(key)}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              </div>
              {journalLoading ? (
                <p className="muted">読み込み中...</p>
              ) : filteredGroupedBets.length === 0 ? (
                <p className="muted">記録されたベットはまだありません。</p>
              ) : (
                <div className="race-group-stack">
                  {filteredGroupedBets.map((group) => (
                    <article key={group.raceId} className="race-group-card">
                      <div className="race-group-head">
                        <div className="race-group-meta">
                          <strong>{group.raceDate} 場:{group.venueId} {group.raceNo}R</strong>
                          <small>race_id: {group.raceIdText}</small>
                        </div>
                        <span className={`status-pill ${group.unsettled ? "status-unsettled" : "status-hit"}`}>
                          {group.unsettled ? "未精算" : "精算済み"}
                        </span>
                      </div>

                      <div className="race-group-actions">
                        <button
                          className="fetch-btn secondary"
                          onClick={() => onSettleRace(group)}
                          disabled={settlingRaceId === String(group.raceId)}
                        >
                          {settlingRaceId === String(group.raceId) ? "精算中..." : "結果確認 / レース精算"}
                        </button>
                      </div>

                      <div className="ticket-stack">
                        {group.bets.map((bet) => {
                          const isEditing = editingBetId === bet.id;
                          return (
                            <div key={bet.id} className="ticket-row">
                              <div className="ticket-main">
                                <div><span className="label">買い目</span><strong>{bet.combo}</strong></div>
                                <div><span className="label">表示</span><strong><ComboBadge combo={bet.combo} /></strong></div>
                                <div>
                                  <span className="label">購入額</span>
                                  {isEditing ? (
                                    <input
                                      type="number"
                                      value={editingDraft.bet_amount}
                                      onChange={(e) => setEditingDraft((d) => ({ ...d, bet_amount: e.target.value }))}
                                    />
                                  ) : (
                                    <strong>JPY {(bet.bet_amount ?? 0).toLocaleString()}</strong>
                                  )}
                                </div>
                                <div>
                                  <span className="label">状態</span>
                                  <span className={`status-pill ${getBetStatusClass(bet.status)}`}>{bet.status === "hit" ? "的中" : bet.status === "miss" ? "ハズレ" : "未精算"}</span>
                                </div>
                                <div><span className="label">購入時オッズ</span><strong>{Number.isFinite(Number(bet.bought_odds)) ? formatMaybeNumber(bet.bought_odds, 1) : "-"}</strong></div>
                                <div><span className="label">払戻</span><strong>JPY {(bet.payout ?? 0).toLocaleString()}</strong></div>
                                <div>
                                  <span className="label">損益</span>
                                  <strong className={getProfitClass(bet.profit_loss)}>JPY {(bet.profit_loss ?? 0).toLocaleString()}</strong>
                                </div>
                              </div>
                              <div className="ticket-sub">
                                <div className="ticket-memo">
                                  <span className="label">メモ</span>
                                  {isEditing ? (
                                    <input
                                      value={editingDraft.memo}
                                      onChange={(e) => setEditingDraft((d) => ({ ...d, memo: e.target.value }))}
                                      placeholder="メモ"
                                    />
                                  ) : (
                                    <span>{bet.memo || "-"}</span>
                                  )}
                                </div>
                                <div className="row-actions">
                                  {isEditing ? (
                                    <>
                                      <input
                                        value={editingDraft.combo}
                                        onChange={(e) => setEditingDraft((d) => ({ ...d, combo: e.target.value }))}
                                        placeholder="1-2-3"
                                      />
                                      <button className="fetch-btn secondary" onClick={() => onSaveEditBet(bet.id)}>
                                        保存
                                      </button>
                                    </>
                                  ) : (
                                    <button className="fetch-btn secondary" onClick={() => onStartEditBet(bet)}>
                                      編集
                                    </button>
                                  )}
                                  <button className="fetch-btn secondary" onClick={() => onDeleteBet(bet.id)}>
                                    削除
                                  </button>
                                </div>
                              </div>
                            </div>
                          );
                        })}
                      </div>

                      <div className="race-summary">
                        <div><span>合計購入</span><strong>JPY {group.totals.bet.toLocaleString()}</strong></div>
                        <div><span>合計払戻</span><strong>JPY {group.totals.payout.toLocaleString()}</strong></div>
                        <div><span>合計損益</span><strong className={getProfitClass(group.totals.pl)}>JPY {group.totals.pl.toLocaleString()}</strong></div>
                        <div><span>的中 / ハズレ</span><strong>{group.hitCount} / {group.missCount}</strong></div>
                      </div>
                    </article>
                  ))}
                </div>
              )}
            </section>
          </>
        )}
      </div>
    </div>
  );
}


