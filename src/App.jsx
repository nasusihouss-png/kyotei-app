import { useEffect, useMemo, useState } from "react";
import "./App.css";

const API_BASE_URL = String(import.meta.env.VITE_API_BASE_URL || "").replace(/\/+$/, "");
const API_BASE = API_BASE_URL ? `${API_BASE_URL}/api` : "/api";

function localDateKey(base = new Date()) {
  const yyyy = base.getFullYear();
  const mm = String(base.getMonth() + 1).padStart(2, "0");
  const dd = String(base.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

async function fetchJsonWithTimeout(url, options = {}) {
  const timeoutMs = Number(options?.timeoutMs || 25000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body?.message || `Request failed (${response.status})`);
    }
    return body;
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error("Request timeout. Please retry.");
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

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

const MANUAL_LAP_FIELDS = [
  { key: "straight_line_score", label: "直線" },
  { key: "turn_entry_score", label: "ターン入口" },
  { key: "turn_exit_score", label: "ターン出口" },
  { key: "acceleration_score", label: "行き足" },
  { key: "stability_score", label: "安定感" }
];

function createManualLapDraft() {
  const rows = {};
  for (let lane = 1; lane <= 6; lane += 1) {
    rows[String(lane)] = {
      straight_line_score: "",
      turn_entry_score: "",
      turn_exit_score: "",
      acceleration_score: "",
      stability_score: ""
    };
  }
  return rows;
}

async function fetchRaceData(date, venueId, raceNo) {
  const url = new URL(`${API_BASE}/race`);
  url.searchParams.set("date", date);
  url.searchParams.set("venueId", String(venueId));
  url.searchParams.set("raceNo", String(raceNo));

  const response = await fetch(url.toString());
  if (!response.ok) throw new Error("Failed to fetch race data");
  return response.json();
}

async function fetchRankingsData(date, mode = "hit_rate") {
  const url = new URL(`${API_BASE}/rankings`);
  url.searchParams.set("date", date);
  url.searchParams.set("mode", mode);
  return fetchJsonWithTimeout(url.toString(), { timeoutMs: 30000 });
}

async function fetchStatsData() {
  const response = await fetch(`${API_BASE}/stats`);
  if (!response.ok) throw new Error("Failed to fetch stats");
  return response.json();
}

async function fetchHistoryData({ includeInvalidated = false } = {}) {
  const url = new URL(`${API_BASE}/results-history`);
  url.searchParams.set("limit", "500");
  if (includeInvalidated) url.searchParams.set("include_invalidated", "1");
  const response = await fetch(url.toString());
  if (!response.ok) throw new Error("Failed to fetch results history");
  return response.json();
}

async function verifyRaceResultApi(raceId, predictionSnapshotId = null) {
  const timeoutMs = 20000;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${API_BASE}/results/verify`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        race_id: raceId,
        prediction_snapshot_id: predictionSnapshotId
      }),
      signal: controller.signal
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      const err = new Error(body?.message || `Verification request failed (${response.status})`);
      err.payload = body;
      throw err;
    }
    return body;
  } catch (err) {
    if (err?.name === "AbortError") {
      const timeoutErr = new Error("Verification request timeout. Please retry.");
      timeoutErr.payload = { status: "VERIFY_FAILED", reason_code: "TIMEOUT" };
      throw timeoutErr;
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

async function invalidateVerificationApi({
  verificationLogId,
  raceId,
  predictionSnapshotId,
  invalidReason = ""
}) {
  return fetchJsonWithTimeout(`${API_BASE}/results/invalidate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      verification_log_id: verificationLogId,
      race_id: raceId,
      prediction_snapshot_id: predictionSnapshotId,
      is_hidden_from_results: true,
      is_invalid_verification: true,
      exclude_from_learning: true,
      invalid_reason: invalidReason
    }),
    timeoutMs: 20000
  });
}

async function restoreVerificationApi({ verificationLogId }) {
  return fetchJsonWithTimeout(`${API_BASE}/results/restore`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      verification_log_id: verificationLogId
    }),
    timeoutMs: 20000
  });
}

async function updateVerificationNoteApi({ verificationLogId, verificationReason = "" }) {
  return fetchJsonWithTimeout(`${API_BASE}/results/verification-note`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      verification_log_id: verificationLogId,
      verification_reason: verificationReason
    }),
    timeoutMs: 20000
  });
}

async function editResultRecordApi({
  raceId,
  predictionSnapshotId = null,
  confirmedResult,
  verificationReason = "",
  invalidReason = ""
}) {
  return fetchJsonWithTimeout(`${API_BASE}/results/edit`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      race_id: raceId,
      prediction_snapshot_id: predictionSnapshotId,
      confirmed_result: confirmedResult,
      verification_reason: verificationReason,
      invalid_reason: invalidReason
    }),
    timeoutMs: 20000
  });
}

async function fetchAnalyticsData(date) {
  const url = new URL(`${API_BASE}/analytics`);
  if (date) url.searchParams.set("date", date);
  const response = await fetch(url.toString());
  if (!response.ok) throw new Error("Failed to fetch analytics");
  return response.json();
}

async function fetchSelfLearningData() {
  const response = await fetch(`${API_BASE}/self-learning?mode=proposal_only&save=1`);
  if (!response.ok) throw new Error("Failed to fetch self-learning");
  return response.json();
}

async function fetchLearningLatestData() {
  const response = await fetch(`${API_BASE}/learning/latest?auto=0`);
  if (!response.ok) throw new Error("Failed to fetch learning latest");
  return response.json();
}

async function runLearningBatchApi({ apply = true, dryRun = false } = {}) {
  return fetchJsonWithTimeout(`${API_BASE}/learning/batch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ apply, dryRun }),
    timeoutMs: 30000
  });
}

async function fetchStartEntryAnalysisData() {
  const response = await fetch(`${API_BASE}/start-entry-analysis`);
  if (!response.ok) throw new Error("Failed to fetch start/entry analysis");
  return response.json();
}

async function fetchManualLapEvaluation({ raceId, date, venueId, raceNo }) {
  const url = new URL(`${API_BASE}/manual-lap-evaluation`);
  if (raceId) url.searchParams.set("raceId", raceId);
  if (!raceId) {
    url.searchParams.set("date", date);
    url.searchParams.set("venueId", String(venueId));
    url.searchParams.set("raceNo", String(raceNo));
  }
  const response = await fetch(url.toString());
  if (!response.ok) throw new Error("Failed to fetch manual lap evaluation");
  return response.json();
}

async function saveManualLapEvaluationApi(payload) {
  const response = await fetch(`${API_BASE}/manual-lap-evaluation`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err?.message || "Failed to save manual lap evaluation");
  }
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
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const msg = body?.message || "Failed to settle placed bets";
    const debug = body?.debug ? ` | debug: ${JSON.stringify(body.debug)}` : "";
    throw new Error(`${msg}${debug}`);
  }
  return body;
}

function formatMaybeNumber(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (Number.isNaN(num)) return String(value);
  return num.toFixed(digits);
}

function getRiskClass(recommendation) {
  const rec = String(recommendation || "").toUpperCase();
  if (rec === "SKIP") return "risk-skip";
  if (rec === "MICRO BET" || rec === "MICRO_BET") return "risk-micro";
  if (rec === "FULL BET" || rec === "FULL_BET") return "risk-full";
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

function getVerifyStatusLabel(status) {
  const s = String(status || "").toUpperCase();
  if (s === "INVALIDATED") return "INVALIDATED";
  if (s === "VERIFIED_HIT") return "VERIFIED_HIT";
  if (s === "VERIFIED_MISS") return "VERIFIED_MISS";
  if (s === "VERIFIED") return "VERIFIED";
  if (s === "VERIFY_FAILED") return "VERIFY_FAILED";
  if (s === "UNVERIFIED") return "UNVERIFIED";
  if (s === "NO_CONFIRMED_RESULT") return "NO_CONFIRMED_RESULT";
  if (s === "NO_BET_SNAPSHOT") return "NO_BET_SNAPSHOT";
  if (s === "VERIFY_SKIPPED") return "VERIFY_SKIPPED";
  if (s === "NOT_VERIFIABLE") return "NOT_VERIFIABLE";
  return "PENDING_RESULT";
}

function getVerifyStatusBadgeClass(status) {
  const s = String(status || "").toUpperCase();
  if (s === "INVALIDATED") return "badge pending";
  if (s === "VERIFIED" || s === "VERIFIED_HIT") return "badge hit";
  if (s === "VERIFIED_MISS") return "badge miss";
  if (s === "VERIFY_FAILED") return "badge miss";
  if (s === "NO_BET_SNAPSHOT" || s === "VERIFY_SKIPPED" || s === "NOT_VERIFIABLE") return "badge pending";
  return "badge pending";
}

function getConfidenceBandLabel(band) {
  const b = String(band || "").toLowerCase();
  if (b === "high") return "High";
  if (b === "medium") return "Medium";
  return "Caution";
}

function getConfidenceBandClass(band) {
  const b = String(band || "").toLowerCase();
  if (b === "high") return "risk-full";
  if (b === "medium") return "risk-small";
  return "risk-skip";
}

function getLearningAutoReasonLabel(reason) {
  const r = String(reason || "");
  if (!r) return "-";
  if (r === "applied") return "学習を実行しました";
  if (r === "insufficient_learning_ready_total") return "学習準備完了データ数がしきい値未満";
  if (r === "not_enough_new_learning_ready") return "新規の学習準備完了データが不足";
  return r;
}

function getTicketTypeLabel(ticketType) {
  const t = String(ticketType || "").toLowerCase();
  if (t === "main") return "本線";
  if (t === "backup") return "押さえ";
  if (t === "longshot") return "穴";
  return "通常";
}

function getTicketTypeClass(ticketType) {
  const t = String(ticketType || "").toLowerCase();
  if (t === "main") return "ttype-main";
  if (t === "backup") return "ttype-backup";
  if (t === "longshot") return "ttype-longshot";
  return "ttype-backup";
}

function getValueTierLabel(tier) {
  const t = String(tier || "");
  if (t === "main_value") return "価値高";
  if (t === "safe_low_value") return "低価値";
  if (t === "speculative") return "準穴";
  if (t === "avoid") return "回避";
  return "通常";
}

function getValueTierClass(tier) {
  const t = String(tier || "");
  if (t === "main_value") return "value-main";
  if (t === "safe_low_value") return "value-low";
  if (t === "speculative") return "value-spec";
  if (t === "avoid") return "value-avoid";
  return "value-normal";
}

function getAvoidLevelLabel(level) {
  const n = Number(level || 0);
  if (n >= 3) return "回避強";
  if (n === 2) return "回避中";
  if (n === 1) return "注意";
  return "通常";
}

function getAvoidLevelClass(level) {
  const n = Number(level || 0);
  if (n >= 3) return "trap-high";
  if (n === 2) return "trap-mid";
  if (n === 1) return "trap-low";
  return "trap-none";
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

function normalizeExactaCombo(value) {
  const digits = String(value || "").match(/[1-6]/g) || [];
  const lanes = digits.slice(0, 2);
  if (lanes.length !== 2) return "";
  if (lanes[0] === lanes[1]) return "";
  return lanes.join("-");
}

function getSavedFinalRecommendedBets(row) {
  const snapshot = Array.isArray(row?.final_recommended_bets_snapshot)
    ? row.final_recommended_bets_snapshot
    : Array.isArray(row?.ai_bets_display_snapshot)
      ? row.ai_bets_display_snapshot
      : [];
  return snapshot
    .map((bet) => {
      const combo = normalizeCombo(bet?.combo ?? bet);
      if (!combo || combo.split("-").length !== 3) return null;
      return {
        ...(bet && typeof bet === "object" ? bet : {}),
        combo,
        recommended_bet: Number.isFinite(Number(bet?.recommended_bet))
          ? Number(bet.recommended_bet)
          : Number.isFinite(Number(bet?.bet))
            ? Number(bet.bet)
            : 0
      };
    })
    .filter(Boolean);
}

function getSavedBoat1HeadBets(row) {
  const snapshot = Array.isArray(row?.boat1_head_bets_snapshot)
    ? row.boat1_head_bets_snapshot
    : [];
  return snapshot
    .map((bet) => {
      const combo = normalizeCombo(bet?.combo ?? bet);
      if (!combo || combo.split("-").length !== 3) return null;
      return {
        ...(bet && typeof bet === "object" ? bet : {}),
        combo,
        recommended_bet: Number.isFinite(Number(bet?.recommended_bet))
          ? Number(bet.recommended_bet)
          : Number.isFinite(Number(bet?.bet))
            ? Number(bet.bet)
            : 0
      };
    })
    .filter(Boolean);
}

function getSavedExactaBets(row) {
  const snapshot = Array.isArray(row?.exacta_recommended_bets_snapshot)
    ? row.exacta_recommended_bets_snapshot
    : Array.isArray(row?.ai_bets_full_snapshot?.exacta_recommended_bets)
      ? row.ai_bets_full_snapshot.exacta_recommended_bets
      : [];
  return snapshot
    .map((bet) => {
      const combo = normalizeExactaCombo(bet?.combo ?? bet);
      if (!combo || combo.split("-").length !== 2) return null;
      return {
        ...(bet && typeof bet === "object" ? bet : {}),
        combo,
        recommended_bet: Number.isFinite(Number(bet?.recommended_bet))
          ? Number(bet.recommended_bet)
          : Number.isFinite(Number(bet?.bet))
            ? Number(bet.bet)
            : 0
      };
    })
    .filter(Boolean);
}

function getResultsBetSnapshotLabel(row) {
  const saved = getSavedFinalRecommendedBets(row);
  return saved.length > 0 ? null : "NO_BET_SNAPSHOT";
}

function getLearningStatusLabel(row) {
  const verification = row?.verification || {};
  const categories = Array.isArray(verification?.mismatch_categories) ? verification.mismatch_categories : [];
  const status = String(row?.verification_status || "").toUpperCase();
  if (!status.startsWith("VERIFIED")) return "PENDING";
  return categories.length > 0 ? "LEARNING_READY" : "VERIFIED_ONLY";
}

function getResultMissPatternTags(row) {
  const verification = row?.verification || {};
  const summary = verification?.summary || {};
  const rawTags = Array.isArray(verification?.miss_pattern_tags)
    ? verification.miss_pattern_tags
    : Array.isArray(summary?.miss_pattern_tags)
      ? summary.miss_pattern_tags
      : [];
  const tagSet = new Set(rawTags.map((tag) => String(tag || "").toLowerCase()));
  const compact = [];
  if (tagSet.has("head_hit")) compact.push("HEAD HIT");
  else if (tagSet.has("head_miss")) compact.push("HEAD MISS");
  if (tagSet.has("second_place_hit")) compact.push("2ND HIT");
  else if (tagSet.has("second_place_miss")) compact.push("2ND MISS");
  if (tagSet.has("third_place_hit")) compact.push("3RD HIT");
  else if (tagSet.has("third_place_miss")) compact.push("3RD MISS");
  if (tagSet.has("second_third_swap")) compact.push("SWAP");
  if (tagSet.has("structure_near_miss") || tagSet.has("structure_near_but_order_miss")) compact.push("NEAR");
  if (tagSet.has("partner_selection_miss")) compact.push("PARTNER MISS");
  if (tagSet.has("third_place_noise")) compact.push("3RD NOISE");
  if (tagSet.has("exacta_hit")) compact.push("EXACTA HIT");
  else if (tagSet.has("exacta_miss")) compact.push("EXACTA MISS");
  if (tagSet.has("boat1_survival_underestimated")) compact.push("1 SURVIVAL");
  if (tagSet.has("outer_head_overpromotion")) compact.push("OUTER OVER");
  if (tagSet.has("attack_scenario_overweight")) compact.push("ATTACK OVER");
  if (tagSet.has("attack_scenario_underweight")) compact.push("ATTACK UNDER");
  return compact;
}

function buildResultsMissPatternSummary(rows) {
  const items = Array.isArray(rows) ? rows : [];
  const summary = {
    headMisses: 0,
    secondPlaceMisses: 0,
    thirdPlaceMisses: 0,
    swaps: 0,
    exactaHits: 0,
    exactaMisses: 0,
    nearMisses: 0
  };
  for (const row of items) {
    const verification = row?.verification || {};
    const rawTags = Array.isArray(verification?.miss_pattern_tags)
      ? verification.miss_pattern_tags
      : Array.isArray(verification?.summary?.miss_pattern_tags)
        ? verification.summary.miss_pattern_tags
        : [];
    const tagSet = new Set(rawTags.map((tag) => String(tag || "").toLowerCase()));
    if (tagSet.has("head_miss")) summary.headMisses += 1;
    if (tagSet.has("second_place_miss")) summary.secondPlaceMisses += 1;
    if (tagSet.has("third_place_miss")) summary.thirdPlaceMisses += 1;
    if (tagSet.has("second_third_swap")) summary.swaps += 1;
    if (tagSet.has("exacta_hit")) summary.exactaHits += 1;
    if (tagSet.has("exacta_miss")) summary.exactaMisses += 1;
    if (tagSet.has("structure_near_miss") || tagSet.has("structure_near_but_order_miss")) summary.nearMisses += 1;
  }
  return summary;
}

function normalizeParticipationDecisionValue(value) {
  const text = String(value || "").trim().toLowerCase();
  if (["recommended", "participate", "full_bet", "full bet"].includes(text)) return "recommended";
  if (["watch", "small_bet", "small bet", "micro_bet", "micro bet"].includes(text)) return "watch";
  if (["not_recommended", "skip"].includes(text)) return "not_recommended";
  return "";
}

function getParticipationDecisionMeta(row) {
  const normalized = normalizeParticipationDecisionValue(
    row?.participation_decision ||
    row?.prediction?.participation_decision ||
    row?.recommendation
  );
  if (normalized === "recommended") {
    return {
      value: normalized,
      label: "Participate",
      className: "badge hit",
      reason: row?.participation_decision_reason || row?.prediction?.participation_decision_reason || ""
    };
  }
  if (normalized === "watch") {
    return {
      value: normalized,
      label: "Watch",
      className: "badge pending",
      reason: row?.participation_decision_reason || row?.prediction?.participation_decision_reason || ""
    };
  }
  if (normalized === "not_recommended") {
    return {
      value: normalized,
      label: "Skip",
      className: "badge miss",
      reason: row?.participation_decision_reason || row?.prediction?.participation_decision_reason || ""
    };
  }
  return {
    value: "",
    label: "Decision Missing",
    className: "badge pending",
    reason: row?.participation_decision_reason || row?.prediction?.participation_decision_reason || "Saved participation decision is missing."
  };
}

function formatHistoryRaceTitle(row) {
  const venue = String(row?.venue_name || row?.venue_id || row?.race_id || "Race").trim();
  const raceNo = Number(row?.race_no);
  if (Number.isInteger(raceNo) && raceNo > 0) {
    return `${venue} ${raceNo}R`;
  }
  return venue;
}

function getVerificationHistoryKey(raceId, predictionSnapshotId = null) {
  if (Number.isFinite(Number(predictionSnapshotId))) {
    return `snapshot:${Number(predictionSnapshotId)}`;
  }
  return `race:${String(raceId || "")}`;
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

function normalizeSelectionByType(betType, input) {
  const type = String(betType || "trifecta").toLowerCase();
  const digits = (String(input || "").match(/[1-6]/g) || []).map((v) => Number(v));
  const uniq = [...new Set(digits)];

  if (type === "trifecta") {
    if (digits.length < 3) return null;
    const lanes = digits.slice(0, 3);
    if (new Set(lanes).size !== 3) return null;
    return lanes.join("-");
  }
  if (type === "exacta") {
    if (digits.length < 2) return null;
    const lanes = digits.slice(0, 2);
    if (lanes[0] === lanes[1]) return null;
    return lanes.join("-");
  }
  if (type === "trio") {
    if (uniq.length < 3) return null;
    return uniq.slice(0, 3).sort((a, b) => a - b).join("-");
  }
  if (type === "quinella" || type === "wide") {
    if (uniq.length < 2) return null;
    return uniq.slice(0, 2).sort((a, b) => a - b).join("-");
  }
  if (type === "win" || type === "place") {
    if (uniq.length < 1) return null;
    return String(uniq[0]);
  }
  return null;
}

function ComboBadge({ combo }) {
  const lanes = splitCombo(combo);
  if (lanes.length !== 2 && lanes.length !== 3) return <span>{combo || "-"}</span>;

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

function getStartDisplayRows(startDisplay) {
  if (!startDisplay || typeof startDisplay !== "object") return [];
  const orderRaw = Array.isArray(startDisplay.start_display_order)
    ? startDisplay.start_display_order
    : [];
  const stMap = startDisplay.start_display_st && typeof startDisplay.start_display_st === "object"
    ? startDisplay.start_display_st
    : {};
  const timingMap = startDisplay.start_display_timing && typeof startDisplay.start_display_timing === "object"
    ? startDisplay.start_display_timing
    : {};

  const parseToUnit = ({ raw, stValue }) => {
    const text = String(raw ?? "").trim().toUpperCase();
    const compact = text.replace(/\s+/g, "");
    const fMatch = compact.match(/^F\.?(\d{1,2})$/);
    if (fMatch) {
      const n = Number(fMatch[1]);
      if (Number.isFinite(n)) return 100 + n;
    }
    const lMatch = compact.match(/^L\.?(\d{1,2})$/);
    if (lMatch) {
      const n = Number(lMatch[1]);
      if (Number.isFinite(n)) return 100 + n;
    }

    let st = null;
    if (stValue !== null && stValue !== undefined && stValue !== "") {
      const num = Number(stValue);
      if (Number.isFinite(num)) st = num;
    }
    if (st === null && compact) {
      if (/^0?\.\d{1,2}$/.test(compact)) {
        const num = Number(compact.startsWith(".") ? `0${compact}` : compact);
        if (Number.isFinite(num)) st = num;
      }
    }
    if (st === null) return null;
    const clipped = Math.max(0, Math.min(0.99, st));
    const hundredths = Math.round(clipped * 100);
    return 100 - hundredths;
  };

  const order = orderRaw
    .map((v) => Number(v))
    .filter((v) => Number.isInteger(v) && v >= 1 && v <= 6);
  const lanes = [1, 2, 3, 4, 5, 6];
  const entryOrderByLane = new Map();
  order.forEach((lane, idx) => entryOrderByLane.set(lane, idx + 1));
  const axisPositionByLane = new Map();
  lanes.forEach((lane) => {
    const timing = timingMap[String(lane)] || {};
    const fallbackSt = Number(stMap[String(lane)]);
    const axisPosition = parseToUnit({
      raw: timing?.raw ?? timing?.display ?? stMap[String(lane)],
      stValue: Number.isFinite(fallbackSt) ? fallbackSt : null
    });
    axisPositionByLane.set(lane, Number.isFinite(axisPosition) ? axisPosition : null);
  });

  return lanes.map((lane) => {
    const axis = axisPositionByLane.get(lane);
    const axisClamped = Number.isFinite(axis) ? Math.max(0, Math.min(120, Number(axis))) : null;
    const leftPct = Number.isFinite(axisClamped) ? (axisClamped / 120) * 100 : null;
    const st = stMap[String(lane)];
    const timing = timingMap[String(lane)] || {};
    const timingDisplay = timing?.display || (Number.isFinite(Number(st)) ? Number(st).toFixed(2) : "--");
    return {
      lane,
      order: entryOrderByLane.get(lane) || null,
      leftPct,
      xUnit: Number.isFinite(axisClamped) ? Number(axisClamped.toFixed(2)) : null,
      st: Number.isFinite(Number(st)) ? Number(st) : null,
      stDisplay: timingDisplay
    };
  });
}

function StartExhibitionDisplay({ startDisplay, compact = false }) {
  const rows = useMemo(() => getStartDisplayRows(startDisplay), [startDisplay]);
  useEffect(() => {
    if (!import.meta.env.DEV) return;
    const debugSamples = ["F.02", "F.01", "0.21", "F.03", "F.14", "F.04"];
    const toUnit = (input) => {
      const raw = String(input || "").trim().toUpperCase().replace(/\s+/g, "");
      const m = raw.match(/^([FL])\.?(\d{1,2})$/);
      if (m) return 100 + Number(m[2]);
      const n = Number(raw.startsWith(".") ? `0${raw}` : raw);
      if (!Number.isFinite(n)) return null;
      return 100 - Math.round(Math.max(0, Math.min(0.99, n)) * 100);
    };
    const mapped = debugSamples.map((v) => {
      const u = toUnit(v);
      const p = Number.isFinite(u) ? Number(((u / 120) * 100).toFixed(3)) : null;
      return { value: v, unit: u, percent: p };
    });
    console.info("[StartDisplayDebug]", mapped);
  }, []);
  if (!rows.length) {
    return <p className="muted">No start exhibition data</p>;
  }

  return (
    <div className={`start-display ${compact ? "compact" : ""}`}>
      {!compact ? (
        <p className="muted strategy-line">
          source: {startDisplay?.start_display_source || "official_pre_race_info"} / layout:{" "}
          {startDisplay?.start_display_layout_mode || "normalized_entry_order"}
          {startDisplay?.source_fetched_at ? ` / updated: ${new Date(startDisplay.source_fetched_at).toLocaleString()}` : ""}
        </p>
      ) : null}
      {rows.map((row) => (
        <div key={`start-${row.lane}`} className="start-row">
          <div className="start-lane">
            <span className={`combo-dot ${BOAT_META[row.lane]?.className || ""}`}>{row.lane}</span>
          </div>
          <div className="start-layout">
            <div className="start-track start-track-120" />
            <div className="start-zero-line" />
            {row.leftPct !== null ? (
              <div className="start-marker-wrap" style={{ left: `${row.leftPct}%` }}>
                <span className={`start-marker ${BOAT_META[row.lane]?.className || ""}`}>{row.lane}</span>
                <small>{row.order ? `進入${row.order}` : "進入--"}</small>
              </div>
            ) : (
              <div className="start-marker-missing">--</div>
            )}
            <div className="start-scale">
              <span>100</span>
              <span>0</span>
              <span>-20</span>
            </div>
          </div>
          <div className="start-st">
            {row.stDisplay || "--"}
          </div>
        </div>
      ))}
    </div>
  );
}

export default function App() {
  const adminMode = useMemo(() => {
    if (typeof window === "undefined") return false;
    const params = new URLSearchParams(window.location.search);
    return params.get("admin") === "1";
  }, []);
  const [screen, setScreen] = useState("predict");
  const [date, setDate] = useState(() => localDateKey());
  const [venueId, setVenueId] = useState(1);
  const [raceNo, setRaceNo] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [data, setData] = useState(null);
  const [rankingsLoading, setRankingsLoading] = useState(false);
  const [rankingsError, setRankingsError] = useState("");
  const [rankingsData, setRankingsData] = useState([]);

  const [statsLoading, setStatsLoading] = useState(false);
  const [stats, setStats] = useState(null);
  const [analytics, setAnalytics] = useState(null);
  const [selfLearning, setSelfLearning] = useState(null);
  const [learningLatest, setLearningLatest] = useState(null);
  const [learningRunLoading, setLearningRunLoading] = useState(false);
  const [learningRunNotice, setLearningRunNotice] = useState("");
  const [learningSnapshots, setLearningSnapshots] = useState([]);
  const [startEntryAnalysis, setStartEntryAnalysis] = useState(null);
  const [history, setHistory] = useState([]);
  const [perfError, setPerfError] = useState("");
  const [verifyingRaceId, setVerifyingRaceId] = useState("");
  const [invalidatingRaceId, setInvalidatingRaceId] = useState("");
  const [restoringRaceId, setRestoringRaceId] = useState("");
  const [verificationNotice, setVerificationNotice] = useState("");
  const [verificationRunStatusByRace, setVerificationRunStatusByRace] = useState({});
  const [verificationReasonByRace, setVerificationReasonByRace] = useState({});
  const [resultsStatusFilter, setResultsStatusFilter] = useState("all");
  const [resultsVenueFilter, setResultsVenueFilter] = useState("all");
  const [resultsParticipationFilter, setResultsParticipationFilter] = useState("all");
  const [bulkVerifyRunning, setBulkVerifyRunning] = useState(false);
  const [bulkVerifySummary, setBulkVerifySummary] = useState(null);
  const [editingResultKey, setEditingResultKey] = useState("");
  const [editingResultForm, setEditingResultForm] = useState({
    raceId: "",
    predictionSnapshotId: null,
    confirmedResult: "",
    verificationReason: "",
    invalidReason: ""
  });
  const [editingResultError, setEditingResultError] = useState("");
  const [editingResultNotice, setEditingResultNotice] = useState("");
  const [editingResultSaveKey, setEditingResultSaveKey] = useState("");

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
    race_date: localDateKey(),
    venue_id: 1,
    race_no: 1,
    combo: "",
    bet_amount: 100,
    memo: ""
  });
  const [manualBetType, setManualBetType] = useState("trifecta");
  const [manualSelectionsText, setManualSelectionsText] = useState("");
  const [manualStake, setManualStake] = useState(100);
  const [manualNote, setManualNote] = useState("");
  const [manualCopiedMeta, setManualCopiedMeta] = useState({
    copied_from_ai: false,
    ai_reference_id: null
  });
  const [builderSlots, setBuilderSlots] = useState({ first: null, second: null, third: null });
  const [quickBetAmount, setQuickBetAmount] = useState(100);
  const [pendingTickets, setPendingTickets] = useState([]);
  const [journalFilter, setJournalFilter] = useState("all");
  const [manualLapScores, setManualLapScores] = useState(createManualLapDraft);
  const [manualLapMemo, setManualLapMemo] = useState("");
  const [manualLapSaving, setManualLapSaving] = useState(false);
  const [manualLapNotice, setManualLapNotice] = useState("");

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
  const sourceMeta = data?.source || {};
  const startDisplay = data?.startDisplay || null;
  const prediction = data?.prediction || {};
  const racers = Array.isArray(data?.racers) ? data.racers : [];
  const predictedEntryOrder = Array.isArray(data?.predicted_entry_order)
    ? data.predicted_entry_order
    : Array.isArray(prediction?.predicted_entry_order)
      ? prediction.predicted_entry_order
      : [];
  const actualEntryOrder = Array.isArray(data?.actual_entry_order)
    ? data.actual_entry_order
    : Array.isArray(prediction?.actual_entry_order)
      ? prediction.actual_entry_order
      : [];
  const entryChanged = typeof data?.entry_changed === "boolean"
    ? data.entry_changed
    : predictedEntryOrder.length > 0 &&
      actualEntryOrder.length > 0 &&
      predictedEntryOrder.join("-") !== actualEntryOrder.join("-");
  const entryChangeType = data?.entry_change_type || prediction?.entry_change_type || "none";
  const predictionBeforeEntryChange = data?.prediction_before_entry_change || prediction?.prediction_before_entry_change || null;
  const predictionAfterEntryChange = data?.prediction_after_entry_change || prediction?.prediction_after_entry_change || null;
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
  const raceFlow = data?.raceFlow || {};
  const raceIndexes = data?.raceIndexes || {};
  const marketTrap = data?.marketTrap || {};
  const ticketStrategy = data?.ticketStrategy || {};
  const preRaceAnalysis = data?.preRaceAnalysis || data?.preRaceForm || {};
  const exhibitionAI = data?.exhibitionAI || {};
  const playerStartProfile = data?.playerStartProfile || {};
  const headSelection = data?.headSelection || {};
  const partnerSelection = data?.partnerSelection || {};
  const partnerPrecision = data?.partnerPrecision || {};
  const roleCandidates = data?.roleCandidates || {};
  const raceStructure = data?.raceStructure || {};
  const venueBias = data?.venueBias || {};
  const wallEvaluation = data?.wallEvaluation || {};
  const headConfidence = data?.headConfidence || {};
  const ticketGenerationV2 = data?.ticketGenerationV2 || {};
  const ticketOptimization = data?.ticketOptimization || {};
  const valueDetection = data?.valueDetection || {};
  const scenarioSuggestions = data?.scenarioSuggestions || {};
  const explainability = data?.explainability || {};
  const manualLapEvaluation = data?.manualLapEvaluation || null;
  const bankrollPlan = data?.bankrollPlan || ticketOptimization?.bankrollPlan || {};
  const raceDecision = data?.raceDecision || {};
  const participationDecision = data?.participationDecision || {};
  const confidenceScores = data?.confidenceScores || raceDecision?.confidence_scores || {};
  const recommendationMode = String(
    data?.raceDecision?.mode || raceRisk?.recommendation || data?.recommendation_label || ""
  ).toUpperCase();
  const isRecommendedRace =
    typeof data?.is_recommended === "boolean"
      ? data.is_recommended
      : recommendationMode
        ? recommendationMode !== "SKIP"
        : true;
  const participationLabel =
    participationDecision?.decision === "recommended"
      ? "Recommended"
      : participationDecision?.decision === "watch"
        ? "Watch / Borderline"
        : participationDecision?.decision === "not_recommended"
          ? "Not Recommended"
          : (isRecommendedRace ? "Recommended" : "Not Recommended");
  const participationClass =
    participationDecision?.decision === "recommended"
      ? "risk-full"
      : participationDecision?.decision === "watch"
        ? "risk-small"
        : "risk-skip";
  const formationPatternLabel =
    data?.prediction?.formation_pattern ||
    data?.formation_pattern ||
    raceDecision?.factors?.formation_pattern ||
    raceStructure?.formation_pattern ||
    "-";
  const attackScenario = data?.attackScenario || prediction?.attackScenario || {};
  const attackScenarioLabel =
    attackScenario?.attack_scenario_label ||
    data?.prediction?.attack_scenario_type ||
    prediction?.attack_scenario_type ||
    null;
  const boat1HeadSection = data?.boat1HeadSection || prediction?.boat1HeadSection || {};
  const boat1HeadBets = Array.isArray(boat1HeadSection?.boat1_head_bets_snapshot)
    ? boat1HeadSection.boat1_head_bets_snapshot
    : Array.isArray(prediction?.boat1_head_bets_snapshot)
      ? prediction.boat1_head_bets_snapshot
      : [];
  const boat1HeadSectionFlag = Number(
    boat1HeadSection?.boat1_head_section_shown ?? prediction?.boat1_head_section_shown ?? 0
  );
  const boat1HeadScore = Number(boat1HeadSection?.boat1_head_score ?? prediction?.boat1_head_score ?? 0);
  const boat1SurvivalResidualScore = Number(
    boat1HeadSection?.boat1_survival_residual_score ?? prediction?.boat1_survival_residual_score ?? 0
  );
  const boat1HeadSectionShown =
    boat1HeadBets.length > 0 &&
    (boat1HeadSectionFlag === 1 || boat1HeadScore >= 20 || boat1SurvivalResidualScore >= 18);
  const boat1PriorityModeApplied = Number(
    boat1HeadSection?.boat1_priority_mode_applied ?? prediction?.boat1_priority_mode_applied ?? 0
  ) === 1;
  const boat1HeadTop8Generated = Number(
    boat1HeadSection?.boat1_head_top8_generated ?? prediction?.boat1_head_top8_generated ?? 0
  ) === 1;
  const boat1HeadRatioInFinalBets = Number(
    boat1HeadSection?.boat1_head_ratio_in_final_bets ?? prediction?.boat1_head_ratio_in_final_bets ?? 0
  );
  const boat1HeadReasonTags = Array.isArray(boat1HeadSection?.boat1_head_reason_tags)
    ? boat1HeadSection.boat1_head_reason_tags
    : Array.isArray(prediction?.boat1_head_reason_tags)
      ? prediction.boat1_head_reason_tags
      : [];
  const exactaSection = data?.exactaSection || prediction?.exactaSection || {};
  const exactaBets = Array.isArray(exactaSection?.exacta_recommended_bets_snapshot)
    ? exactaSection.exacta_recommended_bets_snapshot
    : Array.isArray(prediction?.exacta_recommended_bets_snapshot)
      ? prediction.exacta_recommended_bets_snapshot
      : [];
  const exactaSectionShown =
    Number(exactaSection?.exacta_section_shown ?? prediction?.exacta_section_shown ?? 0) === 1 &&
    exactaBets.length > 0;
  const exactaHeadScore = Number(exactaSection?.exacta_head_score ?? prediction?.exacta_head_score ?? 0);
  const exactaPartnerScore = Number(exactaSection?.exacta_partner_score ?? prediction?.exacta_partner_score ?? 0);
  const exactaReasonTags = Array.isArray(exactaSection?.exacta_reason_tags)
    ? exactaSection.exacta_reason_tags
    : Array.isArray(prediction?.exacta_reason_tags)
      ? prediction.exacta_reason_tags
      : [];
  const defaultReasonTags = [
    ...(Array.isArray(participationDecision?.reason_tags) ? participationDecision.reason_tags : []),
    ...(Array.isArray(explainability?.race_tags) ? explainability.race_tags : [])
  ].filter(Boolean).slice(0, 6);
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
          const recommendedBet = Number.isFinite(Number(bet?.recommended_bet))
            ? Number(bet.recommended_bet)
            : Number.isFinite(Number(bet?.bet))
              ? Number(bet.bet)
              : 100;
          return {
            ...bet,
            prob: Number.isFinite(prob) ? prob : null,
            ev: Number.isFinite(Number(bet?.ev)) ? Number(bet.ev) : evSource?.ev,
            odds: oddsByCombo.get(bet?.combo) ?? null,
            roundedBet: roundBetTo100(recommendedBet),
            ticket_type: bet?.ticket_type || "backup",
            value_score: Number.isFinite(Number(bet?.value_score)) ? Number(bet.value_score) : null,
            bet_value_tier: bet?.bet_value_tier || null,
            overpriced_flag: !!bet?.overpriced_flag,
            underpriced_flag: !!bet?.underpriced_flag,
            trap_flags: Array.isArray(bet?.trap_flags) ? bet.trap_flags : [],
            avoid_level: Number.isFinite(Number(bet?.avoid_level)) ? Number(bet.avoid_level) : 0,
            explanation_tags: Array.isArray(bet?.explanation_tags) ? bet.explanation_tags : [],
            explanation_summary: bet?.explanation_summary || null,
            recommended_bet: roundBetTo100(recommendedBet)
          };
        })
        .sort((a, b) => (Number.isFinite(b?.prob) ? b.prob : -1) - (Number.isFinite(a?.prob) ? a.prob : -1)),
    [recommendedBets, probabilityByCombo, evBets, oddsByCombo]
  );
  const finalRecommendedBets = useMemo(() => {
    const optimized = Array.isArray(ticketOptimization?.optimized_tickets)
      ? ticketOptimization.optimized_tickets
      : [];
    if (optimized.length > 0) {
      return optimized
        .map((row) => ({
          combo: row?.combo,
          prob: Number.isFinite(Number(row?.prob)) ? Number(row.prob) : probabilityByCombo.get(row?.combo) ?? null,
          odds: Number.isFinite(Number(row?.odds)) ? Number(row.odds) : oddsByCombo.get(row?.combo) ?? null,
          ev: Number.isFinite(Number(row?.ev)) ? Number(row.ev) : null,
          ticket_type: row?.ticket_type || "backup",
          recommended_bet: roundBetTo100(row?.recommended_bet ?? 100),
          explanation_tags: Array.isArray(row?.explanation_tags) ? row.explanation_tags : [],
          explanation_summary: row?.explanation_summary || null
        }))
        .sort((a, b) => (Number.isFinite(b?.prob) ? b.prob : -1) - (Number.isFinite(a?.prob) ? a.prob : -1))
        .slice(0, 8);
    }
    return recommendedBetsByProb.slice(0, 8);
  }, [ticketOptimization, recommendedBetsByProb, probabilityByCombo, oddsByCombo]);
  const showInternalBetBreakdown = false;
  const simulatedCombos = useMemo(
    () => (Array.isArray(data?.simulation?.top_combinations) ? data.simulation.top_combinations.slice(0, 5) : []),
    [data]
  );
  const startProfileRows = useMemo(
    () =>
      (Array.isArray(playerStartProfile?.profiles) ? playerStartProfile.profiles : [])
        .slice()
        .sort((a, b) => Number(a?.lane || 0) - Number(b?.lane || 0)),
    [playerStartProfile]
  );
  const laneInsightRows = useMemo(
    () =>
      (Array.isArray(ranking) ? ranking : [])
        .map((row) => {
          const lane = parseLane(row?.racer?.lane);
          const features = row?.features || {};
          return {
            lane,
            display_time_delta_vs_left: features?.display_time_delta_vs_left,
            avg_st_rank_delta_vs_left: features?.avg_st_rank_delta_vs_left,
            left_neighbor_exists: features?.left_neighbor_exists,
            slit_alert_flag: features?.slit_alert_flag,
            f_hold_bias_applied: features?.f_hold_bias_applied,
            expected_actual_st_adjustment: features?.expected_actual_st_adjustment
          };
        })
        .filter((row) => Number.isFinite(row.lane)),
    [ranking]
  );
  const fHoldNoteRows = useMemo(
    () => laneInsightRows.filter((row) => Number(row?.f_hold_bias_applied) === 1),
    [laneInsightRows]
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
  const journalTargetMatchesLoadedRace = useMemo(() => {
    if (!data) return false;
    return (
      String(journalForm.race_date || "") === String(race?.date || "") &&
      Number(journalForm.venue_id) === Number(race?.venueId) &&
      Number(journalForm.race_no) === Number(race?.raceNo)
    );
  }, [data, journalForm.race_date, journalForm.venue_id, journalForm.race_no, race?.date, race?.venueId, race?.raceNo]);
  const journalRaceNotRecommended = !!data && journalTargetMatchesLoadedRace && !isRecommendedRace;
  const disableBetActions = !isRecommendedRace;
  const resultsVerificationOnly = true;
  const verificationSummary = useMemo(() => {
    const items = Array.isArray(history) ? history : [];
    const total = items.length;
    let verified = 0;
    let learningReady = 0;
    let hidden = 0;
    let latestVerifiedAt = null;
    for (const row of items) {
      const invalidated = String(row?.verification_status || "").toUpperCase() === "INVALIDATED" || !!row?.invalidation;
      if (invalidated) {
        hidden += 1;
        continue;
      }
      const isVerified =
        String(row?.verification_status || "").toUpperCase().startsWith("VERIFIED") ||
        !!row?.verification?.verified_at;
      if (!isVerified) continue;
      verified += 1;
      const categories = Array.isArray(row?.verification?.mismatch_categories)
        ? row.verification.mismatch_categories
        : [];
      if (categories.length > 0) learningReady += 1;
      const ts = row?.verification?.verified_at ? new Date(row.verification.verified_at) : null;
      if (ts && Number.isFinite(ts.getTime())) {
        if (!latestVerifiedAt || ts.getTime() > latestVerifiedAt.getTime()) latestVerifiedAt = ts;
      }
    }
    const unverified = Math.max(0, total - verified);
    return {
      total,
      verified,
      unverified,
      verificationRate: total > 0 ? Number(((verified / total) * 100).toFixed(2)) : 0,
      learningReady,
      hidden,
      latestVerifiedAt: latestVerifiedAt ? latestVerifiedAt.toISOString() : null
    };
  }, [history]);
  const filteredHistory = useMemo(() => {
    const items = Array.isArray(history) ? history : [];
    return items.filter((row) => {
      if (resultsVenueFilter !== "all" && String(row?.venue_name || row?.venue_id || "") !== resultsVenueFilter) {
        return false;
      }
      if (resultsParticipationFilter !== "all") {
        const decision = normalizeParticipationDecisionValue(
          row?.participation_decision || row?.prediction?.participation_decision || row?.recommendation
        );
        if (decision !== resultsParticipationFilter) return false;
      }
      if (resultsStatusFilter === "all") return true;
      const status = String(row?.verification_status || "").toLowerCase();
      if (resultsStatusFilter === "unverified") return status === "unverified";
      if (resultsStatusFilter === "verified") return status.startsWith("verified");
      if (resultsStatusFilter === "failed") return status === "verify_failed";
      if (resultsStatusFilter === "hidden") return status === "invalidated";
      if (resultsStatusFilter === "missing") {
        return status === "no_bet_snapshot" || status === "no_confirmed_result" || status === "not_verifiable";
      }
      return true;
    });
  }, [history, resultsParticipationFilter, resultsStatusFilter, resultsVenueFilter]);
  const bulkVerifiableHistory = useMemo(() => {
    return filteredHistory.filter((row) => {
      const status = String(row?.verification_status || "").toUpperCase();
      return status === "UNVERIFIED" || status === "VERIFY_FAILED" || status === "NO_BET_SNAPSHOT";
    });
  }, [filteredHistory]);
  const resultsMissPatternSummary = useMemo(
    () => buildResultsMissPatternSummary(filteredHistory),
    [filteredHistory]
  );
  const resultVenueOptions = useMemo(() => {
    const values = Array.from(
      new Set((Array.isArray(history) ? history : []).map((row) => String(row?.venue_name || row?.venue_id || "").trim()).filter(Boolean))
    ).sort((a, b) => a.localeCompare(b));
    return values;
  }, [history]);
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
      const [statsData, analyticsData, historyData, learningData, startEntryData, learningLatestData] = await Promise.all([
        fetchStatsData(),
        fetchAnalyticsData(date),
        fetchHistoryData({ includeInvalidated: adminMode }),
        fetchSelfLearningData(),
        fetchStartEntryAnalysisData(),
        fetchLearningLatestData()
      ]);
      setStats(statsData);
      setAnalytics(analyticsData || null);
      setHistory(Array.isArray(historyData?.items) ? historyData.items : []);
      setSelfLearning(learningData?.selfLearning || null);
      setLearningSnapshots(Array.isArray(learningData?.snapshots) ? learningData.snapshots : []);
      setStartEntryAnalysis(startEntryData || null);
      setLearningLatest(learningLatestData || null);
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
    if (screen === "results") {
      loadPerformance();
    }
  }, [screen]);

  useEffect(() => {
    if (screen === "journal") {
      loadJournal();
    }
  }, [screen]);

  useEffect(() => {
    if (screen === "rankings") {
      loadRankings();
    }
  }, [screen, date]);

  useEffect(() => {
    if (!verificationNotice) return;
    const t = setTimeout(() => setVerificationNotice(""), 2200);
    return () => clearTimeout(t);
  }, [verificationNotice]);

  useEffect(() => {
    if (!learningRunNotice) return;
    const t = setTimeout(() => setLearningRunNotice(""), 2800);
    return () => clearTimeout(t);
  }, [learningRunNotice]);

  const onFetch = async () => {
    setLoading(true);
    setError("");
    try {
      const result = await fetchRaceData(date, venueId, raceNo);
      setData(result);
      const savedScores = result?.manualLapEvaluation?.scores_by_lane;
      setManualLapScores(savedScores && typeof savedScores === "object" ? savedScores : createManualLapDraft());
      setManualLapMemo(result?.manualLapEvaluation?.race_memo || "");
      setManualLapNotice("");
      setResultForm((prev) => ({ ...prev, raceId: result?.raceId || prev.raceId }));
    } catch (e) {
      setError(e.message || "Failed to fetch race data");
    } finally {
      setLoading(false);
    }
  };

  const onRunLearningNow = async () => {
    setLearningRunLoading(true);
    try {
      const result = await runLearningBatchApi({ apply: true, dryRun: false });
      const usedCount = Number(result?.learning_runtime?.used_verification_count ?? 0);
      const remainingCount = Number(result?.learning_runtime?.remaining_unlearned_count ?? 0);
      setLearningRunNotice(
        `学習実行完了: run_id=${result?.run_id ?? "-"}, sample=${result?.sample_size ?? 0}, used=${usedCount}, remaining=${remainingCount}`
      );
      await loadPerformance();
    } catch (e) {
      setPerfError(e.message || "学習実行に失敗しました");
    } finally {
      setLearningRunLoading(false);
    }
  };

  const loadRankings = async () => {
    setRankingsLoading(true);
    setRankingsError("");
    try {
      const result = await fetchRankingsData(date, "hit_rate");
      const rows = Array.isArray(result?.rankings)
        ? result.rankings
        : Array.isArray(result?.items)
          ? result.items
          : [];
      setRankingsData(rows);
    } catch (e) {
      setRankingsError(e.message || "Failed to fetch rankings");
      // keep last successful list for partial/stale display
    } finally {
      setRankingsLoading(false);
    }
  };

  const onOpenRecommendation = async (row) => {
    const nextVenueId = Number(row?.venueId);
    const nextRaceNo = Number(row?.raceNo);
    if (!Number.isInteger(nextVenueId) || !Number.isInteger(nextRaceNo)) return;

    setVenueId(nextVenueId);
    setRaceNo(nextRaceNo);
    setScreen("predict");
    setLoading(true);
    setError("");
    try {
      const result = await fetchRaceData(date, nextVenueId, nextRaceNo);
      setData(result);
      const savedScores = result?.manualLapEvaluation?.scores_by_lane;
      setManualLapScores(savedScores && typeof savedScores === "object" ? savedScores : createManualLapDraft());
      setManualLapMemo(result?.manualLapEvaluation?.race_memo || "");
      setManualLapNotice("");
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

  const onManualLapScoreChange = (lane, field, value) => {
    const normalized = value === "" ? "" : Math.max(0, Math.min(2, Number(value)));
    setManualLapScores((prev) => ({
      ...prev,
      [String(lane)]: {
        ...(prev[String(lane)] || {}),
        [field]: normalized === "" ? "" : Math.trunc(normalized)
      }
    }));
  };

  const onSaveManualLapEvaluation = async () => {
    const payload = {
      raceId: data?.raceId || undefined,
      date: race.date || date,
      venueId: race.venueId || venueId,
      raceNo: race.raceNo || raceNo,
      scores_by_lane: manualLapScores,
      race_memo: manualLapMemo
    };
    setManualLapSaving(true);
    setManualLapNotice("");
    setError("");
    try {
      await saveManualLapEvaluationApi(payload);
      const result = await fetchRaceData(date, venueId, raceNo);
      setData(result);
      const savedScores = result?.manualLapEvaluation?.scores_by_lane;
      setManualLapScores(savedScores && typeof savedScores === "object" ? savedScores : createManualLapDraft());
      setManualLapMemo(result?.manualLapEvaluation?.race_memo || "");
      setManualLapNotice("手動周回展示評価を保存し、予想へ反映しました。");
      setResultForm((prev) => ({ ...prev, raceId: result?.raceId || prev.raceId }));
    } catch (e) {
      setError(e.message || "Failed to save manual lap evaluation");
    } finally {
      setManualLapSaving(false);
    }
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
        source: ticket?.source || "ai",
        bet_type: ticket?.bet_type || "trifecta",
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
    if (journalRaceNotRecommended) {
      setJournalError("Not Recommended race. Betting is disabled for this race.");
      return;
    }
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
    if (journalRaceNotRecommended) {
      setJournalError("Not Recommended race. Betting is disabled for this race.");
      return;
    }
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
          source: t.source || "ai",
          bet_type: t.bet_type || "trifecta",
          selection: t.combo,
          combo: t.combo,
          bet_amount: roundBetTo100(t.bet_amount),
          bought_odds: Number.isFinite(Number(t.odds)) ? Number(t.odds) : null,
          recommended_prob: Number.isFinite(Number(t.prob)) ? Number(t.prob) : null,
          recommended_ev: Number.isFinite(Number(t.ev)) ? Number(t.ev) : null,
          recommended_bet: roundBetTo100(t.bet_amount),
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

  const onRegisterManualBets = async () => {
    if (journalRaceNotRecommended) {
      setJournalError("Not Recommended race. Betting is disabled for this race.");
      return;
    }
    const lines = String(manualSelectionsText || "")
      .split(/\r?\n|,/)
      .map((v) => v.trim())
      .filter(Boolean);
    if (!lines.length) {
      setJournalError("手動ベットの組番を1件以上入力してください。");
      return;
    }
    const stake = roundBetTo100(manualStake);
    if (!Number.isFinite(Number(stake)) || Number(stake) <= 0) {
      setJournalError("購入額は100円以上で入力してください。");
      return;
    }

    const normalized = [];
    for (const raw of lines) {
      const selection = normalizeSelectionByType(manualBetType, raw);
      if (!selection) {
        setJournalError(`組番形式が不正です: ${raw}`);
        return;
      }
      normalized.push(selection);
    }

    const uniqueSelections = [...new Set(normalized)];
    setBetSaving(true);
    setJournalError("");
    try {
      await createPlacedBetsBulk(
        uniqueSelections.map((selection) => ({
          race_id: journalForm.race_id || undefined,
          race_date: journalForm.race_date,
          venue_id: Number(journalForm.venue_id),
          race_no: Number(journalForm.race_no),
          source: "manual",
          bet_type: manualBetType,
          copied_from_ai: manualCopiedMeta.copied_from_ai ? 1 : 0,
          ai_reference_id: manualCopiedMeta.ai_reference_id || null,
          selection,
          combo: selection,
          bet_amount: stake,
          memo: manualNote || null
        }))
      );
      await loadJournal();
      await loadPerformance();
      setManualSelectionsText("");
      setManualNote("");
      setManualCopiedMeta({
        copied_from_ai: false,
        ai_reference_id: null
      });
      setJournalNotice(`手動ベットを登録しました (${uniqueSelections.length}件)`);
    } catch (e) {
      setJournalError(e.message || "Failed to register manual bets");
    } finally {
      setBetSaving(false);
    }
  };

  const onUsePredictedTicket = (bet) => {
    if (disableBetActions) {
      setJournalError("Not Recommended race. Betting is disabled for this race.");
      return;
    }
    const combo = normalizeCombo(bet?.combo);
    if (!combo || combo.split("-").length !== 3) return;

    const selectedRaceDate = race.date || date;
    const selectedVenueId = Number(race.venueId ?? venueId);
    const selectedRaceNo = Number(race.raceNo ?? raceNo);
    const selectedRaceId =
      data?.raceId ||
      `${String(selectedRaceDate || "").replace(/-/g, "")}_${selectedVenueId}_${selectedRaceNo}`;

    const defaultAmount = roundBetTo100(
      bet?.recommended_bet ?? bet?.roundedBet ?? bet?.bet ?? 100
    );
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
      source: "ai",
      bet_type: "trifecta",
      combo,
      bet_amount: defaultAmount,
      prob: bet?.prob,
      ev: bet?.ev,
      odds: bet?.odds
    });
  };

  const onCopyAiToManual = (bet, sourceTag = "ai_recommendation") => {
    const combo = normalizeCombo(bet?.combo);
    if (!combo || combo.split("-").length !== 3) {
      setJournalError("コピー対象の組番が不正です。");
      return;
    }

    const selectedRaceDate = race.date || date;
    const selectedVenueId = Number(race.venueId ?? venueId);
    const selectedRaceNo = Number(race.raceNo ?? raceNo);
    const selectedRaceId =
      data?.raceId ||
      `${String(selectedRaceDate || "").replace(/-/g, "")}_${selectedVenueId}_${selectedRaceNo}`;
    const defaultAmount = roundBetTo100(
      bet?.recommended_bet ?? bet?.roundedBet ?? bet?.bet ?? 100
    );

    setScreen("journal");
    setJournalForm((prev) => ({
      ...prev,
      race_id: selectedRaceId,
      race_date: selectedRaceDate,
      venue_id: selectedVenueId,
      race_no: selectedRaceNo
    }));
    setManualBetType("trifecta");
    setManualStake(defaultAmount);
    setManualSelectionsText((prev) => {
      const lines = String(prev || "")
        .split(/\r?\n|,/)
        .map((v) => v.trim())
        .filter(Boolean);
      if (lines.includes(combo)) return lines.join("\n");
      return [...lines, combo].join("\n");
    });
    setManualCopiedMeta({
      copied_from_ai: true,
      ai_reference_id: `${selectedRaceId}:${sourceTag}`
    });
    setManualNote((prev) => prev || "AIコピー編集");
    setJournalNotice("AI買い目を手動フォームへコピーしました");
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

  const onVerifyRace = async (raceId, predictionSnapshotId = null, options = {}) => {
    const allowDuringBulk = options?.allowDuringBulk === true;
    if (bulkVerifyRunning && !allowDuringBulk) {
      setPerfError("一括検証の実行中です。完了後に再試行してください。");
      return null;
    }
    if (!raceId) {
      setPerfError("検証対象の race_id が見つかりません");
      return null;
    }
    const verificationKey = getVerificationHistoryKey(raceId, predictionSnapshotId);
    setPerfError("");
    setVerifyingRaceId(verificationKey);
    setVerificationRunStatusByRace((prev) => ({ ...prev, [verificationKey]: "PENDING_RESULT" }));
    setVerificationReasonByRace((prev) => ({ ...prev, [verificationKey]: "" }));
    try {
      const result = await verifyRaceResultApi(raceId, predictionSnapshotId);
      const cats = Array.isArray(result?.verification?.mismatch_categories)
        ? result.verification.mismatch_categories
        : [];
      const status = String(result?.status || "VERIFIED").toUpperCase();
      setVerificationRunStatusByRace((prev) => ({ ...prev, [verificationKey]: status }));
      setVerificationReasonByRace((prev) => ({
        ...prev,
        [verificationKey]: result?.warning || result?.message || ""
      }));
      if (status === "NO_BET_SNAPSHOT" || status === "VERIFY_SKIPPED" || status === "NOT_VERIFIABLE") {
        setVerificationNotice(result?.message || "検証スキップ: AI買い目スナップショットがありません");
      } else {
        setVerificationNotice(
          cats.length
            ? `検証完了: ${cats.join(", ")}`
            : "検証完了: ミスマッチカテゴリなし"
        );
      }
      await loadPerformance();
      return { ok: true, status };
    } catch (e) {
      const payload = e?.payload || {};
      const msg = String(payload?.message || e?.message || "");
      const rawStatus = String(payload?.status || "").toUpperCase();
      const status = rawStatus === "NO_BET_SNAPSHOT"
        ? "NO_BET_SNAPSHOT"
        : rawStatus === "NO_CONFIRMED_RESULT" || msg.includes("confirmed race result is not available")
          ? "NO_CONFIRMED_RESULT"
          : "VERIFY_FAILED";
      setVerificationRunStatusByRace((prev) => ({ ...prev, [verificationKey]: status }));
      setVerificationReasonByRace((prev) => ({ ...prev, [verificationKey]: msg || "検証に失敗しました" }));
      setPerfError(msg || "検証に失敗しました");
      await loadPerformance();
      return { ok: false, status, error: msg || "検証に失敗しました" };
    } finally {
      setVerifyingRaceId("");
    }
  };

  const onVerifyAllUnverified = async () => {
    if (bulkVerifyRunning || verifyingRaceId) return;
    const targets = bulkVerifiableHistory;
    if (!targets.length) {
      setVerificationNotice("一括検証対象の未検証レースはありません。");
      return;
    }
    setBulkVerifyRunning(true);
    setBulkVerifySummary({ attempted: 0, verified: 0, skipped: 0, failed: 0, total: targets.length });
    setPerfError("");
    let attempted = 0;
    let verified = 0;
    let skipped = 0;
    let failed = 0;
    try {
      for (const row of targets) {
        attempted += 1;
        setBulkVerifySummary({ attempted, verified, skipped, failed, total: targets.length });
        const result = await onVerifyRace(row?.race_id, row?.prediction_snapshot_id, { allowDuringBulk: true });
        const status = String(result?.status || "").toUpperCase();
        if (result?.ok && status.startsWith("VERIFIED")) verified += 1;
        else if (status === "NO_BET_SNAPSHOT" || status === "NO_CONFIRMED_RESULT") skipped += 1;
        else if (result?.ok) verified += 1;
        else failed += 1;
        setBulkVerifySummary({ attempted, verified, skipped, failed, total: targets.length });
      }
      setVerificationNotice(`一括検証完了: ${verified}件 verified / ${skipped}件 skipped / ${failed}件 failed`);
      await loadPerformance();
    } finally {
      setBulkVerifyRunning(false);
    }
  };

  const onInvalidateVerification = async (row) => {
    const verificationLogId = Number(row?.verification?.id);
    if (!verificationLogId) {
      setPerfError("無効化対象の検証レコードが見つかりません");
      return;
    }
    const reason = window.prompt("無効化理由を入力してください", row?.verification?.invalid_reason || "") ?? "";
    const ok = window.confirm("この検証レコードを Results から隠し、学習対象から除外しますか？");
    if (!ok) return;

    const invalidationKey = getVerificationHistoryKey(row?.race_id, row?.prediction_snapshot_id);
    setInvalidatingRaceId(invalidationKey);
    setPerfError("");
    try {
      await invalidateVerificationApi({
        verificationLogId,
        raceId: row?.race_id,
        predictionSnapshotId: row?.prediction_snapshot_id,
        invalidReason: reason
      });
      setVerificationNotice("検証レコードを無効化しました。Results と学習対象から除外されます。");
      await loadPerformance();
    } catch (e) {
      setPerfError(e?.message || "検証レコードの無効化に失敗しました");
    } finally {
      setInvalidatingRaceId("");
    }
  };

  const onRestoreVerification = async (row) => {
    const verificationLogId = Number(row?.verification?.id);
    if (!verificationLogId) {
      setPerfError("復元対象の検証レコードが見つかりません");
      return;
    }
    const ok = window.confirm("この検証レコードを復元して Results と学習対象へ戻しますか？");
    if (!ok) return;
    const restoreKey = getVerificationHistoryKey(row?.race_id, row?.prediction_snapshot_id);
    setRestoringRaceId(restoreKey);
    setPerfError("");
    try {
      await restoreVerificationApi({ verificationLogId });
      setVerificationNotice("検証レコードを復元しました。");
      await loadPerformance();
    } catch (e) {
      setPerfError(e?.message || "検証レコードの復元に失敗しました");
    } finally {
      setRestoringRaceId("");
    }
  };

  const onEditVerificationNote = async (row) => {
    const verificationLogId = Number(row?.verification?.id);
    if (!verificationLogId) {
      setPerfError("更新対象の検証レコードが見つかりません");
      return;
    }
    const nextNote = window.prompt("検証メモ / 理由を入力してください", row?.verification?.verification_reason || row?.verification_reason || "") ?? "";
    try {
      await updateVerificationNoteApi({
        verificationLogId,
        verificationReason: nextNote
      });
      setVerificationNotice("検証メモを更新しました。");
      await loadPerformance();
    } catch (e) {
      setPerfError(e?.message || "検証メモの更新に失敗しました");
    }
  };

  const clearVerificationUiStateForRace = (raceId) => {
    const raceKey = String(raceId || "");
    if (!raceKey) return;
    const matchingKeys = new Set(
      history
        .filter((row) => String(row?.race_id || "") === raceKey)
        .map((row) => getVerificationHistoryKey(row?.race_id, row?.prediction_snapshot_id))
    );
    matchingKeys.add(getVerificationHistoryKey(raceKey, null));
    setVerificationRunStatusByRace((prev) =>
      Object.fromEntries(Object.entries(prev).filter(([key]) => !matchingKeys.has(key)))
    );
    setVerificationReasonByRace((prev) =>
      Object.fromEntries(Object.entries(prev).filter(([key]) => !matchingKeys.has(key)))
    );
  };

  const onEditResultRecord = (row) => {
    const actualTop3 = Array.isArray(row?.actual_top3) ? row.actual_top3 : [];
    const currentCombo = normalizeCombo(row?.confirmed_result || actualTop3.join("-"));
    const editKey = getVerificationHistoryKey(row?.race_id, row?.prediction_snapshot_id);
    setEditingResultKey(editKey);
    setEditingResultError("");
    setEditingResultNotice("");
    setPerfError("");
    setScreen("results");
    setEditingResultForm({
      raceId: row?.race_id || "",
      predictionSnapshotId: Number.isFinite(Number(row?.prediction_snapshot_id))
        ? Number(row.prediction_snapshot_id)
        : null,
      confirmedResult: currentCombo || "",
      verificationReason: row?.verification?.verification_reason || row?.verification_reason || "",
      invalidReason: row?.verification?.invalid_reason || ""
    });
    setResultForm({
      raceId: row?.race_id || "",
      finish1: actualTop3[0] ?? "",
      finish2: actualTop3[1] ?? "",
      finish3: actualTop3[2] ?? "",
      payoutCombo: row?.confirmed_result || "",
      payoutAmount: ""
    });
  };

  const onCancelEditResultRecord = () => {
    setEditingResultKey("");
    setEditingResultError("");
    setEditingResultNotice("");
    setEditingResultSaveKey("");
  };

  const onSaveEditedResultRecord = async () => {
    const raceId = String(editingResultForm?.raceId || "").trim();
    const normalizedConfirmedResult = normalizeCombo(editingResultForm?.confirmedResult);
    const finishOrder = normalizedConfirmedResult
      .split("-")
      .map((v) => Number(v))
      .filter((v) => Number.isInteger(v) && v >= 1 && v <= 6);

    if (!raceId) {
      setEditingResultError("race_id が見つかりません。");
      return;
    }
    if (finishOrder.length !== 3 || new Set(finishOrder).size !== 3) {
      setEditingResultError("確定結果は 1-2-3 の形式で、1-6 の重複しない3艇を入力してください。");
      return;
    }

    const saveKey = getVerificationHistoryKey(raceId, editingResultForm?.predictionSnapshotId);
    setEditingResultSaveKey(saveKey);
    setEditingResultError("");
    setEditingResultNotice("");
    setPerfError("");
    try {
      const result = await editResultRecordApi({
        raceId,
        predictionSnapshotId: editingResultForm?.predictionSnapshotId,
        confirmedResult: normalizedConfirmedResult,
        verificationReason: editingResultForm?.verificationReason || "",
        invalidReason: editingResultForm?.invalidReason || ""
      });
      clearVerificationUiStateForRace(raceId);
      await loadPerformance();
      setEditingResultKey("");
      setEditingResultNotice(
        result?.reverification_required
          ? "保存しました。旧検証を無効化し、再検証が必要な状態に更新しました。"
          : "保存しました。"
      );
      setVerificationNotice(
        result?.reverification_required
          ? "結果を更新しました。旧検証は無効化され、再検証が必要です。"
          : "結果を更新しました。"
      );
    } catch (e) {
      setEditingResultError(e?.message || "結果の保存に失敗しました。");
    } finally {
      setEditingResultSaveKey("");
    }
  };

  const onSettleRace = async (group) => {
    const raceId = group?.raceId;
    setSettlingRaceId(String(raceId));
    setJournalError("");
    try {
      const settleResult = await settlePlacedBets({
        race_id: raceId,
        race_date: group?.raceDate,
        venue_id: group?.venueId,
        race_no: group?.raceNo
      });
      console.info("[UI][SETTLEMENT] response", settleResult?.settlement_debug || settleResult);
      await loadJournal();
      await loadPerformance();
      const updatedRows = Number(settleResult?.settlement_debug?.updated_rows ?? settleResult?.updated_rows ?? 0);
      if (updatedRows <= 0) {
        const dbg = settleResult?.settlement_debug || {};
        setJournalError(
          `精算更新0件: race=${dbg.race_id || raceId}, result=${dbg.fetched_result || "-"}, bets=${
            dbg.placed_bets_found ?? "-"
          }, updated=${dbg.updated_rows ?? 0}`
        );
      }
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
    const today = localDateKey(now);
    const weekAgo = new Date(now);
    weekAgo.setDate(now.getDate() - 7);
    const weekAgoStr = localDateKey(weekAgo);
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
            <button className={screen === "results" ? "tab on" : "tab"} onClick={() => setScreen("results")}>結果</button>
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
                <div className="prediction-summary-grid">
                <section className="card summary-card">
                  <h2>レース情報</h2>
                  <div className="metric-grid compact">
                    <div className="metric-item"><span>会場 / レース</span><strong>{race.venueId ?? venueId} {venueName} {race.raceNo ?? raceNo}R</strong></div>
                    <div className="metric-item"><span>レース名</span><strong>{race.raceName || "-"}</strong></div>
                    <div className="metric-item"><span>日付</span><strong>{race.date || date}</strong></div>
                  </div>
                </section>

                {adminMode ? (
                  <section className="card">
                    <h2>手動周回展示評価（管理用）</h2>
                    <p className="muted strategy-line">
                      本番ワークフローでは無効化されています。自動取得の展示データのみ予想に使用します。
                    </p>
                    <div className="manual-lap-grid">
                      {Array.from({ length: 6 }, (_, idx) => idx + 1).map((lane) => (
                        <div key={`lap-${lane}`} className="manual-lap-row">
                          <div className="manual-lap-lane">
                            <span className={`combo-dot ${BOAT_META[lane]?.className || ""}`}>{lane}</span>
                          </div>
                          {MANUAL_LAP_FIELDS.map((field) => (
                            <label key={`lap-${lane}-${field.key}`} className="manual-lap-field">
                              <span>{field.label}</span>
                              <input
                                type="number"
                                min="0"
                                max="2"
                                step="1"
                                value={manualLapScores?.[String(lane)]?.[field.key] ?? ""}
                                onChange={(e) => onManualLapScoreChange(lane, field.key, e.target.value)}
                              />
                            </label>
                          ))}
                        </div>
                      ))}
                    </div>
                    <label className="manual-lap-memo">
                      <span>メモ（任意）</span>
                      <textarea
                        value={manualLapMemo}
                        onChange={(e) => setManualLapMemo(e.target.value)}
                        rows={2}
                      />
                    </label>
                    <div className="row-actions">
                      <button className="fetch-btn" onClick={onSaveManualLapEvaluation} disabled={manualLapSaving}>
                        {manualLapSaving ? "保存中..." : "保存（管理用）"}
                      </button>
                      {manualLapEvaluation?.updated_at ? (
                        <span className="muted">最終更新: {new Date(manualLapEvaluation.updated_at).toLocaleString()}</span>
                      ) : null}
                    </div>
                    {manualLapNotice ? <div className="notice-banner">{manualLapNotice}</div> : null}
                  </section>
                ) : null}

                <section className={`card recommendation summary-card ${getRiskClass(raceRisk.recommendation)}`}>
                  <h2>総合推奨</h2>
                  <div className="recommend-grid">
                    <div><span>参加可否</span><strong className={`status-pill ${participationClass}`}>{participationLabel}</strong></div>
                    <div>
                      <span>頭固定信頼度</span>
                      <strong>
                        {formatMaybeNumber(confidenceScores?.head_fixed_confidence_pct, 1)}%
                        {" "}
                        <span className={`status-pill ${getConfidenceBandClass(confidenceScores?.head_fixed_band)}`}>
                          {getConfidenceBandLabel(confidenceScores?.head_fixed_band)}
                        </span>
                      </strong>
                    </div>
                    <div>
                      <span>推奨買い目信頼度</span>
                      <strong>
                        {formatMaybeNumber(confidenceScores?.recommended_bet_confidence_pct, 1)}%
                        {" "}
                        <span className={`status-pill ${getConfidenceBandClass(confidenceScores?.recommended_bet_band)}`}>
                          {getConfidenceBandLabel(confidenceScores?.recommended_bet_band)}
                        </span>
                      </strong>
                    </div>
                    <div><span>展開パターン</span><strong>{data.racePattern || "-"}</strong></div>
                    <div><span>formation</span><strong>{formationPatternLabel}</strong></div>
                    {attackScenarioLabel ? <div><span>attack</span><strong>{attackScenarioLabel}</strong></div> : null}
                  </div>
                  {!isRecommendedRace ? (
                    <p className="muted strategy-line">このレースは見送り判定です。ベット追加は無効化されています。</p>
                  ) : null}
                  {defaultReasonTags.length > 0 ? (
                    <div className="chips-wrap">
                      {defaultReasonTags.map((tag) => <span className="chip" key={`pd-tag-${tag}`}>{tag}</span>)}
                    </div>
                  ) : null}
                </section>

                <article className="card summary-card">
                  <h2>最終推奨買い目</h2>
                  <div className="summary-inline-meta">
                    <span>{finalRecommendedBets.length}件</span>
                    <span>{attackScenarioLabel || formationPatternLabel || data.racePattern || "-"}</span>
                  </div>
                  <div className="list-stack compact-list">
                    {finalRecommendedBets.map((bet, idx) => (
                      <div key={`${bet.combo}-${idx}`} className="list-stack">
                        <div className="list-row list-row-actions">
                          <strong><ComboBadge combo={bet.combo} /></strong>
                          <span className={`ticket-type ${getTicketTypeClass(bet.ticket_type)}`}>{getTicketTypeLabel(bet.ticket_type)}</span>
                          <span>p {Number.isFinite(bet.prob) ? formatMaybeNumber(bet.prob, 3) : "-"}</span>
                          <span>JPY {(bet.recommended_bet ?? bet.roundedBet).toLocaleString()}</span>
                          <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket(bet)} disabled={disableBetActions} title={disableBetActions ? "Not Recommended race" : ""}>
                            記録に追加
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                </article>

                {boat1HeadSectionShown ? (
                  <article className="card summary-card">
                    <h2>1号艇が頭になる場合の予想</h2>
                    <div className="summary-inline-meta">
                      <span>{boat1HeadBets.length}件</span>
                      <span>
                        score {formatMaybeNumber(boat1HeadScore, 1)}
                        {" / "}
                        residual {formatMaybeNumber(boat1SurvivalResidualScore, 1)}
                        {boat1PriorityModeApplied ? ` / ratio ${formatMaybeNumber(boat1HeadRatioInFinalBets * 100, 1)}%` : ""}
                        {boat1HeadTop8Generated ? " / top8" : ""}
                      </span>
                    </div>
                    {boat1HeadReasonTags.length > 0 ? (
                      <div className="chips-wrap" style={{ marginBottom: 8 }}>
                        {boat1HeadReasonTags.map((tag) => <span className="chip" key={`b1-tag-${tag}`}>{tag}</span>)}
                      </div>
                    ) : null}
                    <div className="list-stack compact-list">
                      {boat1HeadBets.map((bet, idx) => (
                        <div key={`b1-${bet.combo}-${idx}`} className="list-stack">
                          <div className="list-row list-row-actions">
                            <strong><ComboBadge combo={bet.combo} /></strong>
                            <span>p {Number.isFinite(Number(bet?.prob)) ? formatMaybeNumber(bet.prob, 3) : "-"}</span>
                            <span>score {formatMaybeNumber(bet?.boat1_head_score, 1)}</span>
                            <span>JPY {Number(bet?.recommended_bet ?? bet?.bet ?? 0).toLocaleString()}</span>
                            <button
                              className="fetch-btn secondary"
                              onClick={() => onUsePredictedTicket(bet)}
                              disabled={disableBetActions}
                              title={disableBetActions ? "Not Recommended race" : ""}
                            >
                              記録に追加
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </article>
                ) : null}

                {exactaSectionShown ? (
                  <article className="card summary-card">
                    <h2>二連単本線カバー</h2>
                    <div className="summary-inline-meta">
                      <span>{exactaBets.length}件</span>
                      <span>head {formatMaybeNumber(exactaHeadScore, 1)} / partner {formatMaybeNumber(exactaPartnerScore, 1)}</span>
                    </div>
                    {exactaReasonTags.length > 0 ? (
                      <div className="chips-wrap" style={{ marginBottom: 8 }}>
                        {exactaReasonTags.map((tag) => <span className="chip" key={`exacta-tag-${tag}`}>{tag}</span>)}
                      </div>
                    ) : null}
                    <div className="list-stack compact-list">
                      {exactaBets.map((bet, idx) => (
                        <div key={`exacta-${bet.combo}-${idx}`} className="list-stack">
                          <div className="list-row list-row-actions">
                            <strong><ComboBadge combo={bet.combo} /></strong>
                            <span>p {Number.isFinite(Number(bet?.prob)) ? formatMaybeNumber(bet.prob, 3) : "-"}</span>
                            <span>head {formatMaybeNumber(bet?.exacta_head_score, 1)}</span>
                            <span>partner {formatMaybeNumber(bet?.exacta_partner_score, 1)}</span>
                            <span>JPY {Number(bet?.recommended_bet ?? bet?.bet ?? 0).toLocaleString()}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </article>
                ) : null}
                </div>

                <details className="card">
                  <summary>展示・進入の詳細</summary>
                  <div style={{ marginTop: 10 }}>
                    <div className="kv-list" style={{ marginBottom: 10 }}>
                      <div className="kv-row"><span>頭信頼度 raw</span><strong>{formatMaybeNumber(confidenceScores?.head_confidence_raw, 1)}%</strong></div>
                      <div className="kv-row"><span>頭信頼度 calibrated</span><strong>{formatMaybeNumber(confidenceScores?.head_confidence_calibrated, 1)}%</strong></div>
                      <div className="kv-row"><span>買い目信頼度 raw</span><strong>{formatMaybeNumber(confidenceScores?.bet_confidence_raw, 1)}%</strong></div>
                      <div className="kv-row"><span>買い目信頼度 calibrated</span><strong>{formatMaybeNumber(confidenceScores?.bet_confidence_calibrated, 1)}%</strong></div>
                      <div className="kv-row"><span>calibration</span><strong>{confidenceScores?.confidence_calibration_applied ? "applied" : "raw only"}</strong></div>
                      <div className="kv-row"><span>source</span><strong>{confidenceScores?.confidence_calibration_source || "-"}</strong></div>
                    </div>
                    <h3>Start Exhibition</h3>
                    <StartExhibitionDisplay startDisplay={startDisplay} />
                    <p className="muted strategy-line">
                      {sourceMeta?.cache?.fallback === "db_snapshot"
                        ? "公式取得失敗のため保存済みスナップショットを使用"
                        : sourceMeta?.cache?.hit
                          ? "バックエンドキャッシュを使用"
                          : "公式事前情報から自動取得"}
                    </p>
                    <div className="kv-list" style={{ marginTop: 10 }}>
                      <div className="kv-row"><span>entry_changed</span><strong>{entryChanged ? "あり" : "なし"}</strong></div>
                      <div className="kv-row"><span>entry_change_type</span><strong>{entryChangeType || "-"}</strong></div>
                      <div className="kv-row"><span>予測進入順</span><strong><LanePills lanes={predictedEntryOrder} /></strong></div>
                      <div className="kv-row"><span>実進入順</span><strong><LanePills lanes={actualEntryOrder} /></strong></div>
                      <div className="kv-row"><span>attack scenario</span><strong>{attackScenarioLabel || "-"}</strong></div>
                    </div>
                    <div className="table-wrap" style={{ marginTop: 10 }}>
                      <table>
                        <thead>
                          <tr>
                            <th>艇番</th>
                            <th>選手名</th>
                            <th>展示タイム</th>
                            <th>展示ST</th>
                            <th>進入</th>
                            <th>チルト</th>
                          </tr>
                        </thead>
                        <tbody>
                          {racers.map((racer, idx) => (
                            <tr key={`auto-lap-${racer.lane}-${idx}`}>
                              <td>{racer.lane ?? "-"}</td>
                              <td>{racer.name || "-"}</td>
                              <td>{formatMaybeNumber(racer.exhibitionTime, 2)}</td>
                              <td>{formatMaybeNumber(racer.exhibitionST ?? racer.exhibitionSt, 2)}</td>
                              <td>{racer.entryCourse ?? "-"}</td>
                              <td>{formatMaybeNumber(racer.tilt, 1)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </details>

                <details className="card">
                  <summary>選手・左隣比較・F持ちの詳細</summary>
                  <div style={{ marginTop: 10 }}>
                    <div className="table-wrap">
                      <table>
                        <thead><tr><th>艇番</th><th>左隣あり</th><th>展示差</th><th>平均ST順位差</th><th>slit_alert</th><th>F caution</th><th>actual ST補正</th></tr></thead>
                        <tbody>
                          {laneInsightRows.map((row) => (
                            <tr key={`insight-${row.lane}`}>
                              <td>{row.lane}</td>
                              <td>{row.left_neighbor_exists ? "あり" : "-"}</td>
                              <td>{formatMaybeNumber(row.display_time_delta_vs_left, 3)}</td>
                              <td>{formatMaybeNumber(row.avg_st_rank_delta_vs_left, 1)}</td>
                              <td>{row.slit_alert_flag ? "ON" : "-"}</td>
                              <td>{row.f_hold_bias_applied ? "ON" : "-"}</td>
                              <td>{formatMaybeNumber(row.expected_actual_st_adjustment, 3)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    {fHoldNoteRows.length > 0 ? (
                      <p className="muted strategy-line">
                        F-hold caution:
                        {" "}
                        {fHoldNoteRows.map((row) => `${row.lane}号艇(+${formatMaybeNumber(row.expected_actual_st_adjustment, 3)})`).join(", ")}
                      </p>
                    ) : null}
                    <section className="card ranking-card" style={{ marginTop: 10 }}>
                      <h3>AI総合評価ランキング</h3>
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
                    </section>
                    <div className="table-wrap" style={{ marginTop: 10 }}>
                      <table>
                        <thead><tr><th>艇番</th><th>選手名</th><th>級別</th><th>全国勝率</th><th>当地勝率</th><th>モーター2連率</th><th>展示タイム</th><th>展示ST</th><th>進入</th></tr></thead>
                        <tbody>
                          {racers.map((racer, idx) => <tr key={`${racer.lane}-${idx}`}><td>{racer.lane ?? "-"}</td><td>{racer.name || "-"}</td><td>{racer.class || "-"}</td><td>{formatMaybeNumber(racer.nationwideWinRate, 2)}</td><td>{formatMaybeNumber(racer.localWinRate, 2)}</td><td>{formatMaybeNumber(racer.motor2Rate, 2)}</td><td>{formatMaybeNumber(racer.exhibitionTime, 2)}</td><td>{formatMaybeNumber(racer.exhibitionST, 2)}</td><td>{racer.entryCourse ?? "-"}</td></tr>)}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </details>

                <details className="card">
                  <summary>シナリオ・分析の詳細</summary>
                  <div style={{ marginTop: 10 }}>
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
                    <h2>レースフロー</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>flow_mode</span><strong>{raceFlow.race_flow_mode || "-"}</strong></div>
                      <div className="kv-row"><span>nige_prob</span><strong>{formatMaybeNumber((raceFlow.nige_prob ?? 0) * 100, 1)}%</strong></div>
                      <div className="kv-row"><span>sashi_prob</span><strong>{formatMaybeNumber((raceFlow.sashi_prob ?? 0) * 100, 1)}%</strong></div>
                      <div className="kv-row"><span>makuri_prob</span><strong>{formatMaybeNumber((raceFlow.makuri_prob ?? 0) * 100, 1)}%</strong></div>
                      <div className="kv-row"><span>makurizashi_prob</span><strong>{formatMaybeNumber((raceFlow.makurizashi_prob ?? 0) * 100, 1)}%</strong></div>
                      <div className="kv-row"><span>flow_confidence</span><strong>{formatMaybeNumber((raceFlow.flow_confidence ?? 0) * 100, 1)}%</strong></div>
                    </div>
                  </article>

                  <article className="card analysis-card">
                    <h2>進入変化</h2>
                    <div className="kv-list">
                      <div className="kv-row">
                        <span>entry_changed</span>
                        <strong>{entryChanged ? "あり" : "なし"}</strong>
                      </div>
                      <div className="kv-row">
                        <span>entry_change_type</span>
                        <strong>{entryChangeType || "-"}</strong>
                      </div>
                      <div className="kv-row">
                        <span>予測進入順</span>
                        <strong><LanePills lanes={predictedEntryOrder} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>実進入順</span>
                        <strong><LanePills lanes={actualEntryOrder} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>変化前 top3</span>
                        <strong>{Array.isArray(predictionBeforeEntryChange?.top3) ? predictionBeforeEntryChange.top3.join("-") : "-"}</strong>
                      </div>
                      <div className="kv-row">
                        <span>変化後 top3</span>
                        <strong>{Array.isArray(predictionAfterEntryChange?.top3) ? predictionAfterEntryChange.top3.join("-") : "-"}</strong>
                      </div>
                    </div>
                    {entryChanged ? (
                      <p className="muted strategy-line">進入変化を反映して予想・推奨を再計算しています。</p>
                    ) : null}
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
                    <h2>マーケットトラップ</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>trap_score</span><strong>{formatMaybeNumber(marketTrap.trap_score, 2)}</strong></div>
                      <div className="kv-row">
                        <span>trap_flags</span>
                        <strong>{Array.isArray(marketTrap.trap_flags) && marketTrap.trap_flags.length ? marketTrap.trap_flags.join(", ") : "-"}</strong>
                      </div>
                    </div>
                    <p className="muted strategy-line">{marketTrap.summary || "-"}</p>
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
                    <h2>展示AI分析</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>exhibition_time_score</span><strong>{formatMaybeNumber(exhibitionAI.exhibition_time_score, 2)}</strong></div>
                      <div className="kv-row"><span>exhibition_st_score</span><strong>{formatMaybeNumber(exhibitionAI.exhibition_st_score, 2)}</strong></div>
                      <div className="kv-row"><span>exhibition_gap_score</span><strong>{formatMaybeNumber(exhibitionAI.exhibition_gap_score, 2)}</strong></div>
                      <div className="kv-row"><span>exhibition_rank_score</span><strong>{formatMaybeNumber(exhibitionAI.exhibition_rank_score, 2)}</strong></div>
                      <div className="kv-row"><span>exhibition_breakout_score</span><strong>{formatMaybeNumber(exhibitionAI.exhibition_breakout_score, 2)}</strong></div>
                      <div className="kv-row"><span>exhibition_ai_score</span><strong>{formatMaybeNumber(exhibitionAI.exhibition_ai_score, 2)}</strong></div>
                      <div className="kv-row"><span>top_exhibition_lane</span><strong><LanePills lanes={[Number(exhibitionAI.top_exhibition_lane)]} /></strong></div>
                      <div className="kv-row"><span>stable_st_lane</span><strong><LanePills lanes={[Number(exhibitionAI.stable_st_lane)]} /></strong></div>
                      <div className="kv-row"><span>breakout_lane</span><strong><LanePills lanes={[Number(exhibitionAI.breakout_lane)]} /></strong></div>
                      <div className="kv-row"><span>weak_lane</span><strong><LanePills lanes={[Number(exhibitionAI.weak_lane)]} /></strong></div>
                    </div>
                  </article>

                  <article className="card analysis-card">
                    <h2>選手スタートプロファイル</h2>
                    <div className="list-stack">
                      {startProfileRows.length === 0 ? (
                        <p className="muted">データなし</p>
                      ) : (
                        startProfileRows.map((row) => (
                          <div key={`sp-${row.lane}`} className="list-row">
                            <strong><LanePills lanes={[Number(row.lane)]} /></strong>
                            <span>attack {formatMaybeNumber(row.start_attack_score, 1)}</span>
                            <span>stable {formatMaybeNumber(row.start_stability_score, 1)}</span>
                            <span>{row.player_start_profile || "-"}</span>
                          </div>
                        ))
                      )}
                    </div>
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
                    <h2>相手精度</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>2着候補</span><strong><LanePills lanes={partnerPrecision.second_candidates || []} /></strong></div>
                      <div className="kv-row"><span>3着候補</span><strong><LanePills lanes={partnerPrecision.third_candidates || []} /></strong></div>
                      <div className="kv-row"><span>残し内枠</span><strong><LanePills lanes={partnerPrecision.residual_inside_lanes || []} /></strong></div>
                      <div className="kv-row"><span>3着外枠</span><strong><LanePills lanes={partnerPrecision.outside_third_lanes || []} /></strong></div>
                      <div className="kv-row"><span>2着適性</span><strong>{formatMaybeNumber(partnerPrecision.second_place_fit_score, 1)}</strong></div>
                      <div className="kv-row"><span>3着適性</span><strong>{formatMaybeNumber(partnerPrecision.third_place_fit_score, 1)}</strong></div>
                      <div className="kv-row"><span>残しスコア</span><strong>{formatMaybeNumber(partnerPrecision.residual_lane_score, 1)}</strong></div>
                      <div className="kv-row"><span>外3着スコア</span><strong>{formatMaybeNumber(partnerPrecision.outside_third_score, 1)}</strong></div>
                    </div>
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
                    <h2>場バイアス</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>venue_bias_score</span><strong>{formatMaybeNumber(venueBias.venue_bias_score, 2)}</strong></div>
                      <div className="kv-row"><span>venue_inner_reliability</span><strong>{formatMaybeNumber(venueBias.venue_inner_reliability, 2)}</strong></div>
                      <div className="kv-row"><span>venue_chaos_factor</span><strong>{formatMaybeNumber(venueBias.venue_chaos_factor, 2)}</strong></div>
                      <div className="kv-row"><span>venue_style_bias</span><strong>{venueBias.venue_style_bias || "-"}</strong></div>
                    </div>
                    <p className="muted strategy-line">{venueBias.summary || "-"}</p>
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
                    <h2>資金配分プラン</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>mode</span><strong>{bankrollPlan.mode || raceDecision.mode || "-"}</strong></div>
                      <div className="kv-row"><span>race_budget</span><strong>JPY {Number(bankrollPlan.race_budget || 0).toLocaleString()}</strong></div>
                      <div className="kv-row"><span>allocation_style</span><strong>{bankrollPlan.allocation_style || "-"}</strong></div>
                    </div>
                    <p className="muted strategy-line">{bankrollPlan.summary || "-"}</p>
                  </article>

                  <article className="card">
                    <h2>Value検出</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>value_balance_score</span><strong>{formatMaybeNumber(valueDetection.value_balance_score, 2)}</strong></div>
                      <div className="kv-row"><span>low_value_risk</span><strong>{formatMaybeNumber(valueDetection.low_value_risk, 2)}</strong></div>
                      <div className="kv-row"><span>price_quality_score</span><strong>{formatMaybeNumber(valueDetection.price_quality_score, 2)}</strong></div>
                    </div>
                    <p className="muted strategy-line">{valueDetection.summary || "-"}</p>
                  </article>

                  {showInternalBetBreakdown && (
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
                  )}

                  {showInternalBetBreakdown && (
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
                        <div key={`opt-${row.combo}-${idx}`} className="list-stack">
                          <div className="list-row list-row-actions">
                            <strong><ComboBadge combo={row.combo} /></strong>
                            <span className={`ticket-type ${getTicketTypeClass(row.ticket_type)}`}>{getTicketTypeLabel(row.ticket_type)}</span>
                            <span>p {formatMaybeNumber(row.prob, 3)}</span>
                            <span>odds {formatMaybeNumber(row.odds, 1)}</span>
                            <span>ev {formatMaybeNumber(row.ev, 2)}</span>
                            <span className={`ticket-value ${getValueTierClass(row.bet_value_tier)}`}>
                              {getValueTierLabel(row.bet_value_tier)}
                            </span>
                            <span className={`ticket-trap ${getAvoidLevelClass(row.avoid_level)}`}>
                              {getAvoidLevelLabel(row.avoid_level)}
                            </span>
                            <span className="bet-amount-strong">JPY {Number(row.recommended_bet || 0).toLocaleString()}</span>
                            <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket({ combo: row.combo, prob: row.prob, odds: row.odds, ev: row.ev, bet: row.recommended_bet })} disabled={disableBetActions} title={disableBetActions ? "Not Recommended race" : ""}>
                              記録に追加
                            </button>
                            <button className="fetch-btn secondary" onClick={() => onCopyAiToManual({ combo: row.combo, prob: row.prob, odds: row.odds, ev: row.ev, bet: row.recommended_bet }, "optimized_ticket")} title="手動フォームへコピー">
                              手動へコピー
                            </button>
                          </div>
                          {(Array.isArray(row.explanation_tags) && row.explanation_tags.length > 0) ? (
                            <div className="chips-wrap">
                              {row.explanation_tags.slice(0, 4).map((tag) => <span className="chip" key={`opt-exp-${row.combo}-${tag}`}>{tag}</span>)}
                            </div>
                          ) : null}
                          {row.explanation_summary ? <p className="muted strategy-line">{row.explanation_summary}</p> : null}
                        </div>
                      ))}
                    </div>
                  </article>
                  )}

                  {showInternalBetBreakdown && simulatedCombos.length > 0 && (
                    <article className="card">
                      <h2>シミュレーション上位</h2>
                      <div className="list-stack">
                        {simulatedCombos.map((row, idx) => (
                          <div key={`${row.combo}-${idx}`} className="list-row list-row-actions">
                            <strong><ComboBadge combo={row.combo} /></strong>
                            <span>p {formatMaybeNumber(row.prob, 4)}</span>
                            <span>odds {Number.isFinite(oddsByCombo.get(row.combo)) ? formatMaybeNumber(oddsByCombo.get(row.combo), 1) : "-"}</span>
                            <span>-</span>
                            <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket({ combo: row.combo, prob: row.prob, odds: oddsByCombo.get(row.combo), bet: 100 })} disabled={disableBetActions} title={disableBetActions ? "Not Recommended race" : ""}>
                              記録に追加
                            </button>
                            <button className="fetch-btn secondary" onClick={() => onCopyAiToManual({ combo: row.combo, prob: row.prob, odds: oddsByCombo.get(row.combo), bet: 100 }, "simulation_top")} title="手動フォームへコピー">
                              手動へコピー
                            </button>
                          </div>
                        ))}
                      </div>
                    </article>
                  )}

                  {showInternalBetBreakdown && (
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
                          <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket({ combo, bet: 100 })} disabled={disableBetActions} title={disableBetActions ? "Not Recommended race" : ""}>
                            記録に追加
                          </button>
                          <button className="fetch-btn secondary" onClick={() => onCopyAiToManual({ combo, bet: 100 }, "ticket_v2_primary")} title="手動フォームへコピー">
                            手動へコピー
                          </button>
                        </div>
                      ))}
                      {(ticketGenerationV2.secondary_tickets || []).slice(0, 8).map((combo, idx) => (
                        <div key={`tgv2-s-${combo}-${idx}`} className="list-row list-row-actions">
                          <strong><ComboBadge combo={combo} /></strong>
                          <span>押さえ</span>
                          <span>-</span>
                          <span>-</span>
                          <button className="fetch-btn secondary" onClick={() => onUsePredictedTicket({ combo, bet: 100 })} disabled={disableBetActions} title={disableBetActions ? "Not Recommended race" : ""}>
                            記録に追加
                          </button>
                          <button className="fetch-btn secondary" onClick={() => onCopyAiToManual({ combo, bet: 100 }, "ticket_v2_secondary")} title="手動フォームへコピー">
                            手動へコピー
                          </button>
                        </div>
                      ))}
                    </div>
                  </article>
                  )}

                  {showInternalBetBreakdown && (
                  <article className="card">
                    <h2>シナリオ別買い目</h2>
                    <div className="kv-list">
                      <div className="kv-row"><span>scenario_type</span><strong>{scenarioSuggestions.scenario_type || "-"}</strong></div>
                      <div className="kv-row"><span>scenario_confidence</span><strong>{formatMaybeNumber(scenarioSuggestions.scenario_confidence, 1)}</strong></div>
                    </div>
                    <div className="list-stack">
                      <div className="list-row">
                        <span className={`ticket-type ${getTicketTypeClass("main")}`}>本線</span>
                        <strong>
                          {Array.isArray(scenarioSuggestions.main_picks) && scenarioSuggestions.main_picks.length
                            ? scenarioSuggestions.main_picks.slice(0, 6).map((combo, idx) => <ComboBadge combo={combo} key={`sc-main-${combo}-${idx}`} />)
                            : "-"}
                        </strong>
                      </div>
                      <div className="list-row">
                        <span className={`ticket-type ${getTicketTypeClass("backup")}`}>押さえ</span>
                        <strong>
                          {Array.isArray(scenarioSuggestions.backup_picks) && scenarioSuggestions.backup_picks.length
                            ? scenarioSuggestions.backup_picks.slice(0, 6).map((combo, idx) => <ComboBadge combo={combo} key={`sc-back-${combo}-${idx}`} />)
                            : "-"}
                        </strong>
                      </div>
                      <div className="list-row">
                        <span className={`ticket-type ${getTicketTypeClass("longshot")}`}>穴</span>
                        <strong>
                          {Array.isArray(scenarioSuggestions.longshot_picks) && scenarioSuggestions.longshot_picks.length
                            ? scenarioSuggestions.longshot_picks.slice(0, 6).map((combo, idx) => <ComboBadge combo={combo} key={`sc-long-${combo}-${idx}`} />)
                            : "-"}
                        </strong>
                      </div>
                    </div>
                    <p className="muted strategy-line">{scenarioSuggestions.summary || "-"}</p>
                  </article>
                  )}
                </section>
                  </div>
                </details>
              </>
            )}
          </>
        )}

        {screen === "rankings" && (
          <>
            {rankingsError && <div className="error-banner">{rankingsError}</div>}
            <section className="card">
              <div className="section-head recommend-head">
                <h2>AIレースランキング</h2>
                <div className="row-actions">
                  <span className="muted">{date} / hit_rate モード</span>
                  <button className="fetch-btn secondary" onClick={loadRankings} disabled={rankingsLoading}>
                    {rankingsLoading ? "更新中..." : "再取得"}
                  </button>
                </div>
              </div>
              {rankingsLoading ? (
                <p className="muted">ランキングを読み込み中...</p>
              ) : rankingsData.length === 0 ? (
                <p className="muted">ランキング対象レースがありません。</p>
              ) : (
                <div className="recommendation-list">
                  {rankingsData.map((row) => (
                    <article className="recommend-card" key={`rk-${row.rank}-${row.venueId}-${row.raceNo}`}>
                      <div className="recommend-card-head">
                        <strong>#{row.rank} {row.venueId} {row.venueName || "-"} {row.raceNo}R</strong>
                        <div className="row-actions">
                          {row.provisional ? <span className="status-pill status-unsettled">{row.provisional_label || "暫定"}</span> : null}
                          {row.entry_changed ? <span className="status-pill risk-small">Entry changed</span> : null}
                          <span className={`status-pill ${getRiskClass(row.decision_mode)}`}>{row.decision_mode || "-"}</span>
                        </div>
                      </div>
                      <div className="kv-list">
                        <div className="kv-row"><span>ranking_score</span><strong>{formatMaybeNumber(row.ranking_score, 2)}</strong></div>
                        <div className="kv-row"><span>confidence</span><strong>{formatMaybeNumber(row.confidence, 2)}</strong></div>
                        <div className="kv-row"><span>main_head</span><strong><LanePills lanes={[Number(row.main_head)]} /></strong></div>
                        <div className="kv-row"><span>ticket_quality</span><strong>{formatMaybeNumber(row.ticket_quality, 2)}</strong></div>
                        <div className="kv-row"><span>trap_score</span><strong>{formatMaybeNumber(row.trap_score, 2)}</strong></div>
                        <div className="kv-row"><span>value_balance_score</span><strong>{formatMaybeNumber(row.value_balance_score, 2)}</strong></div>
                        {row.entry_changed ? (
                          <div className="kv-row"><span>進入変化</span><strong>{row.entry_change_type || "あり"}</strong></div>
                        ) : null}
                      </div>
                      <p className="muted strategy-line">{row.summary || "-"}</p>
                      <div className="row-actions">
                        <button className="fetch-btn" onClick={() => onOpenRecommendation(row)}>
                          詳細予想へ
                        </button>
                      </div>
                    </article>
                  ))}
                </div>
              )}
            </section>
          </>
        )}

        {screen === "results" && (
          <>
            {perfError && <div className="error-banner">{perfError}</div>}
            {verificationNotice && <div className="notice-banner">{verificationNotice}</div>}
            {learningRunNotice && <div className="notice-banner">{learningRunNotice}</div>}

            <section className="card">
              <h2>検証進捗（AI改善ワークフロー）</h2>
              <p className="muted strategy-line">
                検証済み: 検証ログ保存済み件数 / Learning-ready: ミスマッチ分類付き件数 / 未学習: まだ学習バッチに反映されていない件数
              </p>
              <div className="row-actions" style={{ marginTop: 8 }}>
                <button className="fetch-btn secondary" onClick={onRunLearningNow} disabled={learningRunLoading}>
                  {learningRunLoading ? "学習実行中..." : "今すぐ学習実行"}
                </button>
              </div>
              <div className="stats-grid">
                <article className="card stat-card">
                  <span>総レース記録</span>
                  <strong>{verificationSummary.total}</strong>
                </article>
                <article className="card stat-card">
                  <span>検証済み</span>
                  <strong>{verificationSummary.verified}</strong>
                </article>
                <article className="card stat-card">
                  <span>未検証</span>
                  <strong>{verificationSummary.unverified}</strong>
                </article>
                {adminMode ? (
                  <article className="card stat-card">
                    <span>無効化</span>
                    <strong>{verificationSummary.hidden}</strong>
                  </article>
                ) : null}
                <article className="card stat-card">
                  <span>検証率</span>
                  <strong>{formatMaybeNumber(verificationSummary.verificationRate, 2)}%</strong>
                </article>
                <article className="card stat-card">
                  <span>Learning-ready</span>
                  <strong>{verificationSummary.learningReady}</strong>
                  <small>ミスマッチ分類保存済み</small>
                </article>
                <article className="card stat-card">
                  <span>最新検証</span>
                  <strong>
                    {verificationSummary.latestVerifiedAt
                      ? new Date(verificationSummary.latestVerifiedAt).toLocaleString()
                      : "-"}
                  </strong>
                  <small>学習バッチ入力対象</small>
                </article>
                <article className="card stat-card">
                  <span>最新学習実行</span>
                  <strong>
                    {learningLatest?.continuous_learning?.last_learning_run_at
                      ? new Date(learningLatest.continuous_learning.last_learning_run_at).toLocaleString()
                      : "-"}
                  </strong>
                  <small>run_id: {learningLatest?.continuous_learning?.last_learning_run_id ?? "-"}</small>
                  <small>mode: {learningLatest?.continuous_learning?.last_learning_trigger_mode || "unknown"}</small>
                  {learningLatest?.continuous_learning?.learning_job_running ? (
                    <small>job: running</small>
                  ) : learningLatest?.continuous_learning?.queued_auto_learning ? (
                    <small>job: queued</small>
                  ) : null}
                  {learningLatest?.auto_trigger?.reason ? (
                    <small>auto: {getLearningAutoReasonLabel(learningLatest.auto_trigger.reason)}</small>
                  ) : null}
                  <small>segments: {learningLatest?.segment_learning?.learned_segment_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>前回学習で使用した検証件数</span>
                  <strong>{learningLatest?.continuous_learning?.last_verified_records_used ?? 0}</strong>
                  <small>未学習(learning-ready): {learningLatest?.continuous_learning?.learning_ready_pending ?? 0}</small>
                </article>
              </div>
            </section>
            {!resultsVerificationOnly && (
              <>

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

            {showAdminResultTool && (
              <section className="card">
                <div className="result-form-grid">
                  <h2>{adminMode ? "管理者用 手動結果入力（予備）" : "結果編集"}</h2>
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
              <h2>分析ダッシュボード</h2>
              <div className="stats-grid">
                <article className="card stat-card">
                  <span>総購入</span>
                  <strong>JPY {(analytics?.total?.total_bets ?? 0).toLocaleString()}</strong>
                  <small>払戻 JPY {(analytics?.total?.total_payout ?? 0).toLocaleString()}</small>
                  <small className={getProfitClass(analytics?.total?.total_profit_loss)}>
                    P/L JPY {(analytics?.total?.total_profit_loss ?? 0).toLocaleString()}
                  </small>
                </article>
                <article className="card stat-card">
                  <span>総的中率</span>
                  <strong>{formatMaybeNumber(analytics?.total?.hit_rate, 2)}%</strong>
                  <small>回収率 {formatMaybeNumber(analytics?.total?.recovery_rate, 2)}%</small>
                  <small>精算件数 {analytics?.total?.settled_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>頭予想成功率</span>
                  <strong>{formatMaybeNumber(analytics?.head_prediction?.success_rate, 2)}%</strong>
                  <small>件数 {analytics?.head_prediction?.race_count ?? 0}</small>
                  <small>的中 {analytics?.head_prediction?.hit_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>推奨買い目(固定100)</span>
                  <strong>{formatMaybeNumber(analytics?.recommendation_only_performance?.hit_rate, 2)}%</strong>
                  <small>回収率 {formatMaybeNumber(analytics?.recommendation_only_performance?.recovery_rate, 2)}%</small>
                  <small className={getProfitClass(analytics?.recommendation_only_performance?.total_profit_loss)}>
                    P/L JPY {(analytics?.recommendation_only_performance?.total_profit_loss ?? 0).toLocaleString()}
                  </small>
                </article>
                <article className="card stat-card">
                  <span>資金配分適用</span>
                  <strong>{formatMaybeNumber(analytics?.stake_allocation_performance?.hit_rate, 2)}%</strong>
                  <small>回収率 {formatMaybeNumber(analytics?.stake_allocation_performance?.recovery_rate, 2)}%</small>
                  <small className={getProfitClass(analytics?.stake_allocation_performance?.total_profit_loss)}>
                    P/L JPY {(analytics?.stake_allocation_performance?.total_profit_loss ?? 0).toLocaleString()}
                  </small>
                </article>
              </div>
            </section>

            <section className="card">
              <h2>AI / 手動ベット比較</h2>
              <div className="stats-grid">
                {[
                  { key: "ai_bets", label: "AIベット" },
                  { key: "manual_bets", label: "手動ベット" },
                  { key: "copied_manual_bets", label: "AIコピー手動" },
                  { key: "pure_manual_bets", label: "純手動" }
                ].map((x) => {
                  const s = analytics?.bet_source_comparison?.[x.key] || {};
                  return (
                    <article key={x.key} className="card stat-card">
                      <span>{x.label}</span>
                      <strong>{s.number_of_bets ?? 0}件</strong>
                      <small>購入 JPY {(s.total_stake ?? 0).toLocaleString()}</small>
                      <small>払戻 JPY {(s.total_payout ?? 0).toLocaleString()}</small>
                      <small className={getProfitClass(s.total_profit_loss)}>
                        P/L JPY {(s.total_profit_loss ?? 0).toLocaleString()}
                      </small>
                      <small>的中率 {formatMaybeNumber(s.hit_rate, 2)}%</small>
                      <small>ROI {formatMaybeNumber(s.roi, 2)}%</small>
                    </article>
                  );
                })}
              </div>
            </section>

            <section className="card">
              <h2>日次 / 月次 / 年次サマリー</h2>
              <div className="stats-grid">
                <article className="card stat-card">
                  <span>今日</span>
                  <strong>Bet JPY {(analytics?.periods?.today?.total_bet_amount ?? 0).toLocaleString()}</strong>
                  <small>Hit {formatMaybeNumber(analytics?.periods?.today?.hit_rate, 2)}%</small>
                  <small>Recovery {formatMaybeNumber(analytics?.periods?.today?.recovery_rate, 2)}%</small>
                  <small className={getProfitClass(analytics?.periods?.today?.total_profit_loss)}>
                    P/L JPY {(analytics?.periods?.today?.total_profit_loss ?? 0).toLocaleString()}
                  </small>
                </article>
                <article className="card stat-card">
                  <span>今月</span>
                  <strong>Bet JPY {(analytics?.periods?.month?.total_bet_amount ?? 0).toLocaleString()}</strong>
                  <small>Hit {formatMaybeNumber(analytics?.periods?.month?.hit_rate, 2)}%</small>
                  <small>Recovery {formatMaybeNumber(analytics?.periods?.month?.recovery_rate, 2)}%</small>
                  <small className={getProfitClass(analytics?.periods?.month?.total_profit_loss)}>
                    P/L JPY {(analytics?.periods?.month?.total_profit_loss ?? 0).toLocaleString()}
                  </small>
                </article>
                <article className="card stat-card">
                  <span>今年</span>
                  <strong>Bet JPY {(analytics?.periods?.year?.total_bet_amount ?? 0).toLocaleString()}</strong>
                  <small>Hit {formatMaybeNumber(analytics?.periods?.year?.hit_rate, 2)}%</small>
                  <small>Recovery {formatMaybeNumber(analytics?.periods?.year?.recovery_rate, 2)}%</small>
                  <small className={getProfitClass(analytics?.periods?.year?.total_profit_loss)}>
                    P/L JPY {(analytics?.periods?.year?.total_profit_loss ?? 0).toLocaleString()}
                  </small>
                </article>
              </div>
            </section>

            <section className="card">
              <h2>場別パフォーマンス</h2>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>場</th>
                      <th>購入</th>
                      <th>払戻</th>
                      <th>損益</th>
                      <th>的中率</th>
                      <th>回収率</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(analytics?.venue_performance || []).map((v) => (
                      <tr key={`venue-${v.venue_id}`}>
                        <td>{v.venue_id} {v.venue_name || "-"}</td>
                        <td>JPY {(v.total_bets ?? 0).toLocaleString()}</td>
                        <td>JPY {(v.total_payout ?? 0).toLocaleString()}</td>
                        <td className={getProfitClass(v.total_profit_loss)}>JPY {(v.total_profit_loss ?? 0).toLocaleString()}</td>
                        <td>{formatMaybeNumber(v.hit_rate, 2)}%</td>
                        <td>{formatMaybeNumber(v.recovery_rate, 2)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="card">
              <h2>判定モード別パフォーマンス</h2>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Mode</th>
                      <th>購入</th>
                      <th>払戻</th>
                      <th>損益</th>
                      <th>的中率</th>
                      <th>回収率</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(analytics?.mode_performance || []).map((m) => (
                      <tr key={`mode-${m.mode}`}>
                        <td>{m.mode}</td>
                        <td>JPY {(m.total_bets ?? 0).toLocaleString()}</td>
                        <td>JPY {(m.total_payout ?? 0).toLocaleString()}</td>
                        <td className={getProfitClass(m.total_profit_loss)}>JPY {(m.total_profit_loss ?? 0).toLocaleString()}</td>
                        <td>{formatMaybeNumber(m.hit_rate, 2)}%</td>
                        <td>{formatMaybeNumber(m.recovery_rate, 2)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="card">
              <h2>AI弱点分析</h2>
              <div className="stats-grid">
                <article className="card stat-card">
                  <span>分析対象レース</span>
                  <strong>{analytics?.weakness_analysis?.weakness_summary?.analyzed_races ?? 0}</strong>
                  <small>失敗含む {analytics?.weakness_analysis?.weakness_summary?.races_with_failures ?? 0}</small>
                  <small>失敗率 {formatMaybeNumber(analytics?.weakness_analysis?.weakness_summary?.failure_rate, 2)}%</small>
                </article>
                <article className="card stat-card">
                  <span>頭精度</span>
                  <strong>{formatMaybeNumber(analytics?.weakness_analysis?.head_accuracy_stats?.rate, 2)}%</strong>
                  <small>的中 {analytics?.weakness_analysis?.head_accuracy_stats?.hit ?? 0} / {analytics?.weakness_analysis?.head_accuracy_stats?.total ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>相手精度</span>
                  <strong>{formatMaybeNumber(analytics?.weakness_analysis?.partner_accuracy_stats?.rate, 2)}%</strong>
                  <small>頭的中時の相手一致率</small>
                </article>
                <article className="card stat-card">
                  <span>推奨利用率</span>
                  <strong>{formatMaybeNumber(analytics?.weakness_analysis?.recommendation_usage?.usage_rate, 2)}%</strong>
                  <small>{analytics?.weakness_analysis?.recommendation_usage?.used_races ?? 0} / {analytics?.weakness_analysis?.recommendation_usage?.analyzed_races ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>資金配分利用率</span>
                  <strong>{formatMaybeNumber(analytics?.weakness_analysis?.stake_allocation_usage?.usage_rate, 2)}%</strong>
                  <small>{analytics?.weakness_analysis?.stake_allocation_usage?.used_tickets ?? 0} / {analytics?.weakness_analysis?.stake_allocation_usage?.total_tickets ?? 0}</small>
                </article>
              </div>

              <div className="table-wrap" style={{ marginTop: 10 }}>
                <table>
                  <thead>
                    <tr>
                      <th>主要失敗モード</th>
                      <th>件数</th>
                      <th>比率</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(analytics?.weakness_analysis?.top_failure_modes || []).map((w) => (
                      <tr key={`weak-${w.code}`}>
                        <td>{w.code}</td>
                        <td>{w.count}</td>
                        <td>{formatMaybeNumber(w.rate, 2)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="table-wrap" style={{ marginTop: 10 }}>
                <table>
                  <thead>
                    <tr>
                      <th>場</th>
                      <th>レース数</th>
                      <th>失敗率</th>
                      <th>最多失敗モード</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(analytics?.weakness_analysis?.venue_weakness_stats || []).slice(0, 12).map((v) => (
                      <tr key={`vw-${v.venue_id}`}>
                        <td>{v.venue_id} {v.venue_name || "-"}</td>
                        <td>{v.races ?? 0}</td>
                        <td>{formatMaybeNumber(v.failure_rate, 2)}%</td>
                        <td>{v.top_failure_mode || "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="card">
              <h2>Self Learning（提案モード）</h2>
              <div className="stats-grid">
                <article className="card stat-card">
                  <span>状態</span>
                  <strong>{selfLearning?.status || "-"}</strong>
                  <small>mode: {selfLearning?.mode || "proposal_only"}</small>
                </article>
                <article className="card stat-card">
                  <span>サンプル数</span>
                  <strong>{selfLearning?.sample_size ?? 0}</strong>
                  <small>推奨強化は十分サンプル時のみ</small>
                </article>
                <article className="card stat-card">
                  <span>頭精度</span>
                  <strong>{formatMaybeNumber(selfLearning?.head_precision_performance?.hit_rate, 2)}%</strong>
                  <small>
                    {selfLearning?.head_precision_performance?.hit_count ?? 0} /
                    {" "}
                    {selfLearning?.head_precision_performance?.sample ?? 0}
                  </small>
                </article>
                <article className="card stat-card">
                  <span>展示AI高信号時頭一致</span>
                  <strong>{formatMaybeNumber(selfLearning?.exhibition_ai_performance?.high_signal_head_hit_rate, 2)}%</strong>
                  <small>
                    {selfLearning?.exhibition_ai_performance?.high_signal_head_hit ?? 0} /
                    {" "}
                    {selfLearning?.exhibition_ai_performance?.high_signal_sample ?? 0}
                  </small>
                </article>
              </div>
              <p className="muted strategy-line" style={{ marginTop: 8 }}>
                {selfLearning?.summary || "-"}
              </p>

              <div className="table-wrap" style={{ marginTop: 10 }}>
                <table>
                  <thead>
                    <tr>
                      <th>Weight</th>
                      <th>Current</th>
                      <th>Suggested</th>
                      <th>Delta</th>
                      <th>Confidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(selfLearning?.recommendations || {}).map(([key, row]) => (
                      <tr key={`slr-${key}`}>
                        <td>{key}</td>
                        <td>{formatMaybeNumber(row?.current, 4)}</td>
                        <td>{formatMaybeNumber(row?.suggested, 4)}</td>
                        <td className={getProfitClass(Number(row?.delta || 0))}>{formatMaybeNumber(row?.delta, 4)}</td>
                        <td>{row?.confidence || "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="table-wrap" style={{ marginTop: 10 }}>
                <table>
                  <thead>
                    <tr>
                      <th>Snapshot</th>
                      <th>Sample</th>
                      <th>Mode</th>
                      <th>Summary</th>
                    </tr>
                  </thead>
                  <tbody>
                    {learningSnapshots.slice(0, 10).map((snap) => (
                      <tr key={`sls-${snap.id}`}>
                        <td>{snap.date} {snap.created_at ? `(${new Date(snap.created_at).toLocaleString()})` : ""}</td>
                        <td>{snap.sample_size ?? 0}</td>
                        <td>{snap.mode || "-"}</td>
                        <td>{snap.summary || "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
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
              <h2>スタート展示 / 進入変化集計</h2>
              <div className="stats-grid">
                <article className="card stat-card">
                  <span>分析レース数</span>
                  <strong>{startEntryAnalysis?.totals?.analyzed_races ?? 0}</strong>
                  <small>entry_changed {startEntryAnalysis?.totals?.entry_changed_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>最速ST艇の1着率</span>
                  <strong>{formatMaybeNumber(startEntryAnalysis?.fastest_st_boat_win_rate?.win_rate, 2)}%</strong>
                  <small>
                    {startEntryAnalysis?.fastest_st_boat_win_rate?.wins ?? 0}
                    {" / "}
                    {startEntryAnalysis?.fastest_st_boat_win_rate?.races ?? 0}
                  </small>
                </article>
                <article className="card stat-card">
                  <span>進入変化レース最多決着</span>
                  <strong>{startEntryAnalysis?.entry_changed_summary?.most_common_finishing_order || "-"}</strong>
                  <small>件数 {startEntryAnalysis?.entry_changed_summary?.most_common_finishing_order_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>AI的中率（進入変化あり）</span>
                  <strong>{formatMaybeNumber(startEntryAnalysis?.ai_hit_rate_comparison?.entry_changed_true?.hit_rate, 2)}%</strong>
                  <small>
                    {startEntryAnalysis?.ai_hit_rate_comparison?.entry_changed_true?.hits ?? 0}
                    {" / "}
                    {startEntryAnalysis?.ai_hit_rate_comparison?.entry_changed_true?.races ?? 0}
                  </small>
                </article>
                <article className="card stat-card">
                  <span>AI的中率（進入変化なし）</span>
                  <strong>{formatMaybeNumber(startEntryAnalysis?.ai_hit_rate_comparison?.entry_changed_false?.hit_rate, 2)}%</strong>
                  <small>
                    {startEntryAnalysis?.ai_hit_rate_comparison?.entry_changed_false?.hits ?? 0}
                    {" / "}
                    {startEntryAnalysis?.ai_hit_rate_comparison?.entry_changed_false?.races ?? 0}
                  </small>
                </article>
              </div>

              <div className="table-wrap" style={{ marginTop: 10 }}>
                <table>
                  <thead>
                    <tr>
                      <th>start_display_signature</th>
                      <th>レース数</th>
                      <th>最多決着</th>
                      <th>AI的中率</th>
                      <th>標本数</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(startEntryAnalysis?.by_signature || []).map((row) => (
                      <tr key={`sig-${row.start_display_signature}`}>
                        <td>{row.start_display_signature || "-"}</td>
                        <td>{row.race_count ?? 0}</td>
                        <td>{row.most_common_finishing_order || "-"}</td>
                        <td>{formatMaybeNumber(row.ai_hit_rate, 2)}%</td>
                        <td>{row.ai_hit_sample ?? 0}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
              </>
            )}

            <section className="card">
              <h2>AI予想検証（レース別）</h2>
              <div className="results-toolbar sticky-toolbar" style={{ marginBottom: 10 }}>
                <label>
                  <span>表示</span>
                  <select value={resultsStatusFilter} onChange={(e) => setResultsStatusFilter(e.target.value)}>
                    <option value="all">all</option>
                    <option value="unverified">unverified</option>
                    <option value="verified">verified</option>
                    <option value="failed">failed</option>
                    <option value="missing">missing data</option>
                    {adminMode ? <option value="hidden">hidden / invalidated</option> : null}
                  </select>
                </label>
                <label>
                  <span>会場</span>
                  <select value={resultsVenueFilter} onChange={(e) => setResultsVenueFilter(e.target.value)}>
                    <option value="all">all</option>
                    {resultVenueOptions.map((venue) => (
                      <option key={`venue-filter-${venue}`} value={venue}>{venue}</option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>参加判定</span>
                  <select value={resultsParticipationFilter} onChange={(e) => setResultsParticipationFilter(e.target.value)}>
                    <option value="all">all</option>
                    <option value="recommended">participate</option>
                    <option value="watch">watch</option>
                    <option value="not_recommended">skip</option>
                  </select>
                </label>
                <button
                  type="button"
                  className="fetch-btn"
                  onClick={onVerifyAllUnverified}
                  disabled={bulkVerifyRunning || !!verifyingRaceId || bulkVerifiableHistory.length === 0}
                >
                  {bulkVerifyRunning ? "一括検証中..." : `未検証を一括検証 (${bulkVerifiableHistory.length})`}
                </button>
              </div>
              {bulkVerifySummary ? (
                <p className="muted" style={{ marginBottom: 10 }}>
                  bulk verify: {bulkVerifySummary.attempted}/{bulkVerifySummary.total}
                  {" "}attempted / {bulkVerifySummary.verified} verified / {bulkVerifySummary.skipped} skipped / {bulkVerifySummary.failed} failed
                </p>
              ) : null}
              <div className="result-miss-summary">
                <div className="result-miss-summary-item">
                  <span>head misses</span>
                  <strong>{resultsMissPatternSummary.headMisses}</strong>
                </div>
                <div className="result-miss-summary-item">
                  <span>2nd misses</span>
                  <strong>{resultsMissPatternSummary.secondPlaceMisses}</strong>
                </div>
                <div className="result-miss-summary-item">
                  <span>3rd misses</span>
                  <strong>{resultsMissPatternSummary.thirdPlaceMisses}</strong>
                </div>
                <div className="result-miss-summary-item">
                  <span>swaps</span>
                  <strong>{resultsMissPatternSummary.swaps}</strong>
                </div>
                <div className="result-miss-summary-item">
                  <span>exacta hit / miss</span>
                  <strong>{resultsMissPatternSummary.exactaHits} / {resultsMissPatternSummary.exactaMisses}</strong>
                </div>
                <div className="result-miss-summary-item">
                  <span>near misses</span>
                  <strong>{resultsMissPatternSummary.nearMisses}</strong>
                </div>
              </div>
              {filteredHistory.length === 0 ? <p className="muted">履歴データはまだありません。</p> : (
                <div className="history-stack">
                  {filteredHistory.map((h) => {
                    const savedFinalRecommendedBets = getSavedFinalRecommendedBets(h);
                    const savedBoat1HeadBets = getSavedBoat1HeadBets(h);
                    const savedExactaBets = getSavedExactaBets(h);
                    const participationMeta = getParticipationDecisionMeta(h);
                    const betSnapshotLabel = getResultsBetSnapshotLabel(h);
                    const verificationKey = getVerificationHistoryKey(h.race_id, h.prediction_snapshot_id);
                    const isEditingResult = editingResultKey === verificationKey;
                    const verificationSummaryData = h?.verification?.summary || {};
                    const currentVerifyStatus =
                      verificationRunStatusByRace[verificationKey] || h.verification_status || "PENDING_RESULT";
                    const verifyReason =
                      verificationReasonByRace[verificationKey] ||
                      h.verification_reason ||
                      h?.verification?.summary?.warning ||
                      "";
                    const compactMissTags = getResultMissPatternTags(h);
                    return (
                    <div key={h.history_id || `${h.race_id}-${h.prediction_snapshot_id || h.snapshot_created_at || ""}`} className="history-item compact-history">
                      <div className="history-head">
                        <div className="history-title-block">
                          <strong>{formatHistoryRaceTitle(h)}</strong>
                          <small>{h.race_date || "-"}</small>
                        </div>
                        <div className="row-actions">
                          <span className={participationMeta.className}>{participationMeta.label}</span>
                          <span className={h.hit_miss === "HIT" ? "badge hit" : h.hit_miss === "MISS" ? "badge miss" : "badge pending"}>{h.hit_miss}</span>
                          <span className={getVerifyStatusBadgeClass(currentVerifyStatus)}>
                            {getVerifyStatusLabel(currentVerifyStatus)}
                          </span>
                          <button
                            type="button"
                            className="fetch-btn secondary"
                            onClick={() => onVerifyRace(h.race_id, h.prediction_snapshot_id)}
                            disabled={verifyingRaceId === verificationKey}
                          >
                            {verifyingRaceId === verificationKey ? "検証中..." : "検証"}
                          </button>
                          {h?.verification?.id ? (
                            h?.invalidation ? (
                              <button
                                type="button"
                                className="fetch-btn secondary"
                                onClick={() => onRestoreVerification(h)}
                                disabled={restoringRaceId === verificationKey}
                              >
                                {restoringRaceId === verificationKey ? "復元中..." : "復元"}
                              </button>
                            ) : (
                              <button
                                type="button"
                                className="fetch-btn secondary"
                                onClick={() => onInvalidateVerification(h)}
                                disabled={invalidatingRaceId === verificationKey}
                              >
                                {invalidatingRaceId === verificationKey ? "無効化中..." : "無効化"}
                              </button>
                            )
                          ) : null}
                          <button
                            type="button"
                            className="fetch-btn secondary"
                            onClick={() => onEditResultRecord(h)}
                            disabled={editingResultSaveKey === verificationKey}
                          >
                            結果編集
                          </button>
                          {h?.verification?.id ? (
                            <button
                              type="button"
                              className="fetch-btn secondary"
                              onClick={() => onEditVerificationNote(h)}
                            >
                              メモ
                            </button>
                          ) : null}
                        </div>
                      </div>
                      {!String(currentVerifyStatus || "").startsWith("VERIFIED") && verifyReason ? (
                        <p className="muted strategy-line" style={{ marginTop: 6 }}>{verifyReason}</p>
                      ) : null}
                      {participationMeta.reason ? (
                        <p className="muted strategy-line" style={{ marginTop: 6 }}>
                          participation: {participationMeta.reason}
                        </p>
                      ) : null}
                      {h?.invalidation ? (
                        <p className="muted strategy-line" style={{ marginTop: 6 }}>
                          invalidated: {h.invalidation.invalid_reason || "manual soft invalidation"}
                        </p>
                      ) : null}
                      {isEditingResult ? (
                        <div className="card" style={{ marginTop: 8, padding: 12 }}>
                          <div className="controls-grid">
                            <label>
                              <span>確定結果</span>
                              <input
                                value={editingResultForm.confirmedResult}
                                onChange={(e) =>
                                  setEditingResultForm((prev) => ({
                                    ...prev,
                                    confirmedResult: e.target.value
                                  }))
                                }
                                placeholder="1-2-3"
                              />
                            </label>
                            <label>
                              <span>検証メモ</span>
                              <input
                                value={editingResultForm.verificationReason}
                                onChange={(e) =>
                                  setEditingResultForm((prev) => ({
                                    ...prev,
                                    verificationReason: e.target.value
                                  }))
                                }
                                placeholder="任意"
                              />
                            </label>
                            <label>
                              <span>再検証理由</span>
                              <input
                                value={editingResultForm.invalidReason}
                                onChange={(e) =>
                                  setEditingResultForm((prev) => ({
                                    ...prev,
                                    invalidReason: e.target.value
                                  }))
                                }
                                placeholder="任意"
                              />
                            </label>
                          </div>
                          {editingResultError ? (
                            <p className="error-banner" style={{ marginTop: 8 }}>{editingResultError}</p>
                          ) : null}
                          {editingResultNotice ? (
                            <p className="notice-banner" style={{ marginTop: 8 }}>{editingResultNotice}</p>
                          ) : null}
                          <div className="row-actions" style={{ marginTop: 8 }}>
                            <button
                              type="button"
                              className="fetch-btn"
                              onClick={onSaveEditedResultRecord}
                              disabled={editingResultSaveKey === verificationKey}
                            >
                              {editingResultSaveKey === verificationKey ? "保存中..." : "保存"}
                            </button>
                            <button
                              type="button"
                              className="fetch-btn secondary"
                              onClick={onCancelEditResultRecord}
                              disabled={editingResultSaveKey === verificationKey}
                            >
                              キャンセル
                            </button>
                          </div>
                        </div>
                      ) : null}
                      <div className="history-summary-grid">
                        <div className="history-summary-cell">
                          <span className="history-label">saved bets</span>
                          <div className="history-bet-strip">
                            {savedFinalRecommendedBets.length ? (
                              savedFinalRecommendedBets.map((b, idx) => (
                                <ComboBadge combo={b?.combo} key={`rec-${h.race_id}-${idx}`} />
                              ))
                            ) : betSnapshotLabel}
                          </div>
                        </div>
                        {savedBoat1HeadBets.length > 0 ? (
                          <div className="history-summary-cell">
                            <span className="history-label">1-head bets</span>
                            <div className="history-bet-strip">
                              {savedBoat1HeadBets.map((b, idx) => (
                                <ComboBadge combo={b?.combo} key={`boat1-${h.race_id}-${idx}`} />
                              ))}
                            </div>
                          </div>
                        ) : null}
                        {savedExactaBets.length > 0 ? (
                          <div className="history-summary-cell">
                            <span className="history-label">exacta</span>
                            <div className="history-bet-strip">
                              {savedExactaBets.map((b, idx) => (
                                <ComboBadge combo={b?.combo} key={`exacta-${h.race_id}-${idx}`} />
                              ))}
                            </div>
                          </div>
                        ) : null}
                        <div className="history-summary-cell">
                          <span className="history-label">confirmed result</span>
                          <strong>{h.confirmed_result || "-"}</strong>
                        </div>
                        <div className="history-summary-cell">
                          <span className="history-label">verification</span>
                          <strong>{h.verification?.hit_miss || h.hit_miss || "-"}</strong>
                        </div>
                        <div className="history-summary-cell">
                          <span className="history-label">participation</span>
                          <strong>{participationMeta.label}</strong>
                        </div>
                        <div className="history-summary-cell">
                          <span className="history-label">head / bet</span>
                          <strong>
                            {verificationSummaryData?.head_correct === true ? "YES" : verificationSummaryData?.head_correct === false ? "NO" : "-"}
                            {" / "}
                            {verificationSummaryData?.hit_miss === "HIT" ? "YES" : verificationSummaryData?.hit_miss === "MISS" ? "NO" : "-"}
                          </strong>
                        </div>
                        {savedExactaBets.length > 0 ? (
                          <div className="history-summary-cell">
                            <span className="history-label">exacta result</span>
                            <strong>{h.exacta_verification_status || "-"}</strong>
                          </div>
                        ) : null}
                      </div>
                      {compactMissTags.length ? (
                        <div className="chips-wrap">
                          {compactMissTags.map((tag) => (
                            <span className="chip" key={`miss-pattern-${h.race_id}-${tag}`}>{tag}</span>
                          ))}
                        </div>
                      ) : null}
                      {Array.isArray(h?.verification?.mismatch_categories) && h.verification.mismatch_categories.length ? (
                        <div className="chips-wrap">
                          {h.verification.mismatch_categories.map((tag) => (
                            <span className="chip" key={`mismatch-${h.race_id}-${tag}`}>{tag}</span>
                          ))}
                        </div>
                      ) : null}
                      <details className="result-detail-block" style={{ marginTop: 8 }}>
                        <summary>details</summary>
                        <div style={{ marginTop: 8 }}>
                          <div className="history-grid" style={{ marginTop: 8 }}>
                            <div>predicted top: {Array.isArray(h.predicted_top3) && h.predicted_top3.length ? h.predicted_top3.join("-") : "-"}</div>
                            <div>confirmed result: {h.confirmed_result || "-"}</div>
                            <div>
                              place match:
                              {" "}
                              {verificationSummaryData?.head_correct === true ? "1st HIT" : verificationSummaryData?.head_correct === false ? "1st MISS" : "-"}
                              {" / "}
                              {verificationSummaryData?.second_place_correct === true ? "2nd HIT" : verificationSummaryData?.second_place_correct === false ? "2nd MISS" : "-"}
                              {" / "}
                              {verificationSummaryData?.third_place_correct === true ? "3rd HIT" : verificationSummaryData?.third_place_correct === false ? "3rd MISS" : "-"}
                            </div>
                            <div>near structure: {verificationSummaryData?.structure_near_but_order_miss ? "YES" : "NO"}</div>
                            <div>partner miss: {verificationSummaryData?.partner_selection_miss ? "YES" : "NO"}</div>
                            <div>3rd noise: {verificationSummaryData?.third_place_noise ? "YES" : "NO"}</div>
                            <div>exacta recommendation: {savedExactaBets.length ? savedExactaBets.map((b) => b.combo).join(", ") : "-"}</div>
                            <div>exacta result: {h.exacta_verification_status || "-"}</div>
                            <div>
                              reason tags:
                              {" "}
                              {Array.isArray(verificationSummaryData?.learning_adjustment_reason_tags) && verificationSummaryData.learning_adjustment_reason_tags.length
                                ? verificationSummaryData.learning_adjustment_reason_tags.join(", ")
                                : "-"}
                            </div>
                          </div>
                          {savedFinalRecommendedBets.length > 0 ? (
                            <div className="table-wrap" style={{ marginTop: 8 }}>
                              <table>
                                <thead>
                                  <tr>
                                    <th>AI推奨買い目</th>
                                    <th>確率</th>
                                    <th>オッズ</th>
                                    <th>EV</th>
                                    <th>金額</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {savedFinalRecommendedBets.map((bet, idx) => (
                                    <tr key={`final-bet-${h.history_id || h.race_id}-${idx}`}>
                                      <td><ComboBadge combo={bet?.combo} /></td>
                                      <td>{formatMaybeNumber(bet?.prob, 3)}</td>
                                      <td>{formatMaybeNumber(bet?.odds, 1)}</td>
                                      <td>{formatMaybeNumber(bet?.ev, 2)}</td>
                                      <td>JPY {Number(bet?.recommended_bet ?? bet?.bet ?? 0).toLocaleString()}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : null}
                          {savedBoat1HeadBets.length > 0 ? (
                            <div className="table-wrap" style={{ marginTop: 8 }}>
                              <table>
                                <thead>
                                  <tr>
                                    <th>1号艇頭買い目</th>
                                    <th>確率</th>
                                    <th>score</th>
                                    <th>金額</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {savedBoat1HeadBets.map((bet, idx) => (
                                    <tr key={`boat1-bet-${h.history_id || h.race_id}-${idx}`}>
                                      <td><ComboBadge combo={bet?.combo} /></td>
                                      <td>{formatMaybeNumber(bet?.prob, 3)}</td>
                                      <td>{formatMaybeNumber(bet?.boat1_head_score, 1)}</td>
                                      <td>JPY {Number(bet?.recommended_bet ?? bet?.bet ?? 0).toLocaleString()}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : null}
                          {savedExactaBets.length > 0 ? (
                            <div className="table-wrap" style={{ marginTop: 8 }}>
                              <table>
                                <thead>
                                  <tr>
                                    <th>二連単本線</th>
                                    <th>確率</th>
                                    <th>head</th>
                                    <th>partner</th>
                                    <th>金額</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {savedExactaBets.map((bet, idx) => (
                                    <tr key={`exacta-bet-${h.history_id || h.race_id}-${idx}`}>
                                      <td><ComboBadge combo={bet?.combo} /></td>
                                      <td>{formatMaybeNumber(bet?.prob, 3)}</td>
                                      <td>{formatMaybeNumber(bet?.exacta_head_score, 1)}</td>
                                      <td>{formatMaybeNumber(bet?.exacta_partner_score, 1)}</td>
                                      <td>JPY {Number(bet?.recommended_bet ?? bet?.bet ?? 0).toLocaleString()}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : null}
                          <div className="history-start-display">
                            <h3>Start Exhibition</h3>
                            <StartExhibitionDisplay startDisplay={h.startDisplay || null} compact />
                            <div className="history-grid" style={{ marginTop: 6 }}>
                              <div>進入変化: {h.entry_changed ? "あり" : "なし"}</div>
                              <div>進入タイプ: {h.entry_change_type || "-"}</div>
                              <div>予測進入順: {Array.isArray(h.predicted_entry_order) && h.predicted_entry_order.length ? h.predicted_entry_order.join("-") : "-"}</div>
                              <div>実進入順: {Array.isArray(h.actual_entry_order) && h.actual_entry_order.length ? h.actual_entry_order.join("-") : "-"}</div>
                              <div>予想上位: {Array.isArray(h.predicted_top3) && h.predicted_top3.length ? h.predicted_top3.join("-") : "-"}</div>
                              <div>確定結果: {Array.isArray(h.actual_top3) && h.actual_top3.length ? h.actual_top3.join("-") : "-"}</div>
                              <div>snapshot_id: {h.prediction_snapshot_id ?? "-"}</div>
                              <div>snapshot_source: {h.ai_bets_snapshot_source || "-"}</div>
                              <div>snapshot_at: {h.snapshot_created_at ? new Date(h.snapshot_created_at).toLocaleString() : "-"}</div>
                            </div>
                          </div>
                      {h?.debug_bet_compare ? (
                        <details style={{ marginTop: 8 }}>
                          <summary>debug bet compare</summary>
                          <div className="history-grid" style={{ marginTop: 8 }}>
                            <div>confirmed result: {h?.debug_bet_compare?.confirmed_result || h.confirmed_result || "-"}</div>
                            <div>
                              final hit/miss:
                              {" "}
                              {h?.debug_bet_compare?.final_hit_miss_result || h.hit_miss || "-"}
                            </div>
                            <div>
                              displayed bets:
                              {" "}
                              {Array.isArray(h?.debug_bet_compare?.displayed_in_results) && h.debug_bet_compare.displayed_in_results.length
                                ? h.debug_bet_compare.displayed_in_results.map((b, idx) => (
                                  <ComboBadge combo={b?.combo ?? b} key={`debug-display-${h.history_id || h.race_id}-${idx}`} />
                                ))
                                : "-"}
                            </div>
                            <div>
                              saved snapshot:
                              {" "}
                              {Array.isArray(h?.debug_bet_compare?.saved_display_snapshot) && h.debug_bet_compare.saved_display_snapshot.length
                                ? h.debug_bet_compare.saved_display_snapshot.map((b, idx) => (
                                  <ComboBadge combo={b?.combo ?? b} key={`debug-saved-${h.history_id || h.race_id}-${idx}`} />
                                ))
                                : "-"}
                            </div>
                            <div>
                              verification input:
                              {" "}
                              {Array.isArray(h?.debug_bet_compare?.verification_input_bet_list) && h.debug_bet_compare.verification_input_bet_list.length
                                ? h.debug_bet_compare.verification_input_bet_list.map((b, idx) => (
                                  <ComboBadge combo={b?.combo ?? b} key={`debug-verify-${h.history_id || h.race_id}-${idx}`} />
                                ))
                                : "-"}
                            </div>
                            <div>
                              saved exacta:
                              {" "}
                              {Array.isArray(h?.debug_bet_compare?.saved_exacta_snapshot) && h.debug_bet_compare.saved_exacta_snapshot.length
                                ? h.debug_bet_compare.saved_exacta_snapshot.map((b, idx) => (
                                  <ComboBadge combo={b?.combo ?? b} key={`debug-exacta-saved-${h.history_id || h.race_id}-${idx}`} />
                                ))
                                : "-"}
                            </div>
                            <div>
                              exacta input:
                              {" "}
                              {Array.isArray(h?.debug_bet_compare?.verification_input_exacta_bet_list) && h.debug_bet_compare.verification_input_exacta_bet_list.length
                                ? h.debug_bet_compare.verification_input_exacta_bet_list.map((b, idx) => (
                                  <ComboBadge combo={b?.combo ?? b} key={`debug-exacta-verify-${h.history_id || h.race_id}-${idx}`} />
                                ))
                                : "-"}
                            </div>
                            <div>confirmed exacta: {h?.debug_bet_compare?.confirmed_exacta_result || "-"}</div>
                            <div>final exacta: {h?.debug_bet_compare?.final_exacta_result || "-"}</div>
                          </div>
                        </details>
                      ) : null}
                      {h?.feature_snapshot_debug ? (
                        <details style={{ marginTop: 8 }}>
                          <summary>feature snapshot</summary>
                          <div className="history-grid" style={{ marginTop: 8 }}>
                            <div>exists: {h?.feature_snapshot_debug?.feature_snapshot_exists ? "YES" : "NO"}</div>
                            <div>count: {h?.feature_snapshot_debug?.feature_snapshot_count ?? 0}</div>
                            <div>latest id: {h?.feature_snapshot_debug?.latest_feature_snapshot_id ?? "-"}</div>
                            <div>segment corrections used: {h?.feature_snapshot_debug?.segment_corrections_used_count ?? 0}</div>
                            <div>venue correction: {h?.feature_snapshot_debug?.venue_correction_applied ? "YES" : "NO"}</div>
                            <div>venue segment: {h?.feature_snapshot_debug?.venue_segment_key || "-"}</div>
                            <div>venue sample count: {h?.feature_snapshot_debug?.venue_sample_count ?? 0}</div>
                            <div>confidence calibration: {h?.feature_snapshot_debug?.confidence_calibration_applied ? "YES" : "NO"}</div>
                            <div>calibration source: {h?.feature_snapshot_debug?.confidence_calibration_source || "-"}</div>
                            <div>head raw/cal: {Number.isFinite(Number(h?.feature_snapshot_debug?.head_confidence_raw)) || Number.isFinite(Number(h?.feature_snapshot_debug?.head_confidence_calibrated))
                              ? `${formatMaybeNumber(h?.feature_snapshot_debug?.head_confidence_raw, 1)} / ${formatMaybeNumber(h?.feature_snapshot_debug?.head_confidence_calibrated, 1)}`
                              : "-"}</div>
                            <div>bet raw/cal: {Number.isFinite(Number(h?.feature_snapshot_debug?.bet_confidence_raw)) || Number.isFinite(Number(h?.feature_snapshot_debug?.bet_confidence_calibrated))
                              ? `${formatMaybeNumber(h?.feature_snapshot_debug?.bet_confidence_raw, 1)} / ${formatMaybeNumber(h?.feature_snapshot_debug?.bet_confidence_calibrated, 1)}`
                              : "-"}</div>
                            <div>
                              families:
                              {" "}
                              {Array.isArray(h?.feature_snapshot_debug?.saved_feature_families) && h.feature_snapshot_debug.saved_feature_families.length
                                ? h.feature_snapshot_debug.saved_feature_families.join(", ")
                                : "-"}
                            </div>
                            <div>contributions saved: {h?.feature_snapshot_debug?.contribution_data_exists ? "YES" : "NO"}</div>
                          </div>
                        </details>
                      ) : null}
                      {h.verification ? (
                        <div className="history-grid" style={{ marginTop: 8 }}>
                          <div>learning: {getLearningStatusLabel(h)}</div>
                          <div>saved 1-head bets: {savedBoat1HeadBets.length}</div>
                          <div>saved exacta bets: {savedExactaBets.length}</div>
                          <div>検証日時: {h.verification.verified_at ? new Date(h.verification.verified_at).toLocaleString() : "-"}</div>
                          <div>検証結果: {h.verification.hit_miss || "-"}</div>
                          <div>exacta検証: {h.exacta_verification_status || "-"}</div>
                          <div>verification_version: {h?.verification?.summary?.verification_version ?? "-"}</div>
                          <div>verified_snapshot_id: {h?.verification?.summary?.verified_against_snapshot_id ?? "-"}</div>
                          <div>カテゴリ: {Array.isArray(h.verification.mismatch_categories) && h.verification.mismatch_categories.length ? h.verification.mismatch_categories.join(", ") : "-"}</div>
                          <div>検証理由: {h?.verification?.summary?.verification_reason || "-"}</div>
                        </div>
                      ) : null}
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
                      </details>
                    </div>
                    );
                  })}
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
                        const [first, second, third] = combo.split("-").map((v) => Number(v));
                        setBuilderSlots({ first, second, third });
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
                <button className="fetch-btn secondary" onClick={onAddPendingTicket} disabled={betSaving || journalRaceNotRecommended} title={journalRaceNotRecommended ? "Not Recommended race" : ""}>
                  チケット追加
                </button>
              </div>
              {journalRaceNotRecommended ? (
                <p className="muted strategy-line">Not Recommended: このレースは非推奨のためベット追加/保存を制限中です。</p>
              ) : null}

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
                  <button className="fetch-btn" onClick={onSavePendingTickets} disabled={betSaving || journalRaceNotRecommended} title={journalRaceNotRecommended ? "Not Recommended race" : ""}>
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
                        <span className="chip">{ticket.source === "manual" ? "手動" : "AI"}</span>
                        {ticket.source === "manual" && ticket.copied_from_ai ? <span className="chip">AIコピー</span> : null}
                        <span className="chip">{ticket.bet_type || "trifecta"}</span>
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

              <div className="card" style={{ marginTop: 12 }}>
                <h3>手動ベット登録</h3>
                {manualCopiedMeta.copied_from_ai ? (
                  <div className="notice-banner" style={{ marginBottom: 8 }}>
                    AI買い目コピー編集モード
                    {manualCopiedMeta.ai_reference_id ? ` (${manualCopiedMeta.ai_reference_id})` : ""}
                  </div>
                ) : null}
                <div className="controls-grid">
                  <label>
                    <span>ベット種別</span>
                    <select value={manualBetType} onChange={(e) => setManualBetType(e.target.value)}>
                      <option value="trifecta">trifecta</option>
                      <option value="exacta">exacta</option>
                      <option value="trio">trio</option>
                      <option value="quinella">quinella</option>
                      <option value="wide">wide</option>
                      <option value="win">win</option>
                      <option value="place">place</option>
                    </select>
                  </label>
                  <label>
                    <span>購入額</span>
                    <input
                      type="number"
                      min="100"
                      step="100"
                      value={manualStake}
                      onChange={(e) => setManualStake(roundBetTo100(e.target.value))}
                    />
                  </label>
                  <label>
                    <span>メモ</span>
                    <input
                      value={manualNote}
                      onChange={(e) => setManualNote(e.target.value)}
                      placeholder="任意メモ"
                    />
                  </label>
                </div>
                <label style={{ display: "block", marginTop: 8 }}>
                  <span>組番（複数可: 改行/カンマ区切り）</span>
                  <textarea
                    value={manualSelectionsText}
                    onChange={(e) => setManualSelectionsText(e.target.value)}
                    rows={3}
                    placeholder={manualBetType === "win" || manualBetType === "place" ? "1,2,3" : "1-2-3\n2-1-3"}
                  />
                </label>
                <div className="row-actions" style={{ marginTop: 8 }}>
                  {manualCopiedMeta.copied_from_ai ? (
                    <button
                      className="fetch-btn secondary"
                      onClick={() =>
                        setManualCopiedMeta({
                          copied_from_ai: false,
                          ai_reference_id: null
                        })
                      }
                      type="button"
                    >
                      AIコピー解除
                    </button>
                  ) : null}
                  <button
                    className="fetch-btn secondary"
                    onClick={onRegisterManualBets}
                    disabled={betSaving || journalRaceNotRecommended}
                    title={journalRaceNotRecommended ? "Not Recommended race" : ""}
                  >
                    {betSaving ? "登録中..." : "手動ベットを登録"}
                  </button>
                </div>
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
                                <div><span className="label">種別</span><strong>{bet.bet_type || "trifecta"}</strong></div>
                                <div><span className="label">ソース</span><strong>{bet.source === "manual" ? "Manual" : "AI"}</strong></div>
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


