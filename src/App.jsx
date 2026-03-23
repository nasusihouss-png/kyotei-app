import { Component, useEffect, useMemo, useState } from "react";
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

function buildApiError({ message, url, status = null, payload = null, step = null, method = "GET" }) {
  const error = new Error(message || "API request failed");
  error.apiError = {
    url,
    status,
    method,
    step,
    payload
  };
  return error;
}

function getApiErrorDetails(err) {
  const details =
    err?.apiError && typeof err.apiError === "object"
      ? err.apiError
      : {};
  const payload =
    details?.payload && typeof details.payload === "object"
      ? details.payload
      : {};
  return {
    status: Number.isFinite(Number(details?.status)) ? Number(details.status) : null,
    code: payload?.error || payload?.code || null,
    where: payload?.where || details?.step || null,
    route: payload?.route || null,
    message: err?.message || payload?.message || "Search failed",
    url: details?.url || null
  };
}

function getRaceApiErrorLabel(details = {}) {
  const status = Number.isFinite(Number(details?.status)) ? Number(details.status) : null;
  const code = String(details?.code || "").toUpperCase();
  const message = String(details?.message || "").toLowerCase();
  if (status === 504 || code.includes("TIMEOUT") || message.includes("timeout")) return "API timeout";
  if (code === "SNAPSHOT_MISSING" || code === "SNAPSHOT_NOT_FOUND") return "snapshot missing";
  if (code === "BROKEN_PIPELINE") return "broken pipeline";
  if (status && status >= 500) return "backend 500";
  return "api error";
}

function buildRaceApiErrorMessage(details = {}, fallbackMessage = "Failed to fetch race data") {
  const code = String(details?.code || "").toUpperCase();
  if (code === "SNAPSHOT_MISSING" || code === "SNAPSHOT_NOT_FOUND") return "事前データ未生成";
  if (code === "BROKEN_PIPELINE") return "事前特徴量未生成";
  if (Number(details?.status) === 504 || /timeout/i.test(String(details?.message || ""))) return "API timeout";
  return fallbackMessage;
}

function buildSnapshotGenerationHints(date, venueId, raceNo) {
  const base = "cd backend";
  return [
    `${base} && npm run snapshot:generate -- --date ${date} --venueId ${venueId} --raceNo ${raceNo}`,
    `${base} && npm run snapshot:generate -- --date ${date} --venueId ${venueId} --all-races`
  ];
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

async function fetchRaceData(date, venueId, raceNo, options = {}) {
  const url = new URL(`${API_BASE}/race`);
  url.searchParams.set("date", date);
  url.searchParams.set("venueId", String(venueId));
  url.searchParams.set("raceNo", String(raceNo));
  if (options?.forceRefresh) url.searchParams.set("forceRefresh", "1");
  if (options?.screeningMode) url.searchParams.set("screening", String(options.screeningMode));
  if (Number.isFinite(Number(options?.getRaceDataTimeoutMs))) {
    url.searchParams.set("getRaceDataTimeoutMs", String(Number(options.getRaceDataTimeoutMs)));
  }
  if (Number.isFinite(Number(options?.dataFetchTimeoutMs))) {
    url.searchParams.set("dataFetchTimeoutMs", String(Number(options.dataFetchTimeoutMs)));
  }

  const requestUrl = url.toString();
  const controller = new AbortController();
  const timeoutMs = Number(options?.frontendTimeoutMs || (options?.screeningMode === "hard_race" ? 22000 : 18000));
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let response;
  try {
    response = await fetch(requestUrl, { signal: controller.signal });
  } catch (err) {
    throw buildApiError({
      message: err?.name === "AbortError" ? "Race API request timeout" : (err?.message || "Network request failed"),
      url: requestUrl,
      step: "frontend.fetch:/api/race"
    });
  } finally {
    clearTimeout(timer);
  }

  const rawText = await response.text();
  let body = {};
  let parseFailed = false;
  if (rawText) {
    try {
      body = JSON.parse(rawText);
    } catch {
      parseFailed = true;
    }
  }
  console.info("[frontend:/api/race]", {
    url: requestUrl,
    status: response.status,
    ok: response.ok,
    body_preview: rawText.slice(0, 1200)
  });

  if (parseFailed) {
    console.error("[frontend:/api/race][parse_failed]", {
      url: requestUrl,
      status: response.status,
      body_preview: rawText.slice(0, 1200)
    });
    throw buildApiError({
      message: `Race API returned invalid JSON (${response.status})`,
      url: requestUrl,
      status: response.status,
      step: "frontend.parse:/api/race",
      payload: { raw: rawText.slice(0, 1200) }
    });
  }
  if (!response.ok) {
    console.error("[frontend:/api/race][http_error]", {
      url: requestUrl,
      status: response.status,
      body
    });
    throw buildApiError({
      message: body?.message || `Failed to fetch race data (${response.status})`,
      url: requestUrl,
      status: response.status,
      step: body?.where || "backend:/api/race",
      payload: body
    });
  }
  if (!body || typeof body !== "object") {
    console.error("[frontend:/api/race][invalid_body]", {
      url: requestUrl,
      status: response.status,
      body
    });
    throw buildApiError({
      message: "Race API returned an empty response",
      url: requestUrl,
      status: response.status,
      step: "frontend.validate:/api/race",
      payload: body
    });
  }
  return body;
}

async function fetchHardRacePredictionData(date, venueId) {
  const raceNos = Array.from({ length: 12 }, (_, index) => index + 1);
  const concurrency = 2;
  const rows = new Array(raceNos.length);
  let cursor = 0;

  async function fetchOneRace(targetRaceNo) {
    const attempts = [
      {
        screeningMode: "hard_race",
        getRaceDataTimeoutMs: 12000,
        dataFetchTimeoutMs: 7500
      },
      {
        screeningMode: "hard_race",
        getRaceDataTimeoutMs: 14000,
        dataFetchTimeoutMs: 8500,
        forceRefresh: true
      },
      {
        screeningMode: "hard_race",
        getRaceDataTimeoutMs: 17000,
        dataFetchTimeoutMs: 10000,
        forceRefresh: true
      }
    ];

    let lastError = null;
    for (let index = 0; index < attempts.length; index += 1) {
      try {
        const data = await fetchRaceData(date, venueId, targetRaceNo, attempts[index]);
        return {
          raceNo: targetRaceNo,
          ok: true,
          attemptCount: index + 1,
          data
        };
      } catch (error) {
        lastError = error;
        console.warn("[HardRace][fetch_retry]", {
          raceNo: targetRaceNo,
          attempt: index + 1,
          message: error?.message || String(error || "unknown_error"),
          details: getApiErrorDetails(error)
        });
        const message = String(error?.message || "");
        const maybeTimeout =
          /timeout/i.test(message) ||
          /timed out/i.test(message) ||
          /get_race_data_timeout/i.test(String(error?.apiError?.step || ""));
        if (!maybeTimeout || index === attempts.length - 1) break;
      }
    }

    return {
      raceNo: targetRaceNo,
      ok: false,
      attemptCount: attempts.length,
      error: lastError?.message || "Failed to fetch race data",
      errorDetails: getApiErrorDetails(lastError),
      rawResponse: lastError?.apiError?.payload || null
    };
  }

  async function worker() {
    while (cursor < raceNos.length) {
      const currentIndex = cursor;
      cursor += 1;
      rows[currentIndex] = await fetchOneRace(raceNos[currentIndex]);
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, raceNos.length) }, () => worker()));
  return rows;
}

async function fetchStatsData(filters = {}) {
  const url = new URL(`${API_BASE}/stats`);
  Object.entries(filters || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "" || value === "all") return;
    if (Number.isFinite(Number(value)) && Number(value) === 0) return;
    url.searchParams.set(key, String(value));
  });
  const response = await fetch(url.toString());
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

function sortEvaluationSegments(rows = []) {
  return [...(Array.isArray(rows) ? rows : [])].sort((a, b) => {
    const countDiff = Number(b?.verified_race_count ?? 0) - Number(a?.verified_race_count ?? 0);
    if (countDiff !== 0) return countDiff;
    return Number(b?.trifecta_hit_rate ?? 0) - Number(a?.trifecta_hit_rate ?? 0);
  });
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

function formatComparisonValue(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "--";
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  return num.toFixed(digits);
}

function safePrettyJson(value) {
  try {
    return JSON.stringify(value ?? {}, null, 2);
  } catch {
    return "{}";
  }
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function safeObject(value) {
  return value && typeof value === "object" ? value : {};
}

function safeEntries(value) {
  return value && typeof value === "object" ? Object.entries(value) : [];
}

function safeSetHas(setLike, value) {
  return !!setLike && typeof setLike.has === "function" ? setLike.has(value) : false;
}

function getPredictionFieldMetaEntry(source = {}, key) {
  const metaMap =
    source?.predictionFieldMeta && typeof source.predictionFieldMeta === "object"
      ? source.predictionFieldMeta
      : source?.prediction_field_meta && typeof source.prediction_field_meta === "object"
        ? source.prediction_field_meta
        : {};
  return metaMap?.[key] || null;
}

function isPredictionFieldVerified(source = {}, ...keys) {
  return keys.some((key) => {
    const meta = getPredictionFieldMetaEntry(source, key);
    return !!meta?.is_usable && !!meta?.source && meta?.reason !== "unknown_source";
  });
}

function getLaneScoreDisplayValue(row, key) {
  const safeRow = safeObject(row);
  switch (key) {
    case "lane1st":
      if (!isPredictionFieldVerified(safeRow, "lane1stScore", "lane1stAvg")) return null;
      return firstMeaningfulFiniteValue(
        safeRow?.lane1stScore,
        safeRow?.lane1stAvg,
        safeRow?.laneFirstRate
      );
    case "lane2ren":
      if (!isPredictionFieldVerified(safeRow, "lane2renScore", "lane2renAvg")) return null;
      return firstMeaningfulFiniteValue(
        safeRow?.lane2renScore,
        safeRow?.lane2renAvg,
        safeRow?.lane2RenRate
      );
    case "lane3ren":
      if (!isPredictionFieldVerified(safeRow, "lane3renScore", "lane3renAvg")) return null;
      return firstMeaningfulFiniteValue(
        safeRow?.lane3renScore,
        safeRow?.lane3renAvg,
        safeRow?.lane3RenRate
      );
    default:
      return null;
  }
}

class UiErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch() {
    // fail-open on UI rendering errors
  }

  render() {
    if (this.state.hasError) return this.props.fallback ?? null;
    return this.props.children ?? null;
  }
}

function RenderGuard({ children, fallback = null }) {
  return <UiErrorBoundary fallback={fallback}>{children ?? null}</UiErrorBoundary>;
}

function formatDebugRawValue(value) {
  return value === undefined ? "undefined" : JSON.stringify(value);
}

function formatLaneRawDebugValue(entry) {
  if (!entry || typeof entry !== "object") return "--";
  const value = toFiniteComparisonNumber(entry?.value);
  return value === null ? "--" : String(value);
}

function toFiniteComparisonNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function firstFiniteValue(...values) {
  for (const value of values) {
    const normalized = toFiniteComparisonNumber(value);
    if (normalized !== null) return normalized;
  }
  return null;
}

function firstMeaningfulFiniteValue(...values) {
  let zeroCandidate = null;
  for (const value of values) {
    const normalized = toFiniteComparisonNumber(value);
    if (normalized === null) continue;
    if (normalized === 0) {
      if (zeroCandidate === null) zeroCandidate = 0;
      continue;
    }
    return normalized;
  }
  return zeroCandidate;
}

function getLapTimeDisplayValue(source = {}) {
  return firstFiniteValue(
    source?.lapTime,
    source?.lapTimeRaw,
    source?.kyoteiBiyoriLapTimeRaw,
    source?.kyoteiBiyoriLapTime,
    source?.kyoteibiyori_lap_time_raw,
    source?.kyoteibiyori_lap_time,
    source?.feature_snapshot?.lap_time,
    source?.lap_time
  );
}

function normalizeLaneStats(source = {}) {
  return {
    laneFirstRate: isPredictionFieldVerified(source, "lane1stScore", "lane1stAvg")
      ? firstMeaningfulFiniteValue(
      source?.lane1stScore,
      source?.lane1stAvg,
      source?.lane1st_score,
      source?.lane1st_score_after_reassignment,
      source?.lane1st_score_before_reassignment,
      source?.laneFirstRate,
      source?.lane1stDebug?.final_score,
      source?.lane1stRate_avg,
      source?.lane1stRate_weighted,
      source?.lane1stRate,
      source?.lane_first_rate,
      source?.lane_1st_rate
    )
      : null,
    lane2RenRate: isPredictionFieldVerified(source, "lane2renScore", "lane2renAvg")
      ? firstMeaningfulFiniteValue(
      source?.lane2renScore,
      source?.lane2renAvg,
      source?.lane2ren_score,
      source?.lane2ren_score_after_reassignment,
      source?.lane2ren_score_before_reassignment,
      source?.lane2RenRate,
      source?.lane2renDebug?.final_score,
      source?.lane2renRate_avg,
      source?.lane2renRate_weighted,
      source?.lane2renRate,
      source?.lane_2ren_rate
    )
      : null,
    lane3RenRate: isPredictionFieldVerified(source, "lane3renScore", "lane3renAvg")
      ? firstMeaningfulFiniteValue(
      source?.lane3renScore,
      source?.lane3renAvg,
      source?.lane3ren_score,
      source?.lane3ren_score_after_reassignment,
      source?.lane3ren_score_before_reassignment,
      source?.lane3RenRate,
      source?.lane3renDebug?.final_score,
      source?.lane3renRate_avg,
      source?.lane3renRate_weighted,
      source?.lane3renRate,
      source?.lane_3ren_rate
    )
      : null
  };
}

function hasRenderableKyoteiBiyoriData(rows = [], data = {}) {
  const comparisonRows = safeArray(rows);
  if (comparisonRows.some((row) =>
    row?.kyoteiBiyoriFetched ||
    Number.isFinite(Number(row?.lapTime)) ||
    Number.isFinite(Number(getLaneScoreDisplayValue(row, "lane1st"))) ||
    Number.isFinite(Number(getLaneScoreDisplayValue(row, "lane2ren"))) ||
    Number.isFinite(Number(getLaneScoreDisplayValue(row, "lane3ren")))
  )) {
    return true;
  }
  return !!data?.source?.kyotei_biyori?.ok;
}

function buildKyoteiBiyoriFrontendDebug({ data, playerComparisonRows }) {
  const debug =
    data?.kyoteibiyori_debug ||
    data?.source?.kyotei_biyori?.kyoteibiyori_debug ||
    data?.source?.kyotei_biyori?.request_diagnostics ||
    {};
  const fieldDiagnostics =
    debug?.field_diagnostics ||
    data?.source?.kyotei_biyori?.field_diagnostics || {
      populated_fields: [],
      failed_fields: [],
      per_lane: []
    };
  return {
    fetch_success: debug?.fetch_success ?? debug?.kyoteibiyori_fetch_success ?? data?.source?.kyotei_biyori?.ok ?? false,
    extracted_hrefs: debug?.extracted_hrefs || {},
    actual_fetch_paths: Array.isArray(debug?.actual_fetch_paths) ? debug.actual_fetch_paths : [],
    fallback_reason: debug?.fallback_reason || data?.source?.kyotei_biyori?.fallback_reason || null,
    populated_fields: Array.isArray(fieldDiagnostics?.populated_fields) ? fieldDiagnostics.populated_fields : [],
    failed_fields: Array.isArray(fieldDiagnostics?.failed_fields) ? fieldDiagnostics.failed_fields : [],
    backend_fetch_results: debug?.fetch_results || {},
    backend_parse_results: debug?.parse_results || {},
    lane_rows: Array.isArray(debug?.lane_rows)
      ? debug.lane_rows
      : (Array.isArray(data?.racers) ? data.racers : []).map((racer) => ({
          lane: racer?.lane ?? null,
          lane1stRate_raw: racer?.lane1stRate_raw ?? racer?.laneFirstRate ?? null,
          lane2renRate_raw: racer?.lane2renRate_raw ?? racer?.lane2RenRate ?? null,
          lane3renRate_raw: racer?.lane3renRate_raw ?? racer?.lane3RenRate ?? null,
          lapExStretch_raw: racer?.kyoteiBiyoriLapExStretch ?? racer?.lapExStretch ?? racer?.kyoteiBiyoriLapExhibitionScore ?? racer?.lapExhibitionScore ?? null,
          lapTime_raw: racer?.kyoteiBiyoriLapTimeRaw ?? racer?.kyoteiBiyoriLapTime ?? racer?.lapTime ?? null,
          exhibitionST_raw: racer?.kyoteiBiyoriExhibitionSt ?? racer?.exhibitionSt ?? null,
          motor2ren_raw: racer?.motor2ren ?? racer?.kyoteiBiyoriMotor2Rate ?? racer?.motor2Rate ?? null,
          motor3ren_raw: racer?.motor3ren ?? racer?.kyoteiBiyoriMotor3Rate ?? racer?.motor3Rate ?? null,
          lane1stRate_debug: null,
          lane2renRate_debug: null,
          lane3renRate_debug: null,
          lapExStretch_debug: null,
          lapTime_debug: null,
          exhibitionST_debug: null,
          motor2ren_debug: null,
          motor3ren_debug: null
        })),
    lane1stRate_raw: debug?.lane1stRate_raw || {},
    lane2renRate_raw: debug?.lane2renRate_raw || {},
    lane3renRate_raw: debug?.lane3renRate_raw || {},
    lapExStretch_raw: debug?.lapExStretch_raw || {},
    lapTime_raw: debug?.lapTime_raw || {},
    exhibitionST_raw: debug?.exhibitionST_raw || {},
    motor2ren_raw: debug?.motor2ren_raw || {},
    motor3ren_raw: debug?.motor3ren_raw || {},
    lane1stRate_debug: debug?.lane1stRate || {},
    lane2renRate_debug: debug?.lane2renRate || {},
    lane3renRate_debug: debug?.lane3renRate || {},
    lapExStretch_debug: debug?.lapExStretch || {},
    lapTime_debug: debug?.lapTime || {},
    exhibitionST_debug: debug?.exhibitionST || {},
    motor2ren_debug: debug?.motor2ren || {},
    motor3ren_debug: debug?.motor3ren || {},
    rendered_rows: (Array.isArray(playerComparisonRows) ? playerComparisonRows : []).map((row) => ({
      lane: row?.lane ?? null,
      actual_lane: row?.actualLane ?? row?.lane ?? null,
      course_change_occurred: !!row?.courseChanged,
      lane1stRate_raw: row?.lane1stScore ?? row?.lane1stAvg ?? row?.laneFirstRate ?? null,
      lane2renRate_raw: row?.lane2renScore ?? row?.lane2renAvg ?? row?.lane2RenRate ?? null,
      lane3renRate_raw: row?.lane3renScore ?? row?.lane3renAvg ?? row?.lane3RenRate ?? null,
      lapExStretch_raw: row?.lapExStretch ?? row?.lapScore ?? null,
      lapTime_raw: row?.lapTime ?? null,
      lane_scores_before_reassignment: row?.laneScoreDebug?.beforeReassignment || null,
      lane_scores_after_reassignment: row?.laneScoreDebug?.afterReassignment || null,
      motor2ren_raw: row?.motor2ren ?? row?.motor2Rate ?? null,
      motor3ren_raw: row?.motor3ren ?? row?.motor3Rate ?? null,
      lane1stRate: Number.isFinite(Number(getLaneScoreDisplayValue(row, "lane1st"))),
      lane2renRate: Number.isFinite(Number(getLaneScoreDisplayValue(row, "lane2ren"))),
      lane3renRate: Number.isFinite(Number(getLaneScoreDisplayValue(row, "lane3ren"))),
      lapTime: Number.isFinite(Number(row?.lapTime)),
      exhibitionST: Number.isFinite(Number(row?.exhibitionSt)),
      display_lapExStretch: formatComparisonValue(row?.lapExStretch ?? row?.lapScore, 2),
      display_motor2ren: formatComparisonValue(row?.motor2ren ?? row?.motor2Rate, 2),
      display_lane1stRate: formatComparisonValue(getLaneScoreDisplayValue(row, "lane1st"), 2),
      display_lane2renRate: formatComparisonValue(getLaneScoreDisplayValue(row, "lane2ren"), 2),
      display_lane3renRate: formatComparisonValue(getLaneScoreDisplayValue(row, "lane3ren"), 2),
      display_motor3ren: formatComparisonValue(row?.motor3ren ?? row?.motor3Rate, 2)
    }))
  };
}

function formatSignedRateDelta(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (Number.isNaN(num)) return String(value);
  return `${num >= 0 ? "+" : ""}${num.toFixed(2)}pt`;
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

function clampNumber(min, max, value) {
  return Math.max(min, Math.min(max, Number.isFinite(Number(value)) ? Number(value) : min));
}

function getHardRaceOrderRows(source = {}) {
  const enhancement = source?.aiEnhancement && typeof source.aiEnhancement === "object"
    ? source.aiEnhancement
    : {};
  const orderRows = Array.isArray(enhancement?.treeOrderProbabilities)
    ? enhancement.treeOrderProbabilities
    : Array.isArray(enhancement?.stage5_ticketing?.order_probabilities)
      ? enhancement.stage5_ticketing.order_probabilities
      : Array.isArray(source?.roleCandidates?.finish_order_candidates)
        ? source.roleCandidates.finish_order_candidates
        : [];
  return orderRows
    .map((row) => ({
      combo: normalizeCombo(row?.combo),
      probability: Number(row?.probability)
    }))
    .filter((row) => row.combo && Number.isFinite(row.probability) && row.probability > 0);
}

function getHardRaceFinishRoleRows(source = {}, role) {
  const enhancement = source?.aiEnhancement && typeof source.aiEnhancement === "object"
    ? source.aiEnhancement
    : {};
  const aggregated = enhancement?.aggregatedFinishProbabilities && typeof enhancement.aggregatedFinishProbabilities === "object"
    ? enhancement.aggregatedFinishProbabilities
    : enhancement?.stage5_ticketing?.aggregated_finish_probabilities && typeof enhancement.stage5_ticketing.aggregated_finish_probabilities === "object"
      ? enhancement.stage5_ticketing.aggregated_finish_probabilities
      : {};
  const rows = Array.isArray(aggregated?.[role]) ? aggregated[role] : [];
  return rows
    .map((row) => ({
      lane: parseLane(row?.lane),
      weight: Number(row?.weight)
    }))
    .filter((row) => Number.isInteger(row.lane) && Number.isFinite(row.weight) && row.weight > 0);
}

function sumOrderProbability(orderRows, matcher) {
  return Number(
    orderRows.reduce((sum, row) => {
      const combo = normalizeCombo(row?.combo);
      if (!combo) return sum;
      const [first, second, third] = combo.split("-").map((lane) => Number(lane));
      if (![first, second, third].every((lane) => Number.isInteger(lane))) return sum;
      return matcher({ first, second, third, combo, probability: Number(row?.probability) || 0 })
        ? sum + (Number(row?.probability) || 0)
        : sum;
    }, 0).toFixed(4)
  );
}

function sumFinishWeights(rows, lanes) {
  const laneSet = new Set(lanes);
  return Number(
    rows.reduce((sum, row) => (
      laneSet.has(Number(row?.lane)) ? sum + (Number(row?.weight) || 0) : sum
    ), 0).toFixed(4)
  );
}

function toFiniteOrNull(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function getVenueInsideBias(venueName) {
  const name = String(venueName || "");
  if (["Omura", "Tokuyama", "Ashiya", "Suminoe"].includes(name)) return 1;
  if (["Wakamatsu", "Tamagawa"].includes(name)) return 0.7;
  return 0.35;
}

function getBoatClassScore(value) {
  const text = String(value || "").toUpperCase();
  if (text === "A1") return 1;
  if (text === "A2") return 0.8;
  if (text === "B1") return 0.55;
  if (text === "B2") return 0.35;
  return 0.45;
}

function getRacerWinStrength(racer = {}) {
  const national = toFiniteOrNull(racer?.nationwideWinRate);
  const local = toFiniteOrNull(racer?.localWinRate);
  const classScore = getBoatClassScore(racer?.class);
  const nationalScore = national === null ? null : clampNumber(0, 1, (national - 3.8) / 4.2);
  const localScore = local === null ? null : clampNumber(0, 1, (local - 3.8) / 4.2);
  const blended =
    (nationalScore === null ? 0 : nationalScore * 0.55) +
    (localScore === null ? 0 : localScore * 0.45);
  return {
    value: clampNumber(0, 1, blended * 0.8 + classScore * 0.2),
    hasCore: national !== null || local !== null
  };
}

function getRacerMotorStrength(racer = {}) {
  const motor = toFiniteOrNull(racer?.motor2ren ?? racer?.motor2Rate);
  return {
    value: motor === null ? null : clampNumber(0, 1, (motor - 28) / 35),
    raw: motor,
    hasCore: motor !== null
  };
}

function getRacerBoatStrength(racer = {}) {
  const boat2 = toFiniteOrNull(racer?.boat2Rate);
  const boat3 = toFiniteOrNull(racer?.boat3Rate);
  const blended = [boat2, boat3].filter((value) => value !== null);
  const value = blended.length > 0
    ? blended.reduce((sum, entry) => sum + entry, 0) / blended.length
    : null;
  return {
    value: value === null ? null : clampNumber(0, 1, (value - 28) / 36),
    raw: value,
    hasCore: value !== null
  };
}

function getRacerStartStrength(racer = {}) {
  const avgSt = toFiniteOrNull(racer?.avgSt);
  return {
    value: avgSt === null ? null : clampNumber(0, 1, (0.23 - avgSt) / 0.11),
    raw: avgSt,
    hasCore: avgSt !== null
  };
}

function getRacerRiskPenalty(racer = {}) {
  const fCount = toFiniteOrNull(racer?.fHoldCount ?? racer?.fCount);
  return clampNumber(0, 0.35, fCount === null ? 0 : fCount * 0.08);
}

function getUnderlyingBoatFit(racer = {}, lane, profile = {}) {
  const win = getRacerWinStrength(racer);
  const motor = getRacerMotorStrength(racer);
  const boat = getRacerBoatStrength(racer);
  const start = getRacerStartStrength(racer);
  const lane3renSupport = isPredictionFieldVerified(racer, "lane3renScore", "lane3renAvg")
    ? clampNumber(0, 1, (toFiniteOrNull(racer?.lane3renScore ?? racer?.lane3renAvg ?? racer?.lane3RenRate) || 0) / 100)
    : 0;
  const localBonus = (() => {
    const local = toFiniteOrNull(racer?.localWinRate);
    const national = toFiniteOrNull(racer?.nationwideWinRate);
    if (local === null || national === null) return 0;
    return clampNumber(-0.08, 0.12, (local - national) / 10);
  })();
  const startStability = clampNumber(0, 1, (toFiniteOrNull(profile?.start_stability_score) ?? 50) / 100);
  const sashiScore = clampNumber(0, 1, (toFiniteOrNull(profile?.sashi_style_score) ?? (String(profile?.player_start_profile || "").includes("sashi") ? 66 : 50)) / 100);
  const makuriSashiScore = clampNumber(0, 1, (toFiniteOrNull(profile?.makuri_sashi_style_score ?? profile?.makurizashi_style_score) ?? 50) / 100);
  const laneBias = lane === 2 ? 0.2 : lane === 3 ? 0.18 : lane === 4 ? 0.16 : 0.08;
  const fit =
    (win.value * 0.29) +
    ((motor.value ?? 0.48) * 0.16) +
    ((boat.value ?? 0.46) * 0.12) +
    ((start.value ?? 0.45) * 0.14) +
    lane3renSupport * 0.18 +
    startStability * 0.11 +
    (lane === 2 ? sashiScore * 0.1 : (sashiScore + makuriSashiScore) * 0.05) +
    laneBias +
    localBonus -
    getRacerRiskPenalty(racer) * 0.22;
  return Number((clampNumber(0, 1, fit) * 100).toFixed(1));
}

function buildFixed1234Matrix({
  fixed1234TotalProbability,
  lane2Score,
  lane3Score,
  lane4Score
}) {
  if (!Number.isFinite(fixed1234TotalProbability) || fixed1234TotalProbability <= 0) {
    return {
      matrix: {},
      total: null,
      top4: [],
      top4Total: null,
      concentrationRatio: null
    };
  }

  const secondWeightsBase = {
    2: (Number(lane2Score) || 0) * 1.08,
    3: (Number(lane3Score) || 0) * 1.02,
    4: (Number(lane4Score) || 0) * 0.97
  };
  const thirdWeightsBase = {
    2: (Number(lane2Score) || 0) * 0.96,
    3: (Number(lane3Score) || 0) * 1.04,
    4: (Number(lane4Score) || 0) * 1.03
  };
  const combos = [
    "1-2-3",
    "1-2-4",
    "1-3-2",
    "1-3-4",
    "1-4-2",
    "1-4-3"
  ];
  const rawRows = combos.map((combo) => {
    const [, second, third] = combo.split("-").map(Number);
    const secondWeight = secondWeightsBase[second] || 0;
    const thirdWeight = thirdWeightsBase[third] || 0;
    const compatibility =
      second === 2 ? 1.08 :
      second === 3 ? 1.05 :
      0.99;
    const thirdResidual =
      third === 4 ? 1.05 :
      third === 3 ? 1.02 :
      0.98;
    return {
      combo,
      raw: Math.max(0.0001, secondWeight * thirdWeight * compatibility * thirdResidual)
    };
  });
  const rawTotal = rawRows.reduce((sum, row) => sum + row.raw, 0);
  const matrixEntries = rawRows.map((row) => ({
    combo: row.combo,
    probability: Number(((row.raw / rawTotal) * fixed1234TotalProbability).toFixed(4))
  })).sort((a, b) => b.probability - a.probability);
  const top4 = matrixEntries.slice(0, 4);
  const top4Total = Number(top4.reduce((sum, row) => sum + row.probability, 0).toFixed(4));
  const concentrationRatio = fixed1234TotalProbability > 0
    ? Number((top4Total / fixed1234TotalProbability).toFixed(4))
    : null;
  return {
    matrix: Object.fromEntries(matrixEntries.map((row) => [row.combo, row.probability])),
    total: Number(fixed1234TotalProbability.toFixed(4)),
    top4,
    top4Total,
    concentrationRatio
  };
}

const HARD_RACE_DECISION_THRESHOLDS = {
  buy4_escape_trust: 44,
  buy4_opponent_fit: 58,
  buy4_outside_break_max: 20,
  buy4_box_hit: 0.56,
  buy4_total: 0.34,
  buy4_top4: 0.27,
  buy4_shape_focus: 0.6,
  buy4_shape_concentration: 0.62,
  buy6_escape_trust: 44,
  buy6_opponent_fit: 58,
  buy6_outside_break_max: 20,
  buy6_box_hit: 0.56,
  buy6_total: 0.34,
  borderline_escape_trust: 40,
  borderline_opponent_fit: 52,
  borderline_outside_break_max: 26,
  borderline_box_hit: 0.5,
  borderline_total: 0.28,
  optional_penalty_cap: 0.02
};

const HARD_RACE_RANK_THRESHOLDS = {
  a_anchor: 44,
  a_total: 0.34,
  a_top4: 0.27,
  a_box: 58,
  a_outside_risk_max: 20,
  a_concentration: 0.62,
  b_anchor: 40,
  b_total: 0.28,
  b_top4: 0.22,
  b_box: 52,
  b_outside_risk_max: 26,
  b_concentration: 0.54
};

function finalizeHardRaceContractRow(row = {}) {
  const normalizeProbabilityMap = (probabilityMap) => {
    const entries = Object.entries(probabilityMap || {}).map(([key, value]) => [key, Number(value)]);
    const validEntries = entries.filter(([, value]) => Number.isFinite(value) && value > 0);
    const total = validEntries.reduce((sum, [, value]) => sum + value, 0);
    if (!(total > 0)) return Object.fromEntries(entries.map(([key], index) => [key, index === 0 ? 1 : 0]));
    let running = 0;
    return Object.fromEntries(validEntries.map(([key, value], index) => {
      if (index === validEntries.length - 1) return [key, Number(Math.max(0, 1 - running).toFixed(4))];
      const normalized = Number((value / total).toFixed(4));
      running += normalized;
      return [key, normalized];
    }));
  };
  const requiredScoreMap = {
    boat1_escape_trust: toFiniteOrNull(row?.boat1EscapeTrust ?? row?.boat1_escape_trust ?? row?.boat1AnchorScore ?? row?.boat1_anchor_score),
    opponent_234_fit: toFiniteOrNull(row?.opponent234Fit ?? row?.opponent_234_fit ?? row?.box234FitScore ?? row?.box_234_fit_score),
    outside_break_risk: toFiniteOrNull(row?.outsideBreakRisk ?? row?.outside_break_risk),
    box_hit_score: toFiniteOrNull(row?.boxHitScore ?? row?.box_hit_score ?? row?.fixed1234TotalProbability ?? row?.fixed1234_total_probability)
  };
  const missingRequiredScores = Object.entries(requiredScoreMap)
    .filter(([, value]) => value === null)
    .map(([field]) => field);
  const allMajorScoresMissing = missingRequiredScores.length === Object.keys(requiredScoreMap).length;
  const inputStatus = row?.status || row?.finalStatus || row?.data_status || "UNAVAILABLE";
  const apiConfidenceStatus = row?.confidence_status || row?.data_status || null;
  const derivedDataStatus =
    apiConfidenceStatus ||
    (inputStatus === "BROKEN_PIPELINE" || allMajorScoresMissing
      ? "BROKEN_PIPELINE"
      : inputStatus === "NOT_ELIGIBLE"
        ? "NOT_ELIGIBLE"
        : inputStatus === "FALLBACK"
          ? "FALLBACK"
          : row?.fetchFailed
            ? "BROKEN_PIPELINE"
            : missingRequiredScores.length > 0
              ? "FALLBACK"
              : "READY");
  const normalized = {
    raceNo: Number.isFinite(Number(row?.raceNo ?? row?.race_no)) ? Number(row?.raceNo ?? row?.race_no) : null,
    race_no: Number.isFinite(Number(row?.raceNo ?? row?.race_no)) ? Number(row?.raceNo ?? row?.race_no) : null,
    venueName: row?.venueName || "-",
    status: derivedDataStatus,
    finalStatus: derivedDataStatus,
    data_status: derivedDataStatus,
    confidence_status: row?.confidence_status || derivedDataStatus,
    hardRaceScore: null,
    hard_race_score: null,
    boat1AnchorScore: toFiniteOrNull(row?.boat1AnchorScore ?? row?.boat1_anchor_score),
    boat1_anchor_score: toFiniteOrNull(row?.boat1AnchorScore ?? row?.boat1_anchor_score),
    boat1EscapeTrust: toFiniteOrNull(row?.boat1EscapeTrust ?? row?.boat1_escape_trust ?? row?.boat1AnchorScore ?? row?.boat1_anchor_score),
    boat1_escape_trust: toFiniteOrNull(row?.boat1EscapeTrust ?? row?.boat1_escape_trust ?? row?.boat1AnchorScore ?? row?.boat1_anchor_score),
    box234FitScore: toFiniteOrNull(row?.box234FitScore ?? row?.box_234_fit_score),
    box_234_fit_score: toFiniteOrNull(row?.box234FitScore ?? row?.box_234_fit_score),
    opponent234Fit: toFiniteOrNull(row?.opponent234Fit ?? row?.opponent_234_fit ?? row?.box234FitScore ?? row?.box_234_fit_score),
    opponent_234_fit: toFiniteOrNull(row?.opponent234Fit ?? row?.opponent_234_fit ?? row?.box234FitScore ?? row?.box_234_fit_score),
    pair23Fit: toFiniteOrNull(row?.pair23Fit ?? row?.pair23_fit),
    pair23_fit: toFiniteOrNull(row?.pair23Fit ?? row?.pair23_fit),
    pair24Fit: toFiniteOrNull(row?.pair24Fit ?? row?.pair24_fit),
    pair24_fit: toFiniteOrNull(row?.pair24Fit ?? row?.pair24_fit),
    pair34Fit: toFiniteOrNull(row?.pair34Fit ?? row?.pair34_fit),
    pair34_fit: toFiniteOrNull(row?.pair34Fit ?? row?.pair34_fit),
    head_prob_1: toFiniteOrNull(row?.headProb1 ?? row?.head_prob_1),
    head_prob_2: toFiniteOrNull(row?.headProb2 ?? row?.head_prob_2),
    head_prob_3: toFiniteOrNull(row?.headProb3 ?? row?.head_prob_3),
    head_prob_4: toFiniteOrNull(row?.headProb4 ?? row?.head_prob_4),
    head_prob_5: toFiniteOrNull(row?.headProb5 ?? row?.head_prob_5),
    head_prob_6: toFiniteOrNull(row?.headProb6 ?? row?.head_prob_6),
    killEscapeRisk: toFiniteOrNull(row?.killEscapeRisk ?? row?.kill_escape_risk),
    kill_escape_risk: toFiniteOrNull(row?.killEscapeRisk ?? row?.kill_escape_risk),
    shapeShuffleRisk: toFiniteOrNull(row?.shapeShuffleRisk ?? row?.shape_shuffle_risk),
    shape_shuffle_risk: toFiniteOrNull(row?.shapeShuffleRisk ?? row?.shape_shuffle_risk),
    makuriRisk: toFiniteOrNull(row?.makuriRisk ?? row?.makuri_risk),
    makuri_risk: toFiniteOrNull(row?.makuriRisk ?? row?.makuri_risk),
    outsideHeadRisk: toFiniteOrNull(row?.outsideHeadRisk ?? row?.outside_head_risk),
    outside_head_risk: toFiniteOrNull(row?.outsideHeadRisk ?? row?.outside_head_risk),
    outside2ndRisk: toFiniteOrNull(row?.outside2ndRisk ?? row?.outside_2nd_risk),
    outside_2nd_risk: toFiniteOrNull(row?.outside2ndRisk ?? row?.outside_2nd_risk),
    outside3rdRisk: toFiniteOrNull(row?.outside3rdRisk ?? row?.outside_3rd_risk),
    outside_3rd_risk: toFiniteOrNull(row?.outside3rdRisk ?? row?.outside_3rd_risk),
    outsideBoxBreakRisk: toFiniteOrNull(row?.outsideBoxBreakRisk ?? row?.outside_box_break_risk),
    outside_box_break_risk: toFiniteOrNull(row?.outsideBoxBreakRisk ?? row?.outside_box_break_risk),
    outsideBreakRisk: toFiniteOrNull(row?.outsideBreakRisk ?? row?.outside_break_risk),
    outside_break_risk: toFiniteOrNull(row?.outsideBreakRisk ?? row?.outside_break_risk),
    boxHitScore: toFiniteOrNull(row?.boxHitScore ?? row?.box_hit_score ?? row?.fixed1234TotalProbability ?? row?.fixed1234_total_probability),
    box_hit_score: toFiniteOrNull(row?.boxHitScore ?? row?.box_hit_score ?? row?.fixed1234TotalProbability ?? row?.fixed1234_total_probability),
    shapeFocusScore: toFiniteOrNull(row?.shapeFocusScore ?? row?.shape_focus_score ?? row?.top4Fixed1234Probability ?? row?.top4_fixed1234_probability),
    shape_focus_score: toFiniteOrNull(row?.shapeFocusScore ?? row?.shape_focus_score ?? row?.top4Fixed1234Probability ?? row?.top4_fixed1234_probability),
    fixed1234TotalProbability: toFiniteOrNull(row?.fixed1234TotalProbability ?? row?.fixed1234_total_probability),
    fixed1234_total_probability: toFiniteOrNull(row?.fixed1234TotalProbability ?? row?.fixed1234_total_probability),
    top4Fixed1234Probability: toFiniteOrNull(row?.fixed1234Top4Total ?? row?.top4Fixed1234Probability ?? row?.top4_fixed1234_probability),
    top4_fixed1234_probability: toFiniteOrNull(row?.fixed1234Top4Total ?? row?.top4Fixed1234Probability ?? row?.top4_fixed1234_probability),
    fixed1234Top4Total: toFiniteOrNull(row?.fixed1234Top4Total ?? row?.top4Fixed1234Probability ?? row?.top4_fixed1234_probability),
    fixed1234ShapeConcentration: toFiniteOrNull(row?.fixed1234ShapeConcentration ?? row?.fixed1234_shape_concentration),
    fixed1234_shape_concentration: toFiniteOrNull(row?.fixed1234ShapeConcentration ?? row?.fixed1234_shape_concentration),
    fixed1234Matrix: row?.fixed1234Matrix && typeof row.fixed1234Matrix === "object" ? row.fixed1234Matrix : (row?.fixed1234_matrix && typeof row.fixed1234_matrix === "object" ? row.fixed1234_matrix : {}),
    fixed1234_matrix: row?.fixed1234Matrix && typeof row.fixed1234Matrix === "object" ? row.fixed1234Matrix : (row?.fixed1234_matrix && typeof row.fixed1234_matrix === "object" ? row.fixed1234_matrix : {}),
    fixed1234Top4: Array.isArray(row?.fixed1234Top4) ? row.fixed1234Top4 : (Array.isArray(row?.fixed1234_top4) ? row.fixed1234_top4 : []),
    fixed1234_top4: Array.isArray(row?.fixed1234Top4) ? row.fixed1234Top4 : (Array.isArray(row?.fixed1234_top4) ? row.fixed1234_top4 : []),
    suggestedShape: row?.suggestedShape ?? row?.suggested_shape ?? null,
    suggested_shape: row?.suggestedShape ?? row?.suggested_shape ?? null,
    hardRaceRank: row?.hardRaceRank ?? row?.hard_race_rank ?? row?.screeningDebug?.hard_race_rank ?? null,
    hard_race_rank: row?.hardRaceRank ?? row?.hard_race_rank ?? row?.screeningDebug?.hard_race_rank ?? null,
    operational_pick: row?.operational_pick ?? row?.operationalPick ?? row?.features?.operational_policy?.operational_pick ?? null,
    open_mode: row?.open_mode ?? {},
    hard_mode: row?.hard_mode ?? {},
    head_candidates: Array.isArray(row?.head_candidates) ? row.head_candidates : [],
    head_candidate_ranking: Array.isArray(row?.head_candidate_ranking) ? row.head_candidate_ranking : [],
    head_opponents: Array.isArray(row?.head_opponents) ? row.head_opponents : [],
    outside_danger_scenarios: Array.isArray(row?.outside_danger_scenarios) ? row.outside_danger_scenarios : [],
    fallback_used: row?.fallback_used ?? {},
    source_summary: row?.source_summary ?? {},
    recommendation: row?.recommendation || row?.buyStyleRecommendation || "UNAVAILABLE",
    buyStyleRecommendation: row?.buyStyleRecommendation || row?.recommendation || "UNAVAILABLE",
    decision: row?.decision || row?.recommendation || row?.buyStyleRecommendation || "UNAVAILABLE",
      decision_reason: row?.decision_reason || row?.screeningDebug?.decision_reason || null,
      errors: Array.isArray(row?.errors) ? row.errors : [],
      missing_fields: Array.isArray(row?.missing_fields) ? row.missing_fields : [],
      missing_field_details: row?.missing_field_details && typeof row.missing_field_details === "object" ? row.missing_field_details : {},
      metric_status: row?.metric_status && typeof row.metric_status === "object" ? row.metric_status : {},
      fetchFailed: !!row?.fetchFailed,
      screeningDebug: row?.screeningDebug && typeof row.screeningDebug === "object" ? row.screeningDebug : {}
    };

  const normalizedHeadProbMap = normalizeProbabilityMap({
    1: normalized.head_prob_1,
    2: normalized.head_prob_2,
    3: normalized.head_prob_3,
    4: normalized.head_prob_4,
    5: normalized.head_prob_5,
    6: normalized.head_prob_6
  });
  normalized.head_prob_1 = normalizedHeadProbMap[1] ?? 0;
  normalized.head_prob_2 = normalizedHeadProbMap[2] ?? 0;
  normalized.head_prob_3 = normalizedHeadProbMap[3] ?? 0;
  normalized.head_prob_4 = normalizedHeadProbMap[4] ?? 0;
  normalized.head_prob_5 = normalizedHeadProbMap[5] ?? 0;
  normalized.head_prob_6 = normalizedHeadProbMap[6] ?? 0;

  const normalizedFixed1234Matrix = normalizeProbabilityMap(
    safeEntries(normalized.fixed1234_matrix).reduce((acc, [combo, probability]) => {
      acc[combo] = probability;
      return acc;
    }, {})
  );
  normalized.fixed1234Matrix = normalizedFixed1234Matrix;
  normalized.fixed1234_matrix = normalizedFixed1234Matrix;

  const requiredFields = [
    "race_no",
    "status",
    "boat1_escape_trust",
    "opponent_234_fit",
    "pair23_fit",
    "pair24_fit",
    "pair34_fit",
    "head_prob_1",
    "head_prob_2",
    "head_prob_3",
    "head_prob_4",
    "head_prob_5",
    "head_prob_6",
    "kill_escape_risk",
    "shape_shuffle_risk",
    "makuri_risk",
    "outside_head_risk",
    "outside_2nd_risk",
    "outside_3rd_risk",
    "outside_box_break_risk",
    "outside_break_risk",
    "box_hit_score",
    "shape_focus_score",
    "fixed1234_total_probability",
    "top4_fixed1234_probability",
    "fixed1234_shape_concentration",
    "suggested_shape",
    "decision"
  ];
  const missingFields = [...normalized.missing_fields];
  requiredFields.forEach((field) => {
    if (!(field in normalized) || normalized[field] == null) missingFields.push(field);
  });
  normalized.missing_fields = [...new Set(missingFields)];
  normalized.screeningDebug = {
    fetch_success: normalized.fetchFailed ? false : normalized.screeningDebug.fetch_success ?? normalized.screeningDebug.race_fetch_success ?? true,
    parse_success: normalized.screeningDebug.parse_success ?? !normalized.fetchFailed,
    score_success: normalized.screeningDebug.score_success ?? (normalized.boxHitScore !== null),
    missing_fields: normalized.missing_fields,
    missing_required_scores: missingRequiredScores,
    data_status: normalized.data_status,
    final_status: normalized.finalStatus,
    ...normalized.screeningDebug
  };
  normalized.screeningDebug.response_payload = {
    race_no: normalized.race_no,
    data_status: normalized.data_status,
    confidence_status: normalized.confidence_status,
    boat1_escape_trust: normalized.boat1_escape_trust,
    opponent_234_fit: normalized.opponent_234_fit,
    pair23_fit: normalized.pair23_fit,
    pair24_fit: normalized.pair24_fit,
    pair34_fit: normalized.pair34_fit,
    kill_escape_risk: normalized.kill_escape_risk,
    shape_shuffle_risk: normalized.shape_shuffle_risk,
    makuri_risk: normalized.makuri_risk,
    outside_break_risk: normalized.outside_break_risk,
    box_hit_score: normalized.box_hit_score,
    shape_focus_score: normalized.shape_focus_score,
    fixed1234_total_probability: normalized.fixed1234_total_probability,
    top4_fixed1234_probability: normalized.top4_fixed1234_probability,
    fixed1234_shape_concentration: normalized.fixed1234_shape_concentration,
    suggested_shape: normalized.suggested_shape,
    decision: normalized.decision,
    decision_reason: normalized.decision_reason,
    missing_fields: normalized.missing_fields
  };
  if (normalized.data_status === "BROKEN_PIPELINE" && !normalized.decision_reason) {
    normalized.decision_reason = normalized.errors.length > 0
      ? `Broken pipeline: ${normalized.errors.join(", ")}`
      : normalized.missing_fields.length > 0
        ? `Broken pipeline: missing ${normalized.missing_fields.join(", ")}`
        : "Broken pipeline: required precomputed features are unavailable";
  } else if (normalized.data_status === "FALLBACK" && !normalized.decision_reason) {
    normalized.decision_reason = normalized.missing_fields.length > 0
      ? `Fallback inference: estimated ${normalized.missing_fields.join(", ")}`
      : "Fallback inference: some metrics use stored-feature estimates";
  }
  return normalized;
}

function getHardRaceFieldState(row, fieldName) {
  const metricStatus = row?.metric_status && typeof row.metric_status === "object" ? row.metric_status : {};
  if (metricStatus?.[fieldName]?.reason) return String(metricStatus[fieldName].reason);
  const missingFields = new Set(Array.isArray(row?.missing_fields) ? row.missing_fields : []);
  const missingFieldDetails = row?.missing_field_details && typeof row.missing_field_details === "object" ? row.missing_field_details : {};
  const screeningDebug = row?.screeningDebug && typeof row.screeningDebug === "object" ? row.screeningDebug : {};
  const missingRequiredScores = new Set(Array.isArray(screeningDebug?.missing_required_scores) ? screeningDebug.missing_required_scores : []);
  const optionalMissing = Array.isArray(screeningDebug?.optional_fields_missing) ? screeningDebug.optional_fields_missing : [];
  if (missingFields.has(fieldName) || missingRequiredScores.has(fieldName)) {
    if (optionalMissing.length > 0) return "precomputed feature fallback";
    return "not calculated";
  }
  const relatedReason = Object.entries(missingFieldDetails)
    .find(([key]) => key.includes(".") && (
      fieldName === "boat1_escape_trust"
        ? /\.national_win_rate$|\.local_win_rate$|\.avg_st$|\.f_count$|\.l_count$|\.motor_2ren$|\.motor_3ren$|\.boat_2ren$|\.boat_3ren$/.test(key)
        : fieldName === "opponent_234_fit"
          ? /\.course_3rentai_rate$|\.lane_3rentai_rate$|\.avg_st$|\.motor_2ren$|\.motor_3ren$|\.boat_2ren$|\.stability_rate$|\.breakout_rate$|\.sashi_rate$|\.makuri_rate$|\.makurisashi_rate$|\.zentsuke_tendency$/.test(key)
          : fieldName === "makuri_risk"
            ? /\.makuri_rate$|\.makurisashi_rate$/.test(key)
            : fieldName === "outside_break_risk"
              ? /\.course_3rentai_rate$|\.avg_st$|\.motor_2ren$/.test(key)
              : false
    ));
  if (relatedReason?.[1]?.reason) return String(relatedReason[1].reason);
  return "precomputed feature missing";
}

function renderHardRaceMetric(row, fieldName, value, formatter) {
  if (value != null) return formatter(value);
  const reason = getHardRaceFieldState(row, fieldName);
  return <span title={reason}>-- ({reason})</span>;
}

function formatPercentDisplay(value, digits = 1) {
  if (value === null || value === undefined || value === "") return "--";
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  return `${formatMaybeNumber(num * 100, digits)}%`;
}

function getHardRaceDecisionClass(decision) {
  const value = String(decision || "").toUpperCase();
  if (value === "BUY-6" || value === "BUY-4" || value === "BUY") return "status-hit";
  if (value === "BORDERLINE") return "status-unsettled";
  if (value === "BROKEN_PIPELINE") return "status-unsettled";
  return "risk-small";
}

function getHardRaceRankClass(rank) {
  const value = String(rank || "").toUpperCase();
  if (value === "A") return "status-hit";
  if (value === "B") return "status-unsettled";
  if (value === "BROKEN_PIPELINE") return "status-unsettled";
  return "risk-small";
}

function getHardRaceConfidenceClass(status) {
  const value = String(status || "").toUpperCase();
  if (value === "READY") return "status-hit";
  if (value === "FALLBACK") return "risk-small";
  if (value === "NOT_ELIGIBLE") return "status-unsettled";
  return "status-miss";
}

function getHardRaceRiskTone(value, reverse = false) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "tone-neutral";
  const score = reverse ? 1 - num : num;
  if (score >= 0.58) return "tone-high";
  if (score >= 0.32) return "tone-medium";
  return "tone-low";
}

function buildHeadRankingRows(row = {}) {
  const explicitRanking = Array.isArray(row?.head_candidate_ranking) ? row.head_candidate_ranking : [];
  if (explicitRanking.length > 0) {
    return explicitRanking
      .map((item) => ({
        lane: parseLane(item?.lane),
        probability: Number(item?.probability)
      }))
      .filter((item) => Number.isInteger(item.lane) && Number.isFinite(item.probability))
      .sort((a, b) => b.probability - a.probability);
  }
  return [1, 2, 3, 4, 5, 6]
    .map((lane) => ({
      lane,
      probability: Number(row?.[`head_prob_${lane}`])
    }))
    .filter((item) => Number.isFinite(item.probability))
    .sort((a, b) => b.probability - a.probability);
}

function buildFixed1234ProbabilityRows(row = {}) {
  const matrixEntries = safeEntries(row?.fixed1234Matrix)
    .map(([combo, probability]) => ({
      combo,
      probability: Number(probability)
    }))
    .filter((item) => item.combo && Number.isFinite(item.probability))
    .sort((a, b) => b.probability - a.probability);
  const maxProbability = matrixEntries.length > 0 ? matrixEntries[0].probability : 0;
  return matrixEntries.map((item, index) => ({
    ...item,
    rank: index + 1,
    width: maxProbability > 0 ? Math.max(10, (item.probability / maxProbability) * 100) : 0,
    isTop2: index < 2,
    isTop4: index < 4
  }));
}

function getHardRaceStatusAccent(row = {}) {
  const status = String(row?.confidence_status || row?.data_status || "").toUpperCase();
  if (status === "READY") return "事前特徴量は完成";
  if (status === "FALLBACK") return "補完推定あり";
  if (status === "BROKEN_PIPELINE") return "事前特徴量未生成";
  if (status === "NOT_ELIGIBLE") return "対象外";
  return "確認中";
}

function buildSourceSummaryRows(sourceSummary = {}) {
  const snapshot = sourceSummary?.snapshot && typeof sourceSummary.snapshot === "object" ? sourceSummary.snapshot : {};
  const fallback = sourceSummary?.fallback && typeof sourceSummary.fallback === "object" ? sourceSummary.fallback : {};
  const coverage = snapshot?.coverage && typeof snapshot.coverage === "object" ? snapshot.coverage : {};
  return [
    { label: "mode", value: sourceSummary?.mode || "-" },
    { label: "inference_source", value: sourceSummary?.inference_source || "-" },
    { label: "snapshot", value: `race:${snapshot?.race || "-"} / entries:${snapshot?.entries || "-"} / feature:${snapshot?.feature_snapshot || "-"}` },
    { label: "coverage", value: Number.isFinite(Number(coverage?.ready_fields)) && Number.isFinite(Number(coverage?.total_fields)) ? `${coverage.ready_fields}/${coverage.total_fields}` : "-" },
    { label: "fallback", value: fallback?.used ? `yes (${Array.isArray(fallback?.fields) ? fallback.fields.join(", ") || "-" : "-"})` : "no" },
    { label: "estimated", value: Array.isArray(sourceSummary?.estimated_fields) && sourceSummary.estimated_fields.length > 0 ? sourceSummary.estimated_fields.join(", ") : "none" }
  ];
}

function buildTop6PredictionRows(prediction = {}) {
  const top6 = Array.isArray(prediction?.top6) ? prediction.top6 : [];
  const maxProbability = top6.length > 0 ? Math.max(...top6.map((row) => Number(row?.probability) || 0)) : 0;
  return top6.map((row, index) => ({
    combo: row?.combo || "--",
    probability: Number(row?.probability) || 0,
    tier: row?.tier || (index < 2 ? "本命" : index < 4 ? "対抗" : "抑え"),
    tierKey: row?.tier === "本命" ? "main" : row?.tier === "対抗" ? "challenge" : "cover",
    width: maxProbability > 0 ? Math.max(10, ((Number(row?.probability) || 0) / maxProbability) * 100) : 0
  }));
}

function groupTop6PredictionRows(rows = []) {
  return {
    本命: rows.filter((row) => row?.tier === "本命"),
    対抗: rows.filter((row) => row?.tier === "対抗"),
    抑え: rows.filter((row) => row?.tier === "抑え")
  };
}

function getPredictionChaosTone(label) {
  const value = String(label || "").toUpperCase();
  if (value === "CHAOS" || value === "高") return "tone-high";
  if (value === "NORMAL" || value === "中") return "tone-medium";
  return "tone-low";
}

function getPredictionChaosLabel(prediction = {}) {
  const label = String(prediction?.chaos_label || "").toUpperCase();
  if (label === "高") return "CHAOS";
  if (label === "中") return "NORMAL";
  if (label === "低") return "HARD";
  return prediction?.chaos_label || "--";
}

function getPredictionConfidenceState(sourceMeta = {}) {
  const snapshots = sourceMeta?.local_snapshots && typeof sourceMeta.local_snapshots === "object" ? sourceMeta.local_snapshots : {};
  if (Number(snapshots?.feature_snapshot || 0) > 0) return "OK";
  if (Number(snapshots?.entry_snapshot || 0) > 0) return "PARTIAL";
  if (sourceMeta?.cache?.fallback === "db_snapshot") return "FALLBACK";
  return "PARTIAL";
}

function getPredictionConfidenceClass(value) {
  const status = String(value || "").toUpperCase();
  if (status === "OK") return "status-hit";
  if (status === "PARTIAL") return "status-unsettled";
  if (status === "FALLBACK") return "risk-small";
  return "status-miss";
}

function getOutsideRiskLead(row = {}) {
  const scenarios = [
    { key: "outsideHeadRisk", label: "5,6の頭侵入に注意", value: Number(row?.outsideHeadRisk) },
    { key: "outside2ndRisk", label: "5,6の2着侵入に注意", value: Number(row?.outside2ndRisk) },
    { key: "outside3rdRisk", label: "5,6の3着残りに注意", value: Number(row?.outside3rdRisk) },
    { key: "outsideBoxBreakRisk", label: "1-234-234の箱が崩れやすい", value: Number(row?.outsideBoxBreakRisk) }
  ].filter((item) => Number.isFinite(item.value));
  if (scenarios.length === 0) return "外枠危険は未計測";
  scenarios.sort((a, b) => b.value - a.value);
  if (scenarios[0].value < 0.18) return "外枠侵入リスクは低め";
  return scenarios[0].label;
}

function getHardRaceDecisionCopy(row = {}) {
  const decision = String(row?.buyStyleRecommendation || row?.decision || "").toUpperCase();
  if (decision === "BUY-6") return "6点でそのまま買える本線寄り";
  if (decision === "BUY-4") return "上位4点に絞って買いやすい";
  if (decision === "BORDERLINE") return "見送り候補だが監視価値あり";
  if (decision === "SKIP") return "無理に触らない方が良い";
  if (decision === "BROKEN_PIPELINE") return "事前特徴量の不足で判定保留";
  return "追加確認が必要";
}

function getHardRaceOperationalLabel(row = {}) {
  if (row?.open_mode?.active) return row?.operational_pick || row?.open_mode?.alert_label || "穴候補";
  return row?.operational_pick || "見送り";
}

function getHardRaceConfidenceCopy(status) {
  const value = String(status || "").toUpperCase();
  if (value === "READY") return "必要な事前特徴量が揃っていて pure inference 可能";
  if (value === "FALLBACK") return "一部は保存済み特徴量から補完推定";
  if (value === "BROKEN_PIPELINE") return "事前特徴量が未生成かマッピング不整合";
  if (value === "NOT_ELIGIBLE") return "予想対象外条件です";
  return "データ確認中";
}

function getHardRaceRiskMeta(value, label, copy) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return {
      label,
      valueLabel: "--",
      level: "未計測",
      tone: "tone-neutral",
      copy: copy || "まだ評価できていません。"
    };
  }
  if (num >= 0.58) {
    return {
      label,
      valueLabel: formatPercentDisplay(num),
      level: "高",
      tone: "tone-high",
      copy: copy || "崩れやすいので強く警戒。"
    };
  }
  if (num >= 0.32) {
    return {
      label,
      valueLabel: formatPercentDisplay(num),
      level: "中",
      tone: "tone-medium",
      copy: copy || "相手や点数の調整を検討。"
    };
  }
  return {
    label,
    valueLabel: formatPercentDisplay(num),
    level: "低",
    tone: "tone-low",
    copy: copy || "現時点では大崩れしにくい水準。"
  };
}

function buildHardRaceDangerRows(row = {}) {
  return [
    getHardRaceRiskMeta(row?.outsideHeadRisk, "outside_head_risk", "5,6が頭まで突き抜けると1頭前提が崩れます。"),
    getHardRaceRiskMeta(row?.outside2ndRisk, "outside_2nd_risk", "5,6の2着侵入があると1-23-234系の主力が削られます。"),
    getHardRaceRiskMeta(row?.outside3rdRisk, "outside_3rd_risk", "5,6が3着に残ると薄い目まで広がります。"),
    getHardRaceRiskMeta(row?.outsideBoxBreakRisk, "outside_box_break_risk", "1-234-234の箱全体が壊れる危険度です。")
  ];
}

function getKpiSignalMeta(value, { target = null, lowerIsBetter = false } = {}) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return { tone: "neutral", label: "No Data", deltaText: "検証待ち" };
  }
  if (!Number.isFinite(Number(target))) {
    return { tone: "neutral", label: "Observed", deltaText: "観測値" };
  }
  const diff = lowerIsBetter ? Number(target) - num : num - Number(target);
  if (diff >= 5) {
    return { tone: "up", label: "Improving", deltaText: `target ${formatMaybeNumber(Math.abs(diff), 1)}ptクリア` };
  }
  if (diff >= 0) {
    return { tone: "flat", label: "Near Target", deltaText: `target内 +${formatMaybeNumber(Math.abs(diff), 1)}pt` };
  }
  return { tone: "down", label: "Needs Work", deltaText: `target差 ${formatMaybeNumber(Math.abs(diff), 1)}pt` };
}

function buildHardRaceKpiCards(calibration, context = {}) {
  if (!calibration) return [];
  return [
    {
      key: "buy6InsideHitRate",
      label: "BUY-6 の6点内的中率",
      value: calibration.buy6InsideHitRate,
      numerator: calibration.buy6InsideHits,
      denominator: calibration.buy6Count,
      target: 68,
      lowerIsBetter: false,
      description: "BUY-6判定のレースが6点セット内に収まった率。"
    },
    {
      key: "buy4Top4HitRate",
      label: "BUY-4 の4点内的中率",
      value: calibration.buy4Top4HitRate,
      numerator: calibration.buy4Top4Hits,
      denominator: calibration.buy4Count,
      target: 52,
      lowerIsBetter: false,
      description: "BUY-4判定のレースが上位4点に入った率。"
    },
    {
      key: "borderlinePlusInsideHitRate",
      label: "BORDERLINE以上の6点内",
      value: calibration.borderlinePlusInsideHitRate,
      numerator: calibration.borderlinePlusInsideHits,
      denominator: calibration.borderlinePlusCount,
      target: 60,
      lowerIsBetter: false,
      description: "候補として残したレース全体の安定度。"
    },
    {
      key: "skipInsideRate",
      label: "SKIPの中で6点内だった率",
      value: calibration.skipInsideRate,
      numerator: calibration.skipInsideHits,
      denominator: calibration.skipCount,
      target: 18,
      lowerIsBetter: true,
      description: "見送りの取りこぼし率。低いほど良いです。"
    },
    {
      key: "headHitRate",
      label: "1頭一致率",
      value: calibration.headHitRate,
      numerator: calibration.headHitCount,
      denominator: calibration.reviewedCount,
      target: 55,
      lowerIsBetter: false,
      description: "頭候補1位と実着順1着が一致した率。"
    },
    {
      key: "outsideDetectionRate",
      label: "5,6侵入検知率",
      value: calibration.outsideDetectionRate,
      numerator: calibration.outsideDetectedHits,
      denominator: calibration.outsideActualCount,
      target: 65,
      lowerIsBetter: false,
      description: "5,6絡み実発生レースを危険として拾えた率。"
    },
    {
      key: "insideSixHitRate",
      label: "全体 6点内率",
      value: calibration.insideSixHitRate,
      numerator: calibration.insideSixHitCount,
      denominator: calibration.reviewedCount,
      target: 55,
      lowerIsBetter: false,
      description: "対象レース全体で6点セットに収まった率。"
    },
    {
      key: "falseSkipRate",
      label: "SKIP取りこぼし率",
      value: calibration.falseSkipRate,
      numerator: calibration.falseSkipCount,
      denominator: calibration.reviewedCount,
      target: 12,
      lowerIsBetter: true,
      description: "全レビュー中、SKIP判定なのに6点内だった割合。"
    }
  ].map((item) => ({
    ...item,
    signal: getKpiSignalMeta(item.value, { target: item.target, lowerIsBetter: item.lowerIsBetter }),
    scopeLabel: context?.scopeLabel || "Selected Slate"
  }));
}

function buildHardRaceHistoryMap(rows = [], selectedDate = "", selectedVenueId = null) {
  const map = new Map();
  safeArray(rows).forEach((row) => {
    const rowDate = String(row?.race_date || "").slice(0, 10);
    const rowVenueId = Number(row?.venue_id);
    if (selectedDate && rowDate !== String(selectedDate)) return;
    if (Number.isInteger(Number(selectedVenueId)) && rowVenueId !== Number(selectedVenueId)) return;
    const key = makeRaceKey({
      race_id: row?.race_id,
      race_date: row?.race_date,
      venue_id: row?.venue_id,
      race_no: row?.race_no
    });
    const actualResult = normalizeCombo(
      row?.confirmed_result ||
      row?.verification?.confirmed_result ||
      (Array.isArray(row?.actual_result) ? row.actual_result.join("-") : "")
    );
    map.set(key, {
      raceId: row?.race_id || null,
      actualResult: actualResult || null,
      verificationStatus: row?.verification_status || null
    });
  });
  return map;
}

function buildHardRaceScreeningRow(entry, venueNameFallback = "-") {
  if (entry?.ok && entry?.data?.hardRace1234) {
    return finalizeHardRaceContractRow({
      raceNo: entry?.data?.hardRace1234?.race_no ?? entry?.raceNo ?? null,
      venueName: entry?.data?.race?.venueName || venueNameFallback,
      status: entry?.data?.hardRace1234?.status || "READY",
      data_status: entry?.data?.hardRace1234?.data_status || "READY",
      boat1EscapeTrust: entry?.data?.hardRace1234?.boat1_escape_trust ?? null,
      opponent234Fit: entry?.data?.hardRace1234?.opponent_234_fit ?? null,
      outsideBreakRisk: entry?.data?.hardRace1234?.outside_break_risk ?? null,
      makuriRisk: entry?.data?.hardRace1234?.makuri_risk ?? null,
      fixed1234TotalProbability: entry?.data?.hardRace1234?.fixed1234_total_probability ?? null,
      top4Fixed1234Probability: entry?.data?.hardRace1234?.top4_fixed1234_probability ?? null,
      fixed1234ShapeConcentration: entry?.data?.hardRace1234?.fixed1234_shape_concentration ?? null,
      fixed1234Matrix: entry?.data?.hardRace1234?.fixed1234_matrix ?? {},
      fixed1234Top4: entry?.data?.hardRace1234?.fixed1234_top4 ?? [],
      suggestedShape: entry?.data?.hardRace1234?.suggested_shape ?? null,
      hardRaceRank: entry?.data?.hardRace1234?.hard_race_rank ?? entry?.data?.hardRace1234?.screeningDebug?.hard_race_rank ?? null,
      finalStatus:
        entry?.data?.hardRace1234?.decision === "BUY-4" || entry?.data?.hardRace1234?.decision === "BUY-6"
          ? "BUY"
          : entry?.data?.hardRace1234?.decision === "BORDERLINE"
            ? "BORDERLINE"
            : entry?.data?.hardRace1234?.decision || null,
      recommendation: entry?.data?.hardRace1234?.decision ?? null,
      decision: entry?.data?.hardRace1234?.decision ?? null,
      decision_reason: entry?.data?.hardRace1234?.decision_reason ?? null,
      missing_fields: entry?.data?.hardRace1234?.missing_fields ?? [],
      missing_field_details: entry?.data?.hardRace1234?.missing_field_details ?? {},
      metric_status: entry?.data?.hardRace1234?.metric_status ?? {},
      screeningDebug: entry?.data?.hardRace1234?.screeningDebug ?? {},
      source_summary: entry?.data?.hardRace1234?.source_summary ?? {},
      fetched_urls: entry?.data?.hardRace1234?.fetched_urls ?? {},
      raw_saved_paths: entry?.data?.hardRace1234?.raw_saved_paths ?? {},
      normalized_data: entry?.data?.hardRace1234?.normalized_data ?? null
    });
  }

  const failedDebug = {
    race_fetch_success: false,
    kyoteibiyori_fetch_success: false,
    href_extraction_success: false,
    parse_success: false,
    core_fields_ready: false,
    hard_race_score_ready: false,
    final_status: "UNAVAILABLE",
    attempt_count: Number(entry?.attemptCount || 0),
    failure_message: entry?.error || "Race data unavailable"
  };
  if (!entry?.ok || !entry?.data) {
    return finalizeHardRaceContractRow({
      raceNo: entry?.raceNo ?? null,
      venueName: venueNameFallback,
      hardRaceScore: null,
      boat1AnchorScore: null,
      boat1EscapeTrust: null,
      box234FitScore: null,
      makuriRisk: null,
      outsideBreakRisk: null,
      fixed1234ShapeConcentration: null,
      fixed1234Probability: null,
      finalStatus: "UNAVAILABLE",
      buyRecommendation: "UNAVAILABLE",
      suggestedShape: null,
      positiveReasons: [],
      negativeReasons: [entry?.error || "Race data unavailable"],
      topReasons: [entry?.error || "Race data unavailable"],
      expandableReason: "Screening skipped because race data could not be loaded.",
      sourceData: null,
      fetchFailed: true,
      status: "UNAVAILABLE",
      data_status: "BROKEN_PIPELINE",
      recommendation: "UNAVAILABLE",
      decision: "SKIP",
      decision_reason: entry?.error || "Precomputed race snapshot unavailable",
      errors: [entry?.error || "Race data unavailable"],
      missing_fields: [
        "hard_race_score",
        "boat1_anchor_score",
        "boat1_escape_trust",
        "box_234_fit_score",
        "opponent_234_fit",
        "makuri_risk",
        "outside_break_risk",
        "fixed1234_total_probability",
        "top4_fixed1234_probability",
        "fixed1234_shape_concentration",
        "suggested_shape"
      ],
      screeningDebug: failedDebug
    });
  }

  const source = entry.data;
  const kyoteiFetch = source?.source?.kyotei_biyori && typeof source.source.kyotei_biyori === "object"
    ? source.source.kyotei_biyori
    : {};
  const officialFetch = source?.source?.official_fetch_status && typeof source.source.official_fetch_status === "object"
    ? source.source.official_fetch_status
    : {};
  const racers = Array.isArray(source?.racers) ? source.racers : [];
  const boat1 = racers.find((row) => Number(row?.lane) === 1) || null;
  const lane2 = racers.find((row) => Number(row?.lane) === 2) || null;
  const lane3 = racers.find((row) => Number(row?.lane) === 3) || null;
  const lane4 = racers.find((row) => Number(row?.lane) === 4) || null;
  const lane5 = racers.find((row) => Number(row?.lane) === 5) || null;
  const lane6 = racers.find((row) => Number(row?.lane) === 6) || null;
  const startProfiles = Array.isArray(source?.playerStartProfile?.profiles) ? source.playerStartProfile.profiles : [];
  const startProfileMap = new Map(startProfiles.map((row) => [Number(row?.lane), row]));
  const profile2 = startProfileMap.get(2) || {};
  const profile3 = startProfileMap.get(3) || {};
  const profile4 = startProfileMap.get(4) || {};
  const profile5 = startProfileMap.get(5) || {};
  const profile6 = startProfileMap.get(6) || {};
  const venueBias = getVenueInsideBias(source?.race?.venueName || venueNameFallback);
  const win1 = getRacerWinStrength(boat1 || {});
  const motor1 = getRacerMotorStrength(boat1 || {});
  const boatStrength1 = getRacerBoatStrength(boat1 || {});
  const start1 = getRacerStartStrength(boat1 || {});
  const risk1 = getRacerRiskPenalty(boat1 || {});
  const entryStable = source?.entry_changed ? 0.42 : source?.entry_validation?.validation_ok === false ? 0.55 : 1;
  const movementRisk = source?.entry_changed ? 0.18 : source?.entry_fallback_used ? 0.1 : 0;
  const underlyingFits = [
    { lane: 2, score: lane2 ? getUnderlyingBoatFit(lane2, 2, profile2) : null },
    { lane: 3, score: lane3 ? getUnderlyingBoatFit(lane3, 3, profile3) : null },
    { lane: 4, score: lane4 ? getUnderlyingBoatFit(lane4, 4, profile4) : null }
  ];
  const availableUnderlying = underlyingFits.filter((row) => Number.isFinite(row.score));
  const box234AverageStrength = Number(
    availableUnderlying.reduce((sum, row) => sum + (Number(row.score) || 0), 0) /
    Math.max(1, availableUnderlying.length) /
    100
  );
  const secondWeightMap = new Map([
    [2, clampNumber(0, 1, (toFiniteOrNull(profile2?.sashi_style_score) ?? 60) / 100 * 0.55 + (toFiniteOrNull(profile2?.start_stability_score) ?? 50) / 100 * 0.25 + (getRacerStartStrength(lane2 || {}).value ?? 0.45) * 0.2)],
    [3, clampNumber(0, 1, (toFiniteOrNull(profile3?.makuri_sashi_style_score ?? profile3?.makurizashi_style_score) ?? 55) / 100 * 0.45 + (toFiniteOrNull(profile3?.start_stability_score) ?? 50) / 100 * 0.2 + (getRacerStartStrength(lane3 || {}).value ?? 0.45) * 0.2)],
    [4, clampNumber(0, 1, (toFiniteOrNull(profile4?.makuri_style_score) ?? 50) / 100 * 0.22 + (toFiniteOrNull(profile4?.start_stability_score) ?? 50) / 100 * 0.24 + (getRacerStartStrength(lane4 || {}).value ?? 0.45) * 0.18 + (toFiniteOrNull(lane4?.lane3renScore ?? lane4?.lane3renAvg ?? lane4?.lane3RenRate) ?? 40) / 100 * 0.2)]
  ]);
  const thirdWeightMap = new Map([
    [2, clampNumber(0, 1, (toFiniteOrNull(profile2?.start_stability_score) ?? 50) / 100 * 0.34 + (toFiniteOrNull(lane2?.lane3renScore ?? lane2?.lane3renAvg ?? lane2?.lane3RenRate) ?? 40) / 100 * 0.36 + (getRacerBoatStrength(lane2 || {}).value ?? 0.44) * 0.16)],
    [3, clampNumber(0, 1, (toFiniteOrNull(profile3?.makuri_sashi_style_score ?? profile3?.makurizashi_style_score) ?? 55) / 100 * 0.24 + (toFiniteOrNull(profile3?.start_stability_score) ?? 50) / 100 * 0.24 + (toFiniteOrNull(lane3?.lane3renScore ?? lane3?.lane3renAvg ?? lane3?.lane3RenRate) ?? 45) / 100 * 0.34)],
    [4, clampNumber(0, 1, (toFiniteOrNull(profile4?.start_stability_score) ?? 50) / 100 * 0.2 + (toFiniteOrNull(lane4?.lane3renScore ?? lane4?.lane3renAvg ?? lane4?.lane3RenRate) ?? 45) / 100 * 0.4 + (getRacerBoatStrength(lane4 || {}).value ?? 0.44) * 0.18)]
  ]);

  const coreFieldsPresent = [
    { label: "boat1 win strength", ready: !!win1.hasCore },
    { label: "boat1 motor2ren", ready: !!motor1.hasCore },
    { label: "boat1 avgSt", ready: !!start1.hasCore },
    { label: "lane2 opponent", ready: Number.isFinite(underlyingFits[0]?.score) },
    { label: "lane3 opponent", ready: Number.isFinite(underlyingFits[1]?.score) },
    { label: "lane4 opponent", ready: Number.isFinite(underlyingFits[2]?.score) },
    { label: "venue", ready: !!(source?.race?.venueName || venueNameFallback) }
  ];
  const coreFieldsMissing = coreFieldsPresent.filter((row) => !row.ready).map((row) => row.label);
  const coreFieldsReady = coreFieldsMissing.length <= 1 && !!boat1;
  const optionalMissing = [];
  if (startProfiles.length === 0) optionalMissing.push("player start profiles");
  if (!kyoteiFetch?.kyoteibiyori_fetch_success) optionalMissing.push("biyori style tendency data");

  const boat1AnchorScore = coreFieldsReady
    ? Number(
        clampNumber(
          0,
          100,
          (
            win1.value * 31 +
            (motor1.value ?? 0.46) * 22 +
            (boatStrength1.value ?? 0.46) * 10 +
            (start1.value ?? 0.45) * 14 +
            venueBias * 15 +
            entryStable * 11 -
            risk1 * 28 -
            movementRisk * 18
          ).toFixed(1)
        )
      )
    : null;

  const underlyingSorted = [...availableUnderlying].sort((a, b) => (b.score || 0) - (a.score || 0));
  const lane2SecondSupport = clampNumber(0, 1, ((underlyingFits[0]?.score || 0) / 100) * 0.46 + (secondWeightMap.get(2) || 0) * 0.34 + venueBias * 0.08 + entryStable * 0.06);
  const lane3SecondSupport = clampNumber(0, 1, ((underlyingFits[1]?.score || 0) / 100) * 0.38 + (secondWeightMap.get(3) || 0) * 0.34 + (thirdWeightMap.get(3) || 0) * 0.08 + venueBias * 0.06);
  const lane4SecondSupport = clampNumber(0, 1, ((underlyingFits[2]?.score || 0) / 100) * 0.34 + (secondWeightMap.get(4) || 0) * 0.32 + (thirdWeightMap.get(4) || 0) * 0.1 + venueBias * 0.04);
  const lane2ThirdSupport = clampNumber(0, 1, ((underlyingFits[0]?.score || 0) / 100) * 0.28 + (thirdWeightMap.get(2) || 0) * 0.4 + (secondWeightMap.get(2) || 0) * 0.08 + entryStable * 0.06);
  const lane3ThirdSupport = clampNumber(0, 1, ((underlyingFits[1]?.score || 0) / 100) * 0.34 + (thirdWeightMap.get(3) || 0) * 0.42 + (secondWeightMap.get(3) || 0) * 0.08 + entryStable * 0.04);
  const lane4ThirdSupport = clampNumber(0, 1, ((underlyingFits[2]?.score || 0) / 100) * 0.36 + (thirdWeightMap.get(4) || 0) * 0.42 + (secondWeightMap.get(4) || 0) * 0.06 + entryStable * 0.04);
  const box234FitScore = coreFieldsReady
    ? Number(
        clampNumber(
          0,
          100,
          (
            lane2SecondSupport * 26 +
            lane3SecondSupport * 16 +
            lane4SecondSupport * 12 +
            lane2ThirdSupport * 8 +
            lane3ThirdSupport * 18 +
            lane4ThirdSupport * 16 +
            box234AverageStrength * 10 +
            venueBias * 7 +
            entryStable * 5 +
            (toFiniteOrNull(profile2?.start_stability_score) ?? 50) * 0.05 +
            (toFiniteOrNull(profile3?.start_stability_score) ?? 50) * 0.04 +
            (toFiniteOrNull(profile4?.start_stability_score) ?? 50) * 0.04
          ).toFixed(1)
        )
      )
    : null;

  const outsideHeuristicBase = [
    { lane: 5, racer: lane5 },
    { lane: 6, racer: lane6 }
  ].map(({ lane, racer }) => {
    const profile = lane === 5 ? profile5 : profile6;
    const underneath = racer ? getUnderlyingBoatFit(racer, lane, profile) : null;
    const motor = getRacerMotorStrength(racer || {});
    const boat = getRacerBoatStrength(racer || {});
    const start = getRacerStartStrength(racer || {});
    const makuri = clampNumber(0, 1, (toFiniteOrNull(profile?.makuri_style_score) ?? 50) / 100);
    const breakout = clampNumber(0, 1, (toFiniteOrNull(profile?.breakout_rate) ?? 50) / 100);
    return {
      lane,
      head: clampNumber(0, 1, ((underneath || 0) / 100) * 0.34 + (motor.value ?? 0.4) * 0.24 + (boat.value ?? 0.4) * 0.14 + (start.value ?? 0.4) * 0.12 + makuri * 0.08 + breakout * 0.08),
      second: clampNumber(0, 1, ((underneath || 0) / 100) * 0.44 + (motor.value ?? 0.4) * 0.14 + (boat.value ?? 0.4) * 0.12 + makuri * 0.06 + breakout * 0.08),
      third: clampNumber(0, 1, ((underneath || 0) / 100) * 0.56 + (motor.value ?? 0.4) * 0.08 + (boat.value ?? 0.4) * 0.08 + breakout * 0.06)
    };
  });
  const outsideHeadRiskProb = Number(clampNumber(0, 0.42, outsideHeuristicBase.reduce((sum, row) => sum + row.head, 0) * 0.18).toFixed(4));
  const outsideSecondRiskProb = Number(clampNumber(0, 0.38, outsideHeuristicBase.reduce((sum, row) => sum + row.second, 0) * 0.16).toFixed(4));
  const outsideThirdRiskProb = Number(clampNumber(0, 0.32, outsideHeuristicBase.reduce((sum, row) => sum + row.third, 0) * 0.14).toFixed(4));
  const outsideBreakRisk = coreFieldsReady
    ? Number(clampNumber(0, 100, ((outsideHeadRiskProb * 0.56) + (outsideSecondRiskProb * 0.29) + (outsideThirdRiskProb * 0.15)) * 100).toFixed(1))
    : null;
  const makuriRisk = coreFieldsReady
    ? Number(
        clampNumber(
          0,
          100,
          (
            (clampNumber(0, 1, (toFiniteOrNull(profile2?.sashi_style_score) ?? 50) / 100) * 0.26) +
            (clampNumber(0, 1, (toFiniteOrNull(profile3?.makuri_style_score) ?? 50) / 100) * 0.28) +
            (clampNumber(0, 1, (toFiniteOrNull(profile4?.makuri_style_score) ?? 50) / 100) * 0.24) +
            ((getRacerStartStrength(lane3 || {}).value ?? 0.45) * 0.1) +
            ((getRacerStartStrength(lane4 || {}).value ?? 0.45) * 0.08) +
            (clampNumber(0, 1, (toFiniteOrNull(profile3?.breakout_rate) ?? 50) / 100) * 0.08) +
            (clampNumber(0, 1, (toFiniteOrNull(profile4?.breakout_rate) ?? 50) / 100) * 0.06) -
            (clampNumber(0, 1, (toFiniteOrNull(profile3?.delay_rate) ?? 50) / 100) * 0.06) -
            (clampNumber(0, 1, (toFiniteOrNull(profile4?.delay_rate) ?? 50) / 100) * 0.05) +
            (1 - venueBias) * 0.08
          ) * 100
        ).toFixed(1)
      )
    : null;
  const boat1EscapeTrust = boat1AnchorScore;

  const fixed1234Probability = coreFieldsReady
    ? Number(
        clampNumber(
          0.08,
          0.86,
          (
            (boat1EscapeTrust / 100) * 0.36 +
            (box234FitScore / 100) * 0.32 +
            venueBias * 0.1 +
            entryStable * 0.1 -
            (makuriRisk / 100) * 0.1 -
            (outsideHeadRiskProb * 0.09) -
            (outsideSecondRiskProb * 0.07) -
            (outsideThirdRiskProb * 0.03) -
            risk1 * 0.04
          ).toFixed(4)
        )
      )
    : null;

  const fixed1234MatrixData = coreFieldsReady
    ? buildFixed1234Matrix({
        fixed1234TotalProbability: fixed1234Probability,
        lane2Score: underlyingFits[0]?.score,
        lane3Score: underlyingFits[1]?.score,
        lane4Score: underlyingFits[2]?.score
      })
    : {
        matrix: {},
        total: null,
        top4: [],
        top4Total: null,
        concentrationRatio: null
      };

  const top4Fixed1234Probability = fixed1234MatrixData.top4Total;
  const optionalDataPenalty = Number(
    clampNumber(
      0,
      HARD_RACE_DECISION_THRESHOLDS.optional_penalty_cap,
      (
        (startProfiles.length === 0 ? 0.01 : 0) +
        (!kyoteiFetch?.kyoteibiyori_fetch_success ? 0.006 : 0)
      ).toFixed(4)
    )
  );
  const riskAdjustment = Number(
    clampNumber(
      0,
      0.1,
      (
        movementRisk * 0.15 +
        Math.max(0, (outsideBreakRisk ?? 0) - 48) / 100 * 0.08 +
        Math.max(0, risk1 - 0.11) * 0.08
      ).toFixed(4)
    )
  );
  const adjustedFixed1234TotalProbability = fixed1234Probability === null
    ? null
    : Number(Math.max(0, fixed1234Probability - riskAdjustment - optionalDataPenalty).toFixed(4));
  const fixed1234ShapeConcentration = coreFieldsReady
    ? Number(
        clampNumber(
          0,
          100,
          (
            (adjustedFixed1234TotalProbability ?? 0) * 100 * 0.42 +
            ((top4Fixed1234Probability ?? 0) * 100) * 0.33 +
            ((fixed1234MatrixData.concentrationRatio ?? 0) * 100) * 0.19 -
            (makuriRisk || 0) * 0.08 -
            (outsideHeadRiskProb * 100) * 0.1 -
            (outsideSecondRiskProb * 100) * 0.08
          ).toFixed(1)
        )
      )
    : null;
  const shapeFocusScore = coreFieldsReady
    ? Number(
        clampNumber(
          0,
          1,
          (
            (fixed1234ShapeConcentration ?? 0) * 0.7 +
            ((fixed1234MatrixData.concentrationRatio ?? 0) * 0.15) +
            (Math.min(1, (top4Fixed1234Probability ?? 0) / Math.max(adjustedFixed1234TotalProbability ?? 0.01, 0.01)) * 0.15)
          ).toFixed(4)
        )
      )
    : null;
  const opponent234Fit = box234FitScore;

  const hardRaceScore = coreFieldsReady
    ? Number(
        clampNumber(
          0,
          100,
          (
            boat1EscapeTrust * 0.28 +
            opponent234Fit * 0.36 +
            (fixed1234ShapeConcentration || 0) * 0.18 -
            (outsideBreakRisk || 0) * 0.14 -
            (makuriRisk || 0) * 0.04
          ).toFixed(1)
        )
      )
    : null;

  const shapeBase =
    underlyingSorted[0]?.lane === 4
      ? "1-24-234"
      : underlyingSorted[0]?.lane === 3 && (underlyingSorted[0]?.score || 0) - (underlyingSorted[1]?.score || 0) >= 4
        ? "1-34-234"
        : "1-23-234";
  const suggestedShape = coreFieldsReady && box234FitScore >= 44 ? shapeBase : null;
  const conservativeComposite = hardRaceScore;

  const dominantConservativePattern =
    Array.isArray(fixed1234MatrixData.top4) && fixed1234MatrixData.top4.length > 0
      ? ["1-2-3", "1-3-2", "1-2-4", "1-3-4"].includes(fixed1234MatrixData.top4[0]?.combo)
      : false;
  let skipReason = null;
  if (coreFieldsReady) {
    if ((boat1EscapeTrust ?? 0) < HARD_RACE_DECISION_THRESHOLDS.borderline_escape_trust) {
      skipReason = "boat1 escape trust too weak";
    } else if ((outsideBreakRisk ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.borderline_outside_break_max) {
      skipReason = "outside break risk too high";
    } else if ((opponent234Fit ?? 0) < HARD_RACE_DECISION_THRESHOLDS.borderline_opponent_fit) {
      skipReason = "2/3/4 underneath fit too weak";
    }
  }
  const oldDecision = !coreFieldsReady
    ? "UNAVAILABLE"
    : adjustedFixed1234TotalProbability >= 0.75
      ? "BUY-4"
      : adjustedFixed1234TotalProbability >= 0.6
        ? "BUY-6"
        : adjustedFixed1234TotalProbability >= 0.5
          ? "BORDERLINE"
          : "SKIP";

  const buyStyleRecommendation = !coreFieldsReady
    ? "DATA_ERROR"
    : skipReason === "boat1 escape trust too weak"
      ? "SKIP"
      : (boat1EscapeTrust ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy4_escape_trust &&
          (opponent234Fit ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy4_opponent_fit &&
          (outsideBreakRisk ?? 100) <= HARD_RACE_DECISION_THRESHOLDS.buy4_outside_break_max &&
          (adjustedFixed1234TotalProbability ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy4_total &&
          (adjustedFixed1234TotalProbability ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy4_box_hit &&
          (top4Fixed1234Probability ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy4_top4 &&
          (shapeFocusScore ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy4_shape_focus &&
          (fixed1234ShapeConcentration ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy4_shape_concentration
        ? "BUY-4"
        : (boat1EscapeTrust ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy6_escape_trust &&
            (opponent234Fit ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy6_opponent_fit &&
            (outsideBreakRisk ?? 100) <= HARD_RACE_DECISION_THRESHOLDS.buy6_outside_break_max &&
            (adjustedFixed1234TotalProbability ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy6_total &&
            (adjustedFixed1234TotalProbability ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.buy6_box_hit
          ? "BUY-6"
          : (boat1EscapeTrust ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.borderline_escape_trust &&
              (outsideBreakRisk ?? 100) <= HARD_RACE_DECISION_THRESHOLDS.borderline_outside_break_max &&
              (opponent234Fit ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.borderline_opponent_fit &&
              (adjustedFixed1234TotalProbability ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.borderline_total &&
              (adjustedFixed1234TotalProbability ?? 0) >= HARD_RACE_DECISION_THRESHOLDS.borderline_box_hit
            ? "BORDERLINE"
            : "SKIP";

  const finalStatus = !coreFieldsReady
    ? "DATA_ERROR"
    : buyStyleRecommendation === "BUY-6" || buyStyleRecommendation === "BUY-4"
      ? "BUY"
      : buyStyleRecommendation === "BORDERLINE"
        ? "BORDERLINE"
        : "SKIP";
  const hardRaceRank = !coreFieldsReady
    ? "DATA_ERROR"
    : (boat1EscapeTrust ?? 0) >= HARD_RACE_RANK_THRESHOLDS.a_anchor &&
        (adjustedFixed1234TotalProbability ?? 0) >= HARD_RACE_RANK_THRESHOLDS.a_total &&
        (top4Fixed1234Probability ?? 0) >= HARD_RACE_RANK_THRESHOLDS.a_top4 &&
        (opponent234Fit ?? 0) >= HARD_RACE_RANK_THRESHOLDS.a_box &&
        (outsideBreakRisk ?? 100) <= HARD_RACE_RANK_THRESHOLDS.a_outside_risk_max &&
        (fixed1234ShapeConcentration ?? 0) >= HARD_RACE_RANK_THRESHOLDS.a_concentration
      ? "A"
      : (boat1EscapeTrust ?? 0) >= HARD_RACE_RANK_THRESHOLDS.b_anchor &&
          (adjustedFixed1234TotalProbability ?? 0) >= HARD_RACE_RANK_THRESHOLDS.b_total &&
          (top4Fixed1234Probability ?? 0) >= HARD_RACE_RANK_THRESHOLDS.b_top4 &&
          (opponent234Fit ?? 0) >= HARD_RACE_RANK_THRESHOLDS.b_box &&
          (outsideBreakRisk ?? 100) <= HARD_RACE_RANK_THRESHOLDS.b_outside_risk_max &&
          (fixed1234ShapeConcentration ?? 0) >= HARD_RACE_RANK_THRESHOLDS.b_concentration
        ? "B"
        : "SKIP";

  const positiveReasons = [];
  if (hardRaceRank === "A") positiveReasons.push("best 4-point tier");
  if (hardRaceRank === "B") positiveReasons.push("acceptable 6-point tier");
  if (venueBias >= 0.7) positiveReasons.push("strong inside venue");
  if (boat1EscapeTrust !== null && boat1EscapeTrust >= 68) positiveReasons.push("strong boat1 ST profile");
  if ((makuriRisk ?? 100) <= 38) positiveReasons.push("low 3/4 makuri pressure");
  if (!source?.entry_changed) positiveReasons.push("stable entry");
  if (underlyingSorted[0]?.lane === 2) positiveReasons.push("lane2 underneath support");
  if (underlyingSorted[0]?.lane === 3) positiveReasons.push("lane3 underneath support");
  if (underlyingSorted[0]?.lane === 4) positiveReasons.push("lane4 underneath support");
  const negativeReasons = [];
  if (risk1 >= 0.12) negativeReasons.push("boat1 F/L risk");
  if (source?.entry_changed) negativeReasons.push("course movement risk");
  if ((makuriRisk ?? 0) >= 62) negativeReasons.push("high 3/4 makuri pressure");
  if ((outsideBreakRisk ?? 0) >= 54) negativeReasons.push("outside 5/6 break risk");
  if (skipReason) negativeReasons.push(skipReason);
  if (optionalMissing.length > 0) negativeReasons.push("optional data missing only");
  const topReasons = coreFieldsReady
    ? [...positiveReasons, ...negativeReasons].slice(0, 4)
    : coreFieldsMissing.slice(0, 4);
  const hrefExtractionSuccess =
    kyoteiFetch?.request_diagnostics?.race_list_fetch_success === true ||
    kyoteiFetch?.request_diagnostics?.race_list_match_found === true ||
    Array.isArray(kyoteiFetch?.tried_urls) && kyoteiFetch.tried_urls.length > 0;
  const parseSuccess =
    officialFetch?.racelist === "success" &&
    racers.length >= 6;
  const screeningDebug = {
    race_fetch_success: officialFetch?.racelist === "success",
    kyoteibiyori_fetch_success: kyoteiFetch?.kyoteibiyori_fetch_success === true,
    href_extraction_success: !!hrefExtractionSuccess,
    parse_success: !!parseSuccess,
    core_fields_ready: coreFieldsReady,
    hard_race_score_ready: hardRaceScore !== null,
    final_status: finalStatus,
    buy_style_recommendation: buyStyleRecommendation,
    hard_race_rank: hardRaceRank,
    old_decision: oldDecision,
    top4_fixed1234_probability: top4Fixed1234Probability,
    fixed1234_shape_concentration: fixed1234ShapeConcentration,
    boat1_escape_trust: boat1EscapeTrust,
    opponent_234_fit: opponent234Fit,
    makuri_risk: makuriRisk,
    outside_break_risk: outsideBreakRisk,
    outside_head_risk: outsideHeadRiskProb,
    outside_second_risk: outsideSecondRiskProb,
    outside_third_risk: outsideThirdRiskProb,
    skip_reason: skipReason,
    optional_data_penalty: optionalDataPenalty,
    boat1_anchor_contribution: boat1EscapeTrust,
    box_234_fit_contribution: opponent234Fit,
    fixed1234_total_contribution: adjustedFixed1234TotalProbability,
    top4_fixed1234_contribution: top4Fixed1234Probability,
    decision_reason:
      skipReason ||
      (buyStyleRecommendation === "BUY-4"
        ? "strong 1-anchor with concentrated 1-234-234 top-4 shapes"
        : buyStyleRecommendation === "BUY-6"
          ? "boat1 escape trust and 2/3/4 underneath group fit the six-ticket structure"
          : buyStyleRecommendation === "BORDERLINE"
            ? "boat1 escape trust is usable and 2/3/4 fit remains acceptable"
            : optionalMissing.length > 0
              ? "optional data missing only"
              : "boat1 escape or 2/3/4 six-ticket fit is too weak"),
    positive_reasons: positiveReasons,
    negative_reasons: negativeReasons,
    attempt_count: Number(entry?.attemptCount || 1),
    core_fields_present: coreFieldsPresent.filter((row) => row.ready).map((row) => row.label),
    core_fields_missing: coreFieldsMissing,
    optional_fields_missing: optionalMissing,
    why_unavailable: finalStatus === "DATA_ERROR" ? coreFieldsMissing : [],
    fixed1234_matrix: fixed1234MatrixData.matrix,
    fixed1234_total_probability: fixed1234MatrixData.total,
    fixed1234_top4_total: fixed1234MatrixData.top4Total,
    fixed1234_concentration_ratio: fixed1234MatrixData.concentrationRatio,
    raw_inputs: {
      race_no: Number(source?.race?.raceNo ?? entry?.raceNo ?? null),
      venue_name: source?.race?.venueName || venueNameFallback,
      entry_changed: !!source?.entry_changed,
      official_fetch_status: officialFetch?.racelist || null,
      kyoteibiyori_fetch_ok: !!kyoteiFetch?.kyoteibiyori_fetch_success
    },
    normalized_inputs: {
      venue_bias: venueBias,
      entry_stable: entryStable,
      movement_risk: movementRisk,
      risk1,
      box234_average_strength: box234AverageStrength,
      core_fields_present: coreFieldsPresent.filter((row) => row.ready).map((row) => row.label),
      optional_fields_missing: optionalMissing
    },
    features: {
      lane2_second_support: lane2SecondSupport,
      lane3_second_support: lane3SecondSupport,
      lane4_second_support: lane4SecondSupport,
      lane2_third_support: lane2ThirdSupport,
      lane3_third_support: lane3ThirdSupport,
      lane4_third_support: lane4ThirdSupport,
      outside_head_risk_prob: outsideHeadRiskProb,
      outside_second_risk_prob: outsideSecondRiskProb,
      outside_third_risk_prob: outsideThirdRiskProb
    },
    scores: {
      hard_race_score: hardRaceScore,
      boat1_escape_trust: boat1EscapeTrust,
      opponent_234_fit: opponent234Fit,
      makuri_risk: makuriRisk,
      outside_break_risk: outsideBreakRisk,
      fixed1234_total_probability: fixed1234MatrixData.total,
      top4_fixed1234_probability: top4Fixed1234Probability,
      fixed1234_shape_concentration: fixed1234ShapeConcentration
    },
    official_fetch_status: officialFetch,
    kyoteibiyori_status: {
      ok: !!kyoteiFetch?.ok,
      fallback_used: !!kyoteiFetch?.fallback_used,
      fallback_reason: kyoteiFetch?.fallback_reason || kyoteiFetch?.kyoteibiyori_error_reason || null,
      tried_urls: Array.isArray(kyoteiFetch?.tried_urls) ? kyoteiFetch.tried_urls : [],
      populated_fields: Array.isArray(kyoteiFetch?.field_diagnostics?.populated_fields) ? kyoteiFetch.field_diagnostics.populated_fields : [],
      failed_fields: Array.isArray(kyoteiFetch?.field_diagnostics?.failed_fields) ? kyoteiFetch.field_diagnostics.failed_fields : []
    }
  };

  return finalizeHardRaceContractRow({
    raceNo: Number(source?.race?.raceNo ?? entry?.raceNo ?? null),
    venueName: source?.race?.venueName || venueNameFallback,
    status: finalStatus,
    data_status: finalStatus === "DATA_ERROR" ? "DATA_ERROR" : optionalMissing.length > 0 ? "PARTIAL" : "OK",
    hardRaceScore,
    boat1AnchorScore: boat1EscapeTrust,
    boat1EscapeTrust,
    box234FitScore: opponent234Fit,
    opponent234Fit,
    makuriRisk,
    fixed1234Probability,
    fixed1234Matrix: fixed1234MatrixData.matrix,
    fixed1234TotalProbability: fixed1234MatrixData.total,
    top4Fixed1234Probability,
    fixed1234Top4: fixed1234MatrixData.top4,
    fixed1234Top4Total: fixed1234MatrixData.top4Total,
    fixed1234ShapeConcentration,
    outsideBreakRisk,
    adjustedFixed1234TotalProbability,
    buyStyleRecommendation,
    recommendation: buyStyleRecommendation,
    decision: buyStyleRecommendation,
    decision_reason: screeningDebug.decision_reason,
    hardRaceRank,
    skipReason,
    finalStatus,
    buyRecommendation: finalStatus,
    suggestedShape,
    positiveReasons,
    negativeReasons,
    topReasons,
    fixedShapeCandidates: [
      { shape: "1-23-234", probability: suggestedShape === "1-23-234" && fixed1234Probability !== null ? fixed1234Probability : Math.max(0, (fixed1234Probability || 0) - 0.015) },
      { shape: "1-24-234", probability: suggestedShape === "1-24-234" && fixed1234Probability !== null ? fixed1234Probability : Math.max(0, (fixed1234Probability || 0) - 0.02) },
      { shape: "1-34-234", probability: suggestedShape === "1-34-234" && fixed1234Probability !== null ? fixed1234Probability : Math.max(0, (fixed1234Probability || 0) - 0.02) }
    ],
    orderRowCount: 0,
    conservativeComposite: conservativeComposite === null ? null : Number(conservativeComposite.toFixed(1)),
    expandableReason:
      finalStatus === "UNAVAILABLE"
        ? "Screening data was not complete enough to score this race reliably."
        : suggestedShape === null
          ? "Boat 1 trust or 2/3/4 underneath fit was not strong enough for conservative fixed-head screening."
          : `Selected ${suggestedShape} because boat 1 anchor trust and the 2/3/4 underneath structure were the strongest conservative fit.`,
    sourceData: source,
    fetchFailed: false,
    errors: [],
    missing_fields: [
      hardRaceScore === null ? "hard_race_score" : null,
      boat1EscapeTrust === null ? "boat1_anchor_score" : null,
      boat1EscapeTrust === null ? "boat1_escape_trust" : null,
      box234FitScore === null ? "box_234_fit_score" : null,
      opponent234Fit === null ? "opponent_234_fit" : null,
      makuriRisk === null ? "makuri_risk" : null,
      outsideBreakRisk === null ? "outside_break_risk" : null,
      fixed1234MatrixData.total === null ? "fixed1234_total_probability" : null,
      fixed1234MatrixData.top4Total === null ? "top4_fixed1234_probability" : null,
      fixed1234ShapeConcentration === null ? "fixed1234_shape_concentration" : null,
      suggestedShape === null ? "suggested_shape" : null
    ].filter(Boolean),
    screeningDebug
  });
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
  if (tagSet.has("boat1_inside_partner_underweighted")) compact.push("1-234 UNDER");
  if (tagSet.has("boat1_inside_partner_overweighted")) compact.push("1-234 OVER");
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

function getPredictionQualityLabels(participationDecision, prediction) {
  const scoreSource =
    participationDecision?.participation_score_components && typeof participationDecision.participation_score_components === "object"
      ? participationDecision.participation_score_components
      : prediction?.participation_score_components && typeof prediction.participation_score_components === "object"
        ? prediction.participation_score_components
        : {};
  const labels = [];
  if (Number(scoreSource?.data_quality_score) >= 72) labels.push("High Quality");
  if (Number(scoreSource?.race_stability_score) >= 68) labels.push("Stable");
  if (Number(scoreSource?.partner_clarity_score) >= 64) labels.push("Partner Clear");
  if (Number(scoreSource?.quality_gate_applied) === 1) labels.push("Quality Gate");
  return labels;
}

function getScoreBand(value, thresholds = { high: 72, medium: 55 }) {
  const n = Number(value);
  if (!Number.isFinite(n)) return { label: "Unknown", tone: "muted", meaning: "insufficient data" };
  if (n >= thresholds.high) return { label: "High", tone: "good", meaning: "stable / readable" };
  if (n >= thresholds.medium) return { label: "Medium", tone: "mid", meaning: "usable with caution" };
  return { label: "Low", tone: "bad", meaning: "messy / fragile" };
}

function topLaneFromList(list = [], fallback = null) {
  const lane = Array.isArray(list) ? Number(list[0]) : NaN;
  return Number.isFinite(lane) ? lane : fallback;
}

function compactTicketRows(rows = [], limit = 6) {
  return (Array.isArray(rows) ? rows : []).slice(0, limit);
}

function getCanonicalEntryDebug(data, startDisplayOverride = null) {
  const startDisplay =
    startDisplayOverride && typeof startDisplayOverride === "object"
      ? startDisplayOverride
      : data?.startDisplay && typeof data.startDisplay === "object"
        ? data.startDisplay
        : {};
  const startMeta =
    startDisplay?.start_display_entry_meta && typeof startDisplay.start_display_entry_meta === "object"
      ? startDisplay.start_display_entry_meta
      : {};
  const validation =
    startMeta?.validation && typeof startMeta.validation === "object"
      ? startMeta.validation
      : data?.entry_validation && typeof data.entry_validation === "object"
        ? data.entry_validation
        : {};
  const actualEntryOrder = Array.isArray(startMeta?.actual_entry_order)
    ? startMeta.actual_entry_order
    : Array.isArray(data?.actual_entry_order)
      ? data.actual_entry_order
      : [];
  const actualLaneMap =
    startMeta?.actual_lane_map && typeof startMeta.actual_lane_map === "object"
      ? startMeta.actual_lane_map
      : data?.actual_lane_map && typeof data.actual_lane_map === "object"
        ? data.actual_lane_map
        : {};
  const perBoatLaneMap =
    startMeta?.per_boat_lane_map && typeof startMeta.per_boat_lane_map === "object"
      ? startMeta.per_boat_lane_map
      : data?.per_boat_lane_map && typeof data.per_boat_lane_map === "object"
        ? data.per_boat_lane_map
        : {};
  const fallbackUsed = startMeta?.fallback_used === true || data?.entry_fallback_used === true;
  return {
    authoritativeSource: startMeta?.authoritative_source || data?.actual_entry_authoritative_source || null,
    rawActualEntrySourceText: startMeta?.raw_actual_entry_source_text || data?.raw_actual_entry_source_text || null,
    actualEntryOrder,
    actualLaneMap,
    perBoatLaneMap,
    validation,
    validationPassed: validation?.validation_ok === true,
    fallbackUsed,
    fallbackReason: startMeta?.fallback_reason || data?.entry_fallback_reason || null,
    confirmedActualEntry: validation?.validation_ok === true && fallbackUsed !== true
  };
}

function resolveCanonicalActualLane({ lane, entryDebug = {} }) {
  const baseLane = Number(lane);
  if (!Number.isInteger(baseLane) || baseLane < 1 || baseLane > 6) {
    return {
      actualLane: null,
      courseChanged: false,
      actualLaneConfirmed: false
    };
  }

  const perBoat = entryDebug?.perBoatLaneMap?.[String(baseLane)] || null;
  const mappedLane = Number(perBoat?.actual_lane ?? entryDebug?.actualLaneMap?.[String(baseLane)]);
  const actualLane = entryDebug?.confirmedActualEntry
    ? (Number.isInteger(mappedLane) && mappedLane >= 1 && mappedLane <= 6 ? mappedLane : baseLane)
    : baseLane;

  return {
    actualLane,
    courseChanged: entryDebug?.confirmedActualEntry ? actualLane !== baseLane : false,
    actualLaneConfirmed: entryDebug?.confirmedActualEntry
  };
}

function normalizeProbValue(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n > 1.0001) return n / 100;
  return n;
}

function normalizeDistributionRowsForUi(rows = []) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      lane: Number(row?.lane ?? row?.boat ?? row?.course),
      weight: normalizeProbValue(row?.weight ?? row?.score ?? row?.probability)
    }))
    .filter((row) => Number.isFinite(row.lane) && row.lane >= 1 && row.lane <= 6 && row.weight > 0)
    .sort((a, b) => b.weight - a.weight);
}

function topLaneFromDistributionRows(rows = []) {
  return normalizeDistributionRowsForUi(rows)[0]?.lane ?? null;
}

function distributionSpreadScore(rows = [], take = 3) {
  const normalized = normalizeDistributionRowsForUi(rows);
  if (!normalized.length) return 0;
  const top = normalized[0]?.weight || 0;
  const others = normalized.slice(1, take).reduce((acc, row) => acc + row.weight, 0);
  return Number((Math.max(0, top - others) * 100).toFixed(2));
}

function computeUpsetRiskScore({
  confidenceScores,
  participationDecision,
  prediction,
  roleCandidates,
  raceStructure,
  attackScenarioLabel,
  boat1EscapeProbability,
  evidenceBoatSummaryRows,
  formationPatternLabel
}) {
  const scoreSource =
    participationDecision?.participation_score_components && typeof participationDecision.participation_score_components === "object"
      ? participationDecision.participation_score_components
      : prediction?.participation_score_components && typeof prediction.participation_score_components === "object"
        ? prediction.participation_score_components
        : {};
  const headConfidence = Number(confidenceScores?.head_fixed_confidence_pct ?? confidenceScores?.head_confidence_calibrated ?? 0);
  const recommendationConfidence = Number(confidenceScores?.recommended_bet_confidence_pct ?? confidenceScores?.bet_confidence_calibrated ?? 0);
  const chaosRisk = Math.max(
    Number(raceStructure?.chaos_risk_score ?? 0),
    Math.max(0, 100 - Number(scoreSource?.race_stability_score ?? 0))
  );
  const partnerNoise = Math.max(0, 100 - Number(scoreSource?.partner_clarity_score ?? 0));
  const qualityPenalty = Number(scoreSource?.quality_gate_applied ?? 0) === 1 ? 12 : 0;
  const attackRows = normalizeDistributionRowsForUi(roleCandidates?.attack_scenario_probabilities || []);
  const attackPressure = Math.min(24, (attackRows[0]?.weight || 0) * 40);
  const firstRows = normalizeDistributionRowsForUi(
    roleCandidates?.first_place_candidates ||
    prediction?.confirmed_first_place_probability_json ||
    prediction?.first_place_probability_json
  );
  const secondRows = normalizeDistributionRowsForUi(
    roleCandidates?.second_place_candidates ||
    prediction?.confirmed_second_place_probability_json ||
    prediction?.second_place_probability_json
  );
  const thirdRows = normalizeDistributionRowsForUi(
    roleCandidates?.third_place_candidates ||
    prediction?.confirmed_third_place_probability_json ||
    prediction?.third_place_probability_json
  );
  const outsideGate = roleCandidates?.outside_head_promotion_gate || prediction?.outside_head_promotion_gate_json || {};
  const gateByLane = outsideGate?.by_lane && typeof outsideGate.by_lane === "object" ? outsideGate.by_lane : {};
  const outsideEvidence = [5, 6].reduce((acc, lane) => {
    const row = gateByLane?.[lane] || gateByLane?.[String(lane)] || {};
    return Math.max(acc, Number(row?.matched_evidence_categories_count ?? 0));
  }, 0);
  const innerCollapseScore = Number(
    outsideGate?.inner_collapse_score ??
    outsideGate?.outside_head_promotion_context?.inner_collapse_score ??
    0
  );
  const broadOuterSupport = evidenceBoatSummaryRows.some((row) =>
    (row?.lane === 5 || row?.lane === 6) &&
    Number(row?.independent_evidence_count || 0) >= 3 &&
    Number(row?.head_support_score || 0) > 0.18
  );
  const contradiction =
    attackScenarioLabel &&
    topLaneFromDistributionRows(firstRows) &&
    String(attackScenarioLabel).toLowerCase().includes(String(topLaneFromDistributionRows(firstRows))) === false
      ? 8
      : 0;
  const outsideLeadPressure = String(formationPatternLabel || "").toLowerCase() === "outside_lead" ? 8 : 0;
  const rawScore =
    chaosRisk * 0.28 +
    partnerNoise * 0.22 +
    Math.max(0, 70 - headConfidence) * 0.18 +
    Math.max(0, 62 - recommendationConfidence) * 0.1 +
    Math.max(0, 0.62 - normalizeProbValue(boat1EscapeProbability)) * 100 * 0.16 +
    Math.max(0, 14 - distributionSpreadScore(firstRows, 2)) * 0.8 +
    Math.max(0, 12 - distributionSpreadScore(secondRows, 2)) * 0.55 +
    Math.max(0, 10 - distributionSpreadScore(thirdRows, 3)) * 0.35 +
    attackPressure +
    outsideEvidence * 2.6 +
    innerCollapseScore * 0.22 +
    qualityPenalty +
    contradiction +
    outsideLeadPressure +
    (broadOuterSupport ? 8 : 0);
  return Math.max(0, Math.min(100, Number(rawScore.toFixed(2))));
}

function shouldShowUpsetAlert({
  upsetRiskScore,
  confidenceScores,
  boat1EscapeProbability,
  participationDecision
}) {
  const headConfidence = Number(confidenceScores?.head_fixed_confidence_pct ?? confidenceScores?.head_confidence_calibrated ?? 0);
  const recommendationState = String(participationDecision?.decision || "").toLowerCase();
  if (upsetRiskScore >= 78) return true;
  if (upsetRiskScore < 62) return false;
  if (headConfidence >= 84 && normalizeProbValue(boat1EscapeProbability) >= 0.72 && recommendationState === "recommended") {
    return false;
  }
  return true;
}

function buildUpsetAlert({
  upsetRiskScore,
  showUpsetAlert,
  attackScenarioLabel,
  formationPatternLabel,
  roleCandidates,
  prediction,
  isRecommendedRace,
  backupUrasujiBets,
  finalRecommendedBets,
  evidenceBoatSummaryRows
}) {
  if (!showUpsetAlert) {
    return {
      shown: false,
      level: null,
      reasons: [],
      warningBoats: [],
      scenario: null,
      referenceTickets: [],
      referenceOnly: false
    };
  }
  const level = upsetRiskScore >= 78 ? "大穴警戒" : "穴注意";
  const firstRows = normalizeDistributionRowsForUi(
    roleCandidates?.first_place_candidates ||
    prediction?.confirmed_first_place_probability_json ||
    prediction?.first_place_probability_json
  );
  const secondRows = normalizeDistributionRowsForUi(
    roleCandidates?.second_place_candidates ||
    prediction?.confirmed_second_place_probability_json ||
    prediction?.second_place_probability_json
  );
  const outsideGate = roleCandidates?.outside_head_promotion_gate || prediction?.outside_head_promotion_gate_json || {};
  const gateByLane = outsideGate?.by_lane && typeof outsideGate.by_lane === "object" ? outsideGate.by_lane : {};
  const warningBoats = Array.from(new Set([
    ...firstRows.filter((row) => row.lane >= 4 && row.weight >= 0.1).map((row) => row.lane),
    ...secondRows.filter((row) => row.lane >= 4 && row.weight >= 0.12).map((row) => row.lane),
    ...evidenceBoatSummaryRows
      .filter((row) => Number(row?.independent_evidence_count || 0) >= 2 && row.lane >= 4)
      .map((row) => row.lane)
  ])).slice(0, 4);
  const reasonPool = [];
  if (String(formationPatternLabel || "").toLowerCase() === "outside_lead") reasonPool.push("outside-lead shape is active");
  if (attackScenarioLabel) reasonPool.push(`attack scenario pressure: ${attackScenarioLabel}`);
  if ((outsideGate?.inner_collapse_score ?? 0) >= 58) reasonPool.push("inner collapse evidence is elevated");
  if (warningBoats.some((lane) => {
    const gate = gateByLane?.[lane] || gateByLane?.[String(lane)] || {};
    return Number(gate?.matched_evidence_categories_count || 0) >= 3;
  })) reasonPool.push("multiple outside evidence groups are aligned");
  if (firstRows.length > 1 && (firstRows[0].weight - firstRows[1].weight) <= 0.08) reasonPool.push("first-place edge is narrow");
  if (secondRows.length > 2 && secondRows[2].weight >= 0.12) reasonPool.push("opponent order remains wide");
  const referenceTickets = compactTicketRows(
    backupUrasujiBets.length > 0 ? backupUrasujiBets : finalRecommendedBets.filter((bet) => {
      const firstLane = Number(String(bet?.combo || "").split("-")[0]);
      return firstLane >= 4;
    }),
    isRecommendedRace ? 3 : 2
  );
  return {
    shown: true,
    level,
    reasons: reasonPool.slice(0, 3),
    warningBoats,
    scenario: attackScenarioLabel || formationPatternLabel || "mixed upset pressure",
    referenceTickets,
    referenceOnly: !isRecommendedRace || level === "大穴警戒",
    score: upsetRiskScore
  };
}

function buildSemanticCardStyles({ isRecommendedRace, upsetLevel }) {
  return {
    recommendationTone: isRecommendedRace ? "tone-recommended" : "tone-reference",
    upsetTone: upsetLevel === "大穴警戒" ? "tone-upset-high" : upsetLevel ? "tone-upset-mid" : ""
  };
}

function buildTicketDisplayGroups({
  finalRecommendedBets,
  exactaBets,
  backupUrasujiBets,
  boat1HeadBets,
  isRecommendedRace
}) {
  const mainTrifecta = compactTicketRows(finalRecommendedBets, isRecommendedRace ? 6 : 4);
  const exactaCover = compactTicketRows(exactaBets, isRecommendedRace ? 4 : 2);
  const backupUrasuji = compactTicketRows(backupUrasujiBets, 4);
  const watchOnlyReferences = !isRecommendedRace ? compactTicketRows(boat1HeadBets, 4) : [];
  return {
    mainTrifecta,
    exactaCover,
    backupUrasuji,
    watchOnlyReferences
  };
}

function estimateTicketHitRate(ticket, ticketType, tier) {
  const directProb = normalizeProbValue(ticket?.prob ?? ticket?.estimated_hit_rate);
  if (directProb > 0) return Number(directProb.toFixed(4));
  if (ticketType === "exacta") {
    const head = Number(ticket?.exacta_head_score ?? 0);
    const partner = Number(ticket?.exacta_partner_score ?? 0);
    const derived = Math.max(0, Math.min(1, (head * 0.58 + partner * 0.42) / 100));
    return Number(derived.toFixed(4));
  }
  const boat1Head = Number(ticket?.boat1_head_score ?? 0);
  const generic = boat1Head > 0 ? Math.max(0, Math.min(1, boat1Head / 100)) : 0;
  const tierBoost = tier === "main" ? 0.02 : tier === "cover" ? 0.01 : 0;
  return Number(Math.max(0, Math.min(1, generic + tierBoost)).toFixed(4));
}

function collectAllTicketCandidates({ finalRecommendedBets, exactaBets, backupUrasujiBets }) {
  const tierRank = { main: 3, cover: 2, backup: 1 };
  const makeRow = (ticket, ticketType, tier) => ({
    ticket_type: ticketType,
    ticket: ticket?.combo || "",
    estimated_hit_rate: estimateTicketHitRate(ticket, ticketType, tier),
    recommendation_tier: tier,
    recommendation_tier_rank: tierRank[tier] || 0,
    reason_tags: Array.isArray(ticket?.explanation_tags)
      ? ticket.explanation_tags
      : Array.isArray(ticket?.exacta_reason_tags)
        ? ticket.exacta_reason_tags
        : Array.isArray(ticket?.trap_flags)
          ? ticket.trap_flags
          : [],
    source_row: ticket
  });
  return [
    ...(Array.isArray(finalRecommendedBets) ? finalRecommendedBets : []).map((ticket) => makeRow(ticket, "trifecta", "main")),
    ...(Array.isArray(exactaBets) ? exactaBets : []).map((ticket) => makeRow(ticket, "exacta", "cover")),
    ...(Array.isArray(backupUrasujiBets) ? backupUrasujiBets : []).map((ticket) => makeRow(ticket, "trifecta", "backup"))
  ].filter((row) => row.ticket);
}

function rankTicketCandidatesByHitRate(rows = []) {
  return [...(Array.isArray(rows) ? rows : [])].sort((a, b) => {
    const hitDiff = Number(b?.estimated_hit_rate ?? 0) - Number(a?.estimated_hit_rate ?? 0);
    if (Math.abs(hitDiff) > 0.0005) return hitDiff;
    const tierDiff = Number(b?.recommendation_tier_rank ?? 0) - Number(a?.recommendation_tier_rank ?? 0);
    if (tierDiff !== 0) return tierDiff;
    if (a?.ticket_type !== b?.ticket_type) {
      return a?.ticket_type === "exacta" ? -1 : 1;
    }
    return String(a?.ticket || "").localeCompare(String(b?.ticket || ""));
  });
}

function buildTopRecommendedTickets({ finalRecommendedBets, exactaBets, backupUrasujiBets, maxItems = 10 }) {
  const allCandidates = collectAllTicketCandidates({
    finalRecommendedBets,
    exactaBets,
    backupUrasujiBets
  });
  const deduped = new Map();
  for (const row of allCandidates) {
    if (String(row?.ticket_type || "") !== "trifecta") continue;
    const key = `${row.ticket_type}:${row.ticket}`;
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, row);
      continue;
    }
    const preferred = rankTicketCandidatesByHitRate([existing, row])[0];
    deduped.set(key, preferred);
  }
  return rankTicketCandidatesByHitRate([...deduped.values()])
    .slice(0, Math.max(1, Math.min(10, Number(maxItems) || 10)))
    .map((row, idx) => ({
      ...row,
      rank: idx + 1
    }));
}

function buildBiasPanelPayload({
  evidenceInterpretation,
  evidenceConfirmationFlags,
  evidenceBoatSummaryRows
}) {
  const strongestBoats = compactTicketRows(
    evidenceBoatSummaryRows
      .slice()
      .sort((a, b) => (Number(b?.independent_evidence_count || 0) - Number(a?.independent_evidence_count || 0))),
    3
  );
  return {
    interpretation: compactTicketRows(evidenceInterpretation, 3),
    mainHead: evidenceConfirmationFlags?.main_head_candidate || null,
    mainSecond: evidenceConfirmationFlags?.main_second_candidate || null,
    counterSecond: evidenceConfirmationFlags?.counter_second_candidate || null,
    thirdSurvivors: Array.isArray(evidenceConfirmationFlags?.third_place_survivors)
      ? evidenceConfirmationFlags.third_place_survivors
      : [],
    strongestBoats
  };
}

function buildSummaryHeaderPayload({
  participationLabel,
  participationClass,
  confidenceScores,
  participationDecision,
  prediction,
  roleCandidates,
  evidenceConfirmationFlags,
  top3,
  attackScenarioLabel,
  boat1EscapeProbability
}) {
  const scoreSource =
    participationDecision?.participation_score_components && typeof participationDecision.participation_score_components === "object"
      ? participationDecision.participation_score_components
      : prediction?.participation_score_components && typeof prediction.participation_score_components === "object"
        ? prediction.participation_score_components
        : {};
  const mainHead = evidenceConfirmationFlags?.main_head_candidate || topLaneFromList(roleCandidates?.head_candidates, top3?.[0] || null);
  const mainSecond = evidenceConfirmationFlags?.main_second_candidate || topLaneFromList(roleCandidates?.second_candidates, top3?.[1] || null);
  const thirdSurvivors = Array.isArray(evidenceConfirmationFlags?.third_place_survivors) && evidenceConfirmationFlags.third_place_survivors.length
    ? evidenceConfirmationFlags.third_place_survivors
    : compactTicketRows(roleCandidates?.third_candidates || [], 3);
  const headBand = getConfidenceBandLabel(confidenceScores?.head_fixed_band);
  const betBand = getConfidenceBandLabel(confidenceScores?.recommended_bet_band);
  const partnerBand = getScoreBand(scoreSource?.partner_clarity_score, { high: 68, medium: 54 });
  const riskBand = getScoreBand(100 - Number(scoreSource?.race_stability_score || 0), { high: 55, medium: 35 });
  return {
    status: {
      label: participationLabel,
      className: participationClass
    },
    headConfidence: {
      value: Number(confidenceScores?.head_fixed_confidence_pct ?? confidenceScores?.head_confidence_calibrated ?? 0),
      label: headBand,
      meaning: headBand === "High" ? "head stable" : headBand === "Medium" ? "head readable" : "head caution"
    },
    recommendationStrength: {
      value: Number(confidenceScores?.recommended_bet_confidence_pct ?? confidenceScores?.bet_confidence_calibrated ?? 0),
      label: betBand,
      meaning: betBand === "High" ? "tickets actionable" : betBand === "Medium" ? "bet selective" : "reference only"
    },
    opponentStability: {
      value: Number(scoreSource?.partner_clarity_score ?? 0),
      label: partnerBand.label,
      meaning: partnerBand.meaning
    },
    chaosRisk: {
      value: Math.max(0, 100 - Number(scoreSource?.race_stability_score ?? 0)),
      label: riskBand.label,
      meaning: riskBand.label === "Low" ? "race stable" : riskBand.label === "Medium" ? "mixed structure" : "chaos caution"
    },
    boat1EscapeProbability,
    structure: {
      mainHead,
      mainSecond,
      thirdSurvivors,
      attackScenarioLabel
    }
  };
}

function buildPredictionViewModel({
  race,
  venueName,
  date,
  participationLabel,
  participationClass,
  confidenceScores,
  participationDecision,
  prediction,
  roleCandidates,
  evidenceConfirmationFlags,
  top3,
  attackScenarioLabel,
  finalRecommendedBets,
  exactaBets,
  backupUrasujiBets,
  boat1HeadBets,
  isRecommendedRace,
  evidenceInterpretation,
  evidenceBoatSummaryRows,
  boat1EscapeProbability
}) {
  const savedTopRecommendedTickets = Array.isArray(prediction?.top_recommended_tickets_snapshot)
    ? prediction.top_recommended_tickets_snapshot.filter((row) => String(row?.ticket_type || "trifecta") === "trifecta")
    : [];
  const snapshotContext =
    prediction?.snapshot_context && typeof prediction.snapshot_context === "object"
      ? prediction.snapshot_context
      : {};
  const matchedDictionaryScenarios = Array.isArray(prediction?.matched_dictionary_scenarios_json)
    ? prediction.matched_dictionary_scenarios_json
    : Array.isArray(snapshotContext?.matched_dictionary_scenarios_json)
      ? snapshotContext.matched_dictionary_scenarios_json
      : [];
  const dictionaryPriorAdjustment =
    prediction?.dictionary_prior_adjustment_json && typeof prediction.dictionary_prior_adjustment_json === "object"
      ? prediction.dictionary_prior_adjustment_json
      : snapshotContext?.dictionary_prior_adjustment_json && typeof snapshotContext.dictionary_prior_adjustment_json === "object"
        ? snapshotContext.dictionary_prior_adjustment_json
        : {};
  const dictionaryConditionFlags = Array.isArray(prediction?.dictionary_condition_flags_json)
    ? prediction.dictionary_condition_flags_json
    : Array.isArray(snapshotContext?.dictionary_condition_flags_json)
      ? snapshotContext.dictionary_condition_flags_json
      : [];
  return {
    raceTitle: `${race.venueName || venueName} ${race.raceNo || "-"}R`,
    raceSubtitle: race.raceName || race.date || date,
    summary: buildSummaryHeaderPayload({
      participationLabel,
      participationClass,
      confidenceScores,
      participationDecision,
      prediction,
      roleCandidates,
      evidenceConfirmationFlags,
      top3,
      attackScenarioLabel,
      boat1EscapeProbability
    }),
    tickets: buildTicketDisplayGroups({
      finalRecommendedBets,
      exactaBets,
      backupUrasujiBets,
      boat1HeadBets,
      isRecommendedRace
    }),
    topRecommendedTickets: savedTopRecommendedTickets.length > 0
      ? savedTopRecommendedTickets
      : buildTopRecommendedTickets({
          finalRecommendedBets,
          exactaBets,
          backupUrasujiBets,
          maxItems: 10
        }),
    biasPanel: buildBiasPanelPayload({
      evidenceInterpretation,
      evidenceConfirmationFlags,
      evidenceBoatSummaryRows
    }),
    predictionMeta: {
      playerStatWindowPolicy: prediction?.player_stat_window_policy_json || null,
      playerStatWindowsUsed: Array.isArray(prediction?.player_stat_windows_used_json)
        ? prediction.player_stat_windows_used_json
        : [],
      boat3WeakStHeadSuppressed: Number(prediction?.boat3_weak_st_head_suppressed || 0) === 1,
      boat3WeakStHeadSuppression: prediction?.boat3_weak_st_head_suppression_json || {},
      matchedDictionaryScenarios,
      dictionaryPriorAdjustment,
      dictionaryConditionFlags
    }
  };
}

function getPlayerComparisonRows({ prediction, data }) {
  const snapshotContext =
    prediction?.snapshot_context && typeof prediction.snapshot_context === "object"
      ? prediction.snapshot_context
      : {};
  const entryDebug = getCanonicalEntryDebug(data, data?.startDisplay);
  const snapshotPlayers = Array.isArray(snapshotContext?.players) ? snapshotContext.players : [];
  const debugLaneRows = Array.isArray(data?.kyoteibiyori_debug?.lane_rows) ? data.kyoteibiyori_debug.lane_rows : [];
  const snapshotByLane = new Map(
    snapshotPlayers
      .map((row) => [Number(row?.lane || 0), row])
      .filter(([lane]) => Number.isInteger(lane) && lane > 0)
  );
  const debugByLane = new Map(
    debugLaneRows
      .map((row) => [Number(row?.lane || 0), row])
      .filter(([lane]) => Number.isInteger(lane) && lane > 0)
  );
  const racers = Array.isArray(data?.racers) ? data.racers : [];
  if (racers.length > 0) {
    return racers
      .map((row) => {
        const lane = Number(row?.lane || 0);
        const snapshotRow = snapshotByLane.get(lane) || {};
        const debugRow = debugByLane.get(lane) || {};
        const laneResolution = resolveCanonicalActualLane({ lane, row, snapshotRow, entryDebug });
        const debugLaneStats = normalizeLaneStats(debugRow);
        const liveLaneStats = normalizeLaneStats(row);
        const snapshotLaneStats = normalizeLaneStats(snapshotRow);
        const reassignedLaneScores = {
          lane1st: firstMeaningfulFiniteValue(
            snapshotRow?.lane1st_score_after_reassignment,
            snapshotRow?.feature_snapshot?.lane_fit_1st
          ),
          lane2ren: firstMeaningfulFiniteValue(
            snapshotRow?.lane2ren_score_after_reassignment,
            snapshotRow?.feature_snapshot?.lane_fit_2ren
          ),
          lane3ren: firstMeaningfulFiniteValue(
            snapshotRow?.lane3ren_score_after_reassignment,
            snapshotRow?.feature_snapshot?.lane_fit_3ren
          )
        };
        const beforeReassignmentLaneScores = {
          lane1st: firstMeaningfulFiniteValue(
            row?.lane1stScore,
            row?.lane1stAvg,
            row?.laneFirstRate,
            debugRow?.lane1stScore,
            snapshotRow?.lane1st_score_before_reassignment,
            snapshotRow?.lane1st_score
          ),
          lane2ren: firstMeaningfulFiniteValue(
            row?.lane2renScore,
            row?.lane2renAvg,
            row?.lane2RenRate,
            debugRow?.lane2renScore,
            snapshotRow?.lane2ren_score_before_reassignment,
            snapshotRow?.lane2ren_score
          ),
          lane3ren: firstMeaningfulFiniteValue(
            row?.lane3renScore,
            row?.lane3renAvg,
            row?.lane3RenRate,
            debugRow?.lane3renScore,
            snapshotRow?.lane3ren_score_before_reassignment,
            snapshotRow?.lane3ren_score
          )
        };
        const liveLapTime = getLapTimeDisplayValue(row);
        const liveExhibitionSt = toFiniteComparisonNumber(row?.kyoteiBiyoriExhibitionSt ?? row?.exhibitionSt);
        const liveExhibitionTime = toFiniteComparisonNumber(row?.kyoteiBiyoriExhibitionTime ?? row?.exhibitionTime);
        const liveLapExStretch = toFiniteComparisonNumber(
          row?.kyoteiBiyoriLapExStretch ?? row?.lapExStretch ?? row?.kyoteiBiyoriLapExhibitionScore ?? row?.lapExhibitionScore
        );
        const liveMotor2Rate = toFiniteComparisonNumber(row?.motor2ren ?? row?.kyoteiBiyoriMotor2Rate ?? row?.motor2Rate);
        const liveMotor3Rate = firstFiniteValue(
          row?.motor3ren,
          row?.kyoteiBiyoriMotor3Rate,
          row?.motor3Rate,
          debugRow?.motor3ren_raw,
          snapshotRow?.motor_3rate
        );
        const snapshotLapExStretch = toFiniteComparisonNumber(
          snapshotRow?.kyoteibiyori_lap_ex_stretch ?? snapshotRow?.lap_ex_stretch ?? snapshotRow?.kyoteibiyori_lap_exhibition_score ?? snapshotRow?.lap_exhibition_score
        )
          ?? toFiniteComparisonNumber(snapshotRow?.feature_snapshot?.lap_exhibition_score)
          ?? toFiniteComparisonNumber(snapshotRow?.feature_snapshot?.lap_attack_strength)
          ?? (toFiniteComparisonNumber(snapshotRow?.feature_snapshot?.lap_time_delta_vs_front) !== null
            ? Number(snapshotRow.feature_snapshot.lap_time_delta_vs_front) * 100
            : null);
        return {
          lane,
          boatNumber: lane,
          actualLane: laneResolution.actualLane,
          courseChanged: laneResolution.courseChanged,
          actualLaneConfirmed: laneResolution.actualLaneConfirmed,
          name: row?.name || snapshotRow?.name || `Boat ${lane || "-"}`,
          fCount: row?.fHoldCount === null || row?.fHoldCount === undefined
            ? (snapshotRow?.f_hold_count === null || snapshotRow?.f_hold_count === undefined ? null : Number(snapshotRow.f_hold_count))
            : Number(row.fHoldCount),
          kyoteiBiyoriFetched:
            Number(row?.kyoteiBiyoriFetched) === 1 ||
            Number(snapshotRow?.kyoteibiyori_fetched) === 1,
          lapTime: firstFiniteValue(
            liveLapTime,
            getLapTimeDisplayValue(snapshotRow)
          ),
          exhibitionSt: liveExhibitionSt ?? toFiniteComparisonNumber(snapshotRow?.kyoteibiyori_exhibition_st ?? snapshotRow?.exhibition_st),
          exhibitionTime: liveExhibitionTime ?? toFiniteComparisonNumber(snapshotRow?.kyoteibiyori_exhibition_time ?? snapshotRow?.exhibition_time),
          lapExStretch: liveLapExStretch ?? snapshotLapExStretch,
          lapScore: liveLapExStretch ?? snapshotLapExStretch,
          stretchFootLabel: row?.kyoteiBiyoriStretchFootLabel || row?.stretchFootLabel || snapshotRow?.kyoteibiyori_stretch_foot_label || snapshotRow?.stretch_foot_label || null,
          motor2ren: liveMotor2Rate ?? toFiniteComparisonNumber(snapshotRow?.motor_2rate),
          motor3ren: liveMotor3Rate,
          motor2Rate: liveMotor2Rate ?? toFiniteComparisonNumber(snapshotRow?.motor_2rate),
          motor3Rate: liveMotor3Rate,
          lane1stScore: firstMeaningfulFiniteValue(reassignedLaneScores.lane1st, debugLaneStats.laneFirstRate, liveLaneStats.laneFirstRate, snapshotLaneStats.laneFirstRate),
          lane2renScore: firstMeaningfulFiniteValue(reassignedLaneScores.lane2ren, debugLaneStats.lane2RenRate, liveLaneStats.lane2RenRate, snapshotLaneStats.lane2RenRate),
          lane3renScore: firstMeaningfulFiniteValue(reassignedLaneScores.lane3ren, debugLaneStats.lane3RenRate, liveLaneStats.lane3RenRate, snapshotLaneStats.lane3RenRate),
          lane1stAvg: firstMeaningfulFiniteValue(reassignedLaneScores.lane1st, debugLaneStats.laneFirstRate, liveLaneStats.laneFirstRate, snapshotLaneStats.laneFirstRate),
          lane2renAvg: firstMeaningfulFiniteValue(reassignedLaneScores.lane2ren, debugLaneStats.lane2RenRate, liveLaneStats.lane2RenRate, snapshotLaneStats.lane2RenRate),
          lane3renAvg: firstMeaningfulFiniteValue(reassignedLaneScores.lane3ren, debugLaneStats.lane3RenRate, liveLaneStats.lane3RenRate, snapshotLaneStats.lane3RenRate),
          laneFirstRate: firstMeaningfulFiniteValue(reassignedLaneScores.lane1st, debugLaneStats.laneFirstRate, liveLaneStats.laneFirstRate, snapshotLaneStats.laneFirstRate),
          lane2RenRate: firstMeaningfulFiniteValue(reassignedLaneScores.lane2ren, debugLaneStats.lane2RenRate, liveLaneStats.lane2RenRate, snapshotLaneStats.lane2RenRate),
          lane3RenRate: firstMeaningfulFiniteValue(reassignedLaneScores.lane3ren, debugLaneStats.lane3RenRate, liveLaneStats.lane3RenRate, snapshotLaneStats.lane3RenRate),
          laneScoreDebug: {
            beforeReassignment: beforeReassignmentLaneScores,
            afterReassignment: reassignedLaneScores,
            actualLane: laneResolution.actualLane,
            courseChanged: laneResolution.courseChanged,
            actualLaneConfirmed: laneResolution.actualLaneConfirmed
          }
        };
      })
      .sort((a, b) => (a.actualLane - b.actualLane) || (a.boatNumber - b.boatNumber));
  }
  return snapshotPlayers
    .map((row) => {
      const lane = Number(row?.lane || 0);
      const laneResolution = resolveCanonicalActualLane({ lane, snapshotRow: row, entryDebug });
      return {
        lane,
        boatNumber: lane,
        actualLane: laneResolution.actualLane,
        courseChanged: laneResolution.courseChanged,
        actualLaneConfirmed: laneResolution.actualLaneConfirmed,
        name: row?.name || `Boat ${row?.lane || "-"}`,
        fCount: row?.f_hold_count === null || row?.f_hold_count === undefined ? null : Number(row.f_hold_count),
        kyoteiBiyoriFetched: Number(row?.kyoteibiyori_fetched) === 1,
        lapTime: getLapTimeDisplayValue(row),
        exhibitionSt: toFiniteComparisonNumber(row?.kyoteibiyori_exhibition_st ?? row?.exhibition_st),
        exhibitionTime: toFiniteComparisonNumber(row?.kyoteibiyori_exhibition_time ?? row?.exhibition_time),
        lapExStretch: toFiniteComparisonNumber(row?.kyoteibiyori_lap_ex_stretch ?? row?.lap_ex_stretch ?? row?.kyoteibiyori_lap_exhibition_score ?? row?.lap_exhibition_score),
        lapScore: toFiniteComparisonNumber(row?.kyoteibiyori_lap_ex_stretch ?? row?.lap_ex_stretch ?? row?.kyoteibiyori_lap_exhibition_score ?? row?.lap_exhibition_score),
        stretchFootLabel: row?.kyoteibiyori_stretch_foot_label || row?.stretch_foot_label || null,
        motor2ren: toFiniteComparisonNumber(row?.motor_2rate),
        motor3ren: toFiniteComparisonNumber(row?.motor_3rate),
        motor2Rate: toFiniteComparisonNumber(row?.motor_2rate),
        motor3Rate: toFiniteComparisonNumber(row?.motor_3rate),
        lane1stScore: firstMeaningfulFiniteValue(row?.lane1st_score_after_reassignment, row?.feature_snapshot?.lane_fit_1st, normalizeLaneStats(row).laneFirstRate),
        lane2renScore: firstMeaningfulFiniteValue(row?.lane2ren_score_after_reassignment, row?.feature_snapshot?.lane_fit_2ren, normalizeLaneStats(row).lane2RenRate),
        lane3renScore: firstMeaningfulFiniteValue(row?.lane3ren_score_after_reassignment, row?.feature_snapshot?.lane_fit_3ren, normalizeLaneStats(row).lane3RenRate),
        lane1stAvg: firstMeaningfulFiniteValue(row?.lane1st_score_after_reassignment, row?.feature_snapshot?.lane_fit_1st, normalizeLaneStats(row).laneFirstRate),
        lane2renAvg: firstMeaningfulFiniteValue(row?.lane2ren_score_after_reassignment, row?.feature_snapshot?.lane_fit_2ren, normalizeLaneStats(row).lane2RenRate),
        lane3renAvg: firstMeaningfulFiniteValue(row?.lane3ren_score_after_reassignment, row?.feature_snapshot?.lane_fit_3ren, normalizeLaneStats(row).lane3RenRate),
        laneFirstRate: firstMeaningfulFiniteValue(row?.lane1st_score_after_reassignment, row?.feature_snapshot?.lane_fit_1st, normalizeLaneStats(row).laneFirstRate),
        lane2RenRate: firstMeaningfulFiniteValue(row?.lane2ren_score_after_reassignment, row?.feature_snapshot?.lane_fit_2ren, normalizeLaneStats(row).lane2RenRate),
        lane3RenRate: firstMeaningfulFiniteValue(row?.lane3ren_score_after_reassignment, row?.feature_snapshot?.lane_fit_3ren, normalizeLaneStats(row).lane3RenRate),
        laneScoreDebug: {
          beforeReassignment: {
            lane1st: firstMeaningfulFiniteValue(row?.lane1st_score_before_reassignment, row?.lane1st_score, normalizeLaneStats(row).laneFirstRate),
            lane2ren: firstMeaningfulFiniteValue(row?.lane2ren_score_before_reassignment, row?.lane2ren_score, normalizeLaneStats(row).lane2RenRate),
            lane3ren: firstMeaningfulFiniteValue(row?.lane3ren_score_before_reassignment, row?.lane3ren_score, normalizeLaneStats(row).lane3RenRate)
          },
          afterReassignment: {
            lane1st: firstMeaningfulFiniteValue(row?.lane1st_score_after_reassignment, row?.feature_snapshot?.lane_fit_1st),
            lane2ren: firstMeaningfulFiniteValue(row?.lane2ren_score_after_reassignment, row?.feature_snapshot?.lane_fit_2ren),
            lane3ren: firstMeaningfulFiniteValue(row?.lane3ren_score_after_reassignment, row?.feature_snapshot?.lane_fit_3ren)
          },
          actualLane: laneResolution.actualLane,
          courseChanged: laneResolution.courseChanged,
          actualLaneConfirmed: laneResolution.actualLaneConfirmed
        }
      };
    })
    .sort((a, b) => (a.actualLane - b.actualLane) || (a.boatNumber - b.boatNumber));
}

function buildTopMetricLaneSet(rows, key, direction = "desc") {
  const ranked = rows
    .filter((row) => Number.isFinite(Number(row?.[key])))
    .sort((a, b) => {
      const av = Number(a?.[key]);
      const bv = Number(b?.[key]);
      return direction === "asc" ? av - bv : bv - av;
    });
  const topValues = [...new Set(ranked.slice(0, 2).map((row) => Number(row?.[key])))];
  return new Set(
    ranked
      .filter((row) => topValues.includes(Number(row?.[key])))
      .map((row) => Number(row?.actualLane ?? row?.lane))
  );
}

function buildPremiumPredictionViewModel(args) {
  const base = buildPredictionViewModel(args);
  const upsetRiskScore = computeUpsetRiskScore(args);
  const showUpsetAlert = shouldShowUpsetAlert({
    upsetRiskScore,
    confidenceScores: args.confidenceScores,
    boat1EscapeProbability: args.boat1EscapeProbability,
    participationDecision: args.participationDecision
  });
  const upsetAlert =
    args.prediction?.upset_alert_snapshot && typeof args.prediction.upset_alert_snapshot === "object"
      ? {
          shown: Boolean(args.prediction.upset_alert_snapshot?.shown),
          level: args.prediction.upset_alert_snapshot?.level || null,
          reasons: Array.isArray(args.prediction.upset_alert_snapshot?.reasons)
            ? args.prediction.upset_alert_snapshot.reasons
            : [],
          warningBoats: Array.isArray(args.prediction.upset_alert_snapshot?.warning_boats)
            ? args.prediction.upset_alert_snapshot.warning_boats
            : [],
          scenario: args.prediction.upset_alert_snapshot?.likely_scenario || null,
          referenceTickets: Array.isArray(args.prediction.upset_alert_snapshot?.reference_tickets)
            ? args.prediction.upset_alert_snapshot.reference_tickets
            : [],
          referenceOnly: Boolean(args.prediction.upset_alert_snapshot?.reference_only),
          score: Number(args.prediction.upset_alert_snapshot?.score ?? upsetRiskScore)
        }
      : buildUpsetAlert({
          upsetRiskScore,
          showUpsetAlert,
          attackScenarioLabel: args.attackScenarioLabel,
          formationPatternLabel: args.formationPatternLabel,
          roleCandidates: args.roleCandidates,
          prediction: args.prediction,
          isRecommendedRace: args.isRecommendedRace,
          backupUrasujiBets: args.backupUrasujiBets,
          finalRecommendedBets: args.finalRecommendedBets,
          evidenceBoatSummaryRows: args.evidenceBoatSummaryRows
        });
  return {
    ...base,
    upsetAlert,
    semanticStyles: buildSemanticCardStyles({
      isRecommendedRace: args.isRecommendedRace,
      upsetLevel: upsetAlert.level
    })
  };
}

const EVIDENCE_GROUP_LABELS = {
  motor: "Motor",
  lane_course_fit: "Lane/Course Fit",
  exhibition: "Exhibition",
  risk: "Risk",
  formation_scenario: "Formation/Scenario",
  learning: "Learning"
};

function getEvidenceBiasTable(data, prediction) {
  const direct = data?.evidenceBiasTable;
  if (direct && typeof direct === "object") return direct;
  const snapshot = prediction?.evidence_bias_table_json;
  if (snapshot && typeof snapshot === "object") return snapshot;
  return {};
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
  const entryMeta = startDisplay.start_display_entry_meta && typeof startDisplay.start_display_entry_meta === "object"
    ? startDisplay.start_display_entry_meta
    : {};
  const entryValidation = entryMeta.validation && typeof entryMeta.validation === "object"
    ? entryMeta.validation
    : {};
  const confirmedActualEntry = entryValidation.validation_ok === true && entryMeta.fallback_used !== true;
  const perBoatLaneMap = entryMeta.per_boat_lane_map && typeof entryMeta.per_boat_lane_map === "object"
    ? entryMeta.per_boat_lane_map
    : {};
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
      actualLane: entryOrderByLane.get(lane) || lane,
      moved: confirmedActualEntry && (entryOrderByLane.get(lane) || lane) !== lane,
      confirmedActualEntry,
      fallbackUsed: entryMeta.fallback_used === true,
      fallbackReason: entryMeta.fallback_reason || null,
      perBoatLane: perBoatLaneMap[String(lane)] || null,
      leftPct,
      xUnit: Number.isFinite(axisClamped) ? Number(axisClamped.toFixed(2)) : null,
      st: Number.isFinite(Number(st)) ? Number(st) : null,
      stDisplay: timingDisplay
    };
  }).sort((a, b) => {
    const orderA = Number.isInteger(a.order) ? a.order : 99;
    const orderB = Number.isInteger(b.order) ? b.order : 99;
    return orderA - orderB || a.lane - b.lane;
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
          {startDisplay?.start_display_entry_meta?.fallback_used ? " / actual entry fallback: predicted/base order" : ""}
        </p>
      ) : null}
      {rows.map((row) => (
        <div key={`start-${row.lane}`} className="start-row">
          <div className="start-lane">
            <div className="player-name-cell">
              <span className={`combo-dot ${BOAT_META[row.actualLane]?.className || ""}`}>{row.actualLane ?? "--"}</span>
              <strong>Boat {row.lane}</strong>
              <div className="muted">
                {row.confirmedActualEntry
                  ? row.moved
                    ? `Moved from ${row.lane} to entry ${row.actualLane}`
                    : "No course change"
                  : "Actual entry not confirmed. Using base/predicted order"}
              </div>
            </div>
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
  const [errorDetails, setErrorDetails] = useState(null);
  const [data, setData] = useState(null);
  const [rankingsLoading, setRankingsLoading] = useState(false);
  const [rankingsError, setRankingsError] = useState("");
  const [rankingsData, setRankingsData] = useState([]);
  const [hardRaceCalibration, setHardRaceCalibration] = useState(null);
  const [hardRaceTierFilter, setHardRaceTierFilter] = useState("ALL");

  const [statsLoading, setStatsLoading] = useState(false);
  const [stats, setStats] = useState(null);
  const [evaluationFilters, setEvaluationFilters] = useState({
    venue: "all",
    date_from: "",
    date_to: "",
    recommendation_level: "all",
    formation_pattern: "all",
    only_participated: false,
    only_recommended: false,
    only_boat1_escape_predicted: false,
    only_outside_head_cases: false
  });
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
  const [recentRaceSelections, setRecentRaceSelections] = useState(() => {
    if (typeof window === "undefined") return [];
    try {
      const saved = JSON.parse(window.localStorage.getItem("kyoteiapp_recent_races") || "[]");
      return Array.isArray(saved) ? saved : [];
    } catch {
      return [];
    }
  });

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
  const snapshotGenerationHints = buildSnapshotGenerationHints(date, venueId, raceNo);
  const startDisplay = data?.startDisplay || null;
  const entryPipelineDebug = useMemo(() => getCanonicalEntryDebug(data, startDisplay), [data, startDisplay]);
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
  const displayActualEntryOrder = entryPipelineDebug.confirmedActualEntry ? actualEntryOrder : predictedEntryOrder;
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
  const pureTop6Prediction =
    data?.pureTop6Prediction && typeof data.pureTop6Prediction === "object"
      ? data.pureTop6Prediction
      : prediction?.pure_top6_prediction && typeof prediction.pure_top6_prediction === "object"
        ? prediction.pure_top6_prediction
        : {};
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
  const evidenceBiasTable = getEvidenceBiasTable(data, prediction);
  const raceStructure = data?.raceStructure || {};
  const venueBias = data?.venueBias || {};
  const wallEvaluation = data?.wallEvaluation || {};
  const headConfidence = data?.headConfidence || {};
  const ticketGenerationV2 = data?.ticketGenerationV2 || {};
  const ticketOptimization = data?.ticketOptimization || {};
  const valueDetection = data?.valueDetection || {};
  const scenarioSuggestions =
    data?.scenarioSuggestions && typeof data.scenarioSuggestions === "object"
      ? data.scenarioSuggestions
      : prediction?.scenarioSuggestions && typeof prediction.scenarioSuggestions === "object"
        ? prediction.scenarioSuggestions
        : {};
  const explainability = data?.explainability || {};
  const manualLapEvaluation = data?.manualLapEvaluation || null;
  const bankrollPlan = data?.bankrollPlan || ticketOptimization?.bankrollPlan || {};
  const raceDecision = data?.raceDecision || {};
  const participationDecision = data?.participationDecision || {};
  const confidenceScores = data?.confidenceScores || raceDecision?.confidence_scores || {};
  const recommendationMode = String(
    data?.raceDecision?.mode || raceRisk?.recommendation || data?.recommendation_label || ""
  ).toUpperCase();
  const pureTop6Rows = buildTop6PredictionRows(pureTop6Prediction);
  const pureTop6Groups = groupTop6PredictionRows(pureTop6Rows);
  const pureHeadRanking = Array.isArray(pureTop6Prediction?.head_candidate_ranking) ? pureTop6Prediction.head_candidate_ranking : [];
  const pureHeadTrust = pureHeadRanking[0]?.probability ?? null;
  const predictionConfidenceState = getPredictionConfidenceState(sourceMeta);
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
  const backupUrasujiSection = data?.backupUrasujiSection || prediction?.backupUrasujiSection || {};
  const backupUrasujiBets = Array.isArray(backupUrasujiSection?.backup_urasuji_recommendations_snapshot)
    ? backupUrasujiSection.backup_urasuji_recommendations_snapshot
    : Array.isArray(prediction?.backup_urasuji_recommendations_snapshot)
      ? prediction.backup_urasuji_recommendations_snapshot
      : [];
  const backupUrasujiShown =
    Number(backupUrasujiSection?.backup_urasuji_section_shown ?? prediction?.backup_urasuji_section_shown ?? 0) === 1 &&
    backupUrasujiBets.length > 0;
  const backupUrasujiReasonTags = Array.isArray(backupUrasujiSection?.backup_urasuji_reason_tags)
    ? backupUrasujiSection.backup_urasuji_reason_tags
    : Array.isArray(prediction?.backup_urasuji_reason_tags)
      ? prediction.backup_urasuji_reason_tags
      : [];
  const evidenceGroupRankings = evidenceBiasTable?.per_group_rankings && typeof evidenceBiasTable.per_group_rankings === "object"
    ? evidenceBiasTable.per_group_rankings
    : {};
  const evidenceBoatSummary = evidenceBiasTable?.boat_summary && typeof evidenceBiasTable.boat_summary === "object"
    ? evidenceBiasTable.boat_summary
    : {};
  const evidenceInterpretation = Array.isArray(evidenceBiasTable?.interpretation)
    ? evidenceBiasTable.interpretation
    : [];
  const evidenceConfirmationFlags = evidenceBiasTable?.confirmation_flags && typeof evidenceBiasTable.confirmation_flags === "object"
    ? evidenceBiasTable.confirmation_flags
    : {};
  const evidenceBoatSummaryRows = Object.entries(evidenceBoatSummary)
    .map(([lane, summary]) => ({
      lane: Number(lane),
      ...summary
    }))
    .sort((a, b) => Number(b?.head_support_score || 0) - Number(a?.head_support_score || 0));
  const defaultReasonTags = [
    ...(Array.isArray(participationDecision?.reason_tags) ? participationDecision.reason_tags : []),
    ...(Array.isArray(explainability?.race_tags) ? explainability.race_tags : [])
  ].filter(Boolean).slice(0, 6);
  const predictionQualityLabels = getPredictionQualityLabels(participationDecision, prediction);
  const skipReasonCodes = Array.isArray(raceRisk?.skip_reason_codes) ? raceRisk.skip_reason_codes : [];
  const boat1EscapeProbability = Number(
    roleCandidates?.boat1_escape_probability ??
    prediction?.boat1_escape_probability ??
    0
  );

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
  const quickVenueOptions = useMemo(
    () => [5, 4, 22, 24, 2].map((id) => VENUES.find((v) => v.id === id)).filter(Boolean),
    []
  );
  const predictionViewModel = useMemo(
    () => buildPremiumPredictionViewModel({
      race,
      venueName,
      date,
      participationLabel,
      participationClass,
      confidenceScores,
      participationDecision,
      prediction,
      roleCandidates,
      evidenceConfirmationFlags,
      top3,
      attackScenarioLabel,
      finalRecommendedBets,
      exactaBets,
      backupUrasujiBets,
      boat1HeadBets,
      isRecommendedRace,
      evidenceInterpretation,
      evidenceBoatSummaryRows,
      boat1EscapeProbability,
      formationPatternLabel,
      raceStructure
    }),
    [
      race,
      venueName,
      date,
      participationLabel,
      participationClass,
      confidenceScores,
      participationDecision,
      prediction,
      roleCandidates,
      evidenceConfirmationFlags,
      top3,
      attackScenarioLabel,
      finalRecommendedBets,
      exactaBets,
      backupUrasujiBets,
      boat1HeadBets,
      isRecommendedRace,
      evidenceInterpretation,
      evidenceBoatSummaryRows,
      boat1EscapeProbability,
      formationPatternLabel,
      raceStructure
    ]
  );
  const similarHistoryRows = useMemo(() => {
    if (!Array.isArray(history) || history.length === 0) return [];
    return history
      .filter((row) => {
        const sameVenue = String(row?.venue_name || row?.venue_id || "") === String(race.venueName || venueName || "");
        const sameFormation = String(row?.formation_pattern || "") === String(formationPatternLabel || "");
        return sameVenue || sameFormation;
      })
      .slice(0, 6);
  }, [history, race.venueName, venueName, formationPatternLabel]);
  const playerComparisonRows = useMemo(
    () => getPlayerComparisonRows({ prediction, data }),
    [prediction, data]
  );
  const entrySupplementalDebug = useMemo(() => {
    const fieldChecks = [
      ["lap_time", (row) => Number.isFinite(Number(row?.lapTime))],
      ["exhibition_st", (row) => Number.isFinite(Number(row?.exhibitionSt))],
      ["exhibition_time", (row) => Number.isFinite(Number(row?.exhibitionTime))],
      ["motor2ren", (row) => Number.isFinite(Number(row?.motor2ren ?? row?.motor2Rate))],
      ["motor3ren", (row) => Number.isFinite(Number(row?.motor3ren ?? row?.motor3Rate))]
    ];
    const usable = fieldChecks.filter(([, check]) => playerComparisonRows.some((row) => check(row))).map(([field]) => field);
    const skipped = fieldChecks.filter(([, check]) => !playerComparisonRows.some((row) => check(row))).map(([field]) => field);
    return { usable, skipped };
  }, [playerComparisonRows]);
  const playerMetricLeaders = useMemo(() => ({
    lapTime: buildTopMetricLaneSet(playerComparisonRows, "lapTime", "asc"),
    exhibitionSt: buildTopMetricLaneSet(playerComparisonRows, "exhibitionSt", "asc"),
    exhibitionTime: buildTopMetricLaneSet(playerComparisonRows, "exhibitionTime", "asc"),
    lapScore: buildTopMetricLaneSet(playerComparisonRows, "lapScore", "desc"),
    motor2Rate: buildTopMetricLaneSet(playerComparisonRows, "motor2Rate", "desc"),
    laneFirstRate: buildTopMetricLaneSet(playerComparisonRows, "laneFirstRate", "desc"),
    lane2RenRate: buildTopMetricLaneSet(playerComparisonRows, "lane2RenRate", "desc"),
    lane3RenRate: buildTopMetricLaneSet(playerComparisonRows, "lane3RenRate", "desc")
  }), [playerComparisonRows]);
  const kyoteiBiyoriFrontendDebug = useMemo(
    () => buildKyoteiBiyoriFrontendDebug({ data, playerComparisonRows }),
    [data, playerComparisonRows]
  );
  const safeTopRecommendedTickets = Array.isArray(predictionViewModel?.topRecommendedTickets)
    ? predictionViewModel.topRecommendedTickets
    : [];
  const topRecommendedTop10 = useMemo(
    () => safeTopRecommendedTickets.slice(0, 10),
    [safeTopRecommendedTickets]
  );
  const selectedBest4Tickets = useMemo(
    () => topRecommendedTop10.slice(0, 4),
    [topRecommendedTop10]
  );
  const recommendedShapeSource =
    data?.recommendedShape && typeof data.recommendedShape === "object"
      ? data.recommendedShape
      : prediction?.recommended_shape_debug && typeof prediction.recommended_shape_debug === "object"
        ? prediction.recommended_shape_debug
        : prediction?.snapshot_context?.recommended_shape_debug && typeof prediction.snapshot_context.recommended_shape_debug === "object"
          ? prediction.snapshot_context.recommended_shape_debug
          : null;
  const recommendedShape = recommendedShapeSource
    ? {
        shape: typeof recommendedShapeSource?.shape === "string" ? recommendedShapeSource.shape : null,
        expanded_tickets: Array.isArray(recommendedShapeSource?.expanded_tickets)
          ? recommendedShapeSource.expanded_tickets
          : [],
        reason_tags: Array.isArray(recommendedShapeSource?.reason_tags)
          ? recommendedShapeSource.reason_tags
          : [],
        concentration_metrics: recommendedShapeSource?.concentration_metrics || null,
        shape_generation_error: recommendedShapeSource?.shape_generation_error || null
      }
    : null;
  const recommendedShapeLabel = typeof recommendedShape?.shape === "string" && recommendedShape.shape
    ? recommendedShape.shape
    : null;
  const hitRateEnhancementDebug = useMemo(() => {
    if (prediction?.hit_rate_enhancement_json && typeof prediction.hit_rate_enhancement_json === "object") {
      return prediction.hit_rate_enhancement_json;
    }
    if (prediction?.snapshot_context?.hit_rate_enhancement_json && typeof prediction.snapshot_context.hit_rate_enhancement_json === "object") {
      return prediction.snapshot_context.hit_rate_enhancement_json;
    }
    if (recommendedShapeSource?.hit_rate_enhancement && typeof recommendedShapeSource.hit_rate_enhancement === "object") {
      return recommendedShapeSource.hit_rate_enhancement;
    }
    return null;
  }, [prediction, recommendedShapeSource]);
  const predictionDataUsageDebug = useMemo(() => {
    if (prediction?.prediction_data_usage && typeof prediction.prediction_data_usage === "object") {
      return prediction.prediction_data_usage;
    }
    if (prediction?.snapshot_context?.prediction_data_usage && typeof prediction.snapshot_context.prediction_data_usage === "object") {
      return prediction.snapshot_context.prediction_data_usage;
    }
    if (prediction?.confidence_scores?.prediction_data_usage && typeof prediction.confidence_scores.prediction_data_usage === "object") {
      return prediction.confidence_scores.prediction_data_usage;
    }
    return null;
  }, [prediction]);
  const topExactaFour = useMemo(() => {
    const enhancementExacta = Array.isArray(hitRateEnhancementDebug?.topExactaCandidates)
      ? hitRateEnhancementDebug.topExactaCandidates
      : Array.isArray(hitRateEnhancementDebug?.stage5_ticketing?.top_exacta_candidates)
        ? hitRateEnhancementDebug.stage5_ticketing.top_exacta_candidates
        : [];
    if (enhancementExacta.length > 0) {
      return enhancementExacta.slice(0, 4).map((row, index) => ({
        rank: row?.rank ?? index + 1,
        combo: row?.combo || "--",
        probability: Number(row?.probability),
        source: row?.source || "scenario_tree"
      }));
    }
    return exactaBets
      .map((row, index) => ({
        rank: index + 1,
        combo: row?.combo || "--",
        probability: Number(row?.prob ?? row?.estimated_hit_rate ?? null),
        source: "exacta_snapshot"
      }))
      .filter((row) => row.combo && row.combo !== "--")
      .slice(0, 4);
  }, [hitRateEnhancementDebug, exactaBets]);
  const roleSpecificBonusRows = useMemo(() => {
    const byLane = hitRateEnhancementDebug?.by_lane && typeof hitRateEnhancementDebug.by_lane === "object"
      ? hitRateEnhancementDebug.by_lane
      : {};
    const bonusByLane =
      hitRateEnhancementDebug?.stage2_dynamic?.finish_role_bonuses_by_lane &&
      typeof hitRateEnhancementDebug.stage2_dynamic.finish_role_bonuses_by_lane === "object"
        ? hitRateEnhancementDebug.stage2_dynamic.finish_role_bonuses_by_lane
        : {};
    return Object.keys({ ...byLane, ...bonusByLane })
      .map((laneKey) => {
        const lane = Number(laneKey);
        if (!Number.isInteger(lane)) return null;
        const laneRow = byLane[laneKey] || {};
        const bonuses = bonusByLane[laneKey] || laneRow.finish_role_bonuses || {};
        return {
          lane,
          firstPlaceBonus: bonuses?.firstPlaceBonus ?? null,
          secondPlaceBonus: bonuses?.secondPlaceBonus ?? null,
          thirdPlaceBonus: bonuses?.thirdPlaceBonus ?? null,
          exTimeLeftGapBonus: bonuses?.leftGapAttackSupport ?? laneRow?.ex_time_left_gap_advantage ?? null,
          turningBonus: bonuses?.turningAbilityDelta ?? null,
          straightBonus: bonuses?.straightLineDelta ?? null,
          styleBonus:
            bonuses?.styleRoleFit && typeof bonuses.styleRoleFit === "object"
              ? `1st ${formatMaybeNumber(bonuses.styleRoleFit.first, 2)} / 2nd ${formatMaybeNumber(bonuses.styleRoleFit.second, 2)} / 3rd ${formatMaybeNumber(bonuses.styleRoleFit.third, 2)}`
              : "--"
        };
      })
      .filter(Boolean)
      .sort((a, b) => a.lane - b.lane);
  }, [hitRateEnhancementDebug]);
  const finishRoleScoreRows = useMemo(() => {
    const byLane = hitRateEnhancementDebug?.by_lane && typeof hitRateEnhancementDebug.by_lane === "object"
      ? hitRateEnhancementDebug.by_lane
      : {};
    const primaryHeadLane =
      Number(hitRateEnhancementDebug?.stage4_opponents?.primary_head_lane) ||
      Number(Object.keys(byLane)[0] || 1);
    return Object.keys(byLane)
      .map((laneKey) => {
        const lane = Number(laneKey);
        if (!Number.isInteger(lane)) return null;
        const laneRow = byLane[laneKey] || {};
        const scores = laneRow.finish_role_scores || {};
        const compatibility = laneRow.compatibility_with_head?.[String(primaryHeadLane)] || {};
        const thirdExclusion = laneRow.third_place_exclusion || {};
        return {
          lane,
          firstPlaceScore: scores?.first_place_score ?? null,
          secondPlaceScore: scores?.second_place_score ?? null,
          thirdPlaceScore: scores?.third_place_score ?? null,
          secondCompatibility: compatibility?.second_bonus ?? null,
          thirdCompatibility: compatibility?.third_bonus ?? null,
          secondBreakdown: laneRow.second_place_bonus_breakdown || null,
          thirdBreakdown: laneRow.third_place_bonus_breakdown || null,
          likelyHeadSurvivalContext: scores?.likely_head_survival_context ?? null,
          attackCarryover: scores?.attack_but_not_win_carryover ?? null,
          thirdProxyUsed: scores?.third_place_proxy_used ?? null,
          residualTendency: scores?.residual_tendency ?? null,
          thirdExclusionReasons: Array.isArray(thirdExclusion?.reasons) ? thirdExclusion.reasons : [],
          thirdExclusionPenalty: thirdExclusion?.penalty ?? null
        };
      })
      .filter(Boolean)
      .sort((a, b) => a.lane - b.lane);
  }, [hitRateEnhancementDebug]);
  const upsetSupport = useMemo(() => {
    const support =
      hitRateEnhancementDebug?.upsetSupport && typeof hitRateEnhancementDebug.upsetSupport === "object"
        ? hitRateEnhancementDebug.upsetSupport
        : hitRateEnhancementDebug?.stage5_ticketing?.upset_support && typeof hitRateEnhancementDebug.stage5_ticketing.upset_support === "object"
          ? hitRateEnhancementDebug.stage5_ticketing.upset_support
          : null;
    if (!support) return null;
    return {
      classification: support?.classification || "stable",
      bigUpsetProbability: Number(support?.big_upset_probability ?? 0),
      upsetFormation: support?.upset_formation && typeof support.upset_formation === "object"
        ? support.upset_formation
        : { first_candidates: [], second_candidates: [], third_candidates: [], formation_string: null },
      mediumUpset: support?.medium_upset || { shown: false, exacta_pairs: [], trifecta_tickets: [] },
      bigUpset: support?.big_upset || { shown: false, exacta_pairs: [], trifecta_tickets: [] },
      chosenHeads: Array.isArray(support?.chosen_upset_heads) ? support.chosen_upset_heads : [],
      chosenExactaPairs: Array.isArray(support?.upset_exacta_pairs) ? support.upset_exacta_pairs : [],
      chosenTrifectaTickets: Array.isArray(support?.upset_trifecta_tickets) ? support.upset_trifecta_tickets : [],
      weakBoat1Factors: support?.weak_boat1_factors || {},
      strongAttackerFactors: support?.strong_attacker_factors || {},
      chaosFactors: support?.chaos_factors || {}
    };
  }, [hitRateEnhancementDebug]);

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
  const evaluationFilterOptions = useMemo(() => {
    return stats?.evaluation?.filter_options || {
      venues: [],
      formation_patterns: [],
      recommendation_levels: ["recommended", "caution", "not_recommended"],
      attack_scenarios: []
    };
  }, [stats]);
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
        fetchStatsData({
          ...evaluationFilters,
          only_participated: evaluationFilters.only_participated ? 1 : 0,
          only_recommended: evaluationFilters.only_recommended ? 1 : 0,
          only_boat1_escape_predicted: evaluationFilters.only_boat1_escape_predicted ? 1 : 0,
          only_outside_head_cases: evaluationFilters.only_outside_head_cases ? 1 : 0
        }),
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
    if (screen === "predict" && data && history.length === 0) {
      fetchHistoryData({ includeInvalidated: adminMode })
        .then((historyData) => setHistory(Array.isArray(historyData?.items) ? historyData.items : []))
        .catch(() => {});
    }
  }, [screen, data, history.length, adminMode]);

  useEffect(() => {
    if (screen === "journal") {
      loadJournal();
    }
  }, [screen]);

  useEffect(() => {
    if (screen === "hardRace") {
      loadRankings();
    }
  }, [screen, date, venueId]);

  useEffect(() => {
    if (!verificationNotice) return;
    const t = setTimeout(() => setVerificationNotice(""), 2200);
    return () => clearTimeout(t);
  }, [verificationNotice]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem("kyoteiapp_recent_races", JSON.stringify(recentRaceSelections.slice(0, 8)));
  }, [recentRaceSelections]);

  useEffect(() => {
    if (!learningRunNotice) return;
    const t = setTimeout(() => setLearningRunNotice(""), 2800);
    return () => clearTimeout(t);
  }, [learningRunNotice]);

  useEffect(() => {
    const onKeyDown = (e) => {
      if (screen !== "predict") return;
      if (!(e.altKey && !e.ctrlKey && !e.metaKey)) return;
      const k = String(e.key || "").toLowerCase();
      if (k === "enter") {
        e.preventDefault();
        if (!loading) {
          onFetch();
        }
      } else if (k === "arrowright") {
        e.preventDefault();
        setRaceNo((prev) => Math.min(12, Number(prev || 1) + 1));
      } else if (k === "arrowleft") {
        e.preventDefault();
        setRaceNo((prev) => Math.max(1, Number(prev || 1) - 1));
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [screen, loading, date, venueId, raceNo]);

  const onFetch = async () => {
    setLoading(true);
    setError("");
    setErrorDetails(null);
    try {
      const result = await fetchRaceData(date, venueId, raceNo);
      setData(result);
      const savedScores = result?.manualLapEvaluation?.scores_by_lane;
      setManualLapScores(savedScores && typeof savedScores === "object" ? savedScores : createManualLapDraft());
      setManualLapMemo(result?.manualLapEvaluation?.race_memo || "");
      setManualLapNotice("");
      setResultForm((prev) => ({ ...prev, raceId: result?.raceId || prev.raceId }));
      setRecentRaceSelections((prev) => {
        const next = [
          {
            date,
            venueId,
            venueName: VENUES.find((v) => v.id === Number(venueId))?.name || String(venueId),
            raceNo
          },
          ...prev.filter((row) => !(String(row?.date) === String(date) && Number(row?.venueId) === Number(venueId) && Number(row?.raceNo) === Number(raceNo)))
        ];
        return next.slice(0, 8);
      });
    } catch (e) {
      const details = getApiErrorDetails(e);
      setError(buildRaceApiErrorMessage(details, e.message || "Failed to fetch race data"));
      setErrorDetails(details);
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
      const venueName = VENUES.find((v) => v.id === Number(venueId))?.name || String(venueId);
      const [result, historyData] = await Promise.all([
        fetchHardRacePredictionData(date, venueId),
        fetchHistoryData({ includeInvalidated: adminMode }).catch(() => null)
      ]);
      if (historyData && Array.isArray(historyData?.items)) {
        setHistory(historyData.items);
      }
      const historyMap = buildHardRaceHistoryMap(
        Array.isArray(historyData?.items) ? historyData.items : history,
        date,
        venueId
      );
      const rows = Array.isArray(result)
        ? result.map((entry) => {
            let baseRow;
            try {
              baseRow = buildHardRaceScreeningRow(entry, venueName);
            } catch (rowError) {
              console.error("[HardRace][row_build_failed]", {
                raceNo: entry?.raceNo ?? null,
                message: rowError?.message || String(rowError || "unknown_error"),
                entry
              });
              baseRow = finalizeHardRaceContractRow({
                raceNo: entry?.raceNo ?? null,
                venueName,
                status: "DATA_ERROR",
                finalStatus: "DATA_ERROR",
                data_status: "DATA_ERROR",
                recommendation: "DATA_ERROR",
                buyStyleRecommendation: "DATA_ERROR",
                decision: "DATA_ERROR",
                decision_reason: rowError?.message || "hard_race_row_build_failed",
                errors: [rowError?.message || "hard_race_row_build_failed"],
                missing_fields: [
                  "hard_race_score",
                  "boat1_anchor_score",
                  "boat1_escape_trust",
                  "box_234_fit_score",
                  "opponent_234_fit",
                  "makuri_risk",
                  "outside_break_risk",
                  "fixed1234_total_probability",
                  "top4_fixed1234_probability",
                  "fixed1234_shape_concentration",
                  "suggested_shape"
                ],
                fetchFailed: !entry?.ok,
                screeningDebug: {
                  race_fetch_success: !!entry?.ok,
                  parse_success: false,
                  score_success: false,
                  data_status: "DATA_ERROR",
                  final_status: "DATA_ERROR",
                  missing_fields: [
                    "hard_race_score",
                    "boat1_anchor_score",
                    "boat1_escape_trust",
                    "box_234_fit_score",
                    "opponent_234_fit",
                    "makuri_risk",
                    "outside_break_risk",
                    "fixed1234_total_probability",
                    "top4_fixed1234_probability",
                    "fixed1234_shape_concentration",
                    "suggested_shape"
                  ]
                }
              });
            }
            const key = makeRaceKey({
              race_id: baseRow?.sourceData?.raceId || null,
              race_date: date,
              venue_id: venueId,
              race_no: baseRow?.raceNo
            });
            const review = historyMap.get(key) || null;
            const actualResult = review?.actualResult || null;
            const inSixTargetSet = actualResult
              ? safeSetHas(new Set(Object.keys(baseRow?.fixed1234Matrix || {})), actualResult)
              : null;
            const reviewMissType = actualResult && !inSixTargetSet
              ? baseRow.finalStatus === "SKIP" && String(actualResult).startsWith("1-")
                ? "boat1 survived but fixed1234 model too low"
                : actualResult && /^1-[234]-[234]$/.test(actualResult)
                  ? "2/3/4 underneath fit stronger than predicted"
                  : /^1-/.test(String(actualResult))
                    ? "inside survival underweighted"
                    : "outside danger or attack pressure realized"
              : actualResult && inSixTargetSet && baseRow.finalStatus === "SKIP"
                ? "skip may have been too pessimistic"
                : null;
            return {
              ...baseRow,
              actualResult,
              actualInsideSixTarget: inSixTargetSet,
              reviewMissType
            };
          })
        : [];
      rows.forEach((row) => {
        console.info("[HardRace][row_debug]", {
          race: `${date || "-"} ${row?.venueName || venueName} ${row?.raceNo ?? row?.race_no ?? "-"}R`,
          raw_inputs: row?.screeningDebug?.raw_inputs || null,
          normalized_inputs: row?.screeningDebug?.normalized_inputs || null,
          features: row?.screeningDebug?.features || null,
          scores: row?.screeningDebug?.scores || null,
          response_payload: row?.screeningDebug?.response_payload || null,
          missing_fields: row?.missing_fields || [],
          data_status: row?.data_status || null,
          decision_reason: row?.decision_reason || null
        });
      });
      rows.forEach((row) => {
        if ((Array.isArray(row?.missing_fields) && row.missing_fields.length > 0) || (Array.isArray(row?.errors) && row.errors.length > 0)) {
          console.warn("[HardRace][row_missing_fields]", {
            raceNo: row?.raceNo ?? row?.race_no ?? null,
            status: row?.status || row?.finalStatus || "UNAVAILABLE",
            missing_fields: row?.missing_fields || [],
            errors: row?.errors || []
          });
        }
      });
      const candidateRank = (row) => (
        row?.buyStyleRecommendation === "BUY-4" ? 4
          : row?.buyStyleRecommendation === "BUY-6" ? 3
            : row?.buyStyleRecommendation === "BORDERLINE" ? 2
              : 0
      );
      rows.sort((a, b) => {
        const candidateDiff = candidateRank(b) - candidateRank(a);
        if (candidateDiff !== 0) return candidateDiff;
        const tierRank = (value) => (value === "A" ? 4 : value === "B" ? 3 : value === "SKIP" ? 2 : 0);
        const tierDiff = tierRank(b?.hardRaceRank) - tierRank(a?.hardRaceRank);
        if (tierDiff !== 0) return tierDiff;
        const boxHitDiff = (Number(b?.boxHitScore) || -1) - (Number(a?.boxHitScore) || -1);
        if (Math.abs(boxHitDiff) > 0.00001) return boxHitDiff;
        const statusRank = (value) => (value === "BUY" ? 4 : value === "BORDERLINE" ? 3 : value === "SKIP" ? 2 : value === "DATA_ERROR" ? 1 : 0);
        const statusDiff = statusRank(b?.finalStatus) - statusRank(a?.finalStatus);
        if (statusDiff !== 0) return statusDiff;
        const buyStyleRank = (value) => (value === "BUY-6" ? 4 : value === "BUY-4" ? 3 : value === "BORDERLINE" ? 2 : value === "SKIP" ? 1 : 0);
        const buyStyleDiff = buyStyleRank(b?.buyStyleRecommendation) - buyStyleRank(a?.buyStyleRecommendation);
        if (buyStyleDiff !== 0) return buyStyleDiff;
        const scoreDiff = (Number(b?.fixed1234TotalProbability) || -1) - (Number(a?.fixed1234TotalProbability) || -1);
        if (Math.abs(scoreDiff) > 0.00001) return scoreDiff;
        const hardDiff = (Number(b?.hardRaceScore) || -1) - (Number(a?.hardRaceScore) || -1);
        if (Math.abs(hardDiff) > 0.00001) return hardDiff;
        return (Number(a?.raceNo) || 99) - (Number(b?.raceNo) || 99);
      });
      const candidateRows = rows.filter((row) => ["BUY-4", "BUY-6", "BORDERLINE"].includes(String(row?.buyStyleRecommendation || "")));
      const adoptionTargetCount = candidateRows.length >= 5 ? 5 : candidateRows.length >= 3 ? candidateRows.length : candidateRows.length;
      const adoptionKeys = new Set(
        candidateRows
          .slice()
          .sort((a, b) => (Number(b?.boxHitScore) || -1) - (Number(a?.boxHitScore) || -1))
          .slice(0, adoptionTargetCount)
          .map((row) => `${row?.venueName || "-"}-${row?.raceNo || "-"}`)
      );
      setRankingsData(rows.map((row, index) => ({
        ...row,
        candidateEligible: ["BUY-4", "BUY-6", "BORDERLINE"].includes(String(row?.buyStyleRecommendation || "")),
        adoptionTargetCount,
        operationalSortRank: index + 1,
        adoptedForOperation: adoptionKeys.has(`${row?.venueName || "-"}-${row?.raceNo || "-"}`)
      })));
      const reviewedRows = rows.filter((row) => row.actualResult);
      const hitInsideSix = reviewedRows.filter((row) => row.actualInsideSixTarget === true).length;
      const falseSkips = reviewedRows.filter((row) => row.actualInsideSixTarget === true && row.finalStatus === "SKIP").length;
      const buy6Rows = reviewedRows.filter((row) => row.buyStyleRecommendation === "BUY-6");
      const buy4Rows = reviewedRows.filter((row) => row.buyStyleRecommendation === "BUY-4");
      const borderlinePlusRows = reviewedRows.filter((row) => ["BUY-4", "BUY-6", "BORDERLINE"].includes(String(row?.buyStyleRecommendation || "")));
      const skipRows = reviewedRows.filter((row) => row.buyStyleRecommendation === "SKIP");
      const buy6InsideHits = buy6Rows.filter((row) => row.actualInsideSixTarget === true).length;
      const buy4Top4Hits = buy4Rows.filter((row) => {
        const actual = String(row?.actualResult || "");
        return actual && Array.isArray(row?.fixed1234Top4) && row.fixed1234Top4.some((item) => item?.combo === actual);
      }).length;
      const borderlinePlusInsideHits = borderlinePlusRows.filter((row) => row.actualInsideSixTarget === true).length;
      const skipInsideHits = skipRows.filter((row) => row.actualInsideSixTarget === true).length;
      const headHitRows = reviewedRows.filter((row) => {
        const actualHead = Number(String(row?.actualResult || "").split("-")[0] || NaN);
        const ranking = Array.isArray(row?.head_candidate_ranking) && row.head_candidate_ranking.length > 0
          ? row.head_candidate_ranking
          : [1, 2, 3, 4, 5, 6].map((lane) => ({ lane, probability: Number(row?.[`head_prob_${lane}`] || 0) }))
            .sort((a, b) => (b?.probability || 0) - (a?.probability || 0));
        return actualHead && ranking[0]?.lane === actualHead;
      }).length;
      const outsideActualRows = reviewedRows.filter((row) => /(^5-|^6-|-[56]-)/.test(String(row?.actualResult || "")));
      const outsideDetectedHits = outsideActualRows.filter((row) =>
        Number(row?.outsideHeadRisk || 0) >= 0.22 ||
        Number(row?.outside2ndRisk || 0) >= 0.28 ||
        Number(row?.outsideBoxBreakRisk || 0) >= 0.3
      ).length;
      setHardRaceCalibration({
        scopeKey: `selected:${date}:${venueId}`,
        scopeLabel: `${date} / ${venueName}`,
        reviewedCount: reviewedRows.length,
        insideSixHitCount: hitInsideSix,
        falseSkipCount: falseSkips,
        buy6Count: buy6Rows.length,
        buy4Count: buy4Rows.length,
        borderlinePlusCount: borderlinePlusRows.length,
        skipCount: skipRows.length,
        buy6InsideHits,
        buy4Top4Hits,
        borderlinePlusInsideHits,
        skipInsideHits,
        headHitCount: headHitRows,
        outsideActualCount: outsideActualRows.length,
        outsideDetectedHits,
        insideSixHitRate: reviewedRows.length ? Number(((hitInsideSix / reviewedRows.length) * 100).toFixed(1)) : null,
        falseSkipRate: reviewedRows.length ? Number(((falseSkips / reviewedRows.length) * 100).toFixed(1)) : null,
        buy6InsideHitRate: buy6Rows.length ? Number(((buy6InsideHits / buy6Rows.length) * 100).toFixed(1)) : null,
        buy4Top4HitRate: buy4Rows.length ? Number(((buy4Top4Hits / buy4Rows.length) * 100).toFixed(1)) : null,
        borderlinePlusInsideHitRate: borderlinePlusRows.length ? Number(((borderlinePlusInsideHits / borderlinePlusRows.length) * 100).toFixed(1)) : null,
        skipInsideRate: skipRows.length ? Number(((skipInsideHits / skipRows.length) * 100).toFixed(1)) : null,
        headHitRate: reviewedRows.length ? Number(((headHitRows / reviewedRows.length) * 100).toFixed(1)) : null,
        outsideDetectionRate: outsideActualRows.length ? Number(((outsideDetectedHits / outsideActualRows.length) * 100).toFixed(1)) : null
      });
    } catch (e) {
      setRankingsError(e.message || "Failed to fetch hard race prediction");
    } finally {
      setRankingsLoading(false);
    }
  };

  const filteredHardRaceRows = useMemo(() => {
    if (hardRaceTierFilter === "ALL") return rankingsData;
    return rankingsData.filter((row) => String(row?.hardRaceRank || "UNAVAILABLE") === hardRaceTierFilter);
  }, [rankingsData, hardRaceTierFilter]);

  const hardRaceKpiCards = useMemo(
    () => buildHardRaceKpiCards(hardRaceCalibration, {
      scopeLabel: hardRaceCalibration?.scopeLabel || `${date} / ${VENUES.find((v) => v.id === Number(venueId))?.name || "-"}`
    }),
    [date, hardRaceCalibration, venueId]
  );

  const onOpenRecommendation = async (row) => {
    const nextVenueId = Number(row?.venueId);
    const nextRaceNo = Number(row?.raceNo);
    if (!Number.isInteger(nextVenueId) || !Number.isInteger(nextRaceNo)) return;

    setVenueId(nextVenueId);
    setRaceNo(nextRaceNo);
    setScreen("predict");
    setLoading(true);
    setError("");
    setErrorDetails(null);
    try {
      const result = await fetchRaceData(date, nextVenueId, nextRaceNo);
      setData(result);
      const savedScores = result?.manualLapEvaluation?.scores_by_lane;
      setManualLapScores(savedScores && typeof savedScores === "object" ? savedScores : createManualLapDraft());
      setManualLapMemo(result?.manualLapEvaluation?.race_memo || "");
      setManualLapNotice("");
      setResultForm((prev) => ({ ...prev, raceId: result?.raceId || prev.raceId }));
    } catch (e) {
      const details = getApiErrorDetails(e);
      setError(buildRaceApiErrorMessage(details, e.message || "Failed to fetch race data"));
      setErrorDetails(details);
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
            <button className={screen === "hardRace" ? "tab on" : "tab"} onClick={() => setScreen("hardRace")}>Hard Race Prediction</button>
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
              <div className="predict-quickbar">
                <div className="quick-chip-group">
                  <span className="quick-label">Quick Venue</span>
                  {quickVenueOptions.map((venue) => (
                    <button
                      key={`quick-venue-${venue.id}`}
                      className={`quick-chip ${Number(venueId) === Number(venue.id) ? "active" : ""}`}
                      onClick={() => setVenueId(Number(venue.id))}
                    >
                      {venue.name}
                    </button>
                  ))}
                </div>
                <div className="quick-chip-group">
                  <span className="quick-label">Recent</span>
                  {recentRaceSelections.slice(0, 5).map((row, idx) => (
                    <button
                      key={`recent-race-${idx}-${row.date}-${row.venueId}-${row.raceNo}`}
                      className="quick-chip"
                      onClick={() => {
                        setDate(row.date || localDateKey());
                        setVenueId(Number(row.venueId || 1));
                        setRaceNo(Number(row.raceNo || 1));
                      }}
                    >
                      {row.venueName} {row.raceNo}R
                    </button>
                  ))}
                  <span className="quick-shortcut">Alt+Enter fetch / Alt+←→ race</span>
                </div>
              </div>
            </section>

            {error && (
              <div className="error-banner">
                <div>{error}</div>
                {errorDetails ? (
                  <div className="kv-list" style={{ marginTop: 8 }}>
                    <div className="kv-row"><span>error type</span><strong>{getRaceApiErrorLabel(errorDetails)}</strong></div>
                    <div className="kv-row"><span>status</span><strong>{errorDetails.status ?? "-"}</strong></div>
                    <div className="kv-row"><span>code</span><strong>{errorDetails.code || "-"}</strong></div>
                    <div className="kv-row"><span>where</span><strong>{errorDetails.where || "-"}</strong></div>
                    <div className="kv-row"><span>route</span><strong>{errorDetails.route || "-"}</strong></div>
                    <div className="kv-row"><span>url</span><strong>{errorDetails.url || "-"}</strong></div>
                  </div>
                ) : null}
                {String(errorDetails?.code || "").toUpperCase() === "SNAPSHOT_MISSING" ? (
                  <div className="kv-list" style={{ marginTop: 10 }}>
                    <div className="kv-row"><span>案内</span><strong>事前データ未生成</strong></div>
                    <div className="kv-row"><span>このレース</span><strong>{snapshotGenerationHints[0]}</strong></div>
                    <div className="kv-row"><span>この場の全レース</span><strong>{snapshotGenerationHints[1]}</strong></div>
                  </div>
                ) : null}
              </div>
            )}
            {journalNotice && <div className="notice-banner">{journalNotice}</div>}

            {!data ? (
              <section className="card empty-state">レースを取得すると予想ダッシュボードを表示します。</section>
            ) : (
              <>
                <section className="card summary-card premium-topline-card">
                  <div className="metric-grid compact">
                    <div className="metric-item">
                      <span>Date</span>
                      <strong>{race.date || date}</strong>
                    </div>
                    <div className="metric-item">
                      <span>Race Name</span>
                      <strong>{race.raceName || `${venueName} ${race.raceNo ?? raceNo}R`}</strong>
                    </div>
                  </div>
                </section>

                {safeArray(playerComparisonRows).length > 0 ? (
                  <RenderGuard>
                  <section className="card summary-card premium-player-panel">
                    <div className="premium-card-head">
                      <div>
                        <p className="eyebrow">Step 1</p>
                        <h2>Player / Boat Data List</h2>
                      </div>
                    </div>
                    <div className="table-wrap premium-player-table-wrap">
                      <table className="premium-player-table">
                        <thead>
                          <tr>
                            <th>Boat</th>
                            <th>Entry</th>
                            <th>Player</th>
                            <th>F</th>
                            <th>Lap Time</th>
                            <th>Ex ST</th>
                            <th>Ex Time</th>
                            <th>Motor 2-ren</th>
                          </tr>
                        </thead>
                        <tbody>
                          {safeArray(playerComparisonRows).map((row, idx) => (
                            <tr key={`player-compare-${row?.boatNumber ?? row?.lane ?? idx}`}>
                              <td>
                                <div className="player-boat-cell">
                                  <span className={`combo-dot ${BOAT_META[row?.boatNumber ?? row?.lane]?.className || ""}`}>{row?.boatNumber ?? row?.lane ?? "--"}</span>
                                </div>
                              </td>
                              <td>
                                <div className="player-boat-cell">
                                  <span className={`combo-dot ${BOAT_META[row?.actualLane]?.className || ""}`}>{row?.actualLane ?? "--"}</span>
                                </div>
                              </td>
                              <td>
                                <div className="player-name-cell">
                                  <strong>{row?.name || "-"}</strong>
                                  {row?.actualLaneConfirmed
                                    ? row?.courseChanged
                                      ? <div className="muted">Moved from boat {row?.boatNumber} to entry {row?.actualLane}</div>
                                      : <div className="muted">No course change</div>
                                    : <div className="muted">Actual entry not confirmed. Using base/predicted order</div>}
                                </div>
                              </td>
                              <td>
                                <span className={`f-count-badge ${Number(row?.fCount) > 0 ? "has-f" : ""}`}>F{row?.fCount ?? "--"}</span>
                              </td>
                              <td className={safeSetHas(playerMetricLeaders?.lapTime, row?.actualLane ?? row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.lapTime, 2)}</td>
                              <td className={safeSetHas(playerMetricLeaders?.exhibitionSt, row?.actualLane ?? row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.exhibitionSt, 2)}</td>
                              <td className={safeSetHas(playerMetricLeaders?.exhibitionTime, row?.actualLane ?? row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.exhibitionTime, 2)}</td>
                              <td className={safeSetHas(playerMetricLeaders?.motor2Rate, row?.actualLane ?? row?.lane) ? "metric-hot" : ""}>{formatComparisonValue(row?.motor2ren, 2)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <p className="muted strategy-line">
                      Rows are ordered by actual entry lane when course movement occurs. Lane-score columns are temporarily hidden until raw 枠別情報 parsing is re-verified against the source table.
                    </p>
                    {!hasRenderableKyoteiBiyoriData(playerComparisonRows, data) ? (
                      <p className="muted strategy-line">
                        Supplemental kyoteibiyori data unavailable. Base race data is still loaded.
                      </p>
                    ) : null}
                  </section>
                  </RenderGuard>
                ) : null}

                <RenderGuard>
                <section className="card summary-card premium-start-top-card">
                  <div className="premium-card-head">
                    <div>
                      <p className="eyebrow">Step 2</p>
                      <h2>Start / Entry Layout</h2>
                    </div>
                  </div>
                  <div className="kv-list" style={{ marginBottom: 12 }}>
                    <div className="kv-row"><span>Actual entry flow</span><strong><LanePills lanes={displayActualEntryOrder} /></strong></div>
                    <div className="kv-row"><span>Base / predicted order</span><strong><LanePills lanes={predictedEntryOrder} /></strong></div>
                    <div className="kv-row"><span>Course movement</span><strong>{entryPipelineDebug.confirmedActualEntry ? (entryChanged ? "reordered by actual entry" : "none") : "fallback to base/predicted order"}</strong></div>
                  </div>
                  <StartExhibitionDisplay startDisplay={startDisplay} />
                  <div className="kv-list" style={{ marginTop: 12 }}>
                    <div className="kv-row"><span>Predicted entry</span><strong><LanePills lanes={predictedEntryOrder} /></strong></div>
                    <div className="kv-row"><span>Actual entry</span><strong><LanePills lanes={displayActualEntryOrder} /></strong></div>
                    <div className="kv-row"><span>Entry change</span><strong>{entryPipelineDebug.confirmedActualEntry ? (entryChanged ? "changed" : "none") : "fallback"}</strong></div>
                    <div className="kv-row"><span>Formation</span><strong>{formationPatternLabel || "-"}</strong></div>
                    <div className="kv-row"><span>Attack scenario</span><strong>{attackScenarioLabel || "-"}</strong></div>
                  </div>
                  <div className="kv-list" style={{ marginTop: 12 }}>
                    <div className="kv-row"><span>Entry source</span><strong>{entryPipelineDebug.authoritativeSource || "--"}</strong></div>
                    <div className="kv-row"><span>Validation</span><strong>{entryPipelineDebug.validationPassed ? "passed" : "failed"}</strong></div>
                    <div className="kv-row"><span>Fallback</span><strong>{entryPipelineDebug.fallbackUsed ? (entryPipelineDebug.fallbackReason || "used") : "not used"}</strong></div>
                    <div className="kv-row"><span>Source text</span><strong><code>{formatDebugRawValue(entryPipelineDebug.rawActualEntrySourceText)}</code></strong></div>
                    <div className="kv-row"><span>Usable fields</span><strong>{entrySupplementalDebug.usable.length ? entrySupplementalDebug.usable.join(", ") : "--"}</strong></div>
                    <div className="kv-row"><span>Skipped fields</span><strong>{entrySupplementalDebug.skipped.length ? entrySupplementalDebug.skipped.join(", ") : "--"}</strong></div>
                  </div>
                  <p className="muted strategy-line">
                    {entryPipelineDebug.confirmedActualEntry
                      ? data?.source?.kyotei_biyori?.ok
                        ? "official pre-race + kyoteibiyori merged; rows above follow validated actual entry"
                        : "official pre-race info; rows above follow validated actual entry"
                      : data?.source?.kyotei_biyori?.ok
                        ? "actual entry validation failed, so UI stays on base/predicted order while supplemental data remains attached to each boat"
                        : "actual entry validation failed, so UI stays on base/predicted order"
                    }
                  </p>
                  {!data?.source?.kyotei_biyori?.ok ? (
                    <p className="muted strategy-line">
                      {sourceMeta?.cache?.fallback === "db_snapshot"
                        ? "official fetch unavailable; using saved snapshot"
                        : sourceMeta?.cache?.hit
                          ? "official pre-race from backend cache"
                          : "official pre-race info"}
                    </p>
                  ) : null}
                </section>
                </RenderGuard>

                <RenderGuard>
                <article className={`card summary-card premium-ticket-card ${!isRecommendedRace ? "deemphasized" : ""}`}>
                  <div className="premium-card-head">
                    <div>
                      <p className="eyebrow">Prediction Tab</p>
                      <h2>Pure Top 6 Prediction</h2>
                      <p className="muted strategy-line prediction-tab-note">
                        予想タブ = 毎回6点出す純予想 / Hard Race Prediction = 1-234-234固定買い用
                      </p>
                    </div>
                  </div>
                  <div className="summary-inline-meta prediction-summary-meta">
                    <span>main {pureTop6Prediction?.main_ticket?.combo || "--"}</span>
                    <span>top6 coverage {formatPercentDisplay(pureTop6Prediction?.top6_coverage)}</span>
                    <span>head #1 {pureHeadRanking[0] ? `${pureHeadRanking[0].lane}号艇` : "--"}</span>
                    <span>chaos {getPredictionChaosLabel(pureTop6Prediction)}</span>
                  </div>
                  <section className="prediction-summary-panel">
                  <div className="hardrace-meta-grid prediction-summary-grid" style={{ marginTop: 12 }}>
                    <article className="hardrace-meta-card primary">
                      <span>本命買い目</span>
                      <strong>{pureTop6Prediction?.main_ticket?.combo || "--"}</strong>
                      <small>{pureTop6Prediction?.main_ticket ? `${formatPercentDisplay(pureTop6Prediction.main_ticket.probability)} / 最上位` : "未計算"}</small>
                    </article>
                    <article className="hardrace-meta-card primary">
                      <span>上位6点合計</span>
                      <strong>{formatPercentDisplay(pureTop6Prediction?.top6_coverage)}</strong>
                      <small>この6点で拾える推定カバー率</small>
                    </article>
                    <article className="hardrace-meta-card">
                      <span>頭候補1位</span>
                      <strong>{pureHeadRanking[0] ? `${pureHeadRanking[0].lane}号艇` : "--"}</strong>
                      <small>{pureHeadRanking[0] ? formatPercentDisplay(pureHeadRanking[0].probability) : "未計算"}</small>
                    </article>
                    <article className="hardrace-meta-card primary">
                      <span>1頭信頼度</span>
                      <strong>{formatPercentDisplay(pureHeadTrust)}</strong>
                      <small>頭候補1位の1着率</small>
                    </article>
                    <article className={`hardrace-meta-card ${getPredictionChaosTone(getPredictionChaosLabel(pureTop6Prediction))}`}>
                      <span>chaos_level</span>
                      <strong>{getPredictionChaosLabel(pureTop6Prediction)}</strong>
                      <small>{formatPercentDisplay(pureTop6Prediction?.chaos_level)}</small>
                    </article>
                    <article className="hardrace-meta-card">
                      <span>confidence</span>
                      <strong>{formatPercentDisplay(pureTop6Prediction?.confidence)}</strong>
                      <small>6点予想のまとまり / {predictionConfidenceState}</small>
                    </article>
                  </div>
                  <div className="prediction-role-strip">
                    <span className="hardrace-tag picked">予想タブ: 毎回6点を返す純予想</span>
                    <span className="hardrace-tag top4">Hard Race Prediction: 1-234-234固定買い用</span>
                    <span className={`status-pill ${getPredictionConfidenceClass(predictionConfidenceState)}`}>{`Confidence ${predictionConfidenceState}`}</span>
                    <span className={`status-pill ${getPredictionChaosTone(getPredictionChaosLabel(pureTop6Prediction))}`}>{`Chaos ${getPredictionChaosLabel(pureTop6Prediction)}`}</span>
                  </div>
                  </section>
                  <div className="hardrace-section-grid" style={{ marginTop: 16 }}>
                    <div className="hardrace-block">
                      <div className="hardrace-block-head">
                        <div>
                          <strong>上位6点</strong>
                          <p className="muted">本命2点 / 対抗2点 / 抑え2点。確率順で3秒判断できる形にまとめています。</p>
                        </div>
                      </div>
                      <div className="prediction-coverage-callout">
                        <span className="prediction-coverage-label">Top6 Coverage</span>
                        <strong>{formatPercentDisplay(pureTop6Prediction?.top6_coverage)}</strong>
                      </div>
                      {pureTop6Rows.length > 0 ? (
                        <div className="prediction-tier-grid">
                          {["本命", "対抗", "抑え"].map((tier) => (
                            <section className={`prediction-tier-block tier-${tier === "本命" ? "main" : tier === "対抗" ? "challenge" : "cover"}`} key={`tier-${tier}`}>
                              <div className="prediction-tier-head">
                                <strong>{tier}</strong>
                                <span>{tier === "本命" ? "main" : tier === "対抗" ? "challenge" : "cover"}</span>
                              </div>
                              <div className="hardrace-prob-list">
                                {(pureTop6Groups[tier] || []).map((item, index) => (
                                  <div className={`hardrace-prob-item prediction-prob-item tier-${item.tierKey}`} key={`pure-top6-${tier}-${item.combo}-${index}`}>
                                    <div className="hardrace-prob-meta">
                                      <strong>{index + 1 + (tier === "本命" ? 0 : tier === "対抗" ? 2 : 4)}位 {item.combo}</strong>
                                      <span>{formatPercentDisplay(item.probability)}</span>
                                    </div>
                                    <div className="hardrace-prob-bar">
                                      <div className="hardrace-prob-fill" style={{ width: `${item.width}%` }} />
                                    </div>
                                    <div className="hardrace-prob-tags">
                                      <span className={`hardrace-tag ${item.tier === "本命" ? "picked" : item.tier === "対抗" ? "top4" : "top2"}`}>{item.tier}</span>
                                      <span className="hardrace-tag">{`coverage share ${formatPercentDisplay(item.probability)}`}</span>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </section>
                          ))}
                        </div>
                      ) : (
                        <div className="hardrace-prob-list">
                          <p className="muted">上位6点は未計算です。</p>
                        </div>
                      )}
                    </div>
                    <div className="hardrace-block">
                      <div className="hardrace-block-head">
                        <div>
                          <strong>頭候補ランキング</strong>
                          <p className="muted">上位3艇を強調。1号艇が弱い時は代替頭候補がすぐ見えます。</p>
                        </div>
                      </div>
                      <div className="hardrace-head-ranking-list">
                        {pureHeadRanking.length > 0 ? pureHeadRanking.map((item, index) => (
                          <article className={`hardrace-head-rank-row ${index < 3 ? "top3" : ""} ${index === 0 ? "leader" : ""}`} key={`pure-head-${item.lane}`}>
                            <div className="hardrace-head-rank-main">
                              <span className="rank-pill">#{index + 1}</span>
                              <strong>{item.lane}号艇</strong>
                              {item.lane === 1 && index > 0 ? <span className="hardrace-tag risk">1号艇弱め</span> : null}
                              {index === 0 && item.lane !== 1 ? <span className="hardrace-tag picked">代替頭候補</span> : null}
                            </div>
                            <div className="hardrace-head-rank-side">
                              <div className="hardrace-prob-bar compact">
                                <div className="hardrace-prob-fill" style={{ width: `${Math.max(10, (Number(item?.probability) || 0) * 100)}%` }} />
                              </div>
                              <span>{formatPercentDisplay(item?.probability)}</span>
                            </div>
                          </article>
                        )) : (
                          <p className="muted">頭候補ランキングは未計算です。</p>
                        )}
                      </div>
                    </div>
                    <div className="hardrace-block">
                      <div className="hardrace-block-head">
                        <div>
                          <strong>chaos / confidence</strong>
                          <p className="muted">波乱度と予想のまとまりを先に確認できます。</p>
                        </div>
                      </div>
                      <div className="hardrace-meta-grid">
                        <article className={`hardrace-meta-card ${getPredictionChaosTone(getPredictionChaosLabel(pureTop6Prediction))}`}>
                          <span>Chaos Badge</span>
                          <strong>{getPredictionChaosLabel(pureTop6Prediction)}</strong>
                          <small>{formatPercentDisplay(pureTop6Prediction?.chaos_level)}</small>
                        </article>
                        <article className="hardrace-meta-card">
                          <span>Confidence Score</span>
                          <strong>{formatPercentDisplay(pureTop6Prediction?.confidence)}</strong>
                          <small>上位6点の収束度</small>
                        </article>
                        <article className={`hardrace-meta-card ${getPredictionConfidenceClass(predictionConfidenceState)}`}>
                          <span>Inference Confidence</span>
                          <strong>{predictionConfidenceState}</strong>
                          <small>snapshot 完成度</small>
                        </article>
                        <article className="hardrace-meta-card">
                          <span>Top6 vs Head</span>
                          <strong>{formatPercentDisplay(pureTop6Prediction?.top6_coverage)}</strong>
                          <small>{pureHeadRanking[0] ? `head #1 ${pureHeadRanking[0].lane}号艇 ${formatPercentDisplay(pureHeadRanking[0].probability)}` : "未計算"}</small>
                        </article>
                      </div>
                    </div>
                  </div>
                </article>
                </RenderGuard>

                <RenderGuard>
                <article className={`card summary-card premium-ticket-card top-ranked-card ${!isRecommendedRace ? "deemphasized" : ""}`}>
                  <div className="premium-card-head">
                    <div>
                      <p className="eyebrow">Step 3</p>
                      <h2>Final Top Recommended Tickets</h2>
                    </div>
                  </div>
                  <div className="summary-inline-meta">
                    <span>{topRecommendedTop10.length} / 10</span>
                    <span>sorted by estimated hit rate</span>
                  </div>
                  {recommendedShapeLabel ? (
                    <p className="muted strategy-line">Recommended Shape: {recommendedShapeLabel}</p>
                  ) : null}
                  <div className="ticket-stack compact-list">
                    {topRecommendedTop10.map((row, idx) => (
                      <div key={`top-ticket-${row?.ticket_type || "trifecta"}-${row?.ticket || idx}`} className="premium-ticket-row primary">
                        <div className="ticket-mainline">
                          <span className="rank-pill">#{row?.rank ?? idx + 1}</span>
                          <span className="ticket-type ticket-type-inline ttype-main">3連単</span>
                          <strong><ComboBadge combo={row?.ticket || "--"} /></strong>
                        </div>
                        <div className="ticket-meta">
                          <span className={`ticket-type ${getTicketTypeClass(row?.recommendation_tier === "cover" ? "backup" : row?.recommendation_tier)}`}>
                            {row?.recommendation_tier || "main"}
                          </span>
                          <span>hit {formatMaybeNumber((Number(row?.estimated_hit_rate) || 0) * 100, 1)}%</span>
                        </div>
                      </div>
                    ))}
                  </div>
                  {!isRecommendedRace ? (
                    <p className="muted strategy-line">Low-confidence race. Treat this order as reference only.</p>
                  ) : null}
                </article>
                </RenderGuard>

                <RenderGuard>
                <article className={`card summary-card premium-ticket-card subtle ${!isRecommendedRace ? "deemphasized" : ""}`}>
                  <div className="premium-card-head">
                    <div>
                      <p className="eyebrow">Step 4</p>
                      <h2>Selected Best 4 Tickets</h2>
                    </div>
                  </div>
                  <div className="summary-inline-meta">
                    <span>{selectedBest4Tickets.length} items</span>
                    <span>highest-priority subset from the top recommendation list</span>
                  </div>
                  <div className="list-stack compact-list">
                    {selectedBest4Tickets.map((row, idx) => (
                      <div key={`best4-${row?.ticket || idx}`} className="list-stack">
                        <div className="list-row list-row-actions premium-ticket-row primary">
                          <strong><ComboBadge combo={row?.ticket || "--"} /></strong>
                          <span className={`ticket-type ${getTicketTypeClass(row?.recommendation_tier === "cover" ? "backup" : row?.recommendation_tier)}`}>
                            {row?.recommendation_tier || "main"}
                          </span>
                          <span>hit {formatMaybeNumber((Number(row?.estimated_hit_rate) || 0) * 100, 1)}%</span>
                          <span>#{row?.rank ?? idx + 1}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </article>
                </RenderGuard>

                {topExactaFour.length > 0 ? (
                  <RenderGuard>
                  <article className="card summary-card premium-ticket-card subtle">
                    <div className="premium-card-head">
                      <div>
                        <p className="eyebrow">Support</p>
                        <h2>Top Exacta 4</h2>
                      </div>
                    </div>
                    <div className="summary-inline-meta">
                      <span>{topExactaFour.length} pairs</span>
                      <span>likely order skeleton for the race</span>
                    </div>
                    <div className="ticket-stack compact-list">
                      {topExactaFour.map((row, idx) => (
                        <div key={`top-exacta-${row.combo}-${idx}`} className="premium-ticket-row">
                          <div className="ticket-mainline">
                            <span className="rank-pill">#{row?.rank ?? idx + 1}</span>
                            <span className="ticket-type ticket-type-inline">2連単</span>
                            <strong><ComboBadge combo={row?.combo || "--"} /></strong>
                          </div>
                          <div className="ticket-meta">
                            <span>{Number.isFinite(row?.probability) ? `hit ${formatMaybeNumber(row.probability * 100, 1)}%` : "--"}</span>
                            <span>{row?.source || "-"}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </article>
                  </RenderGuard>
                ) : null}

                {upsetSupport && upsetSupport.bigUpsetProbability > 0 ? (
                  <RenderGuard>
                  <article className="card summary-card premium-ticket-card subtle">
                    <div className="premium-card-head">
                      <div>
                        <p className="eyebrow">Supplemental</p>
                        <h2>Big Upset Probability</h2>
                      </div>
                    </div>
                    <div className="summary-inline-meta">
                      <span>{upsetSupport.classification}</span>
                      <span>compact upset formation view</span>
                    </div>
                    <div className="kv-list" style={{ marginTop: 8 }}>
                      <div className="kv-row">
                        <span>大穴発生確率</span>
                        <strong>{formatMaybeNumber(upsetSupport?.bigUpsetProbability * 100, 1)}%</strong>
                      </div>
                      <div className="kv-row">
                        <span>1着候補</span>
                        <strong><LanePills lanes={upsetSupport?.upsetFormation?.first_candidates || []} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>2着候補</span>
                        <strong><LanePills lanes={upsetSupport?.upsetFormation?.second_candidates || []} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>3着候補</span>
                        <strong><LanePills lanes={upsetSupport?.upsetFormation?.third_candidates || []} /></strong>
                      </div>
                      <div className="kv-row">
                        <span>Formation</span>
                        <strong>{upsetSupport?.upsetFormation?.formation_string || "--"}</strong>
                      </div>
                    </div>
                    {safeArray(upsetSupport?.chosenExactaPairs).length > 0 ? (
                      <div className="ticket-stack compact-list" style={{ marginTop: 10 }}>
                        {safeArray(upsetSupport?.chosenExactaPairs).slice(0, 4).map((row, idx) => (
                          <div key={`upset-exacta-${row?.combo || idx}`} className="premium-ticket-row">
                            <div className="ticket-mainline">
                              <span className="ticket-type ticket-type-inline">2連単</span>
                              <strong><ComboBadge combo={row?.combo || "--"} /></strong>
                            </div>
                            <div className="ticket-meta">
                              <span>{Number.isFinite(Number(row?.probability)) ? `hit ${formatMaybeNumber(Number(row.probability) * 100, 1)}%` : "--"}</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </article>
                  </RenderGuard>
                ) : null}

                <RenderGuard>
                <div className="prediction-summary-grid premium-layout">
                  {adminMode ? (
                    <section className="card">
                      <h2>手動周回展示評価（管理用）</h2>
                      <p className="muted strategy-line">本番ワークフローでは無効化されています。自動取得の展示データのみ予想に使用します。</p>
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
                        <textarea value={manualLapMemo} onChange={(e) => setManualLapMemo(e.target.value)} rows={2} />
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

                  <details className={`card summary-card premium-ticket-card ${!isRecommendedRace ? "deemphasized" : ""}`}>
                    <summary>Main Trifecta</summary>
                    <div style={{ marginTop: 10 }}>
                      <div className="summary-inline-meta">
                        <span>{Array.isArray(predictionViewModel?.tickets?.mainTrifecta) ? predictionViewModel.tickets.mainTrifecta.length : 0}件</span>
                        <span>{attackScenarioLabel || formationPatternLabel || data.racePattern || "-"}</span>
                      </div>
                      <div className="list-stack compact-list">
                        {(Array.isArray(predictionViewModel?.tickets?.mainTrifecta) ? predictionViewModel.tickets.mainTrifecta : []).map((bet, idx) => (
                          <div key={`${bet.combo}-${idx}`} className="list-stack"><div className="list-row list-row-actions premium-ticket-row primary"><strong><ComboBadge combo={bet.combo} /></strong><span className={`ticket-type ${getTicketTypeClass(bet.ticket_type)}`}>{getTicketTypeLabel(bet.ticket_type)}</span><span>p {Number.isFinite(bet.prob) ? formatMaybeNumber(bet.prob, 3) : "-"}</span><span>JPY {(bet.recommended_bet ?? bet.roundedBet).toLocaleString()}</span></div></div>
                        ))}
                      </div>
                    </div>
                  </details>

                  {boat1HeadSectionShown ? (
                    <details className={`card summary-card premium-ticket-card subtle ${!isRecommendedRace ? "deemphasized" : ""}`}>
                      <summary>Boat 1 Head Predictions</summary>
                      <div style={{ marginTop: 10 }}>
                        <pre className="json-preview">{safePrettyJson({ boat1HeadBets, boat1HeadReasonTags, boat1HeadScore, boat1SurvivalResidualScore, boat1PriorityModeApplied, boat1HeadTop8Generated, boat1HeadRatioInFinalBets })}</pre>
                      </div>
                    </details>
                  ) : null}

                  {(exactaSectionShown || backupUrasujiShown) ? (
                    <details className="card summary-card premium-ticket-card subtle">
                      <summary>Supplemental Recommendations</summary>
                      <div style={{ marginTop: 10 }}>
                        {exactaSectionShown ? <pre className="json-preview">{safePrettyJson({ tickets: predictionViewModel?.tickets?.exactaCover || [], exactaHeadScore, exactaPartnerScore, exactaReasonTags })}</pre> : null}
                        {backupUrasujiShown ? <pre className="json-preview">{safePrettyJson({ tickets: predictionViewModel?.tickets?.backupUrasuji || [], backupUrasujiReasonTags })}</pre> : null}
                      </div>
                    </details>
                  ) : null}

                  <details className="card summary-card premium-bias-card">
                    <summary>Supplemental Diagnostics</summary>
                    <div style={{ marginTop: 10 }}>
                      <details style={{ marginBottom: 12 }}>
                        <summary>Prediction Overview</summary>
                        <pre className="json-preview">{safePrettyJson({ summary: predictionViewModel.summary, raceTitle: predictionViewModel.raceTitle, raceSubtitle: predictionViewModel.raceSubtitle, defaultReasonTags, predictionQualityLabels })}</pre>
                      </details>
                      {Object.keys(evidenceGroupRankings).length > 0 ? <details style={{ marginBottom: 12 }}><summary>Premium Evidence Bias</summary><pre className="json-preview">{safePrettyJson(predictionViewModel.biasPanel)}</pre></details> : null}
                      {similarHistoryRows.length > 0 ? <details><summary>Recent Similar Verified</summary><pre className="json-preview">{safePrettyJson(similarHistoryRows)}</pre></details> : null}
                    </div>
                  </details>

                  <details className="card">
                    <summary>Debug Data</summary>
                    <div style={{ marginTop: 10 }}>
                      {hitRateEnhancementDebug ? (
                        <details style={{ marginBottom: 12 }}>
                          <summary>hit-rate enhancement debug</summary>
                          <div style={{ marginTop: 10 }}>
                            {safeArray(roleSpecificBonusRows).length > 0 ? (
                              <details style={{ marginBottom: 12 }}>
                                <summary>role-specific finish bonuses</summary>
                                <div className="table-wrap" style={{ marginTop: 10 }}>
                                  <table className="premium-player-table">
                                    <thead>
                                      <tr>
                                        <th>Boat</th>
                                        <th>1st bonus</th>
                                        <th>2nd bonus</th>
                                        <th>3rd bonus</th>
                                        <th>Ex left-gap</th>
                                        <th>Turning</th>
                                        <th>Straight</th>
                                        <th>Style</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {safeArray(roleSpecificBonusRows).map((row, idx) => (
                                        <tr key={`role-bonus-${row?.lane ?? idx}`}>
                                          <td>
                                            <span className={`combo-dot ${BOAT_META[row?.lane]?.className || ""}`}>{row?.lane ?? "--"}</span>
                                          </td>
                                          <td>{formatMaybeNumber(row?.firstPlaceBonus, 3)}</td>
                                          <td>{formatMaybeNumber(row?.secondPlaceBonus, 3)}</td>
                                          <td>{formatMaybeNumber(row?.thirdPlaceBonus, 3)}</td>
                                          <td>{formatMaybeNumber(row?.exTimeLeftGapBonus, 3)}</td>
                                          <td>{formatMaybeNumber(row?.turningBonus, 3)}</td>
                                          <td>{formatMaybeNumber(row?.straightBonus, 3)}</td>
                                          <td><span className="muted">{row?.styleBonus ?? "--"}</span></td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              </details>
                            ) : null}
                            {safeArray(finishRoleScoreRows).length > 0 ? (
                              <details style={{ marginBottom: 12 }}>
                                <summary>finish-role scores + head compatibility</summary>
                                <div className="table-wrap" style={{ marginTop: 10 }}>
                                  <table className="premium-player-table">
                                    <thead>
                                      <tr>
                                        <th>Boat</th>
                                        <th>1st score</th>
                                        <th>2nd score</th>
                                        <th>3rd score</th>
                                        <th>2nd compat</th>
                                        <th>3rd compat</th>
                                        <th>2nd factors</th>
                                        <th>3rd factors</th>
                                        <th>3rd exclude</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {safeArray(finishRoleScoreRows).map((row, idx) => (
                                        <tr key={`finish-role-score-${row?.lane ?? idx}`}>
                                          <td>
                                            <span className={`combo-dot ${BOAT_META[row?.lane]?.className || ""}`}>{row?.lane ?? "--"}</span>
                                          </td>
                                          <td>{formatMaybeNumber(row?.firstPlaceScore, 3)}</td>
                                          <td>{formatMaybeNumber(row?.secondPlaceScore, 3)}</td>
                                          <td>{formatMaybeNumber(row?.thirdPlaceScore, 3)}</td>
                                          <td>{formatMaybeNumber(row?.secondCompatibility, 3)}</td>
                                          <td>{formatMaybeNumber(row?.thirdCompatibility, 3)}</td>
                                          <td>
                                            <span className="muted">
                                              {row?.secondBreakdown
                                                ? `L2 ${formatMaybeNumber(row?.secondBreakdown?.lane2renScore, 2)} / M2 ${formatMaybeNumber(row?.secondBreakdown?.motor2ren, 2)} / compat ${formatMaybeNumber(row?.secondBreakdown?.compatibility_with_head, 2)} / carry ${formatMaybeNumber(row?.attackCarryover, 2)} / 1-surv ${formatMaybeNumber(row?.likelyHeadSurvivalContext, 2)}`
                                                : "--"}
                                            </span>
                                          </td>
                                          <td>
                                            <span className="muted">
                                              {row?.thirdBreakdown
                                                ? `L3 ${formatMaybeNumber(row?.thirdBreakdown?.lane3renScore, 2)} / flow ${formatMaybeNumber(row?.thirdBreakdown?.flow_in_bonus, 2)} / resid ${formatMaybeNumber(row?.residualTendency, 2)} / proxy ${row?.thirdProxyUsed || "--"} / excl ${formatMaybeNumber(row?.thirdBreakdown?.exclusion_penalty, 2)}`
                                                : "--"}
                                            </span>
                                          </td>
                                          <td>
                                            <span className="muted">
                                              {safeArray(row?.thirdExclusionReasons).length > 0
                                                ? `${safeArray(row?.thirdExclusionReasons).join(", ")} (${formatMaybeNumber(row?.thirdExclusionPenalty, 2)})`
                                                : "--"}
                                            </span>
                                          </td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              </details>
                            ) : null}
                            {hitRateEnhancementDebug?.stage1_static?.lane_finish_priors ? (
                              <details style={{ marginBottom: 12 }}>
                                <summary>lane finish priors + exacta support</summary>
                                <pre className="json-preview">{safePrettyJson({
                                  boat1PriorBoost: hitRateEnhancementDebug?.stage1_static?.boat1_prior_boost,
                                  laneFinishPriors: hitRateEnhancementDebug?.stage1_static?.lane_finish_priors,
                                  headCandidates: hitRateEnhancementDebug?.stage4_opponents?.head_candidate_set,
                                  compatibilityWithHead: hitRateEnhancementDebug?.stage4_opponents?.compatibility_with_head,
                                  selectedShape: recommendedShape,
                                  topExactaCandidates: topExactaFour
                                })}</pre>
                              </details>
                            ) : null}
                            {upsetSupport ? (
                              <details style={{ marginBottom: 12 }}>
                                <summary>upset detection + backup coverage</summary>
                                <pre className="json-preview">{safePrettyJson({
                                  classification: upsetSupport.classification,
                                  bigUpsetProbability: upsetSupport.bigUpsetProbability,
                                  upsetFormation: upsetSupport.upsetFormation,
                                  weakBoat1Factors: upsetSupport.weakBoat1Factors,
                                  strongAttackerFactors: upsetSupport.strongAttackerFactors,
                                  chaosFactors: upsetSupport.chaosFactors,
                                  chosenHeads: upsetSupport.chosenHeads,
                                  chosenExactaPairs: upsetSupport.chosenExactaPairs,
                                  chosenTrifectaTickets: upsetSupport.chosenTrifectaTickets
                                })}</pre>
                              </details>
                            ) : null}
                            <pre className="json-preview">{safePrettyJson(hitRateEnhancementDebug)}</pre>
                          </div>
                        </details>
                      ) : null}
                      {predictionDataUsageDebug ? <details style={{ marginBottom: 12 }}><summary>prediction data usage</summary><pre className="json-preview">{safePrettyJson(predictionDataUsageDebug)}</pre></details> : null}
                      {data?.dataAudit ? (
                        <details style={{ marginBottom: 12 }}>
                          <summary>strict data audit</summary>
                          <div style={{ marginTop: 10 }}>
                            <div className="kv-list" style={{ marginBottom: 10 }}>
                              <div className="kv-row"><span>actual entry validation</span><strong>{data?.dataAudit?.summary?.actual_entry?.validation_passed ? "passed" : "failed"}</strong></div>
                              <div className="kv-row"><span>actual entry fallback</span><strong>{data?.dataAudit?.summary?.actual_entry?.fallback_used ? "yes" : "no"}</strong></div>
                              <div className="kv-row"><span>fallback reason</span><strong>{data?.dataAudit?.summary?.actual_entry?.fallback_reason || "--"}</strong></div>
                              <div className="kv-row"><span>usable fields</span><strong>{safeArray(data?.dataAudit?.summary?.usable_fields).length}</strong></div>
                              <div className="kv-row"><span>unusable fields</span><strong>{safeArray(data?.dataAudit?.summary?.unusable_fields).length}</strong></div>
                            </div>
                            <div className="kv-list" style={{ marginBottom: 10 }}>
                              <div className="kv-row"><span>actual entry order</span><strong>{Array.isArray(data?.dataAudit?.summary?.actual_entry?.parsed_actual_entry_order) ? data.dataAudit.summary.actual_entry.parsed_actual_entry_order.join("-") : "--"}</strong></div>
                              <div className="kv-row"><span>actual lane map</span><strong><code>{formatDebugRawValue(data?.dataAudit?.summary?.actual_entry?.actual_lane_map || {})}</code></strong></div>
                              <div className="kv-row"><span>raw entry source</span><strong><code>{formatDebugRawValue(data?.dataAudit?.summary?.actual_entry?.raw_source_text)}</code></strong></div>
                            </div>
                            <div className="table-wrap" style={{ marginBottom: 10 }}>
                              <table>
                                <thead>
                                  <tr>
                                    <th>Boat</th>
                                    <th>Field</th>
                                    <th>Source</th>
                                    <th>Row</th>
                                    <th>Raw</th>
                                    <th>Normalized</th>
                                    <th>Validated</th>
                                    <th>Usable</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {safeArray(data?.dataAudit?.per_boat).flatMap((row) =>
                                    Object.entries(row?.fields || {}).map(([fieldKey, field]) => (
                                      <tr key={`audit-${row?.boat ?? "x"}-${fieldKey}`}>
                                        <td>{row?.boat ?? "-"}</td>
                                        <td>{field?.label || fieldKey}</td>
                                        <td>{field?.source_type || "--"}</td>
                                        <td>{field?.matched_row_label || field?.matched_section_label || "--"}</td>
                                        <td><code>{formatDebugRawValue(field?.raw_cell_text)}</code></td>
                                        <td><code>{formatDebugRawValue(field?.normalized_value)}</code></td>
                                        <td>{field?.validation_passed ? "yes" : "no"}</td>
                                        <td>{field?.prediction_usable ? "usable" : "unusable"}</td>
                                      </tr>
                                    ))
                                  )}
                                </tbody>
                              </table>
                            </div>
                            <pre className="json-preview">{safePrettyJson(data?.dataAudit)}</pre>
                          </div>
                        </details>
                      ) : null}
                      <details>
                        <summary>kyoteibiyori debug</summary>
                        <div style={{ marginTop: 10 }}>
                          <div className="kv-list" style={{ marginBottom: 10 }}>
                            <div className="kv-row"><span>kyoteibiyori_fetch_success</span><strong>{kyoteiBiyoriFrontendDebug.fetch_success ? "true" : "false"}</strong></div>
                            <div className="kv-row"><span>fallback_reason</span><strong>{formatDebugRawValue(kyoteiBiyoriFrontendDebug.fallback_reason)}</strong></div>
                            <div className="kv-row"><span>actual_fetch_paths</span><strong>{safePrettyJson(kyoteiBiyoriFrontendDebug.actual_fetch_paths)}</strong></div>
                            <div className="kv-row"><span>populated_fields</span><strong>{safePrettyJson(kyoteiBiyoriFrontendDebug.populated_fields)}</strong></div>
                            <div className="kv-row"><span>failed_fields</span><strong>{safePrettyJson(kyoteiBiyoriFrontendDebug.failed_fields)}</strong></div>
                          </div>
                          <div className="table-wrap" style={{ marginBottom: 10 }}>
                            <table>
                              <thead>
                                <tr>
                                  <th>Lane</th>
                                  <th>lane1stRate</th>
                                  <th>lane1stRate source</th>
                                  <th>lane2renRate</th>
                                  <th>lane2renRate source</th>
                                  <th>lane3renRate</th>
                                  <th>lane3renRate source</th>
                                  <th>lapTime</th>
                                  <th>lapTime source</th>
                                  <th>exhibitionST</th>
                                  <th>exhibitionST source</th>
                                  <th>motor2ren</th>
                                  <th>motor2ren source</th>
                                  <th>motor3ren</th>
                                  <th>motor3ren source</th>
                                </tr>
                              </thead>
                              <tbody>
                                {safeArray(kyoteiBiyoriFrontendDebug?.lane_rows).map((row) => (
                                  <tr key={`kyotei-raw-${row?.lane ?? "unknown"}`}>
                                    <td>{row?.lane ?? "-"}</td>
                                    <td><code>{formatDebugRawValue(row?.lane1stRate_raw)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.lane1stRate_debug)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.lane2renRate_raw)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.lane2renRate_debug)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.lane3renRate_raw)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.lane3renRate_debug)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.lapTime_raw)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.lapTime_debug)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.exhibitionST_raw)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.exhibitionST_debug)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.motor2ren_raw)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.motor2ren_debug)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.motor3ren_raw)}</code></td>
                                    <td><code>{formatDebugRawValue(row?.motor3ren_debug)}</code></td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                          <div className="table-wrap" style={{ marginTop: 10 }}>
                            <table>
                              <thead>
                                <tr>
                                  <th>Lane</th>
                                  <th>Metric</th>
                                  <th>今期</th>
                                  <th>6m</th>
                                  <th>3m</th>
                                  <th>1m</th>
                                  <th>当地</th>
                                  <th>一般戦</th>
                                  <th>SG/G1</th>
                                  <th>Verified</th>
                                </tr>
                              </thead>
                              <tbody>
                                {safeArray(kyoteiBiyoriFrontendDebug?.lane_rows).flatMap((row) => (
                                  [
                                    { label: "1着率", debug: row?.lane1stRate_debug },
                                    { label: "2連対率", debug: row?.lane2renRate_debug },
                                    { label: "3連対率", debug: row?.lane3renRate_debug }
                                  ].map((metricRow) => (
                                    <tr key={`kyotei-lane-raw-${row?.lane ?? "unknown"}-${metricRow.label}`}>
                                      <td>{row?.lane ?? "-"}</td>
                                      <td>{metricRow.label}</td>
                                      <td>{formatLaneRawDebugValue(metricRow.debug?.season)}</td>
                                      <td>{formatLaneRawDebugValue(metricRow.debug?.m6)}</td>
                                      <td>{formatLaneRawDebugValue(metricRow.debug?.m3)}</td>
                                      <td>{formatLaneRawDebugValue(metricRow.debug?.m1)}</td>
                                      <td>{formatLaneRawDebugValue(metricRow.debug?.local)}</td>
                                      <td>{formatLaneRawDebugValue(metricRow.debug?.ippansen)}</td>
                                      <td>{formatLaneRawDebugValue(metricRow.debug?.sg_g1)}</td>
                                      <td>{metricRow.debug?.exact_verified ? "verified" : "--"}</td>
                                    </tr>
                                  ))
                                ))}
                              </tbody>
                            </table>
                          </div>
                          <pre className="json-preview">{safePrettyJson(kyoteiBiyoriFrontendDebug)}</pre>
                          <pre className="json-preview">{safePrettyJson(data?.kyoteibiyori_debug || data?.source?.kyotei_biyori?.kyoteibiyori_debug || {})}</pre>
                        </div>
                      </details>
                    </div>
                  </details>
                </div>
                </RenderGuard>

                <RenderGuard>
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
                    <div className="kv-list" style={{ marginTop: 10 }}>
                      <div className="kv-row"><span>kyoteibiyori fetch</span><strong>{data?.source?.kyotei_biyori?.ok ? "success" : "fallback"}</strong></div>
                      <div className="kv-row"><span>fetch path</span><strong>{(data?.source?.kyotei_biyori?.request_diagnostics?.actual_fetch_paths || []).join(" -> ") || "--"}</strong></div>
                      <div className="kv-row"><span>target urls</span><strong>{(data?.source?.kyotei_biyori?.request_diagnostics?.target_urls || []).length || 0}</strong></div>
                      <div className="kv-row"><span>populated fields</span><strong>{(data?.source?.kyotei_biyori?.field_diagnostics?.populated_fields || []).join(", ") || "--"}</strong></div>
                      <div className="kv-row"><span>failed fields</span><strong>{(data?.source?.kyotei_biyori?.field_diagnostics?.failed_fields || []).join(", ") || "--"}</strong></div>
                      <div className="kv-row"><span>fallback reason</span><strong>{data?.source?.kyotei_biyori?.fallback_reason || "--"}</strong></div>
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
                          {safeArray(racers).map((racer, idx) => (
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
                </RenderGuard>

                <RenderGuard>
                <details className="card">
                  <summary>選手・左隣比較・F持ちの詳細</summary>
                  <div style={{ marginTop: 10 }}>
                    <div className="table-wrap">
                      <table>
                        <thead><tr><th>艇番</th><th>左隣あり</th><th>展示差</th><th>平均ST順位差</th><th>slit_alert</th><th>F caution</th><th>actual ST補正</th></tr></thead>
                        <tbody>
                          {safeArray(laneInsightRows).map((row) => (
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
                        {safeArray(fHoldNoteRows).length > 0 ? (
                          <p className="muted strategy-line">
                            F-hold caution:
                            {" "}
                            {safeArray(fHoldNoteRows).map((row) => `${row?.lane ?? "-"}号艇(+${formatMaybeNumber(row?.expected_actual_st_adjustment, 3)})`).join(", ")}
                          </p>
                        ) : null}
                    <section className="card ranking-card" style={{ marginTop: 10 }}>
                      <h3>AI総合評価ランキング</h3>
                      <p className="top3">予想される上位着順: {top3.length ? top3.join("-") : "-"}</p>
                      <div className="list-stack">
                        {safeArray(normalizedRanking).map((racer, idx) => (
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
                          {safeArray(racers).map((racer, idx) => <tr key={`${racer?.lane ?? "x"}-${idx}`}><td>{racer?.lane ?? "-"}</td><td>{racer?.name || "-"}</td><td>{racer?.class || "-"}</td><td>{formatMaybeNumber(racer?.nationwideWinRate, 2)}</td><td>{formatMaybeNumber(racer?.localWinRate, 2)}</td><td>{formatMaybeNumber(racer?.motor2Rate, 2)}</td><td>{formatMaybeNumber(racer?.exhibitionTime, 2)}</td><td>{formatMaybeNumber(racer?.exhibitionST, 2)}</td><td>{racer?.entryCourse ?? "-"}</td></tr>)}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </details>
                </RenderGuard>

                <RenderGuard>
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
                      {safeArray(skipReasonCodes).length === 0 ? <span className="chip">特記事項なし</span> : safeArray(skipReasonCodes).map((code) => <span className="chip" key={code}>{code}</span>)}
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
                      {safeArray(raceDecision?.reason_codes).length === 0
                        ? <span className="chip">NO_REASON</span>
                        : safeArray(raceDecision?.reason_codes).map((code) => <span className="chip" key={`rd-${code}`}>{code}</span>)}
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
                      {safeEntries(headSelection?.win_prob_by_lane)
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
                      {safeArray(startProfileRows).length === 0 ? (
                        <p className="muted">データなし</p>
                      ) : (
                        safeArray(startProfileRows).map((row) => (
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

                  {Object.keys(safeObject(evidenceGroupRankings)).length > 0 ? (
                    <article className="card analysis-card">
                      <h2>Evidence Bias Table</h2>
                      <div className="kv-list">
                        <div className="kv-row"><span>main head</span><strong><LanePills lanes={evidenceConfirmationFlags?.main_head_candidate ? [evidenceConfirmationFlags.main_head_candidate] : []} /></strong></div>
                        <div className="kv-row"><span>main 2nd</span><strong><LanePills lanes={evidenceConfirmationFlags?.main_second_candidate ? [evidenceConfirmationFlags.main_second_candidate] : []} /></strong></div>
                        <div className="kv-row"><span>counter 2nd</span><strong><LanePills lanes={evidenceConfirmationFlags?.counter_second_candidate ? [evidenceConfirmationFlags.counter_second_candidate] : []} /></strong></div>
                        <div className="kv-row"><span>3rd survivors</span><strong><LanePills lanes={Array.isArray(evidenceConfirmationFlags?.third_place_survivors) ? evidenceConfirmationFlags.third_place_survivors : []} /></strong></div>
                      </div>
                      {safeArray(evidenceInterpretation).length > 0 ? (
                        <div className="list-stack" style={{ marginTop: 8 }}>
                          {safeArray(evidenceInterpretation).map((line, idx) => (
                            <p key={`evidence-line-${idx}`} className="muted strategy-line">{line}</p>
                          ))}
                        </div>
                      ) : null}
                      <div className="table-wrap" style={{ marginTop: 10 }}>
                        <table>
                          <thead>
                            <tr>
                              <th>Group</th>
                              <th>Top boats</th>
                            </tr>
                          </thead>
                          <tbody>
                            {safeEntries(evidenceGroupRankings).map(([groupKey, rows]) => (
                              <tr key={`evidence-group-${groupKey}`}>
                                <td>{EVIDENCE_GROUP_LABELS[groupKey] || groupKey}</td>
                                <td>
                                  <div className="chips-wrap">
                                    {(Array.isArray(rows) ? rows : []).map((row) => (
                                      <span className="chip" key={`evidence-group-${groupKey}-${row?.lane}`}>
                                        {row?.lane}
                                        {" "}
                                        H{formatMaybeNumber(row?.head, 2)}
                                        {" / "}
                                        2{formatMaybeNumber(row?.second, 2)}
                                        {" / "}
                                        3{formatMaybeNumber(row?.third, 2)}
                                      </span>
                                    ))}
                                  </div>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <div className="table-wrap" style={{ marginTop: 10 }}>
                        <table>
                          <thead>
                            <tr>
                              <th>Boat</th>
                              <th>Head</th>
                              <th>2nd</th>
                              <th>3rd</th>
                              <th>Risk</th>
                              <th>Independent</th>
                              <th>Groups</th>
                              <th>Warnings</th>
                            </tr>
                          </thead>
                          <tbody>
                            {safeArray(evidenceBoatSummaryRows).map((row) => (
                              <tr key={`evidence-boat-${row.lane}`}>
                                <td><LanePills lanes={[row.lane]} /></td>
                                <td>{formatMaybeNumber(row.head_support_score, 3)}</td>
                                <td>{formatMaybeNumber(row.second_support_score, 3)}</td>
                                <td>{formatMaybeNumber(row.third_support_score, 3)}</td>
                                <td>{formatMaybeNumber(row.risk_penalty, 3)}</td>
                                <td>{row.independent_evidence_count ?? "-"}</td>
                                <td>{Array.isArray(row.strongest_groups) ? row.strongest_groups.map((group) => EVIDENCE_GROUP_LABELS[group] || group).join(", ") : "-"}</td>
                                <td>{Array.isArray(row.warnings) && row.warnings.length ? row.warnings.join(", ") : "-"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </article>
                  ) : null}

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

                  {showInternalBetBreakdown && predictionViewModel.predictionMeta.matchedDictionaryScenarios.length > 0 && (
                  <article className="card">
                    <h2>開発辞書 prior</h2>
                    <div className="kv-list">
                      <div className="kv-row">
                        <span>activated</span>
                        <strong>{Array.isArray(predictionViewModel.predictionMeta.dictionaryPriorAdjustment?.activated_scenario_names)
                          ? predictionViewModel.predictionMeta.dictionaryPriorAdjustment.activated_scenario_names.slice(0, 3).join(", ")
                          : "-"}</strong>
                      </div>
                      <div className="kv-row">
                        <span>priority ranks</span>
                        <strong>{Array.isArray(predictionViewModel.predictionMeta.dictionaryPriorAdjustment?.activated_priority_ranks)
                          ? predictionViewModel.predictionMeta.dictionaryPriorAdjustment.activated_priority_ranks.join(", ")
                          : "-"}</strong>
                      </div>
                    </div>
                    <div className="list-stack">
                      {predictionViewModel.predictionMeta.matchedDictionaryScenarios.slice(0, 5).map((row) => (
                        <div className="list-row" key={`dict-${row.scenario_name}`}>
                          <span className={`ticket-type ${getTicketTypeClass(String(row.priority_rank || "").toUpperCase() === "A" ? "main" : String(row.priority_rank || "").toUpperCase() === "B" ? "counter" : "backup")}`}>
                            {row.priority_rank}
                          </span>
                          <strong>{row.scenario_name}</strong>
                          <span>{formatMaybeNumber(row.match_score, 1)}</span>
                          <span>{Number(row.activated) === 1 ? "active" : "watch"}</span>
                        </div>
                      ))}
                    </div>
                    <div className="chips-wrap">
                      {predictionViewModel.predictionMeta.dictionaryConditionFlags.slice(0, 4).flatMap((row) => (
                        [
                          ...(Array.isArray(row?.success_conditions_satisfied) ? row.success_conditions_satisfied.slice(0, 2) : []),
                          ...(Array.isArray(row?.rejection_conditions_triggered) ? row.rejection_conditions_triggered.slice(0, 1).map((tag) => `reject:${tag}`) : [])
                        ].map((tag) => <span className="chip chip-scenario" key={`dict-flag-${row.scenario_name}-${tag}`}>{tag}</span>)
                      ))}
                    </div>
                  </article>
                  )}
                </section>
                  </div>
                </details>
                </RenderGuard>
              </>
            )}
          </>
        )}

        {screen === "hardRace" && (
          <RenderGuard>
          <>
            {rankingsError && <div className="error-banner">{rankingsError}</div>}
            <section className="card">
              <div className="section-head recommend-head">
                <div>
                  <h2>Hard Race Prediction</h2>
                  <p className="muted strategy-line">Best conservative races for 1-234-234</p>
                </div>
                <div className="row-actions">
                  <span className="muted">{date} / {VENUES.find((v) => v.id === Number(venueId))?.name || "-"}</span>
                  <button className="fetch-btn secondary" onClick={loadRankings} disabled={rankingsLoading}>
                    {rankingsLoading ? "更新中..." : "再取得"}
                  </button>
                </div>
              </div>
              <div className="controls-grid" style={{ marginBottom: 14 }}>
                <label><span>日付</span><input type="date" value={date} onChange={(e) => setDate(e.target.value)} /></label>
                <label><span>場</span><select value={venueId} onChange={(e) => setVenueId(Number(e.target.value))}>{VENUES.map((v) => <option key={v.id} value={v.id}>{v.id} - {v.name}</option>)}</select></label>
                <label><span>Rank filter</span><select value={hardRaceTierFilter} onChange={(e) => setHardRaceTierFilter(e.target.value)}><option value="ALL">All</option><option value="A">A rank</option><option value="B">B rank</option><option value="SKIP">C rank / SKIP</option></select></label>
              </div>
              {hardRaceCalibration?.reviewedCount > 0 ? (
                <section className="hardrace-kpi-section" style={{ marginBottom: 14 }}>
                  <div className="hardrace-kpi-section-head">
                    <div>
                      <span className="hardrace-panel-title">KPI Board</span>
                      <h3>継続改善モニタ</h3>
                      <p className="muted strategy-line">
                        {hardRaceCalibration.scopeLabel} のレビュー結果を集約。期間切替を後から足しやすいよう、KPI はカード単位で独立させています。
                      </p>
                    </div>
                    <div className="hardrace-kpi-meta">
                      <span className="status-pill status-hit">{hardRaceCalibration.reviewedCount} reviews</span>
                      <span className="status-pill status-unsettled">{filteredHardRaceRows.length} visible races</span>
                    </div>
                  </div>
                  <div className="hardrace-kpi-grid">
                    {hardRaceKpiCards.map((item) => (
                      <article className={`hardrace-kpi-card signal-${item.signal.tone}`} key={`hardrace-kpi-${item.key}`}>
                        <div className="hardrace-kpi-topline">
                          <span>{item.label}</span>
                          <span className={`hardrace-kpi-signal tone-${item.signal.tone}`}>{item.signal.label}</span>
                        </div>
                        <strong>{Number.isFinite(Number(item.value)) ? `${formatMaybeNumber(item.value, 1)}%` : "--"}</strong>
                        <small>{item.description}</small>
                        <div className="hardrace-kpi-foot">
                          <span>{Number.isFinite(Number(item.numerator)) && Number.isFinite(Number(item.denominator)) ? `${item.numerator}/${item.denominator}` : "n/a"}</span>
                          <span>{item.signal.deltaText}</span>
                        </div>
                      </article>
                    ))}
                  </div>
                </section>
              ) : null}
              {rankingsLoading ? (
                <p className="muted">保守的な hard race 候補をスキャン中...</p>
              ) : filteredHardRaceRows.length === 0 ? (
                <p className="muted">対象レースがありません。</p>
              ) : (
                <div className="recommendation-list">
                  {filteredHardRaceRows.map((row) => {
                    const headRanking = buildHeadRankingRows(row);
                    const fixed1234Rows = buildFixed1234ProbabilityRows(row);
                    const top2Shapes = fixed1234Rows.slice(0, 2);
                    const outsideLead = getOutsideRiskLead(row);
                    const dangerRows = buildHardRaceDangerRows(row);
                    const fallbackFields = Array.isArray(row?.fallback_used?.fields) ? row.fallback_used.fields : [];
                    return (
                    <article className="recommend-card hardrace-card" key={`hard-race-${row.raceNo}`}>
                      <div className="recommend-card-head">
                        <strong>{row.raceNo}R {row.venueName || "-"}</strong>
                        <div className="row-actions">
                          {row.adoptedForOperation ? <span className="status-pill status-hit">TOP採用</span> : null}
                          <span className={`status-pill ${getHardRaceRankClass(row.hardRaceRank)}`}>{`Rank ${row.hardRaceRank || "-"}`}</span>
                          <span className={`status-pill ${getHardRaceDecisionClass(row.buyStyleRecommendation)}`}>{`Decision ${row.buyStyleRecommendation || "-"}`}</span>
                          <span className={`status-pill ${getHardRaceConfidenceClass(row.confidence_status || row.data_status)}`}>{`Confidence ${row.confidence_status || row.data_status || "-"}`}</span>
                        </div>
                      </div>
                      <section className="hardrace-summary-grid">
                        <div className={`hardrace-summary-hero decision-${String(row.buyStyleRecommendation || "").toLowerCase()}`}>
                          <div className="hardrace-summary-main hardrace-summary-hero-main">
                            <span className="hardrace-panel-title">Summary</span>
                            <div className="hardrace-hero-layout">
                              <div>
                                <span className="hardrace-hero-kicker">Decision</span>
                                <strong className="hardrace-hero-number">{row.buyStyleRecommendation || "--"}</strong>
                                <span className="hardrace-hero-copy">{getHardRaceDecisionCopy(row)}</span>
                              </div>
                              <div className={`hardrace-rank-totem tone-${String(row.hardRaceRank || "-").toLowerCase()}`}>
                                <span>Rank</span>
                                <strong>{row.hardRaceRank || "--"}</strong>
                              </div>
                            </div>
                            {String(row.confidence_status || row.data_status || "").toUpperCase() === "FALLBACK" ? (
                              <div className="hardrace-partial-alert">
                                <strong>FALLBACK</strong>
                                <span>一部は保存済み特徴量から補完しています。source_summary を確認してください。</span>
                              </div>
                            ) : null}
                          </div>
                          <div className="hardrace-badge-stack">
                            <span className={`status-pill ${getHardRaceDecisionClass(row.buyStyleRecommendation)}`}>{row.buyStyleRecommendation || "--"}</span>
                            <span className={`status-pill ${getHardRaceConfidenceClass(row.confidence_status || row.data_status)}`}>{row.confidence_status || row.data_status || "--"}</span>
                            <span className={`status-pill ${getHardRaceRankClass(row.hardRaceRank)}`}>{`Rank ${row.hardRaceRank || "-"}`}</span>
                            {row.open_mode?.active ? <span className="status-pill risk-small">穴候補</span> : null}
                          </div>
                        </div>
                        <div className="hardrace-meta-grid hardrace-summary-metrics">
                          <article className="hardrace-meta-card primary">
                            <span>Inference</span>
                            <strong>{row.confidence_status || row.data_status || "--"}</strong>
                            <small>{getHardRaceStatusAccent(row)} / {getHardRaceConfidenceCopy(row.confidence_status || row.data_status)}</small>
                          </article>
                          <article className="hardrace-meta-card">
                            <span>運用判断</span>
                            <strong>{getHardRaceOperationalLabel(row)}</strong>
                            <small>{row.open_mode?.active ? "Open Race寄りの監視レース" : row.buyStyleRecommendation === "SKIP" ? "見送り優先" : "Hard Race本線候補"}</small>
                          </article>
                          <article className="hardrace-meta-card">
                            <span>P1 Head</span>
                            <strong>{formatPercentDisplay(row.head_prob_1)}</strong>
                            <small>1号艇の頭候補率</small>
                          </article>
                          <article className="hardrace-meta-card">
                            <span>Box Hit 1234</span>
                            <strong>{formatPercentDisplay(row.fixed1234TotalProbability)}</strong>
                            <small>6点合計</small>
                          </article>
                          <article className={`hardrace-meta-card ${getHardRaceRiskTone(row.outsideBoxBreakRisk)}`}>
                            <span>Outside Break</span>
                            <strong>{formatPercentDisplay(row.outsideBoxBreakRisk)}</strong>
                            <small>{outsideLead}</small>
                          </article>
                          <article className="hardrace-meta-card">
                            <span>Suggested Shape</span>
                            <strong>{row.suggestedShape || "--"}</strong>
                            <small>最優先の形</small>
                          </article>
                          <article className="hardrace-meta-card">
                            <span>Top 2 Shapes</span>
                            <strong>{top2Shapes.length > 0 ? top2Shapes.map((item) => item.combo).join(" / ") : "--"}</strong>
                            <small>{top2Shapes.length > 0 ? top2Shapes.map((item) => formatPercentDisplay(item.probability)).join(" / ") : "候補なし"} / 6点内シェア</small>
                          </article>
                        </div>
                        {row.fallback_used?.used || (Array.isArray(row.missing_fields) && row.missing_fields.length > 0) ? (
                          <div className="hardrace-fallback-banner">
                            <strong>{row.fallback_used?.used ? "補完推定あり" : "事前特徴量未生成"}</strong>
                            <span>
                              {row.fallback_used?.used && fallbackFields.length > 0
                                ? `fallback: ${fallbackFields.join(", ")}`
                                : Array.isArray(row.missing_fields) && row.missing_fields.length > 0
                                  ? `missing: ${row.missing_fields.slice(0, 6).join(", ")}`
                                  : "snapshot / feature / mapping を確認してください"}
                              </span>
                          </div>
                        ) : null}
                        <div className="hardrace-role-strip">
                          <span className="hardrace-tag picked">Hard Race Prediction: 1-234-234固定買い用</span>
                          <span className={`status-pill ${getHardRaceDecisionClass(row.buyStyleRecommendation)}`}>{row.buyStyleRecommendation || "--"}</span>
                          <span className={`status-pill ${getHardRaceConfidenceClass(row.confidence_status || row.data_status)}`}>{`Confidence ${row.confidence_status || row.data_status || "-"}`}</span>
                          <span className={`status-pill ${getHardRaceRiskTone(row.outsideBoxBreakRisk)}`}>{`Outside ${formatPercentDisplay(row.outsideBoxBreakRisk)}`}</span>
                          <span className="hardrace-tag top4">{`Shape ${row.suggestedShape || "--"}`}</span>
                        </div>
                      </section>
                      <section className="hardrace-section-grid">
                        <div className="hardrace-block">
                          <div className="hardrace-block-head">
                            <div>
                              <strong>6点個別確率</strong>
                              <p className="muted">高い順。Top 2 と Top 4 をバーで確認できます。</p>
                            </div>
                          </div>
                          <div className="hardrace-prob-list">
                            {fixed1234Rows.length > 0 ? fixed1234Rows.map((item) => (
                              <div className="hardrace-prob-item" key={`matrix-${row.raceNo}-${item.combo}`}>
                                <div className="hardrace-prob-meta">
                                  <strong>{item.combo}</strong>
                                  <span>{formatPercentDisplay(item.probability)}</span>
                                </div>
                                <div className="hardrace-prob-bar">
                                  <div className="hardrace-prob-fill" style={{ width: `${item.width}%` }} />
                                </div>
                                <div className="hardrace-prob-tags">
                                  {item.isTop2 ? <span className="hardrace-tag top2">Top 2</span> : null}
                                  {item.isTop4 ? <span className="hardrace-tag top4">Top 4</span> : null}
                                  {row.suggestedShape === item.combo ? <span className="hardrace-tag picked">Suggested</span> : null}
                                </div>
                              </div>
                            )) : (
                              <p className="muted">6点個別確率は未計算です。</p>
                            )}
                          </div>
                        </div>
                        <div className="hardrace-block">
                          <div className="hardrace-block-head">
                            <div>
                              <strong>頭候補ランキング</strong>
                              <p className="muted">上位3艇を強調。1号艇が弱い時の代替頭もすぐ追えます。</p>
                            </div>
                          </div>
                          <div className="hardrace-head-ranking-list">
                            {headRanking.length > 0 ? headRanking.map((item, index) => (
                              <article
                                className={`hardrace-head-rank-row ${index < 3 ? "top3" : ""} ${index === 0 ? "leader" : ""}`}
                                key={`head-ranking-${row.raceNo}-${item.lane}`}
                              >
                                <div className="hardrace-head-rank-main">
                                  <span className="rank-pill">#{index + 1}</span>
                                  <strong>{item.lane}号艇</strong>
                                  {item.lane === 1 && index > 0 ? <span className="hardrace-tag risk">1号艇注意</span> : null}
                                  {index === 0 && item.lane !== 1 ? <span className="hardrace-tag picked">代替頭本線</span> : null}
                                </div>
                                <div className="hardrace-head-rank-side">
                                  <div className="hardrace-prob-bar compact">
                                    <div className="hardrace-prob-fill" style={{ width: `${Math.max(10, item.probability * 100)}%` }} />
                                  </div>
                                  <span>{formatPercentDisplay(item.probability)}</span>
                                </div>
                              </article>
                            )) : (
                              <p className="muted">頭候補は未計算です。</p>
                            )}
                          </div>
                        </div>
                      </section>
                      <section className="hardrace-block hardrace-danger-block">
                        <div className="hardrace-block-head">
                          <div>
                            <strong>5,6危険シナリオ</strong>
                            <p className="muted">5,6の2着侵入が危ない時はこのブロックだけで気づけます。</p>
                          </div>
                          <span className={`status-pill ${getHardRaceRiskTone(row.outside2ndRisk)}`}>
                            2着侵入 {formatPercentDisplay(row.outside2ndRisk)}
                          </span>
                        </div>
                        <div className="hardrace-danger-grid">
                          {dangerRows.map((item) => (
                            <article className={`hardrace-danger-card ${item.tone}`} key={`danger-${row.raceNo}-${item.label}`}>
                              <div className="hardrace-danger-headline">
                                <span>{item.label}</span>
                                <span className={`hardrace-danger-level ${item.tone}`}>{item.level}</span>
                              </div>
                              <strong>{item.valueLabel}</strong>
                              <small>{item.copy}</small>
                            </article>
                          ))}
                        </div>
                        {Array.isArray(row.outside_danger_scenarios) && row.outside_danger_scenarios.length > 0 ? (
                          <div className="hardrace-chip-row" style={{ marginTop: 8 }}>
                            {row.outside_danger_scenarios.map((item, index) => (
                              <span className="chip chip-scenario" key={`outside-scenario-${row.raceNo}-${index}`}>
                                {item.label} {formatPercentDisplay(item.risk)}
                              </span>
                            ))}
                          </div>
                        ) : null}
                      </section>
                      <details className="hardrace-block hardrace-collapsible">
                        <summary>
                          <span>
                            <strong>source_summary / fallback_used</strong>
                            <small>pure inference で使った事前snapshotと補完有無</small>
                          </span>
                        </summary>
                        <div className="kv-list" style={{ marginTop: 10 }}>
                          {buildSourceSummaryRows(row.source_summary).map((item) => (
                            <div className="kv-row" key={`source-summary-${row.raceNo}-${item.label}`}><span>{item.label}</span><strong>{item.value}</strong></div>
                          ))}
                          <div className="kv-row"><span>decision_reason</span><strong>{row.decision_reason || "-"}</strong></div>
                          <div className="kv-row"><span>actual result</span><strong>{row.actualResult || "--"}</strong></div>
                        </div>
                      </details>
                      {row.open_mode?.active ? (
                        <section className="hardrace-open-panel" style={{ marginTop: 10 }}>
                          <div className="hardrace-block-head">
                            <div>
                              <strong>Open Race / 穴モード</strong>
                              <p className="muted">Hard Race 本線と切り分けて、荒れ前提の候補だけを見せます。</p>
                            </div>
                            <span className="status-pill risk-small">{row.open_mode?.alert_label || "荒れ注意"}</span>
                          </div>
                          <div className="hardrace-open-grid">
                            <article className="hardrace-meta-card">
                              <span>穴頭候補 上位2艇</span>
                              <strong>{Array.isArray(row.head_candidates) && row.head_candidates.length > 0 ? row.head_candidates.slice(0, 2).map((item) => `${item.lane}号艇`).join(" / ") : "--"}</strong>
                              <small>{Array.isArray(row.head_candidates) && row.head_candidates.length > 0 ? row.head_candidates.slice(0, 2).map((item) => formatPercentDisplay(item.probability || (Number(item.score) / 100))).join(" / ") : "候補なし"}</small>
                            </article>
                            <article className="hardrace-meta-card">
                              <span>相手候補 上位3艇</span>
                              <strong>{Array.isArray(row.head_opponents) && row.head_opponents.length > 0 ? row.head_opponents.slice(0, 3).map((item) => `${item.lane}号艇`).join(" / ") : "--"}</strong>
                              <small>{Array.isArray(row.head_opponents) && row.head_opponents.length > 0 ? row.head_opponents.slice(0, 3).map((item) => formatMaybeNumber(item.score, 1)).join(" / ") : "候補なし"} / 追走力</small>
                            </article>
                          </div>
                        </section>
                      ) : null}
                      <p className="muted strategy-line">{row.expandableReason}</p>
                      <div className="hardrace-chip-row" style={{ marginTop: 8 }}>
                        {(Array.isArray(row.topReasons) ? row.topReasons : []).map((reason, index) => (
                          <span className="chip chip-scenario" key={`hard-reason-${row.raceNo}-${index}`}>{reason}</span>
                        ))}
                      </div>
                      <details className="hardrace-details" style={{ marginTop: 10 }}>
                        <summary>生指標を開く</summary>
                        <div className="kv-list" style={{ marginTop: 10 }}>
                          <div className="kv-row"><span>Rank</span><strong>{row.hardRaceRank || "-"}</strong></div>
                          <div className="kv-row"><span>Top shape</span><strong>{row.suggestedShape || "SKIP"}</strong></div>
                          <div className="kv-row"><span>Composite</span><strong>{row.conservativeComposite == null ? "--" : formatMaybeNumber(row.conservativeComposite, 1)}</strong></div>
                          <div className="kv-row"><span>Buy style</span><strong>{row.buyStyleRecommendation || "-"}</strong></div>
                          <div className="kv-row"><span>Decision</span><strong>{row.decision || "-"}</strong></div>
                          <div className="kv-row"><span>Data status</span><strong>{row.data_status || "-"}</strong></div>
                          <div className="kv-row"><span>Confidence</span><strong>{row.confidence_status || row.data_status || "-"}</strong></div>
                          <div className="kv-row"><span>P1 Head</span><strong>{row.head_prob_1 == null ? "--" : `${formatMaybeNumber(row.head_prob_1 * 100, 1)}%`}</strong></div>
                          <div className="kv-row"><span>Head ranking</span><strong>{Array.isArray(row.head_candidate_ranking) && row.head_candidate_ranking.length > 0 ? row.head_candidate_ranking.map((item) => `${item.lane}号艇 ${formatMaybeNumber((item.probability || 0) * 100, 1)}%`).join(" / ") : [1,2,3,4,5,6].map((lane) => `#${lane} ${formatMaybeNumber((row[`head_prob_${lane}`] || 0) * 100, 1)}%`).join(" / ")}</strong></div>
                          <div className="kv-row"><span>Old decision</span><strong>{row.screeningDebug?.old_decision || "-"}</strong></div>
                          <div className="kv-row"><span>boat1_escape_trust</span><strong>{renderHardRaceMetric(row, "boat1_escape_trust", row.boat1EscapeTrust, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>opponent_234_fit</span><strong>{renderHardRaceMetric(row, "opponent_234_fit", row.opponent234Fit, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>pair23_fit</span><strong>{renderHardRaceMetric(row, "pair23_fit", row.pair23Fit, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>pair24_fit</span><strong>{renderHardRaceMetric(row, "pair24_fit", row.pair24Fit, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>pair34_fit</span><strong>{renderHardRaceMetric(row, "pair34_fit", row.pair34Fit, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>kill_escape_risk</span><strong>{renderHardRaceMetric(row, "kill_escape_risk", row.killEscapeRisk, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>shape_shuffle_risk</span><strong>{renderHardRaceMetric(row, "shape_shuffle_risk", row.shapeShuffleRisk, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>makuri_risk</span><strong>{renderHardRaceMetric(row, "makuri_risk", row.makuriRisk, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>outside_break_risk</span><strong>{renderHardRaceMetric(row, "outside_break_risk", row.outsideBreakRisk, (value) => formatMaybeNumber(value, 1))}</strong></div>
                          <div className="kv-row"><span>box hit</span><strong>{renderHardRaceMetric(row, "box_hit_score", row.boxHitScore, (value) => `${formatMaybeNumber(value * 100, 1)}%`)}</strong></div>
                          <div className="kv-row"><span>shape focus</span><strong>{renderHardRaceMetric(row, "shape_focus_score", row.shapeFocusScore, (value) => `${formatMaybeNumber(value * 100, 1)}%`)}</strong></div>
                          <div className="kv-row"><span>Top4 share within fixed1234</span><strong>{renderHardRaceMetric(row, "fixed1234_shape_concentration", row.fixed1234ShapeConcentration, (value) => `${formatMaybeNumber(value * 100, 1)}%`)}</strong></div>
                          <div className="kv-row"><span>outside head risk</span><strong>{row.screeningDebug?.outside_head_risk == null ? "--" : `${formatMaybeNumber((row.screeningDebug.outside_head_risk || 0) * 100, 1)}%`}</strong></div>
                          <div className="kv-row"><span>outside 2nd risk</span><strong>{row.outside2ndRisk == null ? "--" : `${formatMaybeNumber((row.outside2ndRisk || 0) * 100, 1)}%`}</strong></div>
                          <div className="kv-row"><span>outside 3rd risk</span><strong>{row.outside3rdRisk == null ? "--" : `${formatMaybeNumber((row.outside3rdRisk || 0) * 100, 1)}%`}</strong></div>
                          <div className="kv-row"><span>outside box break risk</span><strong>{row.outsideBoxBreakRisk == null ? "--" : `${formatMaybeNumber((row.outsideBoxBreakRisk || 0) * 100, 1)}%`}</strong></div>
                          <div className="kv-row"><span>Top 4 total</span><strong>{renderHardRaceMetric(row, "top4_fixed1234_probability", row.fixed1234Top4Total, (value) => `${formatMaybeNumber(value * 100, 1)}%`)}</strong></div>
                          <div className="kv-row"><span>Shape candidates</span><strong>{Array.isArray(row.fixedShapeCandidates) ? row.fixedShapeCandidates.map((item) => `${item.shape} ${formatMaybeNumber(item.probability * 100, 1)}%`).join(" / ") : "-"}</strong></div>
                        </div>
                        {Array.isArray(row.fixed1234Top4) && row.fixed1234Top4.length > 0 ? (
                          <p className="muted strategy-line" style={{ marginTop: 10 }}>
                            Top 4: {row.fixed1234Top4.map((item) => `${item.combo} ${formatMaybeNumber(item.probability * 100, 1)}%`).join(", ")}
                          </p>
                        ) : null}
                        <p className="muted strategy-line" style={{ marginTop: 10 }}>
                          5,6絡み危険シナリオ:
                          {` 頭 ${row.outsideHeadRisk == null ? "--" : `${formatMaybeNumber(row.outsideHeadRisk * 100, 1)}%`}`}
                          {` / 2着 ${row.outside2ndRisk == null ? "--" : `${formatMaybeNumber(row.outside2ndRisk * 100, 1)}%`}`}
                          {` / 3着 ${row.outside3rdRisk == null ? "--" : `${formatMaybeNumber(row.outside3rdRisk * 100, 1)}%`}`}
                          {` / box break ${row.outsideBoxBreakRisk == null ? "--" : `${formatMaybeNumber(row.outsideBoxBreakRisk * 100, 1)}%`}`}
                        </p>
                        {Array.isArray(row.outside_danger_scenarios) && row.outside_danger_scenarios.length > 0 ? (
                          <p className="muted strategy-line">
                            Danger ranking: {row.outside_danger_scenarios.map((item) => `${item.label} ${formatMaybeNumber((item.risk || 0) * 100, 1)}%`).join(" / ")}
                          </p>
                        ) : null}
                        {Array.isArray(row.positiveReasons) && row.positiveReasons.length > 0 ? (
                          <p className="muted strategy-line" style={{ marginTop: 10 }}>
                            Positive: {row.positiveReasons.join(", ")}
                          </p>
                        ) : null}
                        {Array.isArray(row.negativeReasons) && row.negativeReasons.length > 0 ? (
                          <p className="muted strategy-line">
                            Negative: {row.negativeReasons.join(", ")}
                          </p>
                        ) : null}
                        {row.skipReason ? (
                          <p className="muted strategy-line">
                            Skip reason: {row.skipReason}
                          </p>
                        ) : null}
                        {row.decision_reason ? (
                          <p className="muted strategy-line">
                            Decision reason: {row.decision_reason}
                          </p>
                        ) : null}
                        {Array.isArray(row.errors) && row.errors.length > 0 ? (
                          <p className="muted strategy-line">
                            Errors: {row.errors.join(", ")}
                          </p>
                        ) : null}
                        {Array.isArray(row.missing_fields) && row.missing_fields.length > 0 ? (
                          <p className="muted strategy-line">
                            Missing fields: {row.missing_fields.join(", ")}
                          </p>
                        ) : null}
                        {row.screeningDebug ? (
                          <p className="muted strategy-line">
                            Contributions:
                            {` optional penalty ${formatMaybeNumber((row.screeningDebug.optional_data_penalty || 0) * 100, 1)}%`}
                            {` / escape trust ${formatMaybeNumber(row.screeningDebug.boat1_escape_trust ?? row.screeningDebug.boat1_anchor_contribution, 1)}`}
                            {` / opponent234 ${formatMaybeNumber(row.screeningDebug.opponent_234_fit ?? row.screeningDebug.box_234_fit_contribution, 1)}`}
                            {` / makuri risk ${formatMaybeNumber(row.screeningDebug.makuri_risk, 1)}`}
                            {` / outside risk ${formatMaybeNumber(row.screeningDebug.outside_break_risk, 1)}`}
                            {` / fixed total ${formatMaybeNumber((row.screeningDebug.fixed1234_total_contribution || 0) * 100, 1)}%`}
                            {` / top4 ${formatMaybeNumber((row.screeningDebug.top4_fixed1234_contribution || 0) * 100, 1)}%`}
                            {` / top4 share ${formatMaybeNumber((row.screeningDebug.fixed1234_shape_concentration || 0) * 100, 1)}%`}
                          </p>
                        ) : null}
                        {row.actualResult ? (
                          <p className="muted strategy-line">
                            Review: {row.actualInsideSixTarget ? "actual result was inside fixed-1234 target set" : "actual result was outside fixed-1234 target set"}
                            {row.reviewMissType ? ` / ${row.reviewMissType}` : ""}
                          </p>
                        ) : null}
                        {row.screeningDebug?.final_status === "UNAVAILABLE" && Array.isArray(row.screeningDebug?.why_unavailable) && row.screeningDebug.why_unavailable.length > 0 ? (
                          <p className="muted strategy-line">
                            Why unavailable: {row.screeningDebug.why_unavailable.join(", ")}
                          </p>
                        ) : null}
                        {Array.isArray(row.screeningDebug?.optional_fields_missing) && row.screeningDebug.optional_fields_missing.length > 0 ? (
                          <p className="muted strategy-line">
                            Optional missing: {row.screeningDebug.optional_fields_missing.join(", ")}
                          </p>
                        ) : null}
                        {!row.fetchFailed ? (
                          <div className="row-actions" style={{ marginTop: 10 }}>
                            <button className="fetch-btn" onClick={() => onOpenRecommendation(row.sourceData?.race || row)}>
                              詳細予想へ
                            </button>
                          </div>
                        ) : null}
                        {row.screeningDebug ? (
                          <div className="kv-list" style={{ marginTop: 10 }}>
                            <div className="kv-row"><span>snapshot mode</span><strong>{row.source_summary?.mode || "pure_inference"}</strong></div>
                            <div className="kv-row"><span>core fields</span><strong>{row.screeningDebug.core_fields_ready ? "ready" : "missing"}</strong></div>
                            <div className="kv-row"><span>score ready</span><strong>{row.screeningDebug.score_success ? "yes" : "no"}</strong></div>
                            <div className="kv-row"><span>rank</span><strong>{row.screeningDebug.hard_race_rank || "-"}</strong></div>
                            <div className="kv-row"><span>buy style</span><strong>{row.screeningDebug.buy_style_recommendation || "-"}</strong></div>
                            <div className="kv-row"><span>decision reason</span><strong>{row.screeningDebug.decision_reason || "-"}</strong></div>
                            <div className="kv-row"><span>skip reason</span><strong>{row.screeningDebug.skip_reason || "-"}</strong></div>
                          </div>
                        ) : null}
                      </details>
                    </article>
                  )})}
                </div>
              )}
            </section>
          </>
          </RenderGuard>
        )}

        {screen === "results" && (
          <RenderGuard>
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
              <h2>バックテスト / 評価</h2>
              <div className="filter-row" style={{ marginBottom: 10, flexWrap: "wrap", gap: 10 }}>
                <label>
                  <span>会場</span>
                  <select
                    value={evaluationFilters.venue}
                    onChange={(e) => setEvaluationFilters((prev) => ({ ...prev, venue: e.target.value }))}
                  >
                    <option value="all">全会場</option>
                    {(evaluationFilterOptions.venues || []).map((value) => (
                      <option key={`eval-venue-${value}`} value={value}>{value}</option>
                    ))}
                  </select>
                </label>
                <label>
                  <span>期間From</span>
                  <input
                    type="date"
                    value={evaluationFilters.date_from}
                    onChange={(e) => setEvaluationFilters((prev) => ({ ...prev, date_from: e.target.value }))}
                  />
                </label>
                <label>
                  <span>期間To</span>
                  <input
                    type="date"
                    value={evaluationFilters.date_to}
                    onChange={(e) => setEvaluationFilters((prev) => ({ ...prev, date_to: e.target.value }))}
                  />
                </label>
                <label>
                  <span>推奨レベル</span>
                  <select
                    value={evaluationFilters.recommendation_level}
                    onChange={(e) => setEvaluationFilters((prev) => ({ ...prev, recommendation_level: e.target.value }))}
                  >
                    <option value="all">全レベル</option>
                    <option value="recommended">Recommended</option>
                    <option value="caution">Caution</option>
                    <option value="not_recommended">Not Recommended</option>
                  </select>
                </label>
                <label>
                  <span>隊形</span>
                  <select
                    value={evaluationFilters.formation_pattern}
                    onChange={(e) => setEvaluationFilters((prev) => ({ ...prev, formation_pattern: e.target.value }))}
                  >
                    <option value="all">全隊形</option>
                    {(evaluationFilterOptions.formation_patterns || []).map((value) => (
                      <option key={`eval-formation-${value}`} value={value}>{value}</option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="filter-row" style={{ marginBottom: 10, flexWrap: "wrap", gap: 16 }}>
                {[
                  ["only_participated", "Participated only"],
                  ["only_recommended", "Recommended only"],
                  ["only_boat1_escape_predicted", "Boat1 escape only"],
                  ["only_outside_head_cases", "Outside-head cases only"]
                ].map(([key, label]) => (
                  <label key={key} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <input
                      type="checkbox"
                      checked={!!evaluationFilters[key]}
                      onChange={(e) => setEvaluationFilters((prev) => ({ ...prev, [key]: e.target.checked }))}
                    />
                    <span>{label}</span>
                  </label>
                ))}
                <button className="fetch-btn secondary" onClick={() => setEvaluationFilters({
                  venue: "all",
                  date_from: "",
                  date_to: "",
                  recommendation_level: "all",
                  formation_pattern: "all",
                  only_participated: false,
                  only_recommended: false,
                  only_boat1_escape_predicted: false,
                  only_outside_head_cases: false
                })}>
                  フィルタ解除
                </button>
              </div>
              <div className="stats-grid">
                <article className="card stat-card">
                  <span>検証済みレース</span>
                  <strong>{stats?.evaluation?.overall?.verified_race_count ?? 0}</strong>
                  <small>run_id {stats?.evaluation?.evaluation_run_id ?? "-"}</small>
                  <small>表示 {stats?.evaluation?.filtered_race_count ?? 0} / 全体 {stats?.evaluation?.total_available_race_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>3連単的中率</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.trifecta_hit_rate, 2)}%</strong>
                  <small>的中 {stats?.evaluation?.overall?.trifecta_hit_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>2連単的中率</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.exacta_hit_rate, 2)}%</strong>
                  <small>的中 {stats?.evaluation?.overall?.exacta_hit_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>頭的中率</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.head_hit_rate, 2)}%</strong>
                  <small>的中 {stats?.evaluation?.overall?.head_hit_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>2着精度</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.second_place_hit_rate, 2)}%</strong>
                  <small>3着精度 {formatMaybeNumber(stats?.evaluation?.overall?.third_place_hit_rate, 2)}%</small>
                </article>
                <article className="card stat-card">
                  <span>直近トレンド</span>
                  <strong>{formatSignedRateDelta(stats?.evaluation?.recent_trend?.trifecta_hit_rate_delta)}</strong>
                  <small>2連単 {formatSignedRateDelta(stats?.evaluation?.recent_trend?.exacta_hit_rate_delta)}</small>
                  <small>頭 {formatSignedRateDelta(stats?.evaluation?.recent_trend?.head_hit_rate_delta)}</small>
                </article>
              </div>
              <div className="stats-grid" style={{ marginTop: 10 }}>
                <article className="card stat-card">
                  <span>Boat1逃げ的中率</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.boat1_escape_hit_rate, 2)}%</strong>
                  <small>予測 {stats?.evaluation?.overall?.boat1_escape_prediction_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>Boat1相手精度</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.boat1_escape_opponent_hit_rate, 2)}%</strong>
                  <small>完全一致 {stats?.evaluation?.overall?.boat1_escape_opponent_hit_count ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>Participate Hit</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.participation_hit_rate, 2)}%</strong>
                  <small>件数 {stats?.evaluation?.overall?.participated_races ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>Recommended Hit</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.recommended_only_hit_rate, 2)}%</strong>
                  <small>Caution {formatMaybeNumber(stats?.evaluation?.overall?.caution_only_hit_rate, 2)}%</small>
                </article>
                <article className="card stat-card">
                  <span>Skip妥当性</span>
                  <strong>{formatMaybeNumber(stats?.evaluation?.overall?.skip_correctness_rate, 2)}%</strong>
                  <small>Skip {stats?.evaluation?.overall?.skipped_races ?? 0}</small>
                </article>
                <article className="card stat-card">
                  <span>質ゲート適用</span>
                  <strong>{stats?.evaluation?.overall?.quality_gate_applied_count ?? 0}</strong>
                  <small>Hit {formatMaybeNumber(stats?.evaluation?.overall?.quality_gate_hit_rate, 2)}%</small>
                </article>
              </div>
              <p className="muted strategy-line" style={{ marginTop: 8 }}>
                2着MISS {stats?.evaluation?.overall?.partner_selection_miss_count ?? 0} /
                3着MISS {stats?.evaluation?.overall?.third_place_noise_count ?? 0} /
                NEAR {stats?.evaluation?.overall?.near_miss_count ?? 0} /
                外攻め過剰 {stats?.evaluation?.overall?.outer_head_overpromotion_count ?? 0}
              </p>

              <div className="table-wrap" style={{ marginTop: 10 }}>
                <table>
                  <thead>
                    <tr>
                      <th>Strongest Venues</th>
                      <th>件数</th>
                      <th>3連単</th>
                      <th>2連単</th>
                      <th>頭</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(stats?.evaluation?.highlights?.strongest_venues || []).map((row) => (
                      <tr key={`eval-best-venue-${row.segment_key}`}>
                        <td>{row.segment_key}</td>
                        <td>{row.verified_race_count ?? 0}</td>
                        <td>{formatMaybeNumber(row.trifecta_hit_rate, 2)}%</td>
                        <td>{formatMaybeNumber(row.exacta_hit_rate, 2)}%</td>
                        <td>{formatMaybeNumber(row.head_hit_rate, 2)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <details style={{ marginTop: 10 }}>
                <summary>評価の詳細</summary>
                <div className="stats-grid" style={{ marginTop: 10 }}>
                  <article className="card stat-card">
                    <span>5頭回数</span>
                    <strong>{stats?.evaluation?.outside_head_monitoring?.boat5_main_head_count ?? 0}</strong>
                    <small>頭的中 {formatMaybeNumber(stats?.evaluation?.outside_head_monitoring?.boat5_main_head_first_hit_rate, 2)}%</small>
                  </article>
                  <article className="card stat-card">
                    <span>6頭回数</span>
                    <strong>{stats?.evaluation?.outside_head_monitoring?.boat6_main_head_count ?? 0}</strong>
                    <small>頭的中 {formatMaybeNumber(stats?.evaluation?.outside_head_monitoring?.boat6_main_head_first_hit_rate, 2)}%</small>
                  </article>
                  <article className="card stat-card">
                    <span>外頭推奨回数</span>
                    <strong>{stats?.evaluation?.outside_head_monitoring?.outside_head_recommendation_count ?? 0}</strong>
                    <small>外2/3残り {formatMaybeNumber(stats?.evaluation?.outside_head_monitoring?.outside_second_third_survival_rate, 2)}%</small>
                  </article>
                  <article className="card stat-card">
                    <span>Boat1逃げ診断</span>
                    <strong>{stats?.evaluation?.boat1_escape_diagnostics?.boat1_escape_prediction_count ?? 0}</strong>
                    <small>相手精度 {formatMaybeNumber(stats?.evaluation?.boat1_escape_diagnostics?.boat1_escape_opponent_hit_rate, 2)}%</small>
                  </article>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Miss Category</th>
                        <th>件数</th>
                        <th>率</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(stats?.evaluation?.miss_categories || []).map((row) => (
                        <tr key={`eval-miss-${row.category}`}>
                          <td>{row.category}</td>
                          <td>{row.count ?? 0}</td>
                          <td>{formatMaybeNumber(row.rate, 2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Confidence Bin</th>
                        <th>件数</th>
                        <th>3連単</th>
                        <th>平均確信度</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(stats?.evaluation?.confidence_calibration?.bet_confidence_bins || []).map((row) => (
                        <tr key={`eval-bet-bin-${row.bucket}`}>
                          <td>Bet {row.bucket}</td>
                          <td>{row.race_count ?? 0}</td>
                          <td>{formatMaybeNumber(row.hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber((row.average_confidence ?? 0) * 100, 1)}%</td>
                        </tr>
                      ))}
                      {(stats?.evaluation?.confidence_calibration?.head_confidence_bins || []).map((row) => (
                        <tr key={`eval-head-bin-${row.bucket}`}>
                          <td>Head {row.bucket}</td>
                          <td>{row.race_count ?? 0}</td>
                          <td>{formatMaybeNumber(row.hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber((row.average_confidence ?? 0) * 100, 1)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Weakest Venues</th>
                        <th>件数</th>
                        <th>3連単</th>
                        <th>2着</th>
                        <th>3着</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(stats?.evaluation?.highlights?.weakest_venues || []).map((row) => (
                        <tr key={`eval-worst-venue-${row.segment_key}`}>
                          <td>{row.segment_key}</td>
                          <td>{row.verified_race_count ?? 0}</td>
                          <td>{formatMaybeNumber(row.trifecta_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.second_place_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.third_place_hit_rate, 2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Strong / Weak Formation</th>
                        <th>件数</th>
                        <th>3連単</th>
                        <th>頭</th>
                        <th>2着</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[...(stats?.evaluation?.highlights?.strongest_formations || []), ...(stats?.evaluation?.highlights?.weakest_formations || [])].map((row, idx) => (
                        <tr key={`eval-formation-${row.segment_key}-${idx}`}>
                          <td>{row.segment_key}</td>
                          <td>{row.verified_race_count ?? 0}</td>
                          <td>{formatMaybeNumber(row.trifecta_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.head_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.second_place_hit_rate, 2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Rebalance Version</th>
                        <th>件数</th>
                        <th>3連単</th>
                        <th>2連単</th>
                        <th>頭</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(stats?.evaluation?.comparisons?.rebalance_version || []).map((row) => (
                        <tr key={`eval-rebalance-${row.segment_key}`}>
                          <td>{row.segment_key}</td>
                          <td>{row.verified_race_count ?? 0}</td>
                          <td>{formatMaybeNumber(row.trifecta_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.exacta_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.head_hit_rate, 2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Attack Scenario</th>
                        <th>件数</th>
                        <th>3連単</th>
                        <th>2着</th>
                        <th>参加Hit</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortEvaluationSegments(stats?.evaluation?.segmented_tables?.attack_scenario).slice(0, 8).map((row) => (
                        <tr key={`eval-attack-${row.segment_key}`}>
                          <td>{row.segment_key}</td>
                          <td>{row.verified_race_count ?? 0}</td>
                          <td>{formatMaybeNumber(row.trifecta_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.second_place_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.participation_hit_rate, 2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Recommendation Level</th>
                        <th>件数</th>
                        <th>3連単</th>
                        <th>2連単</th>
                        <th>参加Hit</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortEvaluationSegments(stats?.evaluation?.segmented_tables?.recommendation_level).map((row) => (
                        <tr key={`eval-rec-${row.segment_key}`}>
                          <td>{row.segment_key}</td>
                          <td>{row.verified_race_count ?? 0}</td>
                          <td>{formatMaybeNumber(row.trifecta_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.exacta_hit_rate, 2)}%</td>
                          <td>{formatMaybeNumber(row.participation_hit_rate, 2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Outside Monitor</th>
                        <th>件数</th>
                        <th>率</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[
                        ["outside_lead_overpromotion_count", "outside_lead overpromotion", stats?.evaluation?.outside_head_monitoring?.outside_lead_overpromotion_count],
                        ["chaos_or_not_recommended_outside_head_count", "chaos/not_recommended with outside head", stats?.evaluation?.outside_head_monitoring?.chaos_or_not_recommended_outside_head_count]
                      ].map(([key, label, value]) => (
                        <tr key={`eval-outside-${key}`}>
                          <td>{label}</td>
                          <td>{value ?? 0}</td>
                          <td>-</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="table-wrap" style={{ marginTop: 10 }}>
                  <table>
                    <thead>
                      <tr>
                        <th>Boat1 Family</th>
                        <th>予測</th>
                        <th>捕捉</th>
                        <th>率</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(stats?.evaluation?.boat1_escape_diagnostics?.family_capture_rows || []).map((row) => (
                        <tr key={`eval-boat1fam-${row.family}`}>
                          <td>{row.family}</td>
                          <td>{row.prediction_count ?? 0}</td>
                          <td>{row.captured_count ?? 0}</td>
                          <td>{formatMaybeNumber(row.capture_rate, 2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </details>
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
                    const transientVerifyStatus = verificationRunStatusByRace[verificationKey];
                    const transientVerifyReason = verificationReasonByRace[verificationKey];
                    const currentVerifyStatus = verifyingRaceId === verificationKey
                      ? (transientVerifyStatus || h.verification_status || "PENDING_RESULT")
                      : (h.verification_status || transientVerifyStatus || "PENDING_RESULT");
                    const verifyReason = verifyingRaceId === verificationKey
                      ? (transientVerifyReason || h.verification_reason || h?.verification?.summary?.warning || "")
                      : (h.verification_reason || h?.verification?.summary?.warning || transientVerifyReason || "");
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
                            <div>boat1 partner model: {h?.feature_snapshot_debug?.boat1_partner_model_applied ? "YES" : "NO"}</div>
                            <div>boat1 partner version: {h?.feature_snapshot_debug?.boat1_escape_partner_version || "-"}</div>
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
          </RenderGuard>
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



