const VALID_VENUE_IDS = new Set(Array.from({ length: 24 }, (_, index) => index + 1));

function toTrimmedString(value) {
  return String(value ?? "").trim();
}

export function normalizeVenueIdInput(value) {
  if (Number.isInteger(value) && VALID_VENUE_IDS.has(value)) return value;
  const text = toTrimmedString(value);
  if (!/^\d+$/.test(text)) return null;
  const numeric = Number(text);
  return Number.isInteger(numeric) && VALID_VENUE_IDS.has(numeric) ? numeric : null;
}

export function normalizeRaceNoInput(value) {
  const text = toTrimmedString(value);
  if (!/^\d+$/.test(text)) return null;
  const numeric = Number(text);
  return Number.isInteger(numeric) && numeric >= 1 && numeric <= 12 ? numeric : null;
}

export function buildRaceRequestValidationError(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  Object.assign(error, details);
  return error;
}

function resolveBaseOrigin(baseOrigin) {
  if (baseOrigin) return String(baseOrigin);
  if (typeof window !== "undefined" && window.location?.origin) return window.location.origin;
  return "https://example.invalid";
}

function resolveApiRouteUrl(apiBase, routePath, baseOrigin) {
  const normalizedBase = String(apiBase || "/api").replace(/\/+$/, "");
  if (/^https?:\/\//i.test(normalizedBase)) {
    return new URL(`${normalizedBase}${routePath}`);
  }
  return new URL(`${normalizedBase}${routePath}`, resolveBaseOrigin(baseOrigin));
}

export function buildRaceApiRequest({
  apiBase = "/api",
  baseOrigin = null,
  date,
  venueId,
  raceNo,
  options = {}
} = {}) {
  const normalizedDate = toTrimmedString(date);
  const normalizedVenueId = normalizeVenueIdInput(venueId);
  const normalizedRaceNo = normalizeRaceNoInput(raceNo);

  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedDate)) {
    throw buildRaceRequestValidationError(
      "INVALID_RACE_DATE_FRONTEND",
      "日付が不正です。YYYY-MM-DD を指定してください。",
      { dateRaw: date }
    );
  }
  if (normalizedVenueId === null) {
    throw buildRaceRequestValidationError(
      "INVALID_VENUE_ID_FRONTEND",
      "会場IDが不正です。1〜24 の整数を選択してください。",
      { venueIdRaw: venueId }
    );
  }
  if (normalizedRaceNo === null) {
    throw buildRaceRequestValidationError(
      "INVALID_RACE_NO_FRONTEND",
      "レース番号が不正です。1〜12 を指定してください。",
      { raceNoRaw: raceNo }
    );
  }

  const url = resolveApiRouteUrl(apiBase, "/race", baseOrigin);
  url.searchParams.set("date", normalizedDate);
  url.searchParams.set("venueId", String(normalizedVenueId));
  url.searchParams.set("raceNo", String(normalizedRaceNo));

  if (options?.forceRefresh) url.searchParams.set("forceRefresh", "1");
  if (options?.screeningMode) url.searchParams.set("screening", String(options.screeningMode));
  if (Number.isFinite(Number(options?.getRaceDataTimeoutMs))) {
    url.searchParams.set("getRaceDataTimeoutMs", String(Number(options.getRaceDataTimeoutMs)));
  }
  if (Number.isFinite(Number(options?.dataFetchTimeoutMs))) {
    url.searchParams.set("dataFetchTimeoutMs", String(Number(options.dataFetchTimeoutMs)));
  }

  return {
    url,
    normalized: {
      date: normalizedDate,
      venueId: normalizedVenueId,
      raceNo: normalizedRaceNo
    }
  };
}

export function sanitizeRecentRaceSelections(rows = []) {
  const normalizedRows = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    const normalizedVenueId = normalizeVenueIdInput(row?.venueId);
    const normalizedRaceNo = normalizeRaceNoInput(row?.raceNo);
    const normalizedDate = toTrimmedString(row?.date);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedDate)) continue;
    if (normalizedVenueId === null || normalizedRaceNo === null) continue;
    normalizedRows.push({
      date: normalizedDate,
      venueId: normalizedVenueId,
      venueName: toTrimmedString(row?.venueName) || null,
      raceNo: normalizedRaceNo
    });
  }
  return normalizedRows;
}
