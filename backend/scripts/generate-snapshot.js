import {
  generateDateSnapshots,
  generateRaceSnapshot,
  generateVenueSnapshots,
  summarizeSnapshotGenerationResults
} from "../src/services/snapshot-generator.js";

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = String(argv[index] || "");
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || String(next).startsWith("--")) {
      args[key] = true;
      continue;
    }
    args[key] = next;
    index += 1;
  }
  return args;
}

function toInt(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function printUsage() {
  console.log(`
Usage:
  npm run snapshot:generate -- --date YYYY-MM-DD --venueId 19 --raceNo 1
  npm run snapshot:generate -- --date YYYY-MM-DD --venueId 19 --all-races
  npm run snapshot:generate -- --date YYYY-MM-DD --all-venues
  node scripts/generate-snapshot.js --help

PowerShell:
  Set-Location backend
  cmd /c npm run snapshot:generate -- --date YYYY-MM-DD --venueId 19 --raceNo 1
  cmd /c npm run snapshot:generate -- --date YYYY-MM-DD --venueId 19 --all-races
  cmd /c npm run snapshot:generate -- --date YYYY-MM-DD --all-venues
  cmd /c npm run snapshot:help

Options:
  --date YYYY-MM-DD
  --venueId NUMBER
  --raceNo NUMBER
  --all-races
  --all-venues
  --timeoutMs NUMBER
  --no-kyotei
  --no-force-refresh
  --json
  --help

Behavior:
  - single race: --date + --venueId + --raceNo
  - venue batch: --date + --venueId + --all-races
  - date batch: --date + --all-venues
  - output includes saved snapshot counts and snapshot index status
  - PowerShell users can use "cmd /c npm ..." to avoid npm.ps1 execution policy issues
`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || args.h) {
    printUsage();
    process.exit(0);
  }

  const date = String(args.date || "").trim();
  const venueId = toInt(args.venueId, null);
  const raceNo = toInt(args.raceNo, null);
  const timeoutMs = Math.max(2000, Math.min(toInt(args.timeoutMs, 15000), 30000));
  const includeKyoteiBiyori = !args["no-kyotei"];
  const forceRefresh = !args["no-force-refresh"];
  const asJson = !!args.json;
  const modeFlags = [!!args["all-venues"], !!args["all-races"]].filter(Boolean).length;

  if (!date) {
    printUsage();
    throw new Error("--date is required");
  }
  if (modeFlags > 1) {
    printUsage();
    throw new Error("--all-races and --all-venues cannot be used together");
  }

  let results;
  if (args["all-venues"]) {
    results = await generateDateSnapshots({
      date,
      timeoutMs,
      includeKyoteiBiyori,
      forceRefresh
    });
  } else if (args["all-races"]) {
    if (!venueId) throw new Error("--venueId is required with --all-races");
    results = await generateVenueSnapshots({
      date,
      venueId,
      timeoutMs,
      includeKyoteiBiyori,
      forceRefresh
    });
  } else {
    if (!venueId || !raceNo) throw new Error("--venueId and --raceNo are required for single-race generation");
    results = [await generateRaceSnapshot({
      date,
      venueId,
      raceNo,
      timeoutMs,
      includeKyoteiBiyori,
      forceRefresh
    })];
  }

  const summary = summarizeSnapshotGenerationResults(results);
  if (asJson) {
    console.log(JSON.stringify({ summary, results }, null, 2));
    return;
  }

  console.log(`snapshot generation summary: total=${summary.total} ok=${summary.ok} failed=${summary.failed}`);
  for (const row of results) {
    if (row?.ok) {
      console.log(`[OK] ${row.date} venue=${row.venueId} race=${row.raceNo} race_id=${row.raceId} status=${row.snapshotIndex?.snapshotStatus || "READY"} feature_snapshot=${row.saved?.feature_snapshot || 0} total_ms=${row.timing?.total_ms || 0}`);
    } else {
      console.log(`[FAIL] ${row?.date || "-"} venue=${row?.venueId || "-"} race=${row?.raceNo || "-"} code=${row?.code || "SNAPSHOT_GENERATION_FAILED"} message=${row?.message || "unknown_error"}`);
    }
  }

  if (summary.failed > 0) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(`[snapshot-generator] ${error?.message || error}`);
  process.exit(1);
});
