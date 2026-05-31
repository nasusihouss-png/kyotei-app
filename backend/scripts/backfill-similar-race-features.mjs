#!/usr/bin/env node
import { backfillSimilarRaceFeatures } from "../similar-race-feature-store.js";

const PRIORITY_VENUE_IDS = [24, 18, 21, 13, 5, 12];

function parseArgs(argv = []) {
  const args = {
    dateFrom: null,
    dateTo: null,
    venueIds: PRIORITY_VENUE_IDS,
    dryRun: false,
    limit: 4000,
    progressEvery: 100
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const next = argv[i + 1];
    if (arg === "--from" || arg === "--date-from") {
      args.dateFrom = next;
      i += 1;
    } else if (arg === "--to" || arg === "--date-to") {
      args.dateTo = next;
      i += 1;
    } else if (arg === "--venues" || arg === "--venue-ids") {
      args.venueIds = String(next || "")
        .split(",")
        .map((value) => Number(value.trim()))
        .filter(Number.isInteger);
      i += 1;
    } else if (arg === "--all-venues") {
      args.venueIds = null;
    } else if (arg === "--dry-run") {
      args.dryRun = true;
    } else if (arg === "--limit") {
      const n = Number(next);
      args.limit = Number.isFinite(n) && n > 0 ? Math.trunc(n) : args.limit;
      i += 1;
    } else if (arg === "--progress-every") {
      const n = Number(next);
      args.progressEvery = Number.isFinite(n) && n >= 0 ? Math.trunc(n) : args.progressEvery;
      i += 1;
    } else if (arg === "--help" || arg === "-h") {
      args.help = true;
    }
  }

  return args;
}

function printHelp() {
  console.log(`
Usage:
  node backend/scripts/backfill-similar-race-features.mjs [options]

Options:
  --from YYYY-MM-DD          Start date, inclusive.
  --to YYYY-MM-DD            End date, inclusive.
  --venues 24,18,21          Venue IDs to import. Defaults to priority venues.
  --all-venues               Disable venue filter.
  --dry-run                  Count what would be inserted/updated without writing.
  --limit 4000               Max prediction rows to scan.
  --progress-every 100       Emit progress every N writes. Use 0 for final only.

Default priority venues:
  24 Omura, 18 Tokuyama, 21 Ashiya, 13 Amagasaki, 5 Tamagawa, 12 Suminoe
`.trim());
}

const args = parseArgs(process.argv.slice(2));
if (args.help) {
  printHelp();
  process.exit(0);
}

const summary = backfillSimilarRaceFeatures({
  ...args,
  logger: (event) => {
    console.log(JSON.stringify(event));
  }
});

console.log(JSON.stringify({
  ok: true,
  ...summary
}, null, 2));
