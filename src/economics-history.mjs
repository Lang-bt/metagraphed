// Per-subnet economics HISTORY (depth epic #1307 / time-series economics).
//
// The live economics tier (KV `economics:current`) only ever holds "now". To
// expose how a subnet's alpha price / emission share / stake / registration cost
// move over time, the daily neuron-history rollup cron also snapshots that live
// blob into the dated `economics_history` D1 table (migrations/0024) — one row per
// subnet per UTC day. The read builder below mirrors buildSubnetHistory: pure +
// injectable so the Worker handler just runs the D1 query and calls these, and the
// tests exercise the shape without a live binding.

// Columns written per snapshot row, in the canonical bind order. `registration_cost`
// is sourced from the live row's `registration_cost_tao` (the economics blob keeps
// the `_tao` suffix; the table column drops it, matching the postgres schema).
export const ECONOMICS_HISTORY_COLUMNS = [
  "netuid",
  "snapshot_date",
  "alpha_price_tao",
  "emission_share",
  "total_stake_tao",
  "registration_cost",
];

const SNAPSHOT_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function finiteOrNull(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

// The UTC day (YYYY-MM-DD) for a given epoch-ms instant — the snapshot's date key.
export function economicsSnapshotDate(now = Date.now()) {
  return new Date(now).toISOString().slice(0, 10);
}

// Project one live economics row (from the `economics:current` blob's `subnets`
// array) into a snapshot row. Returns null for a row without a usable integer
// netuid so a malformed entry never poisons the day's write.
function snapshotRow(row, snapshotDate) {
  if (!row || typeof row !== "object") return null;
  const netuid = Number(row.netuid);
  if (!Number.isInteger(netuid) || netuid < 0) return null;
  return {
    netuid,
    snapshot_date: snapshotDate,
    alpha_price_tao: finiteOrNull(row.alpha_price_tao),
    emission_share: finiteOrNull(row.emission_share),
    total_stake_tao: finiteOrNull(row.total_stake_tao),
    registration_cost: finiteOrNull(row.registration_cost_tao),
  };
}

/**
 * Daily rollup: snapshot the live per-subnet economics blob into
 * `economics_history` for the captured UTC day. The blob is read by the caller
 * (workers/api.mjs reads KV `economics:current` via readEconomicsCurrentKv) and
 * passed in, keeping this module free of any KV/binding dependency.
 *
 *  - One INSERT OR REPLACE per subnet, batched through db.batch when available so
 *    the whole day is a single round trip; the PK (netuid, snapshot_date) makes an
 *    intra-day re-run idempotent (the row reflects the last write that day).
 *  - snapshot_date defaults to the UTC day of `now`.
 * Returns {rolled, rows} for cron observability; the caller .catch-isolates it so a
 * failure never affects the rest of the scheduled run.
 */
export async function rollupEconomicsHistory(
  env,
  blob,
  { now = Date.now() } = {},
) {
  const db = env?.METAGRAPH_HEALTH_DB;
  if (!db?.prepare) return { rolled: false, reason: "no-db" };
  const subnets = Array.isArray(blob?.subnets) ? blob.subnets : null;
  if (!subnets) return { rolled: false, reason: "no-economics" };
  const snapshotDate = economicsSnapshotDate(now);
  const placeholders = ECONOMICS_HISTORY_COLUMNS.map(() => "?").join(", ");
  const sql =
    `INSERT OR REPLACE INTO economics_history (${ECONOMICS_HISTORY_COLUMNS.join(", ")}) ` +
    `VALUES (${placeholders})`;
  const statements = [];
  for (const row of subnets) {
    const snap = snapshotRow(row, snapshotDate);
    if (!snap) continue;
    statements.push(
      db.prepare(sql).bind(...ECONOMICS_HISTORY_COLUMNS.map((c) => snap[c])),
    );
  }
  if (statements.length === 0) {
    return { rolled: false, reason: "no-rows", snapshot_date: snapshotDate };
  }
  if (typeof db.batch === "function") {
    await db.batch(statements);
  } else {
    for (const stmt of statements) await stmt.run();
  }
  return { rolled: true, snapshot_date: snapshotDate, rows: statements.length };
}

// SELECT list for reading an economics_history row back (the snapshot columns).
export const ECONOMICS_HISTORY_READ_COLUMNS = ECONOMICS_HISTORY_COLUMNS.filter(
  (c) => c !== "netuid",
).join(", ");

// Per-subnet economics metric-over-time: one point per snapshot_date (the handler
// queries newest first, bounded by MAX_HISTORY_POINTS), mirroring buildSubnetHistory.
export function buildEconomicsHistory(rows, netuid, { window } = {}) {
  return {
    schema_version: 1,
    netuid,
    window: window ?? null,
    point_count: rows.length,
    points: rows.map((r) => ({
      snapshot_date: r.snapshot_date,
      alpha_price_tao: r.alpha_price_tao ?? null,
      emission_share: r.emission_share ?? null,
      total_stake_tao: r.total_stake_tao ?? null,
      registration_cost: r.registration_cost ?? null,
    })),
  };
}

// Re-export so callers can validate a backfill-style snapshot date if needed.
export function isEconomicsSnapshotDate(value) {
  return typeof value === "string" && SNAPSHOT_DATE_RE.test(value);
}
