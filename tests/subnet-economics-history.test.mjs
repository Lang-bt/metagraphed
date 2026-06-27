// Per-subnet economics HISTORY (#1307): the daily rollup writer that snapshots
// the live economics tier into the dated economics_history D1 table, the read
// builder, and the GET /api/v1/subnets/{netuid}/economics/history endpoint
// (window parsing + response shape) — both directly and through the Worker
// dispatch, mirroring the sibling /history coverage in neuron-history.test.mjs.

import assert from "node:assert/strict";
import { describe, test } from "vitest";
import {
  rollupEconomicsHistory,
  buildEconomicsHistory,
  economicsSnapshotDate,
  isEconomicsSnapshotDate,
  ECONOMICS_HISTORY_COLUMNS,
} from "../src/economics-history.mjs";
import { handleSubnetEconomicsHistory } from "../workers/request-handlers/entities.mjs";
import { handleRequest, handleScheduled } from "../workers/api.mjs";
import { NEURON_HISTORY_ROLLUP_CRON } from "../workers/config.mjs";
import { MAX_HISTORY_POINTS } from "../src/neuron-history.mjs";
import { KV_ECONOMICS_CURRENT } from "../src/kv-keys.mjs";
import { createLocalArtifactEnv } from "../scripts/lib.mjs";

const ctx = { waitUntil: (p) => p };

// One live economics blob row (the `economics:current` KV blob's `subnets` shape).
function econRow(overrides = {}) {
  return {
    netuid: 7,
    alpha_price_tao: 0.042,
    emission_share: 0.013,
    total_stake_tao: 123456.7,
    registration_cost_tao: 1.5,
    ...overrides,
  };
}

// One economics_history read row (ECONOMICS_HISTORY columns minus netuid + date).
function historyRow(overrides = {}) {
  return {
    snapshot_date: "2026-06-20",
    alpha_price_tao: 0.042,
    emission_share: 0.013,
    total_stake_tao: 123456.7,
    registration_cost: 1.5,
    ...overrides,
  };
}

// Stub METAGRAPH_HEALTH_DB whose .all() returns the given rows + records the SQL.
function readEnv(rows, captured = {}) {
  return {
    ...createLocalArtifactEnv(),
    METAGRAPH_HEALTH_DB: {
      prepare(sql) {
        captured.sql = sql;
        return {
          bind(...params) {
            captured.params = params;
            return { all: () => Promise.resolve({ results: rows }) };
          },
        };
      },
    },
  };
}

describe("economicsSnapshotDate / isEconomicsSnapshotDate", () => {
  test("derives the UTC day and validates the YYYY-MM-DD shape", () => {
    assert.equal(
      economicsSnapshotDate(Date.parse("2026-06-20T23:59:59Z")),
      "2026-06-20",
    );
    assert.equal(isEconomicsSnapshotDate("2026-06-20"), true);
    assert.equal(isEconomicsSnapshotDate("2026-6-2"), false);
    assert.equal(isEconomicsSnapshotDate(20260620), false);
  });
});

describe("rollupEconomicsHistory", () => {
  test("writes one INSERT OR REPLACE per subnet for the captured UTC day", async () => {
    const statements = [];
    const env = {
      METAGRAPH_HEALTH_DB: {
        prepare(sql) {
          return {
            bind(...params) {
              statements.push({ sql, params });
              return { run: () => Promise.resolve({ meta: { changes: 1 } }) };
            },
          };
        },
        batch: (stmts) => Promise.resolve(stmts.map(() => ({}))),
      },
    };
    const blob = { subnets: [econRow(), econRow({ netuid: 12 })] };
    const res = await rollupEconomicsHistory(env, blob, {
      now: Date.parse("2026-06-20T05:47:00Z"),
    });
    assert.deepEqual(res, {
      rolled: true,
      snapshot_date: "2026-06-20",
      rows: 2,
    });
    assert.equal(statements.length, 2);
    // INSERT OR REPLACE keyed on (netuid, snapshot_date) → idempotent intra-day.
    assert.match(statements[0].sql, /INSERT OR REPLACE INTO economics_history/);
    // registration_cost is sourced from registration_cost_tao, in column order.
    assert.deepEqual(statements[0].params, [
      7,
      "2026-06-20",
      0.042,
      0.013,
      123456.7,
      1.5,
    ]);
    assert.equal(statements[1].params[0], 12);
  });

  test("maps a missing/non-finite metric to null and drops a row without a netuid", async () => {
    const statements = [];
    const env = {
      METAGRAPH_HEALTH_DB: {
        prepare() {
          return {
            bind(...params) {
              statements.push(params);
              return { run: () => Promise.resolve({}) };
            },
          };
        },
      },
    };
    const blob = {
      subnets: [
        econRow({ alpha_price_tao: undefined, registration_cost_tao: null }),
        { alpha_price_tao: 1 }, // no netuid → dropped
      ],
    };
    const res = await rollupEconomicsHistory(env, blob, {
      now: Date.parse("2026-06-20T00:00:00Z"),
    });
    assert.equal(res.rows, 1);
    // [netuid, date, alpha(null), emission, stake, registration(null)]
    assert.equal(statements[0][2], null);
    assert.equal(statements[0][5], null);
  });

  test("no-ops cleanly without a DB binding (cron isolation)", async () => {
    assert.deepEqual(
      await rollupEconomicsHistory({}, { subnets: [econRow()] }),
      {
        rolled: false,
        reason: "no-db",
      },
    );
  });

  test("no-ops when the economics blob is absent or has no subnets", async () => {
    const env = {
      METAGRAPH_HEALTH_DB: { prepare: () => ({ bind: () => ({}) }) },
    };
    assert.equal(
      (await rollupEconomicsHistory(env, null)).reason,
      "no-economics",
    );
    assert.equal(
      (await rollupEconomicsHistory(env, { subnets: [] })).reason,
      "no-rows",
    );
  });
});

describe("buildEconomicsHistory", () => {
  test("shapes a per-subnet economics series (one point per snapshot_date)", () => {
    const out = buildEconomicsHistory([historyRow()], 7, { window: "30d" });
    assert.equal(out.schema_version, 1);
    assert.equal(out.netuid, 7);
    assert.equal(out.window, "30d");
    assert.equal(out.point_count, 1);
    assert.deepEqual(out.points[0], {
      snapshot_date: "2026-06-20",
      alpha_price_tao: 0.042,
      emission_share: 0.013,
      total_stake_tao: 123456.7,
      registration_cost: 1.5,
    });
  });

  test("ECONOMICS_HISTORY_COLUMNS carries the canonical bind order", () => {
    assert.deepEqual(ECONOMICS_HISTORY_COLUMNS, [
      "netuid",
      "snapshot_date",
      "alpha_price_tao",
      "emission_share",
      "total_stake_tao",
      "registration_cost",
    ]);
  });
});

describe("handleSubnetEconomicsHistory (direct)", () => {
  test("a bounded window binds a snapshot_date cutoff + the row cap", async () => {
    const captured = {};
    const env = readEnv([historyRow()], captured);
    const res = await handleSubnetEconomicsHistory(
      new Request(
        "https://api.metagraph.sh/api/v1/subnets/7/economics/history",
      ),
      env,
      7,
      new URL(
        "https://api.metagraph.sh/api/v1/subnets/7/economics/history?window=7d",
      ),
    );
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.data.points[0].alpha_price_tao, 0.042);
    assert.match(captured.sql, /FROM economics_history WHERE netuid = \?/);
    assert.match(captured.sql, /snapshot_date >= \?/);
    assert.ok(captured.params.includes(MAX_HISTORY_POINTS));
  });

  test("?window=all omits the cutoff (still bounded by the row cap)", async () => {
    const captured = {};
    const env = readEnv([historyRow()], captured);
    await handleSubnetEconomicsHistory(
      new Request(
        "https://api.metagraph.sh/api/v1/subnets/7/economics/history",
      ),
      env,
      7,
      new URL(
        "https://api.metagraph.sh/api/v1/subnets/7/economics/history?window=all",
      ),
    );
    assert.doesNotMatch(captured.sql, /snapshot_date >= \?/);
    assert.ok(captured.params.includes(MAX_HISTORY_POINTS));
  });

  test("returns a schema-stable empty series on a cold D1 (never 404)", async () => {
    const env = readEnv([]);
    const res = await handleSubnetEconomicsHistory(
      new Request(
        "https://api.metagraph.sh/api/v1/subnets/7/economics/history",
      ),
      env,
      7,
      new URL("https://api.metagraph.sh/api/v1/subnets/7/economics/history"),
    );
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.data.netuid, 7);
    assert.equal(body.data.point_count, 0);
    assert.deepEqual(body.data.points, []);
  });
});

describe("economics history endpoint (via the Worker dispatch)", () => {
  test("GET /subnets/{n}/economics/history returns a 200 series", async () => {
    const env = readEnv([historyRow()]);
    const res = await handleRequest(
      new Request(
        "https://api.metagraph.sh/api/v1/subnets/7/economics/history?window=90d",
      ),
      env,
      ctx,
    );
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.data.netuid, 7);
    assert.equal(body.data.window, "90d");
    assert.equal(body.data.points[0].emission_share, 0.013);
  });

  test("an unsupported ?window is a 400, never a silent coerce", async () => {
    const res = await handleRequest(
      new Request(
        "https://api.metagraph.sh/api/v1/subnets/7/economics/history?window=400d",
      ),
      readEnv([]),
      ctx,
    );
    assert.equal(res.status, 400);
    const body = await res.json();
    assert.equal(body.error.code, "invalid_query");
    assert.equal(body.meta.parameter, "window");
  });
});

describe("daily rollup cron also snapshots economics (#1307)", () => {
  test("handleScheduled writes economics_history off the live economics KV", async () => {
    const econInserts = [];
    const db = {
      prepare(sql) {
        return {
          bind(...params) {
            if (/INSERT OR REPLACE INTO economics_history/.test(sql)) {
              econInserts.push(params);
            }
            return {
              run: () => Promise.resolve({ meta: { changes: 1 } }),
              all: () => Promise.resolve({ results: [] }),
            };
          },
        };
      },
      batch: (stmts) => Promise.resolve(stmts.map(() => ({}))),
    };
    const blob = {
      // Fresh enough + on-contract is NOT required here: the cron reads the raw
      // KV blob via readEconomicsCurrentKv (no freshness gate on the writer path).
      subnets: [econRow(), econRow({ netuid: 12 })],
    };
    const env = {
      METAGRAPH_HEALTH_DB: db,
      METAGRAPH_CONTROL: {
        get: (key) =>
          key === KV_ECONOMICS_CURRENT
            ? Promise.resolve(blob)
            : Promise.resolve(null),
      },
    };
    const result = await handleScheduled(
      { cron: NEURON_HISTORY_ROLLUP_CRON },
      env,
      ctx,
    );
    assert.equal(result.economics.rolled, true);
    assert.equal(result.economics.rows, 2);
    assert.equal(econInserts.length, 2);
    assert.equal(econInserts[0][0], 7);
    assert.equal(econInserts[1][0], 12);
  });

  test("a cold economics KV leaves the rollup isolated (no-economics)", async () => {
    const env = {
      METAGRAPH_HEALTH_DB: {
        prepare: () => ({
          bind: () => ({
            run: () => Promise.resolve({ meta: { changes: 0 } }),
            all: () => Promise.resolve({ results: [] }),
          }),
        }),
      },
      METAGRAPH_CONTROL: { get: () => Promise.resolve(null) },
    };
    const result = await handleScheduled(
      { cron: NEURON_HISTORY_ROLLUP_CRON },
      env,
      ctx,
    );
    assert.equal(result.economics.rolled, false);
    assert.equal(result.economics.reason, "no-economics");
  });
});
