-- Durable per-subnet economics time-series (D1 mirror of the postgres
-- economics_history table, which exists in deploy/postgres/schema.sql but is
-- unpopulated and not queryable by this Worker).
--
-- The live economics tier (KV `economics:current`, refreshed every few hours)
-- only ever reflects "now". To answer "how did this subnet's alpha price /
-- emission share / stake / registration cost move over time?" we snapshot one
-- row per subnet per UTC day off the live tier on the daily neuron-history
-- rollup cron (workers/config.mjs NEURON_HISTORY_ROLLUP_CRON, "47 5 * * *").
--
-- One row per (netuid, snapshot_date); the PK makes an intra-day re-run an
-- idempotent INSERT OR REPLACE. The table accrues history going forward only —
-- the read endpoint (/api/v1/subnets/{netuid}/economics/history) is correct from
-- day one but sparse until the rollup has run for a while.

CREATE TABLE IF NOT EXISTS economics_history (
  netuid             INTEGER NOT NULL,
  snapshot_date      TEXT    NOT NULL,            -- UTC date, YYYY-MM-DD
  alpha_price_tao    REAL,
  emission_share     REAL,
  total_stake_tao    REAL,
  registration_cost  REAL,
  PRIMARY KEY (netuid, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_economics_history_netuid_date
  ON economics_history (netuid, snapshot_date);
