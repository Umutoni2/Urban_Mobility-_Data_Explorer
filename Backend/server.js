'use strict';

/**
 * NYC Taxi Explorer — Backend Server
 * Node.js + SQLite (better-sqlite3)
 * Single-file backend: data processing, DB management, REST API, static serving
 */

const http     = require('http');
const fs       = require('fs');
const path     = require('path');
const readline = require('readline');
const url      = require('url');
const Database = require('better-sqlite3');

const PORT     = 3001;
const DB_PATH  = path.join(__dirname, 'taxi.db');
const CSV_PATH = path.join(__dirname, '../train.csv');

// ═══════════════════════════════════════════════════════════════════
// ALGORITHM IMPLEMENTATIONS (manual — no built-in library functions)
// ═══════════════════════════════════════════════════════════════════

/**
 * Haversine Distance Formula (manual implementation)
 * Computes great-circle distance between two GPS coordinates.
 * Time complexity: O(1)  Space complexity: O(1)
 *
 * Pseudo-code:
 *   R = 6371 (Earth radius km)
 *   dLat = (lat2 - lat1) * π/180
 *   dLon = (lon2 - lon1) * π/180
 *   a = sin²(dLat/2) + cos(lat1r)*cos(lat2r)*sin²(dLon/2)
 *   return R * 2 * atan2(√a, √(1−a))
 */
function haversine(lat1, lon1, lat2, lon2) {
  const R       = 6371;
  const toRad   = x => x * Math.PI / 180;
  const dLat    = toRad(lat2 - lat1);
  const dLon    = toRad(lon2 - lon1);
  const sinDLat = Math.sin(dLat / 2);
  const sinDLon = Math.sin(dLon / 2);
  const a = sinDLat * sinDLat +
            Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * sinDLon * sinDLon;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * Z-Score Anomaly Detection (manual implementation)
 * Uses variance shortcut: Var(X) = E[X²] − (E[X])²
 * Both computed in a single SQL aggregation — O(n) time, O(1) space.
 *
 * Pseudo-code:
 *   mean     = sumX / n
 *   variance = (sumX2 / n) − mean²
 *   std      = √variance
 *   z_i      = (x_i − mean) / std
 *   flag if |z_i| > threshold
 */
function computeZScoreStats(sumX, sumX2, n) {
  if (n === 0) return { mean: 0, std: 1 };
  const mean     = sumX / n;
  const variance = Math.max(0, (sumX2 / n) - (mean * mean));
  const std      = Math.sqrt(variance);
  return { mean, std: std > 0.0001 ? std : 1 };
}

/**
 * Fare Estimation (derived feature)
 * NYC taxi approximation: base + per-km + per-minute components
 * $2.50 base + $1.56/km (≈ $2.50/mile) + $0.35/min idle surcharge
 */
function estimateFare(distKm, durationSec) {
  return 2.50 + (distKm * 1.56) + ((durationSec / 60) * 0.35);
}

// Day name lookup (no library)
const DAY_NAMES = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];

// ═══════════════════════════════════════════════════════════════════
// DATABASE SETUP — normalized schema
// ═══════════════════════════════════════════════════════════════════

const db = new Database(DB_PATH);

// Maximum SQLite performance for bulk loading
db.exec(`
  PRAGMA journal_mode   = WAL;
  PRAGMA synchronous    = OFF;
  PRAGMA cache_size     = -131072;
  PRAGMA temp_store     = MEMORY;
  PRAGMA mmap_size      = 536870912;
  PRAGMA page_size      = 65536;
  PRAGMA locking_mode   = EXCLUSIVE;
`);

db.exec(`
  -- ── Dimension: vendors ──────────────────────────────────────────
  CREATE TABLE IF NOT EXISTS vendors (
    vendor_id   INTEGER PRIMARY KEY,
    vendor_name TEXT NOT NULL
  );

  INSERT OR IGNORE INTO vendors VALUES (1, 'Vendor 1');
  INSERT OR IGNORE INTO vendors VALUES (2, 'Vendor 2');

  -- ── Dimension: time_dims ────────────────────────────────────────
  -- One row per unique (hour, day_of_week, month) combination.
  -- Pre-computed so trips table only stores a FK integer.
  CREATE TABLE IF NOT EXISTS time_dims (
    time_id     INTEGER PRIMARY KEY,
    hour        INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    month       INTEGER NOT NULL
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_time_dims_hk ON time_dims(hour, day_of_week, month);

  -- ── Fact: trips ──────────────────────────────────────────────────
  CREATE TABLE IF NOT EXISTS trips (
    id                TEXT    PRIMARY KEY,
    vendor_id         INTEGER REFERENCES vendors(vendor_id),
    pickup_datetime   TEXT    NOT NULL,
    dropoff_datetime  TEXT,
    passenger_count   INTEGER NOT NULL,
    pickup_longitude  REAL    NOT NULL,
    pickup_latitude   REAL    NOT NULL,
    dropoff_longitude REAL    NOT NULL,
    dropoff_latitude  REAL    NOT NULL,
    store_and_fwd_flag TEXT,
    trip_duration     INTEGER NOT NULL,
    trip_distance_km  REAL    NOT NULL,
    speed_kmh         REAL    NOT NULL,
    fare_estimate     REAL    NOT NULL,
    time_id           INTEGER REFERENCES time_dims(time_id)
  );

  -- ── Pre-aggregated stats cache ───────────────────────────────────
  -- Populated once after ETL. All chart/KPI queries read from here
  -- instead of scanning the full trips table — guarantees <1s responses.
  CREATE TABLE IF NOT EXISTS stats_cache (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL        -- JSON blob
  );

  -- ── Meta ─────────────────────────────────────────────────────────
  CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
  );

  -- ── Indexes on trips ─────────────────────────────────────────────
  CREATE INDEX IF NOT EXISTS idx_vendor   ON trips(vendor_id);
  CREATE INDEX IF NOT EXISTS idx_time     ON trips(time_id);
  CREATE INDEX IF NOT EXISTS idx_speed    ON trips(speed_kmh);
  CREATE INDEX IF NOT EXISTS idx_dist     ON trips(trip_distance_km);
  CREATE INDEX IF NOT EXISTS idx_pickup   ON trips(pickup_datetime);
  CREATE INDEX IF NOT EXISTS idx_dur      ON trips(trip_duration);
  CREATE INDEX IF NOT EXISTS idx_fare     ON trips(fare_estimate);
`);

