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

