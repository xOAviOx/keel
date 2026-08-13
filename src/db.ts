import Database from "better-sqlite3";
import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";

/**
 * db.ts — opens the embedded SQLite database, turns on WAL, and creates the
 * schema. This one local file is the ENTIRE persistence layer: no server, no
 * cloud, no network. That's what makes the engine free and crash-safe.
 *
 * The `events` table is the append-only source of truth. We NEVER update or
 * delete rows in it — only append. `workflow_state` is a small derived index
 * so the worker can cheaply find runs to resume and timers that are due.
 */

export type DB = Database.Database;

/** Default on-disk location. Overridable via ENGINE_DB (used heavily in tests). */
export function defaultDbPath(): string {
  return process.env.ENGINE_DB ?? resolve(process.cwd(), "data", "engine.db");
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS events (
  id           INTEGER PRIMARY KEY AUTOINCREMENT, -- global order
  workflow_id  TEXT    NOT NULL,                  -- which run
  seq          INTEGER NOT NULL,                  -- deterministic call index within the run
  type         TEXT    NOT NULL,                  -- event type
  payload      TEXT    NOT NULL,                  -- JSON
  created_at   INTEGER NOT NULL                   -- epoch ms, for observability only
);
CREATE INDEX IF NOT EXISTS idx_events_wf_seq ON events (workflow_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_wf_id  ON events (workflow_id, id);

CREATE TABLE IF NOT EXISTS workflow_state (
  workflow_id  TEXT PRIMARY KEY,
  name         TEXT    NOT NULL,   -- registry key
  input        TEXT    NOT NULL,   -- JSON
  status       TEXT    NOT NULL,   -- 'running' | 'sleeping' | 'waiting' | 'completed' | 'failed'
  wake_at      INTEGER,            -- epoch ms when a sleeping wf should be retried, else NULL
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL
);
`;

/** Block the calling thread for `ms` (openDb is synchronous, so is this). */
function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, Math.max(0, ms));
}

/** True for the transient "database is locked" contention we want to retry. */
function isBusyError(err: unknown): boolean {
  const e = err as { code?: string; message?: string } | null;
  return !!e && (e.code === "SQLITE_BUSY" || /database is locked/i.test(e.message ?? ""));
}

/**
 * Apply the two lock-requiring init statements — the WAL switch and the schema
 * creation — retrying on SQLITE_BUSY up to `timeoutMs`. Both are idempotent, so
 * re-running them after losing a lock race is harmless. This is what lets many
 * processes open the same brand-new DB simultaneously without one of them dying
 * with "database is locked".
 */
function initWithRetry(db: DB, timeoutMs = 5000): void {
  const deadline = Date.now() + timeoutMs;
  for (let attempt = 0; ; attempt++) {
    try {
      db.pragma("journal_mode = WAL");
      db.exec(SCHEMA);
      return;
    } catch (err) {
      if (isBusyError(err) && Date.now() < deadline) {
        sleepSync(10 + attempt * 10); // brief linear backoff, then retry
        continue;
      }
      throw err;
    }
  }
}

/**
 * Open a database at `path` (or the default), enable WAL, and apply the schema.
 * Callers that want isolation (tests) pass an explicit path or ":memory:".
 */
export function openDb(path: string = defaultDbPath()): DB {
  if (path !== ":memory:") {
    mkdirSync(dirname(path), { recursive: true });
  }
  const db = new Database(path);
  // Concurrency: multiple worker/CLI processes may open the SAME file. Set the
  // busy timeout first so ordinary write contention (during normal operation)
  // waits for a held lock instead of failing immediately with SQLITE_BUSY.
  db.pragma("busy_timeout = 5000");
  db.pragma("foreign_keys = ON");
  // WAL switch + schema creation both need a brief EXCLUSIVE lock, and — unlike
  // ordinary writes — the WAL transition can return SQLITE_BUSY WITHOUT invoking
  // the busy-timeout handler when several processes initialize the same fresh
  // file at once. So retry these two (idempotent) statements explicitly; that's
  // what makes concurrent cold-start safe rather than a coin-flip.
  initWithRetry(db);
  // Durability: fsync at checkpoint boundaries. NORMAL is the recommended WAL
  // setting — safe against process crashes (our whole point) while fast.
  db.pragma("synchronous = NORMAL");
  return db;
}

/** Lazily-opened shared handle for the app (CLI / worker). */
let shared: DB | undefined;
export function getDb(): DB {
  if (!shared) shared = openDb();
  return shared;
}

export function closeSharedDb(): void {
  if (shared) {
    shared.close();
    shared = undefined;
  }
}
