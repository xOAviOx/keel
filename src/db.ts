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
  status       TEXT    NOT NULL,   -- 'running' | 'sleeping' | 'completed' | 'failed'
  wake_at      INTEGER,            -- epoch ms when a sleeping wf should be retried, else NULL
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL
);
`;

/**
 * Open a database at `path` (or the default), enable WAL, and apply the schema.
 * Callers that want isolation (tests) pass an explicit path or ":memory:".
 */
export function openDb(path: string = defaultDbPath()): DB {
  if (path !== ":memory:") {
    mkdirSync(dirname(path), { recursive: true });
  }
  const db = new Database(path);
  // WAL: concurrent readers + a writer, and crash-safe commits. Essential here.
  db.pragma("journal_mode = WAL");
  // Durability: fsync at checkpoint boundaries. NORMAL is the recommended WAL
  // setting — safe against process crashes (our whole point) while fast.
  db.pragma("synchronous = NORMAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA);
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
