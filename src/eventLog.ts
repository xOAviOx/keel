import type { DB } from "./db";

/**
 * eventLog.ts — the append-only source of truth.
 *
 * Rules:
 *  - We ONLY ever INSERT. Never UPDATE, never DELETE. History is immutable.
 *  - Rows are globally ordered by the autoincrement `id`.
 *  - Within a single run, the `seq` is the deterministic call index (0,1,2,...).
 *    Replay lines up new `ctx.*` calls with logged results by matching `seq`.
 *
 * Payloads are stored as JSON strings. To make `undefined`/`null` round-trip
 * cleanly, activity results are wrapped as `{ value: ... }` by callers; this
 * module just serializes whatever object it's handed.
 */

export type EventType =
  | "WORKFLOW_STARTED"
  | "ACTIVITY_COMPLETED"
  | "ACTIVITY_FAILED"
  | "TIMER_STARTED"
  | "TIMER_FIRED"
  | "SIGNAL_RECEIVED"
  | "SIGNAL_CONSUMED"
  | "WORKFLOW_COMPLETED"
  | "WORKFLOW_FAILED";

export interface EventRow {
  id: number;
  workflow_id: string;
  seq: number;
  type: EventType;
  payload: unknown; // parsed JSON
  created_at: number;
}

interface RawRow {
  id: number;
  workflow_id: string;
  seq: number;
  type: EventType;
  payload: string;
  created_at: number;
}

export class EventLog {
  constructor(private readonly db: DB) {}

  /**
   * Append exactly one event row. Returns the new row id (global order).
   * `payload` is any JSON-serializable value.
   */
  append(workflowId: string, seq: number, type: EventType, payload: unknown): number {
    const info = this.db
      .prepare(
        `INSERT INTO events (workflow_id, seq, type, payload, created_at)
         VALUES (?, ?, ?, ?, ?)`,
      )
      .run(workflowId, seq, type, JSON.stringify(payload ?? null), Date.now());
    return Number(info.lastInsertRowid);
  }

  /** All events for a run, in global append order (id ASC). */
  getEvents(workflowId: string): EventRow[] {
    const rows = this.db
      .prepare(`SELECT * FROM events WHERE workflow_id = ? ORDER BY id ASC`)
      .all(workflowId) as RawRow[];
    return rows.map(parseRow);
  }

  /**
   * The event at a given (seq, type) for a run, or undefined.
   * There should be at most one — a completed activity is written once.
   * If duplicates ever exist, we take the earliest (smallest id) for determinism.
   */
  findEvent(workflowId: string, seq: number, type: EventType): EventRow | undefined {
    const row = this.db
      .prepare(
        `SELECT * FROM events
         WHERE workflow_id = ? AND seq = ? AND type = ?
         ORDER BY id ASC
         LIMIT 1`,
      )
      .get(workflowId, seq, type) as RawRow | undefined;
    return row ? parseRow(row) : undefined;
  }

  /** Convenience: does an event of this (seq, type) already exist? */
  hasEvent(workflowId: string, seq: number, type: EventType): boolean {
    return this.findEvent(workflowId, seq, type) !== undefined;
  }
}

function parseRow(row: RawRow): EventRow {
  return {
    id: row.id,
    workflow_id: row.workflow_id,
    seq: row.seq,
    type: row.type,
    payload: JSON.parse(row.payload),
    created_at: row.created_at,
  };
}
