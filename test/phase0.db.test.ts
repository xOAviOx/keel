import { describe, it, expect } from "vitest";
import { openDb } from "../src/db";

/**
 * Phase 0 — schema bootstrap.
 * Opening the DB must create both tables and all indexes.
 */
describe("Phase 0: db schema", () => {
  it("creates both tables and their indexes", () => {
    const db = openDb(":memory:");

    const tables = db
      .prepare(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
      .all()
      .map((r: any) => r.name);
    expect(tables).toContain("events");
    expect(tables).toContain("workflow_state");

    const indexes = db
      .prepare(`SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%' ORDER BY name`)
      .all()
      .map((r: any) => r.name);
    expect(indexes).toContain("idx_events_wf_seq");
    expect(indexes).toContain("idx_events_wf_id");

    db.close();
  });

  it("enables WAL journal mode on a file-backed db", () => {
    const path = `./data/test-phase0-${Date.now()}.db`;
    const db = openDb(path);
    const mode = (db.pragma("journal_mode", { simple: true }) as string).toLowerCase();
    expect(mode).toBe("wal");
    db.close();
  });
});
