import { describe, it, expect, beforeEach } from "vitest";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDb } from "../src/db";
import { buildRuntime } from "../src/index";
import { Scheduler } from "../src/scheduler";

function run(args: string[], env: Record<string, string>): Promise<{ code: number | null }> {
  return new Promise((resolveP) => {
    const child = spawn(process.execPath, ["--import", "tsx", "src/cli.ts", ...args], {
      cwd: process.cwd(),
      env: { ...process.env, ...env },
    });
    child.on("close", (code) => resolveP({ code }));
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("Phase 9: concurrency + independent recovery", () => {
  let dir: string;
  let ENGINE_DB: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "durable-p9-"));
    ENGINE_DB = join(dir, "engine.db");
  });

  it("three workflows crash concurrently and each recovers independently, charging once each", async () => {
    // Each workflow gets its OWN charge file (an isolated external side effect),
    // but all share ONE engine DB — exercising concurrent writers.
    const chargeFiles = [0, 1, 2].map((i) => join(dir, `charges-${i}.txt`));

    // Launch 3 crashing starts concurrently.
    const results = await Promise.all(
      [0, 1, 2].map((i) =>
        run(["start", "orderFlow", JSON.stringify({ orderId: `c${i}` })], {
          ENGINE_DB,
          CHARGE_FILE: chargeFiles[i]!,
          CRASH_AFTER: "charge",
        }),
      ),
    );
    for (const r of results) expect(r.code).toBe(1);

    // Each charged exactly once; all three runs are stuck 'running'.
    for (const f of chargeFiles) expect(readFileSync(f, "utf8").trim()).toBe("1");

    const db = openDb(ENGINE_DB);
    const { runtime } = buildRuntime(db);
    expect(runtime.listWorkflows()).toHaveLength(3);
    expect(runtime.listWorkflows().every((w) => w.status === "running")).toBe(true);

    // Single recovery process resumes all three (no re-charge happens on replay).
    const recovered = await runtime.recover();
    expect(recovered).toHaveLength(3);

    // All completed; no charge file changed.
    expect(runtime.listWorkflows().every((w) => w.status === "completed")).toBe(true);
    for (const f of chargeFiles) expect(readFileSync(f, "utf8").trim()).toBe("1");

    // Each run has exactly one charge completion.
    for (const w of runtime.listWorkflows()) {
      const charges = runtime.log
        .getEvents(w.workflow_id)
        .filter((e) => e.type === "ACTIVITY_COMPLETED" && (e.payload as any).name === "charge");
      expect(charges).toHaveLength(1);
    }
    db.close();
  });

  it("scheduler advances several due timers safely in one tick", async () => {
    const db = openDb(ENGINE_DB);
    const { runtime } = buildRuntime(db);

    // Start 3 sleeping workflows with short timers.
    const ids = await Promise.all(
      [0, 1, 2].map((i) => runtime.startWorkflow("sleepFlow", { sleepMs: 120, label: `s${i}` })),
    );
    expect(ids.every((id) => runtime.getState(id)!.status === "sleeping")).toBe(true);

    // Wait until all are due, then a single scheduler pass should advance all 3.
    await sleep(200);
    const scheduler = new Scheduler(runtime, { intervalMs: 20 });
    const advanced = await scheduler.tick();
    expect(advanced.sort()).toEqual([...ids].sort());
    expect(ids.every((id) => runtime.getState(id)!.status === "completed")).toBe(true);
    db.close();
  });
});
