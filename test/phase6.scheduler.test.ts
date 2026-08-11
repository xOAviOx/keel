import { describe, it, expect, beforeEach } from "vitest";
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDb } from "../src/db";
import { buildRuntime } from "../src/index";
import { Scheduler } from "../src/scheduler";

function run(args: string[], env: Record<string, string>): Promise<{ code: number | null; stdout: string }> {
  return new Promise((resolveP) => {
    const child = spawn(process.execPath, ["--import", "tsx", "src/cli.ts", ...args], {
      cwd: process.cwd(),
      env: { ...process.env, ...env },
    });
    let stdout = "";
    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.on("close", (code) => resolveP({ code, stdout }));
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("Phase 6: scheduler wakes sleeping workflows across a restart", () => {
  let ENGINE_DB: string;

  beforeEach(() => {
    ENGINE_DB = join(mkdtempSync(join(tmpdir(), "durable-p6-")), "engine.db");
  });

  it("survives a full process exit during sleep, then fires activityB", async () => {
    // 1) Start in a child process. It runs activityA, hits sleep(300ms), parks
    //    as 'sleeping', and the process EXITS — gone for the whole sleep window.
    const started = await run(["start", "sleepFlow", JSON.stringify({ sleepMs: 300, label: "p6" })], {
      ENGINE_DB,
    });
    expect(started.code).toBe(0);
    expect(started.stdout).toContain("status: sleeping");

    // 2) A brand-new runtime (fresh process boundary) opens the same DB.
    const db = openDb(ENGINE_DB);
    const { runtime } = buildRuntime(db);

    // Recovery finds nothing to do (the run is 'sleeping', not 'running').
    expect(await runtime.recover()).toEqual([]);

    // Before the timer is due, a tick does nothing.
    expect(await new Scheduler(runtime).tick()).toEqual([]);
    expect(runtime.listWorkflows()[0]!.status).toBe("sleeping");

    // 3) Poll the scheduler until the timer fires and the workflow completes.
    const scheduler = new Scheduler(runtime, { intervalMs: 40 });
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      await scheduler.tick();
      if (runtime.listWorkflows()[0]!.status === "completed") break;
      await sleep(40);
    }

    const state = runtime.listWorkflows()[0]!;
    expect(state.status).toBe("completed");

    // activityB ran (after the restart); activityA ran exactly once (not re-run).
    const events = runtime.log.getEvents(state.workflow_id);
    const completedNames = events
      .filter((e) => e.type === "ACTIVITY_COMPLETED")
      .map((e: any) => e.payload.name);
    expect(completedNames).toEqual(["A", "B"]);
    expect(events.filter((e) => e.type === "TIMER_STARTED")).toHaveLength(1);
    expect(events.filter((e) => e.type === "TIMER_FIRED")).toHaveLength(1);
    db.close();
  });

  it("handles a workflow with two sequential sleeps (each its own timer)", async () => {
    // Register + run entirely in-process using a two-sleep workflow.
    const db = openDb(ENGINE_DB);
    const { runtime, registry } = buildRuntime(db);
    let bReached = false;
    let cReached = false;
    registry.register("twoSleeps", async (ctx) => {
      await ctx.activity("A", () => "a");
      await ctx.sleep(150);
      bReached = true;
      await ctx.activity("B", () => "b");
      await ctx.sleep(150);
      cReached = true;
      await ctx.activity("C", () => "c");
      return "done";
    });

    const id = await runtime.startWorkflow("twoSleeps", {});
    expect(runtime.getState(id)!.status).toBe("sleeping"); // parked at first sleep
    expect(bReached).toBe(false);

    const scheduler = new Scheduler(runtime, { intervalMs: 30 });
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline) {
      await scheduler.tick();
      if (runtime.getState(id)!.status === "completed") break;
      await sleep(30);
    }

    expect(runtime.getState(id)!.status).toBe("completed");
    expect(cReached).toBe(true);
    const events = runtime.log.getEvents(id);
    expect(events.filter((e) => e.type === "TIMER_STARTED")).toHaveLength(2);
    expect(events.filter((e) => e.type === "TIMER_FIRED")).toHaveLength(2);
    db.close();
  });
});
