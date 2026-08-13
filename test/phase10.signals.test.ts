import { describe, it, expect, beforeEach } from "vitest";
import { spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDb } from "../src/db";
import { buildRuntime } from "../src/index";
import { EventLog, type EventRow } from "../src/eventLog";
import { WorkflowContext } from "../src/context";
import { DeterminismError } from "../src/errors";

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

const completedResult = (events: EventRow[]) =>
  (events.find((e) => e.type === "WORKFLOW_COMPLETED")?.payload as { result: unknown } | undefined)?.result;

const activityNames = (events: EventRow[]) =>
  events.filter((e) => e.type === "ACTIVITY_COMPLETED").map((e) => (e.payload as { name: string }).name);

describe("Phase 10: durable signals (external input)", () => {
  let ENGINE_DB: string;

  beforeEach(() => {
    ENGINE_DB = join(mkdtempSync(join(tmpdir(), "durable-p10-")), "engine.db");
  });

  it("parks as 'waiting' on waitForSignal, then completes when the signal arrives", async () => {
    const { runtime } = buildRuntime(openDb(ENGINE_DB));

    const id = await runtime.startWorkflow("approvalFlow", { docId: "D1" });
    // submit() ran, then the run blocked on waitForSignal("approve").
    expect(runtime.getState(id)!.status).toBe("waiting");
    expect(activityNames(runtime.log.getEvents(id))).toEqual(["submit"]);

    const res = await runtime.deliverSignal(id, "approve", { approved: true, by: "alice" });
    expect(res.status).toBe("completed");

    const events = runtime.log.getEvents(id);
    expect(activityNames(events)).toEqual(["submit", "finalize"]);
    expect(events.filter((e) => e.type === "SIGNAL_RECEIVED")).toHaveLength(1);
    const consumed = events.filter((e) => e.type === "SIGNAL_CONSUMED");
    expect(consumed).toHaveLength(1);
    expect((consumed[0]!.payload as any).value).toEqual({ approved: true, by: "alice" });
    expect(completedResult(events)).toBe("D1: approved");
  });

  it("buffers a signal delivered BEFORE its wait is reached, then consumes it in order", async () => {
    const db = openDb(ENGINE_DB);
    const { runtime, registry } = buildRuntime(db);
    // Two sequential waits for different signals.
    registry.register("twoWaits", async (ctx) => {
      const a = await ctx.waitForSignal<{ n: number }>("alpha");
      const b = await ctx.waitForSignal<{ n: number }>("beta");
      return { a, b };
    });

    const id = await runtime.startWorkflow("twoWaits", {});
    expect(runtime.getState(id)!.status).toBe("waiting"); // parked at wait("alpha")

    // Deliver "beta" first — the wrong signal for the CURRENT wait. It must be
    // buffered (not consumed) and the run must re-park 'waiting' on "alpha".
    const beta = await runtime.deliverSignal(id, "beta", { n: 2 });
    expect(beta.status).toBe("waiting");
    expect(runtime.log.getEvents(id).filter((e) => e.type === "SIGNAL_CONSUMED")).toHaveLength(0);

    // Now deliver "alpha". The first wait consumes alpha and continues; the
    // second wait finds the already-buffered beta and consumes it immediately.
    const alpha = await runtime.deliverSignal(id, "alpha", { n: 1 });
    expect(alpha.status).toBe("completed");

    const events = runtime.log.getEvents(id);
    expect(completedResult(events)).toEqual({ a: { n: 1 }, b: { n: 2 } });

    // beta was delivered (appended) strictly before alpha — proving buffering.
    const recv = events.filter((e) => e.type === "SIGNAL_RECEIVED");
    const betaId = recv.find((e) => (e.payload as any).signalName === "beta")!.id;
    const alphaId = recv.find((e) => (e.payload as any).signalName === "alpha")!.id;
    expect(betaId).toBeLessThan(alphaId);

    // Each wait bound the correct signal by id.
    const consumed = events.filter((e) => e.type === "SIGNAL_CONSUMED").sort((x, y) => x.seq - y.seq);
    expect(consumed.map((e) => (e.payload as any).signalName)).toEqual(["alpha", "beta"]);
    expect((consumed[0]!.payload as any).signalId).toBe(alphaId);
    expect((consumed[1]!.payload as any).signalId).toBe(betaId);
    db.close();
  });

  it("survives a full process exit while 'waiting', then completes on delivery (submit not re-run)", async () => {
    // Start in a child process: submit() runs, waitForSignal parks 'waiting',
    // and the process EXITS. The run is durably blocked with no process alive.
    const started = await run(["start", "approvalFlow", JSON.stringify({ docId: "P10" })], { ENGINE_DB });
    expect(started.code).toBe(0);
    expect(started.stdout).toContain("status: waiting");

    // Fresh process boundary: a brand-new runtime opens the same DB file.
    const db = openDb(ENGINE_DB);
    const { runtime } = buildRuntime(db);

    // A 'waiting' run is NOT touched by boot recovery (that's only for 'running').
    expect(await runtime.recover()).toEqual([]);
    const id = runtime.listWorkflows()[0]!.workflow_id;
    expect(runtime.getState(id)!.status).toBe("waiting");

    // Delivering the signal resumes the run to completion.
    const res = await runtime.deliverSignal(id, "approve", { approved: false, by: "bob" });
    expect(res.status).toBe("completed");

    const events = runtime.log.getEvents(id);
    // submit() ran exactly once (before the crash); it was NOT re-executed on resume.
    expect(activityNames(events)).toEqual(["submit", "finalize"]);
    expect(completedResult(events)).toBe("P10: rejected");
    db.close();
  });

  it("rejects signals to unknown and terminal workflows", async () => {
    const { runtime } = buildRuntime(openDb(ENGINE_DB));

    await expect(runtime.deliverSignal("no-such-id", "x", null)).rejects.toThrow(/No such workflow/);

    const id = await runtime.startWorkflow("approvalFlow", { docId: "T" });
    await runtime.deliverSignal(id, "approve", { approved: true }); // -> completed
    expect(runtime.getState(id)!.status).toBe("completed");

    // A completed run can't accept more signals.
    await expect(runtime.deliverSignal(id, "approve", { approved: true })).rejects.toThrow(/is completed/);
  });

  it("waitForSignal replays the same bound value and never double-consumes a signal", async () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-sig";
    // A signal is delivered (appended at its marker seq; the wait scans by type).
    log.append(wf, -2, "SIGNAL_RECEIVED", { signalName: "go", value: { x: 1 } });

    const first = await new WorkflowContext(wf, log).waitForSignal("go");
    const replay = await new WorkflowContext(wf, log).waitForSignal("go");
    expect(replay).toEqual(first);
    expect(first).toEqual({ x: 1 });
    // Exactly one binding was written, even across two runs.
    expect(log.getEvents(wf).filter((e) => e.type === "SIGNAL_CONSUMED")).toHaveLength(1);

    // Determinism guard: an activity() where the wait used to live (seq 0) throws.
    await expect(new WorkflowContext(wf, log).activity("oops", () => 1)).rejects.toBeInstanceOf(
      DeterminismError,
    );
  });
});
