import { describe, it, expect } from "vitest";
import { openDb } from "../src/db";
import { EventLog } from "../src/eventLog";
import { WorkflowContext } from "../src/context";
import { DeterminismError, isWorkflowSuspended } from "../src/errors";

describe("Phase 5: durable now/random + determinism guardrails", () => {
  it("now() and random() return identical values across replays", async () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-durable";

    const workflow = async (ctx: WorkflowContext) => {
      const t = ctx.now();
      const r = ctx.random();
      const a = await ctx.activity("noop", () => "ok");
      return { t, r, a };
    };

    const first = await workflow(new WorkflowContext(wf, log));
    const replay = await workflow(new WorkflowContext(wf, log));

    expect(replay.t).toBe(first.t); // same logged timestamp
    expect(replay.r).toBe(first.r); // same logged random
    expect(replay.a).toBe("ok");
  });

  it("now() does not drift even if real time passes between runs", async () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-clock";
    const first = ctxNow(wf, log);
    await new Promise((r) => setTimeout(r, 15));
    const replay = ctxNow(wf, log);
    expect(replay).toBe(first);
  });

  it("sleep() suspends on first hit and continues after TIMER_FIRED", async () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-sleep";
    let reachedB = false;

    const workflow = async (ctx: WorkflowContext) => {
      await ctx.activity("A", () => "a");
      await ctx.sleep(200);
      reachedB = true;
      await ctx.activity("B", () => "b");
    };

    // First run: A logged, then sleep suspends.
    let suspended: unknown;
    try {
      await workflow(new WorkflowContext(wf, log));
    } catch (e) {
      suspended = e;
    }
    expect(isWorkflowSuspended(suspended)).toBe(true);
    expect(reachedB).toBe(false);
    expect(log.getEvents(wf).some((e) => e.type === "TIMER_STARTED")).toBe(true);

    // Replay before firing: A replays, sleep suspends AGAIN (no TIMER_FIRED yet).
    reachedB = false;
    await expect(workflow(new WorkflowContext(wf, log))).rejects.toSatisfy(isWorkflowSuspended);
    expect(reachedB).toBe(false);

    // Simulate the scheduler firing the timer (seq of the sleep is 1: A=0, sleep=1).
    const timerSeq = log.getEvents(wf).find((e) => e.type === "TIMER_STARTED")!.seq;
    log.append(wf, timerSeq, "TIMER_FIRED", {});

    // Replay after firing: sleep returns, B runs.
    await workflow(new WorkflowContext(wf, log));
    expect(reachedB).toBe(true);
    expect(log.getEvents(wf).filter((e) => e.type === "ACTIVITY_COMPLETED").map((e: any) => e.payload.name))
      .toEqual(["A", "B"]);
  });

  it("throws DeterminismError if a sleep seq is later hit by an activity", async () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-drift";

    // First run schedules a timer at seq 0.
    await expect(
      (async () => {
        const ctx = new WorkflowContext(wf, log);
        await ctx.sleep(100);
      })(),
    ).rejects.toSatisfy(isWorkflowSuspended);

    // A "changed" body now calls activity() where the sleep used to be (seq 0).
    const ctx2 = new WorkflowContext(wf, log);
    await expect(ctx2.activity("oops", () => 1)).rejects.toBeInstanceOf(DeterminismError);
  });
});

function ctxNow(wf: string, log: EventLog): number {
  return new WorkflowContext(wf, log).now();
}
