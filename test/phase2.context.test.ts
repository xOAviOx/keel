import { describe, it, expect } from "vitest";
import { openDb } from "../src/db";
import { EventLog } from "../src/eventLog";
import { WorkflowContext } from "../src/context";

/**
 * Phase 2 — the heart. An activity's side effect (fn) must run exactly ONCE
 * across an initial run + a replay, and replay must return the logged value.
 */
describe("Phase 2: activity memoization via replay", () => {
  it("runs each activity's fn once; replay returns logged values without invoking fn", async () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-heart";

    // External side-effect counter — the thing we must never double-run.
    let counter = 0;

    // The workflow logic: a single activity that increments the counter.
    const workflow = async (ctx: WorkflowContext) => {
      return ctx.activity("increment", () => {
        counter += 1;
        return counter * 10; // returns 10 on the only real execution
      });
    };

    // First run: fresh context.
    const r1 = await workflow(new WorkflowContext(wf, log));
    // Replay: NEW context, SAME workflowId + log.
    const r2 = await workflow(new WorkflowContext(wf, log));

    expect(counter).toBe(1); // fn executed exactly once
    expect(r1).toBe(10);
    expect(r2).toBe(10); // replay returned the logged value, not a re-run

    // And only one ACTIVITY_COMPLETED was ever written.
    const completed = log.getEvents(wf).filter((e) => e.type === "ACTIVITY_COMPLETED");
    expect(completed).toHaveLength(1);
  });

  it("keeps multiple activities aligned by seq across replay", async () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-multi";
    const runs = { a: 0, b: 0, c: 0 };

    const workflow = async (ctx: WorkflowContext) => {
      const a = await ctx.activity("a", () => { runs.a++; return "A"; });
      const b = await ctx.activity("b", () => { runs.b++; return "B"; });
      const c = await ctx.activity("c", () => { runs.c++; return "C"; });
      return `${a}${b}${c}`;
    };

    const first = await workflow(new WorkflowContext(wf, log));
    const replay = await workflow(new WorkflowContext(wf, log));

    expect(first).toBe("ABC");
    expect(replay).toBe("ABC");
    expect(runs).toEqual({ a: 1, b: 1, c: 1 }); // each side effect once
  });

  it("round-trips null and undefined activity results without re-running fn", async () => {
    const log = new EventLog(openDb(":memory:"));
    const wf = "wf-nullish";
    let calls = 0;

    const workflow = async (ctx: WorkflowContext) => {
      const x = await ctx.activity("returnsUndefined", () => { calls++; return undefined; });
      const y = await ctx.activity("returnsNull", () => { calls++; return null; });
      return { x, y };
    };

    const r1 = await workflow(new WorkflowContext(wf, log));
    const r2 = await workflow(new WorkflowContext(wf, log));

    expect(calls).toBe(2);
    expect(r1).toEqual({ x: undefined, y: null });
    expect(r2).toEqual({ x: undefined, y: null });
  });
});
